from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import launch_chain_timing_repair as timing
from solana_roi.coverage_completeness_repair import _launch_contexts
from solana_roi.ingestion import WalletProfileRegistry
from solana_roi.launch_funding import DexScreenerLaunchCollector
from solana_roi.observation_store import ObservationEventStore
from solana_roi.risk import EntityResolver, RiskDimension, TokenRiskIntelligence


class NoLaunchHttp:
    async def get(self, _url, **_kwargs):
        raise AssertionError("confirmed launch context must bypass pair indexing")


def _risk(tmp_path):
    store = ObservationEventStore(tmp_path / "chain-timing.sqlite3")
    registry = WalletProfileRegistry(store)
    resolver = EntityResolver(store, registry)
    return store, TokenRiskIntelligence(store, entity_resolver=resolver, registry=registry)


def _buy(store, *, mint: str, at: datetime) -> None:
    store.record_swap(
        signature=f"buy-{mint}",
        slot=1,
        observed_at=at.isoformat(),
        received_at=at.isoformat(),
        wallet="buyer-a",
        token_mint=mint,
        side="buy",
        token_amount=1000,
        native_amount_sol=1.0,
        reference_price_sol=0.001,
        ingestion_latency_ms=0,
        source="test",
    )


def _seed_live_context(collector, *, mint, created, observed, signature):
    collector.seed_coverage_context(
        mint,
        created_at=created,
        observed_at=observed,
        complete=True,
    )
    _launch_contexts(collector)[mint]["launch_signature"] = signature


def _timing(store, *, signature, created, launch_slot, head_slot, head_seconds=None):
    timing._write_timing_row(
        store,
        signature=signature,
        launch_slot=launch_slot,
        head_slot=head_slot,
        head_block_time=(created + timedelta(seconds=head_seconds)).timestamp()
        if head_seconds is not None
        else None,
        provider="publicnode",
        slot_latency_ms=20.0,
        block_time_latency_ms=20.0 if head_seconds is not None else None,
        sampled_at=created.isoformat(),
        status="complete",
    )


def test_live_launch_at_or_ahead_of_confirmed_head_ignores_host_clock_offset(tmp_path):
    store, risk = _risk(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=30)
    _buy(store, mint="mint-a", at=created + timedelta(seconds=1))

    collector = DexScreenerLaunchCollector(risk, client=NoLaunchHttp())
    # Deliberately make the host-wall-clock receipt look twenty seconds late. The
    # production v4 proof must use chain ordering instead of this cross-clock delta.
    _seed_live_context(
        collector,
        mint="mint-a",
        created=created,
        observed=created + timedelta(seconds=20),
        signature="launch-a",
    )
    _timing(
        store,
        signature="launch-a",
        created=created,
        launch_slot=100,
        head_slot=99,
    )

    assessed = created + timedelta(seconds=25)
    assert asyncio.run(collector.collect("mint-a", assessed)) is True
    coverage = store.recent_program_coverage(10)[0]
    assert coverage["launch_near_creation"] is True
    assert coverage["launch_lag_ms"] == 0.0
    assert coverage["early_buyers_complete"] is True
    assert collector.policy.max_pair_stream_lag_seconds == 3.0


def test_chain_duration_above_existing_three_second_threshold_fails_closed(tmp_path):
    store, risk = _risk(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=30)
    _buy(store, mint="mint-b", at=created + timedelta(seconds=1))

    collector = DexScreenerLaunchCollector(risk, client=NoLaunchHttp())
    _seed_live_context(
        collector,
        mint="mint-b",
        created=created,
        observed=created,
        signature="launch-b",
    )
    _timing(
        store,
        signature="launch-b",
        created=created,
        launch_slot=100,
        head_slot=110,
        head_seconds=4.0,
    )

    assessed = created + timedelta(seconds=25)
    assert asyncio.run(collector.collect("mint-b", assessed)) is False
    coverage = store.recent_program_coverage(10)[0]
    assert coverage["launch_near_creation"] is False
    assert coverage["launch_lag_ms"] == 4000.0
    assert store.latest_risk_evidence(
        "mint-b", RiskDimension.LAUNCH.value, as_of_received_at=assessed.isoformat()
    ) is None


def test_missing_production_chain_timing_proof_fails_closed(tmp_path):
    store, risk = _risk(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=30)
    _buy(store, mint="mint-c", at=created + timedelta(seconds=1))

    collector = DexScreenerLaunchCollector(risk, client=NoLaunchHttp())
    _seed_live_context(
        collector,
        mint="mint-c",
        created=created,
        observed=created + timedelta(seconds=1),
        signature="launch-c",
    )

    assessed = created + timedelta(seconds=25)
    assert asyncio.run(collector.collect("mint-c", assessed)) is False
    coverage = store.recent_program_coverage(10)[0]
    assert coverage["launch_near_creation"] is False
    assert coverage["launch_lag_ms"] is None


def test_confirmed_head_sampler_is_read_only_hedged_and_persists_first_receipt_proof(tmp_path):
    store, _risk_plane = _risk(tmp_path)
    calls: list[tuple[str, bool]] = []

    class Rpc:
        async def call_with_meta(self, method, _params, *, hedge=False):
            calls.append((method, bool(hedge)))
            if method == "getSlot":
                return 99, "publicnode", 1.0
            raise AssertionError(method)

    plane = SimpleNamespace(store=store, rpc=Rpc())
    asyncio.run(timing._sample_confirmed_chain_head(plane, "launch-d", 100))

    row = timing._timing_row(store, "launch-d")
    assert row is not None
    assert row["status"] == "complete"
    assert row["launch_slot"] == 100
    assert row["head_slot"] == 99
    assert row["head_block_time"] is None
    assert calls == [("getSlot", True)]
    assert getattr(plane, "_roi_launch_timing_complete") == 1


def test_direct_seed_compatibility_keeps_existing_v3_semantics(tmp_path):
    store, risk = _risk(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=12)
    _buy(store, mint="mint-e", at=created + timedelta(seconds=1))

    collector = DexScreenerLaunchCollector(risk, client=NoLaunchHttp())
    collector.seed_coverage_context(
        "mint-e",
        created_at=created,
        observed_at=created + timedelta(seconds=1),
        complete=True,
    )
    assessed = created + timedelta(seconds=10)

    assert asyncio.run(collector.collect("mint-e", assessed)) is True
    coverage = store.recent_program_coverage(10)[0]
    assert coverage["launch_lag_ms"] == 1000.0
