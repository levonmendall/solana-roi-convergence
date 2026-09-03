from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from solana_roi.ingestion import WalletProfileRegistry
from solana_roi.launch_funding import DexScreenerLaunchCollector
from solana_roi.observation_store import ObservationEventStore
from solana_roi.risk import EntityResolver, RiskDimension, TokenRiskIntelligence


class NoLaunchHttp:
    async def get(self, _url, **_kwargs):
        raise AssertionError("live coverage context must bypass pair indexing")


def _risk(tmp_path):
    store = ObservationEventStore(tmp_path / "launch-lateness.sqlite3")
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


def test_negative_cross_clock_delta_is_not_counted_as_late(tmp_path):
    store, risk = _risk(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=12)
    _buy(store, mint="mint-a", at=created + timedelta(seconds=1))

    collector = DexScreenerLaunchCollector(risk, client=NoLaunchHttp())
    collector.seed_coverage_context(
        "mint-a",
        created_at=created,
        observed_at=created - timedelta(seconds=5),
        complete=True,
    )
    assessed = created + timedelta(seconds=10)

    assert asyncio.run(collector.collect("mint-a", assessed)) is True
    coverage = store.recent_program_coverage(10)[0]
    assert coverage["launch_near_creation"] is True
    assert coverage["launch_lag_ms"] == 0.0
    assert coverage["early_buyers_complete"] is True
    assert store.latest_risk_evidence(
        "mint-a", RiskDimension.LAUNCH.value, as_of_received_at=assessed.isoformat()
    ) is not None


def test_positive_lateness_above_existing_threshold_still_fails_closed(tmp_path):
    store, risk = _risk(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=12)
    _buy(store, mint="mint-b", at=created + timedelta(seconds=1))

    collector = DexScreenerLaunchCollector(risk, client=NoLaunchHttp())
    collector.seed_coverage_context(
        "mint-b",
        created_at=created,
        observed_at=created + timedelta(seconds=4),
        complete=True,
    )
    assessed = created + timedelta(seconds=10)

    assert collector.policy.max_pair_stream_lag_seconds == 3.0
    assert asyncio.run(collector.collect("mint-b", assessed)) is False
    coverage = store.recent_program_coverage(10)[0]
    assert coverage["launch_near_creation"] is False
    assert coverage["launch_lag_ms"] == 4000.0
    assert coverage["early_buyers_complete"] is True
    assert store.latest_risk_evidence(
        "mint-b", RiskDimension.LAUNCH.value, as_of_received_at=assessed.isoformat()
    ) is None
