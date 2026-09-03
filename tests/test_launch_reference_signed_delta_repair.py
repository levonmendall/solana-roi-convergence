from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from solana_roi import launch_reference_signed_delta_repair as signed
from solana_roi import launch_reference_timing_repair as reference
from solana_roi import live_poll_redundancy as live_poll
from solana_roi.ingestion import WalletProfileRegistry
from solana_roi.launch_funding import DexScreenerLaunchCollector
from solana_roi.observation_store import ObservationEventStore
from solana_roi.risk import EntityResolver, TokenRiskIntelligence


def _store(tmp_path):
    return ObservationEventStore(tmp_path / "signed-reference-timing.sqlite3")


def _collector(tmp_path):
    store = _store(tmp_path)
    registry = WalletProfileRegistry(store)
    resolver = EntityResolver(store, registry)
    risk = TokenRiskIntelligence(store, entity_resolver=resolver, registry=registry)
    return DexScreenerLaunchCollector(risk)


def _row(store, *, signature, launch_slot, head_slot, created, rpc_ms, age_ms, head_offset_seconds):
    assert reference._write_reference_row(
        store,
        signature=signature,
        launch_slot=launch_slot,
        reference_head_slot=head_slot,
        provider="publicnode",
        rpc_latency_ms=rpc_ms,
        reference_age_ms=age_ms,
        captured_at=created.isoformat(),
    )
    reference._set_reference_block_time(
        store,
        signature,
        block_time=(created + timedelta(seconds=head_offset_seconds)).timestamp(),
    )


def test_earlier_preexisting_head_subtracts_immutable_chain_progress(tmp_path):
    store = _store(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0)
    _row(
        store,
        signature="launch-a",
        launch_slot=100,
        head_slot=99,
        created=created,
        rpc_ms=800.0,
        age_ms=400.0,
        head_offset_seconds=-1.0,
    )

    lag, proof = signed._reference_lag_seconds_signed(
        store,
        signature="launch-a",
        created_at=created,
    )

    assert lag == pytest.approx(0.2)
    assert proof == "preexisting-confirmed-head-signed-chain-upper-bound"
    assert _collector(tmp_path).policy.max_pair_stream_lag_seconds == 3.0


def test_signed_chain_progress_can_reduce_upper_bound_only_to_zero(tmp_path):
    store = _store(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0)
    _row(
        store,
        signature="launch-b",
        launch_slot=100,
        head_slot=98,
        created=created,
        rpc_ms=700.0,
        age_ms=300.0,
        head_offset_seconds=-2.0,
    )

    lag, _proof = signed._reference_lag_seconds_signed(
        store,
        signature="launch-b",
        created_at=created,
    )

    assert lag == 0.0


def test_later_preexisting_head_still_adds_chain_duration(tmp_path):
    store = _store(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0)
    _row(
        store,
        signature="launch-c",
        launch_slot=100,
        head_slot=105,
        created=created,
        rpc_ms=700.0,
        age_ms=300.0,
        head_offset_seconds=1.0,
    )

    lag, _proof = signed._reference_lag_seconds_signed(
        store,
        signature="launch-c",
        created_at=created,
    )

    assert lag == pytest.approx(2.0)


def test_upper_bound_above_unchanged_three_seconds_still_fails_policy(tmp_path):
    store = _store(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0)
    _row(
        store,
        signature="launch-d",
        launch_slot=100,
        head_slot=110,
        created=created,
        rpc_ms=900.0,
        age_ms=600.0,
        head_offset_seconds=2.0,
    )

    lag, _proof = signed._reference_lag_seconds_signed(
        store,
        signature="launch-d",
        created_at=created,
    )

    assert lag == pytest.approx(3.5)
    assert lag > _collector(tmp_path).policy.max_pair_stream_lag_seconds


def test_missing_or_incomplete_reference_block_time_fails_closed(tmp_path):
    store = _store(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0)
    reference._write_reference_row(
        store,
        signature="launch-e",
        launch_slot=100,
        reference_head_slot=99,
        provider="publicnode",
        rpc_latency_ms=500.0,
        reference_age_ms=200.0,
        captured_at=created.isoformat(),
    )

    lag, proof = signed._reference_lag_seconds_signed(
        store,
        signature="launch-e",
        created_at=created,
    )

    assert lag is None
    assert proof == "incomplete_preexisting_chain_reference"


def test_inconsistent_slot_and_block_time_order_fails_closed(tmp_path):
    store = _store(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0)
    _row(
        store,
        signature="launch-f",
        launch_slot=100,
        head_slot=105,
        created=created,
        rpc_ms=500.0,
        age_ms=200.0,
        head_offset_seconds=-1.0,
    )

    lag, proof = signed._reference_lag_seconds_signed(
        store,
        signature="launch-f",
        created_at=created,
    )

    assert lag is None
    assert proof == "non_monotonic_reference_block_time"


def test_hydration_resolves_actual_reference_block_time_even_when_head_is_before_launch(tmp_path, monkeypatch):
    store = _store(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0)
    reference._write_reference_row(
        store,
        signature="launch-g",
        launch_slot=100,
        reference_head_slot=99,
        provider="publicnode",
        rpc_latency_ms=500.0,
        reference_age_ms=200.0,
        captured_at=created.isoformat(),
    )

    async def previous(_self, *, mint, source, launch_signature, created_at):
        assert mint == "mint-g"
        assert source == "PUMP_FUN"
        assert launch_signature == "launch-g"
        return 0, True, 0

    class Pool:
        async def call_with_meta(self, method, params, *, hedge=False):
            assert method == "getBlockTime"
            assert params == [99]
            assert hedge is True
            return (created - timedelta(seconds=1)).timestamp(), "publicnode", 10.0

    monkeypatch.setattr(reference, "_PRE_CHAIN_HYDRATE_MINT_LAUNCH_CONTEXT", previous)
    monkeypatch.setattr(live_poll, "_poll_rpc", lambda _self: Pool())
    monkeypatch.setattr(signed.bridge, "_raw_collectors", lambda _self: SimpleNamespace(launch=None))

    plane = SimpleNamespace(store=store)
    asyncio.run(
        signed._hydrate_mint_launch_context_with_signed_reference(
            plane,
            mint="mint-g",
            source="PUMP_FUN",
            launch_signature="launch-g",
            created_at=created,
        )
    )

    row = reference._reference_row(store, "launch-g")
    assert row is not None
    assert row["status"] == "complete"
    assert float(row["reference_head_block_time"]) == pytest.approx(
        (created - timedelta(seconds=1)).timestamp()
    )
