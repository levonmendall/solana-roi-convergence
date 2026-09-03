from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from solana_roi import launch_reference_timing_repair as reference
from solana_roi.ingestion import WalletProfileRegistry
from solana_roi.launch_funding import DexScreenerLaunchCollector
from solana_roi.observation_store import ObservationEventStore
from solana_roi.risk import EntityResolver, TokenRiskIntelligence


class NoLaunchHttp:
    async def get(self, _url, **_kwargs):
        raise AssertionError("not used")


def _store(tmp_path):
    return ObservationEventStore(tmp_path / "reference-timing.sqlite3")


def _collector(tmp_path):
    store = _store(tmp_path)
    registry = WalletProfileRegistry(store)
    resolver = EntityResolver(store, registry)
    risk = TokenRiskIntelligence(store, entity_resolver=resolver, registry=registry)
    return DexScreenerLaunchCollector(risk, client=NoLaunchHttp())


def test_preexisting_head_before_launch_uses_rtt_plus_reference_age_upper_bound(tmp_path):
    store = _store(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0)
    assert reference._write_reference_row(
        store,
        signature="launch-a",
        launch_slot=100,
        reference_head_slot=99,
        provider="publicnode",
        rpc_latency_ms=800.0,
        reference_age_ms=400.0,
        captured_at=created.isoformat(),
    ) is True

    lag, proof = reference._reference_lag_seconds(
        store,
        signature="launch-a",
        created_at=created,
    )

    assert lag == pytest.approx(1.2)
    assert proof == "preexisting-confirmed-head-upper-bound"
    assert _collector(tmp_path).policy.max_pair_stream_lag_seconds == 3.0


def test_preexisting_later_head_adds_chain_duration_without_probe_latency(tmp_path):
    store = _store(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0)
    reference._write_reference_row(
        store,
        signature="launch-b",
        launch_slot=100,
        reference_head_slot=105,
        provider="publicnode",
        rpc_latency_ms=700.0,
        reference_age_ms=300.0,
        captured_at=created.isoformat(),
    )
    reference._set_reference_block_time(
        store,
        "launch-b",
        block_time=(created + timedelta(seconds=1)).timestamp(),
    )

    lag, proof = reference._reference_lag_seconds(
        store,
        signature="launch-b",
        created_at=created,
    )

    assert lag == pytest.approx(2.0)
    assert proof == "preexisting-confirmed-head-upper-bound"


def test_upper_bound_above_unchanged_three_seconds_remains_rejected_by_policy(tmp_path):
    store = _store(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0)
    reference._write_reference_row(
        store,
        signature="launch-c",
        launch_slot=100,
        reference_head_slot=110,
        provider="publicnode",
        rpc_latency_ms=900.0,
        reference_age_ms=600.0,
        captured_at=created.isoformat(),
    )
    reference._set_reference_block_time(
        store,
        "launch-c",
        block_time=(created + timedelta(seconds=2)).timestamp(),
    )

    lag, _proof = reference._reference_lag_seconds(
        store,
        signature="launch-c",
        created_at=created,
    )

    assert lag == pytest.approx(3.5)
    assert lag > _collector(tmp_path).policy.max_pair_stream_lag_seconds


def test_missing_preexisting_reference_fails_closed(tmp_path):
    store = _store(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0)

    lag, proof = reference._reference_lag_seconds(
        store,
        signature="missing",
        created_at=created,
    )

    assert lag is None
    assert proof == "missing_preexisting_chain_reference"
