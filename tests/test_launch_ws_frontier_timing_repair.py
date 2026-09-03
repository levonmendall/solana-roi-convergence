from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from solana_roi import launch_ws_frontier_timing_repair as frontier
from solana_roi.launch_funding import LaunchFundingPolicy
from solana_roi.observation_store import ObservationEventStore


def _store(tmp_path):
    return ObservationEventStore(tmp_path / "ws-frontier.sqlite3")


def test_recent_frontier_at_or_before_launch_proves_zero_chain_frontier_lag(tmp_path):
    store = _store(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0)
    assert frontier._write_frontier_row(
        store,
        signature="launch-a",
        launch_slot=101,
        frontier_slot=100,
        frontier_provider="publicnode",
        frontier_age_ms=500.0,
        captured_at=created.isoformat(),
        status="captured",
    )

    lag, proof = frontier._ws_frontier_lag_seconds(
        store,
        signature="launch-a",
        created_at=created,
        max_age_seconds=3.0,
    )

    assert lag == 0.0
    assert proof == "recent-preexisting-websocket-frontier-not-ahead"
    assert frontier.FRONTIER_MAX_AGE_SECONDS == 3.0
    assert LaunchFundingPolicy().max_pair_stream_lag_seconds == 3.0
    assert LaunchFundingPolicy().launch_window_seconds == 8.0


def test_frontier_ahead_uses_only_fixed_chain_time_delta(tmp_path):
    store = _store(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0)
    frontier._write_frontier_row(
        store,
        signature="launch-b",
        launch_slot=100,
        frontier_slot=105,
        frontier_provider="solana-mainnet",
        frontier_age_ms=2500.0,
        captured_at=created.isoformat(),
        status="captured",
    )
    frontier._set_frontier_block_time(
        store,
        "launch-b",
        block_time=(created + timedelta(seconds=2)).timestamp(),
    )

    lag, proof = frontier._ws_frontier_lag_seconds(
        store,
        signature="launch-b",
        created_at=created,
        max_age_seconds=3.0,
    )

    assert lag == pytest.approx(2.0)
    assert proof == "preexisting-websocket-chain-frontier-lag"
    # The 2.5-second host age is a freshness gate only; it is not added to lag.
    assert lag < 3.0


def test_stale_or_missing_frontier_fails_closed(tmp_path):
    store = _store(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0)
    frontier._write_frontier_row(
        store,
        signature="launch-c",
        launch_slot=100,
        frontier_slot=99,
        frontier_provider="publicnode",
        frontier_age_ms=3001.0,
        captured_at=created.isoformat(),
        status="captured",
    )

    lag, proof = frontier._ws_frontier_lag_seconds(
        store,
        signature="launch-c",
        created_at=created,
        max_age_seconds=3.0,
    )
    assert lag is None
    assert proof == "stale_preexisting_websocket_frontier"

    lag, proof = frontier._ws_frontier_lag_seconds(
        store,
        signature="missing",
        created_at=created,
        max_age_seconds=3.0,
    )
    assert lag is None
    assert proof == "missing_preexisting_websocket_frontier"


def test_launch_cannot_use_its_own_notification_as_preexisting_frontier(tmp_path):
    store = _store(tmp_path)
    plane = SimpleNamespace(store=store)

    # Capture happens before the current launch notification advances the frontier.
    assert frontier._capture_preexisting_frontier(
        plane,
        "launch-d",
        500,
        receipt_monotonic=10.0,
    ) is False
    frontier._observe_frontier(plane, "publicnode", 500, 10.0)

    row = frontier._frontier_row(store, "launch-d")
    assert row is not None
    assert row["status"] == "missing_recent_frontier"
    assert row["frontier_slot"] is None


def test_highest_recent_preexisting_provider_frontier_is_snapshotted(tmp_path):
    store = _store(tmp_path)
    plane = SimpleNamespace(store=store)
    frontier._observe_frontier(plane, "publicnode", 700, 18.0)
    frontier._observe_frontier(plane, "solana-mainnet", 703, 19.0)

    assert frontier._capture_preexisting_frontier(
        plane,
        "launch-e",
        704,
        receipt_monotonic=20.0,
    ) is True
    row = frontier._frontier_row(store, "launch-e")
    assert row is not None
    assert row["frontier_slot"] == 703
    assert row["frontier_provider"] == "solana-mainnet"
    assert float(row["frontier_age_ms"]) == pytest.approx(1000.0)
