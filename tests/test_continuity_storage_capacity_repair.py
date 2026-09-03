from __future__ import annotations

import asyncio
from datetime import timedelta
from types import SimpleNamespace

from solana_roi import direct_solana as direct_solana_module
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi import poll_watermark_repair as watermark
from solana_roi.continuity_storage_capacity_repair import (
    HYDRATION_METRIC_RETENTION_SECONDS,
    ROUTINE_POLL_PHASE_SPREAD_SECONDS,
    TERMINAL_QUEUE_RETENTION_SECONDS,
    _assigned_endpoint,
    _prune_operational_rows_once,
    _sharded_slot_poll_page,
    _target_phase_seconds,
)
from solana_roi.direct_solana import DirectSolanaIngestionPlane, DirectSolanaJournal, WatchTarget
from solana_roi.observation_store import ObservationEventStore
from solana_roi.solana_rpc import RpcEndpoint


def _targets() -> tuple[WatchTarget, ...]:
    return tuple(
        WatchTarget("program", f"program-{index}", "PUMP_FUN")
        for index in range(7)
    ) + tuple(
        WatchTarget("scout", f"scout-{index}", None)
        for index in range(3)
    )


def test_routine_poll_targets_are_evenly_sharded_and_phase_staggered():
    endpoints = (
        RpcEndpoint("publicnode", "https://public.example", "wss://public.example"),
        RpcEndpoint("solana-mainnet", "https://official.example", "wss://official.example"),
    )
    plane = SimpleNamespace(
        rpc=SimpleNamespace(endpoints=endpoints),
        endpoints=endpoints,
        watch_targets=_targets(),
    )

    assignments = [_assigned_endpoint(plane, target).name for target in plane.watch_targets]
    assert assignments.count("publicnode") == 5
    assert assignments.count("solana-mainnet") == 5
    assert assignments == ["publicnode", "solana-mainnet"] * 5

    phases = [_target_phase_seconds(plane, target) for target in plane.watch_targets]
    assert phases[0] == 0.0
    assert phases[-1] == ROUTINE_POLL_PHASE_SPREAD_SECONDS * 0.9
    assert all(later > earlier for earlier, later in zip(phases, phases[1:]))
    assert live_poll.POLL_INTERVAL_SECONDS == 4.0


def test_sharded_page_preserves_confirmed_watermark_contract_without_cross_provider_fallback(monkeypatch):
    target = WatchTarget("program", "program-a", "PUMP_FUN")
    calls: list[tuple[str, list[object], bool]] = []

    class FakePool:
        async def call_with_meta(self, method, params, *, hedge=False):
            calls.append((method, params, hedge))
            return [{"signature": "sig-a", "slot": 123}], "publicnode", 7.0

    monkeypatch.setattr(
        "solana_roi.continuity_storage_capacity_repair._routine_poll_pool",
        lambda _plane, _target: FakePool(),
    )

    rows, provider, latency = asyncio.run(
        _sharded_slot_poll_page(
            SimpleNamespace(),
            target,
            before="before-a",
            min_context_slot=100,
            limit=1000,
        )
    )

    assert rows == [{"signature": "sig-a", "slot": 123}]
    assert provider == "publicnode"
    assert latency == 7.0
    assert len(calls) == 1
    method, params, hedge = calls[0]
    assert method == "getSignaturesForAddress"
    assert hedge is False
    assert params[0] == "program-a"
    assert params[1] == {
        "commitment": "confirmed",
        "limit": 1000,
        "before": "before-a",
        "minContextSlot": 100,
    }


def test_operational_retention_prunes_only_old_terminal_queue_and_nonhistorical_metrics(tmp_path):
    store = ObservationEventStore(tmp_path / "capacity.sqlite3")
    DirectSolanaJournal(store)
    now = direct_solana_module.utcnow()
    old_queue = (now - timedelta(seconds=TERMINAL_QUEUE_RETENTION_SECONDS + 60.0)).isoformat()
    old_metric = (now - timedelta(seconds=HYDRATION_METRIC_RETENTION_SECONDS + 60.0)).isoformat()
    fresh = now.isoformat()

    with store._lock, store.db:
        for signature, status, updated_at in (
            ("old-complete", "complete", old_queue),
            ("old-failed", "failed", old_queue),
            ("old-pending", "pending", old_queue),
            ("old-processing", "processing", old_queue),
            ("fresh-complete", "complete", fresh),
        ):
            store.db.execute(
                "INSERT INTO direct_solana_hydration_queue("
                "signature, slot, trigger_received_at, source_hint, priority, reason, status, attempts, last_error, updated_at) "
                "VALUES (?, 1, ?, 'PUMP_FUN', 20, 'test', ?, 0, NULL, ?)",
                (signature, old_queue, status, updated_at),
            )

        for signature, hydrated_at, historical in (
            ("old-metric", old_metric, 0),
            ("old-historical", old_metric, 1),
            ("fresh-metric", fresh, 0),
        ):
            store.db.execute(
                "INSERT INTO direct_solana_hydration_metrics("
                "signature, source, trigger_received_at, hydrated_at, rpc_provider, rpc_latency_ms, "
                "total_hydration_ms, normalized, candidate_context_prefilled, historical_recovery) "
                "VALUES (?, 'PUMP_FUN', ?, ?, 'publicnode', 1.0, 1.0, 1, 0, ?)",
                (signature, old_metric, hydrated_at, historical),
            )

    queue_pruned, metrics_pruned = _prune_operational_rows_once(SimpleNamespace(store=store))
    assert queue_pruned == 2
    assert metrics_pruned == 1

    with store._lock:
        queue = {
            str(row["signature"]): str(row["status"])
            for row in store.db.execute(
                "SELECT signature, status FROM direct_solana_hydration_queue"
            ).fetchall()
        }
        metrics = {
            str(row["signature"])
            for row in store.db.execute(
                "SELECT signature FROM direct_solana_hydration_metrics"
            ).fetchall()
        }
        maintenance = store.db.execute(
            "SELECT queue_rows_pruned, metric_rows_pruned FROM direct_solana_storage_maintenance WHERE id=1"
        ).fetchone()

    assert "old-complete" not in queue
    assert "old-failed" not in queue
    assert queue["old-pending"] == "pending"
    assert queue["old-processing"] == "processing"
    assert queue["fresh-complete"] == "complete"
    assert "old-metric" not in metrics
    assert "old-historical" in metrics
    assert "fresh-metric" in metrics
    assert int(maintenance["queue_rows_pruned"]) == 2
    assert int(maintenance["metric_rows_pruned"]) == 1
    store.close()


def test_production_composition_preserves_continuity_and_paper_boundaries():
    from solana_roi.production import app  # noqa: F401
    from solana_roi.config import BASELINE
    from solana_roi.continuity_storage_capacity_repair import _sharded_slot_poll_page

    assert live_poll._poll_target is lease._leased_poll_target
    assert watermark._slot_poll_page is _sharded_slot_poll_page
    assert getattr(DirectSolanaIngestionPlane.run, "_roi_storage_capacity_maintenance", False) is True
    assert getattr(DirectSolanaIngestionPlane.status, "_roi_continuity_storage_capacity", False) is True
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000
    assert BASELINE.max_chase_fraction == 0.15
