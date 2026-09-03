from __future__ import annotations

import sqlite3
import time
from datetime import timedelta
from types import SimpleNamespace

from solana_roi import direct_solana as direct_solana_module
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi import continuity_storage_capacity_repair as capacity
from solana_roi.direct_solana import DirectSolanaJournal
from solana_roi.observation_store import ObservationEventStore
from solana_roi.storage_maintenance_liveness_repair import (
    LIVENESS_SAFE_BATCH_ROWS,
    MAINTENANCE_SQLITE_BUSY_TIMEOUT_MS,
    _nonblocking_prune_once,
    _nonblocking_snapshot,
    _nonblocking_storage_maintenance_worker,
    install_storage_maintenance_liveness_repair,
)


def _seed(store: ObservationEventStore) -> None:
    DirectSolanaJournal(store)
    now = direct_solana_module.utcnow()
    old_queue = (
        now - timedelta(seconds=capacity.TERMINAL_QUEUE_RETENTION_SECONDS + 60.0)
    ).isoformat()
    old_metric = (
        now - timedelta(seconds=capacity.HYDRATION_METRIC_RETENTION_SECONDS + 60.0)
    ).isoformat()
    with store._lock, store.db:
        for signature, status in (
            ("old-complete", "complete"),
            ("old-failed", "failed"),
            ("old-pending", "pending"),
            ("old-processing", "processing"),
        ):
            store.db.execute(
                "INSERT INTO direct_solana_hydration_queue("
                "signature, slot, trigger_received_at, source_hint, priority, reason, status, attempts, last_error, updated_at) "
                "VALUES (?, 1, ?, 'PUMP_FUN', 20, 'test', ?, 0, NULL, ?)",
                (signature, old_queue, status, old_queue),
            )
        for signature, historical in (("old-metric", 0), ("old-historical", 1)):
            store.db.execute(
                "INSERT INTO direct_solana_hydration_metrics("
                "signature, source, trigger_received_at, hydrated_at, rpc_provider, rpc_latency_ms, "
                "total_hydration_ms, normalized, candidate_context_prefilled, historical_recovery) "
                "VALUES (?, 'PUMP_FUN', ?, ?, 'publicnode', 1.0, 1.0, 1, 0, ?)",
                (signature, old_metric, old_metric, historical),
            )


class _ExplodingLock:
    def __enter__(self):
        raise AssertionError("maintenance touched the canonical process RLock")

    def __exit__(self, *_args):
        return False


def test_nonblocking_prune_uses_independent_connection_not_process_rlock(tmp_path):
    store = ObservationEventStore(tmp_path / "liveness.sqlite3")
    _seed(store)
    original_lock = store._lock
    store._lock = _ExplodingLock()
    try:
        queue_rows, metric_rows, busy = _nonblocking_prune_once(SimpleNamespace(store=store))
    finally:
        store._lock = original_lock

    assert busy is False
    assert queue_rows == 2
    assert metric_rows == 1
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
    assert "old-complete" not in queue
    assert "old-failed" not in queue
    assert queue["old-pending"] == "pending"
    assert queue["old-processing"] == "processing"
    assert "old-metric" not in metrics
    assert "old-historical" in metrics
    store.close()


def test_live_writer_contention_is_skipped_without_five_second_busy_wait(tmp_path):
    store = ObservationEventStore(tmp_path / "busy.sqlite3")
    _seed(store)
    store.db.execute("BEGIN IMMEDIATE")
    started = time.perf_counter()
    try:
        queue_rows, metric_rows, busy = _nonblocking_prune_once(SimpleNamespace(store=store))
    finally:
        store.db.execute("ROLLBACK")
    elapsed = time.perf_counter() - started

    assert busy is True
    assert queue_rows == 0
    assert metric_rows == 0
    assert elapsed < 0.5
    store.close()


def test_nonblocking_snapshot_never_requires_process_rlock(tmp_path):
    store = ObservationEventStore(tmp_path / "snapshot.sqlite3")
    _seed(store)
    plane = SimpleNamespace(store=store)
    _nonblocking_prune_once(plane)
    original_lock = store._lock
    store._lock = _ExplodingLock()
    try:
        payload = _nonblocking_snapshot(plane)
    finally:
        store._lock = original_lock

    assert payload["nonblocking_liveness_repair_installed"] is True
    assert payload["shared_process_rlock_used_by_maintenance"] is False
    assert payload["sqlite_busy_wait_ms"] == MAINTENANCE_SQLITE_BUSY_TIMEOUT_MS == 0
    assert payload["maintenance_batch_rows"] == LIVENESS_SAFE_BATCH_ROWS
    assert payload["pending_or_processing_queue_rows_pruned"] is False
    assert payload["canonical_evidence_pruned"] is False
    store.close()


def test_install_keeps_canonical_continuity_worker_identity():
    install_storage_maintenance_liveness_repair()
    assert live_poll._poll_target is lease._leased_poll_target
    assert capacity._storage_maintenance_worker is _nonblocking_storage_maintenance_worker
    assert capacity.MAINTENANCE_BATCH_ROWS == LIVENESS_SAFE_BATCH_ROWS
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000
