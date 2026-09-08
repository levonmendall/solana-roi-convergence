from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace

from solana_roi import continuity_storage_capacity_repair as storage_capacity
from solana_roi import storage_maintenance_lock_isolation_repair as repair


class _ForbiddenCanonicalLock:
    def __enter__(self):
        raise AssertionError("isolated maintenance must not acquire canonical store._lock")

    def __exit__(self, exc_type, exc, tb):
        return False


def _seed(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(
            "CREATE TABLE direct_solana_hydration_queue ("
            "signature TEXT PRIMARY KEY, status TEXT NOT NULL, updated_at TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE direct_solana_hydration_metrics ("
            "signature TEXT PRIMARY KEY, historical_recovery INTEGER NOT NULL, hydrated_at TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE direct_solana_storage_maintenance ("
            "id INTEGER PRIMARY KEY CHECK(id=1), "
            "queue_rows_pruned INTEGER NOT NULL DEFAULT 0, "
            "metric_rows_pruned INTEGER NOT NULL DEFAULT 0, "
            "last_maintenance_at TEXT, last_checkpoint_at TEXT, "
            "last_checkpoint_busy INTEGER, last_checkpoint_log INTEGER, "
            "last_checkpointed INTEGER, last_error TEXT)"
        )
        connection.execute("INSERT INTO direct_solana_storage_maintenance(id) VALUES (1)")
        connection.execute(
            "INSERT INTO direct_solana_hydration_queue(signature,status,updated_at) VALUES "
            "('old-complete','complete','2000-01-01T00:00:00+00:00'),"
            "('pending','pending','2000-01-01T00:00:00+00:00')"
        )
        connection.execute(
            "INSERT INTO direct_solana_hydration_metrics(signature,historical_recovery,hydrated_at) VALUES "
            "('old-metric',0,'2000-01-01T00:00:00+00:00'),"
            "('historical',1,'2000-01-01T00:00:00+00:00')"
        )
        connection.commit()
    finally:
        connection.close()


def _plane(path: Path):
    store = SimpleNamespace(path=path, _lock=_ForbiddenCanonicalLock())
    return SimpleNamespace(store=store)


def test_prune_and_checkpoint_never_require_canonical_python_lock(tmp_path: Path) -> None:
    path = tmp_path / "maintenance.sqlite3"
    _seed(path)
    plane = _plane(path)

    queue_rows, metric_rows = repair._prune_operational_rows_once_isolated(plane)
    assert (queue_rows, metric_rows) == (1, 1)

    checkpoint = repair._checkpoint_wal_isolated(plane)
    assert checkpoint is not None
    assert len(checkpoint) == 3

    connection = sqlite3.connect(path)
    try:
        assert connection.execute(
            "SELECT COUNT(*) FROM direct_solana_hydration_queue WHERE signature='old-complete'"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM direct_solana_hydration_queue WHERE signature='pending'"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM direct_solana_hydration_metrics WHERE signature='old-metric'"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM direct_solana_hydration_metrics WHERE signature='historical'"
        ).fetchone()[0] == 1
    finally:
        connection.close()


def test_maintenance_defers_quickly_when_sqlite_writer_is_busy(tmp_path: Path) -> None:
    path = tmp_path / "busy.sqlite3"
    _seed(path)
    plane = _plane(path)

    blocker = sqlite3.connect(path)
    try:
        blocker.execute("BEGIN IMMEDIATE")
        started = time.monotonic()
        result = repair._prune_operational_rows_once_isolated(plane)
        elapsed = time.monotonic() - started
    finally:
        blocker.rollback()
        blocker.close()

    assert result == (0, 0)
    assert elapsed < 1.5
    state = repair.status()
    assert state["prune_busy_deferrals"] >= 1
    assert state["maintenance_busy_timeout_ms"] == repair.MAINTENANCE_BUSY_TIMEOUT_MS
    assert state["canonical_store_python_lock_acquired_by_retention_prune"] is False
    assert state["canonical_store_python_lock_acquired_by_wal_checkpoint"] is False
    assert state["canonical_evidence_pruned"] is False
    assert state["certification_thresholds_changed"] is False
    assert state["paper_only"] is True
    assert state["live_money_authority"] is False
    assert state["signing_available"] is False
    assert state["transaction_submission_available"] is False


def test_install_replaces_only_maintenance_functions(monkeypatch) -> None:
    original_prune = storage_capacity._prune_operational_rows_once
    original_checkpoint = storage_capacity._checkpoint_wal
    monkeypatch.setattr(storage_capacity, "_prune_operational_rows_once", original_prune)
    monkeypatch.setattr(storage_capacity, "_checkpoint_wal", original_checkpoint)

    repair.install_storage_maintenance_lock_isolation()

    assert storage_capacity._prune_operational_rows_once is repair._prune_operational_rows_once_isolated
    assert storage_capacity._checkpoint_wal is repair._checkpoint_wal_isolated
    assert repair.status()["installed"] is True
