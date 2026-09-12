from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from solana_roi import sqlite_phase_observability as phase_observability
from solana_roi.continuity_storage_capacity_repair import MAINTENANCE_BATCH_ROWS
from solana_roi.direct_solana import DirectSolanaJournal
from solana_roi.storage_maintenance_bounded_io_repair import _bounded_prune_operational_rows_once


def _store(path: Path) -> SimpleNamespace:
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    store = SimpleNamespace(path=path, db=db, _lock=threading.RLock())
    DirectSolanaJournal(store)
    return store


def _seed_metrics(
    store: SimpleNamespace,
    *,
    old_eligible: int,
    old_historical: int = 0,
    recent: int = 0,
) -> None:
    old = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    fresh = datetime.now(timezone.utc).isoformat()
    payload: list[tuple[object, ...]] = []
    index = 0
    for _ in range(old_eligible):
        payload.append((f"eligible-{index:08d}", "PUMP_FUN", old, old, "provider", 1.0, 1.0, 1, 0, 0))
        index += 1
    for _ in range(old_historical):
        payload.append((f"historical-{index:08d}", "PUMP_FUN", old, old, "provider", 1.0, 1.0, 1, 0, 1))
        index += 1
    for _ in range(recent):
        payload.append((f"recent-{index:08d}", "PUMP_FUN", fresh, fresh, "provider", 1.0, 1.0, 1, 0, 0))
        index += 1
    with store._lock, store.db:
        store.db.executemany(
            "INSERT INTO direct_solana_hydration_metrics("
            "signature, source, trigger_received_at, hydrated_at, rpc_provider, rpc_latency_ms, "
            "total_hydration_ms, normalized, candidate_context_prefilled, historical_recovery) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            payload,
        )


def _bounded_scan_plan(store: SimpleNamespace) -> str:
    with store._lock:
        rows = store.db.execute(
            "EXPLAIN QUERY PLAN SELECT rowid, hydrated_at, historical_recovery "
            "FROM direct_solana_hydration_metrics WHERE rowid>? "
            "ORDER BY rowid LIMIT ?",
            (0, MAINTENANCE_BATCH_ROWS),
        ).fetchall()
    return " | ".join(str(row[3]) for row in rows)


def _prune_vm_steps(path: Path, rows: int) -> tuple[int, int]:
    store = _store(path)
    _seed_metrics(store, old_eligible=rows)
    callbacks = 0

    def progress() -> int:
        nonlocal callbacks
        callbacks += 1
        return 0

    store.db.set_progress_handler(progress, 100)
    plane = SimpleNamespace(store=store)
    _queue_rows, metric_rows = _bounded_prune_operational_rows_once(plane)
    store.db.set_progress_handler(None, 0)
    store.db.close()
    return callbacks * 100, metric_rows


def test_hydration_metric_prune_uses_bounded_rowid_keyset(tmp_path: Path) -> None:
    """The production maintenance scan must use the intrinsic rowid keyset."""
    store = _store(tmp_path / "plan.sqlite3")
    _seed_metrics(store, old_eligible=50_000)

    plan = _bounded_scan_plan(store)

    assert "rowid>?" in plan or "INTEGER PRIMARY KEY" in plan, plan
    assert "USE TEMP B-TREE FOR ORDER BY" not in plan, plan
    store.db.close()


def test_hydration_metric_prune_work_does_not_scale_with_history(tmp_path: Path) -> None:
    """Tenfold historical growth must not cause near-tenfold SQLite VM work."""
    small_steps, small_deleted = _prune_vm_steps(tmp_path / "small.sqlite3", 6_000)
    large_steps, large_deleted = _prune_vm_steps(tmp_path / "large.sqlite3", 60_000)

    assert small_deleted == MAINTENANCE_BATCH_ROWS
    assert large_deleted == MAINTENANCE_BATCH_ROWS
    assert small_steps > 0
    assert large_steps <= small_steps * 2, (small_steps, large_steps)


def test_bounded_prune_preserves_historical_and_recent_rows(tmp_path: Path) -> None:
    store = _store(tmp_path / "semantics.sqlite3")
    _seed_metrics(
        store,
        old_eligible=MAINTENANCE_BATCH_ROWS,
        old_historical=37,
        recent=41,
    )
    plane = SimpleNamespace(store=store)

    _queue_rows, metric_rows = _bounded_prune_operational_rows_once(plane)
    assert metric_rows == MAINTENANCE_BATCH_ROWS

    with store._lock:
        historical = store.db.execute(
            "SELECT COUNT(*) FROM direct_solana_hydration_metrics WHERE historical_recovery=1"
        ).fetchone()[0]
        recent = store.db.execute(
            "SELECT COUNT(*) FROM direct_solana_hydration_metrics WHERE signature LIKE 'recent-%'"
        ).fetchone()[0]
    assert historical == 37
    assert recent == 41
    store.db.close()


def test_phase_snapshot_separates_cache_writeback_wal_io_and_process_bounds(
    tmp_path: Path, monkeypatch
) -> None:
    cgroup = tmp_path / "cgroup"
    cgroup.mkdir()
    (cgroup / "memory.stat").write_text(
        "anon 200\nfile 1000\nfile_dirty 100\nfile_writeback 50\n",
        encoding="utf-8",
    )
    (cgroup / "memory.current").write_text("1400\n", encoding="utf-8")
    (cgroup / "memory.max").write_text("2000\n", encoding="utf-8")
    (cgroup / "pids.current").write_text("39\n", encoding="utf-8")
    monkeypatch.setattr(phase_observability, "_CGROUP_ROOT", cgroup)

    db_path = tmp_path / "production.sqlite3"
    db_path.write_bytes(b"db-bytes")
    Path(f"{db_path}-wal").write_bytes(b"w" * 123)
    Path(f"{db_path}-shm").write_bytes(b"s" * 31)

    snapshot = phase_observability.resource_snapshot(SimpleNamespace(path=db_path))

    assert snapshot["anon_bytes"] == 200
    assert snapshot["file_cache_bytes"] == 1000
    assert snapshot["clean_file_cache_bytes_estimate"] == 850
    assert snapshot["file_dirty_bytes"] == 100
    assert snapshot["file_writeback_bytes"] == 50
    assert snapshot["sqlite_wal_bytes"] == 123
    assert snapshot["sqlite_shm_bytes"] == 31
    assert snapshot["pids_current"] == 39
    assert isinstance(snapshot["threads"], int) and snapshot["threads"] > 0
    assert isinstance(snapshot["proc_read_bytes"], int)
    assert isinstance(snapshot["proc_write_bytes"], int)
