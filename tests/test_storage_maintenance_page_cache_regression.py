from __future__ import annotations

import asyncio
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from solana_roi import continuity_storage_capacity_repair as storage_capacity
from solana_roi import sqlite_phase_observability as phase_observability
from solana_roi.continuity_storage_capacity_repair import MAINTENANCE_BATCH_ROWS
from solana_roi.direct_solana import DirectSolanaJournal
from solana_roi.storage_maintenance_lock_isolation_repair import (
    _bounded_storage_maintenance_worker,
    _prune_operational_rows_once_isolated,
)


def _store(path: Path) -> SimpleNamespace:
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    store = SimpleNamespace(path=path, db=db, _lock=threading.RLock())
    DirectSolanaJournal(store)
    return store


def _old_and_fresh() -> tuple[str, str]:
    return (
        (datetime.now(timezone.utc) - timedelta(days=2)).isoformat(),
        datetime.now(timezone.utc).isoformat(),
    )


def _seed_metrics(
    store: SimpleNamespace,
    *,
    old_eligible: int,
    old_historical: int = 0,
    recent: int = 0,
) -> None:
    old, fresh = _old_and_fresh()
    payload: list[tuple[object, ...]] = []
    index = 0
    for _ in range(old_eligible):
        payload.append(
            (
                f"eligible-{index:08d}",
                "PUMP_FUN",
                old,
                old,
                "provider",
                1.0,
                1.0,
                1,
                0,
                0,
            )
        )
        index += 1
    for _ in range(old_historical):
        payload.append(
            (
                f"historical-{index:08d}",
                "PUMP_FUN",
                old,
                old,
                "provider",
                1.0,
                1.0,
                1,
                0,
                1,
            )
        )
        index += 1
    for _ in range(recent):
        payload.append(
            (
                f"recent-{index:08d}",
                "PUMP_FUN",
                fresh,
                fresh,
                "provider",
                1.0,
                1.0,
                1,
                0,
                0,
            )
        )
        index += 1
    with store._lock, store.db:
        store.db.executemany(
            "INSERT INTO direct_solana_hydration_metrics("
            "signature, source, trigger_received_at, hydrated_at, rpc_provider, rpc_latency_ms, "
            "total_hydration_ms, normalized, candidate_context_prefilled, historical_recovery) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            payload,
        )


def _seed_queue(
    store: SimpleNamespace,
    *,
    old_complete: int = 0,
    old_failed: int = 0,
    old_pending: int = 0,
    old_processing: int = 0,
    recent_complete: int = 0,
) -> None:
    old, fresh = _old_and_fresh()
    payload: list[tuple[object, ...]] = []
    index = 0

    def append_rows(count: int, status: str, updated_at: str) -> None:
        nonlocal index
        for _ in range(count):
            signature = f"queue-{status}-{index:08d}"
            payload.append(
                (
                    signature,
                    index,
                    old,
                    "PUMP_FUN",
                    1,
                    "test",
                    status,
                    1,
                    None,
                    updated_at,
                )
            )
            index += 1

    append_rows(old_complete, "complete", old)
    append_rows(old_failed, "failed", old)
    append_rows(old_pending, "pending", old)
    append_rows(old_processing, "processing", old)
    append_rows(recent_complete, "complete", fresh)

    with store._lock, store.db:
        store.db.executemany(
            "INSERT INTO direct_solana_hydration_queue("
            "signature, slot, trigger_received_at, source_hint, priority, reason, "
            "status, attempts, last_error, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            payload,
        )


def _bounded_metric_scan_plan(store: SimpleNamespace) -> str:
    with store._lock:
        rows = store.db.execute(
            "EXPLAIN QUERY PLAN SELECT rowid, hydrated_at, historical_recovery "
            "FROM direct_solana_hydration_metrics WHERE rowid>? "
            "ORDER BY rowid LIMIT ?",
            (0, MAINTENANCE_BATCH_ROWS),
        ).fetchall()
    return " | ".join(str(row[3]) for row in rows)


def _bounded_queue_scan_plan(store: SimpleNamespace) -> str:
    with store._lock:
        rows = store.db.execute(
            "EXPLAIN QUERY PLAN SELECT rowid, status, updated_at "
            "FROM direct_solana_hydration_queue WHERE rowid>? "
            "ORDER BY rowid LIMIT ?",
            (0, MAINTENANCE_BATCH_ROWS),
        ).fetchall()
    return " | ".join(str(row[3]) for row in rows)


def _selector_vm_steps(path: Path, rows: int, *, table: str) -> int:
    store = _store(path)
    if table == "metrics":
        _seed_metrics(store, old_eligible=rows)
        sql = (
            "SELECT rowid, hydrated_at, historical_recovery "
            "FROM direct_solana_hydration_metrics WHERE rowid>? "
            "ORDER BY rowid LIMIT ?"
        )
    elif table == "queue":
        _seed_queue(store, old_complete=rows)
        sql = (
            "SELECT rowid, status, updated_at "
            "FROM direct_solana_hydration_queue WHERE rowid>? "
            "ORDER BY rowid LIMIT ?"
        )
    else:
        raise AssertionError(table)

    callbacks = 0

    def progress() -> int:
        nonlocal callbacks
        callbacks += 1
        return 0

    store.db.set_progress_handler(progress, 100)
    with store._lock:
        selected = store.db.execute(
            sql,
            (0, MAINTENANCE_BATCH_ROWS),
        ).fetchall()
    store.db.set_progress_handler(None, 0)
    store.db.close()
    assert len(selected) == MAINTENANCE_BATCH_ROWS
    return callbacks * 100


def test_hydration_metric_prune_uses_bounded_rowid_keyset(tmp_path: Path) -> None:
    store = _store(tmp_path / "metric-plan.sqlite3")
    _seed_metrics(store, old_eligible=50_000)

    plan = _bounded_metric_scan_plan(store)

    assert "rowid>?" in plan or "INTEGER PRIMARY KEY" in plan, plan
    assert "USE TEMP B-TREE FOR ORDER BY" not in plan, plan
    assert bool(
        getattr(
            _prune_operational_rows_once_isolated,
            "_roi_storage_maintenance_bounded_io",
            False,
        )
    )
    store.db.close()


def test_terminal_queue_prune_uses_bounded_rowid_keyset(tmp_path: Path) -> None:
    store = _store(tmp_path / "queue-plan.sqlite3")
    _seed_queue(store, old_complete=50_000)

    plan = _bounded_queue_scan_plan(store)

    assert "rowid>?" in plan or "INTEGER PRIMARY KEY" in plan, plan
    assert "USE TEMP B-TREE FOR ORDER BY" not in plan, plan
    store.db.close()


def test_metric_selector_work_does_not_scale_with_history(tmp_path: Path) -> None:
    small_steps = _selector_vm_steps(
        tmp_path / "metric-small.sqlite3",
        6_000,
        table="metrics",
    )
    large_steps = _selector_vm_steps(
        tmp_path / "metric-large.sqlite3",
        60_000,
        table="metrics",
    )

    assert small_steps > 0
    assert large_steps <= small_steps * 2, (small_steps, large_steps)


def test_queue_selector_work_does_not_scale_with_history(tmp_path: Path) -> None:
    small_steps = _selector_vm_steps(
        tmp_path / "queue-small.sqlite3",
        6_000,
        table="queue",
    )
    large_steps = _selector_vm_steps(
        tmp_path / "queue-large.sqlite3",
        60_000,
        table="queue",
    )

    assert small_steps > 0
    assert large_steps <= small_steps * 2, (small_steps, large_steps)


def test_bounded_isolated_prune_preserves_protected_rows(tmp_path: Path) -> None:
    store = _store(tmp_path / "semantics.sqlite3")
    _seed_metrics(
        store,
        old_eligible=MAINTENANCE_BATCH_ROWS,
        old_historical=37,
        recent=41,
    )
    _seed_queue(
        store,
        old_complete=2_500,
        old_failed=2_500,
        old_pending=37,
        old_processing=31,
        recent_complete=41,
    )
    plane = SimpleNamespace(store=store)

    queue_rows, metric_rows = _prune_operational_rows_once_isolated(plane)
    assert queue_rows == MAINTENANCE_BATCH_ROWS
    assert metric_rows == MAINTENANCE_BATCH_ROWS

    with store._lock:
        historical = store.db.execute(
            "SELECT COUNT(*) FROM direct_solana_hydration_metrics "
            "WHERE historical_recovery=1"
        ).fetchone()[0]
        recent_metrics = store.db.execute(
            "SELECT COUNT(*) FROM direct_solana_hydration_metrics "
            "WHERE signature LIKE 'recent-%'"
        ).fetchone()[0]
        pending = store.db.execute(
            "SELECT COUNT(*) FROM direct_solana_hydration_queue WHERE status='pending'"
        ).fetchone()[0]
        processing = store.db.execute(
            "SELECT COUNT(*) FROM direct_solana_hydration_queue "
            "WHERE status='processing'"
        ).fetchone()[0]
        recent_queue = store.db.execute(
            "SELECT COUNT(*) FROM direct_solana_hydration_queue "
            "WHERE status='complete' AND updated_at>?",
            ((datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(),),
        ).fetchone()[0]
    assert historical == 37
    assert recent_metrics == 41
    assert pending == 37
    assert processing == 31
    assert recent_queue == 41
    store.db.close()


def test_storage_maintenance_backlog_cannot_spin_at_startup(monkeypatch) -> None:
    observed_timeouts: list[float] = []
    stop = asyncio.Event()
    plane = SimpleNamespace(store=SimpleNamespace())

    monkeypatch.setattr(
        storage_capacity,
        "_prune_operational_rows_once",
        lambda _self: (1, 1),
    )
    monkeypatch.setattr(
        storage_capacity,
        "_checkpoint_wal",
        lambda _self: (0, 0, 0),
    )

    async def fake_wait_for(awaitable, *, timeout: float):
        observed_timeouts.append(float(timeout))
        if hasattr(awaitable, "close"):
            awaitable.close()
        stop.set()
        raise asyncio.TimeoutError

    monkeypatch.setattr(storage_capacity.asyncio, "wait_for", fake_wait_for)
    asyncio.run(_bounded_storage_maintenance_worker(plane, stop))

    assert bool(
        getattr(
            _bounded_storage_maintenance_worker,
            "_roi_storage_maintenance_bounded_cadence",
            False,
        )
    )
    assert observed_timeouts == [storage_capacity.MAINTENANCE_IDLE_SECONDS]


def test_phase_snapshot_separates_cache_writeback_wal_io_and_process_bounds(
    tmp_path: Path,
    monkeypatch,
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
