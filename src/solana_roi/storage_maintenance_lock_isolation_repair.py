from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

from . import continuity_storage_capacity_repair as storage_capacity
from . import direct_solana as direct_solana_module


REPAIR_VERSION = "storage-maintenance-lock-isolation-v3-bounded-dual-rowid-cursors"
BOUNDED_IO_VERSION = "storage-maintenance-bounded-dual-rowid-cursors-v3-time-budget"
MAINTENANCE_BUSY_TIMEOUT_MS = 250
DELETE_CHUNK_ROWS = 400
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
CERTIFICATION_THRESHOLDS_CHANGED = False
CANONICAL_EVIDENCE_PRUNED = False

_STATE_LOCK = threading.Lock()
_STATE: dict[str, Any] = {
    "installed": False,
    "prune_attempts": 0,
    "prune_busy_deferrals": 0,
    "checkpoint_attempts": 0,
    "checkpoint_busy_deferrals": 0,
    "last_error_type": None,
}


def _connection(store: Any) -> sqlite3.Connection:
    path = Path(getattr(store, "path", ""))
    if not str(path):
        raise RuntimeError("canonical SQLite path unavailable for isolated maintenance")
    connection = sqlite3.connect(
        path,
        timeout=max(0.001, float(MAINTENANCE_BUSY_TIMEOUT_MS) / 1000.0),
    )
    connection.row_factory = sqlite3.Row
    connection.execute(f"PRAGMA busy_timeout={int(MAINTENANCE_BUSY_TIMEOUT_MS)}")
    connection.execute("PRAGMA synchronous=FULL")
    return connection


def _ensure_state(connection: sqlite3.Connection) -> None:
    with connection:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS direct_solana_storage_maintenance ("
            "id INTEGER PRIMARY KEY CHECK(id=1), "
            "queue_rows_pruned INTEGER NOT NULL DEFAULT 0, "
            "metric_rows_pruned INTEGER NOT NULL DEFAULT 0, "
            "last_maintenance_at TEXT, last_checkpoint_at TEXT, "
            "last_checkpoint_busy INTEGER, last_checkpoint_log INTEGER, "
            "last_checkpointed INTEGER, last_error TEXT)"
        )
        connection.execute(
            "INSERT OR IGNORE INTO direct_solana_storage_maintenance(id) VALUES (1)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS direct_solana_storage_maintenance_cursor ("
            "id INTEGER PRIMARY KEY CHECK(id=1), "
            "queue_scan_rowid INTEGER NOT NULL DEFAULT 0, "
            "metric_scan_rowid INTEGER NOT NULL DEFAULT 0, "
            "last_queue_scan_rows INTEGER NOT NULL DEFAULT 0, "
            "last_scan_rows INTEGER NOT NULL DEFAULT 0, "
            "last_scan_at TEXT)"
        )
        columns = {
            str(row[1])
            for row in connection.execute(
                "PRAGMA table_info(direct_solana_storage_maintenance_cursor)"
            ).fetchall()
        }
        if "queue_scan_rowid" not in columns:
            connection.execute(
                "ALTER TABLE direct_solana_storage_maintenance_cursor "
                "ADD COLUMN queue_scan_rowid INTEGER NOT NULL DEFAULT 0"
            )
        if "last_queue_scan_rows" not in columns:
            connection.execute(
                "ALTER TABLE direct_solana_storage_maintenance_cursor "
                "ADD COLUMN last_queue_scan_rows INTEGER NOT NULL DEFAULT 0"
            )
        connection.execute(
            "INSERT OR IGNORE INTO direct_solana_storage_maintenance_cursor(id) VALUES (1)"
        )


def _state_inc(name: str) -> None:
    with _STATE_LOCK:
        _STATE[name] = int(_STATE.get(name, 0) or 0) + 1


def _state_error(exc: BaseException | None) -> None:
    with _STATE_LOCK:
        _STATE["last_error_type"] = type(exc).__name__ if exc is not None else None


def _record_error(connection: sqlite3.Connection, message: str) -> None:
    try:
        with connection:
            connection.execute(
                "UPDATE direct_solana_storage_maintenance SET last_error=? WHERE id=1",
                (message,),
            )
    except sqlite3.Error:
        return


def _delete_rowids(
    connection: sqlite3.Connection,
    table: str,
    rowids: list[int],
) -> int:
    if table not in {
        "direct_solana_hydration_queue",
        "direct_solana_hydration_metrics",
    }:
        raise ValueError("unsupported storage-maintenance table")
    deleted = 0
    for start in range(0, len(rowids), DELETE_CHUNK_ROWS):
        chunk = rowids[start : start + DELETE_CHUNK_ROWS]
        if not chunk:
            continue
        placeholders = ",".join("?" for _ in chunk)
        cur = connection.execute(
            f"DELETE FROM {table} WHERE rowid IN ({placeholders})",
            tuple(chunk),
        )
        deleted += int(cur.rowcount or 0)
    return deleted


def _prune_operational_rows_once_isolated(self: Any) -> tuple[int, int]:
    """Prune disposable operational rows with history-independent SQLite work.

    Maintenance stays on its dedicated WAL connection and never takes the canonical
    evidence-store Python lock. Both operational-retention tables advance durable
    physical-row cursors through at most the existing maintenance batch per pass.
    Eligibility predicates are unchanged: queue deletion remains limited to old
    terminal rows, and metric deletion remains limited to old non-historical rows.
    Each cursor wraps at end-of-table so surviving and newly appended rows remain
    reachable on later maintenance passes.
    """

    _state_inc("prune_attempts")
    now = direct_solana_module.utcnow()
    queue_cutoff = (
        now - timedelta(seconds=storage_capacity.TERMINAL_QUEUE_RETENTION_SECONDS)
    ).isoformat()
    metric_cutoff = (
        now - timedelta(seconds=storage_capacity.HYDRATION_METRIC_RETENTION_SECONDS)
    ).isoformat()
    connection = _connection(self.store)
    try:
        _ensure_state(connection)
        with connection:
            cursor_row = connection.execute(
                "SELECT queue_scan_rowid, metric_scan_rowid "
                "FROM direct_solana_storage_maintenance_cursor WHERE id=1"
            ).fetchone()
            queue_cursor = int(cursor_row[0]) if cursor_row is not None else 0
            metric_cursor = int(cursor_row[1]) if cursor_row is not None else 0

            queue_scan_rows = connection.execute(
                "SELECT rowid, status, updated_at "
                "FROM direct_solana_hydration_queue WHERE rowid>? "
                "ORDER BY rowid LIMIT ?",
                (queue_cursor, storage_capacity.MAINTENANCE_BATCH_ROWS),
            ).fetchall()
            eligible_queue_rowids = [
                int(row[0])
                for row in queue_scan_rows
                if str(row[1]) in {"complete", "failed"}
                and str(row[2]) < queue_cutoff
            ]
            queue_rows = _delete_rowids(
                connection,
                "direct_solana_hydration_queue",
                eligible_queue_rowids,
            )
            next_queue_cursor = (
                int(queue_scan_rows[-1][0])
                if queue_scan_rows
                and len(queue_scan_rows) >= storage_capacity.MAINTENANCE_BATCH_ROWS
                else 0
            )

            metric_scan_rows = connection.execute(
                "SELECT rowid, hydrated_at, historical_recovery "
                "FROM direct_solana_hydration_metrics WHERE rowid>? "
                "ORDER BY rowid LIMIT ?",
                (metric_cursor, storage_capacity.MAINTENANCE_BATCH_ROWS),
            ).fetchall()
            eligible_metric_rowids = [
                int(row[0])
                for row in metric_scan_rows
                if int(row[2] or 0) == 0 and str(row[1]) < metric_cutoff
            ]
            metric_rows = _delete_rowids(
                connection,
                "direct_solana_hydration_metrics",
                eligible_metric_rowids,
            )
            next_metric_cursor = (
                int(metric_scan_rows[-1][0])
                if metric_scan_rows
                and len(metric_scan_rows) >= storage_capacity.MAINTENANCE_BATCH_ROWS
                else 0
            )

            connection.execute(
                "UPDATE direct_solana_storage_maintenance_cursor SET "
                "queue_scan_rowid=?, metric_scan_rowid=?, "
                "last_queue_scan_rows=?, last_scan_rows=?, last_scan_at=? WHERE id=1",
                (
                    next_queue_cursor,
                    next_metric_cursor,
                    len(queue_scan_rows),
                    len(metric_scan_rows),
                    now.isoformat(),
                ),
            )
            connection.execute(
                "UPDATE direct_solana_storage_maintenance SET "
                "queue_rows_pruned=queue_rows_pruned+?, metric_rows_pruned=metric_rows_pruned+?, "
                "last_maintenance_at=?, last_error=NULL WHERE id=1",
                (queue_rows, metric_rows, now.isoformat()),
            )
        _state_error(None)
        return queue_rows, metric_rows
    except sqlite3.OperationalError as exc:
        # Maintenance is housekeeping only. If canonical evidence writing owns the
        # SQLite writer lock, defer this pass instead of waiting behind it or taking
        # the in-process evidence lock. The worker will retry on its normal cadence.
        _state_inc("prune_busy_deferrals")
        _state_error(exc)
        _record_error(
            connection,
            f"{type(exc).__name__}: isolated storage maintenance deferred",
        )
        return 0, 0
    finally:
        connection.close()


def _checkpoint_wal_isolated(self: Any) -> tuple[int, int, int] | None:
    """Checkpoint WAL on a dedicated connection with bounded writer contention."""

    _state_inc("checkpoint_attempts")
    now = direct_solana_module.utcnow()
    connection = _connection(self.store)
    try:
        _ensure_state(connection)
        row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        result = (0, 0, 0) if row is None else (int(row[0]), int(row[1]), int(row[2]))
        with connection:
            connection.execute(
                "UPDATE direct_solana_storage_maintenance SET last_checkpoint_at=?, "
                "last_checkpoint_busy=?, last_checkpoint_log=?, last_checkpointed=?, "
                "last_error=NULL WHERE id=1",
                (now.isoformat(), result[0], result[1], result[2]),
            )
        _state_error(None)
        return result
    except sqlite3.OperationalError as exc:
        _state_inc("checkpoint_busy_deferrals")
        _state_error(exc)
        _record_error(
            connection,
            f"{type(exc).__name__}: isolated WAL checkpoint deferred",
        )
        return None
    finally:
        connection.close()


async def _bounded_storage_maintenance_worker(self: Any, stop: asyncio.Event) -> None:
    """Run at most one bounded maintenance batch per normal 60-second interval.

    The former drain mode retried every 100 ms while old rows remained, turning
    startup backlog size into sustained database I/O and dirty/writeback pressure.
    Retention semantics are unchanged. Only housekeeping I/O rate is bounded so
    backlog cannot monopolize the cgroup.
    """

    next_checkpoint = 0.0
    drained_since_checkpoint = False
    while not stop.is_set():
        queue_rows = 0
        metric_rows = 0
        try:
            queue_rows, metric_rows = await asyncio.to_thread(
                storage_capacity._prune_operational_rows_once, self
            )
            if queue_rows or metric_rows:
                drained_since_checkpoint = True
            now_mono = time.monotonic()
            drain_complete = (
                not queue_rows
                and not metric_rows
                and drained_since_checkpoint
            )
            if now_mono >= next_checkpoint or drain_complete:
                await asyncio.to_thread(storage_capacity._checkpoint_wal, self)
                next_checkpoint = (
                    now_mono + storage_capacity.WAL_CHECKPOINT_INTERVAL_SECONDS
                )
                drained_since_checkpoint = False
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            try:
                await asyncio.to_thread(storage_capacity._ensure_maintenance_state, self)
                with self.store._lock, self.store.db:
                    self.store.db.execute(
                        "UPDATE direct_solana_storage_maintenance "
                        "SET last_error=? WHERE id=1",
                        (f"{type(exc).__name__}: storage maintenance failed",),
                    )
            except Exception:
                pass

        try:
            await asyncio.wait_for(
                stop.wait(), timeout=storage_capacity.MAINTENANCE_IDLE_SECONDS
            )
        except asyncio.TimeoutError:
            continue


setattr(
    _prune_operational_rows_once_isolated,
    "_roi_storage_maintenance_lock_isolation",
    True,
)
setattr(
    _prune_operational_rows_once_isolated,
    "_roi_storage_maintenance_bounded_io",
    True,
)
setattr(
    _checkpoint_wal_isolated,
    "_roi_storage_maintenance_lock_isolation",
    True,
)
setattr(
    _bounded_storage_maintenance_worker,
    "_roi_storage_maintenance_bounded_cadence",
    True,
)


def install_storage_maintenance_lock_isolation() -> None:
    """Compose isolated, time-bounded housekeeping plus phase attribution."""

    current_prune = storage_capacity._prune_operational_rows_once
    current_checkpoint = storage_capacity._checkpoint_wal
    current_worker = storage_capacity._storage_maintenance_worker
    prune_ok = bool(
        (
            getattr(
                current_prune,
                "_roi_storage_maintenance_lock_isolation",
                False,
            )
            and getattr(current_prune, "_roi_storage_maintenance_bounded_io", False)
        )
        or getattr(current_prune, "_roi_sqlite_phase_observed", False)
    )
    checkpoint_ok = bool(
        getattr(
            current_checkpoint,
            "_roi_storage_maintenance_lock_isolation",
            False,
        )
        or getattr(current_checkpoint, "_roi_sqlite_phase_observed", False)
    )
    worker_ok = bool(
        getattr(
            current_worker,
            "_roi_storage_maintenance_bounded_cadence",
            False,
        )
    )
    if not (prune_ok and checkpoint_ok and worker_ok):
        storage_capacity._prune_operational_rows_once = (
            _prune_operational_rows_once_isolated
        )
        storage_capacity._checkpoint_wal = _checkpoint_wal_isolated
        storage_capacity._storage_maintenance_worker = (
            _bounded_storage_maintenance_worker
        )
    with _STATE_LOCK:
        _STATE["installed"] = True

    # This existing canonical composition installer is the final owner of the
    # maintenance functions. Install read-only phase attribution only after those
    # final delegates exist so instrumentation cannot be overwritten by composition.
    from .sqlite_phase_observability import install_sqlite_phase_observability

    install_sqlite_phase_observability()


def status() -> dict[str, Any]:
    with _STATE_LOCK:
        state = dict(_STATE)
    return {
        **state,
        "repair_version": REPAIR_VERSION,
        "bounded_io_version": BOUNDED_IO_VERSION,
        "maintenance_connection": "dedicated_sqlite_wal_connection",
        "maintenance_busy_timeout_ms": MAINTENANCE_BUSY_TIMEOUT_MS,
        "queue_scan_mode": "durable_rowid_keyset",
        "queue_scan_max_rows_per_pass": storage_capacity.MAINTENANCE_BATCH_ROWS,
        "metric_scan_mode": "durable_rowid_keyset",
        "metric_scan_max_rows_per_pass": storage_capacity.MAINTENANCE_BATCH_ROWS,
        "backlog_drain_interval_seconds": storage_capacity.MAINTENANCE_IDLE_SECONDS,
        "aggressive_100ms_backlog_drain_disabled": True,
        "queue_deletion_predicate_changed": False,
        "metric_deletion_predicate_changed": False,
        "canonical_store_python_lock_acquired_by_retention_prune": False,
        "canonical_store_python_lock_acquired_by_wal_checkpoint": False,
        "canonical_evidence_pruned": CANONICAL_EVIDENCE_PRUNED,
        "certification_thresholds_changed": CERTIFICATION_THRESHOLDS_CHANGED,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = [
    "BOUNDED_IO_VERSION",
    "DELETE_CHUNK_ROWS",
    "MAINTENANCE_BUSY_TIMEOUT_MS",
    "REPAIR_VERSION",
    "_bounded_storage_maintenance_worker",
    "_checkpoint_wal_isolated",
    "_prune_operational_rows_once_isolated",
    "install_storage_maintenance_lock_isolation",
    "status",
]
