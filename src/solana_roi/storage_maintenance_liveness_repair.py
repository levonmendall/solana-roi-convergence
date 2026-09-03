from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

from . import continuity_storage_capacity_repair as capacity
from . import direct_solana as direct_solana_module


# PR #77 correctly moved large retention work into asyncio.to_thread(), but the
# worker still acquired ObservationEventStore._lock. Event-loop ingestion performs
# synchronous SQLite work behind that same threading.RLock, so a maintenance thread
# holding it can park the entire Uvicorn loop and make the constant-time /health
# route miss Render's unchanged five-second probe. Use an independent WAL connection
# with a zero busy timeout instead: maintenance yields immediately whenever the live
# writer owns SQLite and never makes the event loop wait on a Python lock.
LIVENESS_SAFE_BATCH_ROWS = 500
LIVENESS_SAFE_DRAIN_SECONDS = 0.50
INITIAL_MAINTENANCE_DELAY_SECONDS = 15.0
MAINTENANCE_SQLITE_BUSY_TIMEOUT_MS = 0


def _runtime_state(self: Any) -> dict[str, Any]:
    state = getattr(self, "_roi_nonblocking_storage_maintenance", None)
    if not isinstance(state, dict):
        state = {
            "runs": 0,
            "busy_skips": 0,
            "queue_rows_pruned": 0,
            "metric_rows_pruned": 0,
            "last_run_ms": None,
            "max_run_ms": 0.0,
            "last_run_at": None,
            "last_checkpoint_at": None,
            "last_checkpoint_result": None,
            "last_error": None,
        }
        setattr(self, "_roi_nonblocking_storage_maintenance", state)
    return state


def _db_path(self: Any) -> Path:
    path = Path(getattr(getattr(self, "store", None), "path", ""))
    if not str(path):
        raise RuntimeError("storage maintenance database path is unavailable")
    return path


def _connect(self: Any, *, read_only: bool = False) -> sqlite3.Connection:
    path = _db_path(self)
    if read_only:
        db = sqlite3.connect(
            f"file:{path}?mode=ro",
            uri=True,
            timeout=0.0,
            isolation_level=None,
            check_same_thread=False,
        )
    else:
        db = sqlite3.connect(
            path,
            timeout=0.0,
            isolation_level=None,
            check_same_thread=False,
        )
    db.row_factory = sqlite3.Row
    db.execute(f"PRAGMA busy_timeout={MAINTENANCE_SQLITE_BUSY_TIMEOUT_MS}")
    return db


def _locked_or_busy(exc: BaseException) -> bool:
    return isinstance(exc, sqlite3.OperationalError) and any(
        token in str(exc).lower() for token in ("locked", "busy")
    )


def _ensure_state_table(db: sqlite3.Connection) -> None:
    db.execute(
        "CREATE TABLE IF NOT EXISTS direct_solana_storage_maintenance ("
        "id INTEGER PRIMARY KEY CHECK(id=1), "
        "queue_rows_pruned INTEGER NOT NULL DEFAULT 0, "
        "metric_rows_pruned INTEGER NOT NULL DEFAULT 0, "
        "last_maintenance_at TEXT, last_checkpoint_at TEXT, "
        "last_checkpoint_busy INTEGER, last_checkpoint_log INTEGER, "
        "last_checkpointed INTEGER, last_error TEXT)"
    )
    db.execute("INSERT OR IGNORE INTO direct_solana_storage_maintenance(id) VALUES (1)")


def _nonblocking_prune_once(self: Any) -> tuple[int, int, bool]:
    """Delete one small terminal-only batch or yield immediately to live writes."""

    started = time.perf_counter()
    now = direct_solana_module.utcnow()
    queue_cutoff = (
        now - timedelta(seconds=capacity.TERMINAL_QUEUE_RETENTION_SECONDS)
    ).isoformat()
    metric_cutoff = (
        now - timedelta(seconds=capacity.HYDRATION_METRIC_RETENTION_SECONDS)
    ).isoformat()
    state = _runtime_state(self)
    db: sqlite3.Connection | None = None
    queue_rows = 0
    metric_rows = 0
    busy = False
    try:
        db = _connect(self)
        # BEGIN IMMEDIATE either wins the writer slot now or fails immediately.
        # There is no five-second SQLite busy wait and no shared process RLock.
        db.execute("BEGIN IMMEDIATE")
        _ensure_state_table(db)
        queue_exists = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='direct_solana_hydration_queue' LIMIT 1"
        ).fetchone()
        if queue_exists is not None:
            queue_cur = db.execute(
                "DELETE FROM direct_solana_hydration_queue WHERE signature IN ("
                "SELECT signature FROM direct_solana_hydration_queue "
                "WHERE status IN ('complete','failed') AND updated_at<? "
                "ORDER BY updated_at, signature LIMIT ?)",
                (queue_cutoff, LIVENESS_SAFE_BATCH_ROWS),
            )
            queue_rows = int(queue_cur.rowcount or 0)

        metric_exists = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='direct_solana_hydration_metrics' LIMIT 1"
        ).fetchone()
        if metric_exists is not None:
            metric_cur = db.execute(
                "DELETE FROM direct_solana_hydration_metrics WHERE signature IN ("
                "SELECT signature FROM direct_solana_hydration_metrics "
                "WHERE historical_recovery=0 AND hydrated_at<? "
                "ORDER BY hydrated_at, signature LIMIT ?)",
                (metric_cutoff, LIVENESS_SAFE_BATCH_ROWS),
            )
            metric_rows = int(metric_cur.rowcount or 0)

        db.execute(
            "UPDATE direct_solana_storage_maintenance SET "
            "queue_rows_pruned=queue_rows_pruned+?, "
            "metric_rows_pruned=metric_rows_pruned+?, "
            "last_maintenance_at=?, last_error=NULL WHERE id=1",
            (queue_rows, metric_rows, now.isoformat()),
        )
        db.execute("COMMIT")
        state["last_error"] = None
    except Exception as exc:
        if db is not None:
            try:
                db.execute("ROLLBACK")
            except Exception:
                pass
        if _locked_or_busy(exc):
            busy = True
            state["busy_skips"] = int(state.get("busy_skips", 0) or 0) + 1
        else:
            state["last_error"] = f"{type(exc).__name__}: nonblocking storage prune failed"
    finally:
        if db is not None:
            db.close()
        elapsed_ms = max(0.0, (time.perf_counter() - started) * 1000.0)
        state["runs"] = int(state.get("runs", 0) or 0) + 1
        state["queue_rows_pruned"] = int(state.get("queue_rows_pruned", 0) or 0) + queue_rows
        state["metric_rows_pruned"] = int(state.get("metric_rows_pruned", 0) or 0) + metric_rows
        state["last_run_ms"] = elapsed_ms
        state["max_run_ms"] = max(float(state.get("max_run_ms", 0.0) or 0.0), elapsed_ms)
        state["last_run_at"] = now.isoformat()
    return queue_rows, metric_rows, busy


def _nonblocking_checkpoint(self: Any) -> tuple[int, int, int] | None:
    """Attempt WAL truncation without waiting for readers or the live writer."""

    state = _runtime_state(self)
    db: sqlite3.Connection | None = None
    try:
        db = _connect(self)
        row = db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        result = (0, 0, 0) if row is None else (int(row[0]), int(row[1]), int(row[2]))
        state["last_checkpoint_at"] = direct_solana_module.utcnow().isoformat()
        state["last_checkpoint_result"] = {
            "busy": result[0],
            "log_frames": result[1],
            "checkpointed_frames": result[2],
        }
        if result[0]:
            state["busy_skips"] = int(state.get("busy_skips", 0) or 0) + 1
        return result
    except Exception as exc:
        if _locked_or_busy(exc):
            state["busy_skips"] = int(state.get("busy_skips", 0) or 0) + 1
            return None
        state["last_error"] = f"{type(exc).__name__}: nonblocking WAL checkpoint failed"
        return None
    finally:
        if db is not None:
            db.close()


def _nonblocking_snapshot(self: Any) -> dict[str, Any]:
    state = dict(_runtime_state(self))
    path = _db_path(self)
    persistent: dict[str, Any] = {}
    queue: dict[str, int] = {}
    page_size = 0
    page_count = 0
    freelist = 0
    snapshot_error: str | None = None
    db: sqlite3.Connection | None = None
    try:
        db = _connect(self, read_only=True)
        maintenance_exists = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='direct_solana_storage_maintenance' LIMIT 1"
        ).fetchone()
        if maintenance_exists is not None:
            row = db.execute(
                "SELECT queue_rows_pruned, metric_rows_pruned, last_maintenance_at, "
                "last_checkpoint_at, last_checkpoint_busy, last_checkpoint_log, "
                "last_checkpointed, last_error FROM direct_solana_storage_maintenance WHERE id=1"
            ).fetchone()
            persistent = dict(row) if row is not None else {}
        queue_exists = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='direct_solana_hydration_queue' LIMIT 1"
        ).fetchone()
        if queue_exists is not None:
            queue_rows = db.execute(
                "SELECT status, COUNT(*) AS n FROM direct_solana_hydration_queue GROUP BY status"
            ).fetchall()
            queue = {str(row["status"]): int(row["n"]) for row in queue_rows}
        page_size_row = db.execute("PRAGMA page_size").fetchone()
        page_count_row = db.execute("PRAGMA page_count").fetchone()
        freelist_row = db.execute("PRAGMA freelist_count").fetchone()
        page_size = int(page_size_row[0]) if page_size_row is not None else 0
        page_count = int(page_count_row[0]) if page_count_row is not None else 0
        freelist = int(freelist_row[0]) if freelist_row is not None else 0
    except Exception as exc:
        snapshot_error = f"{type(exc).__name__}: nonblocking storage telemetry unavailable"
    finally:
        if db is not None:
            db.close()

    try:
        wal_path = Path(f"{path}-wal")
        wal_bytes: int | None = wal_path.stat().st_size if wal_path.exists() else 0
    except OSError:
        wal_bytes = None
    try:
        stat = os.statvfs(str(path.parent or Path(".")))
        filesystem_free_bytes: int | None = int(stat.f_bavail) * int(stat.f_frsize)
    except OSError:
        filesystem_free_bytes = None

    persisted_queue_pruned = int(persistent.get("queue_rows_pruned") or 0)
    persisted_metric_pruned = int(persistent.get("metric_rows_pruned") or 0)
    checkpoint = state.get("last_checkpoint_result")
    if checkpoint is None and persistent.get("last_checkpoint_at"):
        checkpoint = {
            "busy": persistent.get("last_checkpoint_busy"),
            "log_frames": persistent.get("last_checkpoint_log"),
            "checkpointed_frames": persistent.get("last_checkpointed"),
        }

    return {
        "installed": True,
        "nonblocking_liveness_repair_installed": True,
        "maintenance_connection": "independent-sqlite-wal-connection",
        "shared_process_rlock_used_by_maintenance": False,
        "sqlite_busy_wait_ms": MAINTENANCE_SQLITE_BUSY_TIMEOUT_MS,
        "busy_policy": "yield-immediately-to-live-writer",
        "initial_maintenance_delay_seconds": INITIAL_MAINTENANCE_DELAY_SECONDS,
        "terminal_queue_retention_seconds": capacity.TERMINAL_QUEUE_RETENTION_SECONDS,
        "nonhistorical_hydration_metric_retention_seconds": capacity.HYDRATION_METRIC_RETENTION_SECONDS,
        "maintenance_batch_rows": LIVENESS_SAFE_BATCH_ROWS,
        "maintenance_drain_seconds": LIVENESS_SAFE_DRAIN_SECONDS,
        "maintenance_runs_session": int(state.get("runs", 0) or 0),
        "maintenance_busy_skips_session": int(state.get("busy_skips", 0) or 0),
        "last_maintenance_run_ms": state.get("last_run_ms"),
        "max_maintenance_run_ms_session": state.get("max_run_ms"),
        "terminal_queue_rows_pruned_total": persisted_queue_pruned,
        "nonhistorical_metric_rows_pruned_total": persisted_metric_pruned,
        "terminal_queue_rows_pruned_session": int(state.get("queue_rows_pruned", 0) or 0),
        "nonhistorical_metric_rows_pruned_session": int(state.get("metric_rows_pruned", 0) or 0),
        "last_maintenance_at": persistent.get("last_maintenance_at") or state.get("last_run_at"),
        "last_checkpoint_at": state.get("last_checkpoint_at") or persistent.get("last_checkpoint_at"),
        "last_checkpoint_result": checkpoint,
        "last_error": state.get("last_error") or persistent.get("last_error") or snapshot_error,
        "hydration_queue_current": queue,
        "sqlite_allocated_bytes": page_size * page_count,
        "sqlite_reusable_freelist_bytes": page_size * freelist,
        "sqlite_wal_bytes": wal_bytes,
        "filesystem_free_bytes": filesystem_free_bytes,
        "automatic_vacuum_enabled": False,
        "wal_checkpoint_truncate_enabled": True,
        "canonical_evidence_pruned": False,
        "historical_recovery_metrics_pruned": False,
        "raw_receipt_retention_or_scope_changed": False,
        "pending_or_processing_queue_rows_pruned": False,
        "certification_thresholds_unchanged": True,
        "paper_only_authority_unchanged": True,
    }


async def _nonblocking_storage_maintenance_worker(self: Any, stop: asyncio.Event) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=INITIAL_MAINTENANCE_DELAY_SECONDS)
        return
    except asyncio.TimeoutError:
        pass

    next_checkpoint = 0.0
    drained_since_checkpoint = False
    while not stop.is_set():
        queue_rows = 0
        metric_rows = 0
        busy = False
        try:
            queue_rows, metric_rows, busy = await asyncio.to_thread(_nonblocking_prune_once, self)
            if queue_rows or metric_rows:
                drained_since_checkpoint = True
            now_mono = time.monotonic()
            drain_complete = not queue_rows and not metric_rows and drained_since_checkpoint and not busy
            if now_mono >= next_checkpoint or drain_complete:
                await asyncio.to_thread(_nonblocking_checkpoint, self)
                next_checkpoint = now_mono + capacity.WAL_CHECKPOINT_INTERVAL_SECONDS
                drained_since_checkpoint = False
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _runtime_state(self)["last_error"] = (
                f"{type(exc).__name__}: nonblocking storage maintenance worker failed"
            )

        if busy:
            delay = LIVENESS_SAFE_DRAIN_SECONDS
        elif queue_rows or metric_rows:
            delay = LIVENESS_SAFE_DRAIN_SECONDS
        else:
            delay = capacity.MAINTENANCE_IDLE_SECONDS
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            continue


def install_storage_maintenance_liveness_repair() -> None:
    # The PR #77 run wrapper resolves these names from the capacity module at task
    # execution time. Replacing the worker/functions here keeps the already-tested
    # run composition and all ingestion/polling semantics intact.
    capacity.MAINTENANCE_BATCH_ROWS = LIVENESS_SAFE_BATCH_ROWS
    capacity.MAINTENANCE_DRAIN_SECONDS = LIVENESS_SAFE_DRAIN_SECONDS
    capacity._storage_maintenance_worker = _nonblocking_storage_maintenance_worker  # type: ignore[assignment]
    capacity._prune_operational_rows_once = _nonblocking_prune_once  # type: ignore[assignment]
    capacity._checkpoint_wal = _nonblocking_checkpoint  # type: ignore[assignment]
    capacity._maintenance_snapshot = _nonblocking_snapshot  # type: ignore[assignment]


__all__ = [
    "INITIAL_MAINTENANCE_DELAY_SECONDS",
    "LIVENESS_SAFE_BATCH_ROWS",
    "LIVENESS_SAFE_DRAIN_SECONDS",
    "MAINTENANCE_SQLITE_BUSY_TIMEOUT_MS",
    "_nonblocking_checkpoint",
    "_nonblocking_prune_once",
    "_nonblocking_snapshot",
    "_nonblocking_storage_maintenance_worker",
    "install_storage_maintenance_liveness_repair",
]
