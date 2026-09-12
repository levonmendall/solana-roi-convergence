from __future__ import annotations

from datetime import timedelta
from typing import Any

from . import continuity_storage_capacity_repair as storage
from . import direct_solana as direct_solana_module


REPAIR_VERSION = "storage-maintenance-bounded-rowid-cursor-v1"
_DELETE_CHUNK_ROWS = 400
_INSTALLED = False


def _ensure_cursor_state(self: Any) -> None:
    storage._ensure_maintenance_state(self)
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "CREATE TABLE IF NOT EXISTS direct_solana_storage_maintenance_cursor ("
            "id INTEGER PRIMARY KEY CHECK(id=1), "
            "metric_scan_rowid INTEGER NOT NULL DEFAULT 0, "
            "last_scan_rows INTEGER NOT NULL DEFAULT 0, "
            "last_scan_at TEXT)"
        )
        self.store.db.execute(
            "INSERT OR IGNORE INTO direct_solana_storage_maintenance_cursor(id) VALUES (1)"
        )


def _delete_metric_rowids(self: Any, rowids: list[int]) -> int:
    deleted = 0
    for start in range(0, len(rowids), _DELETE_CHUNK_ROWS):
        chunk = rowids[start : start + _DELETE_CHUNK_ROWS]
        if not chunk:
            continue
        placeholders = ",".join("?" for _ in chunk)
        cur = self.store.db.execute(
            f"DELETE FROM direct_solana_hydration_metrics WHERE rowid IN ({placeholders})",
            tuple(chunk),
        )
        deleted += int(cur.rowcount or 0)
    return deleted


def _bounded_prune_operational_rows_once(self: Any) -> tuple[int, int]:
    """Prune the same eligible rows without a history-sized ORDER BY scan.

    The prior metric selector searched all historical metrics for the oldest 5,000
    eligible rows on every drain pass. This repair advances a durable physical-row
    cursor through at most ``MAINTENANCE_BATCH_ROWS`` rows per pass, evaluates the
    unchanged retention predicate inside that bounded window, and deletes only rows
    that the former predicate could delete. At end-of-table the cursor wraps to zero
    so every surviving/new row remains eligible for future inspection.
    """
    now = direct_solana_module.utcnow()
    queue_cutoff = (now - timedelta(seconds=storage.TERMINAL_QUEUE_RETENTION_SECONDS)).isoformat()
    metric_cutoff = (now - timedelta(seconds=storage.HYDRATION_METRIC_RETENTION_SECONDS)).isoformat()
    _ensure_cursor_state(self)

    with self.store._lock, self.store.db:
        queue_cur = self.store.db.execute(
            "DELETE FROM direct_solana_hydration_queue WHERE signature IN ("
            "SELECT signature FROM direct_solana_hydration_queue "
            "WHERE status IN ('complete','failed') AND updated_at<? "
            "ORDER BY updated_at, signature LIMIT ?)",
            (queue_cutoff, storage.MAINTENANCE_BATCH_ROWS),
        )
        queue_rows = int(queue_cur.rowcount or 0)

        cursor_row = self.store.db.execute(
            "SELECT metric_scan_rowid FROM direct_solana_storage_maintenance_cursor WHERE id=1"
        ).fetchone()
        cursor = int(cursor_row[0]) if cursor_row is not None else 0
        scan_rows = self.store.db.execute(
            "SELECT rowid, hydrated_at, historical_recovery "
            "FROM direct_solana_hydration_metrics WHERE rowid>? "
            "ORDER BY rowid LIMIT ?",
            (cursor, storage.MAINTENANCE_BATCH_ROWS),
        ).fetchall()

        eligible_rowids = [
            int(row[0])
            for row in scan_rows
            if int(row[2] or 0) == 0 and str(row[1]) < metric_cutoff
        ]
        metric_rows = _delete_metric_rowids(self, eligible_rowids)

        if scan_rows and len(scan_rows) >= storage.MAINTENANCE_BATCH_ROWS:
            next_cursor = int(scan_rows[-1][0])
        else:
            next_cursor = 0
        self.store.db.execute(
            "UPDATE direct_solana_storage_maintenance_cursor SET "
            "metric_scan_rowid=?, last_scan_rows=?, last_scan_at=? WHERE id=1",
            (next_cursor, len(scan_rows), now.isoformat()),
        )
        self.store.db.execute(
            "UPDATE direct_solana_storage_maintenance SET "
            "queue_rows_pruned=queue_rows_pruned+?, metric_rows_pruned=metric_rows_pruned+?, "
            "last_maintenance_at=?, last_error=NULL WHERE id=1",
            (queue_rows, metric_rows, now.isoformat()),
        )

    return queue_rows, metric_rows


def install_storage_maintenance_bounded_io_repair() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    current = storage._prune_operational_rows_once
    if not bool(getattr(current, "_roi_bounded_storage_maintenance", False)):
        setattr(_bounded_prune_operational_rows_once, "_roi_bounded_storage_maintenance", True)
        storage._prune_operational_rows_once = _bounded_prune_operational_rows_once
    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "_bounded_prune_operational_rows_once",
    "install_storage_maintenance_bounded_io_repair",
]
