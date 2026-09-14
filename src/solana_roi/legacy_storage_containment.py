from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from .observation_store import ObservationEventStore


class LegacyContainedObservationEventStore(ObservationEventStore):
    """Legacy-authoritative store with conservative bounded retention maintenance.

    This is deliberately not the historical purge. It only removes positively
    classified low-value rows in tiny rowid batches while the legacy database is
    still authoritative. It never deletes event lineage, paper checkpoints,
    strategy/portfolio authority, unresolved transport, wallet-forward source
    evidence, or current continuity state. It never VACUUMs or checkpoints WAL.
    SQLite contention is fail-open for maintenance only: trading/evidence writes
    remain authoritative and containment simply retries on a later interval.
    """

    _LATEST_SAMPLE_SURFACES: dict[str, tuple[str, int]] = {
        "execution_quote_observations": ("id", 500),
        "shadow_execution_observations": ("id", 500),
        "risk_refresh_measurements": ("id", 500),
        "program_coverage_observations": ("id", 500),
    }

    def __init__(
        self,
        path: str | Path = "data/solana-roi.sqlite3",
        *,
        containment_interval_seconds: float = 60.0,
        batch_rows: int = 128,
        monotonic_fn: Callable[[], float] = time.monotonic,
    ):
        self._containment_interval_seconds = max(5.0, float(containment_interval_seconds))
        self._containment_batch_rows = max(1, min(1024, int(batch_rows)))
        self._containment_monotonic = monotonic_fn
        self._containment_last_run = float("-inf")
        self._containment_scan_cursors: dict[str, int] = {}
        self._containment_runs = 0
        self._containment_deleted = 0
        self._containment_errors = 0
        super().__init__(path)

    @staticmethod
    def _iso_before(value: Any, cutoff: datetime) -> bool:
        if value is None:
            return False
        raw = str(value).strip()
        if not raw:
            return False
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return False
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc) < cutoff

    def _protected_latest_ids(self, connection: sqlite3.Connection, table: str) -> set[int]:
        spec = self._LATEST_SAMPLE_SURFACES.get(table)
        if spec is None:
            return set()
        column, limit = spec
        try:
            rows = connection.execute(
                f"SELECT {column} FROM {table} ORDER BY {column} DESC LIMIT ?", (int(limit),)
            ).fetchall()
        except sqlite3.Error:
            return set()
        return {int(row[0]) for row in rows if row[0] is not None}

    def _eligible(self, table: str, row: sqlite3.Row, *, now: datetime, protected: set[int]) -> bool:
        cutoff7 = now - timedelta(days=7)
        cutoff31 = now - timedelta(days=31)
        keys = set(row.keys())

        if table == "direct_solana_recent_receipts":
            return self._iso_before(row["expires_at"], now)
        if table == "helius_webhook_inbox":
            return str(row["state"] or "") == "complete" and self._iso_before(row["updated_at"], cutoff7)
        if table == "direct_solana_hydration_queue":
            return str(row["status"] or "") == "complete" and self._iso_before(row["updated_at"], cutoff7)
        if table == "wallet_realtime_receipts":
            return str(row["status"] or "") == "complete" and self._iso_before(row["updated_at"], cutoff7)
        if table == "execution_quote_observations":
            return int(row["id"]) not in protected and self._iso_before(row["received_at"], cutoff31)
        if table == "shadow_execution_observations":
            return int(row["id"]) not in protected and self._iso_before(row["completed_at"], cutoff31)
        if table == "risk_refresh_measurements":
            return int(row["id"]) not in protected and self._iso_before(row["completed_at"], cutoff7)
        if table == "program_coverage_observations":
            return int(row["id"]) not in protected and self._iso_before(row["assessed_at"], cutoff31)
        if table == "semantic_candidate_events":
            return self._iso_before(row["received_at"], cutoff31)
        if table == "semantic_candidate_opportunities":
            return self._iso_before(row["last_seen"], cutoff31)
        if table == "semantic_candidate_risk_state":
            return self._iso_before(row["assessed_at"], cutoff31)
        # Unknown table/schema is never deletion authority.
        _ = keys
        return False

    def contain_once(self, *, now: datetime | None = None) -> dict[str, Any]:
        """Run one bounded maintenance pass without scanning table history."""
        instant = now or datetime.now(timezone.utc)
        if instant.tzinfo is None:
            instant = instant.replace(tzinfo=timezone.utc)
        else:
            instant = instant.astimezone(timezone.utc)

        connection = sqlite3.connect(self.path, timeout=0.1)
        connection.row_factory = sqlite3.Row
        deleted_by_table: dict[str, int] = {}
        scanned_by_table: dict[str, int] = {}
        try:
            connection.execute("PRAGMA busy_timeout=100")
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            managed = (
                "direct_solana_recent_receipts",
                "helius_webhook_inbox",
                "direct_solana_hydration_queue",
                "wallet_realtime_receipts",
                "execution_quote_observations",
                "shadow_execution_observations",
                "risk_refresh_measurements",
                "program_coverage_observations",
                "semantic_candidate_events",
                "semantic_candidate_opportunities",
                "semantic_candidate_risk_state",
            )
            for table in managed:
                if table not in tables:
                    continue
                cursor = int(self._containment_scan_cursors.get(table, 0))
                try:
                    rows = connection.execute(
                        f"SELECT rowid AS _roi_rowid,* FROM {table} WHERE rowid>? ORDER BY rowid LIMIT ?",
                        (cursor, self._containment_batch_rows),
                    ).fetchall()
                except sqlite3.Error:
                    continue
                if not rows:
                    self._containment_scan_cursors[table] = 0
                    continue
                scanned_by_table[table] = len(rows)
                protected = self._protected_latest_ids(connection, table)
                delete_rowids = [
                    int(row["_roi_rowid"])
                    for row in rows
                    if self._eligible(table, row, now=instant, protected=protected)
                ]
                self._containment_scan_cursors[table] = int(rows[-1]["_roi_rowid"])
                if delete_rowids:
                    placeholders = ",".join("?" for _ in delete_rowids)
                    result = connection.execute(
                        f"DELETE FROM {table} WHERE rowid IN ({placeholders})",
                        tuple(delete_rowids),
                    )
                    deleted_by_table[table] = max(0, int(result.rowcount))
            connection.commit()
        finally:
            connection.close()

        deleted = sum(deleted_by_table.values())
        self._containment_runs += 1
        self._containment_deleted += deleted
        return {
            "mode": "bounded_positive_legacy_containment",
            "scanned_rows": sum(scanned_by_table.values()),
            "deleted_rows": deleted,
            "scanned_by_table": scanned_by_table,
            "deleted_by_table": deleted_by_table,
            "batch_rows_per_table": self._containment_batch_rows,
            "vacuum": False,
            "wal_checkpoint": False,
            "event_lineage_touched": False,
            "paper_authority_touched": False,
        }

    def _maybe_contain(self) -> None:
        now_mono = float(self._containment_monotonic())
        if now_mono - self._containment_last_run < self._containment_interval_seconds:
            return
        self._containment_last_run = now_mono
        try:
            report = self.contain_once()
            if int(report.get("deleted_rows") or 0) > 0:
                print(
                    "ROI_LEGACY_STORAGE_CONTAINMENT "
                    + __import__("json").dumps(report, sort_keys=True, default=str),
                    flush=True,
                )
        except sqlite3.Error:
            self._containment_errors += 1
        except Exception:
            # Maintenance must never become trading/evidence write authority.
            self._containment_errors += 1

    def append(self, event_type: str, observed_at: str, payload: dict[str, Any]) -> str:
        lineage = super().append(event_type, observed_at, payload)
        self._maybe_contain()
        return lineage

    def containment_status(self) -> dict[str, Any]:
        return {
            "enabled": True,
            "mode": "bounded_positive_legacy_containment",
            "interval_seconds": self._containment_interval_seconds,
            "batch_rows_per_table": self._containment_batch_rows,
            "runs": self._containment_runs,
            "deleted_rows": self._containment_deleted,
            "errors": self._containment_errors,
            "vacuum": False,
            "wal_checkpoint": False,
            "event_lineage_touched": False,
            "paper_authority_touched": False,
        }


__all__ = ["LegacyContainedObservationEventStore"]
