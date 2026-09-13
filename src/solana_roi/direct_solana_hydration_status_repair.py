from __future__ import annotations

import os
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

from .direct_solana import DirectSolanaJournal, utcnow


REPAIR_VERSION = "direct-solana-hydration-status-incremental-state-v1"
BOOTSTRAP_BATCH_ROWS = 5000
RECENT_STATE_MAX_ROWS = 1000
STATUS_SAMPLE_ROWS = 500
BOOTSTRAP_MIN_INTERVAL_SECONDS = 1.0
STEADY_PRUNE_INTERVAL_SECONDS = 60.0
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
CERTIFICATION_THRESHOLDS_CHANGED = False
SOURCE_HISTORY_MUTATED = False


def _ensure_state(self: DirectSolanaJournal) -> None:
    if bool(getattr(self, "_roi_hydration_status_state_ready", False)):
        return
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "CREATE TABLE IF NOT EXISTS direct_solana_hydration_status_recent ("
            "signature TEXT PRIMARY KEY, source_rowid INTEGER NOT NULL UNIQUE, "
            "hydrated_at TEXT NOT NULL, total_hydration_ms REAL NOT NULL, normalized INTEGER NOT NULL)"
        )
        self.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_direct_hydration_status_recent_time "
            "ON direct_solana_hydration_status_recent(hydrated_at DESC, signature DESC)"
        )
        self.store.db.execute(
            "CREATE TABLE IF NOT EXISTS direct_solana_hydration_status_meta ("
            "id INTEGER PRIMARY KEY CHECK(id=1), bootstrap_rowid INTEGER NOT NULL DEFAULT 0, "
            "bootstrap_complete INTEGER NOT NULL DEFAULT 0, last_batch_rows INTEGER NOT NULL DEFAULT 0, "
            "last_batch_at TEXT)"
        )
        self.store.db.execute(
            "INSERT OR IGNORE INTO direct_solana_hydration_status_meta(id) VALUES (1)"
        )
        # New or updated hydration truth is captured immediately while the old source
        # table is reconstructed incrementally.  The source table remains canonical.
        self.store.db.execute(
            "CREATE TRIGGER IF NOT EXISTS trg_direct_hydration_status_insert "
            "AFTER INSERT ON direct_solana_hydration_metrics BEGIN "
            "DELETE FROM direct_solana_hydration_status_recent WHERE signature=NEW.signature; "
            "INSERT INTO direct_solana_hydration_status_recent("
            "signature, source_rowid, hydrated_at, total_hydration_ms, normalized) "
            "SELECT NEW.signature, NEW.rowid, NEW.hydrated_at, NEW.total_hydration_ms, NEW.normalized "
            "WHERE NEW.historical_recovery=0; END"
        )
        self.store.db.execute(
            "CREATE TRIGGER IF NOT EXISTS trg_direct_hydration_status_update "
            "AFTER UPDATE ON direct_solana_hydration_metrics BEGIN "
            "DELETE FROM direct_solana_hydration_status_recent WHERE signature=NEW.signature; "
            "INSERT INTO direct_solana_hydration_status_recent("
            "signature, source_rowid, hydrated_at, total_hydration_ms, normalized) "
            "SELECT NEW.signature, NEW.rowid, NEW.hydrated_at, NEW.total_hydration_ms, NEW.normalized "
            "WHERE NEW.historical_recovery=0; END"
        )
        self.store.db.execute(
            "CREATE TRIGGER IF NOT EXISTS trg_direct_hydration_status_delete "
            "AFTER DELETE ON direct_solana_hydration_metrics BEGIN "
            "DELETE FROM direct_solana_hydration_status_recent WHERE signature=OLD.signature; END"
        )
    setattr(self, "_roi_hydration_status_state_ready", True)


def _drop_clean_file_cache_hint(self: DirectSolanaJournal) -> None:
    """Best-effort release of clean bootstrap pages; never changes durability."""
    advise = getattr(os, "posix_fadvise", None)
    dontneed = getattr(os, "POSIX_FADV_DONTNEED", None)
    if not callable(advise) or dontneed is None:
        return
    try:
        path = Path(getattr(self.store, "path", ""))
        if not str(path) or not path.exists():
            return
        fd = os.open(path, os.O_RDONLY)
        try:
            advise(fd, 0, 0, dontneed)
        finally:
            os.close(fd)
    except OSError:
        return


def _prune_recent_state(self: DirectSolanaJournal) -> int:
    with self.store._lock, self.store.db:
        cur = self.store.db.execute(
            "DELETE FROM direct_solana_hydration_status_recent WHERE signature IN ("
            "SELECT signature FROM direct_solana_hydration_status_recent "
            "ORDER BY hydrated_at DESC, signature DESC LIMIT -1 OFFSET ?)",
            (RECENT_STATE_MAX_ROWS,),
        )
    return max(0, int(cur.rowcount or 0))


def _meta(self: DirectSolanaJournal) -> dict[str, Any]:
    _ensure_state(self)
    with self.store._lock:
        row = self.store.db.execute(
            "SELECT bootstrap_rowid, bootstrap_complete, last_batch_rows, last_batch_at "
            "FROM direct_solana_hydration_status_meta WHERE id=1"
        ).fetchone()
        state_row = self.store.db.execute(
            "SELECT COUNT(*) AS n FROM direct_solana_hydration_status_recent"
        ).fetchone()
    return {
        "bootstrap_rowid": int(row["bootstrap_rowid"] or 0) if row is not None else 0,
        "bootstrap_complete": bool(row["bootstrap_complete"]) if row is not None else False,
        "last_batch_rows": int(row["last_batch_rows"] or 0) if row is not None else 0,
        "last_batch_at": row["last_batch_at"] if row is not None else None,
        "state_rows": int(state_row["n"] or 0) if state_row is not None else 0,
    }


def _emit_phase(
    self: DirectSolanaJournal,
    phase: str,
    *,
    before: dict[str, Any] | None,
    started: float,
    detail: dict[str, Any],
) -> None:
    if before is None:
        return
    try:
        from .sqlite_phase_observability import emit_phase, resource_snapshot

        emit_phase(
            phase,
            before=before,
            after=resource_snapshot(self.store),
            duration_ms=(time.perf_counter() - started) * 1000.0,
            detail=detail,
        )
    except Exception:
        return


def _resource_before(self: DirectSolanaJournal) -> dict[str, Any] | None:
    try:
        from .sqlite_phase_observability import resource_snapshot

        return resource_snapshot(self.store)
    except Exception:
        return None


def _advance_bootstrap(self: DirectSolanaJournal) -> tuple[bool, int, int]:
    """Advance exact source reconstruction by one physical-row keyset batch."""
    _ensure_state(self)
    before = _resource_before(self)
    started = time.perf_counter()
    batch_rows = 0
    next_cursor = 0
    complete = False
    with self.store._lock, self.store.db:
        meta = self.store.db.execute(
            "SELECT bootstrap_rowid, bootstrap_complete "
            "FROM direct_solana_hydration_status_meta WHERE id=1"
        ).fetchone()
        cursor = int(meta["bootstrap_rowid"] or 0) if meta is not None else 0
        if meta is not None and bool(meta["bootstrap_complete"]):
            return True, 0, cursor

        rows = self.store.db.execute(
            "SELECT rowid, signature, hydrated_at, total_hydration_ms, normalized, historical_recovery "
            "FROM direct_solana_hydration_metrics WHERE rowid>? ORDER BY rowid LIMIT ?",
            (cursor, BOOTSTRAP_BATCH_ROWS),
        ).fetchall()
        batch_rows = len(rows)
        for row in rows:
            signature = str(row["signature"])
            if int(row["historical_recovery"] or 0) != 0:
                self.store.db.execute(
                    "DELETE FROM direct_solana_hydration_status_recent WHERE signature=?",
                    (signature,),
                )
                continue
            self.store.db.execute(
                "INSERT INTO direct_solana_hydration_status_recent("
                "signature, source_rowid, hydrated_at, total_hydration_ms, normalized) "
                "VALUES (?, ?, ?, ?, ?) ON CONFLICT(signature) DO UPDATE SET "
                "source_rowid=excluded.source_rowid, hydrated_at=excluded.hydrated_at, "
                "total_hydration_ms=excluded.total_hydration_ms, normalized=excluded.normalized "
                "WHERE excluded.hydrated_at>=direct_solana_hydration_status_recent.hydrated_at",
                (
                    signature,
                    int(row["rowid"]),
                    str(row["hydrated_at"]),
                    float(row["total_hydration_ms"]),
                    int(row["normalized"] or 0),
                ),
            )

        if rows:
            next_cursor = int(rows[-1]["rowid"])
        else:
            next_cursor = cursor
        complete = batch_rows < BOOTSTRAP_BATCH_ROWS
        self.store.db.execute(
            "UPDATE direct_solana_hydration_status_meta SET bootstrap_rowid=?, "
            "bootstrap_complete=?, last_batch_rows=?, last_batch_at=? WHERE id=1",
            (
                next_cursor,
                1 if complete else 0,
                batch_rows,
                utcnow().isoformat(),
            ),
        )
        self.store.db.execute(
            "DELETE FROM direct_solana_hydration_status_recent WHERE signature IN ("
            "SELECT signature FROM direct_solana_hydration_status_recent "
            "ORDER BY hydrated_at DESC, signature DESC LIMIT -1 OFFSET ?)",
            (RECENT_STATE_MAX_ROWS,),
        )

    _drop_clean_file_cache_hint(self)
    _emit_phase(
        self,
        "direct-solana-hydration-status:bootstrap",
        before=before,
        started=started,
        detail={
            "rows_examined": batch_rows,
            "bootstrap_rowid": next_cursor,
            "bootstrap_complete": complete,
            "batch_limit": BOOTSTRAP_BATCH_ROWS,
            "source_history_mutated": False,
        },
    )
    return complete, batch_rows, next_cursor


def _recent_metrics(self: DirectSolanaJournal) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    _ensure_state(self)
    state = _meta(self)
    now_mono = time.monotonic()
    if not bool(state["bootstrap_complete"]):
        next_allowed = float(getattr(self, "_roi_hydration_status_next_bootstrap", 0.0) or 0.0)
        if now_mono >= next_allowed:
            complete, _batch_rows, _cursor = _advance_bootstrap(self)
            setattr(
                self,
                "_roi_hydration_status_next_bootstrap",
                now_mono + BOOTSTRAP_MIN_INTERVAL_SECONDS,
            )
            state = _meta(self)
            if not complete:
                # Exact latest-500 status is unknown until every pre-repair source row
                # has been considered.  Fail closed rather than fall back to the old
                # history-scaled scan or publish a partial percentile as exact.
                return [], state
        else:
            return [], state

    last_prune = float(getattr(self, "_roi_hydration_status_last_prune", 0.0) or 0.0)
    if now_mono - last_prune >= STEADY_PRUNE_INTERVAL_SECONDS:
        _prune_recent_state(self)
        setattr(self, "_roi_hydration_status_last_prune", now_mono)

    before = None
    emit_steady = now_mono >= float(
        getattr(self, "_roi_hydration_status_next_steady_telemetry", 0.0) or 0.0
    )
    if emit_steady:
        before = _resource_before(self)
    started = time.perf_counter()
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT total_hydration_ms, normalized "
            "FROM direct_solana_hydration_status_recent "
            "ORDER BY hydrated_at DESC, signature DESC LIMIT ?",
            (STATUS_SAMPLE_ROWS,),
        ).fetchall()
    metrics = [dict(row) for row in rows]
    if emit_steady:
        _emit_phase(
            self,
            "direct-solana-hydration-status:steady",
            before=before,
            started=started,
            detail={
                "rows_returned": len(metrics),
                "query_mode": "indexed_recent_state",
                "source_table_scanned": False,
            },
        )
        setattr(
            self,
            "_roi_hydration_status_next_steady_telemetry",
            now_mono + 60.0,
        )
    return metrics, _meta(self)


def _bounded_status(self: DirectSolanaJournal) -> dict[str, Any]:
    now = utcnow()
    with self.store._lock:
        providers = [
            dict(row)
            for row in self.store.db.execute(
                "SELECT provider, connected, connected_at, last_message_at, "
                "reconnect_count, last_error_type FROM direct_solana_provider_state ORDER BY provider"
            ).fetchall()
        ]
        global_row = self.store.db.execute(
            "SELECT outage_started_at, unresolved_gap, last_backfill_complete_at, "
            "last_backfill_error FROM direct_solana_global_state WHERE id=1"
        ).fetchone()
        queue_rows = self.store.db.execute(
            "SELECT status, COUNT(*) AS n FROM direct_solana_hydration_queue GROUP BY status"
        ).fetchall()
        source_rows = self.store.db.execute(
            "SELECT source, SUM(receipt_count) AS n FROM direct_solana_minute_receipts "
            "WHERE bucket>=? GROUP BY source",
            ((now - timedelta(hours=1)).replace(second=0, microsecond=0).isoformat(),),
        ).fetchall()

    metrics, hydration_state = _recent_metrics(self)
    values = sorted(float(row["total_hydration_ms"]) for row in metrics)
    p95 = values[min(len(values) - 1, int((len(values) - 1) * 0.95))] if values else None
    queue = {str(row["status"]): int(row["n"]) for row in queue_rows}
    sources = {str(row["source"]): int(row["n"]) for row in source_rows}
    unresolved = bool(global_row["unresolved_gap"]) if global_row is not None else True
    connected = sum(1 for row in providers if bool(row["connected"]))
    outage_started = str(global_row["outage_started_at"] or "") if global_row is not None else ""
    backfill_at = str(global_row["last_backfill_complete_at"] or "") if global_row is not None else ""
    backfill_error = str(global_row["last_backfill_error"] or "") if global_row is not None else ""
    return {
        "durable": True,
        "connected_provider_count": connected,
        "provider_states": providers,
        "continuity_ok": connected >= 1 and not unresolved,
        "unresolved_gap": unresolved,
        "outage_started_at": outage_started or None,
        "last_backfill_complete_at": backfill_at or None,
        "last_backfill_error": backfill_error or None,
        "hydration_queue": queue,
        "raw_receipts_last_hour_by_source": sources,
        "hydration_sample_count": len(metrics),
        "hydration_normalized_count": sum(1 for row in metrics if bool(row["normalized"])),
        "p95_hydration_ms": p95,
        "hydration_status_repair": {
            "version": REPAIR_VERSION,
            "bootstrap_complete": bool(hydration_state["bootstrap_complete"]),
            "bootstrap_rowid": int(hydration_state["bootstrap_rowid"]),
            "last_batch_rows": int(hydration_state["last_batch_rows"]),
            "state_rows": int(hydration_state["state_rows"]),
            "bootstrap_batch_rows": BOOTSTRAP_BATCH_ROWS,
            "steady_query": "indexed_recent_state",
            "source_history_mutated": SOURCE_HISTORY_MUTATED,
            "paper_only": PAPER_ONLY,
            "live_money_authority": LIVE_MONEY_AUTHORITY,
        },
    }


setattr(_bounded_status, "_roi_hydration_status_bounded_io", True)


def configure_direct_solana_hydration_status_repair() -> None:
    """Replace only the history-scaled hydration-statistics read before composition."""
    current = DirectSolanaJournal.status
    if bool(getattr(current, "_roi_hydration_status_bounded_io", False)):
        return
    DirectSolanaJournal.status = _bounded_status  # type: ignore[method-assign]


__all__ = [
    "BOOTSTRAP_BATCH_ROWS",
    "RECENT_STATE_MAX_ROWS",
    "REPAIR_VERSION",
    "STATUS_SAMPLE_ROWS",
    "_advance_bootstrap",
    "_bounded_status",
    "_ensure_state",
    "configure_direct_solana_hydration_status_repair",
]
