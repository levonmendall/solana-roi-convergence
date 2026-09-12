from __future__ import annotations

import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .observation_store import ObservationEventStore


REPAIR_VERSION = "shadow-price-tracked-mints-incremental-state-v2"
BOOTSTRAP_BATCH_ROWS = 5_000
STATE_PRUNE_BATCH_ROWS = 1_000
STATE_PRUNE_INTERVAL_SECONDS = 60.0
STEADY_TELEMETRY_INTERVAL_SECONDS = 60.0
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
CERTIFICATION_THRESHOLDS_CHANGED = False

_STATE_TABLE = "shadow_price_tracked_mints_state"
_META_TABLE = "shadow_price_tracked_mints_meta"
_TRIGGER = "trg_shadow_price_track_first_touch"
_INSTALL_LOCK = threading.Lock()
_TELEMETRY_LOCK = threading.Lock()
_INSTALLED = False
_LAST_PRUNE_MONOTONIC = 0.0
_LAST_STEADY_TELEMETRY_MONOTONIC = 0.0
_ORIGINAL_TRACKED_MINTS = ObservationEventStore.tracked_mints


def _release_db_file_cache(path: Path) -> None:
    fadvise = getattr(os, "posix_fadvise", None)
    advice = getattr(os, "POSIX_FADV_DONTNEED", None)
    if fadvise is None or advice is None:
        return
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        try:
            fadvise(fd, 0, 0, advice)
        except OSError:
            pass
    finally:
        os.close(fd)


def _ensure_state(store: ObservationEventStore) -> None:
    with store._lock, store.db:
        store.db.execute(
            f"CREATE TABLE IF NOT EXISTS {_STATE_TABLE} ("
            "token_mint TEXT PRIMARY KEY, "
            "observed_at TEXT NOT NULL, "
            "source_rowid INTEGER NOT NULL UNIQUE)"
        )
        store.db.execute(
            f"CREATE INDEX IF NOT EXISTS ix_{_STATE_TABLE}_observed "
            f"ON {_STATE_TABLE}(observed_at DESC, token_mint)"
        )
        store.db.execute(
            f"CREATE TABLE IF NOT EXISTS {_META_TABLE} ("
            "id INTEGER PRIMARY KEY CHECK(id=1), "
            "bootstrap_rowid INTEGER NOT NULL DEFAULT 0, "
            "bootstrap_complete INTEGER NOT NULL DEFAULT 0, "
            "bootstrap_horizon_seconds REAL NOT NULL DEFAULT 0, "
            "bootstrap_floor_at TEXT, "
            "last_batch_rows INTEGER NOT NULL DEFAULT 0, "
            "last_batch_at TEXT)"
        )
        store.db.execute(f"INSERT OR IGNORE INTO {_META_TABLE}(id) VALUES (1)")
        store.db.execute(
            f"CREATE TRIGGER IF NOT EXISTS {_TRIGGER} "
            "AFTER INSERT ON token_first_touches BEGIN "
            f"INSERT INTO {_STATE_TABLE}(token_mint, observed_at, source_rowid) "
            "VALUES (NEW.token_mint, NEW.observed_at, NEW.rowid) "
            "ON CONFLICT(token_mint) DO UPDATE SET "
            "observed_at=excluded.observed_at, source_rowid=excluded.source_rowid; "
            "END"
        )


def _meta(store: ObservationEventStore) -> dict[str, Any]:
    _ensure_state(store)
    with store._lock:
        row = store.db.execute(
            f"SELECT bootstrap_rowid, bootstrap_complete, bootstrap_horizon_seconds, "
            f"bootstrap_floor_at, last_batch_rows, last_batch_at FROM {_META_TABLE} WHERE id=1"
        ).fetchone()
    if row is None:
        raise RuntimeError("shadow-price tracked-mint meta state missing")
    return dict(row)


def _reset_for_wider_horizon(
    store: ObservationEventStore,
    *,
    as_of: datetime,
    horizon_seconds: float,
) -> None:
    floor_at = (as_of - timedelta(seconds=max(0.0, float(horizon_seconds)))).isoformat()
    with store._lock, store.db:
        store.db.execute(
            f"UPDATE {_META_TABLE} SET bootstrap_rowid=0, bootstrap_complete=0, "
            "bootstrap_horizon_seconds=?, bootstrap_floor_at=?, last_batch_rows=0, "
            "last_batch_at=? WHERE id=1",
            (float(horizon_seconds), floor_at, as_of.isoformat()),
        )
        # Existing state may contain a subset useful for the wider horizon. Keep it;
        # the bounded source bootstrap fills any missing pre-install rows exactly.


def _read_source_batch(
    store: ObservationEventStore,
    *,
    after_rowid: int,
) -> list[sqlite3.Row]:
    path = Path(store.path).resolve()
    uri = f"{path.as_uri()}?mode=ro"
    reader = sqlite3.connect(uri, uri=True, check_same_thread=False)
    reader.row_factory = sqlite3.Row
    try:
        reader.execute("PRAGMA query_only=ON")
        reader.execute("PRAGMA cache_size=-1024")
        return reader.execute(
            "SELECT rowid, token_mint, observed_at FROM token_first_touches "
            "WHERE rowid>? ORDER BY rowid LIMIT ?",
            (int(after_rowid), BOOTSTRAP_BATCH_ROWS),
        ).fetchall()
    finally:
        reader.close()
        # Bootstrap is the only time we must inspect pre-repair history. Release the
        # canonical DB's clean file cache after each bounded batch so migration work
        # cannot accumulate toward the 2 GiB cgroup ceiling.
        _release_db_file_cache(path)


def _advance_bootstrap(
    store: ObservationEventStore,
    *,
    as_of: datetime,
    horizon_seconds: float,
) -> tuple[bool, int, int]:
    state = _meta(store)
    configured_horizon = float(state.get("bootstrap_horizon_seconds") or 0.0)
    if configured_horizon + 1e-9 < float(horizon_seconds):
        _reset_for_wider_horizon(
            store,
            as_of=as_of,
            horizon_seconds=float(horizon_seconds),
        )
        state = _meta(store)

    if bool(state.get("bootstrap_complete")):
        return True, 0, int(state.get("bootstrap_rowid") or 0)

    floor_at = str(state.get("bootstrap_floor_at") or "")
    if not floor_at:
        _reset_for_wider_horizon(
            store,
            as_of=as_of,
            horizon_seconds=float(horizon_seconds),
        )
        state = _meta(store)
        floor_at = str(state.get("bootstrap_floor_at") or "")

    cursor = int(state.get("bootstrap_rowid") or 0)
    rows = _read_source_batch(store, after_rowid=cursor)
    recent = [
        (str(row["token_mint"]), str(row["observed_at"]), int(row["rowid"]))
        for row in rows
        if str(row["observed_at"]) >= floor_at
    ]
    next_cursor = int(rows[-1]["rowid"]) if rows else cursor
    complete = len(rows) < BOOTSTRAP_BATCH_ROWS

    with store._lock, store.db:
        if recent:
            store.db.executemany(
                f"INSERT INTO {_STATE_TABLE}(token_mint, observed_at, source_rowid) "
                "VALUES (?, ?, ?) ON CONFLICT(token_mint) DO UPDATE SET "
                "observed_at=excluded.observed_at, source_rowid=excluded.source_rowid",
                recent,
            )
        store.db.execute(
            f"UPDATE {_META_TABLE} SET bootstrap_rowid=?, bootstrap_complete=?, "
            "last_batch_rows=?, last_batch_at=? WHERE id=1",
            (next_cursor, 1 if complete else 0, len(rows), as_of.isoformat()),
        )
    return complete, len(rows), next_cursor


def _prune_expired_state_if_due(
    store: ObservationEventStore,
    *,
    cutoff: str,
) -> int:
    global _LAST_PRUNE_MONOTONIC
    now_mono = time.monotonic()
    with _TELEMETRY_LOCK:
        if now_mono - _LAST_PRUNE_MONOTONIC < STATE_PRUNE_INTERVAL_SECONDS:
            return 0
        _LAST_PRUNE_MONOTONIC = now_mono
    with store._lock, store.db:
        cursor = store.db.execute(
            f"DELETE FROM {_STATE_TABLE} WHERE rowid IN ("
            f"SELECT rowid FROM {_STATE_TABLE} WHERE observed_at<? "
            "ORDER BY observed_at LIMIT ?)",
            (cutoff, STATE_PRUNE_BATCH_ROWS),
        )
    return int(cursor.rowcount or 0)


def _should_emit_telemetry(*, ready: bool, batch_rows: int) -> bool:
    global _LAST_STEADY_TELEMETRY_MONOTONIC
    if not ready or batch_rows:
        return True
    now_mono = time.monotonic()
    with _TELEMETRY_LOCK:
        if (
            now_mono - _LAST_STEADY_TELEMETRY_MONOTONIC
            < STEADY_TELEMETRY_INTERVAL_SECONDS
        ):
            return False
        _LAST_STEADY_TELEMETRY_MONOTONIC = now_mono
        return True


def _bounded_tracked_mints(
    self: ObservationEventStore,
    *,
    as_of: datetime,
    horizon_seconds: float = 300.0,
    limit: int = 100,
) -> list[str]:
    from .sqlite_phase_observability import emit_phase, resource_snapshot

    before = resource_snapshot(self)
    started = time.perf_counter()
    ready = False
    batch_rows = 0
    cursor = 0
    pruned_rows = 0
    try:
        ready, batch_rows, cursor = _advance_bootstrap(
            self,
            as_of=as_of,
            horizon_seconds=float(horizon_seconds),
        )
        if not ready:
            return []

        cutoff = (as_of - timedelta(seconds=float(horizon_seconds))).isoformat()
        pruned_rows = _prune_expired_state_if_due(self, cutoff=cutoff)
        with self._lock:
            rows = self.db.execute(
                f"SELECT token_mint FROM {_STATE_TABLE} WHERE observed_at>=? "
                "ORDER BY observed_at DESC, token_mint LIMIT ?",
                (cutoff, int(limit)),
            ).fetchall()
        return [str(row["token_mint"]) for row in rows]
    finally:
        if _should_emit_telemetry(ready=ready, batch_rows=batch_rows):
            after = resource_snapshot(self)
            emit_phase(
                "shadow-price-clock:tracked-mints",
                before=before,
                after=after,
                duration_ms=(time.perf_counter() - started) * 1000.0,
                detail={
                    "bootstrap_ready": ready,
                    "bootstrap_batch_rows": batch_rows,
                    "bootstrap_cursor": cursor,
                    "expired_state_rows_pruned": pruned_rows,
                    "horizon_seconds": float(horizon_seconds),
                    "limit": int(limit),
                    "history_scaled_query_removed": True,
                },
            )


setattr(_bounded_tracked_mints, "_roi_shadow_tracked_mints_bounded", True)


def configure_shadow_price_tracking_state_repair() -> None:
    """Configure the bounded shadow-price hot path before production composition."""

    global _INSTALLED
    with _INSTALL_LOCK:
        current = ObservationEventStore.tracked_mints
        if not bool(getattr(current, "_roi_shadow_tracked_mints_bounded", False)):
            ObservationEventStore.tracked_mints = _bounded_tracked_mints  # type: ignore[method-assign]
        _INSTALLED = True


def status(store: ObservationEventStore | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "installed": _INSTALLED,
        "repair_version": REPAIR_VERSION,
        "bootstrap_batch_rows": BOOTSTRAP_BATCH_ROWS,
        "source_scan_mode": "durable-rowid-keyset",
        "steady_state_query": "indexed-recent-mint-state",
        "startup_full_history_index_build": False,
        "history_scaled_per_tick_query_removed": True,
        "bootstrap_fail_closed_until_exact": True,
        "state_prune_batch_rows": STATE_PRUNE_BATCH_ROWS,
        "state_prune_interval_seconds": STATE_PRUNE_INTERVAL_SECONDS,
        "steady_telemetry_interval_seconds": STEADY_TELEMETRY_INTERVAL_SECONDS,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        "certification_thresholds_changed": CERTIFICATION_THRESHOLDS_CHANGED,
    }
    if store is not None:
        try:
            payload["state"] = _meta(store)
        except Exception as exc:
            payload["state_error_type"] = type(exc).__name__
    return payload


__all__ = [
    "BOOTSTRAP_BATCH_ROWS",
    "REPAIR_VERSION",
    "STATE_PRUNE_BATCH_ROWS",
    "_advance_bootstrap",
    "_bounded_tracked_mints",
    "configure_shadow_price_tracking_state_repair",
    "status",
]
