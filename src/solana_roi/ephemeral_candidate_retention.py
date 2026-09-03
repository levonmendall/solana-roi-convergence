from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .config import BASELINE
from .direct_solana import DirectSolanaIngestionPlane


ENTRY_WINDOW_SECONDS = float(BASELINE.confirmation_window_seconds)
REAPER_INTERVAL_SECONDS = 0.50
EPHEMERAL_HYDRATION_REASONS = frozenset(
    {
        "frozen_scout_processed_trigger",
        "frozen_scout_live_poll_trigger",
        "prospective_launch",
        "deterministic_market_sample",
    }
)

_ORIGINAL_HYDRATE_ONE: Callable[..., Any] | None = None
_ORIGINAL_DIRECT_RUN: Callable[..., Any] | None = None
_ORIGINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _ensure_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS ephemeral_candidate_state ("
            "token_mint TEXT PRIMARY KEY, first_seen_at TEXT NOT NULL, expires_at TEXT NOT NULL, source TEXT)"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_ephemeral_candidate_expiry "
            "ON ephemeral_candidate_state(expires_at)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS anonymous_certification_outcomes ("
            "bucket TEXT NOT NULL, source TEXT NOT NULL, reason TEXT NOT NULL, outcome TEXT NOT NULL, "
            "count INTEGER NOT NULL, max_age_ms REAL NOT NULL DEFAULT 0, "
            "PRIMARY KEY(bucket, source, reason, outcome))"
        )


def _ensure_schema_conn(db: sqlite3.Connection) -> None:
    db.execute(
        "CREATE TABLE IF NOT EXISTS ephemeral_candidate_state ("
        "token_mint TEXT PRIMARY KEY, first_seen_at TEXT NOT NULL, expires_at TEXT NOT NULL, source TEXT)"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS ix_ephemeral_candidate_expiry "
        "ON ephemeral_candidate_state(expires_at)"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS anonymous_certification_outcomes ("
        "bucket TEXT NOT NULL, source TEXT NOT NULL, reason TEXT NOT NULL, outcome TEXT NOT NULL, "
        "count INTEGER NOT NULL, max_age_ms REAL NOT NULL DEFAULT 0, "
        "PRIMARY KEY(bucket, source, reason, outcome))"
    )


def _track_candidate(store: Any, *, token_mint: str, first_seen_at: datetime, source: str | None) -> None:
    _ensure_schema(store)
    expires_at = first_seen_at + timedelta(seconds=ENTRY_WINDOW_SECONDS)
    with store._lock, store.db:
        store.db.execute(
            "INSERT OR IGNORE INTO ephemeral_candidate_state(token_mint, first_seen_at, expires_at, source) "
            "VALUES (?, ?, ?, ?)",
            (token_mint, first_seen_at.isoformat(), expires_at.isoformat(), source),
        )


def _sqlite_table_exists(db: sqlite3.Connection, table_name: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _record_anonymous_outcome_store(
    store: Any,
    *,
    source: str,
    reason: str,
    outcome: str,
    age_ms: float,
    at: datetime | None = None,
) -> None:
    _ensure_schema(store)
    now = at or _utcnow()
    bucket = now.replace(second=0, microsecond=0).isoformat()
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO anonymous_certification_outcomes(bucket, source, reason, outcome, count, max_age_ms) "
            "VALUES (?, ?, ?, ?, 1, ?) ON CONFLICT(bucket, source, reason, outcome) DO UPDATE SET "
            "count=count+1, max_age_ms=MAX(max_age_ms, excluded.max_age_ms)",
            (bucket, source or "UNKNOWN", reason, outcome, float(age_ms)),
        )


def _discard_hydration_row(self: DirectSolanaIngestionPlane, row: dict[str, Any], *, outcome: str) -> None:
    signature = str(row.get("signature") or "")
    reason = str(row.get("reason") or "")
    source = str(row.get("source_hint") or "") or "SCOUT"
    trigger = _parse_dt(row["trigger_received_at"])
    age_ms = max(0.0, (_utcnow() - trigger).total_seconds() * 1000.0)
    with self.store._lock, self.store.db:
        if _sqlite_table_exists(self.store.db, "direct_solana_hydration_queue"):
            self.store.db.execute("DELETE FROM direct_solana_hydration_queue WHERE signature=?", (signature,))
    _record_anonymous_outcome_store(
        self.store,
        source=source,
        reason=reason,
        outcome=outcome,
        age_ms=age_ms,
    )
    setattr(
        self,
        "_roi_ephemeral_hydrations_discarded_session",
        int(getattr(self, "_roi_ephemeral_hydrations_discarded_session", 0) or 0) + 1,
    )


def _candidate_mints_for_signature(store: Any, signature: str) -> list[str]:
    try:
        with store._lock:
            rows = store.db.execute(
                "SELECT DISTINCT token_mint FROM normalized_swaps WHERE signature=? AND token_mint<>''",
                (signature,),
            ).fetchall()
    except Exception:
        return []
    return [str(row[0]) for row in rows if row[0] is not None and str(row[0]).strip()]


async def _bounded_ephemeral_hydrate_one(self: DirectSolanaIngestionPlane, row: dict[str, Any]) -> None:
    if _ORIGINAL_HYDRATE_ONE is None:
        raise RuntimeError("ephemeral candidate retention is not installed")
    reason = str(row.get("reason") or "")
    if reason not in EPHEMERAL_HYDRATION_REASONS:
        await _ORIGINAL_HYDRATE_ONE(self, row)
        return

    trigger = _parse_dt(row["trigger_received_at"])
    remaining = ENTRY_WINDOW_SECONDS - max(0.0, (_utcnow() - trigger).total_seconds())
    if remaining <= 0.0:
        _discard_hydration_row(self, row, outcome="expired_before_entry")
        return

    try:
        await asyncio.wait_for(_ORIGINAL_HYDRATE_ONE(self, row), timeout=remaining)
    except asyncio.TimeoutError:
        _discard_hydration_row(self, row, outcome="expired_in_flight_before_entry")
        return

    signature = str(row.get("signature") or "")
    source = str(row.get("source_hint") or "") or None
    for mint in _candidate_mints_for_signature(self.store, signature):
        _track_candidate(self.store, token_mint=mint, first_seen_at=trigger, source=source)


def _connect_maintenance(path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(path, timeout=0.0, isolation_level=None, check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=0")
    return db


def _anon_outcome_conn(
    db: sqlite3.Connection,
    *,
    bucket: str,
    source: str,
    reason: str,
    outcome: str,
    count: int,
    max_age_ms: float,
) -> None:
    db.execute(
        "INSERT INTO anonymous_certification_outcomes(bucket, source, reason, outcome, count, max_age_ms) "
        "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(bucket, source, reason, outcome) DO UPDATE SET "
        "count=count+excluded.count, max_age_ms=MAX(max_age_ms, excluded.max_age_ms)",
        (bucket, source or "UNKNOWN", reason, outcome, int(count), float(max_age_ms)),
    )


def _reap_sqlite(path: Path, now: datetime | None = None) -> dict[str, Any]:
    at = (now or _utcnow()).astimezone(timezone.utc)
    cutoff = at - timedelta(seconds=ENTRY_WINDOW_SECONDS)
    result: dict[str, Any] = {"busy": False, "queue_rows": 0, "candidate_mints": []}
    db = _connect_maintenance(path)
    try:
        try:
            db.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                result["busy"] = True
                return result
            raise

        _ensure_schema_conn(db)
        if _sqlite_table_exists(db, "direct_solana_hydration_queue"):
            queue_groups = db.execute(
                "SELECT COALESCE(source_hint, 'SCOUT') AS source, reason, COUNT(*) AS n, "
                "MIN(trigger_received_at) AS oldest FROM direct_solana_hydration_queue "
                "WHERE status='pending' AND reason IN (?,?,?,?) AND trigger_received_at<? "
                "GROUP BY COALESCE(source_hint, 'SCOUT'), reason",
                (*sorted(EPHEMERAL_HYDRATION_REASONS), cutoff.isoformat()),
            ).fetchall()
            for row in queue_groups:
                oldest = _parse_dt(row["oldest"])
                _anon_outcome_conn(
                    db,
                    bucket=at.replace(second=0, microsecond=0).isoformat(),
                    source=str(row["source"]),
                    reason=str(row["reason"]),
                    outcome="expired_before_entry",
                    count=int(row["n"]),
                    max_age_ms=max(0.0, (at - oldest).total_seconds() * 1000.0),
                )
                result["queue_rows"] = int(result["queue_rows"]) + int(row["n"])
            db.execute(
                "DELETE FROM direct_solana_hydration_queue WHERE status='pending' "
                "AND reason IN (?,?,?,?) AND trigger_received_at<?",
                (*sorted(EPHEMERAL_HYDRATION_REASONS), cutoff.isoformat()),
            )

        expired = db.execute(
            "SELECT token_mint FROM ephemeral_candidate_state WHERE expires_at<? ORDER BY expires_at LIMIT 500",
            (at.isoformat(),),
        ).fetchall()
        for row in expired:
            mint = str(row["token_mint"])
            db.execute("DELETE FROM ephemeral_candidate_state WHERE token_mint=?", (mint,))
            result["candidate_mints"].append(mint)

        db.execute("COMMIT")
        return result
    except Exception:
        try:
            db.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        db.close()


def _purge_engine_candidates(self: DirectSolanaIngestionPlane, mints: list[str]) -> None:
    if not mints:
        return
    engine = getattr(getattr(self, "service", None), "engine", None)
    if engine is None:
        return
    positions = getattr(getattr(engine, "portfolio", None), "positions", {})
    candidates = getattr(getattr(engine, "strategy", None), "candidates", {})
    marks = getattr(engine, "marks", {})
    changed = False
    for mint in mints:
        position = positions.get(mint) if isinstance(positions, dict) else None
        if position is not None:
            continue
        if isinstance(candidates, dict) and candidates.pop(mint, None) is not None:
            changed = True
        if isinstance(marks, dict) and mint in marks:
            marks.pop(mint, None)
            changed = True
    save = getattr(engine, "_save_checkpoint", None)
    if changed and callable(save):
        save()


async def _ephemeral_reaper(self: DirectSolanaIngestionPlane, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            result = await asyncio.to_thread(_reap_sqlite, Path(self.store.path), _utcnow())
            if result.get("busy"):
                setattr(
                    self,
                    "_roi_ephemeral_reaper_busy_skips_session",
                    int(getattr(self, "_roi_ephemeral_reaper_busy_skips_session", 0) or 0) + 1,
                )
            else:
                mints = list(result.get("candidate_mints") or [])
                setattr(
                    self,
                    "_roi_ephemeral_queue_rows_pruned_session",
                    int(getattr(self, "_roi_ephemeral_queue_rows_pruned_session", 0) or 0)
                    + int(result.get("queue_rows") or 0),
                )
                setattr(
                    self,
                    "_roi_ephemeral_candidates_purged_session",
                    int(getattr(self, "_roi_ephemeral_candidates_purged_session", 0) or 0) + len(mints),
                )
                _purge_engine_candidates(self, mints)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            setattr(self, "_roi_ephemeral_reaper_last_error", f"{type(exc).__name__}: {str(exc)[:200]}")
        try:
            await asyncio.wait_for(stop.wait(), timeout=REAPER_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass


async def _run_with_ephemeral_reaper(self: DirectSolanaIngestionPlane, stop: asyncio.Event) -> None:
    if _ORIGINAL_DIRECT_RUN is None:
        raise RuntimeError("ephemeral candidate retention is not installed")
    _ensure_schema(self.store)
    reaper = asyncio.create_task(_ephemeral_reaper(self, stop), name="ephemeral-candidate-reaper")
    try:
        await _ORIGINAL_DIRECT_RUN(self, stop)
    finally:
        reaper.cancel()
        await asyncio.gather(reaper, return_exceptions=True)


def _direct_status_with_ephemeral_retention(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _ORIGINAL_DIRECT_STATUS is None:
        raise RuntimeError("ephemeral candidate retention is not installed")
    payload = _ORIGINAL_DIRECT_STATUS(self)
    try:
        _ensure_schema(self.store)
        with self.store._lock:
            active_count = int(self.store.db.execute("SELECT COUNT(*) FROM ephemeral_candidate_state").fetchone()[0])
            outcome_count = int(
                self.store.db.execute("SELECT COALESCE(SUM(count),0) FROM anonymous_certification_outcomes").fetchone()[0]
            )
    except Exception:
        active_count = outcome_count = 0

    payload["ephemeral_candidate_retention"] = {
        "installed": True,
        "entry_window_seconds": ENTRY_WINDOW_SECONDS,
        "active_strategy_candidate_state_ephemeral": True,
        "canonical_observation_evidence_retained": True,
        "canonical_observation_tables_pruned": False,
        "paper_entry_required_for_research_evidence": False,
        "expired_candidate_hydration_work_pruned": True,
        "wallet_research_history_independent": True,
        "gap_recovery_exempt_from_candidate_retention": True,
        "active_ephemeral_candidates": active_count,
        "anonymous_hydration_expiry_outcomes": outcome_count,
        "queue_rows_pruned_session": int(getattr(self, "_roi_ephemeral_queue_rows_pruned_session", 0) or 0),
        "in_flight_rows_discarded_session": int(
            getattr(self, "_roi_ephemeral_hydrations_discarded_session", 0) or 0
        ),
        "candidates_purged_session": int(getattr(self, "_roi_ephemeral_candidates_purged_session", 0) or 0),
        "reaper_busy_skips_session": int(getattr(self, "_roi_ephemeral_reaper_busy_skips_session", 0) or 0),
        "last_error": getattr(self, "_roi_ephemeral_reaper_last_error", None),
        "continuity_lease_seconds_unchanged": 12.0,
        "recovery_page_bound_unchanged": "3x1000",
        "max_chase_fraction_unchanged": BASELINE.max_chase_fraction,
        "paper_only_authority_unchanged": True,
        "signing_or_submission_available": False,
    }
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "unentered_strategy_candidate_state_is_ephemeral": True,
                "entry_window_seconds": ENTRY_WINDOW_SECONDS,
                "canonical_observation_evidence_retained": True,
                "paper_entry_not_required_for_research_evidence": True,
            }
        )
    return payload


def install_ephemeral_candidate_retention() -> None:
    global _ORIGINAL_HYDRATE_ONE, _ORIGINAL_DIRECT_RUN, _ORIGINAL_DIRECT_STATUS

    current_hydrate = DirectSolanaIngestionPlane._hydrate_one
    if not bool(getattr(current_hydrate, "_roi_ephemeral_candidate_retention", False)):
        _ORIGINAL_HYDRATE_ONE = current_hydrate
        setattr(_bounded_ephemeral_hydrate_one, "_roi_ephemeral_candidate_retention", True)
        DirectSolanaIngestionPlane._hydrate_one = _bounded_ephemeral_hydrate_one  # type: ignore[method-assign]

    current_run = DirectSolanaIngestionPlane.run
    if not bool(getattr(current_run, "_roi_ephemeral_candidate_retention", False)):
        _ORIGINAL_DIRECT_RUN = current_run
        setattr(_run_with_ephemeral_reaper, "_roi_ephemeral_candidate_retention", True)
        DirectSolanaIngestionPlane.run = _run_with_ephemeral_reaper  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_ephemeral_candidate_retention", False)):
        _ORIGINAL_DIRECT_STATUS = current_status
        try:
            _direct_status_with_ephemeral_retention.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_direct_status_with_ephemeral_retention, "_roi_ephemeral_candidate_retention", True)
        DirectSolanaIngestionPlane.status = _direct_status_with_ephemeral_retention  # type: ignore[method-assign]


__all__ = [
    "ENTRY_WINDOW_SECONDS",
    "EPHEMERAL_HYDRATION_REASONS",
    "_bounded_ephemeral_hydrate_one",
    "_ensure_schema",
    "_purge_engine_candidates",
    "_reap_sqlite",
    "install_ephemeral_candidate_retention",
]
