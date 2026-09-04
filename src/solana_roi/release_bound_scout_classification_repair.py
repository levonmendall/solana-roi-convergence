from __future__ import annotations

import os
import sqlite3
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from . import certification_failure_accounting_repair as accounting
from . import ephemeral_candidate_retention as retention
from . import scout_candidate_continuity_repair as scout
from . import direct_transaction as tx
from .direct_solana import DirectSolanaIngestionPlane
from .observation import LatencyCertificationGate


REPAIR_VERSION = "release-bound-scout-classification-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_ORIGINAL_ACCOUNT_SCOUT_EXPIRY: Callable[..., str] | None = None
_ORIGINAL_REAP_SQLITE: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_SCOUT_NORMALIZE: Callable[..., Any] | None = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _release_commit() -> str:
    for name in ("SOLANA_ROI_RELEASE_COMMIT", "RENDER_GIT_COMMIT", "GITHUB_SHA"):
        value = str(os.getenv(name) or "").strip()
        if value:
            return value
    return "unbound-local-release"


def _ensure_schema_conn(db: sqlite3.Connection) -> None:
    db.execute(
        "CREATE TABLE IF NOT EXISTS release_bound_candidate_failures ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, signature TEXT NOT NULL, "
        "trigger_received_at TEXT NOT NULL, failed_at TEXT NOT NULL, release_commit TEXT NOT NULL, "
        "reason TEXT NOT NULL, outcome TEXT NOT NULL, max_age_ms REAL NOT NULL, "
        "UNIQUE(signature, trigger_received_at, reason, outcome))"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS ix_release_bound_candidate_failure_trigger "
        "ON release_bound_candidate_failures(trigger_received_at)"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS scout_trigger_terminal_classification ("
        "signature TEXT PRIMARY KEY, trigger_received_at TEXT NOT NULL, classified_at TEXT NOT NULL, "
        "release_commit TEXT NOT NULL, classification TEXT NOT NULL, reason TEXT NOT NULL)"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS ix_scout_terminal_classification_trigger "
        "ON scout_trigger_terminal_classification(trigger_received_at)"
    )


def _ensure_schema(store: Any) -> None:
    with store._lock, store.db:
        _ensure_schema_conn(store.db)


def _terminal_classification(store: Any, signature: str) -> dict[str, Any] | None:
    if not signature:
        return None
    _ensure_schema(store)
    with store._lock:
        row = store.db.execute(
            "SELECT signature, trigger_received_at, classified_at, release_commit, classification, reason "
            "FROM scout_trigger_terminal_classification WHERE signature=? LIMIT 1",
            (signature,),
        ).fetchone()
    return dict(row) if row is not None else None


def _record_terminal_non_candidate(
    store: Any,
    *,
    signature: str,
    trigger_received_at: datetime,
    reason: str,
    classified_at: datetime | None = None,
) -> None:
    if not signature:
        return
    at = classified_at or _utcnow()
    _ensure_schema(store)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO scout_trigger_terminal_classification("
            "signature, trigger_received_at, classified_at, release_commit, classification, reason) "
            "VALUES (?, ?, ?, ?, 'non_candidate', ?) "
            "ON CONFLICT(signature) DO UPDATE SET "
            "trigger_received_at=excluded.trigger_received_at, classified_at=excluded.classified_at, "
            "release_commit=excluded.release_commit, classification=excluded.classification, reason=excluded.reason",
            (
                signature,
                trigger_received_at.astimezone(timezone.utc).isoformat(),
                at.astimezone(timezone.utc).isoformat(),
                _release_commit(),
                reason,
            ),
        )


def _record_release_bound_failure(
    store: Any,
    *,
    signature: str,
    trigger_received_at: datetime,
    reason: str,
    outcome: str,
    failed_at: datetime,
) -> None:
    if not signature:
        return
    age_ms = max(0.0, (failed_at - trigger_received_at).total_seconds() * 1000.0)
    _ensure_schema(store)
    with store._lock, store.db:
        store.db.execute(
            "INSERT OR IGNORE INTO release_bound_candidate_failures("
            "signature, trigger_received_at, failed_at, release_commit, reason, outcome, max_age_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                signature,
                trigger_received_at.astimezone(timezone.utc).isoformat(),
                failed_at.astimezone(timezone.utc).isoformat(),
                _release_commit(),
                reason,
                outcome,
                float(age_ms),
            ),
        )


def _account_scout_expiry_release_bound(
    store: Any,
    row: dict[str, Any],
    *,
    outcome: str,
    failed_at: datetime | None = None,
) -> str:
    if _ORIGINAL_ACCOUNT_SCOUT_EXPIRY is None:
        raise RuntimeError("release-bound scout accounting is not installed")
    reason = str(row.get("reason") or "")
    if reason not in accounting.SCOUT_REASONS:
        return _ORIGINAL_ACCOUNT_SCOUT_EXPIRY(store, row, outcome=outcome, failed_at=failed_at)

    signature = str(row.get("signature") or "")
    if _terminal_classification(store, signature) is not None:
        return "classified_non_candidate_terminal"

    candidate = accounting._normalized_candidate_for_signature(store, signature)
    if candidate is not None:
        return _ORIGINAL_ACCOUNT_SCOUT_EXPIRY(store, row, outcome=outcome, failed_at=failed_at)

    at = failed_at or _utcnow()
    trigger = retention._parse_dt(row["trigger_received_at"])
    _record_release_bound_failure(
        store,
        signature=signature,
        trigger_received_at=trigger,
        reason=reason,
        outcome=outcome,
        failed_at=at,
    )
    return "anonymous_unclassified_release_bound"


def _sqlite_table_exists(db: sqlite3.Connection, table_name: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _snapshot_expired_pending_scout_rows(path: Path, at: datetime) -> list[dict[str, str]]:
    cutoff = at - timedelta(seconds=float(retention.ENTRY_WINDOW_SECONDS))
    db = sqlite3.connect(path, timeout=1.0, isolation_level=None, check_same_thread=False)
    db.row_factory = sqlite3.Row
    try:
        if not _sqlite_table_exists(db, "direct_solana_hydration_queue"):
            return []
        rows = db.execute(
            "SELECT signature, reason, trigger_received_at FROM direct_solana_hydration_queue "
            "WHERE status='pending' AND reason IN (?, ?) AND trigger_received_at<?",
            (*sorted(accounting.SCOUT_REASONS), cutoff.isoformat()),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        db.close()


def _record_reaped_release_bound_rows(path: Path, rows: list[dict[str, str]], failed_at: datetime) -> None:
    if not rows:
        return
    db = sqlite3.connect(path, timeout=1.0, isolation_level=None, check_same_thread=False)
    db.row_factory = sqlite3.Row
    try:
        db.execute("PRAGMA busy_timeout=1000")
        db.execute("BEGIN IMMEDIATE")
        _ensure_schema_conn(db)
        has_queue = _sqlite_table_exists(db, "direct_solana_hydration_queue")
        for row in rows:
            signature = str(row.get("signature") or "")
            if not signature:
                continue
            if has_queue:
                still_present = db.execute(
                    "SELECT 1 FROM direct_solana_hydration_queue WHERE signature=? LIMIT 1",
                    (signature,),
                ).fetchone()
                if still_present is not None:
                    continue
            terminal = db.execute(
                "SELECT 1 FROM scout_trigger_terminal_classification WHERE signature=? LIMIT 1",
                (signature,),
            ).fetchone()
            if terminal is not None:
                continue
            trigger = retention._parse_dt(row["trigger_received_at"])
            age_ms = max(0.0, (failed_at - trigger).total_seconds() * 1000.0)
            db.execute(
                "INSERT OR IGNORE INTO release_bound_candidate_failures("
                "signature, trigger_received_at, failed_at, release_commit, reason, outcome, max_age_ms) "
                "VALUES (?, ?, ?, ?, ?, 'expired_before_entry', ?)",
                (
                    signature,
                    trigger.isoformat(),
                    failed_at.isoformat(),
                    _release_commit(),
                    str(row.get("reason") or ""),
                    float(age_ms),
                ),
            )
        db.execute("COMMIT")
    except Exception:
        try:
            db.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        db.close()


def _reap_with_release_bound_accounting(path: Path, now: datetime | None = None) -> dict[str, Any]:
    if _ORIGINAL_REAP_SQLITE is None:
        raise RuntimeError("release-bound scout reaper accounting is not installed")
    at = (now or _utcnow()).astimezone(timezone.utc)
    rows = _snapshot_expired_pending_scout_rows(path, at)
    result = _ORIGINAL_REAP_SQLITE(path, at)
    if result.get("busy"):
        return result
    _record_reaped_release_bound_rows(path, rows, at)
    return result


def _release_bound_failure_rows(store: Any, *, since: datetime | None) -> list[dict[str, Any]]:
    _ensure_schema(store)
    sql = (
        "SELECT signature, trigger_received_at, failed_at, release_commit, reason, outcome, max_age_ms "
        "FROM release_bound_candidate_failures"
    )
    args: list[Any] = []
    if since is not None:
        sql += " WHERE trigger_received_at>=?"
        args.append(since.astimezone(timezone.utc).isoformat())
    sql += " ORDER BY id DESC LIMIT 1000"
    with store._lock:
        rows = store.db.execute(sql, tuple(args)).fetchall()
    return [dict(row) for row in rows]


def _latency_status_release_bound(self: LatencyCertificationGate, *, limit: int = 500) -> dict[str, object]:
    if accounting._ORIGINAL_LATENCY_STATUS is None:
        raise RuntimeError("base latency certification status is unavailable")
    payload = accounting._ORIGINAL_LATENCY_STATUS(self, limit=limit)
    rows = _release_bound_failure_rows(self.store, since=self.prospective_start_at)
    unresolved_count = len(rows)
    reasons = Counter(str(row.get("outcome") or "unknown") for row in rows)
    sampling_complete = unresolved_count == 0
    payload["certified"] = bool(payload.get("certified") and sampling_complete)
    payload["candidate_sampling_complete"] = sampling_complete
    payload["unclassified_scout_trigger_expiry_count"] = unresolved_count
    payload["unclassified_scout_trigger_expiry_outcomes"] = dict(reasons)
    payload["release_bound_failure_accounting"] = {
        "installed": True,
        "version": REPAIR_VERSION,
        "authority_boundary": "trigger_received_at>=prospective_start_at",
        "inherited_pre_release_queue_rows_excluded": True,
        "legacy_failed_at_rows_authoritative": False,
        "release_commit_recorded": True,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
    }
    requirements = payload.setdefault("requirements", {})
    if isinstance(requirements, dict):
        requirements["all_frozen_scout_triggers_must_be_classified_within_entry_window"] = True
        requirements["candidate_entry_window_seconds_unchanged"] = float(retention.ENTRY_WINDOW_SECONDS)
        requirements["failure_accounting_uses_original_trigger_time"] = True
        requirements["inherited_pre_release_queue_rows_excluded"] = True
    return payload


def _normalize_with_terminal_non_candidate(
    result: Any,
    *,
    signature: str,
    trigger_received_at: datetime,
    source_hint: str | None = None,
) -> Any:
    if _ORIGINAL_SCOUT_NORMALIZE is None:
        raise RuntimeError("terminal scout classification is not installed")
    swap = _ORIGINAL_SCOUT_NORMALIZE(
        result,
        signature=signature,
        trigger_received_at=trigger_received_at,
        source_hint=source_hint,
    )
    if swap is not None or source_hint is not None or not isinstance(result, dict):
        return swap

    plane = scout._SCOUT_HYDRATION_PLANE.get()
    if plane is None:
        return swap
    wallet, _error = scout._tracked_scout_wallet(
        result,
        tuple(getattr(plane, "scout_wallets", ()) or ()),
    )
    if wallet is None:
        return swap
    if tx.transaction_sources(result):
        return swap

    _record_terminal_non_candidate(
        plane.store,
        signature=signature,
        trigger_received_at=trigger_received_at,
        reason="supported_swap_source_missing",
    )
    setattr(
        plane,
        "_roi_release_bound_terminal_non_candidates_session",
        int(getattr(plane, "_roi_release_bound_terminal_non_candidates_session", 0) or 0) + 1,
    )
    return swap


def install_release_bound_scout_classification_repair() -> None:
    """Bind scout certification failures to their trigger epoch and terminally classify non-swaps."""

    global _ORIGINAL_ACCOUNT_SCOUT_EXPIRY, _ORIGINAL_REAP_SQLITE, _ORIGINAL_SCOUT_NORMALIZE

    current_account = accounting._account_scout_expiry
    if not bool(getattr(current_account, "_roi_release_bound_scout", False)):
        _ORIGINAL_ACCOUNT_SCOUT_EXPIRY = current_account
        setattr(_account_scout_expiry_release_bound, "_roi_release_bound_scout", True)
        accounting._account_scout_expiry = _account_scout_expiry_release_bound

    current_reap = retention._reap_sqlite
    if bool(getattr(current_reap, "_roi_failure_accounting", False)) and not bool(
        getattr(current_reap, "_roi_release_bound_scout", False)
    ):
        _ORIGINAL_REAP_SQLITE = current_reap
        try:
            _reap_with_release_bound_accounting.__dict__.update(getattr(current_reap, "__dict__", {}))
        except Exception:
            pass
        setattr(_reap_with_release_bound_accounting, "_roi_release_bound_scout", True)
        retention._reap_sqlite = _reap_with_release_bound_accounting

    current_latency = LatencyCertificationGate.status
    if bool(getattr(current_latency, "_roi_failure_accounting", False)) and not bool(
        getattr(current_latency, "_roi_release_bound_scout", False)
    ):
        try:
            _latency_status_release_bound.__dict__.update(getattr(current_latency, "__dict__", {}))
        except Exception:
            pass
        setattr(_latency_status_release_bound, "_roi_release_bound_scout", True)
        LatencyCertificationGate.status = _latency_status_release_bound  # type: ignore[method-assign]

    current_normalize = tx.normalize_standard_transaction
    if bool(getattr(current_normalize, "_roi_scout_candidate_continuity", False)) and not bool(
        getattr(current_normalize, "_roi_release_bound_scout", False)
    ):
        _ORIGINAL_SCOUT_NORMALIZE = current_normalize
        try:
            _normalize_with_terminal_non_candidate.__dict__.update(getattr(current_normalize, "__dict__", {}))
        except Exception:
            pass
        setattr(_normalize_with_terminal_non_candidate, "_roi_release_bound_scout", True)
        tx.normalize_standard_transaction = _normalize_with_terminal_non_candidate


__all__ = [
    "REPAIR_VERSION",
    "_account_scout_expiry_release_bound",
    "_latency_status_release_bound",
    "_record_release_bound_failure",
    "_record_terminal_non_candidate",
    "install_release_bound_scout_classification_repair",
]
