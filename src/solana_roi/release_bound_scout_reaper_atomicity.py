from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from . import certification_failure_accounting_repair as accounting
from . import ephemeral_candidate_retention as retention
from . import release_bound_scout_classification_repair as release_bound


REPAIR_VERSION = "release-bound-scout-reaper-atomicity-v1"
_ORIGINAL_REAP: Callable[..., dict[str, Any]] | None = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _record_expired_rows_before_cleanup(
    path: Path,
    rows: list[dict[str, str]],
    failed_at: datetime,
) -> None:
    if not rows:
        return
    db = sqlite3.connect(path, timeout=1.0, isolation_level=None, check_same_thread=False)
    db.row_factory = sqlite3.Row
    try:
        db.execute("PRAGMA busy_timeout=1000")
        db.execute("BEGIN IMMEDIATE")
        release_bound._ensure_schema_conn(db)
        for row in rows:
            signature = str(row.get("signature") or "")
            if not signature:
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
                    release_bound._release_commit(),
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


def _reap_after_release_bound_accounting(
    path: Path,
    now: datetime | None = None,
) -> dict[str, Any]:
    if _ORIGINAL_REAP is None:
        raise RuntimeError("release-bound scout reaper atomicity is not installed")
    at = (now or _utcnow()).astimezone(timezone.utc)
    rows = release_bound._snapshot_expired_pending_scout_rows(path, at)
    try:
        _record_expired_rows_before_cleanup(path, rows, at)
    except sqlite3.OperationalError as exc:
        if "locked" in str(exc).lower() or "busy" in str(exc).lower():
            # Do not let the canonical reaper delete an expired scout row unless its
            # exact trigger-epoch failure evidence has committed first. A later
            # reaper pass will retry both accounting and cleanup.
            return {
                "busy": True,
                "queue_rows": 0,
                "candidate_mints": [],
                "release_bound_accounting_deferred": True,
            }
        raise
    return _ORIGINAL_REAP(path, at)


def install_release_bound_scout_reaper_atomicity() -> None:
    global _ORIGINAL_REAP
    current = retention._reap_sqlite
    if bool(getattr(current, "_roi_release_bound_scout_reaper_atomicity", False)):
        return
    if not bool(getattr(current, "_roi_release_bound_scout", False)):
        return
    if not bool(getattr(current, "_roi_failure_accounting", False)):
        return
    _ORIGINAL_REAP = current
    try:
        _reap_after_release_bound_accounting.__dict__.update(getattr(current, "__dict__", {}))
    except Exception:
        pass
    setattr(_reap_after_release_bound_accounting, "_roi_release_bound_scout_reaper_atomicity", True)
    retention._reap_sqlite = _reap_after_release_bound_accounting


__all__ = [
    "REPAIR_VERSION",
    "_record_expired_rows_before_cleanup",
    "_reap_after_release_bound_accounting",
    "install_release_bound_scout_reaper_atomicity",
]
