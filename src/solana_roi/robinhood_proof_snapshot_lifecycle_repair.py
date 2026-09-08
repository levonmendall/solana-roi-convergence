from __future__ import annotations

import stat
from pathlib import Path
from typing import Any

from . import batch9_continuity_frontier_proof_repair as batch9


REPAIR_VERSION = "robinhood-proof-snapshot-crash-bounded-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_WORK_PREFIX = ".robinhood-proof-snapshot-active-"
_SQLITE_COMPANION_SUFFIXES = ("", "-wal", "-shm", "-journal")
_INSTALLED = False


def _work_snapshot_path(store_path: str) -> Path:
    canonical = Path(store_path).expanduser().resolve()
    snapshot = canonical.parent / f"{_WORK_PREFIX}{canonical.name}"
    if snapshot == canonical:
        raise RuntimeError("Robinhood proof work snapshot must not be the canonical store")
    return snapshot


def _candidates(path: Path) -> tuple[Path, ...]:
    return tuple(Path(str(path) + suffix) for suffix in _SQLITE_COMPANION_SUFFIXES)


def _unlink_exact_candidate(candidate: Path, *, strict: bool) -> None:
    try:
        mode = candidate.lstat().st_mode
    except FileNotFoundError:
        return

    if not (stat.S_ISREG(mode) or stat.S_ISLNK(mode)):
        if strict:
            raise RuntimeError(f"Refusing to remove non-file Robinhood proof work artifact: {candidate.name}")
        return

    try:
        candidate.unlink()
    except FileNotFoundError:
        return
    except OSError:
        if strict:
            raise


def _reclaim_stale_work_snapshot(path: Path) -> None:
    """Strictly reclaim only the one deterministic work snapshot before reuse."""
    for candidate in _candidates(path):
        _unlink_exact_candidate(candidate, strict=True)


def _cleanup_work_snapshot(path: Path) -> None:
    """Best-effort terminal cleanup; the next refresh retries strictly after a crash."""
    for candidate in _candidates(path):
        _unlink_exact_candidate(candidate, strict=False)


def _crash_bounded_snapshot_path(store_path: str) -> Path:
    """Return one reusable work path, reclaiming any prior crash residue first."""
    snapshot = _work_snapshot_path(store_path)
    _reclaim_stale_work_snapshot(snapshot)
    return snapshot


setattr(_crash_bounded_snapshot_path, "_roi_robinhood_proof_snapshot_crash_bounded", True)
setattr(_cleanup_work_snapshot, "_roi_robinhood_proof_snapshot_crash_bounded", True)


def install_robinhood_proof_snapshot_lifecycle_repair(app: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return

    # Batch 9's online-backup proof isolation remains the canonical algorithm. Only
    # its work-path allocation and terminal cleanup helpers change. This converts
    # unbounded UUID-named persistent leftovers into one deterministic path per
    # canonical Robinhood store. A hard process kill can therefore leave at most one
    # work snapshot, which the next refresh strictly reclaims before rebuilding it.
    batch9._proof_snapshot_path = _crash_bounded_snapshot_path
    batch9._cleanup_sqlite_snapshot = _cleanup_work_snapshot

    app.state.roi_robinhood_proof_snapshot_lifecycle_repair = True
    app.state.roi_robinhood_proof_snapshot_lifecycle_repair_version = REPAIR_VERSION
    app.state.roi_robinhood_proof_snapshot_work_file_policy = "one_deterministic_file_per_store"
    app.state.roi_robinhood_proof_snapshot_crash_reclaimed_before_reuse = True
    app.state.roi_robinhood_proof_snapshot_historical_orphans_deleted = False
    app.state.roi_robinhood_proof_snapshot_paper_only = True
    app.state.roi_robinhood_proof_snapshot_live_money_authority = False
    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "_cleanup_work_snapshot",
    "_crash_bounded_snapshot_path",
    "_reclaim_stale_work_snapshot",
    "_work_snapshot_path",
    "install_robinhood_proof_snapshot_lifecycle_repair",
]
