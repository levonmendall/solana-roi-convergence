from __future__ import annotations

"""Bounded, fail-closed cleanup for production artifacts proven safe to expire.

This module deliberately has no strategy, candidate-selection, execution, signing,
submission, or certification-authority capability. It removes only old regular
certification-export files after conservative filesystem identity/open-file checks.
Anything whose provenance, active-reference status, or replication acknowledgement
is not proven remains untouched.
"""

import os
import stat
import time
from pathlib import Path
from typing import Any

try:  # Render production is Linux; absence of flock means deletion fails closed.
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX portability guard
    fcntl = None  # type: ignore[assignment]


CLEANUP_VERSION = "safe-retention-cleanup-v1"
STALE_EXPORT_AGE_SECONDS = 3600.0
MAX_DIRECTORY_ENTRIES = 2048
MAX_STALE_EXPORT_CANDIDATES = 256
_EXPORT_PREFIX = ".certification-export-"
_EXPORT_SUFFIX = ".sqlite3"
_STATUS_PATH = "/v1/operations/safe-retention-cleanup"


def _same_identity(left: os.stat_result, right: os.stat_result) -> bool:
    return int(left.st_dev) == int(right.st_dev) and int(left.st_ino) == int(right.st_ino)


def _held_open_by_this_process(identity: os.stat_result) -> bool | None:
    """Return whether this process already has the inode open; None means unknown."""

    fd_dir = Path("/proc/self/fd")
    try:
        entries = tuple(fd_dir.iterdir())
    except OSError:
        return None
    for entry in entries:
        try:
            opened = entry.stat()
        except OSError:
            continue
        if _same_identity(identity, opened):
            return True
    return False


def _drop_fd_cache(fd: int) -> None:
    fadvise = getattr(os, "posix_fadvise", None)
    advice = getattr(os, "POSIX_FADV_DONTNEED", None)
    if fadvise is None or advice is None:
        return
    try:
        fadvise(fd, 0, 0, advice)
    except OSError:
        return


def _unlink_stale_export(candidate: Path, *, directory: Path, now: float) -> tuple[bool, str]:
    """Unlink one stale export only when every safety predicate proves true."""

    if candidate.parent != directory:
        return False, "outside_directory"
    if not candidate.name.startswith(_EXPORT_PREFIX) or not candidate.name.endswith(_EXPORT_SUFFIX):
        return False, "name_mismatch"

    try:
        before = candidate.lstat()
    except OSError:
        return False, "lstat_failed"
    if stat.S_ISLNK(before.st_mode):
        return False, "symlink"
    if not stat.S_ISREG(before.st_mode):
        return False, "not_regular_file"
    if int(before.st_size) <= 0:
        return False, "empty_or_incomplete"
    if now - float(before.st_mtime) < STALE_EXPORT_AGE_SECONDS:
        return False, "not_stale"

    held = _held_open_by_this_process(before)
    if held is None:
        return False, "open_file_state_unknown"
    if held:
        return False, "open_by_process"
    if fcntl is None:
        return False, "flock_unavailable"

    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        return False, "nofollow_unavailable"
    try:
        fd = os.open(candidate, os.O_RDONLY | int(nofollow))
    except OSError:
        return False, "open_failed"
    try:
        try:
            opened = os.fstat(fd)
        except OSError:
            return False, "fstat_failed"
        if not _same_identity(before, opened) or not stat.S_ISREG(opened.st_mode):
            return False, "identity_changed"
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return False, "locked_or_busy"

        try:
            final = candidate.lstat()
        except OSError:
            return False, "final_lstat_failed"
        if not _same_identity(opened, final) or stat.S_ISLNK(final.st_mode):
            return False, "identity_changed_before_unlink"
        if now - float(final.st_mtime) < STALE_EXPORT_AGE_SECONDS:
            return False, "not_stale_after_recheck"

        _drop_fd_cache(fd)
        try:
            candidate.unlink()
        except OSError:
            return False, "unlink_failed"
        return True, "removed"
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(fd)


def _bounded_export_candidates(directory: Path) -> tuple[list[Path], bool]:
    candidates: list[Path] = []
    truncated = False
    examined_entries = 0
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                examined_entries += 1
                if examined_entries > MAX_DIRECTORY_ENTRIES:
                    truncated = True
                    break
                if not entry.name.startswith(_EXPORT_PREFIX):
                    continue
                candidates.append(directory / entry.name)
                if len(candidates) >= MAX_STALE_EXPORT_CANDIDATES:
                    truncated = True
                    break
    except OSError:
        raise
    candidates.sort(key=lambda item: item.name)
    return candidates, truncated


def cleanup_stale_certification_exports(store_path: Path, *, now: float | None = None) -> dict[str, Any]:
    """Perform one bounded pass over stale snapshot exports beside the live store."""

    path = Path(store_path)
    directory = path.parent
    result: dict[str, Any] = {
        "version": CLEANUP_VERSION,
        "examined": 0,
        "removed": 0,
        "skipped": 0,
        "bounded": True,
        "directory_entry_limit": MAX_DIRECTORY_ENTRIES,
        "candidate_limit": MAX_STALE_EXPORT_CANDIDATES,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    try:
        candidates, truncated = _bounded_export_candidates(directory)
    except OSError as exc:
        result["error"] = f"{type(exc).__name__}:{exc}"
        return result
    result["scan_truncated"] = bool(truncated)

    at = time.time() if now is None else float(now)
    reasons: dict[str, int] = {}
    for candidate in candidates:
        result["examined"] = int(result["examined"]) + 1
        removed, reason = _unlink_stale_export(candidate, directory=directory, now=at)
        if removed:
            result["removed"] = int(result["removed"]) + 1
        else:
            result["skipped"] = int(result["skipped"]) + 1
        reasons[reason] = int(reasons.get(reason, 0)) + 1
    result["outcomes"] = reasons
    return result


def install_safe_retention_cleanup(app: Any, ingestion_runtime: Any) -> dict[str, Any]:
    """Execute one guarded stale-export pass and expose read-only evidence."""

    store = getattr(ingestion_runtime, "store", None)
    raw_path = getattr(store, "path", None)
    if raw_path is None:
        cleanup: dict[str, Any] = {
            "version": CLEANUP_VERSION,
            "removed": 0,
            "error": "canonical_store_path_unavailable",
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    else:
        cleanup = cleanup_stale_certification_exports(Path(raw_path))

    state = {
        "version": CLEANUP_VERSION,
        "installed": True,
        "startup_stale_export_cleanup": cleanup,
        "scope": ["stale_certification_exports"],
        "candidate_selection_changed": False,
        "provenance_ambiguous_data_deleted": False,
        "replication_journal_deleted": False,
        "robinhood_history_deleted": False,
        "event_ledger_deleted": False,
        "wallet_history_deleted": False,
        "strategy_thresholds_changed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    app.state.roi_safe_retention_cleanup = state
    app.state.roi_safe_retention_cleanup_version = CLEANUP_VERSION

    existing = {getattr(route, "path", None) for route in getattr(app, "routes", ())}
    if _STATUS_PATH not in existing:
        @app.get(_STATUS_PATH)
        def safe_retention_cleanup_status() -> dict[str, Any]:
            return dict(getattr(app.state, "roi_safe_retention_cleanup", state))

    return state
