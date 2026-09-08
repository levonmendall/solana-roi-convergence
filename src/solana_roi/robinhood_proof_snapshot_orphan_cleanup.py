from __future__ import annotations

import json
import logging
import os
import re
import stat
import threading
import time
from pathlib import Path
from typing import Any


REPAIR_VERSION = "robinhood-proof-snapshot-orphan-cleanup-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_ROOT = Path("/var/data")
_ENABLED_ENV = "SOLANA_ROI_ROBINHOOD_PROOF_ORPHAN_CLEANUP_ONCE"
_EXPECTED_RELEASE_ENV = "SOLANA_ROI_ROBINHOOD_PROOF_ORPHAN_CLEANUP_EXPECTED_RELEASE"
_MIN_AGE_SECONDS = 120.0
_START_DELAY_SECONDS = 10.0
_MAX_CANDIDATES = 5000
_HISTORICAL_NAME = re.compile(
    r"^\.robinhood-proof-snapshot-[a-z0-9_]{8}\.sqlite3(?:-(?:wal|shm|journal))?$"
)
_ACTIVE_PREFIX = ".robinhood-proof-snapshot-active-"
_PROTECTED_NAMES = {
    "solana-roi.sqlite3",
    "solana-roi.sqlite3-wal",
    "solana-roi.sqlite3-shm",
    "solana-roi.sqlite3-journal",
    "solana-roi-robinhood-chain.sqlite3",
    "solana-roi-robinhood-chain.sqlite3-wal",
    "solana-roi-robinhood-chain.sqlite3-shm",
    "solana-roi-robinhood-chain.sqlite3-journal",
}
_LOGGER = logging.getLogger(__name__)
_LOCK = threading.Lock()
_SCHEDULED = False
_COMPLETED = False


def _env_true(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _actual_release() -> str:
    return str(os.getenv("RENDER_GIT_COMMIT") or os.getenv("GITHUB_SHA") or "unknown").strip()


def _release_authorized() -> tuple[bool, str, str]:
    actual = _actual_release()
    expected = str(os.getenv(_EXPECTED_RELEASE_ENV) or "").strip()
    return bool(expected and expected == actual), expected, actual


def _base_result(*, expected: str, actual: str) -> dict[str, Any]:
    return {
        "diagnostic": "robinhood_proof_snapshot_orphan_cleanup",
        "repair_version": REPAIR_VERSION,
        "root": str(_ROOT),
        "historical_pattern": _HISTORICAL_NAME.pattern,
        "expected_release": expected,
        "actual_release": actual,
        "expected_release_matched": bool(expected and expected == actual),
        "minimum_age_seconds": _MIN_AGE_SECONDS,
        "max_candidates": _MAX_CANDIDATES,
        "sqlite_opened": False,
        "file_contents_read": False,
        "canonical_databases_touched": False,
        "active_snapshot_touched": False,
        "retention_policy_changed": False,
        "strategy_thresholds_changed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def _emit(payload: dict[str, Any]) -> None:
    _LOGGER.warning(
        "SOLANA_ROI_ROBINHOOD_PROOF_ORPHAN_CLEANUP %s",
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
    )


def _eligible_candidates(root: Path, *, now: float) -> tuple[list[tuple[Path, int]], dict[str, int], str | None]:
    counts = {
        "scanned": 0,
        "historical_matches": 0,
        "too_young": 0,
        "unsafe_type": 0,
        "protected_skips": 0,
        "active_skips": 0,
    }
    try:
        root_mode = root.lstat().st_mode
    except FileNotFoundError:
        return [], counts, "root_missing"
    if stat.S_ISLNK(root_mode):
        return [], counts, "root_is_symlink"
    if not stat.S_ISDIR(root_mode):
        return [], counts, "root_not_directory"

    candidates: list[tuple[Path, int]] = []
    try:
        entries = list(root.iterdir())
    except OSError as exc:
        return [], counts, f"root_scan_failed:{type(exc).__name__}"

    for path in entries:
        counts["scanned"] += 1
        name = path.name
        if name.startswith(_ACTIVE_PREFIX):
            counts["active_skips"] += 1
            continue
        if name in _PROTECTED_NAMES:
            counts["protected_skips"] += 1
            continue
        if not _HISTORICAL_NAME.fullmatch(name):
            continue
        counts["historical_matches"] += 1
        if counts["historical_matches"] > _MAX_CANDIDATES:
            return [], counts, "candidate_limit_exceeded"
        try:
            info = path.lstat()
        except FileNotFoundError:
            continue
        if not stat.S_ISREG(info.st_mode):
            counts["unsafe_type"] += 1
            continue
        age = max(0.0, now - float(info.st_mtime))
        if age < _MIN_AGE_SECONDS:
            counts["too_young"] += 1
            continue
        candidates.append((path, int(info.st_size)))

    # Unexpected filesystem object types under the exact historical namespace are
    # a reason to fail closed rather than partially sweep around them.
    if counts["unsafe_type"]:
        return [], counts, "unsafe_historical_match_type"
    return candidates, counts, None


def _run_cleanup_once(*, root: Path = _ROOT, now: float | None = None) -> dict[str, Any]:
    global _COMPLETED
    with _LOCK:
        authorized, expected, actual = _release_authorized()
        result = _base_result(expected=expected, actual=actual)
        if _COMPLETED:
            result.update({"status": "already_completed", "deleted_files": 0, "deleted_bytes": 0})
            return result
        if not authorized:
            result.update({"status": "refused_release_mismatch", "deleted_files": 0, "deleted_bytes": 0})
            return result

        scan_now = time.time() if now is None else float(now)
        candidates, counts, refusal = _eligible_candidates(root, now=scan_now)
        result.update(counts)
        result["root"] = str(root)
        if refusal is not None:
            result.update(
                {
                    "status": f"refused_{refusal}",
                    "deleted_files": 0,
                    "deleted_bytes": 0,
                    "cleanup_performed": False,
                }
            )
            return result

        deleted_files = 0
        deleted_bytes = 0
        deleted_largest: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        for path, size in candidates:
            # Revalidate immediately before unlink so a path replacement cannot turn
            # the prior metadata decision into a broader deletion capability.
            try:
                info = path.lstat()
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(info.st_mode):
                errors.append({"name": path.name, "error": "type_changed_before_unlink"})
                continue
            if not _HISTORICAL_NAME.fullmatch(path.name) or path.name.startswith(_ACTIVE_PREFIX):
                errors.append({"name": path.name, "error": "name_changed_before_unlink"})
                continue
            age = max(0.0, scan_now - float(info.st_mtime))
            if age < _MIN_AGE_SECONDS:
                continue
            actual_size = int(info.st_size)
            try:
                path.unlink()
            except FileNotFoundError:
                continue
            except OSError as exc:
                errors.append({"name": path.name, "error": type(exc).__name__})
                continue
            deleted_files += 1
            deleted_bytes += actual_size
            deleted_largest.append({"name": path.name, "bytes": actual_size})

        deleted_largest.sort(key=lambda row: (-int(row["bytes"]), str(row["name"])))
        _COMPLETED = True
        result.update(
            {
                "status": "completed" if not errors else "completed_with_unlink_errors",
                "candidate_files": len(candidates),
                "deleted_files": deleted_files,
                "deleted_bytes": deleted_bytes,
                "cleanup_performed": deleted_files > 0,
                "unlink_error_count": len(errors),
                "unlink_errors": errors[:20],
                "largest_deleted": deleted_largest[:20],
                "canonical_databases_touched": False,
                "active_snapshot_touched": False,
            }
        )
        return result


def _timer_target() -> None:
    payload = _run_cleanup_once()
    _emit(payload)


def install_robinhood_proof_snapshot_orphan_cleanup(app: Any) -> None:
    global _SCHEDULED
    app.state.roi_robinhood_proof_snapshot_orphan_cleanup_version = REPAIR_VERSION
    app.state.roi_robinhood_proof_snapshot_orphan_cleanup_enabled = False
    app.state.roi_robinhood_proof_snapshot_orphan_cleanup_exact_historical_pattern = True
    app.state.roi_robinhood_proof_snapshot_orphan_cleanup_paper_only = True
    app.state.roi_robinhood_proof_snapshot_orphan_cleanup_live_money_authority = False

    if not _env_true(_ENABLED_ENV):
        return
    if not bool(getattr(app.state, "roi_robinhood_proof_snapshot_lifecycle_repair", False)):
        authorized, expected, actual = _release_authorized()
        payload = _base_result(expected=expected, actual=actual)
        payload.update(
            {
                "status": "refused_lifecycle_repair_not_installed",
                "expected_release_matched": authorized,
                "deleted_files": 0,
                "deleted_bytes": 0,
                "cleanup_performed": False,
            }
        )
        _emit(payload)
        return
    authorized, expected, actual = _release_authorized()
    if not authorized:
        payload = _base_result(expected=expected, actual=actual)
        payload.update(
            {
                "status": "refused_release_mismatch",
                "deleted_files": 0,
                "deleted_bytes": 0,
                "cleanup_performed": False,
            }
        )
        _emit(payload)
        return
    with _LOCK:
        if _SCHEDULED:
            return
        _SCHEDULED = True
    app.state.roi_robinhood_proof_snapshot_orphan_cleanup_enabled = True
    timer = threading.Timer(_START_DELAY_SECONDS, _timer_target)
    timer.daemon = True
    timer.name = "robinhood-proof-orphan-cleanup-once"
    timer.start()


__all__ = [
    "REPAIR_VERSION",
    "_eligible_candidates",
    "_run_cleanup_once",
    "install_robinhood_proof_snapshot_orphan_cleanup",
]
