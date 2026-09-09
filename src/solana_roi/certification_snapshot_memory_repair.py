from __future__ import annotations

"""Bound certification snapshot page-cache pressure inside the authoritative cgroup.

The isolated certifier still consumes one exact immutable SQLite snapshot.  This
repair changes only how the authoritative runtime materializes that temporary
snapshot: completed backup ranges are synchronously flushed and advised out of
page cache, while raw cgroup usage is checked before it can approach the hard
memory limit.  A memory-bound export fails closed; it never changes strategy,
certification thresholds, continuity semantics, or paper-only authority.
"""

import os
import shutil
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


REPAIR_VERSION = "certification-snapshot-cgroup-memory-v1-progressive-cache-release"
DEFAULT_CACHE_RELEASE_INTERVAL_BYTES = 32 * 1024 * 1024
DEFAULT_RAW_MEMORY_STOP_FRACTION = 0.86
DEFAULT_RAW_MEMORY_MIN_HEADROOM_BYTES = 256 * 1024 * 1024

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
STRATEGY_THRESHOLDS_CHANGED = False
CERTIFICATION_THRESHOLDS_CHANGED = False
CONTINUITY_SEMANTICS_CHANGED = False

_STATE_LOCK = threading.Lock()
_STATE: dict[str, Any] = {
    "installed": False,
    "exports_started": 0,
    "exports_completed": 0,
    "exports_aborted_for_raw_cgroup_headroom": 0,
    "progressive_cache_release_calls": 0,
    "progressive_cache_release_bytes": 0,
    "last_raw_memory_current_bytes": None,
    "last_raw_memory_max_bytes": None,
    "last_raw_memory_headroom_bytes": None,
    "last_raw_memory_fraction": None,
}


def _env_int(name: str, default: int, *, minimum: int) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except ValueError:
        return default


def _env_float(name: str, default: float, *, minimum: float, maximum: float) -> float:
    try:
        return min(maximum, max(minimum, float(os.getenv(name, str(default)))))
    except ValueError:
        return default


def _cache_release_interval_bytes() -> int:
    return _env_int(
        "SOLANA_ROI_CERTIFICATION_CACHE_RELEASE_INTERVAL_BYTES",
        DEFAULT_CACHE_RELEASE_INTERVAL_BYTES,
        minimum=4 * 1024 * 1024,
    )


def _raw_memory_stop_fraction() -> float:
    return _env_float(
        "SOLANA_ROI_CERTIFICATION_RAW_MEMORY_STOP_FRACTION",
        DEFAULT_RAW_MEMORY_STOP_FRACTION,
        minimum=0.60,
        maximum=0.95,
    )


def _raw_memory_min_headroom_bytes() -> int:
    return _env_int(
        "SOLANA_ROI_CERTIFICATION_RAW_MEMORY_MIN_HEADROOM_BYTES",
        DEFAULT_RAW_MEMORY_MIN_HEADROOM_BYTES,
        minimum=64 * 1024 * 1024,
    )


def _read_cgroup_scalar(path: Path) -> int | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        return None
    if not raw or raw == "max":
        return None
    try:
        return max(0, int(raw))
    except ValueError:
        return None


def _raw_cgroup_sample() -> dict[str, int | float | None]:
    root = Path(os.getenv("SOLANA_ROI_CGROUP_ROOT", "/sys/fs/cgroup"))
    current = _read_cgroup_scalar(root / "memory.current")
    maximum = _read_cgroup_scalar(root / "memory.max")
    headroom = None
    fraction = None
    if current is not None and maximum not in (None, 0):
        headroom = max(0, int(maximum) - int(current))
        fraction = float(current) / float(maximum)
    sample: dict[str, int | float | None] = {
        "current": current,
        "maximum": maximum,
        "headroom": headroom,
        "fraction": fraction,
    }
    with _STATE_LOCK:
        _STATE["last_raw_memory_current_bytes"] = current
        _STATE["last_raw_memory_max_bytes"] = maximum
        _STATE["last_raw_memory_headroom_bytes"] = headroom
        _STATE["last_raw_memory_fraction"] = fraction
    return sample


def _fadvise_range(fd: int, offset: int, length: int) -> bool:
    fadvise = getattr(os, "posix_fadvise", None)
    advice = getattr(os, "POSIX_FADV_DONTNEED", None)
    if fadvise is None or advice is None or length <= 0:
        return False
    try:
        fadvise(fd, max(0, int(offset)), max(0, int(length)), advice)
        return True
    except OSError:
        return False


def _flush_drop_range(path: Path, offset: int, length: int, *, flush: bool) -> bool:
    if length <= 0 or not path.exists():
        return False
    flags = os.O_RDWR if flush else os.O_RDONLY
    try:
        fd = os.open(path, flags)
    except OSError:
        return False
    advised = False
    try:
        if flush:
            try:
                os.fsync(fd)
            except OSError:
                # The cgroup guard below is the hard safety boundary; cache advice
                # is an optimization and must never alter snapshot correctness.
                pass
        advised = _fadvise_range(fd, offset, length)
    finally:
        os.close(fd)
    if advised:
        with _STATE_LOCK:
            _STATE["progressive_cache_release_calls"] = int(
                _STATE["progressive_cache_release_calls"]
            ) + 1
            _STATE["progressive_cache_release_bytes"] = int(
                _STATE["progressive_cache_release_bytes"]
            ) + max(0, int(length))
    return advised


def _aggressive_cache_release(source_path: Path, snapshot: Path, completed_bytes: int) -> None:
    # Source pages are immutable for the pinned read transaction. DONTNEED is only
    # advisory: if SQLite needs a page again the kernel simply rereads it.
    try:
        source_bytes = int(source_path.stat().st_size)
    except OSError:
        source_bytes = 0
    if source_bytes > 0:
        _flush_drop_range(source_path, 0, source_bytes, flush=False)
    if completed_bytes > 0:
        _flush_drop_range(snapshot, 0, completed_bytes, flush=True)


def _guard_raw_cgroup_headroom(
    source_path: Path,
    snapshot: Path,
    completed_bytes: int,
) -> None:
    """Abort export before raw cgroup charge can reach the kernel hard limit.

    Render's displayed working-set metric can exclude reclaimable file cache, but
    the cgroup hard limit is enforced against raw memory.current.  We therefore
    first evict file cache and only then fail the export if raw charge still has
    insufficient headroom. Missing cgroup telemetry keeps prior behavior so local
    tests and non-cgroup environments remain supported.
    """
    sample = _raw_cgroup_sample()
    current = sample.get("current")
    maximum = sample.get("maximum")
    if current is None or maximum in (None, 0):
        return

    fraction = float(current) / float(maximum)
    headroom = max(0, int(maximum) - int(current))
    unsafe = (
        fraction >= _raw_memory_stop_fraction()
        or headroom < _raw_memory_min_headroom_bytes()
    )
    if not unsafe:
        return

    _aggressive_cache_release(source_path, snapshot, completed_bytes)
    # Give writeback/cache reclaim a scheduling point without turning this into a
    # retry loop or weakening the 55-second bounded snapshot deadline.
    time.sleep(0.01)
    sample = _raw_cgroup_sample()
    current = sample.get("current")
    maximum = sample.get("maximum")
    if current is None or maximum in (None, 0):
        return
    fraction = float(current) / float(maximum)
    headroom = max(0, int(maximum) - int(current))
    if (
        fraction >= _raw_memory_stop_fraction()
        or headroom < _raw_memory_min_headroom_bytes()
    ):
        with _STATE_LOCK:
            _STATE["exports_aborted_for_raw_cgroup_headroom"] = int(
                _STATE["exports_aborted_for_raw_cgroup_headroom"]
            ) + 1
        raise MemoryError(
            "canonical certification snapshot raw cgroup memory headroom exhausted"
        )


def _bounded_snapshot_store_to_file(store: Any, snapshot: Path) -> tuple[int, int]:
    """Create the unchanged pinned snapshot with bounded page-cache residency."""
    from . import certification_service_split as split

    source_path = getattr(store, "path", None)
    if source_path is None:
        raise RuntimeError("canonical runtime store path unavailable")
    source_path = Path(source_path)
    if not source_path.is_file():
        raise RuntimeError("canonical runtime SQLite file unavailable")

    with _STATE_LOCK:
        _STATE["exports_started"] = int(_STATE["exports_started"]) + 1

    source_uri = f"file:{source_path.resolve()}?mode=ro"
    source = sqlite3.connect(source_uri, uri=True, timeout=5.0)
    destination = sqlite3.connect(snapshot)
    started = time.monotonic()
    page_size = 0
    released_through = 0
    try:
        source.execute("PRAGMA query_only=ON")
        source.execute("PRAGMA busy_timeout=5000")
        source.execute("BEGIN")
        source.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
        page_count = int(source.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(source.execute("PRAGMA page_size").fetchone()[0])
        estimated_bytes = page_count * page_size
        if estimated_bytes > split._snapshot_max_bytes():
            raise RuntimeError("canonical certification snapshot exceeds configured bounded export")

        free_bytes = int(shutil.disk_usage(snapshot.parent).free)
        required_free = estimated_bytes + split._snapshot_free_reserve_bytes()
        if free_bytes < required_free:
            raise OSError("insufficient bounded certification snapshot disk headroom")

        _guard_raw_cgroup_headroom(source_path, snapshot, 0)
        release_interval = _cache_release_interval_bytes()

        def progress(_status: int, remaining: int, total: int) -> None:
            nonlocal released_through
            if time.monotonic() - started > split._snapshot_deadline_seconds():
                raise TimeoutError("canonical certification snapshot exceeded bounded deadline")

            completed = max(0, int(total) - int(remaining)) * page_size
            if completed > released_through and (
                completed - released_through >= release_interval or int(remaining) == 0
            ):
                length = completed - released_through
                _flush_drop_range(snapshot, released_through, length, flush=True)
                _flush_drop_range(source_path, released_through, length, flush=False)
                released_through = completed
            _guard_raw_cgroup_headroom(source_path, snapshot, completed)

        source.backup(
            destination,
            pages=split._snapshot_pages_per_step(),
            progress=progress,
            sleep=split._snapshot_step_sleep_seconds(),
        )
        destination.commit()
        size = int(snapshot.stat().st_size)
        if size > released_through:
            _flush_drop_range(snapshot, released_through, size - released_through, flush=True)
        split._drop_file_cache(source_path)
        split._drop_file_cache(snapshot)
        with _STATE_LOCK:
            _STATE["exports_completed"] = int(_STATE["exports_completed"]) + 1
        return size, estimated_bytes
    finally:
        try:
            source.rollback()
        except sqlite3.Error:
            pass
        destination.close()
        source.close()
        split._drop_file_cache(source_path)
        if snapshot.exists():
            split._drop_file_cache(snapshot)


def status() -> dict[str, Any]:
    with _STATE_LOCK:
        state = dict(_STATE)
    state.update(
        {
            "repair_version": REPAIR_VERSION,
            "progressive_cache_release": True,
            "raw_cgroup_hard_limit_guard": True,
            "raw_memory_stop_fraction": _raw_memory_stop_fraction(),
            "raw_memory_min_headroom_bytes": _raw_memory_min_headroom_bytes(),
            "cache_release_interval_bytes": _cache_release_interval_bytes(),
            "snapshot_consistency_changed": False,
            "strategy_thresholds_changed": STRATEGY_THRESHOLDS_CHANGED,
            "certification_thresholds_changed": CERTIFICATION_THRESHOLDS_CHANGED,
            "continuity_semantics_changed": CONTINUITY_SEMANTICS_CHANGED,
            "paper_only": PAPER_ONLY,
            "live_money_authority": LIVE_MONEY_AUTHORITY,
            "signing_available": SIGNING_AVAILABLE,
            "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        }
    )
    return state


def install_certification_snapshot_memory_repair() -> None:
    from . import certification_service_split as split

    current = split._snapshot_store_to_file
    if bool(getattr(current, "_roi_snapshot_cgroup_memory_repair", False)):
        with _STATE_LOCK:
            _STATE["installed"] = True
        return

    setattr(_bounded_snapshot_store_to_file, "_roi_snapshot_cgroup_memory_repair", True)
    setattr(_bounded_snapshot_store_to_file, "_roi_original_snapshot_store_to_file", current)
    split._snapshot_store_to_file = _bounded_snapshot_store_to_file  # type: ignore[assignment]

    current_status = split.status
    if not bool(getattr(current_status, "_roi_snapshot_cgroup_memory_status", False)):
        def split_status_with_memory_repair() -> dict[str, Any]:
            payload = current_status()
            payload["snapshot_cgroup_memory_repair"] = status()
            return payload

        setattr(split_status_with_memory_repair, "_roi_snapshot_cgroup_memory_status", True)
        split.status = split_status_with_memory_repair  # type: ignore[assignment]

    with _STATE_LOCK:
        _STATE["installed"] = True


__all__ = [
    "REPAIR_VERSION",
    "_bounded_snapshot_store_to_file",
    "_guard_raw_cgroup_headroom",
    "_raw_cgroup_sample",
    "install_certification_snapshot_memory_repair",
    "status",
]
