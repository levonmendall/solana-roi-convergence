from __future__ import annotations

import fcntl
import os
import shutil
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from . import cgroup_oom_forensics as cgroup


COORDINATOR_VERSION = "certification-generation-single-flight-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
CERTIFICATION_THRESHOLDS_CHANGED = False
ECONOMIC_THRESHOLDS_CHANGED = False
CANONICAL_EVIDENCE_RESET = False

DEFAULT_ACQUIRE_TIMEOUT_SECONDS = 120.0
DEFAULT_MEMORY_START_FRACTION = 0.90
DEFAULT_DISK_RESERVE_BYTES = 2 * 1024 * 1024 * 1024
DEFAULT_WAL_START_MAX_BYTES = 1024 * 1024 * 1024

_THREAD_LOCK = threading.RLock()
_STATE_LOCK = threading.Lock()
_LOCAL = threading.local()
_SEQUENCE = 0
_STATE: dict[str, Any] = {
    "active": False,
    "active_surface": None,
    "active_generation_id": None,
    "active_started_at": None,
    "acquisitions": 0,
    "contentions": 0,
    "nested_acquisitions": 0,
    "guard_rejections": 0,
    "last_surface": None,
    "last_generation_id": None,
    "last_started_at": None,
    "last_completed_at": None,
    "last_wait_seconds": None,
    "last_duration_seconds": None,
    "last_guard_reason": None,
}


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _release_commit() -> str:
    for key in ("RENDER_GIT_COMMIT", "GITHUB_SHA", "SOLANA_ROI_RELEASE_COMMIT"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return "unbound-local-release"


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _db_path() -> Path | None:
    raw = os.getenv("SOLANA_ROI_DB_PATH", "").strip()
    return Path(raw) if raw else None


def _data_dir() -> Path:
    db = _db_path()
    if db is not None:
        return db.parent
    render_data = Path("/var/data")
    if render_data.exists():
        return render_data
    return Path(os.getenv("TMPDIR", "/tmp"))


def _lock_path() -> Path:
    explicit = os.getenv("SOLANA_ROI_CERTIFICATION_LEASE_PATH", "").strip()
    return Path(explicit) if explicit else _data_dir() / "certification-generation.lock"


def _wal_size() -> int | None:
    db = _db_path()
    if db is None:
        return None
    try:
        return int(Path(str(db) + "-wal").stat().st_size)
    except OSError:
        return 0


def _disk_free() -> int | None:
    try:
        return int(shutil.disk_usage(_data_dir()).free)
    except OSError:
        return None


@dataclass(frozen=True)
class GenerationLease:
    generation_id: str
    release_commit: str
    surface: str
    started_at: str
    wait_seconds: float
    nested: bool = False


class CertificationResourceGuardError(RuntimeError):
    pass


def resource_guard(surface: str) -> dict[str, Any]:
    """Fail closed before an expensive certification build enters SQLite.

    This is defense in depth, not a replacement for bounded generation work. Missing
    telemetry does not invent a failure; known unsafe resource state does.
    """
    snapshot = cgroup.capture_snapshot(f"certification_resource_guard:{surface}")
    fraction = snapshot.get("memory_fraction")
    wal_bytes = snapshot.get("wal_bytes")
    disk_free = _disk_free()

    memory_limit = _float_env(
        "SOLANA_ROI_CERTIFICATION_MEMORY_START_FRACTION",
        DEFAULT_MEMORY_START_FRACTION,
    )
    disk_reserve = _int_env(
        "SOLANA_ROI_CERTIFICATION_DISK_RESERVE_BYTES",
        DEFAULT_DISK_RESERVE_BYTES,
    )
    wal_limit = _int_env(
        "SOLANA_ROI_CERTIFICATION_WAL_START_MAX_BYTES",
        DEFAULT_WAL_START_MAX_BYTES,
    )

    blockers: list[str] = []
    if isinstance(fraction, (int, float)) and float(fraction) >= memory_limit:
        blockers.append("cgroup_memory_headroom_below_generation_reserve")
    if isinstance(disk_free, int) and disk_free < disk_reserve:
        blockers.append("persistent_disk_headroom_below_generation_reserve")
    if isinstance(wal_bytes, int) and wal_bytes > wal_limit:
        blockers.append("sqlite_wal_above_generation_start_limit")

    result = {
        "safe_to_start": not blockers,
        "surface": str(surface),
        "memory_fraction": fraction,
        "memory_start_limit_fraction": memory_limit,
        "memory_current_bytes": snapshot.get("memory_current_bytes"),
        "memory_max_bytes": snapshot.get("memory_max_bytes"),
        "disk_free_bytes": disk_free,
        "disk_reserve_bytes": disk_reserve,
        "wal_bytes": wal_bytes,
        "wal_start_max_bytes": wal_limit,
        "blockers": blockers,
        "fail_closed": True,
        "serves_stale_as_fresh": False,
    }
    if blockers:
        reason = ",".join(blockers)
        with _STATE_LOCK:
            _STATE["guard_rejections"] = int(_STATE.get("guard_rejections", 0) or 0) + 1
            _STATE["last_guard_reason"] = reason
        raise CertificationResourceGuardError(reason)
    return result


def _next_generation_id() -> str:
    global _SEQUENCE
    with _STATE_LOCK:
        _SEQUENCE += 1
        sequence = _SEQUENCE
    return f"{_release_commit()}:{os.getpid()}:{sequence}"


def _flock_with_timeout(handle: Any, timeout_seconds: float) -> float:
    started = time.monotonic()
    contended = False
    while True:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            if contended:
                with _STATE_LOCK:
                    _STATE["contentions"] = int(_STATE.get("contentions", 0) or 0) + 1
            return max(0.0, time.monotonic() - started)
        except BlockingIOError:
            contended = True
            if time.monotonic() - started >= timeout_seconds:
                raise TimeoutError("certification_generation_single_flight_timeout")
            time.sleep(0.05)


@contextmanager
def exclusive_generation(
    surface: str,
    *,
    timeout_seconds: float = DEFAULT_ACQUIRE_TIMEOUT_SECONDS,
    enforce_resource_guard: bool = True,
) -> Iterator[GenerationLease]:
    """Serialize expensive certification work across threads and processes.

    Nested certification builders in the owning thread inherit the outer generation
    rather than reacquiring the filesystem lease. This permits existing proof
    composition to call forward/evidence helpers without deadlocking while still
    ensuring only one outer store-heavy generation owns production resources.
    """
    depth = int(getattr(_LOCAL, "depth", 0) or 0)
    current = getattr(_LOCAL, "lease", None)
    if depth > 0 and isinstance(current, GenerationLease):
        _LOCAL.depth = depth + 1
        with _STATE_LOCK:
            _STATE["nested_acquisitions"] = int(_STATE.get("nested_acquisitions", 0) or 0) + 1
        nested = GenerationLease(
            generation_id=current.generation_id,
            release_commit=current.release_commit,
            surface=str(surface),
            started_at=current.started_at,
            wait_seconds=0.0,
            nested=True,
        )
        try:
            yield nested
        finally:
            _LOCAL.depth = max(0, int(getattr(_LOCAL, "depth", 1) or 1) - 1)
        return

    started_monotonic = time.monotonic()
    started_at = _utcnow()
    lock_path = _lock_path()
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    with _THREAD_LOCK:
        with lock_path.open("a+", encoding="utf-8") as handle:
            wait_seconds = _flock_with_timeout(handle, max(0.1, float(timeout_seconds)))
            lease = GenerationLease(
                generation_id=_next_generation_id(),
                release_commit=_release_commit(),
                surface=str(surface),
                started_at=started_at,
                wait_seconds=wait_seconds,
            )
            _LOCAL.depth = 1
            _LOCAL.lease = lease
            try:
                if enforce_resource_guard:
                    resource_guard(str(surface))
                with _STATE_LOCK:
                    _STATE.update(
                        {
                            "active": True,
                            "active_surface": str(surface),
                            "active_generation_id": lease.generation_id,
                            "active_started_at": started_at,
                            "acquisitions": int(_STATE.get("acquisitions", 0) or 0) + 1,
                            "last_surface": str(surface),
                            "last_generation_id": lease.generation_id,
                            "last_started_at": started_at,
                            "last_wait_seconds": wait_seconds,
                            "last_guard_reason": None,
                        }
                    )
                yield lease
            finally:
                completed = _utcnow()
                duration = max(0.0, time.monotonic() - started_monotonic)
                with _STATE_LOCK:
                    _STATE.update(
                        {
                            "active": False,
                            "active_surface": None,
                            "active_generation_id": None,
                            "active_started_at": None,
                            "last_completed_at": completed,
                            "last_duration_seconds": duration,
                        }
                    )
                _LOCAL.depth = 0
                _LOCAL.lease = None
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def current_lease() -> dict[str, Any] | None:
    lease = getattr(_LOCAL, "lease", None)
    return asdict(lease) if isinstance(lease, GenerationLease) else None


def status() -> dict[str, Any]:
    with _STATE_LOCK:
        state = dict(_STATE)
    return {
        "coordinator_version": COORDINATOR_VERSION,
        **state,
        "lock_path": str(_lock_path()),
        "cross_process_single_flight": True,
        "nested_builds_reuse_outer_generation": True,
        "resource_guard_enabled": True,
        "memory_start_limit_fraction": _float_env(
            "SOLANA_ROI_CERTIFICATION_MEMORY_START_FRACTION",
            DEFAULT_MEMORY_START_FRACTION,
        ),
        "disk_reserve_bytes": _int_env(
            "SOLANA_ROI_CERTIFICATION_DISK_RESERVE_BYTES",
            DEFAULT_DISK_RESERVE_BYTES,
        ),
        "wal_start_max_bytes": _int_env(
            "SOLANA_ROI_CERTIFICATION_WAL_START_MAX_BYTES",
            DEFAULT_WAL_START_MAX_BYTES,
        ),
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        "certification_thresholds_changed": CERTIFICATION_THRESHOLDS_CHANGED,
        "economic_thresholds_changed": ECONOMIC_THRESHOLDS_CHANGED,
        "canonical_evidence_reset": CANONICAL_EVIDENCE_RESET,
    }


def install_status_route(app: Any) -> None:
    path = "/v1/operations/certification-generation-coordinator"
    if path not in {getattr(route, "path", None) for route in app.routes}:
        app.add_api_route(path, status, methods=["GET"], name="certification_generation_coordinator")
    app.state.roi_certification_generation_coordinator = True
    app.state.roi_certification_generation_coordinator_version = COORDINATOR_VERSION


__all__ = [
    "COORDINATOR_VERSION",
    "CertificationResourceGuardError",
    "GenerationLease",
    "current_lease",
    "exclusive_generation",
    "install_status_route",
    "resource_guard",
    "status",
]
