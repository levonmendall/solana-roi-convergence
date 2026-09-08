from __future__ import annotations

import atexit
import json
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


FORENSICS_VERSION = "cgroup-oom-forensics-v1-bounded-durable-snapshot"
SAMPLE_INTERVAL_SECONDS = 5.0
CURRENT_FILENAME = "runtime-memory-forensics.json"
PREVIOUS_FILENAME = "runtime-memory-forensics.previous.json"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
CERTIFICATION_THRESHOLDS_CHANGED = False
ECONOMIC_THRESHOLDS_CHANGED = False
CANONICAL_EVIDENCE_RESET = False

_LOCK = threading.Lock()
_STOP = threading.Event()
_THREAD: threading.Thread | None = None
_INSTALLED = False
_PROCESS_EPOCH = f"{datetime.now(timezone.utc).isoformat()}:{os.getpid()}"
_ACTIVE_PHASES: dict[str, int] = {}
_LAST_SNAPSHOT: dict[str, Any] | None = None
_PREVIOUS_EPOCH_SNAPSHOT: dict[str, Any] | None = None
_LAST_PERSIST_ERROR: str | None = None
_PERSIST_WRITES = 0


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_scalar(path: Path) -> int | None:
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


def _read_key_values(path: Path) -> dict[str, int]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return {}
    values: dict[str, int] = {}
    for line in lines:
        parts = line.split()
        if len(parts) != 2:
            continue
        try:
            values[str(parts[0])] = int(parts[1])
        except ValueError:
            continue
    return values


def _read_pressure(path: Path) -> dict[str, dict[str, float | int]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return {}
    result: dict[str, dict[str, float | int]] = {}
    for line in lines:
        parts = line.split()
        if not parts:
            continue
        row: dict[str, float | int] = {}
        for part in parts[1:]:
            if "=" not in part:
                continue
            key, raw = part.split("=", 1)
            try:
                row[key] = int(raw) if key == "total" else float(raw)
            except ValueError:
                continue
        result[parts[0]] = row
    return result


def _process_rss_bytes() -> int | None:
    try:
        statm = Path("/proc/self/statm").read_text(encoding="utf-8").split()
        resident_pages = int(statm[1])
        return max(0, resident_pages * int(os.sysconf("SC_PAGE_SIZE")))
    except (OSError, UnicodeError, ValueError, IndexError):
        return None


def _db_path() -> Path | None:
    raw = os.getenv("SOLANA_ROI_DB_PATH", "").strip()
    return Path(raw) if raw else None


def _data_dir() -> Path | None:
    db = _db_path()
    if db is not None:
        return db.parent
    fallback = Path("/var/data")
    return fallback if fallback.exists() else None


def _artifact_sizes() -> dict[str, int | None]:
    db = _db_path()
    if db is None:
        return {"database_bytes": None, "wal_bytes": None, "shm_bytes": None}

    def size(path: Path) -> int | None:
        try:
            return int(path.stat().st_size)
        except OSError:
            return None

    return {
        "database_bytes": size(db),
        "wal_bytes": size(Path(str(db) + "-wal")),
        "shm_bytes": size(Path(str(db) + "-shm")),
    }


def _snapshot_paths() -> tuple[Path | None, Path | None]:
    directory = _data_dir()
    if directory is None:
        return None, None
    return directory / CURRENT_FILENAME, directory / PREVIOUS_FILENAME


def _atomic_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    try:
        tmp.write_text(raw, encoding="utf-8")
        os.replace(tmp, path)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


def _read_json(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def capture_snapshot(reason: str = "periodic") -> dict[str, Any]:
    global _LAST_SNAPSHOT, _LAST_PERSIST_ERROR, _PERSIST_WRITES
    root = Path("/sys/fs/cgroup")
    current = _read_scalar(root / "memory.current")
    maximum = _read_scalar(root / "memory.max")
    peak = _read_scalar(root / "memory.peak")
    events = _read_key_values(root / "memory.events")
    stat = _read_key_values(root / "memory.stat")
    pressure = _read_pressure(root / "memory.pressure")
    with _LOCK:
        active_phases = sorted(name for name, count in _ACTIVE_PHASES.items() if count > 0)
    headroom = max(0, maximum - current) if current is not None and maximum is not None else None
    fraction = (float(current) / float(maximum)) if current is not None and maximum not in (None, 0) else None
    selected_stat = {
        key: int(stat.get(key, 0) or 0)
        for key in (
            "anon",
            "file",
            "file_mapped",
            "file_dirty",
            "file_writeback",
            "active_anon",
            "inactive_anon",
            "active_file",
            "inactive_file",
            "shmem",
            "slab",
            "slab_reclaimable",
            "slab_unreclaimable",
            "kernel",
            "kernel_stack",
            "pagetables",
            "sock",
        )
        if key in stat
    }
    selected_events = {
        key: int(events.get(key, 0) or 0)
        for key in ("low", "high", "max", "oom", "oom_kill", "oom_group_kill")
        if key in events
    }
    snapshot: dict[str, Any] = {
        "forensics_version": FORENSICS_VERSION,
        "captured_at": _utcnow(),
        "reason": str(reason),
        "process_epoch": _PROCESS_EPOCH,
        "pid": os.getpid(),
        "active_phases": active_phases,
        "memory_current_bytes": current,
        "memory_max_bytes": maximum,
        "memory_peak_bytes": peak,
        "memory_headroom_bytes": headroom,
        "memory_fraction": fraction,
        "memory_events": selected_events,
        "memory_stat": selected_stat,
        "memory_pressure": pressure,
        "process_rss_bytes": _process_rss_bytes(),
        **_artifact_sizes(),
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }
    current_path, _ = _snapshot_paths()
    persist_error: str | None = None
    if current_path is not None:
        try:
            _atomic_write(current_path, snapshot)
            _PERSIST_WRITES += 1
        except Exception as exc:  # Observability must never alter runtime/certification semantics.
            persist_error = f"{type(exc).__name__}:{exc}"
    with _LOCK:
        _LAST_SNAPSHOT = dict(snapshot)
        _LAST_PERSIST_ERROR = persist_error
    return snapshot


@contextmanager
def phase(name: str) -> Iterator[None]:
    key = str(name or "unknown")
    with _LOCK:
        _ACTIVE_PHASES[key] = int(_ACTIVE_PHASES.get(key, 0) or 0) + 1
    capture_snapshot(f"phase_start:{key}")
    try:
        yield
    except BaseException:
        capture_snapshot(f"phase_error:{key}")
        raise
    finally:
        capture_snapshot(f"phase_end:{key}")
        with _LOCK:
            remaining = max(0, int(_ACTIVE_PHASES.get(key, 0) or 0) - 1)
            if remaining:
                _ACTIVE_PHASES[key] = remaining
            else:
                _ACTIVE_PHASES.pop(key, None)


def _sampler_main() -> None:
    while not _STOP.wait(SAMPLE_INTERVAL_SECONDS):
        capture_snapshot("periodic")


def _stop_sampler() -> None:
    _STOP.set()


def install_cgroup_oom_forensics() -> None:
    global _INSTALLED, _THREAD, _PREVIOUS_EPOCH_SNAPSHOT, _LAST_PERSIST_ERROR
    with _LOCK:
        if _INSTALLED:
            return
        _INSTALLED = True
    current_path, previous_path = _snapshot_paths()
    prior = _read_json(current_path)
    if prior is not None:
        _PREVIOUS_EPOCH_SNAPSHOT = prior
        if previous_path is not None:
            try:
                _atomic_write(previous_path, prior)
            except Exception as exc:
                _LAST_PERSIST_ERROR = f"{type(exc).__name__}:{exc}"
    capture_snapshot("process_startup")
    thread = threading.Thread(target=_sampler_main, name="cgroup-oom-forensics", daemon=True)
    _THREAD = thread
    thread.start()
    atexit.register(_stop_sampler)


def status() -> dict[str, Any]:
    with _LOCK:
        last = dict(_LAST_SNAPSHOT) if _LAST_SNAPSHOT is not None else None
        previous = dict(_PREVIOUS_EPOCH_SNAPSHOT) if _PREVIOUS_EPOCH_SNAPSHOT is not None else None
        persist_error = _LAST_PERSIST_ERROR
        writes = int(_PERSIST_WRITES)
        installed = bool(_INSTALLED)
    current_path, previous_path = _snapshot_paths()
    return {
        "forensics_version": FORENSICS_VERSION,
        "installed": installed,
        "sampler_running": bool(_THREAD is not None and _THREAD.is_alive()),
        "sample_interval_seconds": SAMPLE_INTERVAL_SECONDS,
        "process_epoch": _PROCESS_EPOCH,
        "current_snapshot": last,
        "previous_epoch_snapshot": previous,
        "current_snapshot_path": str(current_path) if current_path is not None else None,
        "previous_snapshot_path": str(previous_path) if previous_path is not None else None,
        "persist_writes": writes,
        "last_persist_error": persist_error,
        "bounded_persistence": True,
        "history_files_max": 2,
        "read_only_observability": True,
        "certification_thresholds_changed": CERTIFICATION_THRESHOLDS_CHANGED,
        "economic_thresholds_changed": ECONOMIC_THRESHOLDS_CHANGED,
        "canonical_evidence_reset": CANONICAL_EVIDENCE_RESET,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = [
    "FORENSICS_VERSION",
    "SAMPLE_INTERVAL_SECONDS",
    "capture_snapshot",
    "install_cgroup_oom_forensics",
    "phase",
    "status",
]
