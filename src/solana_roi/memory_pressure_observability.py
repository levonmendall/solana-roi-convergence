from __future__ import annotations

"""Low-overhead production attribution for cgroup memory-pressure events.

This module is observability-only.  It reads cgroup-v2 and /proc state and emits a
bounded JSON line when the service approaches its memory envelope.  It never
changes strategy, persistence, certification, or resource-control behavior.
"""

import atexit
import json
import os
import threading
from pathlib import Path
from typing import Any

OBSERVABILITY_VERSION = "memory-pressure-attribution-v1"
SAMPLE_INTERVAL_SECONDS = 5.0
PRESSURE_LOG_FRACTION = 0.70
MAX_PROCESS_ROWS = 16
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_STOP = threading.Event()
_THREAD: threading.Thread | None = None
_LOCK = threading.Lock()
_INSTALLED = False


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
    result: dict[str, int] = {}
    for line in lines:
        parts = line.split()
        if len(parts) != 2:
            continue
        try:
            result[str(parts[0])] = int(parts[1])
        except ValueError:
            continue
    return result


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
        values: dict[str, float | int] = {}
        for part in parts[1:]:
            if "=" not in part:
                continue
            key, raw = part.split("=", 1)
            try:
                values[key] = int(raw) if key == "total" else float(raw)
            except ValueError:
                continue
        result[str(parts[0])] = values
    return result


def _smaps_rollup(path: Path) -> dict[str, int]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return {}
    selected = {
        "Rss": "rss_kib",
        "Pss": "pss_kib",
        "Pss_Anon": "pss_anon_kib",
        "Pss_File": "pss_file_kib",
        "Anonymous": "anonymous_kib",
        "Private_Clean": "private_clean_kib",
        "Private_Dirty": "private_dirty_kib",
    }
    result: dict[str, int] = {}
    for line in lines:
        if ":" not in line:
            continue
        key, raw = line.split(":", 1)
        out = selected.get(key.strip())
        if out is None:
            continue
        parts = raw.strip().split()
        if not parts:
            continue
        try:
            result[out] = max(0, int(parts[0]))
        except ValueError:
            continue
    return result


def _process_rows(cgroup_root: Path, proc_root: Path) -> list[dict[str, Any]]:
    try:
        raw_pids = (cgroup_root / "cgroup.procs").read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        raw_pids = []
    rows: list[dict[str, Any]] = []
    for raw in raw_pids:
        try:
            pid = int(raw.strip())
        except ValueError:
            continue
        if pid <= 0:
            continue
        proc = proc_root / str(pid)
        try:
            comm = (proc / "comm").read_text(encoding="utf-8").strip()[:64]
        except (OSError, UnicodeError):
            comm = "unknown"
        row: dict[str, Any] = {"pid": pid, "comm": comm}
        row.update(_smaps_rollup(proc / "smaps_rollup"))
        rows.append(row)
    rows.sort(key=lambda row: int(row.get("pss_kib") or row.get("rss_kib") or 0), reverse=True)
    return rows[:MAX_PROCESS_ROWS]


def capture_detail(
    cgroup_root: Path | str = Path("/sys/fs/cgroup"),
    proc_root: Path | str = Path("/proc"),
) -> dict[str, Any]:
    root = Path(cgroup_root)
    proc = Path(proc_root)
    current = _read_scalar(root / "memory.current")
    maximum = _read_scalar(root / "memory.max")
    headroom = max(0, maximum - current) if current is not None and maximum is not None else None
    fraction = (
        float(current) / float(maximum)
        if current is not None and maximum not in (None, 0)
        else None
    )
    stat = _read_key_values(root / "memory.stat")
    events = _read_key_values(root / "memory.events")
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
            "pgfault",
            "pgmajfault",
            "workingset_refault_anon",
            "workingset_refault_file",
            "workingset_activate_anon",
            "workingset_activate_file",
        )
        if key in stat
    }
    selected_events = {
        key: int(events.get(key, 0) or 0)
        for key in ("low", "high", "max", "oom", "oom_kill", "oom_group_kill")
        if key in events
    }
    return {
        "version": OBSERVABILITY_VERSION,
        "memory_current_bytes": current,
        "memory_max_bytes": maximum,
        "memory_headroom_bytes": headroom,
        "memory_fraction": fraction,
        "memory_stat": selected_stat,
        "memory_events": selected_events,
        "memory_pressure": _read_pressure(root / "memory.pressure"),
        "processes": _process_rows(root, proc),
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


def emit_if_pressure(reason: str = "periodic") -> dict[str, Any]:
    detail = capture_detail()
    fraction = detail.get("memory_fraction")
    if isinstance(fraction, (int, float)) and float(fraction) >= PRESSURE_LOG_FRACTION:
        payload = dict(detail)
        payload["reason"] = str(reason)
        print(
            "ROI_MEMORY_DETAIL "
            + json.dumps(payload, sort_keys=True, separators=(",", ":")),
            flush=True,
        )
    return detail


def _sampler() -> None:
    while not _STOP.wait(SAMPLE_INTERVAL_SECONDS):
        try:
            emit_if_pressure("periodic")
        except Exception:
            # Observability must never affect production semantics.
            continue


def _stop() -> None:
    _STOP.set()


def install_memory_pressure_observability() -> None:
    global _INSTALLED, _THREAD
    with _LOCK:
        if _INSTALLED:
            return
        _INSTALLED = True
    try:
        emit_if_pressure("install")
    except Exception:
        pass
    thread = threading.Thread(target=_sampler, name="memory-pressure-observability", daemon=True)
    _THREAD = thread
    thread.start()
    atexit.register(_stop)


def status() -> dict[str, Any]:
    return {
        "version": OBSERVABILITY_VERSION,
        "installed": _INSTALLED,
        "sampler_running": bool(_THREAD is not None and _THREAD.is_alive()),
        "sample_interval_seconds": SAMPLE_INTERVAL_SECONDS,
        "pressure_log_fraction": PRESSURE_LOG_FRACTION,
        "max_process_rows": MAX_PROCESS_ROWS,
        "read_only_observability": True,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = [
    "OBSERVABILITY_VERSION",
    "capture_detail",
    "emit_if_pressure",
    "install_memory_pressure_observability",
    "status",
]
