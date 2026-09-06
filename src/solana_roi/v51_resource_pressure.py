from __future__ import annotations

import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RESOURCE_PRESSURE_VERSION = "v51-resource-pressure-v1"
SAMPLE_INTERVAL_SECONDS = 15.0
SAMPLE_WINDOW = 240
MEMORY_WARN_FRACTION = 0.75
MEMORY_CRITICAL_FRACTION = 0.90
MEMORY_GROWTH_WARN_BYTES_PER_MINUTE = 64 * 1024 * 1024
MEMORY_GROWTH_CRITICAL_BYTES_PER_MINUTE = 128 * 1024 * 1024
CPU_THROTTLE_WARN_FRACTION = 0.10
CPU_THROTTLE_CRITICAL_FRACTION = 0.25
MIN_TREND_WINDOW_SECONDS = 300.0

_lock = threading.RLock()
_samples: deque[dict[str, Any]] = deque(maxlen=SAMPLE_WINDOW)
_sampler_thread: threading.Thread | None = None


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def _read_int(path: Path) -> int | None:
    raw = _read_text(path)
    if raw in (None, "", "max"):
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _memory_events(root: Path) -> dict[str, int]:
    result: dict[str, int] = {}
    raw = _read_text(root / "memory.events")
    if raw is None:
        return result
    for line in raw.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        try:
            result[parts[0]] = int(parts[1])
        except ValueError:
            continue
    return result


def _cpu_stat(root: Path) -> dict[str, int]:
    result: dict[str, int] = {}
    raw = _read_text(root / "cpu.stat")
    if raw is None:
        return result
    for line in raw.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        try:
            result[parts[0]] = int(parts[1])
        except ValueError:
            continue
    return result


def _cpu_limit(root: Path) -> float | None:
    raw = _read_text(root / "cpu.max")
    if not raw:
        return None
    parts = raw.split()
    if len(parts) != 2 or parts[0] == "max":
        return None
    try:
        quota = float(parts[0])
        period = float(parts[1])
    except ValueError:
        return None
    if quota <= 0.0 or period <= 0.0:
        return None
    return quota / period


def _proc_rss_bytes() -> int | None:
    raw = _read_text(Path("/proc/self/status"))
    if raw is None:
        return None
    for line in raw.splitlines():
        if not line.startswith("VmRSS:"):
            continue
        parts = line.split()
        if len(parts) < 2:
            return None
        try:
            return int(parts[1]) * 1024
        except ValueError:
            return None
    return None


def _raw_sample(*, cgroup_root: Path | None = None) -> dict[str, Any]:
    root = cgroup_root or Path(os.getenv("SOLANA_ROI_CGROUP_ROOT", "/sys/fs/cgroup"))
    current = _read_int(root / "memory.current")
    maximum = _read_int(root / "memory.max")
    events = _memory_events(root)
    cpu = _cpu_stat(root)
    monotonic = time.monotonic()
    return {
        "sampled_at": _utcnow(),
        "monotonic": monotonic,
        "memory_current_bytes": current,
        "memory_max_bytes": maximum,
        "process_rss_bytes": _proc_rss_bytes(),
        "memory_events": events,
        "cpu_usage_usec": cpu.get("usage_usec"),
        "cpu_throttled_usec": cpu.get("throttled_usec"),
        "cpu_nr_throttled": cpu.get("nr_throttled"),
        "cpu_limit_cores": _cpu_limit(root),
        "load_average": list(os.getloadavg()) if hasattr(os, "getloadavg") else None,
    }


def _append_sample(sample: dict[str, Any]) -> None:
    with _lock:
        _samples.append(dict(sample))


def sample_resource_pressure() -> dict[str, Any]:
    sample = _raw_sample()
    _append_sample(sample)
    return sample


def _sampler_loop() -> None:
    while True:
        try:
            sample_resource_pressure()
        except Exception:
            # Resource telemetry is read-only and must never become runtime authority.
            pass
        time.sleep(SAMPLE_INTERVAL_SECONDS)


def ensure_resource_pressure_sampler() -> None:
    global _sampler_thread
    with _lock:
        if _sampler_thread is not None and _sampler_thread.is_alive():
            return
        _sampler_thread = threading.Thread(
            target=_sampler_loop,
            name="v51-resource-pressure",
            daemon=True,
        )
        _sampler_thread.start()


def _ratio(numerator: int | None, denominator: int | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return max(0.0, float(numerator) / float(denominator))


def _trend(samples: list[dict[str, Any]]) -> dict[str, Any]:
    if len(samples) < 2:
        return {
            "window_seconds": 0.0,
            "memory_growth_bytes_per_minute": None,
            "cpu_throttle_fraction": None,
            "cpu_throttled_events_delta": None,
            "trend_window_sufficient": False,
        }
    first = samples[0]
    last = samples[-1]
    seconds = max(0.0, float(last.get("monotonic") or 0.0) - float(first.get("monotonic") or 0.0))
    first_memory = first.get("memory_current_bytes")
    last_memory = last.get("memory_current_bytes")
    growth = None
    if seconds > 0.0 and first_memory is not None and last_memory is not None:
        growth = (float(last_memory) - float(first_memory)) * 60.0 / seconds
    first_usage = first.get("cpu_usage_usec")
    last_usage = last.get("cpu_usage_usec")
    first_throttled = first.get("cpu_throttled_usec")
    last_throttled = last.get("cpu_throttled_usec")
    throttle_fraction = None
    if None not in (first_usage, last_usage, first_throttled, last_throttled):
        usage_delta = max(0, int(last_usage) - int(first_usage))
        throttled_delta = max(0, int(last_throttled) - int(first_throttled))
        total = usage_delta + throttled_delta
        throttle_fraction = float(throttled_delta) / float(total) if total else 0.0
    first_events = first.get("cpu_nr_throttled")
    last_events = last.get("cpu_nr_throttled")
    events_delta = None
    if first_events is not None and last_events is not None:
        events_delta = max(0, int(last_events) - int(first_events))
    return {
        "window_seconds": seconds,
        "memory_growth_bytes_per_minute": growth,
        "cpu_throttle_fraction": throttle_fraction,
        "cpu_throttled_events_delta": events_delta,
        "trend_window_sufficient": seconds >= MIN_TREND_WINDOW_SECONDS,
    }


def resource_pressure_snapshot() -> dict[str, Any]:
    latest = sample_resource_pressure()
    with _lock:
        samples = list(_samples)
    trend = _trend(samples)
    current = latest.get("memory_current_bytes")
    maximum = latest.get("memory_max_bytes")
    memory_fraction = _ratio(current, maximum)
    events = dict(latest.get("memory_events") or {})
    warnings: list[str] = []
    critical: list[str] = []

    if int(events.get("oom_kill") or 0) > 0 or int(events.get("oom") or 0) > 0:
        critical.append("cgroup_oom_observed")
    if memory_fraction is not None:
        if memory_fraction >= MEMORY_CRITICAL_FRACTION:
            critical.append("memory_utilization_critical")
        elif memory_fraction >= MEMORY_WARN_FRACTION:
            warnings.append("memory_utilization_high")

    if bool(trend.get("trend_window_sufficient")):
        growth = trend.get("memory_growth_bytes_per_minute")
        if growth is not None:
            if growth >= MEMORY_GROWTH_CRITICAL_BYTES_PER_MINUTE and (memory_fraction or 0.0) >= MEMORY_WARN_FRACTION:
                critical.append("sustained_memory_growth_critical")
            elif growth >= MEMORY_GROWTH_WARN_BYTES_PER_MINUTE and (memory_fraction or 0.0) >= 0.50:
                warnings.append("sustained_memory_growth_high")
        throttle = trend.get("cpu_throttle_fraction")
        if throttle is not None:
            if throttle >= CPU_THROTTLE_CRITICAL_FRACTION:
                critical.append("cpu_throttling_critical")
            elif throttle >= CPU_THROTTLE_WARN_FRACTION:
                warnings.append("cpu_throttling_high")

    state = "critical" if critical else "warning" if warnings else "healthy"
    if current is None and maximum is None and latest.get("cpu_usage_usec") is None:
        state = "unavailable"
        warnings.append("cgroup_metrics_unavailable")

    return {
        "resource_pressure_version": RESOURCE_PRESSURE_VERSION,
        "state": state,
        "sampled_at": latest.get("sampled_at"),
        "sample_count": len(samples),
        "memory": {
            "current_bytes": current,
            "max_bytes": maximum,
            "utilization_fraction": memory_fraction,
            "process_rss_bytes": latest.get("process_rss_bytes"),
            "events": events,
        },
        "cpu": {
            "limit_cores": latest.get("cpu_limit_cores"),
            "usage_usec": latest.get("cpu_usage_usec"),
            "throttled_usec": latest.get("cpu_throttled_usec"),
            "nr_throttled": latest.get("cpu_nr_throttled"),
            "load_average": latest.get("load_average"),
        },
        "trend": trend,
        "warnings": warnings,
        "critical": critical,
        "thresholds": {
            "memory_warn_fraction": MEMORY_WARN_FRACTION,
            "memory_critical_fraction": MEMORY_CRITICAL_FRACTION,
            "memory_growth_warn_bytes_per_minute": MEMORY_GROWTH_WARN_BYTES_PER_MINUTE,
            "memory_growth_critical_bytes_per_minute": MEMORY_GROWTH_CRITICAL_BYTES_PER_MINUTE,
            "cpu_throttle_warn_fraction": CPU_THROTTLE_WARN_FRACTION,
            "cpu_throttle_critical_fraction": CPU_THROTTLE_CRITICAL_FRACTION,
            "minimum_trend_window_seconds": MIN_TREND_WINDOW_SECONDS,
        },
        "read_only_observability": True,
        "changes_strategy_authority": False,
        "changes_economic_thresholds": False,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "RESOURCE_PRESSURE_VERSION",
    "ensure_resource_pressure_sampler",
    "resource_pressure_snapshot",
    "sample_resource_pressure",
]
