from __future__ import annotations

import json
import os
import threading
import time
from collections import deque
from typing import Any


METER_VERSION = "robinhood-provider-meter-v1"
_DEFAULT_CU_PER_KIB = 40.0
_DEFAULT_HTTP_BASE_CU = 10.0
_DEFAULT_WS_CONTROL_BASE_CU = 10.0
_DEFAULT_SAFETY_FACTOR = 2.0
_MAX_WINDOW_SECONDS = 300.0

_LOCK = threading.Lock()
_EVENTS: deque[tuple[float, float, str]] = deque(maxlen=200_000)


def _float_env(name: str, default: float, minimum: float = 0.0) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    return max(minimum, value)


def _cu_per_kib() -> float:
    return _float_env("ROBINHOOD_ALCHEMY_EST_CU_PER_KIB", _DEFAULT_CU_PER_KIB)


def _http_base_cu() -> float:
    return _float_env("ROBINHOOD_ALCHEMY_HTTP_BASE_CU", _DEFAULT_HTTP_BASE_CU)


def _ws_control_base_cu() -> float:
    return _float_env("ROBINHOOD_ALCHEMY_WS_CONTROL_BASE_CU", _DEFAULT_WS_CONTROL_BASE_CU)


def _safety_factor() -> float:
    return _float_env("ROBINHOOD_ALCHEMY_USAGE_SAFETY_FACTOR", _DEFAULT_SAFETY_FACTOR, 1.0)


def _json_size(value: Any) -> int:
    try:
        return len(json.dumps(value, separators=(",", ":"), default=str).encode("utf-8"))
    except Exception:
        return 0


def _estimate_cu(*, byte_count: int, base_cu: float = 0.0) -> float:
    raw = max(0.0, float(base_cu)) + (max(0, int(byte_count)) / 1024.0) * _cu_per_kib()
    return raw * _safety_factor()


def _record(cu: float, kind: str) -> None:
    now = time.monotonic()
    with _LOCK:
        _EVENTS.append((now, max(0.0, float(cu)), str(kind)))
        cutoff = now - _MAX_WINDOW_SECONDS
        while _EVENTS and _EVENTS[0][0] < cutoff:
            _EVENTS.popleft()


def record_ws_log(log: dict[str, Any]) -> None:
    _record(_estimate_cu(byte_count=_json_size(log)), "ws_log")


def record_ws_control(method: str, params: list[Any], result: Any | None = None) -> None:
    size = _json_size({"method": method, "params": params}) + _json_size(result)
    _record(_estimate_cu(byte_count=size, base_cu=_ws_control_base_cu()), "ws_control")


def record_http_request(method: str, params: list[Any]) -> None:
    size = _json_size({"method": method, "params": params})
    _record(_estimate_cu(byte_count=size, base_cu=_http_base_cu()), "http_request")


def record_http_response(result: Any) -> None:
    _record(_estimate_cu(byte_count=_json_size(result)), "http_response")


def snapshot(window_seconds: float = 60.0) -> dict[str, Any]:
    window = max(1.0, min(_MAX_WINDOW_SECONDS, float(window_seconds)))
    now = time.monotonic()
    cutoff = now - window
    totals: dict[str, float] = {}
    count = 0
    estimated = 0.0
    with _LOCK:
        events = list(_EVENTS)
    for at, cu, kind in events:
        if at < cutoff:
            continue
        count += 1
        estimated += cu
        totals[kind] = totals.get(kind, 0.0) + cu
    annualized = estimated * (60.0 / window)
    return {
        "window_seconds": window,
        "event_count": count,
        "estimated_cu": estimated,
        "estimated_cu_per_minute": annualized,
        "by_kind_estimated_cu": totals,
        "estimator": {
            "cu_per_kib": _cu_per_kib(),
            "http_base_cu": _http_base_cu(),
            "ws_control_base_cu": _ws_control_base_cu(),
            "safety_factor": _safety_factor(),
            "billing_authority": False,
            "purpose": "conservative local provider-load controller",
        },
    }


def reset_for_tests() -> None:
    with _LOCK:
        _EVENTS.clear()


__all__ = [
    "METER_VERSION",
    "record_ws_log",
    "record_ws_control",
    "record_http_request",
    "record_http_response",
    "snapshot",
    "reset_for_tests",
]
