from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from collections import deque
from contextvars import ContextVar, Token
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse

import httpx

from . import robinhood_alchemy_budget_guard as alchemy_guard
from . import robinhood_chain_runtime as runtime
from . import robinhood_production_ws_transport as transport
from . import robinhood_usage_bounded_transport as bounded


CAPACITY_BUDGET_VERSION = "robinhood-provider-capacity-budget-v1-chainstack-monthly"
DEFAULT_COMBINED_MONTHLY_REQUEST_LIMIT = 6_000_000
DEFAULT_CHAINSTACK_MONTHLY_REQUEST_LIMIT = 3_000_000
DEFAULT_ALCHEMY_MONTHLY_REQUEST_LIMIT = 3_000_000
DEFAULT_CHAINSTACK_ROLLING_RPS_LIMIT = 20
DEFAULT_CRITICAL_RESERVE_FRACTION = 0.10
DEFAULT_BACKGROUND_SHARE = 0.65
DEFAULT_PERSIST_EVERY_REQUESTS = 50
DEFAULT_PERSIST_EVERY_SECONDS = 5.0
DEFAULT_RESTART_SAFETY_REQUESTS = 100
DEFAULT_STATE_PATH = "/var/data/robinhood-provider-monthly-usage.json"
DEFAULT_429_BACKOFF_SECONDS = 0.25
MAX_429_BACKOFF_SECONDS = 2.0

_INSTALLED = False
_ORIGINAL_RPC: Callable[..., Awaitable[Any]] | None = None
_ORIGINAL_WS_RPC: Callable[..., Awaitable[Any]] | None = None
_PRIORITY: ContextVar[str] = ContextVar("robinhood_provider_capacity_priority", default="qualification")
_LOCK = threading.RLock()
_CHAINSTACK_REQUEST_TIMES: deque[float] = deque()
_NEXT_BACKGROUND_AT: dict[str, float] = {}
_STATE: dict[str, Any] | None = None
_STATE_LOADED = False
_DIRTY_REQUESTS = 0
_LAST_PERSIST_MONOTONIC = 0.0
_STATS: dict[str, Any] = {
    "chainstack_429s": 0,
    "chainstack_5xx": 0,
    "provider_http_errors": 0,
    "background_pacing_waits": 0,
    "background_pacing_wait_seconds": 0.0,
    "monthly_budget_rejections": 0,
    "monthly_background_deferrals": 0,
    "persistence_errors": 0,
    "last_http_status": None,
    "last_http_error_provider_kind": None,
    "last_http_error_method": None,
}


class RobinhoodProviderMonthlyBudgetExceeded(RuntimeError):
    """A private provider's configured calendar-month request allowance is exhausted."""


class RobinhoodProviderBackgroundBudgetDeferred(RuntimeError):
    """Background research is deferred to preserve monthly capacity for decisions/exits."""


def _int_env(name: str, default: int, minimum: int = 0) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw is not None else int(default)
    except (TypeError, ValueError):
        value = int(default)
    return max(minimum, value)


def _float_env(name: str, default: float, minimum: float = 0.0, maximum: float | None = None) -> float:
    raw = os.getenv(name)
    try:
        value = float(raw) if raw is not None else float(default)
    except (TypeError, ValueError):
        value = float(default)
    value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _state_path() -> Path:
    return Path(os.getenv("ROBINHOOD_PROVIDER_MONTHLY_USAGE_PATH", DEFAULT_STATE_PATH))


def _provider_kind_from_url(value: str) -> str:
    try:
        host = (urlparse(str(value or "")).hostname or "").lower()
    except Exception:
        return "unknown"
    if "chainstack" in host:
        return "chainstack"
    if "alchemy" in host:
        return "alchemy"
    if "drpc" in host:
        return "drpc"
    if host == (urlparse(runtime.ROBINHOOD_PUBLIC_RPC).hostname or "").lower():
        return "public_rpc"
    return "private_rpc" if host else "unknown"


def _month_key(now: datetime | None = None) -> str:
    current = now or datetime.now(timezone.utc)
    return current.astimezone(timezone.utc).strftime("%Y-%m")


def _seconds_remaining_in_month(now: datetime | None = None) -> float:
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if current.month == 12:
        nxt = datetime(current.year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        nxt = datetime(current.year, current.month + 1, 1, tzinfo=timezone.utc)
    return max(1.0, (nxt - current).total_seconds())


def _provider_limit(kind: str) -> int:
    if kind == "chainstack":
        return _int_env(
            "ROBINHOOD_CHAINSTACK_MONTHLY_REQUEST_LIMIT",
            DEFAULT_CHAINSTACK_MONTHLY_REQUEST_LIMIT,
            1,
        )
    if kind == "alchemy":
        return _int_env(
            "ROBINHOOD_ALCHEMY_MONTHLY_REQUEST_LIMIT",
            DEFAULT_ALCHEMY_MONTHLY_REQUEST_LIMIT,
            1,
        )
    return _int_env("ROBINHOOD_OTHER_PRIVATE_MONTHLY_REQUEST_LIMIT", 0, 0)


def _combined_limit() -> int:
    return _int_env(
        "ROBINHOOD_PROVIDER_COMBINED_MONTHLY_REQUEST_LIMIT",
        DEFAULT_COMBINED_MONTHLY_REQUEST_LIMIT,
        1,
    )


def _critical_reserve_fraction() -> float:
    return _float_env(
        "ROBINHOOD_PROVIDER_MONTHLY_CRITICAL_RESERVE_FRACTION",
        DEFAULT_CRITICAL_RESERVE_FRACTION,
        0.0,
        0.50,
    )


def _background_share() -> float:
    return _float_env(
        "ROBINHOOD_PROVIDER_BACKGROUND_SHARE",
        DEFAULT_BACKGROUND_SHARE,
        0.05,
        0.95,
    )


def _baseline_for_month(month: str) -> dict[str, int]:
    raw = (os.getenv("ROBINHOOD_PROVIDER_MONTHLY_USAGE_BASELINES_JSON") or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    selected = payload.get(month)
    if not isinstance(selected, dict):
        return {}
    result: dict[str, int] = {}
    for kind in ("chainstack", "alchemy"):
        try:
            value = int(selected.get(kind, 0) or 0)
        except (TypeError, ValueError):
            value = 0
        result[kind] = max(0, value)
    return result


def _fresh_state(month: str) -> dict[str, Any]:
    baseline = _baseline_for_month(month)
    return {
        "version": 1,
        "month": month,
        "providers": {
            "chainstack": int(baseline.get("chainstack", 0)),
            "alchemy": int(baseline.get("alchemy", 0)),
        },
        "http_requests": {"chainstack": 0, "alchemy": 0},
        "ws_control_requests": {"chainstack": 0, "alchemy": 0},
        "baseline_requests": {
            "chainstack": int(baseline.get("chainstack", 0)),
            "alchemy": int(baseline.get("alchemy", 0)),
        },
        "updated_at": time.time(),
    }


def _sanitize_state(payload: Any, month: str) -> dict[str, Any]:
    if not isinstance(payload, dict) or str(payload.get("month") or "") != month:
        return _fresh_state(month)
    result = _fresh_state(month)
    for bucket in ("providers", "http_requests", "ws_control_requests", "baseline_requests"):
        source = payload.get(bucket)
        if not isinstance(source, dict):
            continue
        for kind in ("chainstack", "alchemy"):
            try:
                value = int(source.get(kind, result[bucket][kind]) or 0)
            except (TypeError, ValueError):
                value = int(result[bucket][kind])
            result[bucket][kind] = max(0, value)
    result["updated_at"] = float(payload.get("updated_at") or time.time())
    return result


def _load_state_locked() -> dict[str, Any]:
    global _STATE, _STATE_LOADED, _DIRTY_REQUESTS, _LAST_PERSIST_MONOTONIC
    month = _month_key()
    if _STATE_LOADED and isinstance(_STATE, dict) and _STATE.get("month") == month:
        return _STATE
    payload: Any = None
    path = _state_path()
    try:
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        _STATS["persistence_errors"] = int(_STATS.get("persistence_errors", 0) or 0) + 1
    state = _sanitize_state(payload, month)
    if isinstance(payload, dict) and str(payload.get("month") or "") == month:
        safety = _int_env(
            "ROBINHOOD_PROVIDER_RESTART_SAFETY_REQUESTS",
            DEFAULT_RESTART_SAFETY_REQUESTS,
            0,
        )
        for kind in ("chainstack", "alchemy"):
            if int(state["providers"].get(kind, 0) or 0) > 0:
                state["providers"][kind] = int(state["providers"][kind]) + safety
    _STATE = state
    _STATE_LOADED = True
    _DIRTY_REQUESTS = 0
    _LAST_PERSIST_MONOTONIC = time.monotonic()
    return state


def _persist_locked(force: bool = False) -> None:
    global _DIRTY_REQUESTS, _LAST_PERSIST_MONOTONIC
    if _STATE is None:
        return
    threshold = _int_env(
        "ROBINHOOD_PROVIDER_USAGE_PERSIST_EVERY_REQUESTS",
        DEFAULT_PERSIST_EVERY_REQUESTS,
        1,
    )
    seconds = _float_env(
        "ROBINHOOD_PROVIDER_USAGE_PERSIST_EVERY_SECONDS",
        DEFAULT_PERSIST_EVERY_SECONDS,
        0.1,
    )
    now = time.monotonic()
    if not force and _DIRTY_REQUESTS < threshold and now - _LAST_PERSIST_MONOTONIC < seconds:
        return
    path = _state_path()
    temp = path.with_suffix(path.suffix + ".tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(_STATE, sort_keys=True, separators=(",", ":"))
        temp.write_text(payload, encoding="utf-8")
        os.replace(temp, path)
        _DIRTY_REQUESTS = 0
        _LAST_PERSIST_MONOTONIC = now
    except Exception:
        _STATS["persistence_errors"] = int(_STATS.get("persistence_errors", 0) or 0) + 1
        try:
            temp.unlink(missing_ok=True)
        except Exception:
            pass


def _usage_locked(kind: str) -> tuple[int, int]:
    state = _load_state_locked()
    provider = int(state["providers"].get(kind, 0) or 0)
    combined = sum(int(state["providers"].get(item, 0) or 0) for item in ("chainstack", "alchemy"))
    return provider, combined


def _priority_kind() -> str:
    try:
        if alchemy_guard._PRIORITY.get() == "critical":
            return "critical"
    except Exception:
        pass
    return str(_PRIORITY.get() or "qualification")


def set_priority(value: str) -> Token[str]:
    return _PRIORITY.set(str(value or "qualification"))


def reset_priority(token: Token[str]) -> None:
    _PRIORITY.reset(token)


def _monthly_remaining_locked(kind: str) -> tuple[int, int, int, int]:
    provider_used, combined_used = _usage_locked(kind)
    provider_limit = _provider_limit(kind)
    combined_limit = _combined_limit()
    return (
        max(0, provider_limit - provider_used),
        max(0, combined_limit - combined_used),
        provider_limit,
        combined_limit,
    )


def _background_target_rps_locked(kind: str) -> float:
    provider_remaining, combined_remaining, provider_limit, combined_limit = _monthly_remaining_locked(kind)
    reserve = _critical_reserve_fraction()
    state = _load_state_locked()
    provider_used = int(state["providers"].get(kind, 0) or 0)
    combined_used = sum(int(state["providers"].get(item, 0) or 0) for item in ("chainstack", "alchemy"))
    provider_usable_remaining = max(0.0, provider_limit * (1.0 - reserve) - provider_used)
    combined_usable_remaining = max(0.0, combined_limit * (1.0 - reserve) - combined_used)
    usable_remaining = min(
        float(provider_remaining),
        float(combined_remaining),
        provider_usable_remaining,
        combined_usable_remaining,
    )
    if usable_remaining <= 0:
        return 0.0
    return max(0.0, usable_remaining / _seconds_remaining_in_month() * _background_share())


async def _pace_background(kind: str) -> None:
    while True:
        with _LOCK:
            target_rps = _background_target_rps_locked(kind)
            if target_rps <= 0:
                _STATS["monthly_background_deferrals"] = int(
                    _STATS.get("monthly_background_deferrals", 0) or 0
                ) + 1
                raise RobinhoodProviderBackgroundBudgetDeferred(
                    "robinhood provider background monthly budget deferred"
                )
            interval = 1.0 / max(0.01, target_rps)
            now = time.monotonic()
            next_at = max(now, float(_NEXT_BACKGROUND_AT.get(kind, now)))
            _NEXT_BACKGROUND_AT[kind] = next_at + interval
            delay = max(0.0, next_at - now)
        if delay <= 0:
            return
        with _LOCK:
            _STATS["background_pacing_waits"] = int(_STATS.get("background_pacing_waits", 0) or 0) + 1
            _STATS["background_pacing_wait_seconds"] = float(
                _STATS.get("background_pacing_wait_seconds", 0.0) or 0.0
            ) + delay
        await asyncio.sleep(min(delay, 1.0))


async def _chainstack_rolling_limit() -> None:
    limit = _int_env(
        "ROBINHOOD_CHAINSTACK_ROLLING_RPS_LIMIT",
        DEFAULT_CHAINSTACK_ROLLING_RPS_LIMIT,
        1,
    )
    while True:
        with _LOCK:
            now = time.monotonic()
            cutoff = now - 1.0
            while _CHAINSTACK_REQUEST_TIMES and _CHAINSTACK_REQUEST_TIMES[0] <= cutoff:
                _CHAINSTACK_REQUEST_TIMES.popleft()
            if len(_CHAINSTACK_REQUEST_TIMES) < limit:
                _CHAINSTACK_REQUEST_TIMES.append(now)
                return
            delay = max(0.001, _CHAINSTACK_REQUEST_TIMES[0] + 1.0 - now)
        await asyncio.sleep(min(delay, 0.25))


def _reserve_monthly_request(kind: str, transport_kind: str) -> None:
    global _DIRTY_REQUESTS
    if kind not in {"chainstack", "alchemy"}:
        return
    priority = _priority_kind()
    with _LOCK:
        state = _load_state_locked()
        provider_used, combined_used = _usage_locked(kind)
        provider_limit = _provider_limit(kind)
        combined_limit = _combined_limit()
        reserve = _critical_reserve_fraction()
        if provider_used >= provider_limit or combined_used >= combined_limit:
            _STATS["monthly_budget_rejections"] = int(_STATS.get("monthly_budget_rejections", 0) or 0) + 1
            raise RobinhoodProviderMonthlyBudgetExceeded("robinhood provider monthly quota exceeded")
        if priority != "critical":
            provider_soft_limit = int(provider_limit * (1.0 - reserve))
            combined_soft_limit = int(combined_limit * (1.0 - reserve))
            if provider_used >= provider_soft_limit or combined_used >= combined_soft_limit:
                _STATS["monthly_budget_rejections"] = int(_STATS.get("monthly_budget_rejections", 0) or 0) + 1
                raise RobinhoodProviderMonthlyBudgetExceeded(
                    "robinhood provider monthly quota exceeded; critical reserve preserved"
                )
        state["providers"][kind] = int(state["providers"].get(kind, 0) or 0) + 1
        bucket = "ws_control_requests" if transport_kind == "ws_control" else "http_requests"
        state[bucket][kind] = int(state[bucket].get(kind, 0) or 0) + 1
        state["updated_at"] = time.time()
        _DIRTY_REQUESTS += 1
        _persist_locked()


async def _before_request(url: str, transport_kind: str) -> str:
    kind = _provider_kind_from_url(url)
    if kind not in {"chainstack", "alchemy"}:
        return kind
    if _priority_kind() == "background":
        await _pace_background(kind)
    if kind == "chainstack":
        await _chainstack_rolling_limit()
    _reserve_monthly_request(kind, transport_kind)
    return kind


def _retry_after_seconds(exc: httpx.HTTPStatusError) -> float:
    raw = str(exc.response.headers.get("Retry-After") or "").strip()
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = DEFAULT_429_BACKOFF_SECONDS
    return min(MAX_429_BACKOFF_SECONDS, max(0.05, value))


def _record_http_error(kind: str, method: str, exc: httpx.HTTPStatusError) -> None:
    status = int(exc.response.status_code)
    with _LOCK:
        _STATS["provider_http_errors"] = int(_STATS.get("provider_http_errors", 0) or 0) + 1
        _STATS["last_http_status"] = status
        _STATS["last_http_error_provider_kind"] = kind
        _STATS["last_http_error_method"] = str(method)
        if kind == "chainstack" and status == 429:
            _STATS["chainstack_429s"] = int(_STATS.get("chainstack_429s", 0) or 0) + 1
        if kind == "chainstack" and status >= 500:
            _STATS["chainstack_5xx"] = int(_STATS.get("chainstack_5xx", 0) or 0) + 1
    print(
        "ROBINHOOD_PROVIDER_HTTP_STATUS "
        f"provider_kind={kind} method={method} http_status={status}",
        flush=True,
    )


def _guarded_rpc(original: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(rpc_self: Any, method: str, params: list[Any]) -> Any:
        url = str(getattr(rpc_self, "rpc_url", "") or "")
        kind = await _before_request(url, "http")
        try:
            return await original(rpc_self, method, params)
        except httpx.HTTPStatusError as exc:
            _record_http_error(kind, method, exc)
            if kind == "chainstack" and int(exc.response.status_code) == 429:
                await asyncio.sleep(_retry_after_seconds(exc))
                await _before_request(str(getattr(rpc_self, "rpc_url", "") or url), "http")
                try:
                    return await original(rpc_self, method, params)
                except httpx.HTTPStatusError as retry_exc:
                    _record_http_error(kind, method, retry_exc)
                    raise
            raise

    setattr(wrapped, "_roi_robinhood_provider_capacity_budget_rpc", True)
    return wrapped


def _guarded_ws_rpc(original: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(ws: Any, request_id: int, method: str, params: list[Any]) -> Any:
        await _before_request(str(transport._ws_url() or ""), "ws_control")
        return await original(ws, request_id, method, params)

    setattr(wrapped, "_roi_robinhood_provider_capacity_budget_ws", True)
    return wrapped


def install_robinhood_provider_capacity_budget() -> None:
    global _INSTALLED, _ORIGINAL_RPC, _ORIGINAL_WS_RPC
    if _INSTALLED:
        return
    _ORIGINAL_RPC = runtime.RobinhoodRpc.rpc
    if not bool(getattr(runtime.RobinhoodRpc.rpc, "_roi_robinhood_provider_capacity_budget_rpc", False)):
        runtime.RobinhoodRpc.rpc = _guarded_rpc(runtime.RobinhoodRpc.rpc)  # type: ignore[method-assign]
    _ORIGINAL_WS_RPC = bounded._rpc_request
    if not bool(getattr(bounded._rpc_request, "_roi_robinhood_provider_capacity_budget_ws", False)):
        bounded._rpc_request = _guarded_ws_rpc(bounded._rpc_request)
    with _LOCK:
        _load_state_locked()
        _persist_locked(force=True)
    _INSTALLED = True


def status() -> dict[str, Any]:
    with _LOCK:
        state = _load_state_locked()
        providers = {kind: int(state["providers"].get(kind, 0) or 0) for kind in ("chainstack", "alchemy")}
        combined = sum(providers.values())
        provider_limits = {
            "chainstack": _provider_limit("chainstack"),
            "alchemy": _provider_limit("alchemy"),
        }
        return {
            "version": CAPACITY_BUDGET_VERSION,
            "installed": _INSTALLED,
            "month_utc": state["month"],
            "provider_monthly_request_limits": provider_limits,
            "combined_monthly_request_limit": _combined_limit(),
            "provider_month_to_date_requests": providers,
            "combined_month_to_date_requests": combined,
            "provider_remaining_requests": {
                kind: max(0, provider_limits[kind] - providers[kind]) for kind in providers
            },
            "combined_remaining_requests": max(0, _combined_limit() - combined),
            "http_requests_since_accounting_start": dict(state["http_requests"]),
            "ws_control_requests_since_accounting_start": dict(state["ws_control_requests"]),
            "baseline_requests": dict(state["baseline_requests"]),
            "critical_reserve_fraction": _critical_reserve_fraction(),
            "background_share": _background_share(),
            "background_target_rps": {
                kind: round(_background_target_rps_locked(kind), 6) for kind in ("chainstack", "alchemy")
            },
            "chainstack_rolling_rps_limit": _int_env(
                "ROBINHOOD_CHAINSTACK_ROLLING_RPS_LIMIT",
                DEFAULT_CHAINSTACK_ROLLING_RPS_LIMIT,
                1,
            ),
            "durable_usage_path_configured": bool(str(_state_path())),
            "stats": dict(_STATS),
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }


def reset_for_tests() -> None:
    global _STATE, _STATE_LOADED, _DIRTY_REQUESTS, _LAST_PERSIST_MONOTONIC, _INSTALLED
    with _LOCK:
        _STATE = None
        _STATE_LOADED = False
        _DIRTY_REQUESTS = 0
        _LAST_PERSIST_MONOTONIC = 0.0
        _CHAINSTACK_REQUEST_TIMES.clear()
        _NEXT_BACKGROUND_AT.clear()
        for key in list(_STATS):
            if key in {"last_http_status", "last_http_error_provider_kind", "last_http_error_method"}:
                _STATS[key] = None
            elif key == "background_pacing_wait_seconds":
                _STATS[key] = 0.0
            else:
                _STATS[key] = 0
        _INSTALLED = False


__all__ = [
    "CAPACITY_BUDGET_VERSION",
    "RobinhoodProviderBackgroundBudgetDeferred",
    "RobinhoodProviderMonthlyBudgetExceeded",
    "install_robinhood_provider_capacity_budget",
    "reset_for_tests",
    "reset_priority",
    "set_priority",
    "status",
]
