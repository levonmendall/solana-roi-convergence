from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from contextvars import ContextVar
from functools import wraps
from typing import Any, Awaitable, Callable

from . import robinhood_adaptive_lane_controller as adaptive
from . import robinhood_chain_runtime as runtime
from . import robinhood_provider_meter as meter


GUARD_VERSION = "robinhood-alchemy-budget-guard-v2-provider-pool-aware"
DEFAULT_TARGET_CU_PER_MINUTE = 600.0
DEFAULT_BURST_CU = 500.0
DEFAULT_BILLING_SAFETY_MULTIPLIER = 4.0
DEFAULT_MAX_NONCRITICAL_WAIT_SECONDS = 1.5
DEFAULT_ETH_CALL_CACHE_TTL_SECONDS = 0.75
DEFAULT_EMERGENCY_COOLDOWN_SECONDS = 10.0
DEFAULT_PROVIDER_POOL_LIVE_MARKET_CAP = 8
MAX_PROVIDER_POOL_LIVE_MARKET_CAP = 16

_INSTALLED = False
_ORIGINAL_RPC: Callable[..., Awaitable[Any]] | None = None
_ORIGINAL_CONTROL: Callable[..., int] | None = None
_PRIORITY: ContextVar[str] = ContextVar("robinhood_alchemy_priority", default="qualification")
_LOCK = threading.Lock()
_TOKENS = DEFAULT_BURST_CU
_LAST_REFILL = time.monotonic()
_STATS: dict[str, int | float | str | None] = {
    "network_eth_calls": 0,
    "cache_hits": 0,
    "singleflight_hits": 0,
    "budget_waits": 0,
    "budget_rejections": 0,
    "critical_bypasses": 0,
    "prospective_emergency_zeroes": 0,
    "provider_pool_capacity_decisions": 0,
    "provider_pool_failovers_requested": 0,
    "last_rejection_at": None,
}


class RobinhoodAlchemyBudgetExceeded(RuntimeError):
    """A noncritical production Alchemy read exceeded the provider budget."""


def _float_env(name: str, default: float, minimum: float = 0.0) -> float:
    raw = os.getenv(name)
    try:
        value = float(raw) if raw is not None else float(default)
    except (TypeError, ValueError):
        value = float(default)
    return max(minimum, value)


def _target_cu_per_minute() -> float:
    return _float_env("ROBINHOOD_ALCHEMY_HARD_TARGET_CU_PER_MINUTE", DEFAULT_TARGET_CU_PER_MINUTE, 1.0)


def _burst_cu() -> float:
    return _float_env("ROBINHOOD_ALCHEMY_HARD_BURST_CU", DEFAULT_BURST_CU, 1.0)


def _billing_safety_multiplier() -> float:
    return _float_env(
        "ROBINHOOD_ALCHEMY_BILLING_SAFETY_MULTIPLIER",
        DEFAULT_BILLING_SAFETY_MULTIPLIER,
        1.0,
    )


def _max_wait_seconds() -> float:
    return _float_env(
        "ROBINHOOD_ALCHEMY_MAX_NONCRITICAL_WAIT_SECONDS",
        DEFAULT_MAX_NONCRITICAL_WAIT_SECONDS,
        0.0,
    )


def _cache_ttl_seconds() -> float:
    return _float_env(
        "ROBINHOOD_ALCHEMY_ETH_CALL_CACHE_TTL_SECONDS",
        DEFAULT_ETH_CALL_CACHE_TTL_SECONDS,
        0.0,
    )


def _emergency_cooldown_seconds() -> float:
    return _float_env(
        "ROBINHOOD_ALCHEMY_EMERGENCY_COOLDOWN_SECONDS",
        DEFAULT_EMERGENCY_COOLDOWN_SECONDS,
        0.0,
    )


def _provider_pool_live_market_cap() -> int:
    raw = os.getenv("ROBINHOOD_PROVIDER_POOL_LIVE_MARKET_CAP")
    if raw is None:
        raw = str(DEFAULT_PROVIDER_POOL_LIVE_MARKET_CAP)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = DEFAULT_PROVIDER_POOL_LIVE_MARKET_CAP
    return max(1, min(MAX_PROVIDER_POOL_LIVE_MARKET_CAP, value))


def _normalized(value: str) -> str:
    return str(value or "").strip().rstrip("/").lower()


def _production_rpc_url(value: str) -> bool:
    configured = _normalized(os.getenv("ROBINHOOD_RPC_URL") or "")
    candidate = _normalized(value)
    public = _normalized(runtime.ROBINHOOD_PUBLIC_RPC)
    return bool(configured and candidate == configured and candidate != public)


def _provider_pool_capacity_mode() -> tuple[bool, bool]:
    """Return (pool_available, active_is_non_alchemy).

    The failover module imports this guard, so import it lazily.  A healthy active
    non-Alchemy provider is sufficient even while Alchemy itself is cooling down.
    When Alchemy is active, at least one eligible alternate private provider must
    exist before its provider-specific meter is allowed to stop governing candidate
    capacity.  Actual chain-id verification, HTTP/WSS generation switching and
    paper-readiness remain owned by the failover/transport layers.
    """
    try:
        from . import robinhood_provider_failover as failover

        active = failover.active_provider()
        items = tuple(failover.providers())
        if active is None:
            return False, False

        alchemy_http = _normalized(os.getenv("ROBINHOOD_RPC_URL") or "")
        active_is_non_alchemy = bool(alchemy_http and _normalized(active.http) != alchemy_http)
        if active_is_non_alchemy:
            return True, True

        now = time.monotonic()
        for item in items:
            if item.name == active.name:
                continue
            state = failover._PROVIDER_STATE.get(item.name, {})
            if float(state.get("cooldown_until", 0.0) or 0.0) <= now:
                return True, False
    except Exception:
        return False, False
    return False, False


def _request_provider_failover_for_alchemy_pressure() -> bool:
    """Move off Alchemy when its provider-specific meter is at the hard target."""
    try:
        from . import robinhood_provider_failover as failover

        active = failover.active_provider()
        if active is None or not _production_rpc_url(active.http):
            return False
        replacement = failover._switch_from(
            active.name,
            failure_type="AlchemyBudgetPressure",
            immediate=True,
            transport_kind="http",
        )
        if replacement is None or replacement.name == active.name:
            return False
        _bump("provider_pool_failovers_requested")
        return True
    except Exception:
        return False


def _estimated_billing_cu(method: str, params: list[Any]) -> float:
    byte_count = meter._json_size({"method": method, "params": params})
    local = meter._estimate_cu(byte_count=byte_count, base_cu=meter._http_base_cu())
    return max(1.0, local * _billing_safety_multiplier())


def _bump(name: str, amount: int = 1) -> None:
    with _LOCK:
        _STATS[name] = int(_STATS.get(name, 0) or 0) + int(amount)


def _refill_locked(now: float) -> None:
    global _TOKENS, _LAST_REFILL
    elapsed = max(0.0, now - _LAST_REFILL)
    refill_per_second = _target_cu_per_minute() / 60.0
    _TOKENS = min(_burst_cu(), float(_TOKENS) + elapsed * refill_per_second)
    _LAST_REFILL = now


def _try_consume(cost: float) -> bool:
    global _TOKENS
    now = time.monotonic()
    with _LOCK:
        _refill_locked(now)
        if _TOKENS + 1e-9 < cost:
            return False
        _TOKENS -= cost
        return True


async def _acquire_noncritical(cost: float) -> None:
    if _try_consume(cost):
        return
    _bump("budget_waits")
    deadline = time.monotonic() + _max_wait_seconds()
    while time.monotonic() < deadline:
        await asyncio.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        if _try_consume(cost):
            return
    _bump("budget_rejections")
    with _LOCK:
        _STATS["last_rejection_at"] = time.time()
    raise RobinhoodAlchemyBudgetExceeded("robinhood_alchemy_noncritical_provider_budget_exhausted")


def _cache_state(rpc_self: Any) -> tuple[dict[str, tuple[float, Any]], dict[str, asyncio.Task[Any]]]:
    cache = getattr(rpc_self, "_roi_alchemy_eth_call_cache", None)
    if not isinstance(cache, dict):
        cache = {}
        setattr(rpc_self, "_roi_alchemy_eth_call_cache", cache)
    inflight = getattr(rpc_self, "_roi_alchemy_eth_call_inflight", None)
    if not isinstance(inflight, dict):
        inflight = {}
        setattr(rpc_self, "_roi_alchemy_eth_call_inflight", inflight)
    return cache, inflight


def _request_key(method: str, params: list[Any]) -> str:
    try:
        return json.dumps([method, params], sort_keys=True, separators=(",", ":"), default=str)
    except Exception:
        return repr((method, params))


async def _perform_network_call(
    original: Callable[..., Awaitable[Any]],
    rpc_self: Any,
    method: str,
    params: list[Any],
) -> Any:
    if _PRIORITY.get() == "critical":
        _bump("critical_bypasses")
    else:
        await _acquire_noncritical(_estimated_billing_cu(method, params))
    result = await original(rpc_self, method, params)
    _bump("network_eth_calls")
    return result


def _guarded_rpc(original: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(rpc_self: Any, method: str, params: list[Any]) -> Any:
        production = _production_rpc_url(getattr(rpc_self, "rpc_url", ""))
        if not production or method != "eth_call":
            return await original(rpc_self, method, params)

        ttl = _cache_ttl_seconds()
        key = _request_key(method, params)
        cache, inflight = _cache_state(rpc_self)
        now = time.monotonic()
        cached = cache.get(key)
        if cached is not None and now - float(cached[0]) <= ttl:
            _bump("cache_hits")
            return cached[1]

        existing = inflight.get(key)
        if existing is not None and not existing.done():
            _bump("singleflight_hits")
            return await asyncio.shield(existing)

        async def execute() -> Any:
            result = await _perform_network_call(original, rpc_self, method, params)
            cache[key] = (time.monotonic(), result)
            if len(cache) > 512:
                cutoff = time.monotonic() - max(1.0, ttl * 4.0)
                for cache_key, item in list(cache.items()):
                    if float(item[0]) < cutoff:
                        cache.pop(cache_key, None)
                while len(cache) > 512:
                    cache.pop(next(iter(cache)))
            return result

        task = asyncio.create_task(execute(), name="robinhood-alchemy-eth-call")
        inflight[key] = task
        try:
            return await asyncio.shield(task)
        finally:
            if inflight.get(key) is task and task.done():
                inflight.pop(key, None)

    setattr(wrapped, "_roi_alchemy_budget_guard_rpc", True)
    return wrapped


def _guarded_control(self: Any, *, demand: int, open_positions: int) -> int:
    """Protect Alchemy without making its budget a Robinhood opportunity bottleneck."""
    if _ORIGINAL_CONTROL is None:
        return 0

    short_rate, long_rate, effective = adaptive._rates()
    target = adaptive._target_cu_per_minute()
    state = adaptive._state(self)
    now = time.monotonic()
    pool_available, active_is_non_alchemy = _provider_pool_capacity_mode()

    if pool_available:
        failover_requested = False
        if effective >= target and not active_is_non_alchemy:
            failover_requested = _request_provider_failover_for_alchemy_pressure()

        total_cap = _provider_pool_live_market_cap()
        prospective_cap = max(0, total_cap - max(0, int(open_positions)))
        desired = min(max(0, int(demand)), prospective_cap)
        previous = int(state.get("prospective_lane_cap", 0) or 0)
        state["last_control_monotonic"] = now
        state["short_estimated_cu_per_minute"] = short_rate
        state["long_estimated_cu_per_minute"] = long_rate
        state["effective_estimated_cu_per_minute"] = effective
        state["estimated_headroom_cu_per_minute"] = max(0.0, target - effective)
        state["ranked_demand"] = max(0, int(demand))
        state["open_position_count"] = max(0, int(open_positions))
        state["prospective_lane_cap"] = desired
        state["provider_pool_total_live_market_cap"] = total_cap
        state["provider_pool_capacity_mode"] = True
        if desired != previous:
            state["last_change_monotonic"] = now
        state["last_change_reason"] = (
            "provider_pool_alchemy_pressure_failover" if failover_requested else "provider_pool_capacity"
        )
        setattr(self, "_roi_alchemy_emergency_until", 0.0)
        _bump("provider_pool_capacity_decisions")
        return desired

    state["provider_pool_capacity_mode"] = False
    emergency_until = float(getattr(self, "_roi_alchemy_emergency_until", 0.0) or 0.0)
    if effective >= target:
        setattr(self, "_roi_alchemy_emergency_until", now + _emergency_cooldown_seconds())
        state["last_control_monotonic"] = now
        state["short_estimated_cu_per_minute"] = short_rate
        state["long_estimated_cu_per_minute"] = long_rate
        state["effective_estimated_cu_per_minute"] = effective
        state["estimated_headroom_cu_per_minute"] = 0.0
        state["ranked_demand"] = max(0, int(demand))
        state["open_position_count"] = max(0, int(open_positions))
        state["prospective_lane_cap"] = 0
        state["last_change_monotonic"] = now
        state["last_change_reason"] = "provider_budget_emergency"
        _bump("prospective_emergency_zeroes")
        return 0

    if now < emergency_until:
        state["prospective_lane_cap"] = 0
        state["last_change_reason"] = "provider_budget_emergency_cooldown"
        return 0

    return _ORIGINAL_CONTROL(self, demand=demand, open_positions=open_positions)


def _critical_settlement_wrapper(original: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(self: Any, *args: Any, **kwargs: Any) -> Any:
        token = _PRIORITY.set("critical")
        try:
            return await original(self, *args, **kwargs)
        finally:
            _PRIORITY.reset(token)

    setattr(wrapped, "_roi_alchemy_critical_settlement", True)
    return wrapped


def install_robinhood_alchemy_budget_guard(plane_cls: type[Any]) -> None:
    global _INSTALLED, _ORIGINAL_RPC, _ORIGINAL_CONTROL
    if _INSTALLED:
        return

    _ORIGINAL_RPC = runtime.RobinhoodRpc.rpc
    if not bool(getattr(runtime.RobinhoodRpc.rpc, "_roi_alchemy_budget_guard_rpc", False)):
        runtime.RobinhoodRpc.rpc = _guarded_rpc(runtime.RobinhoodRpc.rpc)  # type: ignore[method-assign]

    _ORIGINAL_CONTROL = adaptive._control
    adaptive._control = _guarded_control

    settle_one = getattr(plane_cls, "_settle_one", None)
    if settle_one is not None and not bool(getattr(settle_one, "_roi_alchemy_critical_settlement", False)):
        plane_cls._settle_one = _critical_settlement_wrapper(settle_one)  # type: ignore[attr-defined]

    setattr(plane_cls, "_roi_robinhood_alchemy_budget_guard_version", GUARD_VERSION)
    _INSTALLED = True


def status() -> dict[str, Any]:
    with _LOCK:
        now = time.monotonic()
        _refill_locked(now)
        stats = dict(_STATS)
        tokens = float(_TOKENS)
    pool_available, active_is_non_alchemy = _provider_pool_capacity_mode()
    return {
        "version": GUARD_VERSION,
        "installed": _INSTALLED,
        "hard_target_cu_per_minute": _target_cu_per_minute(),
        "burst_cu": _burst_cu(),
        "billing_safety_multiplier": _billing_safety_multiplier(),
        "max_noncritical_wait_seconds": _max_wait_seconds(),
        "eth_call_cache_ttl_seconds": _cache_ttl_seconds(),
        "available_budget_tokens": tokens,
        "provider_pool_live_market_cap": _provider_pool_live_market_cap(),
        "provider_pool_capacity_available": pool_available,
        "active_provider_is_non_alchemy": active_is_non_alchemy,
        "critical_open_position_settlement_bypasses_budget": True,
        "factory_market_discovery_constrained": False,
        "prospective_live_market_emergency_zero_supported": True,
        "alchemy_budget_restricts_robinhood_when_pool_available": False,
        "public_research_rpc_governed": False,
        "strategy_authority_changed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
        "stats": stats,
    }


def reset_for_tests() -> None:
    global _TOKENS, _LAST_REFILL
    with _LOCK:
        _TOKENS = _burst_cu()
        _LAST_REFILL = time.monotonic()
        for key in list(_STATS):
            _STATS[key] = None if key == "last_rejection_at" else 0


__all__ = [
    "GUARD_VERSION",
    "RobinhoodAlchemyBudgetExceeded",
    "install_robinhood_alchemy_budget_guard",
    "status",
    "reset_for_tests",
]
