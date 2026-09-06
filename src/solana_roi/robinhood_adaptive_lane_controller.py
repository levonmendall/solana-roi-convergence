from __future__ import annotations

import os
import time
from functools import wraps
from typing import Any, Awaitable, Callable

from . import robinhood_provider_budget_transport as budget
from . import robinhood_provider_meter as meter
from . import robinhood_production_ws_transport as transport
from . import robinhood_usage_bounded_transport as bounded
from . import robinhood_chain_runtime as runtime


CONTROLLER_VERSION = "robinhood-adaptive-alchemy-lanes-v1"
DEFAULT_MIN_PROSPECTIVE_LANES = 1
DEFAULT_MAX_PROSPECTIVE_LANES = 4
DEFAULT_TARGET_CU_PER_MINUTE = 600.0
CONTROL_INTERVAL_SECONDS = 1.0
EXPANSION_COOLDOWN_SECONDS = 3.0
SHORT_WINDOW_SECONDS = 10.0
LONG_WINDOW_SECONDS = 60.0
EXPANSION_HEADROOM_FRACTION = 0.75
SOFT_CONTRACTION_FRACTION = 0.75
HARD_CONTRACTION_FRACTION = 0.90
EMERGENCY_FRACTION = 1.00
MIN_PROJECTED_MARGINAL_CU_PER_MINUTE = 50.0

_INSTALLED = False
_ORIGINAL_SELECTED_TARGETS: Callable[[Any], tuple[dict[str, int], dict[str, str]]] | None = None
_ORIGINAL_RPC: Callable[..., Awaitable[Any]] | None = None
_ORIGINAL_ENQUEUE: Callable[..., None] | None = None
_ORIGINAL_WS_RPC_REQUEST: Callable[..., Awaitable[Any]] | None = None


def _int_env(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw is not None else int(default)
    except (TypeError, ValueError):
        value = int(default)
    return max(minimum, min(maximum, value))


def _float_env(name: str, default: float, minimum: float = 0.0) -> float:
    raw = os.getenv(name)
    try:
        value = float(raw) if raw is not None else float(default)
    except (TypeError, ValueError):
        value = float(default)
    return max(minimum, value)


def _min_lanes() -> int:
    return _int_env(
        "ROBINHOOD_ALCHEMY_ADAPTIVE_MIN_LANES",
        DEFAULT_MIN_PROSPECTIVE_LANES,
        0,
        DEFAULT_MAX_PROSPECTIVE_LANES,
    )


def _max_lanes() -> int:
    return _int_env(
        "ROBINHOOD_ALCHEMY_ADAPTIVE_MAX_LANES",
        DEFAULT_MAX_PROSPECTIVE_LANES,
        max(1, _min_lanes()),
        DEFAULT_MAX_PROSPECTIVE_LANES,
    )


def _target_cu_per_minute() -> float:
    return _float_env("ROBINHOOD_ALCHEMY_TARGET_CU_PER_MINUTE", DEFAULT_TARGET_CU_PER_MINUTE, 1.0)


def _state(self: Any) -> dict[str, Any]:
    value = getattr(self, "_roi_adaptive_lane_state", None)
    if not isinstance(value, dict):
        value = {
            "prospective_lane_cap": max(1, _min_lanes()),
            "last_control_monotonic": 0.0,
            "last_change_monotonic": 0.0,
            "last_change_reason": "startup_safe_minimum",
            "short_estimated_cu_per_minute": 0.0,
            "long_estimated_cu_per_minute": 0.0,
            "effective_estimated_cu_per_minute": 0.0,
            "estimated_headroom_cu_per_minute": _target_cu_per_minute(),
            "ranked_demand": 0,
            "open_position_count": 0,
            "projected_next_lane_cu_per_minute": 0.0,
        }
        setattr(self, "_roi_adaptive_lane_state", value)
    return value


def _production_rpc_url(value: str) -> bool:
    configured = (os.getenv("ROBINHOOD_RPC_URL") or "").strip().rstrip("/").lower()
    candidate = str(value or "").strip().rstrip("/").lower()
    public = str(runtime.ROBINHOOD_PUBLIC_RPC).strip().rstrip("/").lower()
    return bool(configured and candidate == configured and candidate != public)


def _metered_rpc(original: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(rpc_self: Any, method: str, params: list[Any]) -> Any:
        production = _production_rpc_url(getattr(rpc_self, "rpc_url", ""))
        if production:
            meter.record_http_request(method, params)
        result = await original(rpc_self, method, params)
        if production:
            meter.record_http_response(result)
        return result

    setattr(wrapped, "_roi_adaptive_provider_meter_rpc", True)
    return wrapped


def _metered_enqueue(original: Callable[..., None]) -> Callable[..., None]:
    @wraps(original)
    def wrapped(self: Any, *, generation: int, log: dict[str, Any], live_authority: bool, source: str) -> None:
        if live_authority:
            meter.record_ws_log(log)
        original(
            self,
            generation=generation,
            log=log,
            live_authority=live_authority,
            source=source,
        )

    setattr(wrapped, "_roi_adaptive_provider_meter_enqueue", True)
    return wrapped


def _metered_ws_rpc(original: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(ws: Any, request_id: int, method: str, params: list[Any]) -> Any:
        result = await original(ws, request_id, method, params)
        meter.record_ws_control(method, params, result)
        return result

    setattr(wrapped, "_roi_adaptive_provider_meter_ws_rpc", True)
    return wrapped


def _rates() -> tuple[float, float, float]:
    short = float(meter.snapshot(SHORT_WINDOW_SECONDS)["estimated_cu_per_minute"])
    long = float(meter.snapshot(LONG_WINDOW_SECONDS)["estimated_cu_per_minute"])
    return short, long, max(short, long)


def _control(self: Any, *, demand: int, open_positions: int) -> int:
    state = _state(self)
    now = time.monotonic()
    current = int(state.get("prospective_lane_cap", max(1, _min_lanes())) or 0)
    minimum = _min_lanes()
    maximum = _max_lanes()
    target = _target_cu_per_minute()

    if now - float(state.get("last_control_monotonic", 0.0) or 0.0) < CONTROL_INTERVAL_SECONDS:
        return max(0, min(maximum, current))

    short_rate, long_rate, effective = _rates()
    state["last_control_monotonic"] = now
    state["short_estimated_cu_per_minute"] = short_rate
    state["long_estimated_cu_per_minute"] = long_rate
    state["effective_estimated_cu_per_minute"] = effective
    state["estimated_headroom_cu_per_minute"] = max(0.0, target - effective)
    state["ranked_demand"] = max(0, int(demand))
    state["open_position_count"] = max(0, int(open_positions))

    desired = current
    reason = "hold"

    if demand <= max(1, minimum):
        desired = max(1, minimum)
        reason = "quiet_or_single_candidate"
    elif effective >= target * EMERGENCY_FRACTION:
        desired = 0 if open_positions > 0 else max(1, minimum)
        reason = "provider_budget_emergency"
    elif effective >= target * HARD_CONTRACTION_FRACTION:
        desired = max(1, minimum)
        reason = "provider_budget_hard_contraction"
    elif effective >= target * SOFT_CONTRACTION_FRACTION:
        desired = min(current, 2)
        reason = "provider_budget_soft_contraction"
    else:
        active_for_projection = max(1, open_positions + max(1, current))
        marginal = max(MIN_PROJECTED_MARGINAL_CU_PER_MINUTE, effective / active_for_projection)
        projected = effective + marginal
        state["projected_next_lane_cu_per_minute"] = projected
        can_expand = (
            current < min(maximum, demand)
            and projected <= target * EXPANSION_HEADROOM_FRACTION
            and now - float(state.get("last_change_monotonic", 0.0) or 0.0) >= EXPANSION_COOLDOWN_SECONDS
        )
        if can_expand:
            desired = current + 1
            reason = "ranked_demand_with_provider_headroom"

    desired = max(0, min(maximum, int(desired)))
    if desired != current:
        state["prospective_lane_cap"] = desired
        state["last_change_monotonic"] = now
        state["last_change_reason"] = reason
    else:
        state["prospective_lane_cap"] = current
        state["last_change_reason"] = reason
    return int(state["prospective_lane_cap"])


def _adaptive_selected_market_targets(self: Any) -> tuple[dict[str, int], dict[str, str]]:
    universe = budget._candidate_universe(self)
    open_markets = budget._open_market_addresses(self)
    rankings = budget._research_rankings(self, universe)
    ranked_candidates = [address for address, _score, _reason in rankings if address not in open_markets]
    prospective_cap = _control(self, demand=len(ranked_candidates), open_positions=len(open_markets))

    selected: dict[str, int] = {}
    reasons: dict[str, str] = {}

    for address in sorted(open_markets):
        descriptor = universe.get(address)
        if descriptor is None:
            continue
        budget._ensure_runtime_market(self, descriptor)
        selected[address] = int(descriptor["launch_block"])
        # Preserve the canonical reason string for downstream compatibility. Status
        # separately proves that open positions do not consume prospective capacity.
        reasons[address] = "open_position_forced_live"

    added = 0
    for address, _score, reason in rankings:
        if address in selected:
            continue
        if added >= prospective_cap:
            break
        descriptor = universe.get(address)
        if descriptor is None:
            continue
        budget._ensure_runtime_market(self, descriptor)
        selected[address] = int(descriptor["launch_block"])
        reasons[address] = reason
        added += 1

    budget._update_research_state(self, universe_size=len(universe))
    setattr(self, "_roi_budget_live_target_reasons", reasons)
    setattr(self, "_roi_budget_live_targets", dict(selected))
    setattr(self, "_roi_adaptive_prospective_lane_count", added)
    setattr(self, "_roi_adaptive_open_position_live_count", len(open_markets & set(selected)))
    return selected, reasons


def _status_wrapper(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    @wraps(original)
    def wrapped(self: Any) -> dict[str, Any]:
        payload = original(self)
        state = dict(_state(self))
        authority = payload.setdefault("production_transport_authority", {})
        selected = dict(getattr(self, "_roi_budget_live_targets", {}) or {})
        open_count = int(getattr(self, "_roi_adaptive_open_position_live_count", 0) or 0)
        prospective_count = int(getattr(self, "_roi_adaptive_prospective_lane_count", 0) or 0)
        prospective_cap = int(state.get("prospective_lane_cap", 1) or 0)
        authority.update(
            {
                "adaptive_lane_controller_enabled": True,
                "adaptive_lane_controller_version": CONTROLLER_VERSION,
                "adaptive_prospective_lane_cap": prospective_cap,
                "adaptive_min_prospective_lanes": _min_lanes(),
                "adaptive_max_prospective_lanes": _max_lanes(),
                "adaptive_target_cu_per_minute": _target_cu_per_minute(),
                "adaptive_short_estimated_cu_per_minute": float(state.get("short_estimated_cu_per_minute", 0.0) or 0.0),
                "adaptive_long_estimated_cu_per_minute": float(state.get("long_estimated_cu_per_minute", 0.0) or 0.0),
                "adaptive_effective_estimated_cu_per_minute": float(state.get("effective_estimated_cu_per_minute", 0.0) or 0.0),
                "adaptive_estimated_headroom_cu_per_minute": float(state.get("estimated_headroom_cu_per_minute", 0.0) or 0.0),
                "adaptive_projected_next_lane_cu_per_minute": float(state.get("projected_next_lane_cu_per_minute", 0.0) or 0.0),
                "adaptive_last_change_reason": state.get("last_change_reason"),
                "adaptive_ranked_demand": int(state.get("ranked_demand", 0) or 0),
                "adaptive_open_positions_forced_live": open_count,
                "adaptive_prospective_live_count": prospective_count,
                "alchemy_live_market_cap": open_count + prospective_cap,
                "alchemy_live_market_cap_mode": "adaptive_prospective_plus_all_forced_open_positions",
                "alchemy_live_market_count": len(selected),
                "open_positions_consume_prospective_cap": False,
                "candidate_discovery_constrained_by_adaptive_cap": False,
                "provider_meter": meter.snapshot(LONG_WINDOW_SECONDS),
            }
        )
        return payload

    setattr(wrapped, "_roi_adaptive_lane_status", True)
    return wrapped


def install_robinhood_adaptive_lane_controller(plane_cls: type[Any]) -> None:
    global _INSTALLED, _ORIGINAL_SELECTED_TARGETS, _ORIGINAL_RPC, _ORIGINAL_ENQUEUE, _ORIGINAL_WS_RPC_REQUEST
    if _INSTALLED:
        return

    _ORIGINAL_SELECTED_TARGETS = budget._selected_market_targets
    budget._selected_market_targets = _adaptive_selected_market_targets

    _ORIGINAL_RPC = runtime.RobinhoodRpc.rpc
    if not bool(getattr(runtime.RobinhoodRpc.rpc, "_roi_adaptive_provider_meter_rpc", False)):
        runtime.RobinhoodRpc.rpc = _metered_rpc(runtime.RobinhoodRpc.rpc)  # type: ignore[method-assign]

    _ORIGINAL_ENQUEUE = bounded._enqueue
    if not bool(getattr(bounded._enqueue, "_roi_adaptive_provider_meter_enqueue", False)):
        bounded._enqueue = _metered_enqueue(bounded._enqueue)

    _ORIGINAL_WS_RPC_REQUEST = bounded._rpc_request
    if not bool(getattr(bounded._rpc_request, "_roi_adaptive_provider_meter_ws_rpc", False)):
        bounded._rpc_request = _metered_ws_rpc(bounded._rpc_request)

    if not bool(getattr(plane_cls.status, "_roi_adaptive_lane_status", False)):
        plane_cls.status = _status_wrapper(plane_cls.status)  # type: ignore[method-assign]

    setattr(plane_cls, "_roi_robinhood_adaptive_lane_controller_version", CONTROLLER_VERSION)
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": CONTROLLER_VERSION,
        "installed": _INSTALLED,
        "adaptive_prospective_lanes": True,
        "min_prospective_lanes": _min_lanes(),
        "max_prospective_lanes": _max_lanes(),
        "target_cu_per_minute": _target_cu_per_minute(),
        "open_positions_outside_prospective_cap": True,
        "candidate_discovery_constrained_by_cap": False,
        "strategy_authority_changed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "CONTROLLER_VERSION",
    "install_robinhood_adaptive_lane_controller",
    "status",
]
