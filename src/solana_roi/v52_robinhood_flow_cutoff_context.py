from __future__ import annotations

"""Apply event-time cutoffs only while evaluating a new Robinhood entry.

Entry/replay decisions must ignore swaps later than the triggering event, while
ongoing position management must continue to age flow evidence against current
wall time so exhaustion/decay semantics are unchanged.
"""

import math
import time
from typing import Any

from .robinhood_chain_profit_maximizer import RobinhoodProfitMaximizerMixin
from . import v52_robinhood_position_lifecycle as lifecycle


CONTEXT_VERSION = "v52-robinhood-entry-flow-cutoff-context-1"
_CONTEXT_ATTR = "_roi_v52_entry_flow_cutoff_ts"
_INSTALLED = False
_BASE_FLOW: Any | None = None
_BASE_V3: Any | None = None
_BASE_V2: Any | None = None


def _finite_ts(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= 0.0:
        return None
    return number


def _trigger_cutoff(venue_object: Any) -> float:
    swaps = list(getattr(venue_object, "recent_swaps", ()) or ())
    for raw in reversed(swaps):
        if not isinstance(raw, dict):
            continue
        observed = _finite_ts(raw.get("observed_ts"))
        if observed is not None:
            return observed
    return time.time()


def _set_context(owner: Any, cutoff: float) -> tuple[bool, Any]:
    had = hasattr(owner, _CONTEXT_ATTR)
    previous = getattr(owner, _CONTEXT_ATTR, None)
    setattr(owner, _CONTEXT_ATTR, cutoff)
    return had, previous


def _restore_context(owner: Any, had: bool, previous: Any) -> None:
    if had:
        setattr(owner, _CONTEXT_ATTR, previous)
    else:
        try:
            delattr(owner, _CONTEXT_ATTR)
        except AttributeError:
            pass


async def _flow_with_decision_context(
    self: Any,
    swaps: Any,
    *,
    deployer: str = "",
    decision_cutoff_ts: float | None = None,
) -> dict[str, Any]:
    if _BASE_FLOW is None:
        raise RuntimeError("v52_flow_cutoff_context_missing_flow_base")
    cutoff = _finite_ts(decision_cutoff_ts)
    if cutoff is None:
        cutoff = _finite_ts(getattr(self, _CONTEXT_ATTR, None))
    if cutoff is None:
        # Position-management calls are not entry replay. Preserve the existing
        # real-time aging semantics rather than freezing state at the last swap.
        cutoff = time.time()
    return await _BASE_FLOW(
        self,
        swaps,
        deployer=deployer,
        decision_cutoff_ts=cutoff,
    )


async def _v3_entry_with_cutoff(self: Any, pool: Any, *, current_block: int) -> None:
    if _BASE_V3 is None:
        raise RuntimeError("v52_flow_cutoff_context_missing_v3_base")
    had, previous = _set_context(self, _trigger_cutoff(pool))
    try:
        await _BASE_V3(self, pool, current_block=current_block)
    finally:
        _restore_context(self, had, previous)


async def _v2_entry_with_cutoff(self: Any, curve: Any) -> None:
    if _BASE_V2 is None:
        raise RuntimeError("v52_flow_cutoff_context_missing_v2_base")
    had, previous = _set_context(self, _trigger_cutoff(curve))
    try:
        await _BASE_V2(self, curve)
    finally:
        _restore_context(self, had, previous)


def install_v52_robinhood_flow_cutoff_context() -> None:
    global _INSTALLED, _BASE_FLOW, _BASE_V3, _BASE_V2
    if _INSTALLED:
        return
    _BASE_FLOW = RobinhoodProfitMaximizerMixin._v5_flow_metrics
    _BASE_V3 = lifecycle._maybe_open_v3_with_lifecycle
    _BASE_V2 = lifecycle._maybe_open_v2_with_lifecycle

    setattr(_flow_with_decision_context, "__wrapped__", _BASE_FLOW)
    setattr(_flow_with_decision_context, "_roi_v52_point_in_time_flow", True)
    setattr(_flow_with_decision_context, "_roi_v52_entry_cutoff_context", True)
    RobinhoodProfitMaximizerMixin._v5_flow_metrics = _flow_with_decision_context  # type: ignore[method-assign]

    setattr(_v3_entry_with_cutoff, "__wrapped__", _BASE_V3)
    setattr(_v3_entry_with_cutoff, "_roi_v52_entry_cutoff_context", True)
    lifecycle._maybe_open_v3_with_lifecycle = _v3_entry_with_cutoff

    setattr(_v2_entry_with_cutoff, "__wrapped__", _BASE_V2)
    setattr(_v2_entry_with_cutoff, "_roi_v52_entry_cutoff_context", True)
    lifecycle._maybe_open_v2_with_lifecycle = _v2_entry_with_cutoff
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": CONTEXT_VERSION,
        "installed": _INSTALLED,
        "entry_decisions_use_event_time_cutoff": True,
        "position_management_uses_wall_clock_aging": True,
        "changes_exit_thresholds": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = ["CONTEXT_VERSION", "install_v52_robinhood_flow_cutoff_context", "status"]
