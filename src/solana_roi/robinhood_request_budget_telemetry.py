from __future__ import annotations

import math
from functools import wraps
from typing import Any, Callable

from . import robinhood_getlogs_provider_guard as getlogs_guard
from . import robinhood_provider_budget_transport as provider_budget


TELEMETRY_VERSION = "robinhood-request-budget-telemetry-v1"
_INSTALLED = False
_ORIGINAL_RESEARCH_PASS = provider_budget._research_pass
_ORIGINAL_STATUS: Callable[[Any], dict[str, Any]] | None = None


class RobinhoodRequestBudgetViolation(RuntimeError):
    pass


async def _budgeted_research_pass(self: Any, rpc: Any) -> None:
    before = provider_budget._research_state(self)
    universe = provider_budget._candidate_universe(self)
    v3_count = sum(1 for item in universe.values() if item.get("kind") == "v3")
    v2_count = sum(1 for item in universe.values() if item.get("kind") == "v2")
    actual_getlogs = 0
    original_get_logs = rpc.get_logs

    async def counted_get_logs(*args: Any, **kwargs: Any) -> Any:
        nonlocal actual_getlogs
        actual_getlogs += 1
        return await original_get_logs(*args, **kwargs)

    rpc.get_logs = counted_get_logs
    error: BaseException | None = None
    try:
        await _ORIGINAL_RESEARCH_PASS(self, rpc)
    except BaseException as exc:
        error = exc
    finally:
        rpc.get_logs = original_get_logs

    after = provider_budget._research_state(self)
    before_cursor = before.get("cursor_block")
    after_cursor = after.get("cursor_block")
    advanced = (
        isinstance(before_cursor, int)
        and isinstance(after_cursor, int)
        and int(after_cursor) > int(before_cursor)
    )
    expected_getlogs = (
        math.ceil(v3_count / provider_budget.RESEARCH_BATCH_SIZE)
        + math.ceil(v2_count / provider_budget.RESEARCH_BATCH_SIZE)
        if advanced
        else 0
    )
    prior_expected = int(after.get("expected_getlogs_total", 0) or 0)
    prior_actual = int(after.get("actual_getlogs_total", 0) or 0)
    prior_violations = int(after.get("request_budget_violations", 0) or 0)
    explained = actual_getlogs == expected_getlogs
    provider_budget._update_research_state(
        self,
        expected_getlogs_last_pass=expected_getlogs,
        actual_getlogs_last_pass=actual_getlogs,
        expected_getlogs_total=prior_expected + expected_getlogs,
        actual_getlogs_total=prior_actual + actual_getlogs,
        request_budget_violations=prior_violations + (0 if explained else 1),
        request_budget_last_pass_explained=explained,
        request_budget_v3_market_count=v3_count,
        request_budget_v2_market_count=v2_count,
        request_budget_batch_size=provider_budget.RESEARCH_BATCH_SIZE,
        request_budget_formula="ceil(v3/64)+ceil(v2/64) when frontier advances; otherwise 0",
    )
    if error is not None:
        raise error
    if not explained:
        provider_budget._update_research_state(
            self,
            ready=False,
            last_error_type="RobinhoodRequestBudgetViolation",
        )
        raise RobinhoodRequestBudgetViolation(
            f"public research getLogs budget mismatch: expected={expected_getlogs} actual={actual_getlogs}"
        )


setattr(_budgeted_research_pass, "_roi_robinhood_request_budget_telemetry", True)


def _status_wrapper(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    @wraps(original)
    def wrapped(self: Any) -> dict[str, Any]:
        payload = original(self)
        research = provider_budget._research_state(self)
        v2v4 = payload.get("v2_v4_observation")
        v2v4_last = v2v4.get("last_range") if isinstance(v2v4, dict) else None
        private_provider = getlogs_guard.status().get("validation_cloud_proof", {})
        payload["robinhood_request_budget"] = {
            "version": TELEMETRY_VERSION,
            "public_research": {
                "universe_size": int(research.get("universe_size", 0) or 0),
                "v3_market_count": int(research.get("request_budget_v3_market_count", 0) or 0),
                "v2_market_count": int(research.get("request_budget_v2_market_count", 0) or 0),
                "batch_size": int(research.get("request_budget_batch_size", provider_budget.RESEARCH_BATCH_SIZE) or provider_budget.RESEARCH_BATCH_SIZE),
                "expected_getlogs_last_pass": int(research.get("expected_getlogs_last_pass", 0) or 0),
                "actual_getlogs_last_pass": int(research.get("actual_getlogs_last_pass", 0) or 0),
                "expected_getlogs_total": int(research.get("expected_getlogs_total", 0) or 0),
                "actual_getlogs_total": int(research.get("actual_getlogs_total", 0) or 0),
                "violations": int(research.get("request_budget_violations", 0) or 0),
                "last_pass_explained": bool(research.get("request_budget_last_pass_explained", True)),
                "formula": research.get("request_budget_formula"),
                "transport": "official_public_robinhood_rpc",
            },
            "private_getlogs": {
                "validation_cloud_ranges_attempted": int(private_provider.get("ranges_attempted", 0) or 0),
                "validation_cloud_ranges_succeeded": int(private_provider.get("ranges_succeeded", 0) or 0),
                "validation_cloud_ranges_failed": int(private_provider.get("ranges_failed", 0) or 0),
                "validation_cloud_range_splits": int(private_provider.get("range_splits", 0) or 0),
                "validation_cloud_fallback_ranges": int(private_provider.get("fallback_ranges", 0) or 0),
            },
            "v2_v4_last_range": dict(v2v4_last) if isinstance(v2v4_last, dict) else None,
            "unexplained_request_amplification_is_certification_failure": True,
            "candidate_universe_reduced": False,
            "strategy_thresholds_changed": False,
            "paper_only": True,
            "live_money_authority": False,
        }
        return payload

    setattr(wrapped, "_roi_robinhood_request_budget_status", True)
    return wrapped


def install_robinhood_request_budget_telemetry(plane_cls: type[Any]) -> None:
    global _INSTALLED, _ORIGINAL_STATUS
    provider_budget._research_pass = _budgeted_research_pass
    current = getattr(plane_cls, "status", None)
    if current is not None and not bool(getattr(current, "_roi_robinhood_request_budget_status", False)):
        _ORIGINAL_STATUS = current
        plane_cls.status = _status_wrapper(current)  # type: ignore[method-assign]
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": TELEMETRY_VERSION,
        "installed": _INSTALLED,
        "public_research_expected_vs_actual": True,
        "public_research_budget_violation_fails_closed": True,
        "private_provider_counters_exposed": True,
        "v2_v4_expected_vs_actual_exposed": True,
        "candidate_universe_reduced": False,
        "strategy_thresholds_changed": False,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "TELEMETRY_VERSION",
    "RobinhoodRequestBudgetViolation",
    "_budgeted_research_pass",
    "install_robinhood_request_budget_telemetry",
    "status",
]
