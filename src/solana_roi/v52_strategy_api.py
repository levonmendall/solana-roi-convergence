from __future__ import annotations

from typing import Any

from .strategy_v51_authority import authority as v51_authority, authority_fingerprint as v51_fingerprint
from .strategy_v52_authority import authority, authority_fingerprint, safety_manifest
from .v52_authoritative_strategy import status as strategy_status
from .v52_profit_confidence_completion import report as profit_confidence_report, status as profit_confidence_status
from .v52_profit_confidence_finalization import status as profit_confidence_finalization_status
from .v52_learning_governance import status as learning_governance_status

STATUS_PATH = "/v1/strategy/authority"
V52_STATUS_PATH = "/v1/strategy/v52"
V51_CONTROL_PATH = "/v1/strategy/control/v51-authority"
PERFORMANCE_24H_PATH = "/v1/strategy/v52/performance/24h"
PERFORMANCE_7D_PATH = "/v1/strategy/v52/performance/7d"
LEARNING_GOVERNANCE_PATH = "/v1/strategy/v52/learning-governance"
_INSTALLED = False


def _payload() -> dict[str, Any]:
    policy = authority()
    safety = safety_manifest()
    return {
        "authoritative_strategy": "v5.2",
        "strategy_version": policy["strategy_version"],
        "authority_id": policy["authority_id"],
        "economic_freeze_epoch": policy["economic_freeze_epoch"],
        "authority_fingerprint": authority_fingerprint(),
        "policy_freeze_origin": policy["policy_freeze_origin"],
        "economic_superiority_claim": policy["economic_superiority_claim"],
        "control_strategy_version": policy["control_strategy_version"],
        "v51_control_final_decision_authority": False,
        "canonical_lanes": list(policy["canonical_lanes"]),
        "target_sizing": dict(policy["target_sizing"]),
        "position_management": dict(policy["position_management"]),
        "detection_intelligence": dict(policy["detection_intelligence"]),
        "execution": dict(policy["execution"]),
        "governance": dict(policy["governance"]),
        "strategy_runtime": strategy_status(),
        "profit_confidence_completion": profit_confidence_status(),
        "learning_governance": learning_governance_status(),
        "profit_confidence_finalization": profit_confidence_finalization_status(),
        "performance_reports": {
            "24h": PERFORMANCE_24H_PATH,
            "7d": PERFORMANCE_7D_PATH,
            "read_only": True,
        },
        "learning_governance_status_path": LEARNING_GOVERNANCE_PATH,
        "safety": safety,
        "canonical": True,
        "paper_only": bool(safety["paper_only"]),
        "live_money_authority": bool(safety["live_money_authority"]),
        "signing_available": bool(safety["signing_available"]),
        "transaction_submission_available": bool(safety["transaction_submission_available"]),
    }


def _v51_control_payload() -> dict[str, Any]:
    payload = dict(v51_authority())
    return {
        **payload,
        "authority_fingerprint": v51_fingerprint(),
        "canonical": False,
        "control_only": True,
        "read_only": True,
        "final_decision_authority": False,
        "paper_entry_authority": False,
        "position_management_authority": False,
        "promotion_authority_for_v52": False,
    }


def _remove_existing_path(app: Any, path: str) -> None:
    routes = getattr(getattr(app, "router", None), "routes", None)
    if routes is None:
        raise RuntimeError("v52_strategy_api_router_unavailable")
    routes[:] = [route for route in routes if getattr(route, "path", None) != path]


def install_v52_strategy_api(app: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _remove_existing_path(app, STATUS_PATH)

    @app.get(STATUS_PATH)
    def canonical_strategy_authority() -> dict[str, Any]:
        return _payload()

    existing = {getattr(route, "path", None) for route in app.routes}
    if V52_STATUS_PATH not in existing:
        @app.get(V52_STATUS_PATH)
        def v52_strategy_status() -> dict[str, Any]:
            return _payload()
    if V51_CONTROL_PATH not in existing:
        @app.get(V51_CONTROL_PATH)
        def v51_control_status() -> dict[str, Any]:
            return _v51_control_payload()
    if PERFORMANCE_24H_PATH not in existing:
        @app.get(PERFORMANCE_24H_PATH)
        def v52_performance_24h() -> dict[str, Any]:
            return profit_confidence_report(24)
    if PERFORMANCE_7D_PATH not in existing:
        @app.get(PERFORMANCE_7D_PATH)
        def v52_performance_7d() -> dict[str, Any]:
            return profit_confidence_report(24 * 7)
    if LEARNING_GOVERNANCE_PATH not in existing:
        @app.get(LEARNING_GOVERNANCE_PATH)
        def v52_learning_governance_status() -> dict[str, Any]:
            return learning_governance_status()

    app.state.roi_strategy_authority_status = _payload
    app.state.roi_v51_control_authority_status = _v51_control_payload
    app.state.roi_v52_performance_24h = lambda: profit_confidence_report(24)
    app.state.roi_v52_performance_7d = lambda: profit_confidence_report(24 * 7)
    app.state.roi_v52_learning_governance_status = learning_governance_status
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "installed": _INSTALLED,
        "status_path": STATUS_PATH,
        "v52_status_path": V52_STATUS_PATH,
        "v51_control_path": V51_CONTROL_PATH,
        "performance_24h_path": PERFORMANCE_24H_PATH,
        "performance_7d_path": PERFORMANCE_7D_PATH,
        "learning_governance_path": LEARNING_GOVERNANCE_PATH,
        "performance_reports_read_only": True,
        "learning_governance_status_read_only": True,
        "authoritative_strategy": "v5.2",
        "v51_control_final_decision_authority": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "LEARNING_GOVERNANCE_PATH",
    "PERFORMANCE_24H_PATH",
    "PERFORMANCE_7D_PATH",
    "STATUS_PATH",
    "V51_CONTROL_PATH",
    "V52_STATUS_PATH",
    "install_v52_strategy_api",
    "status",
]
