from __future__ import annotations

from typing import Any

from .strategy_v51_authority import authority as v51_authority, authority_fingerprint as v51_fingerprint
from .strategy_v52_authority import authority, authority_fingerprint, safety_manifest
from .v52_authoritative_strategy import status as strategy_status

STATUS_PATH = "/v1/strategy/authority"
V52_STATUS_PATH = "/v1/strategy/v52"
V51_CONTROL_PATH = "/v1/strategy/control/v51-authority"
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
        "strategy_runtime": strategy_status(),
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
    """Retire the incumbent canonical route before mounting v5.2.

    FastAPI/Starlette resolves the first matching route, so merely adding another
    handler would leave the earlier v5.1 canonical endpoint authoritative. Removing
    only the exact strategy-authority path preserves every other v5.1 read-only
    proof/diagnostic endpoint.
    """
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

    app.state.roi_strategy_authority_status = _payload
    app.state.roi_v51_control_authority_status = _v51_control_payload
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "installed": _INSTALLED,
        "status_path": STATUS_PATH,
        "v52_status_path": V52_STATUS_PATH,
        "v51_control_path": V51_CONTROL_PATH,
        "authoritative_strategy": "v5.2",
        "v51_control_final_decision_authority": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "STATUS_PATH",
    "V51_CONTROL_PATH",
    "V52_STATUS_PATH",
    "install_v52_strategy_api",
    "status",
]
