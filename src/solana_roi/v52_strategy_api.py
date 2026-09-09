from __future__ import annotations

from typing import Any

from .strategy_v52_authority import authority, authority_fingerprint, safety_manifest
from .v52_authoritative_strategy import status as strategy_status

STATUS_PATH = "/v1/strategy/authority"
V52_STATUS_PATH = "/v1/strategy/v52"
_INSTALLED = False


def _payload() -> dict[str, Any]:
    policy = authority()
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
        "position_management": dict(policy["position_management"]),
        "execution": dict(policy["execution"]),
        "strategy_runtime": strategy_status(),
        "safety": safety_manifest(),
    }


def install_v52_strategy_api(app: Any) -> None:
    global _INSTALLED
    existing = {getattr(route, "path", None) for route in app.routes}
    if STATUS_PATH not in existing:
        @app.get(STATUS_PATH)
        def canonical_strategy_authority() -> dict[str, Any]:
            return _payload()
    if V52_STATUS_PATH not in existing:
        @app.get(V52_STATUS_PATH)
        def v52_strategy_status() -> dict[str, Any]:
            return _payload()
    app.state.roi_strategy_authority_status = _payload
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "installed": _INSTALLED,
        "status_path": STATUS_PATH,
        "v52_status_path": V52_STATUS_PATH,
        "authoritative_strategy": "v5.2",
        "v51_control_final_decision_authority": False,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = ["STATUS_PATH", "V52_STATUS_PATH", "install_v52_strategy_api", "status"]
