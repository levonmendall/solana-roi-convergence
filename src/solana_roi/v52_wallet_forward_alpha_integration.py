from __future__ import annotations

from typing import Any

from . import v52_authoritative_strategy as strategy
from .v52_wallet_forward_alpha import FORWARD_ALPHA_VERSION, wallet_forward_shadow_profile

INTEGRATION_VERSION = "v52-wallet-forward-alpha-integration-v1"
_BASE_TARGET = None


def _context_key(pre: dict[str, Any], lane: str) -> str:
    risk = dict(pre.get("risk") or {})
    risk_signature = str(
        risk.get("risk_signature")
        or risk.get("risk_class")
        or risk.get("class")
        or "unknown"
    )
    return "|".join(
        (
            str(lane or "unknown"),
            str(pre.get("venue") or "UNKNOWN"),
            str(pre.get("lifecycle") or "unknown"),
            str(pre.get("regime") or "unknown"),
            risk_signature,
        )
    )


def _wallet_target(
    adapter: Any,
    pre: dict[str, Any],
    *,
    chase: float | None,
    latency: float | None,
):
    if _BASE_TARGET is None:
        raise RuntimeError("wallet forward alpha integration base target unavailable")
    lane, target, profiles = _BASE_TARGET(adapter, pre, chase=chase, latency=latency)
    if not lane or target <= 0.0:
        return lane, target, profiles

    context_key = _context_key(pre, lane)
    wallet = str(pre.get("wallet") or "")
    wallet_profile = wallet_forward_shadow_profile(
        adapter.store,
        wallet=wallet,
        context_key=context_key,
        reference_capital_usd=500.0,
    )
    if not bool(wallet_profile.get("available")) and wallet_profile.get("reason") == "forward_alpha_schema_not_installed":
        return lane, target, profiles

    profile = dict(profiles.get(lane) or {})
    profile["wallet_forward_alpha"] = {
        **wallet_profile,
        "context_key": context_key,
        "integration_version": INTEGRATION_VERSION,
        "baseline_v52_eligibility_already_established": True,
    }
    profiles[lane] = profile

    if not bool(wallet_profile.get("strategy_influence_enabled")):
        return lane, target, profiles

    multiplier = max(0.80, min(1.10, float(wallet_profile.get("sizing_multiplier") or 1.0)))
    severity = float((pre.get("risk") or {}).get("risk_severity") or 0.0)
    lane_cap = float(strategy._lane_cap(lane, severity))
    adjusted = max(0.0, min(lane_cap, float(target) * multiplier))
    return lane, adjusted, profiles


setattr(_wallet_target, "_roi_v52_wallet_forward_alpha_integration", True)


def install_v52_wallet_forward_alpha_integration() -> None:
    global _BASE_TARGET
    current = strategy._v52_solana_target
    if bool(getattr(current, "_roi_v52_wallet_forward_alpha_integration", False)):
        return
    _BASE_TARGET = current
    strategy._v52_solana_target = _wallet_target


def status() -> dict[str, Any]:
    installed = bool(getattr(strategy._v52_solana_target, "_roi_v52_wallet_forward_alpha_integration", False))
    return {
        "installed": installed,
        "version": INTEGRATION_VERSION,
        "forward_alpha_version": FORWARD_ALPHA_VERSION,
        "bridge_stage": "post_baseline_target_pre_position_construction",
        "may_create_eligibility": False,
        "may_bypass_latency_chase_risk_execution": False,
        "maximum_positive_sizing_multiplier": 1.10,
        "maximum_negative_sizing_multiplier": 0.80,
        "lane_cap_preserved": True,
        "reference_portfolio_usd": 500.0,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = ["INTEGRATION_VERSION", "install_v52_wallet_forward_alpha_integration", "status"]
