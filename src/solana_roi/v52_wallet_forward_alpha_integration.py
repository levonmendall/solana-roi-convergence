from __future__ import annotations

from typing import Any

from . import v52_authoritative_strategy as strategy
from .v52_wallet_forward_alpha import FORWARD_ALPHA_VERSION, wallet_forward_shadow_profile

INTEGRATION_VERSION = "v52-wallet-forward-alpha-integration-v2-prospective-controls"
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


def _no_wallet_pre(pre: dict[str, Any]) -> dict[str, Any]:
    """Same opportunity with the wallet-only lane neutralized.

    The control deliberately keeps lifecycle, venue, regime, risk, graduation,
    cross-venue and execution evidence unchanged. Only the explicitly wallet-driven
    ``elite_wallet_continuation`` lane and wallet identifier are removed, so the
    counterfactual answers whether the same opportunity would have received a v5.2
    target without wallet intelligence. It never controls trading.
    """

    copied = dict(pre)
    copied["wallet"] = ""
    copied["lanes"] = tuple(
        lane for lane in tuple(pre.get("lanes") or ()) if str(lane) != "elite_wallet_continuation"
    )
    return copied


def _record_prospective_controls(
    adapter: Any,
    pre: dict[str, Any],
    *,
    lane: str | None,
    target: float,
    chase: float | None,
    latency: float | None,
) -> None:
    if _BASE_TARGET is None:
        return
    try:
        no_wallet_lane, no_wallet_target, _profiles = _BASE_TARGET(
            adapter,
            _no_wallet_pre(pre),
            chase=chase,
            latency=latency,
        )
        effective_lane = str(lane or no_wallet_lane or "none")
        context_key = _context_key(pre, effective_lane)
        from .v52_wallet_forward_alpha_runtime import record_shadow_decision

        record_shadow_decision(
            adapter=adapter,
            pre=pre,
            lane=lane,
            current_target=max(0.0, float(target or 0.0)),
            no_wallet_lane=no_wallet_lane,
            no_wallet_target=max(0.0, float(no_wallet_target or 0.0)),
            context_key=context_key,
        )
    except Exception:
        # Research shadow failures never alter the authoritative decision path.
        return


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

    # Always record the same-stream wallet-neutral and Wallet Forward Alpha research
    # controls before any production influence is considered. The recorder is
    # append-only/research-only and failures are isolated from authority.
    _record_prospective_controls(
        adapter,
        pre,
        lane=lane,
        target=float(target or 0.0),
        chase=chase,
        latency=latency,
    )

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
        "prospective_three_way_shadow_recorded": True,
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
        "prospective_same_stream_controls": True,
        "wallet_neutral_control_definition": "same opportunity with elite_wallet_continuation removed",
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
