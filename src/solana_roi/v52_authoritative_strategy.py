from __future__ import annotations

from typing import Any, Callable

from . import fomo_paper_strategy as fomo_paper
from . import risk_conditioned_alpha_v5 as solana_strategy
from .robinhood_chain_profit_maximizer import RobinhoodProfitMaximizerMixin
from .strategy_v52_authority import (
    AUTHORITY_ID,
    ECONOMIC_FREEZE_EPOCH,
    LIVE_MONEY_AUTHORITY,
    PAPER_ONLY,
    STRATEGY_VERSION,
    authority_fingerprint,
    position_policy,
)


CONSOLIDATION_VERSION = "v52-authoritative-economic-owner-v1"
_INSTALLED = False
_BASE_SOLANA_CHOOSE: Callable[..., Any] | None = None
_BASE_FOMO_DECISION: Callable[..., Any] | None = None
_BASE_RH_CHOOSE: Callable[..., Any] | None = None


def _starter_fraction(target_fraction: float) -> float:
    target = max(0.0, min(1.0, float(target_fraction)))
    starter = float(position_policy()["starter_fraction_of_target"])
    return max(0.0, min(target, target * starter))


def _authority_metadata(*, target_fraction: float, final_fraction: float) -> dict[str, Any]:
    return {
        "authority_id": AUTHORITY_ID,
        "strategy_version": STRATEGY_VERSION,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "authority_fingerprint": authority_fingerprint(),
        "decision_owner": "v52",
        "compatibility_substrate": "v51_forward_evidence_and_exact_execution",
        "target_fraction_before_v52_capture_policy": float(target_fraction),
        "final_fraction": float(final_fraction),
        "capture_stage": "starter",
        "scale_requires_new_forward_evidence": True,
        "averaging_down_allowed": False,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
    }


def _v52_solana_choose(
    adapter: Any,
    pre: dict[str, Any],
    *,
    chase: float | None = None,
    latency: float | None = None,
) -> tuple[str | None, float, dict[str, Any]]:
    if _BASE_SOLANA_CHOOSE is None:
        raise RuntimeError("v52_solana_base_not_installed")
    lane, target, profiles = _BASE_SOLANA_CHOOSE(adapter, pre, chase=chase, latency=latency)
    copied = {key: dict(value) if isinstance(value, dict) else value for key, value in dict(profiles).items()}
    if not lane or float(target or 0.0) <= 0.0:
        return lane, 0.0, copied
    if latency is not None and float(latency) > 20.0:
        return None, 0.0, copied
    if chase is not None and float(chase) > 0.40:
        return None, 0.0, copied
    final = _starter_fraction(float(target))
    profile = dict(copied.get(lane) or {})
    profile["v52_authority"] = _authority_metadata(target_fraction=float(target), final_fraction=final)
    copied[lane] = profile
    return (lane if final > 0.0 else None), final, copied


def _v52_fomo_decision(
    adapter: Any,
    *,
    observation: dict[str, Any],
    trial: dict[str, Any],
) -> dict[str, Any]:
    if _BASE_FOMO_DECISION is None:
        raise RuntimeError("v52_fomo_base_not_installed")
    result = dict(_BASE_FOMO_DECISION(adapter, observation=observation, trial=trial))
    target = float(result.get("position_fraction") or 0.0)
    if not str(result.get("decision") or "").startswith("paper_enter_") or target <= 0.0:
        result["v52_authority"] = _authority_metadata(target_fraction=target, final_fraction=0.0)
        return result
    latency = float(trial.get("signal_to_entry_seconds") or 0.0)
    if latency > 20.0 or not bool(trial.get("entry_executable")) or not bool(trial.get("exit_executable")):
        result.update(
            {
                "decision": "no_entry_v52_execution_boundary",
                "reason": "v52_exact_two_sided_execution_and_20s_boundary_required",
                "position_fraction": 0.0,
            }
        )
        result["v52_authority"] = _authority_metadata(target_fraction=target, final_fraction=0.0)
        return result
    final = _starter_fraction(target)
    result["decision"] = "paper_enter_v52_starter"
    result["reason"] = "v52_authoritative_continuation_capture_starter"
    result["position_fraction"] = final
    result["v52_authority"] = _authority_metadata(target_fraction=target, final_fraction=final)
    return result


def _v52_robinhood_choose(self: Any, **kwargs: Any) -> tuple[str | None, float, dict[str, Any]]:
    if _BASE_RH_CHOOSE is None:
        raise RuntimeError("v52_robinhood_base_not_installed")
    lane, target, profiles = _BASE_RH_CHOOSE(self, **kwargs)
    copied = {key: dict(value) if isinstance(value, dict) else value for key, value in dict(profiles).items()}
    if not lane or float(target or 0.0) <= 0.0:
        return lane, 0.0, copied
    final = _starter_fraction(float(target))
    profile = dict(copied.get(lane) or {})
    profile["v52_authority"] = _authority_metadata(target_fraction=float(target), final_fraction=final)
    copied[lane] = profile
    return (lane if final > 0.0 else None), final, copied


def install_v52_authoritative_strategy() -> None:
    """Install v5.2 last so it is the sole final economic decision owner.

    The mature v5.1-named modules remain as compatibility substrate for forward
    evidence, exact two-sided quotes, structural hard stops, paper capital and
    settlement. Their economic outputs become v5.2 inputs; they are not final
    authority after this installer returns.
    """
    global _INSTALLED, _BASE_SOLANA_CHOOSE, _BASE_FOMO_DECISION, _BASE_RH_CHOOSE
    if _INSTALLED:
        return
    _BASE_SOLANA_CHOOSE = solana_strategy._choose_lane_and_fraction
    _BASE_FOMO_DECISION = fomo_paper._paper_decision
    _BASE_RH_CHOOSE = RobinhoodProfitMaximizerMixin._v5_choose_lane_fraction
    solana_strategy._choose_lane_and_fraction = _v52_solana_choose
    fomo_paper._paper_decision = _v52_fomo_decision
    RobinhoodProfitMaximizerMixin._v5_choose_lane_fraction = _v52_robinhood_choose
    setattr(solana_strategy._choose_lane_and_fraction, "_roi_v52_final_authority", True)
    setattr(fomo_paper._paper_decision, "_roi_v52_final_authority", True)
    setattr(RobinhoodProfitMaximizerMixin._v5_choose_lane_fraction, "_roi_v52_final_authority", True)
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "consolidation_version": CONSOLIDATION_VERSION,
        "installed": _INSTALLED,
        "authority_id": AUTHORITY_ID,
        "strategy_version": STRATEGY_VERSION,
        "authority_fingerprint": authority_fingerprint(),
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "final_decision_owner": "v52",
        "solana_final_owner": bool(getattr(solana_strategy._choose_lane_and_fraction, "_roi_v52_final_authority", False)),
        "fomo_final_owner": bool(getattr(fomo_paper._paper_decision, "_roi_v52_final_authority", False)),
        "robinhood_final_owner": bool(getattr(RobinhoodProfitMaximizerMixin._v5_choose_lane_fraction, "_roi_v52_final_authority", False)),
        "v51_named_substrate_role": "compatibility_evidence_exact_execution_and_settlement_only",
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
    }


__all__ = ["CONSOLIDATION_VERSION", "install_v52_authoritative_strategy", "status"]
