from __future__ import annotations

from contextvars import ContextVar
from typing import Any, Callable, Iterable, Mapping

from . import fomo_paper_strategy as fomo_paper
from . import risk_conditioned_alpha_v5 as solana_strategy
from . import v52_authoritative_strategy as authoritative
from . import v52_robinhood_position_lifecycle as robinhood_lifecycle
from .strategy_v52_authority import (
    LIVE_MONEY_AUTHORITY,
    PAPER_ONLY,
    execution_policy,
    position_policy as canonical_position_policy,
    target_sizing_policy,
)
from .v52_wallet_alpha_refinement import ContextualWalletScore, WalletAlphaRefinementLedger

REFINEMENT_VERSION = "v52-adaptive-continuation-refinement-v1"
ORDINARY_SCALE_FRACTION = 0.25
EXCEPTIONAL_SCALE_FRACTION = 0.50
BASE_RUNNER_FRACTION = 0.10
MAX_DYNAMIC_RUNNER_FRACTION = 0.25
NORMAL_CHASE_MAX = 0.40
ABSOLUTE_CHASE_MAX = 0.80
MAX_WALLET_UTILIZATION_MULTIPLIER = 1.25

_INSTALLED = False
_WALLET_ALPHA: WalletAlphaRefinementLedger | None = None
_BASE_SOLANA_CHOOSE: Callable[..., Any] | None = None
_BASE_FOMO_DECISION: Callable[..., Any] | None = None
_BASE_ROBINHOOD_CHOOSE: Callable[..., Any] | None = None
_BASE_ROBINHOOD_SETTLE: Callable[..., Any] | None = None

_EXCEPTIONAL_SCALE: ContextVar[bool] = ContextVar("v52_exceptional_scale", default=False)
_RUNNER_FRACTION: ContextVar[float] = ContextVar("v52_runner_fraction", default=BASE_RUNNER_FRACTION)


def _contextual_position_policy(*args: Any, **kwargs: Any) -> dict[str, Any]:
    policy = dict(canonical_position_policy(*args, **kwargs))
    policy["max_scale_fraction_of_target_per_add"] = (
        EXCEPTIONAL_SCALE_FRACTION if _EXCEPTIONAL_SCALE.get() else ORDINARY_SCALE_FRACTION
    )
    policy["runner_fraction_of_target"] = max(
        BASE_RUNNER_FRACTION,
        min(MAX_DYNAMIC_RUNNER_FRACTION, float(_RUNNER_FRACTION.get())),
    )
    policy["ordinary_max_scale_fraction_of_target_per_add"] = ORDINARY_SCALE_FRACTION
    policy["exceptional_max_scale_fraction_of_target_per_add"] = EXCEPTIONAL_SCALE_FRACTION
    policy["max_dynamic_runner_fraction_of_target"] = MAX_DYNAMIC_RUNNER_FRACTION
    return policy


def _severity(payload: Mapping[str, Any]) -> float:
    risk = payload.get("risk")
    if isinstance(risk, Mapping):
        return float(risk.get("risk_severity") or 0.0)
    return float(payload.get("risk_severity") or 0.0)


def _independent_count(payload: Mapping[str, Any]) -> int:
    for key in ("independent_count", "independent_confirmation_count", "skilled_independent_clusters"):
        if key in payload:
            return int(payload.get(key) or 0)
    return 0


def exceptional_continuation_evidence(payload: Mapping[str, Any]) -> bool:
    """Return true only for unusually well-corroborated executable continuation.

    This gate never grants entry by itself. The underlying v5.2 chooser must still
    pass hard stops, exact quote/depth, latency, structural exitability and the
    normal forward-evidence rules.
    """
    if _severity(payload) > 0.20:
        return False
    flow = str(payload.get("flow_state") or payload.get("fomo_state") or "neutral")
    persistent = bool(payload.get("cross_venue_persistence")) or flow in {
        "entity_accumulation",
        "pre_fomo",
        "active_fomo",
    }
    broad_required = int(target_sizing_policy()["detection_intelligence.minimum_broad_independent_clusters"]) if "detection_intelligence.minimum_broad_independent_clusters" in target_sizing_policy() else 5
    corroborated = _independent_count(payload) >= max(5, broad_required)
    return bool(persistent and corroborated)


def chase_classification(chase: float | None, *, exceptional: bool) -> str:
    if chase is None or float(chase) <= NORMAL_CHASE_MAX:
        return "normal"
    if float(chase) > ABSOLUTE_CHASE_MAX:
        return "observe_only"
    return "exceptional_continuation" if exceptional else "observe_only"


def wallet_utilization_multiplier(score: ContextualWalletScore | None) -> float:
    if score is None or not score.eligible_for_strategy_influence:
        return 1.0
    if score.paired_forward_episodes < int(target_sizing_policy()["minimum_forward_samples"]):
        return 1.0
    if score.decayed_marginal_alpha <= 0.0:
        return 1.0
    return min(MAX_WALLET_UTILIZATION_MULTIPLIER, 1.0 + float(score.decayed_marginal_alpha))


def opportunity_priority_score(
    *,
    expected_log_growth: float,
    sample_count: int,
    risk_severity: float,
    wallet_multiplier: float = 1.0,
) -> float:
    evidence = min(1.0, max(0.0, float(sample_count)) / max(1, int(target_sizing_policy()["minimum_forward_samples"])))
    risk_quality = max(0.0, 1.0 - float(risk_severity))
    wallet_quality = max(1.0, min(MAX_WALLET_UTILIZATION_MULTIPLIER, float(wallet_multiplier)))
    return float(expected_log_growth) * 100.0 + evidence * 2.0 + risk_quality + (wallet_quality - 1.0) * 4.0


def rank_eligible_opportunities(candidates: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Deterministically rank candidates that are already decision-eligible."""
    eligible: list[dict[str, Any]] = []
    for item in candidates:
        if not bool(item.get("eligible")):
            continue
        copied = dict(item)
        copied["portfolio_priority_score"] = opportunity_priority_score(
            expected_log_growth=float(item.get("expected_log_growth") or 0.0),
            sample_count=int(item.get("sample_count") or 0),
            risk_severity=float(item.get("risk_severity") or 0.0),
            wallet_multiplier=float(item.get("wallet_multiplier") or 1.0),
        )
        eligible.append(copied)
    return sorted(
        eligible,
        key=lambda item: (-float(item["portfolio_priority_score"]), str(item.get("candidate_id") or "")),
    )


def _score_wallet(pre: Mapping[str, Any], profile: Mapping[str, Any]) -> ContextualWalletScore | None:
    if _WALLET_ALPHA is None:
        return None
    wallet = str(pre.get("wallet") or pre.get("trigger_wallet") or "")
    context_key = str(profile.get("context_key") or "")
    if not wallet or not context_key:
        return None
    try:
        return _WALLET_ALPHA.score(wallet, context_key)
    except Exception:
        return None


def _apply_wallet_and_priority(
    *,
    lane: str,
    pre: Mapping[str, Any],
    fraction: float,
    profiles: dict[str, Any],
) -> tuple[float, dict[str, Any]]:
    profile = dict(profiles.get(lane) or {})
    authority = dict(profile.get("v52_authority") or {})
    wallet_score = _score_wallet(pre, profile)
    multiplier = wallet_utilization_multiplier(wallet_score)
    old_target = max(0.0, float(authority.get("target_fraction") or 0.0))
    open_before = max(0.0, float(authority.get("open_fraction_before") or 0.0))
    cap = authoritative._lane_cap(lane, _severity(pre))
    new_target = min(cap, old_target * multiplier) if old_target > 0.0 else old_target
    final = max(0.0, float(fraction))
    if final > 0.0 and multiplier > 1.0 and old_target > 0.0:
        stage = str(authority.get("capture_stage") or "starter")
        if stage == "starter":
            final = min(new_target, new_target * float(_contextual_position_policy()["starter_fraction_of_target"]))
        else:
            add_cap = new_target * float(_contextual_position_policy()["max_scale_fraction_of_target_per_add"])
            final = min(add_cap, max(0.0, new_target - open_before), final * multiplier)
    priority = opportunity_priority_score(
        expected_log_growth=float(profile.get("best_expected_log_growth") or 0.0),
        sample_count=int(profile.get("sample_count") or 0),
        risk_severity=_severity(pre),
        wallet_multiplier=multiplier,
    )
    authority.update(
        {
            "target_fraction": new_target,
            "final_fraction": final,
            "wallet_target_utilization_multiplier": multiplier,
            "wallet_influence_validated": bool(wallet_score and wallet_score.eligible_for_strategy_influence),
            "wallet_forward_samples": int(wallet_score.paired_forward_episodes) if wallet_score else 0,
            "portfolio_priority_score": priority,
            "portfolio_ranking_only_after_eligibility": True,
        }
    )
    profile["v52_authority"] = authority
    profile["portfolio_priority_score"] = priority
    profiles[lane] = profile
    return final, profiles


def _solana_choose(adapter: Any, pre: dict[str, Any], *, chase: float | None = None, latency: float | None = None) -> tuple[str | None, float, dict[str, Any]]:
    if _BASE_SOLANA_CHOOSE is None:
        raise RuntimeError("v52 adaptive Solana base unavailable")
    exceptional = exceptional_continuation_evidence(pre)
    chase_state = chase_classification(chase, exceptional=exceptional)
    if chase_state == "observe_only":
        return None, 0.0, {}
    token = _EXCEPTIONAL_SCALE.set(exceptional)
    try:
        lane, fraction, profiles = _BASE_SOLANA_CHOOSE(adapter, pre, chase=chase, latency=latency)
    finally:
        _EXCEPTIONAL_SCALE.reset(token)
    copied = {key: dict(value) if isinstance(value, dict) else value for key, value in dict(profiles or {}).items()}
    if not lane or float(fraction or 0.0) <= 0.0:
        return lane, float(fraction or 0.0), copied
    final, copied = _apply_wallet_and_priority(lane=lane, pre=pre, fraction=float(fraction), profiles=copied)
    profile = dict(copied.get(lane) or {})
    auth = dict(profile.get("v52_authority") or {})
    auth["chase_classification"] = chase_state
    auth["exceptional_continuation_evidence"] = exceptional
    auth["scale_cap_fraction_of_target"] = EXCEPTIONAL_SCALE_FRACTION if exceptional else ORDINARY_SCALE_FRACTION
    profile["v52_authority"] = auth
    copied[lane] = profile
    return (lane if final > 0.0 else None), final, copied


def _fomo_decision(adapter: Any, *, observation: dict[str, Any], trial: dict[str, Any]) -> dict[str, Any]:
    if _BASE_FOMO_DECISION is None:
        raise RuntimeError("v52 adaptive FOMO base unavailable")
    state = fomo_paper._safe_json(observation.get("state_json"))
    evidence = {**state, **trial, "flow_state": str(state.get("state") or "unknown")}
    exceptional = exceptional_continuation_evidence(evidence)
    token = _EXCEPTIONAL_SCALE.set(exceptional)
    try:
        result = dict(_BASE_FOMO_DECISION(adapter, observation=observation, trial=trial))
    finally:
        _EXCEPTIONAL_SCALE.reset(token)
    authority = dict(result.get("v52_authority") or (result.get("profile") or {}).get("v52_authority") or {})
    authority["exceptional_continuation_evidence"] = exceptional
    authority["scale_cap_fraction_of_target"] = EXCEPTIONAL_SCALE_FRACTION if exceptional else ORDINARY_SCALE_FRACTION
    authority["portfolio_priority_score"] = opportunity_priority_score(
        expected_log_growth=float((result.get("profile") or {}).get("best_expected_log_growth") or 0.0),
        sample_count=int((result.get("profile") or {}).get("sample_count") or 0),
        risk_severity=_severity(evidence),
    )
    result["v52_authority"] = authority
    if isinstance(result.get("profile"), dict):
        result["profile"] = dict(result["profile"])
        result["profile"]["v52_authority"] = authority
    return result


def _robinhood_choose(self: Any, **kwargs: Any) -> tuple[str | None, float, dict[str, Any]]:
    if _BASE_ROBINHOOD_CHOOSE is None:
        raise RuntimeError("v52 adaptive Robinhood base unavailable")
    exceptional = exceptional_continuation_evidence(kwargs)
    token = _EXCEPTIONAL_SCALE.set(exceptional)
    try:
        lane, fraction, profiles = _BASE_ROBINHOOD_CHOOSE(self, **kwargs)
    finally:
        _EXCEPTIONAL_SCALE.reset(token)
    copied = {key: dict(value) if isinstance(value, dict) else value for key, value in dict(profiles or {}).items()}
    if lane and lane in copied:
        profile = dict(copied[lane])
        authority = dict(profile.get("v52_authority") or {})
        authority["exceptional_continuation_evidence"] = exceptional
        authority["scale_cap_fraction_of_target"] = EXCEPTIONAL_SCALE_FRACTION if exceptional else ORDINARY_SCALE_FRACTION
        authority["portfolio_priority_score"] = opportunity_priority_score(
            expected_log_growth=float(profile.get("best_expected_log_growth") or 0.0),
            sample_count=int(profile.get("sample_count") or 0),
            risk_severity=_severity(kwargs),
        )
        profile["v52_authority"] = authority
        copied[lane] = profile
    return lane, float(fraction or 0.0), copied


async def _robinhood_settle(self: Any, trial: dict[str, Any]) -> None:
    if _BASE_ROBINHOOD_SETTLE is None:
        raise RuntimeError("v52 adaptive Robinhood settle base unavailable")
    runner = BASE_RUNNER_FRACTION
    try:
        trial_id = int(trial["id"])
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT p.last_flow_state,p.last_risk_severity,p.derisk_stage "
                "FROM v52_robinhood_position_lots l JOIN v52_robinhood_positions p ON p.id=l.position_id "
                "WHERE l.trial_id=? LIMIT 1",
                (trial_id,),
            ).fetchone()
        if row is not None and int(row["derisk_stage"] or 0) >= 2 and str(row["last_flow_state"] or "") == "active_fomo" and float(row["last_risk_severity"] or 0.0) <= 0.20:
            runner = MAX_DYNAMIC_RUNNER_FRACTION
    except Exception:
        runner = BASE_RUNNER_FRACTION
    token = _RUNNER_FRACTION.set(runner)
    try:
        await _BASE_ROBINHOOD_SETTLE(self, trial)
    finally:
        _RUNNER_FRACTION.reset(token)


def install_v52_adaptive_continuation_refinement(wallet_alpha: WalletAlphaRefinementLedger) -> None:
    global _INSTALLED, _WALLET_ALPHA, _BASE_SOLANA_CHOOSE, _BASE_FOMO_DECISION
    global _BASE_ROBINHOOD_CHOOSE, _BASE_ROBINHOOD_SETTLE
    if _INSTALLED:
        _WALLET_ALPHA = wallet_alpha
        return
    from .robinhood_chain_paper import RobinhoodChainPaperPlane

    _WALLET_ALPHA = wallet_alpha
    _BASE_SOLANA_CHOOSE = solana_strategy._choose_lane_and_fraction
    _BASE_FOMO_DECISION = fomo_paper._paper_decision
    _BASE_ROBINHOOD_CHOOSE = RobinhoodChainPaperPlane._v5_choose_lane_fraction
    _BASE_ROBINHOOD_SETTLE = RobinhoodChainPaperPlane._settle_one

    authoritative.position_policy = _contextual_position_policy
    robinhood_lifecycle.position_policy = _contextual_position_policy
    solana_strategy._choose_lane_and_fraction = _solana_choose
    fomo_paper._paper_decision = _fomo_decision
    RobinhoodChainPaperPlane._v5_choose_lane_fraction = _robinhood_choose  # type: ignore[method-assign]
    RobinhoodChainPaperPlane._settle_one = _robinhood_settle  # type: ignore[method-assign]

    for wrapper in (solana_strategy._choose_lane_and_fraction, fomo_paper._paper_decision, RobinhoodChainPaperPlane._v5_choose_lane_fraction):
        setattr(wrapper, "_roi_v52_final_authority", True)
        setattr(wrapper, "_roi_v52_adaptive_continuation_refinement", True)
    setattr(RobinhoodChainPaperPlane._settle_one, "_roi_v52_position_lifecycle", True)
    setattr(RobinhoodChainPaperPlane._settle_one, "_roi_v52_adaptive_continuation_refinement", True)
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": REFINEMENT_VERSION,
        "installed": _INSTALLED,
        "ordinary_scale_fraction": ORDINARY_SCALE_FRACTION,
        "exceptional_scale_fraction": EXCEPTIONAL_SCALE_FRACTION,
        "base_runner_fraction": BASE_RUNNER_FRACTION,
        "max_dynamic_runner_fraction": MAX_DYNAMIC_RUNNER_FRACTION,
        "normal_chase_max_fraction": NORMAL_CHASE_MAX,
        "absolute_chase_max_fraction": ABSOLUTE_CHASE_MAX,
        "wallet_minimum_forward_samples": int(target_sizing_policy()["minimum_forward_samples"]),
        "wallet_max_target_utilization_multiplier": MAX_WALLET_UTILIZATION_MULTIPLIER,
        "portfolio_ranking_only_after_eligibility": True,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "ABSOLUTE_CHASE_MAX",
    "BASE_RUNNER_FRACTION",
    "EXCEPTIONAL_SCALE_FRACTION",
    "MAX_DYNAMIC_RUNNER_FRACTION",
    "MAX_WALLET_UTILIZATION_MULTIPLIER",
    "NORMAL_CHASE_MAX",
    "ORDINARY_SCALE_FRACTION",
    "REFINEMENT_VERSION",
    "chase_classification",
    "exceptional_continuation_evidence",
    "install_v52_adaptive_continuation_refinement",
    "opportunity_priority_score",
    "rank_eligible_opportunities",
    "status",
    "wallet_utilization_multiplier",
]
