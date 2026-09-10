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
    authority as strategy_authority,
    detection_policy,
    execution_policy,
    position_policy as canonical_position_policy,
    target_sizing_policy,
)
from .v52_wallet_alpha_refinement import ContextualWalletScore, WalletAlphaRefinementLedger

REFINEMENT_VERSION = "v52-adaptive-continuation-refinement-v3-direct-profit-confidence"
ORDINARY_SCALE_FRACTION = 0.25
STRONG_SCALE_FRACTION = 0.50
EXCEPTIONAL_SCALE_FRACTION = 0.75
ORDINARY_STARTER_FRACTION = 0.25
STRONG_STARTER_FRACTION = 0.50
EXCEPTIONAL_STARTER_FRACTION = 0.75
MIN_DYNAMIC_RUNNER_FRACTION = 0.05
BASE_RUNNER_FRACTION = 0.10
MAX_DYNAMIC_RUNNER_FRACTION = 0.30
NORMAL_CHASE_MAX = 0.40
ABSOLUTE_CHASE_MAX = 0.80
MAX_WALLET_UTILIZATION_MULTIPLIER = 1.50
PARTIAL_WALLET_MIN_SAMPLES = 8
STRONG_WALLET_MIN_SAMPLES = 15

_INSTALLED = False
_WALLET_ALPHA: WalletAlphaRefinementLedger | None = None
_BASE_SOLANA_CHOOSE: Callable[..., Any] | None = None
_BASE_FOMO_DECISION: Callable[..., Any] | None = None
_BASE_ROBINHOOD_CHOOSE: Callable[..., Any] | None = None
_BASE_ROBINHOOD_SETTLE: Callable[..., Any] | None = None

_SCALE_FRACTION: ContextVar[float] = ContextVar("v52_scale_fraction", default=ORDINARY_SCALE_FRACTION)
_EXCEPTIONAL_SCALE: ContextVar[bool] = ContextVar("v52_exceptional_scale", default=False)
_RUNNER_FRACTION: ContextVar[float] = ContextVar("v52_runner_fraction", default=BASE_RUNNER_FRACTION)


def _profit_policy() -> dict[str, Any]:
    raw = strategy_authority().get("profit_confidence_engine")
    return dict(raw) if isinstance(raw, Mapping) else {}


def _policy_float(key: str, default: float) -> float:
    try:
        return float(_profit_policy().get(key, default))
    except (TypeError, ValueError):
        return float(default)


def _policy_int(key: str, default: int) -> int:
    try:
        return int(_profit_policy().get(key, default))
    except (TypeError, ValueError):
        return int(default)


def _contextual_position_policy(*args: Any, **kwargs: Any) -> dict[str, Any]:
    policy = dict(canonical_position_policy(*args, **kwargs))
    scale = max(ORDINARY_SCALE_FRACTION, min(EXCEPTIONAL_SCALE_FRACTION, float(_SCALE_FRACTION.get())))
    runner = max(
        MIN_DYNAMIC_RUNNER_FRACTION,
        min(MAX_DYNAMIC_RUNNER_FRACTION, float(_RUNNER_FRACTION.get())),
    )
    policy["max_scale_fraction_of_target_per_add"] = scale
    policy["runner_fraction_of_target"] = runner
    policy["ordinary_max_scale_fraction_of_target_per_add"] = ORDINARY_SCALE_FRACTION
    policy["strong_max_scale_fraction_of_target_per_add"] = STRONG_SCALE_FRACTION
    policy["exceptional_max_scale_fraction_of_target_per_add"] = EXCEPTIONAL_SCALE_FRACTION
    policy["min_dynamic_runner_fraction_of_target"] = MIN_DYNAMIC_RUNNER_FRACTION
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
    if _severity(payload) > _policy_float("exceptional_max_risk_severity", 0.20):
        return False
    flow = str(payload.get("flow_state") or payload.get("fomo_state") or "neutral")
    persistent = bool(payload.get("cross_venue_persistence")) or flow in {
        "entity_accumulation",
        "pre_fomo",
        "active_fomo",
    }
    broad_required = int(detection_policy()["minimum_broad_independent_clusters"])
    corroborated = _independent_count(payload) >= max(5, broad_required)
    return bool(persistent and corroborated)


def strong_continuation_evidence(payload: Mapping[str, Any]) -> bool:
    if exceptional_continuation_evidence(payload):
        return True
    if _severity(payload) > _policy_float("strong_max_risk_severity", 0.30):
        return False
    flow = str(payload.get("flow_state") or payload.get("fomo_state") or "neutral")
    persistent = bool(payload.get("cross_venue_persistence")) or flow in {
        "entity_accumulation",
        "pre_fomo",
        "active_fomo",
    }
    skilled_required = int(detection_policy()["minimum_skilled_independent_clusters"])
    return bool(persistent and _independent_count(payload) >= skilled_required)


def chase_classification(chase: float | None, *, exceptional: bool) -> str:
    if chase is None or float(chase) <= NORMAL_CHASE_MAX:
        return "normal"
    if float(chase) > ABSOLUTE_CHASE_MAX:
        return "observe_only"
    return "exceptional_continuation" if exceptional else "observe_only"


def wallet_confidence(score: ContextualWalletScore | None) -> float:
    """Continuous forward-only confidence; never grants entry by itself."""
    if score is None:
        return 0.0
    minimum = _policy_int("partial_wallet_influence_min_samples", PARTIAL_WALLET_MIN_SAMPLES)
    full = max(minimum + 1, int(target_sizing_policy()["minimum_forward_samples"]))
    n = int(score.paired_forward_episodes)
    if n < minimum or score.decayed_marginal_alpha <= 0.0 or score.copyability_rate < 0.80:
        return 0.0
    sample_confidence = min(1.0, max(0.0, (n - minimum + 1) / max(1, full - minimum + 1)))
    prior = max(1.0, _policy_float("uncertainty_prior_samples", 12.0))
    shrinkage = n / (n + prior)
    copyability_quality = min(1.0, max(0.0, (score.copyability_rate - 0.80) / 0.20))
    mae_scale = max(1e-6, _policy_float("wallet_mae_penalty_scale", 0.35))
    mae_quality = max(0.0, 1.0 - min(1.0, score.decayed_executable_mae / mae_scale))
    capture = score.decayed_capture_ratio
    capture_quality = 0.5 if capture is None else min(1.0, max(0.0, float(capture)))
    quality = 0.35 + 0.25 * copyability_quality + 0.20 * mae_quality + 0.20 * capture_quality
    return min(1.0, max(0.0, sample_confidence * shrinkage * quality))


def wallet_utilization_multiplier(score: ContextualWalletScore | None) -> float:
    confidence = wallet_confidence(score)
    if confidence <= 0.0 or score is None:
        return 1.0
    alpha = max(0.0, float(score.decayed_marginal_alpha))
    max_boost = MAX_WALLET_UTILIZATION_MULTIPLIER - 1.0
    return 1.0 + min(max_boost, alpha * confidence)


def _profile_sample_count(profile: Mapping[str, Any]) -> int:
    return int(profile.get("sample_count") or 0)


def _profile_growth(profile: Mapping[str, Any]) -> float:
    try:
        return float(profile.get("best_expected_log_growth") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _profile_trimmed_positive(profile: Mapping[str, Any]) -> bool:
    raw = profile.get("trimmed_mean_ex_best")
    if raw is None:
        raw = profile.get("trimmed_mean_residual_roi_ex_best_1_pct")
        if raw is not None:
            try:
                return float(raw) > 0.0
            except (TypeError, ValueError):
                return False
    if raw is None:
        return _profile_growth(profile) > 0.0
    try:
        return float(raw) > 0.0
    except (TypeError, ValueError):
        return False


def profile_confidence(profile: Mapping[str, Any]) -> float:
    """Confidence for direct target adaptation from same-stream forward evidence."""
    n = _profile_sample_count(profile)
    minimum = _policy_int("adaptive_target_min_samples", PARTIAL_WALLET_MIN_SAMPLES)
    full = max(minimum + 1, int(target_sizing_policy()["minimum_forward_samples"]))
    if n < minimum or _profile_growth(profile) <= 0.0 or not _profile_trimmed_positive(profile):
        return 0.0
    drawdown = profile.get("max_drawdown_at_best_fraction")
    if drawdown is None:
        drawdown = profile.get("max_drawdown")
    if drawdown is not None and float(drawdown) > _policy_float("max_forward_drawdown", 0.35):
        return 0.0
    shortfall = profile.get("expected_shortfall_20")
    if shortfall is not None and float(shortfall) <= _policy_float("min_expected_shortfall_20", -0.85):
        return 0.0
    concentration = profile.get("winner_concentration")
    concentration_limit = _policy_float("max_winner_concentration", 0.80)
    if concentration is not None and float(concentration) > concentration_limit and n < 60:
        return 0.0
    sample_confidence = min(1.0, max(0.0, (n - minimum + 1) / max(1, full - minimum + 1)))
    prior = max(1.0, _policy_float("uncertainty_prior_samples", 12.0))
    shrinkage = n / (n + prior)
    return min(1.0, max(0.0, sample_confidence * shrinkage))


def _best_fraction(profile: Mapping[str, Any]) -> float:
    for key in ("best_fraction", "best_paper_position_fraction"):
        if profile.get(key) is not None:
            try:
                return max(0.0, float(profile[key]))
            except (TypeError, ValueError):
                return 0.0
    return 0.0


def adaptive_target_fraction(
    *,
    current_target: float,
    profile: Mapping[str, Any],
    cap: float,
    wallet_multiplier: float = 1.0,
) -> float:
    """Blend bootstrap sizing toward robust forward Kelly-grid sizing, capped by lane."""
    current = max(0.0, float(current_target))
    ceiling = max(0.0, float(cap))
    if current <= 0.0 or ceiling <= 0.0:
        return 0.0
    confidence = profile_confidence(profile)
    best = min(ceiling, _best_fraction(profile))
    if confidence > 0.0 and best > 0.0 and best >= current:
        current = current + confidence * (best - current)
    return min(ceiling, current * max(1.0, float(wallet_multiplier)))


def conviction_tier(
    *,
    pre: Mapping[str, Any],
    profile: Mapping[str, Any],
    wallet_score: ContextualWalletScore | None,
) -> str:
    confidence = profile_confidence(profile)
    wallet_c = wallet_confidence(wallet_score)
    if exceptional_continuation_evidence(pre) and (confidence >= 0.45 or wallet_c >= 0.45):
        return "exceptional"
    if strong_continuation_evidence(pre) and (confidence >= 0.20 or wallet_c >= 0.20):
        return "strong"
    return "ordinary"


def _starter_fraction(tier: str) -> float:
    if tier == "exceptional":
        return EXCEPTIONAL_STARTER_FRACTION
    if tier == "strong":
        return STRONG_STARTER_FRACTION
    return ORDINARY_STARTER_FRACTION


def _scale_fraction(tier: str) -> float:
    if tier == "exceptional":
        return EXCEPTIONAL_SCALE_FRACTION
    if tier == "strong":
        return STRONG_SCALE_FRACTION
    return ORDINARY_SCALE_FRACTION


def opportunity_priority_score(
    *,
    expected_log_growth: float,
    sample_count: int,
    risk_severity: float,
    wallet_multiplier: float = 1.0,
) -> float:
    evidence = min(
        1.0,
        max(0.0, float(sample_count))
        / max(1, int(target_sizing_policy()["minimum_forward_samples"])),
    )
    risk_quality = max(0.0, 1.0 - float(risk_severity))
    wallet_quality = max(
        1.0,
        min(MAX_WALLET_UTILIZATION_MULTIPLIER, float(wallet_multiplier)),
    )
    return (
        float(expected_log_growth) * 100.0
        + evidence * 2.0
        + risk_quality
        + (wallet_quality - 1.0) * 4.0
    )


def rank_eligible_opportunities(candidates: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
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
        key=lambda item: (
            -float(item["portfolio_priority_score"]),
            str(item.get("candidate_id") or ""),
        ),
    )


def _score_wallet(
    pre: Mapping[str, Any],
    profile: Mapping[str, Any],
) -> ContextualWalletScore | None:
    if _WALLET_ALPHA is None:
        return None
    wallet = str(pre.get("wallet") or pre.get("trigger_wallet") or pre.get("entity") or "")
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
    auth = dict(profile.get("v52_authority") or {})
    wallet_score = _score_wallet(pre, profile)
    multiplier = wallet_utilization_multiplier(wallet_score)
    old_target = max(0.0, float(auth.get("target_fraction") or 0.0))
    open_before = max(0.0, float(auth.get("open_fraction_before") or 0.0))
    cap = authoritative._lane_cap(lane, _severity(pre))
    new_target = adaptive_target_fraction(
        current_target=old_target,
        profile=profile,
        cap=cap,
        wallet_multiplier=multiplier,
    )
    tier = conviction_tier(pre=pre, profile=profile, wallet_score=wallet_score)
    final = max(0.0, float(fraction))
    if final > 0.0 and old_target > 0.0:
        stage = str(auth.get("capture_stage") or "starter")
        if stage == "starter":
            final = min(new_target, new_target * _starter_fraction(tier))
        else:
            add_cap = new_target * _scale_fraction(tier)
            final = min(add_cap, max(0.0, new_target - open_before))
    priority = opportunity_priority_score(
        expected_log_growth=_profile_growth(profile),
        sample_count=_profile_sample_count(profile),
        risk_severity=_severity(pre),
        wallet_multiplier=multiplier,
    )
    mode = "none"
    if wallet_score is not None and wallet_confidence(wallet_score) > 0.0:
        mode = "full" if wallet_score.eligible_for_strategy_influence else "partial"
    auth.update(
        {
            "target_fraction": new_target,
            "final_fraction": final,
            "canonical_direct_profit_confidence_enabled": True,
            "conviction_tier": tier,
            "starter_fraction_of_target_applied": _starter_fraction(tier),
            "scale_fraction_of_target_applied": _scale_fraction(tier),
            "forward_profile_confidence": profile_confidence(profile),
            "wallet_target_utilization_multiplier": multiplier,
            "wallet_influence_mode": mode,
            "wallet_influence_validated": bool(
                wallet_score and wallet_score.eligible_for_strategy_influence
            ),
            "wallet_forward_samples": (
                int(wallet_score.paired_forward_episodes) if wallet_score else 0
            ),
            "wallet_confidence": wallet_confidence(wallet_score),
            "portfolio_priority_score": priority,
            "portfolio_ranking_only_after_eligibility": True,
            "lane_cap_preserved": cap,
        }
    )
    profile["v52_authority"] = auth
    profile["portfolio_priority_score"] = priority
    profiles[lane] = profile
    return final, profiles


def _solana_choose(
    adapter: Any,
    pre: dict[str, Any],
    *,
    chase: float | None = None,
    latency: float | None = None,
) -> tuple[str | None, float, dict[str, Any]]:
    if _BASE_SOLANA_CHOOSE is None:
        raise RuntimeError("v52 adaptive Solana base unavailable")
    exceptional = exceptional_continuation_evidence(pre)
    chase_state = chase_classification(chase, exceptional=exceptional)
    if chase_state == "observe_only":
        return None, 0.0, {}
    base_chase = NORMAL_CHASE_MAX if chase_state == "exceptional_continuation" else chase
    scale_token = _SCALE_FRACTION.set(
        EXCEPTIONAL_SCALE_FRACTION if exceptional else ORDINARY_SCALE_FRACTION
    )
    exceptional_token = _EXCEPTIONAL_SCALE.set(exceptional)
    try:
        lane, fraction, profiles = _BASE_SOLANA_CHOOSE(
            adapter,
            pre,
            chase=base_chase,
            latency=latency,
        )
        copied = {
            key: dict(value) if isinstance(value, dict) else value
            for key, value in dict(profiles or {}).items()
        }
        if not lane or float(fraction or 0.0) <= 0.0:
            return lane, float(fraction or 0.0), copied
        final, copied = _apply_wallet_and_priority(
            lane=lane,
            pre=pre,
            fraction=float(fraction),
            profiles=copied,
        )
    finally:
        _EXCEPTIONAL_SCALE.reset(exceptional_token)
        _SCALE_FRACTION.reset(scale_token)
    profile = dict(copied.get(lane) or {})
    auth = dict(profile.get("v52_authority") or {})
    auth["chase_classification"] = chase_state
    auth["adaptive_observed_chase_fraction"] = float(chase) if chase is not None else None
    auth["canonical_chase_boundary_preserved"] = float(
        execution_policy()["chase_observe_only_above_fraction"]
    )
    auth["exceptional_continuation_evidence"] = exceptional
    profile["v52_authority"] = auth
    copied[lane] = profile
    return (lane if final > 0.0 else None), final, copied


def _fomo_decision(
    adapter: Any,
    *,
    observation: dict[str, Any],
    trial: dict[str, Any],
) -> dict[str, Any]:
    if _BASE_FOMO_DECISION is None:
        raise RuntimeError("v52 adaptive FOMO base unavailable")
    state = fomo_paper._safe_json(observation.get("state_json"))
    evidence = {**state, **trial, "flow_state": str(state.get("state") or "unknown")}
    exceptional = exceptional_continuation_evidence(evidence)
    scale_token = _SCALE_FRACTION.set(
        EXCEPTIONAL_SCALE_FRACTION if exceptional else ORDINARY_SCALE_FRACTION
    )
    exceptional_token = _EXCEPTIONAL_SCALE.set(exceptional)
    try:
        result = dict(
            _BASE_FOMO_DECISION(adapter, observation=observation, trial=trial)
        )
    finally:
        _EXCEPTIONAL_SCALE.reset(exceptional_token)
        _SCALE_FRACTION.reset(scale_token)
    profile = dict(result.get("profile") or {})
    auth = dict(
        result.get("v52_authority")
        or profile.get("v52_authority")
        or {}
    )
    if (
        str(result.get("decision") or "").startswith("paper_enter")
        and float(result.get("position_fraction") or 0.0) > 0.0
    ):
        current = float(result["position_fraction"])
        best = _best_fraction(profile)
        confidence = profile_confidence(profile)
        desired = current
        if best > current and confidence > 0.0:
            desired = current + confidence * (best - current)
        cap = float(target_sizing_policy()["fomo_max_target_fraction"])
        available = max(
            0.0,
            1.0 - float(fomo_paper._open_position_fraction(adapter)),
        )
        result["position_fraction"] = min(cap, available, max(current, desired))
    auth["exceptional_continuation_evidence"] = exceptional
    auth["canonical_direct_profit_confidence_enabled"] = True
    auth["forward_profile_confidence"] = profile_confidence(profile)
    auth["portfolio_priority_score"] = opportunity_priority_score(
        expected_log_growth=_profile_growth(profile),
        sample_count=_profile_sample_count(profile),
        risk_severity=_severity(evidence),
    )
    result["v52_authority"] = auth
    if isinstance(result.get("profile"), dict):
        result["profile"] = profile
        result["profile"]["v52_authority"] = auth
    return result


def _robinhood_choose(
    self: Any,
    **kwargs: Any,
) -> tuple[str | None, float, dict[str, Any]]:
    if _BASE_ROBINHOOD_CHOOSE is None:
        raise RuntimeError("v52 adaptive Robinhood base unavailable")
    exceptional = exceptional_continuation_evidence(kwargs)
    scale_token = _SCALE_FRACTION.set(
        EXCEPTIONAL_SCALE_FRACTION if exceptional else ORDINARY_SCALE_FRACTION
    )
    exceptional_token = _EXCEPTIONAL_SCALE.set(exceptional)
    try:
        lane, fraction, profiles = _BASE_ROBINHOOD_CHOOSE(self, **kwargs)
    finally:
        _EXCEPTIONAL_SCALE.reset(exceptional_token)
        _SCALE_FRACTION.reset(scale_token)
    copied = {
        key: dict(value) if isinstance(value, dict) else value
        for key, value in dict(profiles or {}).items()
    }
    if lane and float(fraction or 0.0) > 0.0 and lane in copied:
        final, copied = _apply_wallet_and_priority(
            lane=lane,
            pre=kwargs,
            fraction=float(fraction),
            profiles=copied,
        )
        fraction = final
    if lane and lane in copied:
        profile = dict(copied[lane])
        auth = dict(profile.get("v52_authority") or {})
        auth["exceptional_continuation_evidence"] = exceptional
        auth["canonical_direct_profit_confidence_enabled"] = True
        profile["v52_authority"] = auth
        copied[lane] = profile
        token = str(getattr(self, "_roi_v52_candidate_token", "") or "")
        pending_map = robinhood_lifecycle._pending_map(self)
        if token and token in pending_map:
            pending = dict(pending_map[token])
            pending["target_fraction"] = float(
                auth.get("target_fraction")
                or pending.get("target_fraction")
                or 0.0
            )
            pending["canonical_direct_profit_confidence_enabled"] = True
            pending_map[token] = pending
    return (
        lane if float(fraction or 0.0) > 0.0 else None,
        float(fraction or 0.0),
        copied,
    )


async def _robinhood_settle(self: Any, trial: dict[str, Any]) -> None:
    if _BASE_ROBINHOOD_SETTLE is None:
        raise RuntimeError("v52 adaptive Robinhood settle base unavailable")
    runner = BASE_RUNNER_FRACTION
    try:
        trial_id = int(trial["id"])
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT p.last_flow_state,p.last_risk_severity,p.derisk_stage "
                "FROM v52_robinhood_position_lots l "
                "JOIN v52_robinhood_positions p ON p.id=l.position_id "
                "WHERE l.trial_id=? LIMIT 1",
                (trial_id,),
            ).fetchone()
        if row is not None and int(row["derisk_stage"] or 0) >= 2:
            flow = str(row["last_flow_state"] or "")
            risk = float(row["last_risk_severity"] or 0.0)
            if flow == "active_fomo" and risk <= 0.20:
                runner = MAX_DYNAMIC_RUNNER_FRACTION
            elif flow in {"entity_accumulation", "pre_fomo"} and risk <= 0.30:
                runner = STRONG_STARTER_FRACTION * MAX_DYNAMIC_RUNNER_FRACTION
            elif flow in {"exhaustion", "distribution"} or risk >= 0.55:
                runner = MIN_DYNAMIC_RUNNER_FRACTION
    except Exception:
        runner = BASE_RUNNER_FRACTION
    token = _RUNNER_FRACTION.set(runner)
    try:
        await _BASE_ROBINHOOD_SETTLE(self, trial)
    finally:
        _RUNNER_FRACTION.reset(token)


def _preserve_wrapper_lineage(wrapper: Any, predecessor: Any) -> None:
    if not callable(predecessor):
        raise RuntimeError("v52 adaptive predecessor unavailable")
    setattr(wrapper, "__wrapped__", predecessor)
    predecessor_module = getattr(predecessor, "__module__", None)
    if predecessor_module:
        setattr(wrapper, "__module__", predecessor_module)
    for name, value in vars(predecessor).items():
        if name.startswith("_roi_"):
            setattr(wrapper, name, value)


def install_v52_adaptive_continuation_refinement(
    wallet_alpha: WalletAlphaRefinementLedger,
) -> None:
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

    _preserve_wrapper_lineage(_solana_choose, _BASE_SOLANA_CHOOSE)
    _preserve_wrapper_lineage(_fomo_decision, _BASE_FOMO_DECISION)
    _preserve_wrapper_lineage(_robinhood_choose, _BASE_ROBINHOOD_CHOOSE)
    _preserve_wrapper_lineage(_robinhood_settle, _BASE_ROBINHOOD_SETTLE)

    authoritative.position_policy = _contextual_position_policy
    robinhood_lifecycle.position_policy = _contextual_position_policy
    solana_strategy._choose_lane_and_fraction = _solana_choose
    fomo_paper._paper_decision = _fomo_decision
    RobinhoodChainPaperPlane._v5_choose_lane_fraction = _robinhood_choose  # type: ignore[method-assign]
    RobinhoodChainPaperPlane._settle_one = _robinhood_settle  # type: ignore[method-assign]

    for wrapper in (
        solana_strategy._choose_lane_and_fraction,
        fomo_paper._paper_decision,
        RobinhoodChainPaperPlane._v5_choose_lane_fraction,
    ):
        setattr(wrapper, "_roi_v52_final_authority", True)
        setattr(wrapper, "_roi_v52_adaptive_continuation_refinement", True)
        setattr(wrapper, "_roi_v52_direct_profit_confidence", True)
    setattr(RobinhoodChainPaperPlane._settle_one, "_roi_v52_position_lifecycle", True)
    setattr(
        RobinhoodChainPaperPlane._settle_one,
        "_roi_v52_adaptive_continuation_refinement",
        True,
    )
    setattr(
        RobinhoodChainPaperPlane._settle_one,
        "_roi_v52_direct_profit_confidence",
        True,
    )
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": REFINEMENT_VERSION,
        "installed": _INSTALLED,
        "canonical_direct_profit_confidence_enabled": True,
        "ordinary_starter_fraction": ORDINARY_STARTER_FRACTION,
        "strong_starter_fraction": STRONG_STARTER_FRACTION,
        "exceptional_starter_fraction": EXCEPTIONAL_STARTER_FRACTION,
        "ordinary_scale_fraction": ORDINARY_SCALE_FRACTION,
        "strong_scale_fraction": STRONG_SCALE_FRACTION,
        "exceptional_scale_fraction": EXCEPTIONAL_SCALE_FRACTION,
        "min_dynamic_runner_fraction": MIN_DYNAMIC_RUNNER_FRACTION,
        "base_runner_fraction": BASE_RUNNER_FRACTION,
        "max_dynamic_runner_fraction": MAX_DYNAMIC_RUNNER_FRACTION,
        "normal_chase_max_fraction": NORMAL_CHASE_MAX,
        "absolute_chase_max_fraction": ABSOLUTE_CHASE_MAX,
        "canonical_chase_observe_only_fraction": float(
            execution_policy()["chase_observe_only_above_fraction"]
        ),
        "exceptional_chase_is_overlay_only": True,
        "wrapper_lineage_preserved": True,
        "wallet_partial_influence_min_samples": _policy_int(
            "partial_wallet_influence_min_samples",
            PARTIAL_WALLET_MIN_SAMPLES,
        ),
        "wallet_minimum_forward_samples_for_full_influence": int(
            target_sizing_policy()["minimum_forward_samples"]
        ),
        "wallet_max_target_utilization_multiplier": MAX_WALLET_UTILIZATION_MULTIPLIER,
        "adaptive_target_min_samples": _policy_int(
            "adaptive_target_min_samples",
            PARTIAL_WALLET_MIN_SAMPLES,
        ),
        "portfolio_ranking_only_after_eligibility": True,
        "lane_caps_preserved": True,
        "exact_quote_and_exit_depth_gates_preserved": True,
        "averaging_down_allowed": False,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "ABSOLUTE_CHASE_MAX",
    "BASE_RUNNER_FRACTION",
    "EXCEPTIONAL_SCALE_FRACTION",
    "EXCEPTIONAL_STARTER_FRACTION",
    "MAX_DYNAMIC_RUNNER_FRACTION",
    "MAX_WALLET_UTILIZATION_MULTIPLIER",
    "MIN_DYNAMIC_RUNNER_FRACTION",
    "NORMAL_CHASE_MAX",
    "ORDINARY_SCALE_FRACTION",
    "ORDINARY_STARTER_FRACTION",
    "PARTIAL_WALLET_MIN_SAMPLES",
    "REFINEMENT_VERSION",
    "STRONG_SCALE_FRACTION",
    "STRONG_STARTER_FRACTION",
    "adaptive_target_fraction",
    "chase_classification",
    "conviction_tier",
    "exceptional_continuation_evidence",
    "install_v52_adaptive_continuation_refinement",
    "opportunity_priority_score",
    "profile_confidence",
    "rank_eligible_opportunities",
    "status",
    "strong_continuation_evidence",
    "wallet_confidence",
    "wallet_utilization_multiplier",
]
