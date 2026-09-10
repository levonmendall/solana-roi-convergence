from __future__ import annotations

import pytest

from solana_roi.strategy_v52_authority import authority
from solana_roi.v52_adaptive_continuation_refinement import (
    ABSOLUTE_CHASE_MAX,
    BASE_RUNNER_FRACTION,
    EXCEPTIONAL_SCALE_FRACTION,
    EXCEPTIONAL_STARTER_FRACTION,
    MAX_DYNAMIC_RUNNER_FRACTION,
    MAX_WALLET_UTILIZATION_MULTIPLIER,
    MIN_DYNAMIC_RUNNER_FRACTION,
    NORMAL_CHASE_MAX,
    ORDINARY_SCALE_FRACTION,
    ORDINARY_STARTER_FRACTION,
    PARTIAL_WALLET_MIN_SAMPLES,
    STRONG_SCALE_FRACTION,
    STRONG_STARTER_FRACTION,
    _RUNNER_FRACTION,
    _SCALE_FRACTION,
    _contextual_position_policy,
    adaptive_target_fraction,
    chase_classification,
    conviction_tier,
    exceptional_continuation_evidence,
    opportunity_priority_score,
    profile_confidence,
    rank_eligible_opportunities,
    status,
    strong_continuation_evidence,
    wallet_confidence,
    wallet_utilization_multiplier,
)
from solana_roi.v52_wallet_alpha_refinement import ContextualWalletScore


def _wallet_score(
    *,
    episodes: int,
    alpha: float,
    eligible: bool,
    capture: float = 0.50,
    mae: float = 0.05,
    copyability: float = 1.0,
) -> ContextualWalletScore:
    blockers = () if eligible else ("insufficient_forward_episodes",)
    return ContextualWalletScore(
        wallet="wallet",
        context_key="context",
        paired_forward_episodes=episodes,
        effective_episode_weight=float(episodes),
        decayed_marginal_alpha=alpha,
        decayed_capture_ratio=capture,
        decayed_executable_mae=mae,
        copyability_rate=copyability,
        eligible_for_strategy_influence=eligible,
        blockers=blockers,
    )


def _profile(*, samples: int, best: float = 0.10, growth: float = 0.03) -> dict[str, float | int]:
    return {
        "sample_count": samples,
        "best_fraction": best,
        "best_expected_log_growth": growth,
        "trimmed_mean_ex_best": 0.20,
        "expected_shortfall_20": -0.25,
        "winner_concentration": 0.50,
        "max_drawdown_at_best_fraction": 0.10,
    }


def test_manifest_preserves_hard_authority_and_enables_direct_profit_confidence() -> None:
    payload = authority()
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
    assert payload["execution"]["latency_hard_max_seconds"] == 20.0
    assert payload["execution"]["chase_observe_only_above_fraction"] == NORMAL_CHASE_MAX
    assert payload["position_management"]["minimum_exit_depth_coverage_ratio"] == 2.0
    assert payload["position_management"]["averaging_down_allowed"] is False
    assert payload["profit_confidence_engine"]["canonical_direct_enabled"] is True
    assert payload["profit_confidence_engine"]["partial_wallet_influence_may_grant_entry"] is False
    assert payload["profit_confidence_engine"]["lane_caps_may_be_exceeded"] is False

    overlay = status()
    assert overlay["canonical_direct_profit_confidence_enabled"] is True
    assert overlay["ordinary_starter_fraction"] == ORDINARY_STARTER_FRACTION
    assert overlay["strong_starter_fraction"] == STRONG_STARTER_FRACTION
    assert overlay["exceptional_starter_fraction"] == EXCEPTIONAL_STARTER_FRACTION
    assert overlay["ordinary_scale_fraction"] == ORDINARY_SCALE_FRACTION
    assert overlay["strong_scale_fraction"] == STRONG_SCALE_FRACTION
    assert overlay["exceptional_scale_fraction"] == EXCEPTIONAL_SCALE_FRACTION
    assert overlay["min_dynamic_runner_fraction"] == MIN_DYNAMIC_RUNNER_FRACTION
    assert overlay["max_dynamic_runner_fraction"] == MAX_DYNAMIC_RUNNER_FRACTION
    assert overlay["paper_only"] is True
    assert overlay["live_money_authority"] is False


def test_contextual_position_policy_supports_three_scale_tiers_and_dynamic_runner() -> None:
    assert _contextual_position_policy()["max_scale_fraction_of_target_per_add"] == ORDINARY_SCALE_FRACTION
    token = _SCALE_FRACTION.set(STRONG_SCALE_FRACTION)
    try:
        assert _contextual_position_policy()["max_scale_fraction_of_target_per_add"] == STRONG_SCALE_FRACTION
    finally:
        _SCALE_FRACTION.reset(token)
    token = _SCALE_FRACTION.set(EXCEPTIONAL_SCALE_FRACTION)
    try:
        assert _contextual_position_policy()["max_scale_fraction_of_target_per_add"] == EXCEPTIONAL_SCALE_FRACTION
    finally:
        _SCALE_FRACTION.reset(token)

    token = _RUNNER_FRACTION.set(1.0)
    try:
        assert _contextual_position_policy()["runner_fraction_of_target"] == MAX_DYNAMIC_RUNNER_FRACTION
    finally:
        _RUNNER_FRACTION.reset(token)
    token = _RUNNER_FRACTION.set(0.0)
    try:
        assert _contextual_position_policy()["runner_fraction_of_target"] == MIN_DYNAMIC_RUNNER_FRACTION
    finally:
        _RUNNER_FRACTION.reset(token)


def test_exceptional_and_strong_continuation_require_independent_persistent_evidence() -> None:
    exceptional = {"risk_severity": 0.10, "independent_confirmation_count": 5, "flow_state": "active_fomo"}
    assert exceptional_continuation_evidence(exceptional) is True
    assert strong_continuation_evidence(exceptional) is True
    strong = {"risk_severity": 0.25, "independent_confirmation_count": 3, "flow_state": "pre_fomo"}
    assert exceptional_continuation_evidence(strong) is False
    assert strong_continuation_evidence(strong) is True
    assert strong_continuation_evidence({**strong, "independent_confirmation_count": 2}) is False
    assert strong_continuation_evidence({**strong, "flow_state": "neutral"}) is False


def test_chase_classifier_preserves_hard_ceiling() -> None:
    assert chase_classification(0.40, exceptional=False) == "normal"
    assert chase_classification(0.60, exceptional=False) == "observe_only"
    assert chase_classification(0.60, exceptional=True) == "exceptional_continuation"
    assert chase_classification(ABSOLUTE_CHASE_MAX + 0.000001, exceptional=True) == "observe_only"


def test_wallet_influence_is_graduated_but_full_validation_still_requires_canonical_gate() -> None:
    too_early = _wallet_score(episodes=PARTIAL_WALLET_MIN_SAMPLES - 1, alpha=0.50, eligible=False)
    partial = _wallet_score(episodes=15, alpha=0.50, eligible=False)
    full = _wallet_score(episodes=30, alpha=0.50, eligible=True)
    assert wallet_confidence(too_early) == 0.0
    assert wallet_utilization_multiplier(too_early) == 1.0
    assert 0.0 < wallet_confidence(partial) < wallet_confidence(full)
    assert 1.0 < wallet_utilization_multiplier(partial) < wallet_utilization_multiplier(full)
    assert wallet_utilization_multiplier(full) <= MAX_WALLET_UTILIZATION_MULTIPLIER
    assert wallet_utilization_multiplier(_wallet_score(episodes=30, alpha=-0.01, eligible=True)) == 1.0
    assert wallet_utilization_multiplier(_wallet_score(episodes=30, alpha=0.50, eligible=True, copyability=0.79)) == 1.0


def test_forward_profile_confidence_is_continuous_and_fail_closed_on_bad_tail_evidence() -> None:
    assert profile_confidence(_profile(samples=7)) == 0.0
    mid = profile_confidence(_profile(samples=15))
    full = profile_confidence(_profile(samples=30))
    assert 0.0 < mid < full <= 1.0
    bad_tail = _profile(samples=30)
    bad_tail["expected_shortfall_20"] = -0.90
    assert profile_confidence(bad_tail) == 0.0
    concentrated = _profile(samples=30)
    concentrated["winner_concentration"] = 0.90
    assert profile_confidence(concentrated) == 0.0


def test_adaptive_target_blends_toward_forward_kelly_grid_without_exceeding_lane_cap() -> None:
    current = 0.01
    partial = adaptive_target_fraction(current_target=current, profile=_profile(samples=15, best=0.10), cap=0.20)
    full = adaptive_target_fraction(current_target=current, profile=_profile(samples=30, best=0.10), cap=0.20)
    assert current < partial < full <= 0.10
    boosted = adaptive_target_fraction(
        current_target=current,
        profile=_profile(samples=30, best=0.20),
        cap=0.20,
        wallet_multiplier=MAX_WALLET_UTILIZATION_MULTIPLIER,
    )
    assert boosted == pytest.approx(0.20)


def test_conviction_tier_uses_forward_confidence_plus_persistent_confirmation() -> None:
    ordinary = {"risk_severity": 0.10, "independent_confirmation_count": 1, "flow_state": "neutral"}
    strong = {"risk_severity": 0.20, "independent_confirmation_count": 3, "flow_state": "pre_fomo"}
    exceptional = {"risk_severity": 0.10, "independent_confirmation_count": 5, "flow_state": "active_fomo"}
    profile = _profile(samples=30)
    assert conviction_tier(pre=ordinary, profile=profile, wallet_score=None) == "ordinary"
    assert conviction_tier(pre=strong, profile=profile, wallet_score=None) == "strong"
    assert conviction_tier(pre=exceptional, profile=profile, wallet_score=None) == "exceptional"


def test_portfolio_ranking_only_orders_candidates_already_eligible() -> None:
    candidates = [
        {"candidate_id": "b", "eligible": True, "expected_log_growth": 0.01, "sample_count": 30, "risk_severity": 0.10, "wallet_multiplier": 1.0},
        {"candidate_id": "a", "eligible": True, "expected_log_growth": 0.02, "sample_count": 30, "risk_severity": 0.10, "wallet_multiplier": 1.0},
        {"candidate_id": "x", "eligible": False, "expected_log_growth": 99.0, "sample_count": 1000, "risk_severity": 0.0, "wallet_multiplier": 1.50},
    ]
    ranked = rank_eligible_opportunities(candidates)
    assert [item["candidate_id"] for item in ranked] == ["a", "b"]
    assert all(item["eligible"] for item in ranked)
    assert opportunity_priority_score(expected_log_growth=0.02, sample_count=30, risk_severity=0.10) > opportunity_priority_score(expected_log_growth=0.01, sample_count=30, risk_severity=0.10)
