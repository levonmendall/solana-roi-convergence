from __future__ import annotations

from solana_roi.strategy_v52_authority import authority
from solana_roi.v52_adaptive_continuation_refinement import (
    ABSOLUTE_CHASE_MAX,
    BASE_RUNNER_FRACTION,
    EXCEPTIONAL_SCALE_FRACTION,
    MAX_DYNAMIC_RUNNER_FRACTION,
    MAX_WALLET_UTILIZATION_MULTIPLIER,
    NORMAL_CHASE_MAX,
    ORDINARY_SCALE_FRACTION,
    _EXCEPTIONAL_SCALE,
    _RUNNER_FRACTION,
    _contextual_position_policy,
    chase_classification,
    exceptional_continuation_evidence,
    opportunity_priority_score,
    rank_eligible_opportunities,
    status,
    wallet_utilization_multiplier,
)
from solana_roi.v52_wallet_alpha_refinement import ContextualWalletScore


def _wallet_score(*, episodes: int, alpha: float, eligible: bool) -> ContextualWalletScore:
    return ContextualWalletScore(
        wallet="wallet",
        context_key="context",
        paired_forward_episodes=episodes,
        effective_episode_weight=float(episodes),
        decayed_marginal_alpha=alpha,
        decayed_capture_ratio=0.50,
        decayed_executable_mae=0.05,
        copyability_rate=1.0,
        eligible_for_strategy_influence=eligible,
        blockers=(),
    )


def test_manifest_preserves_hard_authority_and_overlay_stays_separate() -> None:
    payload = authority()
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
    assert payload["execution"]["latency_hard_max_seconds"] == 20.0
    assert payload["execution"]["chase_observe_only_above_fraction"] == NORMAL_CHASE_MAX
    assert payload["position_management"]["minimum_exit_depth_coverage_ratio"] == 2.0
    assert payload["position_management"]["averaging_down_allowed"] is False
    assert payload["position_management"]["max_scale_fraction_of_target_per_add"] == ORDINARY_SCALE_FRACTION
    assert payload["position_management"]["runner_fraction_of_target"] == BASE_RUNNER_FRACTION

    overlay = status()
    assert overlay["ordinary_scale_fraction"] == ORDINARY_SCALE_FRACTION
    assert overlay["exceptional_scale_fraction"] == EXCEPTIONAL_SCALE_FRACTION
    assert overlay["normal_chase_max_fraction"] == NORMAL_CHASE_MAX
    assert overlay["absolute_chase_max_fraction"] == ABSOLUTE_CHASE_MAX
    assert overlay["base_runner_fraction"] == BASE_RUNNER_FRACTION
    assert overlay["max_dynamic_runner_fraction"] == MAX_DYNAMIC_RUNNER_FRACTION
    assert overlay["exceptional_chase_is_overlay_only"] is True
    assert overlay["paper_only"] is True
    assert overlay["live_money_authority"] is False


def test_contextual_scale_defaults_to_ordinary_and_only_exceptional_reaches_half_target() -> None:
    assert _contextual_position_policy()["max_scale_fraction_of_target_per_add"] == ORDINARY_SCALE_FRACTION
    token = _EXCEPTIONAL_SCALE.set(True)
    try:
        assert _contextual_position_policy()["max_scale_fraction_of_target_per_add"] == EXCEPTIONAL_SCALE_FRACTION
    finally:
        _EXCEPTIONAL_SCALE.reset(token)


def test_dynamic_runner_defaults_to_ten_percent_and_caps_at_twenty_five_percent() -> None:
    assert _contextual_position_policy()["runner_fraction_of_target"] == BASE_RUNNER_FRACTION
    token = _RUNNER_FRACTION.set(1.0)
    try:
        assert _contextual_position_policy()["runner_fraction_of_target"] == MAX_DYNAMIC_RUNNER_FRACTION
    finally:
        _RUNNER_FRACTION.reset(token)


def test_exceptional_continuation_requires_low_risk_broad_independent_persistent_evidence() -> None:
    strong = {
        "risk_severity": 0.10,
        "independent_confirmation_count": 5,
        "flow_state": "active_fomo",
    }
    assert exceptional_continuation_evidence(strong) is True
    assert exceptional_continuation_evidence({**strong, "risk_severity": 0.21}) is False
    assert exceptional_continuation_evidence({**strong, "independent_confirmation_count": 4}) is False
    assert exceptional_continuation_evidence({**strong, "flow_state": "neutral"}) is False


def test_chase_classifier_keeps_canonical_zone_and_overlay_ceiling() -> None:
    assert chase_classification(0.40, exceptional=False) == "normal"
    assert chase_classification(0.60, exceptional=False) == "observe_only"
    assert chase_classification(0.60, exceptional=True) == "exceptional_continuation"
    assert chase_classification(0.800001, exceptional=True) == "observe_only"


def test_wallet_influence_requires_forward_validation_and_is_capped() -> None:
    assert wallet_utilization_multiplier(_wallet_score(episodes=29, alpha=0.20, eligible=True)) == 1.0
    assert wallet_utilization_multiplier(_wallet_score(episodes=30, alpha=0.20, eligible=False)) == 1.0
    assert wallet_utilization_multiplier(_wallet_score(episodes=30, alpha=-0.01, eligible=True)) == 1.0
    assert wallet_utilization_multiplier(_wallet_score(episodes=30, alpha=0.10, eligible=True)) == 1.10
    assert wallet_utilization_multiplier(_wallet_score(episodes=30, alpha=2.0, eligible=True)) == MAX_WALLET_UTILIZATION_MULTIPLIER


def test_portfolio_ranking_only_orders_candidates_already_eligible() -> None:
    candidates = [
        {"candidate_id": "b", "eligible": True, "expected_log_growth": 0.01, "sample_count": 30, "risk_severity": 0.10, "wallet_multiplier": 1.0},
        {"candidate_id": "a", "eligible": True, "expected_log_growth": 0.02, "sample_count": 30, "risk_severity": 0.10, "wallet_multiplier": 1.0},
        {"candidate_id": "x", "eligible": False, "expected_log_growth": 99.0, "sample_count": 1000, "risk_severity": 0.0, "wallet_multiplier": 1.25},
    ]
    ranked = rank_eligible_opportunities(candidates)
    assert [item["candidate_id"] for item in ranked] == ["a", "b"]
    assert all(item["eligible"] for item in ranked)
    assert opportunity_priority_score(expected_log_growth=0.02, sample_count=30, risk_severity=0.10) > opportunity_priority_score(expected_log_growth=0.01, sample_count=30, risk_severity=0.10)
