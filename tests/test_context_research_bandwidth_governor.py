from __future__ import annotations

from solana_roi import context_research_bandwidth_governor as governor


def _policy(actions, **overrides):
    values = {
        "side": "buy",
        "candidate_certification": False,
        "observation_lag_ms": 500.0,
        "processing_delay_ms": 250.0,
        "chase_fraction": 0.03,
    }
    values.update(overrides)
    return governor.bandwidth_policy_for_context_actions(list(actions), **values)


def test_positive_or_unmatured_contexts_keep_full_v4_research_bandwidth():
    bootstrap = _policy([])
    assert bootstrap["tier"] == "bootstrap_full_rate"
    assert bootstrap["fraction"] == 1.0

    positive = _policy(["promote_for_future_context_influence"])
    assert positive["tier"] == "promising_full_rate"
    assert positive["fraction"] == 1.0

    mixed = _policy(
        [
            "demote_for_future_context_influence",
            "observe_only_mixed_forward_context",
        ]
    )
    assert mixed["tier"] == "mixed_or_unmatured_full_rate"
    assert mixed["fraction"] == 1.0


def test_only_all_mature_negative_exact_contexts_are_sampled_down():
    policy = _policy(
        [
            "demote_for_future_context_influence",
            "withhold_from_future_context_influence",
        ]
    )
    assert policy["tier"] == "mature_negative_exploration"
    assert policy["fraction"] == governor.MATURE_NEGATIVE_EXPLORATION_FRACTION
    assert 0.0 < policy["fraction"] < 1.0


def test_candidate_certification_and_exit_research_are_never_throttled():
    candidate = _policy(
        ["demote_for_future_context_influence"],
        candidate_certification=True,
        observation_lag_ms=99_000.0,
        processing_delay_ms=99_000.0,
        chase_fraction=0.99,
    )
    assert candidate["tier"] == "candidate_certification_exempt"
    assert candidate["fraction"] == 1.0

    sell = _policy(
        ["demote_for_future_context_influence"],
        side="sell",
        observation_lag_ms=99_000.0,
        processing_delay_ms=99_000.0,
        chase_fraction=0.99,
    )
    assert sell["tier"] == "exit_research_exempt"
    assert sell["fraction"] == 1.0


def test_structurally_inaccessible_entries_keep_only_diagnostic_sampling():
    late = _policy([], observation_lag_ms=19_000.0, processing_delay_ms=2_000.0)
    assert late["tier"] == "structurally_inaccessible_diagnostic"
    assert late["fraction"] == governor.STRUCTURALLY_INACCESSIBLE_DIAGNOSTIC_FRACTION

    chase = _policy([], chase_fraction=0.151)
    assert chase["tier"] == "structurally_inaccessible_diagnostic"
    assert chase["fraction"] == governor.STRUCTURALLY_INACCESSIBLE_DIAGNOSTIC_FRACTION


def test_sampling_is_deterministic_and_retains_exploration():
    signature = "5nV4-bandwidth-sample"
    first = governor._deterministic_selected(signature, 0.25)
    second = governor._deterministic_selected(signature, 0.25)
    assert first is second
    assert governor._deterministic_selected(signature, 1.0) is True
    assert governor._deterministic_selected(signature, 0.0) is False


def test_governor_preserves_authority_and_market_observation_boundaries():
    assert governor.PAPER_ONLY is True
    assert governor.LIVE_MONEY_AUTHORITY is False
    assert governor.SIGNING_AVAILABLE is False
    assert governor.TRANSACTION_SUBMISSION_AVAILABLE is False
    assert governor.ACTIVE_STRATEGY_MUTATION_ALLOWED is False
    assert governor.ACTIVE_TRACKING_MUTATION_ALLOWED is False
    assert governor.HISTORICAL_PROMOTION_AUTHORITY is False
    assert governor.MARKET_OBSERVATION_SCOPE_REDUCED is False
    assert governor.CANDIDATE_CERTIFICATION_THROTTLED is False
