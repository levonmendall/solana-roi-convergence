from __future__ import annotations

import pytest

from solana_roi.v52_final_refinement import (
    ALPHA_LOSS_DELAYS_SECONDS,
    ContinuationEpoch,
    DerivedSignal,
    OpportunityCandidate,
    apply_state_hysteresis,
    assess_continuation_epoch,
    assess_quote_freshness,
    compete_for_portfolio_capital,
    dynamic_quote_max_age_seconds,
    final_freeze_manifest,
    flow_to_price_response,
    safety_manifest,
    stress_exit_capacity,
    time_to_alpha_loss_curve,
)


def _signal(name: str = "independent_buyers", confidence: float = 0.9, freshness: float = 1.0) -> DerivedSignal:
    return DerivedSignal(
        name=name,
        value=100.0,
        confidence=confidence,
        freshness_seconds=freshness,
        provenance=("alchemy_ws", "wallet_cluster_graph"),
    )


def test_derived_signal_requires_confidence_freshness_and_provenance() -> None:
    signal = _signal().validated()
    assert signal.confidence == pytest.approx(0.9)
    assert signal.freshness_seconds == pytest.approx(1.0)
    assert signal.provenance == ("alchemy_ws", "wallet_cluster_graph")
    assert signal.trading_authority is False

    with pytest.raises(ValueError, match="signal_provenance_missing"):
        DerivedSignal("x", 1.0, 0.5, 1.0, ()).validated()


def test_continuation_epoch_expires_without_destroying_candidate_continuity() -> None:
    epoch = ContinuationEpoch(
        candidate_id="mint-1",
        epoch_id="mint-1:continuation:1",
        created_at_seconds=100.0,
        reference_price=1.0,
        evidence=(_signal(),),
        half_life_seconds=60.0,
        expiry_seconds=120.0,
    )
    active = assess_continuation_epoch(epoch, now_seconds=160.0)
    expired = assess_continuation_epoch(epoch, now_seconds=221.0)

    assert active.active is True
    assert active.new_epoch_required is False
    assert 0.0 < active.decayed_confidence < 0.9
    assert expired.expired is True
    assert expired.decayed_confidence == 0.0
    assert expired.new_epoch_required is True
    assert "new_evidence_required" in expired.reason


def test_state_hysteresis_requires_corroboration_and_suppresses_duplicate_reentry_signal() -> None:
    waiting = apply_state_hysteresis(
        previous_state="pre_actionable",
        proposed_state="actionable",
        confirmation_count=1,
        signal_fingerprint="flow-epoch-7",
    )
    confirmed = apply_state_hysteresis(
        previous_state="pre_actionable",
        proposed_state="actionable",
        confirmation_count=2,
        signal_fingerprint="flow-epoch-7",
    )
    duplicate = apply_state_hysteresis(
        previous_state="actionable",
        proposed_state="actionable",
        confirmation_count=2,
        signal_fingerprint="flow-epoch-7",
        last_entry_signal_fingerprint="flow-epoch-7",
    )

    assert waiting.resulting_state == "pre_actionable"
    assert waiting.entry_consideration_allowed is False
    assert confirmed.resulting_state == "actionable"
    assert confirmed.entry_consideration_allowed is True
    assert duplicate.entry_consideration_allowed is False
    assert duplicate.reason == "duplicate_marginal_signal_suppressed"


def test_hard_structural_deterioration_demotes_immediately() -> None:
    result = apply_state_hysteresis(
        previous_state="actionable",
        proposed_state="pre_actionable",
        confirmation_count=0,
        signal_fingerprint="structural-break",
        hard_structural_deterioration=True,
    )
    assert result.resulting_state == "pre_actionable"
    assert result.transition_applied is True
    assert result.entry_consideration_allowed is False


def test_quote_freshness_tightens_as_velocity_rises_but_keeps_20s_hard_ceiling() -> None:
    calm = dynamic_quote_max_age_seconds(0.0001)
    fast = dynamic_quote_max_age_seconds(0.02)
    assert fast < calm < 20.0

    fresh = assess_quote_freshness(
        price_velocity_per_second=0.02,
        signal_to_quote_seconds=0.2,
        quote_to_decision_seconds=0.2,
        decision_to_fill_seconds=0.2,
    )
    stale = assess_quote_freshness(
        price_velocity_per_second=0.02,
        signal_to_quote_seconds=1.2,
        quote_to_decision_seconds=1.2,
        decision_to_fill_seconds=1.2,
    )
    hard_fail = assess_quote_freshness(
        price_velocity_per_second=0.0,
        signal_to_quote_seconds=10.0,
        quote_to_decision_seconds=6.0,
        decision_to_fill_seconds=5.0,
    )

    assert fresh.acceptable is True
    assert stale.hard_max_respected is True
    assert stale.dynamically_fresh is False
    assert hard_fail.hard_max_respected is False
    assert hard_fail.acceptable is False


def test_exit_capacity_stress_limits_entry_and_pyramid_to_degraded_sell_depth() -> None:
    safe = stress_exit_capacity(
        requested_position_notional=8_000,
        exact_sell_depth_notional=100_000,
    )
    too_large = stress_exit_capacity(
        requested_position_notional=10_000,
        exact_sell_depth_notional=100_000,
    )

    assert safe.worst_degraded_depth_notional == pytest.approx(35_000)
    assert safe.maximum_position_notional == pytest.approx(8_750)
    assert safe.passed is True
    assert too_large.passed is False


def test_flow_to_price_response_flags_buying_absorption_and_exhaustion() -> None:
    healthy = flow_to_price_response(
        price_change_fraction=0.08,
        independent_net_buy_notional=10_000,
        new_independent_buyers=20,
        prior_price_change_per_net_buy_dollar=0.000006,
    )
    exhausted = flow_to_price_response(
        price_change_fraction=0.01,
        independent_net_buy_notional=10_000,
        new_independent_buyers=20,
        prior_price_change_per_net_buy_dollar=0.000006,
    )

    assert healthy.marginal_buy_response_ratio is not None
    assert healthy.marginal_buy_response_ratio > 1.0
    assert healthy.exhaustion_risk is False
    assert exhausted.marginal_buy_response_ratio == pytest.approx(1 / 6)
    assert exhausted.seller_absorption_fraction is not None
    assert exhausted.seller_absorption_fraction > 0.8
    assert exhausted.exhaustion_risk is True


def test_portfolio_competition_respects_25pct_family_cap_and_penalizes_overlap() -> None:
    candidates = (
        OpportunityCandidate(
            "a", 0.40, 0.9, 0.9, 0.2, 15_000,
            creator_cluster="creator-1", venue="PUMPSWAP",
        ),
        OpportunityCandidate(
            "b", 0.38, 0.9, 0.9, 0.2, 15_000,
            creator_cluster="creator-1", venue="PUMPSWAP",
        ),
        OpportunityCandidate(
            "c", 0.30, 0.9, 0.9, 0.2, 15_000,
            creator_cluster="creator-2", venue="RAYDIUM",
        ),
    )
    result = compete_for_portfolio_capital(
        candidates,
        portfolio_notional=100_000,
        current_immature_family_notional=5_000,
    )

    assert result[0].candidate_id == "a"
    assert sum(item.allocated_notional for item in result) == pytest.approx(20_000)
    by_id = {item.candidate_id: item for item in result}
    assert by_id["b"].overlap_count >= 1
    assert by_id["b"].competition_score < by_id["b"].base_score
    assert all(item.trading_authority is False for item in result)


def test_time_to_alpha_loss_curve_uses_required_1_to_60_second_measurements() -> None:
    values = {
        1: 1.00,
        2: 0.98,
        5: 0.92,
        10: 0.80,
        20: 0.65,
        30: 0.50,
        60: 0.20,
    }
    curve = time_to_alpha_loss_curve(values, material_loss_fraction=0.10)

    assert tuple(point.delay_seconds for point in curve.points) == ALPHA_LOSS_DELAYS_SECONDS
    assert curve.fastest_material_loss_window_seconds == 10
    assert curve.points[-1].alpha_lost_vs_1s_fraction == pytest.approx(0.80)

    with pytest.raises(ValueError, match="alpha_loss_delay_missing:30"):
        time_to_alpha_loss_curve({key: value for key, value in values.items() if key != 30})


def test_final_freeze_is_deterministic_and_contains_all_final_refinement_features() -> None:
    first = final_freeze_manifest()
    second = final_freeze_manifest()
    assert first == second
    assert first["prospective_testing_only"] is True
    assert first["historical_weekend_evidence_can_retune"] is False
    assert len(first["freeze_fingerprint_sha256"]) == 64
    for feature in (
        "state_hysteresis_anti_flapping",
        "signal_expiration_continuation_epochs",
        "dynamic_quote_freshness",
        "exit_capacity_stress",
        "flow_to_price_response",
        "portfolio_opportunity_competition",
        "signal_confidence_freshness_provenance",
        "time_to_alpha_loss_curve",
    ):
        assert feature in first["frozen_feature_ids"]


def test_final_refinement_preserves_all_locked_execution_and_authority_boundaries() -> None:
    manifest = safety_manifest()
    assert manifest["incumbent_remains_authoritative"] is True
    assert manifest["incumbent_authority_changed"] is False
    assert manifest["challenger_entry_authority"] is False
    assert manifest["research_only"] is True
    assert manifest["paper_only"] is True
    assert manifest["live_money_authority"] is False
    assert manifest["signing_available"] is False
    assert manifest["transaction_submission_available"] is False
    assert manifest["production_composition_hook"] is False
    assert manifest["hard_latency_ceiling_seconds"] == 20.0
    assert manifest["dynamic_quote_freshness_tighter_than_hard_ceiling"] is True
    assert manifest["high_chase_observe_only_threshold"] == 0.40
    assert manifest["exact_two_sided_quotes_required"] is True
    assert manifest["exact_sell_route_required"] is True
    assert manifest["structural_exit_hard_stops_preserved"] is True
    assert manifest["first_slot_pumpfun_sniping_permitted"] is False
    assert manifest["averaging_down_permitted"] is False
    assert manifest["acceleration_alone_can_increase_allocation"] is False
    assert manifest["opportunity_emergence_trading_authority"] is False
    assert manifest["evidence_standards_can_be_lowered_to_force_trades"] is False
    assert manifest["immature_family_cap_fraction"] == pytest.approx(0.25)
