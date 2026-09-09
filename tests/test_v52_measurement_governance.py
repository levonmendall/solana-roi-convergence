from __future__ import annotations

import pytest

from solana_roi.v52_continuation_capture import CHALLENGER_VERSION, INCUMBENT_VERSION
from solana_roi.v52_lossless_candidate_accounting import CANONICAL_LANES
from solana_roi.v52_measurement_governance import (
    ExecutablePathPoint,
    IncrementalSignalPair,
    LifecycleOutcome,
    RegretRecord,
    StrategyCandidateOutcome,
    actionability_conversion,
    assess_incremental_signal_alpha,
    compare_frozen_challenger,
    frozen_challenger_manifest,
    measure_detection_capture,
    measure_executable_excursion,
    regret_decomposition,
    safety_manifest,
    summarize_strategy,
)


def _path() -> tuple[ExecutablePathPoint, ...]:
    return (
        ExecutablePathPoint(0, 0.0, latency_seconds=30.0),
        ExecutablePathPoint(1, 0.0, latency_seconds=8.0, chase_fraction=0.10),
        ExecutablePathPoint(2, 0.20, latency_seconds=8.0),
        ExecutablePathPoint(3, -0.10, latency_seconds=8.0),
        ExecutablePathPoint(4, 1.50, latency_seconds=8.0),
        ExecutablePathPoint(5, 1.20, exact_sell_quote_available=False, latency_seconds=8.0),
        ExecutablePathPoint(6, 0.40, latency_seconds=8.0),
    )


def test_executable_mfe_mae_start_at_first_legitimate_executable_opportunity_after_costs() -> None:
    result = measure_executable_excursion(_path(), realized_net_return_fraction=0.40)

    assert result.first_legitimate_executable_index == 1
    assert result.executable_mfe_fraction == pytest.approx(1.50)
    assert result.executable_mae_fraction == pytest.approx(-0.10)
    assert result.capture_ratio == pytest.approx(0.40 / 1.50)
    assert result.nonexecutable_point_count_after_entry == 1
    assert result.trading_authority is False


def test_executable_measurement_does_not_use_pre_signal_headline_move_or_unquotable_peak() -> None:
    path = (
        ExecutablePathPoint(0, 50.0, latency_seconds=60.0),
        ExecutablePathPoint(1, 0.0, latency_seconds=5.0),
        ExecutablePathPoint(2, 5.0, exact_sell_quote_available=False, latency_seconds=5.0),
        ExecutablePathPoint(3, 0.80, latency_seconds=5.0),
    )
    result = measure_executable_excursion(path)

    assert result.first_legitimate_executable_index == 1
    assert result.executable_mfe_fraction == pytest.approx(0.80)
    assert result.nonexecutable_point_count_after_entry == 1


def test_no_legitimate_executable_opportunity_fails_closed() -> None:
    with pytest.raises(ValueError, match="no_legitimate_executable_opportunity"):
        measure_executable_excursion(
            (
                ExecutablePathPoint(0, 0.0, latency_seconds=21.0),
                ExecutablePathPoint(1, 0.5, chase_fraction=0.41),
            )
        )


def test_detection_capture_ratio_measures_remaining_executable_upside() -> None:
    result = measure_detection_capture(_path(), detection_index=2)

    assert result.first_legitimate_executable_index == 1
    assert result.executable_return_at_detection_fraction == pytest.approx(0.20)
    assert result.total_executable_forward_upside_fraction == pytest.approx(1.50)
    assert result.remaining_upside_at_detection_fraction == pytest.approx(1.30)
    assert result.detection_capture_ratio == pytest.approx(1.30 / 1.50)


def test_detection_before_first_legitimate_executable_opportunity_fails_closed() -> None:
    with pytest.raises(ValueError, match="detection_precedes_legitimate_executable_opportunity"):
        measure_detection_capture(_path(), detection_index=0)


def test_actionability_conversion_tracks_temporary_reject_reactivation_value() -> None:
    result = actionability_conversion(
        (
            LifecycleOutcome(
                "c1", "PUMP_FUN", developing=True, pre_actionable=True,
                temporary_reject=True, reactivated=True, actionable=True,
                entered=True, successful_position=True,
                temporary_reject_later_profitable_actionable=True,
            ),
            LifecycleOutcome("c2", "FOMO", developing=True, temporary_reject=True),
            LifecycleOutcome("c3", "ROBINHOOD", developing=True, actionable=True),
        )
    )

    assert result.candidates_discovered == 3
    assert result.temporary_rejects == 2
    assert result.reactivations == 1
    assert result.actionable_opportunities == 2
    assert result.entered_opportunities == 1
    assert result.successful_positions == 1
    assert result.temporary_rejects_later_profitable_actionable == 1
    assert result.temporary_reject_profitable_reactivation_rate == pytest.approx(0.5)


def test_regret_decomposition_preserves_explicit_causes_and_correct_avoidance() -> None:
    result = regret_decomposition(
        (
            RegretRecord("c1", "PUMP_FUN", 1.2, ("discovered_too_late", "undersizing")),
            RegretRecord("c2", "PUMP_AMM", 0.4, ("premature_exit",)),
            RegretRecord("c3", "FOMO", 0.0, ("correct_avoidance",)),
        )
    )

    assert result.total_missed_executable_alpha_fraction == pytest.approx(1.6)
    assert result.alpha_by_cause["discovered_too_late"] == pytest.approx(1.2)
    assert result.alpha_by_cause["undersizing"] == pytest.approx(1.2)
    assert result.alpha_by_cause["premature_exit"] == pytest.approx(0.4)
    assert result.correct_avoidance_count == 1


def test_correct_avoidance_cannot_be_used_to_hide_positive_missed_alpha() -> None:
    with pytest.raises(ValueError, match="correct_avoidance_cannot_carry_regret"):
        regret_decomposition((RegretRecord("c1", "RAYDIUM", 0.2, ("correct_avoidance",)),))


def _outcome(
    candidate_id: str,
    lane: str,
    index: int,
    version: str,
    *,
    ret: float,
    mfe: float,
    latency: float | None,
    actionable: bool = True,
    detected: bool = True,
    entered: bool = True,
    slippage: float = 0.01,
    reject_regret: float = 0.0,
    exit_regret: float = 0.0,
    capital: float = 0.5,
) -> StrategyCandidateOutcome:
    return StrategyCandidateOutcome(
        candidate_id=candidate_id,
        lane=lane,
        stream_index=index,
        strategy_version=version,
        ground_truth_actionable=actionable,
        detected_actionable=detected,
        entered=entered,
        realized_net_return_fraction=ret,
        executable_mfe_fraction=mfe,
        detection_latency_seconds=latency,
        slippage_fraction=slippage,
        reject_regret_fraction=reject_regret,
        exit_regret_fraction=exit_regret,
        capital_utilization_fraction=capital,
    )


def test_strategy_summary_reports_required_governance_metrics() -> None:
    rows = (
        _outcome("c1", "PUMP_FUN", 0, INCUMBENT_VERSION, ret=0.20, mfe=1.0, latency=12.0),
        _outcome("c2", "PUMP_AMM", 1, INCUMBENT_VERSION, ret=-0.10, mfe=0.4, latency=18.0, exit_regret=0.2),
        _outcome("c3", "FOMO", 2, INCUMBENT_VERSION, ret=0.0, mfe=0.8, latency=None, detected=False, entered=False, reject_regret=0.5),
    )
    metrics = summarize_strategy(rows)

    assert metrics.candidate_count == 3
    assert metrics.compounded_return_after_costs == pytest.approx(1.2 * 0.9 * 1.0 - 1.0)
    assert metrics.mean_detection_latency_seconds == pytest.approx(15.0)
    assert metrics.actionable_opportunity_recall == pytest.approx(2 / 3)
    assert metrics.aggregate_capture_ratio == pytest.approx((0.20 - 0.10) / (1.0 + 0.4))
    assert metrics.executable_mfe_captured_fraction == pytest.approx(0.20)
    assert metrics.max_drawdown_fraction > 0.0
    assert metrics.expected_shortfall_fraction is not None
    assert metrics.losing_trade_frequency == pytest.approx(0.5)
    assert metrics.reject_regret_fraction == pytest.approx(0.5)
    assert metrics.exit_regret_fraction == pytest.approx(0.2)


def test_frozen_challenger_comparison_requires_identical_candidate_stream_and_order() -> None:
    incumbent = (
        _outcome("c1", "PUMP_FUN", 0, INCUMBENT_VERSION, ret=0.1, mfe=0.8, latency=15.0),
        _outcome("c2", "ROBINHOOD", 1, INCUMBENT_VERSION, ret=0.0, mfe=1.0, latency=None, detected=False, entered=False, reject_regret=0.6),
    )
    challenger = (
        _outcome("c1", "PUMP_FUN", 0, CHALLENGER_VERSION, ret=0.4, mfe=0.8, latency=7.0),
        _outcome("c2", "ROBINHOOD", 1, CHALLENGER_VERSION, ret=0.3, mfe=1.0, latency=9.0),
    )
    comparison = compare_frozen_challenger(incumbent, challenger)

    assert comparison.identical_candidate_stream is True
    assert comparison.identical_stream_order is True
    assert comparison.frozen_hypotheses is True
    assert comparison.forward_only is True
    assert comparison.promotion_authority is False
    assert comparison.parameter_mutation_authority is False
    assert comparison.challenger.compounded_return_after_costs > comparison.incumbent.compounded_return_after_costs
    assert comparison.challenger.mean_detection_latency_seconds < comparison.incumbent.mean_detection_latency_seconds

    with pytest.raises(ValueError, match="candidate_stream_mismatch"):
        compare_frozen_challenger(incumbent, challenger[:1])

    reordered = (
        _outcome("c1", "PUMP_FUN", 1, CHALLENGER_VERSION, ret=0.4, mfe=0.8, latency=7.0),
        _outcome("c2", "ROBINHOOD", 0, CHALLENGER_VERSION, ret=0.3, mfe=1.0, latency=9.0),
    )
    with pytest.raises(ValueError, match="candidate_stream_order_or_lane_mismatch"):
        compare_frozen_challenger(incumbent, reordered)


def test_incremental_signal_alpha_requires_minimum_forward_sample_and_never_promotes() -> None:
    insufficient = assess_incremental_signal_alpha(
        "wallet_cascade",
        (IncrementalSignalPair("c1", 0.1, 0.2),),
        minimum_sample_count=3,
    )
    positive = assess_incremental_signal_alpha(
        "wallet_cascade",
        (
            IncrementalSignalPair("c1", 0.1, 0.3),
            IncrementalSignalPair("c2", 0.0, 0.2),
            IncrementalSignalPair("c3", -0.1, 0.0),
            IncrementalSignalPair("c4", 0.1, 0.0),
        ),
        minimum_sample_count=3,
    )

    assert insufficient.eligible is False
    assert insufficient.reason == "insufficient_forward_sample"
    assert insufficient.promotion_authority is False
    assert positive.eligible is True
    assert positive.mean_incremental_return_fraction > 0.0
    assert positive.positive_delta_frequency == pytest.approx(0.75)
    assert positive.positive_incremental_forward_value_observed is True
    assert positive.promotion_authority is False


@pytest.mark.parametrize("lane", CANONICAL_LANES)
def test_measurement_objects_support_every_canonical_lane_without_authority(lane: str) -> None:
    result = actionability_conversion(
        (LifecycleOutcome(f"{lane}-candidate", lane, actionable=True),)
    )
    regret = regret_decomposition(
        (RegretRecord(f"{lane}-miss", lane, 0.1, ("quote_failure",)),)
    )

    assert result.candidates_discovered == 1
    assert result.research_only is True
    assert regret.record_count == 1
    assert regret.research_only is True


def test_frozen_manifest_is_deterministic_and_preserves_v51_control() -> None:
    first = frozen_challenger_manifest()
    second = frozen_challenger_manifest()

    assert first == second
    assert first["challenger_version"] == CHALLENGER_VERSION
    assert first["incumbent_version"] == INCUMBENT_VERSION
    assert first["forward_only_comparison"] is True
    assert first["same_candidate_stream_required"] is True
    assert first["parameter_mutation_authority"] is False
    assert len(first["freeze_fingerprint_sha256"]) == 64


def test_batch6_safety_manifest_preserves_all_nonnegotiable_execution_boundaries() -> None:
    manifest = safety_manifest()

    assert manifest["batch_version"] == "v52-batch6-measurement-governance-1"
    assert manifest["incumbent_remains_authoritative"] is True
    assert manifest["incumbent_authority_changed"] is False
    assert manifest["challenger_entry_authority"] is False
    assert manifest["research_only"] is True
    assert manifest["paper_only"] is True
    assert manifest["live_money_authority"] is False
    assert manifest["signing_available"] is False
    assert manifest["transaction_submission_available"] is False
    assert manifest["production_composition_hook"] is False
    assert manifest["immediate_copy_max_seconds"] == 20.0
    assert manifest["high_chase_observe_only_threshold"] == 0.40
    assert manifest["exact_amount_specific_entry_quotes_preserved"] is True
    assert manifest["exact_amount_specific_exit_quotes_preserved"] is True
    assert manifest["structural_exit_hard_stops_preserved"] is True
    assert manifest["hazards_remain_evidence_or_sizing_modifiers"] is True
    assert manifest["averaging_down_permitted"] is False
    assert manifest["measurement_can_change_strategy_parameters"] is False
    assert manifest["comparison_can_promote_challenger"] is False
