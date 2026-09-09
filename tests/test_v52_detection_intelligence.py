from __future__ import annotations

import pytest

from solana_roi.v52_detection_intelligence import (
    CohortObservation,
    FlowPrint,
    HazardPoint,
    LiquidityPoint,
    SellerPrint,
    build_detection_intelligence_snapshot,
    cohort_relative_anomaly,
    concentration_trajectory,
    creator_funder_priority,
    detect_wallet_cascade,
    discover_wallets_from_successful_candidate,
    dynamic_hazard_direction,
    independent_flow_metrics,
    liquidity_trajectory,
    quality_of_participation,
    safety_manifest,
    seller_pressure,
)
from solana_roi.v52_lossless_candidate_accounting import CANONICAL_LANES


def _independent_buys(count: int, *, quality: float = 0.8) -> tuple[FlowPrint, ...]:
    return tuple(
        FlowPrint(
            wallet_id=f"wallet-{index}",
            funding_cluster_id=f"cluster-{index}",
            side="buy",
            notional=10.0,
            historical_alpha=quality,
            timestamp_index=index,
        )
        for index in range(count)
    )


def _common_funded_buys(count: int, clusters: int) -> tuple[FlowPrint, ...]:
    return tuple(
        FlowPrint(
            wallet_id=f"wallet-{index}",
            funding_cluster_id=f"cluster-{index % clusters}",
            side="buy",
            notional=10.0,
            historical_alpha=0.8,
            timestamp_index=index,
        )
        for index in range(count)
    )


def test_independent_flow_distinguishes_broad_buyers_from_common_funded_sybil_activity() -> None:
    broad = independent_flow_metrics(_independent_buys(85))
    clustered = independent_flow_metrics(_common_funded_buys(100, 10))

    assert broad.unique_buyer_count == 85
    assert broad.independent_buyer_count == 85
    assert broad.buyer_breadth == 1.0
    assert broad.common_funding_ratio == 0.0

    assert clustered.unique_buyer_count == 100
    assert clustered.independent_buyer_count == 10
    assert clustered.buyer_breadth == pytest.approx(0.10)
    assert clustered.common_funding_ratio == pytest.approx(0.90)


def test_participation_quality_uses_independence_holder_growth_and_concentration_direction() -> None:
    strong_flow = independent_flow_metrics(_independent_buys(12))
    weak_flow = independent_flow_metrics(_common_funded_buys(12, 2))

    strong = quality_of_participation(
        strong_flow,
        holder_growth_fraction=0.40,
        concentration_dispersion=0.80,
        concentration_improving=True,
    )
    weak = quality_of_participation(
        weak_flow,
        holder_growth_fraction=0.02,
        concentration_dispersion=0.20,
        concentration_improving=False,
    )

    assert strong.score > weak.score
    assert strong.research_only is True
    assert strong.trading_authority is False


def test_wallet_cascade_requires_independent_skilled_sequence_and_broad_follow_through() -> None:
    cascade = detect_wallet_cascade(
        _independent_buys(8, quality=0.9),
        minimum_wallet_quality=0.7,
        minimum_skilled_independent_clusters=3,
        minimum_broad_independent_clusters=5,
    )
    sybil = detect_wallet_cascade(
        _common_funded_buys(20, 2),
        minimum_wallet_quality=0.7,
        minimum_skilled_independent_clusters=3,
        minimum_broad_independent_clusters=5,
    )

    assert cascade.detected is True
    assert cascade.skilled_independent_cluster_count == 8
    assert cascade.broad_independent_cluster_count == 8
    assert cascade.trading_authority is False

    assert sybil.detected is False
    assert "insufficient_independent_skilled_wallet_sequence" in sybil.reasons
    assert "insufficient_broad_independent_follow_through" in sybil.reasons


def test_wallet_cascade_fails_when_only_skilled_sequence_is_insufficient() -> None:
    rows = (
        FlowPrint("w1", "c1", "buy", 10.0, 0.9, 1),
        FlowPrint("w2", "c2", "buy", 10.0, 0.8, 2),
        FlowPrint("w3", "c3", "buy", 10.0, 0.1, 3),
        FlowPrint("w4", "c4", "buy", 10.0, 0.1, 4),
        FlowPrint("w5", "c5", "buy", 10.0, 0.1, 5),
    )
    result = detect_wallet_cascade(
        rows,
        minimum_wallet_quality=0.7,
        minimum_skilled_independent_clusters=3,
        minimum_broad_independent_clusters=5,
    )
    assert result.detected is False
    assert result.skilled_independent_cluster_count == 2
    assert result.broad_independent_cluster_count == 5
    assert result.reasons == ("insufficient_independent_skilled_wallet_sequence",)


def test_discovered_wallets_start_with_zero_signal_weight_and_require_prospective_validation() -> None:
    discovered = discover_wallets_from_successful_candidate(
        _independent_buys(4),
        candidate_id="winner-1",
        candidate_success_confirmed=True,
        known_wallet_ids=("wallet-0",),
    )

    assert [item.wallet_id for item in discovered] == ["wallet-1", "wallet-2", "wallet-3"]
    assert all(item.initial_signal_weight == 0.0 for item in discovered)
    assert all(item.prospective_validation_required is True for item in discovered)
    assert all(item.observation_authority is True for item in discovered)
    assert all(item.trading_authority is False for item in discovered)


def test_creator_funder_propagation_requires_incremental_alpha_and_decays() -> None:
    no_alpha = creator_funder_priority(
        base_priority=1.0,
        validated_increment=2.0,
        age_minutes=15.0,
        half_life_minutes=15.0,
        incremental_alpha_validated=False,
    )
    fresh = creator_funder_priority(
        base_priority=1.0,
        validated_increment=2.0,
        age_minutes=0.0,
        half_life_minutes=15.0,
        incremental_alpha_validated=True,
    )
    decayed = creator_funder_priority(
        base_priority=1.0,
        validated_increment=2.0,
        age_minutes=15.0,
        half_life_minutes=15.0,
        incremental_alpha_validated=True,
    )

    assert no_alpha.propagated_priority == 1.0
    assert fresh.propagated_priority == 3.0
    assert decayed.propagated_priority == 2.0
    assert decayed.trading_authority is False


def test_liquidity_and_concentration_trajectories_distinguish_strength_from_deterioration() -> None:
    supportive = liquidity_trajectory(
        LiquidityPoint(100.0, 50.0, 1000.0, 0.05),
        LiquidityPoint(160.0, 100.0, 1200.0, 0.03),
        position_notional=20.0,
    )
    deteriorating = liquidity_trajectory(
        LiquidityPoint(160.0, 100.0, 1200.0, 0.03),
        LiquidityPoint(90.0, 40.0, 1300.0, 0.08),
        position_notional=20.0,
    )

    healthy_concentration = concentration_trajectory((0.60, 0.48, 0.37, 0.27))
    bad_concentration = concentration_trajectory((0.18, 0.27, 0.41, 0.58))

    assert supportive.direction == "supportive"
    assert supportive.sell_depth_position_size_ratio == 5.0
    assert supportive.constant_notional_slippage_improvement > 0.0
    assert deteriorating.direction == "deteriorating"

    assert healthy_concentration.direction == "healthy_distribution"
    assert healthy_concentration.delta_fraction == pytest.approx(-0.33)
    assert bad_concentration.direction == "increasing_concentration"
    assert bad_concentration.delta_fraction == pytest.approx(0.40)


def test_seller_pressure_measures_dollar_weighted_distribution_repeat_sellers_and_lp_withdrawal() -> None:
    controlled = seller_pressure(
        (
            SellerPrint("buyer-a", "buy", 100.0, 0.02),
            SellerPrint("buyer-b", "buy", 100.0, 0.02),
            SellerPrint("seller-a", "sell", 10.0, -0.01),
        ),
        lp_withdrawal_fraction=0.0,
    )
    distribution = seller_pressure(
        (
            SellerPrint("buyer-a", "buy", 50.0, 0.01),
            SellerPrint("seller-a", "sell", 100.0, -0.05, early_wallet=True),
            SellerPrint("seller-a", "sell", 100.0, -0.06, early_wallet=True),
            SellerPrint("seller-b", "sell", 80.0, -0.04),
        ),
        lp_withdrawal_fraction=0.60,
    )

    assert distribution.pressure_score > controlled.pressure_score
    assert distribution.sell_notional > distribution.buy_notional
    assert distribution.repeat_seller_count == 1
    assert distribution.largest_seller_notional == 200.0
    assert distribution.early_wallet_sell_fraction > 0.0
    assert distribution.lp_withdrawal_fraction == 0.60
    assert distribution.p90_sell_notional >= distribution.median_sell_notional


def _peer(index: int, *, venue: str = "PUMPSWAP") -> CohortObservation:
    return CohortObservation(
        candidate_id=f"peer-{venue}-{index}",
        token_age_bucket="0_10m",
        venue=venue,
        lifecycle_stage="graduation_early_continuation",
        liquidity_bucket="small",
        market_cap_bucket="micro",
        launch_mechanism="pump_fun_graduation",
        hazard_class="clean",
        market_regime="active",
        independent_buyer_acceleration=float(index),
        liquidity_growth_fraction=float(index) / 100.0,
        participation_quality=min(0.95, 0.30 + index / 100.0),
        seller_pressure_score=max(0.05, 0.60 - index / 100.0),
    )


def test_cohort_relative_anomaly_requires_exact_comparability_and_fails_closed_on_small_sample() -> None:
    candidate = CohortObservation(
        candidate_id="candidate",
        token_age_bucket="0_10m",
        venue="PUMPSWAP",
        lifecycle_stage="graduation_early_continuation",
        liquidity_bucket="small",
        market_cap_bucket="micro",
        launch_mechanism="pump_fun_graduation",
        hazard_class="clean",
        market_regime="active",
        independent_buyer_acceleration=1000.0,
        liquidity_growth_fraction=1.0,
        participation_quality=0.99,
        seller_pressure_score=0.01,
    )

    sufficient = cohort_relative_anomaly(
        candidate,
        tuple(_peer(index) for index in range(40))
        + tuple(_peer(index, venue="RAYDIUM") for index in range(40)),
        minimum_peer_count=20,
        anomaly_percentile_threshold=0.995,
    )
    insufficient = cohort_relative_anomaly(
        candidate,
        tuple(_peer(index) for index in range(5)),
        minimum_peer_count=20,
        anomaly_percentile_threshold=0.995,
    )

    assert sufficient.eligible is True
    assert sufficient.comparable_peer_count == 40
    assert sufficient.independent_buyer_acceleration_percentile == 1.0
    assert sufficient.anomaly_detected is True
    assert sufficient.trading_authority is False

    assert insufficient.eligible is False
    assert insufficient.reason == "insufficient_comparable_cohort"
    assert insufficient.anomaly_detected is False


def test_dynamic_hazard_direction_tracks_slope_persistence_and_interactions_without_overriding_hard_stops() -> None:
    improving = dynamic_hazard_direction(
        (
            HazardPoint(0.70, 100.0, 0.40),
            HazardPoint(0.50, 130.0, 0.60),
            HazardPoint(0.30, 160.0, 0.80),
        )
    )
    deteriorating = dynamic_hazard_direction(
        (
            HazardPoint(0.20, 160.0, 0.80),
            HazardPoint(0.40, 130.0, 0.60),
            HazardPoint(0.70, 90.0, 0.30),
        )
    )

    assert improving.direction == "improving"
    assert improving.severity_slope < 0.0
    assert improving.can_override_structural_hard_stop is False
    assert deteriorating.direction == "deteriorating"
    assert deteriorating.worsening_interval_count == 2


@pytest.mark.parametrize("lane", CANONICAL_LANES)
def test_batch5_snapshot_operates_across_every_canonical_lane_without_granting_authority(lane: str) -> None:
    candidate = CohortObservation(
        candidate_id=f"{lane}-candidate",
        token_age_bucket="0_10m",
        venue="PUMPSWAP",
        lifecycle_stage="graduation_early_continuation",
        liquidity_bucket="small",
        market_cap_bucket="micro",
        launch_mechanism="pump_fun_graduation",
        hazard_class="clean",
        market_regime="active",
        independent_buyer_acceleration=1000.0,
        liquidity_growth_fraction=1.0,
        participation_quality=0.99,
        seller_pressure_score=0.01,
    )
    snapshot = build_detection_intelligence_snapshot(
        candidate_id=candidate.candidate_id,
        lane=lane,
        flow_prints=_independent_buys(8, quality=0.9),
        holder_growth_fraction=0.40,
        concentration_dispersion=0.80,
        concentration_values=(0.60, 0.45, 0.30),
        liquidity_start=LiquidityPoint(100.0, 50.0, 1000.0, 0.05),
        liquidity_end=LiquidityPoint(160.0, 100.0, 1200.0, 0.03),
        position_notional=20.0,
        seller_prints=(
            SellerPrint("buyer-a", "buy", 100.0),
            SellerPrint("seller-a", "sell", 10.0, -0.01),
        ),
        lp_withdrawal_fraction=0.0,
        hazard_points=(
            HazardPoint(0.50, 100.0, 0.50),
            HazardPoint(0.30, 160.0, 0.80),
        ),
        cohort_candidate=candidate,
        cohort_peers=tuple(_peer(index) for index in range(25)),
        minimum_wallet_quality=0.7,
        minimum_skilled_independent_clusters=3,
        minimum_broad_independent_clusters=5,
        minimum_peer_count=20,
        anomaly_percentile_threshold=0.995,
    )

    assert snapshot.lane == lane
    assert snapshot.independent_flow.independent_buyer_count == 8
    assert snapshot.wallet_cascade.detected is True
    assert snapshot.liquidity.direction == "supportive"
    assert snapshot.concentration.direction == "healthy_distribution"
    assert snapshot.hazard_direction.direction == "improving"
    assert snapshot.cohort_anomaly.eligible is True
    assert snapshot.research_only is True
    assert snapshot.trading_authority is False


def test_batch5_safety_manifest_preserves_v51_and_all_core_execution_safety_boundaries() -> None:
    manifest = safety_manifest()

    assert manifest["batch_version"] == "v52-batch5-detection-intelligence-1"
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
    assert manifest["discovered_wallet_initial_signal_weight"] == 0.0
    assert manifest["discovered_wallet_prospective_validation_required"] is True
    assert manifest["creator_funder_trading_authority"] is False
    assert manifest["hazard_direction_cannot_override_structural_hard_stops"] is True
    assert manifest["cohort_exact_comparability_required"] is True
    assert manifest["cohort_insufficient_sample_fails_closed"] is True
    assert manifest["economic_thresholds_changed"] is False
    assert manifest["no_averaging_down_preserved"] is True
    assert manifest["exact_two_sided_quotes_preserved"] is True
    assert manifest["structural_exit_hard_stops_preserved"] is True
