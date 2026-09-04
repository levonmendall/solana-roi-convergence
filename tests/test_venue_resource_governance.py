from __future__ import annotations

from solana_roi import venue_resource_governance as governance
from solana_roi.wallet_venue_lifecycle_research import RAYDIUM_POST_PUMP, RAYDIUM_UNPROVEN


def _profile(
    wallet: str,
    venue: str,
    lifecycle: str,
    *,
    trimmed_roi: float,
    copyable_roi: float | None = None,
    context_score: float | None = None,
) -> dict[str, object]:
    copyable = trimmed_roi if copyable_roi is None else copyable_roi
    score = trimmed_roi if context_score is None else context_score
    return {
        "wallet": wallet,
        "venue": venue,
        "lifecycle_stage": lifecycle,
        "regime": "hot",
        "role": "momentum_alpha",
        "context_score": score,
        "copyable_return_on_deployed_fraction": copyable,
        "copyable_return_on_deployed_fraction_pct": copyable * 100.0,
        "trimmed_mean_residual_roi_ex_best_1": trimmed_roi,
        "trimmed_mean_residual_roi_ex_best_1_pct": trimmed_roi * 100.0,
        "sample_count": 10,
        "mature_forward_context": True,
        "positive_forward_context": trimmed_roi > 0.0,
    }


def test_high_priority_wallet_capacity_is_earned_globally_not_reserved_by_venue() -> None:
    profiles = [
        _profile("ray-a", "RAYDIUM", RAYDIUM_UNPROVEN, trimmed_roi=0.60),
        _profile("ray-b", "RAYDIUM", RAYDIUM_UNPROVEN, trimmed_roi=0.50),
        _profile("pump-a", "PUMP_FUN", "pump_bonding_curve", trimmed_roi=0.20),
        _profile("amm-a", "PUMP_AMM", "pump_amm_post_bonding_curve", trimmed_roi=0.15),
    ]
    states = {str(row["wallet"]): "tracking" for row in profiles}
    plan = governance.build_roi_earned_tracking_plan(
        profiles,
        capacity=2,
        candidate_states=states,
    )

    assert plan["context_assigned_wallets"] == ["ray-a", "ray-b"]
    assert plan["fixed_venue_high_priority_reservations"] is False
    assert plan["high_priority_capacity_earned_by_forward_roi"] is True
    assert plan["ranking_unit"] == "copyable_percentage_roi_not_dollars"
    assert plan["raydium_observation_retained"] is True


def test_raydium_loses_or_regains_scarce_capacity_only_from_forward_roi() -> None:
    weak_ray = _profile("ray", "RAYDIUM", RAYDIUM_POST_PUMP, trimmed_roi=0.02)
    strong_pump = _profile("pump", "PUMP_AMM", "pump_amm_post_bonding_curve", trimmed_roi=0.18)
    states = {"ray": "tracking", "pump": "tracking"}
    plan = governance.build_roi_earned_tracking_plan(
        [weak_ray, strong_pump],
        capacity=1,
        candidate_states=states,
    )
    assert plan["context_assigned_wallets"] == ["pump"]

    strong_ray = _profile("ray", "RAYDIUM", RAYDIUM_POST_PUMP, trimmed_roi=0.21)
    plan = governance.build_roi_earned_tracking_plan(
        [strong_ray, strong_pump],
        capacity=1,
        candidate_states=states,
    )
    assert plan["context_assigned_wallets"] == ["ray"]


def test_raydium_research_compute_starts_lower_and_can_earn_full_rate() -> None:
    post_pump = governance.venue_resource_policy(
        venue="RAYDIUM",
        lifecycle=RAYDIUM_POST_PUMP,
        actions=[],
        side="buy",
        candidate_certification=False,
    )
    assert post_pump["fraction"] == governance.RAYDIUM_POST_PUMP_BOOTSTRAP_FRACTION
    assert post_pump["tier"] == "raydium_continuation_bootstrap"

    unproven = governance.venue_resource_policy(
        venue="RAYDIUM",
        lifecycle=RAYDIUM_UNPROVEN,
        actions=[],
        side="buy",
        candidate_certification=False,
    )
    assert unproven["fraction"] == governance.RAYDIUM_UNPROVEN_BOOTSTRAP_FRACTION
    assert unproven["tier"] == "raydium_unproven_low_priority_bootstrap"

    proven = governance.venue_resource_policy(
        venue="RAYDIUM",
        lifecycle=RAYDIUM_UNPROVEN,
        actions=["promote_for_future_context_influence"],
        side="buy",
        candidate_certification=False,
    )
    assert proven["fraction"] == 1.0
    assert proven["tier"] == "raydium_positive_exact_context_full_rate"

    negative = governance.venue_resource_policy(
        venue="RAYDIUM",
        lifecycle=RAYDIUM_UNPROVEN,
        actions=["demote_for_future_context_influence"],
        side="buy",
        candidate_certification=False,
    )
    assert negative["fraction"] == 1.0
    assert negative["tier"] == "raydium_mature_negative_delegate_existing"


def test_certification_exits_and_non_raydium_are_not_extra_throttled() -> None:
    candidate = governance.venue_resource_policy(
        venue="RAYDIUM",
        lifecycle=RAYDIUM_UNPROVEN,
        actions=[],
        side="buy",
        candidate_certification=True,
    )
    assert candidate["fraction"] == 1.0

    sell = governance.venue_resource_policy(
        venue="RAYDIUM",
        lifecycle=RAYDIUM_UNPROVEN,
        actions=[],
        side="sell",
        candidate_certification=False,
    )
    assert sell["fraction"] == 1.0

    pump = governance.venue_resource_policy(
        venue="PUMP_AMM",
        lifecycle="pump_amm_post_bonding_curve",
        actions=[],
        side="buy",
        candidate_certification=False,
    )
    assert pump["fraction"] == 1.0


def test_launchlab_is_not_fabricated_from_coarse_raydium_evidence() -> None:
    assert governance.RAYDIUM_LAUNCHLAB_EXACT_SUBTYPE_INFERRED is False
    assert governance.RAYDIUM_OBSERVATION_RETAINED is True


def test_safety_and_certification_boundaries_remain_unchanged() -> None:
    assert governance.PAPER_ONLY is True
    assert governance.LIVE_MONEY_AUTHORITY is False
    assert governance.SIGNING_AVAILABLE is False
    assert governance.TRANSACTION_SUBMISSION_AVAILABLE is False
    assert governance.HISTORICAL_PROMOTION_AUTHORITY is False
    assert governance.MARKET_OBSERVATION_SCOPE_REDUCED is False
    assert governance.CANDIDATE_CERTIFICATION_THROTTLED is False
    assert governance.EXIT_RESEARCH_THROTTLED is False
    assert governance.FIXED_VENUE_HIGH_PRIORITY_RESERVATIONS is False
