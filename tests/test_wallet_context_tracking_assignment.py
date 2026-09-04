from __future__ import annotations

from solana_roi.wallet_context_tracking_assignment import (
    build_context_tracking_plan,
    build_future_strategy_assignments,
)


def _profile(
    wallet: str,
    venue: str,
    lifecycle: str,
    role: str,
    score: float,
    roi: float,
    *,
    regime: str = "hot",
) -> dict[str, object]:
    return {
        "wallet": wallet,
        "venue": venue,
        "lifecycle_stage": lifecycle,
        "regime": regime,
        "role": role,
        "context_score": score,
        "copyable_return_on_deployed_fraction": roi,
        "copyable_return_on_deployed_fraction_pct": roi * 100.0,
        "trimmed_mean_residual_roi_ex_best_1": roi,
        "trimmed_mean_residual_roi_ex_best_1_pct": roi * 100.0,
        "sample_count": 10,
        "mature_forward_context": True,
        "positive_forward_context": True,
    }


def test_tracking_capacity_is_partitioned_by_venue_lifecycle_before_global_strength() -> None:
    profiles = [
        _profile("ray-a", "RAYDIUM", "raydium_native_or_migration_unproven", "momentum_alpha", 9.0, 0.60),
        _profile("ray-b", "RAYDIUM", "raydium_native_or_migration_unproven", "confirmation_alpha", 8.0, 0.50),
        _profile("pump-a", "PUMP_FUN", "pump_bonding_curve", "scout_alpha", 3.0, 0.20),
        _profile("amm-a", "PUMP_AMM", "pump_amm_post_bonding_curve", "momentum_alpha", 2.0, 0.15),
    ]
    states = {wallet: "tracking" for wallet in ("ray-a", "ray-b", "pump-a", "amm-a")}
    plan = build_context_tracking_plan(profiles, capacity=3, candidate_states=states)

    assert set(plan["context_assigned_wallets"]) == {"ray-a", "pump-a", "amm-a"}
    assert "ray-b" not in plan["context_assigned_wallets"]
    assert plan["cross_context_success_transfer_allowed"] is False
    assert len(plan["venue_lifecycle_coverage"]) == 3


def test_bootstrap_wallet_can_be_observed_but_never_gets_strategy_eligibility() -> None:
    plan = build_context_tracking_plan(
        [],
        capacity=2,
        candidate_states={"bootstrap-a": "tracking", "bootstrap-b": "tracking"},
        fallback_wallets=["bootstrap-a", "bootstrap-b"],
    )
    assert plan["selected_challenger_wallets"] == ["bootstrap-a", "bootstrap-b"]
    assert all(row["future_paper_strategy_eligible"] is False for row in plan["bootstrap_assignments"])
    assert plan["bootstrap_slots_have_strategy_authority"] is False


def test_future_strategy_assignment_never_transfers_raydium_success_to_pump() -> None:
    profiles = [
        _profile("multi", "RAYDIUM", "raydium_native_or_migration_unproven", "momentum_alpha", 5.0, 0.40),
        {
            **_profile("multi", "PUMP_FUN", "pump_bonding_curve", "scout_alpha", -1.0, -0.10),
            "positive_forward_context": False,
        },
        _profile("pump-specialist", "PUMP_FUN", "pump_bonding_curve", "scout_alpha", 2.0, 0.18),
    ]
    assignments = build_future_strategy_assignments(profiles)
    by_context = {
        (row["venue"], row["lifecycle_stage"], row["role"]): row
        for row in assignments
    }

    ray = by_context[("RAYDIUM", "raydium_native_or_migration_unproven", "momentum_alpha")]
    pump = by_context[("PUMP_FUN", "pump_bonding_curve", "scout_alpha")]
    assert [row["wallet"] for row in ray["wallets"]] == ["multi"]
    assert [row["wallet"] for row in pump["wallets"]] == ["pump-specialist"]
    assert ray["cross_context_success_transfer_allowed"] is False
    assert pump["exact_context_match_required_for_future_paper_authority"] is True
    assert pump["current_paper_strategy_authority"] is False
