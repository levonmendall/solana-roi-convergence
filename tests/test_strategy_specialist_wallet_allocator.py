from solana_roi.strategy_specialist_wallet_allocator import (
    build_strategy_specialist_tracking_plan,
)


def _row(
    wallet: str,
    strategy: str,
    regime: str,
    score: float,
    *,
    risk_class: str = "clean",
    risk_signature: str = "clean",
    positive: bool = True,
    exploration_only: bool = False,
):
    return {
        "wallet": wallet,
        "strategy_family": strategy,
        "venue": "PUMP_AMM",
        "lifecycle_stage": "pump_amm_established_continuation_2_5m",
        "regime": regime,
        "role": "momentum_alpha",
        "risk_class": risk_class,
        "risk_signature": risk_signature,
        "sample_count": 30 if positive else 3,
        "specialist_positive": positive,
        "mature_forward_context": positive,
        "best_expected_log_growth": score,
        "trimmed_mean_residual_roi_ex_best_1": score,
        "context_score": score,
        "exploration_only": exploration_only,
    }


def test_specialist_floors_precede_global_roi_fill():
    rows = [
        _row("elite-mania", "elite_wallet_continuation", "broad_mania", 0.90),
        _row("elite-neutral", "elite_wallet_continuation", "neutral", 0.80),
        _row("creator", "creator_insider_continuation", "broad_mania", 0.20),
        _row(
            "fomo-clean",
            "fomo_continuation",
            "broad_mania",
            0.15,
            risk_class="clean_fomo",
            risk_signature="clean_fomo",
        ),
        _row(
            "hazard",
            "hazard_continuation",
            "broad_mania",
            0.10,
            risk_class="hazard",
            risk_signature="bundled_launch",
        ),
    ]
    states = {row["wallet"]: "tracking" for row in rows}

    plan = build_strategy_specialist_tracking_plan(
        rows,
        capacity=4,
        candidate_states=states,
    )

    selected = set(plan["selected_challenger_wallets"])
    assert "fomo-clean" in selected
    assert "hazard" in selected
    assert "creator" in selected
    assert plan["strategy_regime_specialist_floor_enabled"] is True
    assert plan["remaining_capacity_filled_by_global_forward_roi"] is True
    assert plan["coverage_debt"]


def test_high_risk_wallet_keeps_observation_floor_before_maturity():
    rows = [
        _row("clean", "elite_wallet_continuation", "neutral", 0.20),
        _row(
            "risky-probe",
            "hazard_continuation",
            "neutral",
            0.70,
            risk_class="hazard",
            risk_signature="bundled_launch+creator_linked_trigger",
            positive=False,
            exploration_only=True,
        ),
    ]
    states = {row["wallet"]: "tracking" for row in rows}

    plan = build_strategy_specialist_tracking_plan(
        rows,
        capacity=2,
        candidate_states=states,
    )

    assert plan["selected_challenger_wallets"] == ["clean", "risky-probe"]
    risky = next(
        row for row in plan["context_assignments"] if row["wallet"] == "risky-probe"
    )
    assert risky["observation_only"] is True
    assert risky["future_paper_strategy_eligible"] is False
    assert risky["current_paper_strategy_authority"] is False
    assert plan["mechanical_hard_stops_relaxed"] is False


def test_clean_and_hazard_fomo_are_separate_specialist_surfaces():
    rows = [
        _row(
            "clean-fomo",
            "fomo_continuation",
            "high_speculation",
            0.20,
            risk_class="clean_fomo",
            risk_signature="clean_fomo",
        ),
        _row(
            "hazard-fomo",
            "fomo_continuation",
            "high_speculation",
            0.10,
            risk_class="hazard_fomo",
            risk_signature="hazard_fomo",
        ),
    ]
    states = {row["wallet"]: "tracking" for row in rows}

    plan = build_strategy_specialist_tracking_plan(
        rows,
        capacity=2,
        candidate_states=states,
    )

    assert set(plan["selected_challenger_wallets"]) == {
        "clean-fomo",
        "hazard-fomo",
    }
    assert plan["fomo_clean_hazard_separated"] is True
    assert all(
        row["cross_context_success_transfer_allowed"] is False
        for row in plan["context_assignments"]
    )


def test_bootstrap_wallets_never_gain_strategy_authority():
    plan = build_strategy_specialist_tracking_plan(
        [],
        capacity=2,
        candidate_states={"a": "tracking", "b": "tracking"},
        fallback_wallets=["a", "b"],
    )

    assert plan["selected_challenger_wallets"] == ["a", "b"]
    assert plan["bootstrap_observation_wallets"] == ["a", "b"]
    assert plan["bootstrap_slots_have_strategy_authority"] is False
    assert plan["cross_context_success_transfer_allowed"] is False
    assert plan["live_money_authority"] is False
