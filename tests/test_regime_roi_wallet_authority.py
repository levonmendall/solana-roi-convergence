from solana_roi import regime_roi_wallet_authority as authority


def _row(
    wallet: str,
    strategy: str,
    regime: str,
    roi_pct: float,
    *,
    growth: float = 0.05,
    venue: str = "PUMP_AMM",
    lifecycle: str = "pump_amm_established_continuation_2_5m",
    role: str = "independent_wallet",
    risk_signature: str = "clean",
    risk_class: str = "clean",
):
    return {
        "wallet": wallet,
        "strategy_family": strategy,
        "venue": venue,
        "lifecycle_stage": lifecycle,
        "regime": regime,
        "role": role,
        "risk_signature": risk_signature,
        "risk_class": risk_class,
        "sample_count": 40,
        "mature_forward_context": True,
        "specialist_positive": True,
        "trimmed_mean_residual_roi_ex_best_1_pct": roi_pct,
        "copyable_return_on_deployed_fraction_pct": roi_pct - 1.0,
        "best_expected_log_growth": growth,
        "context_score": growth,
        "exploration_only": False,
    }


def test_regime_matrix_ranks_robust_roi_percent_before_log_growth():
    rows = [
        _row("higher-growth", "elite_wallet_continuation", "neutral", 25.0, growth=0.90),
        _row("higher-roi", "elite_wallet_continuation", "neutral", 40.0, growth=0.10),
        _row("third", "elite_wallet_continuation", "neutral", 15.0, growth=0.20),
    ]
    matrix = authority.build_regime_wallet_matrix(rows)
    neutral = next(item for item in matrix["regimes"] if item["regime"] == "neutral")
    strategy = next(item for item in neutral["strategies"] if item["strategy_family"] == "elite_wallet_continuation")

    assert [row["wallet"] for row in strategy["leaders"]] == ["higher-roi", "higher-growth", "third"]
    assert strategy["leaders"][0]["roi_pct"] == 40.0
    assert matrix["dollar_profit_used_for_ranking"] is False
    assert matrix["ranking_objective"] == "robust_forward_roi_pct_then_expected_log_growth_after_costs"


def test_exact_contexts_do_not_transfer_wallet_success_between_venues():
    rows = [
        _row("pump-leader", "elite_wallet_continuation", "high_speculation", 60.0),
        _row(
            "raydium-leader",
            "elite_wallet_continuation",
            "high_speculation",
            35.0,
            venue="RAYDIUM",
            lifecycle="raydium_post_pump_migration_evidence",
        ),
    ]
    matrix = authority.build_regime_wallet_matrix(rows)
    pump_key = (
        "elite_wallet_continuation",
        "PUMP_AMM",
        "pump_amm_established_continuation_2_5m",
        "high_speculation",
        "independent_wallet",
        "clean",
    )
    raydium_key = (
        "elite_wallet_continuation",
        "RAYDIUM",
        "raydium_post_pump_migration_evidence",
        "high_speculation",
        "independent_wallet",
        "clean",
    )

    assert matrix["exact_lookup"][pump_key][0]["wallet"] == "pump-leader"
    assert matrix["exact_lookup"][raydium_key][0]["wallet"] == "raydium-leader"
    assert matrix["cross_context_success_transfer_allowed"] is False


def test_all_four_regimes_are_explicit_even_when_one_has_no_proven_wallet():
    matrix = authority.build_regime_wallet_matrix([
        _row("neutral-leader", "entity_flow_momentum", "neutral", 30.0),
    ])
    assert [item["regime"] for item in matrix["regimes"]] == list(authority.CANONICAL_REGIMES)
    weak = next(item for item in matrix["regimes"] if item["regime"] == "weak_or_deteriorating")
    strategy = next(item for item in weak["strategies"] if item["strategy_family"] == "entity_flow_momentum")
    assert strategy["state"] == "no_proven_profitable_wallet_yet"
    assert strategy["leaders"] == []


def test_active_regime_leaders_are_selected_before_inactive_higher_roi_wallets(monkeypatch):
    rows = [
        _row("neutral-elite", "elite_wallet_continuation", "neutral", 20.0),
        _row("neutral-creator", "creator_insider_continuation", "neutral", 18.0),
        _row("mania-elite", "elite_wallet_continuation", "broad_mania", 90.0),
        _row("mania-creator", "creator_insider_continuation", "broad_mania", 80.0),
    ]
    states = {row["wallet"]: "tracking" for row in rows}

    def base_plan(remaining, *, capacity, candidate_states=None, fallback_wallets=()):
        selected = [
            row["wallet"] for row in sorted(remaining, key=authority._profit_rank, reverse=True)
            if not candidate_states or candidate_states.get(row["wallet"]) == "tracking"
        ][:capacity]
        return {
            "selected_challenger_wallets": selected,
            "context_assignments": [],
            "bootstrap_observation_wallets": [],
            "coverage_debt": [],
        }

    monkeypatch.setattr(authority, "_ORIGINAL_BUILD", base_plan)
    plan = authority.build_regime_roi_tracking_plan(
        rows,
        capacity=2,
        active_regime="neutral",
        candidate_states=states,
    )

    assert set(plan["selected_challenger_wallets"]) == {"neutral-elite", "neutral-creator"}
    assert plan["active_regime_roi_leaders_first"] is True
    assert plan["dollar_profit_used_for_ranking"] is False


def test_strategy_use_gives_leaders_normal_size_and_caps_challengers():
    leader = {"state": "assigned_regime_roi_leader", "leader_rank": 1}
    second = {"state": "assigned_regime_roi_leader", "leader_rank": 2}
    challenger = {"state": "regime_roi_challenger"}
    unproven = {"state": "no_proven_profitable_exact_context_wallet_yet"}

    assert authority.apply_strategy_use_fraction(0.05, leader) == 0.05
    assert authority.apply_strategy_use_fraction(0.05, second) == 0.0425
    assert authority.apply_strategy_use_fraction(0.05, challenger) == authority.CHALLENGER_FRACTION_CAP
    assert authority.apply_strategy_use_fraction(0.05, unproven) == authority.UNPROVEN_CONTEXT_FRACTION_CAP


def test_hazard_context_remains_assignable_when_forward_roi_is_profitable():
    matrix = authority.build_regime_wallet_matrix([
        _row(
            "hazard-leader",
            "hazard_continuation",
            "broad_mania",
            55.0,
            risk_signature="bundled_launch+creator_linked_trigger",
            risk_class="hazard",
        )
    ])
    context = next(item for item in matrix["exact_contexts"] if item["strategy_family"] == "hazard_continuation")
    assert context["leaders"][0]["wallet"] == "hazard-leader"
    assert context["leaders"][0]["paper_strategy_use_eligible"] is True
    assert context["leaders"][0]["assignment_alone_authorizes_entry"] is False
