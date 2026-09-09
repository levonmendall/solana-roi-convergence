import pytest

from solana_roi.v52_continuous_evolution import (
    BoundedWalletContextModel,
    CapitalOpportunityCostModel,
    ContinuousStrategyEvolution,
    EvidenceIndependenceModel,
    EvidenceSignal,
    ExecutionAttempt,
    FailureArchetypeFeatures,
    FailureArchetypeModel,
    OutcomeCalibrator,
    OutcomeContext,
    OutcomeEpisode,
    PolicyOutcome,
    ProspectivePolicyTournament,
    RealisticPaperExecution,
    RefinedDecisionContext,
    V52ContinuousRefinementEngine,
    WalletContextEvidence,
)


def test_outcome_calibration_is_context_specific_and_shrinks_sparse_samples():
    context = OutcomeContext("pumpfun", "post_graduation", "hot", "clean")
    other = OutcomeContext("raydium", "mature", "normal", "clean")
    model = OutcomeCalibrator(prior_weight=8, confidence_sample=40)
    for value in (0.30, 0.60, 1.20, -0.25):
        model.record(OutcomeEpisode(context, value, structural_collapse=False))
    result = model.calibrate(context)
    other_result = model.calibrate(other)
    assert result.sample_size == 4
    assert result.confidence == pytest.approx(0.10)
    assert 0.0 < result.p_2x < 1.0
    assert result.expected_log_growth > 0.0
    assert other_result.sample_size == 0
    assert other_result.confidence == 0.0


def test_correlated_evidence_does_not_multiply_confirmation():
    signals = [
        EvidenceSignal("buyer_breadth", 1.0, 1.0, 1.0, True, "same_flow", "a"),
        EvidenceSignal("volume", 0.9, 1.0, 1.0, True, "same_flow", "b"),
        EvidenceSignal("price_response", 0.8, 1.0, 1.0, True, "same_flow", "c"),
        EvidenceSignal("liquidity", 0.7, 1.0, 1.0, True, "liquidity", "d"),
    ]
    result = EvidenceIndependenceModel.deconflict(signals)
    assert result.raw_signal_count == 4
    assert result.independent_root_count == 2
    assert result.independent_strength == pytest.approx(1.7)
    assert result.independent_strength < result.raw_strength
    assert result.diversity_factor == pytest.approx(0.5)


def test_missing_signal_provenance_fails_closed_at_refinement_boundary():
    evidence = EvidenceIndependenceModel.deconflict(
        [EvidenceSignal("mystery", 1.0, 1.0, 1.0, False, "unknown", "unknown")]
    )
    failure = FailureArchetypeModel.assess(FailureArchetypeFeatures())
    wallet = BoundedWalletContextModel.adjust("pumpfun|launch|hot|scout", None)
    cash = CapitalOpportunityCostModel.evaluate(
        expected_residual_return=0.20,
        expected_holding_seconds=600,
        future_opportunity_arrivals_per_hour=0.1,
        future_opportunity_expected_return=0.05,
        probability_future_opportunity_qualifies=0.5,
    )
    refined = V52ContinuousRefinementEngine.refine(
        base_confidence=0.8,
        context=RefinedDecisionContext(0.1, 1.0, evidence, failure, wallet, cash),
        hard_gate_passed=True,
    )
    assert refined.deploy_capital is False
    assert "missing_signal_provenance" in refined.blockers


def test_failure_archetype_compresses_starter_and_raises_exit_urgency_without_new_veto():
    benign = FailureArchetypeModel.assess(FailureArchetypeFeatures())
    stressed = FailureArchetypeModel.assess(
        FailureArchetypeFeatures(
            liquidity_growth_stall=1.0,
            marginal_price_response_decay=1.0,
            buyer_quality_deterioration=1.0,
            top_heavy_sell_pressure=1.0,
            repeat_buyer_decay=1.0,
            early_holder_distribution=1.0,
            market_cap_to_exit_depth_stress=1.0,
            coordination_increase=1.0,
        )
    )
    assert stressed.risk_score > benign.risk_score
    assert stressed.starter_size_multiplier < benign.starter_size_multiplier
    assert stressed.exit_urgency > benign.exit_urgency
    assert stressed.hard_veto is False


def test_realistic_execution_supports_partial_fill_requote_and_costs():
    result = RealisticPaperExecution.simulate(
        100.0,
        [
            ExecutionAttempt(1.00, 40.0, extra_slippage_fraction=0.01),
            ExecutionAttempt(1.00, 30.0, route_available=False),
            ExecutionAttempt(1.00, 60.0, adverse_move_fraction=0.02),
        ],
        fee_fraction=0.005,
    )
    assert result.side == "buy"
    assert result.complete is True
    assert result.filled_notional == pytest.approx(100.0)
    assert result.unfilled_notional == pytest.approx(0.0)
    assert result.attempts_used == 3
    assert result.average_price is not None and result.average_price > 1.0
    assert result.total_cost_fraction > 0.0


def test_realistic_sell_execution_marks_adverse_move_below_quote():
    result = RealisticPaperExecution.simulate(
        100.0,
        [ExecutionAttempt(1.0, 100.0, adverse_move_fraction=0.03, extra_slippage_fraction=0.02)],
        side="sell",
        fee_fraction=0.005,
    )
    assert result.complete is True
    assert result.side == "sell"
    assert result.average_price == pytest.approx(0.95)
    assert result.total_cost_fraction == pytest.approx(0.055)


def test_realistic_execution_fails_closed_when_depth_disappears():
    result = RealisticPaperExecution.simulate(
        100.0,
        [ExecutionAttempt(1.0, 25.0), ExecutionAttempt(1.0, 100.0, quote_fresh=False)],
    )
    assert result.complete is False
    assert result.filled_notional == pytest.approx(25.0)
    assert result.unfilled_notional == pytest.approx(75.0)
    assert result.failure_reason == "stale_quote"


def test_cash_option_can_defer_mediocre_trade_in_opportunity_rich_regime():
    quiet = CapitalOpportunityCostModel.evaluate(
        expected_residual_return=0.10,
        expected_holding_seconds=1800,
        future_opportunity_arrivals_per_hour=0.1,
        future_opportunity_expected_return=0.10,
        probability_future_opportunity_qualifies=0.2,
    )
    rich = CapitalOpportunityCostModel.evaluate(
        expected_residual_return=0.10,
        expected_holding_seconds=1800,
        future_opportunity_arrivals_per_hour=8.0,
        future_opportunity_expected_return=0.20,
        probability_future_opportunity_qualifies=0.5,
    )
    assert quiet.deploy is True
    assert rich.deploy is False
    assert rich.cash_option_rate > quiet.cash_option_rate


def test_wallet_context_requires_forward_copyable_quality_and_is_bounded():
    immature = WalletContextEvidence(
        "pumpfun|launch|hot|scout", 5, 0.4, 2.0, 0.95, 0.0, 0.0, 0.15, 0.15, -0.2
    )
    mature = WalletContextEvidence(
        "pumpfun|launch|hot|scout", 40, 0.4, 2.0, 0.95, 0.02, 0.02, 0.8, 0.8, -0.8
    )
    blocked = BoundedWalletContextModel.adjust("pumpfun|launch|hot|scout", immature)
    allowed = BoundedWalletContextModel.adjust("pumpfun|launch|hot|scout", mature)
    assert blocked.eligible is False
    assert blocked.confidence_delta == 0.0
    assert allowed.eligible is True
    assert allowed.confidence_delta == pytest.approx(0.15)
    assert allowed.sizing_multiplier == pytest.approx(1.15)
    assert allowed.exit_urgency_delta == pytest.approx(-0.20)


def _seed_tournament(tournament: ProspectivePolicyTournament, n: int = 20) -> None:
    for index in range(n):
        stream = f"episode-{index:03d}"
        tournament.record(PolicyOutcome("incumbent", stream, 0.01, 0.10, True))
        tournament.record(PolicyOutcome("challenger", stream, 0.03, 0.08, True))


def test_policy_tournament_requires_paired_same_stream_forward_evidence():
    tournament = ProspectivePolicyTournament(min_paired_episodes=20)
    for index in range(19):
        tournament.record(PolicyOutcome("incumbent", f"i-{index}", 0.01, 0.1, True))
        tournament.record(PolicyOutcome("challenger", f"different-{index}", 0.50, 0.0, True))
    decision = tournament.compare("incumbent", ["challenger"])
    assert decision.eligible is False
    assert "insufficient_paired_forward_episodes" in decision.blockers


def test_continuous_evolution_has_no_freeze_or_cooldown_for_ordinary_parameters():
    evolution = ContinuousStrategyEvolution(
        {"starter_fraction": 0.05, "runner_fraction": 0.20}, version="v5.2"
    )
    first = evolution.evolve({"starter_fraction": 0.06}, rationale="forward calibration improved")
    second = evolution.evolve({"runner_fraction": 0.25}, rationale="runner capture improved")
    assert first.sequence == 2
    assert second.sequence == 3
    assert second.parent_version == first.strategy_version
    assert len(evolution.history) == 3
    assert len({epoch.fingerprint for epoch in evolution.history}) == 3
    assert first.protected_change is False
    assert second.protected_change is False


def test_protected_strategy_constraints_can_evolve_only_through_explicit_validated_path():
    evolution = ContinuousStrategyEvolution({"starter_fraction": 0.05}, version="v5.2")
    with pytest.raises(ValueError, match="protected strategy keys require"):
        evolution.evolve(
            {"absolute_latency_ceiling_seconds": 18.0},
            rationale="ordinary evolution cannot silently change protected constraints",
        )
    with pytest.raises(ValueError, match="validation/test reference"):
        evolution.evolve_protected_strategy_constraint(
            {"absolute_latency_ceiling_seconds": 18.0},
            rationale="validated tighter latency policy",
            validation_refs=(),
        )
    epoch = evolution.evolve_protected_strategy_constraint(
        {"absolute_latency_ceiling_seconds": 18.0},
        rationale="validated tighter latency policy",
        validation_refs=("test:test_v52_latency", "forward:epoch-42"),
    )
    assert epoch.protected_change is True
    assert epoch.config["absolute_latency_ceiling_seconds"] == pytest.approx(18.0)
    assert epoch.evidence_refs == ("test:test_v52_latency", "forward:epoch-42")


def test_live_money_authority_boundary_remains_immutable_under_both_evolution_paths():
    evolution = ContinuousStrategyEvolution({"runner_fraction": 0.20}, version="v5.2")
    with pytest.raises(ValueError, match="immutable authority"):
        evolution.evolve({"signing_enabled": True}, rationale="must stay paper only")
    with pytest.raises(ValueError, match="immutable authority"):
        evolution.evolve_protected_strategy_constraint(
            {"signing_enabled": True},
            rationale="must stay paper only",
            validation_refs=("test:authority",),
        )


def test_tournament_winner_can_immediately_create_next_auditable_strategy_epoch():
    tournament = ProspectivePolicyTournament(min_paired_episodes=20, min_improvement_ratio=1.03)
    _seed_tournament(tournament)
    decision = tournament.compare("incumbent", ["challenger"])
    assert decision.eligible is True
    assert decision.winner == "challenger"
    assert decision.improvement_ratio is not None and decision.improvement_ratio > 1.03

    evolution = ContinuousStrategyEvolution({"runner_fraction": 0.20}, version="v5.2")
    epoch = evolution.evolve_from_tournament(
        decision,
        {"challenger": {"runner_fraction": 0.30}},
    )
    assert epoch is not None
    assert epoch.config["runner_fraction"] == pytest.approx(0.30)
    assert epoch.parent_version == "v5.2"
    assert "prospective_policy_tournament" in epoch.rationale
    assert epoch.evidence_refs == ("paired_policy:challenger",)
    assert epoch.config["paper_only"] is True
    assert epoch.config["signing_enabled"] is False


def test_refinement_combines_calibration_independence_failure_wallet_and_cash_without_authority_bypass():
    evidence = EvidenceIndependenceModel.deconflict(
        [
            EvidenceSignal("wallet", 0.9, 0.9, 1.0, True, "wallet", "wallet-ledger"),
            EvidenceSignal("liquidity", 0.8, 0.8, 1.0, True, "liquidity", "dex"),
            EvidenceSignal("price", 0.8, 0.9, 1.0, True, "flow", "dex"),
        ]
    )
    failure = FailureArchetypeModel.assess(
        FailureArchetypeFeatures(marginal_price_response_decay=0.2, top_heavy_sell_pressure=0.1)
    )
    wallet = BoundedWalletContextModel.adjust(
        "pumpfun|post_graduation|hot|confirmation",
        WalletContextEvidence(
            "pumpfun|post_graduation|hot|confirmation",
            50,
            0.30,
            1.6,
            0.90,
            0.02,
            0.02,
            confidence_delta=0.05,
            sizing_delta=0.05,
        ),
    )
    cash = CapitalOpportunityCostModel.evaluate(
        expected_residual_return=0.30,
        expected_holding_seconds=900,
        future_opportunity_arrivals_per_hour=0.2,
        future_opportunity_expected_return=0.10,
        probability_future_opportunity_qualifies=0.3,
    )
    context = RefinedDecisionContext(0.12, 0.8, evidence, failure, wallet, cash)
    allowed = V52ContinuousRefinementEngine.refine(
        base_confidence=0.75, context=context, hard_gate_passed=True
    )
    blocked = V52ContinuousRefinementEngine.refine(
        base_confidence=0.75, context=context, hard_gate_passed=False
    )
    assert allowed.deploy_capital is True
    assert allowed.ranking_score > 0.0
    assert allowed.sizing_multiplier > 0.0
    assert blocked.deploy_capital is False
    assert "existing_v52_hard_gate_failed" in blocked.blockers
