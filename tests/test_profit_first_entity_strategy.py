from __future__ import annotations

from solana_roi.profit_first_entity_strategy import (
    ACTIVE_V3_1_COHORT_MUTATION_ALLOWED,
    Decision,
    EntityGraph,
    EntityLink,
    ForwardOutcome,
    Lane,
    OpportunitySnapshot,
    OutcomeLedger,
    ProfitFirstEntityStrategy,
    ProfitFirstPolicy,
    SIGNING_AVAILABLE,
    TRANSACTION_SUBMISSION_AVAILABLE,
)


def test_creator_association_is_not_an_automatic_veto():
    wallet = "creator-wallet"
    engine = ProfitFirstEntityStrategy()
    snapshot = OpportunitySnapshot(
        token="TOKEN",
        trigger_wallet=wallet,
        creator_wallet=wallet,
        confirming_wallets=("independent-wallet",),
        creator_cluster_net_flow_fraction=0.10,
    )
    lanes = engine.eligible_lanes(snapshot)
    assert Lane.CREATOR_CONTINUATION in lanes
    assert engine.structural_blockers(snapshot) == ()
    decision = engine.evaluate_lane(snapshot, Lane.CREATOR_CONTINUATION)
    assert decision.decision == Decision.SHADOW
    assert "creator" not in " ".join(decision.blockers)


def test_side_wallets_collapse_to_one_confirmation_entity():
    graph = EntityGraph(
        [
            EntityLink("side-a", "side-b", "common_funder"),
            EntityLink("side-b", "side-c", "shared_control"),
        ]
    )
    engine = ProfitFirstEntityStrategy(entity_graph=graph)
    snapshot = OpportunitySnapshot(
        token="TOKEN",
        trigger_wallet="scout",
        creator_wallet="creator",
        confirming_wallets=("side-a", "side-b", "side-c", "independent"),
    )
    context = engine.context_for_lane(snapshot, Lane.SMART_MONEY_SWARM)
    assert context.independent_confirmation_count == 2


def test_creator_side_wallets_do_not_fake_independent_confirmation():
    graph = EntityGraph([EntityLink("creator", "side-a", "funded_by_creator")])
    engine = ProfitFirstEntityStrategy(entity_graph=graph)
    snapshot = OpportunitySnapshot(
        token="TOKEN",
        trigger_wallet="creator",
        creator_wallet="creator",
        confirming_wallets=("side-a", "outside"),
    )
    context = engine.context_for_lane(snapshot, Lane.CREATOR_CONTINUATION)
    assert context.independent_confirmation_count == 1


def test_soft_manipulation_features_change_bucket_not_tradeability():
    engine = ProfitFirstEntityStrategy()
    snapshot = OpportunitySnapshot(
        token="TOKEN",
        trigger_wallet="creator",
        creator_wallet="creator",
        creator_cluster_net_flow_fraction=-0.10,
        early_buyer_exit_fraction=0.30,
        soft_risk_flags=frozenset({"bundled_launch", "sniper_heavy"}),
    )
    context = engine.context_for_lane(snapshot, Lane.CREATOR_CONTINUATION)
    assert context.creator_flow_state == "distributing"
    assert context.early_exit_bin == ">20%"
    assert context.soft_risk_bin == "2+"
    assert engine.structural_blockers(snapshot) == ()


def test_unsellable_or_unexitable_conditions_remain_hard_stops():
    engine = ProfitFirstEntityStrategy()
    snapshot = OpportunitySnapshot(
        token="TOKEN",
        trigger_wallet="wallet",
        creator_wallet="creator",
        exit_executable=False,
        hard_risk_flags=frozenset({"liquidity_unexitable"}),
    )
    decision = engine.evaluate_lane(snapshot, Lane.CLEAN_SCOUT)
    assert decision.decision == Decision.REJECT
    assert "exit_quote_unavailable" in decision.blockers
    assert "liquidity_unexitable" in decision.blockers


def _seed_lane(engine: ProfitFirstEntityStrategy, snapshot: OpportunitySnapshot, lane: Lane, returns: list[float]):
    context = engine.context_for_lane(snapshot, lane)
    for value in returns:
        engine.ledger.add(ForwardOutcome(context=context, net_return=value))


def test_creator_lane_can_win_if_residual_returns_are_best():
    policy = ProfitFirstPolicy(min_forward_outcomes_for_selection=5, min_exact_bucket_samples=5, min_lane_samples=5)
    engine = ProfitFirstEntityStrategy(policy=policy)
    creator = OpportunitySnapshot(
        token="TOKEN",
        trigger_wallet="creator",
        creator_wallet="creator",
        confirming_wallets=("outside",),
        creator_cluster_net_flow_fraction=0.10,
    )
    _seed_lane(engine, creator, Lane.CREATOR_CONTINUATION, [0.45, 0.30, 0.70, -0.10, 0.50])
    _seed_lane(engine, creator, Lane.UNFILTERED_ELITE, [0.05, 0.03, 0.04, -0.02, 0.02])
    result = engine.evaluate_all(creator)
    assert result[Lane.CREATOR_CONTINUATION.value].decision == Decision.ENTER
    unified = result["unified_profit_maximizer"]
    assert unified.selected_lane == Lane.CREATOR_CONTINUATION
    assert unified.predicted_mean_net_return > 0.0


def test_negative_creator_distribution_cohort_is_not_forced_profitable():
    policy = ProfitFirstPolicy(min_forward_outcomes_for_selection=5, min_exact_bucket_samples=5, min_lane_samples=5)
    engine = ProfitFirstEntityStrategy(policy=policy)
    snapshot = OpportunitySnapshot(
        token="TOKEN",
        trigger_wallet="creator",
        creator_wallet="creator",
        creator_cluster_net_flow_fraction=-0.20,
        early_buyer_exit_fraction=0.40,
    )
    _seed_lane(engine, snapshot, Lane.CREATOR_CONTINUATION, [-0.40, -0.20, 0.05, -0.15, -0.30])
    decision = engine.evaluate_lane(snapshot, Lane.CREATOR_CONTINUATION)
    assert decision.decision == Decision.WAIT
    assert decision.position_fraction == 0.0


def test_expected_log_growth_sizing_is_data_driven_not_fixed_at_half_percent():
    policy = ProfitFirstPolicy(
        min_forward_outcomes_for_selection=5,
        min_exact_bucket_samples=5,
        min_lane_samples=5,
        position_fraction_grid=(0.005, 0.01, 0.05, 0.10, 0.20),
    )
    engine = ProfitFirstEntityStrategy(policy=policy)
    snapshot = OpportunitySnapshot(token="TOKEN", trigger_wallet="scout", creator_wallet="creator")
    _seed_lane(engine, snapshot, Lane.CLEAN_SCOUT, [0.40, 0.30, 0.25, 0.35, -0.05])
    decision = engine.evaluate_lane(snapshot, Lane.CLEAN_SCOUT)
    assert decision.decision == Decision.ENTER
    assert decision.position_fraction > 0.005


def test_chase_and_observation_latency_boundaries_are_preserved():
    engine = ProfitFirstEntityStrategy()
    snapshot = OpportunitySnapshot(
        token="TOKEN",
        trigger_wallet="scout",
        chase_fraction=0.151,
        observation_lag_seconds=20.1,
    )
    blockers = engine.structural_blockers(snapshot)
    assert "chase_above_limit" in blockers
    assert "observation_lag_above_limit" in blockers


def test_new_strategy_cannot_silently_mutate_v3_or_use_live_money():
    engine = ProfitFirstEntityStrategy()
    status = engine.status()
    assert ACTIVE_V3_1_COHORT_MUTATION_ALLOWED is False
    assert SIGNING_AVAILABLE is False
    assert TRANSACTION_SUBMISSION_AVAILABLE is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["creator_association_is_automatic_veto"] is False
