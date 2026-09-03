from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi.observation_store import ObservationEventStore
from solana_roi.profit_first_entity_final import (
    ACTIVE_OLD_COHORT_MUTATION_ALLOWED,
    FINAL_STRATEGY_VERSION,
    HISTORICAL_PROMOTION_AUTHORITY,
    LIVE_MONEY_AUTHORITY,
    PAPER_ONLY,
    SIGNING_AVAILABLE,
    TRANSACTION_SUBMISSION_AVAILABLE,
    ExitFeatures,
    FinalDecision,
    FinalForwardOutcome,
    FinalLane,
    FinalOpportunity,
    FinalPolicy,
    FinalProfitFirstStrategy,
    MarketRegime,
    SignalDecayCurve,
    SizingConstraints,
    WalkForwardLedger,
    build_robustness_report,
)
from solana_roi.profit_first_entity_final_research import FinalProfitFirstResearchAdapter


def _opportunity(**overrides):
    values = dict(
        token="mint-a",
        source_signature="sig-a",
        observed_at="2026-09-03T20:00:00+00:00",
        trigger_entity="entity:scout",
        creator_entity="entity:creator",
        independent_confirmation_count=2,
        creator_linked_trigger=False,
        creator_flow_state="neutral",
        chase_fraction=0.05,
        signal_to_entry_seconds=5.0,
        round_trip_cost_fraction=0.02,
        entry_executable=True,
        exit_executable=True,
        regime=MarketRegime.HIGH_SPECULATION,
        independent_demand_strength=0.8,
    )
    values.update(overrides)
    return FinalOpportunity(**values)


def _seed(strategy, opportunity, lane, returns, *, phase="forward"):
    context = strategy.context(opportunity, lane)
    for index, value in enumerate(returns):
        strategy.ledger.add(
            FinalForwardOutcome(
                context=context,
                net_return=value,
                source_signature=f"seed-{lane.value}-{index}",
                release_commit="release-a",
                observed_at=opportunity.observed_at,
                signal_to_entry_seconds=opportunity.signal_to_entry_seconds,
                position_fraction=0.05,
                evidence_phase=phase,
            )
        )


def test_creator_association_is_feature_not_blanket_rejection_and_can_qualify():
    policy = FinalPolicy(min_forward_outcomes_for_selection=5, min_exact_bucket_samples=5, min_lane_regime_samples=5)
    strategy = FinalProfitFirstStrategy(policy=policy)
    opportunity = _opportunity(
        trigger_entity="entity:creator",
        creator_entity="entity:creator",
        creator_linked_trigger=True,
        creator_flow_state="accumulating",
        independent_confirmation_count=2,
    )
    assert strategy.structural_blockers(opportunity) == ()
    _seed(strategy, opportunity, FinalLane.CREATOR_INSIDER_CONTINUATION, [0.40, 0.25, -0.05, 0.35, 0.20])
    decision = strategy.evaluate_lane(opportunity, FinalLane.CREATOR_INSIDER_CONTINUATION)
    assert decision.decision == FinalDecision.PAPER_ENTER
    assert decision.position_fraction > 0.005
    assert decision.live_money_authority is False


def test_creator_lane_requires_genuinely_independent_external_demand():
    strategy = FinalProfitFirstStrategy()
    opportunity = _opportunity(
        trigger_entity="entity:creator",
        creator_entity="entity:creator",
        creator_linked_trigger=True,
        independent_confirmation_count=0,
    )
    decision = strategy.evaluate_lane(opportunity, FinalLane.CREATOR_INSIDER_CONTINUATION)
    assert decision.decision == FinalDecision.WAIT
    assert "creator_lane_requires_independent_external_demand" in decision.blockers


def test_entity_flow_momentum_is_distinct_and_requires_independent_entity_flow():
    strategy = FinalProfitFirstStrategy()
    good = _opportunity(independent_confirmation_count=2, independent_demand_strength=0.7)
    bad = _opportunity(independent_confirmation_count=0, independent_demand_strength=0.0)
    assert strategy.lane_eligibility(good, FinalLane.ENTITY_FLOW_MOMENTUM)[0] is True
    assert strategy.lane_eligibility(bad, FinalLane.ENTITY_FLOW_MOMENTUM)[0] is False
    assert FinalLane.ENTITY_FLOW_MOMENTUM.value != FinalLane.ELITE_WALLET_CONTINUATION.value


def test_creator_distribution_is_major_dynamic_exit_feature():
    strategy = FinalProfitFirstStrategy()
    signal = strategy.exit_model.evaluate(ExitFeatures(creator_distribution=True))
    assert signal.should_exit is True
    assert "creator_distribution" in signal.reasons


def test_historical_evidence_has_zero_promotion_authority():
    policy = FinalPolicy(min_forward_outcomes_for_selection=5, min_exact_bucket_samples=5, min_lane_regime_samples=5)
    strategy = FinalProfitFirstStrategy(policy=policy)
    opportunity = _opportunity()
    _seed(strategy, opportunity, FinalLane.ELITE_WALLET_CONTINUATION, [0.50] * 20, phase="historical")
    decision = strategy.evaluate_lane(opportunity, FinalLane.ELITE_WALLET_CONTINUATION)
    assert decision.decision == FinalDecision.SHADOW
    assert decision.forward_sample_count == 0
    assert HISTORICAL_PROMOTION_AUTHORITY is False


def test_unified_profit_maximizer_selects_best_forward_net_geometric_edge():
    policy = FinalPolicy(min_forward_outcomes_for_selection=5, min_exact_bucket_samples=5, min_lane_regime_samples=5)
    strategy = FinalProfitFirstStrategy(policy=policy)
    opportunity = _opportunity()
    _seed(strategy, opportunity, FinalLane.CLEAN_SCOUT, [0.03, 0.02, 0.04, -0.01, 0.02])
    _seed(strategy, opportunity, FinalLane.ELITE_WALLET_CONTINUATION, [0.30, 0.25, 0.35, -0.05, 0.28])
    _seed(strategy, opportunity, FinalLane.ENTITY_FLOW_MOMENTUM, [0.10, 0.12, -0.03, 0.08, 0.11])
    result = strategy.evaluate_all(opportunity)
    assert result["unified_profit_maximizer"].selected_lane == FinalLane.ELITE_WALLET_CONTINUATION
    assert result["unified_profit_maximizer"].decision == FinalDecision.PAPER_ENTER


def test_unified_has_no_authority_before_required_final_forward_evidence():
    strategy = FinalProfitFirstStrategy()
    result = strategy.evaluate_all(_opportunity())
    unified = result["unified_profit_maximizer"]
    assert unified.decision == FinalDecision.SHADOW
    assert unified.position_fraction == 0.0
    assert "unified_profit_maximizer_has_no_authority_until_forward_gate" in unified.blockers


def test_position_sizing_can_exceed_half_percent_but_respects_constraints():
    policy = FinalPolicy(min_forward_outcomes_for_selection=5, min_exact_bucket_samples=5, min_lane_regime_samples=5)
    strategy = FinalProfitFirstStrategy(policy=policy)
    opportunity = _opportunity()
    _seed(strategy, opportunity, FinalLane.ELITE_WALLET_CONTINUATION, [0.40, 0.30, 0.25, 0.35, -0.05])
    constraints = SizingConstraints(
        liquidity_headroom_fraction=0.10,
        entity_concentration_headroom_fraction=0.05,
        correlation_headroom_fraction=0.08,
        confidence_multiplier=1.0,
    )
    decision = strategy.evaluate_lane(opportunity, FinalLane.ELITE_WALLET_CONTINUATION, constraints)
    assert decision.decision == FinalDecision.PAPER_ENTER
    assert decision.position_fraction > 0.005
    assert decision.position_fraction <= 0.05


def test_signal_decay_uses_actual_forward_latency_without_weakening_certification_threshold():
    strategy = FinalProfitFirstStrategy()
    opportunity = _opportunity()
    context = strategy.context(opportunity, FinalLane.ELITE_WALLET_CONTINUATION)
    outcomes = [
        FinalForwardOutcome(context, 0.10, "a", "r", opportunity.observed_at, 1.5, 0.01),
        FinalForwardOutcome(context, 0.05, "b", "r", opportunity.observed_at, 21.0, 0.01),
    ]
    curve = SignalDecayCurve.from_outcomes(outcomes)
    assert next(row for row in curve if row.delay_seconds == 2).sample_count == 1
    assert next(row for row in curve if row.delay_seconds == 30).sample_count == 1
    assert strategy.policy.max_certified_observation_latency_seconds == 20.0


def test_robustness_report_exposes_winner_concentration_after_top_winners_removed():
    strategy = FinalProfitFirstStrategy()
    opportunity = _opportunity()
    context = strategy.context(opportunity, FinalLane.ELITE_WALLET_CONTINUATION)
    outcomes = [
        FinalForwardOutcome(context, 10.0, "winner", "r", opportunity.observed_at, 2.0, 0.20),
        *[
            FinalForwardOutcome(context, -0.20, f"loss-{i}", "r", opportunity.observed_at, 2.0, 0.20)
            for i in range(19)
        ],
    ]
    report = build_robustness_report(outcomes)
    assert report.top_1pct_removed_return is not None
    assert report.top_5pct_removed_return is not None
    assert report.top_10pct_removed_return is not None
    assert report.winner_concentrated is True


def test_final_strategy_preserves_nonnegotiable_authority_and_old_cohort_boundary():
    assert FINAL_STRATEGY_VERSION == "roi-convergence-v4.0-profit-first-entity-1"
    assert PAPER_ONLY is True
    assert LIVE_MONEY_AUTHORITY is False
    assert SIGNING_AVAILABLE is False
    assert TRANSACTION_SUBMISSION_AVAILABLE is False
    assert ACTIVE_OLD_COHORT_MUTATION_ALLOWED is False


class _Rpc:
    _roi_wallet_research_pool = True


class _EntityResolver:
    def component(self, wallet: str, *, as_of: datetime) -> set[str]:
        # A future-discovered relation must not exist before 20:30 UTC.
        cutoff = datetime(2026, 9, 3, 20, 30, tzinfo=timezone.utc)
        if wallet in {"side-a", "side-b"} and as_of >= cutoff:
            return {"side-a", "side-b"}
        return {wallet}

    def entity_id_for(self, wallet: str, *, fallback_entity_id: str | None, as_of: datetime) -> str:
        return "graph:" + wallet


class _Risk:
    async def snapshot(self, token_mint: str, observed_at: datetime, **_: object):
        return SimpleNamespace(
            unacceptable_liquidity=False,
            bundled_launch=False,
            sniper_heavy=False,
            abnormal_sell_pressure=False,
            common_funded_early_wallet_cluster=False,
            scout_deployer_connection=False,
            early_buyers_exiting=False,
        )


class _Discovery:
    def __init__(self, store):
        self.store = store
        self.rpc = _Rpc()
        self.entity_resolver = _EntityResolver()
        self.risk = _Risk()

    async def _risk_flags(self, swap):
        return True, False, False


def _create_forward_table(store):
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE wallet_discovery_forward_observations ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, signature TEXT NOT NULL UNIQUE, wallet TEXT NOT NULL, "
            "token_mint TEXT NOT NULL, side TEXT NOT NULL, token_amount REAL NOT NULL, observed_at TEXT NOT NULL, "
            "received_at TEXT NOT NULL, wallet_price_sol REAL NOT NULL, copyable_price_sol REAL, chase_fraction REAL, "
            "copyable INTEGER NOT NULL, observation_lag_ms REAL NOT NULL, risk_complete INTEGER NOT NULL, "
            "manipulation_flag INTEGER NOT NULL, side_wallet_flag INTEGER NOT NULL, source TEXT NOT NULL)"
        )


def _row(signature="buy-a", wallet="scout", side="buy", at=None):
    at = at or datetime(2026, 9, 3, 20, 0, tzinfo=timezone.utc)
    return {
        "signature": signature,
        "wallet": wallet,
        "token_mint": "mint-a",
        "side": side,
        "token_amount": 999999.0,
        "observed_at": at.isoformat(),
        "received_at": (at + timedelta(milliseconds=250)).isoformat(),
        "wallet_price_sol": 0.009,
        "copyable_price_sol": 0.0095,
        "chase_fraction": 0.05,
        "copyable": 1,
        "observation_lag_ms": 250.0,
        "risk_complete": 1,
        "manipulation_flag": 0,
        "side_wallet_flag": 0,
        "source": "test",
    }


def test_final_execution_uses_500_account_sizing_not_source_wallet_notional(monkeypatch, tmp_path):
    monkeypatch.setenv("RENDER_GIT_COMMIT", "release-final")
    store = ObservationEventStore(tmp_path / "final.sqlite3")
    _create_forward_table(store)
    adapter = FinalProfitFirstResearchAdapter(_Discovery(store))  # type: ignore[arg-type]

    async def sol_usd():
        return 100.0

    async def decimals(_mint):
        return 6

    calls = []

    async def route(input_mint, output_mint, amount):
        calls.append((input_mint, output_mint, amount))
        if len(calls) == 1:
            return {"out_amount": 50_000_000, "fee_lamports": 5_000}
        return {"out_amount": 49_000_000, "fee_lamports": 5_000}

    adapter._sol_usd = sol_usd  # type: ignore[method-assign]
    adapter.execution._token_decimals = decimals  # type: ignore[method-assign]
    adapter.execution._route = route  # type: ignore[method-assign]
    result = asyncio.run(adapter._execution(_row(), 0.10))
    assert result is not None
    assert result["input_usd"] == 50.0
    assert calls[0][2] == 500_000_000
    assert calls[0][2] != int(_row()["token_amount"])
    assert result["entry_cost_sol"] > 0.5
    assert result["quote_latency_ms"] >= 0.0


def test_final_epoch_is_clean_release_bound_and_all_five_lanes_share_source_observation(monkeypatch, tmp_path):
    monkeypatch.setenv("RENDER_GIT_COMMIT", "release-final")
    store = ObservationEventStore(tmp_path / "final.sqlite3")
    _create_forward_table(store)
    adapter = FinalProfitFirstResearchAdapter(_Discovery(store))  # type: ignore[arg-type]

    async def execution(_row, fraction):
        return {
            "paper_nav_usd": 500.0,
            "position_fraction": fraction,
            "input_usd": 500.0 * fraction,
            "sol_usd": 100.0,
            "input_lamports": 100_000_000,
            "entry_fee_lamports": 5_000,
            "entry_cost_sol": 0.100005,
            "token_raw": 10_000_000,
            "decimals": 6,
            "entry_price_sol": 0.0100005,
            "exit_net_sol": 0.098,
            "round_trip_cost_fraction": 0.020049,
            "chase_fraction": 0.05,
            "signal_to_entry_seconds": 1.0,
            "quote_latency_ms": 10.0,
        }

    async def risk(_row, _at):
        return set(), set(), 0.0

    adapter._execution = execution  # type: ignore[method-assign]
    adapter.execution._risk = risk  # type: ignore[method-assign]
    adapter.execution._deployer = lambda *_args: "creator"  # type: ignore[method-assign]
    adapter._confirmation_context = lambda *_args: ("entity:scout", "entity:creator", 2)  # type: ignore[method-assign]
    adapter._creator_flow_state = lambda *_args: "neutral"  # type: ignore[method-assign]
    adapter._market_regime = lambda *_args: MarketRegime.NEUTRAL  # type: ignore[method-assign]

    asyncio.run(adapter._buy(_row()))
    with store._lock:
        rows = store.db.execute(
            "SELECT lane,observation_group,observed_at,received_at,strategy_version FROM profit_first_final_trials WHERE epoch_id=? ORDER BY lane",
            (adapter.epoch_id,),
        ).fetchall()
    assert len(rows) == 5
    assert len({row["observation_group"] for row in rows}) == 1
    assert len({row["observed_at"] for row in rows}) == 1
    assert len({row["received_at"] for row in rows}) == 1
    assert {row["strategy_version"] for row in rows} == {FINAL_STRATEGY_VERSION}
    status = adapter.status()
    assert status["clean_final_version_epoch"] is True
    assert status["parent_research_evidence_rewritten"] is False
    assert status["all_lanes_receive_identical_chronological_source_observation"] is True
    assert status["continuity_lease_seconds_unchanged"] == 12.0
    assert status["recovery_bound_unchanged"] == "3x1000"


def test_entity_relationship_lookup_is_point_in_time_and_future_link_cannot_leak_backward(monkeypatch, tmp_path):
    monkeypatch.setenv("RENDER_GIT_COMMIT", "release-final")
    store = ObservationEventStore(tmp_path / "final.sqlite3")
    _create_forward_table(store)
    adapter = FinalProfitFirstResearchAdapter(_Discovery(store))  # type: ignore[arg-type]
    before = datetime(2026, 9, 3, 20, 0, tzinfo=timezone.utc)
    after = datetime(2026, 9, 3, 21, 0, tzinfo=timezone.utc)
    before_graph = adapter.execution._entity_graph(("side-a", "side-b"), before)
    after_graph = adapter.execution._entity_graph(("side-a", "side-b"), after)
    assert before_graph.entity_id("side-a") != before_graph.entity_id("side-b")
    assert after_graph.entity_id("side-a") == after_graph.entity_id("side-b")
