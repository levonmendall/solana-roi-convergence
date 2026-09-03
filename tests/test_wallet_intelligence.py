from datetime import datetime, timezone

from solana_roi.observation_store import ObservationEventStore
from solana_roi.wallet_intelligence import ContinuousWalletIntelligence, WalletPerformanceSnapshot

T0 = datetime(2026, 9, 2, tzinfo=timezone.utc)


def snapshot(
    wallet: str,
    entity_id: str,
    *,
    episodes: int = 60,
    copyable_return: float = 0.40,
    growth: float = 0.30,
    profit_factor: float = 1.6,
    hit_rate: float = 0.62,
    drawdown: float = 0.20,
    copyability: float = 0.90,
    manipulation: float = 0.02,
    side_wallet: float = 0.02,
) -> WalletPerformanceSnapshot:
    return WalletPerformanceSnapshot(
        wallet=wallet,
        entity_id=entity_id,
        observed_at=T0,
        closed_episodes=episodes,
        copyable_return_on_capital=copyable_return,
        geometric_growth=growth,
        profit_factor=profit_factor,
        hit_rate=hit_rate,
        max_drawdown=drawdown,
        copyability_rate=copyability,
        manipulation_risk=manipulation,
        side_wallet_risk=side_wallet,
        median_entry_lag_ms=900.0,
    )


def build(tmp_path):
    store = ObservationEventStore(tmp_path / "wallet-intelligence.sqlite3")
    for i, wallet in enumerate(("incumbent-a", "incumbent-b", "incumbent-c")):
        store.upsert_wallet_profile(
            wallet=wallet,
            entity_id=f"entity-{i}",
            tier="S",
            first_touch_sample_size=100,
            historically_eligible=True,
            updated_at=T0.isoformat(),
        )
    return store, ContinuousWalletIntelligence(store)


def seed_incumbents(intelligence):
    intelligence.record_snapshot(snapshot("incumbent-a", "entity-0", copyable_return=0.35, profit_factor=1.5, drawdown=0.20))
    intelligence.record_snapshot(snapshot("incumbent-b", "entity-1", copyable_return=0.22, profit_factor=1.35, drawdown=0.25))
    intelligence.record_snapshot(snapshot("incumbent-c", "entity-2", copyable_return=0.30, profit_factor=1.45, drawdown=0.22))


def test_superior_wallet_is_staged_for_future_cohort_without_mutating_current(tmp_path):
    store, intelligence = build(tmp_path)
    seed_incumbents(intelligence)
    intelligence.record_snapshot(snapshot("challenger", "entity-new", copyable_return=0.55, profit_factor=1.9, drawdown=0.18))

    before = intelligence.current_incumbents()
    proposal = intelligence.propose_next_cohort(parent_version="roi-convergence-v3.1", strategy_version="roi-convergence-v3.2-research-1")
    after = intelligence.current_incumbents()

    assert proposal["proposed"] is True
    assert proposal["promotion"] == "challenger"
    assert proposal["demotion"] == "incumbent-b"
    assert "challenger" in proposal["cohort"]
    assert before == after
    assert proposal["active_cohort_mutated"] is False
    assert store.verify() is True


def test_manipulation_risk_blocks_even_high_return_wallet(tmp_path):
    _, intelligence = build(tmp_path)
    seed_incumbents(intelligence)
    intelligence.record_snapshot(snapshot("suspicious", "entity-new", copyable_return=3.0, profit_factor=4.0, manipulation=0.8))

    proposal = intelligence.propose_next_cohort(parent_version="roi-convergence-v3.1", strategy_version="roi-convergence-v3.2-research-2")

    assert proposal["proposed"] is False
    decision = proposal["candidate_decisions"][0]
    assert "manipulation_risk_too_high" in decision["blockers"]


def test_missing_incumbent_forward_evidence_fails_closed(tmp_path):
    _, intelligence = build(tmp_path)
    intelligence.record_snapshot(snapshot("challenger", "entity-new", copyable_return=1.0, profit_factor=2.0))

    proposal = intelligence.propose_next_cohort(parent_version="roi-convergence-v3.1", strategy_version="roi-convergence-v3.2-research-3")

    assert proposal["proposed"] is False
    assert proposal["blockers"] == ["incumbent_forward_evidence_incomplete"]


def test_same_entity_cannot_replace_incumbent(tmp_path):
    _, intelligence = build(tmp_path)
    seed_incumbents(intelligence)
    intelligence.record_snapshot(snapshot("side-wallet", "entity-1", copyable_return=0.90, profit_factor=2.2, drawdown=0.15))

    proposal = intelligence.propose_next_cohort(parent_version="roi-convergence-v3.1", strategy_version="roi-convergence-v3.2-research-4")

    assert proposal["proposed"] is False
    decision = proposal["candidate_decisions"][0]
    assert "same_economic_entity_as_incumbent" in decision["blockers"]


def test_same_entity_as_any_remaining_incumbent_is_blocked(tmp_path):
    _, intelligence = build(tmp_path)
    seed_incumbents(intelligence)
    # incumbent-b is the weakest, so this challenger would leave incumbent-a in
    # the cohort and create false independence if cohort-wide identity were not checked.
    intelligence.record_snapshot(snapshot("side-wallet-of-a", "entity-0", copyable_return=0.90, profit_factor=2.2, drawdown=0.15))

    proposal = intelligence.propose_next_cohort(parent_version="roi-convergence-v3.1", strategy_version="roi-convergence-v3.2-research-5")

    assert proposal["proposed"] is False
    decision = proposal["candidate_decisions"][0]
    assert "same_economic_entity_as_incumbent_cohort" in decision["blockers"]
