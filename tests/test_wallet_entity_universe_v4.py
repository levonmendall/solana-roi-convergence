from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from solana_roi.observation_store import ObservationEventStore
from solana_roi.profit_first_entity_final import (
    ExitFeatures,
    FinalDecision,
    FinalForwardOutcome,
    FinalLane,
    FinalOpportunity,
    FinalPolicy,
    FinalProfitFirstStrategy,
    MarketRegime,
)
from solana_roi.wallet_entity_universe_v4 import (
    ACTIVE_STRATEGY_MUTATION_ALLOWED,
    HISTORICAL_PROMOTION_AUTHORITY,
    LIVE_MONEY_AUTHORITY,
    PAPER_ONLY,
    SEED_BY_ADDRESS,
    SEED_ENTITIES,
    SIGNING_AVAILABLE,
    TRANSACTION_SUBMISSION_AVAILABLE,
    RoleScore,
    TokenScopedEntityResolver,
    WalletEntityUniverseV4,
    WalletRole,
    record_token_entity_link,
    residual_return,
    score_role,
)


NOW = datetime(2026, 9, 3, 20, 0, tzinfo=timezone.utc)
JIJO = "4BdKaxN8G6ka4GYtQQWk4G4dZRUTX2vQH9GcXdBREFUk"
TRUNOEST = "ardinRsN1mNYVeoJWTBsWeYeXvuR9UUDGMsCDKpb6AT"
DECU = "4vw54BmAogeRV3vPKWyFet5yf8DTLcREzdSzx4rw9Ud9"
WUGI = "862TYSvRYoiHAK3F3WwTRYAfuGiQaGdxedN9AGvRGWo2"
DOC = "DYAn4XpAkN5mhiXkRB7dGq4Jadnx6XYgu8L5b3WGhbrt"
THEO = "Bi4rd5FH5bYEN8scZ7wevxNZyNmKHdaBcvewdPFxYdLt"
SCHOEN = "5hAgYC8TJCcEZV7LTXAzkTrm7YL29YXyQQJPCNrG84zM"
CENTED = "CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o"


class _Registry:
    def get(self, _wallet: str):
        return None


class _LegacyResolver:
    min_confidence = 0.95
    registry = _Registry()

    def entity_id_for(self, wallet: str, *, fallback_entity_id, as_of):
        return f"legacy:{wallet}"


class _Policy:
    max_tracked_challengers = 12


class _Discovery:
    def __init__(self, store):
        self.store = store
        self.entity_resolver = _LegacyResolver()
        self.policy = _Policy()
        self.now_fn = lambda: NOW


def _candidate_schema(store):
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS wallet_discovery_candidates ("
            "wallet TEXT PRIMARY KEY, first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL, "
            "broad_sample_count INTEGER NOT NULL DEFAULT 0, distinct_token_count INTEGER NOT NULL DEFAULT 0, "
            "state TEXT NOT NULL, historical_closed_episodes INTEGER NOT NULL DEFAULT 0, "
            "historical_return_on_capital REAL NOT NULL DEFAULT 0, historical_profit_factor REAL NOT NULL DEFAULT 0, "
            "historical_hit_rate REAL NOT NULL DEFAULT 0, historical_max_drawdown REAL NOT NULL DEFAULT 0, "
            "forward_started_at TEXT, last_signature TEXT, last_polled_at TEXT, next_screen_at TEXT, "
            "forward_epoch_resets INTEGER NOT NULL DEFAULT 0, last_error TEXT)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS wallet_discovery_forward_observations ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, signature TEXT NOT NULL UNIQUE, wallet TEXT NOT NULL, "
            "token_mint TEXT NOT NULL, side TEXT NOT NULL DEFAULT 'buy', token_amount REAL NOT NULL DEFAULT 1, "
            "observed_at TEXT NOT NULL, received_at TEXT NOT NULL, wallet_price_sol REAL NOT NULL DEFAULT 1, "
            "copyable_price_sol REAL, chase_fraction REAL, copyable INTEGER NOT NULL DEFAULT 1, "
            "observation_lag_ms REAL NOT NULL DEFAULT 100, risk_complete INTEGER NOT NULL DEFAULT 1, "
            "manipulation_flag INTEGER NOT NULL DEFAULT 0, side_wallet_flag INTEGER NOT NULL DEFAULT 0, "
            "source TEXT NOT NULL DEFAULT 'test')"
        )


def _store(tmp_path):
    store = ObservationEventStore(tmp_path / "universe.sqlite3")
    _candidate_schema(store)
    return store


def _link(store, token, left, right, *, observed=NOW, received=NOW):
    return record_token_entity_link(
        store,
        token_mint=token,
        wallet_a=left,
        wallet_b=right,
        relationship="test_token_relationship",
        confidence=0.99,
        observed_at=observed,
        received_at=received,
        source="test",
    )


def _opportunity(**overrides):
    values = dict(
        token="mint-a",
        source_signature="sig-a",
        observed_at=NOW.isoformat(),
        trigger_entity=f"entity:{DECU}",
        creator_entity=f"entity:{DECU}",
        independent_confirmation_count=1,
        creator_linked_trigger=True,
        creator_flow_state="accumulating",
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


def test_named_seed_set_and_theo_canonical_address_are_exact():
    assert {seed.address for seed in SEED_ENTITIES} == {
        JIJO, TRUNOEST, DECU, WUGI, DOC, THEO, SCHOEN, CENTED
    }
    assert SEED_BY_ADDRESS[JIJO].name == "Jijo"
    assert WalletRole.SCOUT_ALPHA in SEED_BY_ADDRESS[JIJO].initial_roles
    assert WalletRole.SIGNAL_DECAY in SEED_BY_ADDRESS[THEO].initial_roles


def test_jijo_can_be_clean_scout_when_token_specific_independence_is_established(tmp_path):
    resolver = TokenScopedEntityResolver(_Discovery(_store(tmp_path)))
    trigger, creator, independent_wallets, independent_entities, _ = resolver.resolve_context(
        "token-a", JIJO, "creator-a", (), as_of=NOW
    )
    assert trigger != creator
    assert independent_wallets == ()
    assert independent_entities == ()
    assert WalletRole.SCOUT_ALPHA in SEED_BY_ADDRESS[JIJO].initial_roles


@pytest.mark.parametrize("wallet", [WUGI, DOC])
def test_creator_linked_wallet_does_not_count_as_independent_confirmation(tmp_path, wallet):
    store = _store(tmp_path)
    resolver = TokenScopedEntityResolver(_Discovery(store))
    _link(store, "token-a", wallet, "creator-a")
    _, creator, independent_wallets, independent_entities, _ = resolver.resolve_context(
        "token-a", "scout-a", "creator-a", (wallet,), as_of=NOW
    )
    assert creator is not None
    assert independent_wallets == ()
    assert independent_entities == ()


def test_decu_creator_activity_is_not_automatic_rejection_and_profitable_forward_continuation_can_qualify():
    policy = FinalPolicy(
        min_forward_outcomes_for_selection=2,
        min_exact_bucket_samples=2,
        min_lane_regime_samples=2,
    )
    strategy = FinalProfitFirstStrategy(policy=policy)
    opportunity = _opportunity()
    assert strategy.structural_blockers(opportunity) == ()
    context = strategy.context(opportunity, FinalLane.CREATOR_INSIDER_CONTINUATION)
    for index, value in enumerate((0.25, 0.35)):
        strategy.ledger.add(
            FinalForwardOutcome(
                context=context,
                net_return=value,
                source_signature=f"decu-{index}",
                release_commit="release",
                observed_at=NOW.isoformat(),
                signal_to_entry_seconds=5.0,
                position_fraction=0.05,
                evidence_phase="forward",
            )
        )
    decision = strategy.evaluate_lane(opportunity, FinalLane.CREATOR_INSIDER_CONTINUATION)
    assert decision.decision == FinalDecision.PAPER_ENTER
    assert decision.live_money_authority is False


def test_creator_distribution_is_strong_negative_exit_feature():
    strategy = FinalProfitFirstStrategy()
    signal = strategy.exit_model.evaluate(ExitFeatures(creator_distribution=True))
    assert signal.should_exit is True
    assert "creator_distribution" in signal.reasons


def test_side_wallets_collapse_to_one_token_specific_entity_and_cannot_fake_consensus(tmp_path):
    store = _store(tmp_path)
    resolver = TokenScopedEntityResolver(_Discovery(store))
    _link(store, "token-a", "side-a", "side-b")
    _, _, independent_wallets, independent_entities, relation_count = resolver.resolve_context(
        "token-a", "scout-a", "creator-a", ("side-a", "side-b"), as_of=NOW
    )
    assert len(independent_wallets) == 1
    assert len(independent_entities) == 1
    assert relation_count == 1


def test_same_wallet_can_be_independent_on_token_a_but_linked_on_token_b(tmp_path):
    store = _store(tmp_path)
    resolver = TokenScopedEntityResolver(_Discovery(store))
    _link(store, "token-b", WUGI, "creator-b")
    _, _, independent_a, _, _ = resolver.resolve_context(
        "token-a", "scout", "creator-b", (WUGI,), as_of=NOW
    )
    _, _, independent_b, _, _ = resolver.resolve_context(
        "token-b", "scout", "creator-b", (WUGI,), as_of=NOW
    )
    assert independent_a == (WUGI,)
    assert independent_b == ()


def test_point_in_time_token_relationship_cannot_leak_backward(tmp_path):
    store = _store(tmp_path)
    resolver = TokenScopedEntityResolver(_Discovery(store))
    future = NOW + timedelta(minutes=30)
    _link(store, "token-a", "side-a", "side-b", observed=future, received=future)
    entities_before, _ = resolver.components("token-a", ("side-a", "side-b"), as_of=NOW)
    entities_after, _ = resolver.components("token-a", ("side-a", "side-b"), as_of=future + timedelta(seconds=1))
    assert entities_before["side-a"] != entities_before["side-b"]
    assert entities_after["side-a"] == entities_after["side-b"]


def test_wallet_roles_are_separate_and_poor_scout_can_still_be_strong_exit_signal():
    scout = score_role(WalletRole.SCOUT_ALPHA, (-0.20, -0.10, -0.05))
    exit_alpha = score_role(WalletRole.EXIT_ALPHA, (0.25, 0.30, 0.20))
    assert scout.score is not None and scout.score < 0
    assert exit_alpha.score is not None and exit_alpha.score > 0
    assert scout.role != exit_alpha.role


def test_headline_wallet_roi_cannot_override_residual_copyable_return():
    source_headline = 6.0 / 1.0 - 1.0
    copyable = residual_return(system_entry_price=5.8, exit_price=6.0)
    steadier = residual_return(system_entry_price=1.0, exit_price=1.4)
    assert source_headline == pytest.approx(5.0)
    assert copyable == pytest.approx(0.0344827586)
    assert steadier > copyable


def test_pre_observation_gains_are_excluded_from_role_value():
    assert residual_return(system_entry_price=100.0, exit_price=103.0) == pytest.approx(0.03)


def _role_score(role, score, samples=5):
    return RoleScore(
        role=role,
        sample_count=samples,
        mean_residual_return=score,
        geometric_value=score,
        positive_rate=1.0 if score > 0 else 0.0,
        confidence=1.0,
        score=score,
    )


def test_internal_challengers_can_replace_named_seeds_for_future_tracking_influence(monkeypatch, tmp_path):
    store = _store(tmp_path)
    discovery = _Discovery(store)
    universe = WalletEntityUniverseV4(discovery)
    challengers = [f"challenger-{index:02d}" for index in range(13)]
    with store._lock, store.db:
        for index, wallet in enumerate(challengers):
            store.db.execute(
                "INSERT INTO wallet_discovery_candidates("
                "wallet,first_seen_at,last_seen_at,broad_sample_count,distinct_token_count,state,"
                "historical_closed_episodes,historical_return_on_capital,historical_profit_factor,forward_started_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (wallet, NOW.isoformat(), NOW.isoformat(), 10, 5, "tracking", 5, 0.10 + index / 100, 1.2, NOW.isoformat()),
            )
    scores = {
        wallet: {WalletRole.COPYABLE_ROC: _role_score(WalletRole.COPYABLE_ROC, 0.20 + index / 100)}
        for index, wallet in enumerate(challengers)
    }
    for seed in SEED_ENTITIES:
        scores[seed.address] = {
            WalletRole.COPYABLE_ROC: _role_score(WalletRole.COPYABLE_ROC, -0.10)
        }
    monkeypatch.setattr(universe, "role_scores", lambda: scores)
    selected = universe.select_tracked_wallets()
    nonincumbent = [wallet for wallet in selected if wallet.startswith("challenger-")]
    assert len(nonincumbent) == discovery.policy.max_tracked_challengers
    assert not any(seed.address in selected for seed in SEED_ENTITIES)


def test_universe_expansion_yields_to_existing_tracking_capacity(monkeypatch, tmp_path):
    store = _store(tmp_path)
    discovery = _Discovery(store)
    universe = WalletEntityUniverseV4(discovery)
    with store._lock, store.db:
        for index in range(100):
            wallet = f"broad-{index:03d}"
            store.db.execute(
                "INSERT INTO wallet_discovery_candidates("
                "wallet,first_seen_at,last_seen_at,broad_sample_count,distinct_token_count,state,forward_started_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (wallet, NOW.isoformat(), NOW.isoformat(), 1, 1, "tracking", NOW.isoformat()),
            )
    monkeypatch.setattr(universe, "role_scores", lambda: {})
    selected = universe.select_tracked_wallets()
    assert len(selected) <= discovery.policy.max_tracked_challengers


def test_named_seeds_are_hypotheses_not_permanent_whitelist(tmp_path):
    universe = WalletEntityUniverseV4(_Discovery(_store(tmp_path)))
    status = universe.status()
    assert status["named_seed_is_permanent_whitelist"] is False
    assert status["challengers_can_replace_seeds_for_future_influence"] is True
    assert status["active_strategy_mutation_allowed"] is False


def test_historical_evidence_cannot_authorize_v4_promotion():
    assert HISTORICAL_PROMOTION_AUTHORITY is False
    assert ACTIVE_STRATEGY_MUTATION_ALLOWED is False


def test_required_wallet_entity_telemetry_surface_is_present(tmp_path):
    universe = WalletEntityUniverseV4(_Discovery(_store(tmp_path)))
    status = universe.status()
    required = {
        "total_observed_addresses",
        "resolved_economic_entities",
        "high_priority_entities",
        "tracking_capacity_limit",
        "active_seed_entities",
        "discovered_challengers",
        "entity_relationships",
        "point_in_time_relationship_count",
        "scout_alpha_leaders",
        "creator_alpha_leaders",
        "momentum_alpha_leaders",
        "confirmation_alpha_leaders",
        "exit_alpha_leaders",
        "copyable_roc_leaders",
        "signal_decay_leaders",
        "regime_wallet_value",
        "signal_redundancy",
        "current_role_for_high_priority_entity",
        "entity_independence_state",
        "forward_sample_counts",
        "candidate_promotion_blockers",
        "historical_evidence",
        "prospective_evidence",
        "active_strategy_mutation_allowed",
    }
    assert required <= set(status)
    assert status["entity_independence_state"] == "token_specific_point_in_time_no_permanent_wallet_label"
    assert status["historical_evidence"]["promotion_authority"] is False


def test_paper_only_boundary_remains_absolute():
    assert PAPER_ONLY is True
    assert LIVE_MONEY_AUTHORITY is False
    assert SIGNING_AVAILABLE is False
    assert TRANSACTION_SUBMISSION_AVAILABLE is False
    assert ACTIVE_STRATEGY_MUTATION_ALLOWED is False
