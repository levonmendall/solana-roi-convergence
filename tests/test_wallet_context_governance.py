from __future__ import annotations

from types import SimpleNamespace

from solana_roi.observation_store import ObservationEventStore
from solana_roi.wallet_context_governance import WalletContextGovernance
from solana_roi.wallet_entity_universe_v4 import SEED_BY_ADDRESS


def _store(tmp_path):
    store = ObservationEventStore(tmp_path / "wallet-context-governance.sqlite3")
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS wallet_discovery_candidates ("
            "wallet TEXT PRIMARY KEY, state TEXT NOT NULL)"
        )
    return store


def _profile(
    wallet: str,
    *,
    venue: str = "RAYDIUM",
    lifecycle: str = "raydium_native_or_migration_unproven",
    regime: str = "neutral",
    role: str = "momentum_alpha",
    score: float = 0.10,
    trimmed: float = 0.08,
    roi: float = 0.12,
    samples: int = 12,
    seed_name: str | None = None,
):
    return {
        "wallet": wallet,
        "seed_name": seed_name,
        "venue": venue,
        "lifecycle_stage": lifecycle,
        "regime": regime,
        "role": role,
        "sample_count": samples,
        "context_score": score,
        "context_confidence": 0.8,
        "copyable_return_on_deployed_fraction": roi,
        "copyable_return_on_deployed_fraction_pct": roi * 100.0,
        "median_residual_roi": roi,
        "median_residual_roi_pct": roi * 100.0,
        "trimmed_mean_residual_roi_ex_best_1": trimmed,
        "trimmed_mean_residual_roi_ex_best_1_pct": trimmed * 100.0,
        "positive_rate": 0.7,
        "positive_rate_pct": 70.0,
        "mature_forward_context": samples >= 5,
        "positive_forward_context": score > 0.0,
    }


def _governance(store):
    universe = SimpleNamespace(store=store)
    router = SimpleNamespace(universe=universe, store=store)
    return WalletContextGovernance(router)


def test_mature_negative_seed_is_demotion_candidate(tmp_path):
    store = _store(tmp_path)
    seed_wallet, seed = next(iter(SEED_BY_ADDRESS.items()))
    profile = _profile(
        seed_wallet,
        seed_name=seed.name,
        score=-0.05,
        trimmed=-0.03,
        roi=-0.04,
    )

    status = _governance(store).evaluate([profile])

    assert len(status["demotion_candidates"]) == 1
    row = status["demotion_candidates"][0]
    assert row["wallet"] == seed_wallet
    assert row["recommended_action"] == "demote_for_future_context_influence"
    assert status["named_seed_is_permanent_whitelist"] is False
    assert status["incumbent_can_lose_future_context_influence"] is True


def test_mature_robust_positive_challenger_is_promotion_candidate(tmp_path):
    store = _store(tmp_path)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO wallet_discovery_candidates(wallet,state) VALUES (?,?)",
            ("challenger", "tracking"),
        )

    status = _governance(store).evaluate([_profile("challenger")])

    assert len(status["promotion_candidates"]) == 1
    row = status["promotion_candidates"][0]
    assert row["wallet"] == "challenger"
    assert row["recommended_action"] == "promote_for_future_context_influence"
    assert row["robust_positive_after_best_trade_trim"] is True
    assert row["recommendation_has_tracking_mutation_authority"] is False
    assert row["recommendation_has_strategy_authority"] is False


def test_one_moonshot_does_not_create_positive_promotion_recommendation(tmp_path):
    store = _store(tmp_path)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO wallet_discovery_candidates(wallet,state) VALUES (?,?)",
            ("moonshot", "tracking"),
        )
    profile = _profile("moonshot", score=0.40, trimmed=-0.02, roi=0.90)

    status = _governance(store).evaluate([profile])

    assert not status["promotion_candidates"]
    row = status["all_context_recommendations"][0]
    assert row["recommended_action"] == "withhold_from_future_context_influence"
    assert row["robust_positive_after_best_trade_trim"] is False


def test_replacement_requires_exact_same_context(tmp_path):
    store = _store(tmp_path)
    seed_wallet, seed = next(iter(SEED_BY_ADDRESS.items()))
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO wallet_discovery_candidates(wallet,state) VALUES (?,?)",
            ("same-context", "tracking"),
        )
        store.db.execute(
            "INSERT INTO wallet_discovery_candidates(wallet,state) VALUES (?,?)",
            ("other-venue", "tracking"),
        )

    incumbent = _profile(
        seed_wallet,
        seed_name=seed.name,
        score=-0.02,
        trimmed=-0.01,
        roi=-0.01,
    )
    same_context = _profile("same-context", score=0.15, trimmed=0.10, roi=0.18)
    other_venue = _profile(
        "other-venue",
        venue="PUMP_FUN",
        lifecycle="pump_bonding_curve",
        score=0.90,
        trimmed=0.80,
        roi=1.20,
    )

    status = _governance(store).evaluate([incumbent, same_context, other_venue])

    assert len(status["context_replacement_pairs"]) == 1
    pair = status["context_replacement_pairs"][0]
    assert pair["incumbent_wallet"] == seed_wallet
    assert pair["challenger_wallet"] == "same-context"
    assert pair["recommended_action"] == "replace_incumbent_for_future_context_influence"
    assert pair["same_venue_lifecycle_regime_role_required"] is True
    assert pair["cross_context_success_transfer_allowed"] is False


def test_governance_is_shadow_only_and_does_not_modify_fomo_scope(tmp_path):
    status = _governance(_store(tmp_path)).evaluate([])

    assert status["fomo_scope_modified"] is False
    assert status["recommendations_have_tracking_mutation_authority"] is False
    assert status["recommendations_have_strategy_authority"] is False
    assert status["active_strategy_mutation_allowed"] is False
    assert status["active_tracking_mutation_allowed"] is False
    assert status["historical_promotion_authority"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
