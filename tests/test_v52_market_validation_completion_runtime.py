from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone

import pytest

from solana_roi import v52_market_validation_completion as completion
from solana_roi import v52_market_validation_completion_runtime as runtime_module
from solana_roi import v52_market_validation_controls as controls
from solana_roi import v52_market_validation_governance as governance


class _Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row

    def append(self, *_args, **_kwargs) -> None:
        return None


def _runtime():
    store = _Store()
    controller = controls.MarketValidationController(store)
    governed = governance.MarketValidationGovernance(controller)
    engine = completion.MarketValidationCompletion(controller, governed)
    hardened = runtime_module.install_v52_market_validation_completion_runtime(engine)
    return store, controller, governed, engine, hardened


def _seed_returns(store: _Store, at: datetime, values: list[float], lane: str = "graduation_continuation") -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_profit_signal_events ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,lane TEXT NOT NULL,observed_at TEXT NOT NULL,realized_net_return REAL)"
        )
        store.db.executemany(
            "INSERT INTO v52_profit_signal_events(lane,observed_at,realized_net_return) VALUES (?,?,?)",
            [(lane, (at - timedelta(minutes=index + 1)).isoformat(), value) for index, value in enumerate(values)],
        )


def test_point_in_time_actor_linkage_excludes_future_and_never_invents_clusters() -> None:
    _, _, _, _, hardened = _runtime()
    at = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    payload = {
        "participants": [
            {"wallet": "w1", "funding_cluster_id": "cluster-a", "observed_at": (at - timedelta(seconds=2)).isoformat(), "side": "buy"},
            {"wallet": "w2", "funding_cluster_id": "cluster-a", "observed_at": (at - timedelta(seconds=1)).isoformat(), "side": "buy"},
            {"wallet": "w3", "observed_at": (at - timedelta(milliseconds=500)).isoformat(), "side": "buy"},
            {"wallet": "future", "funding_cluster_id": "cluster-future", "observed_at": (at + timedelta(seconds=1)).isoformat(), "side": "buy"},
        ]
    }
    rows, source = hardened.participants(payload, None, at)
    actors = governance.independent_actor_metrics(rows)
    assert source == "payload"
    assert {row["wallet"] for row in rows} == {"w1", "w2", "w3"}
    assert actors.unique_wallets == 3
    assert actors.independent_economic_actors == 2
    # w3 has no explicit identity link and therefore remains its own actor.
    assert actors.linked_wallet_clustering == pytest.approx(1.0 / 3.0)


def test_durable_actor_discovery_requires_token_time_and_explicit_identity_columns() -> None:
    store, _, _, _, hardened = _runtime()
    at = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE real_wallet_observations ("
            "id INTEGER PRIMARY KEY,token_mint TEXT,observed_at TEXT,wallet TEXT,linked_entity_id TEXT,side TEXT,amount_sol REAL)"
        )
        store.db.executemany(
            "INSERT INTO real_wallet_observations(token_mint,observed_at,wallet,linked_entity_id,side,amount_sol) VALUES (?,?,?,?,?,?)",
            [
                ("T", (at - timedelta(seconds=2)).isoformat(), "w1", "entity-1", "buy", 1.0),
                ("T", (at - timedelta(seconds=1)).isoformat(), "w2", "entity-1", "buy", 2.0),
                ("T", (at + timedelta(seconds=1)).isoformat(), "future", "entity-2", "buy", 3.0),
            ],
        )
    rows, source = hardened.participants({}, "T", at)
    actors = governance.independent_actor_metrics(rows)
    assert source == "durable"
    assert len(rows) == 2
    assert actors.unique_wallets == 2
    assert actors.independent_economic_actors == 1


def test_reduced_capital_multiplier_is_evidence_derived_not_fixed_half() -> None:
    store, _, _, engine, _ = _runtime()
    at = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    values = [0.02 if index % 2 == 0 else -0.01 for index in range(60)]
    _seed_returns(store, at, values)
    state = engine.lane_capital_state("pump_amm", at)
    assert state.mode == "reduced"
    assert 0.0 < state.capital_multiplier < 1.0
    assert state.capital_multiplier != pytest.approx(0.50)
    assert "evidence_derived" in state.reason


def test_fomo_archetypes_calibrate_separately_but_keep_one_execution_lane() -> None:
    store, _, _, engine, _ = _runtime()
    at = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    for index, state in enumerate(("active_fomo", "fomo_exhaustion")):
        engine.evaluate_candidate(
            lane="fomo",
            observed_at=at + timedelta(seconds=index),
            payload={"source_signature": f"sig-{index}", "token_mint": f"T{index}", "velocity": 1.0 + index},
            authoritative_fraction=0.05,
            lifecycle_state="pump_amm_post_graduation",
            graduation_state="graduated",
            market_state=state,
            discovery_route="fomo",
        )
    with store._lock:
        calibration = [str(row[0]) for row in store.db.execute(
            "SELECT DISTINCT calibration_lane FROM v52_market_validation_completion_evaluations ORDER BY calibration_lane"
        ).fetchall()]
        execution = [str(row[0]) for row in store.db.execute(
            "SELECT DISTINCT execution_lane FROM v52_market_validation_completion_evaluations"
        ).fetchall()]
    assert calibration == ["fomo::active_fomo", "fomo::fomo_exhaustion"]
    assert execution == ["fomo"]


def test_duplicate_candidate_does_not_pollute_feature_history() -> None:
    store, _, _, engine, _ = _runtime()
    at = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    kwargs = dict(
        lane="pump_amm",
        observed_at=at,
        payload={"source_signature": "dup", "token_mint": "T", "velocity": 1.0, "buy_sell_imbalance": 0.2},
        authoritative_fraction=0.05,
        lifecycle_state="graduated",
        graduation_state="graduated",
    )
    first = engine.evaluate_candidate(**kwargs)
    with store._lock:
        count_before = int(store.db.execute("SELECT COUNT(*) FROM v52_market_validation_features").fetchone()[0])
    second = engine.evaluate_candidate(**kwargs)
    with store._lock:
        count_after = int(store.db.execute("SELECT COUNT(*) FROM v52_market_validation_features").fetchone()[0])
        registry = int(store.db.execute("SELECT COUNT(*) FROM v52_market_validation_completion_evaluations").fetchone()[0])
    assert first == second
    assert count_after == count_before
    assert registry == 1


def test_graduation_only_shadow_enters_at_real_graduation_mark_then_resolves_from_own_entry() -> None:
    store, _, _, engine, _ = _runtime()
    at = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    engine.evaluate_candidate(
        lane="pump_fun",
        observed_at=at,
        payload={"source_signature": "grad-a", "token_mint": "T"},
        authoritative_fraction=0.10,
        lifecycle_state="pump_bonding_curve",
        graduation_state="pre_graduation",
    )
    engine.record_market_mark(
        token_mint="T",
        marked_at=at + timedelta(seconds=10),
        price=2.0,
        cost_fraction=0.01,
        graduation_state="graduated",
    )
    with store._lock:
        entry = store.db.execute("SELECT * FROM v52_market_validation_shadow_entries").fetchone()
        shadow = store.db.execute(
            "SELECT decision_fraction,net_return FROM v52_market_validation_shadow_variants WHERE variant_id='A_graduation_only'"
        ).fetchone()
    assert entry is not None
    assert float(entry["entry_price"]) == pytest.approx(2.0)
    assert float(shadow["decision_fraction"]) == pytest.approx(0.10)
    assert shadow["net_return"] is None

    engine.record_market_mark(
        token_mint="T",
        marked_at=at + timedelta(seconds=30),
        price=2.2,
        cost_fraction=0.01,
        graduation_state="post_graduation",
    )
    with store._lock:
        resolved = store.db.execute(
            "SELECT net_return,portfolio_contribution,outcome_status FROM v52_market_validation_shadow_variants WHERE variant_id='A_graduation_only'"
        ).fetchone()
    assert float(resolved["net_return"]) == pytest.approx(0.08)
    assert float(resolved["portfolio_contribution"]) == pytest.approx(0.008)
    assert resolved["outcome_status"] == "resolved_graduation_counterfactual"


def test_sequential_ablation_persists_actual_with_and_without_values_without_causal_claim() -> None:
    store, _, _, engine, _ = _runtime()
    at = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    engine.evaluate_candidate(
        lane="pump_amm",
        observed_at=at,
        payload={"source_signature": "ablate", "token_mint": "T"},
        authoritative_fraction=0.10,
        lifecycle_state="graduated",
        graduation_state="graduated",
    )
    variants = {
        "A_graduation_only": 0.01,
        "B_v52_no_wallet": 0.02,
        "C_v52_full": 0.03,
        "D_v52_plus_graduation_quality": 0.04,
        "E_v52_plus_graduation_quality_decay": 0.05,
        "F_v52_plus_graduation_quality_decay_lane_calibration": 0.06,
        "G_full_proposed_alpha_gated": 0.07,
    }
    engine.resolve_outcome(candidate_key="ablate", observed_at=at, net_return=0.03, variant_returns=variants)
    with store._lock:
        row = store.db.execute(
            "SELECT with_component_return,without_component_return,incremental_return,causal_claim "
            "FROM v52_market_validation_component_ablation WHERE component='graduation_quality'"
        ).fetchone()
    assert float(row["with_component_return"]) == pytest.approx(0.04)
    assert float(row["without_component_return"]) == pytest.approx(0.03)
    assert float(row["incremental_return"]) == pytest.approx(0.01)
    assert int(row["causal_claim"]) == 0


def test_runtime_status_preserves_paper_only_authority_and_does_not_fabricate_pons() -> None:
    _, _, _, _, hardened = _runtime()
    payload = hardened.status()
    assert payload["duplicate_candidate_observation_protection"] is True
    assert payload["fomo_archetype_specific_calibration"] is True
    assert payload["evidence_derived_reduced_multiplier"] is True
    assert payload["fixed_reduced_multiplier"] is None
    assert payload["sequential_a_to_g_ablation"] is True
    assert payload["distinct_pons_lane_present_in_repository"] is False
    assert payload["pons_lane_fabricated"] is False
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
