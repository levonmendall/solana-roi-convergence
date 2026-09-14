from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone

import pytest

from solana_roi import v52_market_validation_controls as controls
from solana_roi import v52_market_validation_governance as governance
from solana_roi import v52_market_validation_completion as completion


class _Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row

    def append(self, *_args, **_kwargs) -> None:
        return None


def _engine() -> tuple[_Store, controls.MarketValidationController, governance.MarketValidationGovernance, completion.MarketValidationCompletion]:
    store = _Store()
    controller = controls.MarketValidationController(store)
    governed = governance.MarketValidationGovernance(controller)
    engine = completion.MarketValidationCompletion(controller, governed)
    return store, controller, governed, engine


def _seed_lane_returns(store: _Store, at: datetime, values: list[float]) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_profit_signal_events ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,lane TEXT NOT NULL,observed_at TEXT NOT NULL,realized_net_return REAL)"
        )
        store.db.executemany(
            "INSERT INTO v52_profit_signal_events(lane,observed_at,realized_net_return) VALUES (?,?,?)",
            [("graduation_continuation", (at - timedelta(minutes=i + 1)).isoformat(), value) for i, value in enumerate(values)],
        )


def test_a_to_g_variants_are_permanent_research_only_and_sequential() -> None:
    _, _, _, engine = _engine()
    decision = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    grad = governance.PointInTimeComposite(0.8, True, {}, {}, {}, decision.isoformat())
    lane = governance.PointInTimeComposite(0.7, True, {}, {}, {}, decision.isoformat())
    decay = governance.ContinuationPersistence(0.6, True, 0.8, 0.6, 0.16, 0.84, {}, {}, 30.0, 30.0)
    state = completion.LaneCapitalState("pump_amm", "reduced", True, 0.5, "test", decision.isoformat())

    variants = engine.shadow_decisions(
        authoritative_fraction=0.10,
        authority={"wallet_target_utilization_multiplier": 1.25},
        lifecycle_state="pump_amm_post_graduation",
        graduation_state="graduated",
        graduation_quality=grad,
        lane_relative_score=lane,
        continuation=decay,
        lane_state=state,
    )

    assert tuple(variants) == completion.SHADOW_VARIANTS
    assert all(item["controls_trading"] is False for item in variants.values())
    assert variants["A_graduation_only"]["position_fraction"] > 0
    assert variants["B_v52_no_wallet"]["position_fraction"] < variants["C_v52_full"]["position_fraction"]
    assert variants["G_full_proposed_alpha_gated"]["position_fraction"] < variants["F_v52_plus_graduation_quality_decay_lane_calibration"]["position_fraction"]


def test_graduation_only_never_enters_pre_graduation() -> None:
    _, _, _, engine = _engine()
    at = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    empty = governance.PointInTimeComposite(None, False, {}, {}, {}, at.isoformat())
    decay = governance.ContinuationPersistence(None, False, None, None, 0.0, 1.0, {}, {}, None, None)
    state = completion.LaneCapitalState("pump_fun", "insufficient_evidence", True, 1.0, "cold", at.isoformat())
    variants = engine.shadow_decisions(
        authoritative_fraction=0.05,
        authority={},
        lifecycle_state="pump_bonding_curve",
        graduation_state="pre_graduation",
        graduation_quality=empty,
        lane_relative_score=empty,
        continuation=decay,
        lane_state=state,
    )
    assert variants["A_graduation_only"]["position_fraction"] == 0.0


def test_reduced_state_exists_before_observe_only_and_is_reversible() -> None:
    store, _, _, engine = _engine()
    at = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    _seed_lane_returns(store, at, [-0.8] * 60)
    first = engine.lane_capital_state("pump_amm", at)
    second = engine.lane_capital_state("pump_amm", at + timedelta(minutes=15))
    third = engine.lane_capital_state("pump_amm", at + timedelta(minutes=30))
    assert first.mode == "reduced"
    assert second.mode == "reduced"
    assert third.mode == "observe_only"
    assert third.capital_allowed is False

    recover = at + timedelta(minutes=40)
    _seed_lane_returns(store, recover, [1.0] * 180)
    engine.lane_capital_state("pump_amm", at + timedelta(minutes=60))
    engine.lane_capital_state("pump_amm", at + timedelta(minutes=75))
    active = engine.lane_capital_state("pump_amm", at + timedelta(minutes=90))
    assert active.mode == "active"
    assert active.capital_multiplier == 1.0


def test_independent_actor_enrichment_is_point_in_time() -> None:
    store, _, _, engine = _engine()
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE wallet_discovery_forward_observations ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,token_mint TEXT,received_at TEXT,wallet TEXT,side TEXT,"
            "funding_cluster_id TEXT,creator_associated INTEGER,amount_sol REAL)"
        )
    decision = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    with store._lock, store.db:
        store.db.executemany(
            "INSERT INTO wallet_discovery_forward_observations(token_mint,received_at,wallet,side,funding_cluster_id,creator_associated,amount_sol) VALUES (?,?,?,?,?,?,?)",
            [
                ("T", (decision - timedelta(seconds=2)).isoformat(), "w1", "buy", "cluster-a", 0, 1.0),
                ("T", (decision - timedelta(seconds=1)).isoformat(), "w2", "buy", "cluster-a", 0, 1.0),
                ("T", (decision + timedelta(seconds=1)).isoformat(), "w3", "buy", "cluster-b", 1, 1.0),
            ],
        )
    rows = engine.participants_before("T", decision)
    metrics = governance.independent_actor_metrics(rows)
    assert len(rows) == 2
    assert metrics.unique_wallets == 2
    assert metrics.independent_economic_actors == 1
    assert metrics.creator_or_funder_associated_actors == 0


def test_evaluation_records_point_in_time_decision_all_variants_and_horizons() -> None:
    store, _, _, engine = _engine()
    at = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    evaluation = engine.evaluate_candidate(
        lane="pump_amm",
        observed_at=at,
        payload={
            "source_signature": "sig-1",
            "token_mint": "T",
            "wallet": "W",
            "buy_sell_imbalance": 0.2,
            "transaction_velocity": 1.5,
            "v52_authority": {"wallet_target_utilization_multiplier": 1.0},
        },
        authoritative_fraction=0.05,
        lifecycle_state="pump_amm_post_graduation",
        graduation_state="graduated",
        earliest_executable_price=0.001,
        eligible=True,
        executed=True,
        discovery_route="pumpswap",
        market_state="active_fomo",
    )
    assert evaluation.candidate_key == "sig-1"
    with store._lock:
        pit = store.db.execute("SELECT future_outcome_json FROM v52_market_validation_point_in_time").fetchone()
        shadows = store.db.execute("SELECT COUNT(*) FROM v52_market_validation_shadow_variants").fetchone()[0]
        horizons = store.db.execute("SELECT COUNT(*) FROM v52_market_validation_continuation_horizons").fetchone()[0]
    assert pit["future_outcome_json"] is None
    assert shadows == 7
    assert horizons == len(completion.CONTINUATION_HORIZONS_SECONDS) + 2


def test_horizon_marks_resolve_only_when_due_and_include_costs() -> None:
    store, _, _, engine = _engine()
    at = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    engine.evaluate_candidate(
        lane="pump_amm",
        observed_at=at,
        payload={"source_signature": "sig-h", "token_mint": "T"},
        authoritative_fraction=0.05,
        lifecycle_state="graduated",
        graduation_state="graduated",
        earliest_executable_price=1.0,
    )
    assert engine.record_market_mark(token_mint="T", marked_at=at + timedelta(seconds=14), price=1.10, cost_fraction=0.01) == 0
    resolved = engine.record_market_mark(token_mint="T", marked_at=at + timedelta(seconds=31), price=1.10, cost_fraction=0.01)
    assert resolved == 2
    with store._lock:
        row = store.db.execute(
            "SELECT gross_forward_return,net_executable_forward_return FROM v52_market_validation_continuation_horizons WHERE horizon_label='15s'"
        ).fetchone()
    assert row["gross_forward_return"] == pytest.approx(0.10)
    assert row["net_executable_forward_return"] == pytest.approx(0.09)


def test_resolve_outcome_updates_lane_accounting_and_b_to_g_without_fabricating_a() -> None:
    store, _, _, engine = _engine()
    at = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    engine.evaluate_candidate(
        lane="pump_amm",
        observed_at=at,
        payload={"source_signature": "sig-o", "token_mint": "T"},
        authoritative_fraction=0.10,
        lifecycle_state="pre_graduation",
        graduation_state="pre_graduation",
    )
    engine.resolve_outcome(candidate_key="sig-o", observed_at=at, net_return=0.20, gross_return=0.23, fees_fraction=0.01, slippage_fraction=0.02)
    metrics = engine.lane_accounting("pump_amm", as_of=at + timedelta(minutes=1), window=timedelta(days=1))
    assert metrics["executed_trade_count"] == 1
    assert metrics["net_return"] == pytest.approx(0.20)
    with store._lock:
        a = store.db.execute("SELECT outcome_status,net_return FROM v52_market_validation_shadow_variants WHERE variant_id='A_graduation_only'").fetchone()
        c = store.db.execute("SELECT outcome_status,net_return FROM v52_market_validation_shadow_variants WHERE variant_id='C_v52_full'").fetchone()
    assert a["outcome_status"] == "requires_graduation_entry_counterfactual"
    assert a["net_return"] is None
    assert c["outcome_status"] == "resolved_same_path"
    assert c["net_return"] == pytest.approx(0.20)


def test_controlled_summary_explicitly_marks_insufficient_evidence() -> None:
    _, _, _, engine = _engine()
    at = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    engine.evaluate_candidate(
        lane="pump_fun",
        observed_at=at,
        payload={"source_signature": "sig-s", "token_mint": "T"},
        authoritative_fraction=0.01,
        lifecycle_state="pump_bonding_curve",
        graduation_state="pre_graduation",
    )
    report = engine.controlled_variant_summary(as_of=at + timedelta(minutes=1), window=timedelta(days=1), starting_capital=500.0)
    assert set(report) == set(completion.SHADOW_VARIANTS)
    assert report["C_v52_full"]["validation_status"] == "insufficient_point_in_time_evidence"
    assert report["C_v52_full"]["starting_capital"] == 500.0


def test_status_declares_full_requested_runtime_capabilities_without_live_authority() -> None:
    _, _, _, engine = _engine()
    payload = engine.status()
    assert payload["a_to_g_shadow_variants"] == list(completion.SHADOW_VARIANTS)
    assert payload["continuation_horizons_seconds"] == [15, 30, 60, 90, 120, 180, 300]
    assert payload["lane_states"] == ["active", "reduced", "observe_only", "insufficient_evidence"]
    assert payload["all_shadow_variants_non_authoritative"] is True
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
