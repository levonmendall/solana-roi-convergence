from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone

import pytest

from solana_roi import v52_market_validation_controls as controls
from solana_roi import v52_market_validation_governance as governance


class _Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row


def _controller() -> tuple[_Store, controls.MarketValidationController, governance.MarketValidationGovernance]:
    store = _Store()
    controller = controls.MarketValidationController(store)
    governed = governance.MarketValidationGovernance(controller)
    return store, controller, governed


def test_independent_actor_count_collapses_linked_wallets_and_detects_contamination() -> None:
    metrics = governance.independent_actor_metrics(
        [
            {"wallet": "w1", "funding_cluster_id": "cluster-a", "notional": 10.0},
            {"wallet": "w2", "funding_cluster_id": "cluster-a", "notional": 10.0},
            {"wallet": "w3", "linked_entity_id": "entity-b", "notional": 10.0, "creator_associated": True},
            {"wallet": "w4", "linked_entity_id": "entity-b", "notional": 10.0, "funder_associated": True},
        ]
    )

    assert metrics.unique_wallets == 4
    assert metrics.independent_economic_actors == 2
    assert metrics.independent_buyer_breadth == pytest.approx(0.5)
    assert metrics.creator_or_funder_associated_actors == 1
    assert metrics.creator_or_funder_contamination == pytest.approx(0.5)
    assert metrics.linked_wallet_clustering == pytest.approx(0.5)


def test_point_in_time_history_never_uses_later_observations() -> None:
    _, controller, governed = _controller()
    before = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    decision = before + timedelta(minutes=1)
    after = decision + timedelta(minutes=1)
    controller.observe_features("pump_amm", {"velocity": 1.0}, observed_at=before.isoformat())
    controller.observe_features("pump_amm", {"velocity": 999.0}, observed_at=after.isoformat())

    history = governed.history_before("pump_amm", ("velocity",), decision)

    assert history["velocity"] == [1.0]


def test_graduation_composite_uses_evidence_families_to_limit_double_counting() -> None:
    _, controller, governed = _controller()
    decision = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    features = tuple(key for keys in governance.GRADUATION_FEATURE_FAMILIES.values() for key in keys)
    for offset in range(40):
        controller.observe_features(
            "pump_amm",
            {key: float(offset) for key in features},
            observed_at=(decision - timedelta(minutes=offset + 1)).isoformat(),
        )
    metrics = {key: 35.0 for key in features}
    metrics.update(
        {
            "graduation_speed_seconds": 2.0,
            "concentration": 2.0,
            "creator_associated_activity": 2.0,
            "funder_associated_activity": 2.0,
            "linked_wallet_clustering": 2.0,
            "repeated_entity_activity": 2.0,
            "abnormal_coordinated_buying": 2.0,
            "suspicious_liquidity_behavior": 2.0,
        }
    )

    quality = governed.graduation_quality_at("pump_amm", metrics, decision)

    assert quality.calibrated is True
    assert quality.score is not None
    assert set(quality.family_scores) == set(governance.GRADUATION_FEATURE_FAMILIES)
    assert quality.future_observations_used == 0
    assert len(quality.family_scores) < len(quality.component_percentiles)


def test_continuation_model_treats_missing_follow_through_as_negative_evidence_without_hard_exit() -> None:
    _, controller, governed = _controller()
    decision = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    features = tuple(key for keys in governance.CONTINUATION_FEATURE_FAMILIES.values() for key in keys)
    for offset in range(40):
        controller.observe_features(
            "pump_amm",
            {key: float(offset) / 40.0 for key in features},
            observed_at=(decision - timedelta(minutes=offset + 1)).isoformat(),
        )
    current = {key: 0.01 for key in features}
    current["maximum_adverse_excursion"] = 0.99
    current["seconds_since_graduation"] = 120.0
    current["seconds_since_entry"] = 90.0

    decay = governed.continuation_at(
        "pump_amm",
        current,
        decision,
        expected_continuation=0.90,
    )

    assert decay.calibrated is True
    assert decay.actual_continuation is not None and decay.actual_continuation < 0.25
    assert decay.no_continuation_negative_evidence > 0.0
    assert decay.evidence_multiplier < 1.0
    assert decay.hard_exit_seconds is None


def _create_return_table(store: _Store) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE v52_profit_signal_events ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, lane TEXT NOT NULL, observed_at TEXT NOT NULL, "
            "realized_net_return REAL)"
        )


def _insert_returns(store: _Store, lane: str, at: datetime, values: list[float]) -> None:
    with store._lock, store.db:
        store.db.executemany(
            "INSERT INTO v52_profit_signal_events(lane,observed_at,realized_net_return) VALUES (?,?,?)",
            [(lane, (at - timedelta(minutes=i + 1)).isoformat(), value) for i, value in enumerate(values)],
        )


def test_lane_gating_requires_persistent_multi_horizon_negative_evidence() -> None:
    store, _, governed = _controller()
    _create_return_table(store)
    start = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    _insert_returns(store, "graduation_continuation", start - timedelta(hours=1), [-0.80] * 60)

    first = governed.governed_lane_state_at("pump_amm", start)
    second = governed.governed_lane_state_at("pump_amm", start + timedelta(minutes=15))
    third = governed.governed_lane_state_at("pump_amm", start + timedelta(minutes=30))

    assert first.capital_allowed is True
    assert second.capital_allowed is True
    assert third.mode == "observe_only"
    assert third.capital_allowed is False
    assert third.negative_streak >= governance.DEACTIVATION_PERSISTENCE_BUCKETS


def test_observe_only_lane_reactivates_only_after_persistent_recovery() -> None:
    store, _, governed = _controller()
    _create_return_table(store)
    start = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    _insert_returns(store, "graduation_continuation", start - timedelta(hours=1), [-0.80] * 60)
    governed.governed_lane_state_at("pump_amm", start)
    governed.governed_lane_state_at("pump_amm", start + timedelta(minutes=15))
    disabled = governed.governed_lane_state_at("pump_amm", start + timedelta(minutes=30))
    assert disabled.mode == "observe_only"

    recovery_at = start + timedelta(minutes=40)
    _insert_returns(store, "graduation_continuation", recovery_at, [1.0] * 180)
    one = governed.governed_lane_state_at("pump_amm", start + timedelta(minutes=60))
    two = governed.governed_lane_state_at("pump_amm", start + timedelta(minutes=75))
    three = governed.governed_lane_state_at("pump_amm", start + timedelta(minutes=90))

    assert one.mode == "observe_only"
    assert two.mode == "observe_only"
    assert three.mode == "active"
    assert three.capital_allowed is True
    assert three.positive_streak >= governance.REACTIVATION_PERSISTENCE_BUCKETS


def test_small_sample_never_disables_existing_v52_authority() -> None:
    store, _, governed = _controller()
    _create_return_table(store)
    at = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    _insert_returns(store, "graduation_continuation", at, [-1.0] * 3)

    state = governed.governed_lane_state_at("pump_amm", at + timedelta(minutes=1))
    gate = governed.alpha_gate_at("pump_amm", at + timedelta(minutes=1))

    assert state.mode == "insufficient_evidence"
    assert state.capital_allowed is True
    assert gate.mode == "preserve_v52"
    assert gate.capital_allowed is True


def test_point_in_time_decision_keeps_future_outcome_separate() -> None:
    store, _, governed = _controller()
    observed = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    governed.record_point_in_time_decision(
        candidate_key="candidate-1",
        lane="pump_amm",
        observed_at=observed,
        lifecycle_state="graduated",
        graduation_state="confirmed",
        raw_features={"velocity": 1.0},
        relative_features={"velocity_percentile": 0.8},
        graduation_quality=0.7,
        continuation_persistence=0.6,
        lane_alpha_mode="active",
        wallet_evidence={"wallet_quality": 0.8},
        v52_decision={"decision": "paper_enter"},
        shadow_decision={"v52_no_wallet": "paper_enter"},
        earliest_executable_price=0.001,
    )
    with store._lock:
        row = store.db.execute(
            "SELECT future_outcome_json,paper_only,live_money_authority FROM v52_market_validation_point_in_time"
        ).fetchone()
    assert row["future_outcome_json"] is None
    assert row["paper_only"] == 1 and row["live_money_authority"] == 0

    governed.resolve_future_outcome("candidate-1", observed, {"net_return": 0.25})
    with store._lock:
        row = store.db.execute(
            "SELECT future_outcome_json FROM v52_market_validation_point_in_time"
        ).fetchone()
    assert "0.25" in row["future_outcome_json"]


def test_component_attribution_is_paired_and_never_claims_causality() -> None:
    result = governance.component_attribution(
        {
            "v52_full": 0.20,
            "v52_no_wallet": 0.10,
            "graduation_only_continuation": 0.04,
            "full_proposed": 0.25,
            "no_decay": 0.18,
            "no_lane_gating": 0.22,
        }
    )

    assert result["wallet_intelligence"].incremental_return == pytest.approx(0.10)
    assert result["pre_graduation_entry_bundle"].incremental_return == pytest.approx(0.06)
    assert result["post_graduation_decay"].incremental_return == pytest.approx(0.07)
    assert result["lane_gating"].incremental_return == pytest.approx(0.03)
    assert all(item.causal_claim is False for item in result.values())


def test_governance_status_preserves_authority_boundaries() -> None:
    payload = governance.status()

    assert payload["future_leakage_allowed"] is False
    assert payload["shadow_strategies_control_trading"] is False
    assert payload["hard_post_graduation_exit_seconds"] is None
    assert payload["activation_deactivation_hysteresis"] is True
    assert payload["automatic_reactivation"] is True
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
