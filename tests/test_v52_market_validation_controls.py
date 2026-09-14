from __future__ import annotations

import sqlite3
import threading

import pytest

from solana_roi import v52_market_validation_controls as controls


class _Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row


def _history(keys: tuple[str, ...], n: int = 20) -> dict[str, list[float]]:
    return {key: [float(i) for i in range(n)] for key in keys}


def test_graduation_quality_is_lane_relative_without_fixed_buyer_or_time_thresholds() -> None:
    history = _history(controls.GRADUATION_COMPONENTS)
    metrics = {
        "graduation_speed_seconds": 2.0,
        "independent_buyer_breadth": 18.0,
        "buyer_acceleration": 18.0,
        "buy_sell_imbalance": 18.0,
        "concentration": 2.0,
        "liquidity_formation": 18.0,
        "creator_associated_activity": 2.0,
    }

    quality = controls.graduation_quality_score(metrics, history, minimum_samples=20)

    assert quality.calibrated is True
    assert quality.score is not None and quality.score > 0.75
    assert quality.organic_participation_evidence == quality.score
    assert quality.fixed_seconds_threshold_used is False
    assert quality.fixed_buyer_threshold_used is False


def test_same_absolute_opportunity_scores_differ_by_lane_population() -> None:
    metrics = {key: 50.0 for key in controls.LANE_RELATIVE_COMPONENTS}
    weak_lane = {key: [float(i) for i in range(20)] for key in controls.LANE_RELATIVE_COMPONENTS}
    strong_lane = {key: [100.0 + float(i) for i in range(20)] for key in controls.LANE_RELATIVE_COMPONENTS}

    weak_relative = controls.lane_relative_opportunity_score(metrics, weak_lane, minimum_samples=20)
    strong_relative = controls.lane_relative_opportunity_score(metrics, strong_lane, minimum_samples=20)

    assert weak_relative.calibrated is True
    assert strong_relative.calibrated is True
    assert weak_relative.score == pytest.approx(1.0)
    assert strong_relative.score == pytest.approx(0.0)


def test_post_graduation_decay_treats_no_continuation_as_negative_evidence_without_hard_clock() -> None:
    history = {
        "seconds_since_graduation": [float(i) for i in range(20)],
        "seconds_since_entry": [float(i) for i in range(20)],
        "continuation_persistence": [i / 20.0 for i in range(20)],
    }
    weak = controls.post_graduation_decay_clock(
        seconds_since_graduation=99.0,
        seconds_since_entry=99.0,
        continuation_persistence=0.01,
        history=history,
        minimum_samples=20,
    )
    strong = controls.post_graduation_decay_clock(
        seconds_since_graduation=1.0,
        seconds_since_entry=1.0,
        continuation_persistence=0.95,
        history=history,
        minimum_samples=20,
    )

    assert weak.calibrated is True
    assert weak.no_continuation_negative_evidence > strong.no_continuation_negative_evidence
    assert weak.evidence_multiplier < strong.evidence_multiplier
    assert weak.hard_wait_seconds is None
    assert weak.hard_stop_seconds is None


def test_decay_clock_is_neutral_during_cold_start() -> None:
    decay = controls.post_graduation_decay_clock(
        seconds_since_graduation=500.0,
        seconds_since_entry=400.0,
        continuation_persistence=0.0,
        history={
            "seconds_since_graduation": [1.0, 2.0],
            "seconds_since_entry": [1.0, 2.0],
            "continuation_persistence": [0.8, 0.9],
        },
        minimum_samples=20,
    )

    assert decay.calibrated is False
    assert decay.evidence_multiplier == 1.0
    assert decay.no_continuation_negative_evidence == 0.0


def test_fomo_market_state_is_orthogonal_to_execution_route() -> None:
    pumpswap = controls.classify_market_context(
        "pump_amm", discovery_route="pumpswap", market_state="active_fomo"
    )
    robinhood = controls.classify_market_context(
        "robinhood", discovery_route="robinhood", market_state="active_fomo"
    )

    assert pumpswap.execution_lane == "pump_amm"
    assert robinhood.execution_lane == "robinhood"
    assert pumpswap.market_archetype == robinhood.market_archetype == "fomo"
    assert pumpswap.discovery_route != robinhood.discovery_route


def test_permanent_shadow_controls_have_no_trading_authority() -> None:
    shadows = controls.shadow_strategy_definitions()

    assert set(shadows) == {
        "graduation_only_continuation",
        "v52_no_wallet",
        "v52_full",
    }
    assert all(item["controls_trading"] is False for item in shadows.values())
    assert shadows["graduation_only_continuation"]["pre_graduation_entry"] is False
    assert shadows["v52_no_wallet"]["wallet_intelligence"] is False
    assert shadows["v52_full"]["wallet_intelligence"] is True


def test_lane_alpha_gate_preserves_cold_start_and_disables_negative_realized_lane() -> None:
    cold = controls.lane_alpha_gate(
        "pump_amm",
        realized_returns=[-0.1] * 5,
        counterfactual_returns=[0.2] * 30,
        minimum_samples=30,
    )
    negative = controls.lane_alpha_gate(
        "pump_amm",
        realized_returns=[-0.01] * 30,
        counterfactual_returns=[0.02] * 30,
        minimum_samples=30,
    )
    positive = controls.lane_alpha_gate(
        "robinhood",
        realized_returns=[0.01] * 30,
        counterfactual_returns=[-0.01] * 30,
        minimum_samples=30,
    )

    assert cold.mode == "preserve_v52" and cold.capital_allowed is True
    assert negative.mode == "observe_only" and negative.capital_allowed is False
    assert negative.strategy_specific_underperformance is True
    assert positive.mode == "active" and positive.capital_allowed is True


def test_durable_controller_uses_aliases_and_keeps_shadow_rows_non_authoritative() -> None:
    store = _Store()
    controller = controls.MarketValidationController(store)
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE v52_profit_signal_events ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, lane TEXT, realized_net_return REAL)"
        )
        store.db.executemany(
            "INSERT INTO v52_profit_signal_events(lane,realized_net_return) VALUES (?,?)",
            [("graduation_continuation", -0.01)] * 30,
        )

    controller.record_shadow_outcome(
        "candidate-1",
        "pump_amm",
        "graduation_only_continuation",
        observed_at="2026-09-13T00:00:00+00:00",
        net_return=0.10,
        resolved_at="2026-09-13T00:10:00+00:00",
        evidence={"paired": True},
    )
    gate = controller.alpha_gate("pumpswap")

    assert gate.mode == "observe_only"
    assert gate.realized_samples == 30
    with store._lock:
        row = store.db.execute(
            "SELECT controls_trading,paper_only,live_money_authority "
            "FROM v52_market_validation_shadow_outcomes"
        ).fetchone()
    assert dict(row) == {
        "controls_trading": 0,
        "paper_only": 1,
        "live_money_authority": 0,
    }


def test_policy_explicitly_preserves_rejected_fixed_rules() -> None:
    payload = controls.status()

    assert payload["graduation_quality_fixed_thresholds"] is False
    assert payload["post_graduation_hard_wait_seconds"] is None
    assert payload["post_graduation_hard_stop_seconds"] is None
    assert payload["ordinary_starter_fraction_unchanged"] == 0.25
    assert payload["fixed_minus_12_stop_added"] is False
    assert payload["fixed_plus_30_principal_recovery_added"] is False
    assert payload["pre_graduation_entries_abandoned"] is False
    assert payload["shadow_strategies_control_trading"] is False
