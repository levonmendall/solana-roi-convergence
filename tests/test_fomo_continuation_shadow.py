from __future__ import annotations

import sqlite3
from types import SimpleNamespace

from solana_roi.fomo_continuation_shadow import (
    FOMO_LANE,
    FomoContinuationShadow,
    FomoOutcome,
    build_fomo_features,
    classify_fomo_state,
)


class Store:
    def __init__(self) -> None:
        import threading
        self._lock = threading.RLock()
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row


def _features(**overrides):
    values = dict(
        token_mint="mint",
        observed_at="2026-09-04T00:00:00+00:00",
        venue="pump_fun",
        lifecycle="early_continuation",
        regime="high_speculation",
        independent_buyers_short=4,
        independent_buyers_long=5,
        buys_short=8,
        buys_long=10,
        sells_short=1,
        sells_long=3,
        buy_volume_short=12.0,
        buy_volume_long=15.0,
        sell_volume_short=1.0,
        sell_volume_long=3.0,
        momentum_wallet_participation=2,
        creator_accumulating=True,
        creator_distributing=False,
        early_holder_exit_fraction=0.02,
        chase_fraction=0.06,
        signal_to_entry_seconds=5.0,
        quote_deterioration_fraction=0.0,
        depth_growth_fraction=0.2,
        exit_slippage_deterioration_fraction=0.0,
        risk_complete=True,
    )
    values.update(overrides)
    return build_fomo_features(**values)


def test_active_fomo_requires_real_acceleration_and_accessibility() -> None:
    features = _features()
    state = classify_fomo_state(features)
    assert state.state == "active_fomo"
    assert state.structurally_accessible is True
    assert features.new_buyer_acceleration > 1.0
    assert features.transaction_frequency_acceleration > 1.0
    assert features.net_buy_flow_acceleration > 1.0


def test_late_fomo_fails_closed_on_chase_latency_and_distribution() -> None:
    features = _features(
        chase_fraction=0.20,
        signal_to_entry_seconds=25.0,
        creator_distributing=True,
        early_holder_exit_fraction=0.30,
    )
    state = classify_fomo_state(features)
    assert state.state == "late_or_inaccessible_fomo"
    assert state.structurally_accessible is False
    assert "chase_above_limit" in state.blockers
    assert "signal_to_entry_above_limit" in state.blockers
    assert "creator_distributing" in state.blockers
    assert "early_holder_distribution" in state.blockers


def test_missing_execution_evidence_fails_closed() -> None:
    features = _features(chase_fraction=None, signal_to_entry_seconds=None, risk_complete=False)
    state = classify_fomo_state(features)
    assert state.structurally_accessible is False
    assert set(state.blockers) >= {"chase_unknown", "signal_to_entry_unknown", "risk_incomplete"}


def test_shadow_store_reports_percentage_roi_trim_and_decay() -> None:
    store = Store()
    shadow = FomoContinuationShadow(store, release_commit="abc")
    features = _features()
    state = classify_fomo_state(features)
    shadow.record_observation(source_signature="sig-obs", features=features, state=state)
    returns = [0.10, 0.20, -0.05, 0.30, 0.15, 0.05]
    delays = [1.0, 2.0, 5.0, 10.0, 20.0, 30.0]
    for index, (value, delay) in enumerate(zip(returns, delays)):
        shadow.record_outcome(
            FomoOutcome(
                source_signature=f"sig-{index}",
                observed_at="2026-09-04T00:00:00+00:00",
                venue="pump_fun",
                lifecycle="early_continuation",
                regime="high_speculation",
                fomo_state="active_fomo",
                signal_to_entry_seconds=delay,
                net_return=value,
                release_commit="abc",
            )
        )
    status = shadow.status()
    assert status["lane"] == FOMO_LANE
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["active_strategy_mutation_allowed"] is False
    assert status["outcome_count"] == 6
    assert status["mean_residual_roi_pct"] is not None
    assert status["trimmed_mean_residual_roi_ex_best_1_pct"] is not None
    assert status["signal_decay"]["5"]["sample_count"] == 1


def test_feature_model_is_not_just_confirmation_count() -> None:
    flat = _features(
        independent_buyers_short=1,
        independent_buyers_long=4,
        buys_short=1,
        buys_long=4,
        buy_volume_short=1.0,
        buy_volume_long=4.0,
        sell_volume_short=1.0,
        sell_volume_long=3.0,
        momentum_wallet_participation=0,
        creator_accumulating=False,
        depth_growth_fraction=0.0,
    )
    accelerating = _features()
    flat_state = classify_fomo_state(flat)
    accelerating_state = classify_fomo_state(accelerating)
    assert accelerating_state.score > flat_state.score
    assert accelerating.new_buyer_acceleration > flat.new_buyer_acceleration
    assert accelerating.net_buy_flow_acceleration > flat.net_buy_flow_acceleration
