from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from solana_roi.observation_store import ObservationEventStore
from solana_roi.profit_first_entity_final import UNIFIED_LANE
from solana_roi.wallet_venue_lifecycle_research import (
    PUMP_AMM_POST_BONDING,
    PUMP_BONDING_CURVE,
    RAYDIUM_POST_PUMP,
    RAYDIUM_UNPROVEN,
    VenueLifecycleResearch,
    lifecycle_stage,
    venue_from_source,
)


NOW = datetime(2026, 9, 4, 14, 0, tzinfo=timezone.utc)
EPOCH = "venue-lifecycle-test-epoch"


class _Universe:
    def __init__(self, store):
        self.store = store

    def _epoch_id(self):
        return EPOCH


def _store(tmp_path):
    store = ObservationEventStore(tmp_path / "venue-lifecycle.sqlite3")
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE wallet_discovery_forward_observations ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, signature TEXT UNIQUE NOT NULL, wallet TEXT NOT NULL, "
            "token_mint TEXT NOT NULL, side TEXT NOT NULL, received_at TEXT NOT NULL, source TEXT NOT NULL, "
            "copyable INTEGER NOT NULL, risk_complete INTEGER NOT NULL, observation_lag_ms REAL NOT NULL, "
            "chase_fraction REAL, processing_delay_ms REAL)"
        )
        store.db.execute(
            "CREATE TABLE profit_first_final_epochs ("
            "epoch_id TEXT PRIMARY KEY, strategy_version TEXT, release_commit TEXT, started_at TEXT NOT NULL, "
            "manifest_json TEXT, paper_only INTEGER, live_money_authority INTEGER, historical_promotion_authority INTEGER)"
        )
        store.db.execute(
            "CREATE TABLE profit_first_final_trials ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_id TEXT NOT NULL, source_signature TEXT NOT NULL, "
            "lane TEXT NOT NULL, opportunity_json TEXT)"
        )
        store.db.execute(
            "CREATE TABLE profit_first_final_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_id TEXT NOT NULL, source_signature TEXT NOT NULL, "
            "trigger_wallet TEXT NOT NULL, token_mint TEXT NOT NULL, lane TEXT NOT NULL, "
            "position_fraction REAL NOT NULL, net_return REAL NOT NULL, signal_to_entry_seconds REAL NOT NULL, "
            "exit_observed_at TEXT NOT NULL, evidence_phase TEXT NOT NULL)"
        )
        store.db.execute(
            "INSERT INTO profit_first_final_epochs(epoch_id,started_at) VALUES (?,?)",
            (EPOCH, NOW.isoformat()),
        )
    return store


def _observation(store, *, signature, wallet, token, venue, seconds, copyable=True, chase=0.05):
    at = NOW + timedelta(seconds=seconds)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO wallet_discovery_forward_observations("
            "signature,wallet,token_mint,side,received_at,source,copyable,risk_complete,observation_lag_ms,chase_fraction,processing_delay_ms) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                signature,
                wallet,
                token,
                "buy",
                at.isoformat(),
                f"solana-direct:{venue}:buy",
                1 if copyable else 0,
                1,
                800.0,
                chase,
                120.0,
            ),
        )


def _outcome(store, *, signature, wallet, token, lane, net_return, fraction=0.10, entity="entity:test", seconds=60):
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO profit_first_final_trials(epoch_id,source_signature,lane,opportunity_json) VALUES (?,?,?,?)",
            (EPOCH, signature, lane, json.dumps({"trigger_entity": entity})),
        )
        store.db.execute(
            "INSERT INTO profit_first_final_outcomes("
            "epoch_id,source_signature,trigger_wallet,token_mint,lane,position_fraction,net_return,"
            "signal_to_entry_seconds,exit_observed_at,evidence_phase) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                EPOCH,
                signature,
                wallet,
                token,
                lane,
                fraction,
                net_return,
                2.0,
                (NOW + timedelta(seconds=seconds)).isoformat(),
                "forward",
            ),
        )


def test_venue_and_lifecycle_classification_is_fail_closed():
    assert venue_from_source("solana-direct:PUMP_FUN:buy") == "PUMP_FUN"
    assert venue_from_source("solana-direct:PUMP_AMM:sell") == "PUMP_AMM"
    assert venue_from_source("solana-direct:RAYDIUM:buy") == "RAYDIUM"
    assert venue_from_source("unknown") is None
    assert lifecycle_stage("PUMP_FUN") == PUMP_BONDING_CURVE
    assert lifecycle_stage("PUMP_AMM") == PUMP_AMM_POST_BONDING
    assert lifecycle_stage("RAYDIUM", prior_pump_evidence=True) == RAYDIUM_POST_PUMP
    assert lifecycle_stage("RAYDIUM", prior_pump_evidence=False) == RAYDIUM_UNPROVEN


def test_raydium_is_post_pump_only_when_prior_current_epoch_pump_evidence_exists(tmp_path):
    store = _store(tmp_path)
    _observation(store, signature="pump-a", wallet="wallet-a", token="token-a", venue="PUMP_FUN", seconds=1)
    _observation(store, signature="ray-a", wallet="wallet-a", token="token-a", venue="RAYDIUM", seconds=2)
    _observation(store, signature="ray-native", wallet="wallet-a", token="token-native", venue="RAYDIUM", seconds=3)

    rows = VenueLifecycleResearch(_Universe(store))._observations(NOW.isoformat())
    by_signature = {row["signature"]: row for row in rows}
    assert by_signature["pump-a"]["lifecycle_stage"] == PUMP_BONDING_CURVE
    assert by_signature["ray-a"]["lifecycle_stage"] == RAYDIUM_POST_PUMP
    assert by_signature["ray-native"]["lifecycle_stage"] == RAYDIUM_UNPROVEN


def test_wallet_segments_keep_pump_and_raydium_copyability_separate(tmp_path):
    store = _store(tmp_path)
    _observation(store, signature="pump-1", wallet="wallet-a", token="token-a", venue="PUMP_FUN", seconds=1, copyable=False, chase=0.30)
    _observation(store, signature="ray-1", wallet="wallet-a", token="token-b", venue="RAYDIUM", seconds=2, copyable=True, chase=0.04)

    status = VenueLifecycleResearch(_Universe(store)).status()
    segments = {(row["venue"], row["lifecycle_stage"]): row for row in status["wallet_segments"]}
    assert segments[("PUMP_FUN", PUMP_BONDING_CURVE)]["copyability_rate"] == 0.0
    assert segments[("RAYDIUM", RAYDIUM_UNPROVEN)]["copyability_rate"] == 1.0
    assert segments[("PUMP_FUN", PUMP_BONDING_CURVE)]["median_chase_fraction"] == pytest.approx(0.30)
    assert segments[("RAYDIUM", RAYDIUM_UNPROVEN)]["median_chase_fraction"] == pytest.approx(0.04)


def test_settled_metrics_count_only_unified_lane_and_preserve_point_in_time_entity(tmp_path):
    store = _store(tmp_path)
    _observation(store, signature="sig-a", wallet="wallet-a", token="token-a", venue="PUMP_FUN", seconds=1)
    _outcome(store, signature="sig-a", wallet="wallet-a", token="token-a", lane=UNIFIED_LANE, net_return=0.50, entity="entity:at-entry")
    _outcome(store, signature="sig-a", wallet="wallet-a", token="token-a", lane="clean_scout_alpha", net_return=0.50, entity="entity:other")

    status = VenueLifecycleResearch(_Universe(store)).status()
    segment = next(row for row in status["wallet_segments"] if row["wallet"] == "wallet-a")
    assert segment["closed_outcomes"] == 1
    assert segment["copyable_return_on_deployed_fraction"] == pytest.approx(0.50)
    entity = next(row for row in status["entity_segments"] if row["entity_id"] == "entity:at-entry")
    assert entity["closed_outcomes"] == 1
    assert all(row["entity_id"] != "entity:other" for row in status["entity_segments"])


def test_research_status_never_has_strategy_or_live_money_authority(tmp_path):
    status = VenueLifecycleResearch(_Universe(_store(tmp_path))).status()
    assert status["research_only"] is True
    assert status["segment_scores_have_trade_authority"] is False
    assert status["active_strategy_mutation_allowed"] is False
    assert status["historical_promotion_authority"] is False
    assert status["strategy_thresholds_unchanged"] is True
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
