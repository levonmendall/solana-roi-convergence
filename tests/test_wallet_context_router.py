from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from solana_roi.observation_store import ObservationEventStore
from solana_roi.profit_first_entity_final import UNIFIED_LANE
from solana_roi.wallet_context_router import (
    WalletContextRouter,
    classify_observation_accessibility,
)
from solana_roi.wallet_entity_universe_v4 import WalletRole
from solana_roi.wallet_venue_lifecycle_research import (
    PUMP_BONDING_CURVE,
    RAYDIUM_UNPROVEN,
)


NOW = datetime(2026, 9, 4, 15, 0, tzinfo=timezone.utc)
EPOCH = "wallet-context-router-test"


class _Universe:
    def __init__(self, store, capacity=12):
        self.store = store
        self.discovery = SimpleNamespace(policy=SimpleNamespace(max_tracked_challengers=capacity))

    def _epoch_id(self):
        return EPOCH


def _store(tmp_path):
    store = ObservationEventStore(tmp_path / "wallet-context-router.sqlite3")
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
            "CREATE TABLE profit_first_final_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_id TEXT NOT NULL, source_signature TEXT NOT NULL, "
            "trigger_wallet TEXT NOT NULL, token_mint TEXT NOT NULL, lane TEXT NOT NULL, "
            "position_fraction REAL NOT NULL, net_return REAL NOT NULL, signal_to_entry_seconds REAL NOT NULL, "
            "evidence_phase TEXT NOT NULL, exit_signature TEXT, exit_observed_at TEXT, context_json TEXT)"
        )
        store.db.execute(
            "CREATE TABLE profit_first_final_trials ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_id TEXT NOT NULL, source_signature TEXT NOT NULL, "
            "token_mint TEXT NOT NULL, trigger_wallet TEXT NOT NULL, lane TEXT NOT NULL, received_at TEXT NOT NULL)"
        )
        store.db.execute(
            "CREATE TABLE v4_entity_signal_context ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_id TEXT NOT NULL, token_mint TEXT NOT NULL, "
            "trigger_wallet TEXT NOT NULL, received_at TEXT NOT NULL, independent_wallets_json TEXT NOT NULL)"
        )
        store.db.execute(
            "CREATE TABLE profit_first_final_exit_signals ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_id TEXT NOT NULL, source_signature TEXT NOT NULL, "
            "seller_wallet TEXT NOT NULL, features_json TEXT NOT NULL, signal_json TEXT NOT NULL)"
        )
        store.db.execute(
            "INSERT INTO profit_first_final_epochs(epoch_id,started_at) VALUES (?,?)",
            (EPOCH, NOW.isoformat()),
        )
    return store


def _observation(
    store,
    *,
    signature,
    wallet,
    token,
    venue,
    seconds,
    copyable=True,
    chase=0.05,
    observation_lag_ms=800.0,
    processing_delay_ms=120.0,
):
    at = NOW + timedelta(seconds=seconds)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO wallet_discovery_forward_observations("
            "signature,wallet,token_mint,side,received_at,source,copyable,risk_complete,"
            "observation_lag_ms,chase_fraction,processing_delay_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                signature,
                wallet,
                token,
                "buy",
                at.isoformat(),
                f"solana-direct:{venue}:buy",
                1 if copyable else 0,
                1,
                observation_lag_ms,
                chase,
                processing_delay_ms,
            ),
        )
    return at


def _outcome(
    store,
    *,
    signature,
    wallet,
    token,
    lane,
    net_return,
    fraction=0.10,
    signal_to_entry=2.0,
    regime="neutral",
    exit_signature=None,
):
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO profit_first_final_outcomes("
            "epoch_id,source_signature,trigger_wallet,token_mint,lane,position_fraction,net_return,"
            "signal_to_entry_seconds,evidence_phase,exit_signature,exit_observed_at,context_json) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                EPOCH,
                signature,
                wallet,
                token,
                lane,
                fraction,
                net_return,
                signal_to_entry,
                "forward",
                exit_signature,
                (NOW + timedelta(minutes=5)).isoformat(),
                json.dumps({"regime": regime}),
            ),
        )


def test_accessibility_disqualifies_execution_race_not_pump_observation():
    accessible = classify_observation_accessibility(
        {
            "venue": "PUMP_FUN",
            "lifecycle_stage": PUMP_BONDING_CURVE,
            "copyable": True,
            "observation_lag_ms": 800.0,
            "processing_delay_ms": 120.0,
            "chase_fraction": 0.05,
        }
    )
    assert accessible["structurally_accessible"] is True
    assert accessible["millisecond_sniping_targeted"] is False
    assert accessible["first_slot_execution_authority"] is False
    assert accessible["source_pre_observation_return_authority"] is False
    assert accessible["pump_fun_usage"] == "discovery_and_residual_continuation_only_not_first_slot_sniping"

    late = classify_observation_accessibility(
        {
            "venue": "PUMP_FUN",
            "lifecycle_stage": PUMP_BONDING_CURVE,
            "copyable": True,
            "observation_lag_ms": 19_000.0,
            "processing_delay_ms": 2_000.0,
            "chase_fraction": 0.20,
        }
    )
    assert late["structurally_accessible"] is False
    assert "outside_strategy_entry_ceiling" in late["reasons"]
    assert "outside_max_chase" in late["reasons"]


def test_same_wallet_earns_separate_pump_scout_and_raydium_momentum_contexts(tmp_path):
    store = _store(tmp_path)
    for index in range(5):
        pump_sig = f"pump-{index}"
        ray_sig = f"ray-{index}"
        _observation(
            store,
            signature=pump_sig,
            wallet="wallet-a",
            token=f"pump-token-{index}",
            venue="PUMP_FUN",
            seconds=index + 1,
        )
        _outcome(
            store,
            signature=pump_sig,
            wallet="wallet-a",
            token=f"pump-token-{index}",
            lane="clean_scout_alpha",
            net_return=0.20 + index * 0.01,
            regime="high_speculation",
        )
        _observation(
            store,
            signature=ray_sig,
            wallet="wallet-a",
            token=f"ray-token-{index}",
            venue="RAYDIUM",
            seconds=20 + index,
        )
        _outcome(
            store,
            signature=ray_sig,
            wallet="wallet-a",
            token=f"ray-token-{index}",
            lane="entity_flow_momentum",
            net_return=0.10 + index * 0.01,
            regime="neutral",
        )

    router = WalletContextRouter(_Universe(store))
    profiles = router.context_profiles()
    pump = next(
        row
        for row in profiles
        if row["wallet"] == "wallet-a"
        and row["venue"] == "PUMP_FUN"
        and row["role"] == WalletRole.SCOUT_ALPHA.value
    )
    ray = next(
        row
        for row in profiles
        if row["wallet"] == "wallet-a"
        and row["venue"] == "RAYDIUM"
        and row["role"] == WalletRole.MOMENTUM_ALPHA.value
    )
    assert pump["lifecycle_stage"] == PUMP_BONDING_CURVE
    assert pump["regime"] == "high_speculation"
    assert pump["mature_forward_context"] is True
    assert ray["lifecycle_stage"] == RAYDIUM_UNPROVEN
    assert ray["regime"] == "neutral"
    assert ray["mature_forward_context"] is True

    routes = router.route_map(profiles)
    pump_route = next(row for row in routes if row["venue"] == "PUMP_FUN")
    ray_route = next(row for row in routes if row["venue"] == "RAYDIUM")
    assert WalletRole.SCOUT_ALPHA.value in pump_route["roles"]
    assert WalletRole.MOMENTUM_ALPHA.value not in pump_route["roles"]
    assert WalletRole.MOMENTUM_ALPHA.value in ray_route["roles"]
    assert WalletRole.SCOUT_ALPHA.value not in ray_route["roles"]
    assert pump_route["scout_and_momentum_confirmations_are_interchangeable"] is False


def test_confirmation_wallet_does_not_gain_scout_role(tmp_path):
    store = _store(tmp_path)
    for index in range(5):
        signature = f"momentum-{index}"
        token = f"token-{index}"
        at = _observation(
            store,
            signature=signature,
            wallet="trigger-wallet",
            token=token,
            venue="PUMP_FUN",
            seconds=index + 1,
        )
        _outcome(
            store,
            signature=signature,
            wallet="trigger-wallet",
            token=token,
            lane="entity_flow_momentum",
            net_return=0.15,
            regime="high_speculation",
        )
        with store._lock, store.db:
            store.db.execute(
                "INSERT INTO profit_first_final_trials("
                "epoch_id,source_signature,token_mint,trigger_wallet,lane,received_at) VALUES (?,?,?,?,?,?)",
                (EPOCH, signature, token, "trigger-wallet", "entity_flow_momentum", at.isoformat()),
            )
            store.db.execute(
                "INSERT INTO v4_entity_signal_context("
                "epoch_id,token_mint,trigger_wallet,received_at,independent_wallets_json) VALUES (?,?,?,?,?)",
                (EPOCH, token, "trigger-wallet", at.isoformat(), json.dumps(["confirm-wallet"])),
            )

    profiles = WalletContextRouter(_Universe(store)).context_profiles()
    confirmation = next(
        row
        for row in profiles
        if row["wallet"] == "confirm-wallet"
        and row["role"] == WalletRole.CONFIRMATION_ALPHA.value
    )
    assert confirmation["sample_count"] == 5
    assert confirmation["mature_forward_context"] is True
    assert all(
        not (
            row["wallet"] == "confirm-wallet"
            and row["role"] == WalletRole.SCOUT_ALPHA.value
        )
        for row in profiles
    )


def test_copyable_roi_ranking_uses_percentage_and_trimmed_robustness(tmp_path):
    store = _store(tmp_path)
    returns = [0.10, 0.11, 0.09, 0.12, 0.08, 5.00]
    for index, value in enumerate(returns):
        signature = f"roi-{index}"
        token = f"roi-token-{index}"
        _observation(
            store,
            signature=signature,
            wallet="roi-wallet",
            token=token,
            venue="RAYDIUM",
            seconds=index + 1,
        )
        _outcome(
            store,
            signature=signature,
            wallet="roi-wallet",
            token=token,
            lane=UNIFIED_LANE,
            net_return=value,
            signal_to_entry=5.0 if index < 3 else 10.0,
            regime="neutral",
        )

    status = WalletContextRouter(_Universe(store)).status()
    leader = next(row for row in status["copyable_roi_leaders"] if row["wallet"] == "roi-wallet")
    assert status["roi_ranking_basis"] == "percentage_copyable_executable_residual_return_not_dollar_pnl"
    assert leader["sample_count"] == 6
    assert leader["median_residual_roi"] == pytest.approx(0.105)
    assert leader["trimmed_mean_residual_roi_ex_best_1"] == pytest.approx(0.10)
    assert leader["copyable_return_on_deployed_fraction"] == pytest.approx(sum(returns) / len(returns))
    profile = next(
        row
        for row in status["context_profiles"]
        if row["wallet"] == "roi-wallet" and row["role"] == WalletRole.COPYABLE_ROC.value
    )
    assert profile["latency_residual_roi_curve"]["lte_5s"]["sample_count"] == 3
    assert profile["latency_residual_roi_curve"]["lte_10s"]["sample_count"] == 3


def test_router_is_shadow_only_and_tracking_recommendations_respect_capacity(tmp_path):
    store = _store(tmp_path)
    for wallet_index in range(3):
        wallet = f"wallet-{wallet_index}"
        for sample in range(5):
            signature = f"{wallet}-{sample}"
            token = f"{wallet}-token-{sample}"
            _observation(
                store,
                signature=signature,
                wallet=wallet,
                token=token,
                venue="PUMP_FUN",
                seconds=wallet_index * 10 + sample + 1,
            )
            _outcome(
                store,
                signature=signature,
                wallet=wallet,
                token=token,
                lane="clean_scout_alpha",
                net_return=0.10 + wallet_index * 0.02,
                regime="neutral",
            )

    status = WalletContextRouter(_Universe(store, capacity=2)).status()
    assert len(status["recommended_tracking_set"]) == 2
    assert all(row["recommendation_has_tracking_authority"] is False for row in status["recommended_tracking_set"])
    assert status["context_scores_have_trade_authority"] is False
    assert status["context_recommendations_have_tracking_mutation_authority"] is False
    assert status["active_strategy_mutation_allowed"] is False
    assert status["historical_promotion_authority"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
    assert status["accessibility"]["first_slot_or_subsecond_required_edge"] == "structurally_disqualified"
    assert status["accessibility"]["pump_fun_bonding_curve_removed_from_observation"] is False
