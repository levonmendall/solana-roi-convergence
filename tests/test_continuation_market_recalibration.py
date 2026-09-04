from __future__ import annotations

import asyncio
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import continuation_market_recalibration as recal


class Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.execute(
            "CREATE TABLE normalized_swaps ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, signature TEXT, wallet TEXT, token_mint TEXT, side TEXT, "
            "token_amount REAL, native_amount_sol REAL, reference_price_sol REAL, observed_at TEXT, received_at TEXT, source TEXT)"
        )


def _swap(store: Store, *, token: str, wallet: str, side: str, seconds_ago: float, native: float = 1.0) -> None:
    now = datetime.now(timezone.utc)
    at = (now - timedelta(seconds=seconds_ago)).isoformat()
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO normalized_swaps(signature,wallet,token_mint,side,token_amount,native_amount_sol,reference_price_sol,observed_at,received_at,source) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (f"{wallet}-{side}-{seconds_ago}", wallet, token, side, 1.0, native, 1.0, at, at, "PUMP_AMM"),
        )


def test_chase_bands_cover_extended_microcap_moves() -> None:
    assert recal.continuation_chase_band(0.10) == "0_15pct"
    assert recal.continuation_chase_band(0.20) == "15_25pct"
    assert recal.continuation_chase_band(0.35) == "25_40pct"
    assert recal.continuation_chase_band(0.60) == "40_75pct"
    assert recal.continuation_chase_band(1.00) == "75_125pct"
    assert recal.continuation_chase_band(1.50) == "gt_125pct"


def test_latency_is_context_not_twenty_second_veto() -> None:
    assert recal.continuation_latency_band(25.0) == "20_30s"
    assert recal.continuation_latency_band(45.0) == "30_60s"
    assert recal.continuation_latency_band(90.0) == "1_2m"
    assert recal.continuation_latency_band(240.0) == "2_5m"
    assert recal.continuation_latency_band(600.0) == "gt_5m"
    row = {
        "side": "buy",
        "signature": "sig",
        "token_mint": "mint",
        "wallet": "wallet",
        "wallet_price_sol": 1.0,
        "observation_lag_ms": 180_000.0,
    }
    assert recal.continuation_strategy_evaluation_eligible(row) is True


def test_extended_chase_requires_persistent_current_flow() -> None:
    store = Store()
    for index, age in enumerate((45, 25, 10, 4)):
        _swap(store, token="mint", wallet=f"buyer-{index}", side="buy", seconds_ago=age, native=1.0)
    _swap(store, token="mint", wallet="seller", side="sell", seconds_ago=20, native=0.5)
    state = recal._residual_state(store, "mint", chase=1.50, latency=75.0, round_trip_cost=0.20)
    assert state["actionable"] is True
    assert state["state"] == "extended_continuation"


def test_flow_reversal_remains_a_state_based_veto() -> None:
    store = Store()
    _swap(store, token="mint", wallet="buyer", side="buy", seconds_ago=20, native=0.5)
    _swap(store, token="mint", wallet="seller-1", side="sell", seconds_ago=8, native=1.0)
    _swap(store, token="mint", wallet="seller-2", side="sell", seconds_ago=4, native=1.0)
    state = recal._residual_state(store, "mint", chase=0.60, latency=45.0, round_trip_cost=0.10)
    assert state["actionable"] is False
    assert state["state"] == "flow_reversed"


def test_high_chase_latency_and_cost_fail_small_instead_of_zero() -> None:
    assert recal._continuation_fraction_cap(0.60, 45.0, 0.20) == 0.005
    assert recal._continuation_fraction_cap(1.50, 600.0, 0.40) == 0.0025


def test_robinhood_neutral_independent_flow_becomes_bootstrap_continuation(monkeypatch) -> None:
    async def original(_self, _swaps, *, deployer=""):
        return {
            "state": "neutral",
            "entity_resolution_complete": True,
            "trigger_actor": "0x" + "1" * 40,
            "trigger_entity": "0x" + "2" * 40,
            "buy_count_60s": 1,
            "sell_count_60s": 0,
            "independent_entities_60s": 1,
            "buy_sell_quote_ratio": 1.25,
            "buy_count_acceleration": 1.0,
            "price_change_60s": 0.80,
        }

    monkeypatch.setattr(recal, "_ORIGINAL_RH_FLOW", original)
    result = asyncio.run(recal._rh_flow_without_sniper_cap(SimpleNamespace(), [], deployer=""))
    assert result["state"] == "bootstrap_continuation"


def test_robinhood_fomo_state_has_no_forty_percent_price_change_ceiling(monkeypatch) -> None:
    async def original(_self, _swaps, *, deployer=""):
        return {
            "state": "entity_accumulation",
            "entity_resolution_complete": True,
            "trigger_actor": "0x" + "1" * 40,
            "trigger_entity": "0x" + "2" * 40,
            "buy_count_60s": 5,
            "sell_count_60s": 1,
            "independent_entities_60s": 4,
            "buy_sell_quote_ratio": 2.0,
            "buy_count_acceleration": 1.5,
            "price_change_60s": 1.20,
        }

    monkeypatch.setattr(recal, "_ORIGINAL_RH_FLOW", original)
    result = asyncio.run(recal._rh_flow_without_sniper_cap(SimpleNamespace(), [], deployer=""))
    assert result["state"] == "active_fomo"


def test_recalibration_has_no_live_money_or_submission_authority() -> None:
    assert recal.PAPER_ONLY is True
    assert recal.LIVE_MONEY_AUTHORITY is False
    assert recal.SIGNING_AVAILABLE is False
    assert recal.TRANSACTION_SUBMISSION_AVAILABLE is False
