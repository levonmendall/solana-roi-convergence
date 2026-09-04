from __future__ import annotations

import asyncio
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from solana_roi.observation_store import ObservationEventStore
from solana_roi.robinhood_chain_paper import (
    BOOTSTRAP_PAPER_FRACTION,
    LIVE_MONEY_AUTHORITY,
    MAX_HOLD_SECONDS,
    PAPER_TRADING_AUTHORITY,
    ROBINHOOD_CHAIN_ID,
    RobinhoodChainPaperPlane,
    V3Pool,
    classify_context_returns,
)


def _plane(tmp_path, monkeypatch) -> RobinhoodChainPaperPlane:
    monkeypatch.setenv("ROBINHOOD_ENTITY_RESOLUTION_REQUIRED", "true")
    monkeypatch.setenv("ROBINHOOD_RWA_FILTER_REQUIRED", "true")
    store = ObservationEventStore(tmp_path / "robinhood.sqlite3")
    return RobinhoodChainPaperPlane(store, release_commit="test-release")


def test_robinhood_is_active_paper_not_shadow_and_never_live_money() -> None:
    assert ROBINHOOD_CHAIN_ID == 4663
    assert PAPER_TRADING_AUTHORITY is True
    assert LIVE_MONEY_AUTHORITY is False


def test_paper_bootstrap_promotes_and_demotes_from_forward_returns() -> None:
    bootstrap = classify_context_returns([0.10] * 29)
    promoted = classify_context_returns([0.10] * 30)
    demoted = classify_context_returns([-0.10] * 30)

    assert bootstrap["state"] == "bootstrap_paper_evidence"
    assert bootstrap["best_paper_position_fraction"] > 0
    assert promoted["state"] == "promoted_paper_context"
    assert promoted["best_paper_position_fraction"] > 0
    assert demoted["state"] == "demoted_paper_context"
    assert demoted["best_paper_position_fraction"] == 0
    assert promoted["historical_promotion_authority"] is False


def test_related_wallets_collapse_before_fomo_confirmation(tmp_path, monkeypatch) -> None:
    plane = _plane(tmp_path, monkeypatch)
    now = time.time()
    a = "0x" + "a" * 40
    b = "0x" + "b" * 40
    c = "0x" + "c" * 40
    common = "0x" + "d" * 40
    swaps = deque(
        [
            {"side": "buy", "actor": a, "quote_amount_wei": 100, "price_eth": 1.00, "observed_ts": now},
            {"side": "buy", "actor": b, "quote_amount_wei": 100, "price_eth": 1.02, "observed_ts": now},
            {"side": "buy", "actor": c, "quote_amount_wei": 100, "price_eth": 1.03, "observed_ts": now},
        ]
    )
    raw = plane._recent_metrics(swaps, now_ts=now)
    collapsed = plane._recent_metrics(
        swaps,
        now_ts=now,
        entity_map={a: common, b: common, c: common},
    )
    assert raw["independent_entities_60s"] == 3
    assert collapsed["independent_entities_60s"] == 1
    assert collapsed["state"] == "neutral"


def test_entity_resolution_failure_blocks_paper_fomo(tmp_path, monkeypatch) -> None:
    plane = _plane(tmp_path, monkeypatch)
    actor_a = "0x" + "1" * 40
    actor_b = "0x" + "2" * 40
    actor_c = "0x" + "3" * 40
    now = time.time()
    swaps = deque(
        [
            {"side": "buy", "actor": actor_a, "quote_amount_wei": 100, "price_eth": 1.00, "observed_ts": now},
            {"side": "buy", "actor": actor_b, "quote_amount_wei": 100, "price_eth": 1.03, "observed_ts": now},
            {"side": "buy", "actor": actor_c, "quote_amount_wei": 100, "price_eth": 1.06, "observed_ts": now},
            {"side": "buy", "actor": actor_c, "quote_amount_wei": 100, "price_eth": 1.08, "observed_ts": now},
        ]
    )
    plane._entity_anchor = AsyncMock(return_value=None)
    metrics = asyncio.run(plane._resolved_metrics(swaps))
    assert metrics["state"] == "entity_resolution_incomplete"
    assert metrics["entity_resolution_complete"] is False
    assert metrics["independent_entities_60s"] == 0
    asyncio.run(plane.close())


def test_official_stock_token_registry_excludes_rwas_from_direct_v3(tmp_path, monkeypatch) -> None:
    plane = _plane(tmp_path, monkeypatch)
    stock = "0x" + "9" * 40
    meme = "0x" + "8" * 40

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self):
            return {
                "assets": [
                    {
                        "deployments": [
                            {"chainId": 4663, "contractAddress": stock},
                            {"chainId": 1, "contractAddress": "0x" + "7" * 40},
                        ]
                    }
                ]
            }

    plane.rpc.client.get = AsyncMock(return_value=Response())
    assert asyncio.run(plane._direct_v3_token_allowed(stock)) is False
    assert asyncio.run(plane._direct_v3_token_allowed(meme)) is True
    assert plane._rwa_registry_available is True
    asyncio.run(plane.close())


def test_context_returns_never_pool_venues(tmp_path, monkeypatch) -> None:
    plane = _plane(tmp_path, monkeypatch)
    entity = "0x" + "4" * 40
    with plane.store._lock, plane.store.db:
        for index in range(30):
            plane.store.db.execute(
                "INSERT INTO robinhood_paper_trials("
                "release_commit,strategy_version,token,market,venue,lifecycle,trigger_actor,trigger_entity,fomo_state,context_state,"
                "position_fraction,entry_quote_in_wei,entry_token_raw,entry_gas_wei,entry_total_cost_wei,entry_price_eth,"
                "entry_round_trip_cost_fraction,opened_at,decision_reason,paper_only,live_money_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    "test-release", "v", f"token-ray-{index}", f"market-ray-{index}", "UNISWAP_V3_DIRECT", "new_weth_pool",
                    entity, entity, "active_fomo", "bootstrap", 0.01, "1", "1", "0", "1", 1.0, 0.0,
                    datetime.now(timezone.utc).isoformat(), "test",
                ),
            )
            trial_id = plane.store.db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            plane.store.db.execute(
                "INSERT INTO robinhood_paper_outcomes("
                "release_commit,trial_id,token,market,venue,lifecycle,trigger_actor,trigger_entity,fomo_state,position_fraction,"
                "net_return,paper_nav_multiplier,exit_quote_out_wei,exit_gas_wei,exit_reason,settled_at,paper_only,live_money_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    "test-release", trial_id, f"token-ray-{index}", f"market-ray-{index}", "UNISWAP_V3_DIRECT", "new_weth_pool",
                    entity, entity, "active_fomo", 0.01, 0.10, 1.001, "1", "0", "test", datetime.now(timezone.utc).isoformat(),
                ),
            )
        plane.store.db.execute(
            "INSERT INTO robinhood_paper_trials("
            "release_commit,strategy_version,token,market,venue,lifecycle,trigger_actor,trigger_entity,fomo_state,context_state,"
            "position_fraction,entry_quote_in_wei,entry_token_raw,entry_gas_wei,entry_total_cost_wei,entry_price_eth,"
            "entry_round_trip_cost_fraction,opened_at,decision_reason,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            ("test-release", "v", "token-p", "market-p", "PONS_V2_CURVE", "bonding_curve", entity, entity,
             "active_fomo", "bootstrap", 0.01, "1", "1", "0", "1", 1.0, 0.0, datetime.now(timezone.utc).isoformat(), "test"),
        )
        trial_id = plane.store.db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        plane.store.db.execute(
            "INSERT INTO robinhood_paper_outcomes("
            "release_commit,trial_id,token,market,venue,lifecycle,trigger_actor,trigger_entity,fomo_state,position_fraction,"
            "net_return,paper_nav_multiplier,exit_quote_out_wei,exit_gas_wei,exit_reason,settled_at,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            ("test-release", trial_id, "token-p", "market-p", "PONS_V2_CURVE", "bonding_curve", entity, entity,
             "active_fomo", 0.01, -0.50, 0.995, "0", "0", "test", datetime.now(timezone.utc).isoformat()),
        )
    direct = plane._context_returns(entity, "UNISWAP_V3_DIRECT", "new_weth_pool")
    pons = plane._context_returns(entity, "PONS_V2_CURVE", "bonding_curve")
    assert len(direct) == 30 and all(value == pytest.approx(0.10) for value in direct)
    assert pons == [-0.50]
    fraction, profile = plane._position_fraction(entity, "UNISWAP_V3_DIRECT", "new_weth_pool")
    assert profile["state"] == "promoted_paper_context"
    assert fraction > BOOTSTRAP_PAPER_FRACTION


def test_context_promotion_uses_economic_entity_not_raw_actor(tmp_path, monkeypatch) -> None:
    plane = _plane(tmp_path, monkeypatch)
    entity = "0x" + "e" * 40
    actor_a = "0x" + "a" * 40
    actor_b = "0x" + "b" * 40
    with plane.store._lock, plane.store.db:
        for index in range(30):
            actor = actor_a if index % 2 == 0 else actor_b
            plane.store.db.execute(
                "INSERT INTO robinhood_paper_trials("
                "release_commit,strategy_version,token,market,venue,lifecycle,trigger_actor,trigger_entity,fomo_state,context_state,"
                "position_fraction,entry_quote_in_wei,entry_token_raw,entry_gas_wei,entry_total_cost_wei,entry_price_eth,"
                "entry_round_trip_cost_fraction,opened_at,decision_reason,paper_only,live_money_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    "test-release", "v", f"token-{index}", f"market-{index}", "UNISWAP_V3_DIRECT", "new_weth_pool",
                    actor, entity, "active_fomo", "bootstrap", 0.01, "1", "1", "0", "1", 1.0, 0.0,
                    datetime.now(timezone.utc).isoformat(), "test",
                ),
            )
            trial_id = plane.store.db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            plane.store.db.execute(
                "INSERT INTO robinhood_paper_outcomes("
                "release_commit,trial_id,token,market,venue,lifecycle,trigger_actor,trigger_entity,fomo_state,position_fraction,"
                "net_return,paper_nav_multiplier,exit_quote_out_wei,exit_gas_wei,exit_reason,settled_at,paper_only,live_money_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    "test-release", trial_id, f"token-{index}", f"market-{index}", "UNISWAP_V3_DIRECT", "new_weth_pool",
                    actor, entity, "active_fomo", 0.01, 0.10, 1.001, "1", "0", "test",
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
    assert plane._context_returns(entity, "UNISWAP_V3_DIRECT", "new_weth_pool") == pytest.approx([0.10] * 30)
    assert plane._context_returns(actor_a, "UNISWAP_V3_DIRECT", "new_weth_pool") == []
    fraction, profile = plane._position_fraction(entity, "UNISWAP_V3_DIRECT", "new_weth_pool")
    assert profile["state"] == "promoted_paper_context"
    assert fraction > BOOTSTRAP_PAPER_FRACTION


def test_unexitable_position_becomes_full_paper_loss_at_max_hold(tmp_path, monkeypatch) -> None:
    plane = _plane(tmp_path, monkeypatch)
    token = "0x" + "5" * 40
    pool_address = "0x" + "6" * 40
    actor = "0x" + "7" * 40
    entity = "0x" + "8" * 40
    pool = V3Pool(token=token, pool=pool_address, token0="0x0bd7d308f8e1639fab988df18a8011f41eacad73", token1=token, fee=3000)
    plane.v3_pools[pool_address] = pool
    opened = (datetime.now(timezone.utc) - timedelta(seconds=MAX_HOLD_SECONDS + 1)).isoformat()
    with plane.store._lock, plane.store.db:
        plane.store.db.execute(
            "INSERT INTO robinhood_paper_trials("
            "release_commit,strategy_version,token,market,venue,lifecycle,trigger_actor,trigger_entity,fomo_state,context_state,"
            "position_fraction,entry_quote_in_wei,entry_token_raw,entry_gas_wei,entry_total_cost_wei,entry_price_eth,"
            "entry_round_trip_cost_fraction,opened_at,decision_reason,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            ("test-release", "v", token, pool_address, "UNISWAP_V3_DIRECT", "new_weth_pool", actor, entity,
             "active_fomo", "bootstrap", 0.01, "100", "1000", "1", "101", 0.1, 0.05, opened, "test"),
        )
        trial = dict(plane.store.db.execute("SELECT * FROM robinhood_paper_trials LIMIT 1").fetchone())
    plane.rpc.gas_price = AsyncMock(return_value=1)
    plane.rpc.v3_quote_exact_input = AsyncMock(side_effect=RuntimeError("no route"))
    plane._resolved_metrics = AsyncMock(return_value={"state": "neutral"})
    asyncio.run(plane._settle_one(trial))
    row = plane.store.db.execute("SELECT * FROM robinhood_paper_outcomes LIMIT 1").fetchone()
    assert row is not None
    assert float(row["net_return"]) == -1.0
    assert str(row["exit_reason"]) in {"stop_loss", "max_hold"}
    asyncio.run(plane.close())
