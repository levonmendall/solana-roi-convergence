from __future__ import annotations

import asyncio
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

from solana_roi.observation_store import ObservationEventStore
from solana_roi.robinhood_chain_core import MAX_HOLD_SECONDS, V3Pool, WETH
from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane
from solana_roi.strategy_v51_authority import ECONOMIC_FREEZE_EPOCH
from solana_roi.v51_robinhood_consolidation import refresh_robinhood_candidate_learning


def _plane(tmp_path, monkeypatch) -> RobinhoodChainPaperPlane:
    # Importing production exercises the exact explicit composition used by Render.
    from solana_roi import production

    assert production.app.state.roi_v51_final_economic_authority is True
    assert production.app.state.roi_v51_economic_composition_explicit is True
    monkeypatch.setenv("ROBINHOOD_ENTITY_RESOLUTION_REQUIRED", "true")
    monkeypatch.setenv("ROBINHOOD_RWA_FILTER_REQUIRED", "true")
    return RobinhoodChainPaperPlane(
        ObservationEventStore(tmp_path / "robinhood-e2e.sqlite3"),
        release_commit="production-path-e2e",
    )


def _pool() -> V3Pool:
    token = "0x" + "8" * 40
    actor = "0x" + "7" * 40
    now = time.time()
    return V3Pool(
        token=token,
        pool="0x" + "6" * 40,
        token0=WETH,
        token1=token,
        fee=3000,
        first_price_eth=1.0,
        recent_swaps=deque(
            [
                {
                    "side": "buy",
                    "actor": actor,
                    "quote_amount_wei": 10**18,
                    "price_eth": 1.05,
                    "observed_ts": now,
                    "tx_hash": "0x" + "a" * 64,
                    "log_index": 1,
                }
            ],
            maxlen=500,
        ),
    )


def test_real_production_robinhood_path_reaches_entry_settlement_and_learning(tmp_path, monkeypatch) -> None:
    plane = _plane(tmp_path, monkeypatch)
    pool = _pool()
    plane.v3_pools[pool.pool] = pool
    plane._caught_up = True
    actor = "0x" + "7" * 40
    entity = "0x" + "9" * 40

    plane._direct_v3_token_allowed = AsyncMock(return_value=True)
    plane._v5_flow_metrics = AsyncMock(
        return_value={
            "state": "active_fomo",
            "entity_resolution_complete": True,
            "trigger_actor": actor,
            "trigger_entity": entity,
            "trigger_is_creator": False,
            "independent_entities_60s": 3,
            "buy_count_60s": 4,
            "sell_count_60s": 0,
            "buy_sell_quote_ratio": 4.0,
            "buy_count_acceleration": 2.0,
            "creator_sell_pressure": 0.0,
        }
    )
    plane._quote_v3_round_trip = AsyncMock(
        return_value={
            "amount_in_wei": 10**16,
            "token_out": 10**18,
            "entry_gas_wei": 1,
            "exit_gas_wei": 1,
            "entry_total_cost_wei": 10**16 + 1,
            "immediate_exit_wei": 99 * 10**14,
            "round_trip_cost_fraction": 0.01,
            "entry_price_eth": 0.01,
        }
    )

    asyncio.run(plane._maybe_open_v3(pool, current_block=100))

    with plane.store._lock, plane.store.db:
        trial_row = plane.store.db.execute(
            "SELECT * FROM robinhood_paper_trials ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert trial_row is not None
        trial_id = int(trial_row["id"])
        ledger = plane.store.db.execute(
            "SELECT * FROM v51_robinhood_candidate_ledger ORDER BY observed_at DESC LIMIT 1"
        ).fetchone()
        assert ledger is not None
        assert ledger["decision"] == "paper_enter"
        assert int(ledger["trial_id"]) == trial_id
        assert ledger["economic_freeze_epoch"] == ECONOMIC_FREEZE_EPOCH

        opened = (datetime.now(timezone.utc) - timedelta(seconds=MAX_HOLD_SECONDS + 1)).isoformat()
        plane.store.db.execute(
            "UPDATE robinhood_paper_trials SET opened_at=? WHERE id=?",
            (opened, trial_id),
        )
        trial = dict(
            plane.store.db.execute("SELECT * FROM robinhood_paper_trials WHERE id=?", (trial_id,)).fetchone()
        )

    plane.rpc.gas_price = AsyncMock(return_value=1)
    plane.rpc.v3_quote_exact_input = AsyncMock(side_effect=RuntimeError("forced max-hold no-route"))
    asyncio.run(plane._settle_one(trial))
    refresh_robinhood_candidate_learning(plane.store)

    with plane.store._lock:
        outcome = plane.store.db.execute(
            "SELECT * FROM robinhood_paper_outcomes WHERE trial_id=? LIMIT 1", (trial_id,)
        ).fetchone()
        assert outcome is not None
        stages = {
            str(row["stage"]): str(row["status"])
            for row in plane.store.db.execute(
                "SELECT stage,status FROM v51_candidate_pipeline_audit "
                "WHERE surface='ROBINHOOD_CHAIN' ORDER BY stage_index"
            ).fetchall()
        }
    assert set(stages) >= {
        "ingestion",
        "candidate",
        "context",
        "execution_evidence",
        "decision",
        "position",
        "settlement",
        "learning",
    }
    assert stages["settlement"] == "complete"
    assert stages["learning"] == "complete"
    asyncio.run(plane.close())


def test_real_production_robinhood_preselection_return_is_never_silent(tmp_path, monkeypatch) -> None:
    plane = _plane(tmp_path, monkeypatch)
    pool = _pool()
    plane._caught_up = False

    asyncio.run(plane._maybe_open_v3(pool, current_block=100))

    with plane.store._lock:
        row = plane.store.db.execute(
            "SELECT decision,decision_reason,trial_id FROM v51_robinhood_candidate_ledger LIMIT 1"
        ).fetchone()
        assert row is not None
        assert row["decision"] == "paper_reject"
        assert row["decision_reason"] == "runtime_not_ready_for_paper_decision"
        assert row["trial_id"] is None
        debt = plane.store.db.execute(
            "SELECT COUNT(*) AS n FROM v51_robinhood_candidate_ledger "
            "WHERE decision NOT IN ('paper_enter','paper_reject')"
        ).fetchone()
        assert int(debt["n"]) == 0
    asyncio.run(plane.close())
