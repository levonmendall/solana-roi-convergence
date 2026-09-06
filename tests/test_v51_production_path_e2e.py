from __future__ import annotations

import asyncio
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

# The black-box regression must compose the same final production graph before
# importing the Robinhood plane. Running this test in isolation previously imported
# the compatibility plane first and therefore depended on unrelated test ordering.
from solana_roi import production as production_runtime
from solana_roi.observation_store import ObservationEventStore
from solana_roi.robinhood_chain_core import MAX_HOLD_SECONDS, V3Pool, WETH
from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane
from solana_roi import robinhood_pumpfun_shadow_boundary as shadow
from solana_roi.strategy_v51_authority import ECONOMIC_FREEZE_EPOCH
from solana_roi.v51_robinhood_consolidation import refresh_robinhood_candidate_learning


def _plane(tmp_path, monkeypatch) -> RobinhoodChainPaperPlane:
    assert production_runtime.app.state.roi_v51_final_economic_authority is True
    assert production_runtime.app.state.roi_v51_economic_composition_explicit is True
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


def _quote() -> dict[str, int | float]:
    return {
        "amount_in_wei": 10**16,
        "token_out": 10**18,
        "entry_gas_wei": 1,
        "exit_gas_wei": 1,
        "entry_total_cost_wei": 10**16 + 1,
        "immediate_exit_wei": 99 * 10**14,
        "round_trip_cost_fraction": 0.01,
        # Match the candidate's observed signal price so the forward context lands
        # in the same 0-5% chase band as the mature evidence seeded below.
        "entry_price_eth": 1.05,
    }


def _set_verified_frontier(plane: RobinhoodChainPaperPlane, *, caught_up: bool) -> None:
    # The final production entry guard requires a fresh head read and a durable
    # decision cursor. Mock only that external head seam; all downstream strategy,
    # store, trial, settlement and learning code remains real.
    plane._caught_up = caught_up
    plane._cursor = 100
    plane._latest_block = 100
    plane.rpc.block_number = AsyncMock(return_value=100)


def _seed_promoted_shadow_context(
    plane: RobinhoodChainPaperPlane,
    pool: V3Pool,
    *,
    actor: str,
    entity: str,
) -> None:
    """Seed the 30 durable zero-allocation outcomes required for paper promotion.

    The production boundary intentionally forbids bootstrap paper allocation. This
    fixture therefore supplies mature positive *forward shadow* evidence rather than
    bypassing or monkeypatching the promotion gate. The real chooser consumes these
    persisted rows during the E2E decision.
    """
    risk = {"risk_signature": "clean", "risk_severity": 0.0}
    quote = _quote()
    lane = "fomo_continuation"
    for index in range(shadow.MIN_FORWARD_OUTCOMES_FOR_PAPER_ENTRY):
        source = f"production-e2e-shadow-{index}"
        shadow._insert_shadow_trials(
            plane,
            source_key=source,
            token=pool.token,
            market=pool.pool,
            venue=pool.venue,
            lifecycle="new_weth_pool",
            actor=actor,
            entity=entity,
            role="independent_entity",
            regime="high_speculation",
            flow_state="active_fomo",
            risk=risk,
            lanes=[lane],
            quote=quote,
            probe_fraction=0.01,
            signal_price_eth=1.05,
            chase_fraction=0.0,
            latency_seconds=0.0,
        )
        with plane.store._lock, plane.store.db:
            trial = plane.store.db.execute(
                "SELECT * FROM robinhood_v5_shadow_trials WHERE release_commit=? AND source_key=? AND lane=?",
                (plane.release_commit, source, lane),
            ).fetchone()
            assert trial is not None
            settled_at = f"2026-09-05T00:00:{index:02d}+00:00"
            plane.store.db.execute(
                "INSERT INTO robinhood_v5_shadow_outcomes("
                "shadow_trial_id,release_commit,strategy_version,token,market,venue,lifecycle,trigger_entity,lane,regime,risk_signature,"
                "context_key,probe_fraction,net_return,exit_quote_out_wei,exit_gas_wei,exit_reason,settled_at,paper_allocation_fraction,"
                "paper_only,live_money_authority,paper_promotion_authority) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0.0,1,0,0)",
                (
                    int(trial["id"]),
                    plane.release_commit,
                    shadow.SHADOW_BOUNDARY_VERSION,
                    str(trial["token"]),
                    str(trial["market"]),
                    str(trial["venue"]),
                    str(trial["lifecycle"]),
                    str(trial["trigger_entity"]),
                    str(trial["lane"]),
                    str(trial["regime"]),
                    str(trial["risk_signature"]),
                    str(trial["context_key"]),
                    float(trial["probe_fraction"]),
                    0.10,
                    "1100",
                    "0",
                    "seeded_forward_shadow_profit",
                    settled_at,
                ),
            )
            plane.store.db.execute(
                "UPDATE robinhood_v5_shadow_trials SET settled_at=?,exit_reason=? WHERE id=?",
                (settled_at, "seeded_forward_shadow_profit", int(trial["id"])),
            )


def test_real_production_robinhood_path_reaches_entry_settlement_and_learning(tmp_path, monkeypatch) -> None:
    plane = _plane(tmp_path, monkeypatch)
    pool = _pool()
    plane.v3_pools[pool.pool] = pool
    _set_verified_frontier(plane, caught_up=True)
    actor = "0x" + "7" * 40
    entity = "0x" + "9" * 40
    _seed_promoted_shadow_context(plane, pool, actor=actor, entity=entity)

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
    plane._quote_v3_round_trip = AsyncMock(return_value=_quote())

    asyncio.run(plane._maybe_open_v3(pool, current_block=100))

    with plane.store._lock, plane.store.db:
        trial_row = plane.store.db.execute(
            "SELECT * FROM robinhood_paper_trials ORDER BY id DESC LIMIT 1"
        ).fetchone()
        ledger = plane.store.db.execute(
            "SELECT * FROM v51_robinhood_candidate_ledger ORDER BY observed_at DESC LIMIT 1"
        ).fetchone()
        assert trial_row is not None, dict(ledger) if ledger is not None else "no candidate ledger row"
        trial_id = int(trial_row["id"])
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
    _set_verified_frontier(plane, caught_up=False)

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
