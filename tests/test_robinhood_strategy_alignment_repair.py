from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from solana_roi.observation_store import ObservationEventStore
from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane
from solana_roi.robinhood_chain_profit_maximizer import ROBINHOOD_V5_VERSION
from solana_roi.robinhood_strategy_alignment_repair import (
    REPAIR_VERSION,
    _ensure_discovery_schema,
    _research_rankings,
)


def _plane(store: ObservationEventStore, release: str) -> RobinhoodChainPaperPlane:
    return RobinhoodChainPaperPlane(store, release_commit=release)


def _insert_trial_and_outcome(
    plane: RobinhoodChainPaperPlane,
    *,
    entity: str,
    release: str,
    net_return: float,
    token: str,
    market: str,
) -> int:
    venue = "UNISWAP_V3_DIRECT"
    lifecycle = "new_weth_pool"
    lane = "elite_entity_continuation"
    role = "independent_entity"
    regime = "high_speculation"
    flow_state = "entity_accumulation"
    risk_signature = "clean"
    now = datetime.now(timezone.utc).isoformat()
    context_key = plane._v5_context_key(
        entity=entity,
        role=role,
        lane=lane,
        venue=venue,
        lifecycle=lifecycle,
        regime=regime,
        risk_signature=risk_signature,
        flow_state=flow_state,
    )
    with plane.store._lock, plane.store.db:
        cursor = plane.store.db.execute(
            "INSERT INTO robinhood_paper_trials("
            "release_commit,strategy_version,token,market,venue,lifecycle,trigger_actor,trigger_entity,fomo_state,context_state,"
            "position_fraction,entry_quote_in_wei,entry_token_raw,entry_gas_wei,entry_total_cost_wei,entry_price_eth,"
            "entry_round_trip_cost_fraction,opened_at,decision_reason,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                release,
                ROBINHOOD_V5_VERSION,
                token,
                market,
                venue,
                lifecycle,
                entity,
                entity,
                flow_state,
                f"v5:{lane}",
                0.01,
                "100",
                "100",
                "0",
                "100",
                1.0,
                0.0,
                now,
                "test",
            ),
        )
        trial_id = int(cursor.lastrowid)
        plane.store.db.execute(
            "INSERT INTO robinhood_v5_trial_context("
            "trial_id,release_commit,strategy_version,lane,trigger_role,regime,flow_state,risk_signature,risk_severity,risk_json,"
            "context_key,latency_band,lifecycle_progress,threshold_challenger,candidate_lanes_json,created_at,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                trial_id,
                release,
                ROBINHOOD_V5_VERSION,
                lane,
                role,
                regime,
                flow_state,
                risk_signature,
                0.0,
                "{}",
                context_key,
                "chain_poll",
                None,
                0,
                "[]",
                now,
            ),
        )
        plane.store.db.execute(
            "INSERT INTO robinhood_paper_outcomes("
            "release_commit,trial_id,token,market,venue,lifecycle,trigger_actor,trigger_entity,fomo_state,position_fraction,"
            "net_return,paper_nav_multiplier,exit_quote_out_wei,exit_gas_wei,exit_reason,settled_at,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                release,
                trial_id,
                token,
                market,
                venue,
                lifecycle,
                entity,
                entity,
                flow_state,
                0.01,
                net_return,
                1.0 + 0.01 * net_return,
                "100",
                "0",
                "test",
                now,
            ),
        )
    return trial_id


def test_release_sha_no_longer_resets_nav_or_compatible_context_learning(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "robinhood.sqlite3")
    entity = "0x" + "a" * 40
    plane1 = _plane(store, "release-one")
    _insert_trial_and_outcome(
        plane1,
        entity=entity,
        release="release-one",
        net_return=0.20,
        token="0x" + "1" * 40,
        market="0x" + "2" * 40,
    )
    plane2 = _plane(store, "release-two")
    _insert_trial_and_outcome(
        plane2,
        entity=entity,
        release="release-two",
        net_return=0.10,
        token="0x" + "3" * 40,
        market="0x" + "4" * 40,
    )

    expected_nav = 500.0 * (1.0 + 0.01 * 0.20) * (1.0 + 0.01 * 0.10)
    assert plane2._paper_nav_usd() == pytest.approx(expected_nav)
    values, source = plane2._v5_context_returns(
        entity=entity,
        role="independent_entity",
        lane="elite_entity_continuation",
        venue="UNISWAP_V3_DIRECT",
        lifecycle="new_weth_pool",
        regime="high_speculation",
        risk_signature="clean",
        flow_state="entity_accumulation",
    )
    assert values == pytest.approx([0.20, 0.10])
    assert source.endswith("cross_release")
    asyncio.run(plane1.close())
    asyncio.run(plane2.close())


def test_unsettled_paper_position_and_market_metadata_survive_deploy(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "robinhood.sqlite3")
    token = "0x" + "5" * 40
    pool = "0x" + "6" * 40
    entity = "0x" + "7" * 40
    plane1 = _plane(store, "release-one")
    plane1._persist_launch(
        protocol="uniswap_v3",
        venue="UNISWAP_V3_DIRECT",
        lifecycle="new_weth_pool",
        token=token,
        pool=pool,
        fee=3000,
        launch_block=123,
        paper_eligible=True,
    )
    now = datetime.now(timezone.utc).isoformat()
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO robinhood_paper_trials("
            "release_commit,strategy_version,token,market,venue,lifecycle,trigger_actor,trigger_entity,fomo_state,context_state,"
            "position_fraction,entry_quote_in_wei,entry_token_raw,entry_gas_wei,entry_total_cost_wei,entry_price_eth,"
            "entry_round_trip_cost_fraction,opened_at,decision_reason,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                "release-one",
                ROBINHOOD_V5_VERSION,
                token,
                pool,
                "UNISWAP_V3_DIRECT",
                "new_weth_pool",
                entity,
                entity,
                "pre_fomo",
                "v5:elite_entity_continuation",
                0.04,
                "100",
                "100",
                "0",
                "100",
                1.0,
                0.0,
                now,
                "test",
            ),
        )

    plane2 = _plane(store, "release-two")
    assert pool in plane2.v3_pools
    assert plane2._token_open(token) is True
    assert plane2._open_exposure() == pytest.approx(0.04)
    assert plane2.status()["durable_strategy_memory"]["open_paper_trials_all_releases"] == 1
    asyncio.run(plane1.close())
    asyncio.run(plane2.close())


def test_entity_discovery_ranks_forward_marks_but_has_no_promotion_authority(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "robinhood.sqlite3")
    plane = _plane(store, "release-one")
    _ensure_discovery_schema(plane)
    entity = "0x" + "8" * 40
    actor = "0x" + "9" * 40
    now = datetime.now(timezone.utc).isoformat()
    with store._lock, store.db:
        for index, value in enumerate((0.05, 0.08, 0.04, 0.06, 0.07), start=1):
            store.db.execute(
                "INSERT INTO robinhood_entity_discovery_observations("
                "swap_id,strategy_version,release_commit,entity,actor,venue,lifecycle,token,market,side,quote_amount_wei,"
                "price_eth,block_number,observed_at,mark_price_eth,mark_return,marked_at,research_only,paper_promotion_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    index,
                    ROBINHOOD_V5_VERSION,
                    "release-one",
                    entity,
                    actor,
                    "UNISWAP_V3_DIRECT",
                    "new_weth_pool",
                    "0x" + str((index % 3) + 1) * 40,
                    "0x" + "f" * 40,
                    "buy",
                    "100",
                    1.0,
                    100 + index,
                    now,
                    1.0 + value,
                    value,
                    now,
                ),
            )

    ranking = _research_rankings(plane)[0]
    assert ranking["entity"] == entity
    assert ranking["distinct_tokens"] == 3
    assert ranking["marked_buy_observations"] == 5
    assert ranking["priority_research_challenger"] is True
    assert ranking["research_only"] is True
    assert ranking["historical_or_mark_evidence_has_paper_promotion_authority"] is False
    assert ranking["ranking_can_bypass_forward_paper_maturity"] is False
    status = plane.status()
    assert status["durable_strategy_memory"]["repair_version"] == REPAIR_VERSION
    assert status["entity_discovery"]["provider_requests_added"] == 0
    assert status["entity_discovery"]["paper_authority_still_requires_forward_settled_paper_outcomes"] is True
    asyncio.run(plane.close())
