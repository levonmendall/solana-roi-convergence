from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone

from solana_roi import v51_robinhood_consolidation as consolidation
from solana_roi.risk_conditioned_alpha_v51 import _rh_context_returns_v51
from solana_roi.v51_robinhood_phase9_65_69 import (
    CATCHUP_MODE,
    PROOF_MAX_SNAPSHOT_AGE_SECONDS,
    _apply_historical_contract,
    _candidate_disposition_report,
    _runtime_status_wrapper,
    _validated_proof,
    economic_family,
)


class Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


class Plane:
    def __init__(self, store: Store) -> None:
        self.store = store
        self.release_commit = "release-a"


class ReturnProbe:
    def __init__(self, store: Store) -> None:
        self.store = store
        self.release_commit = "release-a"

    def _v5_context_key(self, **kwargs):
        return "|".join(
            (
                str(kwargs["entity"]),
                str(kwargs["role"]),
                str(kwargs["lane"]),
                str(kwargs["venue"]),
                str(kwargs["lifecycle"]),
                str(kwargs["regime"]),
                str(kwargs["risk_signature"]),
                str(kwargs["flow_state"]),
                "chain_poll",
            )
        )


def test_65_historical_scan_is_not_a_readiness_condition() -> None:
    status = _apply_historical_contract(
        {
            "caught_up": False,
            "historical_caught_up": False,
            "historical_block_lag": 999_999,
            "catchup_capacity": {"catchup_mode": True, "catchup_stalled": True},
        }
    )
    assert status["caught_up"] is True
    assert status["historical_caught_up"] is True
    assert status["catchup_mode"] == CATCHUP_MODE
    assert status["historical_block_lag"] == 0
    assert status["historical_lag_blocks_readiness"] is False
    assert status["catchup_capacity"]["catchup_mode"] == "latest_seed_plus_reorg_insurance"
    assert status["catchup_capacity"]["bounded_short_reorg_insurance_only"] is True


def test_66_and_67_runtime_status_exposes_isolation_and_stale_proof_fails_closed() -> None:
    now = datetime.now(timezone.utc).isoformat()

    def base_status():
        return {
            "runtime_ready": True,
            "worker_isolation": {
                "dedicated_sqlite_store": True,
                "status_served_from_nonblocking_cache": True,
                "uvicorn_event_loop_runs_robinhood_chain_worker": False,
                "canonical_store_shared_for_robinhood_writes": False,
            },
            "v51_proof": {
                "available": True,
                "generated_at": now,
                "proof_state": "confirmed",
            },
        }

    status = _runtime_status_wrapper(base_status)()
    assert status["caught_up"] is True
    assert status["phase9_65_69"]["worker_isolation"]["contract_passed"] is True
    assert status["v51_proof"]["runtime_ready"] is True
    assert status["v51_proof"]["anchor_policy_passed"] is True
    assert status["v51_proof"]["proof_generated_at"] == now
    assert status["v51_proof"]["max_snapshot_age_seconds"] == PROOF_MAX_SNAPSHOT_AGE_SECONDS
    assert status["v51_proof"]["available"] is True

    missing_timestamp = _validated_proof(
        {"available": True, "proof_state": "confirmed"},
        runtime_ready=True,
        anchor_policy_passed=True,
    )
    assert missing_timestamp["available"] is False
    assert missing_timestamp["proof_state"] == "stale"

    stale = _validated_proof(
        {
            "available": True,
            "generated_at": (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat(),
            "proof_state": "confirmed",
        },
        runtime_ready=True,
        anchor_policy_passed=True,
    )
    assert stale["available"] is False
    assert stale["proof_snapshot_stale"] is True


def test_68_every_ledger_candidate_has_one_terminal_disposition_and_reject_counterfactual() -> None:
    store = Store()
    plane = Plane(store)
    traces = (
        (
            {
                "candidate_id": "rh-create:v3:token-a:tx-a:1",
                "token": "token-a",
                "market": "pool-a",
                "venue": "PONS_V1_UNISWAP_V3",
                "lifecycle": "launch_protected_v3",
                "selected_lane": None,
                "position_fraction": 0.0,
            },
            "paper_reject",
            "created_market_requires_forward_flow_or_reserve_update_before_lane_selection",
        ),
        (
            {
                "candidate_id": "rh-event:v2:token-b:tx-b:2",
                "token": "token-b",
                "market": "curve-b",
                "venue": "PONS_V2_CURVE",
                "lifecycle": "bonding_curve",
                "selected_lane": "entity_flow_accumulation",
                "position_fraction": 0.01,
            },
            "paper_enter",
            "canonical_v51_paper_entry",
        ),
        (
            {
                "candidate_id": "rh-event:v3:token-c:tx-c:3",
                "token": "token-c",
                "market": "pool-c",
                "venue": "UNISWAP_V3_DIRECT",
                "lifecycle": "new_weth_pool",
                "selected_lane": None,
                "position_fraction": 0.0,
            },
            "paper_reject",
            "all_candidate_lanes_killed",
        ),
    )
    for trace, decision, reason in traces:
        consolidation._upsert_ledger(plane, trace, decision=decision, reason=reason)

    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE v51_rejected_counterfactuals (surface TEXT,candidate_id TEXT)"
        )
        store.db.executemany(
            "INSERT INTO v51_rejected_counterfactuals(surface,candidate_id) VALUES ('ROBINHOOD_CHAIN',?)",
            [(traces[0][0]["candidate_id"],), (traces[2][0]["candidate_id"],)],
        )

    report = _candidate_disposition_report(store)
    assert report["candidate_count"] == 3
    assert report["terminal_disposition_count"] == 3
    assert report["terminal_disposition_debt_count"] == 0
    assert report["created_market_candidate_count"] == 1
    assert report["reserve_or_swap_update_candidate_count"] == 2
    assert report["counterfactual_logging_debt_count"] == 0
    assert report["coverage_complete"] is True


def test_69_robinhood_promotion_evidence_cannot_pool_v2_and_v3() -> None:
    store = Store()
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE robinhood_paper_trials ("
            "id INTEGER PRIMARY KEY,trigger_entity TEXT,venue TEXT,lifecycle TEXT)"
        )
        store.db.execute(
            "CREATE TABLE robinhood_v5_trial_context ("
            "trial_id INTEGER PRIMARY KEY,context_key TEXT,trigger_role TEXT,lane TEXT,regime TEXT,risk_signature TEXT)"
        )
        store.db.execute(
            "CREATE TABLE robinhood_paper_outcomes ("
            "id INTEGER PRIMARY KEY,trial_id INTEGER,release_commit TEXT,net_return REAL)"
        )
        v2_key = "entity-a|independent_entity|elite_entity_continuation|PONS_V2_CURVE|bonding_curve|neutral|clean|neutral|chain_poll"
        v3_key = "entity-a|independent_entity|elite_entity_continuation|PONS_V1_UNISWAP_V3|post_protection_v3|neutral|clean|neutral|chain_poll"
        store.db.execute(
            "INSERT INTO robinhood_paper_trials VALUES (1,'entity-a','PONS_V2_CURVE','bonding_curve')"
        )
        store.db.execute(
            "INSERT INTO robinhood_paper_trials VALUES (2,'entity-a','PONS_V1_UNISWAP_V3','post_protection_v3')"
        )
        store.db.execute(
            "INSERT INTO robinhood_v5_trial_context VALUES (1,?,'independent_entity','elite_entity_continuation','neutral','clean')",
            (v2_key,),
        )
        store.db.execute(
            "INSERT INTO robinhood_v5_trial_context VALUES (2,?,'independent_entity','elite_entity_continuation','neutral','clean')",
            (v3_key,),
        )
        store.db.execute("INSERT INTO robinhood_paper_outcomes VALUES (1,1,'release-a',0.10)")
        store.db.execute("INSERT INTO robinhood_paper_outcomes VALUES (2,2,'release-a',-0.50)")

    values, source = _rh_context_returns_v51(
        ReturnProbe(store),
        entity="entity-a",
        role="independent_entity",
        lane="elite_entity_continuation",
        venue="PONS_V2_CURVE",
        lifecycle="bonding_curve",
        regime="neutral",
        risk_signature="clean",
        flow_state="neutral",
    )
    assert values == [0.10]
    assert source in {"same_entity_lane_venue_lifecycle_regime_risk", "exact_entity_bootstrap"}
    assert economic_family(venue="PONS_V2_CURVE", lifecycle="bonding_curve") == "PONS_V2"
    assert economic_family(venue="PONS_V1_UNISWAP_V3", lifecycle="post_protection_v3") == "UNISWAP_V3"
    assert economic_family(venue="PONS_V1_UNISWAP_V3", lifecycle="post_graduation_continuation") == "POST_GRADUATION_CONTINUATION"
