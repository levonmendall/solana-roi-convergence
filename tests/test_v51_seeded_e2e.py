from __future__ import annotations

import json
import sqlite3
import threading

from solana_roi.strategy_v51_authority import authority
from solana_roi.v51_seeded_e2e import run_seeded_equivalence_case


class Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


def _stages(store: Store, candidate_id: str) -> list[sqlite3.Row]:
    with store._lock:
        return store.db.execute(
            "SELECT stage,stage_index,status,reason,payload_json FROM v51_candidate_pipeline_audit "
            "WHERE surface='SEEDED_E2E' AND candidate_id=? ORDER BY stage_index",
            (candidate_id,),
        ).fetchall()


def test_seeded_qualifying_event_reaches_learning() -> None:
    store = Store()
    result = run_seeded_equivalence_case(
        store,
        {
            "candidate_id": "qualified-1",
            "token": "mint-a",
            "venue": "PUMP_AMM",
            "lifecycle": "early_post_graduation",
            "lane": "graduation_continuation",
            "risk_signature": "clean",
            "risk_severity": 0.0,
            "structurally_tradeable": True,
            "entry_executable": True,
            "exit_executable": True,
            "latency_seconds": 3.0,
            "chase_fraction": 0.08,
            "round_trip_cost_fraction": 0.03,
            "base_position_fraction": 0.01,
            "settled_net_return": 0.25,
        },
    )
    assert result["decision"] == "paper_enter"
    assert result["synthetic"] is True
    assert result["certification_eligible"] is False
    assert result["promotion_eligible"] is False
    rows = _stages(store, "qualified-1")
    assert [row["stage"] for row in rows] == authority()["pipeline_stages"]
    assert rows[-1]["stage"] == "learning"
    assert rows[-1]["status"] == "complete"
    assert all(json.loads(str(row["payload_json"]))["synthetic"] is True for row in rows)


def test_seeded_nonqualification_has_explicit_fail_closed_reason() -> None:
    store = Store()
    result = run_seeded_equivalence_case(
        store,
        {
            "candidate_id": "rejected-1",
            "token": "mint-b",
            "venue": "PUMP_FUN",
            "lifecycle": "bonding_curve",
            "lane": "elite_wallet_continuation",
            "structurally_tradeable": True,
            "entry_executable": False,
            "exit_executable": True,
            "latency_seconds": 4.0,
            "chase_fraction": 0.10,
            "round_trip_cost_fraction": 0.04,
        },
    )
    assert result == {
        "decision": "paper_reject",
        "reason": "exact_entry_or_exit_execution_evidence_unavailable",
        "authority_id": "roi-convergence-v5.1-consolidated-proof-1",
        "economic_freeze_epoch": "v51-consolidated-proof-20260905",
        "synthetic": True,
        "certification_eligible": False,
        "promotion_eligible": False,
    }
    rows = _stages(store, "rejected-1")
    assert [row["stage"] for row in rows] == [
        "ingestion", "candidate", "context", "execution_evidence", "decision", "position"
    ]
    assert rows[-1]["status"] == "not_opened"
    assert rows[-1]["reason"] == "exact_entry_or_exit_execution_evidence_unavailable"
    assert all(json.loads(str(row["payload_json"]))["synthetic"] is True for row in rows)
