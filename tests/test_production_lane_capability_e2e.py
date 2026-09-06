from __future__ import annotations

import json
import sqlite3
import threading

import pytest

from solana_roi.strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH, authority
from solana_roi.v51_economic_certification import build_economic_certification
from solana_roi.v51_lane_capability_e2e import (
    LANE_DESCRIPTORS,
    run_five_lane_capability_matrix,
    run_lane_capability_case,
)
from solana_roi.v51_promotion_proof import event_cluster_id
from solana_roi.v51_seeded_e2e import run_seeded_equivalence_case
from solana_roi.v51_synthetic_provenance import (
    PROVENANCE_VERSION,
    SYNTHETIC_SURFACE,
    register_synthetic_provenance,
    synthetic_provenance_for,
    synthetic_registry_snapshot,
)


class Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


def _stages(store: Store, candidate_id: str) -> list[sqlite3.Row]:
    with store._lock:
        return store.db.execute(
            "SELECT stage,stage_index,status,reason,payload_json FROM v51_candidate_pipeline_audit "
            "WHERE surface=? AND candidate_id=? ORDER BY stage_index",
            (SYNTHETIC_SURFACE, candidate_id),
        ).fetchall()


def _seed_one_canonical_fomo_outcome(store: Store) -> None:
    with store._lock, store.db:
        store.db.executescript(
            "CREATE TABLE v51_economic_freeze_releases ("
            "release_commit TEXT NOT NULL,economic_freeze_epoch TEXT NOT NULL,authority_id TEXT NOT NULL);"
            "CREATE TABLE fomo_paper_outcomes ("
            "id INTEGER PRIMARY KEY,release_commit TEXT NOT NULL,source_signature TEXT NOT NULL,"
            "token_mint TEXT,trigger_wallet TEXT,venue TEXT,lifecycle TEXT,regime TEXT,"
            "position_fraction REAL,net_return REAL,settled_at TEXT);"
            "CREATE TABLE fomo_paper_trials ("
            "id INTEGER PRIMARY KEY,release_commit TEXT NOT NULL,source_signature TEXT NOT NULL,"
            "fomo_state TEXT,signal_to_entry_seconds REAL,entry_cost_sol REAL);"
        )
        store.db.execute(
            "INSERT INTO v51_economic_freeze_releases(release_commit,economic_freeze_epoch,authority_id) "
            "VALUES (?,?,?)",
            ("canonical-release", ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID),
        )
        store.db.execute(
            "INSERT INTO fomo_paper_trials("
            "release_commit,source_signature,fomo_state,signal_to_entry_seconds,entry_cost_sol) "
            "VALUES (?,?,?,?,?)",
            ("canonical-release", "canonical-fomo-1", "active_fomo", 2.0, 0.001),
        )
        store.db.execute(
            "INSERT INTO fomo_paper_outcomes("
            "release_commit,source_signature,token_mint,trigger_wallet,venue,lifecycle,regime,"
            "position_fraction,net_return,settled_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                "canonical-release",
                "canonical-fomo-1",
                "canonical-token",
                "canonical-wallet",
                "PUMP_AMM",
                "early_post_graduation",
                "high_speculation",
                0.01,
                0.10,
                "2026-09-06T00:00:00+00:00",
            ),
        )


def test_all_five_lanes_have_positive_full_pipeline_capability() -> None:
    store = Store()
    for lane_name, descriptor in LANE_DESCRIPTORS.items():
        result = run_lane_capability_case(store, lane_name, qualifying=True)
        candidate_id = f"batch3-{lane_name}-positive"
        rows = _stages(store, candidate_id)

        assert result["result"]["decision"] == "paper_enter"
        assert result["result"]["synthetic"] is True
        assert result["result"]["certification_eligible"] is False
        assert result["result"]["promotion_eligible"] is False
        assert result["economic_surface"] == descriptor["economic_surface"]
        assert result["strategy_family"] == descriptor["strategy_family"]
        assert [row["stage"] for row in rows] == authority()["pipeline_stages"]
        assert rows[-1]["stage"] == "learning"
        assert rows[-1]["status"] == "complete"

        for row in rows:
            payload = json.loads(str(row["payload_json"]))
            assert payload["synthetic"] is True
            assert payload["certification_eligible"] is False
            assert payload["promotion_eligible"] is False
            provenance = payload["provenance"]
            assert provenance["provenance_version"] == PROVENANCE_VERSION
            assert provenance["lane"] == descriptor["lane"]
            assert provenance["economic_surface"] == descriptor["economic_surface"]
            assert provenance["venue"] == descriptor["venue"]
            assert provenance["synthetic"] is True
            assert provenance["paper_only"] is True
            assert provenance["live_money_authority"] is False


def test_all_five_lanes_have_explicit_legitimate_negative_paths() -> None:
    store = Store()
    for lane_name, descriptor in LANE_DESCRIPTORS.items():
        result = run_lane_capability_case(store, lane_name, qualifying=False)
        candidate_id = f"batch3-{lane_name}-negative"
        rows = _stages(store, candidate_id)

        assert result["result"]["decision"] == "paper_reject"
        assert result["result"]["reason"] == descriptor["negative_reason"]
        assert rows[-1]["stage"] == "position"
        assert rows[-1]["status"] == "not_opened"
        assert rows[-1]["reason"] == descriptor["negative_reason"]
        assert all(json.loads(str(row["payload_json"]))["synthetic"] is True for row in rows)


def test_synthetic_provenance_is_immutable() -> None:
    store = Store()
    first = register_synthetic_provenance(
        store,
        candidate_id="immutable-1",
        origin="batch3",
        lane="elite_wallet_continuation",
        economic_surface="SOLANA",
        venue="PUMP_FUN",
        release_commit="batch3-isolated-capability",
    )
    same = register_synthetic_provenance(
        store,
        candidate_id="immutable-1",
        origin="batch3",
        lane="elite_wallet_continuation",
        economic_surface="SOLANA",
        venue="PUMP_FUN",
        release_commit="batch3-isolated-capability",
    )
    assert first == same
    assert synthetic_provenance_for(store, "immutable-1")["synthetic"] is True

    with pytest.raises(RuntimeError, match="synthetic_provenance_is_immutable"):
        register_synthetic_provenance(
            store,
            candidate_id="immutable-1",
            origin="rewritten-origin",
            lane="elite_wallet_continuation",
            economic_surface="SOLANA",
            venue="PUMP_FUN",
            release_commit="batch3-isolated-capability",
        )


def test_seeded_harness_refuses_canonical_surface_writes() -> None:
    store = Store()
    with pytest.raises(ValueError, match="seeded_e2e_must_use_isolated_synthetic_surface"):
        run_seeded_equivalence_case(
            store,
            {
                "candidate_id": "bad-surface",
                "surface": "SOLANA",
                "economic_surface": "SOLANA",
                "venue": "PUMP_FUN",
                "lane": "elite_wallet_continuation",
            },
        )


def test_synthetic_matrix_does_not_change_canonical_certification_counts() -> None:
    store = Store()
    _seed_one_canonical_fomo_outcome(store)
    before = build_economic_certification(store)
    assert before["closed_outcome_count"] == 1

    matrix = run_five_lane_capability_matrix(store)
    after = build_economic_certification(store)

    assert matrix["positive_case_count"] == 5
    assert matrix["negative_case_count"] == 5
    assert matrix["certification_eligible_case_count"] == 0
    assert matrix["promotion_eligible_case_count"] == 0
    assert before["closed_outcome_count"] == after["closed_outcome_count"] == 1
    assert before["families"].keys() == after["families"].keys()
    with store._lock:
        canonical_outcomes = store.db.execute("SELECT COUNT(*) AS n FROM fomo_paper_outcomes").fetchone()
    assert int(canonical_outcomes["n"]) == 1

    provenance = synthetic_registry_snapshot(store)
    assert provenance["synthetic_candidate_count"] == 10
    assert set(provenance["by_lane"]) == {descriptor["lane"] for descriptor in LANE_DESCRIPTORS.values()}
    assert provenance["synthetic_rows_are_certification_eligible"] is False
    assert provenance["synthetic_rows_are_promotion_eligible"] is False
    assert provenance["canonical_economic_tables_written_by_registry"] is False


def test_cross_lane_family_clustering_isolated_for_same_token_lifecycle() -> None:
    token = "shared-token"
    lifecycle = "continuation"
    cluster_ids = {
        descriptor["strategy_family"]: event_cluster_id(
            {
                "token_mint": token,
                "lifecycle": lifecycle,
                "surface": descriptor["economic_surface"],
                "venue": descriptor["venue"],
                "family": descriptor["strategy_family"],
            },
            family=descriptor["strategy_family"],
        )
        for descriptor in LANE_DESCRIPTORS.values()
    }
    assert len(cluster_ids) == 5
    assert len(set(cluster_ids.values())) == 5


def test_batch3_harness_preserves_paper_only_authority() -> None:
    store = Store()
    matrix = run_five_lane_capability_matrix(store)
    assert matrix["paper_only"] is True
    assert matrix["live_money_authority"] is False
    assert matrix["changes_strategy_authority"] is False
    assert matrix["changes_economic_thresholds"] is False
    for lane in matrix["lanes"].values():
        for case in lane.values():
            assert case["paper_only"] is True
            assert case["live_money_authority"] is False
            assert case["changes_strategy_authority"] is False
            assert case["changes_economic_thresholds"] is False
