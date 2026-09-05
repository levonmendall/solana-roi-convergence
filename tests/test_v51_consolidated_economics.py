from __future__ import annotations

import sqlite3
import threading

from solana_roi.strategy_v51_authority import authority, hazard_requirements
from solana_roi.v51_candidate_pipeline import record_seeded_stage
from solana_roi.v51_economic_core import (
    bootstrap_execution_multiplier,
    execution_stress_profiles,
    hierarchical_profile,
    robust_profile,
)


class Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


def test_canonical_authority_is_paper_only_and_freezes_v51() -> None:
    payload = authority()
    assert payload["strategy_version"] == "roi-convergence-v5.1-context-exactness-1"
    assert payload["economic_freeze_epoch"] == "v51-consolidated-proof-20260905"
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
    assert payload["execution"]["latency_hard_max_seconds"] == 20.0
    assert payload["execution"]["chase_observe_only_above_fraction"] == 0.40


def test_latency_is_maximum_not_economic_approval() -> None:
    fast = bootstrap_execution_multiplier(
        latency_seconds=2.0,
        chase_fraction=0.05,
        round_trip_cost_fraction=0.02,
        risk_severity=0.0,
        risk_signature="clean",
    )
    late = bootstrap_execution_multiplier(
        latency_seconds=19.0,
        chase_fraction=0.05,
        round_trip_cost_fraction=0.02,
        risk_severity=0.0,
        risk_signature="clean",
    )
    inaccessible = bootstrap_execution_multiplier(
        latency_seconds=20.01,
        chase_fraction=0.05,
        round_trip_cost_fraction=0.02,
        risk_severity=0.0,
        risk_signature="clean",
    )
    assert 0.0 < late < fast <= 1.0
    assert inaccessible == 0.0


def test_hazards_raise_evidence_burden_and_reduce_probe_size() -> None:
    clean = hazard_requirements(0.0, "clean")
    moderate = hazard_requirements(0.30, "creator_distributing")
    extreme = hazard_requirements(0.90, "creator_distributing+bundled_launch+high_snipe_tax")
    assert clean["minimum_independent_outcomes"] < moderate["minimum_independent_outcomes"] < extreme["minimum_independent_outcomes"]
    assert clean["bootstrap_size_multiplier"] > moderate["bootstrap_size_multiplier"] > extreme["bootstrap_size_multiplier"]


def test_hierarchical_pooling_requires_exact_and_same_entity_evidence() -> None:
    exact = [0.08] * 10
    same_entity_parent = [0.04] * 20
    profile = hierarchical_profile(exact, same_entity_parent, (), risk_severity=0.0, risk_signature="clean")
    assert profile["exact_sample_count"] == 10
    assert profile["independent_evidence_count"] == 30
    assert profile["promoted"] is True
    assert profile["best_expected_log_growth"] > 0.0


def test_lane_kill_requires_sufficient_robust_negative_evidence() -> None:
    exact = [-0.10] * 10
    parent = [-0.05] * 50
    profile = hierarchical_profile(exact, parent, (), risk_severity=0.0, risk_signature="clean")
    assert profile["independent_evidence_count"] == 60
    assert profile["killed"] is True
    assert profile["state"] == "killed_negative_robust_edge"


def test_execution_stress_degrades_positive_edge() -> None:
    values = [0.10, 0.08, 0.06, 0.04] * 10
    base = robust_profile(values)
    stress = execution_stress_profiles(values)
    assert stress["mild"]["best_expected_log_growth"] < base["best_expected_log_growth"]
    assert stress["severe"]["best_expected_log_growth"] < stress["mild"]["best_expected_log_growth"]


def test_seeded_pipeline_has_exact_eight_stage_order() -> None:
    store = Store()
    stages = authority()["pipeline_stages"]
    for stage in stages:
        record_seeded_stage(store, "candidate-1", stage)
    with store._lock:
        rows = store.db.execute(
            "SELECT stage,stage_index,status FROM v51_candidate_pipeline_audit "
            "WHERE surface='SEEDED_E2E' AND candidate_id='candidate-1' ORDER BY stage_index"
        ).fetchall()
    assert [row["stage"] for row in rows] == stages
    assert [row["stage_index"] for row in rows] == list(range(8))
    assert all(row["status"] == "complete" for row in rows)
