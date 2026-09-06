from __future__ import annotations

import json
import sqlite3
import threading

from solana_roi.v51_latency_challenger import (
    build_latency_challenger_research,
    latency_research_band,
)


class Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


def _seed(store: Store) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE profit_first_final_trials ("
            "release_commit TEXT,source_signature TEXT,lane TEXT,signal_to_entry_seconds REAL,"
            "round_trip_cost_fraction REAL,opportunity_json TEXT,context_json TEXT,decision_json TEXT)"
        )
        store.db.execute(
            "CREATE TABLE v51_candidates ("
            "surface TEXT,candidate_id TEXT,release_commit TEXT,venue TEXT,lifecycle TEXT,raw_chase_fraction REAL)"
        )
    # Let the production helper own the canonical counterfactual schema.
    from solana_roi.v51_evidence_analytics import ensure_counterfactual_schema

    ensure_counterfactual_schema(store)
    rows = (
        ("s19", 19.0, "PUMP_FUN", "pump_bonding_curve", 0.04),
        ("s28", 28.0, "PUMP_AMM", "pump_amm_early_post_graduation_30_120s", 0.08),
        ("s65", 65.0, "RAYDIUM", "raydium_post_pump_migration_evidence", 0.10),
        ("s120", 120.0, "FOMO", "active_fomo", 0.12),
    )
    with store._lock, store.db:
        for signature, latency, venue, lifecycle, chase in rows:
            store.db.execute(
                "INSERT INTO profit_first_final_trials VALUES (?,?,?,?,?,?,?,?)",
                (
                    "release-a",
                    signature,
                    "unified_profit_maximizer",
                    latency,
                    0.02,
                    json.dumps({"venue": venue, "lifecycle": lifecycle, "chase_fraction": chase}),
                    json.dumps({"venue": venue, "lifecycle": lifecycle, "chase_fraction": chase}),
                    "{}",
                ),
            )
            store.db.execute(
                "INSERT INTO v51_candidates VALUES (?,?,?,?,?,?)",
                ("FOMO" if venue == "FOMO" else "SOLANA", signature, "release-a", venue, lifecycle, chase),
            )
        returns = {"s19": 0.03, "s28": 0.18, "s65": 0.11, "s120": -0.07}
        for signature, value in returns.items():
            surface = "FOMO" if signature == "s120" else "SOLANA"
            store.db.execute(
                "INSERT INTO v51_rejected_counterfactuals("
                "surface,candidate_id,release_commit,token_mint,decision_reason,decision_observed_at,"
                "forward_net_return,resolution_source,counterfactual_state,hazard_signature,hazard_severity,"
                "payload_json,updated_at,retrospective_entry_authority,paper_only,live_money_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,0,1,0)",
                (
                    surface,
                    signature,
                    "release-a",
                    "mint-" + signature,
                    "research_seed",
                    "2026-09-05T00:00:00+00:00",
                    value,
                    "seeded_forward_outcome",
                    "resolved_shadow_forward_outcome",
                    "clean",
                    0.0,
                    "{}",
                    "2026-09-05T00:01:00+00:00",
                ),
            )


def test_latency_band_edges() -> None:
    assert latency_research_band(20.0) == "authorized_le_20s"
    assert latency_research_band(20.0001) == "challenger_20_40s"
    assert latency_research_band(40.0) == "challenger_20_40s"
    assert latency_research_band(40.0001) == "challenger_40_90s"
    assert latency_research_band(90.0) == "challenger_40_90s"
    assert latency_research_band(90.0001) == "later_lifecycle_gt_90s"


def test_challenger_is_measurement_only_and_preserves_20s_authority() -> None:
    store = Store()
    _seed(store)
    proof = build_latency_challenger_research(store)

    assert proof["current_authorized_hard_max_seconds"] == 20.0
    assert proof["current_authority_changed"] is False
    assert proof["retrospective_entry_authority"] is False
    assert proof["paper_only"] is True
    assert proof["live_money_authority"] is False
    assert proof["above_current_authority_candidate_count"] == 3
    assert proof["resolved_above_current_authority_count"] == 3

    assert proof["bands"]["authorized_le_20s"]["candidate_count"] == 1
    assert proof["bands"]["challenger_20_40s"]["candidate_count"] == 1
    assert proof["bands"]["challenger_40_90s"]["candidate_count"] == 1
    assert proof["bands"]["later_lifecycle_gt_90s"]["candidate_count"] == 1

    cohort = proof[
        "cohorts"
    ]["SOLANA|PUMP_AMM|pump_amm_early_post_graduation_30_120s|challenger_20_40s"]
    assert cohort["resolved_count"] == 1
    assert cohort["positive_count"] == 1
    assert cohort["return_profile"]["mean_return"] == 0.18
