from __future__ import annotations

import sqlite3

from solana_roi import robinhood_entity_universe as universe
from solana_roi import robinhood_pumpfun_wallet_intelligence as intelligence
from solana_roi import robinhood_pumpfun_wallet_intelligence_integration as integration
from solana_roi import robinhood_pumpfun_wallet_selection as selection
from solana_roi import robinhood_wallet_intelligence_v51_alignment as old_alignment
from solana_roi import robinhood_wallet_selection_authority_boundary_repair as boundary
from solana_roi.risk_conditioned_alpha_v51 import _context_key_v51, _parse_context_key
from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane


def _state_db() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute(
        "CREATE TABLE robinhood_wallet_selection_candidates ("
        "actor TEXT PRIMARY KEY,state TEXT NOT NULL,seed_label TEXT,last_error TEXT)"
    )
    db.execute(
        "CREATE TABLE robinhood_wallet_selection_broad_samples (swap_id INTEGER PRIMARY KEY,actor TEXT)"
    )
    db.execute(
        "CREATE TABLE robinhood_wallet_selection_forward (swap_id INTEGER PRIMARY KEY,actor TEXT)"
    )
    db.execute(
        "CREATE TABLE robinhood_wallet_intelligence_forward ("
        "swap_id INTEGER PRIMARY KEY,copyable_price_eth REAL,chase_fraction REAL,"
        "observation_lag_ms REAL,copyable INTEGER NOT NULL,"
        "immediate_copyable INTEGER NOT NULL DEFAULT 0)"
    )
    return db


def test_authority_repair_preserves_active_wallets_and_restores_only_wrong_rejections() -> None:
    db = _state_db()
    db.executemany(
        "INSERT INTO robinhood_wallet_selection_candidates(actor,state,seed_label,last_error) VALUES (?,?,?,?)",
        [
            ("0xactive", "tracking", None, None),
            ("0xseed", "seed_tracking", "seed", None),
            (
                "0xwrong",
                "forward_rejected",
                None,
                boundary.WRONG_INTELLIGENCE_REJECTION_PREFIX + " copyability_rate_below_minimum",
            ),
            (
                "0xwrongseed",
                "forward_rejected",
                "seed",
                boundary.WRONG_INTELLIGENCE_REJECTION_PREFIX + " observation_context_failed",
            ),
            (
                "0xlegitimate",
                "forward_rejected",
                None,
                "mature forward geometric value nonpositive; quality slot released",
            ),
        ],
    )
    db.execute("INSERT INTO robinhood_wallet_selection_broad_samples VALUES (1,'0xactive')")
    db.execute("INSERT INTO robinhood_wallet_selection_forward VALUES (1,'0xactive')")
    db.executemany(
        "INSERT INTO robinhood_wallet_intelligence_forward("
        "swap_id,copyable_price_eth,chase_fraction,observation_lag_ms,copyable,immediate_copyable) "
        "VALUES (?,?,?,?,?,?)",
        [
            (1, 1.25, 0.25, 35_000.0, 1, 0),
            (2, 1.10, 0.10, 10_000.0, 1, 0),
        ],
    )

    before_candidates = {
        str(row["actor"])
        for row in db.execute("SELECT actor FROM robinhood_wallet_selection_candidates").fetchall()
    }
    before_counts = {
        "candidates": db.execute("SELECT COUNT(*) FROM robinhood_wallet_selection_candidates").fetchone()[0],
        "broad": db.execute("SELECT COUNT(*) FROM robinhood_wallet_selection_broad_samples").fetchone()[0],
        "forward": db.execute("SELECT COUNT(*) FROM robinhood_wallet_selection_forward").fetchone()[0],
        "intelligence": db.execute("SELECT COUNT(*) FROM robinhood_wallet_intelligence_forward").fetchone()[0],
    }

    with db:
        result = boundary._repair_persisted_authority_state(db)

    states = {
        str(row["actor"]): str(row["state"])
        for row in db.execute(
            "SELECT actor,state FROM robinhood_wallet_selection_candidates ORDER BY actor"
        ).fetchall()
    }
    assert states["0xactive"] == "tracking"
    assert states["0xseed"] == "seed_tracking"
    assert states["0xwrong"] == "tracking"
    assert states["0xwrongseed"] == "seed_tracking"
    assert states["0xlegitimate"] == "forward_rejected"
    assert result["restored_misplaced_intelligence_rejection_count"] == 2
    assert result["active_tracking_set_not_reduced_by_repair"] is True
    assert result["candidate_identity_set_preserved"] is True
    assert result["row_counts_preserved"] is True

    after_candidates = {
        str(row["actor"])
        for row in db.execute("SELECT actor FROM robinhood_wallet_selection_candidates").fetchall()
    }
    after_counts = {
        "candidates": db.execute("SELECT COUNT(*) FROM robinhood_wallet_selection_candidates").fetchone()[0],
        "broad": db.execute("SELECT COUNT(*) FROM robinhood_wallet_selection_broad_samples").fetchone()[0],
        "forward": db.execute("SELECT COUNT(*) FROM robinhood_wallet_selection_forward").fetchone()[0],
        "intelligence": db.execute("SELECT COUNT(*) FROM robinhood_wallet_intelligence_forward").fetchone()[0],
    }
    assert after_candidates == before_candidates
    assert after_counts == before_counts


def test_pr169_strategy_observable_rewrite_is_reversed_to_diagnostic_semantics() -> None:
    db = _state_db()
    db.executemany(
        "INSERT INTO robinhood_wallet_intelligence_forward("
        "swap_id,copyable_price_eth,chase_fraction,observation_lag_ms,copyable,immediate_copyable) "
        "VALUES (?,?,?,?,?,?)",
        [
            # PR169 would have made this strategy-observable despite 25% / 35s.
            (1, 1.25, 0.25, 35_000.0, 1, 0),
            (2, 1.10, 0.10, 10_000.0, 1, 0),
        ],
    )
    with db:
        boundary._restore_diagnostic_copy_semantics(db)
    rows = {
        int(row["swap_id"]): dict(row)
        for row in db.execute(
            "SELECT swap_id,copyable,immediate_copyable FROM robinhood_wallet_intelligence_forward ORDER BY swap_id"
        ).fetchall()
    }
    assert rows[1]["copyable"] == 0
    assert rows[1]["immediate_copyable"] == 0
    assert rows[2]["copyable"] == 1
    assert rows[2]["immediate_copyable"] == 1


def test_production_wallet_selection_authority_is_quality_not_copyability() -> None:
    assert bool(
        getattr(
            RobinhoodChainPaperPlane,
            "_roi_robinhood_wallet_selection_authority_boundary_installed",
            False,
        )
    )
    assert getattr(
        RobinhoodChainPaperPlane,
        "_roi_robinhood_wallet_selection_authority_boundary_version",
    ) == boundary.REPAIR_VERSION
    assert universe.build_entity_universe is selection.build_quality_entity_universe
    assert universe._payload is integration._ORIGINAL_UNIVERSE_PAYLOAD
    assert selection._demote_mature_negative_candidates is intelligence._ORIGINAL_DEMOTE
    assert selection._demote_mature_negative_candidates is not intelligence._demote_with_copyable_intelligence
    assert getattr(
        RobinhoodChainPaperPlane,
        "_roi_robinhood_pumpfun_wallet_intelligence_authority_active",
    ) is False
    assert getattr(
        RobinhoodChainPaperPlane,
        "_roi_robinhood_wallet_intelligence_v51_alignment_authority_active",
    ) is False


def test_pr169_helper_monkeypatches_are_not_final_production_intelligence_semantics() -> None:
    assert old_alignment._ORIGINAL_ENRICH is not None
    assert old_alignment._ORIGINAL_ENSURE_SCHEMA is not None
    assert intelligence._enrich_forward_observations is old_alignment._ORIGINAL_ENRICH
    assert intelligence._ensure_schema is old_alignment._ORIGINAL_ENSURE_SCHEMA
    assert intelligence._candidate_profile is old_alignment._ORIGINAL_CANDIDATE_PROFILE


def test_chase_latency_and_cost_remain_downstream_v51_strategy_context() -> None:
    key = _context_key_v51(
        {
            "trigger_entity": "entity:wallet",
            "venue": "PUMP_AMM",
            "lifecycle": "pump_amm_active",
            "regime": "momentum",
            "role": "momentum_alpha",
            "risk": {"risk_signature": "clean"},
            "flow_state": "accumulation",
            "round_trip_cost_fraction": 0.10,
        },
        "elite_entity_continuation",
        chase=0.25,
        latency=35.0,
    )
    parsed = _parse_context_key(key)
    assert parsed["entity"] == "entity:wallet"
    assert parsed["chase_band"] == "challenger_15_25pct"
    assert parsed["latency_band"] == "30_60s"
    assert parsed["execution_cost_band"] == "7_15pct"
