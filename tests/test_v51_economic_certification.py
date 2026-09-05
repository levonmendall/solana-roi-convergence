from __future__ import annotations

import sqlite3
import threading

from solana_roi.strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH
from solana_roi.v51_economic_certification import build_economic_certification


class Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


def _seed() -> Store:
    store = Store()
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE v51_economic_freeze_releases (release_commit TEXT PRIMARY KEY,economic_freeze_epoch TEXT,authority_id TEXT,authority_fingerprint TEXT,registered_at TEXT,paper_only INTEGER,live_money_authority INTEGER)"
        )
        store.db.execute(
            "INSERT INTO v51_economic_freeze_releases VALUES ('release-a',?,?, 'fp','now',1,0)",
            (ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID),
        )
        store.db.execute(
            "CREATE TABLE risk_conditioned_alpha_v5_outcomes (id INTEGER PRIMARY KEY,release_commit TEXT,source_signature TEXT,token_mint TEXT,lane TEXT,venue TEXT,lifecycle TEXT,regime TEXT,risk_signature TEXT,context_key TEXT,position_fraction REAL,net_return REAL,settled_at TEXT)"
        )
        store.db.execute(
            "CREATE TABLE risk_conditioned_alpha_v5_trials (id INTEGER PRIMARY KEY,release_commit TEXT,source_signature TEXT,lane TEXT,trigger_wallet TEXT,flow_state TEXT,risk_severity REAL,chase_band TEXT,latency_band TEXT,round_trip_cost_fraction REAL)"
        )
        for i in range(40):
            wallet = "wallet-a" if i % 2 == 0 else "wallet-b"
            net = 0.08 if wallet == "wallet-a" else 0.03
            context = f"entity:{wallet}|graduation_continuation|PUMP_AMM|pump_amm_early_post_graduation_30_120s|high_speculation|independent_wallet|clean|active|le_15pct|2_5s|le_3pct"
            signature = f"sig-{i}"
            store.db.execute(
                "INSERT INTO risk_conditioned_alpha_v5_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (i + 1, "release-a", signature, f"mint-{i}", "graduation_continuation", "PUMP_AMM", "pump_amm_early_post_graduation_30_120s", "high_speculation", "clean", context, 0.01, net, f"2026-09-05T00:{i:02d}:00+00:00"),
            )
            store.db.execute(
                "INSERT INTO risk_conditioned_alpha_v5_trials VALUES (?,?,?,?,?,?,?,?,?,?)",
                (i + 1, "release-a", signature, "graduation_continuation", wallet, "active", 0.0, "le_15pct", "2_5s", 0.02),
            )
    return store


def test_certification_reports_required_economic_proof_metrics() -> None:
    payload = build_economic_certification(_seed())
    family = payload["families"]["PUMP_AMM"]
    assert payload["economic_freeze_epoch"] == ECONOMIC_FREEZE_EPOCH
    assert payload["historical_pre_epoch_promotion_authority"] is False
    assert family["closed_outcome_count"] == 40
    assert family["independent_event_count"] == 40
    assert family["compounded_nav_multiple"] > 1.0
    profile = family["robust_profile"]
    for key in (
        "best_expected_log_growth",
        "expected_shortfall_20",
        "max_drawdown_at_best_fraction",
        "leave_best_trade_out_mean",
        "remove_top_3_mean",
        "remove_top_5_mean",
        "mean_return_ci95_lower",
        "mean_return_ci95_upper",
    ):
        assert key in profile
    assert "latency_sensitivity" in family
    assert "execution_cost_sensitivity" in family
    assert "execution_stress" in family


def test_wallet_identity_is_measured_against_identity_free_context() -> None:
    payload = build_economic_certification(_seed())
    attribution = payload["incremental_alpha"]
    assert attribution["baseline"] == "matched_forward_context_excluding_entity_identity"
    assert attribution["attributable_outcome_count"] == 40
    wallet_a = attribution["entity_family_attribution"]["PUMP_AMM|wallet-a"]
    wallet_b = attribution["entity_family_attribution"]["PUMP_AMM|wallet-b"]
    assert wallet_a["residual_profile"]["mean_return"] > 0.0
    assert wallet_b["residual_profile"]["mean_return"] < 0.0


def test_research_allocation_is_ranked_by_forward_capital_efficiency() -> None:
    payload = build_economic_certification(_seed())
    assert payload["research_family_ranking"][0] == "PUMP_AMM"
    assert payload["paper_allocation_weights"].get("PUMP_AMM", 0.0) <= 0.25
    assert payload["paper_cash_weight"] >= 0.75
