from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from solana_roi import v51_measurement_integrity as measurement
from solana_roi.v51_candidate_ledger import ensure_schema, record_solana_candidate, record_stage_event
from solana_roi.v51_evidence_analytics import (
    _portfolio_reconcile,
    _promotion_certification_from_records,
    build_cross_family_correlation,
    build_forward_proof_slo,
    build_hazard_calibration,
    refresh_execution_cost_ledger,
    refresh_rejected_counterfactuals,
)
from solana_roi.v51_promotion_proof import (
    _ensure_release_with_attestation,
    benjamini_hochberg,
    cluster_rows,
    evidence_partition,
    refresh_release_attestation,
)


ROOT = Path(__file__).resolve().parents[1]


class Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()

    def append(self, *_args, **_kwargs) -> None:
        return None


def _swap(signature: str, *, wallet: str = "wallet-a", token: str = "mint-a") -> SimpleNamespace:
    observed = datetime(2026, 9, 5, 20, 0, tzinfo=timezone.utc)
    return SimpleNamespace(
        signature=signature,
        slot=123,
        observed_at=observed,
        received_at=observed + timedelta(seconds=1),
        wallet=wallet,
        token_mint=token,
        side="buy",
        token_amount=100.0,
        native_amount_sol=1.0,
        reference_price_sol=0.01,
        source="solana-direct:PUMP_AMM:buy",
        ingestion_latency_ms=1000.0,
    )


def test_23_current_release_starts_unattested_then_earns_solana_attestation(monkeypatch) -> None:
    release = "1" * 40
    monkeypatch.setenv("GITHUB_SHA", release)
    store = Store()

    import solana_roi.v51_promotion_proof as promotion

    # The broad suite may already have imported production, which installs the
    # attestation wrapper. Always point this unit test at the immutable pre-wrapper
    # compatibility registrar instead of ever wiring the wrapper back to itself.
    base = getattr(
        measurement,
        "_ORIGINAL_ENSURE_RELEASE_COMPATIBILITY",
        measurement.ensure_release_compatibility,
    )
    if bool(getattr(base, "_roi_v51_release_attestation_gate", False)):
        from solana_roi.v51_measurement_integrity_hardening import (
            _ensure_release_compatibility_fail_closed,
        )

        base = _ensure_release_compatibility_fail_closed
    monkeypatch.setattr(promotion, "_ORIGINAL_ENSURE_RELEASE_COMPATIBILITY", base)
    row = _ensure_release_with_attestation(store, release)
    assert row is not None
    assert int(row["promotion_eligible"]) == 0
    assert str(row["reason"]) == "current_release_pending_live_attestation"

    record_solana_candidate(store, _swap("sig-attested"), release_commit=release)
    record_stage_event(
        store,
        surface="SOLANA",
        candidate_id="sig-attested",
        release_commit=release,
        stage="context",
        status="complete",
        reason="context_ready",
    )
    record_stage_event(
        store,
        surface="SOLANA",
        candidate_id="sig-attested",
        release_commit=release,
        stage="execution_evidence",
        status="complete",
        reason="amount_specific_entry_and_exit_evidence",
    )
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE v51_wallet_discovery_forward_lineage("
            "release_commit TEXT,measurement_epoch TEXT,source_candidate_id TEXT)"
        )
        store.db.execute(
            "INSERT INTO v51_wallet_discovery_forward_lineage VALUES (?,?,?)",
            (release, measurement.MEASUREMENT_EPOCH, "sig-attested"),
        )

    attestation = refresh_release_attestation(store, release_commit=release, surfaces=("SOLANA",))
    assert attestation["attested"] is True
    assert attestation["surfaces"]["SOLANA"]["candidate_coverage_valid"] is True
    with store._lock:
        compatibility = store.db.execute(
            "SELECT promotion_eligible,reason FROM v51_release_compatibility WHERE release_commit=?", (release,)
        ).fetchone()
    assert int(compatibility["promotion_eligible"]) == 1
    assert compatibility["reason"] == "live_release_attestation_confirmed"


def test_24_25_26_promotion_certification_clusters_events_excludes_discovery_and_fdr_controls() -> None:
    duplicate = [
        {"family": "PUMP_AMM", "token_mint": "same", "lifecycle": "post", "net_return": 0.10, "source_signature": "a"},
        {"family": "PUMP_AMM", "token_mint": "same", "lifecycle": "post", "net_return": 0.20, "source_signature": "b"},
    ]
    assert len(cluster_rows(duplicate, family="PUMP_AMM")) == 1

    discovery_token = None
    for index in range(10_000):
        token = f"discovery-{index}"
        row = {"family": "PUMP_AMM", "token_mint": token, "lifecycle": "post"}
        cluster_id = cluster_rows([{**row, "net_return": 0.1}], family="PUMP_AMM")[0]["event_cluster_id"]
        if evidence_partition(cluster_id) == "discovery":
            discovery_token = token
            break
    assert discovery_token is not None
    assert cluster_rows(
        [{"family": "PUMP_AMM", "token_mint": discovery_token, "lifecycle": "post", "net_return": 1.0}],
        family="PUMP_AMM",
        promotion_only=True,
    ) == []

    rows = [
        {
            "family": "PUMP_AMM",
            "token_mint": f"mint-{index}",
            "lifecycle": "post_graduation",
            "net_return": 0.10,
            "source_signature": f"sig-{index}",
            "settled_at": f"2026-09-{(index % 28) + 1:02d}T12:00:00+00:00",
            "risk_signature": "clean",
            "risk_severity": 0.0,
        }
        for index in range(160)
    ]
    certification = _promotion_certification_from_records(rows)
    family = certification["families"]["PUMP_AMM"]
    assert family["independent_event_cluster_count"] < family["raw_outcome_count"]
    assert family["validation_cluster_count"] > 0
    assert family["holdout_cluster_count"] > 0
    assert family["fdr_accepted"] is True
    assert family["promotion_claim_valid"] is True
    assert certification["paper_allocation_weights"]["PUMP_AMM"] <= 0.25

    controlled = benjamini_hochberg({"real": 0.001, "lucky": 0.2, "noise": 0.9}, q=0.10)
    assert controlled == {"real": True, "lucky": False, "noise": False}


def test_27_fomo_round_trip_cost_is_normalized_from_amount_specific_unified_trial() -> None:
    store = Store()
    release = "2" * 40
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE v51_economic_freeze_releases(release_commit TEXT,economic_freeze_epoch TEXT,authority_id TEXT)"
        )
        store.db.execute(
            "INSERT INTO v51_economic_freeze_releases VALUES (?,?,?)",
            (release, "v51-consolidated-proof-20260905", "roi-convergence-v5.1-consolidated-proof-1"),
        )
        store.db.execute(
            "CREATE TABLE fomo_paper_outcomes(id INTEGER PRIMARY KEY,release_commit TEXT,source_signature TEXT,token_mint TEXT,"
            "trigger_wallet TEXT,venue TEXT,lifecycle TEXT,regime TEXT,position_fraction REAL,net_return REAL,settled_at TEXT)"
        )
        store.db.execute(
            "CREATE TABLE fomo_paper_trials(release_commit TEXT,source_signature TEXT,fomo_state TEXT,"
            "signal_to_entry_seconds REAL,entry_cost_sol REAL)"
        )
        store.db.execute(
            "CREATE TABLE profit_first_final_trials(release_commit TEXT,source_signature TEXT,lane TEXT,round_trip_cost_fraction REAL)"
        )
        store.db.execute(
            "INSERT INTO fomo_paper_outcomes VALUES (1,?,?,?,?,?,?,?,?,?,?)",
            (release, "fomo-sig", "mint-f", "wallet-f", "PUMP_AMM", "post", "hot", 0.01, 0.2, "2026-09-05T20:00:00+00:00"),
        )
        store.db.execute(
            "INSERT INTO fomo_paper_trials VALUES (?,?,?,?,?)",
            (release, "fomo-sig", "active_fomo", 3.0, 0.5),
        )
        store.db.execute(
            "INSERT INTO profit_first_final_trials VALUES (?,?,?,?)",
            (release, "fomo-sig", "unified_profit_maximizer", 0.06),
        )
    proof = refresh_execution_cost_ledger(store)
    assert proof["normalized_outcome_count"] == 1
    with store._lock:
        row = store.db.execute(
            "SELECT round_trip_cost_fraction,cost_source FROM v51_execution_cost_ledger WHERE source_signature='fomo-sig'"
        ).fetchone()
    assert float(row["round_trip_cost_fraction"]) == 0.06
    assert row["cost_source"] == "profit_first_amount_specific_round_trip_cost"


def test_28_rejected_counterfactual_resolves_shadow_outcome_without_retroactive_entry(monkeypatch) -> None:
    release = "3" * 40
    monkeypatch.setenv("GITHUB_SHA", release)
    store = Store()
    ensure_schema(store)
    record_solana_candidate(store, _swap("reject-sig"), release_commit=release)
    record_stage_event(
        store,
        surface="SOLANA",
        candidate_id="reject-sig",
        release_commit=release,
        stage="position",
        status="not_opened",
        reason="hazard_insufficient_evidence",
    )
    with store._lock, store.db:
        store.db.execute("CREATE TABLE profit_first_final_outcomes(source_signature TEXT,net_return REAL)")
        store.db.execute("INSERT INTO profit_first_final_outcomes VALUES ('reject-sig',0.55)")
    proof = refresh_rejected_counterfactuals(store)
    assert proof["rejected_candidate_count"] == 1
    assert proof["resolved_positive_count"] == 1
    assert proof["retrospective_entry_authority"] is False
    with store._lock:
        entry_tables = store.db.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='paper_positions'"
        ).fetchone()[0]
    assert entry_tables == 0


def test_29_hazard_calibration_is_diagnostic_and_does_not_change_authority() -> None:
    store = Store()
    proof = build_hazard_calibration(store)
    assert proof["changes_current_hazard_multipliers"] is False
    assert set(proof["bins"]) == {"clean", "low", "moderate", "high", "extreme"}
    assert proof["current_hazard_evidence_burden"]["extreme"]["minimum_independent_outcomes"] == 90


def test_30_cross_family_correlation_requires_aligned_forward_periods() -> None:
    store = Store()
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = []
    for index in range(300):
        at = (start + timedelta(days=index)).isoformat()
        value = 0.05 if index % 2 == 0 else -0.02
        rows.extend(
            [
                {"family": "PUMP_AMM", "token_mint": f"p-{index}", "lifecycle": "post", "net_return": value, "settled_at": at},
                {"family": "RAYDIUM", "token_mint": f"r-{index}", "lifecycle": "native", "net_return": value, "settled_at": at},
            ]
        )
    proof = build_cross_family_correlation(store, rows=rows)
    pair = proof["pairs"]["PUMP_AMM|RAYDIUM"]
    assert pair["aligned_period_count"] >= proof["minimum_aligned_periods"]
    assert pair["mature"] is True
    assert float(pair["pearson_correlation"]) > 0.9
    assert proof["unknown_correlation_is_zero"] is False


def test_31_active_family_cap_remains_frozen_at_25_percent() -> None:
    from solana_roi.strategy_v51_authority import authority

    allocation = authority()["allocation"]
    assert allocation["immature_family_max_weight"] == 0.25
    assert allocation["permanent_family_max_weight"] == 0.50


def test_32_one_capital_base_caps_overlapping_requested_positions() -> None:
    rows = [
        {
            "surface": "SOLANA_ALPHA",
            "source_signature": "a",
            "position_fraction": 0.75,
            "net_return": 0.10,
            "entry_at": "2026-09-05T20:00:00+00:00",
            "settled_at": "2026-09-05T21:00:00+00:00",
        },
        {
            "surface": "FOMO",
            "source_signature": "b",
            "position_fraction": 0.75,
            "net_return": 0.10,
            "entry_at": "2026-09-05T20:00:00+00:00",
            "settled_at": "2026-09-05T21:00:00+00:00",
        },
    ]
    proof = _portfolio_reconcile(rows)
    assert proof["cash_capacity_shortfall_count"] == 1
    assert proof["overlapping_positions_share_one_capital_base"] is True
    assert abs(float(proof["ending_nav"]) - 1.10) < 1e-9


def test_33_forward_slo_exposes_old_coverage_debt(monkeypatch) -> None:
    release = "4" * 40
    monkeypatch.setenv("GITHUB_SHA", release)
    store = Store()
    ensure_schema(store)
    old = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    record_stage_event(
        store,
        surface="SOLANA",
        candidate_id="old-debt",
        release_commit=release,
        stage="context",
        status="coverage_debt",
        reason="context_missing",
        observed_at=old,
    )
    proof = build_forward_proof_slo(store)
    assert proof["coverage_debt_count"] == 1
    assert float(proof["oldest_coverage_debt_age_seconds"]) >= 500
    assert proof["proof_state"] == "degraded"


def test_34_superseded_import_order_hook_is_deleted_and_explicit_authority_remains() -> None:
    assert not (ROOT / "src" / "solana_roi" / "v51_final_production_install.py").exists()
    production = (ROOT / "src" / "solana_roi" / "production.py").read_text(encoding="utf-8")
    assert "install_v51_production_authority(app, ingestion_runtime)" in production
    isolation = (ROOT / "src" / "solana_roi" / "robinhood_worker_isolation_repair.py").read_text(encoding="utf-8")
    assert "v51_final_production_install" not in isolation
    retirement = (ROOT / "src" / "solana_roi" / "v51_architecture_retirement.py").read_text(encoding="utf-8")
    assert '"state": "deleted"' in retirement
    assert "v51_final_production_install.py" in retirement
