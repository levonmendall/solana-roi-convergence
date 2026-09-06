from __future__ import annotations

import math
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from solana_roi.strategy_v51_authority import authority
from solana_roi.v51_economic_clustering import cluster_economic_rows
from solana_roi.v51_economic_core import robust_profile, stress_returns
from solana_roi.v51_measurement_integrity import MEASUREMENT_EPOCH
from solana_roi.v51_phase14_profitability_certification import (
    CLASS_INSUFFICIENT_EVIDENCE,
    MIN_CONTINUOUS_PRODUCTION_SECONDS,
    compose_phase14_profitability_certification,
)
from solana_roi.v51_promotion_proof import evidence_partition, event_cluster_id
from solana_roi.v51_return_validation import (
    INVALID_ECONOMIC_MEASUREMENT,
    STATISTICS_VERSION,
    persist_invalid_measurement_debt,
    return_integrity_summary,
    validate_return,
)


NOW = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)


class Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


def _rows(*, count: int = 45, value: float = 1.0) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    index = 0
    while len(rows) < count:
        row: dict[str, object] = {
            "family": "PUMP_AMM",
            "surface": "SOLANA",
            "token_mint": f"mint-{index}",
            "source_signature": f"sig-{index}",
            "lifecycle": "post_graduation",
            "risk_signature": "clean",
            "risk_severity": 0.0,
            "measurement_epoch": MEASUREMENT_EPOCH,
            "execution_model_epoch": "v51-execution-model-20260905-1",
            "settled_at": (NOW - timedelta(hours=30) + timedelta(minutes=index)).isoformat(),
            "net_return": value,
        }
        index += 1
        if evidence_partition(event_cluster_id(row, family="PUMP_AMM")) == "discovery":
            continue
        rows.append(row)
    return rows


def _promotion(rows: list[dict[str, object]]) -> dict[str, object]:
    holdout = 0
    validation = 0
    for row in rows:
        partition = evidence_partition(event_cluster_id(row, family="PUMP_AMM"))
        holdout += int(partition == "holdout")
        validation += int(partition == "validation")
    return {
        "measurement_epoch": MEASUREMENT_EPOCH,
        "promotion_eligible_measurement": True,
        "families": {
            "PUMP_AMM": {
                "promotion_claim_valid": True,
                "legacy_pre103_promotion_claim_valid": True,
                "independent_event_cluster_count": len(rows),
                "validation_cluster_count": validation,
                "holdout_cluster_count": holdout,
            }
        },
    }


def _compose(rows: list[dict[str, object]]) -> dict[str, object]:
    return compose_phase14_profitability_certification(
        rows,
        promotion_certification=_promotion(rows),
        candidate_coverage={
            "coverage_complete": True,
            "coverage_debt_count": 0,
            "proof_state": "confirmed",
            "measurement_epoch": MEASUREMENT_EPOCH,
        },
        forward_certification={
            "checks": {
                "35_exact_live_release": {"pass": True},
                "41_current_release_attestation": {"pass": True},
            }
        },
        operations_proof={
            "continuity": {
                "continuity_epoch": authority()["economic_freeze_epoch"],
                "process_started_at": (
                    NOW - timedelta(seconds=MIN_CONTINUOUS_PRODUCTION_SECONDS + 60.0)
                ).isoformat(),
            },
            "backpressure": {"healthy": True},
        },
        now=NOW,
    )


def test_103_exact_total_loss_is_valid_and_preserved_in_core_statistics() -> None:
    validated = validate_return(-1.0)
    assert validated.validity is True
    assert validated.normalized_fraction == -1.0

    profile = robust_profile([-1.0, 0.5, 1.0], bootstrap_samples=80)
    assert profile["sample_count"] == 3
    assert profile["mean_return"] == pytest.approx(1.0 / 6.0)
    assert profile["expected_shortfall_20"] == -1.0


def test_103_repeated_total_losses_and_top3_removal_keep_minus_100pct() -> None:
    repeated = robust_profile([-1.0, -1.0, -1.0], bootstrap_samples=80)
    assert repeated["sample_count"] == 3
    assert repeated["mean_return"] == -1.0
    assert repeated["median_return"] == -1.0

    values = [8.0, 4.0, 2.0, -1.0, -1.0]
    remaining = sorted(values, reverse=True)[3:]
    removed = robust_profile(remaining, bootstrap_samples=80)
    assert removed["sample_count"] == 2
    assert removed["mean_return"] == -1.0
    assert removed["expected_shortfall_20"] == -1.0


def test_103_total_loss_survives_stress_and_hazard_clustering() -> None:
    scenario = {
        "extra_latency_seconds": 10.0,
        "extra_round_trip_cost_fraction": 0.05,
        "adverse_selection_fraction": 0.05,
        "failure_probability": 0.10,
    }
    stressed = stress_returns([-1.0], scenario)
    assert len(stressed) == 1
    assert math.isfinite(stressed[0])
    assert stressed[0] <= 0.0

    row = {
        "family": "PUMP_AMM",
        "token_mint": "hazard-total-loss",
        "source_signature": "hazard-sig",
        "lifecycle": "post_graduation",
        "risk_signature": "creator_distributing",
        "risk_severity": 0.50,
        "net_return": -1.0,
    }
    clusters = cluster_economic_rows([row], family="PUMP_AMM")
    assert len(clusters) == 1
    assert clusters[0]["net_return"] == -1.0
    assert clusters[0]["cluster_valid_economic_measurement_count"] == 1


def test_103_total_loss_can_be_worst_locked_holdout_observation() -> None:
    rows = _rows()
    holdout = next(
        row for row in rows
        if evidence_partition(event_cluster_id(row, family="PUMP_AMM")) == "holdout"
    )
    holdout["net_return"] = -1.0
    proof = _compose(rows)
    family = proof["families"]["PUMP_AMM"]
    profile = family["98_locked_holdout_profitability"]["holdout_profile"]
    holdout_n = family["98_locked_holdout_profitability"]["holdout_cluster_count"]
    assert profile["sample_count"] == holdout_n
    assert profile["mean_return"] == pytest.approx((holdout_n - 2.0) / holdout_n)
    assert profile["mean_return"] < 1.0


def test_104_invalid_returns_are_explicit_debt_never_zero_imputation() -> None:
    cases = (
        (None, "missing_return"),
        ("not-a-number", "malformed_return"),
        (float("nan"), "non_finite_return"),
        (float("inf"), "non_finite_return"),
        (-1.000001, "return_below_total_loss_bound"),
    )
    for raw, reason in cases:
        result = validate_return(raw)
        assert result.validity is False
        assert result.normalized_fraction is None
        assert result.invalid_reason == reason
        assert result.state == INVALID_ECONOMIC_MEASUREMENT

    rows = [
        {"surface": "SOLANA", "source_signature": "valid", "net_return": 0.25},
        {"surface": "SOLANA", "source_signature": "bad", "net_return": "bad"},
    ]
    integrity = return_integrity_summary(rows)
    assert integrity["raw_measurement_count"] == 2
    assert integrity["valid_economic_measurement_count"] == 1
    assert integrity["measurement_debt_count"] == 1
    assert integrity["proof_eligible"] is False
    assert integrity["no_imputation"] is True


def test_104_invalid_reason_is_persisted_and_invalid_row_blocks_phase14() -> None:
    store = Store()
    rows = [
        {
            "surface": "SOLANA",
            "family": "PUMP_AMM",
            "source_signature": "bad-return",
            "net_return": "broken",
        }
    ]
    persisted = persist_invalid_measurement_debt(store, rows)
    assert persisted["measurement_debt_count"] == 1
    with store._lock:
        debt = store.db.execute(
            "SELECT invalid_reason,paper_only,live_money_authority FROM v51_invalid_economic_measurements"
        ).fetchone()
    assert debt["invalid_reason"] == "malformed_return"
    assert debt["paper_only"] == 1
    assert debt["live_money_authority"] == 0

    strong = _rows()
    strong.append(
        {
            "family": "PUMP_AMM",
            "surface": "SOLANA",
            "token_mint": "bad-return-token",
            "source_signature": "bad-return-proof",
            "lifecycle": "post_graduation",
            "risk_signature": "clean",
            "risk_severity": 0.0,
            "net_return": "broken",
            "settled_at": NOW.isoformat(),
        }
    )
    proof = _compose(strong)
    family = proof["families"]["PUMP_AMM"]
    assert family["economic_measurement_integrity"]["measurement_debt_count"] == 1
    assert family["economic_measurement_integrity"]["proof_eligible"] is False
    assert "invalid_economic_measurement_integrity_threshold_exceeded" in family["blockers"]
    assert family["production_proven"] is False
    assert proof["classification"] == CLASS_INSUFFICIENT_EVIDENCE


def test_105_contract_carries_required_lineage_fields() -> None:
    result = validate_return(
        -1.0,
        source_surface="SOLANA",
        source_signature="signature-1",
        measurement_epoch="measurement-1",
        execution_model_epoch="execution-1",
    ).to_dict()
    assert result["raw_value"] == -1.0
    assert result["normalized_fraction"] == -1.0
    assert result["validity"] is True
    assert result["invalid_reason"] is None
    assert result["source_surface"] == "SOLANA"
    assert result["source_signature"] == "signature-1"
    assert result["measurement_epoch"] == "measurement-1"
    assert result["execution_model_epoch"] == "execution-1"


def test_105_named_statistical_surfaces_import_canonical_validator() -> None:
    root = Path(__file__).resolve().parents[1] / "src" / "solana_roi"
    surfaces = (
        "v51_economic_core.py",
        "v51_phase14_profitability_certification.py",
        "v51_evidence_analytics.py",
        "cross_regime_paper_allocator.py",
        "v51_latency_challenger.py",
        "wallet_venue_lifecycle_research.py",
    )
    for name in surfaces:
        text = (root / name).read_text(encoding="utf-8")
        assert "v51_return_validation" in text, name


def test_106_statistics_v2_publishes_immutable_before_after_reconciliation() -> None:
    rows = _rows()
    rows[0]["net_return"] = -1.0
    proof = _compose(rows)
    migration = proof["103_108_statistics_migration"]
    family = proof["families"]["PUMP_AMM"]
    rebuild = family["statistics_rebuild"]
    assert proof["statistics_version"] == STATISTICS_VERSION
    assert migration["proof_statistics_version"] == STATISTICS_VERSION
    assert migration["newly_included_exact_total_loss_outcome_count"] == 1
    assert rebuild["recomputed_from_immutable_outcomes"] is True
    assert rebuild["mutates_source_outcomes"] is False
    assert "before_family_statistics" in rebuild
    assert "after_family_statistics" in rebuild
    assert "before_promotion_state" in rebuild
    assert "after_promotion_state" in rebuild
    assert "before_phase14_classification" in rebuild
    assert "after_phase14_classification" in rebuild


def test_107_robust_bootstrap_metrics_coexist_with_legacy_normal_intervals() -> None:
    values = [4.0, 1.2, 0.4, 0.1, -0.2, -0.7, -1.0] * 5
    cluster_ids = [f"event-{index // 5}" for index in range(len(values))]
    first = robust_profile(values, cluster_ids=cluster_ids, bootstrap_samples=120)
    second = robust_profile(values, cluster_ids=cluster_ids, bootstrap_samples=120)
    assert first["mean_return_ci95_lower"] is not None
    assert first["mean_return_ci95_upper"] is not None
    assert first["mean_return_bootstrap_ci95_lower"] is not None
    assert first["median_return_bootstrap_ci95_lower"] is not None
    assert first["expected_log_growth_bootstrap_ci95_lower"] is not None
    assert first["expected_shortfall_20_bootstrap_ci95_lower"] is not None
    assert first["lower_confidence_expected_log_growth"] == first["expected_log_growth_bootstrap_ci95_lower"]
    assert first["remove_top_1_expected_log_growth_bootstrap_ci95_lower"] is not None
    assert first["remove_top_3_expected_log_growth_bootstrap_ci95_lower"] is not None
    assert first["legacy_normal_confidence_retained_for_migration"] is True
    assert first["bootstrap_unit"] == "token_event_cluster"
    assert first["mean_return_bootstrap_ci95_lower"] == second["mean_return_bootstrap_ci95_lower"]


def test_108_phase14_selects_fraction_on_validation_and_never_reoptimizes_holdout() -> None:
    proof = _compose(_rows())
    locked = proof["families"]["PUMP_AMM"]["98_locked_holdout_profitability"]
    assert locked["selected_fraction_source"] == "validation"
    assert locked["holdout_fraction_reoptimized"] is False
    assert locked["holdout_profile"]["fraction_selection_mode"] == "preselected_fixed_fraction"
    assert locked["holdout_profile"]["best_fraction"] == locked["selected_fraction"]
    assert locked["validation_profile"]["best_fraction"] == locked["selected_fraction"]


def test_108_fixed_fraction_profile_cannot_optimize_on_holdout_sample() -> None:
    validation = robust_profile([0.05] * 30, bootstrap_samples=80)
    selected = validation["best_fraction"]
    holdout = [4.0, -0.95, -0.95, -0.95, -0.95, -0.95] * 5
    locked = robust_profile(holdout, fixed_fraction=selected, bootstrap_samples=80)
    assert locked["best_fraction"] == selected
    assert locked["fraction_selection_mode"] == "preselected_fixed_fraction"


def test_103_108_preserve_frozen_v51_safety_and_20_second_ceiling() -> None:
    spec = authority()
    assert spec["execution"]["latency_hard_max_seconds"] == 20.0
    assert spec["paper_only"] is True
    assert spec["live_money_authority"] is False
    assert spec["signing_available"] is False
    assert spec["transaction_submission_available"] is False
