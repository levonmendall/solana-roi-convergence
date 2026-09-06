from __future__ import annotations

from datetime import datetime, timedelta, timezone

from solana_roi.strategy_v51_authority import authority
from solana_roi.v51_measurement_integrity import MEASUREMENT_EPOCH
from solana_roi.v51_phase14_profitability_certification import (
    CLASS_ECONOMICALLY_PROMISING,
    CLASS_INSUFFICIENT_EVIDENCE,
    CLASS_PRODUCTION_PROVEN,
    MIN_CONTINUOUS_PRODUCTION_SECONDS,
    compose_phase14_profitability_certification,
)
from solana_roi.v51_promotion_proof import evidence_partition, event_cluster_id


NOW = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)


def _rows(*, family: str = "PUMP_AMM", non_discovery_count: int = 40, mode: str = "strong") -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    index = 0
    while len(rows) < non_discovery_count:
        row: dict[str, object] = {
            "family": family,
            "token_mint": f"mint-{family}-{index}",
            "source_signature": f"sig-{family}-{index}",
            "lifecycle": "post_graduation",
            "risk_signature": "clean",
            "risk_severity": 0.0,
            "settled_at": (NOW - timedelta(hours=30) + timedelta(minutes=index)).isoformat(),
        }
        partition = evidence_partition(event_cluster_id(row, family=family))
        index += 1
        if partition == "discovery":
            continue
        if mode == "strong":
            value = 1.0
        elif mode == "stress_fails":
            value = 0.20
        elif mode == "holdout_fails":
            value = -0.10 if partition == "holdout" else 1.0
        elif mode == "single_winner":
            value = 20.0 if not rows else -0.02
        else:
            raise AssertionError(mode)
        row["net_return"] = value
        rows.append(row)
    return rows


def _promotion(rows: list[dict[str, object]], *, claim: bool = True) -> dict[str, object]:
    family = str(rows[0]["family"])
    holdout = 0
    validation = 0
    for row in rows:
        partition = evidence_partition(event_cluster_id(row, family=family))
        holdout += int(partition == "holdout")
        validation += int(partition == "validation")
    return {
        "measurement_epoch": MEASUREMENT_EPOCH,
        "promotion_eligible_measurement": True,
        "families": {
            family: {
                "promotion_claim_valid": claim,
                "independent_event_cluster_count": len(rows),
                "validation_cluster_count": validation,
                "holdout_cluster_count": holdout,
            }
        },
    }


def _coverage(*, complete: bool = True, epoch: str = MEASUREMENT_EPOCH) -> dict[str, object]:
    return {
        "coverage_complete": complete,
        "coverage_debt_count": 0 if complete else 1,
        "proof_state": "confirmed" if complete else "partial",
        "measurement_epoch": epoch,
    }


def _forward(*, release_pass: bool = True, attestation_pass: bool = True) -> dict[str, object]:
    return {
        "checks": {
            "35_exact_live_release": {"pass": release_pass},
            "41_current_release_attestation": {"pass": attestation_pass},
        }
    }


def _operations(*, uptime_seconds: float = MIN_CONTINUOUS_PRODUCTION_SECONDS + 60.0, healthy: bool = True) -> dict[str, object]:
    started = NOW - timedelta(seconds=uptime_seconds)
    return {
        "continuity": {
            "continuity_epoch": authority()["economic_freeze_epoch"],
            "process_started_at": started.isoformat(),
        },
        "backpressure": {"healthy": healthy},
    }


def _compose(
    rows: list[dict[str, object]],
    *,
    coverage: dict[str, object] | None = None,
    promotion: dict[str, object] | None = None,
    forward: dict[str, object] | None = None,
    operations: dict[str, object] | None = None,
) -> dict[str, object]:
    return compose_phase14_profitability_certification(
        rows,
        promotion_certification=promotion or _promotion(rows),
        candidate_coverage=coverage or _coverage(),
        forward_certification=forward or _forward(),
        operations_proof=operations or _operations(),
        now=NOW,
    )


def test_95_each_family_matures_independently() -> None:
    mature = _rows(family="PUMP_AMM", non_discovery_count=40)
    immature = _rows(family="RAYDIUM", non_discovery_count=8)
    rows = [*mature, *immature]
    promotion = {
        "measurement_epoch": MEASUREMENT_EPOCH,
        "promotion_eligible_measurement": True,
        "families": {
            "PUMP_AMM": {"promotion_claim_valid": True},
            "RAYDIUM": {"promotion_claim_valid": True},
        },
    }
    proof = _compose(rows, promotion=promotion)
    assert proof["families"]["PUMP_AMM"]["95_forward_family_maturity"]["pass"] is True
    assert proof["families"]["RAYDIUM"]["95_forward_family_maturity"]["pass"] is False
    assert proof["families"]["RAYDIUM"]["production_proven"] is False


def test_96_single_winner_cannot_carry_final_certification() -> None:
    rows = _rows(mode="single_winner")
    proof = _compose(rows)
    family = proof["families"]["PUMP_AMM"]
    assert family["96_top_winner_removal"]["pass"] is False
    assert family["96_top_winner_removal"]["remove_top_1_pass"] is False
    assert family["production_proven"] is False
    assert proof["classification"] == CLASS_INSUFFICIENT_EVIDENCE


def test_97_all_frozen_execution_stresses_are_formal_gates() -> None:
    rows = _rows(mode="stress_fails")
    proof = _compose(rows)
    family = proof["families"]["PUMP_AMM"]
    assert family["97_stressed_profitability"]["pass"] is False
    assert family["97_stressed_profitability"]["scenarios"]["severe"]["pass"] is False
    assert family["production_proven"] is False


def test_98_losing_locked_holdout_blocks_proven_label() -> None:
    rows = _rows(mode="holdout_fails")
    proof = _compose(rows)
    family = proof["families"]["PUMP_AMM"]
    assert family["98_locked_holdout_profitability"]["holdout_cluster_count"] > 0
    assert family["98_locked_holdout_profitability"]["pass"] is False
    assert family["production_proven"] is False


def test_99_economic_success_with_incomplete_coverage_is_only_promising() -> None:
    rows = _rows()
    proof = _compose(rows, coverage=_coverage(complete=False))
    assert proof["99_opportunity_coverage"]["pass"] is False
    assert proof["economically_promising"] is True
    assert proof["production_proven"] is False
    assert proof["classification"] == CLASS_ECONOMICALLY_PROMISING


def test_100_invalid_measurement_epoch_blocks_production_proof() -> None:
    rows = _rows()
    bad_coverage = _coverage(epoch="wrong-epoch")
    proof = _compose(rows, coverage=bad_coverage)
    assert proof["100_measurement_epoch_validity"]["pass"] is False
    assert proof["economically_promising"] is True
    assert proof["production_proven"] is False


def test_101_requires_24_hours_current_uninterrupted_production_runtime() -> None:
    rows = _rows()
    proof = _compose(rows, operations=_operations(uptime_seconds=3600.0))
    continuity = proof["101_operational_continuity"]
    assert continuity["minimum_continuous_production_hours"] == 24.0
    assert continuity["current_process_uptime_not_accumulated_across_restarts"] is True
    assert continuity["pass"] is False
    assert proof["classification"] == CLASS_ECONOMICALLY_PROMISING


def test_102_all_gates_publish_production_proven_separately_from_promising() -> None:
    rows = _rows()
    proof = _compose(rows)
    assert proof["economically_promising"] is True
    assert proof["production_proven"] is True
    assert proof["classification"] == CLASS_PRODUCTION_PROVEN
    assert proof["economically_promising_families"] == ["PUMP_AMM"]
    assert proof["production_proven_families"] == ["PUMP_AMM"]
    assert proof["102_classification_contract"]["economic_success_does_not_imply_production_proof"] is True


def test_phase14_preserves_frozen_v51_authority_and_paper_only_boundary() -> None:
    spec = authority()
    assert spec["authority_id"] == "roi-convergence-v5.1-consolidated-proof-1"
    assert spec["economic_freeze_epoch"] == "v51-consolidated-proof-20260905"
    assert spec["execution"]["latency_hard_max_seconds"] == 20.0
    assert spec["paper_only"] is True
    assert spec["live_money_authority"] is False
    assert spec["signing_available"] is False
    assert spec["transaction_submission_available"] is False

    proof = _compose(_rows())
    assert proof["changes_strategy_authority"] is False
    assert proof["changes_economic_thresholds"] is False
    assert proof["changes_selection_authority"] is False
    assert proof["changes_sizing_authority"] is False
    assert proof["changes_exit_authority"] is False
    assert proof["changes_promotion_economics"] is False
    assert proof["paper_only"] is True
    assert proof["live_money_authority"] is False
    assert proof["signing_available"] is False
    assert proof["transaction_submission_available"] is False


def test_phase14_production_composition_exposes_final_certification_route() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    production = (root / "src" / "solana_roi" / "v51_production_authority.py").read_text(encoding="utf-8")
    api = (root / "src" / "solana_roi" / "v51_phase14_api.py").read_text(encoding="utf-8")
    assert "install_phase14_profitability_certification(" in production
    assert '"/v1/strategy/final-certification"' in api
    assert "roi_v51_phase14_95_102" in production
