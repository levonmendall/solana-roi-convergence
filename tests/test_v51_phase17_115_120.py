from __future__ import annotations

from datetime import datetime, timedelta, timezone

from solana_roi.v51_exit_execution_terminal_fomo_followup import ACTIVE_EXECUTION_MODEL_EPOCH
from solana_roi.v51_measurement_integrity import MEASUREMENT_EPOCH
from solana_roi.v51_phase17_context_certification import (
    CONTEXT_DIMENSIONS,
    LIVE_MONEY_AUTHORITY,
    LIVE_PROMOTION_AUTHORITY,
    PAPER_ONLY,
    SIGNING_AVAILABLE,
    STRATEGY_MUTATION_AUTHORITY,
    TRANSACTION_SUBMISSION_AVAILABLE,
    build_phase17_context_certification,
    status,
)


def _row(index: int, *, lifecycle: str = "post_graduation", execution_epoch: str = ACTIVE_EXECUTION_MODEL_EPOCH) -> dict:
    return {
        "family": "PUMP_AMM",
        "surface": "SOLANA",
        "venue": "PUMP_AMM",
        "lifecycle": lifecycle,
        "regime": "continuation",
        "risk_signature": "clean",
        "risk_severity": 0.0,
        "flow_state": "organic",
        "chase_band": "0_5pct",
        "latency_band": "5_10s",
        "cost_band": "1_3pct",
        "round_trip_cost_fraction": 0.02,
        "token_mint": f"mint-{lifecycle}-{index}",
        "source_signature": f"sig-{lifecycle}-{index}",
        "settled_at": f"2026-09-05T12:{index % 60:02d}:00+00:00",
        "net_return": 2.0,
        "measurement_epoch": MEASUREMENT_EPOCH,
        "execution_model_epoch": execution_epoch,
    }


def _strong_rows(*, lifecycle: str = "post_graduation", count: int = 180) -> list[dict]:
    return [_row(index, lifecycle=lifecycle) for index in range(count)]


def _promotion() -> dict:
    return {
        "measurement_epoch": MEASUREMENT_EPOCH,
        "promotion_eligible_measurement": True,
        "families": {"PUMP_AMM": {"promotion_claim_valid": True, "state": "promoted_positive_hierarchical_edge"}},
    }


def _forward(*, solana_attested: bool = True, robinhood_ready: bool = False) -> dict:
    return {
        "system_forward_certified": bool(robinhood_ready),
        "checks": {
            "35_exact_live_release": {"pass": True},
            "36_paper_only_safety_boundary": {"pass": True},
            "37_solana_transport": {"ready": True},
            "38_fomo_transport": {"ready": True},
            "39_robinhood_transport": {"ready": robinhood_ready},
            "41_current_release_attestation": {
                "pass": bool(solana_attested and robinhood_ready),
                "attested": bool(solana_attested and robinhood_ready),
                "surfaces": {
                    "solana": {"present": True, "attested": solana_attested, "reasons": [] if solana_attested else ["solana_pending"]},
                    "fomo": {"present": True, "attested": True, "reasons": []},
                    "robinhood": {"present": True, "attested": robinhood_ready, "reasons": [] if robinhood_ready else ["robinhood_unhealthy"]},
                },
            },
        },
    }


def _coverage(*, robinhood_debt: int = 2, solana_debt: int = 0, debt_rows: list[dict] | None = None) -> dict:
    total = robinhood_debt + solana_debt
    payload = {
        "measurement_epoch": MEASUREMENT_EPOCH,
        "coverage_complete": total == 0,
        "coverage_debt_count": total,
        "proof_state": "confirmed" if total == 0 else "partial",
        "stage_summary": {
            "SOLANA": {"context": {"coverage_debt": solana_debt}},
            "ROBINHOOD_CHAIN": {"context": {"coverage_debt": robinhood_debt}},
        },
        "robinhood": {"coverage_debt_count": robinhood_debt, "coverage_complete": robinhood_debt == 0},
    }
    if debt_rows is not None:
        payload["coverage_debt_rows"] = debt_rows
    return payload


def _operations() -> dict:
    return {
        "continuity": {
            "process_started_at": (datetime.now(timezone.utc) - timedelta(days=2)).isoformat(),
            "continuity_epoch": "v51-consolidated-proof-20260905",
        },
        "backpressure": {"healthy": True},
    }


def _build(rows: list[dict], *, forward: dict | None = None, coverage: dict | None = None) -> dict:
    return build_phase17_context_certification(
        rows,
        promotion_certification=_promotion(),
        candidate_coverage=coverage or _coverage(),
        forward_certification=forward or _forward(),
        operations_proof=_operations(),
        base_family_certifications={"PUMP_AMM": {"economically_promising": True}},
    )


def _pump_context(report: dict) -> dict:
    rows = list(report["exact_context_proof_ledger"].values())
    assert len(rows) == 1
    return rows[0]


def test_115_robinhood_global_failure_does_not_block_proven_pump_amm_family() -> None:
    report = _build(_strong_rows())
    assert report["system_certification"]["pass"] is False
    assert report["system_certification"]["surface_transport"]["ROBINHOOD_CHAIN"]["ready"] is False
    assert report["family_certification"]["PUMP_AMM"]["production_proven"] is True
    context = _pump_context(report)
    assert context["production_proven"] is True
    assert context["required_surfaces"] == ["SOLANA"]
    assert context["coverage"]["relevant_coverage_debt_count"] == 0


def test_116_missing_required_solana_attestation_blocks_pump_amm_family() -> None:
    report = _build(_strong_rows(), forward=_forward(solana_attested=False))
    context = _pump_context(report)
    assert context["production_proven"] is False
    assert "required_surface_release_attestation_missing" in context["blockers"]
    assert report["family_certification"]["PUMP_AMM"]["production_proven"] is False


def test_117_118_family_sample_cannot_subsidize_underproven_exact_context() -> None:
    rows = _strong_rows(lifecycle="post_graduation", count=180)
    rows.extend(_strong_rows(lifecycle="late_decay", count=10))
    report = _build(rows)
    ledger = report["exact_context_proof_ledger"]
    assert len(ledger) == 2
    strong = next(value for value in ledger.values() if value["context"]["lifecycle"] == "post_graduation")
    weak = next(value for value in ledger.values() if value["context"]["lifecycle"] == "late_decay")
    assert strong["production_proven"] is True
    assert weak["production_proven"] is False
    assert "exact_context_evidence_maturity_not_met" in weak["blockers"]
    assert report["family_certification"]["PUMP_AMM"]["production_proven"] is False
    assert report["family_certification"]["PUMP_AMM"]["family_n_cannot_subsidize_unproven_context"] is True


def test_120_context_coverage_debt_ignores_unrelated_robinhood_but_blocks_matching_pump_debt() -> None:
    rows = _strong_rows()
    unrelated = _build(
        rows,
        coverage=_coverage(
            robinhood_debt=1,
            solana_debt=0,
            debt_rows=[{"surface": "ROBINHOOD_CHAIN", "family": "ROBINHOOD_CHAIN", "venue": "ROBINHOOD_CHAIN"}],
        ),
    )
    assert _pump_context(unrelated)["coverage"]["healthy"] is True

    matching = _build(
        rows,
        coverage=_coverage(
            robinhood_debt=0,
            solana_debt=1,
            debt_rows=[
                {
                    "surface": "SOLANA",
                    "family": "PUMP_AMM",
                    "venue": "PUMP_AMM",
                    "lifecycle": "post_graduation",
                    "regime": "continuation",
                    "risk_signature": "clean",
                }
            ],
        ),
    )
    context = _pump_context(matching)
    assert context["coverage"]["healthy"] is False
    assert context["coverage"]["relevant_coverage_debt_count"] == 1
    assert "relevant_context_coverage_debt" in context["blockers"]
    debt = matching["context_specific_coverage_debt"]
    assert debt["by_dimension"]["venue"]["PUMP_AMM"] == 1
    assert debt["by_dimension"]["family"]["PUMP_AMM"] == 1


def test_118_mixed_execution_epochs_fail_closed_without_pooling() -> None:
    rows = _strong_rows(count=180)
    for index in range(0, len(rows), 2):
        rows[index]["execution_model_epoch"] = "old-execution-model"
    report = _build(rows)
    context = _pump_context(report)
    assert context["epoch_compatibility"]["pass"] is False
    assert len(context["epoch_compatibility"]["execution_model_epochs"]) == 2
    assert "exact_context_measurement_or_execution_epoch_incompatible" in context["blockers"]
    assert report["silent_measurement_or_execution_epoch_pooling_allowed"] is False


def test_117_ledger_publishes_exact_context_dimensions_and_required_proof_metrics() -> None:
    report = _build(_strong_rows())
    context = _pump_context(report)
    assert tuple(report["context_dimensions"]) == CONTEXT_DIMENSIONS
    assert set(context["context"]) == set(CONTEXT_DIMENSIONS)
    for field in (
        "raw_n",
        "independent_n",
        "validation_n",
        "holdout_n",
        "selected_fraction",
        "log_growth",
        "robust_lower_bound",
        "es20",
        "drawdown",
        "top_winner_robustness_pass",
        "stress_result",
        "promotion_state",
        "kill_state",
    ):
        assert field in context
    assert context["validation_n"] > 0
    assert context["holdout_n"] > 0
    assert context["family_level_evidence_cannot_subsidize_this_context"] is True


def test_phase17_contract_preserves_paper_only_authority() -> None:
    contract = status()
    assert contract["repairs"] == [115, 116, 117, 118, 119, 120]
    assert PAPER_ONLY is True
    assert LIVE_MONEY_AUTHORITY is False
    assert SIGNING_AVAILABLE is False
    assert TRANSACTION_SUBMISSION_AVAILABLE is False
    assert STRATEGY_MUTATION_AUTHORITY is False
    assert LIVE_PROMOTION_AUTHORITY is False
    assert contract["retrospective_entry_authority"] is False
