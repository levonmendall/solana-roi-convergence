from __future__ import annotations

import json
from pathlib import Path

import pytest

from solana_roi.v52_lossless_candidate_accounting import (
    BATCH_VERSION,
    CANONICAL_LANES,
    CandidateAccountingRecord,
    ObservedCandidate,
    TerminalDisposition,
    attach_lossless_accounting,
    reconcile_candidates,
    safety_manifest,
)


def _observed() -> list[ObservedCandidate]:
    return [
        ObservedCandidate(candidate_id=f"{lane}-candidate", lane=lane)
        for lane in CANONICAL_LANES
    ]


def _active_records() -> list[CandidateAccountingRecord]:
    return [
        CandidateAccountingRecord.active_record(
            candidate_id=f"{lane}-candidate",
            lane=lane,
        )
        for lane in CANONICAL_LANES
    ]


def _matrix() -> dict:
    return {
        "lanes": {lane: {"synthetic": True} for lane in CANONICAL_LANES},
        "all_cases_synthetic": True,
        "certification_eligible_case_count": 0,
        "promotion_eligible_case_count": 0,
        "paper_only": True,
        "live_money_authority": False,
        "changes_strategy_authority": False,
        "changes_economic_thresholds": False,
    }


def test_all_five_lanes_accounted_pass_hard_gate() -> None:
    accounting = reconcile_candidates(_observed(), _active_records())

    assert tuple(accounting.lanes) == CANONICAL_LANES
    assert accounting.observed_candidate_count == 5
    assert accounting.active_candidate_count == 5
    assert accounting.terminal_candidate_count == 0
    assert accounting.accounted_candidate_count == 5
    assert accounting.unexplained_disappearance_count == 0
    assert accounting.unexpected_candidate_count == 0
    assert accounting.validation_errors == ()
    assert accounting.accounting_valid is True
    assert accounting.certification_ready is True


@pytest.mark.parametrize(
    "disposition",
    [
        TerminalDisposition.EXITED.value,
        TerminalDisposition.PERMANENTLY_REJECTED.value,
        TerminalDisposition.EXPIRED.value,
        TerminalDisposition.INVALIDATED.value,
    ],
)
def test_explicit_terminal_dispositions_are_accounted(disposition: str) -> None:
    observed = _observed()
    records = _active_records()
    records[0] = CandidateAccountingRecord.terminal_record(
        candidate_id=observed[0].candidate_id,
        lane=observed[0].lane,
        terminal_disposition=disposition,
    )

    accounting = reconcile_candidates(observed, records)

    assert accounting.active_candidate_count == 4
    assert accounting.terminal_candidate_count == 1
    assert accounting.accounted_candidate_count == 5
    assert accounting.unexplained_disappearance_count == 0
    assert accounting.certification_ready is True


def test_one_silent_disappearance_fails_readiness_closed() -> None:
    observed = _observed()
    records = _active_records()[1:]

    accounting = reconcile_candidates(observed, records)

    assert accounting.unexplained_disappearance_count == 1
    assert accounting.unexplained_candidate_ids == ("pump_fun-candidate",)
    assert accounting.lanes["pump_fun"].unexplained_disappearance_count == 1
    assert accounting.accounting_valid is True
    assert accounting.certification_ready is False


def test_per_lane_unexplained_counts_sum_to_aggregate() -> None:
    observed = _observed()
    records = _active_records()[2:]

    accounting = reconcile_candidates(observed, records)

    assert accounting.unexplained_disappearance_count == 2
    assert (
        sum(
            lane.unexplained_disappearance_count
            for lane in accounting.lanes.values()
        )
        == accounting.unexplained_disappearance_count
    )


def test_missing_candidate_identity_fails_closed() -> None:
    observed = _observed() + [ObservedCandidate(candidate_id="  ", lane="pump_fun")]

    accounting = reconcile_candidates(observed, _active_records())

    assert any("candidate_id_missing" in error for error in accounting.validation_errors)
    assert accounting.accounting_valid is False
    assert accounting.certification_ready is False


def test_duplicate_candidate_identity_fails_closed() -> None:
    observed = _observed() + [
        ObservedCandidate(candidate_id="pump_fun-candidate", lane="pump_fun")
    ]

    accounting = reconcile_candidates(observed, _active_records())

    assert any("duplicate_candidate_id:pump_fun-candidate" in error for error in accounting.validation_errors)
    assert accounting.accounting_valid is False
    assert accounting.certification_ready is False


def test_conflicting_candidate_lane_fails_closed() -> None:
    observed = _observed()
    records = _active_records()
    records[0] = CandidateAccountingRecord.active_record(
        "pump_fun-candidate",
        "pump_amm",
    )

    accounting = reconcile_candidates(observed, records)

    assert any("candidate_lane_conflict:pump_fun-candidate" in error for error in accounting.validation_errors)
    assert accounting.unexplained_disappearance_count == 1
    assert accounting.certification_ready is False


def test_unknown_lane_fails_closed() -> None:
    observed = _observed() + [ObservedCandidate("unknown-candidate", "not_a_lane")]

    accounting = reconcile_candidates(observed, _active_records())

    assert any("unknown_lane:not_a_lane" in error for error in accounting.validation_errors)
    assert accounting.accounting_valid is False
    assert accounting.certification_ready is False


def test_unknown_terminal_disposition_fails_closed() -> None:
    observed = _observed()
    records = _active_records()
    records[0] = CandidateAccountingRecord.terminal_record(
        "pump_fun-candidate",
        "pump_fun",
        "mystery",
    )

    accounting = reconcile_candidates(observed, records)

    assert any("unknown_terminal_disposition:mystery" in error for error in accounting.validation_errors)
    assert accounting.unexplained_disappearance_count == 1
    assert accounting.certification_ready is False


def test_active_record_cannot_also_be_terminal() -> None:
    observed = _observed()
    records = _active_records()
    records[0] = CandidateAccountingRecord(
        candidate_id="pump_fun-candidate",
        lane="pump_fun",
        active=True,
        terminal_disposition=TerminalDisposition.EXITED.value,
    )

    accounting = reconcile_candidates(observed, records)

    assert any("active_record_has_terminal_disposition" in error for error in accounting.validation_errors)
    assert accounting.unexplained_disappearance_count == 1
    assert accounting.certification_ready is False


def test_duplicate_or_conflicting_accounting_record_fails_closed() -> None:
    observed = _observed()
    duplicate = _active_records()[0]
    records = _active_records() + [duplicate]

    duplicate_result = reconcile_candidates(observed, records)
    assert any("duplicate_accounting_record:pump_fun-candidate" in error for error in duplicate_result.validation_errors)
    assert duplicate_result.certification_ready is False

    records = _active_records() + [
        CandidateAccountingRecord.terminal_record(
            "pump_fun-candidate",
            "pump_fun",
            TerminalDisposition.EXITED.value,
        )
    ]
    conflict_result = reconcile_candidates(observed, records)
    assert any("conflicting_accounting_record:pump_fun-candidate" in error for error in conflict_result.validation_errors)
    assert conflict_result.certification_ready is False


def test_unexpected_record_fails_closed_without_hiding_disappearance_count() -> None:
    records = _active_records() + [
        CandidateAccountingRecord.active_record("unexpected", "fomo")
    ]

    accounting = reconcile_candidates(_observed(), records)

    assert accounting.unexplained_disappearance_count == 0
    assert accounting.unexpected_candidate_count == 1
    assert accounting.unexpected_candidate_ids == ("unexpected",)
    assert accounting.accounting_valid is False
    assert accounting.certification_ready is False


def test_five_lane_wrapper_exposes_hard_unexplained_disappearance_gate() -> None:
    accounting = reconcile_candidates(_observed(), _active_records()[1:])

    status = attach_lossless_accounting(_matrix(), accounting)

    assert status["unexplained_disappearance_count"] == 1
    assert status["certification_ready"] is False
    assert status["v52_batch2"]["hard_readiness_gate_passed"] is False
    assert "unexplained_candidate_disappearance" in status["v52_batch2"]["readiness_blockers"]
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["changes_strategy_authority"] is False
    assert status["changes_economic_thresholds"] is False


def test_incomplete_five_lane_matrix_fails_closed() -> None:
    accounting = reconcile_candidates(_observed(), _active_records())
    matrix = _matrix()
    matrix["lanes"].pop("robinhood")

    status = attach_lossless_accounting(matrix, accounting)

    assert status["unexplained_disappearance_count"] == 0
    assert status["certification_ready"] is False
    assert "five_lane_matrix_incomplete" in status["v52_batch2"]["readiness_blockers"]


def test_safety_manifest_preserves_incumbent_and_zero_live_authority() -> None:
    manifest = safety_manifest()

    assert manifest["batch_version"] == BATCH_VERSION
    assert manifest["incumbent_remains_authoritative"] is True
    assert manifest["incumbent_authority_changed"] is False
    assert manifest["challenger_entry_authority"] is False
    assert manifest["research_only"] is True
    assert manifest["paper_only"] is True
    assert manifest["live_money_authority"] is False
    assert manifest["signing_available"] is False
    assert manifest["transaction_submission_available"] is False
    assert manifest["production_composition_hook"] is False
    assert manifest["unexplained_disappearance_hard_gate"] is True
    assert tuple(manifest["canonical_lanes"]) == CANONICAL_LANES


def test_batch2_module_remains_explicitly_test_only() -> None:
    policy_path = Path(__file__).resolve().parents[1] / "module_reachability_policy.json"
    policy = json.loads(policy_path.read_text(encoding="utf-8"))

    assert "solana_roi.v52_lossless_candidate_accounting" in policy["test_only"]
    assert policy["production_root"] == "solana_roi.production"
