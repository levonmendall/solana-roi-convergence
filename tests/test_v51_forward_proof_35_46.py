from __future__ import annotations

from solana_roi.strategy_v51_authority import authority
from solana_roi.v51_forward_certification import (
    CERTIFICATION_VERSION,
    compose_forward_certification,
)


RELEASE = "a" * 40


def _unified() -> dict:
    plane = {
        "runtime_ready": True,
        "blockers": [],
        "all_regimes_e2e_achievable": True,
        "all_regimes_e2e_proven": False,
    }
    return {
        "release_commit": RELEASE,
        "solana": dict(plane),
        "fomo": dict(plane),
        "robinhood": dict(plane),
        "overall": {
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        },
    }


def _evidence() -> dict:
    return {
        "forward_proof_slo": {
            "proof_state": "confirmed",
            "stage_events_last_60m": 8,
            "coverage_debt_count": 0,
        },
        "release_attestation": {
            "release_commit": RELEASE,
            "attested": True,
        },
        "promotion_certification": {
            "families": {
                "PUMP_AMM": {"promotion_claim_valid": True},
                "FOMO_HAZARD": {"promotion_claim_valid": False},
            }
        },
        "rejected_counterfactuals": {
            "rejected_candidate_count": 2,
            "resolved_count": 2,
            "pending_count": 0,
            "resolved_positive_count": 1,
        },
        "hazard_calibration": {
            "changes_current_hazard_multipliers": False,
            "bins": {
                "clean": {"settled_entered_count": 2, "rejected_resolved_count": 1},
                "high": {"settled_entered_count": 1, "rejected_resolved_count": 0},
            },
        },
        "cross_family_correlation": {
            "pairs": {
                "PUMP_AMM|FOMO_HAZARD": {
                    "mature": True,
                    "pearson_correlation": 0.2,
                }
            }
        },
        "maturity_allocation_proof": {
            "families": {
                "PUMP_AMM": {"future_50pct_eligibility": True},
                "FOMO_HAZARD": {"future_50pct_eligibility": False},
            }
        },
        "portfolio_reconciliation": {
            "family_navs_are_not_summed_as_independent_capital": True,
            "audit_epoch_portfolio": {"overlapping_positions_share_one_capital_base": True},
            "promotion_compatible_portfolio": {"overlapping_positions_share_one_capital_base": True},
        },
    }


def _cert(unified: dict | None = None, evidence: dict | None = None, expected: str = RELEASE) -> dict:
    return compose_forward_certification(
        unified_status=unified or _unified(),
        evidence=evidence or _evidence(),
        expected_release_commit=expected,
    )


def test_35_exact_release_binding_fails_closed_on_sha_mismatch() -> None:
    result = _cert(expected="b" * 40)
    assert result["checks"]["35_exact_live_release"]["pass"] is False
    assert result["state"] == "release_mismatch"
    assert result["system_forward_certified"] is False


def test_36_paper_only_boundary_is_required_and_cannot_expose_execution() -> None:
    unified = _unified()
    unified["overall"]["signing_available"] = True
    result = _cert(unified=unified)
    assert result["checks"]["36_paper_only_safety_boundary"]["pass"] is False
    assert result["state"] == "safety_boundary_failed"
    assert result["live_money_authority"] is False
    assert result["transaction_submission_available"] is False


def test_37_39_each_transport_has_an_independent_fail_closed_gate() -> None:
    for surface, key in (
        ("solana", "37_solana_transport"),
        ("fomo", "38_fomo_transport"),
        ("robinhood", "39_robinhood_transport"),
    ):
        unified = _unified()
        unified[surface]["runtime_ready"] = False
        unified[surface]["blockers"] = [f"{surface}_not_ready"]
        result = _cert(unified=unified)
        assert result["checks"][key]["ready"] is False
        assert result["state"] == "transport_degraded"
        assert f"{surface}_transport_not_ready" in result["blockers"]


def test_40_no_recent_candidate_flow_is_evidence_debt_not_transport_failure() -> None:
    evidence = _evidence()
    evidence["forward_proof_slo"]["stage_events_last_60m"] = 0
    result = _cert(evidence=evidence)
    assert result["checks"]["40_real_forward_candidate_flow"]["pass"] is False
    assert result["hard_operational_gates_ok"] is True
    assert result["state"] == "collecting_forward_evidence"


def test_41_current_release_must_earn_live_attestation() -> None:
    evidence = _evidence()
    evidence["release_attestation"]["attested"] = False
    result = _cert(evidence=evidence)
    assert result["checks"]["41_current_release_attestation"]["pass"] is False
    assert result["system_forward_certified"] is False


def test_42_only_existing_validation_holdout_promotion_claims_are_consumed() -> None:
    result = _cert()
    check = result["checks"]["42_validation_holdout_family_economics"]
    assert check["pass"] is True
    assert check["promotion_eligible_families"] == ["PUMP_AMM"]
    assert check["active_frozen_family_cap"] == authority()["allocation"]["immature_family_max_weight"]
    assert result["changes_economic_thresholds"] is False


def test_43_unresolved_rejected_opportunities_keep_forward_certificate_collecting() -> None:
    evidence = _evidence()
    evidence["rejected_counterfactuals"]["pending_count"] = 1
    evidence["rejected_counterfactuals"]["resolved_count"] = 1
    result = _cert(evidence=evidence)
    check = result["checks"]["43_rejected_opportunity_counterfactuals"]
    assert check["pass"] is False
    assert check["retrospective_entry_authority"] is False
    assert result["state"] == "collecting_forward_evidence"


def test_44_hazard_calibration_remains_diagnostic_and_non_authoritative() -> None:
    result = _cert()
    check = result["checks"]["44_hazard_calibration"]
    assert check["pass"] is True
    assert check["observation_count"] == 4
    assert check["diagnostic_only"] is True
    assert result["hazard_calibration_is_diagnostic_only"] is True


def test_45_correlation_maturity_does_not_raise_the_active_frozen_cap() -> None:
    result = _cert()
    check = result["checks"]["45_correlation_and_one_capital_base"]
    spec = authority()
    assert check["pass"] is True
    assert check["mature_correlation_pair_count"] == 1
    assert check["future_mature_scaling_eligible_families"] == ["PUMP_AMM"]
    assert check["active_frozen_family_cap"] == spec["allocation"]["immature_family_max_weight"]
    assert check["permanent_authority_ceiling"] == spec["allocation"]["permanent_family_max_weight"]
    assert check["active_cap_changed"] is False


def test_45_one_capital_base_is_a_hard_measurement_gate() -> None:
    evidence = _evidence()
    evidence["portfolio_reconciliation"]["promotion_compatible_portfolio"][
        "overlapping_positions_share_one_capital_base"
    ] = False
    result = _cert(evidence=evidence)
    assert result["checks"]["45_correlation_and_one_capital_base"]["pass"] is False
    assert result["state"] == "measurement_degraded"


def test_46_final_certificate_requires_hard_gates_and_real_family_claim() -> None:
    result = _cert()
    assert result["certification_version"] == CERTIFICATION_VERSION
    assert result["checks"]["46_final_forward_certification"]["pass"] is True
    assert result["system_forward_certified"] is True
    assert result["promotion_eligible_families_under_existing_v51_claims"] == ["PUMP_AMM"]
    assert result["changes_strategy_authority"] is False
    assert result["paper_only"] is True
    assert result["live_money_authority"] is False


def test_46_correlation_is_required_only_for_future_mature_scaling_not_current_cap() -> None:
    evidence = _evidence()
    evidence["cross_family_correlation"]["pairs"] = {}
    evidence["maturity_allocation_proof"]["families"]["PUMP_AMM"]["future_50pct_eligibility"] = False
    result = _cert(evidence=evidence)
    assert result["system_forward_certified"] is True
    assert result["future_mature_scaling_eligible_families"] == []
    assert result["correlation_maturity_is_not_required_for_current_frozen_cap"] is True
