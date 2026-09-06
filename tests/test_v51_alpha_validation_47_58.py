from __future__ import annotations

from copy import deepcopy

from solana_roi.strategy_v51_authority import authority
from solana_roi.v51_alpha_validation import (
    ALPHA_CERTIFICATE_VERSION,
    compose_alpha_certificate,
)


def _family(*, promoted: bool = True) -> dict:
    return {
        "raw_outcome_count": 50,
        "independent_event_cluster_count": 30,
        "minimum_independent_outcomes": 20,
        "validation_cluster_count": 15,
        "holdout_cluster_count": 15,
        "fdr_accepted": True,
        "promotion_claim_valid": promoted,
        "robust_profile": {
            "best_expected_log_growth": 0.03 if promoted else 0.0,
        },
        "promotion_kill_profile": {"killed": False},
        "execution_stress": {
            "material": {"best_expected_log_growth": 0.01 if promoted else -0.01}
        },
        "capital_efficiency_score": 0.02 if promoted else 0.0,
    }


def _evidence() -> dict:
    return {
        "promotion_certification": {
            "families": {
                "PUMP_AMM": _family(promoted=True),
                "FOMO_HAZARD": _family(promoted=False),
            }
        },
        "rejected_counterfactuals": {
            "rejected_candidate_count": 8,
            "resolved_count": 8,
            "pending_count": 0,
            "resolved_positive_count": 2,
        },
        "maturity_allocation_proof": {
            "families": {
                "PUMP_AMM": {"future_50pct_eligibility": True},
                "FOMO_HAZARD": {"future_50pct_eligibility": False},
            }
        },
        "portfolio_reconciliation": {
            "family_navs_are_not_summed_as_independent_capital": True,
        },
    }


def _forward() -> dict:
    return {
        "release_commit": "a" * 40,
        "state": "certified",
        "hard_operational_gates_ok": True,
        "blockers": [],
    }


def _candidate() -> dict:
    return {
        "candidate_flow_complete": True,
        "canonical_candidate_count": 40,
        "recent_stage_events_last_60m": 20,
        "coverage_debt_count": 0,
        "has_prospective_candidates": True,
        "by_surface": {},
    }


def _settlement() -> dict:
    return {
        "settlement_proof_complete": True,
        "settlement_lineage_complete": True,
        "settlement_evidence_present": True,
        "paper_entry_count": 10,
        "settled_entry_count": 10,
        "pending_settlement_count": 0,
        "by_surface": {},
    }


def _wallet() -> dict:
    return {
        "ranking_unit": "forward_percentage_roi_residual_not_dollar_profit",
        "assignment_scope": "family_x_context_x_entity; no_cross_context_success_transfer",
        "challengers": [],
        "positive_forward_identity_challengers": [],
        "automatic_strategy_mutation": False,
    }


def _capital() -> dict:
    return {
        "research_and_promoted_capital_reported_separately": True,
        "promoted_families": ["PUMP_AMM"],
        "promoted_strategy_nav": {"portfolio_roi_fraction": 0.15},
        "research_probe_nav": {"portfolio_roi_fraction": -0.03},
        "combined_audit_nav": {"portfolio_roi_fraction": 0.10},
        "active_allocation_changed": False,
    }


def _cert(**overrides) -> dict:
    values = {
        "forward": _forward(),
        "evidence": _evidence(),
        "candidate_flow": _candidate(),
        "settlement": _settlement(),
        "wallet_entity": _wallet(),
        "capital_views": _capital(),
    }
    values.update(overrides)
    return compose_alpha_certificate(**values)


def test_47_transport_continuity_is_a_hard_gate() -> None:
    forward = _forward()
    forward["hard_operational_gates_ok"] = False
    forward["state"] = "transport_degraded"
    result = _cert(forward=forward)
    assert result["checks"]["47_production_transport_continuity"]["pass"] is False
    assert result["state"] == "operationally_degraded"
    assert result["after_cost_positive_compounded_alpha_proven"] is False


def test_48_no_recent_prospective_candidate_flow_cannot_prove_alpha() -> None:
    candidate = _candidate()
    candidate["candidate_flow_complete"] = False
    candidate["recent_stage_events_last_60m"] = 0
    result = _cert(candidate_flow=candidate)
    assert result["checks"]["48_prospective_candidate_flow_completeness"]["pass"] is False
    assert result["state"] == "collecting_candidate_evidence"


def test_49_pending_rejects_remain_pending_and_have_no_retrospective_authority() -> None:
    evidence = _evidence()
    evidence["rejected_counterfactuals"]["pending_count"] = 3
    evidence["rejected_counterfactuals"]["resolved_count"] = 5
    result = _cert(evidence=evidence)
    check = result["checks"]["49_rejected_opportunity_counterfactual_resolution"]
    assert check["pass"] is False
    assert check["pending_count"] == 3
    assert check["retrospective_entry_authority"] is False
    assert result["state"] == "resolving_rejected_counterfactuals"


def test_50_unsettled_paper_entry_blocks_realized_execution_proof() -> None:
    settlement = _settlement()
    settlement.update(
        settlement_proof_complete=False,
        settlement_lineage_complete=False,
        paper_entry_count=10,
        settled_entry_count=9,
        pending_settlement_count=1,
    )
    result = _cert(settlement=settlement)
    assert result["checks"]["50_settlement_and_realized_execution_proof"]["pass"] is False
    assert result["state"] == "collecting_settlement_evidence"


def test_51_53_holdout_and_family_promotion_are_existing_claims_only() -> None:
    result = _cert()
    assert result["checks"]["51_locked_validation_holdout_accumulation"]["pass"] is True
    assert result["checks"]["52_independent_event_statistical_proof"]["pass"] is True
    assert result["checks"]["53_family_promotion_engine"]["promoted_families"] == ["PUMP_AMM"]
    assert result["checks"]["53_family_promotion_engine"]["read_only_evidence_lifecycle"] is True
    assert result["changes_strategy_authority"] is False


def test_54_wallet_entity_intelligence_uses_percentage_roi_and_does_not_auto_mutate() -> None:
    result = _cert()
    check = result["checks"]["54_adaptive_wallet_entity_intelligence"]
    assert check["pass"] is True
    assert check["ranking_unit"] == "forward_percentage_roi_residual_not_dollar_profit"
    assert check["automatic_strategy_mutation"] is False


def test_55_research_and_promoted_nav_are_separate() -> None:
    result = _cert()
    check = result["checks"]["55_research_vs_promoted_capital"]
    assert check["pass"] is True
    assert check["promoted_strategy_nav"]["portfolio_roi_fraction"] == 0.15
    assert check["research_probe_nav"]["portfolio_roi_fraction"] == -0.03
    assert check["active_allocation_changed"] is False


def test_56_future_scaling_does_not_change_active_frozen_cap() -> None:
    result = _cert()
    check = result["checks"]["56_portfolio_level_scaling_proof"]
    spec = authority()
    assert check["pass"] is True
    assert check["future_mature_scaling_eligible_families"] == ["PUMP_AMM"]
    assert check["active_frozen_family_cap"] == spec["allocation"]["immature_family_max_weight"]
    assert check["permanent_authority_ceiling"] == spec["allocation"]["permanent_family_max_weight"]
    assert check["active_cap_changed"] is False


def test_57_negative_mature_family_is_degrading_without_mutating_strategy() -> None:
    evidence = _evidence()
    proof = evidence["promotion_certification"]["families"]["FOMO_HAZARD"]
    proof["independent_event_cluster_count"] = 30
    proof["minimum_independent_outcomes"] = 20
    proof["validation_cluster_count"] = 15
    proof["holdout_cluster_count"] = 15
    proof["robust_profile"]["best_expected_log_growth"] = -0.02
    result = _cert(evidence=evidence)
    family = result["checks"]["57_promotion_degradation_kill_logic"]["families"]["FOMO_HAZARD"]
    assert family["state"] == "degrading"
    assert family["changes_strategy_authority"] is False


def test_58_forward_alpha_certificate_requires_all_real_evidence_gates() -> None:
    result = _cert()
    assert result["alpha_certificate_version"] == ALPHA_CERTIFICATE_VERSION
    assert result["checks"]["58_forward_alpha_certificate"]["pass"] is True
    assert result["state"] == "forward_alpha_proven"
    assert result["after_cost_positive_compounded_alpha_proven"] is True
    assert result["paper_only"] is True
    assert result["live_money_authority"] is False
    assert result["signing_available"] is False
    assert result["transaction_submission_available"] is False


def test_58_no_family_promotion_remains_collecting_not_proven() -> None:
    evidence = deepcopy(_evidence())
    evidence["promotion_certification"]["families"]["PUMP_AMM"] = _family(promoted=False)
    capital = _capital()
    capital["promoted_strategy_nav"] = {"portfolio_roi_fraction": 0.0}
    capital["promoted_families"] = []
    result = _cert(evidence=evidence, capital_views=capital)
    assert result["after_cost_positive_compounded_alpha_proven"] is False
    assert result["state"] == "collecting_validation_holdout"
    assert "no_family_has_locked_holdout_promotion_claim" in result["blockers"]
