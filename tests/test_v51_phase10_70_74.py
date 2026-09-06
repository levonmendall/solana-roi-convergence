from __future__ import annotations

from solana_roi.strategy_v51_authority import authority
from solana_roi.v51_dashboard import render_economic_dashboard
from solana_roi.v51_economic_core import robust_profile
from solana_roi.v51_phase10_proof import dashboard_funnel, family_proof_confidence
from solana_roi.v51_system_proof import SYSTEM_STATES, system_state


def test_70_system_state_has_exact_four_state_contract() -> None:
    assert SYSTEM_STATES == (
        "READY_FOR_FORWARD_PROOF",
        "DEGRADED",
        "INSUFFICIENT_EVIDENCE",
        "INVALID_MEASUREMENT_EPOCH",
    )
    assert system_state(
        forward_certification={"system_forward_certified": True, "hard_operational_gates_ok": True},
        coverage={}, certification={}, promotion={}
    ) == "READY_FOR_FORWARD_PROOF"
    assert system_state(
        forward_certification={"system_forward_certified": False, "hard_operational_gates_ok": True},
        coverage={}, certification={}, promotion={}
    ) == "INSUFFICIENT_EVIDENCE"
    assert system_state(
        forward_certification={"system_forward_certified": False, "hard_operational_gates_ok": False},
        coverage={}, certification={}, promotion={}
    ) == "DEGRADED"
    assert system_state(
        forward_certification={"system_forward_certified": False, "hard_operational_gates_ok": True},
        coverage={"proof_state": "invalid_measurement_epoch"}, certification={}, promotion={}
    ) == "INVALID_MEASUREMENT_EPOCH"


def test_71_72_production_has_separate_liveness_readiness_and_system_proof_routes() -> None:
    from solana_roi import production

    routes = {getattr(route, "path", None): route for route in production.app.routes}
    for path in ("/health", "/v1/liveness", "/readiness", "/v1/system-proof", "/v1/system-proof/dashboard"):
        assert path in routes
    liveness = routes["/v1/liveness"].endpoint()
    assert liveness["status"] == "ok"
    assert liveness["liveness_only"] is True
    assert liveness["runtime_or_sqlite_checked"] is False
    assert liveness["trading_research_readiness_implied"] is False
    assert liveness["readiness_path"] == "/readiness"


def test_73_dashboard_funnel_separates_operational_and_research_counts() -> None:
    local = {
        "stage_summary": {
            "SOLANA": {
                "candidate": {"complete": 5},
                "decision": {"complete": 4},
                "position": {"paper_position_authorized": 2},
                "settlement": {"complete": 1},
            },
            "FOMO": {
                "candidate": {"complete": 3},
                "decision": {"complete": 3},
                "position": {"paper_position_authorized": 1},
                "settlement": {"complete": 1},
            },
        }
    }
    merged = {"coverage_debt_count": 2}
    certification = {
        "closed_outcome_count": 4,
        "families": {
            "PUMP_AMM": {"closed_outcome_count": 3},
            "RAYDIUM": {"closed_outcome_count": 1},
        },
    }
    promotion = {
        "families": {
            "PUMP_AMM": {"promotion_claim_valid": True},
            "RAYDIUM": {"promotion_claim_valid": False},
        }
    }
    robinhood = {
        "phase9_65_69": {
            "candidate_dispositions": {
                "candidate_count": 2,
                "terminal_disposition_count": 2,
                "rejected_candidate_count": 1,
            }
        },
        "rejected_counterfactuals": {
            "rejected_candidate_count": 1,
            "resolved_positive_count": 1,
        },
    }
    funnel = dashboard_funnel(
        local_coverage=local,
        merged_coverage=merged,
        certification=certification,
        promotion=promotion,
        local_counterfactuals={"rejected_candidate_count": 3, "resolved_positive_count": 1},
        robinhood_proof=robinhood,
    )
    assert funnel["detected_opportunities"] == 10
    assert funnel["evaluated_opportunities"] == 9
    assert funnel["coverage_debt"] == 2
    assert funnel["paper_entries"] == 4
    assert funnel["settled_trades"] == 4
    assert funnel["promoted_trades"] == 3
    assert funnel["research_probes"] == 4
    assert funnel["missed_opportunities"] == 2


def test_74_family_confidence_exposes_all_required_metrics_and_real_log_growth_lcb() -> None:
    profile = robust_profile([0.10, 0.05, -0.02, 0.08, 0.03])
    assert profile["expected_log_growth_ci95_lower"] is not None
    assert profile["expected_log_growth_ci95_upper"] is not None
    certification = {
        "families": {
            "PUMP_AMM": {
                "closed_outcome_count": 5,
                "independent_event_count": 5,
                "net_roi_sum": 0.24,
                "compounded_nav_multiple": 1.012,
                "robust_profile": profile,
                "promotion_kill_profile": {"state": "mature_unproven"},
                "latency_sensitivity": {"le_2s": {"mean_return": 0.04}},
                "execution_cost_sensitivity": {"le_3pct": {"mean_return": 0.03}},
                "execution_stress": {"material": {"best_expected_log_growth": 0.001}},
            }
        }
    }
    promotion = {
        "families": {
            "PUMP_AMM": {
                "raw_outcome_count": 5,
                "independent_event_cluster_count": 4,
                "validation_cluster_count": 3,
                "holdout_cluster_count": 1,
                "minimum_independent_outcomes": 30,
                "promotion_claim_valid": False,
            }
        }
    }
    proof = family_proof_confidence(certification, promotion)["PUMP_AMM"]
    required = {
        "raw_n", "independent_n", "holdout_n", "net_roi", "compounded_nav",
        "expected_log_growth", "lcb_expected_log_growth", "es20", "max_drawdown",
        "winner_concentration", "top_1_removed", "top_3_removed", "latency_sensitivity",
        "cost_sensitivity", "stress_performance", "promotion_state",
    }
    assert required.issubset(proof)
    assert proof["raw_n"] == 5
    assert proof["independent_n"] == 4
    assert proof["holdout_n"] == 1
    assert proof["lcb_expected_log_growth"] == profile["expected_log_growth_ci95_lower"]

    html = render_economic_dashboard(
        certification,
        {"coverage_complete": False, "coverage_debt_count": 2},
        {},
        funnel={
            "detected_opportunities": 10,
            "evaluated_opportunities": 9,
            "coverage_debt": 2,
            "paper_entries": 4,
            "settled_trades": 4,
            "promoted_trades": 3,
            "research_probes": 4,
            "missed_opportunities": 2,
        },
        proof_confidence={"PUMP_AMM": proof},
    )
    for label in (
        "Detected opportunities", "Evaluated opportunities", "Coverage debt", "Paper entries",
        "Settled trades", "Promoted trades", "Research probes", "Missed opportunities",
        "Raw N", "Independent N", "Holdout N", "Net ROI", "Compounded NAV",
        "Expected log growth", "LCB log growth", "ES20", "Winner concentration",
        "Top-1 removed", "Top-3 removed", "Latency sensitivity", "Cost sensitivity",
        "Stress performance", "Promotion state",
    ):
        assert label in html


def test_phase10_does_not_change_frozen_v51_economics() -> None:
    spec = authority()
    assert spec["execution"]["latency_hard_max_seconds"] == 20.0
    assert spec["paper_only"] is True
    assert spec["live_money_authority"] is False
    assert spec["signing_available"] is False
    assert spec["transaction_submission_available"] is False
