from __future__ import annotations

from solana_roi.v51_dashboard import render_economic_dashboard
from solana_roi.v51_execution_stress_diagnostics import MECHANISM_SCENARIOS, mechanism_stress_profiles


def test_mechanism_stress_is_explicit_and_diagnostic_only_by_construction() -> None:
    values = [0.20, 0.10, -0.05, 0.08, 0.04]
    payload = mechanism_stress_profiles(values)
    assert set(payload) == {
        "priority_fee",
        "block_placement",
        "mev_adverse_selection",
        "quote_deterioration",
        "transaction_failure",
    }
    assert set(payload) == set(MECHANISM_SCENARIOS)
    for mechanism in payload.values():
        assert set(mechanism) == {"mild", "material", "severe"}
        for scenario in mechanism.values():
            assert "profile" in scenario
            assert scenario["profile"]["sample_count"] == len(values)


def test_dashboard_renders_same_proof_without_independent_authority() -> None:
    certification = {
        "authority_id": "roi-convergence-v5.1-consolidated-proof-1",
        "economic_freeze_epoch": "v51-consolidated-proof-20260905",
        "closed_outcome_count": 12,
        "paper_cash_weight": 0.75,
        "research_family_ranking": ["PUMP_AMM"],
        "paper_allocation_weights": {"PUMP_AMM": 0.25},
        "families": {
            "PUMP_AMM": {
                "closed_outcome_count": 12,
                "independent_event_count": 12,
                "compounded_nav_multiple": 1.03,
                "robust_profile": {
                    "best_expected_log_growth": 0.001,
                    "expected_shortfall_20": -0.03,
                    "max_drawdown_at_best_fraction": 0.02,
                },
                "promotion_kill_profile": {"state": "bootstrap_hierarchical_evidence"},
            }
        },
    }
    coverage = {"coverage_complete": True, "coverage_debt_count": 0}
    stress = {"mechanisms": list(MECHANISM_SCENARIOS)}
    html = render_economic_dashboard(certification, coverage, stress)
    assert "PAPER ONLY" in html
    assert "PUMP_AMM" in html
    assert "12" in html
    assert "priority_fee" in html
    assert "NO LIVE-MONEY AUTHORITY" in html
