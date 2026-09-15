from __future__ import annotations

import inspect

from solana_roi import v51_evidence_analytics as analytics


def test_execution_cost_ledger_skips_unchanged_conflict_updates() -> None:
    source = inspect.getsource(analytics.refresh_execution_cost_ledger)
    assert "WHERE v51_execution_cost_ledger.family IS NOT excluded.family" in source
    assert "OR v51_execution_cost_ledger.release_commit IS NOT excluded.release_commit" in source
    assert "OR v51_execution_cost_ledger.token_mint IS NOT excluded.token_mint" in source
    assert "OR v51_execution_cost_ledger.round_trip_cost_fraction IS NOT excluded.round_trip_cost_fraction" in source
    assert "OR v51_execution_cost_ledger.cost_source IS NOT excluded.cost_source" in source
    assert "OR v51_execution_cost_ledger.paper_only IS NOT 1" in source
    assert "OR v51_execution_cost_ledger.live_money_authority IS NOT 0" in source


def test_rejected_counterfactuals_skip_unchanged_conflict_updates() -> None:
    source = inspect.getsource(analytics.refresh_rejected_counterfactuals)
    assert "WHERE v51_rejected_counterfactuals.release_commit IS NOT excluded.release_commit" in source
    assert "OR v51_rejected_counterfactuals.token_mint IS NOT excluded.token_mint" in source
    assert "OR v51_rejected_counterfactuals.decision_reason IS NOT excluded.decision_reason" in source
    assert "OR v51_rejected_counterfactuals.decision_observed_at IS NOT excluded.decision_observed_at" in source
    assert "OR v51_rejected_counterfactuals.forward_net_return IS NOT excluded.forward_net_return" in source
    assert "OR v51_rejected_counterfactuals.resolution_source IS NOT excluded.resolution_source" in source
    assert "OR v51_rejected_counterfactuals.counterfactual_state IS NOT excluded.counterfactual_state" in source
    assert "OR v51_rejected_counterfactuals.hazard_signature IS NOT excluded.hazard_signature" in source
    assert "OR v51_rejected_counterfactuals.hazard_severity IS NOT excluded.hazard_severity" in source
    assert "OR v51_rejected_counterfactuals.payload_json IS NOT excluded.payload_json" in source
    assert "OR v51_rejected_counterfactuals.retrospective_entry_authority IS NOT 0" in source
    assert "OR v51_rejected_counterfactuals.paper_only IS NOT 1" in source
    assert "OR v51_rejected_counterfactuals.live_money_authority IS NOT 0" in source


def test_noop_suppression_does_not_change_authority_contract() -> None:
    cost_source = inspect.getsource(analytics.refresh_execution_cost_ledger)
    counterfactual_source = inspect.getsource(analytics.refresh_rejected_counterfactuals)
    assert '"paper_only": True' in cost_source
    assert '"live_money_authority": False' in cost_source
    assert '"retrospective_entry_authority": False' in counterfactual_source
    assert '"paper_only": True' in counterfactual_source
    assert '"live_money_authority": False' in counterfactual_source
