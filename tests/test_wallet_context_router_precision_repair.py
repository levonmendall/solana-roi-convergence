from __future__ import annotations

from types import SimpleNamespace

import pytest

from solana_roi import wallet_context_router as router_module
from solana_roi.wallet_context_router import WalletContextRouter
from solana_roi.wallet_context_router_precision_repair import (
    REPAIR_VERSION,
    install_wallet_context_router_precision_repair,
)
from solana_roi.wallet_entity_universe_v4 import WalletRole


def test_precision_repair_fails_closed_when_accessibility_evidence_is_missing():
    install_wallet_context_router_precision_repair()

    missing = router_module.classify_observation_accessibility(
        {
            "venue": "PUMP_FUN",
            "lifecycle_stage": "pump_bonding_curve",
            "copyable": True,
            "risk_complete": True,
        }
    )
    assert missing["structurally_accessible"] is False
    assert "observation_latency_unknown" in missing["reasons"]
    assert "processing_delay_unknown" in missing["reasons"]
    assert "chase_unknown" in missing["reasons"]
    assert missing["observed_pipeline_seconds"] is None
    assert missing["missing_accessibility_evidence_fails_closed"] is True


def test_precision_repair_fails_closed_on_explicit_incomplete_risk():
    install_wallet_context_router_precision_repair()

    incomplete = router_module.classify_observation_accessibility(
        {
            "venue": "RAYDIUM",
            "lifecycle_stage": "raydium_native_or_migration_unproven",
            "copyable": True,
            "risk_complete": False,
            "observation_lag_ms": 500.0,
            "processing_delay_ms": 100.0,
            "chase_fraction": 0.03,
        }
    )
    assert incomplete["structurally_accessible"] is False
    assert "risk_incomplete_at_observation" in incomplete["reasons"]


def test_precision_repair_adds_explicit_percentage_roi_fields_without_removing_fractions():
    install_wallet_context_router_precision_repair()

    rows = [
        {
            "net_return": value,
            "position_fraction": 0.10,
            "signal_to_entry_seconds": 5.0,
        }
        for value in (0.10, 0.11, 0.09, 0.12, 0.08, 5.00)
    ]
    metrics = router_module._context_metrics(WalletRole.COPYABLE_ROC, rows)

    assert metrics["median_residual_roi"] == pytest.approx(0.105)
    assert metrics["median_residual_roi_pct"] == pytest.approx(10.5)
    assert metrics["trimmed_mean_residual_roi_ex_best_1"] == pytest.approx(0.10)
    assert metrics["trimmed_mean_residual_roi_ex_best_1_pct"] == pytest.approx(10.0)
    assert metrics["copyable_return_on_deployed_fraction_pct"] == pytest.approx(
        metrics["copyable_return_on_deployed_fraction"] * 100.0
    )
    assert metrics["latency_residual_roi_curve"]["lte_5s"]["median_residual_roi_pct"] == pytest.approx(10.5)


def test_precision_status_keeps_zero_authority_and_declares_percentage_units(monkeypatch):
    install_wallet_context_router_precision_repair()

    original = router_module.WalletContextRouter.status

    # The installer already wrapped status. Substitute the wrapped instance's original
    # dependency through the module global so this unit test does not need a database.
    import solana_roi.wallet_context_router_precision_repair as repair

    saved = repair._ORIGINAL_STATUS
    repair._ORIGINAL_STATUS = lambda self: {
        "router_version": "wallet-context-router-v1",
        "roi_ranking_basis": "percentage_copyable_executable_residual_return_not_dollar_pnl",
        "copyable_roi_leaders": [
            {
                "copyable_return_on_deployed_fraction": 0.25,
                "median_residual_roi": 0.10,
                "trimmed_mean_residual_roi_ex_best_1": 0.08,
                "compounded_fraction_scaled_return": 0.03,
            }
        ],
        "route_map": [],
        "context_scores_have_trade_authority": False,
        "context_recommendations_have_tracking_mutation_authority": False,
        "active_strategy_mutation_allowed": False,
        "historical_promotion_authority": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    try:
        payload = original(SimpleNamespace())
    finally:
        repair._ORIGINAL_STATUS = saved

    assert payload["router_version"] == REPAIR_VERSION
    assert payload["roi_percentage_fields_explicit"] is True
    assert payload["raw_fraction_fields_retained_for_backward_compatibility"] is True
    assert payload["accessibility_missing_evidence_fails_closed"] is True
    leader = payload["copyable_roi_leaders"][0]
    assert leader["copyable_return_on_deployed_fraction_pct"] == pytest.approx(25.0)
    assert leader["median_residual_roi_pct"] == pytest.approx(10.0)
    assert payload["context_scores_have_trade_authority"] is False
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
