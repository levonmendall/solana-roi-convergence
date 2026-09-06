from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from solana_roi.models import IntentKind, TradeIntent
from solana_roi.portfolio import (
    PORTFOLIO_CORE_VERSION,
    PaperPortfolio,
    allocate_family_capital,
    max_drawdown,
)
from solana_roi.statistics import (
    STATISTICAL_CORE_VERSION,
    benjamini_hochberg,
    bootstrap_ci,
    drawdown,
    evidence_state,
    event_cluster_profile,
    expected_log_growth,
    expected_shortfall,
    maturity_kill_profile,
    positive_edge_p_value,
    sizing_profile,
    validate_return,
    winner_removal_profile,
)
from solana_roi.strategy_v51_authority import authority


ROOT = Path(__file__).resolve().parents[1]
T0 = datetime(2026, 9, 6, tzinfo=timezone.utc)


def _installer_calls(source: str) -> list[str]:
    tree = ast.parse(source)
    return [
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id.startswith("install_")
    ]


def test_125_package_import_is_passive_and_has_no_installer_calls() -> None:
    source = (ROOT / "src/solana_roi/__init__.py").read_text(encoding="utf-8")
    assert _installer_calls(source) == []

    script = (
        "import sys; import solana_roi; "
        "assert 'solana_roi.api' not in sys.modules; "
        "assert 'solana_roi.production' not in sys.modules; "
        "assert 'solana_roi.production_system' not in sys.modules"
    )
    subprocess.run([sys.executable, "-c", script], cwd=ROOT, check=True)


def test_125_production_entrypoint_has_one_explicit_root() -> None:
    production_source = (ROOT / "src/solana_roi/production.py").read_text(encoding="utf-8")
    assert "from .production_system import" in production_source
    assert _installer_calls(production_source) == []

    from solana_roi.production import production_system

    status = production_system.status()
    assert status["healthy"] is True
    assert status["single_production_composition_root"] is True
    assert status["package_import_has_runtime_install_side_effects"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False

    required = {
        "ingestion",
        "evidence",
        "candidate",
        "strategy",
        "execution",
        "settlement",
        "learning",
        "certification",
        "portfolio",
        "statistics",
    }
    assert required == set(status["components"])
    assert all(status["components"][name]["available"] for name in required)


def test_126_production_root_does_not_hide_installer_debt() -> None:
    """Repair 126 is complete only when production has no compatibility installer chain."""
    source = (ROOT / "src/solana_roi/production_system.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    dynamic_installer_registry = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_adapter"
    ]
    # This assertion intentionally prevents a green Phase 18 gate until the
    # migration boundary has actually been retired rather than renamed.
    assert dynamic_installer_registry == []
    assert "CompatibilityAdapter" not in source
    assert "method-assign" not in source


def test_127_canonical_portfolio_preserves_frozen_family_cap_and_drawdown() -> None:
    spec = authority()
    assert float(spec["allocation"]["immature_family_max_weight"]) == 0.25
    allocation = allocate_family_capital({"PUMP_AMM": 2.0, "RAYDIUM": 1.0})
    assert allocation["portfolio_core_version"] == PORTFOLIO_CORE_VERSION
    assert allocation["active_family_cap"] == 0.25
    assert allocation["paper_allocation_weights"] == {"PUMP_AMM": 0.25, "RAYDIUM": 0.25}
    assert allocation["paper_cash_weight"] == pytest.approx(0.50)
    assert allocation["allocation_policy_changed"] is False
    assert max_drawdown([100.0, 120.0, 90.0, 110.0]) == pytest.approx(0.25)


def test_127_trade_return_is_actual_committed_capital_not_whole_nav() -> None:
    portfolio = PaperPortfolio()
    portfolio.apply(
        TradeIntent(IntentKind.OPEN_FULL, "MINT-A", T0, fraction_of_full_position=1.0),
        scout_wallet="scout",
        reference_price=1.0,
        family="PUMP_AMM",
        context="clean|continuation",
    )
    position = portfolio.positions["MINT-A"]
    committed = position.entry_capital_usd
    assert committed == pytest.approx(12.5)
    assert portfolio.cash_usd == pytest.approx(500.0 - committed)

    open_status = portfolio.accounting_status({"MINT-A": 1.5})
    assert open_status["capital_committed_usd"] == pytest.approx(committed)
    assert open_status["unrealized_pnl_usd"] > 0.0
    assert open_status["family_capital_committed_usd"] == {"PUMP_AMM": pytest.approx(committed)}
    assert open_status["whole_nav_trade_attribution"] is False
    assert open_status["trade_return_denominator"] == "actual_position_entry_capital"

    portfolio.apply(
        TradeIntent(IntentKind.EXIT_THESIS, "MINT-A", T0, reason="test-exit"),
        scout_wallet="scout",
        reference_price=2.0,
    )
    outcome = portfolio.closed[-1]
    assert outcome.starting_nav_usd == pytest.approx(committed)
    assert outcome.starting_nav_usd != pytest.approx(500.0)
    assert outcome.ending_nav_usd == pytest.approx(committed + outcome.net_pnl_usd)
    assert outcome.return_on_starting_nav == pytest.approx(outcome.net_pnl_usd / committed)
    assert portfolio.accounting_status()["pending_exits"] == []
    assert portfolio.accounting_status()["open_paper_position_count"] == 0


def test_128_canonical_statistics_are_fail_closed_and_keep_total_loss() -> None:
    total_loss = validate_return(-1.0)
    assert total_loss.validity is True
    assert total_loss.normalized_fraction == -1.0

    assert positive_edge_p_value([0.1, 0.2, 0.3]) < 0.5
    accepted = benjamini_hochberg({"a": 0.01, "b": 0.04, "c": 0.80}, q=0.10)
    assert accepted == {"a": True, "b": True, "c": False}

    proof = evidence_state([{"net_return": 0.2}, {"net_return": None}], minimum_valid=1)
    assert proof["statistical_core_version"] == STATISTICAL_CORE_VERSION
    assert proof["proof_eligible"] is False
    assert proof["invalid_returns_are_never_imputed"] is True

    winner = winner_removal_profile([1.0, 0.2, -1.0, 0.1], fixed_fraction=0.01)
    assert winner["exact_total_losses_remain_valid"] is True


def test_128_statistics_facade_owns_required_inference_contracts() -> None:
    values = [0.40, -0.25, 0.20, -1.0, 0.10, 0.35]
    clusters = ["a", "a", "b", "c", "d", "d"]

    assert expected_log_growth(values, fraction=0.01) is not None
    assert drawdown(values, fraction=0.01) > 0.0
    assert expected_shortfall(values) == pytest.approx(-1.0)

    first = event_cluster_profile(values, cluster_ids=clusters, fixed_fraction=0.01, bootstrap_samples=80)
    second = event_cluster_profile(values, cluster_ids=clusters, fixed_fraction=0.01, bootstrap_samples=80)
    assert first == second
    assert first["bootstrap_unit"] == "token_event_cluster"
    assert first["fraction_selection_mode"] == "preselected_fixed_fraction"

    ci = bootstrap_ci(values, cluster_ids=clusters, fixed_fraction=0.01, bootstrap_samples=80)
    assert ci["mean_lower"] is not None and ci["mean_upper"] is not None
    assert float(ci["mean_lower"]) <= float(ci["mean_upper"])

    holdout = sizing_profile(values, preselected_fraction=0.02, cluster_ids=clusters)
    assert holdout["best_fraction"] == pytest.approx(0.02)
    assert holdout["fraction_selection_mode"] == "preselected_fixed_fraction"
    assert holdout["preselected_holdout_fraction_required"] is True

    maturity = maturity_kill_profile(
        values,
        [0.05, -0.05],
        [0.02, 0.01],
        risk_signature="clean",
    )
    assert maturity["state"] in {
        "killed_negative_robust_edge",
        "promoted_positive_hierarchical_edge",
        "mature_unproven",
        "bootstrap_hierarchical_evidence",
    }


def test_129_superseded_v4_manifests_are_archived_not_executable_roots() -> None:
    assert not (ROOT / "strategy_v4_manifest.json").exists()
    assert not (ROOT / "strategy_v4_final_manifest.json").exists()
    for name in ("strategy_v4_manifest.json", "strategy_v4_final_manifest.json"):
        payload = json.loads((ROOT / "docs/archive/v4" / name).read_text(encoding="utf-8"))
        assert "ARCHIVED" in payload["_archive_notice"]
        assert "no current strategy or production authority" in payload["_archive_notice"]
        assert payload["paper_only"] is True
        assert payload["live_money_authority"] is False


def test_129_130_static_reachability_audit_is_strict() -> None:
    subprocess.run(
        [sys.executable, "scripts/audit_dead_modules.py", "--strict"],
        cwd=ROOT,
        check=True,
    )


def test_130_cold_start_routes_components_release_and_safety() -> None:
    from fastapi.testclient import TestClient
    from solana_roi.production import COMPOSITION_STATUS_PATH, production_system

    routes = {getattr(route, "path", None) for route in production_system.app.routes}
    assert COMPOSITION_STATUS_PATH in routes
    assert "/health" in routes
    assert "/v1/strategy/final-certification" in routes
    assert "/v1/strategy/authority" in routes
    assert "/v1/system-proof" in routes

    status = production_system.status()
    assert status["unavailable_required_components"] == []
    assert status["required_component_count"] == 10
    assert status["production_entrypoint"] == "solana_roi.production:app"
    assert float(authority()["execution"]["latency_hard_max_seconds"]) == 20.0

    with TestClient(production_system.app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["paper_only"] is True
        composition = client.get(COMPOSITION_STATUS_PATH)
        assert composition.status_code == 200
        assert composition.json()["healthy"] is True

    release = "a" * 40
    env = dict(os.environ)
    env["GITHUB_SHA"] = release
    subprocess.run(
        [
            sys.executable,
            "-c",
            "from solana_roi.v51_measurement_integrity import current_release_commit; "
            f"assert current_release_commit() == '{release}'",
        ],
        cwd=ROOT,
        env=env,
        check=True,
    )

    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
