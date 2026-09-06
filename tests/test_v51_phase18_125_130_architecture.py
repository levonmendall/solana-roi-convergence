from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path

import pytest

from solana_roi.portfolio import PORTFOLIO_CORE_VERSION, allocate_family_capital, max_drawdown
from solana_roi.statistics import (
    STATISTICAL_CORE_VERSION,
    benjamini_hochberg,
    evidence_state,
    positive_edge_p_value,
    validate_return,
    winner_removal_profile,
)
from solana_roi.strategy_v51_authority import authority


ROOT = Path(__file__).resolve().parents[1]


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


def test_125_126_production_entrypoint_has_one_explicit_root() -> None:
    production_source = (ROOT / "src/solana_roi/production.py").read_text(encoding="utf-8")
    assert "from .production_system import" in production_source
    assert _installer_calls(production_source) == []

    from solana_roi.production import production_system

    status = production_system.status()
    assert status["healthy"] is True
    assert status["single_production_composition_root"] is True
    assert status["package_import_has_runtime_install_side_effects"] is False
    assert status["compatibility_adapters_self_activate"] is False
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


def test_129_static_registry_audit_and_no_package_installer_side_effects() -> None:
    subprocess.run(
        [sys.executable, "scripts/audit_dead_modules.py", "--strict"],
        cwd=ROOT,
        check=True,
    )


def test_130_cold_start_routes_components_release_and_safety() -> None:
    from solana_roi.production import COMPOSITION_STATUS_PATH, production_system

    routes = {getattr(route, "path", None) for route in production_system.app.routes}
    assert COMPOSITION_STATUS_PATH in routes
    assert "/v1/strategy/final-certification" in routes
    assert "/v1/strategy/authority" in routes
    assert "/v1/system-proof" in routes

    status = production_system.status()
    assert status["unavailable_required_components"] == []
    assert status["required_component_count"] == 10
    assert status["production_entrypoint"] == "solana_roi.production:app"

    assert float(authority()["execution"]["latency_hard_max_seconds"]) == 20.0

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
