from __future__ import annotations

from pathlib import Path


def test_render_preserves_certified_production_entrypoint() -> None:
    blueprint = Path("render.yaml").read_text()
    assert "startCommand: uvicorn solana_roi.production:app" in blueprint
    assert "SOLANA_ROI_ACTIVE_STRATEGY_AUTHORITY" in blueprint
    assert "roi-convergence-v5.1-consolidated-proof-1" in blueprint


def test_production_installs_explicit_final_v51_authority_and_routes() -> None:
    from solana_roi import production

    assert production.app.state.roi_v51_final_economic_authority is True
    assert production.app.state.roi_v51_economic_composition_explicit is True
    assert production.app.state.roi_v51_economic_composition == "v51-explicit-production-authority-v1"
    paths = {getattr(route, "path", None) for route in production.app.routes}
    assert "/v1/strategy/authority" in paths
    assert "/v1/strategy/consolidation" in paths
    assert "/v1/strategy/candidate-coverage" in paths
    assert "/v1/strategy/economic-certification" in paths
    assert "/v1/strategy/promotion-certification" in paths
    assert "/v1/strategy/incremental-alpha" in paths
    assert "/v1/strategy/research-allocation" in paths
    assert "/v1/strategy/execution-stress" in paths
    assert "/v1/strategy/execution-cost-ledger" in paths
    assert "/v1/strategy/rejected-counterfactuals" in paths
    assert "/v1/strategy/hazard-calibration" in paths
    assert "/v1/strategy/correlation-proof" in paths
    assert "/v1/strategy/allocation-maturity" in paths
    assert "/v1/strategy/portfolio-reconciliation" in paths
    assert "/v1/strategy/forward-slo" in paths
    assert "/v1/strategy/economic-dashboard" in paths


def test_legacy_import_hook_is_deleted_and_no_longer_owns_strategy_economics() -> None:
    from solana_roi import production  # noqa: F401
    from solana_roi import robinhood_runtime_install as runtime_install

    assert bool(
        getattr(runtime_install.install_robinhood_chain_paper_runtime, "_roi_v51_final_authority", False)
    ) is False
    assert not Path("src/solana_roi/v51_final_production_install.py").exists()
    isolation_source = Path("src/solana_roi/robinhood_worker_isolation_repair.py").read_text()
    assert "v51_final_production_install" not in isolation_source


def test_final_strategy_functions_are_marked_as_consolidated() -> None:
    from solana_roi import production  # noqa: F401
    from solana_roi import fomo_paper_strategy as fomo
    from solana_roi import risk_conditioned_alpha_v5 as v5
    from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane
    from solana_roi.robinhood_chain_profit_maximizer import RobinhoodProfitMaximizerMixin

    assert v5._choose_lane_and_fraction.__module__.endswith("v51_consolidated_strategy")
    assert fomo._paper_decision.__module__.endswith("v51_consolidated_strategy")
    assert RobinhoodProfitMaximizerMixin._v5_choose_lane_fraction.__module__.endswith("v51_robinhood_consolidation")
    assert RobinhoodProfitMaximizerMixin._v5_learned_exit_policy.__module__.endswith("v51_robinhood_consolidation")

    # Pre-lane coverage composes around the already-certified final entry methods.
    # functools.wraps preserves their module lineage and live-frontier markers while
    # the dedicated coverage marker proves the audit wrapper is present.
    assert bool(getattr(RobinhoodChainPaperPlane._maybe_open_v2, "_roi_v51_prelane_coverage", False)) is True
    assert bool(getattr(RobinhoodChainPaperPlane._maybe_open_v3, "_roi_v51_prelane_coverage", False)) is True
    assert bool(getattr(RobinhoodChainPaperPlane._maybe_open_v3, "_roi_fresh_live_frontier_entry_guard", False)) is True
    assert RobinhoodChainPaperPlane._maybe_open_v3.__module__.endswith("robinhood_chain_profit_maximizer")
