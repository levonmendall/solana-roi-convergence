from __future__ import annotations

from typing import Any, Callable

from . import v52_robinhood_position_lifecycle as robinhood_lifecycle
from .strategy_v52_authority import (
    AUTHORITY_ID,
    ECONOMIC_FREEZE_EPOCH,
    LIVE_MONEY_AUTHORITY,
    PAPER_ONLY,
    SIGNING_AVAILABLE,
    STRATEGY_VERSION,
    TRANSACTION_SUBMISSION_AVAILABLE,
    authority_fingerprint,
    strategy_evolution_snapshot,
)
from .v52_authoritative_strategy import install_v52_authoritative_strategy, status as strategy_status
from .v52_robinhood_storage_compatibility import (
    install_v52_robinhood_storage_compatibility,
    status as robinhood_storage_status,
)
from .v52_robinhood_exit_authority import (
    install_v52_robinhood_exit_authority,
    status as robinhood_exit_status,
)
from .v52_robinhood_position_lifecycle import (
    install_v52_robinhood_position_lifecycle,
    lifecycle_status as robinhood_lifecycle_status,
)
from .v52_robinhood_candidate_reconciliation import (
    install_v52_robinhood_candidate_reconciliation,
    status as robinhood_candidate_reconciliation_status,
)
from .v52_strategy_api import install_v52_strategy_api, status as api_status
from .v52_wallet_alpha_refinement import WalletAlphaRefinementLedger
from .v52_wallet_intelligence_alignment import (
    install_v52_wallet_intelligence_alignment,
    status as wallet_alignment_status,
)
from .v52_adaptive_continuation_refinement import (
    install_v52_adaptive_continuation_refinement,
    status as adaptive_continuation_status,
)
from .v52_profit_confidence_completion import (
    install_v52_profit_confidence_completion,
    status as profit_confidence_status,
)
from .v52_profit_confidence_finalization import (
    install_v52_profit_confidence_finalization,
    status as profit_confidence_finalization_status,
)
from .v52_learning_governance import (
    install_v52_learning_governance,
    status as learning_governance_status,
)
from .v52_learning_governance_hardening import (
    install_v52_learning_governance_hardening,
    status as learning_governance_hardening_status,
)

COMPOSITION_VERSION = "v52-explicit-production-authority-v11-learning-governance-hardened"
_INSTALLED = False
_RUNTIME: Any | None = None
_WALLET_ALPHA: WalletAlphaRefinementLedger | None = None


def _copy_robinhood_lineage(wrapper: Any, predecessor: Any) -> None:
    if not callable(predecessor):
        raise RuntimeError("v52 Robinhood lifecycle predecessor unavailable")
    setattr(wrapper, "__wrapped__", predecessor)
    for name, value in vars(predecessor).items():
        if name.startswith("_roi_") and not hasattr(wrapper, name):
            setattr(wrapper, name, value)


def _bind_concrete_robinhood_lifecycle_owner() -> None:
    from .robinhood_chain_paper import RobinhoodChainPaperPlane

    final_wrapper = getattr(robinhood_lifecycle, "_choose_with_lifecycle")
    current = RobinhoodChainPaperPlane._v5_choose_lane_fraction
    if current is final_wrapper:
        return
    setattr(robinhood_lifecycle, "_BASE_CHOOSE", current)
    _copy_robinhood_lineage(final_wrapper, current)
    RobinhoodChainPaperPlane._v5_choose_lane_fraction = final_wrapper  # type: ignore[method-assign]
    setattr(RobinhoodChainPaperPlane._v5_choose_lane_fraction, "_roi_v52_final_authority", True)
    setattr(RobinhoodChainPaperPlane._v5_choose_lane_fraction, "_roi_v52_position_lifecycle", True)


def _preserve_robinhood_wrapper_contracts() -> None:
    from .robinhood_chain_paper import RobinhoodChainPaperPlane

    pairs = (
        (RobinhoodChainPaperPlane._maybe_open_v3, getattr(robinhood_lifecycle, "_BASE_MAYBE_V3", None)),
        (RobinhoodChainPaperPlane._maybe_open_v2, getattr(robinhood_lifecycle, "_BASE_MAYBE_V2", None)),
    )
    for wrapper, predecessor in pairs:
        _copy_robinhood_lineage(wrapper, predecessor)


def _resolve_runtime(runtime_provider: Callable[[], Any] | Any) -> Any:
    return runtime_provider() if callable(runtime_provider) else runtime_provider


def wallet_alpha_refinement() -> WalletAlphaRefinementLedger:
    if _WALLET_ALPHA is None:
        raise RuntimeError("v5.2 wallet alpha refinement not installed")
    return _WALLET_ALPHA


def install_v52_production_authority(app: Any, runtime_provider: Callable[[], Any] | Any) -> None:
    """Install the single governed v5.2 paper authority in final wrapper order."""
    global _INSTALLED, _RUNTIME, _WALLET_ALPHA
    runtime = _resolve_runtime(runtime_provider)
    install_v52_authoritative_strategy()
    install_v52_robinhood_storage_compatibility()
    install_v52_robinhood_exit_authority()
    install_v52_robinhood_position_lifecycle()
    _bind_concrete_robinhood_lifecycle_owner()
    install_v52_robinhood_candidate_reconciliation()
    _preserve_robinhood_wrapper_contracts()
    install_v52_wallet_intelligence_alignment(runtime)
    if _WALLET_ALPHA is None or _WALLET_ALPHA.store is not runtime.store:
        _WALLET_ALPHA = WalletAlphaRefinementLedger(runtime.store)
    install_v52_adaptive_continuation_refinement(_WALLET_ALPHA)
    install_v52_profit_confidence_completion(runtime)
    install_v52_learning_governance(runtime)
    install_v52_learning_governance_hardening()
    install_v52_profit_confidence_finalization()
    install_v52_strategy_api(app)

    strategy_epoch = strategy_evolution_snapshot()
    app.state.roi_v51_final_economic_authority = False
    app.state.roi_v51_shadow_control = True
    app.state.roi_v52_final_economic_authority = True
    app.state.roi_v52_economic_composition = COMPOSITION_VERSION
    app.state.roi_v52_economic_composition_explicit = True
    app.state.roi_v52_robinhood_storage_compatibility = True
    app.state.roi_v52_robinhood_exit_authority = True
    app.state.roi_v52_robinhood_position_lifecycle = True
    app.state.roi_v52_robinhood_candidate_reconciliation = True
    app.state.roi_v52_continuous_strategy_evolution = True
    app.state.roi_v52_wallet_intelligence_alignment = True
    app.state.roi_v52_wallet_alpha_refinement = True
    app.state.roi_v52_adaptive_continuation_refinement = True
    app.state.roi_v52_profit_confidence_completion = True
    app.state.roi_v52_learning_governance = True
    app.state.roi_v52_learning_governance_hardening = True
    app.state.roi_v52_profit_confidence_finalization = True
    app.state.roi_authoritative_strategy_version = STRATEGY_VERSION
    app.state.roi_authority_id = AUTHORITY_ID
    app.state.roi_authority_fingerprint = authority_fingerprint()
    app.state.roi_economic_freeze_epoch = ECONOMIC_FREEZE_EPOCH
    app.state.roi_strategy_baseline_epoch = ECONOMIC_FREEZE_EPOCH
    app.state.roi_active_strategy_epoch = strategy_epoch
    _RUNTIME = runtime
    _INSTALLED = True


def status() -> dict[str, Any]:
    runtime = dict(strategy_status())
    storage = robinhood_storage_status()
    robinhood_exit = robinhood_exit_status()
    lifecycle = robinhood_lifecycle_status()
    reconciliation = robinhood_candidate_reconciliation_status()
    adaptive = adaptive_continuation_status()
    completion = profit_confidence_status()
    learning = learning_governance_status()
    hardening = learning_governance_hardening_status()
    finalization = profit_confidence_finalization_status()
    strategy_epoch = strategy_evolution_snapshot()

    if _RUNTIME is None:
        wallet_alignment = {
            "installed": False,
            "reason": "runtime_not_installed",
            "paper_only": True,
            "live_money_authority": False,
        }
    else:
        wallet_alignment = wallet_alignment_status(_RUNTIME)
    if _WALLET_ALPHA is None:
        wallet_alpha = {
            "version": "v52-wallet-alpha-refinement-v1",
            "installed": False,
            "paper_only": True,
            "live_money_authority": False,
        }
    else:
        wallet_alpha = dict(_WALLET_ALPHA.status())
        wallet_alpha["installed"] = True

    runtime["robinhood_rows_use_compatibility_storage_version"] = True
    runtime["robinhood_v52_authority_from_release_epoch"] = True
    runtime["continuous_strategy_evolution_enabled"] = True
    runtime["active_strategy_epoch"] = strategy_epoch
    runtime["new_rows_carry_v52_strategy_version"] = "solana_fomo_robinhood_profit_confidence_plus_hardened_learning_governance"
    runtime["robinhood_scale_in_authority"] = bool(
        lifecycle.get("installed")
        and lifecycle.get("aggregate_exact_exitability_before_add")
        and lifecycle.get("stressed_exit_capacity_before_entry_or_add")
        and lifecycle.get("scale_requires_new_forward_strength")
        and not lifecycle.get("averaging_down_allowed")
    )
    runtime["staged_derisk_runner_authority"] = bool(
        lifecycle.get("installed") and lifecycle.get("staged_derisk_runner_authority")
    )
    runtime["adaptive_continuation_refinement"] = bool(adaptive.get("installed"))
    runtime["profit_confidence_completion"] = bool(completion.get("installed"))
    runtime["learning_governance"] = bool(learning.get("installed"))
    runtime["learning_governance_hardening"] = bool(hardening.get("installed"))
    runtime["profit_confidence_finalization"] = bool(finalization.get("installed"))

    all_owned = bool(
        runtime.get("solana_final_owner")
        and runtime.get("fomo_final_owner")
        and runtime.get("robinhood_final_owner")
        and runtime.get("robinhood_forward_profile_owner")
        and runtime.get("robinhood_scale_in_authority")
        and runtime.get("staged_derisk_runner_authority")
        and adaptive.get("installed")
        and completion.get("installed")
        and completion.get("parallel_exact_quote_acquisition")
        and float(completion.get("minimum_exit_depth_coverage_ratio") or 0.0) >= 2.0
        and learning.get("installed")
        and learning.get("bayesian_posterior_confidence")
        and learning.get("wallet_distribution_reversal_primary_exit_signal")
        and learning.get("lane_specific_learned_decay")
        and learning.get("automatic_challenger_generation")
        and learning.get("concurrent_named_same_stream_tournament")
        and learning.get("automatic_forward_promotion")
        and learning.get("automatic_forward_demotion")
        and hardening.get("installed")
        and hardening.get("stable_auto_challenger_ids")
        and hardening.get("fresh_same_stream_epoch_after_promotion")
        and not hardening.get("old_forward_evidence_reuse_for_next_promotion")
        and finalization.get("installed")
        and finalization.get("numeric_lane_cap_guard")
        and finalization.get("absolute_signal_age_guard")
        and float(finalization.get("absolute_chase_max_fraction") or 1.0) <= 0.80
        and float(finalization.get("latency_hard_max_seconds") or 99.0) <= 20.0
        and wallet_alignment.get("installed")
        and storage.get("v52_authority_from_release_epoch")
        and robinhood_exit.get("final_exit_policy_owner") == "v52"
        and lifecycle.get("installed")
        and reconciliation.get("installed")
    )

    return {
        "composition_version": COMPOSITION_VERSION,
        "installed": _INSTALLED,
        "authority_id": AUTHORITY_ID,
        "strategy_version": STRATEGY_VERSION,
        "authority_fingerprint": authority_fingerprint(),
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "baseline_strategy_epoch": ECONOMIC_FREEZE_EPOCH,
        "continuous_strategy_evolution_enabled": True,
        "active_strategy_epoch": strategy_epoch,
        "final_economic_authority": "v5.2",
        "v51_final_economic_authority": False,
        "v51_shadow_control": True,
        "v51_named_substrate_role": "transport_evidence_exact_execution_paper_capital_settlement_and_read_only_control",
        "strategy_runtime": runtime,
        "wallet_intelligence_alignment": wallet_alignment,
        "wallet_alpha_refinement": wallet_alpha,
        "adaptive_continuation_refinement": adaptive,
        "profit_confidence_completion": completion,
        "learning_governance": learning,
        "learning_governance_hardening": hardening,
        "profit_confidence_finalization": finalization,
        "robinhood_storage_compatibility": storage,
        "robinhood_exit_authority": robinhood_exit,
        "robinhood_position_lifecycle": lifecycle,
        "robinhood_candidate_reconciliation": reconciliation,
        "strategy_api": api_status(),
        "all_decision_surfaces_v52_owned": all_owned,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = [
    "COMPOSITION_VERSION",
    "install_v52_production_authority",
    "status",
    "wallet_alpha_refinement",
]
