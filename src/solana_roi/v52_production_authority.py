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
from .v52_wallet_production_alignment import (
    install_v52_wallet_production_alignment,
    status as wallet_alignment_status,
)

COMPOSITION_VERSION = "v52-explicit-production-authority-v7-wallet-production-alignment"
_INSTALLED = False


def _copy_robinhood_lineage(wrapper: Any, predecessor: Any) -> None:
    """Expose only real predecessor lineage on the final production wrapper."""
    if not callable(predecessor):
        raise RuntimeError("v52 Robinhood lifecycle predecessor unavailable")
    setattr(wrapper, "__wrapped__", predecessor)
    for name, value in vars(predecessor).items():
        if name.startswith("_roi_") and not hasattr(wrapper, name):
            setattr(wrapper, name, value)


def _bind_concrete_robinhood_lifecycle_owner() -> None:
    """Bind the final chooser at the concrete composed production class.

    Mature Robinhood repairs may install a class-level chooser before v5.2 is
    composed. Patching only the source mixin is therefore insufficient: the
    concrete class can legitimately shadow the mixin and continue resolving its
    previously composed chooser. Capture that exact predecessor and put the v5.2
    lifecycle wrapper at the concrete boundary as well. The predecessor's real
    reachability markers remain visible for architecture introspection; no marker
    is manufactured by this layer.
    """
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
    """Keep predecessor reachability markers visible on final v5.2 entry wrappers."""
    from .robinhood_chain_paper import RobinhoodChainPaperPlane

    pairs = (
        (
            RobinhoodChainPaperPlane._maybe_open_v3,
            getattr(robinhood_lifecycle, "_BASE_MAYBE_V3", None),
        ),
        (
            RobinhoodChainPaperPlane._maybe_open_v2,
            getattr(robinhood_lifecycle, "_BASE_MAYBE_V2", None),
        ),
    )
    for wrapper, predecessor in pairs:
        _copy_robinhood_lineage(wrapper, predecessor)


def install_v52_production_authority(
    app: Any,
    runtime_provider: Callable[[], Any] | Any,
) -> None:
    """Install governed v5.2 paper authority after the compatibility substrate.

    v5.1-named transport, evidence, exact-quote, paper-capital and settlement
    modules remain reusable infrastructure. This call replaces their final
    economic-decision and learned-exit ownership with v5.2 authority. The named
    ``economic_freeze_epoch`` remains a compatibility key for persisted release
    lineage only; strategy policy may advance through append-only governed epochs.

    Wallet discovery is bound here, at the final authority boundary, so copyability
    rules cannot remain anchored to a stale baseline after a governed v5.2 strategy
    epoch advances. Broad discovery consumes the already-persisted normalized Solana
    journal rather than re-fetching and re-normalizing the same transaction.

    Robinhood's durable storage version remains compatibility metadata only. The
    position-lifecycle layer is installed after the final Robinhood storage and
    exit-policy wrappers so it owns aggregate lot accounting, scale validation,
    staged de-risking, runner retention and second-leg re-entry without changing
    transport, identity or exact-quote semantics. Candidate accounting is then
    reconciled only after the validated lifecycle commit exists.
    """
    global _INSTALLED
    install_v52_authoritative_strategy()
    install_v52_wallet_production_alignment(runtime_provider)
    install_v52_robinhood_storage_compatibility()
    install_v52_robinhood_exit_authority()
    install_v52_robinhood_position_lifecycle()
    _bind_concrete_robinhood_lifecycle_owner()
    install_v52_robinhood_candidate_reconciliation()
    _preserve_robinhood_wrapper_contracts()
    install_v52_strategy_api(app)
    strategy_epoch = strategy_evolution_snapshot()
    app.state.roi_v51_final_economic_authority = False
    app.state.roi_v51_shadow_control = True
    app.state.roi_v52_final_economic_authority = True
    app.state.roi_v52_economic_composition = COMPOSITION_VERSION
    app.state.roi_v52_economic_composition_explicit = True
    app.state.roi_v52_wallet_production_alignment = True
    app.state.roi_v52_robinhood_storage_compatibility = True
    app.state.roi_v52_robinhood_exit_authority = True
    app.state.roi_v52_robinhood_position_lifecycle = True
    app.state.roi_v52_robinhood_candidate_reconciliation = True
    app.state.roi_v52_continuous_strategy_evolution = True
    app.state.roi_authoritative_strategy_version = STRATEGY_VERSION
    app.state.roi_authority_id = AUTHORITY_ID
    app.state.roi_authority_fingerprint = authority_fingerprint()
    # Backward-compatible field retained for persisted release/outcome joins.
    app.state.roi_economic_freeze_epoch = ECONOMIC_FREEZE_EPOCH
    app.state.roi_strategy_baseline_epoch = ECONOMIC_FREEZE_EPOCH
    app.state.roi_active_strategy_epoch = strategy_epoch
    _INSTALLED = True


def status() -> dict[str, Any]:
    runtime = dict(strategy_status())
    wallet_alignment = wallet_alignment_status()
    storage = robinhood_storage_status()
    robinhood_exit = robinhood_exit_status()
    lifecycle = robinhood_lifecycle_status()
    reconciliation = robinhood_candidate_reconciliation_status()
    strategy_epoch = strategy_evolution_snapshot()
    # Robinhood preserves its compatibility table strategy label. Economic
    # authority and forward-learning scope come from registered v5.2 lineage;
    # live policy values come from the current governed strategy epoch.
    runtime["robinhood_rows_use_compatibility_storage_version"] = True
    runtime["robinhood_v52_authority_from_release_epoch"] = True
    runtime["continuous_strategy_evolution_enabled"] = True
    runtime["active_strategy_epoch"] = strategy_epoch
    runtime["new_rows_carry_v52_strategy_version"] = "solana_and_fomo_plus_explicit_robinhood_lifecycle_ledger"
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
        "wallet_production_alignment": wallet_alignment,
        "robinhood_storage_compatibility": storage,
        "robinhood_exit_authority": robinhood_exit,
        "robinhood_position_lifecycle": lifecycle,
        "robinhood_candidate_reconciliation": reconciliation,
        "strategy_api": api_status(),
        "all_decision_surfaces_v52_owned": bool(
            runtime.get("solana_final_owner")
            and runtime.get("fomo_final_owner")
            and runtime.get("robinhood_final_owner")
            and runtime.get("robinhood_forward_profile_owner")
            and runtime.get("robinhood_scale_in_authority")
            and runtime.get("staged_derisk_runner_authority")
            and wallet_alignment.get("installed")
            and wallet_alignment.get("zero_duplicate_normalized_handoff")
            and storage.get("v52_authority_from_release_epoch")
            and robinhood_exit.get("final_exit_policy_owner") == "v52"
            and lifecycle.get("installed")
            and reconciliation.get("installed")
        ),
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = ["COMPOSITION_VERSION", "install_v52_production_authority", "status"]
