from __future__ import annotations

from typing import Any, Callable

from .strategy_v52_authority import (
    AUTHORITY_ID,
    ECONOMIC_FREEZE_EPOCH,
    LIVE_MONEY_AUTHORITY,
    PAPER_ONLY,
    SIGNING_AVAILABLE,
    STRATEGY_VERSION,
    TRANSACTION_SUBMISSION_AVAILABLE,
    authority_fingerprint,
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
from .v52_strategy_api import install_v52_strategy_api, status as api_status

COMPOSITION_VERSION = "v52-explicit-production-authority-v3-robinhood-storage-epoch-compat"
_INSTALLED = False


def install_v52_production_authority(
    app: Any,
    runtime_provider: Callable[[], Any] | Any,
) -> None:
    """Install v5.2 after the mature compatibility substrate.

    v5.1-named transport, evidence, exact-quote, paper-capital and settlement
    modules remain reusable infrastructure. This call replaces their final
    economic-decision and learned-exit ownership with the frozen v5.2 authority.
    Robinhood's durable storage version remains compatibility metadata only; its
    v5.2 learning authority is bound to the frozen release/authority epoch.
    """
    global _INSTALLED
    _ = runtime_provider
    install_v52_authoritative_strategy()
    install_v52_robinhood_storage_compatibility()
    install_v52_robinhood_exit_authority()
    install_v52_strategy_api(app)
    app.state.roi_v51_final_economic_authority = False
    app.state.roi_v51_shadow_control = True
    app.state.roi_v52_final_economic_authority = True
    app.state.roi_v52_economic_composition = COMPOSITION_VERSION
    app.state.roi_v52_economic_composition_explicit = True
    app.state.roi_v52_robinhood_storage_compatibility = True
    app.state.roi_v52_robinhood_exit_authority = True
    app.state.roi_authoritative_strategy_version = STRATEGY_VERSION
    app.state.roi_authority_id = AUTHORITY_ID
    app.state.roi_authority_fingerprint = authority_fingerprint()
    app.state.roi_economic_freeze_epoch = ECONOMIC_FREEZE_EPOCH
    _INSTALLED = True


def status() -> dict[str, Any]:
    runtime = strategy_status()
    storage = robinhood_storage_status()
    robinhood_exit = robinhood_exit_status()
    # Robinhood preserves its compatibility table strategy label. Economic
    # authority and forward-learning scope come from the registered v5.2 epoch.
    runtime = dict(runtime)
    runtime["robinhood_rows_use_compatibility_storage_version"] = True
    runtime["robinhood_v52_authority_from_release_epoch"] = True
    runtime["new_rows_carry_v52_strategy_version"] = "solana_and_fomo_only"
    return {
        "composition_version": COMPOSITION_VERSION,
        "installed": _INSTALLED,
        "authority_id": AUTHORITY_ID,
        "strategy_version": STRATEGY_VERSION,
        "authority_fingerprint": authority_fingerprint(),
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "final_economic_authority": "v5.2",
        "v51_final_economic_authority": False,
        "v51_shadow_control": True,
        "v51_named_substrate_role": "transport_evidence_exact_execution_paper_capital_settlement_and_read_only_control",
        "strategy_runtime": runtime,
        "robinhood_storage_compatibility": storage,
        "robinhood_exit_authority": robinhood_exit,
        "strategy_api": api_status(),
        "all_decision_surfaces_v52_owned": bool(
            runtime.get("solana_final_owner")
            and runtime.get("fomo_final_owner")
            and runtime.get("robinhood_final_owner")
            and runtime.get("robinhood_forward_profile_owner")
            and storage.get("v52_authority_from_release_epoch")
            and robinhood_exit.get("final_exit_policy_owner") == "v52"
        ),
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = ["COMPOSITION_VERSION", "install_v52_production_authority", "status"]
