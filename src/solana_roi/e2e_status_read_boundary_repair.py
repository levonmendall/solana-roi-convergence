from __future__ import annotations

from typing import Any, Callable

from . import unified_strategy_status as unified


REPAIR_VERSION = "e2e-status-read-boundary-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False


def _required_status_base(runtime: Any) -> dict[str, Any]:
    """Build only the state consumed by the unified E2E contract.

    The legacy dedicated E2E route called the complete ingestion-status endpoint,
    which also verifies the entire append-only event chain and gathers unrelated
    audit surfaces. That work is valid for the deep ingestion audit, but it is not
    consumed by ``build_unified_strategy_status`` and can grow without bound with
    production evidence. Keep E2E certification on the same live transport/wallet
    truth while excluding unrelated full-store verification from the request path.
    """
    return {
        "data_plane": "direct-solana",
        "direct_solana": runtime.direct_ingestion.status(),
        "wallet_discovery": runtime.wallet_discovery.status(),
    }


def build_bounded_e2e_status(
    runtime_provider: Callable[[], Any],
    robinhood_status_provider: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    runtime = runtime_provider()
    robinhood = robinhood_status_provider()
    payload = unified.build_unified_strategy_status(
        _required_status_base(runtime),
        runtime,
        robinhood,
    )
    return {
        "status_contract_version": payload["status_contract_version"],
        "release_commit": payload["release_commit"],
        "solana": payload["solana"],
        "fomo": payload["fomo"],
        "robinhood": payload["robinhood"],
        "overall": payload["overall"],
        "read_boundary": {
            "repair_version": REPAIR_VERSION,
            "full_ingestion_status_invoked": False,
            "full_event_chain_verification_invoked": False,
            "strategy_contract_or_gate_relaxed": False,
            "paper_only": PAPER_ONLY,
            "live_money_authority": LIVE_MONEY_AUTHORITY,
            "signing_available": SIGNING_AVAILABLE,
            "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        },
    }


def install_e2e_status_read_boundary_repair(
    app: Any,
    runtime_provider: Callable[[], Any],
) -> None:
    """Replace only the dedicated E2E read endpoint after canonical composition.

    The canonical ingestion-status endpoint remains unchanged and can still perform
    the complete append-only audit when explicitly requested. Certification keeps
    the exact same unified E2E builder and therefore the same transport, regime,
    Robinhood and paper-authority assertions; only unrelated work is removed from
    this one read path.
    """
    if bool(getattr(app.state, "roi_e2e_status_read_boundary", False)):
        return

    from . import robinhood_runtime_install as robinhood_runtime

    route = None
    for candidate in app.routes:
        if getattr(candidate, "path", None) == "/v1/strategy/e2e-status":
            route = candidate
            break
    if route is None:
        raise RuntimeError("strategy E2E status route not found after canonical composition")

    def bounded_e2e_status() -> dict[str, Any]:
        return build_bounded_e2e_status(runtime_provider, robinhood_runtime._status)

    setattr(bounded_e2e_status, "_roi_e2e_status_read_boundary", True)
    route.endpoint = bounded_e2e_status
    dependant = getattr(route, "dependant", None)
    if dependant is not None:
        dependant.call = bounded_e2e_status

    app.state.roi_e2e_status_read_boundary = True
    app.state.roi_e2e_status_read_boundary_version = REPAIR_VERSION


__all__ = [
    "REPAIR_VERSION",
    "build_bounded_e2e_status",
    "install_e2e_status_read_boundary_repair",
]
