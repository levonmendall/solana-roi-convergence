from __future__ import annotations

from typing import Any, Callable

from .v51_consolidated_strategy import install_v51_consolidated_strategy
from .v51_robinhood_candidate_coverage import install_v51_robinhood_candidate_coverage
from .v51_robinhood_consolidation import install_v51_robinhood_consolidation
from .v51_strategy_api import install_v51_strategy_api

COMPOSITION_VERSION = "v51-explicit-production-authority-v1"
_INSTALLED = False


def install_isolated_robinhood_proof_cache(module: Any) -> None:
    """Publish Robinhood proof from its private worker/store into status cache.

    This function is observability plumbing only. It does not install strategy
    economics or wrap the Robinhood production installer.
    """
    from . import robinhood_worker_isolation_repair as isolation

    current = isolation._ORIGINAL_STATUS
    if current is None or bool(getattr(current, "_roi_v51_isolated_proof", False)):
        return

    def status_with_v51_proof() -> dict[str, Any]:
        payload = dict(current())
        plane = getattr(module, "_PLANE", None)
        if plane is None:
            payload["v51_proof"] = {
                "available": False,
                "reason": "isolated_robinhood_plane_not_ready",
                "paper_only": True,
                "live_money_authority": False,
            }
            return payload
        try:
            from .v51_robinhood_consolidation import refresh_robinhood_candidate_learning
            from .v51_robinhood_proof import cached_robinhood_proof

            install_v51_consolidated_strategy(
                store=plane.store,
                release_commit=getattr(plane, "release_commit", None),
            )
            refresh_robinhood_candidate_learning(plane.store)
            proof = cached_robinhood_proof(plane.store)
            proof["available"] = True
            payload["v51_proof"] = proof
        except Exception as exc:
            payload["v51_proof"] = {
                "available": False,
                "reason": "isolated_robinhood_proof_failed_closed",
                "error_type": type(exc).__name__,
                "paper_only": True,
                "live_money_authority": False,
            }
        return payload

    setattr(status_with_v51_proof, "_roi_v51_isolated_proof", True)
    isolation._ORIGINAL_STATUS = status_with_v51_proof


def install_v51_production_authority(
    app: Any,
    runtime_provider: Callable[[], Any] | Any,
) -> None:
    """Install frozen v5.1 economics explicitly at the production boundary.

    ``production.py`` calls this only after the canonical Solana/FOMO graph and the
    Robinhood transport worker are installed. Reliability/compatibility modules may
    remain underneath, but none of them can become the final economic authority by
    import order. This function changes no frozen v5.1 economic rule.
    """
    global _INSTALLED
    from . import robinhood_runtime_install as module
    from .post182_production_proof_wiring_repair import (
        install_post183_production_proof_wiring_repair,
    )
    from .robinhood_chain_paper import RobinhoodChainPaperPlane

    # The first production proof after PR #182 showed two observability/evidence
    # call sites were still one wrapper below the live graph. PR #183 gives us this
    # explicit final production boundary, so attach the actual economic scout
    # normalizer and final real-WebSocket durable-frontier verifier here. These are
    # evidence-only repairs: they cannot select, size, promote or allocate.
    install_post183_production_proof_wiring_repair()

    install_v51_consolidated_strategy()
    install_v51_robinhood_consolidation()
    install_v51_robinhood_candidate_coverage(RobinhoodChainPaperPlane)
    install_isolated_robinhood_proof_cache(module)
    install_v51_strategy_api(
        app,
        runtime_provider,
        robinhood_status_provider=module._status,
    )
    app.state.roi_v51_final_economic_authority = True
    app.state.roi_v51_economic_composition = COMPOSITION_VERSION
    app.state.roi_v51_economic_composition_explicit = True
    app.state.roi_post183_production_proof_wiring = True
    # PR #182 proof/readiness is prepared before Robinhood transport captures its
    # status provider. Mark that proof surface explicitly here without granting it
    # strategy or allocation authority.
    app.state.roi_final_production_proof_readiness = True
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "composition_version": COMPOSITION_VERSION,
        "installed": _INSTALLED,
        "economic_authority_installation": "explicit_call_from_solana_roi.production_after_robinhood_transport_install",
        "proof_readiness_prepared_before_robinhood_transport": True,
        "post183_production_proof_wiring": True,
        "legacy_repair_modules_are_final_economic_authority": False,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "COMPOSITION_VERSION",
    "install_isolated_robinhood_proof_cache",
    "install_v51_production_authority",
    "status",
]
