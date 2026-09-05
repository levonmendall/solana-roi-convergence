from __future__ import annotations

from typing import Any, Callable

from .v51_consolidated_strategy import install_v51_consolidated_strategy
from .v51_measurement_compatibility_filters import (
    install_measurement_compatible_promotion_filters,
    status as compatibility_filter_status,
)
from .v51_measurement_integrity import install_measurement_integrity, proof_metadata, status as measurement_status
from .v51_robinhood_candidate_coverage import install_v51_robinhood_candidate_coverage
from .v51_robinhood_consolidation import install_v51_robinhood_consolidation
from .v51_strategy_api import install_v51_strategy_api

COMPOSITION_VERSION = "v51-explicit-production-authority-v2-measurement-integrity"
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
                **proof_metadata(None, proof_state="unavailable"),
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
                **proof_metadata(plane.store, proof_state="unavailable"),
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
    import order. Measurement integrity is installed before the runtime object is
    lazily constructed so upstream v3.1 evidence filters cannot censor canonical
    v5.1 research observations. No frozen v5.1 economic rule is changed.
    """
    global _INSTALLED
    from . import robinhood_runtime_install as module
    from .post182_production_proof_wiring_repair import (
        install_post183_production_proof_wiring_repair,
    )
    from .robinhood_chain_paper import RobinhoodChainPaperPlane

    # PR #185 binds proof/frontier behavior to the actual final live call sites.
    install_post183_production_proof_wiring_repair()

    # Items 1-10 measurement-integrity release: install canonical candidate ingress,
    # append-only stage lineage, v5.1 research observation ceilings, release
    # compatibility epochs and proof freshness before any lazy runtime construction.
    install_measurement_integrity()
    install_measurement_compatible_promotion_filters()

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
    app.state.roi_v51_measurement_integrity = True
    app.state.roi_v51_measurement_compatibility_filters = True
    app.state.roi_post183_production_proof_wiring = True
    app.state.roi_final_production_proof_readiness = True
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "composition_version": COMPOSITION_VERSION,
        "installed": _INSTALLED,
        "economic_authority_installation": "explicit_call_from_solana_roi.production_after_robinhood_transport_install",
        "measurement_integrity_installation": "explicit_same_production_boundary_before_lazy_runtime_construction",
        "measurement_integrity": measurement_status(),
        "measurement_compatibility_filters": compatibility_filter_status(),
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
