from __future__ import annotations

from typing import Any, Callable

from .robinhood_decision_tail_repair import (
    install_robinhood_decision_tail_repair,
    status as decision_tail_status,
)
from .robinhood_live_getlogs_resilience import (
    install_robinhood_live_getlogs_resilience,
    status as live_getlogs_resilience_status,
)
from .robinhood_sequencer_frontier_repair import (
    install_robinhood_sequencer_frontier,
    status as sequencer_frontier_status,
)
from .v51_alpha_validation import install_alpha_validation
from .v51_attestation_sources import install_primary_attestation_sources, status as attestation_source_status
from .v51_consolidated_strategy import install_v51_consolidated_strategy
from .v51_cost_normalization import install_api_cost_normalization, status as cost_normalization_status
from .v51_empty_epoch_slo_repair import install_empty_epoch_slo_repair, status as empty_epoch_slo_status
from .v51_forward_certification import install_forward_certification
from .v51_measurement_compatibility_filters import (
    install_measurement_compatible_promotion_filters,
    status as compatibility_filter_status,
)
from .v51_measurement_integrity import install_measurement_integrity, proof_metadata, status as measurement_status
from .v51_measurement_integrity_hardening import (
    install_measurement_integrity_hardening,
    status as measurement_hardening_status,
)
from .v51_promotion_proof import install_release_attestation_gate, status as promotion_proof_status
from .v51_robinhood_candidate_coverage import install_v51_robinhood_candidate_coverage
from .v51_robinhood_consolidation import install_v51_robinhood_consolidation
from .v51_strategy_api import install_v51_strategy_api

COMPOSITION_VERSION = "v51-explicit-production-authority-v1"
_INSTALLED = False


def install_isolated_robinhood_proof_cache(module: Any) -> None:
    """Compatibility hook for proof publication from the isolated Robinhood store."""
    from . import robinhood_worker_isolation_repair as isolation

    # v2 worker isolation publishes proof on a separate SQLite connection/thread.
    # Do not wrap the fast live-status function with proof SQL again.
    if getattr(isolation, "PROOF_PUBLISH_SECONDS", None) is not None and getattr(isolation, "_BASE_STATUS", None) is not None:
        module._STATE["v51_proof_publication"] = "separate_sqlite_connection_threadpool"
        return

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
    """Install frozen v5.1 economics explicitly at the production boundary."""
    global _INSTALLED
    from . import robinhood_runtime_install as module
    from .post182_production_proof_wiring_repair import (
        install_post183_production_proof_wiring_repair,
    )
    from .robinhood_chain_paper import RobinhoodChainPaperPlane

    install_robinhood_live_getlogs_resilience()
    install_robinhood_decision_tail_repair()
    install_post183_production_proof_wiring_repair()
    install_measurement_integrity()
    install_measurement_integrity_hardening()
    install_release_attestation_gate()
    install_primary_attestation_sources()
    install_measurement_compatible_promotion_filters()
    install_api_cost_normalization()
    install_empty_epoch_slo_repair()

    install_v51_consolidated_strategy()
    install_v51_robinhood_consolidation()
    install_v51_robinhood_candidate_coverage(RobinhoodChainPaperPlane)
    # Final transport layer: stream the official public sequencer head continuously
    # and acquire only exact-current-block relevant logs. It changes no economic gate.
    install_robinhood_sequencer_frontier(RobinhoodChainPaperPlane)
    install_isolated_robinhood_proof_cache(module)
    install_v51_strategy_api(
        app,
        runtime_provider,
        robinhood_status_provider=module._status,
    )
    install_forward_certification(
        app,
        runtime_provider=runtime_provider,
        robinhood_status_provider=module._status,
    )
    install_alpha_validation(
        app,
        runtime_provider=runtime_provider,
        robinhood_status_provider=module._status,
    )
    app.state.roi_v51_final_economic_authority = True
    app.state.roi_v51_economic_composition = COMPOSITION_VERSION
    app.state.roi_v51_economic_composition_explicit = True
    app.state.roi_v51_measurement_integrity = True
    app.state.roi_v51_release_attestation_gate = True
    app.state.roi_v51_primary_attestation_sources = True
    app.state.roi_v51_measurement_compatibility_filters = True
    app.state.roi_v51_cost_normalization = True
    app.state.roi_v51_empty_epoch_slo = True
    app.state.roi_v51_forward_certification = True
    app.state.roi_v51_alpha_validation_47_58 = True
    app.state.roi_robinhood_live_getlogs_resilience = True
    app.state.roi_robinhood_decision_tail = True
    app.state.roi_robinhood_sequencer_frontier = True
    app.state.roi_post183_production_proof_wiring = True
    app.state.roi_final_production_proof_readiness = True
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "composition_version": COMPOSITION_VERSION,
        "installed": _INSTALLED,
        "economic_authority_installation": "explicit_call_from_solana_roi.production_after_robinhood_transport_install",
        "measurement_integrity_installation": "separate_compatibility_plane_at_same_explicit_production_boundary",
        "forward_certification_installation": "read_only_cross_surface_composition_of_existing_transport_and_evidence_proof_planes",
        "alpha_validation_47_58_installation": "read_only_prospective_alpha_certificate_over_existing_frozen_v51_claims",
        "robinhood_live_getlogs_resilience": live_getlogs_resilience_status(),
        "robinhood_decision_tail": decision_tail_status(),
        "robinhood_sequencer_frontier": sequencer_frontier_status(),
        "robinhood_proof_refresh": "separate_sqlite_connection_threadpool_not_live_frontier",
        "empty_epoch_forward_slo": empty_epoch_slo_status(),
        "measurement_integrity": measurement_status(),
        "measurement_integrity_hardening": measurement_hardening_status(),
        "live_release_attestation": promotion_proof_status(),
        "attestation_sources": attestation_source_status(),
        "measurement_compatibility_filters": compatibility_filter_status(),
        "execution_cost_normalization": cost_normalization_status(),
        "proof_readiness_prepared_before_robinhood_transport": True,
        "post183_production_proof_wiring": True,
        "legacy_repair_modules_are_final_economic_authority": False,
        "forward_certification_changes_strategy_authority": False,
        "alpha_validation_changes_strategy_authority": False,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "COMPOSITION_VERSION",
    "install_isolated_robinhood_proof_cache",
    "install_v51_production_authority",
    "status",
]
