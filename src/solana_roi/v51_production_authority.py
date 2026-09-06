from __future__ import annotations

from typing import Any, Callable

from . import robinhood_live_frontier_verification_repair as robinhood_frontier
from .robinhood_decision_tail_repair import (
    install_robinhood_decision_tail_repair,
    status as decision_tail_status,
)
from .robinhood_live_getlogs_resilience import (
    install_robinhood_live_getlogs_resilience,
    status as live_getlogs_resilience_status,
)
from .robinhood_phase9_anchor_seed import (
    install_robinhood_phase9_anchor_seed,
    status as robinhood_phase9_anchor_status,
)
from .robinhood_production_provider_finalizer import (
    install_robinhood_production_provider_finalizer,
    status as production_provider_status,
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
from .v51_latency_challenger_api import install_v51_latency_challenger_api
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
from .v51_robinhood_phase9_65_69 import (
    install_robinhood_phase9_65_69,
    status as robinhood_phase9_status,
)
from .v51_strategy_api import install_v51_strategy_api

# Capture the already-proven final entry guard before the sequencer transport installs.
# Direct regression calls retain this helper. The actual running production instance
# is switched to provider/v5.1 event-time authority by the finalizer below.
_ORIGINAL_ROBINHOOD_FRESH_HEAD_READY = robinhood_frontier._fresh_head_ready

COMPOSITION_VERSION = "v51-explicit-production-authority-v2-robinhood-phase9-65-69"
_INSTALLED = False


def install_isolated_robinhood_proof_cache(module: Any) -> None:
    """Publish Robinhood proof without putting proof SQL back on the live event loop."""
    from . import robinhood_worker_isolation_repair as isolation

    current = isolation._ORIGINAL_STATUS
    if current is None or bool(getattr(current, "_roi_v51_isolated_proof", False)):
        return

    # v2 worker isolation refreshes proof on a separate SQLite connection/threadpool.
    # Preserve the historical wrapped-status contract by attaching only the already-
    # built snapshot here; this wrapper performs no proof DB work and is not used by
    # the fast status publisher, which calls isolation._BASE_STATUS directly.
    if getattr(isolation, "PROOF_PUBLISH_SECONDS", None) is not None and getattr(isolation, "_BASE_STATUS", None) is not None:
        def status_with_offloaded_v51_proof() -> dict[str, Any]:
            payload = dict(current())
            proof = isolation._current_proof_snapshot()
            if proof is None:
                payload["v51_proof"] = {
                    "available": False,
                    "reason": "isolated_robinhood_proof_snapshot_not_ready",
                    **proof_metadata(None, proof_state="unavailable"),
                }
            else:
                payload["v51_proof"] = proof
            return payload

        setattr(status_with_offloaded_v51_proof, "_roi_v51_isolated_proof", True)
        isolation._ORIGINAL_STATUS = status_with_offloaded_v51_proof
        module._STATE["v51_proof_publication"] = "separate_sqlite_connection_threadpool"
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

    # PR195 sequencer remains a research/compatibility substrate. It is installed
    # first so the provider finalizer wraps the true final run/status graph. Direct
    # tests keep the established fresh-head helper; only a real running instance is
    # switched to provider/v5.1 event-time authority.
    install_robinhood_sequencer_frontier(RobinhoodChainPaperPlane)
    robinhood_frontier._fresh_head_ready = _ORIGINAL_ROBINHOOD_FRESH_HEAD_READY  # type: ignore[assignment]
    install_robinhood_production_provider_finalizer(
        RobinhoodChainPaperPlane,
        legacy_fresh_ready=_ORIGINAL_ROBINHOOD_FRESH_HEAD_READY,
    )

    # The provider WebSocket runner bypasses the older forward-only poll loop, so
    # explicitly seed only the latest head plus the existing bounded 64-block factory
    # metadata insurance before the production subscription starts. No old swaps are
    # replayed and the archival cursor never regains readiness authority.
    install_robinhood_phase9_anchor_seed(RobinhoodChainPaperPlane)

    # Phase 9 tightens Robinhood runtime/evidence contracts without altering frozen
    # strategy economics. It retires historical-lag readiness semantics, validates
    # proof freshness at the nonblocking cache boundary, gives creation and reserve/
    # swap opportunities durable pre-lane identities, and makes venue/lifecycle
    # economic separation an explicit proof invariant.
    install_robinhood_phase9_65_69(RobinhoodChainPaperPlane, module)

    # Preserve historical architecture markers while making the provider wrapper the
    # final runtime implementation. No signer, transaction submission, or live-money
    # authority is introduced.
    setattr(RobinhoodChainPaperPlane.run, "_roi_robinhood_forward_only_run", True)

    install_isolated_robinhood_proof_cache(module)
    install_v51_strategy_api(
        app,
        runtime_provider,
        robinhood_status_provider=module._status,
    )
    install_v51_latency_challenger_api(
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
    app.state.roi_v51_latency_challenger_research = True
    app.state.roi_v51_forward_certification = True
    app.state.roi_v51_alpha_validation_47_58 = True
    app.state.roi_robinhood_live_getlogs_resilience = True
    app.state.roi_robinhood_decision_tail = True
    app.state.roi_robinhood_sequencer_frontier = True
    app.state.roi_robinhood_production_provider_finalizer = True
    app.state.roi_robinhood_phase9_anchor_seed = True
    app.state.roi_robinhood_phase9_65_69 = True
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
        "latency_challenger_installation": "read_only_rejected_counterfactual_research_over_existing_frozen_v51_evidence",
        "robinhood_live_getlogs_resilience": live_getlogs_resilience_status(),
        "robinhood_decision_tail": decision_tail_status(),
        "robinhood_sequencer_frontier": sequencer_frontier_status(),
        "robinhood_production_provider": production_provider_status(),
        "robinhood_phase9_anchor_seed": robinhood_phase9_anchor_status(),
        "robinhood_phase9_65_69": robinhood_phase9_status(),
        "robinhood_entry_freshness_checks": "running_worker_provider_v51_event_time; direct_test_calls_legacy_helper",
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
        "latency_challenger_changes_strategy_authority": False,
        "phase9_65_69_changes_strategy_authority": False,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "COMPOSITION_VERSION",
    "install_isolated_robinhood_proof_cache",
    "install_v51_production_authority",
    "status",
]
