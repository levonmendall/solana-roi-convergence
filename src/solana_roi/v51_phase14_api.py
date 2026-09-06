from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from .v51_phase14_profitability_certification import PHASE14_VERSION
from .v51_phase17_attestation_hardening import (
    ATTESTATION_HARDENING_VERSION,
    install_phase17_surface_attestation_hardening,
)
from .v51_phase17_context_certification import (
    PHASE17_VERSION,
    build_phase17_profitability_certification,
)
from .v51_resource_pressure import (
    RESOURCE_PRESSURE_VERSION,
    ensure_resource_pressure_sampler,
    resource_pressure_snapshot,
)
from .v51_strategy_api import _isolated_robinhood_proof_state


API_VERSION = "v51-phase17-context-certification-api-v3"
PRODUCTION_PROOF_API_VERSION = "v51-canonical-production-proof-v1"


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _attestation_policy() -> dict[str, Any]:
    return {
        "version": ATTESTATION_HARDENING_VERSION,
        "surface_scoped_attestation_required": True,
        "aggregate_attestation_fallback_allowed": False,
    }


def _insufficient_certificate() -> dict[str, Any]:
    return {
        "phase14_version": PHASE14_VERSION,
        "phase17_version": PHASE17_VERSION,
        "api_version": API_VERSION,
        "classification": "INSUFFICIENT_EVIDENCE",
        "economically_promising": False,
        "production_proven": False,
        "system_certification_pass": False,
        "blockers": ["canonical_system_proof_cache_unavailable"],
        "surface_attestation_hardening_version": ATTESTATION_HARDENING_VERSION,
        "surface_scoped_attestation_required": True,
        "aggregate_attestation_fallback_allowed": False,
        "changes_strategy_authority": False,
        "changes_economic_thresholds": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def install_phase14_profitability_certification(
    app: Any,
    runtime_provider: Callable[[], Any] | Any,
    *,
    robinhood_status_provider: Callable[[], dict[str, Any]] | None = None,
) -> None:
    if bool(getattr(app.state, "roi_v51_phase14_95_102", False)):
        return

    # Current-release certification is surface-scoped. An aggregate attestation can
    # no longer substitute for missing Solana, FOMO, or Robinhood evidence.
    install_phase17_surface_attestation_hardening()
    # Resource sampling is read-only, bounded, and has no strategy/economic authority.
    ensure_resource_pressure_sampler()

    def current_system_proof() -> dict[str, Any] | None:
        proof_provider = getattr(app.state, "roi_v51_system_proof_precompute", None)
        if not callable(proof_provider):
            return None
        proof = proof_provider()
        return dict(proof) if isinstance(proof, dict) else None

    def current_certificate(system_proof: dict[str, Any] | None = None) -> dict[str, Any]:
        runtime = runtime_provider() if callable(runtime_provider) else runtime_provider
        proof = system_proof if isinstance(system_proof, dict) else current_system_proof()
        if not isinstance(proof, dict):
            return _insufficient_certificate()

        strategy = _dict(proof.get("strategy_evidence"))
        promotion = _dict(strategy.get("promotion_certification"))
        forward = _dict(strategy.get("forward_certification"))
        coverage = _dict(proof.get("candidate_coverage"))
        operations = _dict(proof.get("operations_proof"))
        robinhood_proof, robinhood_proof_state = _isolated_robinhood_proof_state(
            robinhood_status_provider
        )
        certificate = build_phase17_profitability_certification(
            runtime.store,
            promotion_certification=promotion,
            candidate_coverage=coverage,
            forward_certification=forward,
            operations_proof=operations,
            robinhood_proof=robinhood_proof,
        )
        certificate.update(
            {
                "api_version": API_VERSION,
                "canonical_system_proof_path": "/v1/system-proof",
                "canonical_production_proof_path": "/v1/strategy/production-proof",
                "canonical_system_proof_state": proof.get("state"),
                "canonical_system_proof_release": proof.get("release"),
                "system_proof_cache": proof.get("system_proof_cache"),
                "robinhood_proof_state": robinhood_proof_state,
                "surface_attestation_hardening_version": ATTESTATION_HARDENING_VERSION,
                "surface_scoped_attestation_required": True,
                "aggregate_attestation_fallback_allowed": False,
                "certificate_is_read_only": True,
            }
        )
        return certificate

    existing = {getattr(route, "path", None) for route in app.routes}
    if "/v1/strategy/final-certification" not in existing:

        @app.get("/v1/strategy/final-certification")
        def phase14_final_certification() -> dict[str, Any]:
            return current_certificate()

    if "/v1/strategy/production-proof" not in existing:

        @app.get("/v1/strategy/production-proof")
        def canonical_production_proof() -> dict[str, Any]:
            proof = current_system_proof()
            if not isinstance(proof, dict):
                certificate = _insufficient_certificate()
                return {
                    "production_proof_api_version": PRODUCTION_PROOF_API_VERSION,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "state": "DEGRADED",
                    "ready_for_forward_proof": False,
                    "release": {},
                    "authority": {
                        "paper_only": True,
                        "live_money_authority": False,
                        "signing_available": False,
                        "transaction_submission_available": False,
                    },
                    "candidate_accounting": {},
                    "forward_certification": {},
                    "final_certification": certificate,
                    "worker_readiness": {},
                    "resource_pressure": resource_pressure_snapshot(),
                    "surface_attestation_policy": _attestation_policy(),
                    "resource_pressure_version": RESOURCE_PRESSURE_VERSION,
                    "blockers": ["canonical_system_proof_cache_unavailable"],
                    "read_only_observability": True,
                    "changes_strategy_authority": False,
                    "changes_economic_thresholds": False,
                    "paper_only": True,
                    "live_money_authority": False,
                    "signing_available": False,
                    "transaction_submission_available": False,
                }

            strategy = _dict(proof.get("strategy_evidence"))
            forward = _dict(strategy.get("forward_certification"))
            coverage = _dict(proof.get("candidate_coverage"))
            operations = _dict(proof.get("operations_proof"))
            runtime = _dict(proof.get("runtime"))
            final = current_certificate(proof)
            release = _dict(proof.get("release"))
            authority = _dict(proof.get("authority"))
            stage_summary = _dict(coverage.get("stage_summary"))
            resource_pressure = resource_pressure_snapshot()
            blockers = list(runtime.get("blockers") or [])
            blockers.extend(str(value) for value in (final.get("blockers") or []) if str(value) not in blockers)
            if resource_pressure.get("state") == "critical" and "critical_resource_pressure" not in blockers:
                blockers.append("critical_resource_pressure")

            return {
                "production_proof_api_version": PRODUCTION_PROOF_API_VERSION,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "state": proof.get("state"),
                "ready_for_forward_proof": bool(proof.get("ready_for_forward_proof")),
                "release": release,
                "authority": authority,
                "epochs": {
                    "economic_freeze_epoch": authority.get("economic_freeze_epoch"),
                    "measurement_epoch": final.get("measurement_epoch"),
                    "execution_model_epoch": final.get("execution_model_epoch"),
                },
                "candidate_accounting": {
                    "coverage_complete": bool(coverage.get("coverage_complete")),
                    "coverage_debt_count": int(coverage.get("coverage_debt_count") or 0),
                    "proof_state": coverage.get("proof_state"),
                    "stage_summary": stage_summary,
                    "robinhood": coverage.get("robinhood"),
                    "full_candidate_coverage": coverage,
                },
                "forward_certification": forward,
                "final_certification": final,
                "worker_readiness": {
                    "surfaces": runtime.get("surfaces"),
                    "forward_certification_state": runtime.get("forward_certification_state"),
                    "hard_operational_gates_ok": runtime.get("hard_operational_gates_ok"),
                    "backpressure": operations.get("backpressure"),
                    "continuity": operations.get("continuity"),
                    "resource_attribution": operations.get("resource_attribution"),
                },
                "resource_pressure": resource_pressure,
                "surface_attestation_policy": _attestation_policy(),
                "resource_pressure_version": RESOURCE_PRESSURE_VERSION,
                "blockers": blockers,
                "read_only_observability": True,
                "changes_strategy_authority": False,
                "changes_economic_thresholds": False,
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
            }

    app.state.roi_v51_phase14_95_102 = True
    app.state.roi_v51_phase14_final_certification_path = "/v1/strategy/final-certification"
    app.state.roi_v51_production_proof_path = "/v1/strategy/production-proof"
    app.state.roi_v51_phase14_version = PHASE14_VERSION
    app.state.roi_v51_phase17_version = PHASE17_VERSION


__all__ = [
    "API_VERSION",
    "PRODUCTION_PROOF_API_VERSION",
    "install_phase14_profitability_certification",
]
