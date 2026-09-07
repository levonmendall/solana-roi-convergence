from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from .strategy_v51_authority import ECONOMIC_FREEZE_EPOCH
from .v51_batch6_production_proof_release_gate import (
    BATCH6_PROOF_VERSION,
    build_batch6_production_proof_gate,
)
from .v51_candidate_lane_accounting import build_five_lane_candidate_accounting
from .v51_measurement_integrity import MEASUREMENT_EPOCH
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


def _unavailable_batch6_gate(reason: str) -> dict[str, Any]:
    return {
        "batch6_production_proof_version": BATCH6_PROOF_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "proof_id": None,
        "pass": False,
        "verdict": "FAIL_CLOSED",
        "single_fail_closed_verdict": True,
        "missing_or_unknown_evidence_fails_closed": True,
        "economic_epoch": ECONOMIC_FREEZE_EPOCH,
        "measurement_epoch": MEASUREMENT_EPOCH,
        "batch6_starts_new_measurement_epoch": False,
        "assertions": {},
        "blockers": [reason],
        "artifact": {
            "table": "v51_batch6_production_proof_artifacts",
            "proof_id": None,
            "persisted": False,
            "reason": reason,
        },
        "read_only_strategy_authority": True,
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

    install_phase17_surface_attestation_hardening()
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
            runtime_obj = runtime_provider() if callable(runtime_provider) else runtime_provider
            proof = current_system_proof()
            if not isinstance(proof, dict):
                certificate = _insufficient_certificate()
                store = getattr(runtime_obj, "store", None)
                batch6_gate = (
                    build_batch6_production_proof_gate(
                        store,
                        system_proof={},
                        candidate_accounting={},
                        forward_certification={},
                    )
                    if store is not None
                    else _unavailable_batch6_gate("canonical_runtime_store_unavailable")
                )
                return {
                    "production_proof_api_version": PRODUCTION_PROOF_API_VERSION,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "state": "DEGRADED",
                    "ready_for_forward_proof": False,
                    "production_proof_pass": False,
                    "release": {},
                    "authority": {
                        "paper_only": True,
                        "live_money_authority": False,
                        "signing_available": False,
                        "transaction_submission_available": False,
                    },
                    "epochs": {
                        "economic_epoch": ECONOMIC_FREEZE_EPOCH,
                        "measurement_epoch": MEASUREMENT_EPOCH,
                        "batch6_starts_new_measurement_epoch": False,
                    },
                    "candidate_accounting": {},
                    "forward_certification": {},
                    "final_certification": certificate,
                    "batch6_release_gate": batch6_gate,
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
            five_lane_accounting = build_five_lane_candidate_accounting(
                runtime_obj.store,
                merged_coverage=coverage,
            )
            candidate_accounting = {
                "coverage_complete": bool(coverage.get("coverage_complete")),
                "coverage_debt_count": int(coverage.get("coverage_debt_count") or 0),
                "proof_state": coverage.get("proof_state"),
                "stage_summary": stage_summary,
                "robinhood": coverage.get("robinhood"),
                "accounting_version": five_lane_accounting.get("accounting_version"),
                "lane_accounting": five_lane_accounting.get("lanes"),
                "candidate_conservation": five_lane_accounting.get("candidate_conservation"),
                "classification_anomalies": five_lane_accounting.get("classification_anomalies"),
                "local_accounted_subtotal": five_lane_accounting.get("local_accounted_subtotal"),
                "accounting_scope": five_lane_accounting.get("scope"),
                "conservation_equation": five_lane_accounting.get("equation"),
                "full_candidate_coverage": coverage,
            }
            batch6_gate = build_batch6_production_proof_gate(
                runtime_obj.store,
                system_proof=proof,
                candidate_accounting=candidate_accounting,
                forward_certification=forward,
            )
            base_ready = bool(proof.get("ready_for_forward_proof"))
            ready_for_forward_proof = bool(base_ready and batch6_gate.get("pass"))
            resource_pressure = resource_pressure_snapshot()
            blockers = list(runtime.get("blockers") or [])
            blockers.extend(str(value) for value in (final.get("blockers") or []) if str(value) not in blockers)
            for value in batch6_gate.get("blockers") or []:
                blocker = f"batch6_release_gate:{value}"
                if blocker not in blockers:
                    blockers.append(blocker)
            if resource_pressure.get("state") == "critical" and "critical_resource_pressure" not in blockers:
                blockers.append("critical_resource_pressure")

            return {
                "production_proof_api_version": PRODUCTION_PROOF_API_VERSION,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "state": proof.get("state") if ready_for_forward_proof or not base_ready else "BLOCKED_BATCH6_RELEASE_GATE",
                "ready_for_forward_proof": ready_for_forward_proof,
                "production_proof_pass": ready_for_forward_proof,
                "underlying_system_proof_ready": base_ready,
                "release": release,
                "authority": authority,
                "epochs": {
                    "economic_epoch": batch6_gate.get("economic_epoch"),
                    "economic_freeze_epoch": authority.get("economic_freeze_epoch"),
                    "measurement_epoch": batch6_gate.get("measurement_epoch"),
                    "execution_model_epoch": final.get("execution_model_epoch"),
                    "batch6_starts_new_measurement_epoch": False,
                },
                "candidate_accounting": candidate_accounting,
                "forward_certification": forward,
                "final_certification": final,
                "batch6_release_gate": batch6_gate,
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
