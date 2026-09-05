from __future__ import annotations

import os
from typing import Any, Callable

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from .strategy_v51_authority import authority, authority_fingerprint
from .v51_candidate_ledger import refresh_candidate_pipeline
from .v51_consolidated_strategy import status as consolidation_status
from .v51_dashboard import render_economic_dashboard
from .v51_economic_certification import build_economic_certification
from .v51_execution_stress_diagnostics import build_execution_mechanism_stress
from .v51_measurement_integrity import (
    cached_proof_state,
    decorate_proof,
    proof_age_seconds,
    proof_metadata,
    status as measurement_status,
)
from .v51_proof_merge import merge_candidate_coverages, merge_economic_certifications

API_VERSION = "v51-strategy-proof-api-v3-measurement-integrity"
_INSTALLED = False


def _runtime(provider: Any) -> Any:
    return provider() if callable(provider) else provider


def _release_commit() -> str | None:
    for key in ("RENDER_GIT_COMMIT", "SOLANA_ROI_RELEASE_COMMIT", "GIT_COMMIT"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return None


def _isolated_robinhood_proof_state(
    status_provider: Callable[[], dict[str, Any]] | None,
) -> tuple[dict[str, Any] | None, str]:
    """Read only the already-cached Robinhood proof from the main API thread."""
    if status_provider is None:
        return None, "unavailable"
    try:
        status = status_provider()
    except Exception:
        return None, "unavailable"
    if not isinstance(status, dict):
        return None, "unavailable"
    if bool(status.get("failed_closed")) or not bool(status.get("runtime_ready")):
        return None, "unavailable"
    proof = status.get("v51_proof")
    if not isinstance(proof, dict) or not bool(proof.get("available")):
        return None, "unavailable"
    snapshot = dict(proof)
    state = cached_proof_state(snapshot)
    snapshot["proof_state"] = state
    snapshot["proof_age_seconds"] = proof_age_seconds(snapshot)
    if state not in {"confirmed", "partial"}:
        return None, state
    return snapshot, state


def _isolated_robinhood_proof(
    status_provider: Callable[[], dict[str, Any]] | None,
) -> dict[str, Any] | None:
    proof, _state = _isolated_robinhood_proof_state(status_provider)
    return proof


def _merged_certification(
    runtime: Any,
    status_provider: Callable[[], dict[str, Any]] | None,
) -> dict[str, Any]:
    primary = build_economic_certification(runtime.store)
    proof, rh_state = _isolated_robinhood_proof_state(status_provider)
    secondary = proof.get("economic_certification") if proof is not None else None
    payload = merge_economic_certifications(
        primary,
        secondary if isinstance(secondary, dict) else None,
    )
    payload["robinhood_proof_state"] = rh_state
    state = "confirmed" if bool(payload.get("robinhood_proof_available")) and rh_state == "confirmed" else "partial"
    return decorate_proof(payload, runtime.store, proof_state=state)


def _robinhood_coverage_text(state: str, coverage_complete: bool) -> str:
    if state == "confirmed" and coverage_complete:
        return (
            "confirmed: every concrete forward-only Robinhood v2/v3 opportunity is registered before canonical "
            "strategy preselection and receives a terminal paper action or explicit fail-closed rejection"
        )
    if state == "stale":
        return "stale: isolated Robinhood candidate proof exceeded its freshness limit; unified coverage fails closed"
    if state == "epoch_mismatch":
        return "epoch_mismatch: isolated Robinhood proof is not measurement/execution compatible with the current release"
    if state == "invalid_measurement_epoch":
        return "invalid_measurement_epoch: Robinhood proof belongs to a release excluded from promotion authority"
    if state == "partial":
        return "partial: Robinhood proof is available but not sufficient to claim complete candidate coverage"
    return "unavailable: isolated Robinhood candidate proof is not currently available; unified coverage fails closed"


def _merged_coverage(
    runtime: Any,
    status_provider: Callable[[], dict[str, Any]] | None,
) -> dict[str, Any]:
    primary = refresh_candidate_pipeline(runtime.store)
    proof, rh_state = _isolated_robinhood_proof_state(status_provider)
    robinhood = proof.get("candidate_coverage") if proof is not None else None
    payload = merge_candidate_coverages(
        primary,
        robinhood if isinstance(robinhood, dict) else None,
    )
    payload["robinhood_proof_state"] = rh_state
    payload["robinhood_detection_coverage"] = _robinhood_coverage_text(
        rh_state,
        bool(payload.get("robinhood_proof_available")) and bool((payload.get("robinhood") or {}).get("coverage_complete")),
    )
    payload["robinhood_duplicate_provider_work_for_coverage"] = False
    overall = "confirmed" if bool(payload.get("coverage_complete")) and rh_state == "confirmed" else "partial"
    return decorate_proof(payload, runtime.store, proof_state=overall)


def _merged_mechanism_stress(
    runtime: Any,
    status_provider: Callable[[], dict[str, Any]] | None,
) -> dict[str, Any]:
    primary = build_execution_mechanism_stress(runtime.store)
    proof, rh_state = _isolated_robinhood_proof_state(status_provider)
    secondary = proof.get("execution_mechanism_stress") if proof is not None else None
    families = dict(primary.get("families") or {})
    if isinstance(secondary, dict):
        families.update(dict(secondary.get("families") or {}))
    payload = {
        **primary,
        "families": families,
        "robinhood_proof_available": isinstance(secondary, dict),
        "robinhood_proof_state": rh_state,
        "proof_transport": "canonical_store_plus_nonblocking_isolated_robinhood_cache",
    }
    return decorate_proof(payload, runtime.store, proof_state="confirmed" if rh_state == "confirmed" else "partial")


def _proof_meta_subset(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        key: payload.get(key)
        for key in (
            "generated_at",
            "proof_age_seconds",
            "evidence_through",
            "release_commit",
            "authority_id",
            "economic_freeze_epoch",
            "measurement_epoch",
            "execution_model_epoch",
            "measurement_fingerprint",
            "execution_model_fingerprint",
            "proof_state",
            "proof_max_age_seconds",
            "promotion_eligible_measurement",
        )
    }


def install_v51_strategy_api(
    app: FastAPI,
    runtime_provider: Callable[[], Any] | Any,
    *,
    robinhood_status_provider: Callable[[], dict[str, Any]] | None = None,
) -> None:
    global _INSTALLED
    if _INSTALLED:
        return

    @app.get("/v1/strategy/authority")
    def v51_authority() -> dict[str, Any]:
        return {
            **authority(),
            "authority_fingerprint": authority_fingerprint(),
            "api_version": API_VERSION,
            "canonical": True,
            "measurement_integrity": measurement_status(),
            **proof_metadata(None, proof_state="confirmed"),
        }

    @app.get("/v1/strategy/consolidation")
    def v51_consolidation() -> dict[str, Any]:
        runtime = _runtime(runtime_provider)
        payload = consolidation_status(runtime.store, _release_commit())
        proof, rh_state = _isolated_robinhood_proof_state(robinhood_status_provider)
        payload["isolated_robinhood_proof_available"] = proof is not None
        payload["robinhood_proof_state"] = rh_state
        payload["robinhood_proof_transport"] = "nonblocking_worker_status_cache"
        payload["economic_authority_composition"] = "explicit_solana_roi.production_boundary"
        payload["legacy_import_hook_has_economic_authority"] = False
        payload["measurement_integrity"] = measurement_status(runtime.store)
        return decorate_proof(payload, runtime.store, proof_state="confirmed" if rh_state == "confirmed" else "partial")

    @app.get("/v1/strategy/candidate-coverage")
    def v51_candidate_coverage() -> dict[str, Any]:
        runtime = _runtime(runtime_provider)
        return _merged_coverage(runtime, robinhood_status_provider)

    @app.get("/v1/strategy/economic-certification")
    def v51_economic_certification() -> dict[str, Any]:
        runtime = _runtime(runtime_provider)
        return _merged_certification(runtime, robinhood_status_provider)

    @app.get("/v1/strategy/incremental-alpha")
    def v51_incremental_alpha() -> dict[str, Any]:
        runtime = _runtime(runtime_provider)
        payload = _merged_certification(runtime, robinhood_status_provider)
        return {
            **_proof_meta_subset(payload),
            "incremental_alpha": payload["incremental_alpha"],
            "robinhood_proof_available": payload.get("robinhood_proof_available", False),
            "robinhood_proof_state": payload.get("robinhood_proof_state"),
            "paper_only": True,
            "live_money_authority": False,
        }

    @app.get("/v1/strategy/research-allocation")
    def v51_research_allocation() -> dict[str, Any]:
        runtime = _runtime(runtime_provider)
        payload = _merged_certification(runtime, robinhood_status_provider)
        return {
            **_proof_meta_subset(payload),
            "research_family_ranking": payload["research_family_ranking"],
            "paper_allocation_weights": payload["paper_allocation_weights"],
            "paper_cash_weight": payload["paper_cash_weight"],
            "families": {
                key: {
                    "independent_event_count": value["independent_event_count"],
                    "capital_efficiency_score": value["capital_efficiency_score"],
                    "promotion_kill_profile": value["promotion_kill_profile"],
                }
                for key, value in payload["families"].items()
            },
            "robinhood_proof_available": payload.get("robinhood_proof_available", False),
            "robinhood_proof_state": payload.get("robinhood_proof_state"),
            "paper_only": True,
            "live_money_authority": False,
        }

    @app.get("/v1/strategy/execution-stress")
    def v51_execution_stress() -> dict[str, Any]:
        runtime = _runtime(runtime_provider)
        payload = _merged_certification(runtime, robinhood_status_provider)
        mechanisms = _merged_mechanism_stress(runtime, robinhood_status_provider)
        return {
            **_proof_meta_subset(payload),
            "stress_policy": payload["paper_live_boundary_stress_policy"],
            "family_stress": {
                key: value["execution_stress"] for key, value in payload["families"].items()
            },
            "mechanism_specific_stress": mechanisms,
            "robinhood_proof_available": payload.get("robinhood_proof_available", False),
            "robinhood_proof_state": payload.get("robinhood_proof_state"),
            "paper_only": True,
            "live_money_authority": False,
            "note": "stress evidence quantifies the paper-to-live execution gap; it does not grant live execution authority",
        }

    @app.get("/v1/strategy/economic-dashboard", response_class=HTMLResponse)
    def v51_economic_dashboard() -> HTMLResponse:
        runtime = _runtime(runtime_provider)
        certification = _merged_certification(runtime, robinhood_status_provider)
        coverage = _merged_coverage(runtime, robinhood_status_provider)
        mechanisms = _merged_mechanism_stress(runtime, robinhood_status_provider)
        return HTMLResponse(render_economic_dashboard(certification, coverage, mechanisms))

    _INSTALLED = True


__all__ = [
    "API_VERSION",
    "_isolated_robinhood_proof",
    "_isolated_robinhood_proof_state",
    "_merged_certification",
    "_merged_coverage",
    "_merged_mechanism_stress",
    "install_v51_strategy_api",
]
