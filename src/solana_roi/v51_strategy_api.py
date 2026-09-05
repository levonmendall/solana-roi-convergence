from __future__ import annotations

import os
from typing import Any, Callable

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from .strategy_v51_authority import authority, authority_fingerprint
from .v51_candidate_pipeline import refresh_candidate_pipeline
from .v51_consolidated_strategy import status as consolidation_status
from .v51_dashboard import render_economic_dashboard
from .v51_economic_certification import build_economic_certification
from .v51_execution_stress_diagnostics import build_execution_mechanism_stress
from .v51_proof_merge import merge_candidate_coverages, merge_economic_certifications

API_VERSION = "v51-strategy-proof-api-v2"
_INSTALLED = False


def _runtime(provider: Any) -> Any:
    return provider() if callable(provider) else provider


def _release_commit() -> str | None:
    for key in ("RENDER_GIT_COMMIT", "SOLANA_ROI_RELEASE_COMMIT", "GIT_COMMIT"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return None


def _isolated_robinhood_proof(
    status_provider: Callable[[], dict[str, Any]] | None,
) -> dict[str, Any] | None:
    """Read only the already-cached Robinhood proof from the main API thread."""
    if status_provider is None:
        return None
    try:
        status = status_provider()
    except Exception:
        return None
    if not isinstance(status, dict):
        return None
    if bool(status.get("failed_closed")) or not bool(status.get("runtime_ready")):
        return None
    proof = status.get("v51_proof")
    if not isinstance(proof, dict) or not bool(proof.get("available")):
        return None
    return proof


def _merged_certification(
    runtime: Any,
    status_provider: Callable[[], dict[str, Any]] | None,
) -> dict[str, Any]:
    primary = build_economic_certification(runtime.store)
    proof = _isolated_robinhood_proof(status_provider)
    secondary = proof.get("economic_certification") if proof is not None else None
    return merge_economic_certifications(
        primary,
        secondary if isinstance(secondary, dict) else None,
    )


def _merged_coverage(
    runtime: Any,
    status_provider: Callable[[], dict[str, Any]] | None,
) -> dict[str, Any]:
    primary = refresh_candidate_pipeline(runtime.store)
    proof = _isolated_robinhood_proof(status_provider)
    robinhood = proof.get("candidate_coverage") if proof is not None else None
    payload = merge_candidate_coverages(
        primary,
        robinhood if isinstance(robinhood, dict) else None,
    )
    payload["robinhood_detection_coverage"] = (
        "every concrete forward-only Robinhood v2/v3 opportunity is registered before canonical strategy "
        "preselection; every candidate receives paper_enter or an explicit fail-closed paper_reject"
    )
    payload["robinhood_duplicate_provider_work_for_coverage"] = False
    return payload


def _merged_mechanism_stress(
    runtime: Any,
    status_provider: Callable[[], dict[str, Any]] | None,
) -> dict[str, Any]:
    primary = build_execution_mechanism_stress(runtime.store)
    proof = _isolated_robinhood_proof(status_provider)
    secondary = proof.get("execution_mechanism_stress") if proof is not None else None
    families = dict(primary.get("families") or {})
    if isinstance(secondary, dict):
        families.update(dict(secondary.get("families") or {}))
    return {
        **primary,
        "families": families,
        "robinhood_proof_available": isinstance(secondary, dict),
        "proof_transport": "canonical_store_plus_nonblocking_isolated_robinhood_cache",
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
        }

    @app.get("/v1/strategy/consolidation")
    def v51_consolidation() -> dict[str, Any]:
        runtime = _runtime(runtime_provider)
        payload = consolidation_status(runtime.store, _release_commit())
        payload["isolated_robinhood_proof_available"] = bool(
            _isolated_robinhood_proof(robinhood_status_provider)
        )
        payload["robinhood_proof_transport"] = "nonblocking_worker_status_cache"
        payload["economic_authority_composition"] = "explicit_solana_roi.production_boundary"
        payload["legacy_import_hook_has_economic_authority"] = False
        return payload

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
            "authority_id": payload["authority_id"],
            "economic_freeze_epoch": payload["economic_freeze_epoch"],
            "incremental_alpha": payload["incremental_alpha"],
            "robinhood_proof_available": payload.get("robinhood_proof_available", False),
            "paper_only": True,
            "live_money_authority": False,
        }

    @app.get("/v1/strategy/research-allocation")
    def v51_research_allocation() -> dict[str, Any]:
        runtime = _runtime(runtime_provider)
        payload = _merged_certification(runtime, robinhood_status_provider)
        return {
            "authority_id": payload["authority_id"],
            "economic_freeze_epoch": payload["economic_freeze_epoch"],
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
            "paper_only": True,
            "live_money_authority": False,
        }

    @app.get("/v1/strategy/execution-stress")
    def v51_execution_stress() -> dict[str, Any]:
        runtime = _runtime(runtime_provider)
        payload = _merged_certification(runtime, robinhood_status_provider)
        mechanisms = _merged_mechanism_stress(runtime, robinhood_status_provider)
        return {
            "authority_id": payload["authority_id"],
            "economic_freeze_epoch": payload["economic_freeze_epoch"],
            "stress_policy": payload["paper_live_boundary_stress_policy"],
            "family_stress": {
                key: value["execution_stress"] for key, value in payload["families"].items()
            },
            "mechanism_specific_stress": mechanisms,
            "robinhood_proof_available": payload.get("robinhood_proof_available", False),
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
    "_merged_certification",
    "_merged_coverage",
    "_merged_mechanism_stress",
    "install_v51_strategy_api",
]
