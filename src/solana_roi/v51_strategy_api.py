from __future__ import annotations

import os
from typing import Any, Callable

from fastapi import FastAPI

from .strategy_v51_authority import authority, authority_fingerprint
from .v51_candidate_pipeline import refresh_candidate_pipeline
from .v51_consolidated_strategy import status as consolidation_status
from .v51_economic_certification import build_economic_certification
from .v51_proof_merge import merge_candidate_coverages, merge_economic_certifications

API_VERSION = "v51-strategy-proof-api-v1"
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
    # The worker-isolation status reader marks stale/dead snapshots failed closed.
    # Do not use an old proof merely because it remains present in the cached body.
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
        return payload

    @app.get("/v1/strategy/candidate-coverage")
    def v51_candidate_coverage() -> dict[str, Any]:
        runtime = _runtime(runtime_provider)
        primary = refresh_candidate_pipeline(runtime.store)
        proof = _isolated_robinhood_proof(robinhood_status_provider)
        robinhood = proof.get("candidate_coverage") if proof is not None else None
        payload = merge_candidate_coverages(
            primary,
            robinhood if isinstance(robinhood, dict) else None,
        )
        payload["robinhood_detection_coverage"] = (
            "canonical lane-selection candidates are attributed in the isolated Robinhood worker without a second polling path; "
            "preselection noise that never reaches entity/lifecycle/risk lane evaluation is not counted as an economic opportunity"
        )
        return payload

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
        return {
            "authority_id": payload["authority_id"],
            "economic_freeze_epoch": payload["economic_freeze_epoch"],
            "stress_policy": payload["paper_live_boundary_stress_policy"],
            "family_stress": {
                key: value["execution_stress"] for key, value in payload["families"].items()
            },
            "robinhood_proof_available": payload.get("robinhood_proof_available", False),
            "paper_only": True,
            "live_money_authority": False,
            "note": "stress evidence quantifies the paper-to-live execution gap; it does not grant live execution authority",
        }

    _INSTALLED = True


__all__ = [
    "API_VERSION",
    "_isolated_robinhood_proof",
    "_merged_certification",
    "install_v51_strategy_api",
]
