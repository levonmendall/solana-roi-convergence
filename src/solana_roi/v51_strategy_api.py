from __future__ import annotations

import os
from typing import Any, Callable

from fastapi import FastAPI

from .strategy_v51_authority import authority, authority_fingerprint
from .v51_candidate_pipeline import refresh_candidate_pipeline
from .v51_consolidated_strategy import status as consolidation_status
from .v51_economic_certification import build_economic_certification

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


def install_v51_strategy_api(app: FastAPI, runtime_provider: Callable[[], Any] | Any) -> None:
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
        return consolidation_status(runtime.store, _release_commit())

    @app.get("/v1/strategy/candidate-coverage")
    def v51_candidate_coverage() -> dict[str, Any]:
        runtime = _runtime(runtime_provider)
        return refresh_candidate_pipeline(runtime.store)

    @app.get("/v1/strategy/economic-certification")
    def v51_economic_certification() -> dict[str, Any]:
        runtime = _runtime(runtime_provider)
        return build_economic_certification(runtime.store)

    @app.get("/v1/strategy/incremental-alpha")
    def v51_incremental_alpha() -> dict[str, Any]:
        runtime = _runtime(runtime_provider)
        payload = build_economic_certification(runtime.store)
        return {
            "authority_id": payload["authority_id"],
            "economic_freeze_epoch": payload["economic_freeze_epoch"],
            "incremental_alpha": payload["incremental_alpha"],
            "paper_only": True,
            "live_money_authority": False,
        }

    @app.get("/v1/strategy/research-allocation")
    def v51_research_allocation() -> dict[str, Any]:
        runtime = _runtime(runtime_provider)
        payload = build_economic_certification(runtime.store)
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
            "paper_only": True,
            "live_money_authority": False,
        }

    @app.get("/v1/strategy/execution-stress")
    def v51_execution_stress() -> dict[str, Any]:
        runtime = _runtime(runtime_provider)
        payload = build_economic_certification(runtime.store)
        return {
            "authority_id": payload["authority_id"],
            "economic_freeze_epoch": payload["economic_freeze_epoch"],
            "stress_policy": payload["paper_live_boundary_stress_policy"],
            "family_stress": {key: value["execution_stress"] for key, value in payload["families"].items()},
            "paper_only": True,
            "live_money_authority": False,
            "note": "stress evidence quantifies the paper-to-live execution gap; it does not grant live execution authority",
        }

    _INSTALLED = True


__all__ = ["API_VERSION", "install_v51_strategy_api"]
