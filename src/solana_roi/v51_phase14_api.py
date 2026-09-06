from __future__ import annotations

from typing import Any, Callable

from .v51_phase14_profitability_certification import (
    PHASE14_VERSION,
    build_phase14_profitability_certification,
)
from .v51_strategy_api import _isolated_robinhood_proof_state


API_VERSION = "v51-phase14-final-certification-api-v1"


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def install_phase14_profitability_certification(
    app: Any,
    runtime_provider: Callable[[], Any] | Any,
    *,
    robinhood_status_provider: Callable[[], dict[str, Any]] | None = None,
) -> None:
    if bool(getattr(app.state, "roi_v51_phase14_95_102", False)):
        return

    existing = {getattr(route, "path", None) for route in app.routes}
    if "/v1/strategy/final-certification" not in existing:

        @app.get("/v1/strategy/final-certification")
        def phase14_final_certification() -> dict[str, Any]:
            runtime = runtime_provider() if callable(runtime_provider) else runtime_provider
            proof_provider = getattr(app.state, "roi_v51_system_proof_precompute", None)
            if not callable(proof_provider):
                return {
                    "phase14_version": PHASE14_VERSION,
                    "api_version": API_VERSION,
                    "classification": "INSUFFICIENT_EVIDENCE",
                    "economically_promising": False,
                    "production_proven": False,
                    "blockers": ["canonical_system_proof_cache_unavailable"],
                    "changes_strategy_authority": False,
                    "changes_economic_thresholds": False,
                    "paper_only": True,
                    "live_money_authority": False,
                    "signing_available": False,
                    "transaction_submission_available": False,
                }

            system_proof = proof_provider()
            strategy = _dict(system_proof.get("strategy_evidence"))
            promotion = _dict(strategy.get("promotion_certification"))
            forward = _dict(strategy.get("forward_certification"))
            coverage = _dict(system_proof.get("candidate_coverage"))
            operations = _dict(system_proof.get("operations_proof"))
            robinhood_proof, robinhood_proof_state = _isolated_robinhood_proof_state(
                robinhood_status_provider
            )
            certificate = build_phase14_profitability_certification(
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
                    "canonical_system_proof_state": system_proof.get("state"),
                    "canonical_system_proof_release": system_proof.get("release"),
                    "system_proof_cache": system_proof.get("system_proof_cache"),
                    "robinhood_proof_state": robinhood_proof_state,
                    "certificate_is_read_only": True,
                }
            )
            return certificate

    app.state.roi_v51_phase14_95_102 = True
    app.state.roi_v51_phase14_final_certification_path = "/v1/strategy/final-certification"
    app.state.roi_v51_phase14_version = PHASE14_VERSION


__all__ = ["API_VERSION", "install_phase14_profitability_certification"]
