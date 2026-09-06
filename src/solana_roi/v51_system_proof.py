from __future__ import annotations

import copy
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

from fastapi.responses import HTMLResponse, JSONResponse

from .api import legacy_webhook_worker_enabled
from .strategy_v51_authority import authority, authority_fingerprint
from .v51_candidate_ledger import refresh_candidate_pipeline
from .v51_dashboard import render_economic_dashboard
from .v51_evidence_analytics import (
    build_forward_proof_slo,
    build_hazard_calibration,
    build_portfolio_reconciliation,
    refresh_execution_cost_ledger,
    refresh_rejected_counterfactuals,
)
from .v51_forward_certification import _e2e_status, build_forward_certification
from .v51_phase10_proof import PHASE10_VERSION, dashboard_funnel, family_proof_confidence
from .v51_phase12_13_operations import OPERATIONS_VERSION, build_operations_proof
from .v51_schema_cache import install_v51_schema_cache, stats as schema_cache_stats
from .v51_strategy_api import (
    _isolated_robinhood_proof_state,
    _merged_certification,
    _merged_coverage,
    _merged_mechanism_stress,
    _merged_promotion_certification,
)

SYSTEM_PROOF_VERSION = "v51-canonical-system-proof-v3-phase13-precompute-schema-cache"
SYSTEM_STATES = (
    "READY_FOR_FORWARD_PROOF",
    "DEGRADED",
    "INSUFFICIENT_EVIDENCE",
    "INVALID_MEASUREMENT_EPOCH",
)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _expected_release() -> str | None:
    for key in ("RENDER_GIT_COMMIT", "SOLANA_ROI_RELEASE_COMMIT", "GIT_COMMIT"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return None


def _invalid_epoch_state(*payloads: dict[str, Any]) -> bool:
    invalid = {"invalid_measurement_epoch", "epoch_mismatch"}
    for payload in payloads:
        for key in ("proof_state", "robinhood_proof_state", "state"):
            if str(payload.get(key) or "").lower() in invalid:
                return True
        blockers = [str(item).lower() for item in (payload.get("blockers") or [])]
        if any("invalid_measurement_epoch" in item or "epoch_mismatch" in item for item in blockers):
            return True
    return False


def system_state(
    *,
    forward_certification: dict[str, Any],
    coverage: dict[str, Any],
    certification: dict[str, Any],
    promotion: dict[str, Any],
    operations_healthy: bool = True,
) -> str:
    if _invalid_epoch_state(forward_certification, coverage, certification, promotion):
        return "INVALID_MEASUREMENT_EPOCH"
    if not operations_healthy:
        return "DEGRADED"
    if bool(forward_certification.get("system_forward_certified")):
        return "READY_FOR_FORWARD_PROOF"
    if bool(forward_certification.get("hard_operational_gates_ok")):
        return "INSUFFICIENT_EVIDENCE"
    return "DEGRADED"


def _settlement_section(
    local_coverage: dict[str, Any],
    certification: dict[str, Any],
    local_slo: dict[str, Any],
    robinhood_proof: dict[str, Any] | None,
) -> dict[str, Any]:
    summary = _dict(local_coverage.get("stage_summary"))
    local_settled = 0
    local_pending = 0
    for surface in summary.values():
        settlement = _dict(_dict(surface).get("settlement"))
        local_settled += int(settlement.get("complete") or 0)
        local_pending += int(settlement.get("pending") or 0)
    rh_slo = _dict(_dict(robinhood_proof).get("forward_proof_slo"))
    return {
        "closed_outcome_count": int(certification.get("closed_outcome_count") or 0),
        "local_candidate_settlement_complete": local_settled,
        "local_candidate_settlement_pending": max(local_pending, int(local_slo.get("pending_settlement_count") or 0)),
        "robinhood_pending_settlement_count": int(rh_slo.get("pending_settlement_count") or 0),
        "settlement_is_paper_only": True,
        "live_money_authority": False,
    }


def build_system_proof(
    app: Any,
    runtime: Any,
    *,
    robinhood_status_provider: Callable[[], dict[str, Any]] | None = None,
    http_metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    proof_started = time.perf_counter()
    expected_release = _expected_release()
    unified = _e2e_status(app)
    local_coverage = refresh_candidate_pipeline(runtime.store)
    coverage = _merged_coverage(runtime, robinhood_status_provider)
    certification = _merged_certification(runtime, robinhood_status_provider)
    promotion = _merged_promotion_certification(runtime, robinhood_status_provider)
    mechanism_stress = _merged_mechanism_stress(runtime, robinhood_status_provider)
    proof, robinhood_proof_state = _isolated_robinhood_proof_state(robinhood_status_provider)
    robinhood_status: dict[str, Any] = {}
    if robinhood_status_provider is not None:
        try:
            raw_status = robinhood_status_provider()
            robinhood_status = dict(raw_status) if isinstance(raw_status, dict) else {}
        except Exception as exc:
            robinhood_status = {"runtime_ready": False, "error_type": type(exc).__name__}

    forward = build_forward_certification(
        runtime.store,
        unified_status=unified,
        expected_release_commit=expected_release,
        robinhood_proof=proof,
        robinhood_proof_state=robinhood_proof_state,
    )
    local_counterfactuals = refresh_rejected_counterfactuals(runtime.store)
    local_slo = build_forward_proof_slo(runtime.store)
    hazard = build_hazard_calibration(runtime.store)
    execution_costs = refresh_execution_cost_ledger(runtime.store)
    portfolio = build_portfolio_reconciliation(runtime.store)
    confidence = family_proof_confidence(certification, promotion)
    funnel = dashboard_funnel(
        local_coverage=local_coverage,
        merged_coverage=coverage,
        certification=certification,
        promotion=promotion,
        local_counterfactuals=local_counterfactuals,
        robinhood_proof=proof,
    )
    http = _dict(http_metrics)
    operations = build_operations_proof(
        runtime,
        unified_status=unified,
        robinhood_status=robinhood_status,
        legacy_webhook_worker_enabled=legacy_webhook_worker_enabled(),
        proof_cycle_duration_seconds=max(0.0, time.perf_counter() - proof_started),
        http_request_count=int(http.get("request_count") or 0),
        http_total_duration_seconds=float(http.get("total_duration_seconds") or 0.0),
    )
    background = _dict(operations.get("background_work"))
    background.update(
        {
            "system_proof_precompute_worker_enabled": bool(
                getattr(app.state, "roi_v51_system_proof_precompute_worker_enabled", False)
            ),
            "system_proof_precompute_state": getattr(
                app.state, "roi_v51_system_proof_precompute_state", "not_started"
            ),
            "system_proof_precompute_last_completed_at": getattr(
                app.state, "roi_v51_system_proof_precompute_last_completed_at", None
            ),
            "schema_introspection_cache": schema_cache_stats(),
        }
    )
    operations["background_work"] = background
    operations_healthy = bool(_dict(operations.get("backpressure")).get("healthy", False))
    state = system_state(
        forward_certification=forward,
        coverage=coverage,
        certification=certification,
        promotion=promotion,
        operations_healthy=operations_healthy,
    )
    spec = authority()
    overall = _dict(unified.get("overall"))
    release_commit = str(unified.get("release_commit") or expected_release or "").strip() or None

    return {
        "system_proof_version": SYSTEM_PROOF_VERSION,
        "phase10_version": PHASE10_VERSION,
        "phase12_13_operations_version": OPERATIONS_VERSION,
        "generated_at": _utcnow(),
        "state": state,
        "ready_for_forward_proof": state == "READY_FOR_FORWARD_PROOF",
        "release": {
            "release_commit": release_commit,
            "expected_release_commit": expected_release,
            "exact_release_bound": bool(release_commit and (not expected_release or release_commit == expected_release)),
            "release_attestation": forward.get("checks", {}).get("41_current_release_attestation"),
        },
        "authority": {
            "authority_id": spec.get("authority_id"),
            "strategy_version": spec.get("strategy_version"),
            "economic_freeze_epoch": spec.get("economic_freeze_epoch"),
            "authority_fingerprint": authority_fingerprint(),
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        },
        "runtime": {
            "overall": overall,
            "surfaces": {name: unified.get(name) for name in ("solana", "fomo", "robinhood")},
            "forward_certification_state": forward.get("state"),
            "hard_operational_gates_ok": bool(forward.get("hard_operational_gates_ok")),
            "backpressure_healthy": operations_healthy,
            "blockers": list(forward.get("blockers") or []) + ([] if operations_healthy else ["persistent_backpressure_or_dropped_work"]),
        },
        "candidate_coverage": coverage,
        "execution_evidence": {
            "execution_cost_ledger": execution_costs,
            "mechanism_stress": mechanism_stress,
            "amount_specific_entry_and_exit_quotes_required": bool(
                _dict(spec.get("execution")).get("amount_specific_entry_and_exit_quotes_required")
            ),
        },
        "strategy_evidence": {
            "economic_certification": certification,
            "promotion_certification": promotion,
            "forward_certification": forward,
            "proof_confidence_by_family": confidence,
        },
        "paper_portfolio": {
            "paper_nav_usd": getattr(getattr(runtime, "engine", None), "nav_usd", None),
            "paper_cash_usd": getattr(getattr(getattr(runtime, "engine", None), "portfolio", None), "cash_usd", None),
            "paper_allocation_weights": promotion.get("paper_allocation_weights") or certification.get("paper_allocation_weights") or {},
            "paper_cash_weight": promotion.get("paper_cash_weight", certification.get("paper_cash_weight")),
            "portfolio_reconciliation": portfolio,
            "one_capital_base": True,
        },
        "settlement": _settlement_section(local_coverage, certification, local_slo, proof),
        "learning": {
            "rejected_counterfactuals_local": local_counterfactuals,
            "rejected_counterfactuals_robinhood": _dict(proof).get("rejected_counterfactuals"),
            "hazard_calibration": hazard,
            "retrospective_entry_authority": False,
        },
        "resource_health": {
            "operations": operations,
            "schema_introspection_cache": schema_cache_stats(),
            "robinhood_main_uvicorn_sqlite_reads": False,
            "liveness_endpoint_requires_runtime_or_sqlite": False,
            "readiness_endpoint_is_deep": True,
        },
        "operations_proof": operations,
        "dashboard_funnel": funnel,
        "proof_confidence_by_family": confidence,
        "readiness_contract": {
            "liveness_paths": ["/health", "/v1/liveness"],
            "deep_readiness_path": "/readiness",
            "canonical_system_proof_path": "/v1/system-proof",
            "canonical_dashboard_path": "/v1/system-proof/dashboard",
            "render_health_uses_deep_readiness": False,
            "external_verification_should_monitor": "/readiness",
        },
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def _failed_closed_system_proof(error: Exception) -> dict[str, Any]:
    spec = authority()
    return {
        "system_proof_version": SYSTEM_PROOF_VERSION,
        "phase10_version": PHASE10_VERSION,
        "phase12_13_operations_version": OPERATIONS_VERSION,
        "generated_at": _utcnow(),
        "state": "DEGRADED",
        "ready_for_forward_proof": False,
        "release": {"release_commit": _expected_release(), "expected_release_commit": _expected_release()},
        "authority": {
            "authority_id": spec.get("authority_id"),
            "strategy_version": spec.get("strategy_version"),
            "economic_freeze_epoch": spec.get("economic_freeze_epoch"),
            "paper_only": True,
            "live_money_authority": False,
        },
        "runtime": {"blockers": ["system_proof_composition_failed_closed"], "error_type": type(error).__name__},
        "candidate_coverage": {},
        "execution_evidence": {},
        "strategy_evidence": {},
        "paper_portfolio": {},
        "settlement": {},
        "learning": {},
        "resource_health": {
            "system_proof_error_type": type(error).__name__,
            "schema_introspection_cache": schema_cache_stats(),
        },
        "operations_proof": {},
        "dashboard_funnel": {},
        "proof_confidence_by_family": {},
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def install_system_proof(
    app: Any,
    runtime_provider: Callable[[], Any] | Any,
    *,
    robinhood_status_provider: Callable[[], dict[str, Any]] | None = None,
) -> None:
    if bool(getattr(app.state, "roi_v51_system_proof_70_74", False)):
        return

    # Phase 13 caches the proof-plane schema inspection helpers before any public
    # proof request can traverse them. PRAGMA schema_version automatically clears
    # cached table/column metadata after DDL.
    install_v51_schema_cache()

    cache_lock = threading.RLock()
    cache: dict[str, Any] = {"payload": None, "built_monotonic": 0.0}
    http_lock = threading.RLock()
    http_metrics: dict[str, Any] = {"request_count": 0, "total_duration_seconds": 0.0}
    try:
        cache_seconds = max(1.0, float(os.getenv("SOLANA_ROI_SYSTEM_PROOF_CACHE_SECONDS", "15")))
    except ValueError:
        cache_seconds = 15.0

    def http_snapshot() -> dict[str, Any]:
        with http_lock:
            return dict(http_metrics)

    def current_proof() -> dict[str, Any]:
        now = time.monotonic()
        with cache_lock:
            payload = cache.get("payload")
            age = now - float(cache.get("built_monotonic") or 0.0)
            if isinstance(payload, dict) and age <= cache_seconds:
                result = copy.deepcopy(payload)
                result["system_proof_cache"] = {
                    "hit": True,
                    "age_seconds": max(0.0, age),
                    "max_age_seconds": cache_seconds,
                    "shared_by_system_proof_readiness_and_dashboard": True,
                    "kept_warm_by_background_precompute": True,
                }
                return result
        try:
            runtime = runtime_provider() if callable(runtime_provider) else runtime_provider
            result = build_system_proof(
                app,
                runtime,
                robinhood_status_provider=robinhood_status_provider,
                http_metrics=http_snapshot(),
            )
        except Exception as exc:
            result = _failed_closed_system_proof(exc)
        with cache_lock:
            cache["payload"] = copy.deepcopy(result)
            cache["built_monotonic"] = time.monotonic()
        result["system_proof_cache"] = {
            "hit": False,
            "age_seconds": 0.0,
            "max_age_seconds": cache_seconds,
            "shared_by_system_proof_readiness_and_dashboard": True,
            "kept_warm_by_background_precompute": True,
        }
        return result

    # Lightweight attribution only; no request body or sensitive content is stored.
    @app.middleware("http")
    async def phase13_http_work_attribution(request: Any, call_next: Any) -> Any:
        started = time.perf_counter()
        try:
            return await call_next(request)
        finally:
            elapsed = max(0.0, time.perf_counter() - started)
            with http_lock:
                http_metrics["request_count"] = int(http_metrics["request_count"]) + 1
                http_metrics["total_duration_seconds"] = float(http_metrics["total_duration_seconds"]) + elapsed

    existing = {getattr(route, "path", None) for route in app.routes}
    if "/v1/liveness" not in existing:
        @app.get("/v1/liveness")
        def liveness_endpoint() -> dict[str, Any]:
            return {
                "status": "ok",
                "liveness_only": True,
                "runtime_or_sqlite_checked": False,
                "trading_research_readiness_implied": False,
                "readiness_path": "/readiness",
                "system_proof_path": "/v1/system-proof",
                "paper_only": True,
                "live_money_authority": False,
            }

    if "/v1/system-proof" not in existing:
        @app.get("/v1/system-proof")
        def system_proof_endpoint() -> dict[str, Any]:
            return current_proof()

    if "/readiness" not in existing:
        @app.get("/readiness")
        def readiness_endpoint() -> JSONResponse:
            payload = current_proof()
            ready = payload.get("state") == "READY_FOR_FORWARD_PROOF"
            body = {
                "ready": ready,
                "state": payload.get("state"),
                "release": payload.get("release"),
                "runtime": payload.get("runtime"),
                "candidate_coverage": {
                    "coverage_complete": _dict(payload.get("candidate_coverage")).get("coverage_complete"),
                    "coverage_debt_count": _dict(payload.get("candidate_coverage")).get("coverage_debt_count"),
                    "proof_state": _dict(payload.get("candidate_coverage")).get("proof_state"),
                },
                "forward_certification": _dict(_dict(payload.get("strategy_evidence")).get("forward_certification")),
                "operations_proof": payload.get("operations_proof"),
                "system_proof_cache": payload.get("system_proof_cache"),
                "system_proof_path": "/v1/system-proof",
                "paper_only": True,
                "live_money_authority": False,
            }
            return JSONResponse(content=body, status_code=200 if ready else 503)

    if "/v1/system-proof/dashboard" not in existing:
        @app.get("/v1/system-proof/dashboard", response_class=HTMLResponse)
        def system_proof_dashboard_endpoint() -> HTMLResponse:
            payload = current_proof()
            strategy = _dict(payload.get("strategy_evidence"))
            execution = _dict(payload.get("execution_evidence"))
            return HTMLResponse(
                render_economic_dashboard(
                    _dict(strategy.get("economic_certification")),
                    _dict(payload.get("candidate_coverage")),
                    _dict(execution.get("mechanism_stress")),
                    funnel=_dict(payload.get("dashboard_funnel")),
                    proof_confidence=_dict(payload.get("proof_confidence_by_family")),
                )
            )

    # The FastAPI lifespan starts this callback in asyncio.to_thread, keeping the
    # shared proof cache warm without adding proof SQL to the Uvicorn event loop.
    app.state.roi_v51_system_proof_precompute = current_proof
    app.state.roi_v51_system_proof_precompute_seconds = cache_seconds
    app.state.roi_v51_system_proof_precompute_state = "configured"
    app.state.roi_v51_system_proof_http_metrics = http_metrics
    app.state.roi_v51_system_proof_70_74 = True
    app.state.roi_v51_phase12_13_83_94 = True
    app.state.roi_v51_system_proof_cache_seconds = cache_seconds
    app.state.roi_v51_schema_introspection_cache = True


__all__ = [
    "SYSTEM_PROOF_VERSION",
    "SYSTEM_STATES",
    "build_system_proof",
    "install_system_proof",
    "system_state",
]
