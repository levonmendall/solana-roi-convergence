from __future__ import annotations

import asyncio
import copy
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

from . import render_runtime_bootstrap_repair as render_bootstrap


REPAIR_VERSION = "production-proof-read-boundary-v2-stable-worker-chain"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
SNAPSHOT_INTERVAL_SECONDS = 15.0
SNAPSHOT_STALE_SECONDS = 300.0
SNAPSHOT_JOIN_SECONDS = 0.50

_SNAPSHOT_LOCK = threading.Lock()
_SNAPSHOT: dict[str, Any] | None = None
_SNAPSHOT_PUBLISHED_MONOTONIC: float | None = None
_SNAPSHOT_STATS: dict[str, Any] = {
    "attempts": 0,
    "successes": 0,
    "failures": 0,
    "consecutive_failures": 0,
    "last_started_at": None,
    "last_completed_at": None,
    "last_duration_seconds": None,
    "last_error_type": None,
}
_ORIGINAL_RUNTIME_WORKERS: Callable[..., Any] | None = None
_ORIGINAL_PRODUCTION_PROOF: Callable[[], dict[str, Any]] | None = None


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _release_commit() -> str:
    for key in ("RENDER_GIT_COMMIT", "GITHUB_SHA", "SOLANA_ROI_RELEASE_COMMIT"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return "unbound-local-release"


def _cache_state() -> dict[str, Any]:
    with _SNAPSHOT_LOCK:
        published = _SNAPSHOT_PUBLISHED_MONOTONIC
        available = _SNAPSHOT is not None
        stats = dict(_SNAPSHOT_STATS)
    age = max(0.0, time.monotonic() - published) if isinstance(published, (int, float)) else None
    fresh = bool(available and age is not None and age <= SNAPSHOT_STALE_SECONDS)
    return {
        "repair_version": REPAIR_VERSION,
        "snapshot_available": available,
        "snapshot_fresh": fresh,
        "snapshot_age_seconds": age,
        "snapshot_stale_seconds": SNAPSHOT_STALE_SECONDS,
        "snapshot_interval_seconds": SNAPSHOT_INTERVAL_SECONDS,
        "http_request_executes_deep_proof_builder": False,
        "snapshot_precomputed_off_request_path": True,
        "attempts": int(stats.get("attempts", 0) or 0),
        "successes": int(stats.get("successes", 0) or 0),
        "failures": int(stats.get("failures", 0) or 0),
        "consecutive_failures": int(stats.get("consecutive_failures", 0) or 0),
        "last_started_at": stats.get("last_started_at"),
        "last_completed_at": stats.get("last_completed_at"),
        "last_duration_seconds": stats.get("last_duration_seconds"),
        "last_error_type": stats.get("last_error_type"),
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def _fail_closed_payload(reason: str) -> dict[str, Any]:
    blocker = str(reason or "production_proof_snapshot_unavailable")
    return {
        "production_proof_api_version": "v51-canonical-production-proof-v1",
        "generated_at": _utcnow(),
        "state": "DEGRADED",
        "ready_for_forward_proof": False,
        "production_proof_pass": False,
        "release": {"release_commit": _release_commit()},
        "authority": {
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        },
        "candidate_accounting": {},
        "forward_certification": {},
        "final_certification": {
            "classification": "INSUFFICIENT_EVIDENCE",
            "blockers": [blocker],
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        },
        "batch6_release_gate": {
            "pass": False,
            "verdict": "FAIL_CLOSED",
            "blockers": [blocker],
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        },
        "worker_readiness": {},
        "resource_pressure": {"state": "unavailable", "read_only_observability": True},
        "surface_attestation_policy": {
            "surface_scoped_attestation_required": True,
            "aggregate_attestation_fallback_allowed": False,
        },
        "blockers": [blocker],
        "read_boundary": {
            "repair_version": REPAIR_VERSION,
            "state": "failed_closed",
            "reason": blocker,
            "http_request_executes_deep_proof_builder": False,
            "snapshot_precomputed_off_request_path": True,
            "strategy_contract_or_gate_relaxed": False,
            "cache": _cache_state(),
        },
        "read_only_observability": True,
        "changes_strategy_authority": False,
        "changes_economic_thresholds": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def _publish_snapshot(payload: dict[str, Any]) -> None:
    global _SNAPSHOT, _SNAPSHOT_PUBLISHED_MONOTONIC
    copied = copy.deepcopy(payload)
    boundary = copied.setdefault("read_boundary", {})
    if isinstance(boundary, dict):
        boundary.update(
            {
                "repair_version": REPAIR_VERSION,
                "state": "ready",
                "snapshot_published_at": _utcnow(),
                "http_request_executes_deep_proof_builder": False,
                "snapshot_precomputed_off_request_path": True,
                "strategy_contract_or_gate_relaxed": False,
            }
        )
    with _SNAPSHOT_LOCK:
        _SNAPSHOT = copied
        _SNAPSHOT_PUBLISHED_MONOTONIC = time.monotonic()


def _cached_production_proof() -> dict[str, Any]:
    """Return an immutable proof snapshot and never touch the canonical store."""
    with _SNAPSHOT_LOCK:
        payload = copy.deepcopy(_SNAPSHOT) if _SNAPSHOT is not None else None
        published = _SNAPSHOT_PUBLISHED_MONOTONIC
    if payload is None or not isinstance(published, (int, float)):
        return _fail_closed_payload("production_proof_snapshot_not_ready")
    age = max(0.0, time.monotonic() - float(published))
    if age > SNAPSHOT_STALE_SECONDS:
        return _fail_closed_payload("production_proof_snapshot_stale")
    boundary = payload.setdefault("read_boundary", {})
    if isinstance(boundary, dict):
        boundary["snapshot_age_seconds"] = age
        boundary["cache"] = _cache_state()
    return payload


def _snapshot_thread_main(builder: Callable[[], dict[str, Any]], stop: threading.Event) -> None:
    while not stop.is_set():
        started = time.monotonic()
        with _SNAPSHOT_LOCK:
            _SNAPSHOT_STATS["attempts"] = int(_SNAPSHOT_STATS.get("attempts", 0) or 0) + 1
            _SNAPSHOT_STATS["last_started_at"] = _utcnow()
        error_type: str | None = None
        try:
            payload = builder()
            if not isinstance(payload, dict):
                raise TypeError("production proof builder returned non-dict payload")
            _publish_snapshot(payload)
        except BaseException as exc:
            error_type = type(exc).__name__
        duration = max(0.0, time.monotonic() - started)
        with _SNAPSHOT_LOCK:
            _SNAPSHOT_STATS["last_completed_at"] = _utcnow()
            _SNAPSHOT_STATS["last_duration_seconds"] = duration
            _SNAPSHOT_STATS["last_error_type"] = error_type
            if error_type is None:
                _SNAPSHOT_STATS["successes"] = int(_SNAPSHOT_STATS.get("successes", 0) or 0) + 1
                _SNAPSHOT_STATS["consecutive_failures"] = 0
            else:
                _SNAPSHOT_STATS["failures"] = int(_SNAPSHOT_STATS.get("failures", 0) or 0) + 1
                _SNAPSHOT_STATS["consecutive_failures"] = int(
                    _SNAPSHOT_STATS.get("consecutive_failures", 0) or 0
                ) + 1
        stop.wait(SNAPSHOT_INTERVAL_SECONDS)


async def _runtime_workers_with_production_proof_snapshot(runtime: Any, stop: asyncio.Event) -> None:
    if _ORIGINAL_RUNTIME_WORKERS is None or _ORIGINAL_PRODUCTION_PROOF is None:
        raise RuntimeError("production proof snapshot worker missing canonical dependencies")
    thread_stop = threading.Event()
    thread = threading.Thread(
        target=_snapshot_thread_main,
        args=(_ORIGINAL_PRODUCTION_PROOF, thread_stop),
        name="production-proof-snapshot-publisher",
        daemon=True,
    )
    thread.start()
    try:
        await _ORIGINAL_RUNTIME_WORKERS(runtime, stop)
    finally:
        thread_stop.set()
        await asyncio.to_thread(thread.join, SNAPSHOT_JOIN_SECONDS)


setattr(_runtime_workers_with_production_proof_snapshot, "_roi_production_proof_snapshot_worker", True)


def _current_worker_chain_already_contains_production_proof(current_workers: Callable[..., Any]) -> bool:
    """Detect an existing production-proof worker even when E2E wrapped it later."""
    if bool(getattr(current_workers, "_roi_production_proof_snapshot_worker", False)):
        return True
    if not bool(getattr(current_workers, "_roi_e2e_status_snapshot_worker", False)):
        return False
    try:
        from . import e2e_status_read_boundary_repair as e2e

        delegated = getattr(e2e, "_ORIGINAL_RUNTIME_WORKERS", None)
    except Exception:
        delegated = None
    return bool(getattr(delegated, "_roi_production_proof_snapshot_worker", False))


def install_production_proof_read_boundary_repair(app: Any) -> None:
    """Move expensive canonical production-proof composition off the HTTP path."""
    global _ORIGINAL_RUNTIME_WORKERS, _ORIGINAL_PRODUCTION_PROOF
    if bool(getattr(app.state, "roi_production_proof_read_boundary", False)):
        return

    route = None
    for candidate in app.routes:
        if getattr(candidate, "path", None) == "/v1/strategy/production-proof":
            route = candidate
            break
    if route is None:
        raise RuntimeError("canonical production proof route not found after v5.1 composition")

    original = getattr(route, "endpoint", None)
    if not callable(original):
        raise RuntimeError("canonical production proof endpoint is not callable")
    _ORIGINAL_PRODUCTION_PROOF = original

    setattr(_cached_production_proof, "_roi_production_proof_read_boundary", True)
    setattr(_cached_production_proof, "_roi_production_proof_precomputed_snapshot", True)
    route.endpoint = _cached_production_proof
    dependant = getattr(route, "dependant", None)
    if dependant is not None:
        dependant.call = _cached_production_proof

    current_workers = render_bootstrap._run_runtime_workers
    if _current_worker_chain_already_contains_production_proof(current_workers):
        setattr(current_workers, "_roi_production_proof_snapshot_worker", True)
    else:
        _ORIGINAL_RUNTIME_WORKERS = current_workers
        delegates_e2e = bool(getattr(current_workers, "_roi_e2e_status_snapshot_worker", False))
        setattr(_runtime_workers_with_production_proof_snapshot, "_roi_e2e_status_snapshot_worker", delegates_e2e)
        render_bootstrap._run_runtime_workers = _runtime_workers_with_production_proof_snapshot  # type: ignore[assignment]

    existing = {getattr(candidate, "path", None) for candidate in app.routes}
    register_get = getattr(app, "get", None)
    if "/v1/strategy/production-proof/cache" not in existing and callable(register_get):
        @app.get("/v1/strategy/production-proof/cache")
        def production_proof_cache() -> dict[str, Any]:
            return _cache_state()

    app.state.roi_production_proof_read_boundary = True
    app.state.roi_production_proof_read_boundary_version = REPAIR_VERSION
    app.state.roi_production_proof_http_deep_builder_disabled = True
    app.state.roi_production_proof_strategy_contract_relaxed = False
    app.state.roi_production_proof_paper_only = True
    app.state.roi_production_proof_live_money_authority = False


__all__ = [
    "REPAIR_VERSION",
    "SNAPSHOT_INTERVAL_SECONDS",
    "SNAPSHOT_STALE_SECONDS",
    "_cache_state",
    "_cached_production_proof",
    "_current_worker_chain_already_contains_production_proof",
    "_publish_snapshot",
    "install_production_proof_read_boundary_repair",
]
