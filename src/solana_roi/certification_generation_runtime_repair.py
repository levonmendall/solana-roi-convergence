from __future__ import annotations

import asyncio
import copy
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

from . import e2e_status_read_boundary_repair as e2e
from . import production_proof_read_boundary_repair as proof
from . import render_runtime_bootstrap_repair as render_bootstrap
from . import v51_forward_certification as forward
from .certification_generation_coordinator import (
    COORDINATOR_VERSION,
    CertificationResourceGuardError,
    exclusive_generation,
    install_status_route,
)


REPAIR_VERSION = "certification-generation-runtime-v1-single-flight-forward-cache"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
CERTIFICATION_THRESHOLDS_CHANGED = False
ECONOMIC_THRESHOLDS_CHANGED = False
CANONICAL_EVIDENCE_RESET = False
FORWARD_SNAPSHOT_INTERVAL_SECONDS = 15.0
FORWARD_SNAPSHOT_STALE_SECONDS = 45.0
FORWARD_SNAPSHOT_JOIN_SECONDS = 0.50

_INSTALLED = False
_ORIGINAL_E2E_BUILDER: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_PRODUCTION_PROOF: Callable[[], dict[str, Any]] | None = None
_ORIGINAL_FORWARD_BUILDER: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_FORWARD_ENDPOINT: Callable[[], dict[str, Any]] | None = None
_ORIGINAL_RUNTIME_WORKERS: Callable[..., Any] | None = None

_FORWARD_LOCK = threading.Lock()
_FORWARD_SNAPSHOT: dict[str, Any] | None = None
_FORWARD_PUBLISHED_MONOTONIC: float | None = None
_FORWARD_STATS: dict[str, Any] = {
    "attempts": 0,
    "successes": 0,
    "failures": 0,
    "guard_rejections": 0,
    "consecutive_failures": 0,
    "last_started_at": None,
    "last_completed_at": None,
    "last_duration_seconds": None,
    "last_error_type": None,
    "last_generation_id": None,
}


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _annotate(payload: dict[str, Any], lease: Any, *, surface: str) -> dict[str, Any]:
    result = payload
    boundary = result.setdefault("certification_generation", {})
    if isinstance(boundary, dict):
        boundary.update(
            {
                "coordinator_version": COORDINATOR_VERSION,
                "runtime_repair_version": REPAIR_VERSION,
                "generation_id": lease.generation_id,
                "release_commit": lease.release_commit,
                "surface": surface,
                "started_at": lease.started_at,
                "wait_seconds": lease.wait_seconds,
                "nested": lease.nested,
                "single_flight": True,
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
            }
        )
    return result


def _coordinated_e2e_builder(*args: Any, **kwargs: Any) -> dict[str, Any]:
    if _ORIGINAL_E2E_BUILDER is None:
        raise RuntimeError("coordinated E2E builder missing canonical delegate")
    with exclusive_generation("e2e_status_build") as lease:
        payload = _ORIGINAL_E2E_BUILDER(*args, **kwargs)
        if not isinstance(payload, dict):
            raise TypeError("canonical E2E builder returned non-dict payload")
        return _annotate(payload, lease, surface="e2e_status")


setattr(_coordinated_e2e_builder, "_roi_certification_single_flight", True)


def _coordinated_production_proof() -> dict[str, Any]:
    if _ORIGINAL_PRODUCTION_PROOF is None:
        raise RuntimeError("coordinated production proof missing canonical delegate")
    with exclusive_generation("production_proof_build") as lease:
        payload = _ORIGINAL_PRODUCTION_PROOF()
        if not isinstance(payload, dict):
            raise TypeError("canonical production proof returned non-dict payload")
        return _annotate(payload, lease, surface="production_proof")


setattr(_coordinated_production_proof, "_roi_certification_single_flight", True)


def _coordinated_forward_builder(*args: Any, **kwargs: Any) -> dict[str, Any]:
    if _ORIGINAL_FORWARD_BUILDER is None:
        raise RuntimeError("coordinated forward certification missing canonical delegate")
    with exclusive_generation("forward_certification_build") as lease:
        payload = _ORIGINAL_FORWARD_BUILDER(*args, **kwargs)
        if not isinstance(payload, dict):
            raise TypeError("canonical forward certification returned non-dict payload")
        return _annotate(payload, lease, surface="forward_certification")


setattr(_coordinated_forward_builder, "_roi_certification_single_flight", True)


def _forward_cache_state() -> dict[str, Any]:
    with _FORWARD_LOCK:
        available = _FORWARD_SNAPSHOT is not None
        published = _FORWARD_PUBLISHED_MONOTONIC
        stats = dict(_FORWARD_STATS)
    age = max(0.0, time.monotonic() - published) if isinstance(published, (int, float)) else None
    return {
        "repair_version": REPAIR_VERSION,
        "snapshot_available": available,
        "snapshot_fresh": bool(available and age is not None and age <= FORWARD_SNAPSHOT_STALE_SECONDS),
        "snapshot_age_seconds": age,
        "snapshot_interval_seconds": FORWARD_SNAPSHOT_INTERVAL_SECONDS,
        "snapshot_stale_seconds": FORWARD_SNAPSHOT_STALE_SECONDS,
        "http_request_executes_deep_forward_builder": False,
        "attempts": int(stats.get("attempts", 0) or 0),
        "successes": int(stats.get("successes", 0) or 0),
        "failures": int(stats.get("failures", 0) or 0),
        "guard_rejections": int(stats.get("guard_rejections", 0) or 0),
        "consecutive_failures": int(stats.get("consecutive_failures", 0) or 0),
        "last_started_at": stats.get("last_started_at"),
        "last_completed_at": stats.get("last_completed_at"),
        "last_duration_seconds": stats.get("last_duration_seconds"),
        "last_error_type": stats.get("last_error_type"),
        "last_generation_id": stats.get("last_generation_id"),
        "single_flight": True,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def _fail_closed_forward(reason: str) -> dict[str, Any]:
    blocker = str(reason or "forward_certification_snapshot_unavailable")
    return {
        "certification_version": forward.CERTIFICATION_VERSION,
        "state": "measurement_degraded",
        "system_forward_certified": False,
        "hard_operational_gates_ok": False,
        "evidence_maturity_ok": False,
        "blockers": [blocker],
        "changes_strategy_authority": False,
        "changes_economic_thresholds": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
        "certification_generation": {
            "coordinator_version": COORDINATOR_VERSION,
            "runtime_repair_version": REPAIR_VERSION,
            "single_flight": True,
            "state": "failed_closed",
            "reason": blocker,
            "cache": _forward_cache_state(),
        },
    }


def _publish_forward(payload: dict[str, Any]) -> None:
    global _FORWARD_SNAPSHOT, _FORWARD_PUBLISHED_MONOTONIC
    copied = copy.deepcopy(payload)
    with _FORWARD_LOCK:
        _FORWARD_SNAPSHOT = copied
        _FORWARD_PUBLISHED_MONOTONIC = time.monotonic()


def _cached_forward_endpoint() -> dict[str, Any]:
    with _FORWARD_LOCK:
        payload = copy.deepcopy(_FORWARD_SNAPSHOT) if _FORWARD_SNAPSHOT is not None else None
        published = _FORWARD_PUBLISHED_MONOTONIC
    if payload is None or not isinstance(published, (int, float)):
        return _fail_closed_forward("forward_certification_snapshot_not_ready")
    age = max(0.0, time.monotonic() - float(published))
    if age > FORWARD_SNAPSHOT_STALE_SECONDS:
        return _fail_closed_forward("forward_certification_snapshot_stale")
    generation = payload.setdefault("certification_generation", {})
    if isinstance(generation, dict):
        generation["snapshot_age_seconds"] = age
        generation["cache"] = _forward_cache_state()
    return payload


setattr(_cached_forward_endpoint, "_roi_forward_certification_precomputed_snapshot", True)
setattr(_cached_forward_endpoint, "_roi_certification_single_flight", True)


def _forward_thread_main(stop: threading.Event) -> None:
    while not stop.is_set():
        started = time.monotonic()
        with _FORWARD_LOCK:
            _FORWARD_STATS["attempts"] = int(_FORWARD_STATS.get("attempts", 0) or 0) + 1
            _FORWARD_STATS["last_started_at"] = _utcnow()
        error_type: str | None = None
        generation_id: str | None = None
        try:
            if _ORIGINAL_FORWARD_ENDPOINT is None:
                raise RuntimeError("forward snapshot worker missing canonical endpoint")
            payload = _ORIGINAL_FORWARD_ENDPOINT()
            if not isinstance(payload, dict):
                raise TypeError("canonical forward endpoint returned non-dict payload")
            generation = payload.get("certification_generation")
            if isinstance(generation, dict):
                generation_id = str(generation.get("generation_id") or "") or None
            _publish_forward(payload)
        except BaseException as exc:
            error_type = type(exc).__name__
            if isinstance(exc, CertificationResourceGuardError):
                with _FORWARD_LOCK:
                    _FORWARD_STATS["guard_rejections"] = int(_FORWARD_STATS.get("guard_rejections", 0) or 0) + 1
        duration = max(0.0, time.monotonic() - started)
        with _FORWARD_LOCK:
            _FORWARD_STATS["last_completed_at"] = _utcnow()
            _FORWARD_STATS["last_duration_seconds"] = duration
            _FORWARD_STATS["last_error_type"] = error_type
            _FORWARD_STATS["last_generation_id"] = generation_id
            if error_type is None:
                _FORWARD_STATS["successes"] = int(_FORWARD_STATS.get("successes", 0) or 0) + 1
                _FORWARD_STATS["consecutive_failures"] = 0
            else:
                _FORWARD_STATS["failures"] = int(_FORWARD_STATS.get("failures", 0) or 0) + 1
                _FORWARD_STATS["consecutive_failures"] = int(_FORWARD_STATS.get("consecutive_failures", 0) or 0) + 1
        stop.wait(FORWARD_SNAPSHOT_INTERVAL_SECONDS)


async def _runtime_workers_with_forward_snapshot(runtime: Any, stop: asyncio.Event) -> None:
    if _ORIGINAL_RUNTIME_WORKERS is None:
        raise RuntimeError("forward snapshot worker missing canonical runtime workers")
    thread_stop = threading.Event()
    thread = threading.Thread(
        target=_forward_thread_main,
        args=(thread_stop,),
        name="forward-certification-snapshot-publisher",
        daemon=True,
    )
    thread.start()
    try:
        await _ORIGINAL_RUNTIME_WORKERS(runtime, stop)
    finally:
        thread_stop.set()
        await asyncio.to_thread(thread.join, FORWARD_SNAPSHOT_JOIN_SECONDS)


setattr(_runtime_workers_with_forward_snapshot, "_roi_forward_certification_snapshot_worker", True)
setattr(_runtime_workers_with_forward_snapshot, "_roi_certification_single_flight", True)


def install_certification_generation_runtime_repair(app: Any) -> None:
    global _INSTALLED, _ORIGINAL_E2E_BUILDER, _ORIGINAL_PRODUCTION_PROOF
    global _ORIGINAL_FORWARD_BUILDER, _ORIGINAL_FORWARD_ENDPOINT, _ORIGINAL_RUNTIME_WORKERS
    if _INSTALLED or bool(getattr(app.state, "roi_certification_generation_runtime_repair", False)):
        return

    _ORIGINAL_E2E_BUILDER = e2e.build_bounded_e2e_status
    if not bool(getattr(_ORIGINAL_E2E_BUILDER, "_roi_certification_single_flight", False)):
        e2e.build_bounded_e2e_status = _coordinated_e2e_builder  # type: ignore[assignment]

    _ORIGINAL_PRODUCTION_PROOF = proof._ORIGINAL_PRODUCTION_PROOF
    if _ORIGINAL_PRODUCTION_PROOF is None:
        raise RuntimeError("production proof read boundary not installed before certification coordinator")
    if not bool(getattr(_ORIGINAL_PRODUCTION_PROOF, "_roi_certification_single_flight", False)):
        proof._ORIGINAL_PRODUCTION_PROOF = _coordinated_production_proof

    _ORIGINAL_FORWARD_BUILDER = forward.build_forward_certification
    if not bool(getattr(_ORIGINAL_FORWARD_BUILDER, "_roi_certification_single_flight", False)):
        forward.build_forward_certification = _coordinated_forward_builder  # type: ignore[assignment]

    route = None
    for candidate in app.routes:
        if getattr(candidate, "path", None) == "/v1/strategy/forward-certification":
            route = candidate
            break
    if route is None:
        raise RuntimeError("forward certification route not found after canonical composition")
    endpoint = getattr(route, "endpoint", None)
    if not callable(endpoint):
        raise RuntimeError("forward certification endpoint is not callable")
    _ORIGINAL_FORWARD_ENDPOINT = endpoint
    route.endpoint = _cached_forward_endpoint
    dependant = getattr(route, "dependant", None)
    if dependant is not None:
        dependant.call = _cached_forward_endpoint

    _ORIGINAL_RUNTIME_WORKERS = render_bootstrap._run_runtime_workers
    if not bool(getattr(_ORIGINAL_RUNTIME_WORKERS, "_roi_forward_certification_snapshot_worker", False)):
        render_bootstrap._run_runtime_workers = _runtime_workers_with_forward_snapshot  # type: ignore[assignment]

    install_status_route(app)
    cache_path = "/v1/strategy/forward-certification/cache"
    if cache_path not in {getattr(candidate, "path", None) for candidate in app.routes}:
        app.add_api_route(cache_path, _forward_cache_state, methods=["GET"], name="forward_certification_cache")

    app.state.roi_certification_generation_runtime_repair = True
    app.state.roi_certification_generation_runtime_repair_version = REPAIR_VERSION
    app.state.roi_certification_generation_single_flight = True
    app.state.roi_forward_certification_http_deep_builder_disabled = True
    app.state.roi_certification_generation_strategy_contract_relaxed = False
    app.state.roi_certification_generation_paper_only = True
    app.state.roi_certification_generation_live_money_authority = False
    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "_cached_forward_endpoint",
    "_forward_cache_state",
    "install_certification_generation_runtime_repair",
]
