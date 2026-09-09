from __future__ import annotations

import asyncio
import copy
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

from . import unified_strategy_status as unified
from .certification_generation_coordinator import CertificationResourceGuardError, resource_guard
from .cgroup_oom_forensics import phase as memory_forensics_phase


REPAIR_VERSION = "e2e-status-read-boundary-v3-owned-publication-postbuild-guard"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
SNAPSHOT_INTERVAL_SECONDS = 15.0
SNAPSHOT_STALE_SECONDS = 120.0
SNAPSHOT_JOIN_SECONDS = 0.50

_SNAPSHOT_LOCK = threading.Lock()
_SNAPSHOT: dict[str, Any] | None = None
_SNAPSHOT_PUBLISHED_MONOTONIC: float | None = None
_SNAPSHOT_STATS: dict[str, Any] = {
    "attempts": 0,
    "successes": 0,
    "failures": 0,
    "guard_rejections": 0,
    "consecutive_failures": 0,
    "last_started_at": None,
    "last_completed_at": None,
    "last_duration_seconds": None,
    "last_error_type": None,
}
_ORIGINAL_RUNTIME_WORKERS: Callable[..., Any] | None = None
_ORIGINAL_PROBE_STATUS: Callable[..., dict[str, dict[str, Any]]] | None = None


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _release_commit() -> str:
    for key in ("RENDER_GIT_COMMIT", "GITHUB_SHA", "SOLANA_ROI_RELEASE_COMMIT"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return "unbound-local-release"


def _required_status_base(runtime: Any) -> dict[str, Any]:
    """Build only the state consumed by the unified E2E contract.

    This function is intentionally retained as the deep/background builder. It may
    acquire production SQLite locks and therefore must never be invoked from the HTTP
    request path after this repair is installed.
    """
    return {
        "data_plane": "direct-solana",
        "direct_solana": runtime.direct_ingestion.status(),
        "wallet_discovery": runtime.wallet_discovery.status(),
    }


def _empty_probe_status(store: Any, release_commit: str) -> dict[str, dict[str, Any]]:
    """Read probe evidence without creating schema from a status path."""
    result = {
        regime: {
            "completed": False,
            "source_signature": None,
            "venue": None,
            "lifecycle": None,
            "execution_notional_fraction": None,
            "net_return_pct": None,
            "completed_at": None,
        }
        for regime in unified.REGIMES
    }
    if not unified._table_exists(store, "regime_paper_e2e_probes"):
        return result
    try:
        with store._lock:
            rows = store.db.execute(
                "SELECT source_signature,regime,venue,lifecycle,execution_notional_fraction,net_return,completed_at "
                "FROM regime_paper_e2e_probes WHERE release_commit=? ORDER BY id",
                (release_commit,),
            ).fetchall()
    except Exception:
        return result
    for row in rows:
        regime = str(row["regime"])
        if regime not in result:
            continue
        result[regime] = {
            "completed": True,
            "source_signature": str(row["source_signature"]),
            "venue": str(row["venue"]),
            "lifecycle": str(row["lifecycle"]),
            "execution_notional_fraction": float(row["execution_notional_fraction"]),
            "net_return_pct": float(row["net_return"]) * 100.0,
            "completed_at": str(row["completed_at"]),
        }
    return result


def build_bounded_e2e_status(
    runtime_provider: Callable[[], Any],
    robinhood_status_provider: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    """Compute the canonical E2E status for background publication.

    The economic/transport contract is unchanged. The only semantic difference from
    v1 is that this expensive builder is no longer called by the HTTP request itself.
    """
    runtime = runtime_provider()
    robinhood = robinhood_status_provider()
    payload = unified.build_unified_strategy_status(
        _required_status_base(runtime),
        runtime,
        robinhood,
    )
    return {
        "status_contract_version": payload["status_contract_version"],
        "release_commit": payload["release_commit"],
        "solana": payload["solana"],
        "fomo": payload["fomo"],
        "robinhood": payload["robinhood"],
        "overall": payload["overall"],
        "read_boundary": {
            "repair_version": REPAIR_VERSION,
            "full_ingestion_status_invoked": False,
            "full_event_chain_verification_invoked": False,
            "http_request_executes_deep_status_builder": False,
            "snapshot_precomputed_off_request_path": True,
            "snapshot_stale_seconds": SNAPSHOT_STALE_SECONDS,
            "publication_uses_owned_builder_payload": True,
            "post_build_resource_guard": True,
            "post_build_memory_limit_fraction": 0.90,
            "strategy_contract_or_gate_relaxed": False,
            "paper_only": PAPER_ONLY,
            "live_money_authority": LIVE_MONEY_AUTHORITY,
            "signing_available": SIGNING_AVAILABLE,
            "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        },
    }


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
        "http_request_executes_deep_status_builder": False,
        "snapshot_precomputed_off_request_path": True,
        "publication_uses_owned_builder_payload": True,
        "post_build_resource_guard": True,
        "post_build_memory_limit_fraction": 0.90,
        "attempts": int(stats.get("attempts", 0) or 0),
        "successes": int(stats.get("successes", 0) or 0),
        "failures": int(stats.get("failures", 0) or 0),
        "guard_rejections": int(stats.get("guard_rejections", 0) or 0),
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
    blocker = str(reason or "e2e_status_snapshot_unavailable")
    overall = {
        "all_strategy_transports_ready": False,
        "all_regimes_paper_capable": False,
        "all_regimes_e2e_achievable": False,
        "all_regimes_e2e_proven": False,
        "blockers": [blocker],
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    lane = {
        "runtime_ready": False,
        "all_regimes_paper_capable": False,
        "all_regimes_e2e_achievable": False,
        "all_regimes_e2e_proven": False,
        "blockers": [blocker],
        "regimes": {},
    }
    return {
        "status_contract_version": unified.STATUS_CONTRACT_VERSION,
        "release_commit": _release_commit(),
        "solana": dict(lane),
        "fomo": dict(lane),
        "robinhood": dict(lane),
        "overall": overall,
        "read_boundary": {
            "repair_version": REPAIR_VERSION,
            "state": "failed_closed",
            "reason": blocker,
            "full_ingestion_status_invoked": False,
            "full_event_chain_verification_invoked": False,
            "http_request_executes_deep_status_builder": False,
            "snapshot_precomputed_off_request_path": True,
            "snapshot_stale_seconds": SNAPSHOT_STALE_SECONDS,
            "publication_uses_owned_builder_payload": True,
            "post_build_resource_guard": True,
            "post_build_memory_limit_fraction": 0.90,
            "strategy_contract_or_gate_relaxed": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
            "cache": _cache_state(),
        },
    }


def _publish_snapshot(payload: dict[str, Any]) -> None:
    """Publish the builder-owned payload without a second whole-status copy."""
    global _SNAPSHOT, _SNAPSHOT_PUBLISHED_MONOTONIC
    published = time.monotonic()
    boundary = payload.setdefault("read_boundary", {})
    if isinstance(boundary, dict):
        boundary.update(
            {
                "state": "ready",
                "snapshot_published_at": _utcnow(),
                "snapshot_precomputed_off_request_path": True,
                "http_request_executes_deep_status_builder": False,
                "publication_uses_owned_builder_payload": True,
                "post_build_resource_guard": True,
            }
        )
    with _SNAPSHOT_LOCK:
        _SNAPSHOT = payload
        _SNAPSHOT_PUBLISHED_MONOTONIC = published


def _cached_e2e_status() -> dict[str, Any]:
    """Return only an isolated snapshot; never wait for production SQLite work."""
    with _SNAPSHOT_LOCK:
        payload = copy.deepcopy(_SNAPSHOT) if _SNAPSHOT is not None else None
        published = _SNAPSHOT_PUBLISHED_MONOTONIC
    if payload is None or not isinstance(published, (int, float)):
        return _fail_closed_payload("e2e_status_snapshot_not_ready")
    age = max(0.0, time.monotonic() - float(published))
    if age > SNAPSHOT_STALE_SECONDS:
        return _fail_closed_payload("e2e_status_snapshot_stale")
    boundary = payload.setdefault("read_boundary", {})
    if isinstance(boundary, dict):
        boundary["snapshot_age_seconds"] = age
        boundary["cache"] = _cache_state()
    return payload


def _snapshot_thread_main(
    runtime: Any,
    robinhood_status_provider: Callable[[], dict[str, Any]],
    stop: threading.Event,
) -> None:
    while not stop.is_set():
        started = time.monotonic()
        started_at = _utcnow()
        with _SNAPSHOT_LOCK:
            _SNAPSHOT_STATS["attempts"] = int(_SNAPSHOT_STATS.get("attempts", 0) or 0) + 1
            _SNAPSHOT_STATS["last_started_at"] = started_at
        error_type: str | None = None
        try:
            with memory_forensics_phase("e2e_status_build"):
                payload = build_bounded_e2e_status(lambda: runtime, robinhood_status_provider)
            if not isinstance(payload, dict):
                raise TypeError("E2E status builder returned non-dict payload")
            resource_guard("e2e_status_post_build_pre_publish")
            _publish_snapshot(payload)
        except BaseException as exc:
            error_type = type(exc).__name__
            if isinstance(exc, CertificationResourceGuardError):
                with _SNAPSHOT_LOCK:
                    _SNAPSHOT_STATS["guard_rejections"] = int(
                        _SNAPSHOT_STATS.get("guard_rejections", 0) or 0
                    ) + 1
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


async def _runtime_workers_with_e2e_snapshot(runtime: Any, stop: asyncio.Event) -> None:
    if _ORIGINAL_RUNTIME_WORKERS is None:
        raise RuntimeError("E2E snapshot worker missing canonical runtime worker")
    from . import robinhood_runtime_install as robinhood_runtime

    thread_stop = threading.Event()
    thread = threading.Thread(
        target=_snapshot_thread_main,
        args=(runtime, robinhood_runtime._status, thread_stop),
        name="e2e-status-snapshot-publisher",
        daemon=True,
    )
    thread.start()
    try:
        await _ORIGINAL_RUNTIME_WORKERS(runtime, stop)
    finally:
        thread_stop.set()
        await asyncio.to_thread(thread.join, SNAPSHOT_JOIN_SECONDS)


setattr(_runtime_workers_with_e2e_snapshot, "_roi_e2e_status_snapshot_worker", True)


def install_e2e_status_read_boundary_repair(
    app: Any,
    runtime_provider: Callable[[], Any],
) -> None:
    """Make the dedicated E2E HTTP surface constant-time and fail-closed.

    Deep aggregation remains available as a background computation, but HTTP never
    acquires the trading/evidence SQLite lock. Installation is app-scoped: composing
    one FastAPI app must never prevent a later app in the same interpreter from being
    composed correctly.
    """
    global _ORIGINAL_RUNTIME_WORKERS, _ORIGINAL_PROBE_STATUS
    if bool(getattr(app.state, "roi_e2e_status_read_boundary_v2", False)):
        return

    from . import render_runtime_bootstrap_repair as render_bootstrap

    route = None
    for candidate in app.routes:
        if getattr(candidate, "path", None) == "/v1/strategy/e2e-status":
            route = candidate
            break
    if route is None:
        raise RuntimeError("strategy E2E status route not found after canonical composition")

    # Status is a read path. Probe-table creation remains owned by the probe write
    # path; merely asking for E2E status can no longer issue DDL on the live store.
    if _ORIGINAL_PROBE_STATUS is None:
        _ORIGINAL_PROBE_STATUS = unified._probe_status
        unified._probe_status = _empty_probe_status  # type: ignore[assignment]

    setattr(_cached_e2e_status, "_roi_e2e_status_read_boundary", True)
    setattr(_cached_e2e_status, "_roi_e2e_status_precomputed_snapshot", True)
    route.endpoint = _cached_e2e_status
    dependant = getattr(route, "dependant", None)
    if dependant is not None:
        dependant.call = _cached_e2e_status

    current_workers = render_bootstrap._run_runtime_workers
    if not bool(getattr(current_workers, "_roi_e2e_status_snapshot_worker", False)):
        _ORIGINAL_RUNTIME_WORKERS = current_workers
        render_bootstrap._run_runtime_workers = _runtime_workers_with_e2e_snapshot  # type: ignore[assignment]

    existing = {getattr(candidate, "path", None) for candidate in app.routes}
    register_get = getattr(app, "get", None)
    if "/v1/strategy/e2e-status/cache" not in existing and callable(register_get):
        @app.get("/v1/strategy/e2e-status/cache")
        def e2e_status_cache() -> dict[str, Any]:
            return _cache_state()

    app.state.roi_e2e_status_read_boundary = True
    app.state.roi_e2e_status_read_boundary_v2 = True
    app.state.roi_e2e_status_read_boundary_version = REPAIR_VERSION
    app.state.roi_e2e_status_http_deep_builder_disabled = True


__all__ = [
    "REPAIR_VERSION",
    "SNAPSHOT_INTERVAL_SECONDS",
    "SNAPSHOT_STALE_SECONDS",
    "_cache_state",
    "_cached_e2e_status",
    "_empty_probe_status",
    "_publish_snapshot",
    "build_bounded_e2e_status",
    "install_e2e_status_read_boundary_repair",
]
