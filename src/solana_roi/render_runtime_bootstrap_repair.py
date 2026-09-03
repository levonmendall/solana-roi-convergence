from __future__ import annotations

import asyncio
import os
import sqlite3
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from typing import Any, Callable


BOOTSTRAP_RETRY_SECONDS = 0.50
_BOOTSTRAP_STATE: dict[str, Any] = {
    "state": "not_started",
    "attempts": 0,
    "lock_retries": 0,
    "started_at": None,
    "ready_at": None,
    "last_error_type": None,
    "last_error_message": None,
    "lifespan_active": False,
}
_RUNTIME: Any | None = None
_API: Any | None = None
_ORIGINAL_INGESTION_RUNTIME: Callable[[], Any] | None = None


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_sqlite_lock_error(exc: BaseException) -> bool:
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    message = str(exc).lower()
    return "locked" in message or "busy" in message


def _public_status() -> dict[str, Any]:
    return {
        "installed": True,
        "state": str(_BOOTSTRAP_STATE["state"]),
        "attempts": int(_BOOTSTRAP_STATE["attempts"]),
        "lock_retries": int(_BOOTSTRAP_STATE["lock_retries"]),
        "started_at": _BOOTSTRAP_STATE["started_at"],
        "ready_at": _BOOTSTRAP_STATE["ready_at"],
        "last_error_type": _BOOTSTRAP_STATE["last_error_type"],
        "last_error_message": _BOOTSTRAP_STATE["last_error_message"],
        "liveness_decoupled_from_sqlite_bootstrap": True,
        "deep_runtime_fail_closed_until_ready": True,
        "sqlite_lock_retries_are_background_only": True,
        "certification_research_architecture_installed_before_api_capture": True,
        "wallet_research_operationally_isolated": True,
        "process_wide_rpc_workload_governor": True,
        "ephemeral_candidate_retention_installed_before_api_capture": True,
        "active_strategy_candidate_state_ephemeral": True,
        "canonical_observation_evidence_retained": True,
        "target_frontier_continuity_repair_installed_before_api_capture": True,
        "real_gap_recovery_uses_confirmed_target_frontier": True,
        "certification_failure_accounting_installed_before_api_capture": True,
        "failed_candidate_attempts_remain_in_certification_denominators": True,
        "certification_thresholds_unchanged": True,
        "continuity_lease_unchanged": True,
        "recovery_bound_unchanged": True,
        "full_raw_receipt_scope_unchanged": True,
        "paper_only_authority_unchanged": True,
        "signing_or_submission_available": False,
    }


async def _wait_or_stop(stop: asyncio.Event, delay: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=max(0.01, float(delay)))
    except asyncio.TimeoutError:
        return


async def _build_runtime_until_ready(stop: asyncio.Event) -> Any | None:
    global _RUNTIME
    if _ORIGINAL_INGESTION_RUNTIME is None:
        raise RuntimeError("Render runtime bootstrap repair is not installed")

    while not stop.is_set():
        _BOOTSTRAP_STATE["attempts"] = int(_BOOTSTRAP_STATE["attempts"]) + 1
        _BOOTSTRAP_STATE["state"] = "building_runtime"
        try:
            runtime = await asyncio.to_thread(_ORIGINAL_INGESTION_RUNTIME)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _is_sqlite_lock_error(exc):
                _BOOTSTRAP_STATE["state"] = "waiting_for_persistent_store"
                _BOOTSTRAP_STATE["lock_retries"] = int(_BOOTSTRAP_STATE["lock_retries"]) + 1
                _BOOTSTRAP_STATE["last_error_type"] = type(exc).__name__
                _BOOTSTRAP_STATE["last_error_message"] = str(exc)[:300] or type(exc).__name__
                await _wait_or_stop(stop, BOOTSTRAP_RETRY_SECONDS)
                continue
            _BOOTSTRAP_STATE["state"] = "failed_closed"
            _BOOTSTRAP_STATE["last_error_type"] = type(exc).__name__
            _BOOTSTRAP_STATE["last_error_message"] = str(exc)[:300] or type(exc).__name__
            return None

        _RUNTIME = runtime
        _BOOTSTRAP_STATE["state"] = "ready"
        _BOOTSTRAP_STATE["ready_at"] = _utcnow_iso()
        _BOOTSTRAP_STATE["last_error_type"] = None
        _BOOTSTRAP_STATE["last_error_message"] = None
        return runtime
    return None


async def _run_runtime_workers(runtime: Any, stop: asyncio.Event) -> None:
    webhook_stop = asyncio.Event()
    direct_stop = asyncio.Event()
    wallet_stop = asyncio.Event()
    clock_stop: asyncio.Event | None = None

    webhook_task = asyncio.create_task(
        runtime.webhook_worker.run(webhook_stop), name="legacy-helius-webhook-worker"
    )
    direct_task = asyncio.create_task(
        runtime.direct_ingestion.run(direct_stop), name="direct-solana-ingestion"
    )
    wallet_task = asyncio.create_task(
        runtime.wallet_discovery.run(wallet_stop), name="continuous-wallet-discovery"
    )
    clock_task: asyncio.Task[None] | None = None
    if os.getenv("SOLANA_ROI_SHADOW_CLOCK_ENABLED", "").strip().lower() in {"1", "true", "yes"}:
        clock_stop = asyncio.Event()
        clock_task = asyncio.create_task(runtime.price_clock.run(clock_stop), name="shadow-price-clock")

    try:
        await stop.wait()
    finally:
        wallet_stop.set()
        direct_stop.set()
        webhook_stop.set()
        if clock_stop is not None:
            clock_stop.set()
        if clock_task is not None:
            with suppress(asyncio.CancelledError):
                await clock_task
        with suppress(asyncio.CancelledError):
            await wallet_task
        with suppress(asyncio.CancelledError):
            await direct_task
        with suppress(asyncio.CancelledError):
            await webhook_task


async def _bootstrap_and_run(stop: asyncio.Event) -> None:
    runtime = await _build_runtime_until_ready(stop)
    if runtime is None or stop.is_set():
        return
    await _run_runtime_workers(runtime, stop)


def _guarded_ingestion_runtime() -> Any:
    if _RUNTIME is not None:
        return _RUNTIME
    if not bool(_BOOTSTRAP_STATE["lifespan_active"]):
        if _ORIGINAL_INGESTION_RUNTIME is None:
            raise RuntimeError("Render runtime bootstrap repair is not installed")
        return _ORIGINAL_INGESTION_RUNTIME()
    if _API is None:
        raise RuntimeError("Render runtime bootstrap repair is not installed")
    raise _API.HTTPException(
        status_code=503,
        detail={
            "error": "runtime_bootstrap_not_ready",
            "runtime_bootstrap": _public_status(),
        },
    )


@asynccontextmanager
async def _render_handoff_lifespan(_app: Any):
    global _RUNTIME
    _RUNTIME = None
    _BOOTSTRAP_STATE.update(
        {
            "state": "starting",
            "attempts": 0,
            "lock_retries": 0,
            "started_at": _utcnow_iso(),
            "ready_at": None,
            "last_error_type": None,
            "last_error_message": None,
            "lifespan_active": True,
        }
    )
    stop = asyncio.Event()
    bootstrap_task = asyncio.create_task(_bootstrap_and_run(stop), name="runtime-bootstrap-handoff")
    try:
        yield
    finally:
        stop.set()
        with suppress(asyncio.CancelledError):
            await bootstrap_task
        _BOOTSTRAP_STATE["lifespan_active"] = False
        if _BOOTSTRAP_STATE["state"] == "ready":
            _BOOTSTRAP_STATE["state"] = "stopped"


def install_render_runtime_bootstrap_handoff() -> None:
    global _API, _ORIGINAL_INGESTION_RUNTIME
    if _API is not None and bool(getattr(_API.app.state, "roi_runtime_bootstrap_handoff", False)):
        return

    from .certification_failure_accounting_repair import install_certification_failure_accounting_repair
    from .certification_research_architecture import install_certification_research_architecture
    from .continuity_target_frontier_repair import install_continuity_target_frontier_repair
    from .ephemeral_candidate_retention import install_ephemeral_candidate_retention

    install_certification_research_architecture()
    install_ephemeral_candidate_retention()
    install_continuity_target_frontier_repair()
    install_certification_failure_accounting_repair()

    from . import api as api_module

    _API = api_module
    _ORIGINAL_INGESTION_RUNTIME = api_module.ingestion_runtime
    api_module.ingestion_runtime = _guarded_ingestion_runtime  # type: ignore[assignment]
    api_module.app.router.lifespan_context = _render_handoff_lifespan
    api_module.app.state.roi_runtime_bootstrap_handoff = True

    if not any(getattr(route, "path", None) == "/v1/runtime-bootstrap/status" for route in api_module.app.routes):
        api_module.app.add_api_route(
            "/v1/runtime-bootstrap/status",
            _public_status,
            methods=["GET"],
            name="runtime_bootstrap_status",
        )


__all__ = [
    "BOOTSTRAP_RETRY_SECONDS",
    "_build_runtime_until_ready",
    "_guarded_ingestion_runtime",
    "_is_sqlite_lock_error",
    "_public_status",
    "_render_handoff_lifespan",
    "install_render_runtime_bootstrap_handoff",
]
