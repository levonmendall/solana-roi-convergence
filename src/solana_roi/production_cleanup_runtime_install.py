from __future__ import annotations

"""Authoritative one-shot cleanup ownership inside the Render bootstrap boundary.

The production ASGI service must become live before expensive SQLite maintenance,
but the canonical runtime and all database-writing workers must remain stopped until
that maintenance either succeeds or fails closed.  This installer wraps the guarded
background bootstrap rather than the FastAPI import path, so enabling cleanup never
constructs the runtime first and never hides a long VACUUM behind ASGI startup.
"""

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any, Awaitable, Callable

from . import production_data_cleanup as cleanup
from . import render_runtime_bootstrap_repair as bootstrap

INSTALL_VERSION = "production-data-cleanup-runtime-install-v1"
STATUS_PATH = "/v1/operations/production-data-cleanup"
_REGISTRATION_ATTR = "roi_production_data_cleanup_runtime_registered"
_LOG = logging.getLogger("solana_roi.production_data_cleanup")

BootstrapCallable = Callable[[asyncio.Event], Awaitable[None]]


def _env_true(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _disabled_state() -> dict[str, Any]:
    return {
        "install_version": INSTALL_VERSION,
        "cleanup_version": cleanup.CLEANUP_VERSION,
        "enabled": False,
        "status": "disabled",
        "runtime_quiesced_until_cleanup_complete": True,
        "liveness_available_during_cleanup": True,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def _arguments_from_environment() -> tuple[Path, str, int | None, float]:
    role = os.getenv(cleanup.ROLE_ENV, "authoritative").strip().lower()
    if role != "authoritative":
        raise cleanup.CleanupBlocked(
            "authoritative production cleanup runtime requires role=authoritative"
        )
    run_id = os.getenv(cleanup.RUN_ID_ENV, "").strip()
    if not run_id:
        raise cleanup.CleanupBlocked("cleanup enabled but run id is not configured")
    raw_path = (
        os.getenv("SOLANA_ROI_DATABASE_PATH", "").strip()
        or os.getenv("SOLANA_ROI_DB_PATH", "").strip()
        or "data/solana-roi.sqlite3"
    )
    raw_watermark = os.getenv(cleanup.ACK_WATERMARK_ENV, "").strip()
    try:
        watermark = int(raw_watermark) if raw_watermark else None
    except ValueError as exc:
        raise cleanup.CleanupBlocked("cleanup acknowledged watermark is not an integer") from exc
    try:
        telemetry_hours = float(os.getenv(cleanup.TELEMETRY_HOURS_ENV, "24"))
    except ValueError as exc:
        raise cleanup.CleanupBlocked("cleanup telemetry retention hours is not numeric") from exc
    return Path(raw_path), run_id, watermark, telemetry_hours


def _deleted_total(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return max(0, value)
    if isinstance(value, dict):
        return sum(_deleted_total(item) for item in value.values())
    return 0


def _summary(result: dict[str, Any]) -> dict[str, Any]:
    reclaimed = dict(result.get("reclaimed") or {})
    return {
        "install_version": INSTALL_VERSION,
        "cleanup_version": result.get("version") or cleanup.CLEANUP_VERSION,
        "enabled": True,
        "status": str(result.get("status") or "unknown"),
        "run_id": result.get("run_id"),
        "role": result.get("role"),
        "idempotent_replay": bool(result.get("idempotent_replay", False)),
        "deleted_rows": _deleted_total(result.get("deleted_rows")),
        "orphan_files_removed": int(
            dict(result.get("orphan_files") or {}).get("removed") or 0
        ),
        "database_bytes_reclaimed": int(reclaimed.get("database_bytes") or 0),
        "wal_bytes_reclaimed": int(reclaimed.get("wal_bytes") or 0),
        "filesystem_free_bytes_delta": int(
            reclaimed.get("filesystem_free_bytes") or 0
        ),
        "compaction_mode": dict(result.get("compaction") or {}).get("mode"),
        "before_integrity_ok": bool(
            dict(dict(result.get("before") or {}).get("integrity") or {}).get("ok")
        ),
        "after_integrity_ok": bool(
            dict(dict(result.get("after") or {}).get("integrity") or {}).get("ok")
        ),
        "runtime_quiesced_until_cleanup_complete": True,
        "liveness_available_during_cleanup": True,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def _emit(prefix: str, payload: dict[str, Any]) -> None:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    print(f"{prefix} {raw}", flush=True)
    _LOG.warning("%s %s", prefix, raw)


async def _run_cleanup_if_enabled(app: Any) -> bool:
    """Return True only when normal runtime bootstrap may continue."""
    if not _env_true(cleanup.ENABLED_ENV):
        app.state.roi_production_data_cleanup = _disabled_state()
        return True

    bootstrap._BOOTSTRAP_STATE["state"] = "production_data_cleanup"
    try:
        database_path, run_id, watermark, telemetry_hours = _arguments_from_environment()
        result = await asyncio.to_thread(
            cleanup.execute_cleanup,
            database_path,
            role="authoritative",
            run_id=run_id,
            acknowledged_watermark=watermark,
            telemetry_hours=telemetry_hours,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        state = {
            "install_version": INSTALL_VERSION,
            "cleanup_version": cleanup.CLEANUP_VERSION,
            "enabled": True,
            "status": "blocked",
            "error_type": type(exc).__name__,
            "error": str(exc)[:500] or type(exc).__name__,
            "runtime_quiesced_until_cleanup_complete": True,
            "runtime_started_after_cleanup": False,
            "liveness_available_during_cleanup": True,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
        app.state.roi_production_data_cleanup = state
        bootstrap._BOOTSTRAP_STATE["state"] = "failed_closed"
        bootstrap._BOOTSTRAP_STATE["last_error_type"] = type(exc).__name__
        bootstrap._BOOTSTRAP_STATE["last_error_message"] = state["error"]
        _emit("ROI_PRODUCTION_DATA_CLEANUP_BLOCKED", state)
        return False

    state = _summary(result)
    state["runtime_started_after_cleanup"] = True
    app.state.roi_production_data_cleanup = state
    _emit("ROI_PRODUCTION_DATA_CLEANUP", result)
    return True


def install_production_cleanup_runtime(app: Any) -> dict[str, Any]:
    """Install exactly once without touching storage at import/composition time."""
    if bool(getattr(app.state, _REGISTRATION_ATTR, False)):
        return dict(
            getattr(app.state, "roi_production_data_cleanup", _disabled_state())
        )

    original: BootstrapCallable = bootstrap._bootstrap_and_run

    async def _cleanup_owned_bootstrap(stop: asyncio.Event) -> None:
        allowed = await _run_cleanup_if_enabled(app)
        if not allowed or stop.is_set():
            return
        await original(stop)

    setattr(_cleanup_owned_bootstrap, "_roi_production_data_cleanup_bootstrap", True)
    setattr(_cleanup_owned_bootstrap, "_roi_previous_bootstrap", original)
    bootstrap._bootstrap_and_run = _cleanup_owned_bootstrap

    initial = _disabled_state()
    initial["status"] = "pending" if _env_true(cleanup.ENABLED_ENV) else "disabled"
    initial["enabled"] = _env_true(cleanup.ENABLED_ENV)
    app.state.roi_production_data_cleanup = initial
    app.state.roi_production_data_cleanup_version = cleanup.CLEANUP_VERSION
    setattr(app.state, _REGISTRATION_ATTR, True)

    routes = {getattr(route, "path", None) for route in getattr(app, "routes", ())}
    if STATUS_PATH not in routes:
        @app.get(STATUS_PATH)
        def production_data_cleanup_status() -> dict[str, Any]:
            return dict(
                getattr(app.state, "roi_production_data_cleanup", _disabled_state())
            )

    return dict(initial)


__all__ = [
    "INSTALL_VERSION",
    "STATUS_PATH",
    "_run_cleanup_if_enabled",
    "install_production_cleanup_runtime",
]
