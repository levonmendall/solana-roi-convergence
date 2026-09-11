from __future__ import annotations

"""Cleanup-capable isolated-certifier service entrypoint.

This module wraps the already-proven certifier FastAPI application without changing
its certification worker.  The wrapper owns the certifier replica disk for the whole
process lifetime.  A cleanup-capable release must first run normally with cleanup
disabled and complete at least one certifier cycle; only a later deployment of the
same release SHA may execute destructive SQLite maintenance.

During an enabled cleanup deployment HTTP liveness remains available.  The certifier
worker is not started until cleanup succeeds.  Any cleanup/preflight failure therefore
fails closed: no certification cycle can mutate or consume the replica while its
maintenance state is ambiguous.
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import Header

from . import certifier_service as certifier
from . import production_data_cleanup_v4 as cleanup
from . import production_disk_ownership as disk_ownership
from .certification_replica_client import _replica_path

SERVICE_VERSION = "isolated-certifier-cleanup-entrypoint-v2-path-telemetry"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
STATUS_PATH = "/v1/operations/production-data-cleanup"
_LOG = logging.getLogger("solana_roi.certifier_production_data_cleanup")

_ORIGINAL_LIFESPAN = certifier.app.router.lifespan_context
_STATE: dict[str, Any] = {
    "service_version": SERVICE_VERSION,
    "cleanup_version": cleanup.CLEANUP_VERSION,
    "enabled": False,
    "status": "not_started",
    "runtime_disk_ownership_required": True,
    "same_release_lease_establishment_required": True,
    "worker_quiesced_until_cleanup_complete": True,
    "paper_only": True,
    "live_money_authority": False,
    "signing_available": False,
    "transaction_submission_available": False,
}


def _env_true(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _emit(prefix: str, payload: dict[str, Any]) -> None:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    print(f"{prefix} {raw}", flush=True)
    _LOG.warning("%s %s", prefix, raw)


def _set_state(**updates: Any) -> dict[str, Any]:
    _STATE.update(updates)
    _STATE.update(
        {
            "service_version": SERVICE_VERSION,
            "cleanup_version": cleanup.CLEANUP_VERSION,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    )
    return dict(_STATE)


def _cleanup_arguments() -> tuple[str, str]:
    role = os.getenv(cleanup.ROLE_ENV, "certifier").strip().lower()
    if role != "certifier":
        raise cleanup.CleanupBlocked("certifier production cleanup requires role=certifier")
    run_id = os.getenv(cleanup.RUN_ID_ENV, "").strip()
    if not run_id:
        raise cleanup.CleanupBlocked("cleanup enabled but run id is not configured")
    return role, run_id


def _summary(result: dict[str, Any]) -> dict[str, Any]:
    reclaimed = dict(result.get("reclaimed") or {})
    compaction = dict(result.get("compaction") or {})
    before = dict(result.get("before") or {})
    after = dict(result.get("after") or {})
    return {
        "enabled": True,
        "status": str(result.get("status") or "unknown"),
        "run_id": result.get("run_id"),
        "role": result.get("role"),
        "idempotent_replay": bool(result.get("idempotent_replay", False)),
        "database_bytes_reclaimed": int(reclaimed.get("database_bytes") or 0),
        "wal_bytes_reclaimed": int(reclaimed.get("wal_bytes") or 0),
        "filesystem_free_bytes_delta": int(reclaimed.get("filesystem_free_bytes") or 0),
        "compaction_mode": compaction.get("mode"),
        "before_integrity_ok": bool(dict(before.get("integrity") or {}).get("ok")),
        "after_integrity_ok": bool(dict(after.get("integrity") or {}).get("ok")),
        "worker_started_after_cleanup": True,
        "runtime_disk_ownership_required": True,
        "same_release_lease_establishment_required": True,
        "worker_quiesced_until_cleanup_complete": True,
    }


async def _mark_established_after_success(
    database_path: Path,
    lease: disk_ownership.RuntimeDiskLease,
    stop: asyncio.Event,
    baseline_successes: int,
) -> None:
    """Establish cleanup eligibility only after this process proves a live cycle."""
    while not stop.is_set():
        with certifier._LOCK:
            successes = int(certifier._STATE.get("successes", 0) or 0)
        if successes > baseline_successes:
            try:
                marker = await asyncio.to_thread(
                    disk_ownership.mark_same_release_established,
                    database_path,
                    lease,
                )
                _set_state(
                    enabled=False,
                    status="disabled_release_established",
                    lease_establishment=marker,
                )
                _emit("ROI_CERTIFIER_CLEANUP_LEASE_ESTABLISHED", marker)
            except Exception as exc:
                blocked = _set_state(
                    enabled=False,
                    status="lease_establishment_blocked",
                    error_type=type(exc).__name__,
                    error=str(exc)[:500],
                )
                _emit("ROI_CERTIFIER_CLEANUP_LEASE_BLOCKED", blocked)
            return
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.25)
        except TimeoutError:
            continue


async def _run_enabled_cleanup(database_path: Path) -> dict[str, Any]:
    if not disk_ownership.same_release_established(database_path):
        raise cleanup.CleanupBlocked(
            "destructive certifier cleanup requires this exact release SHA to have completed "
            "a certifier cycle once with cleanup disabled while holding the disk lease"
        )
    role, run_id = _cleanup_arguments()
    if not database_path.exists() or not database_path.is_file():
        raise cleanup.CleanupBlocked(f"certifier replica database does not exist: {database_path}")
    return await asyncio.to_thread(
        cleanup.execute_cleanup,
        database_path,
        role=role,
        run_id=run_id,
        acknowledged_watermark=None,
    )


@asynccontextmanager
async def lifespan(app: Any) -> AsyncIterator[None]:
    database_path = Path(_replica_path()).resolve()
    stop = asyncio.Event()
    lease: disk_ownership.RuntimeDiskLease | None = None
    marker_task: asyncio.Task[None] | None = None
    enabled = _env_true(cleanup.ENABLED_ENV)
    waiting = _set_state(
        enabled=enabled,
        status="waiting_for_runtime_disk_ownership",
        database_path=str(database_path),
    )
    _emit("ROI_CERTIFIER_CLEANUP_RUNTIME", waiting)
    try:
        lease = await disk_ownership.acquire_runtime_disk_lease(database_path, stop=stop)
        owned = _set_state(disk_ownership=lease.status())
        _emit("ROI_CERTIFIER_CLEANUP_RUNTIME", owned)
        if enabled:
            _set_state(status="cleanup_preflight_or_execution")
            try:
                result = await _run_enabled_cleanup(database_path)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                blocked = _set_state(
                    status="blocked",
                    error_type=type(exc).__name__,
                    error=str(exc)[:500],
                    worker_started_after_cleanup=False,
                )
                _emit("ROI_CERTIFIER_PRODUCTION_DATA_CLEANUP_BLOCKED", blocked)
                # Yield liveness without entering the original lifespan, so the
                # certification worker stays stopped and all deep surfaces fail closed.
                yield
                return

            _set_state(**_summary(result))
            _emit("ROI_CERTIFIER_PRODUCTION_DATA_CLEANUP", result)
            async with _ORIGINAL_LIFESPAN(app):
                yield
            return

        with certifier._LOCK:
            baseline_successes = int(certifier._STATE.get("successes", 0) or 0)
        _set_state(status="disabled_running_normally")
        async with _ORIGINAL_LIFESPAN(app):
            marker_task = asyncio.create_task(
                _mark_established_after_success(
                    database_path,
                    lease,
                    stop,
                    baseline_successes,
                ),
                name="certifier-cleanup-release-establishment",
            )
            yield
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        blocked = _set_state(
            status="blocked",
            error_type=type(exc).__name__,
            error=str(exc)[:500],
            worker_started_after_cleanup=False,
        )
        _emit("ROI_CERTIFIER_PRODUCTION_DATA_CLEANUP_BLOCKED", blocked)
        yield
    finally:
        stop.set()
        if marker_task is not None:
            marker_task.cancel()
            try:
                await marker_task
            except asyncio.CancelledError:
                pass
        if lease is not None:
            lease.release()
            ownership = dict(_STATE.get("disk_ownership") or {})
            ownership["owned"] = False
            _set_state(disk_ownership=ownership)


certifier.app.router.lifespan_context = lifespan
app = certifier.app

_existing_routes = {getattr(route, "path", None) for route in app.routes}
if STATUS_PATH not in _existing_routes:
    @app.get(STATUS_PATH)
    def production_data_cleanup_status(
        x_certification_token: str | None = Header(default=None, alias="X-Certification-Token"),
    ) -> dict[str, Any]:
        certifier._require_token(x_certification_token)
        return dict(_STATE)


__all__ = [
    "LIVE_MONEY_AUTHORITY",
    "PAPER_ONLY",
    "SERVICE_VERSION",
    "SIGNING_AVAILABLE",
    "STATUS_PATH",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "app",
    "lifespan",
]
