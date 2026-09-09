from __future__ import annotations

import hmac
import json
import logging
import os
import shutil
import sqlite3
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable

from fastapi import Header, HTTPException
from starlette.background import BackgroundTask
from starlette.responses import FileResponse

from . import certification_generation_runtime_repair as certification_runtime
from . import e2e_status_read_boundary_repair as e2e
from . import production_proof_read_boundary_repair as production_proof
from . import render_runtime_bootstrap_repair as render_bootstrap


SPLIT_VERSION = "certification-service-split-v5-snapshot-cache-release"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
DEFAULT_REMOTE_TIMEOUT_SECONDS = 5.0
DEFAULT_SNAPSHOT_MAX_BYTES = 8 * 1024 * 1024 * 1024
DEFAULT_SNAPSHOT_PAGES_PER_STEP = 4096
DEFAULT_SNAPSHOT_STEP_SLEEP_SECONDS = 0.002
DEFAULT_SNAPSHOT_DEADLINE_SECONDS = 55.0
DEFAULT_SNAPSHOT_FREE_RESERVE_BYTES = 512 * 1024 * 1024
DEFAULT_STALE_EXPORT_SECONDS = 3600.0

_ORIGINAL_RUNTIME_WORKERS: Callable[..., Any] | None = None
_SNAPSHOT_EXPORT_LOCK = threading.Lock()
_STATE_LOCK = threading.Lock()
_LOGGER = logging.getLogger(__name__)
_SNAPSHOT_STATE: dict[str, Any] = {
    "attempts": 0,
    "successes": 0,
    "failures": 0,
    "busy_rejections": 0,
    "last_started_monotonic": None,
    "last_completed_monotonic": None,
    "last_duration_seconds": None,
    "last_size_bytes": None,
    "last_estimated_bytes": None,
    "last_error_type": None,
    "last_source_name": None,
}


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def split_runtime_enabled() -> bool:
    return _truthy(os.getenv("SOLANA_ROI_CERTIFICATION_SPLIT_RUNTIME"))


def _release_commit() -> str:
    for key in ("RENDER_GIT_COMMIT", "GITHUB_SHA", "SOLANA_ROI_RELEASE_COMMIT"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return "unbound-local-release"


def _certifier_url() -> str:
    return os.getenv("SOLANA_ROI_CERTIFIER_URL", "").strip().rstrip("/")


def _shared_token() -> str:
    return os.getenv("SOLANA_ROI_CERTIFICATION_SHARED_TOKEN", "").strip()


def _remote_timeout() -> float:
    try:
        return max(
            0.5,
            float(
                os.getenv(
                    "SOLANA_ROI_CERTIFIER_HTTP_TIMEOUT_SECONDS",
                    str(DEFAULT_REMOTE_TIMEOUT_SECONDS),
                )
            ),
        )
    except ValueError:
        return DEFAULT_REMOTE_TIMEOUT_SECONDS


def _snapshot_max_bytes() -> int:
    try:
        return max(
            64 * 1024 * 1024,
            int(
                os.getenv(
                    "SOLANA_ROI_CERTIFICATION_SNAPSHOT_MAX_BYTES",
                    str(DEFAULT_SNAPSHOT_MAX_BYTES),
                )
            ),
        )
    except ValueError:
        return DEFAULT_SNAPSHOT_MAX_BYTES


def _snapshot_pages_per_step() -> int:
    try:
        return max(
            64,
            int(
                os.getenv(
                    "SOLANA_ROI_CERTIFICATION_SNAPSHOT_PAGES_PER_STEP",
                    str(DEFAULT_SNAPSHOT_PAGES_PER_STEP),
                )
            ),
        )
    except ValueError:
        return DEFAULT_SNAPSHOT_PAGES_PER_STEP


def _snapshot_step_sleep_seconds() -> float:
    try:
        return max(
            0.0,
            float(
                os.getenv(
                    "SOLANA_ROI_CERTIFICATION_SNAPSHOT_STEP_SLEEP_SECONDS",
                    str(DEFAULT_SNAPSHOT_STEP_SLEEP_SECONDS),
                )
            ),
        )
    except ValueError:
        return DEFAULT_SNAPSHOT_STEP_SLEEP_SECONDS


def _snapshot_deadline_seconds() -> float:
    try:
        return max(
            5.0,
            float(
                os.getenv(
                    "SOLANA_ROI_CERTIFICATION_SNAPSHOT_DEADLINE_SECONDS",
                    str(DEFAULT_SNAPSHOT_DEADLINE_SECONDS),
                )
            ),
        )
    except ValueError:
        return DEFAULT_SNAPSHOT_DEADLINE_SECONDS


def _snapshot_free_reserve_bytes() -> int:
    try:
        return max(
            64 * 1024 * 1024,
            int(
                os.getenv(
                    "SOLANA_ROI_CERTIFICATION_SNAPSHOT_FREE_RESERVE_BYTES",
                    str(DEFAULT_SNAPSHOT_FREE_RESERVE_BYTES),
                )
            ),
        )
    except ValueError:
        return DEFAULT_SNAPSHOT_FREE_RESERVE_BYTES


def _surface_release(path: str, payload: dict[str, Any]) -> str:
    if path.endswith("/e2e-status") or path.endswith("/forward-certification"):
        return str(payload.get("release_commit") or "")
    release = payload.get("release")
    return str(release.get("release_commit") or "") if isinstance(release, dict) else ""


def _failed_closed(path: str, reason: str) -> dict[str, Any]:
    if path.endswith("/e2e-status"):
        payload = e2e._fail_closed_payload(reason)
    elif path.endswith("/forward-certification"):
        payload = certification_runtime._fail_closed_forward(reason)
    else:
        payload = production_proof._fail_closed_payload(reason)
    boundary = payload.setdefault("certification_service_split", {})
    if isinstance(boundary, dict):
        boundary.update(
            {
                "split_version": SPLIT_VERSION,
                "state": "failed_closed",
                "reason": str(reason),
                "runtime_executes_local_certification_builders": False,
                "remote_certifier_required": True,
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
            }
        )
    return payload


def _remote_surface(path: str) -> dict[str, Any]:
    base = _certifier_url()
    token = _shared_token()
    if not base or not token:
        return _failed_closed(path, "certification_service_split_not_configured")

    request = urllib.request.Request(
        f"{base}{path}",
        headers={
            "Accept": "application/json",
            "X-Certification-Token": token,
            "User-Agent": "solana-roi-authoritative-runtime/1",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=_remote_timeout()) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
        return _failed_closed(path, "remote_certification_service_unavailable")
    if not isinstance(payload, dict):
        return _failed_closed(path, "remote_certification_service_invalid_payload")

    expected = _release_commit()
    observed = _surface_release(path, payload)
    if not observed or observed != expected:
        return _failed_closed(
            path,
            f"remote_certification_release_mismatch:{observed or 'missing'}:{expected}",
        )

    boundary = payload.setdefault("certification_service_split", {})
    if isinstance(boundary, dict):
        boundary.update(
            {
                "split_version": SPLIT_VERSION,
                "state": "ready",
                "release_commit": expected,
                "runtime_executes_local_certification_builders": False,
                "remote_certifier_required": True,
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
            }
        )
    return payload


def _replace_get_route(app: Any, path: str) -> None:
    route = next((candidate for candidate in app.routes if getattr(candidate, "path", None) == path), None)
    if route is None:
        raise RuntimeError(f"certification split route not found: {path}")

    def endpoint() -> dict[str, Any]:
        return _remote_surface(path)

    setattr(endpoint, "_roi_remote_certification_proxy", True)
    route.endpoint = endpoint
    dependant = getattr(route, "dependant", None)
    if dependant is not None:
        dependant.call = endpoint


def _snapshot_directory(store: Any) -> Path:
    source_path = getattr(store, "path", None)
    if source_path is None:
        raise RuntimeError("canonical runtime store path unavailable")
    source_path = Path(source_path)
    override = os.getenv("SOLANA_ROI_CERTIFICATION_SNAPSHOT_DIR", "").strip()
    directory = Path(override) if override else source_path.parent
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _drop_file_cache(path: Path) -> bool:
    """Best-effort file-specific cache release; never a correctness dependency."""
    fadvise = getattr(os, "posix_fadvise", None)
    advice = getattr(os, "POSIX_FADV_DONTNEED", None)
    if fadvise is None or advice is None:
        return False
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return False
    try:
        try:
            fadvise(fd, 0, 0, advice)
            return True
        except OSError:
            return False
    finally:
        os.close(fd)


def _dispose_snapshot(path: Path) -> None:
    _drop_file_cache(path)
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _cleanup_stale_exports(directory: Path) -> int:
    removed = 0
    now = time.time()
    try:
        candidates = tuple(directory.glob(".certification-export-*.sqlite3"))
    except OSError:
        return 0
    for candidate in candidates:
        try:
            if not candidate.is_file():
                continue
            if now - candidate.stat().st_mtime < DEFAULT_STALE_EXPORT_SECONDS:
                continue
            _dispose_snapshot(candidate)
            removed += 1
        except OSError:
            continue
    return removed


def _snapshot_store_to_file(store: Any, snapshot: Path) -> tuple[int, int]:
    """Create one bounded, immutable SQLite certification snapshot.

    A dedicated read-only connection pins a WAL read transaction before backup so
    a continuously mutating production database cannot force the backup to chase a
    moving source forever. Writes remain available; WAL growth is bounded by the
    explicit backup deadline. The destination is a temporary file on the runtime's
    existing persistent disk, not ``/tmp`` and never a certifier-mounted disk.
    File-specific cache advice is issued after the copy so the 1+ GB proof export
    does not unnecessarily remain charged to the authoritative runtime cgroup.
    """
    source_path = getattr(store, "path", None)
    if source_path is None:
        raise RuntimeError("canonical runtime store path unavailable")
    source_path = Path(source_path)
    if not source_path.is_file():
        raise RuntimeError("canonical runtime SQLite file unavailable")

    source_uri = f"file:{source_path.resolve()}?mode=ro"
    source = sqlite3.connect(source_uri, uri=True, timeout=5.0)
    destination = sqlite3.connect(snapshot)
    started = time.monotonic()
    try:
        source.execute("PRAGMA query_only=ON")
        source.execute("PRAGMA busy_timeout=5000")
        source.execute("BEGIN")
        source.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
        page_count = int(source.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(source.execute("PRAGMA page_size").fetchone()[0])
        estimated_bytes = page_count * page_size
        if estimated_bytes > _snapshot_max_bytes():
            raise RuntimeError("canonical certification snapshot exceeds configured bounded export")

        free_bytes = int(shutil.disk_usage(snapshot.parent).free)
        required_free = estimated_bytes + _snapshot_free_reserve_bytes()
        if free_bytes < required_free:
            raise OSError("insufficient bounded certification snapshot disk headroom")

        def progress(_status: int, _remaining: int, _total: int) -> None:
            if time.monotonic() - started > _snapshot_deadline_seconds():
                raise TimeoutError("canonical certification snapshot exceeded bounded deadline")

        source.backup(
            destination,
            pages=_snapshot_pages_per_step(),
            progress=progress,
            sleep=_snapshot_step_sleep_seconds(),
        )
        destination.commit()
        return int(snapshot.stat().st_size), estimated_bytes
    finally:
        try:
            source.rollback()
        except sqlite3.Error:
            pass
        destination.close()
        source.close()
        _drop_file_cache(source_path)
        if snapshot.exists():
            _drop_file_cache(snapshot)


def _record_snapshot_result(
    *,
    success: bool,
    started: float,
    size: int | None = None,
    estimated: int | None = None,
    error_type: str | None = None,
    source_name: str | None = None,
) -> None:
    with _STATE_LOCK:
        _SNAPSHOT_STATE["last_completed_monotonic"] = time.monotonic()
        _SNAPSHOT_STATE["last_duration_seconds"] = max(0.0, time.monotonic() - started)
        _SNAPSHOT_STATE["last_size_bytes"] = size
        _SNAPSHOT_STATE["last_estimated_bytes"] = estimated
        _SNAPSHOT_STATE["last_error_type"] = error_type
        _SNAPSHOT_STATE["last_source_name"] = source_name
        key = "successes" if success else "failures"
        _SNAPSHOT_STATE[key] = int(_SNAPSHOT_STATE.get(key, 0) or 0) + 1


def _install_snapshot_route(app: Any, runtime_provider: Callable[[], Any]) -> None:
    path = "/v1/operations/certification-db-snapshot"
    if path in {getattr(route, "path", None) for route in app.routes}:
        return

    @app.get(path)
    def certification_db_snapshot(
        x_certification_token: str | None = Header(default=None, alias="X-Certification-Token"),
    ) -> FileResponse:
        expected = _shared_token()
        if not expected:
            raise HTTPException(
                status_code=503,
                detail="certification snapshot authentication is not configured",
            )
        if not hmac.compare_digest(x_certification_token or "", expected):
            raise HTTPException(status_code=401, detail="invalid certification snapshot authorization")

        if not _SNAPSHOT_EXPORT_LOCK.acquire(blocking=False):
            with _STATE_LOCK:
                _SNAPSHOT_STATE["busy_rejections"] = int(_SNAPSHOT_STATE.get("busy_rejections", 0) or 0) + 1
            raise HTTPException(status_code=503, detail="certification snapshot export already in progress")

        started = time.monotonic()
        snapshot: Path | None = None
        source_name: str | None = None
        try:
            with _STATE_LOCK:
                _SNAPSHOT_STATE["attempts"] = int(_SNAPSHOT_STATE.get("attempts", 0) or 0) + 1
                _SNAPSHOT_STATE["last_started_monotonic"] = started
                _SNAPSHOT_STATE["last_error_type"] = None

            runtime = runtime_provider()
            store = getattr(runtime, "store", None)
            if store is None:
                raise HTTPException(status_code=503, detail="canonical runtime store unavailable")
            source_path = Path(getattr(store, "path", ""))
            source_name = source_path.name or None
            directory = _snapshot_directory(store)
            _cleanup_stale_exports(directory)
            fd, raw_path = tempfile.mkstemp(
                prefix=".certification-export-",
                suffix=".sqlite3",
                dir=str(directory),
            )
            os.close(fd)
            snapshot = Path(raw_path)

            _LOGGER.info(
                "SOLANA_ROI_CERTIFICATION_SNAPSHOT_START release=%s source=%s",
                _release_commit(),
                source_name or "unknown",
            )
            size, estimated = _snapshot_store_to_file(store, snapshot)
            _record_snapshot_result(
                success=True,
                started=started,
                size=size,
                estimated=estimated,
                source_name=source_name,
            )
            _LOGGER.info(
                "SOLANA_ROI_CERTIFICATION_SNAPSHOT_COMPLETE release=%s bytes=%s duration_seconds=%.3f",
                _release_commit(),
                size,
                max(0.0, time.monotonic() - started),
            )
        except HTTPException:
            if snapshot is not None:
                _dispose_snapshot(snapshot)
            _record_snapshot_result(
                success=False,
                started=started,
                error_type="HTTPException",
                source_name=source_name,
            )
            raise
        except Exception as exc:
            if snapshot is not None:
                _dispose_snapshot(snapshot)
            _record_snapshot_result(
                success=False,
                started=started,
                error_type=type(exc).__name__,
                source_name=source_name,
            )
            _LOGGER.warning(
                "SOLANA_ROI_CERTIFICATION_SNAPSHOT_FAILED release=%s error_type=%s duration_seconds=%.3f",
                _release_commit(),
                type(exc).__name__,
                max(0.0, time.monotonic() - started),
            )
            raise HTTPException(
                status_code=503,
                detail=f"canonical certification snapshot failed closed:{type(exc).__name__}",
            ) from exc
        finally:
            _SNAPSHOT_EXPORT_LOCK.release()

        assert snapshot is not None
        return FileResponse(
            snapshot,
            media_type="application/vnd.sqlite3",
            filename="solana-roi-certification.sqlite3",
            headers={
                "X-Release-Commit": _release_commit(),
                "X-Certification-Snapshot-Bytes": str(size),
                "X-Certification-Split-Version": SPLIT_VERSION,
            },
            background=BackgroundTask(_dispose_snapshot, snapshot),
        )


def _strip_local_certification_workers() -> None:
    """Keep only the bounded 15-second forward publisher in the runtime process.

    E2E and production-proof builders remain isolated in the certifier cgroup. The
    forward publisher is intentionally retained because its existing 45-second stale
    contract cannot be satisfied behind a full-database export whose bounded deadline
    is 55 seconds. It reads the authoritative live store, remains single-flight and
    resource-guarded, and still cannot sign, submit or grant live-money authority.
    """
    global _ORIGINAL_RUNTIME_WORKERS
    base = e2e._ORIGINAL_RUNTIME_WORKERS
    if not callable(base):
        raise RuntimeError("certification split cannot resolve canonical runtime worker base")
    forward_worker = certification_runtime._runtime_workers_with_forward_snapshot
    if not callable(forward_worker):
        raise RuntimeError("certification split cannot resolve bounded forward publisher")
    _ORIGINAL_RUNTIME_WORKERS = render_bootstrap._run_runtime_workers
    certification_runtime._ORIGINAL_RUNTIME_WORKERS = base

    async def runtime_workers_with_local_forward_only(runtime: Any, stop: Any) -> None:
        await forward_worker(runtime, stop)

    setattr(runtime_workers_with_local_forward_only, "_roi_e2e_status_snapshot_worker", True)
    setattr(runtime_workers_with_local_forward_only, "_roi_production_proof_snapshot_worker", True)
    setattr(runtime_workers_with_local_forward_only, "_roi_forward_certification_snapshot_worker", True)
    setattr(runtime_workers_with_local_forward_only, "_roi_certification_split_runtime", True)
    setattr(runtime_workers_with_local_forward_only, "_roi_local_certification_builders_disabled", False)
    setattr(runtime_workers_with_local_forward_only, "_roi_local_heavy_certification_builders_disabled", True)
    setattr(runtime_workers_with_local_forward_only, "_roi_local_forward_publisher_retained", True)
    render_bootstrap._run_runtime_workers = runtime_workers_with_local_forward_only  # type: ignore[assignment]


def status() -> dict[str, Any]:
    with _STATE_LOCK:
        snapshot_state = dict(_SNAPSHOT_STATE)
    return {
        "split_version": SPLIT_VERSION,
        "enabled": split_runtime_enabled(),
        "role": "authoritative_runtime",
        "certifier_url_configured": bool(_certifier_url()),
        "shared_auth_configured": bool(_shared_token()),
        "runtime_executes_local_certification_builders": False,
        "runtime_executes_local_heavy_certification_builders": False,
        "runtime_executes_local_forward_publisher": bool(split_runtime_enabled()),
        "forward_publication_interval_seconds": certification_runtime.FORWARD_SNAPSHOT_INTERVAL_SECONDS,
        "forward_publication_stale_seconds": certification_runtime.FORWARD_SNAPSHOT_STALE_SECONDS,
        "forward_stale_threshold_changed": False,
        "canonical_sqlite_owner": "authoritative_runtime",
        "snapshot_export": "pinned_wal_read_transaction_bounded_online_backup",
        "snapshot_holds_runtime_store_lock": False,
        "snapshot_uses_runtime_persistent_disk": True,
        "snapshot_shared_writable_disk": False,
        "snapshot_single_flight": True,
        "snapshot_file_cache_release_advisory": True,
        "snapshot_response_cleanup_releases_cache": True,
        "snapshot_deadline_seconds": _snapshot_deadline_seconds(),
        "snapshot_pages_per_step": _snapshot_pages_per_step(),
        "snapshot_step_sleep_seconds": _snapshot_step_sleep_seconds(),
        "snapshot_max_bytes": _snapshot_max_bytes(),
        "snapshot_free_reserve_bytes": _snapshot_free_reserve_bytes(),
        "snapshot_state": snapshot_state,
        "shared_writable_disk": False,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        "strategy_thresholds_changed": False,
        "certification_thresholds_changed": False,
        "continuity_semantics_changed": False,
    }


def install_certification_service_split(app: Any, runtime_provider: Callable[[], Any]) -> None:
    path = "/v1/operations/certification-service-split"
    if path not in {getattr(route, "path", None) for route in app.routes}:
        app.add_api_route(path, status, methods=["GET"], name="certification_service_split")

    app.state.roi_certification_service_split_version = SPLIT_VERSION
    app.state.roi_certification_service_split_enabled = split_runtime_enabled()
    if not split_runtime_enabled():
        return

    _install_snapshot_route(app, runtime_provider)
    # E2E and production proof stay remote. Forward keeps the precomputed local
    # endpoint so its existing 15s publication / 45s stale contract remains viable.
    for surface in (
        "/v1/strategy/e2e-status",
        "/v1/strategy/production-proof",
    ):
        _replace_get_route(app, surface)

    _strip_local_certification_workers()

    app.state.roi_v51_system_proof_precompute = None
    app.state.roi_v51_system_proof_precompute_worker_enabled = False

    app.state.roi_certification_local_heavy_workers_disabled = True
    app.state.roi_certification_local_forward_publisher = True
    app.state.roi_certification_remote_proxy = True
    app.state.roi_certification_snapshot_export = True
    app.state.roi_certification_shared_writable_disk = False
    app.state.roi_certification_strategy_contract_relaxed = False
    app.state.roi_certification_paper_only = True
    app.state.roi_certification_live_money_authority = False


__all__ = [
    "SPLIT_VERSION",
    "_cleanup_stale_exports",
    "_dispose_snapshot",
    "_drop_file_cache",
    "_snapshot_store_to_file",
    "install_certification_service_split",
    "split_runtime_enabled",
    "status",
]
