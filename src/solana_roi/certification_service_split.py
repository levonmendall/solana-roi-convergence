from __future__ import annotations

import hmac
import json
import os
import sqlite3
import tempfile
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


SPLIT_VERSION = "certification-service-split-v2-nonblocking-snapshot"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
DEFAULT_REMOTE_TIMEOUT_SECONDS = 5.0
DEFAULT_SNAPSHOT_MAX_BYTES = 8 * 1024 * 1024 * 1024
DEFAULT_SNAPSHOT_PAGES_PER_STEP = 512
DEFAULT_SNAPSHOT_STEP_SLEEP_SECONDS = 0.01

_ORIGINAL_RUNTIME_WORKERS: Callable[..., Any] | None = None


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


def _snapshot_store_to_file(store: Any, snapshot: Path) -> int:
    """Create a point-in-time SQLite copy without holding the live writer lock.

    The canonical runtime remains the only writer. A separate read-only connection
    participates in SQLite WAL snapshot semantics while ``Connection.backup`` copies
    bounded page batches and yields between them. This lets ingestion, paper lifecycle,
    settlement and reconciliation continue while certification receives an immutable
    copy. The certifier never mounts or writes the runtime disk.
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
    try:
        source.execute("PRAGMA query_only=ON")
        source.execute("PRAGMA busy_timeout=5000")
        source.backup(
            destination,
            pages=_snapshot_pages_per_step(),
            sleep=_snapshot_step_sleep_seconds(),
        )
        destination.commit()
    finally:
        destination.close()
        source.close()
    return int(snapshot.stat().st_size)


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

        runtime = runtime_provider()
        store = getattr(runtime, "store", None)
        if store is None:
            raise HTTPException(status_code=503, detail="canonical runtime store unavailable")

        fd, raw_path = tempfile.mkstemp(prefix="roi-certification-", suffix=".sqlite3", dir="/tmp")
        os.close(fd)
        snapshot = Path(raw_path)
        try:
            size = _snapshot_store_to_file(store, snapshot)
            if size > _snapshot_max_bytes():
                snapshot.unlink(missing_ok=True)
                raise HTTPException(
                    status_code=503,
                    detail="canonical certification snapshot exceeds configured bounded export",
                )
        except HTTPException:
            raise
        except Exception as exc:
            snapshot.unlink(missing_ok=True)
            raise HTTPException(
                status_code=503,
                detail=f"canonical certification snapshot failed closed:{type(exc).__name__}",
            ) from exc

        return FileResponse(
            snapshot,
            media_type="application/vnd.sqlite3",
            filename="solana-roi-certification.sqlite3",
            headers={
                "X-Release-Commit": _release_commit(),
                "X-Certification-Snapshot-Bytes": str(size),
                "X-Certification-Split-Version": SPLIT_VERSION,
            },
            background=BackgroundTask(snapshot.unlink, missing_ok=True),
        )


def _strip_local_certification_workers() -> None:
    global _ORIGINAL_RUNTIME_WORKERS
    base = e2e._ORIGINAL_RUNTIME_WORKERS
    if not callable(base):
        raise RuntimeError("certification split cannot resolve canonical runtime worker base")
    _ORIGINAL_RUNTIME_WORKERS = render_bootstrap._run_runtime_workers

    async def runtime_workers_without_local_certification(runtime: Any, stop: Any) -> None:
        await base(runtime, stop)

    # Preserve introspection markers while making the transfer explicit. The
    # certification wrappers are intentionally bypassed because their heavy work is
    # now owned by another Render cgroup; the base real-time worker chain is retained.
    setattr(runtime_workers_without_local_certification, "_roi_e2e_status_snapshot_worker", True)
    setattr(runtime_workers_without_local_certification, "_roi_production_proof_snapshot_worker", True)
    setattr(runtime_workers_without_local_certification, "_roi_forward_certification_snapshot_worker", True)
    setattr(runtime_workers_without_local_certification, "_roi_certification_split_runtime", True)
    setattr(runtime_workers_without_local_certification, "_roi_local_certification_builders_disabled", True)
    render_bootstrap._run_runtime_workers = runtime_workers_without_local_certification  # type: ignore[assignment]


def status() -> dict[str, Any]:
    return {
        "split_version": SPLIT_VERSION,
        "enabled": split_runtime_enabled(),
        "role": "authoritative_runtime",
        "certifier_url_configured": bool(_certifier_url()),
        "shared_auth_configured": bool(_shared_token()),
        "runtime_executes_local_certification_builders": False if split_runtime_enabled() else True,
        "canonical_sqlite_owner": "authoritative_runtime",
        "snapshot_export": "sqlite_online_backup_point_in_time_read_only_connection",
        "snapshot_holds_runtime_store_lock": False,
        "snapshot_pages_per_step": _snapshot_pages_per_step(),
        "snapshot_step_sleep_seconds": _snapshot_step_sleep_seconds(),
        "snapshot_max_bytes": _snapshot_max_bytes(),
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
    for surface in (
        "/v1/strategy/e2e-status",
        "/v1/strategy/forward-certification",
        "/v1/strategy/production-proof",
    ):
        _replace_get_route(app, surface)

    _strip_local_certification_workers()

    # The legacy system-proof warmer is also certification/analytics work. Keep its
    # callback installed for code reachability, but prevent the authoritative
    # runtime lifespan from starting the local precompute task.
    app.state.roi_v51_system_proof_precompute = None
    app.state.roi_v51_system_proof_precompute_worker_enabled = False

    app.state.roi_certification_local_heavy_workers_disabled = True
    app.state.roi_certification_remote_proxy = True
    app.state.roi_certification_snapshot_export = True
    app.state.roi_certification_shared_writable_disk = False
    app.state.roi_certification_strategy_contract_relaxed = False
    app.state.roi_certification_paper_only = True
    app.state.roi_certification_live_money_authority = False


__all__ = [
    "SPLIT_VERSION",
    "install_certification_service_split",
    "split_runtime_enabled",
    "status",
]
