from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException


SERVICE_VERSION = "isolated-certifier-service-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_LOCK = threading.Lock()
_ARTIFACTS: dict[str, dict[str, Any]] = {}
_PUBLISHED: dict[str, float] = {}
_STATE: dict[str, Any] = {
    "cycles": 0,
    "successes": 0,
    "failures": 0,
    "last_started_at_monotonic": None,
    "last_completed_at_monotonic": None,
    "last_error_type": None,
    "last_snapshot_release": None,
    "active_child_pid": None,
}

_SURFACE_FILES = {
    "e2e": "e2e.json",
    "forward": "forward.json",
    "production": "production.json",
}
_STALE_SECONDS = {
    "e2e": 120.0,
    "forward": 45.0,
    "production": 300.0,
}


def _release_commit() -> str:
    for key in ("RENDER_GIT_COMMIT", "GITHUB_SHA", "SOLANA_ROI_RELEASE_COMMIT"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return "unbound-local-release"


def _runtime_url() -> str:
    return os.getenv("SOLANA_ROI_RUNTIME_URL", "").strip().rstrip("/")


def _token() -> str:
    return os.getenv("SOLANA_ROI_CERTIFICATION_SHARED_TOKEN", "").strip()


def _interval_seconds() -> float:
    try:
        return max(0.0, float(os.getenv("SOLANA_ROI_CERTIFIER_CYCLE_INTERVAL_SECONDS", "2")))
    except ValueError:
        return 2.0


def _child_timeout_seconds() -> float:
    try:
        return max(30.0, float(os.getenv("SOLANA_ROI_CERTIFIER_CHILD_TIMEOUT_SECONDS", "300")))
    except ValueError:
        return 300.0


def _require_token(value: str | None) -> None:
    import hmac

    expected = _token()
    if not expected:
        raise HTTPException(status_code=503, detail="certifier authentication is not configured")
    if not hmac.compare_digest(value or "", expected):
        raise HTTPException(status_code=401, detail="invalid certifier authorization")


def _fail_closed(surface: str, reason: str) -> dict[str, Any]:
    if surface == "e2e":
        from . import e2e_status_read_boundary_repair as e2e

        payload = e2e._fail_closed_payload(reason)
    elif surface == "forward":
        from . import certification_generation_runtime_repair as certification_runtime

        payload = certification_runtime._fail_closed_forward(reason)
    else:
        from . import production_proof_read_boundary_repair as proof

        payload = proof._fail_closed_payload(reason)
    boundary = payload.setdefault("isolated_certifier", {})
    if isinstance(boundary, dict):
        boundary.update(
            {
                "service_version": SERVICE_VERSION,
                "state": "failed_closed",
                "reason": reason,
                "release_commit": _release_commit(),
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
            }
        )
    return payload


def _cached(surface: str) -> dict[str, Any]:
    with _LOCK:
        payload = json.loads(json.dumps(_ARTIFACTS.get(surface))) if surface in _ARTIFACTS else None
        published = _PUBLISHED.get(surface)
    if not isinstance(payload, dict) or not isinstance(published, (int, float)):
        return _fail_closed(surface, f"isolated_certifier_{surface}_not_ready")
    age = max(0.0, time.monotonic() - published)
    if age > _STALE_SECONDS[surface]:
        return _fail_closed(surface, f"isolated_certifier_{surface}_stale")
    boundary = payload.setdefault("isolated_certifier", {})
    if isinstance(boundary, dict):
        boundary.update(
            {
                "service_version": SERVICE_VERSION,
                "state": "ready",
                "snapshot_age_seconds": age,
                "release_commit": _release_commit(),
                "child_process_isolation": True,
                "shared_writable_disk": False,
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
            }
        )
    return payload


def _surface_release(surface: str, payload: dict[str, Any]) -> str:
    if surface in {"e2e", "forward"}:
        return str(payload.get("release_commit") or "")
    release = payload.get("release")
    return str(release.get("release_commit") or "") if isinstance(release, dict) else ""


def _publish_file(surface: str, path: Path, expected_release: str) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict):
        return False
    if _surface_release(surface, payload) != expected_release:
        return False
    with _LOCK:
        _ARTIFACTS[surface] = payload
        _PUBLISHED[surface] = time.monotonic()
    return True


def _download_snapshot(destination: Path) -> str:
    base = _runtime_url()
    token = _token()
    if not base or not token:
        raise RuntimeError("runtime snapshot source is not configured")
    request = urllib.request.Request(
        f"{base}/v1/operations/certification-db-snapshot",
        headers={
            "Accept": "application/vnd.sqlite3",
            "X-Certification-Token": token,
            "User-Agent": "solana-roi-isolated-certifier/1",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=60.0) as response:
            release = str(response.headers.get("X-Release-Commit") or "")
            with destination.open("wb") as handle:
                shutil.copyfileobj(response, handle, length=1024 * 1024)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise RuntimeError(f"runtime snapshot download failed:{type(exc).__name__}") from exc
    if not release:
        raise RuntimeError("runtime snapshot missing release binding")
    if release != _release_commit():
        raise RuntimeError(f"runtime snapshot release mismatch:{release}:{_release_commit()}")
    if destination.stat().st_size <= 0:
        raise RuntimeError("runtime snapshot is empty")
    return release


def _run_cycle_sync(stop_requested: threading.Event) -> None:
    with _LOCK:
        _STATE["cycles"] = int(_STATE.get("cycles", 0) or 0) + 1
        _STATE["last_started_at_monotonic"] = time.monotonic()
        _STATE["last_error_type"] = None

    error_type: str | None = None
    success = False
    try:
        with tempfile.TemporaryDirectory(prefix="roi-isolated-certifier-") as raw_dir:
            directory = Path(raw_dir)
            snapshot = directory / "canonical.sqlite3"
            output = directory / "artifacts"
            output.mkdir()
            release = _download_snapshot(snapshot)
            with _LOCK:
                _STATE["last_snapshot_release"] = release

            env = dict(os.environ)
            env["SOLANA_ROI_DB_PATH"] = str(snapshot)
            env["SOLANA_ROI_RELEASE_COMMIT"] = release
            env.pop("SOLANA_ROI_CERTIFICATION_SPLIT_RUNTIME", None)
            command = [
                sys.executable,
                "-m",
                "solana_roi.certifier_job",
                "--output-dir",
                str(output),
            ]
            process = subprocess.Popen(
                command,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            with _LOCK:
                _STATE["active_child_pid"] = process.pid
            published: set[str] = set()
            started = time.monotonic()
            while process.poll() is None:
                if stop_requested.is_set():
                    process.terminate()
                    break
                if time.monotonic() - started > _child_timeout_seconds():
                    process.kill()
                    raise TimeoutError("isolated certification child exceeded bounded runtime")
                for surface, filename in _SURFACE_FILES.items():
                    if surface not in published and _publish_file(surface, output / filename, release):
                        published.add(surface)
                time.sleep(0.25)

            stdout, stderr = process.communicate(timeout=5)
            _ = stdout
            for surface, filename in _SURFACE_FILES.items():
                if surface not in published and _publish_file(surface, output / filename, release):
                    published.add(surface)
            if process.returncode != 0:
                tail = (stderr or "")[-1000:].replace("\n", " ")
                raise RuntimeError(f"isolated certification child failed:{process.returncode}:{tail}")
            if published != set(_SURFACE_FILES):
                raise RuntimeError(f"isolated certification child incomplete:{sorted(published)}")
            success = True
    except BaseException as exc:
        error_type = type(exc).__name__
    finally:
        with _LOCK:
            _STATE["active_child_pid"] = None
            _STATE["last_completed_at_monotonic"] = time.monotonic()
            _STATE["last_error_type"] = error_type
            if success:
                _STATE["successes"] = int(_STATE.get("successes", 0) or 0) + 1
            else:
                _STATE["failures"] = int(_STATE.get("failures", 0) or 0) + 1


async def _worker(stop: asyncio.Event) -> None:
    while not stop.is_set():
        thread_stop = threading.Event()
        task = asyncio.create_task(asyncio.to_thread(_run_cycle_sync, thread_stop))
        stop_wait = asyncio.create_task(stop.wait())
        done, _pending = await asyncio.wait({task, stop_wait}, return_when=asyncio.FIRST_COMPLETED)
        if stop_wait in done and not task.done():
            thread_stop.set()
            await task
            return
        stop_wait.cancel()
        try:
            await stop_wait
        except asyncio.CancelledError:
            pass
        if stop.is_set():
            return
        try:
            await asyncio.wait_for(stop.wait(), timeout=_interval_seconds())
        except TimeoutError:
            continue


@asynccontextmanager
async def lifespan(app: FastAPI):
    stop = asyncio.Event()
    task = asyncio.create_task(_worker(stop), name="isolated-certification-worker")
    app.state.roi_isolated_certifier_worker = True
    try:
        yield
    finally:
        stop.set()
        await task


app = FastAPI(title="Solana ROI Isolated Certifier", version="1.0", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, Any]:
    with _LOCK:
        state = dict(_STATE)
        freshness = {
            surface: (
                max(0.0, time.monotonic() - published)
                if isinstance(published, (int, float))
                else None
            )
            for surface, published in _PUBLISHED.items()
        }
    return {
        "status": "ok",
        "liveness_only": True,
        "service_version": SERVICE_VERSION,
        "role": "isolated_certification",
        "release_commit": _release_commit(),
        "runtime_url_configured": bool(_runtime_url()),
        "shared_auth_configured": bool(_token()),
        "artifacts": freshness,
        "state": state,
        "child_process_isolation": True,
        "shared_writable_disk": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


@app.get("/v1/strategy/e2e-status")
def e2e_status(
    x_certification_token: str | None = Header(default=None, alias="X-Certification-Token"),
) -> dict[str, Any]:
    _require_token(x_certification_token)
    return _cached("e2e")


@app.get("/v1/strategy/forward-certification")
def forward_certification(
    x_certification_token: str | None = Header(default=None, alias="X-Certification-Token"),
) -> dict[str, Any]:
    _require_token(x_certification_token)
    return _cached("forward")


@app.get("/v1/strategy/production-proof")
def production_proof(
    x_certification_token: str | None = Header(default=None, alias="X-Certification-Token"),
) -> dict[str, Any]:
    _require_token(x_certification_token)
    return _cached("production")


@app.get("/v1/operations/isolated-certifier")
def isolated_certifier_status(
    x_certification_token: str | None = Header(default=None, alias="X-Certification-Token"),
) -> dict[str, Any]:
    _require_token(x_certification_token)
    return health()


__all__ = ["SERVICE_VERSION", "app", "health"]
