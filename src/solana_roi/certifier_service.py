from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import sqlite3
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


SERVICE_VERSION = "isolated-certifier-service-v3-snapshot-transfer-integrity"
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
    "last_snapshot_expected_bytes": None,
    "last_snapshot_received_bytes": None,
    "last_snapshot_validation": None,
    "last_published_surfaces": [],
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
_SQLITE_MAGIC = b"SQLite format 3\x00"


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


def _safe_error_message(exc: BaseException) -> str:
    """Return bounded operational diagnostics without leaking service credentials."""
    text = str(exc).replace("\n", " ").replace("\r", " ")
    token = _token()
    if token:
        text = text.replace(token, "[redacted]")
    text = re.sub(
        r"(?i)\b(authorization|password|secret|token)\b(\s*[:=]\s*)([^\s,;]+)",
        r"\1\2[redacted]",
        text,
    )
    return text[-1600:]


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


def _validate_sqlite_snapshot(path: Path, expected_bytes: int) -> int:
    """Validate transfer completeness before a snapshot can reach the child process.

    The runtime already creates the snapshot with SQLite's online-backup API. The
    certifier therefore validates transport integrity here: exact advertised byte
    count, SQLite header/page geometry, and a read-only schema open. This prevents a
    truncated HTTP body from being mistaken for a valid point-in-time database while
    avoiding a second full-database integrity scan before the bounded child starts.
    """
    actual_bytes = int(path.stat().st_size)
    if expected_bytes <= 0:
        raise RuntimeError("runtime snapshot advertised invalid byte count")
    if actual_bytes != expected_bytes:
        raise RuntimeError(
            f"runtime snapshot byte-count mismatch:{actual_bytes}:{expected_bytes}"
        )

    with path.open("rb") as handle:
        header = handle.read(100)
    if len(header) < 100 or not header.startswith(_SQLITE_MAGIC):
        raise RuntimeError("runtime snapshot SQLite header invalid")

    page_size_raw = int.from_bytes(header[16:18], "big")
    page_size = 65536 if page_size_raw == 1 else page_size_raw
    if page_size < 512 or page_size > 65536 or page_size & (page_size - 1):
        raise RuntimeError("runtime snapshot SQLite page size invalid")
    header_page_count = int.from_bytes(header[28:32], "big")
    if header_page_count <= 0:
        raise RuntimeError("runtime snapshot SQLite page count invalid")
    geometry_bytes = page_size * header_page_count
    if geometry_bytes != actual_bytes:
        raise RuntimeError(
            f"runtime snapshot SQLite geometry mismatch:{actual_bytes}:{geometry_bytes}"
        )

    uri = f"file:{path.resolve()}?mode=ro&immutable=1"
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        connection.execute("PRAGMA query_only=ON")
        connection.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
    except sqlite3.DatabaseError as exc:
        raise RuntimeError(
            f"runtime snapshot SQLite open failed:{type(exc).__name__}"
        ) from exc
    finally:
        if connection is not None:
            connection.close()
    return actual_bytes


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
    release = ""
    expected_bytes: int | None = None
    content_length: int | None = None
    try:
        with urllib.request.urlopen(request, timeout=60.0) as response:
            release = str(response.headers.get("X-Release-Commit") or "")
            raw_expected = str(response.headers.get("X-Certification-Snapshot-Bytes") or "").strip()
            if not raw_expected:
                raise RuntimeError("runtime snapshot missing byte-count binding")
            try:
                expected_bytes = int(raw_expected)
            except ValueError as exc:
                raise RuntimeError("runtime snapshot byte-count binding invalid") from exc
            raw_content_length = str(response.headers.get("Content-Length") or "").strip()
            if raw_content_length:
                try:
                    content_length = int(raw_content_length)
                except ValueError as exc:
                    raise RuntimeError("runtime snapshot content-length invalid") from exc
                if content_length != expected_bytes:
                    raise RuntimeError(
                        f"runtime snapshot header-length mismatch:{content_length}:{expected_bytes}"
                    )
            with destination.open("wb") as handle:
                shutil.copyfileobj(response, handle, length=1024 * 1024)
    except RuntimeError:
        raise
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise RuntimeError(f"runtime snapshot download failed:{type(exc).__name__}") from exc

    if not release:
        raise RuntimeError("runtime snapshot missing release binding")
    if release != _release_commit():
        raise RuntimeError(f"runtime snapshot release mismatch:{release}:{_release_commit()}")
    if expected_bytes is None:
        raise RuntimeError("runtime snapshot missing validated byte count")

    try:
        actual_bytes = _validate_sqlite_snapshot(destination, expected_bytes)
    except BaseException:
        with _LOCK:
            _STATE["last_snapshot_expected_bytes"] = expected_bytes
            _STATE["last_snapshot_received_bytes"] = (
                int(destination.stat().st_size) if destination.exists() else None
            )
            _STATE["last_snapshot_validation"] = "failed"
        raise

    with _LOCK:
        _STATE["last_snapshot_expected_bytes"] = expected_bytes
        _STATE["last_snapshot_received_bytes"] = actual_bytes
        _STATE["last_snapshot_validation"] = "passed"
    return release


def _run_cycle_sync(stop_requested: threading.Event) -> None:
    with _LOCK:
        _STATE["cycles"] = int(_STATE.get("cycles", 0) or 0) + 1
        _STATE["last_started_at_monotonic"] = time.monotonic()

    error_type: str | None = None
    error_message: str | None = None
    success = False
    published: set[str] = set()
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
        error_message = _safe_error_message(exc)
        diagnostic = {
            "event": "isolated_certifier_cycle_failed",
            "error_type": error_type,
            "error_message": error_message,
            "published_surfaces": sorted(published),
            "release_commit": _release_commit(),
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
        print(
            "SOLANA_ROI_CERTIFIER_CYCLE_FAILED "
            + json.dumps(diagnostic, sort_keys=True, separators=(",", ":")),
            flush=True,
        )
    finally:
        with _LOCK:
            _STATE["active_child_pid"] = None
            _STATE["last_completed_at_monotonic"] = time.monotonic()
            _STATE["last_published_surfaces"] = sorted(published)
            # Preserve the last failure through the next cycle. It is cleared only
            # by a complete successful e2e+forward+production cycle.
            if success:
                _STATE["last_error_type"] = None
                _STATE["successes"] = int(_STATE.get("successes", 0) or 0) + 1
            else:
                _STATE["last_error_type"] = error_type
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
        "snapshot_transfer_integrity": {
            "advertised_byte_count_required": True,
            "content_length_cross_checked_when_present": True,
            "sqlite_header_and_page_geometry_checked": True,
            "read_only_schema_open_checked": True,
            "full_integrity_scan_duplicated_before_child": False,
        },
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


__all__ = [
    "SERVICE_VERSION",
    "_download_snapshot",
    "_validate_sqlite_snapshot",
    "app",
    "health",
]
