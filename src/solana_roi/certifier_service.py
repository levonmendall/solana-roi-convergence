from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException

from .certification_chunk_transfer import download_snapshot_chunked
from .certification_replica_client import clone_replica_for_cycle, status as replica_status, synchronize_replica


SERVICE_VERSION = "isolated-certifier-service-v6-child-lifecycle"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_LOCK = threading.Lock()
_CYCLE_SINGLE_FLIGHT = threading.Lock()
_ARTIFACTS: dict[str, dict[str, Any]] = {}
_PUBLISHED: dict[str, float] = {}
_STATE: dict[str, Any] = {
    "cycles": 0,
    "successes": 0,
    "failures": 0,
    "consecutive_failures": 0,
    "last_started_at_monotonic": None,
    "last_completed_at_monotonic": None,
    "last_error_type": None,
    "last_snapshot_release": None,
    "last_snapshot_expected_bytes": None,
    "last_snapshot_received_bytes": None,
    "last_snapshot_validation": None,
    "last_published_surfaces": [],
    "active_cycle": False,
    "active_child_pid": None,
    "last_child_pid": None,
    "last_child_returncode": None,
    "child_launches": 0,
    "child_reaps": 0,
    "child_terminate_requests": 0,
    "child_forced_kills": 0,
    "child_reap_failures": 0,
    "cycle_overlap_rejections": 0,
    "replica_bootstraps": 0,
    "replica_delta_cycles": 0,
    "last_replica_transport": None,
    "last_replica_watermark": None,
    "last_replica_delta_changes": None,
    "last_replica_delta_bytes": None,
    "last_local_clone_method": None,
    "current_retry_delay_seconds": None,
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
_NATIVE_THREAD_ENV_KEYS = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "BLIS_NUM_THREADS",
)


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
    """Normal certification cadence after a successful replica-backed cycle."""
    try:
        return max(5.0, float(os.getenv("SOLANA_ROI_CERTIFIER_CYCLE_INTERVAL_SECONDS", "15")))
    except ValueError:
        return 15.0


def _retry_delay_seconds(consecutive_failures: int) -> float:
    """Bound retry pressure without weakening any certification staleness gate."""
    base = max(5.0, _interval_seconds())
    exponent = max(0, min(3, int(consecutive_failures) - 1))
    return min(60.0, base * (2 ** exponent))


def _child_timeout_seconds() -> float:
    try:
        return max(30.0, float(os.getenv("SOLANA_ROI_CERTIFIER_CHILD_TIMEOUT_SECONDS", "300")))
    except ValueError:
        return 300.0


def _safe_error_message(exc: BaseException) -> str:
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
                "incremental_replica_required": True,
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
                "incremental_replica": True,
                "authoritative_full_snapshot_per_cycle": False,
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
    """Compatibility validator retained for bootstrap and regression contracts."""
    actual_bytes = int(path.stat().st_size)
    if expected_bytes <= 0:
        raise RuntimeError("runtime snapshot advertised invalid byte count")
    if actual_bytes != expected_bytes:
        raise RuntimeError(f"runtime snapshot byte-count mismatch:{actual_bytes}:{expected_bytes}")
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
        raise RuntimeError(f"runtime snapshot SQLite geometry mismatch:{actual_bytes}:{geometry_bytes}")
    uri = f"file:{path.resolve()}?mode=ro&immutable=1"
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        connection.execute("PRAGMA query_only=ON")
        connection.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
    except sqlite3.DatabaseError as exc:
        raise RuntimeError(f"runtime snapshot SQLite open failed:{type(exc).__name__}") from exc
    finally:
        if connection is not None:
            connection.close()
    return actual_bytes


def _download_snapshot(destination: Path) -> str:
    """Legacy one-shot bootstrap helper retained for existing regression callers."""
    release, expected_bytes = download_snapshot_chunked(
        destination,
        base=_runtime_url(),
        token=_token(),
        expected_release=_release_commit(),
    )
    try:
        actual_bytes = _validate_sqlite_snapshot(destination, expected_bytes)
    except BaseException:
        with _LOCK:
            _STATE["last_snapshot_expected_bytes"] = expected_bytes
            _STATE["last_snapshot_received_bytes"] = int(destination.stat().st_size) if destination.exists() else None
            _STATE["last_snapshot_validation"] = "failed"
        raise
    with _LOCK:
        _STATE["last_snapshot_expected_bytes"] = expected_bytes
        _STATE["last_snapshot_received_bytes"] = actual_bytes
        _STATE["last_snapshot_validation"] = "passed"
    return release


def _record_replica_sync(sync: dict[str, Any]) -> None:
    with _LOCK:
        if bool(sync.get("bootstrapped")):
            _STATE["replica_bootstraps"] = int(_STATE.get("replica_bootstraps", 0) or 0) + 1
            _STATE["last_snapshot_expected_bytes"] = sync.get("bootstrap_bytes")
            _STATE["last_snapshot_received_bytes"] = sync.get("bootstrap_bytes")
            _STATE["last_snapshot_validation"] = "passed"
        else:
            _STATE["replica_delta_cycles"] = int(_STATE.get("replica_delta_cycles", 0) or 0) + 1
            _STATE["last_snapshot_validation"] = "not_required_incremental_delta"
        _STATE["last_snapshot_release"] = sync.get("release_commit")
        _STATE["last_replica_transport"] = sync.get("last_transport")
        _STATE["last_replica_watermark"] = sync.get("watermark")
        _STATE["last_replica_delta_changes"] = sync.get("delta_change_count")
        _STATE["last_replica_delta_bytes"] = sync.get("delta_payload_bytes")


def _child_environment(snapshot: Path, release: str) -> dict[str, str]:
    """Build a certifier-only environment with bounded native thread fan-out."""
    env = dict(os.environ)
    env["SOLANA_ROI_DB_PATH"] = str(snapshot)
    env["SOLANA_ROI_RELEASE_COMMIT"] = release
    env.pop("SOLANA_ROI_CERTIFICATION_SPLIT_RUNTIME", None)
    for key in _NATIVE_THREAD_ENV_KEYS:
        env[key] = "1"
    return env


def _tail_text(path: Path, max_bytes: int = 4096) -> str:
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - max_bytes))
            raw = handle.read(max_bytes)
    except OSError:
        return ""
    return raw.decode("utf-8", errors="replace")[-1000:].replace("\n", " ").replace("\r", " ")


def _reap_child(
    process: subprocess.Popen[Any],
    *,
    terminate_first: bool = False,
    kill_first: bool = False,
) -> int:
    """Guarantee the certifier child reaches a waited/reaped terminal state."""
    terminated = False
    forced_kill = False
    if process.poll() is None:
        if kill_first:
            process.kill()
            forced_kill = True
        elif terminate_first:
            process.terminate()
            terminated = True
            try:
                process.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                process.kill()
                forced_kill = True
    try:
        returncode = int(process.wait(timeout=5.0))
    except subprocess.TimeoutExpired:
        process.kill()
        forced_kill = True
        returncode = int(process.wait(timeout=5.0))
    with _LOCK:
        _STATE["child_reaps"] = int(_STATE.get("child_reaps", 0) or 0) + 1
        if terminated:
            _STATE["child_terminate_requests"] = int(_STATE.get("child_terminate_requests", 0) or 0) + 1
        if forced_kill:
            _STATE["child_forced_kills"] = int(_STATE.get("child_forced_kills", 0) or 0) + 1
        _STATE["last_child_returncode"] = returncode
        if _STATE.get("active_child_pid") == process.pid:
            _STATE["active_child_pid"] = None
    return returncode


def _run_cycle_single_flight(stop_requested: threading.Event) -> bool:
    with _LOCK:
        _STATE["cycles"] = int(_STATE.get("cycles", 0) or 0) + 1
        _STATE["last_started_at_monotonic"] = time.monotonic()
        _STATE["active_cycle"] = True
    error_type: str | None = None
    success = False
    published: set[str] = set()
    process: subprocess.Popen[Any] | None = None
    child_reaped = False
    try:
        replica, sync = synchronize_replica(base=_runtime_url(), token=_token(), expected_release=_release_commit())
        _record_replica_sync(sync)
        with tempfile.TemporaryDirectory(prefix="roi-isolated-certifier-") as raw_dir:
            directory = Path(raw_dir)
            snapshot = directory / "canonical.sqlite3"
            output = directory / "artifacts"
            stdout_path = directory / "child.stdout.log"
            stderr_path = directory / "child.stderr.log"
            output.mkdir()
            clone_method = clone_replica_for_cycle(replica, snapshot)
            _validate_sqlite_snapshot(snapshot, int(snapshot.stat().st_size))
            with _LOCK:
                _STATE["last_local_clone_method"] = clone_method
            release = _release_commit()
            env = _child_environment(snapshot, release)
            command = [sys.executable, "-m", "solana_roi.certifier_job", "--output-dir", str(output)]
            with stdout_path.open("wb") as stdout_handle, stderr_path.open("wb") as stderr_handle:
                process = subprocess.Popen(command, env=env, stdout=stdout_handle, stderr=stderr_handle)
                with _LOCK:
                    _STATE["active_child_pid"] = process.pid
                    _STATE["last_child_pid"] = process.pid
                    _STATE["child_launches"] = int(_STATE.get("child_launches", 0) or 0) + 1
                started = time.monotonic()
                timed_out = False
                stop_exit = False
                while process.poll() is None:
                    if stop_requested.is_set():
                        stop_exit = True
                        break
                    if time.monotonic() - started > _child_timeout_seconds():
                        timed_out = True
                        break
                    for surface, filename in _SURFACE_FILES.items():
                        if surface not in published and _publish_file(surface, output / filename, release):
                            published.add(surface)
                    time.sleep(0.25)

                if stop_exit:
                    _reap_child(process, terminate_first=True)
                    child_reaped = True
                    raise RuntimeError("isolated certification child stopped during service shutdown")
                if timed_out:
                    _reap_child(process, kill_first=True)
                    child_reaped = True
                    raise TimeoutError("isolated certification child exceeded bounded runtime")

                returncode = _reap_child(process)
                child_reaped = True

            for surface, filename in _SURFACE_FILES.items():
                if surface not in published and _publish_file(surface, output / filename, release):
                    published.add(surface)
            if returncode != 0:
                tail = _tail_text(stderr_path)
                raise RuntimeError(f"isolated certification child failed:{returncode}:{tail}")
            if published != set(_SURFACE_FILES):
                raise RuntimeError(f"isolated certification child incomplete:{sorted(published)}")
            success = True
    except BaseException as exc:
        error_type = type(exc).__name__
        diagnostic = {
            "event": "isolated_certifier_cycle_failed",
            "error_type": error_type,
            "error_message": _safe_error_message(exc),
            "published_surfaces": sorted(published),
            "release_commit": _release_commit(),
            "incremental_replica": True,
            "authoritative_full_snapshot_per_cycle": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
        print("SOLANA_ROI_CERTIFIER_CYCLE_FAILED " + json.dumps(diagnostic, sort_keys=True, separators=(",", ":")), flush=True)
    finally:
        if process is not None and not child_reaped:
            try:
                _reap_child(process, terminate_first=True)
                child_reaped = True
            except BaseException as reap_exc:
                with _LOCK:
                    _STATE["child_reap_failures"] = int(_STATE.get("child_reap_failures", 0) or 0) + 1
                if error_type is None:
                    error_type = type(reap_exc).__name__
                success = False
        with _LOCK:
            _STATE["active_cycle"] = False
            _STATE["last_completed_at_monotonic"] = time.monotonic()
            _STATE["last_published_surfaces"] = sorted(published)
            if success:
                _STATE["last_error_type"] = None
                _STATE["successes"] = int(_STATE.get("successes", 0) or 0) + 1
                _STATE["consecutive_failures"] = 0
            else:
                _STATE["last_error_type"] = error_type
                _STATE["failures"] = int(_STATE.get("failures", 0) or 0) + 1
                _STATE["consecutive_failures"] = int(_STATE.get("consecutive_failures", 0) or 0) + 1
    return success


def _run_cycle_sync(stop_requested: threading.Event) -> bool:
    if not _CYCLE_SINGLE_FLIGHT.acquire(blocking=False):
        with _LOCK:
            _STATE["cycle_overlap_rejections"] = int(_STATE.get("cycle_overlap_rejections", 0) or 0) + 1
        print(
            "SOLANA_ROI_CERTIFIER_CYCLE_OVERLAP_REJECTED "
            + json.dumps(
                {
                    "event": "isolated_certifier_cycle_overlap_rejected",
                    "release_commit": _release_commit(),
                    "paper_only": True,
                    "live_money_authority": False,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            flush=True,
        )
        return False
    try:
        return _run_cycle_single_flight(stop_requested)
    finally:
        _CYCLE_SINGLE_FLIGHT.release()


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
        success = bool(task.result())
        with _LOCK:
            failures = int(_STATE.get("consecutive_failures", 0) or 0)
        delay = _interval_seconds() if success else _retry_delay_seconds(failures)
        with _LOCK:
            _STATE["current_retry_delay_seconds"] = delay
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
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
            surface: max(0.0, time.monotonic() - published) if isinstance(published, (int, float)) else None
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
        "incremental_replica": replica_status(),
        "snapshot_transfer_integrity": {
            "resumable_bounded_chunks": True,
            "full_snapshot_normal_cycle": False,
            "full_snapshot_role": "bootstrap_recovery_reconciliation_only",
            "normal_cycle_transport": "bounded_incremental_delta",
            "replica_delta_applied_transactionally": True,
            "replica_identity_release_epoch_schema_and_watermark_bound": True,
            "child_receives_disposable_local_clone": True,
            "authoritative_full_snapshot_per_cycle": False,
            "sqlite_header_and_page_geometry_checked": True,
            "child_output_uses_bounded_file_tail_not_pipes": True,
            "child_native_thread_limit": 1,
            "cycle_single_flight": True,
            "child_reap_required_before_pid_clear": True,
        },
        "child_process_isolation": True,
        "shared_writable_disk": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


@app.get("/v1/strategy/e2e-status")
def e2e_status(x_certification_token: str | None = Header(default=None, alias="X-Certification-Token")) -> dict[str, Any]:
    _require_token(x_certification_token)
    return _cached("e2e")


@app.get("/v1/strategy/forward-certification")
def forward_certification(x_certification_token: str | None = Header(default=None, alias="X-Certification-Token")) -> dict[str, Any]:
    _require_token(x_certification_token)
    return _cached("forward")


@app.get("/v1/strategy/production-proof")
def production_proof(x_certification_token: str | None = Header(default=None, alias="X-Certification-Token")) -> dict[str, Any]:
    _require_token(x_certification_token)
    return _cached("production")


@app.get("/v1/operations/isolated-certifier")
def isolated_certifier_status(x_certification_token: str | None = Header(default=None, alias="X-Certification-Token")) -> dict[str, Any]:
    _require_token(x_certification_token)
    return health()


__all__ = ["SERVICE_VERSION", "_download_snapshot", "_validate_sqlite_snapshot", "app", "health"]
