from __future__ import annotations

import hmac
import json
import os
import secrets
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable

from fastapi import Header, HTTPException, Query
from starlette.responses import Response

from . import certification_service_split as split


TRANSFER_VERSION = "certification-snapshot-chunk-transfer-v1"
DEFAULT_CHUNK_BYTES = 16 * 1024 * 1024
MAX_CHUNK_BYTES = 32 * 1024 * 1024
DEFAULT_TRANSFER_TTL_SECONDS = 15 * 60.0

_ACTIVE_LOCK = threading.Lock()
_ACTIVE: dict[str, dict[str, Any]] = {}


def _chunk_bytes() -> int:
    try:
        return min(
            MAX_CHUNK_BYTES,
            max(
                1024 * 1024,
                int(os.getenv("SOLANA_ROI_CERTIFICATION_TRANSFER_CHUNK_BYTES", str(DEFAULT_CHUNK_BYTES))),
            ),
        )
    except ValueError:
        return DEFAULT_CHUNK_BYTES


def _transfer_ttl_seconds() -> float:
    try:
        return max(
            60.0,
            float(
                os.getenv(
                    "SOLANA_ROI_CERTIFICATION_TRANSFER_TTL_SECONDS",
                    str(DEFAULT_TRANSFER_TTL_SECONDS),
                )
            ),
        )
    except ValueError:
        return DEFAULT_TRANSFER_TTL_SECONDS


def _require_shared_token(value: str | None) -> None:
    expected = split._shared_token()
    if not expected:
        raise HTTPException(status_code=503, detail="certification snapshot authentication is not configured")
    if not hmac.compare_digest(value or "", expected):
        raise HTTPException(status_code=401, detail="invalid certification snapshot authorization")


def _remove_active(snapshot_id: str) -> Path | None:
    with _ACTIVE_LOCK:
        record = _ACTIVE.pop(snapshot_id, None)
    if not isinstance(record, dict):
        return None
    path = record.get("path")
    return Path(path) if path else None


def _dispose_active(snapshot_id: str) -> bool:
    path = _remove_active(snapshot_id)
    if path is None:
        return False
    split._dispose_snapshot(path)
    return True


def _cleanup_expired() -> int:
    now = time.monotonic()
    expired: list[str] = []
    with _ACTIVE_LOCK:
        for snapshot_id, record in _ACTIVE.items():
            created = float(record.get("created_monotonic") or 0.0)
            touched = float(record.get("last_access_monotonic") or created)
            if now - max(created, touched) >= _transfer_ttl_seconds():
                expired.append(snapshot_id)
    for snapshot_id in expired:
        _dispose_active(snapshot_id)
    return len(expired)


def _manifest_record(snapshot_id: str) -> dict[str, Any] | None:
    with _ACTIVE_LOCK:
        record = _ACTIVE.get(snapshot_id)
        if not isinstance(record, dict):
            return None
        record["last_access_monotonic"] = time.monotonic()
        return dict(record)


def install_authoritative_snapshot_chunk_transfer(app: Any, runtime_provider: Callable[[], Any]) -> None:
    """Install a resumable, authenticated snapshot transport beside the legacy endpoint.

    The immutable SQLite export remains owned by the authoritative runtime's persistent
    disk. The certifier receives it through bounded chunks and explicitly releases the
    export after exact byte and SQLite validation. No writable disk is shared between
    services and no strategy, certification, signing, or live-money authority changes.
    """

    manifest_path = "/v1/operations/certification-db-snapshot-manifest"
    chunk_path = "/v1/operations/certification-db-snapshot-chunk/{snapshot_id}"
    release_path = "/v1/operations/certification-db-snapshot-chunk/{snapshot_id}"
    existing = {getattr(route, "path", None) for route in app.routes}

    if manifest_path not in existing:

        @app.get(manifest_path)
        def certification_db_snapshot_manifest(
            x_certification_token: str | None = Header(default=None, alias="X-Certification-Token"),
        ) -> dict[str, Any]:
            _require_shared_token(x_certification_token)
            _cleanup_expired()

            if not split._SNAPSHOT_EXPORT_LOCK.acquire(blocking=False):
                with split._STATE_LOCK:
                    split._SNAPSHOT_STATE["busy_rejections"] = int(
                        split._SNAPSHOT_STATE.get("busy_rejections", 0) or 0
                    ) + 1
                raise HTTPException(status_code=503, detail="certification snapshot export already in progress")

            started = time.monotonic()
            snapshot: Path | None = None
            source_name: str | None = None
            try:
                with split._STATE_LOCK:
                    split._SNAPSHOT_STATE["attempts"] = int(split._SNAPSHOT_STATE.get("attempts", 0) or 0) + 1
                    split._SNAPSHOT_STATE["last_started_monotonic"] = started
                    split._SNAPSHOT_STATE["last_error_type"] = None

                runtime = runtime_provider()
                store = getattr(runtime, "store", None)
                if store is None:
                    raise HTTPException(status_code=503, detail="canonical runtime store unavailable")
                source_path = Path(getattr(store, "path", ""))
                source_name = source_path.name or None
                directory = split._snapshot_directory(store)
                split._cleanup_stale_exports(directory)
                fd, raw_path = tempfile.mkstemp(
                    prefix=".certification-export-",
                    suffix=".sqlite3",
                    dir=str(directory),
                )
                os.close(fd)
                snapshot = Path(raw_path)

                split._LOGGER.info(
                    "SOLANA_ROI_CERTIFICATION_CHUNK_SNAPSHOT_START release=%s source=%s",
                    split._release_commit(),
                    source_name or "unknown",
                )
                size, estimated = split._snapshot_store_to_file(store, snapshot)
                split._record_snapshot_result(
                    success=True,
                    started=started,
                    size=size,
                    estimated=estimated,
                    source_name=source_name,
                )
                snapshot_id = secrets.token_urlsafe(24)
                record = {
                    "path": str(snapshot),
                    "release_commit": split._release_commit(),
                    "size_bytes": int(size),
                    "created_monotonic": time.monotonic(),
                    "last_access_monotonic": time.monotonic(),
                }
                with _ACTIVE_LOCK:
                    _ACTIVE[snapshot_id] = record
                split._LOGGER.info(
                    "SOLANA_ROI_CERTIFICATION_CHUNK_SNAPSHOT_READY release=%s snapshot_id=%s bytes=%s duration_seconds=%.3f",
                    split._release_commit(),
                    snapshot_id,
                    size,
                    max(0.0, time.monotonic() - started),
                )
                return {
                    "transfer_version": TRANSFER_VERSION,
                    "snapshot_id": snapshot_id,
                    "release_commit": split._release_commit(),
                    "size_bytes": int(size),
                    "chunk_bytes": _chunk_bytes(),
                    "expires_after_seconds": _transfer_ttl_seconds(),
                    "paper_only": True,
                    "live_money_authority": False,
                    "signing_available": False,
                    "transaction_submission_available": False,
                }
            except HTTPException:
                if snapshot is not None:
                    split._dispose_snapshot(snapshot)
                split._record_snapshot_result(
                    success=False,
                    started=started,
                    error_type="HTTPException",
                    source_name=source_name,
                )
                raise
            except Exception as exc:
                if snapshot is not None:
                    split._dispose_snapshot(snapshot)
                split._record_snapshot_result(
                    success=False,
                    started=started,
                    error_type=type(exc).__name__,
                    source_name=source_name,
                )
                split._LOGGER.warning(
                    "SOLANA_ROI_CERTIFICATION_CHUNK_SNAPSHOT_FAILED release=%s error_type=%s duration_seconds=%.3f",
                    split._release_commit(),
                    type(exc).__name__,
                    max(0.0, time.monotonic() - started),
                )
                raise HTTPException(
                    status_code=503,
                    detail=f"canonical certification snapshot failed closed:{type(exc).__name__}",
                ) from exc
            finally:
                split._SNAPSHOT_EXPORT_LOCK.release()

    if chunk_path not in existing:

        @app.get(chunk_path)
        def certification_db_snapshot_chunk(
            snapshot_id: str,
            offset: int = Query(default=0, ge=0),
            length: int = Query(default=DEFAULT_CHUNK_BYTES, ge=1, le=MAX_CHUNK_BYTES),
            x_certification_token: str | None = Header(default=None, alias="X-Certification-Token"),
        ) -> Response:
            _require_shared_token(x_certification_token)
            _cleanup_expired()
            record = _manifest_record(snapshot_id)
            if record is None:
                raise HTTPException(status_code=404, detail="certification snapshot transfer not found")

            release = str(record.get("release_commit") or "")
            if release != split._release_commit():
                _dispose_active(snapshot_id)
                raise HTTPException(status_code=409, detail="certification snapshot release changed")
            path = Path(str(record.get("path") or ""))
            total = int(record.get("size_bytes") or 0)
            if not path.is_file() or total <= 0:
                _dispose_active(snapshot_id)
                raise HTTPException(status_code=410, detail="certification snapshot transfer expired")
            if offset >= total:
                raise HTTPException(status_code=416, detail="certification snapshot offset outside file")

            bounded = min(int(length), _chunk_bytes(), total - offset)
            try:
                with path.open("rb") as handle:
                    handle.seek(offset)
                    payload = handle.read(bounded)
            except OSError as exc:
                raise HTTPException(status_code=503, detail="certification snapshot chunk read failed") from exc
            finally:
                split._drop_file_cache(path)
            if len(payload) != bounded:
                raise HTTPException(status_code=503, detail="certification snapshot chunk truncated")

            return Response(
                content=payload,
                media_type="application/octet-stream",
                headers={
                    "X-Release-Commit": release,
                    "X-Certification-Snapshot-Id": snapshot_id,
                    "X-Certification-Snapshot-Offset": str(offset),
                    "X-Certification-Snapshot-Total-Bytes": str(total),
                    "X-Certification-Snapshot-Chunk-Bytes": str(len(payload)),
                    "X-Certification-Transfer-Version": TRANSFER_VERSION,
                },
            )

    # GET and DELETE share the same path; checking method-specific duplicates is
    # unnecessary because this installer is invoked once per production composition.
    if not any(
        getattr(route, "path", None) == release_path and "DELETE" in getattr(route, "methods", set())
        for route in app.routes
    ):

        @app.delete(release_path)
        def certification_db_snapshot_release(
            snapshot_id: str,
            x_certification_token: str | None = Header(default=None, alias="X-Certification-Token"),
        ) -> dict[str, Any]:
            _require_shared_token(x_certification_token)
            released = _dispose_active(snapshot_id)
            return {
                "transfer_version": TRANSFER_VERSION,
                "snapshot_id": snapshot_id,
                "released": released,
                "paper_only": True,
                "live_money_authority": False,
            }


def _open_json(request: urllib.request.Request, *, timeout: float) -> dict[str, Any]:
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("runtime snapshot manifest invalid")
    return payload


def _release_remote_snapshot(base: str, token: str, snapshot_id: str) -> None:
    encoded = urllib.parse.quote(snapshot_id, safe="")
    request = urllib.request.Request(
        f"{base}/v1/operations/certification-db-snapshot-chunk/{encoded}",
        method="DELETE",
        headers={
            "Accept": "application/json",
            "X-Certification-Token": token,
            "User-Agent": "solana-roi-isolated-certifier/2",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=15.0) as response:
            response.read()
    except (urllib.error.URLError, TimeoutError, OSError):
        # Server-side TTL cleanup is the correctness-independent fallback.
        return


def download_snapshot_chunked(
    destination: Path,
    *,
    base: str,
    token: str,
    expected_release: str,
) -> tuple[str, int]:
    """Download one immutable runtime snapshot through exact, resumable-sized chunks."""
    if not base or not token:
        raise RuntimeError("runtime snapshot source is not configured")

    manifest_request = urllib.request.Request(
        f"{base}/v1/operations/certification-db-snapshot-manifest",
        headers={
            "Accept": "application/json",
            "X-Certification-Token": token,
            "User-Agent": "solana-roi-isolated-certifier/2",
        },
    )
    try:
        manifest = _open_json(manifest_request, timeout=90.0)
    except RuntimeError:
        raise
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"runtime snapshot manifest failed:{type(exc).__name__}") from exc

    snapshot_id = str(manifest.get("snapshot_id") or "").strip()
    release = str(manifest.get("release_commit") or "").strip()
    try:
        expected_bytes = int(manifest.get("size_bytes") or 0)
        advertised_chunk = int(manifest.get("chunk_bytes") or DEFAULT_CHUNK_BYTES)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("runtime snapshot manifest numeric binding invalid") from exc
    if not snapshot_id:
        raise RuntimeError("runtime snapshot manifest missing snapshot id")
    if not release:
        raise RuntimeError("runtime snapshot manifest missing release binding")
    if release != expected_release:
        _release_remote_snapshot(base, token, snapshot_id)
        raise RuntimeError(f"runtime snapshot release mismatch:{release}:{expected_release}")
    if expected_bytes <= 0:
        _release_remote_snapshot(base, token, snapshot_id)
        raise RuntimeError("runtime snapshot advertised invalid byte count")

    chunk_bytes = min(MAX_CHUNK_BYTES, max(1024 * 1024, advertised_chunk))
    encoded = urllib.parse.quote(snapshot_id, safe="")
    offset = 0
    try:
        with destination.open("wb") as handle:
            while offset < expected_bytes:
                requested = min(chunk_bytes, expected_bytes - offset)
                query = urllib.parse.urlencode({"offset": offset, "length": requested})
                request = urllib.request.Request(
                    f"{base}/v1/operations/certification-db-snapshot-chunk/{encoded}?{query}",
                    headers={
                        "Accept": "application/octet-stream",
                        "X-Certification-Token": token,
                        "User-Agent": "solana-roi-isolated-certifier/2",
                    },
                )
                try:
                    with urllib.request.urlopen(request, timeout=45.0) as response:
                        observed_release = str(response.headers.get("X-Release-Commit") or "")
                        observed_id = str(response.headers.get("X-Certification-Snapshot-Id") or "")
                        observed_offset = int(response.headers.get("X-Certification-Snapshot-Offset") or -1)
                        observed_total = int(response.headers.get("X-Certification-Snapshot-Total-Bytes") or 0)
                        raw = response.read(requested + 1)
                except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
                    raise RuntimeError(
                        f"runtime snapshot chunk failed:{offset}:{type(exc).__name__}"
                    ) from exc

                if observed_release != expected_release:
                    raise RuntimeError(
                        f"runtime snapshot chunk release mismatch:{observed_release}:{expected_release}"
                    )
                if observed_id != snapshot_id:
                    raise RuntimeError("runtime snapshot chunk id mismatch")
                if observed_offset != offset:
                    raise RuntimeError(
                        f"runtime snapshot chunk offset mismatch:{observed_offset}:{offset}"
                    )
                if observed_total != expected_bytes:
                    raise RuntimeError(
                        f"runtime snapshot chunk total mismatch:{observed_total}:{expected_bytes}"
                    )
                if len(raw) != requested:
                    raise RuntimeError(
                        f"runtime snapshot chunk length mismatch:{offset}:{len(raw)}:{requested}"
                    )
                handle.write(raw)
                offset += len(raw)
    finally:
        _release_remote_snapshot(base, token, snapshot_id)

    actual = int(destination.stat().st_size)
    if actual != expected_bytes:
        raise RuntimeError(f"runtime snapshot byte-count mismatch:{actual}:{expected_bytes}")
    return release, expected_bytes


__all__ = [
    "DEFAULT_CHUNK_BYTES",
    "MAX_CHUNK_BYTES",
    "TRANSFER_VERSION",
    "download_snapshot_chunked",
    "install_authoritative_snapshot_chunk_transfer",
]
