from __future__ import annotations

"""Temporarily suppress SQLite writer auto-checkpoints during logical bootstrap.

The authoritative logical bootstrap is a bounded read-only scan, but SQLite's default
writer-side ``wal_autocheckpoint`` threshold is about 1000 pages. On the production
4 KiB database that repeatedly copied a ~4 MiB WAL back into the 1.7 GiB main file
while the bootstrap reader was trying to evict clean cache. This lease changes only
physical checkpoint cadence while bootstrap requests are active. It restores the
writer's exact original setting on completion or inactivity and pauses fail-closed if
the WAL reaches a bounded maintenance ceiling.

The lease is installed only on the already-registered authoritative FastAPI bootstrap
routes for the concrete production app. The underlying logical-bootstrap module
functions remain unchanged so package imports, direct unit tests, and non-production
stores never inherit production-only writer requirements.
"""

import os
import sqlite3
import threading
import weakref
from pathlib import Path
from typing import Any, Callable

from fastapi import HTTPException

LEASE_VERSION = "certification-bootstrap-autocheckpoint-lease-v2-production-routes"
DEFAULT_IDLE_SECONDS = 45.0
MIN_IDLE_SECONDS = 35.0
MAX_IDLE_SECONDS = 120.0
DEFAULT_MAX_WAL_BYTES = 64 * 1024 * 1024
MIN_MAX_WAL_BYTES = 32 * 1024 * 1024
MAX_MAX_WAL_BYTES = 256 * 1024 * 1024
STATE_ATTR = "_roi_certification_bootstrap_autocheckpoint_lease"
MANIFEST_PATH = "/v1/operations/certification-db-logical-bootstrap"
PAGE_PATH = "/v1/operations/certification-db-logical-bootstrap-page"

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
STRATEGY_THRESHOLDS_CHANGED = False
CERTIFICATION_THRESHOLDS_CHANGED = False
CANONICAL_EVIDENCE_RESET = False

_INSTALLED = False


def _idle_seconds() -> float:
    try:
        return max(
            MIN_IDLE_SECONDS,
            min(
                MAX_IDLE_SECONDS,
                float(
                    os.getenv(
                        "SOLANA_ROI_CERTIFICATION_BOOTSTRAP_AUTOCHECKPOINT_LEASE_SECONDS",
                        str(DEFAULT_IDLE_SECONDS),
                    )
                ),
            ),
        )
    except ValueError:
        return DEFAULT_IDLE_SECONDS


def _max_wal_bytes() -> int:
    try:
        return max(
            MIN_MAX_WAL_BYTES,
            min(
                MAX_MAX_WAL_BYTES,
                int(
                    os.getenv(
                        "SOLANA_ROI_CERTIFICATION_BOOTSTRAP_MAX_WAL_BYTES",
                        str(DEFAULT_MAX_WAL_BYTES),
                    )
                ),
            ),
        )
    except ValueError:
        return DEFAULT_MAX_WAL_BYTES


def _wal_path(store: Any) -> Path:
    return Path(str(Path(getattr(store, "path", ""))) + "-wal")


def _wal_size_bytes(store: Any) -> int:
    try:
        return int(_wal_path(store).stat().st_size)
    except OSError:
        return 0


def _state(store: Any) -> dict[str, Any] | None:
    value = getattr(store, STATE_ATTR, None)
    return value if isinstance(value, dict) else None


def _read_autocheckpoint_locked(store: Any) -> int:
    row = store.db.execute("PRAGMA wal_autocheckpoint").fetchone()
    if row is None:
        raise RuntimeError("authoritative SQLite wal_autocheckpoint unavailable")
    return max(0, int(row[0]))


def _set_autocheckpoint_locked(store: Any, pages: int) -> None:
    store.db.execute(f"PRAGMA wal_autocheckpoint={max(0, int(pages))}")


def _cancel_timer(state: dict[str, Any]) -> None:
    timer = state.get("timer")
    if timer is not None:
        try:
            timer.cancel()
        except Exception:
            pass
    state["timer"] = None


def _restore_locked(store: Any, state: dict[str, Any], *, reason: str, cancel_timer: bool = True) -> bool:
    if cancel_timer:
        _cancel_timer(state)
    if not bool(state.get("active")):
        return False
    original = max(0, int(state.get("original_pages") or 0))
    try:
        _set_autocheckpoint_locked(store, original)
    except (sqlite3.Error, AttributeError, RuntimeError):
        state["active"] = False
        state["restore_reason"] = f"{reason}:store_unavailable"
        return False
    state["active"] = False
    state["restore_reason"] = reason
    state["generation"] = int(state.get("generation") or 0) + 1
    print(
        "ROI_BOOTSTRAP_AUTOCHECKPOINT_LEASE "
        f"event=restore reason={reason} original_pages={original} "
        f"wal_bytes={_wal_size_bytes(store)}",
        flush=True,
    )
    return True


def _expire(store_ref: "weakref.ReferenceType[Any]", generation: int) -> None:
    store = store_ref()
    if store is None:
        return
    lock = getattr(store, "_lock", None)
    if lock is None:
        return
    try:
        with lock:
            state = _state(store)
            if state is None or not bool(state.get("active")):
                return
            if int(state.get("generation") or -1) != int(generation):
                return
            _restore_locked(store, state, reason="idle_timeout", cancel_timer=False)
    except Exception:
        return


def _sync_and_release(path: Path) -> None:
    sync = getattr(os, "fdatasync", None) or getattr(os, "fsync", None)
    fadvise = getattr(os, "posix_fadvise", None)
    advice = getattr(os, "POSIX_FADV_DONTNEED", None)
    for candidate in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm")):
        try:
            fd = os.open(candidate, os.O_RDONLY)
        except OSError:
            continue
        try:
            if sync is not None:
                try:
                    sync(fd)
                except OSError:
                    pass
            if fadvise is not None and advice is not None:
                try:
                    fadvise(fd, 0, 0, advice)
                except OSError:
                    pass
        finally:
            os.close(fd)


def _maintenance_checkpoint_locked(store: Any) -> tuple[int | None, int | None, int | None, str | None]:
    try:
        row = store.db.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        if row is None or len(row) < 3:
            return None, None, None, None
        return int(row[0]), int(row[1]), int(row[2]), None
    except (sqlite3.Error, AttributeError) as exc:
        return None, None, None, f"{type(exc).__name__}:{exc}"


def refresh(store: Any) -> dict[str, Any]:
    """Acquire/refresh the bounded lease before any bootstrap memory guard runs."""

    lock = getattr(store, "_lock", None)
    path = Path(getattr(store, "path", ""))
    if lock is None or not path.is_file() or not hasattr(store, "db"):
        raise HTTPException(status_code=503, detail="certification bootstrap checkpoint lease unavailable")

    maintenance: tuple[int | None, int | None, int | None, str | None] | None = None
    maintenance_wal = 0
    with lock:
        state = _state(store)
        if state is None:
            state = {
                "active": False,
                "generation": 0,
                "original_pages": None,
                "timer": None,
                "table_names": (),
                "restore_reason": None,
            }
            setattr(store, STATE_ATTR, state)

        maintenance_wal = _wal_size_bytes(store)
        if maintenance_wal >= _max_wal_bytes():
            if bool(state.get("active")):
                _restore_locked(store, state, reason="wal_bound", cancel_timer=True)
            maintenance = _maintenance_checkpoint_locked(store)
        else:
            if not bool(state.get("active")):
                original = _read_autocheckpoint_locked(store)
                _set_autocheckpoint_locked(store, 0)
                state["original_pages"] = original
                state["active"] = True
                state["restore_reason"] = None
                print(
                    "ROI_BOOTSTRAP_AUTOCHECKPOINT_LEASE "
                    f"event=acquire original_pages={original} idle_seconds={_idle_seconds():.3f} "
                    f"wal_bound_bytes={_max_wal_bytes()} wal_bytes={maintenance_wal}",
                    flush=True,
                )

            _cancel_timer(state)
            state["generation"] = int(state.get("generation") or 0) + 1
            generation = int(state["generation"])
            timer = threading.Timer(_idle_seconds(), _expire, args=(weakref.ref(store), generation))
            timer.daemon = True
            state["timer"] = timer
            timer.start()
            return dict(state)

    assert maintenance is not None
    _sync_and_release(path)
    busy, log_frames, checkpointed_frames, error = maintenance
    print(
        "ROI_BOOTSTRAP_AUTOCHECKPOINT_LEASE "
        "event=wal_maintenance_pause "
        f"wal_bytes={maintenance_wal} wal_bound_bytes={_max_wal_bytes()} "
        f"busy={busy if busy is not None else 'unknown'} "
        f"log_frames={log_frames if log_frames is not None else 'unknown'} "
        f"checkpointed_frames={checkpointed_frames if checkpointed_frames is not None else 'unknown'} "
        f"error={(error or 'none').replace(chr(10), ' ')[:160]}",
        flush=True,
    )
    raise HTTPException(
        status_code=503,
        detail="certification logical bootstrap paused: bounded WAL checkpoint maintenance",
    )


def set_manifest_tables(store: Any, manifest: dict[str, Any]) -> None:
    tables = manifest.get("tables")
    names = tuple(
        str(item.get("name") or "")
        for item in tables
        if isinstance(item, dict) and str(item.get("name") or "")
    ) if isinstance(tables, list) else ()
    lock = getattr(store, "_lock", None)
    if lock is None:
        return
    with lock:
        state = _state(store)
        if state is not None:
            state["table_names"] = names


def finish_if_complete(store: Any, payload: dict[str, Any]) -> bool:
    if not bool(payload.get("done")):
        return False
    table = str(payload.get("table") or "")
    lock = getattr(store, "_lock", None)
    if lock is None:
        return False
    with lock:
        state = _state(store)
        if state is None:
            return False
        names = tuple(state.get("table_names") or ())
        if not names or table != str(names[-1]):
            return False
        return _restore_locked(store, state, reason="bootstrap_complete", cancel_timer=True)


def finish(store: Any, *, reason: str = "explicit") -> bool:
    lock = getattr(store, "_lock", None)
    if lock is None:
        return False
    with lock:
        state = _state(store)
        if state is None:
            return False
        return _restore_locked(store, state, reason=reason, cancel_timer=True)


def _runtime_store(runtime_provider: Callable[[], Any]) -> Any:
    try:
        runtime = runtime_provider()
    except Exception as exc:
        raise HTTPException(status_code=503, detail="canonical certification runtime unavailable") from exc
    store = getattr(runtime, "store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="canonical certification store unavailable")
    return store


def _find_route(app: Any, path: str) -> Any | None:
    return next((candidate for candidate in app.routes if getattr(candidate, "path", None) == path), None)


def _route(app: Any, path: str) -> Any:
    route = _find_route(app, path)
    if route is None:
        raise RuntimeError(f"certification bootstrap route not found: {path}")
    dependant = getattr(route, "dependant", None)
    if dependant is None or not callable(getattr(dependant, "call", None)):
        raise RuntimeError(f"certification bootstrap route callable unavailable: {path}")
    return route


def _replace_route_call(route: Any, endpoint: Callable[..., dict[str, Any]]) -> None:
    route.endpoint = endpoint
    route.dependant.call = endpoint


def _runtime_provider_from_endpoint(endpoint: Any) -> Callable[[], Any] | None:
    closure = getattr(endpoint, "__closure__", None)
    freevars = getattr(getattr(endpoint, "__code__", None), "co_freevars", ())
    if not closure or not freevars:
        return None
    for name, cell in zip(freevars, closure):
        if name != "runtime_provider":
            continue
        try:
            value = cell.cell_contents
        except ValueError:
            continue
        if callable(value):
            return value
    return None


def install_certification_bootstrap_autocheckpoint_lease(
    app: Any,
    runtime_provider: Callable[[], Any] | None = None,
) -> None:
    """Wrap only this production app's registered bootstrap routes.

    When certification split runtime is disabled the bootstrap transport is not
    registered at all; that is an intentional inactive state, so the lease is a
    no-op. If split runtime is enabled, both routes are mandatory and absence of
    either one remains a hard composition failure. FastAPI has already compiled
    parameter/header dependencies for the original endpoints, so replacing
    ``dependant.call`` preserves the exact HTTP contract while avoiding module-global
    mutation of logical ``_manifest``/``_page``.
    """

    global _INSTALLED
    marker = "roi_certification_bootstrap_autocheckpoint_lease"
    if bool(getattr(app.state, marker, False)) and bool(
        getattr(app.state, "roi_certification_bootstrap_autocheckpoint_lease_active", False)
    ):
        return

    from . import certification_incremental_replication as replication
    from . import certification_service_split as split

    manifest_candidate = _find_route(app, MANIFEST_PATH)
    page_candidate = _find_route(app, PAGE_PATH)
    if manifest_candidate is None and page_candidate is None and not split.split_runtime_enabled():
        app.state.roi_certification_bootstrap_autocheckpoint_lease = False
        app.state.roi_certification_bootstrap_autocheckpoint_lease_active = False
        app.state.roi_certification_bootstrap_autocheckpoint_lease_version = LEASE_VERSION
        return
    if manifest_candidate is None or page_candidate is None:
        missing = MANIFEST_PATH if manifest_candidate is None else PAGE_PATH
        raise RuntimeError(f"certification bootstrap route not found: {missing}")

    manifest_route = _route(app, MANIFEST_PATH)
    page_route = _route(app, PAGE_PATH)
    original_manifest = manifest_route.dependant.call
    original_page = page_route.dependant.call

    provider = runtime_provider
    if provider is None:
        provider = _runtime_provider_from_endpoint(original_manifest)
    if provider is None:
        provider = _runtime_provider_from_endpoint(original_page)
    if provider is None:
        raise RuntimeError("certification bootstrap runtime provider unavailable")

    def manifest_endpoint(x_certification_token: str | None = None) -> dict[str, Any]:
        replication._require_shared_token(x_certification_token)
        store = _runtime_store(provider)
        refresh(store)
        payload = original_manifest(x_certification_token=x_certification_token)
        set_manifest_tables(store, payload)
        return payload

    setattr(manifest_endpoint, "_roi_bootstrap_autocheckpoint_lease", True)
    setattr(manifest_endpoint, "_roi_original_endpoint", original_manifest)

    def page_endpoint(
        table: str,
        epoch: str,
        schema_fingerprint: str,
        cursor: str | None = None,
        limit: int = 250,
        x_certification_token: str | None = None,
    ) -> dict[str, Any]:
        replication._require_shared_token(x_certification_token)
        store = _runtime_store(provider)
        refresh(store)
        payload = original_page(
            table=table,
            epoch=epoch,
            schema_fingerprint=schema_fingerprint,
            cursor=cursor,
            limit=limit,
            x_certification_token=x_certification_token,
        )
        finish_if_complete(store, payload)
        return payload

    setattr(page_endpoint, "_roi_bootstrap_autocheckpoint_lease", True)
    setattr(page_endpoint, "_roi_original_endpoint", original_page)

    _replace_route_call(manifest_route, manifest_endpoint)
    _replace_route_call(page_route, page_endpoint)
    _INSTALLED = True
    app.state.roi_certification_bootstrap_autocheckpoint_lease = True
    app.state.roi_certification_bootstrap_autocheckpoint_lease_active = True
    app.state.roi_certification_bootstrap_autocheckpoint_lease_version = LEASE_VERSION


def status() -> dict[str, Any]:
    return {
        "lease_version": LEASE_VERSION,
        "installed": _INSTALLED,
        "scope": "authoritative_registered_bootstrap_routes_only",
        "module_global_logical_functions_mutated": False,
        "idle_seconds": _idle_seconds(),
        "max_wal_bytes": _max_wal_bytes(),
        "original_autocheckpoint_restored": True,
        "wal_bound_fail_closed": True,
        "strategy_thresholds_changed": STRATEGY_THRESHOLDS_CHANGED,
        "certification_thresholds_changed": CERTIFICATION_THRESHOLDS_CHANGED,
        "canonical_evidence_reset": CANONICAL_EVIDENCE_RESET,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = [
    "LEASE_VERSION",
    "finish",
    "finish_if_complete",
    "install_certification_bootstrap_autocheckpoint_lease",
    "refresh",
    "set_manifest_tables",
    "status",
]
