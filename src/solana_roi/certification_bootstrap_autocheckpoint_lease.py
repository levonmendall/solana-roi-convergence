from __future__ import annotations

"""Bound SQLite checkpoint/cache pressure around certification bootstrap.

The authoritative logical bootstrap is a bounded read-only scan, while the canonical
writer uses SQLite WAL. Production proved two distinct failure modes:

* the default ``wal_autocheckpoint`` cadence repeatedly copied a small WAL back into
  the large main database while bootstrap was trying to shed file cache; and
* after that cadence was suppressed, an already-large physical WAL could cross the
  bounded maintenance ceiling before the certifier's first request.

This module changes only physical checkpoint cadence. It primes a temporary lease after
the canonical runtime is restored but before live workers start, refreshes that lease on
bootstrap requests, restores the exact original setting on completion/inactivity, and
uses a zero-wait TRUNCATE checkpoint only when the configured WAL ceiling is reached.
A busy, failed, or non-shrinking maintenance attempt remains fail-closed with HTTP 503.
No strategy, certification, evidence, signing, submission, or live-money authority is
changed.
"""

import asyncio
import inspect
import os
import sqlite3
import threading
import weakref
from functools import wraps
from pathlib import Path
from typing import Any, Callable

from fastapi import BackgroundTasks, HTTPException
from starlette.concurrency import run_in_threadpool

LEASE_VERSION = "certification-bootstrap-autocheckpoint-lease-v7-route-lifecycle-offload"
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


def _ensure_state_locked(store: Any) -> dict[str, Any]:
    state = _state(store)
    if state is None:
        state = {
            "active": False,
            "generation": 0,
            "original_pages": None,
            "timer": None,
            "table_names": (),
            "restore_reason": None,
            "last_maintenance_wal_before": None,
            "last_maintenance_wal_after": None,
        }
        setattr(store, STATE_ATTR, state)
    return state


def _cancel_timer(state: dict[str, Any]) -> None:
    timer = state.get("timer")
    if timer is not None:
        try:
            timer.cancel()
        except Exception:
            pass
    state["timer"] = None


def _restore_locked(
    store: Any,
    state: dict[str, Any],
    *,
    reason: str,
    cancel_timer: bool = True,
) -> bool:
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
    """Flush dirty SQLite files and advise away their cache without checkpointing."""

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


def _maintenance_checkpoint_locked(
    store: Any,
) -> tuple[int | None, int | None, int | None, str | None]:
    """Attempt one zero-wait TRUNCATE checkpoint at the explicit WAL ceiling.

    PASSIVE can copy every committed frame yet leave the physical WAL file allocated,
    which caused the same byte ceiling to re-trigger forever in production. TRUNCATE
    resets the file only when SQLite can obtain the required locks immediately. The
    connection's prior busy timeout is restored exactly afterwards. Any busy result or
    error is telemetry and remains fail-closed at the caller.
    """

    original_timeout: int | None = None
    try:
        timeout_row = store.db.execute("PRAGMA busy_timeout").fetchone()
        original_timeout = int(timeout_row[0]) if timeout_row is not None else 0
        store.db.execute("PRAGMA busy_timeout=0")
        row = store.db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if row is None or len(row) < 3:
            return None, None, None, "RuntimeError:wal checkpoint result unavailable"
        return int(row[0]), int(row[1]), int(row[2]), None
    except (sqlite3.Error, AttributeError, RuntimeError, TypeError, ValueError) as exc:
        return None, None, None, f"{type(exc).__name__}:{exc}"
    finally:
        if original_timeout is not None:
            try:
                store.db.execute(f"PRAGMA busy_timeout={max(0, original_timeout)}")
            except (sqlite3.Error, AttributeError):
                pass


def _schedule_lease_locked(store: Any, state: dict[str, Any], *, wal_bytes: int) -> dict[str, Any]:
    if not bool(state.get("active")):
        original = _read_autocheckpoint_locked(store)
        _set_autocheckpoint_locked(store, 0)
        state["original_pages"] = original
        state["active"] = True
        state["restore_reason"] = None
        print(
            "ROI_BOOTSTRAP_AUTOCHECKPOINT_LEASE "
            f"event=acquire original_pages={original} idle_seconds={_idle_seconds():.3f} "
            f"wal_bound_bytes={_max_wal_bytes()} wal_bytes={wal_bytes}",
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


def refresh(store: Any) -> dict[str, Any]:
    """Acquire/refresh the bounded lease before any bootstrap memory guard runs."""

    lock = getattr(store, "_lock", None)
    path = Path(getattr(store, "path", ""))
    if lock is None or not path.is_file() or not hasattr(store, "db"):
        raise HTTPException(status_code=503, detail="certification bootstrap checkpoint lease unavailable")

    maintenance_wal = 0
    maintenance: tuple[int | None, int | None, int | None, str | None] | None = None
    with lock:
        state = _ensure_state_locked(store)
        maintenance_wal = _wal_size_bytes(store)
        if maintenance_wal < _max_wal_bytes():
            return _schedule_lease_locked(store, state, wal_bytes=maintenance_wal)

        if bool(state.get("active")):
            _restore_locked(store, state, reason="wal_bound", cancel_timer=True)
        maintenance = _maintenance_checkpoint_locked(store)

    assert maintenance is not None
    _sync_and_release(path)
    busy, log_frames, checkpointed_frames, error = maintenance
    wal_after = _wal_size_bytes(store)
    recovered = bool(error is None and busy == 0 and wal_after < _max_wal_bytes())
    print(
        "ROI_BOOTSTRAP_AUTOCHECKPOINT_LEASE "
        f"event={'wal_maintenance_recovered' if recovered else 'wal_maintenance_pause'} "
        f"wal_bytes_before={maintenance_wal} wal_bytes_after={wal_after} "
        f"wal_bound_bytes={_max_wal_bytes()} "
        f"busy={busy if busy is not None else 'unknown'} "
        f"log_frames={log_frames if log_frames is not None else 'unknown'} "
        f"checkpointed_frames={checkpointed_frames if checkpointed_frames is not None else 'unknown'} "
        f"error={(error or 'none').replace(chr(10), ' ')[:160]}",
        flush=True,
    )

    if not recovered:
        raise HTTPException(
            status_code=503,
            detail="certification logical bootstrap paused: bounded WAL checkpoint maintenance",
        )

    with lock:
        state = _ensure_state_locked(store)
        current_wal = _wal_size_bytes(store)
        state["last_maintenance_wal_before"] = int(maintenance_wal)
        state["last_maintenance_wal_after"] = int(current_wal)
        if current_wal >= _max_wal_bytes():
            raise HTTPException(
                status_code=503,
                detail="certification logical bootstrap paused: WAL refilled during bounded maintenance",
            )
        return _schedule_lease_locked(store, state, wal_bytes=current_wal)


def prime_before_workers(store: Any) -> dict[str, Any]:
    """Prime the lease and shed startup cache before live runtime workers begin.

    Runtime construction can legitimately read a large historical working set. The
    certifier's first manifest may arrive later. This handoff acquires the same bounded
    lease used by the HTTP routes before workers can auto-checkpoint into the main DB.
    If the inherited WAL already exceeds the ceiling, ``refresh`` first performs one
    zero-wait ceiling-maintenance TRUNCATE and proceeds only after the physical WAL is
    proven below the unchanged bound.
    """

    state = refresh(store)
    path = Path(getattr(store, "path", ""))
    from . import durable_bootstrap_memory_repair as memory

    before = memory._cgroup_memory()
    _sync_and_release(path)
    memory._trim_process_heap()
    after = memory._cgroup_memory()
    print(
        "ROI_BOOTSTRAP_PREWORKER_QUIESCE "
        f"before={before.get('current_bytes', 'unknown')} "
        f"after={after.get('current_bytes', 'unknown')} "
        f"file_before={before.get('file_bytes', 'unknown')} "
        f"file_after={after.get('file_bytes', 'unknown')} "
        f"dirty_before={before.get('file_dirty_bytes', 'unknown')} "
        f"dirty_after={after.get('file_dirty_bytes', 'unknown')} "
        f"wal_bytes={_wal_size_bytes(store)} "
        "unconditional_checkpoint_attempted=false ceiling_maintenance_enabled=true",
        flush=True,
    )
    return state


def set_manifest_tables(store: Any, manifest: dict[str, Any]) -> None:
    tables = manifest.get("tables")
    names = (
        tuple(
            str(item.get("name") or "")
            for item in tables
            if isinstance(item, dict) and str(item.get("name") or "")
        )
        if isinstance(tables, list)
        else ()
    )
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


def _replace_route_call(route: Any, endpoint: Callable[..., Any]) -> None:
    route.endpoint = endpoint
    route.dependant.call = endpoint


async def _call_endpoint_inline(endpoint: Callable[..., Any], **kwargs: Any) -> Any:
    """Keep async endpoints on-loop and run synchronous endpoint bodies in AnyIO's bounded pool."""

    if inspect.iscoroutinefunction(endpoint):
        return await endpoint(**kwargs)
    result = await run_in_threadpool(endpoint, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


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


def _install_preworker_quiesce() -> None:
    """Wrap the already-composed worker chain without changing authority markers."""

    from . import render_runtime_bootstrap_repair as render_bootstrap

    current_workers = render_bootstrap._run_runtime_workers
    if bool(getattr(current_workers, "_roi_bootstrap_preworker_quiesce", False)):
        return

    @wraps(current_workers)
    async def workers_with_bootstrap_quiesce(runtime: Any, stop: Any) -> Any:
        store = getattr(runtime, "store", None)
        if store is None:
            raise RuntimeError("canonical runtime store unavailable before worker start")
        await asyncio.to_thread(prime_before_workers, store)
        return await current_workers(runtime, stop)

    setattr(workers_with_bootstrap_quiesce, "_roi_bootstrap_preworker_quiesce", True)
    setattr(workers_with_bootstrap_quiesce, "_roi_original_runtime_workers", current_workers)
    render_bootstrap._run_runtime_workers = workers_with_bootstrap_quiesce


def install_certification_bootstrap_autocheckpoint_lease(
    app: Any,
    runtime_provider: Callable[[], Any] | None = None,
) -> None:
    """Wrap the production bootstrap routes and prime the lease before workers."""

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

    async def manifest_endpoint(
        x_certification_token: str | None = None,
    ) -> dict[str, Any]:
        replication._require_shared_token(x_certification_token)
        store = _runtime_store(provider)
        await run_in_threadpool(refresh, store)
        payload = await _call_endpoint_inline(
            original_manifest,
            x_certification_token=x_certification_token,
        )
        await run_in_threadpool(set_manifest_tables, store, payload)
        return payload

    setattr(manifest_endpoint, "_roi_bootstrap_autocheckpoint_lease", True)
    setattr(manifest_endpoint, "_roi_original_endpoint", original_manifest)

    async def page_endpoint(
        background_tasks: BackgroundTasks,
        table: str,
        epoch: str,
        schema_fingerprint: str,
        cursor: str | None = None,
        limit: int = 250,
        x_certification_token: str | None = None,
    ) -> dict[str, Any]:
        replication._require_shared_token(x_certification_token)
        store = _runtime_store(provider)
        await run_in_threadpool(refresh, store)
        payload = await _call_endpoint_inline(
            original_page,
            background_tasks=background_tasks,
            table=table,
            epoch=epoch,
            schema_fingerprint=schema_fingerprint,
            cursor=cursor,
            limit=limit,
            x_certification_token=x_certification_token,
        )
        await run_in_threadpool(finish_if_complete, store, payload)
        return payload

    setattr(page_endpoint, "_roi_bootstrap_autocheckpoint_lease", True)
    setattr(page_endpoint, "_roi_original_endpoint", original_page)

    _replace_route_call(manifest_route, manifest_endpoint)
    _replace_route_call(page_route, page_endpoint)
    _install_preworker_quiesce()
    _INSTALLED = True
    app.state.roi_certification_bootstrap_autocheckpoint_lease = True
    app.state.roi_certification_bootstrap_autocheckpoint_lease_active = True
    app.state.roi_certification_bootstrap_autocheckpoint_lease_version = LEASE_VERSION


def status() -> dict[str, Any]:
    return {
        "lease_version": LEASE_VERSION,
        "installed": _INSTALLED,
        "scope": "authoritative_preworker_plus_registered_bootstrap_routes",
        "module_global_logical_functions_mutated": False,
        "route_wrappers_async": True,
        "background_tasks_forwarded": True,
        "anyio_sync_worker_route_wrapper": True,
        "lease_route_state_offloop": True,
        "preworker_lease_priming": True,
        "preworker_checkpoint_enabled": False,
        "preworker_unconditional_checkpoint_enabled": False,
        "preworker_ceiling_maintenance_enabled": True,
        "preworker_sync_and_file_cache_release": True,
        "idle_seconds": _idle_seconds(),
        "max_wal_bytes": _max_wal_bytes(),
        "maintenance_checkpoint_mode": "truncate_zero_wait_at_ceiling_only",
        "maintenance_success_reacquires_lease": True,
        "maintenance_busy_or_error_fail_closed": True,
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
    "prime_before_workers",
    "refresh",
    "set_manifest_tables",
    "status",
]
