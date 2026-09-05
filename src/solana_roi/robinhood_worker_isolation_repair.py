from __future__ import annotations

import asyncio
import copy
import os
import sqlite3
import threading
import time
from contextlib import suppress
from pathlib import Path
from typing import Any, Callable

from .observation_store import ObservationEventStore
from .robinhood_chain_paper import RobinhoodChainPaperPlane


REPAIR_VERSION = "robinhood-dedicated-worker-isolation-v1"
STATUS_PUBLISH_SECONDS = 1.0
STATUS_STALE_SECONDS = 5.0
THREAD_JOIN_TIMEOUT_SECONDS = 3.0
THREAD_NAME = "robinhood-chain-paper-isolated"

_STATUS_LOCK = threading.Lock()
_STATUS_SNAPSHOT: dict[str, Any] | None = None
_STATUS_PUBLISHED_MONOTONIC: float | None = None
_WORKER_THREAD: threading.Thread | None = None
_WORKER_STOP: threading.Event | None = None
_ORIGINAL_STATUS: Callable[[], dict[str, Any]] | None = None
_INSTALLED = False


def _runtime_install_module() -> Any:
    # Imported lazily because this repair is intentionally installed only after
    # robinhood_runtime_install has finished defining its worker and status hooks.
    from . import robinhood_runtime_install as module

    return module


def _dedicated_store_path(canonical_store: Any) -> Path:
    canonical = Path(canonical_store.path).expanduser().resolve()
    configured = os.getenv("ROBINHOOD_CHAIN_STORE_PATH", "").strip()
    if configured:
        candidate = Path(configured).expanduser()
        if not candidate.is_absolute():
            candidate = canonical.parent / candidate
    else:
        suffix = canonical.suffix or ".sqlite3"
        candidate = canonical.with_name(f"{canonical.stem}-robinhood-chain{suffix}")
    candidate = candidate.resolve()
    if candidate == canonical:
        raise RuntimeError("Robinhood dedicated store must not be the canonical Solana SQLite file")
    return candidate


def _worker_isolation_metadata(*, store_path: str | None = None) -> dict[str, Any]:
    thread = _WORKER_THREAD
    return {
        "repair_version": REPAIR_VERSION,
        "worker_topology": "dedicated_os_thread_with_private_asyncio_loop",
        "worker_thread_name": THREAD_NAME,
        "worker_thread_alive": bool(thread is not None and thread.is_alive()),
        "dedicated_sqlite_store": True,
        "dedicated_store_path": store_path,
        "canonical_store_shared_for_robinhood_writes": False,
        "canonical_store_used_only_for_one_time_cursor_seed": True,
        "status_served_from_nonblocking_cache": True,
        "uvicorn_event_loop_runs_robinhood_chain_worker": False,
        "catchup_batch_limit_changed": False,
        "catchup_poll_cadence_changed": False,
        "paper_decision_gate_changed": False,
        "strategy_thresholds_changed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def _publish_snapshot(payload: dict[str, Any], *, store_path: str | None) -> None:
    global _STATUS_SNAPSHOT, _STATUS_PUBLISHED_MONOTONIC
    published = copy.deepcopy(payload)
    published["worker_isolation"] = _worker_isolation_metadata(store_path=store_path)
    with _STATUS_LOCK:
        _STATUS_SNAPSHOT = published
        _STATUS_PUBLISHED_MONOTONIC = time.monotonic()


def _failed_closed_payload(error: str, *, store_path: str | None = None) -> dict[str, Any]:
    module = _runtime_install_module()
    return {
        "enabled": True,
        "chain": "ROBINHOOD_CHAIN",
        "chain_id": 4663,
        "strategy_version": getattr(module, "ROBINHOOD_V5_VERSION", "robinhood-chain-paper"),
        "paper_only": True,
        "paper_trading_authority": False,
        "shadow_only": False,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
        "runtime_ready": False,
        "failed_closed": True,
        "caught_up_for_paper_decisions": False,
        "paper_decision_transport_ready": False,
        "error": error,
        "production_install": dict(getattr(module, "_STATE", {})),
        "worker_isolation": _worker_isolation_metadata(store_path=store_path),
    }


def _nonblocking_status() -> dict[str, Any]:
    """Return Robinhood telemetry without ever waiting on its SQLite/thread locks."""

    module = _runtime_install_module()
    with _STATUS_LOCK:
        snapshot = copy.deepcopy(_STATUS_SNAPSHOT)
        published_at = _STATUS_PUBLISHED_MONOTONIC

    if snapshot is None:
        return _failed_closed_payload(
            getattr(module, "_STARTUP_ERROR", None) or "isolated_robinhood_worker_not_ready"
        )

    age = max(0.0, time.monotonic() - published_at) if published_at is not None else None
    isolation = snapshot.setdefault("worker_isolation", {})
    if isinstance(isolation, dict):
        isolation.update(_worker_isolation_metadata(store_path=isolation.get("dedicated_store_path")))
        isolation["status_cache_age_seconds"] = age
        isolation["status_cache_stale_after_seconds"] = STATUS_STALE_SECONDS

    snapshot["production_install"] = dict(getattr(module, "_STATE", {}))
    thread_alive = bool(_WORKER_THREAD is not None and _WORKER_THREAD.is_alive())
    stale = age is None or age > STATUS_STALE_SECONDS
    if stale or not thread_alive:
        snapshot["runtime_ready"] = False
        snapshot["failed_closed"] = True
        snapshot["paper_trading_authority"] = False
        snapshot["caught_up_for_paper_decisions"] = False
        snapshot["paper_decision_transport_ready"] = False
        snapshot["error"] = (
            "robinhood_isolated_worker_thread_not_alive"
            if not thread_alive
            else "robinhood_isolated_status_snapshot_stale"
        )
        if isinstance(isolation, dict):
            isolation["status_cache_stale"] = stale
    elif isinstance(isolation, dict):
        isolation["status_cache_stale"] = False
    return snapshot


def _seed_cursor_from_canonical(plane: Any, canonical_store: Any) -> int | None:
    """Carry forward only the durable processed-block cursor into the private store.

    The cursor is written by the canonical Robinhood implementation only after the
    entire block range is processed. Copying it therefore cannot skip an unfinished
    range. Strategy outcomes, launches, and release-bound promotion evidence are not
    copied; prior canonical rows remain immutable audit history.
    """

    if getattr(plane, "_cursor", None) is not None:
        return int(plane._cursor)
    try:
        with canonical_store._lock:
            row = canonical_store.db.execute(
                "SELECT value FROM robinhood_chain_state WHERE key='cursor_block'"
            ).fetchone()
    except sqlite3.Error:
        return None
    if row is None:
        return None
    value = int(row["value"] if hasattr(row, "keys") else row[0])
    plane._set_cursor(value)
    return value


async def _status_publisher(local_stop: asyncio.Event, *, store_path: str) -> None:
    while not local_stop.is_set():
        try:
            if _ORIGINAL_STATUS is not None:
                _publish_snapshot(_ORIGINAL_STATUS(), store_path=store_path)
        except Exception as exc:
            module = _runtime_install_module()
            module._STARTUP_ERROR = f"{type(exc).__name__}: Robinhood status snapshot failed"
        try:
            await asyncio.wait_for(local_stop.wait(), timeout=STATUS_PUBLISH_SECONDS)
        except TimeoutError:
            pass


async def _thread_stop_bridge(thread_stop: threading.Event, local_stop: asyncio.Event) -> None:
    while not thread_stop.is_set():
        await asyncio.sleep(0.10)
    local_stop.set()


async def _worker_async(
    canonical_store: Any,
    thread_stop: threading.Event,
    *,
    plane_factory: Callable[..., Any] = RobinhoodChainPaperPlane,
    store_factory: Callable[..., Any] = ObservationEventStore,
) -> None:
    module = _runtime_install_module()
    dedicated_store: Any | None = None
    plane: Any | None = None
    local_stop = asyncio.Event()
    bridge_task: asyncio.Task[None] | None = None
    publisher_task: asyncio.Task[None] | None = None
    store_path: Path | None = None
    try:
        store_path = _dedicated_store_path(canonical_store)
        dedicated_store = store_factory(store_path)
        plane = plane_factory(dedicated_store)
        module._PLANE = plane
        module._STARTUP_ERROR = None
        seeded_cursor = _seed_cursor_from_canonical(plane, canonical_store)
        module._STATE.update(
            {
                "state": "running" if plane.enabled else "disabled",
                "worker_isolation": "dedicated_os_thread_with_private_asyncio_loop",
                "dedicated_store": str(store_path),
                "cursor_seeded_from_canonical": seeded_cursor,
            }
        )
        if _ORIGINAL_STATUS is not None:
            _publish_snapshot(_ORIGINAL_STATUS(), store_path=str(store_path))
        if not plane.enabled:
            return

        bridge_task = asyncio.create_task(
            _thread_stop_bridge(thread_stop, local_stop), name="robinhood-thread-stop-bridge"
        )
        publisher_task = asyncio.create_task(
            _status_publisher(local_stop, store_path=str(store_path)),
            name="robinhood-thread-status-publisher",
        )
        await plane.run(local_stop)
        if not local_stop.is_set() and not thread_stop.is_set():
            raise RuntimeError("Robinhood isolated worker returned unexpectedly")
    finally:
        local_stop.set()
        for task in (bridge_task, publisher_task):
            if task is not None and not task.done():
                task.cancel()
        for task in (bridge_task, publisher_task):
            if task is not None:
                with suppress(asyncio.CancelledError, Exception):
                    await task
        if plane is not None:
            with suppress(Exception):
                await plane.close()
        if dedicated_store is not None:
            with suppress(Exception):
                dedicated_store.close()
        if thread_stop.is_set():
            module._STATE["state"] = "stopped"


def _worker_thread_main(
    canonical_store: Any,
    thread_stop: threading.Event,
    *,
    plane_factory: Callable[..., Any] = RobinhoodChainPaperPlane,
    store_factory: Callable[..., Any] = ObservationEventStore,
) -> None:
    module = _runtime_install_module()
    try:
        asyncio.run(
            _worker_async(
                canonical_store,
                thread_stop,
                plane_factory=plane_factory,
                store_factory=store_factory,
            )
        )
    except BaseException as exc:
        module._STARTUP_ERROR = f"{type(exc).__name__}: {exc}"
        module._STATE["state"] = "failed_closed"
        store_path: str | None = None
        with suppress(Exception):
            store_path = str(_dedicated_store_path(canonical_store))
        _publish_snapshot(
            _failed_closed_payload(module._STARTUP_ERROR, store_path=store_path),
            store_path=store_path,
        )
    finally:
        module._PLANE = None


def _start_worker_thread(
    canonical_store: Any,
    *,
    plane_factory: Callable[..., Any] = RobinhoodChainPaperPlane,
    store_factory: Callable[..., Any] = ObservationEventStore,
) -> tuple[threading.Thread, threading.Event]:
    global _WORKER_THREAD, _WORKER_STOP
    stop = threading.Event()
    thread = threading.Thread(
        target=_worker_thread_main,
        args=(canonical_store, stop),
        kwargs={"plane_factory": plane_factory, "store_factory": store_factory},
        name=THREAD_NAME,
        daemon=True,
    )
    _WORKER_THREAD = thread
    _WORKER_STOP = stop
    thread.start()
    return thread, stop


async def _isolated_runtime_workers(runtime: Any, stop: asyncio.Event) -> None:
    """Keep Robinhood completely off the Uvicorn loop and canonical SQLite file.

    Production's post-bootstrap runtime always owns a canonical durable store. Some
    focused regression harnesses intentionally exercise only the canonical worker
    composition with minimal runtime doubles that omit storage entirely. Those
    doubles cannot host Robinhood and therefore delegate unchanged to the original
    workers rather than forcing tests to fabricate production-only state.
    """

    module = _runtime_install_module()
    original_workers = getattr(module, "_ORIGINAL_RUNTIME_WORKERS", None)
    if original_workers is None:
        raise RuntimeError("Robinhood production worker composition is not installed")

    canonical_store = getattr(runtime, "store", None)
    if canonical_store is None or not hasattr(canonical_store, "path"):
        module._STATE["worker_isolation_skipped_no_store"] = True
        await original_workers(runtime, stop)
        return

    module._STATE["worker_isolation_skipped_no_store"] = False
    module._STATE["attempts"] = int(module._STATE.get("attempts", 0)) + 1
    thread, thread_stop = _start_worker_thread(canonical_store)
    try:
        await original_workers(runtime, stop)
    finally:
        thread_stop.set()
        await asyncio.to_thread(thread.join, THREAD_JOIN_TIMEOUT_SECONDS)
        if thread.is_alive():
            module._STATE["state"] = "shutdown_timeout_daemon_thread"
        elif stop.is_set() and module._STATE.get("state") != "failed_closed":
            module._STATE["state"] = "stopped"


setattr(_isolated_runtime_workers, "_roi_robinhood_dedicated_worker_isolation", True)
setattr(_nonblocking_status, "_roi_robinhood_dedicated_worker_isolation", True)


def install_robinhood_worker_isolation_repair() -> None:
    global _ORIGINAL_STATUS, _INSTALLED
    if _INSTALLED:
        return
    module = _runtime_install_module()
    _ORIGINAL_STATUS = module._status
    module._runtime_workers_with_robinhood = _isolated_runtime_workers
    module._status = _nonblocking_status
    module._STATE.update(
        {
            "worker_isolation_repair": REPAIR_VERSION,
            "worker_isolation": "dedicated_os_thread_with_private_asyncio_loop",
        }
    )
    _INSTALLED = True


# This module is imported at the very end of robinhood_runtime_install, after that
# module has defined its app-facing installer. Wrap that final production function
# so the consolidated v5.1 authority is installed only after every existing Solana,
# FOMO and Robinhood compatibility layer has finished composing. This preserves the
# certified solana_roi.production:app Render entrypoint and constant-time liveness.
from .v51_final_production_install import install_v51_final_production_hook

install_v51_final_production_hook()


__all__ = [
    "REPAIR_VERSION",
    "STATUS_STALE_SECONDS",
    "THREAD_JOIN_TIMEOUT_SECONDS",
    "_dedicated_store_path",
    "_nonblocking_status",
    "_seed_cursor_from_canonical",
    "_start_worker_thread",
    "install_robinhood_worker_isolation_repair",
]
