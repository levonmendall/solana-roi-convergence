from __future__ import annotations

import asyncio
import threading
from contextlib import suppress
from typing import Any, Callable

from . import render_runtime_bootstrap_repair as render_bootstrap


REPAIR_VERSION = "canonical-worker-isolation-v1"
THREAD_NAME = "solana-fomo-canonical-isolated"
THREAD_JOIN_TIMEOUT_SECONDS = 3.0
RESTART_BACKOFF_SECONDS = 1.0
SUPERVISOR_POLL_SECONDS = 0.20

_ORIGINAL_RUNTIME_WORKERS: Callable[[Any, asyncio.Event], Any] | None = None
_WORKER_THREAD: threading.Thread | None = None
_WORKER_STOP: threading.Event | None = None
_INSTALLED = False
_STATE: dict[str, Any] = {
    "installed": False,
    "state": "not_started",
    "attempts": 0,
    "unexpected_exits": 0,
    "last_error": None,
}


async def _thread_stop_bridge(thread_stop: threading.Event, local_stop: asyncio.Event) -> None:
    while not thread_stop.is_set():
        await asyncio.sleep(0.10)
    local_stop.set()


async def _worker_async(runtime: Any, thread_stop: threading.Event) -> None:
    if _ORIGINAL_RUNTIME_WORKERS is None:
        raise RuntimeError("canonical worker isolation is not installed")
    local_stop = asyncio.Event()
    bridge = asyncio.create_task(
        _thread_stop_bridge(thread_stop, local_stop),
        name="canonical-worker-thread-stop-bridge",
    )
    try:
        await _ORIGINAL_RUNTIME_WORKERS(runtime, local_stop)
        if not thread_stop.is_set() and not local_stop.is_set():
            raise RuntimeError("canonical runtime workers returned unexpectedly")
    finally:
        local_stop.set()
        if not bridge.done():
            bridge.cancel()
        with suppress(asyncio.CancelledError):
            await bridge


def _worker_thread_main(runtime: Any, thread_stop: threading.Event) -> None:
    try:
        asyncio.run(_worker_async(runtime, thread_stop))
    except BaseException as exc:
        _STATE["last_error"] = f"{type(exc).__name__}: {exc}"
        if not thread_stop.is_set():
            _STATE["unexpected_exits"] = int(_STATE.get("unexpected_exits", 0)) + 1
            _STATE["state"] = "failed_closed_restart_pending"
    else:
        if thread_stop.is_set():
            _STATE["state"] = "stopped"


def _start_worker_thread(runtime: Any) -> tuple[threading.Thread, threading.Event]:
    global _WORKER_THREAD, _WORKER_STOP
    thread_stop = threading.Event()
    thread = threading.Thread(
        target=_worker_thread_main,
        args=(runtime, thread_stop),
        name=THREAD_NAME,
        daemon=True,
    )
    _WORKER_THREAD = thread
    _WORKER_STOP = thread_stop
    _STATE["attempts"] = int(_STATE.get("attempts", 0)) + 1
    _STATE["state"] = "running"
    _STATE["last_error"] = None
    thread.start()
    return thread, thread_stop


async def _join_worker(thread: threading.Thread, thread_stop: threading.Event) -> None:
    thread_stop.set()
    await asyncio.to_thread(thread.join, THREAD_JOIN_TIMEOUT_SECONDS)
    if thread.is_alive():
        _STATE["state"] = "shutdown_timeout_daemon_thread"
    elif _STATE.get("state") != "failed_closed_restart_pending":
        _STATE["state"] = "stopped"


async def _isolated_runtime_workers(runtime: Any, stop: asyncio.Event) -> None:
    """Supervise canonical Solana/FOMO workers outside the Uvicorn event loop.

    Render health and API routing remain on the process main event loop. The existing
    canonical worker coroutine graph is moved intact onto one dedicated OS thread and
    private asyncio loop, so synchronous SQLite, TLS setup, JSON/CPU work or research
    bookkeeping inside any canonical task can no longer starve `/v1/strategy/baseline`.

    The worker graph, ordering, RPC pools, continuity rules and strategy semantics are
    not rewritten. If the isolated worker loop exits unexpectedly it is restarted
    fail-closed after a bounded backoff while the web process remains observable.
    """

    while not stop.is_set():
        thread, thread_stop = _start_worker_thread(runtime)
        while not stop.is_set() and thread.is_alive():
            try:
                await asyncio.wait_for(stop.wait(), timeout=SUPERVISOR_POLL_SECONDS)
            except asyncio.TimeoutError:
                continue
        if stop.is_set():
            await _join_worker(thread, thread_stop)
            return

        # The thread is already dead here. Do not let an unexpected canonical worker
        # failure terminate ASGI; readiness remains fail-closed through canonical
        # runtime state while the supervisor retries the same worker graph.
        thread_stop.set()
        _STATE["state"] = "failed_closed_restart_pending"
        try:
            await asyncio.wait_for(stop.wait(), timeout=RESTART_BACKOFF_SECONDS)
        except asyncio.TimeoutError:
            continue


setattr(_isolated_runtime_workers, "_roi_canonical_worker_isolation", True)


def isolation_status() -> dict[str, Any]:
    thread = _WORKER_THREAD
    payload = dict(_STATE)
    payload.update(
        {
            "repair_version": REPAIR_VERSION,
            "worker_topology": "dedicated_os_thread_with_private_asyncio_loop",
            "worker_thread_name": THREAD_NAME,
            "worker_thread_alive": bool(thread is not None and thread.is_alive()),
            "uvicorn_event_loop_runs_canonical_solana_fomo_workers": False,
            "canonical_worker_graph_changed": False,
            "canonical_sqlite_store_changed": False,
            "market_scope_changed": False,
            "continuity_thresholds_changed": False,
            "strategy_thresholds_changed": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    )
    return payload


def install_canonical_worker_isolation() -> None:
    global _ORIGINAL_RUNTIME_WORKERS, _INSTALLED
    if _INSTALLED:
        return
    current = render_bootstrap._run_runtime_workers
    if bool(getattr(current, "_roi_canonical_worker_isolation", False)):
        _INSTALLED = True
        return
    _ORIGINAL_RUNTIME_WORKERS = current
    render_bootstrap._run_runtime_workers = _isolated_runtime_workers
    _STATE.update(
        {
            "installed": True,
            "state": "installed_waiting_for_runtime",
            "attempts": 0,
            "unexpected_exits": 0,
            "last_error": None,
        }
    )
    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "THREAD_NAME",
    "_isolated_runtime_workers",
    "install_canonical_worker_isolation",
    "isolation_status",
]
