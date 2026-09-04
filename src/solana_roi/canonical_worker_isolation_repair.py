from __future__ import annotations

import asyncio
import threading
from contextlib import suppress
from typing import Any, Callable

from . import render_runtime_bootstrap_repair as render_bootstrap


REPAIR_VERSION = "canonical-worker-isolation-v2"
THREAD_NAME = "solana-fomo-canonical-isolated"
THREAD_JOIN_TIMEOUT_SECONDS = 3.0
WORKER_START_TIMEOUT_SECONDS = 2.0
RESTART_BACKOFF_SECONDS = 1.0
SUPERVISOR_POLL_SECONDS = 0.20

_ORIGINAL_RUNTIME_WORKERS: Callable[[Any, asyncio.Event], Any] | None = None
_WORKER_THREAD: threading.Thread | None = None
_WORKER_STOP: threading.Event | None = None
_WORKER_READY: threading.Event | None = None
_INSTALLED = False
_STATE: dict[str, Any] = {
    "installed": False,
    "state": "not_started",
    "attempts": 0,
    "unexpected_exits": 0,
    "last_error": None,
    "worker_graph_started": False,
}


async def _thread_stop_bridge(thread_stop: threading.Event, local_stop: asyncio.Event) -> None:
    while not thread_stop.is_set():
        await asyncio.sleep(0.10)
    local_stop.set()


async def _worker_async(
    runtime: Any,
    thread_stop: threading.Event,
    ready: threading.Event,
) -> None:
    if _ORIGINAL_RUNTIME_WORKERS is None:
        raise RuntimeError("canonical worker isolation is not installed")
    local_stop = asyncio.Event()
    bridge = asyncio.create_task(
        _thread_stop_bridge(thread_stop, local_stop),
        name="canonical-worker-thread-stop-bridge",
    )
    worker_task = asyncio.create_task(
        _ORIGINAL_RUNTIME_WORKERS(runtime, local_stop),
        name="canonical-worker-graph",
    )
    try:
        # Give the original graph enough private-loop turns to create and enter its
        # child workers before the outer ASGI supervisor can honor an immediate
        # shutdown. This preserves the pre-isolation worker-start contract without
        # running any canonical work on Uvicorn's event loop.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        _STATE["worker_graph_started"] = True
        ready.set()

        await worker_task
        if not thread_stop.is_set() and not local_stop.is_set():
            raise RuntimeError("canonical runtime workers returned unexpectedly")
    finally:
        local_stop.set()
        ready.set()
        if not worker_task.done():
            worker_task.cancel()
        with suppress(asyncio.CancelledError):
            await worker_task
        if not bridge.done():
            bridge.cancel()
        with suppress(asyncio.CancelledError):
            await bridge


def _worker_thread_main(
    runtime: Any,
    thread_stop: threading.Event,
    ready: threading.Event,
) -> None:
    try:
        asyncio.run(_worker_async(runtime, thread_stop, ready))
    except BaseException as exc:
        _STATE["last_error"] = f"{type(exc).__name__}: {exc}"
        ready.set()
        if not thread_stop.is_set():
            _STATE["unexpected_exits"] = int(_STATE.get("unexpected_exits", 0)) + 1
            _STATE["state"] = "failed_closed_restart_pending"
    else:
        if thread_stop.is_set():
            _STATE["state"] = "stopped"


def _start_worker_thread(runtime: Any) -> tuple[threading.Thread, threading.Event, threading.Event]:
    global _WORKER_THREAD, _WORKER_STOP, _WORKER_READY
    thread_stop = threading.Event()
    ready = threading.Event()
    thread = threading.Thread(
        target=_worker_thread_main,
        args=(runtime, thread_stop, ready),
        name=THREAD_NAME,
        daemon=True,
    )
    _WORKER_THREAD = thread
    _WORKER_STOP = thread_stop
    _WORKER_READY = ready
    _STATE["attempts"] = int(_STATE.get("attempts", 0)) + 1
    _STATE["state"] = "starting"
    _STATE["last_error"] = None
    _STATE["worker_graph_started"] = False
    thread.start()
    return thread, thread_stop, ready


async def _join_worker(thread: threading.Thread, thread_stop: threading.Event) -> None:
    thread_stop.set()
    await asyncio.to_thread(thread.join, THREAD_JOIN_TIMEOUT_SECONDS)
    if thread.is_alive():
        _STATE["state"] = "shutdown_timeout_daemon_thread"
    elif _STATE.get("state") != "failed_closed_restart_pending":
        _STATE["state"] = "stopped"


async def _wait_for_worker_start(
    thread: threading.Thread,
    ready: threading.Event,
) -> bool:
    started = await asyncio.to_thread(ready.wait, WORKER_START_TIMEOUT_SECONDS)
    if started and bool(_STATE.get("worker_graph_started")):
        _STATE["state"] = "running"
        return True
    if not thread.is_alive():
        return False
    _STATE["last_error"] = "TimeoutError: canonical worker graph did not publish start barrier"
    _STATE["state"] = "failed_closed_restart_pending"
    return False


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

    first_attempt = True
    while first_attempt or not stop.is_set():
        first_attempt = False
        thread, thread_stop, ready = _start_worker_thread(runtime)
        started = await _wait_for_worker_start(thread, ready)

        if stop.is_set():
            # Even immediate/pre-set shutdown enters the original worker graph once.
            # The cross-thread barrier above prevents the scheduler race caught by CI.
            await _join_worker(thread, thread_stop)
            return

        if not started:
            thread_stop.set()
            await asyncio.to_thread(thread.join, THREAD_JOIN_TIMEOUT_SECONDS)
            _STATE["state"] = "failed_closed_restart_pending"
            try:
                await asyncio.wait_for(stop.wait(), timeout=RESTART_BACKOFF_SECONDS)
            except asyncio.TimeoutError:
                continue
            return

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
    ready = _WORKER_READY
    payload = dict(_STATE)
    payload.update(
        {
            "repair_version": REPAIR_VERSION,
            "worker_topology": "dedicated_os_thread_with_private_asyncio_loop",
            "worker_thread_name": THREAD_NAME,
            "worker_thread_alive": bool(thread is not None and thread.is_alive()),
            "worker_start_barrier_set": bool(ready is not None and ready.is_set()),
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
            "worker_graph_started": False,
        }
    )
    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "THREAD_NAME",
    "WORKER_START_TIMEOUT_SECONDS",
    "_isolated_runtime_workers",
    "install_canonical_worker_isolation",
    "isolation_status",
]
