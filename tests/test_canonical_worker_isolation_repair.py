from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace

import pytest

from solana_roi import canonical_worker_isolation_repair as repair


@pytest.fixture(autouse=True)
def _restore_isolation_module_state():
    """Keep focused isolation tests from mutating later production-composition tests."""
    original_workers = repair._ORIGINAL_RUNTIME_WORKERS
    original_thread = repair._WORKER_THREAD
    original_stop = repair._WORKER_STOP
    original_ready = repair._WORKER_READY
    original_installed = repair._INSTALLED
    original_state = dict(repair._STATE)
    try:
        yield
    finally:
        current_stop = repair._WORKER_STOP
        current_thread = repair._WORKER_THREAD
        if current_stop is not None:
            current_stop.set()
        if current_thread is not None and current_thread.is_alive():
            current_thread.join(timeout=1.0)
        repair._ORIGINAL_RUNTIME_WORKERS = original_workers
        repair._WORKER_THREAD = original_thread
        repair._WORKER_STOP = original_stop
        repair._WORKER_READY = original_ready
        repair._INSTALLED = original_installed
        repair._STATE.clear()
        repair._STATE.update(original_state)


def _reset_state() -> None:
    repair._WORKER_THREAD = None
    repair._WORKER_STOP = None
    repair._WORKER_READY = None
    repair._STATE.clear()
    repair._STATE.update(
        {
            "installed": True,
            "state": "test",
            "attempts": 0,
            "unexpected_exits": 0,
            "last_error": None,
            "worker_graph_started": False,
        }
    )


def test_canonical_worker_graph_runs_on_dedicated_os_thread() -> None:
    _reset_state()
    main_thread = threading.get_ident()
    worker_threads: list[int] = []
    started = threading.Event()

    async def original_workers(_runtime, stop: asyncio.Event) -> None:
        worker_threads.append(threading.get_ident())
        started.set()
        await stop.wait()

    repair._ORIGINAL_RUNTIME_WORKERS = original_workers
    runtime = SimpleNamespace()

    async def scenario() -> int:
        stop = asyncio.Event()
        task = asyncio.create_task(repair._isolated_runtime_workers(runtime, stop))
        ticks = 0
        while not started.is_set():
            ticks += 1
            await asyncio.sleep(0.005)
        for _ in range(10):
            ticks += 1
            await asyncio.sleep(0)
        stop.set()
        await task
        return ticks

    ticks = asyncio.run(scenario())
    assert ticks >= 10
    assert worker_threads
    assert worker_threads[0] != main_thread
    status = repair.isolation_status()
    assert status["uvicorn_event_loop_runs_canonical_solana_fomo_workers"] is False
    assert status["canonical_worker_graph_changed"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False


def test_pre_set_stop_still_enters_original_worker_graph_once() -> None:
    _reset_state()
    calls = 0
    started = threading.Event()

    async def original_workers(_runtime, stop: asyncio.Event) -> None:
        nonlocal calls
        calls += 1
        started.set()
        await stop.wait()

    repair._ORIGINAL_RUNTIME_WORKERS = original_workers

    async def scenario() -> None:
        stop = asyncio.Event()
        stop.set()
        await repair._isolated_runtime_workers(SimpleNamespace(), stop)

    asyncio.run(scenario())
    assert started.is_set()
    assert calls == 1


def test_unexpected_worker_exit_is_supervised_without_raising_into_asgi(monkeypatch) -> None:
    _reset_state()
    attempts = 0

    async def original_workers(_runtime, _stop: asyncio.Event) -> None:
        nonlocal attempts
        attempts += 1
        return

    repair._ORIGINAL_RUNTIME_WORKERS = original_workers
    monkeypatch.setattr(repair, "RESTART_BACKOFF_SECONDS", 0.01)
    monkeypatch.setattr(repair, "SUPERVISOR_POLL_SECONDS", 0.01)

    async def scenario() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(repair._isolated_runtime_workers(SimpleNamespace(), stop))
        for _ in range(100):
            if attempts >= 2:
                break
            await asyncio.sleep(0.005)
        stop.set()
        await task

    asyncio.run(scenario())
    assert attempts >= 2
    assert repair._STATE["unexpected_exits"] >= 1
