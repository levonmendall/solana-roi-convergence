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


def test_slow_start_cannot_launch_replacement_before_prior_thread_exits(monkeypatch) -> None:
    _reset_state()
    release_startup = threading.Event()
    first_started = threading.Event()
    threads = []
    active = 0
    peak_active = 0
    lock = threading.Lock()

    async def original_workers(_runtime, stop: asyncio.Event) -> None:
        nonlocal active, peak_active
        with lock:
            threads.append(threading.current_thread())
            active += 1
            peak_active = max(active, peak_active)
        try:
            first_started.set()
            # Model synchronous SQLite/bootstrap work before the private event
            # loop can publish readiness or process its stop bridge.
            release_startup.wait(timeout=2.0)
            await stop.wait()
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(repair, "_ORIGINAL_RUNTIME_WORKERS", original_workers)
    monkeypatch.setattr(repair, "WORKER_START_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(repair, "THREAD_JOIN_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(repair, "RESTART_BACKOFF_SECONDS", 0.005)
    monkeypatch.setattr(repair, "SUPERVISOR_POLL_SECONDS", 0.005)

    async def scenario() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(repair._isolated_runtime_workers(SimpleNamespace(), stop))
        try:
            assert await asyncio.to_thread(first_started.wait, 1.0)
            await asyncio.sleep(0.10)
            assert len(threads) == 1
            assert repair._STATE["attempts"] == 1
            release_startup.set()
            # A replacement may start once the timed-out generation has exited.
            for _ in range(100):
                if len(threads) >= 2:
                    break
                await asyncio.sleep(0.01)
            assert len(threads) >= 2
            assert peak_active == 1
        finally:
            release_startup.set()
            stop.set()
            await task
            for thread in threads:
                await asyncio.to_thread(thread.join, 1.0)
            assert not any(thread.is_alive() for thread in threads)

    asyncio.run(scenario())
