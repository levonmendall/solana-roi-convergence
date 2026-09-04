from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace

from solana_roi import canonical_worker_isolation_repair as repair


def _reset_state() -> None:
    repair._WORKER_THREAD = None
    repair._WORKER_STOP = None
    repair._STATE.update(
        {
            "installed": True,
            "state": "test",
            "attempts": 0,
            "unexpected_exits": 0,
            "last_error": None,
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
