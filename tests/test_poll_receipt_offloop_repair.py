from __future__ import annotations

import asyncio
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from solana_roi import poll_receipt_offloop_repair as repair
from solana_roi import strategy_relevant_continuity as strategy
from solana_roi.direct_solana import WatchTarget


def _pids_current() -> int:
    try:
        return int(Path("/sys/fs/cgroup/pids.current").read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return len([thread for thread in threading.enumerate() if thread.is_alive()])


def _live_thread_ids() -> set[int]:
    return {
        int(thread.ident)
        for thread in threading.enumerate()
        if thread.is_alive() and thread.ident is not None
    }


def test_program_poll_receipt_persistence_runs_off_event_loop() -> None:
    main_thread = threading.get_ident()

    class Journal:
        def __init__(self) -> None:
            self.threads: list[int] = []

        def record_receipt(self, **_kwargs: Any) -> bool:
            self.threads.append(threading.get_ident())
            time.sleep(0.04)
            return True

    plane = SimpleNamespace(journal=Journal())
    target = WatchTarget(kind="program", address="program-address", source_hint="PUMP_AMM")
    rows = [{"signature": "sig-1", "slot": 123, "err": None}]

    async def scenario() -> int:
        task = asyncio.create_task(repair._record_poll_rows_scoped_offloop(plane, target, rows))
        ticks = 0
        while not task.done():
            ticks += 1
            await asyncio.sleep(0.005)
        assert await task == 1
        return ticks

    ticks = asyncio.run(scenario())
    assert ticks >= 6
    assert plane.journal.threads
    assert all(thread_id != main_thread for thread_id in plane.journal.threads)


def test_scout_installer_cycle_is_not_entered_and_checkpoint_order_is_preserved(monkeypatch) -> None:
    """Regress the production installer-order recursion exactly at its unsafe edge.

    A later continuity installation can capture a delegate whose body resolves back
    to the poll-receipt wrapper. The old implementation entered ``to_thread`` and
    called ``asyncio.run`` on that captured delegate, so every recursive pass built a
    fresh event loop/default executor. Canonical scout persistence must never consult
    that delegate: receipt persistence and enqueue/checkpoint ordering are one bounded
    synchronous worker operation.
    """

    main_thread = threading.get_ident()
    delegated = {"count": 0}

    class Journal:
        def __init__(self) -> None:
            self.events: list[tuple[str, str, int]] = []

        def record_receipt(self, **kwargs: Any) -> bool:
            self.events.append(("receipt", str(kwargs["signature"]), threading.get_ident()))
            return True

        def enqueue(self, **kwargs: Any) -> None:
            self.events.append(("enqueue", str(kwargs["signature"]), threading.get_ident()))
            assert kwargs["priority"] == 0
            assert kwargs["reason"] == "frozen_scout_live_poll_trigger"
            assert kwargs["source_hint"] is None

    plane = SimpleNamespace(journal=Journal())
    target = WatchTarget(kind="scout", address="scout-address", source_hint="SCOUT_ALPHA")
    rows = [
        {"signature": "sig-1", "slot": 123, "err": None},
        {"signature": "sig-2", "slot": 124, "err": None},
    ]

    async def captured_wrapper(self: Any, wrapped_target: WatchTarget, wrapped_rows: list[dict[str, Any]]) -> int:
        delegated["count"] += 1
        return await repair._record_poll_rows_scoped_offloop(self, wrapped_target, wrapped_rows)

    monkeypatch.setattr(strategy, "_ORIGINAL_RECORD_POLL_ROWS", captured_wrapper)

    assert asyncio.run(repair._record_poll_rows_scoped_offloop(plane, target, rows)) == 2
    assert delegated["count"] == 0, "canonical scout path re-entered captured async wrapper"
    assert [(kind, signature) for kind, signature, _thread in plane.journal.events] == [
        ("receipt", "sig-1"),
        ("enqueue", "sig-1"),
        ("receipt", "sig-2"),
        ("enqueue", "sig-2"),
    ]
    worker_threads = {thread_id for _kind, _signature, thread_id in plane.journal.events}
    assert worker_threads
    assert main_thread not in worker_threads
    assert len(worker_threads) == 1


def test_repeated_scout_receipts_retries_and_cancellation_keep_threads_and_pids_bounded(monkeypatch) -> None:
    """Production-shaped stress proof for the former recursive worker creator."""

    class Journal:
        def __init__(self) -> None:
            self.receipts = 0
            self.enqueues = 0
            self.lock = threading.Lock()

        def record_receipt(self, **_kwargs: Any) -> bool:
            time.sleep(0.002)
            with self.lock:
                self.receipts += 1
            return True

        def enqueue(self, **_kwargs: Any) -> None:
            with self.lock:
                self.enqueues += 1

    plane = SimpleNamespace(journal=Journal())
    target = WatchTarget(kind="scout", address="scout-address", source_hint="SCOUT_ALPHA")
    rows = [{"signature": "sig-stress", "slot": 999, "err": None}]
    delegated = {"count": 0}

    async def captured_wrapper(self: Any, wrapped_target: WatchTarget, wrapped_rows: list[dict[str, Any]]) -> int:
        delegated["count"] += 1
        return await repair._record_poll_rows_scoped_offloop(self, wrapped_target, wrapped_rows)

    # Reproduce the unsafe captured-wrapper shape. The fixed production path must
    # remain independent of it for every real journal write.
    monkeypatch.setattr(strategy, "_ORIGINAL_RECORD_POLL_ROWS", captured_wrapper)

    baseline_threads = _live_thread_ids()
    baseline_pids = _pids_current()
    pid_samples = [baseline_pids]

    async def exercise() -> None:
        for _ in range(96):
            assert await asyncio.wait_for(
                repair._record_poll_rows_scoped_offloop(plane, target, rows),
                timeout=1.0,
            ) == 1
            pid_samples.append(_pids_current())

        # Model overlapping retries plus cancellation. Cancelling a caller may leave
        # its already-started synchronous persistence call to finish, but it must not
        # spawn another event loop/executor layer or recursively create workers.
        tasks = [
            asyncio.create_task(repair._record_poll_rows_scoped_offloop(plane, target, rows))
            for _ in range(8)
        ]
        await asyncio.sleep(0)
        for task in tasks[::2]:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(0.05)
        pid_samples.append(_pids_current())

    asyncio.run(exercise())
    final_threads = _live_thread_ids()
    final_pids = _pids_current()

    assert delegated["count"] == 0
    assert plane.journal.receipts >= 96
    assert plane.journal.enqueues == plane.journal.receipts
    assert max(pid_samples) <= baseline_pids + 10, {
        "baseline_pids_current": baseline_pids,
        "peak_pids_current": max(pid_samples),
        "samples": pid_samples,
    }
    assert final_pids <= baseline_pids + 2, {
        "baseline_pids_current": baseline_pids,
        "final_pids_current": final_pids,
    }
    assert len(final_threads - baseline_threads) <= 1, {
        "baseline_threads": sorted(baseline_threads),
        "final_threads": sorted(final_threads),
        "new_threads": sorted(final_threads - baseline_threads),
    }


def test_offloop_repair_does_not_change_recovery_or_authority_constants() -> None:
    from solana_roi import live_poll_redundancy as live_poll

    # These are the existing frozen polling/recovery boundaries; the repair changes
    # only the execution thread used for durable poll-receipt persistence.
    assert live_poll.POLL_INTERVAL_SECONDS == 4.0
    assert live_poll.POLL_LIMIT == 1000
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
