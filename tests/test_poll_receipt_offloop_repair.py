from __future__ import annotations

import asyncio
import threading
import time
from types import SimpleNamespace

from solana_roi import strategy_relevant_continuity as strategy
from solana_roi.direct_solana import WatchTarget


def test_program_poll_receipt_persistence_runs_off_event_loop() -> None:
    main_thread = threading.get_ident()

    class Journal:
        def __init__(self) -> None:
            self.threads: list[int] = []

        def record_receipt(self, **_kwargs) -> bool:
            self.threads.append(threading.get_ident())
            time.sleep(0.04)
            return True

    plane = SimpleNamespace(journal=Journal())
    target = WatchTarget(kind="program", address="program-address", source_hint="PUMP_AMM")
    rows = [{"signature": "sig-1", "slot": 123, "err": None}]

    async def scenario() -> int:
        task = asyncio.create_task(strategy._record_poll_rows_scoped(plane, target, rows))
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
    assert bool(getattr(strategy._record_poll_rows_scoped, "_roi_poll_receipt_offloop", False))


def test_offloop_repair_does_not_change_recovery_or_authority_constants() -> None:
    from solana_roi import live_poll_redundancy as live_poll

    assert live_poll.POLL_INTERVAL_SECONDS == 4.0
    assert live_poll.POLL_LIMIT == 1000
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
