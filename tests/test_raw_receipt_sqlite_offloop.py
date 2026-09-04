from __future__ import annotations

import asyncio
import threading
import time
from datetime import datetime, timezone
from types import SimpleNamespace

from solana_roi import direct_solana as direct_solana_module
from solana_roi import public_data_economics as economics
from solana_roi import raw_receipt_dispatch_repair as raw_dispatch


def _message(signature: str) -> dict:
    return {
        "method": "logsNotification",
        "params": {
            "subscription": 1,
            "result": {
                "context": {"slot": 123},
                "value": {"signature": signature, "err": None, "logs": []},
            },
        },
    }


def _plane(journal) -> SimpleNamespace:
    return SimpleNamespace(
        journal=journal,
        _launch_like=lambda _logs: False,
        coverage_status_fn=lambda: {
            "program_source_counts": {"PUMP_AMM": 10},
            "requirements": {"min_normalized_swaps_per_source": 10},
        },
    )


def test_slow_raw_receipt_sqlite_does_not_block_event_loop() -> None:
    main_thread = threading.get_ident()

    class Journal:
        def __init__(self) -> None:
            self.threads: list[int] = []

        def touch_provider(self, _provider, _received_at) -> None:
            self.threads.append(threading.get_ident())
            time.sleep(0.04)

        def record_receipt(self, **_kwargs) -> bool:
            self.threads.append(threading.get_ident())
            time.sleep(0.04)
            return True

        def enqueue(self, **_kwargs) -> None:
            raise AssertionError("source is already empirically complete")

    journal = Journal()
    plane = _plane(journal)
    target = SimpleNamespace(kind="program", source_hint="PUMP_AMM", address="pump-amm")

    async def scenario() -> int:
        task = asyncio.create_task(
            economics._selective_notification_handler(
                plane,
                "publicnode",
                {1: target},
                _message("slow-sqlite"),
            )
        )
        ticks = 0
        while not task.done():
            ticks += 1
            await asyncio.sleep(0.005)
        await task
        return ticks

    ticks = asyncio.run(scenario())
    assert ticks >= 8
    assert journal.threads
    assert all(thread_id != main_thread for thread_id in journal.threads)
    assert bool(getattr(economics._selective_notification_handler, "_roi_raw_receipt_sqlite_offloop", False))


def test_to_thread_preserves_socket_read_context(monkeypatch) -> None:
    fixed = datetime(2026, 9, 4, 22, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(direct_solana_module, "utcnow", raw_dispatch._receipt_aware_utcnow)

    class Journal:
        def __init__(self) -> None:
            self.received_at = None

        def touch_provider(self, _provider, received_at) -> None:
            self.received_at = received_at

        def record_receipt(self, **kwargs) -> bool:
            assert kwargs["received_at"] == fixed
            return True

        def enqueue(self, **_kwargs) -> None:
            raise AssertionError("source is already empirically complete")

    journal = Journal()
    plane = _plane(journal)
    target = SimpleNamespace(kind="program", source_hint="PUMP_AMM", address="pump-amm")
    token = raw_dispatch._RECEIPT_WALL_TIME.set(fixed)
    try:
        asyncio.run(
            economics._selective_notification_handler(
                plane,
                "publicnode",
                {1: target},
                _message("context-copy"),
            )
        )
    finally:
        raw_dispatch._RECEIPT_WALL_TIME.reset(token)

    assert journal.received_at == fixed
