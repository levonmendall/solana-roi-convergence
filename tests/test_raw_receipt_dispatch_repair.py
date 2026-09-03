from __future__ import annotations

import asyncio
import itertools
from datetime import datetime, timezone
from types import SimpleNamespace

from solana_roi import raw_receipt_dispatch_repair as repair
from solana_roi import poll_recoverability_lease as lease
from solana_roi.launch_funding import LaunchFundingPolicy


def _message(*, logs=None, slot=123, signature="launch-sig"):
    return {
        "method": "logsNotification",
        "params": {
            "subscription": 1,
            "result": {
                "context": {"slot": slot},
                "value": {
                    "signature": signature,
                    "err": None,
                    "logs": logs or ["Program log: Instruction: Create"],
                },
            },
        },
    }


def test_compact_notification_preserves_only_handler_contract():
    received = datetime(2026, 9, 3, 3, 30, tzinfo=timezone.utc)
    compact = repair._compact_notification(
        _message(logs=["x" * 10000]),
        launch_like=True,
        received_at=received,
        received_monotonic=12.5,
    )

    assert compact["params"]["subscription"] == 1
    assert compact["params"]["result"]["context"]["slot"] == 123
    assert compact["params"]["result"]["value"]["signature"] == "launch-sig"
    assert compact["params"]["result"]["value"]["logs"] == [repair._LAUNCH_SENTINEL]
    assert compact["_roi_raw_received_at"] == received.isoformat()
    assert compact["_roi_raw_received_monotonic"] == 12.5
    assert compact["_roi_frontier_precaptured"] is True


def test_receipt_context_clock_is_local_to_dispatch():
    original_now = repair._ORIGINAL_UTCNOW()
    bound = datetime(2026, 9, 3, 3, 31, tzinfo=timezone.utc)
    token = repair._RECEIPT_WALL_TIME.set(bound)
    try:
        assert repair._receipt_aware_utcnow() == bound
    finally:
        repair._RECEIPT_WALL_TIME.reset(token)
    assert repair._receipt_aware_utcnow() >= original_now


def test_reader_precaptures_frontier_and_enqueues_without_inline_durable_handler(monkeypatch):
    events = []

    def capture(_plane, signature, slot, received_monotonic):
        events.append(("capture", signature, slot, received_monotonic))
        return True

    def observe(_plane, provider, slot, received_monotonic):
        events.append(("observe", provider, slot, received_monotonic))

    monkeypatch.setattr(repair.frontier, "_capture_preexisting_frontier", capture)
    monkeypatch.setattr(repair.frontier, "_observe_frontier", observe)

    calls = []

    async def durable_handler(_plane, _provider, _targets, _message):
        calls.append("durable")

    plane = SimpleNamespace(
        _roi_raw_receipt_dispatch_queue=asyncio.PriorityQueue(maxsize=repair.RAW_RECEIPT_QUEUE_MAX),
        _roi_raw_receipt_dispatch_sequence=itertools.count(),
        _roi_raw_receipt_dispatch_fatal=None,
        _launch_like=lambda logs: bool(logs),
    )
    target = SimpleNamespace(kind="program", address="program", source_hint="PUMP_FUN")
    queued = repair._queued_handler(durable_handler)

    async def scenario():
        await queued(plane, "publicnode", {1: target}, _message())
        item = plane._roi_raw_receipt_dispatch_queue.get_nowait()
        plane._roi_raw_receipt_dispatch_queue.task_done()
        return item

    item = asyncio.run(scenario())

    assert calls == []
    assert [event[0] for event in events] == ["capture", "observe"]
    assert item[0] == 0
    assert item[-1]["params"]["result"]["value"]["logs"] == [repair._LAUNCH_SENTINEL]
    assert getattr(plane, "_roi_raw_receipt_dispatch_received") == 1
    assert getattr(plane, "_roi_raw_receipt_dispatch_launch_received") == 1


def test_governed_timing_and_continuity_thresholds_are_unchanged():
    assert LaunchFundingPolicy().max_pair_stream_lag_seconds == 3.0
    assert LaunchFundingPolicy().launch_window_seconds == 8.0
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert repair.RAW_RECEIPT_QUEUE_MAX == 4096
