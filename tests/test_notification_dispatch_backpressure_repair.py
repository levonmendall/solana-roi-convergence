from __future__ import annotations

import asyncio

import pytest

from solana_roi.handshake_pump import MAX_INFLIGHT_NOTIFICATION_HANDLERS
from solana_roi.notification_dispatch_backpressure_repair import (
    _bounded_drain_dispatch_capacity,
    install_notification_dispatch_backpressure_repair,
)
from solana_roi import public_ws_shard_transport_repair as public_shards


def test_saturated_dispatch_waits_for_owned_handler_instead_of_raising() -> None:
    async def scenario() -> None:
        gate = asyncio.Event()

        async def blocked() -> None:
            await gate.wait()

        async def short() -> None:
            await asyncio.sleep(0.01)

        tasks = {
            asyncio.create_task(blocked())
            for _ in range(MAX_INFLIGHT_NOTIFICATION_HANDLERS - 1)
        }
        tasks.add(asyncio.create_task(short()))
        try:
            await asyncio.wait_for(_bounded_drain_dispatch_capacity(tasks), timeout=1.0)
            assert len(tasks) < MAX_INFLIGHT_NOTIFICATION_HANDLERS
            assert any(not task.done() for task in tasks)
        finally:
            gate.set()
            await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(scenario())


def test_real_handler_failure_still_propagates_during_capacity_drain() -> None:
    async def scenario() -> None:
        gate = asyncio.Event()

        async def blocked() -> None:
            await gate.wait()

        async def broken() -> None:
            await asyncio.sleep(0)
            raise RuntimeError("owned notification handler failed")

        tasks = {
            asyncio.create_task(blocked())
            for _ in range(MAX_INFLIGHT_NOTIFICATION_HANDLERS - 1)
        }
        tasks.add(asyncio.create_task(broken()))
        try:
            with pytest.raises(RuntimeError, match="owned notification handler failed"):
                await _bounded_drain_dispatch_capacity(tasks)
        finally:
            gate.set()
            await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(scenario())


def test_install_replaces_public_shard_capacity_policy_without_changing_bound() -> None:
    expected_bound = MAX_INFLIGHT_NOTIFICATION_HANDLERS
    install_notification_dispatch_backpressure_repair()
    assert public_shards._cooperative_dispatch_capacity is _bounded_drain_dispatch_capacity
    assert MAX_INFLIGHT_NOTIFICATION_HANDLERS == expected_bound == 32
    assert bool(getattr(public_shards._cooperative_dispatch_capacity, "_roi_bounded_drain", False))
