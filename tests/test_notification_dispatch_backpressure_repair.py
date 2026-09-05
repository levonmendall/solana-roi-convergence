from __future__ import annotations

import asyncio

import pytest

from solana_roi.direct_solana import WatchTarget
from solana_roi.handshake_pump import MAX_INFLIGHT_NOTIFICATION_HANDLERS
from solana_roi.notification_dispatch_backpressure_repair import (
    _bounded_drain_dispatch_capacity,
    _strategy_critical_target_shards,
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


def test_strategy_scouts_are_isolated_from_program_firehose_without_extra_sockets() -> None:
    targets = tuple(
        [WatchTarget("scout", f"scout-{index}", None) for index in range(3)]
        + [WatchTarget("program", f"program-{index}", "PUMP_FUN") for index in range(7)]
    )
    shards = _strategy_critical_target_shards(targets, "publicnode", 4)

    assert len(shards) == 3
    assert sorted(target.address for shard in shards for target in shard) == sorted(
        target.address for target in targets
    )
    assert any(shard and all(target.kind == "scout" for target in shard) for shard in shards)
    assert all(
        not ({target.kind for target in shard} == {"scout", "program"})
        for shard in shards
    )


def test_install_replaces_public_shard_capacity_and_shard_policy_without_changing_bound() -> None:
    expected_bound = MAX_INFLIGHT_NOTIFICATION_HANDLERS
    install_notification_dispatch_backpressure_repair()
    assert public_shards._cooperative_dispatch_capacity is _bounded_drain_dispatch_capacity
    assert public_shards._target_shards is _strategy_critical_target_shards
    assert MAX_INFLIGHT_NOTIFICATION_HANDLERS == expected_bound == 32
    assert bool(getattr(public_shards._cooperative_dispatch_capacity, "_roi_bounded_drain", False))
    assert bool(getattr(public_shards._target_shards, "_roi_strategy_critical_isolated", False))
