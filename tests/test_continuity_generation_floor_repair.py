from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import continuity_generation_floor_repair as repair
from solana_roi import continuity_immediate_recovery_repair as immediate
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi.direct_solana import WatchTarget


def _target() -> WatchTarget:
    return WatchTarget(kind="program", address="pump-amm", source_hint="PUMP_AMM")


def test_confirmed_snapshot_records_same_slot_replay_safe_generation_floor(monkeypatch):
    target = _target()
    plane = SimpleNamespace()

    async def confirmed(_self, _target, routine_cursor, generation, _candidates):
        assert routine_cursor == 317
        assert generation == 4
        return 760, {
            "source": "confirmed-target-websocket-frontier-at-gap",
            "generation": 4,
            "confirmed_frontier_slot": 761,
            "effective_cursor_slot": 760,
            "same_slot_replay_required": True,
        }

    monkeypatch.setattr(repair, "_ORIGINAL_CONFIRMED_SNAPSHOT", confirmed)
    effective, anchor = asyncio.run(
        repair._confirmed_snapshot_with_generation_floor(
            plane,
            target,
            317,
            4,
            [{"signature": "sig-761", "slot": 761}],
        )
    )

    assert effective == 760
    assert anchor is not None
    assert repair._confirmed_floor(plane, target, 4) == 760
    assert repair._confirmed_floor(plane, target, 3) is None
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000


def test_every_isolated_recovery_uses_confirmed_generation_floor(monkeypatch):
    target = _target()
    plane = SimpleNamespace()
    repair._remember_confirmed_floor(
        plane,
        target,
        4,
        760,
        {
            "source": "confirmed-target-websocket-frontier-at-gap",
            "confirmed_frontier_slot": 761,
            "same_slot_replay_required": True,
        },
    )
    called = []

    async def isolated(_self, _target, cursor, generation):
        called.append((cursor, generation))
        return ([{"signature": "sig-762", "slot": 762}], True, "publicnode", 10.0)

    monkeypatch.setattr(repair, "_ORIGINAL_ISOLATED_RECOVERY", isolated)
    result = asyncio.run(
        repair._isolated_recovery_with_generation_floor(plane, target, 499, 4)
    )

    assert called == [(760, 4)]
    assert result[1] is True
    assert plane._roi_generation_floor_stale_cursor_advances == 1
    assert plane._roi_generation_floor_last_advance["requested_cursor_slot"] == 499
    assert plane._roi_generation_floor_last_advance["effective_cursor_slot"] == 760


def test_same_generation_gap_task_is_consumed_even_if_routine_cursor_moved(monkeypatch):
    target = _target()
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace(
        _roi_real_ws_gap_generations={key: 4},
        _roi_poll_recoverability_runtime={key: {"cursor_ws_generation": 3}},
    )
    fallback_called = []

    async def fallback(_proxy, _plane, _target, cursor):
        fallback_called.append(cursor)
        raise AssertionError("same-generation snapshot recovery must remain authoritative")

    monkeypatch.setattr(repair, "_ORIGINAL_PROXY_FETCH", fallback)

    async def scenario():
        task = asyncio.create_task(
            asyncio.sleep(
                0,
                result=([{"signature": "sig-762", "slot": 762}], True, "publicnode", 11.0),
            )
        )
        immediate._recovery_tasks(plane)[key] = {
            "generation": 4,
            "cursor_slot": 317,
            "task": task,
        }
        return await repair._proxy_fetch_with_generation_task_authority(
            object(), plane, target, 499
        )

    result = asyncio.run(scenario())

    assert result[1] is True
    assert fallback_called == []
    assert key not in immediate._recovery_tasks(plane)
    assert plane._roi_generation_floor_cursor_mismatch_tasks_consumed == 1
    assert plane._roi_generation_floor_last_task_cursor_mismatch == {
        "target": key,
        "generation": 4,
        "task_routine_cursor_slot": 317,
        "caller_cursor_slot": 499,
    }


def test_unconfirmed_frontier_never_creates_generation_floor():
    target = _target()
    plane = SimpleNamespace()
    repair._remember_confirmed_floor(
        plane,
        target,
        4,
        760,
        {
            "source": "routine-poll-slot-fallback",
            "confirmed_frontier_slot": 761,
            "same_slot_replay_required": False,
        },
    )
    assert repair._confirmed_floor(plane, target, 4) is None
