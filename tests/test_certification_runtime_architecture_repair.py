from __future__ import annotations

import asyncio
from collections import deque
from types import SimpleNamespace

import pytest

from solana_roi import certification_runtime_architecture_repair as repair
from solana_roi import continuity_high_volume_checkpoint_architecture as checkpoint
from solana_roi import continuity_immediate_recovery_repair as immediate
from solana_roi import continuity_recovery_isolation_repair as isolation
from solana_roi import continuity_target_frontier_repair as frontier
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi import production_capacity_repair as capacity
from solana_roi.direct_solana import WatchTarget
from solana_roi.solana_rpc import RpcEndpoint


def test_scout_healthy_websocket_uses_same_confirmed_frontier_checkpoint(monkeypatch):
    target = WatchTarget(kind="scout", address="scout-wallet", source_hint="SCOUT")
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace(
        _roi_target_ws_frontiers={
            key: deque(
                [
                    {
                        "signature": "scout-sig-500",
                        "slot": 500,
                        "provider": "publicnode",
                        "observed_monotonic": 10.0,
                    }
                ],
                maxlen=16,
            )
        }
    )
    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda _self, _target: True)
    monkeypatch.setattr(lease, "_current_ws_generation", lambda _self, _target: 4)

    async def confirmed(_self, _target, routine_cursor, generation):
        assert routine_cursor == 100
        assert generation == 4
        return 499, {
            "source": "confirmed-target-websocket-frontier",
            "generation": 4,
            "signature": "scout-sig-500",
            "confirmed_frontier_slot": 500,
            "effective_cursor_slot": 499,
            "confirmation_provider": "publicnode",
            "confirmation_latency_ms": 5.0,
            "same_slot_replay_required": True,
        }

    async def fallback(*_args, **_kwargs):
        raise AssertionError("healthy scout frontier should not replay stale history")

    monkeypatch.setattr(frontier, "_confirmed_target_frontier_cursor", confirmed)
    monkeypatch.setattr(checkpoint, "_ORIGINAL_SLOT_FETCH", fallback)

    rows, complete, provider, latency = asyncio.run(
        repair._universal_checkpointed_slot_fetch_delta(plane, target, 100)
    )
    assert complete is True
    assert provider == "publicnode"
    assert latency == 5.0
    assert rows[0]["slot"] == 499
    assert rows[0]["signature"] == ""
    assert checkpoint._checkpoint_counts(plane)[key] == 1
    assert checkpoint._last_checkpoints(plane)[key]["universal_frozen_target_checkpoint"] is True


def test_universal_checkpoint_still_falls_back_when_websocket_not_authoritative(monkeypatch):
    target = WatchTarget(kind="scout", address="scout-wallet", source_hint="SCOUT")
    plane = SimpleNamespace()
    calls: list[int] = []

    async def fallback(_self, _target, cursor):
        calls.append(cursor)
        return [], True, "publicnode", 3.0

    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda _self, _target: False)
    monkeypatch.setattr(checkpoint, "_ORIGINAL_SLOT_FETCH", fallback)
    result = asyncio.run(repair._universal_checkpointed_slot_fetch_delta(plane, target, 77))
    assert calls == [77]
    assert result == ([], True, "publicnode", 3.0)


def test_generation_upper_boundary_is_used_as_exclusive_before_signature(monkeypatch):
    target = WatchTarget(kind="program", address="pump", source_hint="PUMP_FUN")
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace(
        _roi_gap_recovery_upper_boundaries={
            key: {
                "generation": 9,
                "signature": "first-post-gap",
                "slot": 500,
                "source": "first-successfully-recorded-post-gap-websocket-receipt",
            }
        }
    )
    monkeypatch.setattr(immediate, "_generation", lambda _self, _target: 9)
    calls: list[dict] = []

    class Rpc:
        async def call_with_meta(self, method, params, *, hedge=False):
            assert method == "getSignaturesForAddress"
            assert hedge is True
            calls.append(dict(params[1]))
            return [
                {"signature": "gap-row", "slot": 101, "err": None},
                {"signature": "floor-row", "slot": 100, "err": None},
            ], "publicnode", 4.0

    monkeypatch.setattr(isolation, "_recovery_rpc", lambda _self: Rpc())
    rows, complete, provider, latency, meta = asyncio.run(
        repair._interval_bounded_gap_fetch_delta(plane, target, 100)
    )
    assert complete is True
    assert provider == "publicnode"
    assert latency == 4.0
    assert calls[0]["before"] == "first-post-gap"
    assert [row["signature"] for row in rows] == ["gap-row"]
    assert meta["generation_upper_boundary_applied"] is True
    assert meta["generation_upper_boundary_slot"] == 500
    assert meta["hard_page_limit"] == 3
    assert meta["hard_page_size"] == 1000


def test_first_successfully_recorded_post_gap_receipt_freezes_upper_boundary(monkeypatch):
    target = WatchTarget(kind="scout", address="scout-wallet", source_hint="SCOUT")
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace(_roi_immediate_gap_recovery_tasks={key: {"generation": 3}})
    monkeypatch.setattr(immediate, "_generation", lambda _self, _target: 3)

    async def original(_self, _provider, _targets, _message):
        return None

    monkeypatch.setattr(repair, "_ORIGINAL_NOTIFICATION", original)
    message = {
        "method": "logsNotification",
        "params": {
            "subscription": 1,
            "result": {
                "context": {"slot": 700},
                "value": {"signature": "post-gap-live", "err": None, "logs": []},
            },
        },
    }
    asyncio.run(
        repair._notification_with_recovery_upper_boundary(
            plane, "publicnode", {1: target}, message
        )
    )
    boundary = repair._recovery_upper_boundaries(plane)[key]
    assert boundary["generation"] == 3
    assert boundary["signature"] == "post-gap-live"
    assert boundary["slot"] == 700
    assert boundary["exclusive_before_signature"] is True


def test_recovery_task_done_callback_retrieves_failure_without_erasing_task(monkeypatch):
    target = WatchTarget(kind="program", address="pump", source_hint="PUMP_FUN")
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace(_roi_immediate_gap_recovery_tasks={})

    async def failing():
        raise RuntimeError("expected fail-closed recovery")

    def original(_self, _target, generation):
        task = asyncio.create_task(failing())
        immediate._recovery_tasks(_self)[key] = {
            "generation": generation,
            "cursor_slot": 100,
            "task": task,
        }

    monkeypatch.setattr(repair, "_ORIGINAL_KICK", original)

    async def exercise():
        repair._managed_recovery_kick(plane, target, 2)
        task = immediate._recovery_tasks(plane)[key]["task"]
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert task.done()
        assert isinstance(task.exception(), RuntimeError)
        return task

    task = asyncio.run(exercise())
    assert immediate._recovery_tasks(plane)[key]["task"] is task
    assert plane._roi_owned_recovery_task_outcomes == 1
    assert plane._roi_owned_recovery_last_outcome["outcome"] == "failed"
    assert not repair._ACTIVE_RECOVERY_TASKS


def test_background_historical_work_skips_cycle_during_continuity_pressure(monkeypatch):
    discovery = SimpleNamespace()
    monkeypatch.setattr(repair, "_research_pressure_reason", lambda: "continuity_recovery_active")

    async def should_not_run(_self):
        raise AssertionError("historical research must yield to continuity")

    monkeypatch.setattr(repair, "_ORIGINAL_DISCOVER_RAW", should_not_run)
    monkeypatch.setattr(repair, "_ORIGINAL_SCREEN_ONE", should_not_run)
    assert asyncio.run(repair._discover_raw_with_cpu_backpressure(discovery)) == 0
    assert asyncio.run(repair._screen_one_with_cpu_backpressure(discovery)) is False
    assert discovery._roi_cpu_backpressure_broad_skips == 1
    assert discovery._roi_cpu_backpressure_screen_skips == 1


def test_safe_hedge_bypasses_known_cooling_endpoint_before_task_creation(monkeypatch):
    healthy = RpcEndpoint("healthy", "https://healthy.example", "wss://healthy.example")
    cooling = RpcEndpoint("cooling", "https://cooling.example", "wss://cooling.example")
    calls: list[str] = []

    class Pool:
        endpoints = (healthy, cooling)
        hedge_delay_seconds = 0.01

        def _ordered(self, _method):
            return [healthy, cooling]

        async def _call_endpoint(self, endpoint, _method, _params):
            calls.append(endpoint.name)
            return {"ok": True}, endpoint.name, 1.0

    pool = Pool()
    monkeypatch.setattr(
        capacity,
        "_cooldown_remaining",
        lambda _pool, endpoint, **_kwargs: 120.0 if endpoint.name == "cooling" else 0.0,
    )
    monkeypatch.setattr(capacity, "_official_pair_requires_sequential_fallback", lambda _pool: False)

    async def original(*_args, **_kwargs):
        raise AssertionError("safe hedged path should own cooling filtering")

    monkeypatch.setattr(repair, "_ORIGINAL_RPC_CALL_WITH_META", original)
    result = asyncio.run(repair._safe_hedged_call_with_meta(pool, "getSlot", [], hedge=True))
    assert result[1] == "healthy"
    assert calls == ["healthy"]
    assert pool._roi_cooling_endpoints_bypassed == 1


def test_certification_boundaries_remain_unchanged():
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000
