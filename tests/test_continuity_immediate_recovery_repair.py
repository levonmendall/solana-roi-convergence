from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import continuity_durability_repair as durability
from solana_roi import continuity_immediate_recovery_repair as immediate
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi.direct_solana import WatchTarget


def _target() -> WatchTarget:
    return WatchTarget(kind="program", address="program-a", source_hint="PUMP_FUN")


def test_immediate_recovery_retries_inside_same_fixed_lease(monkeypatch):
    target = _target()
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace(_roi_real_ws_gap_generations={key: 1})
    calls = 0

    async def fake_fetch(_plane, _target, _cursor_slot):
        nonlocal calls
        calls += 1
        if calls == 1:
            return [], False, "publicnode", 10.0
        return [{"signature": "sig", "slot": 101}], True, "publicnode", 10.0

    monkeypatch.setattr(durability, "_hedged_gap_fetch_delta", fake_fetch)

    rows, complete, provider, _latency = asyncio.run(
        immediate._recover_until_lease_boundary(plane, target, 100, 1)
    )

    assert complete is True
    assert provider == "publicnode"
    assert rows[0]["slot"] == 101
    assert calls == 2
    assert getattr(plane, "_roi_immediate_gap_recovery_retries") == 1
    assert getattr(plane, "_roi_immediate_gap_recovery_completed_after_retry") == 1
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000


def test_proxy_consumes_recovery_started_at_gap_instead_of_starting_late_read():
    target = _target()
    key = live_poll._poll_target_key(target)

    class Base:
        async def _slot_fetch_delta(self, *_args):
            raise AssertionError("base fetch should not run when immediate recovery is ready")

    async def scenario():
        plane = SimpleNamespace(_roi_real_ws_gap_generations={key: 1})
        lease._runtime(plane)[key] = {"cursor_ws_generation": 0}

        async def ready():
            return ([{"signature": "sig", "slot": 101}], True, "publicnode", 5.0)

        task = asyncio.create_task(ready())
        immediate._recovery_tasks(plane)[key] = {
            "generation": 1,
            "cursor_slot": 100,
            "task": task,
            "started_monotonic": 1.0,
        }
        proxy = immediate._ImmediateRecoveryProxy(Base())
        result = await proxy._slot_fetch_delta(plane, target, 100)
        return plane, result

    plane, result = asyncio.run(scenario())

    assert result[1] is True
    assert result[0][0]["slot"] == 101
    assert getattr(plane, "_roi_immediate_gap_recovery_consumed") == 1


def test_failed_immediate_recovery_stays_fail_closed(monkeypatch):
    target = _target()
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace(_roi_real_ws_gap_generations={key: 1})

    async def fail_fetch(_plane, _target, _cursor_slot):
        raise RuntimeError("provider failure")

    times = iter([0.0, 13.0, 13.0])
    monkeypatch.setattr(durability, "_hedged_gap_fetch_delta", fail_fetch)
    monkeypatch.setattr(immediate.time, "monotonic", lambda: next(times, 13.0))

    try:
        asyncio.run(immediate._recover_until_lease_boundary(plane, target, 100, 1))
    except RuntimeError as exc:
        assert "provider failure" in str(exc)
    else:
        raise AssertionError("expired recovery must fail closed")

    assert getattr(plane, "_roi_immediate_gap_recovery_failed") == 1
