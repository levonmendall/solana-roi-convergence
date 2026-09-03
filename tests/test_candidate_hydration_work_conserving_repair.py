from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import candidate_hydration_work_conserving_repair as repair
from solana_roi import runtime_guards as guards


class Plane:
    worker_count = 12

    def __init__(self):
        self.journal = object()
        self.hydrated = []
        self.expired = 0

    async def _hydrate_one(self, row):
        self.hydrated.append(row)
        self.stop.set()


def test_worker_partition_preserves_three_candidate_and_three_background_reservations():
    plane = SimpleNamespace(worker_count=12)
    fast, background_reserved, flex = repair._worker_counts(plane)
    assert fast == 3
    assert background_reserved == 3
    assert flex == 6
    assert fast + background_reserved + flex == 12
    assert guards.DIRECT_FAST_WORKER_SLOTS == 3


def test_flex_background_worker_claims_urgent_candidate_before_background(monkeypatch):
    plane = Plane()
    plane.stop = asyncio.Event()
    calls = []

    def claim(_journal, *, fast_only):
        calls.append(fast_only)
        if fast_only:
            return {"signature": "urgent", "priority": 0}
        return {"signature": "background", "priority": 10}

    monkeypatch.setattr(guards, "_claim_priority", claim)
    monkeypatch.setattr(guards, "_expire_stale_background", lambda _self: 0)

    async def scenario():
        task = asyncio.create_task(
            repair._work_conserving_reserved_worker(plane, plane.stop, fast_only=False),
            name="direct-solana-background:5",
        )
        await task

    asyncio.run(scenario())

    assert calls == [True]
    assert plane.hydrated == [{"signature": "urgent", "priority": 0}]
    assert getattr(plane, "_roi_candidate_hydration_flex_flex_candidate_claims") == 1


def test_reserved_background_worker_never_lends_its_capacity(monkeypatch):
    plane = Plane()
    plane.stop = asyncio.Event()
    calls = []

    def claim(_journal, *, fast_only):
        calls.append(fast_only)
        return {"signature": "background", "priority": 10}

    monkeypatch.setattr(guards, "_claim_priority", claim)
    monkeypatch.setattr(guards, "_expire_stale_background", lambda _self: 0)

    async def scenario():
        task = asyncio.create_task(
            repair._work_conserving_reserved_worker(plane, plane.stop, fast_only=False),
            name="direct-solana-background:1",
        )
        await task

    asyncio.run(scenario())

    assert calls == [False]
    assert plane.hydrated == [{"signature": "background", "priority": 10}]
    assert getattr(plane, "_roi_candidate_hydration_flex_background_claims") == 1


def test_flex_worker_falls_back_to_background_when_no_urgent_row(monkeypatch):
    plane = Plane()
    plane.stop = asyncio.Event()
    calls = []

    def claim(_journal, *, fast_only):
        calls.append(fast_only)
        if fast_only:
            return None
        return {"signature": "background", "priority": 20}

    monkeypatch.setattr(guards, "_claim_priority", claim)
    monkeypatch.setattr(guards, "_expire_stale_background", lambda _self: 0)

    async def scenario():
        task = asyncio.create_task(
            repair._work_conserving_reserved_worker(plane, plane.stop, fast_only=False),
            name="direct-solana-background:7",
        )
        await task

    asyncio.run(scenario())

    assert calls == [True, False]
    assert plane.hydrated == [{"signature": "background", "priority": 20}]


def test_candidate_reserved_worker_remains_candidate_only(monkeypatch):
    plane = Plane()
    plane.stop = asyncio.Event()
    calls = []

    def claim(_journal, *, fast_only):
        calls.append(fast_only)
        return {"signature": "candidate", "priority": 0}

    monkeypatch.setattr(guards, "_claim_priority", claim)

    async def scenario():
        task = asyncio.create_task(
            repair._work_conserving_reserved_worker(plane, plane.stop, fast_only=True),
            name="direct-solana-fast:0",
        )
        await task

    asyncio.run(scenario())

    assert calls == [True]
    assert plane.hydrated == [{"signature": "candidate", "priority": 0}]
    assert getattr(plane, "_roi_candidate_hydration_flex_reserved_candidate_claims") == 1
