from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import continuity_gap_clock_repair as repair
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi.direct_solana import WatchTarget


def test_real_gap_clock_rebases_existing_lease_age_to_gap_onset(monkeypatch):
    target = WatchTarget("program", "program-a", "PUMP_FUN")
    plane = SimpleNamespace()
    key = live_poll._poll_target_key(target)
    lease._ws_gap_generations(plane)[key] = 2
    lease._runtime(plane)[key] = {
        "cursor_ws_generation": 1,
        "last_success_monotonic": 80.0,
    }
    repair._gap_clocks(plane)[key] = {
        "generation": 2,
        "started_monotonic": 100.0,
    }
    monkeypatch.setattr(repair, "_ORIGINAL_MONOTONIC", lambda: 105.0)
    token = repair._POLL_CONTEXT.set((plane, target))
    try:
        virtual_now = repair._gap_aware_monotonic()
    finally:
        repair._POLL_CONTEXT.reset(token)

    # The unchanged worker subtracts its prior success (80). The repaired clock
    # returns 85, so the lease age is the actual five seconds since the gap, not
    # the twenty-five seconds since the older poll success.
    assert virtual_now == 85.0
    assert virtual_now - 80.0 == 5.0
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0


def test_gap_generation_records_actual_monotonic_origin(monkeypatch):
    target = WatchTarget("scout", "wallet-a", None)
    plane = SimpleNamespace()
    endpoint = SimpleNamespace(name="publicnode")
    key = live_poll._poll_target_key(target)
    lease._ws_gap_generations(plane)[key] = 0

    async def previous(self, _endpoint, _target, **_kwargs):
        lease._ws_gap_generations(self)[key] = 1

    monkeypatch.setattr(repair, "_PREVIOUS_SET_TARGET_STATE", previous)
    monkeypatch.setattr(repair, "_ORIGINAL_MONOTONIC", lambda: 123.5)

    asyncio.run(
        repair._gap_clock_set_target_state(
            plane,
            endpoint,
            target,
            connected=False,
            error_type="ConnectionClosedError",
        )
    )

    row = repair._gap_clocks(plane)[key]
    assert row["generation"] == 1
    assert row["started_monotonic"] == 123.5
    assert row["started_at"]


def test_no_active_gap_uses_original_clock_and_keeps_canonical_worker(monkeypatch):
    target = WatchTarget("program", "program-b", "RAYDIUM")
    plane = SimpleNamespace()
    key = live_poll._poll_target_key(target)
    lease._ws_gap_generations(plane)[key] = 3
    lease._runtime(plane)[key] = {
        "cursor_ws_generation": 3,
        "last_success_monotonic": 110.0,
    }
    monkeypatch.setattr(repair, "_ORIGINAL_MONOTONIC", lambda: 114.0)
    token = repair._POLL_CONTEXT.set((plane, target))
    try:
        assert repair._gap_aware_monotonic() == 114.0
    finally:
        repair._POLL_CONTEXT.reset(token)

    assert live_poll._poll_target is lease._leased_poll_target
    assert target.__class__ is WatchTarget
