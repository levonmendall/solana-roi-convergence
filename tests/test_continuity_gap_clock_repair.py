from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import continuity_gap_clock_repair as repair
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi.direct_solana import WatchTarget


def test_real_gap_lease_age_starts_at_gap_not_previous_poll_success():
    target = WatchTarget("program", "program-a", "PUMP_FUN")
    plane = SimpleNamespace()
    key = live_poll._poll_target_key(target)
    lease._ws_gap_generations(plane)[key] = 2
    repair._gap_clocks(plane)[key] = {
        "generation": 2,
        "started_monotonic": 100.0,
        "resolved_generation": None,
    }

    age, attempt_age, source = repair._lease_ages(
        plane,
        target,
        cursor_generation=1,
        last_success_monotonic=80.0,
        attempt_started_monotonic=103.0,
        now_monotonic=105.0,
    )

    assert age == 5.0
    assert attempt_age == 3.0
    assert source == "real_websocket_gap_onset"
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0


def test_gap_generation_records_monotonic_origin_and_wakes_recovery(monkeypatch):
    target = WatchTarget("scout", "wallet-a", None)
    plane = SimpleNamespace()
    endpoint = SimpleNamespace(name="publicnode")
    key = live_poll._poll_target_key(target)
    lease._ws_gap_generations(plane)[key] = 0

    async def previous(self, _endpoint, _target, **_kwargs):
        lease._ws_gap_generations(self)[key] = 1

    monkeypatch.setattr(repair, "_PREVIOUS_SET_TARGET_STATE", previous)
    monkeypatch.setattr(repair.time, "monotonic", lambda: 123.5)

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
    assert row["resolved_generation"] is None
    assert repair._wake_for(plane, target).is_set() is True


def test_resolved_gap_clock_no_longer_rebases_lease_age():
    target = WatchTarget("program", "program-b", "RAYDIUM")
    plane = SimpleNamespace()
    key = live_poll._poll_target_key(target)
    lease._ws_gap_generations(plane)[key] = 3
    repair._gap_clocks(plane)[key] = {
        "generation": 3,
        "started_monotonic": 100.0,
        "resolved_generation": 3,
    }

    age, attempt_age, source = repair._lease_ages(
        plane,
        target,
        cursor_generation=2,
        last_success_monotonic=110.0,
        attempt_started_monotonic=112.0,
        now_monotonic=114.0,
    )

    assert age == 4.0
    assert attempt_age == 2.0
    assert source == "last_successful_poll"
