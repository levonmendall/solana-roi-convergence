from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi import poll_watermark_repair as watermark
from solana_roi import target_quorum
from solana_roi.direct_solana import WatchTarget


def test_expired_scout_poll_standby_rearms_under_same_stable_websocket(monkeypatch):
    """A poll-only failure must not strand a strategy scout at 2/3 coverage.

    The real WebSocket remained continuously available and its generation did not
    change, so no prospective evidence interval was lost. Re-baselining the
    redundant poll standby is therefore safe. This must not mark or close a
    strategy outage, and it must not manufacture recovery of a real recorded gap.
    """

    target = WatchTarget("scout", "scout-a", "SCOUT_A")
    stop = asyncio.Event()
    quorum_calls: list[bool] = []
    journal_calls: list[tuple[str, object]] = []
    clock = SimpleNamespace(value=0.0)

    class Journal:
        def mark_outage(self, started_at):
            journal_calls.append(("mark", started_at))

        def close_outage(self, *, complete, error):
            journal_calls.append(("close", (complete, error)))

    async def fake_page(*_args, **_kwargs):
        return [{"signature": "baseline", "slot": 100}], "publicnode", 5.0

    async def failing_delta(*_args, **_kwargs):
        clock.value = 2.0
        raise TimeoutError("poll-only provider timeout while websocket remains healthy")

    async def fake_rearm(*_args, **_kwargs):
        return 250, "solana-mainnet", 7.0

    async def fake_quorum(*_args, connected, **_kwargs):
        quorum_calls.append(bool(connected))
        if len(quorum_calls) == 1:
            clock.value = 1.0
        else:
            stop.set()

    monkeypatch.setattr(lease, "_monotonic", lambda: clock.value)
    monkeypatch.setattr(watermark, "_slot_poll_page", fake_page)
    monkeypatch.setattr(watermark, "_slot_fetch_delta", failing_delta)
    monkeypatch.setattr(lease.standby, "_try_rearm_under_websocket", fake_rearm)
    monkeypatch.setattr(target_quorum, "_quorum_set_target_state", fake_quorum)
    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda *_args: True)
    monkeypatch.setattr(live_poll, "POLL_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(lease, "POLL_RECOVERABILITY_LEASE_SECONDS", 0.0)

    plane = SimpleNamespace(journal=Journal())
    asyncio.run(lease._leased_poll_target(plane, target, stop))

    assert quorum_calls == [True, True]
    assert journal_calls == []
    row = live_poll._poll_state(plane)[live_poll._poll_target_key(target)]
    assert row["connected"] is True
    assert row["cursor_slot"] == 250
    assert row["ws_gap_generation_at_cursor"] == 0
    assert row["overflow_rearmed_under_websocket"] is True
    assert row["recorded_gap_standby_rearmed"] is False


def test_expired_scout_poll_still_fails_closed_when_stable_rearm_fails(monkeypatch):
    target = WatchTarget("scout", "scout-a", "SCOUT_A")
    stop = asyncio.Event()
    quorum_calls: list[bool] = []
    clock = SimpleNamespace(value=0.0)

    async def fake_page(*_args, **_kwargs):
        return [{"signature": "baseline", "slot": 100}], "publicnode", 5.0

    async def failing_delta(*_args, **_kwargs):
        clock.value = 2.0
        raise TimeoutError("persistent poll failure")

    async def failed_rearm(*_args, **_kwargs):
        return None

    async def fake_quorum(*_args, connected, **_kwargs):
        quorum_calls.append(bool(connected))
        if len(quorum_calls) == 1:
            clock.value = 1.0
        else:
            stop.set()

    monkeypatch.setattr(lease, "_monotonic", lambda: clock.value)
    monkeypatch.setattr(watermark, "_slot_poll_page", fake_page)
    monkeypatch.setattr(watermark, "_slot_fetch_delta", failing_delta)
    monkeypatch.setattr(lease.standby, "_try_rearm_under_websocket", failed_rearm)
    monkeypatch.setattr(target_quorum, "_quorum_set_target_state", fake_quorum)
    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda *_args: True)
    monkeypatch.setattr(live_poll, "POLL_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(lease, "POLL_RECOVERABILITY_LEASE_SECONDS", 0.0)

    plane = SimpleNamespace(journal=SimpleNamespace())
    asyncio.run(lease._leased_poll_target(plane, target, stop))

    assert quorum_calls == [True, False]
    row = live_poll._poll_state(plane)[live_poll._poll_target_key(target)]
    assert row["connected"] is False
    assert row["last_error_type"] == "LivePollFreshnessLeaseExpired"
