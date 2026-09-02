from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi import poll_watermark_repair as watermark
from solana_roi import target_quorum
from solana_roi.direct_solana import DirectSolanaIngestionPlane, WatchTarget
from solana_roi.solana_rpc import RpcEndpoint


def test_real_websocket_zero_coverage_increments_target_generation(monkeypatch):
    target = WatchTarget("program", "program-a", "PUMP_FUN")
    coverage = iter([True, False])

    async def fake_original(*_args, **_kwargs):
        return None

    monkeypatch.setattr(lease, "_ORIGINAL_QUORUM_SET_TARGET_STATE", fake_original)
    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda _self, _target: next(coverage))
    plane = SimpleNamespace()
    endpoint = RpcEndpoint("publicnode", "https://example.com", "wss://example.com")

    asyncio.run(
        lease._tracked_quorum_set_target_state(
            plane, endpoint, target, connected=False, error_type="ConnectionClosedError"
        )
    )

    assert lease._ws_gap_generations(plane)[live_poll._poll_target_key(target)] == 1


def test_poll_provider_does_not_increment_real_websocket_generation(monkeypatch):
    target = WatchTarget("program", "program-a", "PUMP_FUN")

    async def fake_original(*_args, **_kwargs):
        return None

    monkeypatch.setattr(lease, "_ORIGINAL_QUORUM_SET_TARGET_STATE", fake_original)
    plane = SimpleNamespace()
    asyncio.run(
        lease._tracked_quorum_set_target_state(
            plane, live_poll._POLL_ENDPOINT, target, connected=False
        )
    )
    assert lease._ws_gap_generations(plane) == {}


def test_transient_poll_failure_stays_quorum_covered_inside_lease(monkeypatch):
    target = WatchTarget("program", "program-a", "PUMP_AMM")
    stop = asyncio.Event()
    quorum_calls: list[bool] = []

    async def fake_page(*_args, **_kwargs):
        return [{"signature": "baseline", "slot": 100}], "publicnode", 5.0

    async def fake_delta(*_args, **_kwargs):
        raise TimeoutError("temporary public RPC timeout")

    async def fake_quorum(*_args, connected, **_kwargs):
        quorum_calls.append(bool(connected))
        if len(quorum_calls) >= 2:
            stop.set()

    monkeypatch.setattr(watermark, "_slot_poll_page", fake_page)
    monkeypatch.setattr(watermark, "_slot_fetch_delta", fake_delta)
    monkeypatch.setattr(target_quorum, "_quorum_set_target_state", fake_quorum)
    monkeypatch.setattr(live_poll, "POLL_INTERVAL_SECONDS", 0.01)

    plane = SimpleNamespace()
    asyncio.run(lease._leased_poll_target(plane, target, stop))

    assert quorum_calls == [True, True]
    row = live_poll._poll_state(plane)[live_poll._poll_target_key(target)]
    assert row["connected"] is True
    assert row["degraded_recoverable"] is True
    assert row["recoverability_lease_seconds"] == lease.POLL_RECOVERABILITY_LEASE_SECONDS


def test_poll_failure_with_expired_lease_withdraws_quorum(monkeypatch):
    target = WatchTarget("program", "program-a", "PUMP_AMM")
    stop = asyncio.Event()
    quorum_calls: list[bool] = []

    async def fake_page(*_args, **_kwargs):
        return [{"signature": "baseline", "slot": 100}], "publicnode", 5.0

    async def fake_delta(*_args, **_kwargs):
        raise TimeoutError("persistent public RPC timeout")

    async def fake_quorum(*_args, connected, **_kwargs):
        quorum_calls.append(bool(connected))
        if len(quorum_calls) >= 2:
            stop.set()

    monkeypatch.setattr(watermark, "_slot_poll_page", fake_page)
    monkeypatch.setattr(watermark, "_slot_fetch_delta", fake_delta)
    monkeypatch.setattr(target_quorum, "_quorum_set_target_state", fake_quorum)
    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda *_args: True)
    monkeypatch.setattr(live_poll, "POLL_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(lease, "POLL_RECOVERABILITY_LEASE_SECONDS", 0.0)

    plane = SimpleNamespace()
    asyncio.run(lease._leased_poll_target(plane, target, stop))

    assert quorum_calls == [True, False]
    row = live_poll._poll_state(plane)[live_poll._poll_target_key(target)]
    assert row["connected"] is False
    assert row["last_error_type"] == "LivePollFreshnessLeaseExpired"


def test_recovery_started_inside_lease_keeps_coverage_when_rpc_finishes_late(monkeypatch):
    target = WatchTarget("program", "program-a", "PUMP_AMM")
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
        clock.value = 1.0
        return [{"signature": "baseline", "slot": 100}], "publicnode", 5.0

    async def slow_failing_delta(*_args, **_kwargs):
        # The bounded attempt starts eight seconds after the last success but its
        # timeout/failover completes eight seconds later, beyond the 12s lease.
        clock.value = 17.0
        raise TimeoutError("slow public RPC timeout/failover")

    async def fake_quorum(*_args, connected, **_kwargs):
        quorum_calls.append(bool(connected))
        if len(quorum_calls) == 1:
            clock.value = 9.0
        else:
            stop.set()

    monkeypatch.setattr(lease, "_monotonic", lambda: clock.value)
    monkeypatch.setattr(watermark, "_slot_poll_page", fake_page)
    monkeypatch.setattr(watermark, "_slot_fetch_delta", slow_failing_delta)
    monkeypatch.setattr(target_quorum, "_quorum_set_target_state", fake_quorum)
    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda *_args: True)
    monkeypatch.setattr(live_poll, "POLL_INTERVAL_SECONDS", 0.01)

    plane = SimpleNamespace(journal=Journal())
    asyncio.run(lease._leased_poll_target(plane, target, stop))

    assert quorum_calls == [True, True]
    assert journal_calls == []
    row = live_poll._poll_state(plane)[live_poll._poll_target_key(target)]
    assert row["connected"] is True
    assert row["degraded_recoverable"] is True
    assert row["lease_age_seconds"] == 16.0
    assert row["attempt_started_lease_age_seconds"] == 8.0
    assert row["inflight_attempt_grace"] is True
    assert row["last_error_type"] == "TimeoutError"


def test_recorded_websocket_gap_rearms_only_future_poll_standby(monkeypatch):
    target = WatchTarget("program", "program-a", "PUMP_AMM")
    key = live_poll._poll_target_key(target)
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
        raise TimeoutError("poll recovery failed after real websocket loss")

    async def fake_rearm(*_args, **_kwargs):
        return 250, "solana-mainnet", 7.0

    async def fake_quorum(plane, *_args, connected, **_kwargs):
        quorum_calls.append(bool(connected))
        if len(quorum_calls) == 1:
            # A real zero-coverage transition occurred and WebSocket coverage has
            # now returned before the expired poll recovery is classified.
            lease._ws_gap_generations(plane)[key] = 1
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
    assert journal_calls[0][0] == "mark"
    assert journal_calls[1] == (
        "close",
        (False, lease.IRRECOVERABLE_POLL_GAP_ERROR),
    )
    assert len(journal_calls) == 2
    row = live_poll._poll_state(plane)[key]
    assert row["connected"] is True
    assert row["cursor_slot"] == 250
    assert row["ws_gap_generation_at_cursor"] == 1
    assert row["recorded_gap_standby_rearmed"] is True
    assert plane._roi_poll_recoverability_runtime[key][
        "latched_irrecoverable_ws_generation"
    ] == 1


def test_standby_rearm_rejects_websocket_generation_change_in_flight(monkeypatch):
    target = WatchTarget("program", "program-a", "PUMP_AMM")
    generations = iter([0, 1])

    async def fake_rearm(*_args, **_kwargs):
        return 250, "solana-mainnet", 7.0

    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda *_args: True)
    monkeypatch.setattr(
        lease,
        "_current_ws_generation",
        lambda *_args: next(generations),
    )
    monkeypatch.setattr(lease.standby, "_try_rearm_under_websocket", fake_rearm)

    result = asyncio.run(
        lease._try_rearm_with_stable_websocket(
            SimpleNamespace(), target, 100, expected_generation=0
        )
    )
    assert result is None


def test_irrecoverable_poll_interval_latches_release_gap():
    calls: list[tuple[str, object]] = []

    class Journal:
        def mark_outage(self, started_at):
            calls.append(("mark", started_at))

        def close_outage(self, *, complete, error):
            calls.append(("close", (complete, error)))

    lease._latch_irrecoverable_gap(SimpleNamespace(journal=Journal()))
    assert calls[0][0] == "mark"
    assert calls[1] == ("close", (False, lease.IRRECOVERABLE_POLL_GAP_ERROR))


def test_poll_recoverability_lease_installed_intrinsically():
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll._poll_target is lease._leased_poll_target
    assert target_quorum._quorum_set_target_state is lease._tracked_quorum_set_target_state
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_poll_recoverability_lease", False))
