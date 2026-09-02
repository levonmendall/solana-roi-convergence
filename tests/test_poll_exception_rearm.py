from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_exception_rearm as repair
from solana_roi import poll_recoverability_lease as lease
from solana_roi import poll_standby_rearm as standby
from solana_roi import poll_watermark_repair as watermark
from solana_roi import target_quorum
from solana_roi.direct_solana import DirectSolanaIngestionPlane, WatchTarget


def test_delta_exception_routes_to_incomplete_only_under_continuous_websocket(monkeypatch):
    target = WatchTarget("program", "program-a", "PUMP_FUN")
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace(_roi_poll_recoverability_runtime={key: {"cursor_ws_generation": 0}})

    async def failing_delta(*_args, **_kwargs):
        raise TimeoutError("transient paginated read failure")

    monkeypatch.setattr(repair, "_ORIGINAL_FETCH_DELTA", failing_delta)
    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda *_args: True)
    monkeypatch.setattr(lease, "_current_ws_generation", lambda *_args: 0)

    result = asyncio.run(repair._exception_rearm_fetch_delta(plane, target, 100))
    assert result == ([], False, None, None)


def test_delta_exception_re_raises_after_real_websocket_gap_generation(monkeypatch):
    target = WatchTarget("program", "program-a", "PUMP_AMM")
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace(_roi_poll_recoverability_runtime={key: {"cursor_ws_generation": 0}})

    async def failing_delta(*_args, **_kwargs):
        raise TimeoutError("transient paginated read failure")

    monkeypatch.setattr(repair, "_ORIGINAL_FETCH_DELTA", failing_delta)
    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda *_args: True)
    monkeypatch.setattr(lease, "_current_ws_generation", lambda *_args: 1)

    with pytest.raises(TimeoutError):
        asyncio.run(repair._exception_rearm_fetch_delta(plane, target, 100))


def test_recoverable_delta_exception_uses_existing_same_target_standby_rearm(monkeypatch):
    target = WatchTarget("program", "program-a", "PUMP_FUN")
    stop = asyncio.Event()
    quorum_calls: list[bool] = []
    page_calls = 0

    async def fake_page(*_args, **_kwargs):
        nonlocal page_calls
        page_calls += 1
        slot = 100 if page_calls == 1 else 200
        return [{"signature": f"s{slot}", "slot": slot}], "publicnode", 5.0

    async def failing_delta(*_args, **_kwargs):
        raise TimeoutError("transient paginated read failure")

    async def fake_quorum(*_args, connected, **_kwargs):
        quorum_calls.append(bool(connected))
        if len(quorum_calls) >= 2:
            stop.set()

    monkeypatch.setattr(repair, "_ORIGINAL_FETCH_DELTA", failing_delta)
    monkeypatch.setattr(watermark, "_slot_fetch_delta", repair._exception_rearm_fetch_delta)
    monkeypatch.setattr(watermark, "_slot_poll_page", fake_page)
    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda *_args: True)
    monkeypatch.setattr(target_quorum, "_quorum_set_target_state", fake_quorum)
    monkeypatch.setattr(live_poll, "POLL_INTERVAL_SECONDS", 0.01)

    plane = SimpleNamespace()
    asyncio.run(lease._leased_poll_target(plane, target, stop))

    assert quorum_calls == [True, True]
    row = live_poll._poll_state(plane)[live_poll._poll_target_key(target)]
    assert row["connected"] is True
    assert row["cursor_slot"] == 200
    assert row["overflow_rearmed_under_websocket"] is True


def test_poll_exception_rearm_installed_intrinsically():
    assert watermark._slot_fetch_delta is repair._exception_rearm_fetch_delta
    assert live_poll._fetch_delta is repair._exception_rearm_fetch_delta
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_poll_exception_rearm", False))
