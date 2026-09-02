from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_standby_rearm as rearm
from solana_roi import poll_watermark_repair as watermark
from solana_roi.direct_solana import DirectSolanaIngestionPlane, WatchTarget


def test_overflow_rearm_requires_same_target_websocket(monkeypatch):
    target = WatchTarget("program", "program-a", "PUMP_FUN")
    called = False

    async def fake_page(*_args, **_kwargs):
        nonlocal called
        called = True
        return [{"signature": "head", "slot": 200}], "publicnode", 10.0

    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda _self, _target: False)
    monkeypatch.setattr(watermark, "_slot_poll_page", fake_page)

    result = asyncio.run(rearm._try_rearm_under_websocket(SimpleNamespace(), target, 100))
    assert result is None
    assert called is False


def test_overflow_rearm_moves_only_to_current_confirmed_head(monkeypatch):
    target = WatchTarget("program", "program-a", "PUMP_FUN")
    captured: dict[str, object] = {}

    async def fake_page(_self, _target, **kwargs):
        captured.update(kwargs)
        return [{"signature": "head", "slot": 250}], "publicnode", 12.5

    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda _self, _target: True)
    monkeypatch.setattr(watermark, "_slot_poll_page", fake_page)

    plane = SimpleNamespace()
    result = asyncio.run(rearm._try_rearm_under_websocket(plane, target, 100))

    assert result == (250, "publicnode", 12.5)
    assert captured["min_context_slot"] == 100
    assert captured["limit"] == 1
    key = live_poll._poll_target_key(target)
    assert plane._roi_poll_overflow_rearms[key] == 1


def test_overflow_rearm_rejects_nonadvancing_head(monkeypatch):
    target = WatchTarget("scout", "wallet-a", None)

    async def fake_page(_self, _target, **_kwargs):
        return [{"signature": "same", "slot": 100}], "publicnode", 8.0

    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda _self, _target: True)
    monkeypatch.setattr(watermark, "_slot_poll_page", fake_page)

    result = asyncio.run(rearm._try_rearm_under_websocket(SimpleNamespace(), target, 100))
    assert result is None


def test_poll_standby_rearm_installed_intrinsically():
    assert live_poll._poll_target is rearm._standby_poll_target
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_poll_standby_rearm", False))
