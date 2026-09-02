from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_chain_head_rearm as chain_rearm
from solana_roi import poll_recoverability_lease as recoverability
from solana_roi import poll_standby_rearm as rearm
from solana_roi.direct_solana import DirectSolanaIngestionPlane, WatchTarget


def test_overflow_rearm_requires_same_target_websocket(monkeypatch):
    target = WatchTarget("program", "program-a", "PUMP_FUN")
    called = False

    class Pool:
        async def call_with_meta(self, *_args, **_kwargs):
            nonlocal called
            called = True
            return 200, "publicnode", 10.0

    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda _self, _target: False)
    monkeypatch.setattr(live_poll, "_poll_rpc", lambda _self: Pool())

    result = asyncio.run(rearm._try_rearm_under_websocket(SimpleNamespace(), target, 100))
    assert result is None
    assert called is False


def test_overflow_rearm_moves_only_to_current_confirmed_chain_head(monkeypatch):
    target = WatchTarget("program", "program-a", "PUMP_FUN")
    captured: dict[str, object] = {}

    class Pool:
        async def call_with_meta(self, method, params, *, hedge):
            captured["method"] = method
            captured["params"] = params
            captured["hedge"] = hedge
            return 250, "publicnode", 12.5

    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda _self, _target: True)
    monkeypatch.setattr(live_poll, "_poll_rpc", lambda _self: Pool())

    plane = SimpleNamespace()
    result = asyncio.run(rearm._try_rearm_under_websocket(plane, target, 100))

    assert result == (250, "publicnode", 12.5)
    assert captured["method"] == "getSlot"
    assert captured["params"] == [{"commitment": "confirmed"}]
    assert captured["hedge"] is True
    key = live_poll._poll_target_key(target)
    assert plane._roi_poll_overflow_rearms[key] == 1


def test_overflow_rearm_rejects_nonadvancing_chain_head(monkeypatch):
    target = WatchTarget("scout", "wallet-a", None)

    class Pool:
        async def call_with_meta(self, *_args, **_kwargs):
            return 100, "publicnode", 8.0

    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda _self, _target: True)
    monkeypatch.setattr(live_poll, "_poll_rpc", lambda _self: Pool())

    result = asyncio.run(rearm._try_rearm_under_websocket(SimpleNamespace(), target, 100))
    assert result is None


def test_poll_standby_rearm_remains_installed_beneath_recoverability_lease():
    assert rearm._try_rearm_under_websocket is chain_rearm._try_rearm_from_confirmed_chain_head
    assert live_poll._poll_target is recoverability._leased_poll_target
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_poll_standby_rearm", False))
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_poll_recoverability_lease", False))
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_poll_chain_head_rearm", False))
