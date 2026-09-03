from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from solana_roi import continuity_durability_repair as repair
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_exception_rearm as exception_rearm
from solana_roi import poll_recoverability_lease as lease
from solana_roi import poll_watermark_repair as watermark
from solana_roi.direct_solana import WatchTarget


def test_time_critical_recoverability_poll_page_uses_read_only_rpc_hedging(monkeypatch):
    calls: list[tuple[str, bool]] = []

    class Rpc:
        async def call_with_meta(self, method, _params, *, hedge=False):
            calls.append((method, bool(hedge)))
            return [], "publicnode", 1.0

    monkeypatch.setattr(live_poll, "_poll_rpc", lambda _self: Rpc())
    target = WatchTarget("program", "program-a", "PUMP_FUN")
    rows, provider, latency = asyncio.run(
        repair._lease_slot_poll_page(SimpleNamespace(), target, limit=1)
    )

    assert rows == []
    assert provider == "publicnode"
    assert latency == 1.0
    assert calls == [("getSignaturesForAddress", True)]


def test_incomplete_bounded_delta_after_real_ws_gap_routes_into_lease(monkeypatch):
    async def full_page(_self, _target, **_kwargs):
        return [
            {"signature": "s105", "slot": 105},
            {"signature": "s104", "slot": 104},
        ], "publicnode", 2500.0

    monkeypatch.setattr(repair, "_lease_slot_poll_page", full_page)
    monkeypatch.setattr(live_poll, "POLL_LIMIT", 2)
    monkeypatch.setattr(live_poll, "POLL_CURSOR_MAX_PAGES", 1)
    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda _self, _target: False)
    target = WatchTarget("program", "program-a", "PUMP_AMM")

    with pytest.raises(repair.RecoverableLivePollDeltaIncomplete):
        asyncio.run(repair._lease_slot_fetch_delta(SimpleNamespace(), target, 100))


def test_continuity_repair_preserves_existing_module_contracts_and_fixed_lease():
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    # Established offline/helper contracts remain untouched.
    assert watermark._slot_fetch_delta is exception_rearm._exception_rearm_fetch_delta
    assert live_poll._fetch_delta is exception_rearm._exception_rearm_fetch_delta
    # Only the production recoverability worker sees the repaired hedged view.
    assert lease.watermark._slot_fetch_delta is repair._lease_slot_fetch_delta
    assert lease.watermark._slot_poll_page is repair._lease_slot_poll_page
