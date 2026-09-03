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


def test_real_gap_recovery_page_uses_read_only_rpc_hedging(monkeypatch):
    calls: list[tuple[str, bool]] = []

    class Rpc:
        async def call_with_meta(self, method, _params, *, hedge=False):
            calls.append((method, bool(hedge)))
            return [], "publicnode", 1.0

    monkeypatch.setattr(live_poll, "_poll_rpc", lambda _self: Rpc())
    target = WatchTarget("program", "program-a", "PUMP_FUN")
    rows, provider, latency = asyncio.run(
        repair._hedged_gap_poll_page(SimpleNamespace(), target, limit=1)
    )

    assert rows == []
    assert provider == "publicnode"
    assert latency == 1.0
    assert calls == [("getSignaturesForAddress", True)]


def test_incomplete_bounded_delta_after_tracked_ws_gap_routes_into_existing_lease(monkeypatch):
    target = WatchTarget("program", "program-a", "PUMP_AMM")
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace()
    lease._runtime(plane)[key] = {"cursor_ws_generation": 0}
    lease._ws_gap_generations(plane)[key] = 1

    async def incomplete(_self, _target, _cursor_slot):
        return [], False, "publicnode", 2500.0

    monkeypatch.setattr(watermark, "_slot_fetch_delta", incomplete)

    with pytest.raises(repair.RecoverableLivePollDeltaIncomplete):
        asyncio.run(lease.watermark._slot_fetch_delta(plane, target, 100))


def test_continuity_repair_preserves_existing_module_contracts_and_fixed_lease():
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    # Established helpers remain canonical and continue to be monkeypatchable.
    assert watermark._slot_fetch_delta is exception_rearm._exception_rearm_fetch_delta
    assert live_poll._fetch_delta is exception_rearm._exception_rearm_fetch_delta
    assert lease.watermark._base is watermark
    assert lease.watermark._slot_poll_page is watermark._slot_poll_page
