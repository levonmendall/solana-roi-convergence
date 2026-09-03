from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from solana_roi import continuity_durability_repair as repair
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi import poll_watermark_repair as watermark
from solana_roi.direct_solana import WatchTarget


def test_time_critical_poll_page_uses_read_only_rpc_hedging(monkeypatch):
    calls: list[tuple[str, bool]] = []

    class Rpc:
        async def call_with_meta(self, method, _params, *, hedge=False):
            calls.append((method, bool(hedge)))
            return [], "publicnode", 1.0

    monkeypatch.setattr(live_poll, "_poll_rpc", lambda _self: Rpc())
    target = WatchTarget("program", "program-a", "PUMP_FUN")
    rows, provider, latency = asyncio.run(
        repair._hedged_slot_poll_page(SimpleNamespace(), target, limit=1)
    )

    assert rows == []
    assert provider == "publicnode"
    assert latency == 1.0
    assert calls == [("getSignaturesForAddress", True)]


def test_incomplete_bounded_delta_is_routed_into_existing_recoverability_lease(monkeypatch):
    async def incomplete(_self, _target, _cursor_slot):
        return [], False, "publicnode", 2500.0

    monkeypatch.setattr(repair, "_ORIGINAL_SLOT_FETCH_DELTA", incomplete)
    target = WatchTarget("program", "program-a", "PUMP_AMM")

    with pytest.raises(repair.RecoverableLivePollDeltaIncomplete):
        asyncio.run(
            repair._lease_aware_slot_fetch_delta(SimpleNamespace(), target, 100)
        )


def test_continuity_repair_preserves_fixed_fail_closed_lease_contract():
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert watermark._slot_fetch_delta is repair._lease_aware_slot_fetch_delta
    assert watermark._slot_poll_page is repair._hedged_slot_poll_page
    assert live_poll._fetch_delta is repair._lease_aware_slot_fetch_delta
