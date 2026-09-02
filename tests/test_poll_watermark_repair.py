from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_pagination_context as pagination
from solana_roi.direct_solana import DirectSolanaIngestionPlane, WatchTarget
from solana_roi.poll_watermark_repair import _slot_fetch_delta, _slot_poll_page


def test_slot_poll_page_uses_min_context_slot_without_until(monkeypatch):
    captured: dict[str, object] = {}

    class Pool:
        async def call_with_meta(self, method, params, *, hedge):
            captured["method"] = method
            captured["params"] = params
            captured["hedge"] = hedge
            return [{"signature": "new", "slot": 124}], "publicnode", 11.0

    monkeypatch.setattr(live_poll, "_poll_rpc", lambda _self: Pool())
    rows, provider, latency = asyncio.run(
        _slot_poll_page(
            SimpleNamespace(),
            WatchTarget("program", "program-a", "PUMP_AMM"),
            before="before-sig",
            min_context_slot=123,
            limit=1000,
        )
    )

    assert rows[0]["signature"] == "new"
    assert provider == "publicnode"
    assert latency == 11.0
    assert captured["method"] == "getSignaturesForAddress"
    assert captured["hedge"] is False
    config = captured["params"][1]
    assert config["limit"] == 1000
    assert config["before"] == "before-sig"
    assert config["minContextSlot"] == 123
    assert "until" not in config


def test_slot_delta_completes_on_confirmed_slot_boundary_without_signature_cursor(monkeypatch):
    original_limit = live_poll.POLL_LIMIT
    original_pages = live_poll.POLL_CURSOR_MAX_PAGES
    live_poll.POLL_LIMIT = 3
    live_poll.POLL_CURSOR_MAX_PAGES = 3
    pages = [
        ([
            {"signature": "s105", "slot": 105},
            {"signature": "s104", "slot": 104},
            {"signature": "s103", "slot": 103},
        ], "publicnode", 10.0),
        ([
            {"signature": "s102", "slot": 102},
            {"signature": "s101", "slot": 101},
            {"signature": "different-old-signature", "slot": 100},
        ], "solana-mainnet", 12.0),
    ]
    context_slots: list[int] = []

    async def fake_page(_self, _target, **kwargs):
        context_slots.append(int(kwargs["min_context_slot"]))
        return pages.pop(0)

    monkeypatch.setattr("solana_roi.poll_watermark_repair._slot_poll_page", fake_page)
    try:
        rows, complete, provider, latency = asyncio.run(
            _slot_fetch_delta(
                SimpleNamespace(),
                WatchTarget("program", "program-a", "RAYDIUM"),
                100,
            )
        )
    finally:
        live_poll.POLL_LIMIT = original_limit
        live_poll.POLL_CURSOR_MAX_PAGES = original_pages

    assert complete is True
    assert provider == "solana-mainnet"
    assert latency == 12.0
    assert context_slots == [100, 105]
    assert [row["slot"] for row in rows] == [101, 102, 103, 104, 105]
    assert all(row["signature"] != "different-old-signature" for row in rows)


def test_slot_delta_fails_closed_when_bounded_window_never_reaches_watermark(monkeypatch):
    original_limit = live_poll.POLL_LIMIT
    original_pages = live_poll.POLL_CURSOR_MAX_PAGES
    live_poll.POLL_LIMIT = 2
    live_poll.POLL_CURSOR_MAX_PAGES = 2
    pages = [
        ([{"signature": "s105", "slot": 105}, {"signature": "s104", "slot": 104}], "publicnode", 5.0),
        ([{"signature": "s103", "slot": 103}, {"signature": "s102", "slot": 102}], "publicnode", 5.0),
    ]

    async def fake_page(_self, _target, **_kwargs):
        return pages.pop(0)

    monkeypatch.setattr("solana_roi.poll_watermark_repair._slot_poll_page", fake_page)
    try:
        rows, complete, _provider, _latency = asyncio.run(
            _slot_fetch_delta(SimpleNamespace(), WatchTarget("program", "program-a", "PUMP_FUN"), 100)
        )
    finally:
        live_poll.POLL_LIMIT = original_limit
        live_poll.POLL_CURSOR_MAX_PAGES = original_pages

    assert rows == []
    assert complete is False


def test_slot_watermark_repair_installed_intrinsically():
    assert live_poll._fetch_delta is pagination._context_fresh_fetch_delta
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_slot_watermark_poll", False))
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_poll_pagination_context", False))
