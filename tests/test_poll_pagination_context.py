from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_pagination_context as pagination
from solana_roi import poll_watermark_repair as watermark
from solana_roi.direct_solana import DirectSolanaIngestionPlane, WatchTarget


def test_later_page_requires_context_at_newest_seen_slot(monkeypatch):
    original_limit = live_poll.POLL_LIMIT
    original_pages = live_poll.POLL_CURSOR_MAX_PAGES
    live_poll.POLL_LIMIT = 3
    live_poll.POLL_CURSOR_MAX_PAGES = 3
    calls: list[dict[str, object]] = []
    pages = [
        ([
            {"signature": "s105", "slot": 105},
            {"signature": "s104", "slot": 104},
            {"signature": "s103", "slot": 103},
        ], "publicnode", 10.0),
        ([
            {"signature": "s102", "slot": 102},
            {"signature": "s101", "slot": 101},
            {"signature": "s100", "slot": 100},
        ], "solana-mainnet", 12.0),
    ]

    async def fake_page(_self, _target, **kwargs):
        calls.append(dict(kwargs))
        return pages.pop(0)

    monkeypatch.setattr(watermark, "_slot_poll_page", fake_page)
    try:
        rows, complete, provider, latency = asyncio.run(
            pagination._context_fresh_fetch_delta(
                SimpleNamespace(),
                WatchTarget("program", "program-a", "PUMP_FUN"),
                100,
            )
        )
    finally:
        live_poll.POLL_LIMIT = original_limit
        live_poll.POLL_CURSOR_MAX_PAGES = original_pages

    assert complete is True
    assert provider == "solana-mainnet"
    assert latency == 12.0
    assert calls[0]["min_context_slot"] == 100
    assert calls[1]["min_context_slot"] == 105
    assert calls[1]["before"] == "s103"
    assert [row["slot"] for row in rows] == [101, 102, 103, 104, 105]


def test_backend_too_stale_for_previous_before_fails_transiently(monkeypatch):
    original_limit = live_poll.POLL_LIMIT
    original_pages = live_poll.POLL_CURSOR_MAX_PAGES
    live_poll.POLL_LIMIT = 2
    live_poll.POLL_CURSOR_MAX_PAGES = 3
    calls = 0

    async def fake_page(_self, _target, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            assert kwargs["min_context_slot"] == 100
            return (
                [
                    {"signature": "s105", "slot": 105},
                    {"signature": "s104", "slot": 104},
                ],
                "publicnode",
                5.0,
            )
        assert kwargs["min_context_slot"] == 105
        raise RuntimeError("backend cannot satisfy minContextSlot")

    monkeypatch.setattr(watermark, "_slot_poll_page", fake_page)
    try:
        with pytest.raises(RuntimeError, match="minContextSlot"):
            asyncio.run(
                pagination._context_fresh_fetch_delta(
                    SimpleNamespace(),
                    WatchTarget("program", "program-a", "PUMP_AMM"),
                    100,
                )
            )
    finally:
        live_poll.POLL_LIMIT = original_limit
        live_poll.POLL_CURSOR_MAX_PAGES = original_pages


def test_true_bounded_overflow_still_fails_closed(monkeypatch):
    original_limit = live_poll.POLL_LIMIT
    original_pages = live_poll.POLL_CURSOR_MAX_PAGES
    live_poll.POLL_LIMIT = 2
    live_poll.POLL_CURSOR_MAX_PAGES = 2
    calls: list[int] = []
    pages = [
        ([{"signature": "s106", "slot": 106}, {"signature": "s105", "slot": 105}], "publicnode", 4.0),
        ([{"signature": "s104", "slot": 104}, {"signature": "s103", "slot": 103}], "solana-mainnet", 4.5),
    ]

    async def fake_page(_self, _target, **kwargs):
        calls.append(int(kwargs["min_context_slot"]))
        return pages.pop(0)

    monkeypatch.setattr(watermark, "_slot_poll_page", fake_page)
    try:
        rows, complete, provider, latency = asyncio.run(
            pagination._context_fresh_fetch_delta(
                SimpleNamespace(),
                WatchTarget("program", "program-a", "RAYDIUM"),
                100,
            )
        )
    finally:
        live_poll.POLL_LIMIT = original_limit
        live_poll.POLL_CURSOR_MAX_PAGES = original_pages

    assert rows == []
    assert complete is False
    assert provider == "solana-mainnet"
    assert latency == 4.5
    assert calls == [100, 106]


def test_pagination_context_installed_without_replacing_recoverability_worker():
    assert watermark._slot_fetch_delta is pagination._context_fresh_fetch_delta
    assert live_poll._fetch_delta is pagination._context_fresh_fetch_delta
    assert live_poll._poll_target.__name__ == "_leased_poll_target"
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_poll_pagination_context", False))
