from __future__ import annotations

import asyncio
from types import SimpleNamespace


def test_production_composition_installs_scout_and_high_volume_repairs():
    from solana_roi import poll_watermark_repair as watermark
    from solana_roi.direct_solana import DirectSolanaIngestionPlane
    from solana_roi.production import app  # noqa: F401

    assert getattr(DirectSolanaIngestionPlane.status, "_roi_scout_candidate_continuity", False) is True
    assert getattr(watermark._slot_poll_page, "_roi_high_volume_signature_cursor", False) is True
    assert getattr(watermark._slot_fetch_delta, "_roi_high_volume_signature_cursor", False) is True


def test_high_volume_exact_cursor_preserves_same_slot_rows(monkeypatch):
    from solana_roi import high_volume_signature_cursor_repair as repair
    from solana_roi import poll_watermark_repair as watermark
    from solana_roi.direct_solana import WatchTarget

    target = WatchTarget(kind="program", address="pump-amm", source_hint="PUMP_AMM")
    plane = SimpleNamespace()
    setattr(
        plane,
        "_roi_high_volume_exact_poll_cursors",
        {
            "program:pump-amm": {
                "signature": "boundary",
                "slot": 100,
                "provider": "publicnode",
                "source": "test",
            }
        },
    )

    calls: list[dict[str, object]] = []

    class FakePool:
        async def call_with_meta(self, method, params, hedge=False):
            assert method == "getSignaturesForAddress"
            calls.append(params[1])
            return ([{"signature": "same-slot-new", "slot": 100}], "publicnode", 1.0)

    monkeypatch.setattr(repair.storage, "_routine_poll_pool", lambda self, target: FakePool())
    original_fetch = repair._ORIGINAL_SLOT_FETCH_DELTA
    if original_fetch is None:
        repair._ORIGINAL_SLOT_FETCH_DELTA = watermark._slot_fetch_delta
    try:
        rows, complete, provider, _latency = asyncio.run(
            repair._fetch_delta_with_high_volume_exact_cursor(plane, target, 100)
        )
    finally:
        repair._ORIGINAL_SLOT_FETCH_DELTA = original_fetch

    assert complete is True
    assert provider == "publicnode"
    assert [row["signature"] for row in rows] == ["same-slot-new"]
    assert calls[0]["until"] == "boundary"
    assert calls[0]["minContextSlot"] == 100
    cursor = plane._roi_high_volume_exact_poll_cursors["program:pump-amm"]
    assert cursor["signature"] == "same-slot-new"
    assert cursor["slot"] == 100


def test_high_volume_exact_cursor_keeps_hard_3x1000_fail_closed(monkeypatch):
    from solana_roi import high_volume_signature_cursor_repair as repair
    from solana_roi import live_poll_redundancy as live_poll
    from solana_roi import poll_watermark_repair as watermark
    from solana_roi.direct_solana import WatchTarget

    target = WatchTarget(kind="program", address="pump-fun", source_hint="PUMP_FUN")
    plane = SimpleNamespace()
    setattr(
        plane,
        "_roi_high_volume_exact_poll_cursors",
        {
            "program:pump-fun": {
                "signature": "boundary",
                "slot": 200,
                "provider": "publicnode",
                "source": "test",
            }
        },
    )
    page_number = 0

    class FakePool:
        async def call_with_meta(self, method, params, hedge=False):
            nonlocal page_number
            page_number += 1
            rows = [
                {"signature": f"p{page_number}-{index}", "slot": 201 + page_number}
                for index in range(live_poll.POLL_LIMIT)
            ]
            return rows, "publicnode", 1.0

    monkeypatch.setattr(repair.storage, "_routine_poll_pool", lambda self, target: FakePool())
    original_fetch = repair._ORIGINAL_SLOT_FETCH_DELTA
    if original_fetch is None:
        repair._ORIGINAL_SLOT_FETCH_DELTA = watermark._slot_fetch_delta
    try:
        rows, complete, _provider, _latency = asyncio.run(
            repair._fetch_delta_with_high_volume_exact_cursor(plane, target, 200)
        )
    finally:
        repair._ORIGINAL_SLOT_FETCH_DELTA = original_fetch

    assert complete is False
    assert rows == []
    assert page_number == live_poll.POLL_CURSOR_MAX_PAGES
    cursor = plane._roi_high_volume_exact_poll_cursors["program:pump-fun"]
    assert cursor["signature"] == "boundary"
