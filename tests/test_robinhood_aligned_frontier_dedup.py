from __future__ import annotations

import asyncio

from solana_roi import robinhood_provider_efficiency_repair as repair


class _Plane:
    def __init__(self, *, historical: int, live: int) -> None:
        self._cursor = historical
        self._roi_live_epoch_cursor = live
        self._roi_live_frontier_ranges_completed = 0
        self._roi_live_epoch_ready = True
        self._roi_live_epoch_last_range = None
        self.persisted: list[int] = []

    def _set_cursor(self, value: int) -> None:
        self._cursor = int(value)
        self.persisted.append(int(value))


def test_aligned_completed_live_range_advances_durable_historical_cursor(monkeypatch) -> None:
    plane = _Plane(historical=100, live=100)

    async def advance(self) -> None:
        self._roi_live_epoch_cursor = 104
        self._roi_live_frontier_ranges_completed += 1
        self._roi_live_epoch_last_range = {"from_block": 101, "to_block": 104, "market_logs": 3}
        self._roi_live_epoch_ready = True

    monkeypatch.setattr(repair, "_ORIGINAL_ADVANCE_LIVE_EPOCH", advance)
    asyncio.run(repair._advance_live_epoch_with_aligned_history_reuse(plane))

    assert plane._cursor == 104
    assert plane.persisted == [104]
    assert plane._roi_live_frontier_ranges_reused_for_historical == 1
    assert plane._roi_live_frontier_blocks_reused_for_historical == 4
    assert plane._roi_live_frontier_last_reused_range == {
        "from_block": 101,
        "to_block": 104,
        "blocks": 4,
        "reason": "aligned_live_range_already_fully_acquired",
    }


def test_historical_backlog_is_never_skipped(monkeypatch) -> None:
    plane = _Plane(historical=80, live=100)

    async def advance(self) -> None:
        self._roi_live_epoch_cursor = 104
        self._roi_live_frontier_ranges_completed += 1
        self._roi_live_epoch_last_range = {"from_block": 101, "to_block": 104, "market_logs": 3}
        self._roi_live_epoch_ready = True

    monkeypatch.setattr(repair, "_ORIGINAL_ADVANCE_LIVE_EPOCH", advance)
    asyncio.run(repair._advance_live_epoch_with_aligned_history_reuse(plane))

    assert plane._cursor == 80
    assert plane.persisted == []
    assert not hasattr(plane, "_roi_live_frontier_ranges_reused_for_historical")


def test_reanchor_without_completed_live_range_is_not_reused(monkeypatch) -> None:
    plane = _Plane(historical=100, live=100)

    async def reanchor(self) -> None:
        self._roi_live_epoch_cursor = 300
        self._roi_live_epoch_last_range = None
        self._roi_live_epoch_ready = False

    monkeypatch.setattr(repair, "_ORIGINAL_ADVANCE_LIVE_EPOCH", reanchor)
    asyncio.run(repair._advance_live_epoch_with_aligned_history_reuse(plane))

    assert plane._cursor == 100
    assert plane.persisted == []


def test_status_declares_no_coverage_or_authority_change() -> None:
    payload = repair.status()
    assert payload["historical_backlog_skipped"] is False
    assert payload["large_gap_reanchor_reused_as_history"] is False
    assert payload["block_coverage_reduced"] is False
    assert payload["market_coverage_reduced"] is False
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
