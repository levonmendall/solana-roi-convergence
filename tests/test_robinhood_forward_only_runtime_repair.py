from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import robinhood_forward_only_runtime_repair as repair


class _StopAfterOne:
    def __init__(self) -> None:
        self._set = False

    def is_set(self) -> bool:
        return self._set

    async def wait(self) -> None:
        self._set = True
        return None


def test_bounded_metadata_recovery_never_uses_archival_cursor() -> None:
    assert repair._bounded_metadata_start(latest=2000, previous_live_cursor=None) == 1937
    assert repair._bounded_metadata_start(latest=2000, previous_live_cursor=1990) == 1991
    assert repair._bounded_metadata_start(latest=2000, previous_live_cursor=1000) == 1937


def test_forward_only_base_poll_never_calls_historical_scanner() -> None:
    calls: list[str] = []

    async def historical(_self):
        calls.append("historical")
        raise AssertionError("historical scanner must not run")

    class Plane:
        _caught_up = True
        _last_poll_at = None
        _last_success_at = None
        _last_error = "old"

        async def _settle_open_positions(self):
            calls.append("settle")

    plane = Plane()
    wrapped = repair._forward_only_base_poll(historical)
    asyncio.run(wrapped(plane))

    assert calls == ["settle"]
    assert plane._caught_up is False
    assert plane._last_error is None
    assert plane._roi_forward_only_cycles == 1


def test_forward_only_epoch_anchors_current_head_without_swap_backfill(monkeypatch) -> None:
    factory_ranges: list[tuple[int, int]] = []
    market_calls: list[tuple[int, int]] = []

    async def sync(_self, *, from_block: int, to_block: int) -> int:
        factory_ranges.append((from_block, to_block))
        return 0

    async def markets(_self, *, from_block: int, to_block: int):
        market_calls.append((from_block, to_block))
        return []

    class Rpc:
        async def chain_id(self) -> int:
            return 46630

        async def block_number(self) -> int:
            return 2000

    plane = SimpleNamespace(
        rpc=Rpc(),
        _cursor=1000,
        _latest_block=None,
        _caught_up=False,
        v3_pools={},
        v2_curves={},
    )

    monkeypatch.setattr(repair.frontier, "_sync_factory_state", sync)
    monkeypatch.setattr(repair.frontier, "_fetch_market_logs", markets)
    monkeypatch.setattr(repair.frontier, "_record_epoch", lambda *_args, **_kwargs: None)

    asyncio.run(repair._forward_only_advance_live_epoch(plane))

    assert plane._cursor == 1000
    assert plane._roi_live_epoch_anchor_block == 2000
    assert plane._roi_live_epoch_cursor == 2000
    assert plane._roi_live_epoch_ready is False
    assert factory_ranges == [(1937, 2000)]
    assert market_calls == []
    assert plane._roi_forward_only_last_metadata_recovery["swap_backfill_performed"] is False


def test_long_outage_reanchors_with_bounded_factory_metadata_only(monkeypatch) -> None:
    factory_ranges: list[tuple[int, int]] = []
    market_calls: list[tuple[int, int]] = []

    async def sync(_self, *, from_block: int, to_block: int) -> int:
        factory_ranges.append((from_block, to_block))
        return 0

    async def markets(_self, *, from_block: int, to_block: int):
        market_calls.append((from_block, to_block))
        return []

    class Rpc:
        async def block_number(self) -> int:
            return 2100

    plane = SimpleNamespace(
        rpc=Rpc(),
        _cursor=1000,
        _latest_block=2000,
        _caught_up=False,
        _roi_forward_only_chain_id_verified=True,
        _roi_live_epoch_anchor_block=2000,
        _roi_live_epoch_cursor=2000,
        _roi_live_epoch_ready=True,
        v3_pools={},
        v2_curves={},
    )

    monkeypatch.setattr(repair.frontier, "_sync_factory_state", sync)
    monkeypatch.setattr(repair.frontier, "_fetch_market_logs", markets)
    monkeypatch.setattr(repair.frontier, "_record_epoch", lambda *_args, **_kwargs: None)

    asyncio.run(repair._forward_only_advance_live_epoch(plane))

    assert factory_ranges == [(2037, 2100)]
    assert market_calls == []
    assert plane._roi_live_epoch_anchor_block == 2100
    assert plane._roi_live_epoch_cursor == 2100
    assert plane._roi_live_epoch_ready is False
    assert plane._cursor == 1000


def test_production_plane_retires_historical_backfill() -> None:
    from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane

    assert RobinhoodChainPaperPlane._roi_robinhood_forward_only_runtime_installed is True
    assert getattr(RobinhoodChainPaperPlane.run, "_roi_robinhood_forward_only_run", False) is True
    assert getattr(
        RobinhoodChainPaperPlane._poll_once,
        "_roi_verified_live_epoch_poll",
        False,
    ) is True
    wrapped = getattr(RobinhoodChainPaperPlane._poll_once, "__wrapped__", None)
    assert wrapped is not None
    assert getattr(wrapped, "_roi_robinhood_entity_universe", False) is True
