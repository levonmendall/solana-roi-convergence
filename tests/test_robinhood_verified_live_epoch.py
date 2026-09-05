from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi.robinhood_chain_decision import RobinhoodDecisionMixin
from solana_roi import robinhood_live_frontier_verification_repair as repair
from solana_roi import robinhood_forward_only_runtime_repair as forward


class _Rpc:
    def __init__(self, *heads: int) -> None:
        self.heads = list(heads)

    async def block_number(self) -> int:
        if len(self.heads) > 1:
            return self.heads.pop(0)
        return self.heads[0]


def test_decision_transport_can_be_live_while_historical_cursor_is_behind() -> None:
    plane = SimpleNamespace(
        _caught_up=False,
        _roi_live_epoch_cursor=2000,
        _roi_live_epoch_ready=True,
        _roi_live_epoch_suppress_entries=False,
    )
    assert RobinhoodDecisionMixin._paper_decision_transport_ready(plane) is True


def test_epoch_anchor_keeps_archival_cursor_and_only_recovers_bounded_factory_metadata(monkeypatch) -> None:
    synced: list[tuple[int, int]] = []

    async def sync(_self, *, from_block: int, to_block: int) -> int:
        synced.append((from_block, to_block))
        return 0

    class Rpc(_Rpc):
        async def chain_id(self) -> int:
            return 46630

    monkeypatch.setattr(repair, "_sync_factory_state", sync)
    monkeypatch.setattr(repair, "_record_epoch", lambda *_args, **_kwargs: None)
    plane = SimpleNamespace(
        rpc=Rpc(2000, 2000),
        _cursor=1000,
        _latest_block=None,
        _caught_up=False,
        v3_pools={},
        v2_curves={},
    )

    asyncio.run(forward._forward_only_advance_live_epoch(plane))
    assert plane._cursor == 1000
    assert plane._roi_live_epoch_anchor_block == 2000
    assert plane._roi_live_epoch_cursor == 2000
    assert plane._roi_live_epoch_ready is False
    assert synced == [(1937, 2000)]

    asyncio.run(forward._forward_only_advance_live_epoch(plane))
    assert plane._cursor == 1000
    assert plane._roi_live_epoch_ready is True


def test_prospective_range_becomes_authoritative_only_after_complete_processing(monkeypatch) -> None:
    market = SimpleNamespace(pool="0xpool")
    decision_calls: list[tuple[int, bool]] = []
    processing_suppression: list[bool] = []

    async def sync(_self, *, from_block: int, to_block: int) -> int:
        return 0

    async def market_logs(_self, *, from_block: int, to_block: int):
        return [("v3", market, {"blockNumber": hex(to_block)})]

    class Plane:
        def __init__(self) -> None:
            self.rpc = _Rpc(2001)
            self._cursor = 1000
            self._latest_block = 2000
            self._caught_up = False
            self._roi_forward_only_chain_id_verified = True
            self._roi_live_epoch_anchor_block = 2000
            self._roi_live_epoch_cursor = 2000
            self._roi_live_epoch_ready = True
            self.v3_pools = {}
            self.v2_curves = {}

        async def _process_v3_swap(self, _market, _log, *, live, observed_at):
            assert live is True
            assert observed_at
            processing_suppression.append(bool(self._roi_live_epoch_suppress_entries))
            await self._maybe_open_v3(_market, current_block=2001)

        async def _process_v2_curve_log(self, *_args, **_kwargs):
            raise AssertionError("no v2 log expected")

        async def _maybe_open_v3(self, _market, *, current_block: int):
            if self._roi_live_epoch_suppress_entries:
                return
            decision_calls.append((current_block, self._caught_up))

        async def _maybe_open_v2(self, _market):
            raise AssertionError("no v2 decision expected")

    monkeypatch.setattr(repair, "_sync_factory_state", sync)
    monkeypatch.setattr(repair, "_fetch_market_logs", market_logs)
    plane = Plane()
    asyncio.run(forward._forward_only_advance_live_epoch(plane))

    assert processing_suppression == [True]
    assert decision_calls == [(2001, False)]
    assert plane._roi_live_epoch_cursor == 2001
    assert plane._roi_live_epoch_ready is True
    assert plane._cursor == 1000


def test_fresh_entry_guard_uses_live_cursor_not_historical_cursor() -> None:
    plane = SimpleNamespace(
        rpc=_Rpc(2001),
        _cursor=1000,
        _latest_block=2000,
        _caught_up=False,
        _roi_live_epoch_cursor=2000,
        _roi_live_epoch_ready=True,
        _roi_live_epoch_suppress_entries=False,
    )
    assert asyncio.run(repair._fresh_head_ready(plane)) is True
    assert plane._caught_up is False


def test_large_live_gap_reanchors_without_swap_replay(monkeypatch) -> None:
    synced: list[tuple[int, int]] = []

    async def sync(_self, *, from_block: int, to_block: int) -> int:
        synced.append((from_block, to_block))
        return 0

    monkeypatch.setattr(repair, "_sync_factory_state", sync)
    monkeypatch.setattr(repair, "_record_epoch", lambda *_args, **_kwargs: None)
    plane = SimpleNamespace(
        rpc=_Rpc(2100),
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
    asyncio.run(forward._forward_only_advance_live_epoch(plane))
    assert synced == [(2037, 2100)]
    assert plane._roi_live_epoch_anchor_block == 2100
    assert plane._roi_live_epoch_cursor == 2100
    assert plane._roi_live_epoch_ready is False
    assert plane._cursor == 1000


def test_status_exposes_live_readiness_separately_from_archival_cursor() -> None:
    class Plane:
        _cursor = 1000
        _latest_block = 2001
        _caught_up = False
        _roi_live_epoch_anchor_block = 2000
        _roi_live_epoch_cursor = 2000
        _roi_live_epoch_ready = True
        _roi_live_epoch_suppress_entries = False

    wrapped = repair._status_with_frontier_verification(
        lambda _self: {
            "caught_up_for_paper_decisions": False,
            "paper_decision_transport_ready": False,
            "catchup_capacity": {"paper_entries_allowed_during_catchup": False},
        }
    )
    payload = wrapped(Plane())
    assert payload["historical_caught_up"] is False
    assert payload["historical_block_lag"] is None
    assert payload["historical_backfill_enabled"] is False
    assert payload["caught_up_for_paper_decisions"] is True
    assert payload["paper_decision_transport_ready"] is True
    assert payload["block_lag"] == 1
    assert payload["live_frontier_verification"]["retrospective_entry_authority"] is False
    assert payload["live_frontier_verification"]["historical_backfill_enabled"] is False
    assert payload["live_frontier_verification"]["historical_data_preserved"] is True


def test_production_plane_installs_verified_live_epoch_last() -> None:
    from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane

    assert RobinhoodChainPaperPlane._roi_live_frontier_verification_installed is True
    assert RobinhoodChainPaperPlane._roi_robinhood_forward_only_runtime_installed is True
    assert getattr(RobinhoodChainPaperPlane._poll_once, "_roi_verified_live_epoch_poll", False) is True
    assert getattr(RobinhoodChainPaperPlane._maybe_open_v3, "_roi_fresh_live_frontier_entry_guard", False) is True
