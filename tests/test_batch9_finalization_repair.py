from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import batch9_continuity_frontier_proof_repair as batch9
from solana_roi import batch9_finalization_repair as repair
from solana_roi import continuity_storage_capacity_repair as storage
from solana_roi import high_volume_signature_cursor_repair as high_volume
from solana_roi import poll_exception_rearm as exception_rearm
from solana_roi import poll_pagination_context as pagination
from solana_roi import poll_watermark_repair as watermark
from solana_roi import robinhood_production_ws_transport as transport
from solana_roi import robinhood_usage_bounded_transport as bounded


def test_production_finalizer_preserves_canonical_top_level_identities() -> None:
    from solana_roi.production import app  # noqa: F401

    assert watermark._slot_fetch_delta is exception_rearm._exception_rearm_fetch_delta
    assert pagination._HIGH_VOLUME_DELTA_HOOK is high_volume._maybe_fetch_high_volume_exact_cursor
    assert getattr(watermark._slot_poll_page, "_roi_high_volume_standby_priority", False) is True
    assert getattr(watermark._slot_poll_page, "_roi_high_volume_signature_cursor", False) is True
    assert transport._production_ws_run is bounded._production_ws_run
    assert high_volume._ORIGINAL_SLOT_POLL_PAGE is batch9._slot_page_with_durable_scout_checkpoint
    assert storage._sharded_slot_poll_page is not batch9._slot_page_with_durable_scout_checkpoint


def test_generation_anchor_quarantines_first_live_block(monkeypatch) -> None:
    seen: list[tuple[bool, bool]] = []

    async def original(self, items, *, generation: int) -> None:
        seen.extend(
            (bool(item.get("live_authority", False)), bool(self._roi_live_epoch_suppress_entries))
            for item in items
        )

    monkeypatch.setattr(repair, "_ORIGINAL_BOUNDED_PROCESS_BLOCK", original)
    monkeypatch.setattr(repair.prod_ws, "_state", lambda _self: {"generation": 7})

    plane = SimpleNamespace(
        _cursor=90,
        _caught_up=False,
        _roi_live_epoch_suppress_entries=False,
        v3_pools={},
        v2_curves={},
    )
    first = [{"live_authority": True, "log": {"blockNumber": hex(100)}}]
    asyncio.run(repair._process_block_with_generation_anchor(plane, first, generation=7))

    assert plane._roi_live_epoch_anchor_block == 100
    assert plane._roi_live_epoch_cursor == 100
    assert plane._roi_prod_ws_epoch_generation == 7
    assert repair.batch9._strict_live_epoch_active(plane) is True
    assert seen == [(False, True)]

    seen.clear()
    second = [{"live_authority": True, "log": {"blockNumber": hex(101)}}]
    asyncio.run(repair._process_block_with_generation_anchor(plane, second, generation=7))
    assert seen == [(True, False)]


def test_generation_start_invalidates_prior_epoch_readiness(monkeypatch) -> None:
    monkeypatch.setattr(repair, "_ORIGINAL_READER_GENERATION_START", lambda _self: 9)
    plane = SimpleNamespace(
        _caught_up=True,
        _roi_live_epoch_ready=True,
        _roi_prod_ws_epoch_generation=8,
    )

    generation = repair._reader_generation_start_with_epoch_reset(plane)

    assert generation == 9
    assert plane._roi_prod_ws_epoch_generation is None
    assert plane._roi_live_epoch_ready is False
    assert plane._caught_up is False
    assert plane._roi_production_transport_block_reason == "robinhood_production_websocket_reanchoring"
