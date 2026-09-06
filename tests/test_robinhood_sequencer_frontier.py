from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

from solana_roi import robinhood_chain_runtime as runtime
from solana_roi import robinhood_live_frontier_verification_repair as frontier
from solana_roi import robinhood_sequencer_frontier_repair as sequencer
from solana_roi import robinhood_worker_isolation_repair as isolation


def test_sequence_numbers_parse_public_feed_envelope() -> None:
    payload = {
        "version": 1,
        "messages": [
            {"sequenceNumber": 123, "blockHash": "0xabc"},
            {"sequenceNumber": "124"},
            {"sequenceNumber": None},
            {"other": 9},
        ],
    }
    assert sequencer._sequence_numbers(payload) == [123, 124]
    assert sequencer._sequence_numbers(None) == []


def test_feed_freshness_and_two_block_gate_are_unchanged() -> None:
    assert sequencer.DECISION_LAG_BLOCKS == runtime.LIVE_LAG_BLOCKS == 2

    plane = SimpleNamespace(
        _roi_sequencer_synchronized=True,
        _roi_sequencer_continuity_ok=True,
        _roi_sequencer_last_monotonic=time.monotonic(),
        _roi_sequencer_head_block=102,
        _roi_live_epoch_cursor=100,
        _roi_live_epoch_ready=True,
        _roi_live_epoch_suppress_entries=False,
        _latest_block=None,
    )
    assert asyncio.run(sequencer._fresh_feed_head_ready(plane)) is True
    assert plane._roi_live_frontier_last_lag == 2

    plane._roi_sequencer_head_block = 103
    plane._roi_live_epoch_ready = True
    plane._roi_sequencer_last_monotonic = time.monotonic()
    assert asyncio.run(sequencer._fresh_feed_head_ready(plane)) is False
    assert plane._roi_live_epoch_ready is False


def test_stale_or_unsynchronized_feed_fails_closed() -> None:
    plane = SimpleNamespace(
        _roi_sequencer_synchronized=False,
        _roi_sequencer_continuity_ok=True,
        _roi_sequencer_last_monotonic=time.monotonic(),
        _roi_sequencer_head_block=100,
        _roi_live_epoch_cursor=100,
        _roi_live_epoch_ready=True,
        _roi_live_epoch_suppress_entries=False,
    )
    assert asyncio.run(sequencer._fresh_feed_head_ready(plane)) is False
    assert plane._roi_live_epoch_ready is False

    plane._roi_sequencer_synchronized = True
    plane._roi_live_epoch_ready = True
    plane._roi_sequencer_last_monotonic = time.monotonic() - sequencer.FEED_STALE_SECONDS - 0.1
    assert asyncio.run(sequencer._fresh_feed_head_ready(plane)) is False


def test_exact_block_event_query_never_expands_range() -> None:
    calls: list[dict[str, object]] = []

    class Rpc:
        async def get_logs(self, **kwargs):
            calls.append(kwargs)
            return [
                {"blockNumber": "0x7b", "transactionIndex": "0x1", "logIndex": "0x2"},
                {"blockNumber": "0x7b", "transactionIndex": "0x0", "logIndex": "0x1"},
            ]

    plane = SimpleNamespace(rpc=Rpc())
    rows = asyncio.run(sequencer._exact_block_events(plane, 123))
    assert len(calls) == 1
    assert calls[0]["from_block"] == 123
    assert calls[0]["to_block"] == 123
    assert calls[0]["topics"] == [list(sequencer._EVENT_TOPICS)]
    assert rows[0]["transactionIndex"] == "0x0"


def test_skipped_blocks_have_no_retrospective_trade_authority(monkeypatch) -> None:
    metadata_calls: list[tuple[int, int]] = []
    exact_blocks: list[int] = []

    class Rpc:
        async def chain_id(self):
            return runtime.ROBINHOOD_CHAIN_ID

    plane = SimpleNamespace(
        rpc=Rpc(),
        _roi_forward_only_chain_id_verified=True,
        _roi_sequencer_synchronized=True,
        _roi_sequencer_continuity_ok=True,
        _roi_sequencer_last_monotonic=time.monotonic(),
        _roi_sequencer_head_block=110,
        _roi_sequencer_generation=0,
        _roi_sequencer_processed_generation=0,
        _roi_live_epoch_cursor=100,
        _roi_live_epoch_ready=True,
        _roi_live_epoch_suppress_entries=False,
        _latest_block=110,
        v3_pools={},
        v2_curves={},
    )

    async def metadata(_self, *, from_block: int, to_block: int):
        metadata_calls.append((from_block, to_block))
        return 0

    async def exact(_self, block: int):
        exact_blocks.append(block)
        return []

    monkeypatch.setattr(frontier, "_sync_factory_state", metadata)
    monkeypatch.setattr(sequencer, "_exact_block_events", exact)
    monkeypatch.setattr(sequencer, "_fresh_feed_head_ready", lambda _self: asyncio.sleep(0, result=True))
    monkeypatch.setattr(sequencer.post177, "_schedule_rwa_refresh", lambda _self: None)
    monkeypatch.setattr(sequencer.post177, "_clear_pending_markets", lambda _self: None)

    asyncio.run(sequencer._advance_sequencer_frontier(plane))
    assert exact_blocks == [110]
    assert metadata_calls == [(101, 109)]
    assert plane._roi_live_epoch_cursor == 110
    assert plane._roi_live_epoch_last_range["stale_trade_blocks_skipped"] == 9
    assert plane._roi_live_epoch_last_range["stale_trade_blocks_have_retrospective_entry_authority"] is False
    assert plane._roi_live_epoch_last_range["head_source"] == "robinhood_public_sequencer_feed"


def test_fast_status_publisher_never_calls_proof_wrapped_status(monkeypatch) -> None:
    stop = asyncio.Event()
    calls = {"base": 0, "wrapped": 0}

    def base_status():
        calls["base"] += 1
        return {"runtime_ready": True, "paper_only": True, "live_money_authority": False}

    def wrapped_status():
        calls["wrapped"] += 1
        raise AssertionError("proof-wrapped status must not run on live event loop")

    def publish(_payload, *, store_path):
        assert store_path == "/tmp/rh.sqlite3"
        stop.set()

    monkeypatch.setattr(isolation, "_BASE_STATUS", base_status)
    monkeypatch.setattr(isolation, "_ORIGINAL_STATUS", wrapped_status)
    monkeypatch.setattr(isolation, "_publish_snapshot", publish)
    asyncio.run(isolation._status_publisher(stop, store_path="/tmp/rh.sqlite3"))
    assert calls == {"base": 1, "wrapped": 0}


def test_worker_metadata_declares_proof_offload() -> None:
    metadata = isolation._worker_isolation_metadata(store_path="/tmp/rh.sqlite3")
    assert metadata["proof_refresh_uses_separate_sqlite_connection"] is True
    assert metadata["proof_refresh_runs_in_worker_threadpool"] is True
    assert metadata["proof_blocks_live_frontier"] is False
    assert metadata["paper_decision_gate_changed"] is False
    assert metadata["paper_only"] is True
    assert metadata["live_money_authority"] is False
