from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi.direct_solana import DirectSolanaJournal, WatchTarget
from solana_roi.launch_funding import LaunchFundingPolicy
from solana_roi.observation_store import ObservationEventStore
from solana_roi.production_hotpath_repair import (
    FUNDING_TRANSACTION_GLOBAL_CONCURRENCY,
    _funding_source_result_concurrent,
    _persist_background_batch_aggregated,
)


def _dispatch_item(
    *,
    signature: str,
    slot: int,
    received_at: datetime,
    sequence: int,
    source: str = "PUMP_FUN",
):
    target = WatchTarget(kind="program", address="program", source_hint=source)
    message = {
        "method": "logsNotification",
        "params": {
            "subscription": 1,
            "result": {
                "context": {"slot": slot},
                "value": {"signature": signature, "err": None, "logs": []},
            },
        },
    }
    return (
        10,
        time.monotonic(),
        sequence,
        received_at,
        "publicnode",
        {1: target},
        message,
    )


def test_aggregated_minute_persistence_matches_canonical_rolling_hash(tmp_path):
    canonical_store = ObservationEventStore(tmp_path / "canonical.sqlite3")
    canonical_journal = DirectSolanaJournal(canonical_store)
    canonical_journal.set_provider("publicnode", connected=True)

    repaired_store = ObservationEventStore(tmp_path / "repaired.sqlite3")
    repaired_journal = DirectSolanaJournal(repaired_store)
    repaired_journal.set_provider("publicnode", connected=True)
    repaired_plane = SimpleNamespace(store=repaired_store, journal=repaired_journal)

    at = datetime(2026, 9, 3, 14, 0, tzinfo=timezone.utc)
    items = []
    for index in range(64):
        received_at = at + timedelta(milliseconds=index * 10)
        signature = f"sig-{index:03d}"
        slot = 1000 + index
        canonical_journal.record_receipt(
            signature=signature,
            source_key="PUMP_FUN",
            slot=slot,
            received_at=received_at,
            launch_like=False,
        )
        items.append(
            _dispatch_item(
                signature=signature,
                slot=slot,
                received_at=received_at,
                sequence=index,
            )
        )

    assert _persist_background_batch_aggregated(repaired_plane, items) == len(items)

    with canonical_store._lock:
        canonical = canonical_store.db.execute(
            "SELECT receipt_count, first_received_at, last_received_at, last_slot, rolling_sha256 "
            "FROM direct_solana_minute_receipts WHERE source='PUMP_FUN'"
        ).fetchone()
    with repaired_store._lock:
        repaired = repaired_store.db.execute(
            "SELECT receipt_count, first_received_at, last_received_at, last_slot, rolling_sha256 "
            "FROM direct_solana_minute_receipts WHERE source='PUMP_FUN'"
        ).fetchone()

    assert dict(repaired) == dict(canonical)
    assert getattr(repaired_plane, "_roi_production_hotpath_minute_state_reads") == 1
    assert getattr(repaired_plane, "_roi_production_hotpath_minute_state_writes") == 1


def _funding_tx(wallet: str, funder: str | None, block_time: int) -> dict:
    instructions = []
    if funder is not None:
        instructions.append(
            {
                "program": "system",
                "parsed": {
                    "type": "transfer",
                    "info": {
                        "source": funder,
                        "destination": wallet,
                        "lamports": 250_000_000,
                    },
                },
            }
        )
    return {
        "blockTime": block_time,
        "transaction": {"message": {"instructions": instructions}},
        "meta": {"innerInstructions": []},
    }


class ConcurrentFundingRpc:
    def __init__(self, *, buy_at: datetime):
        self.buy_at = buy_at
        self.active = 0
        self.max_active = 0
        self.tx_calls: list[str] = []

    async def get_signatures_for_address(self, _wallet, *, before=None, limit=1000, hedge=False):
        assert before is None
        assert limit == 1000
        assert hedge is True
        rows = [
            {
                "signature": f"tx-{index}",
                "slot": 990 - index,
                "blockTime": int(self.buy_at.timestamp()) - 1 - index,
                "err": None,
            }
            for index in range(8)
        ]
        return rows, "publicnode", 1.0

    async def get_transaction(self, signature, *, hedge=False):
        assert hedge is True
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.tx_calls.append(signature)
        try:
            await asyncio.sleep(0.01)
            index = int(str(signature).split("-")[-1])
            funder = "newest-funder" if index == 2 else ("older-funder" if index == 5 else None)
            return (
                _funding_tx("buyer-a", funder, int(self.buy_at.timestamp()) - 1 - index),
                "publicnode",
                1.0,
            )
        finally:
            self.active -= 1


def test_funding_transactions_overlap_but_newest_qualifying_source_still_wins():
    buy_at = datetime.now(timezone.utc)
    rpc = ConcurrentFundingRpc(buy_at=buy_at)
    collector = SimpleNamespace(rpc=rpc, policy=LaunchFundingPolicy())

    source, complete, reason = asyncio.run(
        _funding_source_result_concurrent(
            collector,
            "buyer-a",
            buy_at,
            1000,
        )
    )

    assert complete is True
    assert reason == "latest_qualifying_source_found"
    assert source is not None
    assert source.funder == "newest-funder"
    assert rpc.max_active > 1
    assert rpc.max_active <= FUNDING_TRANSACTION_GLOBAL_CONCURRENCY


class FailingOlderFundingRpc(ConcurrentFundingRpc):
    async def get_transaction(self, signature, *, hedge=False):
        if signature == "tx-5":
            await asyncio.sleep(0.005)
            raise TimeoutError("older row failed")
        return await super().get_transaction(signature, hedge=hedge)


def test_speculative_older_rpc_failure_cannot_override_newer_valid_funding_source():
    buy_at = datetime.now(timezone.utc)
    rpc = FailingOlderFundingRpc(buy_at=buy_at)
    collector = SimpleNamespace(rpc=rpc, policy=LaunchFundingPolicy())

    source, complete, reason = asyncio.run(
        _funding_source_result_concurrent(
            collector,
            "buyer-a",
            buy_at,
            1000,
        )
    )

    assert complete is True
    assert reason == "latest_qualifying_source_found"
    assert source is not None
    assert source.funder == "newest-funder"
