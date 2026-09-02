from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import direct_solana as direct_solana_module
from solana_roi import launch_coverage_bridge as bridge_module
from solana_roi.launch_coverage_bridge import (
    _hydrate_prospective_launch,
    launch_created_at_from_transaction,
    launch_mint_from_transaction,
)
from solana_roi.observation_store import ObservationEventStore


MINT = "9xQeWvG816bUx9EPfEZ1dmh7x9Yi4WBQPvWs7xQbX6L"
OTHER_MINT = "So11111111111111111111111111111111111111113"


def _initialize_mint_transaction(mint: str, *, block_time: int | None = None) -> dict:
    result = {
        "transaction": {
            "message": {
                "instructions": [
                    {
                        "program": "spl-token",
                        "parsed": {
                            "type": "initializeMint2",
                            "info": {"mint": mint},
                        },
                    }
                ]
            }
        },
        "meta": {"preTokenBalances": [], "postTokenBalances": [{"mint": mint}]},
    }
    if block_time is not None:
        result["blockTime"] = block_time
    return result


def test_launch_mint_prefers_one_explicit_initialize_mint():
    assert launch_mint_from_transaction(_initialize_mint_transaction(MINT)) == MINT


def test_launch_mint_falls_back_to_one_new_post_token_balance():
    transaction = {
        "transaction": {"message": {"instructions": []}},
        "meta": {
            "preTokenBalances": [{"mint": "ExistingMint111111111111111111111111111111111"}],
            "postTokenBalances": [
                {"mint": "ExistingMint111111111111111111111111111111111"},
                {"mint": MINT},
            ],
        },
    }
    assert launch_mint_from_transaction(transaction) == MINT


def test_launch_mint_fails_closed_when_new_mint_is_ambiguous():
    transaction = {
        "transaction": {"message": {"instructions": []}},
        "meta": {
            "preTokenBalances": [],
            "postTokenBalances": [{"mint": MINT}, {"mint": OTHER_MINT}],
        },
    }
    assert launch_mint_from_transaction(transaction) is None


def test_launch_created_at_uses_confirmed_chain_block_time():
    created = datetime.now(timezone.utc).replace(microsecond=0)
    transaction = _initialize_mint_transaction(MINT, block_time=int(created.timestamp()))
    assert launch_created_at_from_transaction(transaction) == created


def test_non_swap_launch_publishes_coverage_without_candidate_latency_or_quote(monkeypatch, tmp_path):
    store = ObservationEventStore(tmp_path / "bridge.sqlite3")
    created_at = datetime.now(timezone.utc) - timedelta(seconds=20)
    chain_created_at = datetime.fromtimestamp(int(created_at.timestamp()), tz=timezone.utc)
    context_at = chain_created_at + timedelta(seconds=3)

    class Journal:
        def __init__(self):
            self.finishes = []
            self.hydrations = []

        def finish(self, signature, *, error=None, retry=False):
            self.finishes.append((signature, error, retry))

        def record_hydration(self, **kwargs):
            self.hydrations.append(kwargs)

    class Rpc:
        async def get_signatures_for_address(self, mint, *, limit, hedge):
            assert mint == MINT
            assert limit > 0
            assert hedge is True
            return ([{"signature": "context-swap", "blockTime": int(context_at.timestamp()), "err": None}], "publicnode", 10.0)

        async def get_transaction(self, signature, *, hedge):
            assert signature == "context-swap"
            assert hedge is True
            return {"transaction": True}, "publicnode", 12.0

    class LaunchCollector:
        def __init__(self):
            self.seeded = {}

        def seed_created_at(self, mint, at):
            self.seeded[mint] = at

    class RawCollectors:
        def __init__(self):
            self.launch = LaunchCollector()

        async def refresh_coverage(self, mint, at, *, current_swap=None):
            assert mint == MINT
            assert current_swap is None
            assert self.launch.seeded[mint] == chain_created_at
            store.record_program_coverage(
                token_mint=mint,
                pair_created_at=self.launch.seeded[mint].isoformat(),
                assessed_at=at.isoformat(),
                launch_lag_ms=1000.0,
                launch_near_creation=True,
                early_buy_count=3,
                early_buyer_count=3,
                early_buyers_complete=True,
            )

    class Plane:
        def __init__(self):
            self.store = store
            self.journal = Journal()
            self.rpc = Rpc()
            self.raw_collectors = RawCollectors()
            self.service = SimpleNamespace(collectors=SimpleNamespace(inner=self.raw_collectors))
            self.persisted = []

        async def _get_transaction_ready(self, signature, *, hedge, attempts):
            assert signature == "launch-signature"
            assert hedge is True
            assert attempts == 4
            return _initialize_mint_transaction(MINT, block_time=int(created_at.timestamp())), "publicnode", 15.0

        async def _pair_created_at(self, mint):
            raise AssertionError("DexScreener fallback must not run when chain blockTime is present")

        def _persist_context_swap(self, swap):
            self.persisted.append(swap)

    monkeypatch.setattr(
        direct_solana_module,
        "normalize_standard_transaction",
        lambda *args, **kwargs: SimpleNamespace(token_mint=MINT),
    )

    original_called = False

    async def original(_self, _row):
        nonlocal original_called
        original_called = True

    plane = Plane()
    asyncio.run(
        _hydrate_prospective_launch(
            plane,
            {
                "signature": "launch-signature",
                "trigger_received_at": created_at.isoformat(),
                "source_hint": "PUMP_FUN",
                "priority": 10,
                "reason": "prospective_launch",
                "attempts": 0,
            },
            original,
        )
    )

    assert original_called is False
    assert len(plane.persisted) == 1
    assert store.recent_program_coverage(10)[0]["token_mint"] == MINT
    assert store.recent_risk_refreshes(10) == []
    assert plane.journal.finishes == [("launch-signature", None, False)]
    assert plane.journal.hydrations[-1]["normalized"] is False
    assert plane.journal.hydrations[-1]["candidate_context_prefilled"] is True
    assert plane.raw_collectors.launch.seeded[MINT] == chain_created_at
    assert getattr(plane, "_roi_launch_bridge_chain_created_at") == 1
    assert getattr(plane, "_roi_launch_bridge_coverage_rows") == 1


def test_incomplete_context_retries_without_widening_launch_window(monkeypatch, tmp_path):
    store = ObservationEventStore(tmp_path / "bridge-retry.sqlite3")
    created_at = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=20)
    context_at = created_at + timedelta(seconds=2)

    class Journal:
        def __init__(self):
            self.finishes = []
            self.hydrations = []

        def finish(self, signature, *, error=None, retry=False):
            self.finishes.append((signature, error, retry))

        def record_hydration(self, **kwargs):
            self.hydrations.append(kwargs)

    class Rpc:
        async def get_signatures_for_address(self, mint, *, limit, hedge):
            assert hedge is True
            return ([{"signature": "slow-context", "blockTime": int(context_at.timestamp()), "err": None}], "publicnode", 10.0)

        async def get_transaction(self, signature, *, hedge):
            assert hedge is True
            await asyncio.sleep(0.05)
            return {"transaction": True}, "publicnode", 50.0

    class LaunchCollector:
        def __init__(self):
            self.seeded = {}

        def seed_created_at(self, mint, at):
            self.seeded[mint] = at

    class RawCollectors:
        def __init__(self):
            self.launch = LaunchCollector()

        async def refresh_coverage(self, mint, at, *, current_swap=None):
            store.record_program_coverage(
                token_mint=mint,
                pair_created_at=self.launch.seeded[mint].isoformat(),
                assessed_at=at.isoformat(),
                launch_lag_ms=None,
                launch_near_creation=False,
                early_buy_count=0,
                early_buyer_count=0,
                early_buyers_complete=False,
            )

    class Plane:
        def __init__(self):
            self.store = store
            self.journal = Journal()
            self.rpc = Rpc()
            self.raw_collectors = RawCollectors()
            self.service = SimpleNamespace(collectors=SimpleNamespace(inner=self.raw_collectors))

        async def _get_transaction_ready(self, signature, *, hedge, attempts):
            return _initialize_mint_transaction(MINT, block_time=int(created_at.timestamp())), "publicnode", 15.0

        async def _pair_created_at(self, mint):
            raise AssertionError("chain timestamp should be authoritative")

        def _persist_context_swap(self, swap):
            raise AssertionError("timed-out context must not be persisted")

    monkeypatch.setattr(bridge_module, "LAUNCH_CONTEXT_DEADLINE_SECONDS", 0.001)
    plane = Plane()

    async def original(_self, _row):
        raise AssertionError("resolved launch must not use legacy hydrator")

    asyncio.run(
        _hydrate_prospective_launch(
            plane,
            {
                "signature": "launch-signature",
                "trigger_received_at": created_at.isoformat(),
                "source_hint": "PUMP_FUN",
                "priority": 10,
                "reason": "prospective_launch",
                "attempts": 0,
            },
            original,
        )
    )

    assert plane.journal.finishes[-1] == (
        "launch-signature",
        "launch context acquisition incomplete; immutable launch window retained",
        True,
    )
    assert getattr(plane, "_roi_launch_bridge_context_timeouts") == 1
    assert getattr(plane, "_roi_launch_bridge_context_incomplete") == 1
    assert getattr(plane, "_roi_launch_bridge_coverage_rows") == 1


def test_non_launch_rows_still_use_existing_hydrator():
    from solana_roi.launch_coverage_bridge import _launch_aware_hydrator

    calls = []

    async def original(_self, row):
        calls.append(row["reason"])

    wrapped = _launch_aware_hydrator(original)
    asyncio.run(wrapped(object(), {"reason": "frozen_scout_processed_trigger", "source_hint": None}))
    assert calls == ["frozen_scout_processed_trigger"]
