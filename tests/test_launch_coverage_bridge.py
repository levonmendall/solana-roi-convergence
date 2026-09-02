from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import direct_solana as direct_solana_module
from solana_roi.launch_coverage_bridge import (
    _hydrate_prospective_launch,
    launch_mint_from_transaction,
)
from solana_roi.observation_store import ObservationEventStore


MINT = "9xQeWvG816bUx9EPfEZ1dmh7x9Yi4WBQPvWs7xQbX6L"
OTHER_MINT = "So11111111111111111111111111111111111111113"


def _initialize_mint_transaction(mint: str) -> dict:
    return {
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


def test_non_swap_launch_publishes_coverage_without_candidate_latency_or_quote(monkeypatch, tmp_path):
    store = ObservationEventStore(tmp_path / "bridge.sqlite3")
    created_at = datetime.now(timezone.utc) - timedelta(seconds=20)
    context_at = created_at + timedelta(seconds=3)

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
            assert hedge is False
            return ([{"signature": "context-swap", "blockTime": int(context_at.timestamp()), "err": None}], "publicnode", 10.0)

        async def get_transaction(self, signature, *, hedge):
            assert signature == "context-swap"
            assert hedge is False
            return {"transaction": True}, "publicnode", 12.0

    class RawCollectors:
        async def refresh_coverage(self, mint, at, *, current_swap=None):
            assert mint == MINT
            assert current_swap is None
            store.record_program_coverage(
                token_mint=mint,
                pair_created_at=created_at.isoformat(),
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
            self.service = SimpleNamespace(collectors=SimpleNamespace(inner=RawCollectors()))
            self.persisted = []

        async def _get_transaction_ready(self, signature, *, hedge, attempts):
            assert signature == "launch-signature"
            assert hedge is False
            assert attempts == 4
            return _initialize_mint_transaction(MINT), "publicnode", 15.0

        async def _pair_created_at(self, mint):
            assert mint == MINT
            return created_at

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


def test_non_launch_rows_still_use_existing_hydrator():
    from solana_roi.launch_coverage_bridge import _launch_aware_hydrator

    calls = []

    async def original(_self, row):
        calls.append(row["reason"])

    wrapped = _launch_aware_hydrator(original)
    asyncio.run(wrapped(object(), {"reason": "frozen_scout_processed_trigger", "source_hint": None}))
    assert calls == ["frozen_scout_processed_trigger"]
