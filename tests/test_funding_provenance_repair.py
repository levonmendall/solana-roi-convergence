from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import funding_provenance_repair as funding
from solana_roi.direct_funding import SolanaRpcFundingCollector
from solana_roi.ingestion import WalletProfileRegistry
from solana_roi.launch_funding import LaunchFundingPolicy
from solana_roi.observation_store import ObservationEventStore
from solana_roi.risk import EntityResolver, RiskDimension, TokenRiskIntelligence


def _create_account_tx(wallet: str, funder: str, block_time: int) -> dict:
    return {
        "blockTime": block_time,
        "transaction": {
            "message": {
                "instructions": [
                    {
                        "program": "system",
                        "parsed": {
                            "type": "createAccount",
                            "info": {
                                "source": funder,
                                "newAccount": wallet,
                                "lamports": 250_000_000,
                            },
                        },
                    }
                ]
            }
        },
        "meta": {"innerInstructions": []},
    }


def test_system_create_account_lamports_are_real_funding_provenance():
    rows = funding._native_inbound_transfers_extended(
        _create_account_tx("buyer-a", "funder-a", 1),
        "buyer-a",
    )
    assert rows == [("funder-a", 250_000_000)]


class SameSecondRpc:
    def __init__(self, *, buy_at: datetime, row_slot: int, fail_first_signatures: bool = False):
        self.buy_at = buy_at
        self.row_slot = row_slot
        self.fail_first_signatures = fail_first_signatures
        self.signature_calls = 0

    async def get_signatures_for_address(self, _wallet, *, before=None, limit=1000, hedge=False):
        assert hedge is True
        assert limit == 1000
        self.signature_calls += 1
        if self.fail_first_signatures and self.signature_calls == 1:
            raise TimeoutError("transient public RPC timeout")
        return (
            [
                {
                    "signature": "funding-tx",
                    "slot": self.row_slot,
                    "blockTime": int(self.buy_at.timestamp()),
                    "err": None,
                }
            ],
            "publicnode",
            5.0,
        )

    async def get_transaction(self, _signature, *, hedge=False):
        assert hedge is True
        return (
            _create_account_tx("buyer-a", "funder-a", int(self.buy_at.timestamp())),
            "publicnode",
            5.0,
        )


def _source_collector(rpc):
    return SimpleNamespace(
        rpc=rpc,
        policy=LaunchFundingPolicy(),
    )


def test_same_second_funding_is_accepted_only_from_strictly_earlier_slot():
    buy_at = datetime.now(timezone.utc).replace(microsecond=700000)
    collector = _source_collector(SameSecondRpc(buy_at=buy_at, row_slot=99))

    source, complete, reason = asyncio.run(
        funding._funding_source_result_slot_aware(
            collector,
            "buyer-a",
            buy_at,
            100,
        )
    )

    assert complete is True
    assert reason == "latest_qualifying_source_found"
    assert source is not None
    assert source.funder == "funder-a"
    assert getattr(collector, "_roi_funding_provenance_same_second_prebuy_rows") == 1


def test_same_second_same_or_later_slot_is_excluded():
    buy_at = datetime.now(timezone.utc).replace(microsecond=700000)
    collector = _source_collector(SameSecondRpc(buy_at=buy_at, row_slot=100))

    source, complete, reason = asyncio.run(
        funding._funding_source_result_slot_aware(
            collector,
            "buyer-a",
            buy_at,
            100,
        )
    )

    assert complete is True
    assert source is None
    assert reason == "provider_history_exhausted_short_page"


def test_transient_signature_rpc_failure_recovers_inside_bounded_read_attempts():
    buy_at = datetime.now(timezone.utc).replace(microsecond=700000)
    rpc = SameSecondRpc(buy_at=buy_at, row_slot=99, fail_first_signatures=True)
    collector = _source_collector(rpc)

    source, complete, _reason = asyncio.run(
        funding._funding_source_result_slot_aware(
            collector,
            "buyer-a",
            buy_at,
            100,
        )
    )

    assert complete is True
    assert source is not None
    assert rpc.signature_calls == 2
    assert funding.FUNDING_RPC_ATTEMPTS == 2
    assert collector.policy.max_history_pages == 5
    assert collector.policy.min_funding_transfer_sol == 0.05


def _risk(tmp_path):
    store = ObservationEventStore(tmp_path / "funding-v3.sqlite3")
    registry = WalletProfileRegistry(store)
    resolver = EntityResolver(store, registry)
    return store, TokenRiskIntelligence(store, entity_resolver=resolver, registry=registry)


def test_complete_launch_window_with_one_buyer_can_complete_slot_aware_funding(tmp_path):
    store, risk = _risk(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=12)
    buy_at = created + timedelta(seconds=1, microseconds=700000)
    assessed = created + timedelta(seconds=10)
    store.record_swap(
        signature="buy-1",
        slot=100,
        observed_at=buy_at.isoformat(),
        received_at=buy_at.isoformat(),
        wallet="buyer-a",
        token_mint="mint",
        side="buy",
        token_amount=1000,
        native_amount_sol=1.0,
        reference_price_sol=0.001,
        ingestion_latency_ms=0,
        source="test",
    )
    store.record_program_coverage(
        token_mint="mint",
        pair_created_at=created.isoformat(),
        assessed_at=assessed.isoformat(),
        launch_lag_ms=0.0,
        launch_near_creation=True,
        early_buy_count=1,
        early_buyer_count=1,
        early_buyers_complete=True,
    )

    rpc = SameSecondRpc(buy_at=buy_at, row_slot=99)
    collector = SolanaRpcFundingCollector(risk, rpc)
    collector.seed_coverage_context("mint", complete=True)

    assert asyncio.run(collector.collect("mint", assessed)) is True
    coverage = store.recent_program_coverage(10)[0]
    assert coverage["funding_complete"] is True
    evidence = store.latest_risk_evidence(
        "mint",
        RiskDimension.FUNDING.value,
        as_of_received_at=assessed.isoformat(),
    )
    assert evidence is not None
    assert evidence["payload"]["early_buyer_wallets"] == ["buyer-a"]
