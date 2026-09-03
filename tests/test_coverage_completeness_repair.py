from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from solana_roi.direct_funding import SolanaRpcFundingCollector
from solana_roi.ingestion import WalletProfileRegistry
from solana_roi.launch_funding import DexScreenerLaunchCollector
from solana_roi.observation_store import ObservationEventStore
from solana_roi.risk import EntityResolver, RiskDimension, TokenRiskIntelligence


class NoLaunchHttp:
    async def get(self, _url, **_kwargs):
        raise AssertionError("confirmed chain creation time must bypass pair indexing")


def _risk(tmp_path):
    store = ObservationEventStore(tmp_path / "coverage-v2.sqlite3")
    registry = WalletProfileRegistry(store)
    resolver = EntityResolver(store, registry)
    return store, TokenRiskIntelligence(store, entity_resolver=resolver, registry=registry)


def _swap(store, *, sig, mint, wallet, at, slot=1, sol=1.0):
    store.record_swap(
        signature=sig,
        slot=slot,
        observed_at=at.isoformat(),
        received_at=at.isoformat(),
        wallet=wallet,
        token_mint=mint,
        side="buy",
        token_amount=1000,
        native_amount_sol=sol,
        reference_price_sol=sol / 1000,
        ingestion_latency_ms=0,
        source="test",
    )


def test_live_launch_receipt_and_complete_window_drive_coverage_not_buyer_count(tmp_path):
    store, risk = _risk(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=12)
    # One real buy occurs six seconds after creation. Old semantics called this
    # late and incomplete even though the launch itself was observed immediately.
    _swap(store, sig="buy-1", mint="mint", wallet="buyer-a", at=created + timedelta(seconds=6))

    collector = DexScreenerLaunchCollector(risk, client=NoLaunchHttp())
    collector.seed_coverage_context(
        "mint",
        created_at=created,
        observed_at=created + timedelta(seconds=1),
        complete=True,
    )
    assessed = created + timedelta(seconds=10)

    assert asyncio.run(collector.collect("mint", assessed)) is True
    coverage = store.recent_program_coverage(10)[0]
    assert coverage["launch_near_creation"] is True
    assert coverage["early_buyers_complete"] is True
    assert coverage["early_buy_count"] == 1
    assert coverage["early_buyer_count"] == 1
    assert coverage["launch_lag_ms"] == 1000.0
    assert store.latest_risk_evidence(
        "mint", RiskDimension.LAUNCH.value, as_of_received_at=assessed.isoformat()
    ) is not None


def test_incomplete_launch_window_remains_fail_closed_even_with_three_buyers(tmp_path):
    store, risk = _risk(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=12)
    for index, wallet in enumerate(("a", "b", "c")):
        _swap(
            store,
            sig=f"buy-{wallet}",
            mint="mint",
            wallet=wallet,
            at=created + timedelta(seconds=1 + index * 0.1),
            slot=10 + index,
        )

    collector = DexScreenerLaunchCollector(risk, client=NoLaunchHttp())
    collector.seed_coverage_context(
        "mint",
        created_at=created,
        observed_at=created + timedelta(seconds=1),
        complete=False,
    )
    assessed = created + timedelta(seconds=10)

    assert asyncio.run(collector.collect("mint", assessed)) is False
    coverage = store.recent_program_coverage(10)[0]
    assert coverage["launch_near_creation"] is True
    assert coverage["early_buyers_complete"] is False
    assert store.latest_risk_evidence(
        "mint", RiskDimension.LAUNCH.value, as_of_received_at=assessed.isoformat()
    ) is None


class RecentFundingRpc:
    def __init__(self, *, wallet="buyer-a", funder="funder-a"):
        self.wallet = wallet
        self.funder = funder
        self.hedges: list[tuple[str, bool]] = []
        self.before_at = datetime.now(timezone.utc)

    async def get_signatures_for_address(self, _wallet, *, before=None, limit=1000, hedge=False):
        self.hedges.append(("signatures", bool(hedge)))
        return ([{
            "signature": "funding-tx",
            "blockTime": int((self.before_at - timedelta(minutes=5)).timestamp()),
            "err": None,
        }], "publicnode", 1.0)

    async def get_transaction(self, _signature, *, hedge=False):
        self.hedges.append(("transaction", bool(hedge)))
        return ({
            "blockTime": int((self.before_at - timedelta(minutes=5)).timestamp()),
            "transaction": {
                "message": {
                    "instructions": [{
                        "program": "system",
                        "parsed": {
                            "type": "transfer",
                            "info": {
                                "source": self.funder,
                                "destination": self.wallet,
                                "lamports": 250_000_000,
                            },
                        },
                    }]
                }
            },
            "meta": {"innerInstructions": []},
        }, "publicnode", 1.0)


def test_funding_returns_latest_source_without_scanning_irrelevant_older_history(tmp_path):
    _store, risk = _risk(tmp_path)
    rpc = RecentFundingRpc()
    before_at = rpc.before_at
    collector = SolanaRpcFundingCollector(risk, rpc)

    source, complete = asyncio.run(collector._source_result("buyer-a", before_at))

    assert complete is True
    assert source is not None
    assert source.funder == "funder-a"
    assert rpc.hedges == [("signatures", True), ("transaction", True)]


def test_one_actual_early_buyer_can_have_complete_provenance_when_window_is_complete(tmp_path):
    store, risk = _risk(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=12)
    buy_at = created + timedelta(seconds=1)
    _swap(store, sig="buy-1", mint="mint", wallet="buyer-a", at=buy_at)
    assessed = created + timedelta(seconds=10)
    store.record_program_coverage(
        token_mint="mint",
        pair_created_at=created.isoformat(),
        assessed_at=assessed.isoformat(),
        launch_lag_ms=1000.0,
        launch_near_creation=True,
        early_buy_count=1,
        early_buyer_count=1,
        early_buyers_complete=True,
    )

    rpc = RecentFundingRpc()
    rpc.before_at = buy_at
    collector = SolanaRpcFundingCollector(risk, rpc)
    collector.seed_coverage_context("mint", complete=True)

    assert asyncio.run(collector.collect("mint", assessed)) is True
    coverage = store.recent_program_coverage(10)[0]
    assert coverage["funding_complete"] is True
    evidence = store.latest_risk_evidence(
        "mint", RiskDimension.FUNDING.value, as_of_received_at=assessed.isoformat()
    )
    assert evidence is not None
    assert evidence["payload"]["early_buyer_wallets"] == ["buyer-a"]
