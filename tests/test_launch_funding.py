from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from solana_roi.ingestion import WalletProfileRegistry
from solana_roi.launch_funding import DexScreenerLaunchCollector, HeliusFundingCollector, LaunchFundingPolicy
from solana_roi.risk import EntityResolver, RiskDimension, TokenRiskIntelligence
from solana_roi.storage import AppendOnlyEventStore


class FakeResponse:
    def __init__(self, payload): self.payload = payload
    def raise_for_status(self): return None
    def json(self): return self.payload


class LaunchHttp:
    def __init__(self, created_ms): self.created_ms = created_ms
    async def get(self, url, **kwargs):
        return FakeResponse([{"chainId":"solana","pairCreatedAt":self.created_ms,"liquidity":{"usd":5000}}])


class NoLaunchHttp:
    async def get(self, url, **kwargs):
        raise AssertionError("seeded chain timestamp must bypass external pair discovery")


class FundingHttp:
    def __init__(self, transfers):
        self.transfers = transfers
        self.calls = {}
    async def get(self, url, **kwargs):
        wallet = url.split("/addresses/")[1].split("/transactions")[0]
        count = self.calls.get(wallet, 0)
        self.calls[wallet] = count + 1
        if count > 0:
            return FakeResponse([])
        return FakeResponse([self.transfers[wallet]])


def _risk(tmp_path):
    store = AppendOnlyEventStore(tmp_path / "lf.sqlite3")
    registry = WalletProfileRegistry(store)
    resolver = EntityResolver(store, registry)
    return store, TokenRiskIntelligence(store, entity_resolver=resolver, registry=registry)


def _swap(store, *, sig, mint, wallet, side, slot, at, sol):
    store.record_swap(signature=sig, slot=slot, observed_at=at.isoformat(), received_at=at.isoformat(), wallet=wallet, token_mint=mint, side=side, token_amount=1000, native_amount_sol=sol, reference_price_sol=sol/1000, ingestion_latency_ms=0, source="test")


def test_launch_requires_pair_alignment_and_flags_same_slot_cluster(tmp_path):
    store, risk = _risk(tmp_path)
    created = datetime.now(timezone.utc) - timedelta(seconds=12)
    for i, wallet in enumerate(("a","b","c")):
        _swap(store, sig=str(i), mint="mint", wallet=wallet, side="buy", slot=10, at=created+timedelta(seconds=1+i*0.1), sol=1.0)
    collector = DexScreenerLaunchCollector(risk, client=LaunchHttp(int(created.timestamp()*1000)))
    at = created + timedelta(seconds=10)
    assert asyncio.run(collector.collect("mint", at))
    row = store.latest_risk_evidence("mint", RiskDimension.LAUNCH.value, as_of_received_at=at.isoformat())
    assert row["payload"]["bundled_launch"] is True
    assert row["payload"]["sniper_heavy"] is True


def test_seeded_chain_timestamp_removes_pair_indexer_from_critical_path(tmp_path):
    store, risk = _risk(tmp_path)
    created = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=12)
    for i, wallet in enumerate(("a", "b", "c")):
        _swap(
            store,
            sig=f"seeded-{i}",
            mint="mint",
            wallet=wallet,
            side="buy",
            slot=11,
            at=created + timedelta(seconds=1 + i * 0.1),
            sol=1.0,
        )
    collector = DexScreenerLaunchCollector(risk, client=NoLaunchHttp())
    collector.seed_created_at("mint", created)
    at = created + timedelta(seconds=10)
    assert asyncio.run(collector.collect("mint", at))
    coverage = store.recent_program_coverage(10)[0]
    assert coverage["pair_created_at"] == created.isoformat()


def test_launch_refuses_stream_that_started_late(tmp_path):
    store, risk = _risk(tmp_path)
    created = datetime.now(timezone.utc) - timedelta(seconds=20)
    for i, wallet in enumerate(("a","b","c")):
        _swap(store, sig=str(i), mint="mint", wallet=wallet, side="buy", slot=20+i, at=created+timedelta(seconds=6+i*0.1), sol=1.0)
    collector = DexScreenerLaunchCollector(risk, client=LaunchHttp(int(created.timestamp()*1000)))
    assert asyncio.run(collector.collect("mint", created+timedelta(seconds=15))) is False
    assert store.latest_risk_evidence("mint", RiskDimension.LAUNCH.value, as_of_received_at=(created+timedelta(seconds=15)).isoformat()) is None


def test_funding_requires_complete_history_and_links_strong_common_funder(tmp_path):
    store, risk = _risk(tmp_path)
    now = datetime.now(timezone.utc)
    buyers = ("a","b","c")
    for i, wallet in enumerate(buyers):
        _swap(store, sig=f"buy-{wallet}", mint="mint", wallet=wallet, side="buy", slot=30+i, at=now-timedelta(seconds=10-i), sol=1.0)
    ts = int((now-timedelta(minutes=5)).timestamp())
    transfers = {
        "a":{"signature":"fa","timestamp":ts,"nativeTransfers":[{"fromUserAccount":"funder","toUserAccount":"a","amount":1_000_000_000}]},
        "b":{"signature":"fb","timestamp":ts+30,"nativeTransfers":[{"fromUserAccount":"funder","toUserAccount":"b","amount":1_010_000_000}]},
        "c":{"signature":"fc","timestamp":ts+60,"nativeTransfers":[{"fromUserAccount":"other","toUserAccount":"c","amount":2_000_000_000}]},
    }
    policy = LaunchFundingPolicy(funding_early_buyer_count=3, max_history_pages=3)
    collector = HeliusFundingCollector(risk, "key", client=FundingHttp(transfers), policy=policy)
    assert asyncio.run(collector.collect("mint", now))
    row = store.latest_risk_evidence("mint", RiskDimension.FUNDING.value, as_of_received_at=now.isoformat())
    assert set(row["payload"]["early_buyer_wallets"]) == set(buyers)
    assert risk.entity_resolver.same_entity("a", "b", as_of=now)
    assert not risk.entity_resolver.same_entity("a", "c", as_of=now)
