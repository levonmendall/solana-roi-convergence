from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from solana_roi.collecting_ingestion import CollectingLiveEvidenceIngestionService
from solana_roi.engine import PaperTradingEngine
from solana_roi.ingestion import NormalizedSwap, WalletProfile, WalletProfileRegistry
from solana_roi.live_collectors import (
    DexScreenerLiquidityCollector,
    HeliusAuthorityCollector,
    HeliusDeployerCollector,
    LiveRiskCollectors,
    PersistedSwapFlowCollector,
)
from solana_roi.models import WalletTier
from solana_roi.risk import EntityResolver, RiskDimension, TokenRiskIntelligence
from solana_roi.storage import AppendOnlyEventStore


class FakeRpc:
    def __init__(self, responses):
        self.responses = responses
    async def call(self, method, params):
        value = self.responses[method]
        return value(params) if callable(value) else value


class FakeResponse:
    def __init__(self, payload): self.payload = payload
    def raise_for_status(self): return None
    def json(self): return self.payload


class FakeHttp:
    def __init__(self, payload): self.payload = payload
    async def get(self, url, **kwargs): return FakeResponse(self.payload)


def _risk(tmp_path):
    store = AppendOnlyEventStore(tmp_path / "live.sqlite3")
    registry = WalletProfileRegistry(store)
    resolver = EntityResolver(store, registry)
    return store, registry, TokenRiskIntelligence(store, entity_resolver=resolver, registry=registry)


def test_authority_collector_records_parsed_mint(tmp_path):
    _, _, risk = _risk(tmp_path)
    rpc = FakeRpc({"getAccountInfo": {"value": {"data": {"parsed": {"info": {"mintAuthority": None, "freezeAuthority": "freeze"}}}}}})
    now = datetime.now(timezone.utc)
    assert asyncio.run(HeliusAuthorityCollector(risk, rpc).collect("mint", now))
    row = risk.store.latest_risk_evidence("mint", RiskDimension.AUTHORITY.value, as_of_received_at=now.isoformat())
    assert row["payload"] == {"freeze_authority_active": True, "mint_authority_active": False}


def test_liquidity_uses_deepest_single_pool(tmp_path):
    _, _, risk = _risk(tmp_path)
    http = FakeHttp([
        {"chainId": "solana", "liquidity": {"usd": 1200}, "marketCap": 50000},
        {"chainId": "solana", "liquidity": {"usd": 4000}, "marketCap": 100000},
    ])
    now = datetime.now(timezone.utc)
    assert asyncio.run(DexScreenerLiquidityCollector(risk, client=http).collect("mint", now))
    row = risk.store.latest_risk_evidence("mint", RiskDimension.LIQUIDITY.value, as_of_received_at=now.isoformat())
    assert row["payload"]["liquidity_usd"] == 4000.0
    assert row["payload"]["market_cap_usd"] == 100000.0


def test_deployer_requires_exhausted_history(tmp_path):
    _, _, risk = _risk(tmp_path)
    rpc = FakeRpc({
        "getSignaturesForAddress": [{"signature": "new"}, {"signature": "birth"}],
        "getTransaction": {"meta": {"err": None}, "transaction": {"message": {"accountKeys": [{"pubkey": "creator", "signer": True}]}}},
    })
    now = datetime.now(timezone.utc)
    assert asyncio.run(HeliusDeployerCollector(risk, rpc).collect("mint", now))
    row = risk.store.latest_risk_evidence("mint", RiskDimension.DEPLOYER.value, as_of_received_at=now.isoformat())
    assert row["payload"]["deployer_wallet"] == "creator"


def test_flow_excludes_future_swaps(tmp_path):
    store, _, risk = _risk(tmp_path)
    now = datetime.now(timezone.utc)
    def record(sig, wallet, side, seconds, sol):
        at = now + timedelta(seconds=seconds)
        store.record_swap(signature=sig, slot=1, observed_at=at.isoformat(), received_at=at.isoformat(), wallet=wallet, token_mint="mint", side=side, token_amount=100, native_amount_sol=sol, reference_price_sol=sol/100, ingestion_latency_ms=0, source="test")
    record("1", "a", "buy", 0, 1.0)
    record("2", "b", "buy", 1, 1.0)
    record("3", "a", "sell", 2, 0.7)
    record("future", "c", "sell", 20, 10.0)
    at = now + timedelta(seconds=3)
    assert asyncio.run(PersistedSwapFlowCollector(risk).collect("mint", at))
    row = risk.store.latest_risk_evidence("mint", RiskDimension.FLOW.value, as_of_received_at=at.isoformat())
    assert row["payload"]["early_buyers_exiting"] is True
    assert row["payload"]["abnormal_sell_pressure"] is False


def test_first_touch_is_frozen_before_complete_risk(tmp_path):
    store, registry, risk = _risk(tmp_path)
    now = datetime.now(timezone.utc)
    registry.register(WalletProfile("scout", "entity-s", WalletTier.S, 100, True, now))
    collectors = LiveRiskCollectors(risk, authority=None, liquidity=None, deployer=None, flow=PersistedSwapFlowCollector(risk))
    service = CollectingLiveEvidenceIngestionService(
        engine=PaperTradingEngine(store=store), store=store, registry=registry,
        risk_provider=risk, collectors=collectors, promote_paper_signals=False,
    )
    swap = NormalizedSwap("sig", 1, now, now, "scout", "mint", "buy", 1000, 1, 0.001)
    decision = asyncio.run(service.ingest_swap(swap))
    assert decision.decision == "record_only"
    assert store.first_touch("mint")["wallet"] == "scout"
    assert "original first touch preserved" in decision.reason
