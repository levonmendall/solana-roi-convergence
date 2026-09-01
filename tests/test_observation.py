from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from solana_roi.collecting_ingestion import CollectingLiveEvidenceIngestionService
from solana_roi.engine import PaperTradingEngine
from solana_roi.ingestion import NormalizedSwap, StaticRiskEvidenceProvider, WalletProfile, WalletProfileRegistry
from solana_roi.models import RiskSnapshot, WalletTier
from solana_roi.observation import DexScreenerSolMarkProvider, LatencyCertificationGate, ShadowPriceClock, TimedRiskCollectors, WSOL_MINT
from solana_roi.observation_store import ObservationEventStore


class FakeRisk:
    def readiness(self, mint, as_of):
        return {"token_mint": mint, "complete": True, "fresh": True, "present": {}, "fresh_dimensions": {}}


class FakeCollectors:
    async def refresh(self, mint, at, current_swap=None): return None
    def status(self): return {"automated_dimensions": ["test"], "still_fail_closed": []}


class FakeResponse:
    def __init__(self, payload): self.payload = payload
    def raise_for_status(self): return None
    def json(self): return self.payload


class FakeHttp:
    def __init__(self, payload): self.payload = payload
    async def get(self, url, **kwargs): return FakeResponse(self.payload)


class FakeMarkProvider:
    def __init__(self, now): self.now = now
    async def mark(self, mint):
        return {"token_mint":mint,"observed_at":self.now,"received_at":self.now,"price_sol":0.002,"source":"fake","source_ref":"pair"}


def test_timed_collectors_persist_actual_completion(tmp_path):
    store = ObservationEventStore(tmp_path / "o.sqlite3")
    base = datetime(2026, 9, 1, tzinfo=timezone.utc)
    times = iter([base + timedelta(seconds=1), base + timedelta(seconds=1.2)])
    perfs = iter([10.0, 10.2])
    wrapper = TimedRiskCollectors(
        FakeCollectors(), risk=FakeRisk(), store=store,
        now_fn=lambda: next(times), perf_fn=lambda: next(perfs),
    )
    swap = NormalizedSwap("sig",1,base,base+timedelta(milliseconds=100),"w","mint","buy",100,1,0.01)
    asyncio.run(wrapper.refresh("mint", swap.received_at, current_swap=swap))
    row = store.recent_risk_refreshes(1)[0]
    assert round(row["elapsed_ms"]) == 200
    assert round(row["end_to_end_ms"]) == 1200
    assert row["complete"] and row["fresh"]


def test_latency_gate_never_auto_activates(tmp_path):
    store = ObservationEventStore(tmp_path / "o.sqlite3")
    at = datetime(2026, 9, 1, tzinfo=timezone.utc)
    for i in range(100):
        t = at + timedelta(seconds=i)
        store.record_risk_refresh(
            token_mint=f"m{i}", trigger_observed_at=t.isoformat(), trigger_received_at=t.isoformat(),
            started_at=t.isoformat(), completed_at=(t+timedelta(milliseconds=1000)).isoformat(),
            elapsed_ms=1000, ingestion_latency_ms=250, end_to_end_ms=1250,
            complete=True, fresh=True, readiness={"complete":True,"fresh":True},
        )
    status = LatencyCertificationGate(store).status()
    assert status["certified"] is True
    assert status["automatic_activation"] is False


def test_dexscreener_mark_requires_wsol_quote_and_uses_deepest(tmp_path):
    now = datetime(2026,9,1,tzinfo=timezone.utc)
    payload = [
        {"chainId":"solana","quoteToken":{"address":"USDC"},"liquidity":{"usd":999999},"priceNative":"0.5","pairAddress":"wrong"},
        {"chainId":"solana","quoteToken":{"address":WSOL_MINT},"liquidity":{"usd":1000},"priceNative":"0.001","pairAddress":"small"},
        {"chainId":"solana","quoteToken":{"address":WSOL_MINT},"liquidity":{"usd":5000},"priceNative":"0.002","pairAddress":"deep"},
    ]
    provider = DexScreenerSolMarkProvider(client=FakeHttp(payload), now_fn=lambda: now)
    mark = asyncio.run(provider.mark("mint"))
    assert mark["price_sol"] == 0.002
    assert mark["source_ref"] == "deep"


def test_shadow_clock_records_swap_and_periodic_marks_without_paper_trade(tmp_path):
    store = ObservationEventStore(tmp_path / "o.sqlite3")
    engine = PaperTradingEngine(store=store)
    now = datetime.now(timezone.utc)
    store.claim_first_touch(token_mint="mint", signature="first", wallet="w", entity_id="e", tier="S", observed_at=now.isoformat(), reference_price_sol=0.001)
    clock = ShadowPriceClock(store=store, engine=engine, provider=FakeMarkProvider(now), drive_paper_engine=False, now_fn=lambda: now)
    swap = NormalizedSwap("sig",1,now,now,"w","mint","buy",1000,1,0.001)
    clock.record_swap_mark(swap)
    assert asyncio.run(clock.tick()) == 1
    assert len(store.recent_price_marks("mint", since_received_at=(now-timedelta(seconds=1)).isoformat())) == 2
    assert engine.portfolio.positions == {}


def test_collecting_service_blocks_optimistic_post_risk_fill(tmp_path):
    store = ObservationEventStore(tmp_path / "o.sqlite3")
    engine = PaperTradingEngine(store=store)
    now = datetime.now(timezone.utc)
    registry = WalletProfileRegistry(store)
    registry.register(WalletProfile("scout","entity",WalletTier.S,100,True,now))
    clock = ShadowPriceClock(store=store, engine=engine, drive_paper_engine=False, now_fn=lambda: now+timedelta(seconds=1))
    service = CollectingLiveEvidenceIngestionService(
        engine=engine, store=store, registry=registry,
        risk_provider=StaticRiskEvidenceProvider(RiskSnapshot(observed_at=now)),
        collectors=FakeCollectors(), mark_recorder=clock, promote_paper_signals=True,
        decision_clock=lambda: now+timedelta(seconds=1),
    )
    swap = NormalizedSwap("sig",1,now,now,"scout","mint","buy",1000,1,0.001)
    decision = asyncio.run(service.ingest_swap(swap))
    assert decision.decision == "record_only"
    assert "post-risk reference-price" in decision.reason
    assert engine.portfolio.positions == {}
