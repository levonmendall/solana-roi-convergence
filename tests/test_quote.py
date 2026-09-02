from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi.collecting_ingestion import CollectingLiveEvidenceIngestionService
from solana_roi.engine import PaperTradingEngine
from solana_roi.ingestion import NormalizedSwap, StaticRiskEvidenceProvider, WalletProfile, WalletProfileRegistry
from solana_roi.models import RiskSnapshot, WalletTier
from solana_roi.observation_store import ObservationEventStore
from solana_roi.quote import ExecutableQuote, ExecutableQuoteLedger, JupiterQuoteOnlyClient, QuoteCertificationGate, ShadowExecutableQuoteHandoff, USDC_MINT


class FakeResponse:
    def __init__(self, payload): self.payload = payload
    def raise_for_status(self): return None
    def json(self): return self.payload


class FakeJupiterHttp:
    async def get(self, url, **kwargs):
        params = kwargs["params"]
        if params["outputMint"] == USDC_MINT:
            return FakeResponse({"outAmount":"20000000","router":"metis","feeBps":2})
        return FakeResponse({"outAmount":"6250000000","router":"metis","feeBps":50})


class FakeRpc:
    async def call(self, method, params):
        assert method == "getTokenSupply"
        return {"value":{"decimals":6}}


class FakeCollectors:
    async def refresh(self, mint, at, current_swap=None): return None
    def status(self): return {}


class FakeQuoteHandoff:
    def __init__(self): self.calls = []
    async def observe(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(usable=True, effective_price_sol=0.00105, reason="within chase ceiling")


def test_jupiter_quote_is_amount_specific_and_quote_only():
    base = datetime(2026,9,1,tzinfo=timezone.utc)
    perfs = iter([1.0, 1.1])
    client = JupiterQuoteOnlyClient(
        jupiter_api_key="jup", helius_api_key="helius", client=FakeJupiterHttp(), rpc=FakeRpc(),
        now_fn=lambda: base, perf_fn=lambda: next(perfs),
    )
    quote = asyncio.run(client.quote_buy(
        token_mint="mint", stage="starter", notional_usd=12.5,
        scout_reference_price_sol=0.0000095, trigger_observed_at=base-timedelta(seconds=1),
        max_chase_fraction=0.15,
    ))
    assert round(quote.input_sol, 6) == 0.0625
    assert quote.output_token_units == 6250.0
    assert round(quote.effective_price_sol, 8) == 0.00001
    assert quote.router == "metis"
    assert quote.fee_bps == 50
    assert quote.usable is True
    assert round(quote.chain_to_quote_ms) == 1000


def test_quote_gate_is_measurement_only(tmp_path):
    store = ObservationEventStore(tmp_path / "q.sqlite3")
    ledger = ExecutableQuoteLedger(store)
    base = datetime(2026,9,1,tzinfo=timezone.utc)
    for i in range(100):
        ledger.record(ExecutableQuote(
            token_mint=f"m{i}", stage="starter", requested_notional_usd=3.75,
            input_sol=.02, sol_usd=187.5, output_token_units=1000,
            effective_price_sol=.00002, scout_reference_price_sol=.000019,
            drift_fraction=.0526, router="metis", fee_bps=50, token_decimals=6,
            quoted_at=base, received_at=base, quote_latency_ms=500,
            chain_to_quote_ms=1500, usable=True, reason="within chase ceiling",
        ))
    status = QuoteCertificationGate(ledger).status()
    assert status["certified"] is True
    assert status["automatic_activation"] is False


def test_handoff_sizes_starter_from_current_paper_nav(tmp_path):
    store = ObservationEventStore(tmp_path / "q.sqlite3")
    engine = PaperTradingEngine(store=store)
    client = SimpleNamespace()
    calls = []
    async def quote_buy(**kwargs):
        calls.append(kwargs)
        return ExecutableQuote(
            token_mint=kwargs["token_mint"], stage=kwargs["stage"], requested_notional_usd=kwargs["notional_usd"],
            input_sol=.02, sol_usd=187.5, output_token_units=1000, effective_price_sol=.00002,
            scout_reference_price_sol=kwargs["scout_reference_price_sol"], drift_fraction=.05,
            router="metis", fee_bps=50, token_decimals=6,
            quoted_at=datetime.now(timezone.utc), received_at=datetime.now(timezone.utc), quote_latency_ms=100,
            chain_to_quote_ms=500, usable=True, reason="within chase ceiling",
        )
    client.quote_buy = quote_buy
    handoff = ShadowExecutableQuoteHandoff(
        store=store, client=client,
        full_position_notional_fn=lambda: engine.portfolio.full_position_notional(engine.marks),
        max_chase_fraction=engine.config.max_chase_fraction,
    )
    asyncio.run(handoff.observe(token_mint="mint",stage="starter",fraction_of_full_position=.3,scout_reference_price_sol=.000019,trigger_observed_at=datetime.now(timezone.utc)))
    assert round(calls[0]["notional_usd"], 2) == 3.75


def test_clean_s_tier_shadow_touch_requests_starter_quote(tmp_path):
    store = ObservationEventStore(tmp_path / "q.sqlite3")
    engine = PaperTradingEngine(store=store)
    now = datetime.now(timezone.utc)
    registry = WalletProfileRegistry(store)
    registry.register(WalletProfile("scout","entity",WalletTier.S,100,True,now))
    quote = FakeQuoteHandoff()
    service = CollectingLiveEvidenceIngestionService(
        engine=engine, store=store, registry=registry,
        risk_provider=StaticRiskEvidenceProvider(RiskSnapshot(observed_at=now)),
        collectors=FakeCollectors(), quote_handoff=quote, promote_paper_signals=False,
        decision_clock=lambda: now,
    )
    swap = NormalizedSwap("sig",1,now,now,"scout","mint","buy",1000,1,0.001)
    decision = asyncio.run(service.ingest_swap(swap))
    assert decision.decision == "shadow_first_touch"
    assert quote.calls[0]["stage"] == "starter"
    assert quote.calls[0]["fraction_of_full_position"] == .3
    assert engine.portfolio.positions == {}


def test_premature_paper_promotion_flag_still_fails_closed(tmp_path):
    store = ObservationEventStore(tmp_path / "q.sqlite3")
    engine = PaperTradingEngine(store=store)
    now = datetime.now(timezone.utc)
    registry = WalletProfileRegistry(store)
    registry.register(WalletProfile("scout","entity",WalletTier.S,100,True,now))
    quote = FakeQuoteHandoff()
    service = CollectingLiveEvidenceIngestionService(
        engine=engine, store=store, registry=registry,
        risk_provider=StaticRiskEvidenceProvider(RiskSnapshot(observed_at=now)),
        collectors=FakeCollectors(), quote_handoff=quote, promote_paper_signals=True,
        decision_clock=lambda: now,
    )
    swap = NormalizedSwap("sig-promote",1,now,now,"scout","mint-promote","buy",1000,1,0.001)
    decision = asyncio.run(service.ingest_swap(swap))
    assert decision.decision == "record_only"
    assert "final forward-cohort activation gate" in decision.reason
    assert quote.calls == []
    assert engine.portfolio.positions == {}
