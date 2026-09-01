from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from solana_roi.engine import PaperTradingEngine
from solana_roi.ingestion import HeliusEnhancedWebhookParser, LiveEvidenceIngestionService, NormalizedSwap, StaticRiskEvidenceProvider, WalletProfile, WalletProfileRegistry
from solana_roi.models import CandidateStatus, RiskSnapshot, WalletTier
from solana_roi.storage import AppendOnlyEventStore


def _swap(signature: str, wallet: str, at: datetime, price: float = 0.001) -> NormalizedSwap:
    return NormalizedSwap(signature, 1, at, at + timedelta(milliseconds=250), wallet, "TOKEN", "buy", 1000.0, price * 1000.0, price)


def test_helius_parser_normalizes_sol_buy():
    payload = [{"type":"SWAP","signature":"sig","slot":123,"timestamp":1788283200,"feePayer":"wallet","events":{"swap":{"nativeInput":{"amount":"1000000000"},"tokenOutputs":[{"userAccount":"wallet","mint":"mint","rawTokenAmount":{"tokenAmount":"2000000","decimals":6}}]}}}]
    rows = HeliusEnhancedWebhookParser().parse(payload, received_at=datetime(2026,9,1,12,0,1,tzinfo=timezone.utc))
    assert len(rows) == 1
    assert rows[0].wallet == "wallet"
    assert rows[0].token_mint == "mint"
    assert rows[0].token_amount == 2.0
    assert rows[0].reference_price_sol == 0.5


def test_risk_unavailable_is_record_only(tmp_path):
    store = AppendOnlyEventStore(tmp_path / "e.sqlite3")
    registry = WalletProfileRegistry(store)
    now = datetime.now(timezone.utc)
    registry.register(WalletProfile("scout", "entity-1", WalletTier.S, 100, True, now))
    engine = PaperTradingEngine(store=store)
    service = LiveEvidenceIngestionService(engine=engine, store=store, registry=registry)
    decision = asyncio.run(service.ingest_swap(_swap("sig-1", "scout", now)))
    assert decision.decision == "record_only"
    assert store.evidence_counts()["normalized_swaps"] == 1
    assert store.evidence_counts()["token_first_touches"] == 0
    assert engine.strategy.candidates == {}


def test_clean_independent_confirmation_and_duplicate_idempotency(tmp_path):
    store = AppendOnlyEventStore(tmp_path / "e.sqlite3")
    registry = WalletProfileRegistry(store)
    now = datetime.now(timezone.utc)
    registry.register(WalletProfile("scout", "entity-scout", WalletTier.S, 100, True, now))
    registry.register(WalletProfile("confirm", "entity-confirm", WalletTier.A, 80, True, now))
    engine = PaperTradingEngine(store=store)
    service = LiveEvidenceIngestionService(engine=engine, store=store, registry=registry, risk_provider=StaticRiskEvidenceProvider(RiskSnapshot(observed_at=now)))
    first = asyncio.run(service.ingest_swap(_swap("sig-1", "scout", now)))
    second_swap = _swap("sig-2", "confirm", now + timedelta(seconds=8), 0.0011)
    second = asyncio.run(service.ingest_swap(second_swap))
    duplicate = asyncio.run(service.ingest_swap(second_swap))
    assert first.decision == "first_touch"
    assert second.decision == "confirmation"
    assert duplicate.decision == "duplicate"
    assert engine.strategy.candidates["TOKEN"].status is CandidateStatus.CONFIRMED
    assert store.evidence_counts()["normalized_swaps"] == 2
    assert store.verify()
