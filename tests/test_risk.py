from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from solana_roi.engine import PaperTradingEngine
from solana_roi.ingestion import LiveEvidenceIngestionService, NormalizedSwap, StaticRiskEvidenceProvider, WalletProfile, WalletProfileRegistry
from solana_roi.models import RiskSnapshot, WalletTier
from solana_roi.risk import AuthorityEvidence, DeployerEvidence, EntityLink, EntityResolver, FlowEvidence, FundingEvidence, LaunchEvidence, LiquidityEvidence, TokenRiskIntelligence
from solana_roi.storage import AppendOnlyEventStore


def _registry(store: AppendOnlyEventStore, now: datetime) -> WalletProfileRegistry:
    registry = WalletProfileRegistry(store)
    registry.register(WalletProfile("scout", "entity-scout", WalletTier.S, 100, True, now))
    registry.register(WalletProfile("confirm", "entity-confirm", WalletTier.A, 80, True, now))
    return registry


def _complete(risk: TokenRiskIntelligence, token: str, at: datetime, *, received_at: datetime | None = None, liquidity: float = 5_000.0, market_cap: float | None = 100_000.0, deployer: str | None = None) -> None:
    risk.record_bundle(
        token,
        authority=AuthorityEvidence(False, False),
        liquidity=LiquidityEvidence(liquidity, market_cap),
        launch=LaunchEvidence(False, False),
        flow=FlowEvidence(False, False),
        funding=FundingEvidence(("buyer-a", "buyer-b")),
        deployer=DeployerEvidence(deployer),
        observed_at=at,
        received_at=received_at or at,
        source="test",
    )


def test_incomplete_risk_fails_closed(tmp_path):
    store = AppendOnlyEventStore(tmp_path / "risk.sqlite3")
    now = datetime.now(timezone.utc)
    registry = _registry(store, now)
    resolver = EntityResolver(store, registry)
    risk = TokenRiskIntelligence(store, registry=registry, entity_resolver=resolver)
    risk.record_authority("TOKEN", AuthorityEvidence(False, False), observed_at=now, received_at=now, source="test")
    assert asyncio.run(risk.snapshot("TOKEN", now, scout_wallet="scout", scout_entity_id="entity-scout")) is None


def test_risk_is_point_in_time_by_received_timestamp(tmp_path):
    store = AppendOnlyEventStore(tmp_path / "risk.sqlite3")
    now = datetime.now(timezone.utc)
    registry = _registry(store, now)
    resolver = EntityResolver(store, registry)
    risk = TokenRiskIntelligence(store, registry=registry, entity_resolver=resolver)
    _complete(risk, "TOKEN", now + timedelta(seconds=9), received_at=now + timedelta(seconds=10))
    assert asyncio.run(risk.snapshot("TOKEN", now + timedelta(seconds=5), scout_wallet="scout", scout_entity_id="entity-scout")) is None
    snapshot = asyncio.run(risk.snapshot("TOKEN", now + timedelta(seconds=10), scout_wallet="scout", scout_entity_id="entity-scout"))
    assert snapshot is not None and snapshot.clean


def test_liquidity_and_authority_vetoes(tmp_path):
    store = AppendOnlyEventStore(tmp_path / "risk.sqlite3")
    now = datetime.now(timezone.utc)
    registry = _registry(store, now)
    resolver = EntityResolver(store, registry)
    risk = TokenRiskIntelligence(store, registry=registry, entity_resolver=resolver)
    risk.record_bundle(
        "TOKEN",
        authority=AuthorityEvidence(True, False),
        liquidity=LiquidityEvidence(1_000.0, 100_000.0),
        launch=LaunchEvidence(False, False),
        flow=FlowEvidence(False, False),
        funding=FundingEvidence(()),
        deployer=DeployerEvidence(None),
        observed_at=now,
        received_at=now,
        source="test",
    )
    snapshot = asyncio.run(risk.snapshot("TOKEN", now, scout_wallet="scout", scout_entity_id="entity-scout"))
    assert snapshot is not None
    assert snapshot.dangerous_authority
    assert snapshot.unacceptable_liquidity
    assert not snapshot.clean


def test_high_confidence_entity_link_collapses_addresses(tmp_path):
    store = AppendOnlyEventStore(tmp_path / "risk.sqlite3")
    now = datetime.now(timezone.utc)
    registry = _registry(store, now)
    resolver = EntityResolver(store, registry)
    assert resolver.record_link(EntityLink("scout", "confirm", "confirmed_common_controller", 0.99, now, now, "test"))
    assert resolver.same_entity("scout", "confirm", as_of=now, fallback_entity_a="entity-scout", fallback_entity_b="entity-confirm")


def test_low_confidence_entity_link_does_not_collapse_addresses(tmp_path):
    store = AppendOnlyEventStore(tmp_path / "risk.sqlite3")
    now = datetime.now(timezone.utc)
    registry = _registry(store, now)
    resolver = EntityResolver(store, registry)
    assert resolver.record_link(EntityLink("scout", "confirm", "suspected_common_funder", 0.70, now, now, "test"))
    assert not resolver.same_entity("scout", "confirm", as_of=now, fallback_entity_a="entity-scout", fallback_entity_b="entity-confirm")


def test_scout_deployer_and_common_funded_cluster_veto(tmp_path):
    store = AppendOnlyEventStore(tmp_path / "risk.sqlite3")
    now = datetime.now(timezone.utc)
    registry = _registry(store, now)
    registry.register(WalletProfile("deployer", "entity-deployer", WalletTier.REJECT, 0, False, now))
    registry.register(WalletProfile("buyer-a", "entity-a", WalletTier.REJECT, 0, False, now))
    registry.register(WalletProfile("buyer-b", "entity-b", WalletTier.REJECT, 0, False, now))
    resolver = EntityResolver(store, registry)
    resolver.record_link(EntityLink("scout", "deployer", "confirmed_control", 1.0, now, now, "test"))
    resolver.record_link(EntityLink("buyer-a", "buyer-b", "confirmed_common_funder", 0.99, now, now, "test"))
    risk = TokenRiskIntelligence(store, registry=registry, entity_resolver=resolver)
    _complete(risk, "TOKEN", now, deployer="deployer")
    snapshot = asyncio.run(risk.snapshot("TOKEN", now, scout_wallet="scout", scout_entity_id="entity-scout"))
    assert snapshot is not None
    assert snapshot.scout_deployer_connection
    assert snapshot.common_funded_early_wallet_cluster


def test_shadow_mode_records_signal_without_changing_paper_account(tmp_path):
    store = AppendOnlyEventStore(tmp_path / "risk.sqlite3")
    now = datetime.now(timezone.utc)
    registry = _registry(store, now)
    engine = PaperTradingEngine(store=store)
    service = LiveEvidenceIngestionService(
        engine=engine,
        store=store,
        registry=registry,
        risk_provider=StaticRiskEvidenceProvider(RiskSnapshot(observed_at=now)),
        promote_paper_signals=False,
    )
    swap = NormalizedSwap("sig", 1, now, now + timedelta(milliseconds=100), "scout", "TOKEN", "buy", 1000.0, 1.0, 0.001)
    decision = asyncio.run(service.ingest_swap(swap))
    assert decision.decision == "shadow_first_touch"
    assert engine.strategy.candidates == {}
    assert engine.portfolio.positions == {}
