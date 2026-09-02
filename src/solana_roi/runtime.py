from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .collecting_ingestion import CollectingLiveEvidenceIngestionService
from .engine import PaperTradingEngine
from .ingestion import WalletProfile, WalletProfileRegistry
from .launch_funding import CompleteLiveRiskCollectors, build_complete_live_collectors
from .models import WalletTier
from .observation import LatencyCertificationGate, ShadowPriceClock, TimedRiskCollectors
from .observation_store import ObservationEventStore
from .quote import ExecutableQuoteLedger, JupiterQuoteOnlyClient, QuoteCertificationGate, ShadowExecutableQuoteHandoff
from .risk import EntityResolver, RiskPolicy, TokenRiskIntelligence


@dataclass(slots=True)
class IngestionRuntime:
    store: ObservationEventStore
    engine: PaperTradingEngine
    registry: WalletProfileRegistry
    entity_resolver: EntityResolver
    risk: TokenRiskIntelligence
    raw_collectors: CompleteLiveRiskCollectors
    collectors: TimedRiskCollectors
    price_clock: ShadowPriceClock
    latency_gate: LatencyCertificationGate
    quote_handoff: ShadowExecutableQuoteHandoff
    quote_gate: QuoteCertificationGate
    service: CollectingLiveEvidenceIngestionService
    paper_signal_promotion_enabled: bool = False
    paper_signal_promotion_blocker: str = "latency and amount-specific executable quote handoff are not yet prospectively certified"


def _wallet_profiles_from_env() -> list[WalletProfile]:
    raw = os.getenv("SOLANA_ROI_WALLET_PROFILES_JSON", "").strip()
    if not raw:
        return []
    payload: Any = json.loads(raw)
    if not isinstance(payload, list):
        raise ValueError("SOLANA_ROI_WALLET_PROFILES_JSON must be a JSON array")
    profiles: list[WalletProfile] = []
    now = datetime.now(timezone.utc)
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("wallet profile entries must be JSON objects")
        profiles.append(WalletProfile(
            wallet=str(item["wallet"]),
            entity_id=str(item["entity_id"]),
            tier=WalletTier(str(item["tier"]).upper()),
            first_touch_sample_size=int(item.get("first_touch_sample_size", 0)),
            historically_eligible=bool(item.get("historically_eligible", True)),
            updated_at=now,
        ))
    return profiles


def _quote_client() -> JupiterQuoteOnlyClient | None:
    jupiter = os.getenv("JUPITER_API_KEY", "").strip()
    helius = os.getenv("HELIUS_API_KEY", "").strip()
    if not jupiter or not helius:
        return None
    return JupiterQuoteOnlyClient(jupiter_api_key=jupiter, helius_api_key=helius)


def build_runtime() -> IngestionRuntime:
    store = ObservationEventStore(Path(os.getenv("SOLANA_ROI_DB_PATH", "data/solana-roi.sqlite3")))
    engine = PaperTradingEngine(store=store)
    registry = WalletProfileRegistry(store)
    for profile in _wallet_profiles_from_env():
        registry.register(profile)
    policy = RiskPolicy()
    entity_resolver = EntityResolver(store, registry, min_confidence=policy.confirmed_entity_link_confidence)
    risk = TokenRiskIntelligence(store, entity_resolver=entity_resolver, registry=registry, policy=policy)
    raw_collectors = build_complete_live_collectors(risk)
    collectors = TimedRiskCollectors(raw_collectors, risk=risk, store=store)
    price_clock = ShadowPriceClock(
        store=store,
        engine=engine,
        interval_seconds=float(os.getenv("SOLANA_ROI_SHADOW_CLOCK_SECONDS", "1.0")),
        tracking_horizon_seconds=float(os.getenv("SOLANA_ROI_PRICE_TRACKING_HORIZON_SECONDS", "300")),
        drive_paper_engine=False,
    )
    latency_gate = LatencyCertificationGate(store)
    quote_handoff = ShadowExecutableQuoteHandoff(
        store=store,
        client=_quote_client(),
        full_position_notional_fn=lambda: engine.portfolio.full_position_notional(engine.marks),
        max_chase_fraction=engine.config.max_chase_fraction,
    )
    quote_gate = QuoteCertificationGate(quote_handoff.ledger)
    service = CollectingLiveEvidenceIngestionService(
        engine=engine,
        store=store,
        registry=registry,
        risk_provider=risk,
        entity_resolver=entity_resolver,
        promote_paper_signals=False,
        collectors=collectors,
        mark_recorder=price_clock,
        quote_handoff=quote_handoff,
    )
    return IngestionRuntime(
        store=store,
        engine=engine,
        registry=registry,
        entity_resolver=entity_resolver,
        risk=risk,
        raw_collectors=raw_collectors,
        collectors=collectors,
        price_clock=price_clock,
        latency_gate=latency_gate,
        quote_handoff=quote_handoff,
        quote_gate=quote_gate,
        service=service,
    )
