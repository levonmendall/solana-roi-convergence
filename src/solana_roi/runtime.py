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
from .risk import EntityResolver, RiskPolicy, TokenRiskIntelligence
from .storage import AppendOnlyEventStore


@dataclass(slots=True)
class IngestionRuntime:
    store: AppendOnlyEventStore
    engine: PaperTradingEngine
    registry: WalletProfileRegistry
    entity_resolver: EntityResolver
    risk: TokenRiskIntelligence
    collectors: CompleteLiveRiskCollectors
    service: CollectingLiveEvidenceIngestionService
    paper_signal_promotion_enabled: bool = False
    paper_signal_promotion_blocker: str = "risk evidence collection is not yet latency-certified for the forward cohort"


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


def build_runtime() -> IngestionRuntime:
    store = AppendOnlyEventStore(Path(os.getenv("SOLANA_ROI_DB_PATH", "data/solana-roi.sqlite3")))
    engine = PaperTradingEngine(store=store)
    registry = WalletProfileRegistry(store)
    for profile in _wallet_profiles_from_env():
        registry.register(profile)
    policy = RiskPolicy()
    entity_resolver = EntityResolver(store, registry, min_confidence=policy.confirmed_entity_link_confidence)
    risk = TokenRiskIntelligence(store, entity_resolver=entity_resolver, registry=registry, policy=policy)
    collectors = build_complete_live_collectors(risk)
    service = CollectingLiveEvidenceIngestionService(
        engine=engine,
        store=store,
        registry=registry,
        risk_provider=risk,
        entity_resolver=entity_resolver,
        promote_paper_signals=False,
        collectors=collectors,
    )
    return IngestionRuntime(
        store=store,
        engine=engine,
        registry=registry,
        entity_resolver=entity_resolver,
        risk=risk,
        collectors=collectors,
        service=service,
    )
