from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .activation import CandidateActivationGate, ForwardCohortController
from .collecting_ingestion import CollectingLiveEvidenceIngestionService
from .durable_engine import DurablePaperTradingEngine
from .ingestion import WalletProfile, WalletProfileRegistry
from .launch_funding import CompleteLiveRiskCollectors, build_complete_live_collectors
from .models import WalletTier
from .observation import LatencyCertificationGate, ShadowPriceClock, TimedRiskCollectors
from .observation_store import ObservationEventStore
from .quote import JupiterQuoteOnlyClient, QuoteCertificationGate
from .risk import EntityResolver, RiskPolicy, TokenRiskIntelligence
from .shadow_execution import (
    JupiterShadowTransactionSimulator,
    ShadowAwareQuoteCertificationGate,
    ShadowExecutionCertificationGate,
    ShadowWalletExecutableQuoteHandoff,
)
from .source_coverage import SourceAwareProgramCoverageCertificationGate
from .webhook_queue import DurableHeliusWebhookQueue, HeliusWebhookWorker


def _env_true(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes"}


class RuntimeForwardCohortController(ForwardCohortController):
    """Add runtime-only operational invariants to the immutable cohort readiness gate."""

    def _base_readiness(self) -> dict[str, Any]:
        status = super()._base_readiness()
        clock_enabled = _env_true("SOLANA_ROI_SHADOW_CLOCK_ENABLED")
        status["continuous_price_clock_enabled"] = clock_enabled
        status["requirements"]["continuous_price_clock_enabled"] = clock_enabled
        status["passed"] = all(bool(value) for value in status["requirements"].values())
        return status


@dataclass(slots=True)
class IngestionRuntime:
    store: ObservationEventStore
    engine: DurablePaperTradingEngine
    registry: WalletProfileRegistry
    entity_resolver: EntityResolver
    risk: TokenRiskIntelligence
    raw_collectors: CompleteLiveRiskCollectors
    collectors: TimedRiskCollectors
    price_clock: ShadowPriceClock
    latency_gate: LatencyCertificationGate
    quote_handoff: ShadowWalletExecutableQuoteHandoff
    quote_gate: ShadowAwareQuoteCertificationGate
    coverage_gate: SourceAwareProgramCoverageCertificationGate
    cohort_controller: ForwardCohortController
    activation_gate: CandidateActivationGate
    service: CollectingLiveEvidenceIngestionService
    webhook_queue: DurableHeliusWebhookQueue
    webhook_worker: HeliusWebhookWorker

    @property
    def paper_signal_promotion_enabled(self) -> bool:
        return self.cohort_controller.is_armed()

    @property
    def paper_signal_promotion_blocker(self) -> str | None:
        if self.paper_signal_promotion_enabled:
            return None
        status = self.cohort_controller.status()
        failed = [name for name, passed in status["requirements"].items() if not passed]
        if not status["manifest_frozen"]:
            failed.append("strategy_manifest_not_frozen")
        return ",".join(failed) if failed else "explicit_one_time_arm_required"


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


def _shadow_simulator(client: JupiterQuoteOnlyClient | None) -> JupiterShadowTransactionSimulator | None:
    if client is None:
        return None
    wallet = os.getenv("SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY", "").strip()
    if not wallet:
        return None
    try:
        return JupiterShadowTransactionSimulator(
            jupiter_api_key=client.jupiter_api_key,
            shadow_wallet_public_key=wallet,
            http_client=client.client,
            rpc=client.rpc,
        )
    except ValueError:
        return None


def build_runtime() -> IngestionRuntime:
    store = ObservationEventStore(Path(os.getenv("SOLANA_ROI_DB_PATH", "data/solana-roi.sqlite3")))
    engine = DurablePaperTradingEngine(store=store)
    registry = WalletProfileRegistry(store)
    for profile in _wallet_profiles_from_env():
        registry.register(profile)
    policy = RiskPolicy()
    entity_resolver = EntityResolver(store, registry, min_confidence=policy.confirmed_entity_link_confidence)
    risk = TokenRiskIntelligence(store, entity_resolver=entity_resolver, registry=registry, policy=policy)
    raw_collectors = build_complete_live_collectors(risk)
    collectors = TimedRiskCollectors(raw_collectors, risk=risk, store=store)
    latency_gate = LatencyCertificationGate(store)

    quote_client = _quote_client()
    shadow_wallet = os.getenv("SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY", "").strip()
    quote_handoff = ShadowWalletExecutableQuoteHandoff(
        store=store,
        client=quote_client,
        simulator=_shadow_simulator(quote_client),
        full_position_notional_fn=lambda: engine.portfolio.full_position_notional(engine.marks),
        max_chase_fraction=engine.config.max_chase_fraction,
    )
    base_quote_gate = QuoteCertificationGate(quote_handoff.ledger)
    shadow_gate = ShadowExecutionCertificationGate(
        quote_handoff.shadow_ledger,
        shadow_wallet_public_key=shadow_wallet,
    )
    quote_gate = ShadowAwareQuoteCertificationGate(base_quote_gate, shadow_gate)

    coverage_gate = SourceAwareProgramCoverageCertificationGate(
        store,
        configured_fn=lambda: bool(
            raw_collectors.coverage_asserted
            and raw_collectors.launch is not None
            and raw_collectors.funding is not None
        ),
    )
    cohort_controller = RuntimeForwardCohortController(
        store=store,
        engine=engine,
        config=engine.config,
        risk_policy=policy,
        latency_gate=latency_gate,
        quote_gate=quote_gate,
        coverage_gate=coverage_gate,
    )
    activation_gate = CandidateActivationGate(
        controller=cohort_controller,
        engine=engine,
        store=store,
    )
    price_clock = ShadowPriceClock(
        store=store,
        engine=engine,
        interval_seconds=float(os.getenv("SOLANA_ROI_SHADOW_CLOCK_SECONDS", "1.0")),
        tracking_horizon_seconds=float(os.getenv("SOLANA_ROI_PRICE_TRACKING_HORIZON_SECONDS", "300")),
        drive_paper_engine=cohort_controller.is_armed(),
    )
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
        activation_gate=activation_gate,
    )
    webhook_queue = DurableHeliusWebhookQueue(store)
    webhook_worker = HeliusWebhookWorker(queue=webhook_queue, service=service)
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
        coverage_gate=coverage_gate,
        cohort_controller=cohort_controller,
        activation_gate=activation_gate,
        service=service,
        webhook_queue=webhook_queue,
        webhook_worker=webhook_worker,
    )
