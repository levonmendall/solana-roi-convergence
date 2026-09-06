from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .activation import CandidateActivationGate, ForwardCohortController
from .certification_epoch import ensure_release_certification_epoch
from .collecting_ingestion import CollectingLiveEvidenceIngestionService
from .config import BASELINE
from .direct_funding import SolanaRpcFundingCollector
from .direct_quote import DirectRpcJupiterQuoteClient
from .direct_risk_collectors import SolanaAuthorityCollector, SolanaDeployerCollector
from .direct_solana import DirectSolanaIngestionPlane
from .durable_engine import DurablePaperTradingEngine
from .ingestion import WalletProfile, WalletProfileRegistry
from .launch_funding import CompleteLiveRiskCollectors, DexScreenerLaunchCollector
from .live_collectors import DexScreenerLiquidityCollector, PersistedSwapFlowCollector
from .models import WalletTier
from .observation import LatencyCertificationGate, ShadowPriceClock, TimedRiskCollectors
from .observation_store import ObservationEventStore
from .prospective_shadow import ProspectiveShadowExecutionCertificationGate
from .quote import JupiterQuoteOnlyClient, QuoteCertificationGate
from .risk import EntityResolver, RiskPolicy, TokenRiskIntelligence
from .shadow_execution import (
    JupiterShadowTransactionSimulator,
    ShadowAwareQuoteCertificationGate,
    ShadowWalletExecutableQuoteHandoff,
)
from .solana_rpc import SolanaRpcPool, rpc_endpoints_from_env
from .source_coverage import SourceAwareProgramCoverageCertificationGate
from .wallet_discovery import ContinuousWalletDiscovery, WalletDiscoveryPolicy
from .wallet_intelligence import ContinuousWalletIntelligence
from .webhook_ingress import CompositeHeliusWebhookIngestion
from .webhook_queue import DurableHeliusWebhookQueue, HeliusWebhookWorker


def _env_true(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes"}


class RuntimeForwardCohortController(ForwardCohortController):
    """Add runtime-only operational invariants to the immutable cohort readiness gate."""

    webhook_queue: DurableHeliusWebhookQueue | None = None
    direct_ingestion: DirectSolanaIngestionPlane | None = None

    def _direct_stream_status(self) -> dict[str, Any]:
        if self.direct_ingestion is None:
            return {
                "enabled": False,
                "continuity_ok": False,
                "strategy_scope_reduced": True,
                "full_program_scope": [],
            }
        return self.direct_ingestion.status()

    @staticmethod
    def _direct_stream_status_ok(direct_status: dict[str, Any]) -> bool:
        return bool(
            direct_status.get("enabled")
            and direct_status.get("continuity_ok")
            and not direct_status.get("strategy_scope_reduced")
            and len(direct_status.get("full_program_scope") or []) == 7
        )

    def _direct_stream_continuity_ok(self) -> bool:
        return self._direct_stream_status_ok(self._direct_stream_status())

    def runtime_continuity_ok(self) -> bool:
        """Require both durable paper state and live full-scope data-plane continuity.

        CandidateActivationGate calls this method immediately before every paper
        authorization. A direct-stream loss therefore fails closed even after the
        cohort has already been frozen and armed.
        """
        return bool(super().runtime_continuity_ok() and self._direct_stream_continuity_ok())

    def _base_readiness(self) -> dict[str, Any]:
        status = super()._base_readiness()
        clock_enabled = _env_true("SOLANA_ROI_SHADOW_CLOCK_ENABLED")
        queue_status = self.webhook_queue.status() if self.webhook_queue is not None else {"pending": 1}
        queue_drained = int(queue_status.get("pending", 1)) == 0
        direct_status = self._direct_stream_status()
        direct_ok = self._direct_stream_status_ok(direct_status)
        status["continuous_price_clock_enabled"] = clock_enabled
        status["webhook_queue"] = queue_status
        status["direct_solana"] = direct_status
        status["requirements"]["continuous_price_clock_enabled"] = clock_enabled
        status["requirements"]["durable_webhook_queue_drained"] = queue_drained
        status["requirements"]["direct_full_scope_stream_continuity"] = direct_ok
        status["passed"] = all(bool(value) for value in status["requirements"].values())
        return status


@dataclass(slots=True)
class IngestionRuntime:
    store: ObservationEventStore
    engine: DurablePaperTradingEngine
    registry: WalletProfileRegistry
    entity_resolver: EntityResolver
    risk: TokenRiskIntelligence
    rpc_pool: SolanaRpcPool
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
    direct_ingestion: DirectSolanaIngestionPlane
    wallet_intelligence: ContinuousWalletIntelligence
    wallet_discovery: ContinuousWalletDiscovery
    webhook_queue: DurableHeliusWebhookQueue
    webhook_worker: HeliusWebhookWorker
    certification_epoch: datetime

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


def _quote_client(rpc_pool: SolanaRpcPool) -> JupiterQuoteOnlyClient | None:
    jupiter = os.getenv("JUPITER_API_KEY", "").strip()
    if not jupiter:
        return None
    return DirectRpcJupiterQuoteClient(jupiter_api_key=jupiter, rpc=rpc_pool)


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


def _wallet_discovery_policy() -> WalletDiscoveryPolicy:
    return WalletDiscoveryPolicy(
        broad_sample_modulus=int(os.getenv("SOLANA_ROI_WALLET_DISCOVERY_SAMPLE_MODULUS", "20")),
        broad_scan_limit=int(os.getenv("SOLANA_ROI_WALLET_DISCOVERY_SCAN_LIMIT", "600")),
        historical_max_signatures=int(os.getenv("SOLANA_ROI_WALLET_DISCOVERY_HISTORY_SIGNATURES", "120")),
        historical_rpc_concurrency=int(os.getenv("SOLANA_ROI_WALLET_DISCOVERY_HISTORY_CONCURRENCY", "6")),
        historical_min_closed_episodes=int(os.getenv("SOLANA_ROI_WALLET_DISCOVERY_MIN_HISTORY_EPISODES", "5")),
        historical_min_distinct_tokens=int(os.getenv("SOLANA_ROI_WALLET_DISCOVERY_MIN_HISTORY_TOKENS", "5")),
        historical_min_return_on_capital=float(os.getenv("SOLANA_ROI_WALLET_DISCOVERY_MIN_HISTORY_RETURN", "0.05")),
        historical_min_profit_factor=float(os.getenv("SOLANA_ROI_WALLET_DISCOVERY_MIN_HISTORY_PROFIT_FACTOR", "1.05")),
        max_tracked_challengers=int(os.getenv("SOLANA_ROI_WALLET_DISCOVERY_MAX_TRACKED", "12")),
        forward_poll_limit=int(os.getenv("SOLANA_ROI_WALLET_DISCOVERY_FORWARD_POLL_LIMIT", "100")),
        forward_max_pages=int(os.getenv("SOLANA_ROI_WALLET_DISCOVERY_FORWARD_MAX_PAGES", "3")),
        forward_rpc_concurrency=int(os.getenv("SOLANA_ROI_WALLET_DISCOVERY_FORWARD_CONCURRENCY", "6")),
        poll_interval_seconds=float(os.getenv("SOLANA_ROI_WALLET_DISCOVERY_POLL_SECONDS", "10")),
        rescreen_hours=float(os.getenv("SOLANA_ROI_WALLET_DISCOVERY_RESCREEN_HOURS", "6")),
        max_observation_lag_seconds=float(os.getenv("SOLANA_ROI_WALLET_DISCOVERY_MAX_LAG_SECONDS", "20")),
        min_risk_coverage_rate=float(os.getenv("SOLANA_ROI_WALLET_DISCOVERY_MIN_RISK_COVERAGE", "0.80")),
        max_chase_fraction=BASELINE.max_chase_fraction,
    )


def build_runtime() -> IngestionRuntime:
    store = ObservationEventStore(Path(os.getenv("SOLANA_ROI_DB_PATH", "data/solana-roi.sqlite3")))
    certification_epoch = ensure_release_certification_epoch(store)
    webhook_queue = DurableHeliusWebhookQueue(store)
    engine = DurablePaperTradingEngine(store=store)
    registry = WalletProfileRegistry(store)
    profiles = _wallet_profiles_from_env()
    for profile in profiles:
        registry.register(profile)

    rpc_pool = SolanaRpcPool(
        rpc_endpoints_from_env(),
        timeout_seconds=float(os.getenv("SOLANA_ROI_RPC_TIMEOUT_SECONDS", "2.5")),
        hedge_delay_seconds=float(os.getenv("SOLANA_ROI_RPC_HEDGE_DELAY_SECONDS", "0.15")),
    )
    policy = RiskPolicy()
    entity_resolver = EntityResolver(store, registry, min_confidence=policy.confirmed_entity_link_confidence)
    risk = TokenRiskIntelligence(store, entity_resolver=entity_resolver, registry=registry, policy=policy)

    coverage_enabled = _env_true("SOLANA_ROI_PROGRAM_WIDE_SWAP_COVERAGE")
    raw_collectors = CompleteLiveRiskCollectors(
        risk,
        authority=SolanaAuthorityCollector(risk, rpc_pool),
        liquidity=DexScreenerLiquidityCollector(risk),
        deployer=SolanaDeployerCollector(risk, rpc_pool),
        flow=PersistedSwapFlowCollector(risk),
        launch=DexScreenerLaunchCollector(risk) if coverage_enabled else None,
        funding=SolanaRpcFundingCollector(risk, rpc_pool) if coverage_enabled else None,
        coverage_asserted=coverage_enabled,
    )
    collectors = TimedRiskCollectors(raw_collectors, risk=risk, store=store)
    latency_gate = LatencyCertificationGate(store, prospective_start_at=certification_epoch)

    quote_client = _quote_client(rpc_pool)
    shadow_wallet = os.getenv("SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY", "").strip()
    quote_handoff = ShadowWalletExecutableQuoteHandoff(
        store=store,
        client=quote_client,
        simulator=_shadow_simulator(quote_client),
        full_position_notional_fn=lambda: engine.portfolio.full_position_notional(engine.marks),
        max_chase_fraction=engine.config.max_chase_fraction,
    )
    base_quote_gate = QuoteCertificationGate(
        quote_handoff.ledger,
        prospective_start_at=certification_epoch,
    )
    shadow_gate = ProspectiveShadowExecutionCertificationGate(
        quote_handoff.shadow_ledger,
        shadow_wallet_public_key=shadow_wallet,
        prospective_start_at=certification_epoch,
    )
    quote_gate = ShadowAwareQuoteCertificationGate(base_quote_gate, shadow_gate)

    coverage_gate = SourceAwareProgramCoverageCertificationGate(
        store,
        configured_fn=lambda: bool(
            raw_collectors.coverage_asserted
            and raw_collectors.launch is not None
            and raw_collectors.funding is not None
        ),
        prospective_start_at=certification_epoch,
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
    cohort_controller.webhook_queue = webhook_queue
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
    direct_ingestion = DirectSolanaIngestionPlane(
        store=store,
        service=service,
        scout_wallets=tuple(profile.wallet for profile in profiles),
        rpc_pool=rpc_pool,
        coverage_status_fn=coverage_gate.status,
        worker_count=int(os.getenv("SOLANA_ROI_DIRECT_HYDRATION_WORKERS", "12")),
        market_sample_modulus=int(os.getenv("SOLANA_ROI_DIRECT_MARKET_SAMPLE_MODULUS", "20")),
        audit_sample_modulus=int(os.getenv("SOLANA_ROI_DIRECT_AUDIT_SAMPLE_MODULUS", "200")),
        candidate_context_deadline_seconds=float(os.getenv("SOLANA_ROI_DIRECT_CONTEXT_DEADLINE_SECONDS", "3.0")),
        candidate_context_max_signatures=int(os.getenv("SOLANA_ROI_DIRECT_CONTEXT_MAX_SIGNATURES", "600")),
        gap_backfill_max_pages=int(os.getenv("SOLANA_ROI_DIRECT_GAP_BACKFILL_MAX_PAGES", "5")),
    )
    cohort_controller.direct_ingestion = direct_ingestion

    wallet_intelligence = ContinuousWalletIntelligence(store)
    wallet_discovery = ContinuousWalletDiscovery(
        store=store,
        rpc=rpc_pool,
        entity_resolver=entity_resolver,
        risk=risk,
        risk_collectors=raw_collectors,
        intelligence=wallet_intelligence,
        policy=_wallet_discovery_policy(),
        enabled=_env_true("SOLANA_ROI_WALLET_DISCOVERY_ENABLED"),
    )

    webhook_ingress = CompositeHeliusWebhookIngestion(service)
    webhook_worker = HeliusWebhookWorker(queue=webhook_queue, service=webhook_ingress)
    return IngestionRuntime(
        store=store,
        engine=engine,
        registry=registry,
        entity_resolver=entity_resolver,
        risk=risk,
        rpc_pool=rpc_pool,
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
        direct_ingestion=direct_ingestion,
        wallet_intelligence=wallet_intelligence,
        wallet_discovery=wallet_discovery,
        webhook_queue=webhook_queue,
        webhook_worker=webhook_worker,
        certification_epoch=certification_epoch,
    )