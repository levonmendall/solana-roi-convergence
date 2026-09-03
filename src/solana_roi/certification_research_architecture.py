from __future__ import annotations

import os
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from . import continuity_recovery_isolation_repair as continuity_recovery
from . import production_capacity_repair as capacity
from . import runtime as runtime_module
from .direct_funding import SolanaRpcFundingCollector
from .direct_risk_collectors import SolanaAuthorityCollector, SolanaDeployerCollector
from .direct_solana import DirectSolanaIngestionPlane
from .launch_funding import CompleteLiveRiskCollectors, DexScreenerLaunchCollector
from .live_collectors import DexScreenerLiquidityCollector, PersistedSwapFlowCollector
from .risk import RiskPolicy, TokenRiskIntelligence
from .rpc_workload_governor import (
    WORKLOAD_CRITICAL,
    WORKLOAD_RESEARCH,
    install_rpc_workload_governor,
    rpc_workload,
)
from .solana_rpc import RpcEndpoint, SolanaRpcPool
from .wallet_discovery import ContinuousWalletDiscovery


DEFAULT_BACKGROUND_HYDRATION_MAX_AGE_SECONDS = 120.0
_ORIGINAL_BUILD_RUNTIME: Callable[[], Any] | None = None
_ORIGINAL_DISCOVERY_RUN: Callable[..., Any] | None = None
_ORIGINAL_DISCOVERY_RUN_ONCE: Callable[..., Any] | None = None
_ORIGINAL_DISCOVERY_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_DIRECT_HYDRATE_ONE: Callable[..., Any] | None = None
_ORIGINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_RESEARCH_PRESSURE_REASON: Callable[..., str | None] | None = None
_ORIGINAL_GAP_FETCH_DELTA: Callable[..., Any] | None = None


def _env_true(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes"}


def _background_hydration_max_age_seconds() -> float:
    try:
        value = float(
            os.getenv(
                "SOLANA_ROI_BACKGROUND_HYDRATION_MAX_AGE_SECONDS",
                str(DEFAULT_BACKGROUND_HYDRATION_MAX_AGE_SECONDS),
            )
        )
    except (TypeError, ValueError):
        value = DEFAULT_BACKGROUND_HYDRATION_MAX_AGE_SECONDS
    return max(30.0, value)


def _is_metered_alchemy_endpoint(endpoint: RpcEndpoint) -> bool:
    try:
        host = endpoint.http_url.split("/", 3)[2].lower()
    except Exception:
        host = ""
    return endpoint.name.strip().lower() == "alchemy" or host.endswith(".alchemy.com")


def _new_research_rpc(core_rpc: SolanaRpcPool) -> SolanaRpcPool:
    timeout = float(os.getenv("SOLANA_ROI_WALLET_RESEARCH_RPC_TIMEOUT_SECONDS", "2.5"))
    hedge_delay = float(os.getenv("SOLANA_ROI_WALLET_RESEARCH_HEDGE_DELAY_SECONDS", "0.10"))
    metered_enabled = _env_true("SOLANA_ROI_ENABLE_METERED_ALCHEMY")
    endpoints = tuple(
        endpoint
        for endpoint in tuple(core_rpc.endpoints)
        if metered_enabled or not _is_metered_alchemy_endpoint(endpoint)
    )
    if not endpoints:
        raise RuntimeError("wallet research requires at least one configured non-metered Solana RPC endpoint")
    pool = SolanaRpcPool(
        endpoints,
        timeout_seconds=timeout,
        hedge_delay_seconds=hedge_delay,
    )
    setattr(pool, "_roi_wallet_research_pool", True)
    setattr(pool, "_roi_shared_with_certification_pool", False)
    setattr(pool, "_roi_metered_alchemy_enabled", any(_is_metered_alchemy_endpoint(row) for row in endpoints))
    return pool


def _new_research_collectors(runtime: Any, research_rpc: SolanaRpcPool) -> tuple[TokenRiskIntelligence, CompleteLiveRiskCollectors]:
    policy = RiskPolicy()
    research_risk = TokenRiskIntelligence(
        runtime.store,
        entity_resolver=runtime.entity_resolver,
        registry=runtime.registry,
        policy=policy,
    )
    coverage_enabled = _env_true("SOLANA_ROI_PROGRAM_WIDE_SWAP_COVERAGE")
    collectors = CompleteLiveRiskCollectors(
        research_risk,
        authority=SolanaAuthorityCollector(research_risk, research_rpc),
        liquidity=DexScreenerLiquidityCollector(research_risk),
        deployer=SolanaDeployerCollector(research_risk, research_rpc),
        flow=PersistedSwapFlowCollector(research_risk),
        launch=DexScreenerLaunchCollector(research_risk) if coverage_enabled else None,
        funding=SolanaRpcFundingCollector(research_risk, research_rpc) if coverage_enabled else None,
        coverage_asserted=coverage_enabled,
    )
    return research_risk, collectors


def _configure_discovery_proxy(discovery: Any, *, rpc: SolanaRpcPool, risk: Any, collectors: Any) -> None:
    kwargs = getattr(discovery, "_kwargs", None)
    if isinstance(kwargs, dict):
        kwargs["rpc"] = rpc
        kwargs["risk"] = risk
        kwargs["risk_collectors"] = collectors
        setattr(discovery, "_roi_wallet_research_pool", rpc)
        setattr(discovery, "_roi_certification_research_isolation", True)
        return

    # Compatibility path for unit tests or direct runtimes without startup proxying.
    discovery.rpc = rpc
    discovery.risk = risk
    discovery.risk_collectors = collectors
    setattr(discovery, "_roi_certification_research_isolation", True)


def _build_runtime_with_research_isolation() -> Any:
    if _ORIGINAL_BUILD_RUNTIME is None:
        raise RuntimeError("certification/research architecture is not installed")
    runtime = _ORIGINAL_BUILD_RUNTIME()
    research_rpc = _new_research_rpc(runtime.rpc_pool)
    research_risk, research_collectors = _new_research_collectors(runtime, research_rpc)
    _configure_discovery_proxy(
        runtime.wallet_discovery,
        rpc=research_rpc,
        risk=research_risk,
        collectors=research_collectors,
    )
    return runtime


async def _research_discovery_run(self: ContinuousWalletDiscovery, stop: Any) -> None:
    if _ORIGINAL_DISCOVERY_RUN is None:
        raise RuntimeError("wallet discovery research isolation is not installed")
    with rpc_workload(WORKLOAD_RESEARCH):
        await _ORIGINAL_DISCOVERY_RUN(self, stop)


async def _research_discovery_run_once(self: ContinuousWalletDiscovery) -> dict[str, Any]:
    if _ORIGINAL_DISCOVERY_RUN_ONCE is None:
        raise RuntimeError("wallet discovery research isolation is not installed")
    with rpc_workload(WORKLOAD_RESEARCH):
        return await _ORIGINAL_DISCOVERY_RUN_ONCE(self)


def _isolated_research_pressure_reason(self: ContinuousWalletDiscovery) -> str | None:
    if bool(getattr(self.rpc, "_roi_wallet_research_pool", False)):
        # Core raw-dispatch pressure must never stop the independent background
        # evaluator. The process-wide governor already guarantees that research
        # cannot consume the critical continuity reservation or more than its own
        # bounded slot. Only degradation of the research pool itself pauses it.
        if capacity._rpc_redundancy_degraded(self.rpc):
            return "isolated_research_rpc_redundancy_degraded"
        return None
    if _ORIGINAL_RESEARCH_PRESSURE_REASON is None:
        return None
    return _ORIGINAL_RESEARCH_PRESSURE_REASON(self)


def _research_discovery_status(self: ContinuousWalletDiscovery) -> dict[str, Any]:
    if _ORIGINAL_DISCOVERY_STATUS is None:
        raise RuntimeError("wallet discovery research isolation is not installed")
    payload = _ORIGINAL_DISCOVERY_STATUS(self)
    isolated = bool(getattr(self.rpc, "_roi_wallet_research_pool", False))
    payload["resource_isolation"] = {
        "installed": True,
        "operationally_independent_from_certification": isolated,
        "separate_rpc_pool_object": isolated,
        "shared_physical_public_providers": True,
        "process_wide_rpc_governor_applies": True,
        "rpc_workload_class": WORKLOAD_RESEARCH,
        "historical_analysis_allowed": True,
        "historical_screen_has_active_strategy_authority": False,
        "challenger_forward_proof_required_before_promotion": True,
        "active_v3_1_cohort_mutation_allowed": False,
        "background_discovery_continues_during_core_queue_pressure": isolated,
        "metered_alchemy_required": False,
        "metered_alchemy_default_enabled": False,
        "metered_alchemy_explicit_opt_in_only": True,
        "metered_alchemy_enabled": bool(getattr(self.rpc, "_roi_metered_alchemy_enabled", False)) if isolated else False,
        "paper_only": True,
        "live_money_authority": False,
        "research_rpc": self.rpc.status() if isolated else None,
    }
    return payload


async def _deadline_aware_hydrate_one(self: DirectSolanaIngestionPlane, row: dict[str, Any]) -> None:
    if _ORIGINAL_DIRECT_HYDRATE_ONE is None:
        raise RuntimeError("deadline-aware hydration architecture is not installed")
    reason = str(row.get("reason") or "")
    if reason == "deterministic_market_sample":
        try:
            trigger = datetime.fromisoformat(str(row["trigger_received_at"]))
            if trigger.tzinfo is None:
                trigger = trigger.replace(tzinfo=timezone.utc)
            age_seconds = max(0.0, (datetime.now(timezone.utc) - trigger.astimezone(timezone.utc)).total_seconds())
        except Exception:
            age_seconds = 0.0
        max_age = _background_hydration_max_age_seconds()
        if age_seconds > max_age:
            self.journal.finish(
                str(row["signature"]),
                error="expired_non_authoritative_background_hydration",
                retry=False,
            )
            setattr(
                self,
                "_roi_expired_background_hydrations",
                int(getattr(self, "_roi_expired_background_hydrations", 0) or 0) + 1,
            )
            return
    await _ORIGINAL_DIRECT_HYDRATE_ONE(self, row)


async def _critical_gap_fetch_delta(*args: Any, **kwargs: Any) -> Any:
    if _ORIGINAL_GAP_FETCH_DELTA is None:
        raise RuntimeError("critical gap-recovery workload classification is not installed")
    with rpc_workload(WORKLOAD_CRITICAL):
        return await _ORIGINAL_GAP_FETCH_DELTA(*args, **kwargs)


def _direct_status_with_architecture(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _ORIGINAL_DIRECT_STATUS is None:
        raise RuntimeError("certification runtime architecture is not installed")
    payload = _ORIGINAL_DIRECT_STATUS(self)
    by_reason: list[dict[str, Any]] = []
    try:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT reason, COUNT(*) AS n, MIN(trigger_received_at) AS oldest "
                "FROM direct_solana_hydration_queue WHERE status='pending' "
                "GROUP BY reason ORDER BY n DESC"
            ).fetchall()
        by_reason = [
            {
                "reason": str(row["reason"]),
                "pending": int(row["n"]),
                "oldest_trigger_received_at": str(row["oldest"] or "") or None,
            }
            for row in rows
        ]
    except Exception:
        by_reason = []

    payload["certification_runtime_architecture"] = {
        "installed": True,
        "full_market_observer_preserved": True,
        "full_program_scope_unchanged": True,
        "frozen_scout_tracking_preserved": True,
        "wallet_research_operationally_isolated": True,
        "process_wide_rpc_governor": True,
        "critical_gap_recovery_has_reserved_capacity": True,
        "certification_workload_has_priority_over_research": True,
        "background_market_sample_deadline_seconds": _background_hydration_max_age_seconds(),
        "expired_background_hydrations_session": int(
            getattr(self, "_roi_expired_background_hydrations", 0) or 0
        ),
        "expired_background_rows_keep_raw_receipt_evidence": True,
        "launch_hydration_deadline_changed": False,
        "frozen_scout_hydration_deadline_changed": False,
        "gap_recovery_deadline_changed": False,
        "continuity_lease_seconds_unchanged": 12.0,
        "recovery_page_bound_unchanged": "3x1000",
        "certification_thresholds_unchanged": True,
        "strategy_thresholds_unchanged": True,
        "paper_only_authority_unchanged": True,
        "signing_or_submission_available": False,
        "pending_hydration_by_reason": by_reason,
    }
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "certification_research_plane_isolation": True,
                "wallet_research_uses_separate_rpc_pool": True,
                "rpc_capacity_governed_process_wide": True,
                "critical_rpc_capacity_reserved": True,
                "stale_non_authoritative_market_samples_expire_without_rpc": True,
                "full_raw_market_scope_preserved": True,
                "active_strategy_wallets_preserved": True,
                "metered_alchemy_required": False,
                "metered_alchemy_default_enabled": False,
                "metered_alchemy_explicit_opt_in_only": True,
                "certification_thresholds_unchanged": True,
                "paper_only_authority_unchanged": True,
            }
        )
    return payload


def install_certification_research_architecture() -> None:
    global _ORIGINAL_BUILD_RUNTIME
    global _ORIGINAL_DISCOVERY_RUN, _ORIGINAL_DISCOVERY_RUN_ONCE, _ORIGINAL_DISCOVERY_STATUS
    global _ORIGINAL_DIRECT_HYDRATE_ONE, _ORIGINAL_DIRECT_STATUS
    global _ORIGINAL_RESEARCH_PRESSURE_REASON, _ORIGINAL_GAP_FETCH_DELTA

    install_rpc_workload_governor()

    current_build = runtime_module.build_runtime
    if not bool(getattr(current_build, "_roi_certification_research_architecture", False)):
        _ORIGINAL_BUILD_RUNTIME = current_build
        try:
            _build_runtime_with_research_isolation.__dict__.update(getattr(current_build, "__dict__", {}))
        except Exception:
            pass
        setattr(_build_runtime_with_research_isolation, "_roi_certification_research_architecture", True)
        runtime_module.build_runtime = _build_runtime_with_research_isolation  # type: ignore[assignment]

    current_run = ContinuousWalletDiscovery.run
    if not bool(getattr(current_run, "_roi_certification_research_architecture", False)):
        _ORIGINAL_DISCOVERY_RUN = current_run
        try:
            _research_discovery_run.__dict__.update(getattr(current_run, "__dict__", {}))
        except Exception:
            pass
        setattr(_research_discovery_run, "_roi_certification_research_architecture", True)
        ContinuousWalletDiscovery.run = _research_discovery_run  # type: ignore[method-assign]

    current_run_once = ContinuousWalletDiscovery.run_once
    if not bool(getattr(current_run_once, "_roi_certification_research_architecture", False)):
        _ORIGINAL_DISCOVERY_RUN_ONCE = current_run_once
        try:
            _research_discovery_run_once.__dict__.update(getattr(current_run_once, "__dict__", {}))
        except Exception:
            pass
        setattr(_research_discovery_run_once, "_roi_certification_research_architecture", True)
        ContinuousWalletDiscovery.run_once = _research_discovery_run_once  # type: ignore[method-assign]

    current_discovery_status = ContinuousWalletDiscovery.status
    if not bool(getattr(current_discovery_status, "_roi_certification_research_architecture", False)):
        _ORIGINAL_DISCOVERY_STATUS = current_discovery_status
        try:
            _research_discovery_status.__dict__.update(getattr(current_discovery_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_research_discovery_status, "_roi_certification_research_architecture", True)
        ContinuousWalletDiscovery.status = _research_discovery_status  # type: ignore[method-assign]

    if not bool(getattr(capacity._research_pressure_reason, "_roi_certification_research_architecture", False)):
        _ORIGINAL_RESEARCH_PRESSURE_REASON = capacity._research_pressure_reason
        setattr(_isolated_research_pressure_reason, "_roi_certification_research_architecture", True)
        capacity._research_pressure_reason = _isolated_research_pressure_reason  # type: ignore[assignment]

    current_hydrate = DirectSolanaIngestionPlane._hydrate_one
    if not bool(getattr(current_hydrate, "_roi_certification_research_architecture", False)):
        _ORIGINAL_DIRECT_HYDRATE_ONE = current_hydrate
        try:
            _deadline_aware_hydrate_one.__dict__.update(getattr(current_hydrate, "__dict__", {}))
        except Exception:
            pass
        setattr(_deadline_aware_hydrate_one, "_roi_certification_research_architecture", True)
        DirectSolanaIngestionPlane._hydrate_one = _deadline_aware_hydrate_one  # type: ignore[method-assign]

    current_direct_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_direct_status, "_roi_certification_research_architecture", False)):
        _ORIGINAL_DIRECT_STATUS = current_direct_status
        try:
            _direct_status_with_architecture.__dict__.update(getattr(current_direct_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_direct_status_with_architecture, "_roi_certification_research_architecture", True)
        DirectSolanaIngestionPlane.status = _direct_status_with_architecture  # type: ignore[method-assign]

    current_gap_fetch = continuity_recovery._isolated_gap_fetch_delta
    if not bool(getattr(current_gap_fetch, "_roi_certification_research_architecture", False)):
        _ORIGINAL_GAP_FETCH_DELTA = current_gap_fetch
        setattr(_critical_gap_fetch_delta, "_roi_certification_research_architecture", True)
        continuity_recovery._isolated_gap_fetch_delta = _critical_gap_fetch_delta  # type: ignore[assignment]


__all__ = [
    "DEFAULT_BACKGROUND_HYDRATION_MAX_AGE_SECONDS",
    "_background_hydration_max_age_seconds",
    "_build_runtime_with_research_isolation",
    "_deadline_aware_hydrate_one",
    "_is_metered_alchemy_endpoint",
    "_isolated_research_pressure_reason",
    "_new_research_rpc",
    "install_certification_research_architecture",
]
