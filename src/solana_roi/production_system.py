from __future__ import annotations

import asyncio
import importlib
from dataclasses import dataclass
from typing import Any

COMPOSITION_VERSION = "v51-production-composition-root-125-130-v7-two-native-slices"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
COMPOSITION_STATUS_PATH = "/v1/operations/production-composition"


@dataclass(frozen=True)
class ComponentHealth:
    name: str
    owner_module: str
    attribute: str
    required: bool
    available: bool
    detail: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "owner_module": self.owner_module,
            "attribute": self.attribute,
            "required": self.required,
            "available": self.available,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class CompatibilityAdapter:
    name: str
    module: str
    installer: str
    owner: str

    def activate(self) -> None:
        imported = importlib.import_module(self.module)
        callback = getattr(imported, self.installer)
        callback()

    def as_dict(self) -> dict[str, str]:
        return {
            "name": self.name,
            "module": self.module,
            "installer": self.installer,
            "owner": self.owner,
        }


@dataclass(frozen=True)
class ProductionSystem:
    app: Any
    ingestion_runtime: Any
    components: tuple[ComponentHealth, ...]
    compatibility_adapters: tuple[CompatibilityAdapter, ...]

    @property
    def healthy(self) -> bool:
        return all(component.available for component in self.components if component.required)

    def status(self) -> dict[str, Any]:
        return {
            "composition_version": COMPOSITION_VERSION,
            "healthy": self.healthy,
            "components": {component.name: component.as_dict() for component in self.components},
            "required_component_count": sum(1 for component in self.components if component.required),
            "unavailable_required_components": [
                component.name for component in self.components if component.required and not component.available
            ],
            "compatibility_adapters": [adapter.as_dict() for adapter in self.compatibility_adapters],
            "compatibility_adapters_self_activate": False,
            "compatibility_adapter_activation": "lazy_ordered_only_from_build_production_system",
            "single_production_composition_root": True,
            "package_import_has_runtime_install_side_effects": False,
            "production_entrypoint": "solana_roi.production:app",
            "composition_status_path": COMPOSITION_STATUS_PATH,
            "paper_only": PAPER_ONLY,
            "live_money_authority": LIVE_MONEY_AUTHORITY,
            "signing_available": SIGNING_AVAILABLE,
            "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        }


def _adapter(name: str, module: str, installer: str, owner: str) -> CompatibilityAdapter:
    return CompatibilityAdapter(name, module, installer, owner)


# Compatibility adapters are a finite migration boundary, not a place for new
# economic behavior. Lazy activation preserves the previously certified sequencing:
# each module is imported only after the preceding adapter has been activated.
_COMPATIBILITY_ADAPTERS: tuple[CompatibilityAdapter, ...] = (
    _adapter("runtime_guards", "solana_roi.runtime_guards", "install_runtime_guards", "ingestion"),
    _adapter("stream_resilience", "solana_roi.stream_resilience", "install_stream_resilience", "ingestion"),
    _adapter("transport_hardening", "solana_roi.transport_hardening", "install_transport_hardening", "ingestion"),
    _adapter("handshake_pump", "solana_roi.handshake_pump", "install_handshake_pump", "ingestion"),
    _adapter("target_stream_fanout", "solana_roi.target_stream_fanout", "install_target_stream_fanout", "ingestion"),
    _adapter("target_quorum", "solana_roi.target_quorum", "install_target_quorum", "ingestion"),
    _adapter("stream_redundancy", "solana_roi.stream_redundancy", "install_stream_redundancy", "ingestion"),
    _adapter("live_poll_redundancy", "solana_roi.live_poll_redundancy", "install_live_poll_redundancy", "ingestion"),
    _adapter("poll_watermark", "solana_roi.poll_watermark_repair", "install_poll_watermark_repair", "ingestion"),
    _adapter("poll_standby_rearm", "solana_roi.poll_standby_rearm", "install_poll_standby_rearm", "ingestion"),
    _adapter("poll_recoverability_lease", "solana_roi.poll_recoverability_lease", "install_poll_recoverability_lease", "ingestion"),
    _adapter("poll_pagination_context", "solana_roi.poll_pagination_context", "install_poll_pagination_context", "ingestion"),
    _adapter("poll_exception_rearm", "solana_roi.poll_exception_rearm", "install_poll_exception_rearm", "ingestion"),
    _adapter("poll_chain_head_rearm", "solana_roi.poll_chain_head_rearm", "install_poll_chain_head_rearm", "ingestion"),
    _adapter("continuity_startup_barrier", "solana_roi.continuity_startup_barrier", "install_continuity_startup_barrier", "ingestion"),
    _adapter("continuity_durability", "solana_roi.continuity_durability_repair", "install_continuity_durability_repair", "ingestion"),
    _adapter("continuity_gap_clock", "solana_roi.continuity_gap_clock_repair", "install_continuity_gap_clock_repair", "ingestion"),
    _adapter("continuity_recovery_isolation", "solana_roi.continuity_recovery_isolation_repair", "install_continuity_recovery_isolation_repair", "ingestion"),
    _adapter("alchemy_multiplexed_stream", "solana_roi.alchemy_multiplexed_stream", "install_alchemy_multiplexed_stream", "ingestion"),
    _adapter("alchemy_handshake_pump", "solana_roi.alchemy_handshake_pump", "install_alchemy_handshake_pump", "ingestion"),
    _adapter("public_data_economics", "solana_roi.public_data_economics", "install_public_data_economics", "ingestion"),
    _adapter("public_ws_shard_transport", "solana_roi.public_ws_shard_transport_repair", "install_public_ws_shard_transport_repair", "ingestion"),
    _adapter("launch_coverage_bridge", "solana_roi.launch_coverage_bridge", "install_launch_coverage_bridge", "evidence"),
    _adapter("coverage_completeness", "solana_roi.coverage_completeness_repair", "install_coverage_completeness_repair", "evidence"),
    _adapter("launch_lateness", "solana_roi.launch_lateness_repair", "install_launch_lateness_repair", "evidence"),
    _adapter("launch_ws_frontier_timing", "solana_roi.launch_ws_frontier_timing_repair", "install_launch_ws_frontier_timing_repair", "evidence"),
    _adapter("production_boundary_compatibility", "solana_roi.production_boundary_compatibility", "install_production_boundary_compatibility", "evidence"),
    _adapter("funding_provenance", "solana_roi.funding_provenance_repair", "install_funding_provenance_repair", "evidence"),
    _adapter("poll_receipt_offloop", "solana_roi.poll_receipt_offloop_repair", "install_poll_receipt_offloop_repair", "ingestion"),
    _adapter("production_capacity", "solana_roi.production_capacity_repair", "install_production_capacity_repair", "ingestion"),
    _adapter("certification_hotpath", "solana_roi.certification_hotpath_repair", "install_certification_hotpath_repair", "certification"),
    _adapter("wallet_realtime_tracking", "solana_roi.wallet_realtime_tracking_repair", "install_wallet_realtime_tracking_repair", "learning"),
    _adapter("wallet_live_priority", "solana_roi.wallet_live_priority_repair", "install_wallet_live_priority_repair", "learning"),
    _adapter("full_scope_dispatch_capacity", "solana_roi.full_scope_dispatch_capacity_repair", "install_full_scope_dispatch_capacity_repair", "ingestion"),
    _adapter("wallet_realtime_status_compat", "solana_roi.wallet_realtime_status_compat", "install_wallet_realtime_status_compatibility", "learning"),
    _adapter("wallet_realtime_intelligence_boundary", "solana_roi.wallet_realtime_intelligence_boundary", "install_wallet_realtime_intelligence_boundary", "learning"),
    _adapter("wallet_intelligence_startup", "solana_roi.wallet_intelligence_startup_repair", "install_wallet_intelligence_startup_isolation", "learning"),
    _adapter("wallet_discovery_startup", "solana_roi.wallet_discovery_startup_repair", "install_wallet_discovery_startup_isolation", "learning"),
    _adapter("web_liveness_isolation", "solana_roi.web_liveness_isolation_repair", "install_web_liveness_isolation", "ingestion"),
    _adapter("continuity_storage_capacity", "solana_roi.continuity_storage_capacity_repair", "install_continuity_storage_capacity_repair", "ingestion"),
    _adapter("render_runtime_bootstrap", "solana_roi.render_runtime_bootstrap_repair", "install_render_runtime_bootstrap_handoff", "ingestion"),
    _adapter("wallet_venue_lifecycle_research", "solana_roi.wallet_venue_lifecycle_research", "install_wallet_venue_lifecycle_research", "learning"),
    _adapter("wallet_context_router", "solana_roi.wallet_context_router", "install_wallet_context_router", "strategy"),
    _adapter("wallet_context_router_precision", "solana_roi.wallet_context_router_precision_repair", "install_wallet_context_router_precision_repair", "strategy"),
    _adapter("post104_production_architecture", "solana_roi.post104_production_architecture_repair", "install_post104_production_architecture_repair", "strategy"),
    _adapter("candidate_execution_evidence_plane", "solana_roi.candidate_execution_evidence_plane", "install_candidate_execution_evidence_plane", "execution"),
    _adapter("semantic_candidate_attribution", "solana_roi.semantic_candidate_attribution_architecture", "install_semantic_candidate_attribution_architecture", "candidate"),
)

_BUILT: ProductionSystem | None = None


def _install_direct_stream_resource_guards() -> None:
    """Temporary final-layer equivalence guard while Repair 126 migrates ingestion."""
    from . import direct_solana as direct_solana_module
    from .direct_solana import DirectSolanaIngestionPlane

    if not bool(getattr(DirectSolanaIngestionPlane._handle_notification, "_roi_cooperative_yield", False)):
        original = DirectSolanaIngestionPlane._handle_notification

        async def handle(self: Any, provider: str, subscription_targets: dict[int, Any], message: dict[str, Any]) -> None:
            await original(self, provider, subscription_targets, message)
            await asyncio.sleep(0)

        try:
            handle.__dict__.update(getattr(original, "__dict__", {}))
        except Exception:
            pass
        setattr(handle, "_roi_cooperative_yield", True)
        DirectSolanaIngestionPlane._handle_notification = handle  # type: ignore[method-assign]

    current_connect = direct_solana_module.websockets.connect
    if not bool(getattr(current_connect, "_roi_memory_bounded", False)):
        def connect(*args: Any, **kwargs: Any) -> Any:
            requested_queue = kwargs.get("max_queue")
            requested_size = kwargs.get("max_size")
            kwargs["max_queue"] = 64 if requested_queue is None else min(int(requested_queue), 64)
            kwargs["max_size"] = 256 * 1024 if requested_size is None else min(int(requested_size), 256 * 1024)
            return current_connect(*args, **kwargs)

        try:
            connect.__dict__.update(getattr(current_connect, "__dict__", {}))
        except Exception:
            pass
        setattr(connect, "_roi_memory_bounded", True)
        direct_solana_module.websockets.connect = connect  # type: ignore[assignment]


def _component(name: str, module: str, attribute: str, *, required: bool = True) -> ComponentHealth:
    try:
        imported = importlib.import_module(module)
        value = getattr(imported, attribute)
    except Exception as exc:
        return ComponentHealth(name, module, attribute, required, False, f"{type(exc).__name__}:{exc}")
    return ComponentHealth(name, module, attribute, required, callable(value) or value is not None, "reachable")


def _required_components() -> tuple[ComponentHealth, ...]:
    return (
        _component("ingestion", "solana_roi.direct_solana", "DirectSolanaIngestionPlane"),
        _component("evidence", "solana_roi.v51_evidence_analytics", "build_evidence_validity_bundle"),
        _component("candidate", "solana_roi.v51_candidate_ledger", "refresh_candidate_pipeline"),
        _component("strategy", "solana_roi.strategy_v51_authority", "authority"),
        _component("execution", "solana_roi.v51_exact_exit_execution", "observe_exact_exit_order"),
        _component("settlement", "solana_roi.profit_first_entity_final_research", "FinalProfitFirstResearchAdapter"),
        _component("learning", "solana_roi.v51_evidence_analytics", "build_hazard_calibration"),
        _component("certification", "solana_roi.v51_phase17_context_certification", "build_phase17_context_certification"),
        _component("portfolio", "solana_roi.portfolio", "allocate_family_capital"),
        _component("statistics", "solana_roi.statistics", "robust_profile"),
    )


def _mount_composition_status(app: Any) -> None:
    existing = {getattr(route, "path", None) for route in app.routes}
    if COMPOSITION_STATUS_PATH in existing:
        return

    @app.get(COMPOSITION_STATUS_PATH)
    def production_composition_status() -> dict[str, Any]:
        system = getattr(app.state, "roi_production_system", None)
        if system is None:
            return {
                "composition_version": COMPOSITION_VERSION,
                "healthy": False,
                "reason": "production_system_not_attached",
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
            }
        return system.status()


def build_production_system() -> ProductionSystem:
    global _BUILT
    if _BUILT is not None:
        return _BUILT

    for adapter in _COMPATIBILITY_ADAPTERS:
        adapter.activate()
    _install_direct_stream_resource_guards()

    from .api import app, ingestion_runtime
    from .robinhood_runtime_install import install_robinhood_chain_paper_runtime
    from .v51_production_authority import install_v51_production_authority

    install_robinhood_chain_paper_runtime(app, ingestion_runtime)
    install_v51_production_authority(app, ingestion_runtime)

    components = _required_components()
    missing = [component.name for component in components if component.required and not component.available]
    if missing:
        raise RuntimeError("mandatory production components unavailable: " + ",".join(missing))

    system = ProductionSystem(
        app=app,
        ingestion_runtime=ingestion_runtime,
        components=components,
        compatibility_adapters=_COMPATIBILITY_ADAPTERS,
    )
    if not system.healthy:
        raise RuntimeError("production composition failed closed")

    app.state.roi_production_system = system
    app.state.roi_production_composition_status = system.status
    _mount_composition_status(app)
    _BUILT = system
    return system


production_system = build_production_system()
app = production_system.app
ingestion_runtime = production_system.ingestion_runtime

__all__ = [
    "COMPOSITION_STATUS_PATH",
    "COMPOSITION_VERSION",
    "CompatibilityAdapter",
    "ComponentHealth",
    "ProductionSystem",
    "app",
    "build_production_system",
    "ingestion_runtime",
    "production_system",
]
