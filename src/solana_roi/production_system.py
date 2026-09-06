from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable

from .alchemy_handshake_pump import install_alchemy_handshake_pump
from .alchemy_multiplexed_stream import install_alchemy_multiplexed_stream
from .candidate_execution_evidence_plane import install_candidate_execution_evidence_plane
from .candidate_risk_window_repair import install_candidate_risk_window_repair
from .certification_hotpath_repair import install_certification_hotpath_repair
from .continuity_durability_repair import install_continuity_durability_repair
from .continuity_gap_clock_repair import install_continuity_gap_clock_repair
from .continuity_recovery_isolation_repair import install_continuity_recovery_isolation_repair
from .continuity_startup_barrier import install_continuity_startup_barrier
from .continuity_storage_capacity_repair import install_continuity_storage_capacity_repair
from .coverage_completeness_repair import install_coverage_completeness_repair
from .execution_realism import install_execution_realism
from .full_scope_dispatch_capacity_repair import install_full_scope_dispatch_capacity_repair
from .funding_provenance_repair import install_funding_provenance_repair
from .handshake_pump import install_handshake_pump
from .launch_coverage_bridge import install_launch_coverage_bridge
from .launch_lateness_repair import install_launch_lateness_repair
from .launch_ws_frontier_timing_repair import install_launch_ws_frontier_timing_repair
from .live_poll_redundancy import install_live_poll_redundancy
from .poll_chain_head_rearm import install_poll_chain_head_rearm
from .poll_exception_rearm import install_poll_exception_rearm
from .poll_pagination_context import install_poll_pagination_context
from .poll_receipt_offloop_repair import install_poll_receipt_offloop_repair
from .poll_recoverability_lease import install_poll_recoverability_lease
from .poll_standby_rearm import install_poll_standby_rearm
from .poll_watermark_repair import install_poll_watermark_repair
from .post104_production_architecture_repair import install_post104_production_architecture_repair
from .production_boundary_compatibility import install_production_boundary_compatibility
from .production_capacity_repair import install_production_capacity_repair
from .public_data_economics import install_public_data_economics
from .public_ws_shard_transport_repair import install_public_ws_shard_transport_repair
from .render_runtime_bootstrap_repair import install_render_runtime_bootstrap_handoff
from .runtime_guards import install_runtime_guards
from .semantic_candidate_attribution_architecture import install_semantic_candidate_attribution_architecture
from .stream_redundancy import install_stream_redundancy
from .stream_resilience import install_stream_resilience
from .target_quorum import install_target_quorum
from .target_stream_fanout import install_target_stream_fanout
from .transport_hardening import install_transport_hardening
from .wallet_context_router import install_wallet_context_router
from .wallet_context_router_precision_repair import install_wallet_context_router_precision_repair
from .wallet_discovery_startup_repair import install_wallet_discovery_startup_isolation
from .wallet_intelligence_startup_repair import install_wallet_intelligence_startup_isolation
from .wallet_live_priority_repair import install_wallet_live_priority_repair
from .wallet_realtime_intelligence_boundary import install_wallet_realtime_intelligence_boundary
from .wallet_realtime_status_compat import install_wallet_realtime_status_compatibility
from .wallet_realtime_tracking_repair import install_wallet_realtime_tracking_repair
from .wallet_venue_lifecycle_research import install_wallet_venue_lifecycle_research
from .web_liveness_isolation_repair import install_web_liveness_isolation

COMPOSITION_VERSION = "v51-production-composition-root-125-130-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False


@dataclass(frozen=True)
class ComponentHealth:
    name: str
    module: str
    required: bool
    available: bool
    detail: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "module": self.module,
            "required": self.required,
            "available": self.available,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class ProductionSystem:
    app: Any
    ingestion_runtime: Any
    components: tuple[ComponentHealth, ...]
    compatibility_installers: tuple[str, ...]

    @property
    def healthy(self) -> bool:
        return all(component.available for component in self.components if component.required)

    def status(self) -> dict[str, Any]:
        return {
            "composition_version": COMPOSITION_VERSION,
            "healthy": self.healthy,
            "components": {component.name: component.as_dict() for component in self.components},
            "compatibility_installers": list(self.compatibility_installers),
            "compatibility_installers_self_activate": False,
            "single_production_composition_root": True,
            "package_import_has_runtime_install_side_effects": False,
            "paper_only": PAPER_ONLY,
            "live_money_authority": LIVE_MONEY_AUTHORITY,
            "signing_available": SIGNING_AVAILABLE,
            "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        }


# Ordered compatibility adapters. They are invoked only here. This deliberately
# preserves the already-certified behavior while retiring import-time installation
# from solana_roi.__init__ and solana_roi.production.
_COMPATIBILITY_INSTALLERS: tuple[tuple[str, Callable[[], None]], ...] = (
    ("runtime_guards", install_runtime_guards),
    ("stream_resilience", install_stream_resilience),
    ("transport_hardening", install_transport_hardening),
    ("handshake_pump", install_handshake_pump),
    ("target_stream_fanout", install_target_stream_fanout),
    ("target_quorum", install_target_quorum),
    ("stream_redundancy", install_stream_redundancy),
    ("live_poll_redundancy", install_live_poll_redundancy),
    ("poll_watermark", install_poll_watermark_repair),
    ("poll_standby_rearm", install_poll_standby_rearm),
    ("poll_recoverability_lease", install_poll_recoverability_lease),
    ("poll_pagination_context", install_poll_pagination_context),
    ("poll_exception_rearm", install_poll_exception_rearm),
    ("poll_chain_head_rearm", install_poll_chain_head_rearm),
    ("continuity_startup_barrier", install_continuity_startup_barrier),
    ("continuity_durability", install_continuity_durability_repair),
    ("continuity_gap_clock", install_continuity_gap_clock_repair),
    ("continuity_recovery_isolation", install_continuity_recovery_isolation_repair),
    ("alchemy_multiplexed_stream", install_alchemy_multiplexed_stream),
    ("alchemy_handshake_pump", install_alchemy_handshake_pump),
    ("public_data_economics", install_public_data_economics),
    ("public_ws_shard_transport", install_public_ws_shard_transport_repair),
    ("launch_coverage_bridge", install_launch_coverage_bridge),
    ("coverage_completeness", install_coverage_completeness_repair),
    ("launch_lateness", install_launch_lateness_repair),
    ("launch_ws_frontier_timing", install_launch_ws_frontier_timing_repair),
    ("production_boundary_compatibility", install_production_boundary_compatibility),
    ("funding_provenance", install_funding_provenance_repair),
    ("execution_realism", install_execution_realism),
    ("poll_receipt_offloop", install_poll_receipt_offloop_repair),
    ("production_capacity", install_production_capacity_repair),
    ("certification_hotpath", install_certification_hotpath_repair),
    ("wallet_realtime_tracking", install_wallet_realtime_tracking_repair),
    ("wallet_live_priority", install_wallet_live_priority_repair),
    ("full_scope_dispatch_capacity", install_full_scope_dispatch_capacity_repair),
    ("wallet_realtime_status_compat", install_wallet_realtime_status_compatibility),
    ("wallet_realtime_intelligence_boundary", install_wallet_realtime_intelligence_boundary),
    ("wallet_intelligence_startup", install_wallet_intelligence_startup_isolation),
    ("wallet_discovery_startup", install_wallet_discovery_startup_isolation),
    ("web_liveness_isolation", install_web_liveness_isolation),
    ("continuity_storage_capacity", install_continuity_storage_capacity_repair),
    ("render_runtime_bootstrap", install_render_runtime_bootstrap_handoff),
    ("candidate_risk_window", install_candidate_risk_window_repair),
    ("wallet_venue_lifecycle_research", install_wallet_venue_lifecycle_research),
    ("wallet_context_router", install_wallet_context_router),
    ("wallet_context_router_precision", install_wallet_context_router_precision_repair),
    ("post104_production_architecture", install_post104_production_architecture_repair),
    ("candidate_execution_evidence_plane", install_candidate_execution_evidence_plane),
    ("semantic_candidate_attribution", install_semantic_candidate_attribution_architecture),
)

_BUILT: ProductionSystem | None = None


def _install_direct_stream_first_class_guards() -> None:
    """Install the two direct-stream resource guards owned by the composition root.

    These were formerly hidden module-import side effects in production.py. Keeping
    the tiny resource envelope here makes the production order explicit without
    changing any market/economic authority.
    """
    from . import direct_solana as direct_solana_module
    from .direct_solana import DirectSolanaIngestionPlane

    if not bool(getattr(DirectSolanaIngestionPlane._handle_notification, "_roi_cooperative_yield", False)):
        original = DirectSolanaIngestionPlane._handle_notification

        async def handle(self: Any, provider: str, subscription_targets: dict[int, Any], message: dict[str, Any]) -> None:
            await original(self, provider, subscription_targets, message)
            await asyncio.sleep(0)

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

        setattr(connect, "_roi_memory_bounded", True)
        direct_solana_module.websockets.connect = connect  # type: ignore[assignment]


def _component(name: str, module: str, attribute: str, *, required: bool = True) -> ComponentHealth:
    try:
        imported = __import__(module, fromlist=[attribute])
        value = getattr(imported, attribute)
    except Exception as exc:
        return ComponentHealth(name, module, required, False, f"{type(exc).__name__}:{exc}")
    return ComponentHealth(name, module, required, callable(value) or value is not None, f"attribute={attribute}")


def _required_components() -> tuple[ComponentHealth, ...]:
    return (
        _component("ingestion", "solana_roi.direct_solana", "DirectSolanaIngestionPlane"),
        _component("evidence", "solana_roi.v51_evidence_analytics", "build_evidence_validity_bundle"),
        _component("candidate", "solana_roi.v51_candidate_ledger", "refresh_candidate_pipeline"),
        _component("strategy", "solana_roi.v51_production_authority", "install_v51_production_authority"),
        _component("execution", "solana_roi.v51_exact_exit_execution", "status"),
        _component("settlement", "solana_roi.settlement", "SettlementSimulator"),
        _component("learning", "solana_roi.v51_evidence_analytics", "build_hazard_calibration"),
        _component("certification", "solana_roi.v51_phase17_context_certification", "build_phase17_context_certification"),
        _component("portfolio", "solana_roi.portfolio", "allocate_family_capital"),
        _component("statistics", "solana_roi.statistics", "robust_profile"),
    )


def build_production_system() -> ProductionSystem:
    global _BUILT
    if _BUILT is not None:
        return _BUILT

    for _name, installer in _COMPATIBILITY_INSTALLERS:
        installer()
    _install_direct_stream_first_class_guards()

    # Preserve the canonical frozen baseline binding before api builds the runtime.
    from . import runtime as runtime_module
    from .config import BASELINE
    runtime_module.BASELINE = BASELINE

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
        compatibility_installers=tuple(name for name, _installer in _COMPATIBILITY_INSTALLERS),
    )
    if not system.healthy:
        raise RuntimeError("production composition failed closed")

    # Cold-start observability is attached to app state and is therefore available
    # without querying providers or granting strategy authority.
    app.state.roi_production_system = system
    app.state.roi_production_composition_status = system.status
    _BUILT = system
    return system


production_system = build_production_system()
app = production_system.app
ingestion_runtime = production_system.ingestion_runtime

__all__ = [
    "COMPOSITION_VERSION",
    "ComponentHealth",
    "ProductionSystem",
    "app",
    "build_production_system",
    "ingestion_runtime",
    "production_system",
]
