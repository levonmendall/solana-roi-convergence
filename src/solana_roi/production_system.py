from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any

COMPOSITION_VERSION = "v52-production-composition-root-authoritative-v1-over-v51-compatible-runtime-v13-batch9-finalized-v14-same-release-continuity-successor-v15-production-proof-read-boundary-v16-target-scoped-successor-evidence-v17-storage-maintenance-lock-isolation-v18-bounded-context-runtime-memory-v19-rpc-task-terminal-ownership-v20-certification-single-flight"
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
class ProductionSystem:
    app: Any
    ingestion_runtime: Any
    components: tuple[ComponentHealth, ...]

    @property
    def healthy(self) -> bool:
        return all(component.available for component in self.components if component.required)

    def _paper_lifecycle_status(self) -> dict[str, Any]:
        try:
            from . import v51_paper_lifecycle_runtime as lifecycle

            return dict(lifecycle.status())
        except Exception as exc:
            return {
                "installed": False,
                "worker_running": False,
                "lifecycle_proven": False,
                "last_error": f"{type(exc).__name__}:{exc}",
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
            }

    def _runtime_memory_capacity_status(self) -> dict[str, Any]:
        try:
            from . import runtime_memory_capacity_repair as memory_capacity

            return dict(memory_capacity.status())
        except Exception as exc:
            return {
                "installed": False,
                "last_error": f"{type(exc).__name__}:{exc}",
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
            }

    def _rpc_task_ownership_status(self) -> dict[str, Any]:
        try:
            from . import rpc_task_ownership_repair as task_ownership

            return dict(task_ownership.status())
        except Exception as exc:
            return {
                "installed": False,
                "last_error": f"{type(exc).__name__}:{exc}",
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
            }

    def status(self) -> dict[str, Any]:
        lifecycle = self._paper_lifecycle_status()
        runtime_memory_capacity = self._runtime_memory_capacity_status()
        rpc_task_ownership = self._rpc_task_ownership_status()
        code_present = bool(self.healthy and lifecycle.get("installed"))
        worker_active = bool(lifecycle.get("worker_running"))
        lifecycle_proven = bool(lifecycle.get("lifecycle_proven"))
        if lifecycle_proven:
            lifecycle_state = "LIFECYCLE_PROVEN"
        elif worker_active:
            lifecycle_state = "WORKER_ACTIVE"
        elif code_present:
            lifecycle_state = "CODE_PRESENT"
        else:
            lifecycle_state = "UNAVAILABLE"
        return {
            "composition_version": COMPOSITION_VERSION,
            "healthy": self.healthy,
            "composition_healthy": self.healthy,
            "health_semantics": {
                "code_present": code_present,
                "worker_active": worker_active,
                "lifecycle_proven": lifecycle_proven,
                "state": lifecycle_state,
            },
            "authoritative_strategy": "v5.2",
            "authoritative_strategy_version": getattr(
                self.app.state, "roi_authoritative_strategy_version", None
            ),
            "authority_id": getattr(self.app.state, "roi_authority_id", None),
            "authority_fingerprint": getattr(self.app.state, "roi_authority_fingerprint", None),
            "economic_freeze_epoch": getattr(self.app.state, "roi_economic_freeze_epoch", None),
            "v52_final_economic_authority": bool(
                getattr(self.app.state, "roi_v52_final_economic_authority", False)
            ),
            "v51_final_economic_authority": bool(
                getattr(self.app.state, "roi_v51_final_economic_authority", False)
            ),
            "v51_shadow_control": bool(getattr(self.app.state, "roi_v51_shadow_control", False)),
            "paper_execution_lifecycle": lifecycle,
            "runtime_memory_capacity": runtime_memory_capacity,
            "rpc_task_ownership": rpc_task_ownership,
            "components": {component.name: component.as_dict() for component in self.components},
            "required_component_count": sum(1 for component in self.components if component.required),
            "unavailable_required_components": [
                component.name for component in self.components if component.required and not component.available
            ],
            "compatibility_adapters": [],
            "compatibility_adapter_registry_retired": True,
            "compatibility_runtime_migration_active": True,
            "compatibility_runtime_roots": [
                "solana_roi.legacy_package_runtime_composition",
                "solana_roi.legacy_production_composition",
            ],
            "single_production_composition_root": True,
            "package_import_has_runtime_install_side_effects": False,
            "production_entrypoint": "solana_roi.production:app",
            "composition_status_path": COMPOSITION_STATUS_PATH,
            "e2e_status_read_boundary": bool(getattr(self.app.state, "roi_e2e_status_read_boundary", False)),
            "e2e_status_read_boundary_version": getattr(
                self.app.state, "roi_e2e_status_read_boundary_version", None
            ),
            "production_proof_read_boundary": bool(
                getattr(self.app.state, "roi_production_proof_read_boundary", False)
            ),
            "production_proof_read_boundary_version": getattr(
                self.app.state, "roi_production_proof_read_boundary_version", None
            ),
            "certification_generation_single_flight": bool(
                getattr(self.app.state, "roi_certification_generation_single_flight", False)
            ),
            "certification_generation_runtime_repair_version": getattr(
                self.app.state, "roi_certification_generation_runtime_repair_version", None
            ),
            "forward_certification_http_deep_builder_disabled": bool(
                getattr(self.app.state, "roi_forward_certification_http_deep_builder_disabled", False)
            ),
            "batch9_continuity_frontier_proof_repair": bool(
                getattr(self.app.state, "roi_batch9_continuity_frontier_proof_repair", False)
            ),
            "batch9_continuity_frontier_proof_repair_version": getattr(
                self.app.state, "roi_batch9_continuity_frontier_proof_repair_version", None
            ),
            "batch9_canonical_contracts_preserved": bool(
                getattr(self.app.state, "roi_batch9_canonical_contracts_preserved", False)
            ),
            "same_release_continuity_epoch_repair": bool(
                getattr(self.app.state, "roi_same_release_continuity_epoch_repair", False)
            ),
            "same_release_continuity_epoch_repair_version": getattr(
                self.app.state, "roi_same_release_continuity_epoch_repair_version", None
            ),
            "target_scoped_successor_evidence_repair": bool(
                getattr(self.app.state, "roi_target_scoped_successor_evidence_repair", False)
            ),
            "target_scoped_successor_evidence_repair_version": getattr(
                self.app.state, "roi_target_scoped_successor_evidence_repair_version", None
            ),
            "storage_maintenance_lock_isolation": bool(
                getattr(self.app.state, "roi_storage_maintenance_lock_isolation", False)
            ),
            "storage_maintenance_lock_isolation_version": getattr(
                self.app.state, "roi_storage_maintenance_lock_isolation_version", None
            ),
            "runtime_memory_capacity_repair": bool(
                getattr(self.app.state, "roi_runtime_memory_capacity_repair", False)
            ),
            "runtime_memory_capacity_repair_version": getattr(
                self.app.state, "roi_runtime_memory_capacity_repair_version", None
            ),
            "rpc_task_ownership_repair": bool(
                getattr(self.app.state, "roi_rpc_task_ownership_repair", False)
            ),
            "rpc_task_ownership_repair_version": getattr(
                self.app.state, "roi_rpc_task_ownership_repair_version", None
            ),
            "paper_only": PAPER_ONLY,
            "live_money_authority": LIVE_MONEY_AUTHORITY,
            "signing_available": SIGNING_AVAILABLE,
            "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        }


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
        _component("strategy", "solana_roi.strategy_v52_authority", "authority"),
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
                "composition_healthy": False,
                "health_semantics": {
                    "code_present": False,
                    "worker_active": False,
                    "lifecycle_proven": False,
                    "state": "UNAVAILABLE",
                },
                "reason": "production_system_not_attached",
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
            }
        return system.status()


_BUILT: ProductionSystem | None = None


def build_production_system() -> ProductionSystem:
    global _BUILT
    if _BUILT is not None:
        return _BUILT

    from . import legacy_package_runtime_composition as _legacy_package_runtime_composition
    from . import legacy_production_composition as _legacy_production_composition
    from . import render_runtime_bootstrap_repair as _render_runtime_bootstrap
    from .batch9_finalization_repair import install_batch9_finalization_repair
    from .certification_generation_runtime_repair import install_certification_generation_runtime_repair
    from .certification_proof_memory_repair import install_certification_proof_memory_repair
    from .e2e_status_read_boundary_repair import install_e2e_status_read_boundary_repair
    from .production_proof_read_boundary_repair import install_production_proof_read_boundary_repair
    from .rpc_task_ownership_repair import (
        REPAIR_VERSION as RPC_TASK_OWNERSHIP_REPAIR_VERSION,
        install_rpc_task_ownership_repair,
    )
    from .runtime_memory_capacity_repair import (
        REPAIR_VERSION as RUNTIME_MEMORY_CAPACITY_REPAIR_VERSION,
        install_runtime_memory_capacity_repair,
    )
    from .same_release_continuity_epoch_repair import (
        REPAIR_VERSION as SAME_RELEASE_CONTINUITY_REPAIR_VERSION,
        install_same_release_continuity_epoch_repair,
    )
    from .storage_maintenance_lock_isolation_repair import (
        REPAIR_VERSION as STORAGE_MAINTENANCE_LOCK_ISOLATION_VERSION,
        install_storage_maintenance_lock_isolation,
    )
    from .target_scoped_successor_evidence_repair import (
        REPAIR_VERSION as TARGET_SCOPED_SUCCESSOR_EVIDENCE_REPAIR_VERSION,
        install_target_scoped_successor_evidence_repair,
    )
    from .v52_production_authority import install_v52_production_authority

    _ = _legacy_package_runtime_composition
    app = _legacy_production_composition.app
    ingestion_runtime = _legacy_production_composition.ingestion_runtime

    install_runtime_memory_capacity_repair()
    app.state.roi_runtime_memory_capacity_repair = True
    app.state.roi_runtime_memory_capacity_repair_version = RUNTIME_MEMORY_CAPACITY_REPAIR_VERSION
    install_rpc_task_ownership_repair()
    app.state.roi_rpc_task_ownership_repair = True
    app.state.roi_rpc_task_ownership_repair_version = RPC_TASK_OWNERSHIP_REPAIR_VERSION
    install_storage_maintenance_lock_isolation()
    app.state.roi_storage_maintenance_lock_isolation = True
    app.state.roi_storage_maintenance_lock_isolation_version = STORAGE_MAINTENANCE_LOCK_ISOLATION_VERSION
    install_batch9_finalization_repair(app)
    install_same_release_continuity_epoch_repair()
    app.state.roi_same_release_continuity_epoch_repair = True
    app.state.roi_same_release_continuity_epoch_repair_version = SAME_RELEASE_CONTINUITY_REPAIR_VERSION
    install_target_scoped_successor_evidence_repair()
    app.state.roi_target_scoped_successor_evidence_repair = True
    app.state.roi_target_scoped_successor_evidence_repair_version = (
        TARGET_SCOPED_SUCCESSOR_EVIDENCE_REPAIR_VERSION
    )
    install_e2e_status_read_boundary_repair(app, ingestion_runtime)
    install_production_proof_read_boundary_repair(app)
    install_certification_generation_runtime_repair(app)
    install_certification_proof_memory_repair(app)

    # v5.1-named modules above remain proven compatibility infrastructure. Install
    # the frozen v5.2 economic authority only after that substrate is complete so
    # Solana, FOMO and Robinhood all have exactly one final decision owner.
    install_v52_production_authority(app, ingestion_runtime)

    # The certification worker wraps a chain that already owns both E2E and
    # production-proof snapshot publishers. Preserve those marker contracts on
    # the new outer worker so later app/test composition remains idempotent and
    # cannot re-wrap the same mutable delegate globals into a recursion cycle.
    certification_workers = _render_runtime_bootstrap._run_runtime_workers
    if bool(getattr(certification_workers, "_roi_forward_certification_snapshot_worker", False)):
        setattr(certification_workers, "_roi_e2e_status_snapshot_worker", True)
        setattr(certification_workers, "_roi_production_proof_snapshot_worker", True)

    components = _required_components()
    missing = [component.name for component in components if component.required and not component.available]
    if missing:
        raise RuntimeError("mandatory production components unavailable: " + ",".join(missing))

    system = ProductionSystem(
        app=app,
        ingestion_runtime=ingestion_runtime,
        components=components,
    )
    if not system.healthy:
        raise RuntimeError("production composition failed closed")

    if not bool(getattr(app.state, "roi_v52_final_economic_authority", False)):
        raise RuntimeError("v5.2 final economic authority not installed")
    if bool(getattr(app.state, "roi_v51_final_economic_authority", False)):
        raise RuntimeError("v5.1 retained final economic authority after v5.2 cutover")

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
    "ComponentHealth",
    "ProductionSystem",
    "app",
    "build_production_system",
    "ingestion_runtime",
    "production_system",
]
