from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any

COMPOSITION_VERSION = "v51-production-composition-root-125-130-v11-explicit-compatibility"
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

    def status(self) -> dict[str, Any]:
        return {
            "composition_version": COMPOSITION_VERSION,
            "healthy": self.healthy,
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


_BUILT: ProductionSystem | None = None


def build_production_system() -> ProductionSystem:
    global _BUILT
    if _BUILT is not None:
        return _BUILT

    # Package import stays passive. The exact previously green repair composition is
    # activated only from this one production root while the remaining installers are
    # migrated natively into their owner modules. This preserves the proven runtime
    # behavior without restoring hidden package-import authority.
    from . import legacy_package_runtime_composition as _legacy_package_runtime_composition
    from . import legacy_production_composition as _legacy_production_composition

    _ = _legacy_package_runtime_composition
    app = _legacy_production_composition.app
    ingestion_runtime = _legacy_production_composition.ingestion_runtime

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
