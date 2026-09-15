from __future__ import annotations

"""Deterministic readiness boundary for the active-storage retention registry.

The positive registry is assembled by several reconciliation modules.  Production
maintenance must never depend on whichever runtime module happened to import first.
This module imports the central manifest, registers the exact additional current
runtime tables observed in production, validates the resulting positive registry,
and only then marks the registry ready.

Registration is in-memory only: importing this module does not open, mutate, prune,
or delete production storage.
"""

from dataclasses import dataclass
from typing import Iterable

from . import storage_manifest as manifest
from . import storage_retention as base

R = base.RetentionClass
C = base.RetentionContract


@dataclass(frozen=True)
class RegistryReadiness:
    ready: bool
    deferred: bool
    reason: str | None
    registered_datasets: tuple[str, ...]


def _c(
    dataset: str,
    owner: str,
    retention_class: R,
    purpose: str,
    consumer: str,
    *,
    age: str | None = None,
    rows: int | None = None,
    bytes_: int | None = None,
    prune: str = "replace superseded state",
    startup: bool = False,
    certification: bool = False,
) -> C:
    return C(
        dataset,
        owner,
        retention_class,
        purpose,
        consumer,
        "hot",
        age,
        rows,
        bytes_,
        "archive only after positive classification",
        prune,
        startup,
        certification,
    )


# Exact current-runtime datasets observed in production on the 2026-09-15
# canonical release.  This is deliberately an exact list, not a prefix/wildcard
# allowance and not a legacy classification.
CURRENT_RUNTIME_CONTRACTS: tuple[C, ...] = (
    _c("anonymous_candidate_latency_failures", "candidate-attribution", R.DIAGNOSTIC_TTL, "Recent anonymous candidate latency failure evidence", "operations/candidate attribution", age="7d", rows=100_000, bytes_=67_108_864, prune="resolved evidence older than 7d"),
    _c("candidate_compute_admission_decisions", "candidate-compute-admission", R.BOUNDED_WINDOW, "Point-in-time expensive-compute admission decisions", "candidate certification/diagnostics", age="31d", rows=250_000, bytes_=134_217_728, prune="terminal decision older than 31d", certification=True),
    _c("context_research_bandwidth_decisions", "context-research-bandwidth", R.BOUNDED_WINDOW, "Point-in-time research bandwidth decisions", "research governance/diagnostics", age="31d", rows=250_000, bytes_=134_217_728, prune="decision older than 31d", certification=True),
    _c("continuation_market_context", "continuation-market-context", R.STRATEGY_EVIDENCE, "Point-in-time continuation market context", "v5.2 strategy/research", age="31d", rows=250_000, bytes_=134_217_728, prune="resolved context older than 31d", certification=True),
    _c("direct_solana_hydration_status_meta", "direct-solana-hydration", R.CURRENT_STATE, "Current Direct Solana hydration status frontier", "runtime/operations", rows=64, bytes_=4_194_304, startup=True, certification=True),
    _c("direct_solana_hydration_status_recent", "direct-solana-hydration", R.BOUNDED_WINDOW, "Recent Direct Solana hydration status evidence", "runtime/operations", age="7d", rows=100_000, bytes_=67_108_864, prune="terminal status older than 7d", certification=True),
    _c("direct_solana_release_continuity_epoch", "direct-solana-continuity", R.CURRENT_STATE, "Current and immediately prior Direct Solana release continuity epochs", "runtime/certification", rows=256, bytes_=8_388_608, prune="keep current frontier plus bounded prior epochs", startup=True, certification=True),
    _c("economic_signal_shadow_audit", "economic-signal-continuation", R.STRATEGY_EVIDENCE, "Lookahead-free economic signal shadow audit", "strategy/research", age="31d", rows=100_000, bytes_=67_108_864, prune="resolved audit older than 31d", certification=True),
    _c("execution_quote_failures", "execution-evidence", R.DIAGNOSTIC_TTL, "Recent paper execution quote failure evidence", "paper execution/operations", age="7d", rows=100_000, bytes_=67_108_864, prune="resolved failure older than 7d", certification=True),
    _c("fomo_learning_entry_windows", "fomo-learning", R.STRATEGY_EVIDENCE, "Point-in-time FOMO entry-window learning evidence", "v5.2 research", age="31d", rows=250_000, bytes_=134_217_728, prune="resolved evidence older than 31d", certification=True),
    _c("fomo_learning_post_entry_flow", "fomo-learning", R.STRATEGY_EVIDENCE, "Point-in-time FOMO post-entry flow learning evidence", "v5.2 research", age="31d", rows=250_000, bytes_=134_217_728, prune="resolved evidence older than 31d", certification=True),
    _c("fomo_shadow_observations", "fomo-shadow", R.STRATEGY_EVIDENCE, "FOMO shadow observations without trading authority", "v5.2 research", age="31d", rows=250_000, bytes_=134_217_728, prune="observation older than 31d after outcome resolution", certification=True),
    _c("fomo_shadow_outcomes", "fomo-shadow", R.STRATEGY_EVIDENCE, "Resolved FOMO shadow outcomes", "v5.2 research", age="31d", rows=250_000, bytes_=134_217_728, prune="resolved outcome older than 31d", certification=True),
    _c("risk_conditioned_alpha_v5_outcomes", "risk-conditioned-alpha-v5", R.STRATEGY_EVIDENCE, "Resolved risk-conditioned alpha outcomes", "v5.2 research", age="31d", rows=250_000, bytes_=134_217_728, prune="resolved outcome older than 31d", certification=True),
    _c("risk_conditioned_alpha_v5_trials", "risk-conditioned-alpha-v5", R.STRATEGY_EVIDENCE, "Point-in-time risk-conditioned alpha trials", "v5.2 research", age="31d", rows=250_000, bytes_=134_217_728, prune="resolved trial older than 31d", certification=True),
    _c("strategy_learning_compatibility_releases", "continuous-strategy-learning", R.CURRENT_STATE, "Current strategy-learning compatibility release frontier", "strategy learning/runtime", rows=256, bytes_=8_388_608, prune="keep current plus bounded prior compatibility releases", startup=True, certification=True),
    _c("strategy_learning_exit_paths", "continuous-strategy-learning", R.STRATEGY_EVIDENCE, "Point-in-time learned exit-path evidence", "strategy learning", age="31d", rows=250_000, bytes_=134_217_728, prune="resolved path older than 31d", certification=True),
    _c("strategy_learning_final_paths", "continuous-strategy-learning", R.STRATEGY_EVIDENCE, "Final resolved strategy-learning path evidence", "strategy learning", age="31d", rows=250_000, bytes_=134_217_728, prune="resolved path older than 31d", certification=True),
    _c("strategy_learning_horizon_marks", "continuous-strategy-learning", R.STRATEGY_EVIDENCE, "Forward horizon marks for point-in-time learning", "strategy learning", age="31d", rows=500_000, bytes_=201_326_592, prune="resolved mark older than 31d", certification=True),
    _c("strategy_learning_subjects", "continuous-strategy-learning", R.STRATEGY_EVIDENCE, "Subjects with unresolved/recent strategy-learning evidence", "strategy learning", age="31d", rows=250_000, bytes_=134_217_728, prune="terminal subject older than 31d", certification=True),
    _c("venue_resource_governance_decisions", "venue-resource-governance", R.BOUNDED_WINDOW, "Point-in-time venue/provider resource governance decisions", "runtime/operations", age="31d", rows=250_000, bytes_=134_217_728, prune="decision older than 31d", certification=True),
)

_EXPECTED_CURRENT_DATASETS = frozenset(contract.dataset for contract in CURRENT_RUNTIME_CONTRACTS)
_REGISTRY_READY = False


def finalize_storage_registry() -> RegistryReadiness:
    """Idempotently establish the complete exact positive registry."""

    global _REGISTRY_READY
    if _REGISTRY_READY:
        return registry_readiness()
    for contract in CURRENT_RUNTIME_CONTRACTS:
        existing = base.RETENTION_REGISTRY.get(contract.dataset)
        if existing is not None and existing != contract:
            raise RuntimeError(f"conflicting retention contract:{contract.dataset}")
        base.RETENTION_REGISTRY[contract.dataset] = contract
    manifest.validate_manifest()
    missing = sorted(_EXPECTED_CURRENT_DATASETS.difference(base.RETENTION_REGISTRY))
    if missing:
        raise RuntimeError("storage registry finalization incomplete:" + ",".join(missing))
    _REGISTRY_READY = True
    return registry_readiness()


def storage_registry_ready() -> bool:
    return bool(_REGISTRY_READY)


def registry_readiness() -> RegistryReadiness:
    ready = storage_registry_ready()
    return RegistryReadiness(
        ready=ready,
        deferred=not ready,
        reason=None if ready else "storage_registry_not_finalized",
        registered_datasets=tuple(sorted(_EXPECTED_CURRENT_DATASETS.intersection(base.RETENTION_REGISTRY))),
    )


def maintenance_readiness_gate() -> RegistryReadiness:
    """Pure preflight used by maintenance callers before any storage mutation.

    A caller receiving ``deferred=True`` must perform no mutation/deletion.  The
    production package finalizes the registry before runtime composition, so the
    normal path is ready; keeping this explicit gate makes the pre-ready safety
    contract testable and fail-closed.
    """

    return registry_readiness()


def assert_registered_after_readiness(datasets: Iterable[str]) -> None:
    if not storage_registry_ready():
        raise RuntimeError("storage registry is not finalized")
    base.assert_registered(datasets)


# Deterministic package bootstrap.  In-memory registration only.
finalize_storage_registry()


__all__ = [
    "CURRENT_RUNTIME_CONTRACTS",
    "RegistryReadiness",
    "assert_registered_after_readiness",
    "finalize_storage_registry",
    "maintenance_readiness_gate",
    "registry_readiness",
    "storage_registry_ready",
]
