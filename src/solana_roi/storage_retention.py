from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable


class RetentionClass(str, Enum):
    CURRENT_STATE = "CURRENT_STATE"
    TRADE_HISTORY = "TRADE_HISTORY"
    STRATEGY_EVIDENCE = "STRATEGY_EVIDENCE"
    WALLET_INTELLIGENCE_CURRENT = "WALLET_INTELLIGENCE_CURRENT"
    WALLET_INTELLIGENCE_ARCHIVE = "WALLET_INTELLIGENCE_ARCHIVE"
    RESEARCH_ARCHIVE = "RESEARCH_ARCHIVE"
    BOUNDED_WINDOW = "BOUNDED_WINDOW"
    TRANSPORT_JOURNAL = "TRANSPORT_JOURNAL"
    DIAGNOSTIC_TTL = "DIAGNOSTIC_TTL"
    REBUILDABLE = "REBUILDABLE"
    TEMPORARY = "TEMPORARY"
    LEGACY_UNCLASSIFIED = "LEGACY_UNCLASSIFIED"


@dataclass(frozen=True)
class RetentionContract:
    dataset: str
    owner: str
    retention_class: RetentionClass
    purpose: str
    consumer: str
    hot_or_cold: str
    max_hot_age: str | None
    max_hot_rows: int | None
    max_hot_bytes: int | None
    archive_policy: str
    prune_condition: str
    startup_access: bool
    certification_access: bool


# These are the only datasets that belong in a freshly created active store.
# Legacy tables are intentionally not silently registered here: adding a new
# durable dataset requires an explicit retention contract.
_ACTIVE_CONTRACTS = (
    RetentionContract("system_current", "runtime", RetentionClass.CURRENT_STATE, "Current release, schema, migration and system watermarks", "runtime/bootstrap", "hot", None, 64, 1_048_576, "checkpoint", "replace by key", True, True),
    RetentionContract("strategy_current", "strategy", RetentionClass.CURRENT_STATE, "Current v5.2 strategy state and version", "strategy/runtime", "hot", None, 64, 4_194_304, "checkpoint", "replace superseded state", True, True),
    RetentionContract("wallet_current", "wallet-intelligence", RetentionClass.WALLET_INTELLIGENCE_CURRENT, "Current wallet scores, rolling features, confidence and provenance", "v5.2/wallet-intelligence", "hot", "rolling", 250_000, 268_435_456, "wallet archive after positive classification", "replace superseded current wallet state", True, True),
    RetentionContract("provider_current", "providers", RetentionClass.CURRENT_STATE, "Current provider capability, freshness and source watermarks", "runtime/certification", "hot", None, 512, 4_194_304, "checkpoint", "replace by provider", True, True),
    RetentionContract("portfolio_current", "paper-engine", RetentionClass.CURRENT_STATE, "Current paper portfolio, fills, positions and marks required for continuation", "paper-engine", "hot", None, 4096, 33_554_432, "exact trade history separately", "replace superseded current state", True, True),
    RetentionContract("active_candidates", "strategy", RetentionClass.CURRENT_STATE, "Legitimate unresolved strategy candidates", "v5.2/paper-engine", "hot", "until terminal", 250_000, 134_217_728, "trade/strategy archive only when justified", "delete after terminal outcome is durably represented", True, True),
    RetentionContract("active_lifecycles", "strategy", RetentionClass.CURRENT_STATE, "Legitimate unresolved lifecycle state", "v5.2/runtime", "hot", "until terminal", 250_000, 134_217_728, "strategy/trade archive only when justified", "delete after terminal lifecycle settlement", True, True),
    RetentionContract("checkpoint_current", "storage", RetentionClass.CURRENT_STATE, "Trusted continuation checkpoint and semantic-equivalence hashes", "runtime/certifier", "hot", None, 8, 33_554_432, "retain sealed checkpoint with epoch", "replace after verified successor", True, True),
    RetentionContract("continuity_current", "continuity", RetentionClass.CURRENT_STATE, "Current continuity epochs, boundaries and high-water marks", "runtime/certifier", "hot", None, 4096, 16_777_216, "checkpoint", "replace superseded continuity state", True, True),
    RetentionContract("certification_current", "certification", RetentionClass.CURRENT_STATE, "Current certification state, release epoch and acknowledgement watermarks", "certifier", "hot", None, 4096, 16_777_216, "checkpoint", "replace superseded certification state", True, True),
    RetentionContract("bounded_market_evidence", "market-evidence", RetentionClass.BOUNDED_WINDOW, "Recent market evidence needed for live decisions and exact decision snapshots", "v5.2/runtime", "hot", "bounded-window", 2_000_000, 536_870_912, "retain decision/trade evidence only", "age/row/byte budget", False, True),
    RetentionContract("bounded_forward_deltas", "storage", RetentionClass.BOUNDED_WINDOW, "Forward-only deltas newer than the trusted checkpoint", "runtime/certifier", "hot", "until checkpointed", 2_000_000, 536_870_912, "fold into verified checkpoint", "verified checkpoint advances beyond delta", True, True),
    RetentionContract("bounded_transport_state", "certification", RetentionClass.TRANSPORT_JOURNAL, "Replication transport pending durable acknowledgement", "authoritative/certifier", "hot", "until acknowledged", 1_000_000, 268_435_456, "none", "durably applied and acknowledged", True, True),
    RetentionContract("bounded_recent_diagnostics", "operations", RetentionClass.DIAGNOSTIC_TTL, "Recent operational evidence needed to diagnose unresolved defects", "operations", "hot", "ttl", 250_000, 134_217_728, "compact incident bundle only", "ttl or workflow completion", False, False),
    RetentionContract("storage_epoch_state", "storage", RetentionClass.CURRENT_STATE, "Active storage epoch identity, budgets and rollover state", "runtime/operations", "hot", None, 8, 1_048_576, "seal with checkpoint", "replace by current epoch", True, True),
)

RETENTION_REGISTRY: dict[str, RetentionContract] = {c.dataset: c for c in _ACTIVE_CONTRACTS}


def contract_for(dataset: str) -> RetentionContract:
    try:
        return RETENTION_REGISTRY[dataset]
    except KeyError as exc:
        raise ValueError(f"unregistered persistent dataset: {dataset}") from exc


def assert_registered(datasets: Iterable[str]) -> None:
    unknown = sorted({name for name in datasets if name not in RETENTION_REGISTRY})
    if unknown:
        raise ValueError("unregistered persistent datasets: " + ", ".join(unknown))


def startup_table_allowlist() -> tuple[str, ...]:
    return tuple(sorted(c.dataset for c in _ACTIVE_CONTRACTS if c.startup_access))


def certification_table_allowlist() -> tuple[str, ...]:
    return tuple(sorted(c.dataset for c in _ACTIVE_CONTRACTS if c.certification_access))


def validate_registry() -> None:
    if len(RETENTION_REGISTRY) != len(_ACTIVE_CONTRACTS):
        raise ValueError("duplicate retention dataset")
    for contract in _ACTIVE_CONTRACTS:
        if not contract.purpose.strip() or not contract.consumer.strip():
            raise ValueError(f"dataset {contract.dataset} lacks concrete purpose/consumer")
        if contract.hot_or_cold not in {"hot", "cold"}:
            raise ValueError(f"dataset {contract.dataset} has invalid hot_or_cold")
        if contract.retention_class is RetentionClass.LEGACY_UNCLASSIFIED:
            raise ValueError(f"legacy-unclassified dataset cannot be created in active storage: {contract.dataset}")


validate_registry()
