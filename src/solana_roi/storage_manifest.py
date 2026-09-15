from __future__ import annotations

from . import storage_retention as base
# These modules extend the same positive registry with persistence that was
# added after the original storage branch: current v5.2 validation state,
# constructor-reachable runtime continuity/transport state, and the exact
# non-authoritative legacy tables observed in the verified compact successor.
# Importing them here makes the central manifest the single registration
# boundary used by active schema validation and certification scope.
from . import storage_current_v52_reconciliation as _current_v52_reconciliation  # noqa: F401
from . import storage_runtime_persistence_reconciliation as _runtime_persistence_reconciliation  # noqa: F401
from . import storage_legacy_schema_reconciliation as _legacy_schema_reconciliation

R = base.RetentionClass
C = base.RetentionContract


def _c(name: str, owner: str, cls: R, purpose: str, consumer: str, *, age: str | None = None, rows: int | None = None, bytes_: int | None = None, prune: str = "replace superseded state", startup: bool = False, certification: bool = False) -> C:
    return C(name, owner, cls, purpose, consumer, "hot", age, rows, bytes_, "archive only after positive classification", prune, startup, certification)


# Explicit production compatibility contracts.  These are *not* permission to
# import their historical contents.  Migration copies only the bounded/current
# rows selected by storage_current_state_extractor or storage_shadow_migration.
EXTRA_CONTRACTS = (
    _c("system_status","runtime",R.CURRENT_STATE,"Current system status singleton/state","runtime",rows=256,bytes_=4_194_304,startup=True,certification=True),
    _c("system_runtime_state","runtime",R.CURRENT_STATE,"Current runtime composition/worker state","runtime",rows=4096,bytes_=16_777_216,startup=True,certification=True),
    _c("strategy_controls","strategy",R.CURRENT_STATE,"Current governed strategy controls and versions","strategy",rows=256,bytes_=4_194_304,startup=True,certification=True),
    _c("provider_health","providers",R.CURRENT_STATE,"Current provider health/capability state","providers",rows=4096,bytes_=16_777_216,startup=True,certification=True),
    _c("provider_runtime_state","providers",R.CURRENT_STATE,"Current provider runtime/failover state","providers",rows=4096,bytes_=16_777_216,startup=True,certification=True),
    _c("paper_engine_snapshot","paper-engine",R.CURRENT_STATE,"Current paper-engine publication snapshot","api/certifier",rows=64,bytes_=16_777_216,startup=True,certification=True),
    _c("discovery_candidates","discovery",R.CURRENT_STATE,"Unresolved/current discovery candidates only","discovery/strategy",age="until terminal",rows=250_000,bytes_=134_217_728,prune="terminal after durable outcome",startup=True,certification=True),
    _c("candidate_lifecycle","strategy",R.CURRENT_STATE,"Unresolved/current candidate lifecycle state","strategy",age="until terminal",rows=250_000,bytes_=134_217_728,prune="terminal after durable outcome",startup=True,certification=True),
    _c("lifecycle_audit","strategy",R.BOUNDED_WINDOW,"Recent lifecycle transition audit needed for unresolved candidates","strategy/diagnostics",age="31d",rows=250_000,bytes_=134_217_728,prune="31d after terminal",certification=True),
    _c("wallet_performance","wallet-intelligence",R.WALLET_INTELLIGENCE_CURRENT,"Current wallet performance materialization","wallet intelligence",rows=250_000,bytes_=134_217_728,startup=True,certification=True),
    _c("wallet_scorecard","wallet-intelligence",R.WALLET_INTELLIGENCE_CURRENT,"Current wallet scorecard materialization","wallet intelligence",rows=250_000,bytes_=134_217_728,startup=True,certification=True),
    _c("wallet_universe_state","wallet-intelligence",R.CURRENT_STATE,"Current wallet universe frontier/state","wallet intelligence",rows=4096,bytes_=16_777_216,startup=True,certification=True),
    _c("wallet_seed_source","wallet-intelligence",R.CURRENT_STATE,"Current wallet seed source frontier","wallet intelligence",rows=4096,bytes_=16_777_216,startup=True,certification=True),
    _c("wallet_seed_candidates","wallet-intelligence",R.WALLET_INTELLIGENCE_CURRENT,"Current candidate wallet seed set","wallet intelligence",rows=250_000,bytes_=134_217_728,startup=True,certification=True),
    _c("wallet_seed_eligibility","wallet-intelligence",R.WALLET_INTELLIGENCE_CURRENT,"Current seed eligibility materialization","wallet intelligence",rows=250_000,bytes_=134_217_728,startup=True,certification=True),
    _c("wallet_runtime_state","wallet-intelligence",R.WALLET_INTELLIGENCE_CURRENT,"Current wallet worker/runtime state","wallet intelligence",rows=250_000,bytes_=134_217_728,startup=True,certification=True),
    _c("wallet_intelligence_snapshots","wallet-intelligence",R.WALLET_INTELLIGENCE_CURRENT,"Latest plus bounded wallet promotion evidence","ContinuousWalletIntelligence",age="31d",rows=250_000,bytes_=134_217_728,prune="keep latest per wallet plus 31d",startup=True,certification=True),
    _c("adaptive_wallet_cohorts","wallet-intelligence",R.CURRENT_STATE,"Governed proposed/approved cohort versions","ContinuousWalletIntelligence",rows=4096,bytes_=16_777_216,startup=True,certification=True),
    _c("discovery_universe_state","discovery",R.CURRENT_STATE,"Current discovery universe frontier","discovery",rows=4096,bytes_=16_777_216,startup=True,certification=True),
    _c("stream_cursor_state","ingestion",R.CURRENT_STATE,"Current ingestion sequence/cursor watermarks","ingestion",rows=16_384,bytes_=32_554_432,startup=True,certification=True),
    _c("market_quotes","market-data",R.BOUNDED_WINDOW,"Recent quotes needed for active/recent candidates","runtime",age="31d",rows=1_000_000,bytes_=268_435_456,prune="31d except latest active-subject mark",certification=True),
    _c("observation_events","ingestion",R.BOUNDED_WINDOW,"Post-transition normalized observation tail","runtime",age="31d",rows=500_000,bytes_=268_435_456,prune="checkpointed tail older than 31d",startup=True,certification=True),
    _c("incremental_event_integrity_repair_state","continuity",R.CURRENT_STATE,"Current event-integrity anchor/frontier","continuity",rows=64,bytes_=4_194_304,startup=True,certification=True),
    _c("incremental_event_integrity_repair_diagnostic","continuity",R.DIAGNOSTIC_TTL,"Recent integrity repair diagnostics","operations",age="7d",rows=100_000,bytes_=67_108_864,prune="7d",certification=False),
    _c("rolling_recent_window_state","runtime",R.CURRENT_STATE,"Current rolling-window frontier","runtime",rows=250_000,bytes_=67_108_864,startup=True,certification=True),
    _c("rolling_window_index","runtime",R.BOUNDED_WINDOW,"Bounded rolling evidence index","runtime",age="31d",rows=1_000_000,bytes_=268_435_456,prune="window expiry",certification=True),
    _c("wallet_bounded_evidence","wallet-intelligence",R.BOUNDED_WINDOW,"Bounded current wallet evidence","wallet intelligence",age="31d",rows=1_000_000,bytes_=268_435_456,prune="31d/row budget",certification=True),
    _c("closed_trade_history","paper-engine",R.TRADE_HISTORY,"Recent exact closed paper trades needed for live metrics","paper-engine/metrics",age="31d",rows=100_000,bytes_=67_108_864,prune="older than 31d after cold archive classification",certification=True),
    _c("continuity_epoch","continuity",R.CURRENT_STATE,"Current/recent continuity epochs and high-water marks","continuity",rows=4096,bytes_=16_777_216,startup=True,certification=True),
    _c("replication_watermark","certification",R.CURRENT_STATE,"Current replication acknowledgement/frontier","certification",rows=4096,bytes_=16_777_216,startup=True,certification=True),
    _c("graceful_shutdown_state","runtime",R.CURRENT_STATE,"Last graceful shutdown/restart handoff state","runtime",rows=128,bytes_=4_194_304,startup=True,certification=True),
    _c("certification_release","certification",R.CURRENT_STATE,"Current/recent release certification identity","certification",rows=128,bytes_=16_777_216,startup=True,certification=True),
    _c("certification_release_evidence","certification",R.BOUNDED_WINDOW,"Bounded evidence for current/recent release","certification",age="31d",rows=10_000,bytes_=67_108_864,prune="release no longer current and 31d",certification=True),
    _c("certification_state","certification",R.CURRENT_STATE,"Current certification machine state","certification",rows=4096,bytes_=16_777_216,startup=True,certification=True),
    _c("replication_handshake","certification",R.CURRENT_STATE,"Current replication handshake state","certification",rows=128,bytes_=4_194_304,startup=True,certification=True),
    _c("replication_handshake_ack","certification",R.CURRENT_STATE,"Current replication handshake acknowledgement","certification",rows=128,bytes_=4_194_304,startup=True,certification=True),
    _c("parity_check","certification",R.BOUNDED_WINDOW,"Recent active-state parity proofs","certification",age="31d",rows=4096,bytes_=16_777_216,prune="31d",certification=True),
    _c("certification_cycle_evidence","certification",R.BOUNDED_WINDOW,"Recent certification-cycle evidence","certification",age="31d",rows=10_000,bytes_=67_108_864,prune="31d",certification=True),
    _c("certification_replication_changes","certification",R.TRANSPORT_JOURNAL,"Unacknowledged row-identity delta journal","certification",age="until acknowledged",rows=1_000_000,bytes_=268_435_456,prune="acknowledged",startup=True,certification=True),
    _c("certification_replication_meta","certification",R.CURRENT_STATE,"Replication epoch/schema fingerprint metadata","certification",rows=64,bytes_=1_048_576,startup=True,certification=True),
)

for contract in EXTRA_CONTRACTS:
    existing = base.RETENTION_REGISTRY.get(contract.dataset)
    if existing is not None and existing != contract:
        raise RuntimeError(f"conflicting retention contract:{contract.dataset}")
    base.RETENTION_REGISTRY[contract.dataset] = contract

RETENTION_REGISTRY = base.RETENTION_REGISTRY
RetentionClass = base.RetentionClass
RetentionContract = base.RetentionContract
contract_for = base.contract_for
assert_registered = base.assert_registered


def startup_table_allowlist() -> tuple[str,...]:
    return tuple(sorted(name for name,c in RETENTION_REGISTRY.items() if c.startup_access))


def certification_table_allowlist() -> tuple[str,...]:
    return tuple(sorted(name for name,c in RETENTION_REGISTRY.items() if c.certification_access))


def validate_manifest() -> None:
    exact_legacy = {
        contract.dataset: contract
        for contract in _legacy_schema_reconciliation.LEGACY_RETAINED_CONTRACTS
    }
    for name, contract in RETENTION_REGISTRY.items():
        if contract.retention_class is RetentionClass.LEGACY_UNCLASSIFIED:
            expected = exact_legacy.get(name)
            if expected is None or contract != expected:
                raise RuntimeError(
                    f"legacy-unclassified dataset is not an exact approved retained-legacy contract:{name}"
                )
            if contract.startup_access or contract.certification_access:
                raise RuntimeError(f"retained legacy dataset cannot have runtime/certification access:{name}")
            if contract.hot_or_cold != "cold":
                raise RuntimeError(f"retained legacy dataset must remain cold/non-authoritative:{name}")
            continue
        if contract.hot_or_cold != "hot":
            raise RuntimeError(f"cold non-legacy dataset cannot enter active database:{name}")


validate_manifest()
