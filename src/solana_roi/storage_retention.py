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


def _c(
    dataset: str,
    owner: str,
    retention_class: RetentionClass,
    purpose: str,
    consumer: str,
    *,
    age: str | None = None,
    rows: int | None = None,
    bytes_: int | None = None,
    archive: str = "none",
    prune: str = "replace superseded state",
    startup: bool = False,
    certification: bool = False,
) -> RetentionContract:
    return RetentionContract(
        dataset=dataset,
        owner=owner,
        retention_class=retention_class,
        purpose=purpose,
        consumer=consumer,
        hot_or_cold="hot",
        max_hot_age=age,
        max_hot_rows=rows,
        max_hot_bytes=bytes_,
        archive_policy=archive,
        prune_condition=prune,
        startup_access=startup,
        certification_access=certification,
    )


# Positive registry: an active database may only contain datasets listed here.
# Historical legacy tables are not implicitly admitted.  Compatibility tables
# below are intentionally bounded adapters used while production consumers are
# moved onto the compact logical current-state contracts.
_ACTIVE_CONTRACTS = (
    _c("system_current", "runtime", RetentionClass.CURRENT_STATE, "Current release/schema/system watermarks", "runtime/bootstrap", rows=64, bytes_=1_048_576, startup=True, certification=True),
    _c("strategy_current", "strategy", RetentionClass.CURRENT_STATE, "Current v5.2 strategy state/version/governance", "strategy/runtime", rows=128, bytes_=4_194_304, startup=True, certification=True),
    _c("wallet_current", "wallet-intelligence", RetentionClass.WALLET_INTELLIGENCE_CURRENT, "Current wallet scores/features/confidence/provenance", "v5.2/wallet-intelligence", age="rolling", rows=250_000, bytes_=268_435_456, archive="wallet archive after positive classification", startup=True, certification=True),
    _c("provider_current", "providers", RetentionClass.CURRENT_STATE, "Current provider capability/freshness/source watermarks", "runtime/certification", rows=2048, bytes_=8_388_608, startup=True, certification=True),
    _c("portfolio_current", "paper-engine", RetentionClass.CURRENT_STATE, "Exact paper portfolio/candidates/positions/fills/marks continuation checkpoint", "paper-engine", rows=4096, bytes_=33_554_432, archive="exact closed-trade archive separately", startup=True, certification=True),
    _c("active_candidates", "strategy", RetentionClass.CURRENT_STATE, "Unresolved decision candidates", "v5.2/paper-engine", age="until terminal", rows=250_000, bytes_=134_217_728, archive="strategy/trade archive only when justified", prune="terminal and durably represented", startup=True, certification=True),
    _c("active_lifecycles", "strategy", RetentionClass.CURRENT_STATE, "Unresolved lifecycle state", "v5.2/runtime", age="until terminal", rows=250_000, bytes_=134_217_728, archive="strategy/trade archive only when justified", prune="terminal and durably represented", startup=True, certification=True),
    _c("checkpoint_current", "storage", RetentionClass.CURRENT_STATE, "Trusted transition checkpoint and semantic hashes", "runtime/certifier", rows=8, bytes_=33_554_432, archive="seal with epoch", startup=True, certification=True),
    _c("continuity_current", "continuity", RetentionClass.CURRENT_STATE, "Current continuity epochs/boundaries/high-water marks", "runtime/certifier", rows=4096, bytes_=16_777_216, startup=True, certification=True),
    _c("certification_current", "certification", RetentionClass.CURRENT_STATE, "Current certification release/epoch/acknowledgement state", "certifier", rows=4096, bytes_=16_777_216, startup=True, certification=True),
    _c("bounded_market_evidence", "market-evidence", RetentionClass.BOUNDED_WINDOW, "Recent evidence required for current decisions", "v5.2/runtime", age="31d", rows=2_000_000, bytes_=536_870_912, archive="retain only positively required decision/trade evidence", prune="31d/row/byte budget", certification=True),
    _c("bounded_forward_deltas", "storage", RetentionClass.BOUNDED_WINDOW, "Forward deltas newer than trusted checkpoint", "runtime/certifier", age="until checkpointed", rows=2_000_000, bytes_=536_870_912, archive="fold into verified checkpoint", prune="verified checkpoint advances beyond delta", startup=True, certification=True),
    _c("bounded_transport_state", "certification", RetentionClass.TRANSPORT_JOURNAL, "Unacknowledged certification transport journal", "authoritative/certifier", age="until acknowledged", rows=1_000_000, bytes_=268_435_456, prune="durably applied and acknowledged", startup=True, certification=True),
    _c("bounded_recent_diagnostics", "operations", RetentionClass.DIAGNOSTIC_TTL, "Recent unresolved operational evidence", "operations", age="7d", rows=250_000, bytes_=134_217_728, archive="compact incident bundle only", prune="ttl or incident completion"),
    _c("storage_epoch_state", "storage", RetentionClass.CURRENT_STATE, "Active storage epoch identity/budgets/rollover state", "runtime/operations", rows=8, bytes_=1_048_576, archive="seal with checkpoint", startup=True, certification=True),

    # v5.2 market-validation persistence.  Calibration reads at most 250 values
    # per (lane, feature); retain exactly that decision window rather than the
    # unbounded append history that existed before this repair.
    _c("v52_market_validation_features", "v5.2-market-validation", RetentionClass.STRATEGY_EVIDENCE, "Lane-relative/graduation/decay calibration values", "MarketValidationController", age="31d", rows=75_000, bytes_=67_108_864, archive="research archive only after positive classification", prune="keep newest 250 per lane+feature and at most 31d", certification=True),
    _c("v52_market_validation_shadow_outcomes", "v5.2-market-validation", RetentionClass.STRATEGY_EVIDENCE, "Realized/counterfactual lane-alpha evidence", "MarketValidationController/MarketValidationGovernance", age="31d", rows=100_000, bytes_=67_108_864, archive="research archive only", prune="resolved older than 31d after validation windows", certification=True),
    _c("v52_lane_gate_state", "v5.2-market-validation", RetentionClass.CURRENT_STATE, "Current hysteretic lane eligibility state", "MarketValidationGovernance", rows=64, bytes_=1_048_576, startup=True, certification=True),
    _c("v52_market_validation_point_in_time", "v5.2-market-validation", RetentionClass.STRATEGY_EVIDENCE, "Lookahead-free decision snapshots for 24h/7d/30d validation", "MarketValidationGovernance", age="31d", rows=150_000, bytes_=134_217_728, archive="research archive only", prune="older than 31d once outcomes represented", certification=True),

    # Wallet forward-alpha persistence.  The longest production validation
    # window is 30d and the estimator half-life is 72h, so 31d is sufficient
    # for live scoring while preventing a new all-history database.
    _c("v52_wallet_point_in_time_observations", "wallet-forward-alpha", RetentionClass.WALLET_INTELLIGENCE_ARCHIVE, "Lookahead-free wallet/candidate observations used by forward-alpha scoring", "WalletForwardAlphaEngine", age="31d", rows=250_000, bytes_=201_326_592, archive="cold research archive only after positive classification", prune="older than 31d when no unresolved forward outcome depends on row", certification=True),
    _c("v52_wallet_forward_outcomes", "wallet-forward-alpha", RetentionClass.WALLET_INTELLIGENCE_ARCHIVE, "Execution-realistic forward outcomes and matched-control alpha", "WalletForwardAlphaEngine", age="31d", rows=500_000, bytes_=268_435_456, archive="cold research archive only", prune="older than 31d", certification=True),
    _c("v52_wallet_integrity_snapshots", "wallet-forward-alpha", RetentionClass.WALLET_INTELLIGENCE_ARCHIVE, "Point-in-time wallet integrity used by scoring", "WalletForwardAlphaEngine", age="31d", rows=250_000, bytes_=134_217_728, archive="cold wallet archive only", prune="keep newest per wallet plus 31d window", certification=True),
    _c("v52_wallet_forward_validation", "wallet-forward-alpha", RetentionClass.STRATEGY_EVIDENCE, "Current and recent validation verdicts controlling allowable wallet influence", "WalletForwardAlphaEngine", age="31d", rows=256, bytes_=8_388_608, archive="research report bundle", prune="keep newest plus 31d", startup=True, certification=True),

    # Bounded compatibility adapters.  These names are deliberately explicit;
    # active mode must never discover arbitrary legacy tables and bless them.
    _c("events", "bounded-compat", RetentionClass.BOUNDED_WINDOW, "Post-transition append-only event tail", "paper-engine/current adapters", age="31d", rows=500_000, bytes_=268_435_456, prune="checkpointed tail older than 31d", startup=True, certification=True),
    _c("wallet_profiles", "bounded-compat", RetentionClass.WALLET_INTELLIGENCE_CURRENT, "Current wallet profile keyed by wallet", "wallet intelligence", rows=250_000, bytes_=134_217_728, startup=True, certification=True),
    _c("normalized_swaps", "bounded-compat", RetentionClass.BOUNDED_WINDOW, "Recent normalized swaps required by wallet and candidate evidence", "wallet intelligence/strategy", age="31d", rows=1_000_000, bytes_=402_653_184, prune="older than 31d after decision evidence is sealed", certification=True),
    _c("token_first_touches", "bounded-compat", RetentionClass.BOUNDED_WINDOW, "Recent first-touch identities used by active candidates", "strategy/wallet intelligence", age="31d", rows=250_000, bytes_=134_217_728, prune="terminal candidate older than 31d", certification=True),
    _c("risk_evidence", "bounded-compat", RetentionClass.BOUNDED_WINDOW, "Recent risk evidence used by unresolved/current decisions", "risk/strategy", age="31d", rows=1_000_000, bytes_=268_435_456, prune="older than 31d after decision sealing", certification=True),
    _c("entity_links", "bounded-compat", RetentionClass.WALLET_INTELLIGENCE_CURRENT, "Recent/current entity relationships", "wallet intelligence", age="31d", rows=500_000, bytes_=201_326_592, prune="superseded/older than 31d", certification=True),
    _c("risk_refresh_measurements", "bounded-compat", RetentionClass.DIAGNOSTIC_TTL, "Recent risk refresh latency evidence", "risk/operations", age="7d", rows=100_000, bytes_=67_108_864, prune="7d", certification=False),
    _c("price_marks", "bounded-compat", RetentionClass.BOUNDED_WINDOW, "Recent marks needed for current active positions/candidates", "paper-engine/risk", age="31d", rows=1_000_000, bytes_=268_435_456, prune="older than 31d except latest mark for active subject", certification=True),
    _c("program_coverage_observations", "bounded-compat", RetentionClass.BOUNDED_WINDOW, "Recent program coverage for active/recent candidates", "discovery", age="31d", rows=250_000, bytes_=134_217_728, prune="terminal older than 31d", certification=True),
    _c("paper_engine_checkpoint", "bounded-compat", RetentionClass.CURRENT_STATE, "Exact existing paper-engine continuation state", "DurablePaperTradingEngine", rows=1, bytes_=33_554_432, startup=True, certification=True),
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
        if contract.max_hot_rows is not None and contract.max_hot_rows <= 0:
            raise ValueError(f"dataset {contract.dataset} has invalid row budget")


validate_registry()
