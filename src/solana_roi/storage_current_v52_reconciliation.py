from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, MutableMapping, Sequence

from . import storage_retention as base

R = base.RetentionClass
C = base.RetentionContract


def _c(
    name: str,
    owner: str,
    cls: R,
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
        name,
        owner,
        cls,
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


# These contracts reconcile persistence added to current v5.2 main after the
# original storage-transition branch was cut.  They are intentionally explicit:
# no table is admitted merely because it exists in the legacy database.
CURRENT_V52_CONTRACTS = (
    # These are live decision dependencies, not rebuildable diagnostics. Keep
    # their exact rows in the semantic seal; exceeding the extraction bound
    # blocks rollover rather than silently forgetting invalid releases or
    # immutable challenger/candidate evidence. No age-based deletion is
    # authorized here.
    _c(
        "candidate_execution_plane_snapshots",
        "candidate-execution-evidence",
        R.STRATEGY_EVIDENCE,
        "Point-in-time candidate decision, risk readiness, timing and failure attribution",
        "candidate execution evidence/certification diagnostics",
        rows=100_000,
        bytes_=134_217_728,
        prune="retain until candidate attribution dependency and reconstruction proof permits removal",
        startup=True,
        certification=True,
    ),
    _c(
        "v51_release_compatibility",
        "measurement-integrity",
        R.CURRENT_STATE,
        "Release measurement and execution compatibility, including known invalid epochs",
        "measurement compatibility filters/exit execution/promotion proof",
        rows=4096,
        bytes_=16_777_216,
        prune="retain until release-reader dependency and reconstruction proof permits removal",
        startup=True,
        certification=True,
    ),
    _c(
        "v52_tournament_exact_evidence",
        "v5.2-learning-governance",
        R.STRATEGY_EVIDENCE,
        "Immutable same-stream challenger evidence identities and conflict detection",
        "record_exact_challenger_outcome/governed tournament",
        rows=100_000,
        bytes_=67_108_864,
        prune="retain until tournament dependency and reconstruction proof permits removal",
        startup=True,
        certification=True,
    ),
    _c(
        "v52_wallet_forward_runtime_state",
        "wallet-forward-alpha",
        R.CURRENT_STATE,
        "Prospective wallet-forward validation epoch and last worker progress",
        "WalletForwardAlphaRuntime/bootstrap",
        rows=1,
        bytes_=1_048_576,
        startup=True,
        certification=True,
    ),
    _c(
        "v52_wallet_forward_integrity_seen",
        "wallet-forward-alpha",
        R.BOUNDED_WINDOW,
        "Recent signature de-duplication for wallet integrity capture",
        "WalletForwardAlphaRuntime",
        age="31d",
        rows=500_000,
        bytes_=67_108_864,
        prune="older than 31d after prospective window",
        certification=True,
    ),
    _c(
        "v52_wallet_forward_shadow_decisions",
        "wallet-forward-alpha",
        R.STRATEGY_EVIDENCE,
        "Point-in-time current/no-wallet/forward-wallet shadow decisions",
        "WalletForwardAlphaRuntime validation",
        age="31d",
        rows=500_000,
        bytes_=201_326_592,
        prune="resolved decision older than 31d; unresolved always retained",
        certification=True,
    ),
    _c(
        "v52_wallet_forward_shadow_outcomes",
        "wallet-forward-alpha",
        R.STRATEGY_EVIDENCE,
        "Execution-realistic outcomes for wallet-forward shadow decisions",
        "WalletForwardAlphaRuntime validation",
        age="31d",
        rows=500_000,
        bytes_=201_326_592,
        prune="resolved older than 31d",
        certification=True,
    ),
    _c(
        "v52_wallet_forward_replay_runs",
        "wallet-forward-alpha",
        R.STRATEGY_EVIDENCE,
        "Bounded 24h/7d/30d replay verdict history",
        "WalletForwardAlphaRuntime validation/status",
        age="31d",
        rows=10_000,
        bytes_=33_554_432,
        prune="keep newest plus 31d",
        startup=True,
        certification=True,
    ),
    _c(
        "v52_market_validation_lane_events",
        "v5.2-market-validation",
        R.STRATEGY_EVIDENCE,
        "Lane opportunity/decision/outcome rows for current validation windows",
        "MarketValidationCompletion",
        age="31d",
        rows=500_000,
        bytes_=201_326_592,
        prune="resolved older than 31d; unresolved always retained",
        certification=True,
    ),
    _c(
        "v52_market_validation_shadow_variants",
        "v5.2-market-validation",
        R.STRATEGY_EVIDENCE,
        "A-G point-in-time counterfactual strategy variants",
        "MarketValidationCompletion",
        age="31d",
        rows=2_000_000,
        bytes_=268_435_456,
        prune="resolved older than 31d; unresolved always retained",
        certification=True,
    ),
    _c(
        "v52_market_validation_continuation_horizons",
        "v5.2-market-validation",
        R.STRATEGY_EVIDENCE,
        "Post-decision continuation persistence across fixed horizons",
        "MarketValidationCompletion",
        age="31d",
        rows=2_000_000,
        bytes_=268_435_456,
        prune="resolved older than 31d; unresolved always retained",
        certification=True,
    ),
    _c(
        "v52_market_validation_component_ablation",
        "v5.2-market-validation",
        R.STRATEGY_EVIDENCE,
        "Sequential component incremental-return evidence",
        "MarketValidationCompletion",
        age="31d",
        rows=1_000_000,
        bytes_=134_217_728,
        prune="resolved older than 31d; unresolved always retained",
        certification=True,
    ),
    _c(
        "v52_market_validation_completion_evaluations",
        "v5.2-market-validation",
        R.STRATEGY_EVIDENCE,
        "Hardened point-in-time completion evaluation cache",
        "MarketValidationCompletionHardening",
        age="31d",
        rows=500_000,
        bytes_=201_326_592,
        prune="older than 31d after decision window",
        certification=True,
    ),
    _c(
        "v52_market_validation_shadow_entries",
        "v5.2-market-validation",
        R.STRATEGY_EVIDENCE,
        "Executable shadow entry anchors for A-G variants",
        "MarketValidationCompletionHardening",
        age="31d",
        rows=1_000_000,
        bytes_=134_217_728,
        prune="older than 31d unless referenced by unresolved variant",
        certification=True,
    ),
    _c(
        "v52_market_validation_hardening_audit",
        "v5.2-market-validation",
        R.DIAGNOSTIC_TTL,
        "Recent hardening diagnostics; not investment authority",
        "operations/diagnostics",
        age="7d",
        rows=250_000,
        bytes_=67_108_864,
        prune="7d TTL",
        certification=False,
    ),
)

for contract in CURRENT_V52_CONTRACTS:
    existing = base.RETENTION_REGISTRY.get(contract.dataset)
    if existing is not None and existing != contract:
        raise RuntimeError(f"conflicting current-v5.2 retention contract:{contract.dataset}")
    base.RETENTION_REGISTRY[contract.dataset] = contract


def _cutoff(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


def _row_dict(row: sqlite3.Row) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key in row.keys():
        value = row[key]
        result[str(key)] = {"hex": value.hex()} if isinstance(value, bytes) else value
    return result


def augment_current_state_truth(
    connection: sqlite3.Connection,
    tables: set[str],
    truth: MutableMapping[str, Any],
    counts: MutableMapping[str, int],
) -> None:
    """Add current-main resume-critical state to the transition truth.

    The wallet-forward runtime start time is the clock that makes the 24h/7d/30d
    prospective validation genuine across deploys. Losing or resetting it would
    change strategy eligibility semantics, so migration treats it as exact state.
    """
    table = "v52_wallet_forward_runtime_state"
    if table not in tables:
        return
    rows = connection.execute(f'SELECT * FROM "{table}" ORDER BY id LIMIT 2').fetchall()
    if len(rows) > 1:
        raise RuntimeError("current-state extraction blocked: wallet-forward runtime state is not singleton")
    payload = [_row_dict(row) for row in rows]
    strategy = truth.setdefault("strategy", {})
    if not isinstance(strategy, MutableMapping):
        raise RuntimeError("current-state extraction blocked: strategy section is not mutable mapping")
    strategy[table] = payload
    counts[table] = len(payload)


def copy_bounded_current_v52(
    source: sqlite3.Connection,
    dest: sqlite3.Connection,
    *,
    copy_query: Any,
    counts: MutableMapping[str, int],
) -> None:
    """Copy only current/recent or unresolved evidence added by current v5.2."""
    existing = {
        str(row[0])
        for row in source.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    }
    cutoff31 = _cutoff(31)
    cutoff7 = _cutoff(7)
    specs: Sequence[tuple[str, str, tuple[Any, ...]]] = (
        (
            "v52_wallet_forward_integrity_seen",
            "SELECT * FROM v52_wallet_forward_integrity_seen WHERE recorded_at>=?",
            (cutoff31,),
        ),
        (
            "v52_wallet_forward_shadow_decisions",
            "SELECT d.* FROM v52_wallet_forward_shadow_decisions d "
            "WHERE d.observed_at>=? OR NOT EXISTS (SELECT 1 FROM v52_wallet_forward_shadow_outcomes o WHERE o.decision_id=d.id)",
            (cutoff31,),
        ),
        (
            "v52_wallet_forward_shadow_outcomes",
            "SELECT * FROM v52_wallet_forward_shadow_outcomes WHERE resolved_at>=?",
            (cutoff31,),
        ),
        (
            "v52_wallet_forward_replay_runs",
            "SELECT * FROM v52_wallet_forward_replay_runs WHERE evaluated_at>=? OR id=(SELECT MAX(id) FROM v52_wallet_forward_replay_runs)",
            (cutoff31,),
        ),
        (
            "v52_market_validation_lane_events",
            "SELECT * FROM v52_market_validation_lane_events WHERE observed_at>=? OR net_return IS NULL",
            (cutoff31,),
        ),
        (
            "v52_market_validation_shadow_variants",
            "SELECT * FROM v52_market_validation_shadow_variants WHERE observed_at>=? OR net_return IS NULL",
            (cutoff31,),
        ),
        (
            "v52_market_validation_continuation_horizons",
            "SELECT * FROM v52_market_validation_continuation_horizons WHERE observed_at>=? OR resolved_at IS NULL",
            (cutoff31,),
        ),
        (
            "v52_market_validation_component_ablation",
            "SELECT * FROM v52_market_validation_component_ablation WHERE observed_at>=? OR resolved_at IS NULL",
            (cutoff31,),
        ),
        (
            "v52_market_validation_completion_evaluations",
            "SELECT * FROM v52_market_validation_completion_evaluations WHERE observed_at>=?",
            (cutoff31,),
        ),
        (
            "v52_market_validation_shadow_entries",
            "SELECT e.* FROM v52_market_validation_shadow_entries e WHERE e.observed_at>=? OR EXISTS ("
            "SELECT 1 FROM v52_market_validation_shadow_variants v WHERE v.candidate_key=e.candidate_key "
            "AND v.observed_at=e.observed_at AND v.variant_id=e.variant_id AND v.net_return IS NULL)",
            (cutoff31,),
        ),
        (
            "v52_market_validation_hardening_audit",
            "SELECT * FROM v52_market_validation_hardening_audit WHERE observed_at>=?",
            (cutoff7,),
        ),
    )
    for table, sql, args in specs:
        if table not in existing:
            continue
        copied = int(copy_query(source, dest, table, sql, args))
        if copied:
            counts[table] = copied


def prune_current_v52_database(path: Path | str, *, now: datetime | None = None) -> dict[str, int]:
    """Bound current-main validation evidence without touching authority state.

    This function deliberately never deletes the wallet-forward runtime singleton,
    unresolved decisions, unresolved horizons, unresolved ablations, or unresolved
    shadow entries. It is therefore suitable for active-store maintenance but is
    not a legacy purge mechanism.
    """
    # Keep a single enforcement implementation. The historical copy in this
    # reconciliation module used a different parent/outcome deletion order and
    # could erase the proof needed to retire a resolved decision.
    from .storage_current_v52_pruning import prune_current_v52_database as canonical_prune

    return canonical_prune(path, now=now)


def registered_dataset_names() -> tuple[str, ...]:
    return tuple(sorted(contract.dataset for contract in CURRENT_V52_CONTRACTS))


__all__ = [
    "CURRENT_V52_CONTRACTS",
    "augment_current_state_truth",
    "copy_bounded_current_v52",
    "prune_current_v52_database",
    "registered_dataset_names",
]
