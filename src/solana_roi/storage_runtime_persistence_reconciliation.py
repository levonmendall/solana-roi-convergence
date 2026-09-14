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


# Persistence created by constructors reached from production build_runtime().
# Every table is admitted for a concrete continuation/certification reason; this
# is deliberately not a discovery list of whatever happens to exist in legacy.
RUNTIME_CONTRACTS = (
    _c("certification_release_epochs", "certification", R.CURRENT_STATE,
       "Exact prospective evidence start boundary by release commit", "certification gates",
       rows=256, bytes_=4_194_304, startup=True, certification=True),
    _c("forward_cohort_manifest", "strategy", R.CURRENT_STATE,
       "Frozen governed paper-cohort manifest and hash", "ForwardCohortController",
       rows=1, bytes_=4_194_304, startup=True, certification=True),
    _c("forward_cohort_arm_state", "strategy", R.CURRENT_STATE,
       "One-time paper-cohort arming state", "ForwardCohortController",
       rows=1, bytes_=1_048_576, startup=True, certification=True),
    _c("helius_webhook_inbox", "ingestion", R.TRANSPORT_JOURNAL,
       "Accepted webhook payloads until downstream ingestion completes", "HeliusWebhookWorker",
       age="until complete + 7d", rows=250_000, bytes_=268_435_456,
       prune="never drop pending; completed older than 7d", startup=True, certification=True),
    _c("direct_solana_recent_receipts", "direct-solana", R.TEMPORARY,
       "Short raw-receipt frontier used for bounded discovery/context", "DirectSolanaJournal/wallet discovery",
       age="expires_at", rows=500_000, bytes_=134_217_728,
       prune="expires_at passed", startup=True, certification=False),
    _c("direct_solana_minute_receipts", "direct-solana", R.BOUNDED_WINDOW,
       "Recent per-source receipt continuity aggregates", "DirectSolanaJournal status/certification",
       age="31d", rows=250_000, bytes_=67_108_864,
       prune="bucket older than 31d", startup=True, certification=True),
    _c("direct_solana_hydration_queue", "direct-solana", R.TRANSPORT_JOURNAL,
       "Durable receipt hydration work until terminal completion", "DirectSolanaJournal workers",
       age="until complete + 7d", rows=500_000, bytes_=201_326_592,
       prune="never drop non-complete; completed older than 7d", startup=True, certification=True),
    _c("direct_solana_hydration_metrics", "direct-solana", R.BOUNDED_WINDOW,
       "Recent live-vs-recovery hydration evidence", "source coverage/certification",
       age="31d", rows=500_000, bytes_=134_217_728,
       prune="older than 31d with matching normalized evidence outside hot window", certification=True),
    _c("direct_solana_provider_state", "direct-solana", R.CURRENT_STATE,
       "Provider connectivity/reconnect continuity materialization", "DirectSolanaJournal",
       rows=64, bytes_=1_048_576, startup=True, certification=True),
    _c("direct_solana_global_state", "direct-solana", R.CURRENT_STATE,
       "Unresolved outage/gap and last recovery boundary", "DirectSolanaJournal",
       rows=1, bytes_=1_048_576, startup=True, certification=True),
    _c("wallet_discovery_candidates", "wallet-intelligence", R.WALLET_INTELLIGENCE_CURRENT,
       "Current discovered/screened/tracked wallet frontier and forward epoch", "ContinuousWalletDiscovery",
       rows=250_000, bytes_=134_217_728, startup=True, certification=True),
    _c("wallet_discovery_broad_samples", "wallet-intelligence", R.RESEARCH_ARCHIVE,
       "Raw broad-scan samples still needed to preserve screening counters", "ContinuousWalletDiscovery",
       age="31d plus unresolved screening wallets", rows=1_000_000, bytes_=268_435_456,
       prune="older rows only after wallet no longer needs broad-screen reconstruction", startup=True),
    _c("wallet_discovery_forward_observations", "wallet-intelligence", R.WALLET_INTELLIGENCE_ARCHIVE,
       "Prospective copyable observations feeding wallet-forward scoring", "ContinuousWalletDiscovery/WalletForwardAlphaRuntime",
       age="31d", rows=1_000_000, bytes_=402_653_184,
       prune="older than 31d after dependent validation/outcomes are sealed", startup=True, certification=True),
    _c("wallet_discovery_state", "wallet-intelligence", R.CURRENT_STATE,
       "Raw-receipt discovery high-water and cycle state", "ContinuousWalletDiscovery",
       rows=1, bytes_=1_048_576, startup=True, certification=True),
    _c("wallet_realtime_state", "wallet-intelligence", R.CURRENT_STATE,
       "Per-wallet real-time epoch, signature/slot frontier and reset count", "RealtimeWalletTracker",
       rows=250_000, bytes_=67_108_864, startup=True, certification=True),
    _c("wallet_realtime_receipts", "wallet-intelligence", R.TRANSPORT_JOURNAL,
       "Durable real-time wallet receipt hydration queue", "RealtimeWalletTracker",
       age="until complete + 7d", rows=500_000, bytes_=201_326_592,
       prune="never drop non-complete; completed older than 7d", startup=True, certification=True),
    _c("wallet_realtime_runtime", "wallet-intelligence", R.CURRENT_STATE,
       "Realtime wallet worker cycle/provider/recovery state", "RealtimeWalletTracker",
       rows=1, bytes_=1_048_576, startup=True, certification=True),
    _c("execution_quote_observations", "execution-observation", R.BOUNDED_WINDOW,
       "Recent amount-specific quote evidence for prospective certification", "QuoteCertificationGate",
       age="31d plus latest 500", rows=250_000, bytes_=134_217_728,
       prune="older than 31d except latest 500", certification=True),
    _c("shadow_execution_observations", "execution-observation", R.BOUNDED_WINDOW,
       "Recent unsigned simulation evidence for prospective execution certification", "ProspectiveShadowExecutionCertificationGate",
       age="31d plus latest 500", rows=250_000, bytes_=134_217_728,
       prune="older than 31d except latest 500", certification=True),
    _c("semantic_candidate_events", "candidate-attribution", R.BOUNDED_WINDOW,
       "Recent venue-native semantic scout attribution facts; never entry authority", "Semantic candidate attribution/status",
       age="31d", rows=500_000, bytes_=201_326_592,
       prune="received_at older than 31d", startup=True, certification=True),
    _c("semantic_candidate_opportunities", "candidate-attribution", R.BOUNDED_WINDOW,
       "Recent venue-native watch-state materialization; entry_authority is always zero", "Semantic candidate attribution/status",
       age="31d", rows=250_000, bytes_=134_217_728,
       prune="last_seen older than 31d", startup=True, certification=True),
    _c("semantic_candidate_risk_state", "candidate-attribution", R.BOUNDED_WINDOW,
       "Recent risk-readthrough materialization; entry_authority is always zero", "Semantic candidate attribution/status",
       age="31d", rows=250_000, bytes_=134_217_728,
       prune="assessed_at older than 31d", startup=True, certification=True),
)

for contract in RUNTIME_CONTRACTS:
    existing = base.RETENTION_REGISTRY.get(contract.dataset)
    if existing is not None and existing != contract:
        raise RuntimeError(f"conflicting runtime-persistence retention contract:{contract.dataset}")
    base.RETENTION_REGISTRY[contract.dataset] = contract


def _row_dict(row: sqlite3.Row) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key in row.keys():
        value = row[key]
        result[str(key)] = {"hex": value.hex()} if isinstance(value, bytes) else value
    return result


def _bounded_query(
    connection: sqlite3.Connection,
    sql: str,
    args: Sequence[Any],
    *,
    limit: int,
    label: str,
) -> list[dict[str, Any]]:
    rows = connection.execute(sql + " LIMIT ?", (*tuple(args), int(limit) + 1)).fetchall()
    if len(rows) > limit:
        raise RuntimeError(f"runtime current-state extraction blocked: {label} exceeds {limit} rows")
    return [_row_dict(row) for row in rows]


def _table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row[1])
        for row in connection.execute(f'PRAGMA table_info("{table}")').fetchall()
    }


def augment_runtime_current_state_truth(
    connection: sqlite3.Connection,
    tables: set[str],
    truth: MutableMapping[str, Any],
    counts: MutableMapping[str, int],
) -> None:
    """Seal authority, continuity and unresolved durable transport into truth."""

    def put(section: str, table: str, sql: str, args: Sequence[Any] = (), limit: int = 4096) -> None:
        if table not in tables:
            return
        payload = truth.setdefault(section, {})
        if not isinstance(payload, MutableMapping):
            raise RuntimeError(f"runtime-state extraction blocked: section not mapping:{section}")
        rows = _bounded_query(connection, sql, args, limit=limit, label=table)
        payload[table] = rows
        counts[table] = len(rows)

    put("strategy", "forward_cohort_manifest", "SELECT * FROM forward_cohort_manifest ORDER BY id", limit=1)
    put("strategy", "forward_cohort_arm_state", "SELECT * FROM forward_cohort_arm_state ORDER BY id", limit=1)
    put("certification", "certification_release_epochs", "SELECT * FROM certification_release_epochs ORDER BY started_at", limit=256)

    put("provider_source", "direct_solana_provider_state", "SELECT * FROM direct_solana_provider_state ORDER BY provider", limit=64)
    put("continuity", "direct_solana_global_state", "SELECT * FROM direct_solana_global_state ORDER BY id", limit=1)
    put("continuity", "wallet_discovery_state", "SELECT * FROM wallet_discovery_state ORDER BY id", limit=1)
    put("wallet", "wallet_discovery_candidates", "SELECT * FROM wallet_discovery_candidates ORDER BY wallet", limit=250_000)
    put("wallet", "wallet_realtime_state", "SELECT * FROM wallet_realtime_state ORDER BY wallet", limit=250_000)
    put("continuity", "wallet_realtime_runtime", "SELECT * FROM wallet_realtime_runtime ORDER BY id", limit=1)

    # Accepted but not fully completed transport is current state. Completed
    # historical payloads are copied separately only as a small bounded window.
    put(
        "continuity",
        "helius_webhook_inbox",
        "SELECT * FROM helius_webhook_inbox WHERE state<>'complete' ORDER BY id",
        limit=250_000,
    )
    put(
        "continuity",
        "direct_solana_hydration_queue",
        "SELECT * FROM direct_solana_hydration_queue WHERE status<>'complete' ORDER BY updated_at,signature",
        limit=500_000,
    )
    put(
        "continuity",
        "wallet_realtime_receipts",
        "SELECT * FROM wallet_realtime_receipts WHERE status<>'complete' ORDER BY id",
        limit=500_000,
    )


def copy_bounded_runtime_evidence(
    source: sqlite3.Connection,
    destination: sqlite3.Connection,
    *,
    copy_query: Any,
    counts: MutableMapping[str, int],
    now: datetime | None = None,
) -> None:
    """Copy recent/relevant runtime evidence, never arbitrary legacy history."""
    instant = now or datetime.now(timezone.utc)
    cutoff31 = (instant - timedelta(days=31)).isoformat()
    cutoff7 = (instant - timedelta(days=7)).isoformat()
    existing = {
        str(row[0])
        for row in source.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    }

    # Helius changed its terminal timestamp from completed_at to updated_at.
    # A production shadow may be built from either supported schema generation,
    # so select the bounded rows using the columns the source actually exposes.
    if "helius_webhook_inbox" in existing:
        webhook_columns = _table_columns(source, "helius_webhook_inbox")
        if "updated_at" in webhook_columns:
            webhook_sql = "SELECT * FROM helius_webhook_inbox WHERE state<>'complete' OR updated_at>=?"
        elif "completed_at" in webhook_columns:
            webhook_sql = "SELECT * FROM helius_webhook_inbox WHERE state<>'complete' OR completed_at>=?"
        else:
            raise RuntimeError(
                "migration blocked: helius_webhook_inbox lacks supported terminal timestamp"
            )
        copied = int(copy_query(source, destination, "helius_webhook_inbox", webhook_sql, (cutoff7,)))
        if copied:
            counts["helius_webhook_inbox"] = max(counts.get("helius_webhook_inbox", 0), copied)

    specs: Sequence[tuple[str, str, tuple[Any, ...]]] = (
        ("direct_solana_recent_receipts", "SELECT * FROM direct_solana_recent_receipts WHERE expires_at>=?", (instant.isoformat(),)),
        ("direct_solana_minute_receipts", "SELECT * FROM direct_solana_minute_receipts WHERE bucket>=?", (cutoff31,)),
        ("direct_solana_hydration_queue", "SELECT * FROM direct_solana_hydration_queue WHERE status<>'complete' OR updated_at>=?", (cutoff7,)),
        ("direct_solana_hydration_metrics", "SELECT * FROM direct_solana_hydration_metrics WHERE hydrated_at>=?", (cutoff31,)),
        (
            "wallet_discovery_broad_samples",
            "SELECT b.* FROM wallet_discovery_broad_samples b WHERE b.received_at>=? OR EXISTS ("
            "SELECT 1 FROM wallet_discovery_candidates c WHERE c.wallet=b.wallet AND c.state IN ('discovered','screen_rejected'))",
            (cutoff31,),
        ),
        ("wallet_discovery_forward_observations", "SELECT * FROM wallet_discovery_forward_observations WHERE received_at>=?", (cutoff31,)),
        ("wallet_realtime_receipts", "SELECT * FROM wallet_realtime_receipts WHERE status<>'complete' OR updated_at>=?", (cutoff7,)),
        ("execution_quote_observations", "SELECT * FROM execution_quote_observations WHERE received_at>=? OR id IN (SELECT id FROM execution_quote_observations ORDER BY id DESC LIMIT 500)", (cutoff31,)),
        ("shadow_execution_observations", "SELECT * FROM shadow_execution_observations WHERE completed_at>=? OR id IN (SELECT id FROM shadow_execution_observations ORDER BY id DESC LIMIT 500)", (cutoff31,)),
        ("semantic_candidate_events", "SELECT * FROM semantic_candidate_events WHERE received_at>=?", (cutoff31,)),
        ("semantic_candidate_opportunities", "SELECT * FROM semantic_candidate_opportunities WHERE last_seen>=?", (cutoff31,)),
        ("semantic_candidate_risk_state", "SELECT * FROM semantic_candidate_risk_state WHERE assessed_at>=?", (cutoff31,)),
        # Preserve the 31-day hot normalized-swap window plus every older row that
        # is still necessary to prove a token-first-touch chronology conflict.
        (
            "normalized_swaps",
            "SELECT s.* FROM normalized_swaps s WHERE s.received_at>=? OR EXISTS ("
            "SELECT 1 FROM token_first_touches t JOIN wallet_profiles w ON w.wallet=s.wallet "
            "WHERE t.token_mint=s.token_mint AND s.side='buy' AND w.historically_eligible=1 "
            "AND w.tier IN ('S','A') AND julianday(s.observed_at)<julianday(t.observed_at))",
            (cutoff31,),
        ),
        # Program coverage and risk-refresh gates consume the latest 500 rows, so
        # retain that exact decision surface even if activity is older than the
        # nominal hot window.
        ("program_coverage_observations", "SELECT * FROM program_coverage_observations WHERE assessed_at>=? OR id IN (SELECT id FROM program_coverage_observations ORDER BY assessed_at DESC,id DESC LIMIT 500)", (cutoff31,)),
        ("risk_refresh_measurements", "SELECT * FROM risk_refresh_measurements WHERE completed_at>=? OR id IN (SELECT id FROM risk_refresh_measurements ORDER BY id DESC LIMIT 500)", (cutoff7,)),
        ("risk_evidence", "SELECT * FROM risk_evidence WHERE received_at>=? OR id IN (SELECT MAX(id) FROM risk_evidence GROUP BY token_mint,dimension)", (cutoff31,)),
    )
    for table, sql, args in specs:
        if table not in existing:
            continue
        copied = int(copy_query(source, destination, table, sql, args))
        if copied:
            counts[table] = max(counts.get(table, 0), copied)


def advance_registered_sequences(source: sqlite3.Connection, destination: sqlite3.Connection) -> dict[str, int]:
    """Carry forward AUTOINCREMENT high-water marks without copying old rows."""
    source_has_sequence = source.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'"
    ).fetchone()
    destination_has_sequence = destination.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'"
    ).fetchone()
    if source_has_sequence is None or destination_has_sequence is None:
        return {}
    destination_tables = {
        str(row[0])
        for row in destination.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    }
    rows = source.execute("SELECT name,seq FROM sqlite_sequence").fetchall()
    advanced: dict[str, int] = {}
    for name_raw, seq_raw in rows:
        name = str(name_raw)
        if name not in destination_tables or name not in base.RETENTION_REGISTRY:
            continue
        seq = int(seq_raw or 0)
        current = destination.execute("SELECT seq FROM sqlite_sequence WHERE name=?", (name,)).fetchone()
        if current is None:
            destination.execute("INSERT INTO sqlite_sequence(name,seq) VALUES(?,?)", (name, seq))
        elif int(current[0] or 0) < seq:
            destination.execute("UPDATE sqlite_sequence SET seq=? WHERE name=?", (seq, name))
        advanced[name] = seq
    return advanced


def registered_dataset_names() -> tuple[str, ...]:
    return tuple(sorted(contract.dataset for contract in RUNTIME_CONTRACTS))


__all__ = [
    "RUNTIME_CONTRACTS",
    "augment_runtime_current_state_truth",
    "copy_bounded_runtime_evidence",
    "advance_registered_sequences",
    "registered_dataset_names",
]
