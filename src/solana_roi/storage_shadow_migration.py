from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from . import storage_manifest
from .active_storage import ActiveStorage, decode_current_payload, payload_hash
from .observation_store import ObservationEventStore
from .storage_current_state_extractor import LegacyCurrentStateExtractor
from .storage_current_v52_reconciliation import copy_bounded_current_v52
from .storage_runtime_persistence_reconciliation import (
    advance_registered_sequences,
    copy_bounded_runtime_evidence,
)
from .storage_transition import build_checkpoint_payload, persist_verified_checkpoint, verify_semantic_equivalence

COPY_BATCH_ROWS = 2_048


@dataclass(frozen=True)
class ShadowMigrationReport:
    release_sha: str
    source_path: str
    source_size_bytes: int
    active_path: str
    active_size_bytes: int
    active_wal_bytes: int
    source_schema_fingerprint: str
    semantic_hash: str
    equivalent: bool
    mismatched_sections: tuple[str, ...]
    copied_rows: dict[str, int]


def _connect_ro(path: Path) -> sqlite3.Connection:
    uri = f"file:{path.resolve()}?mode=ro&cache=private"
    conn = sqlite3.connect(uri, uri=True, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def _table_ddl(conn: sqlite3.Connection, table: str) -> str:
    row = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    if row is None or not str(row[0] or "").strip():
        raise RuntimeError(f"migration blocked: table DDL unavailable:{table}")
    return str(row[0])


def _columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")')]


def _copy_dict_rows(source: sqlite3.Connection, dest: sqlite3.Connection, table: str, rows: Sequence[Mapping[str, Any]]) -> int:
    if not rows:
        return 0
    storage_manifest.contract_for(table)
    source_columns = _columns(source, table)
    if _exists(dest, table):
        destination_columns = _columns(dest, table)
        if destination_columns != source_columns:
            raise RuntimeError(
                f"migration blocked: compatibility schema mismatch:{table}:"
                f"source={source_columns}:destination={destination_columns}"
            )
    else:
        dest.execute(_table_ddl(source, table))
    columns = list(rows[0].keys())
    if any(list(row.keys()) != columns for row in rows):
        raise RuntimeError(f"migration blocked: inconsistent row shape:{table}")
    real = set(source_columns)
    if set(columns) - real:
        raise RuntimeError(f"migration blocked: synthetic columns selected for {table}:{sorted(set(columns)-real)}")
    qcols = ",".join('"' + c.replace('"', '""') + '"' for c in columns)
    placeholders = ",".join("?" for _ in columns)
    def values():
        for row in rows:
            item: list[Any] = []
            for column in columns:
                value = row.get(column)
                if isinstance(value, dict) and set(value) == {"hex"}:
                    value = bytes.fromhex(str(value["hex"]))
                item.append(value)
            yield tuple(item)

    dest.executemany(f'INSERT OR REPLACE INTO "{table}"({qcols}) VALUES({placeholders})', values())
    return len(rows)


def _query_dicts(conn: sqlite3.Connection, sql: str, args: Sequence[Any] = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(sql, tuple(args)).fetchall()]


def _copy_query(source: sqlite3.Connection, dest: sqlite3.Connection, table: str, sql: str, args: Sequence[Any] = ()) -> int:
    cursor = source.execute(sql, tuple(args))
    copied = 0
    try:
        while True:
            batch = cursor.fetchmany(COPY_BATCH_ROWS)
            if not batch:
                return copied
            copied += _copy_dict_rows(source, dest, table, [dict(row) for row in batch])
            del batch
    finally:
        cursor.close()


def _cutoff(days: int = 31) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


def _portfolio_subjects(truth: Mapping[str, Any]) -> tuple[set[str], set[str]]:
    state = dict(dict(truth["portfolio"])["state"])
    mints = {str(x) for x in dict(state.get("candidates") or {})}
    mints.update(str(x) for x in dict(state.get("positions") or {}))
    mints.update(str(x) for x in dict(state.get("marks") or {}))
    wallets: set[str] = set()
    for payload in dict(state.get("candidates") or {}).values():
        if isinstance(payload, Mapping):
            for key in ("scout_wallet", "confirmation_wallet"):
                if payload.get(key):
                    wallets.add(str(payload[key]))
    for payload in dict(state.get("positions") or {}).values():
        if isinstance(payload, Mapping) and payload.get("scout_wallet"):
            wallets.add(str(payload["scout_wallet"]))
    return mints, wallets


def _copy_current_sections(source: sqlite3.Connection, dest: sqlite3.Connection, truth: Mapping[str, Any], counts: dict[str, int]) -> None:
    for section in ("strategy", "wallet", "provider_source", "active_candidates", "active_lifecycles", "continuity", "certification", "replication_watermarks"):
        payload = truth.get(section) or {}
        if not isinstance(payload, Mapping):
            continue
        for table, rows in payload.items():
            if isinstance(rows, list) and rows:
                counts[str(table)] = counts.get(str(table), 0) + _copy_dict_rows(source, dest, str(table), rows)
    if _exists(source, "paper_engine_checkpoint"):
        counts["paper_engine_checkpoint"] = _copy_query(source, dest, "paper_engine_checkpoint", "SELECT * FROM paper_engine_checkpoint WHERE id=1")


def _ensure_paper_checkpoint(
    source: sqlite3.Connection,
    dest: sqlite3.Connection,
    truth: Mapping[str, Any],
    counts: dict[str, int],
) -> None:
    """Materialize only a genesis checkpoint already proven valid by extraction.

    The legacy durable engine legitimately has no checkpoint row before its
    first engine event. Active storage needs a physical checkpoint so that its
    sealed transition anchor remains restartable after legacy is unavailable.
    No non-genesis state is ever synthesized here.
    """
    table = "paper_engine_checkpoint"
    if not _exists(source, table):
        raise RuntimeError("migration blocked: paper_engine_checkpoint missing")
    if not _exists(dest, table):
        dest.execute(_table_ddl(source, table))
    existing = dest.execute("SELECT id FROM paper_engine_checkpoint WHERE id=1").fetchone()
    if existing is not None:
        counts[table] = max(1, int(counts.get(table, 0)))
        return
    portfolio = truth.get("portfolio")
    if not isinstance(portfolio, Mapping):
        raise RuntimeError("migration blocked: portfolio truth missing")
    if not bool(portfolio.get("genesis_materialized")) or bool(portfolio.get("source_checkpoint_present", True)):
        raise RuntimeError("migration blocked: source paper checkpoint row unavailable outside proven genesis")
    if int(portfolio.get("last_engine_event_id", -1)) != 0:
        raise RuntimeError("migration blocked: synthesized genesis has nonzero engine event id")
    state = portfolio.get("state")
    if not isinstance(state, Mapping):
        raise RuntimeError("migration blocked: synthesized genesis state missing")
    raw = json.dumps(dict(state), sort_keys=True, separators=(",", ":"), allow_nan=False)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    if digest != str(portfolio.get("state_sha256") or ""):
        raise RuntimeError("migration blocked: synthesized genesis digest mismatch")
    dest.execute(
        "INSERT INTO paper_engine_checkpoint(id,saved_at,last_engine_event_id,state_json,state_sha256) VALUES(1,?,?,?,?)",
        (datetime.now(timezone.utc).isoformat(), 0, raw, digest),
    )
    counts[table] = 1


def _copy_latest_250_features(source: sqlite3.Connection, dest: sqlite3.Connection) -> int:
    table = "v52_market_validation_features"
    if not _exists(source, table):
        return 0
    columns = _columns(source, table)
    selected = ",".join('ranked."' + c.replace('"', '""') + '"' for c in columns)
    return _copy_query(
        source, dest, table,
        "SELECT " + selected + " FROM (SELECT f.*,ROW_NUMBER() OVER(PARTITION BY lane,feature ORDER BY id DESC) AS _roi_rank FROM v52_market_validation_features AS f) AS ranked WHERE ranked._roi_rank<=250 ORDER BY ranked.id",
    )


def _copy_bounded_v52(source: sqlite3.Connection, dest: sqlite3.Connection, counts: dict[str, int]) -> None:
    cutoff = _cutoff(31)
    count = _copy_latest_250_features(source, dest)
    if count:
        counts["v52_market_validation_features"] = count
    specs = (
        ("v52_market_validation_shadow_outcomes", "SELECT * FROM v52_market_validation_shadow_outcomes WHERE resolved_at IS NULL OR resolved_at>=?", (cutoff,)),
        ("v52_market_validation_point_in_time", "SELECT * FROM v52_market_validation_point_in_time WHERE observed_at>=? OR future_outcome_json IS NULL", (cutoff,)),
        ("v52_wallet_point_in_time_observations", "SELECT * FROM v52_wallet_point_in_time_observations WHERE detected_at>=?", (cutoff,)),
        ("v52_wallet_forward_outcomes", "SELECT * FROM v52_wallet_forward_outcomes WHERE available_at>=?", (cutoff,)),
        ("v52_wallet_integrity_snapshots", "SELECT * FROM v52_wallet_integrity_snapshots WHERE observed_at>=? OR id IN (SELECT MAX(id) FROM v52_wallet_integrity_snapshots GROUP BY wallet)", (cutoff,)),
        ("v52_wallet_forward_validation", "SELECT * FROM v52_wallet_forward_validation WHERE evaluated_at>=? OR id=(SELECT MAX(id) FROM v52_wallet_forward_validation)", (cutoff,)),
    )
    for table, sql, args in specs:
        if _exists(source, table):
            counts[table] = _copy_query(source, dest, table, sql, args)


def _copy_wallet_materializations(source: sqlite3.Connection, dest: sqlite3.Connection, counts: dict[str, int]) -> None:
    cutoff = _cutoff(31)
    if _exists(source, "wallet_intelligence_snapshots"):
        counts["wallet_intelligence_snapshots"] = _copy_query(
            source, dest, "wallet_intelligence_snapshots",
            "SELECT * FROM wallet_intelligence_snapshots WHERE observed_at>=? OR id IN (SELECT MAX(id) FROM wallet_intelligence_snapshots GROUP BY wallet)", (cutoff,),
        )
    if _exists(source, "adaptive_wallet_cohorts"):
        counts["adaptive_wallet_cohorts"] = _copy_query(source, dest, "adaptive_wallet_cohorts", "SELECT * FROM adaptive_wallet_cohorts")


def _in_clause(values: Iterable[str]) -> tuple[str, list[str]]:
    items = sorted({str(v) for v in values if str(v)})
    return ",".join("?" for _ in items), items


def _copy_active_subject_evidence(source: sqlite3.Connection, dest: sqlite3.Connection, truth: Mapping[str, Any], counts: dict[str, int]) -> None:
    mints, wallets = _portfolio_subjects(truth)
    if _exists(source, "wallet_profiles"):
        for row in source.execute("SELECT wallet FROM wallet_profiles WHERE historically_eligible=1 AND tier IN ('S','A')"):
            wallets.add(str(row[0]))
    cutoff = _cutoff(31)
    mint_sql, mint_args = _in_clause(mints)
    wallet_sql, wallet_args = _in_clause(wallets)
    if mint_args and _exists(source, "token_first_touches"):
        counts["token_first_touches"] = _copy_query(source, dest, "token_first_touches", f"SELECT * FROM token_first_touches WHERE token_mint IN ({mint_sql})", mint_args)
    if mint_args and _exists(source, "risk_evidence"):
        counts["risk_evidence"] = _copy_query(source, dest, "risk_evidence", f"SELECT * FROM risk_evidence WHERE token_mint IN ({mint_sql})", mint_args)
    if mint_args and _exists(source, "program_coverage_observations"):
        counts["program_coverage_observations"] = _copy_query(source, dest, "program_coverage_observations", f"SELECT * FROM program_coverage_observations WHERE token_mint IN ({mint_sql})", mint_args)
    if mint_args and _exists(source, "price_marks"):
        counts["price_marks"] = _copy_query(source, dest, "price_marks", f"SELECT * FROM price_marks WHERE token_mint IN ({mint_sql}) AND received_at>=?", [*mint_args, cutoff])
    if mint_args and _exists(source, "normalized_swaps"):
        counts["normalized_swaps"] = _copy_query(source, dest, "normalized_swaps", f"SELECT * FROM normalized_swaps WHERE token_mint IN ({mint_sql}) AND observed_at>=?", [*mint_args, cutoff])
    if wallet_args and _exists(source, "entity_links"):
        counts["entity_links"] = _copy_query(source, dest, "entity_links", f"SELECT * FROM entity_links WHERE received_at>=? AND (wallet_a IN ({wallet_sql}) OR wallet_b IN ({wallet_sql}))", [cutoff, *wallet_args, *wallet_args])


def _write_logical_truth(storage: ActiveStorage, truth: Mapping[str, Any]) -> None:
    storage.replace_current("strategy_current", "state_key", "transition", truth["strategy"])
    storage.replace_current("wallet_current", "wallet_id", "__transition_state__", truth["wallet"])
    storage.replace_current("wallet_current", "wallet_id", "__transition_watermarks__", truth["wallet_evidence_watermarks"])
    storage.replace_current("provider_current", "provider_id", "__transition__", truth["provider_source"])
    storage.replace_current("portfolio_current", "state_key", "transition", truth["portfolio"], last_engine_event_id=int(truth["portfolio"]["last_engine_event_id"]))
    storage.replace_current("active_candidates", "candidate_id", "__transition__", truth["active_candidates"])
    storage.replace_current("active_lifecycles", "lifecycle_id", "__transition__", truth["active_lifecycles"])
    storage.replace_current("continuity_current", "state_key", "transition", truth["continuity"])
    storage.replace_current("certification_current", "state_key", "transition", truth["certification"])
    storage.replace_current("system_current", "state_key", "latest_event_ids", truth["latest_event_ids"])
    storage.replace_current("system_current", "state_key", "freshness", truth["freshness"])
    storage.replace_current("system_current", "state_key", "replication_watermarks", truth["replication_watermarks"])


def _payload(conn: sqlite3.Connection, table: str, keycol: str, key: str) -> Any:
    row = conn.execute(f'SELECT payload_json FROM "{table}" WHERE "{keycol}"=?', (key,)).fetchone()
    if row is None:
        raise RuntimeError(f"active logical truth missing:{table}:{key}")
    return decode_current_payload(str(row[0]))


def read_logical_truth(path: Path | str) -> dict[str, Any]:
    conn = sqlite3.connect(Path(path))
    conn.row_factory = sqlite3.Row
    try:
        return {
            "strategy": _payload(conn, "strategy_current", "state_key", "transition"),
            "wallet": _payload(conn, "wallet_current", "wallet_id", "__transition_state__"),
            "wallet_evidence_watermarks": _payload(conn, "wallet_current", "wallet_id", "__transition_watermarks__"),
            "provider_source": _payload(conn, "provider_current", "provider_id", "__transition__"),
            "freshness": _payload(conn, "system_current", "state_key", "freshness"),
            "latest_event_ids": _payload(conn, "system_current", "state_key", "latest_event_ids"),
            "active_candidates": _payload(conn, "active_candidates", "candidate_id", "__transition__"),
            "active_lifecycles": _payload(conn, "active_lifecycles", "lifecycle_id", "__transition__"),
            "portfolio": _payload(conn, "portfolio_current", "state_key", "transition"),
            "replication_watermarks": _payload(conn, "system_current", "state_key", "replication_watermarks"),
            "certification": _payload(conn, "certification_current", "state_key", "transition"),
            "continuity": _payload(conn, "continuity_current", "state_key", "transition"),
        }
    finally:
        conn.close()


def build_shadow_database(*, legacy_path: Path | str, active_path: Path | str, release_sha: str, replace_existing: bool = False) -> ShadowMigrationReport:
    legacy = Path(legacy_path)
    active = Path(active_path)
    if legacy.resolve() == active.resolve():
        raise RuntimeError("active path must differ from legacy path")
    if active.exists():
        if not replace_existing:
            raise RuntimeError(f"shadow active database already exists:{active}")
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(active) + suffix)
            if candidate.exists():
                candidate.unlink()
    extraction = LegacyCurrentStateExtractor(legacy).extract()
    storage = ActiveStorage(active)
    storage.initialize(epoch_id=f"shadow-{uuid.uuid4().hex}")
    schema_store = ObservationEventStore(active)
    schema_store.close()
    copied: dict[str, int] = {}
    sequence_frontiers: dict[str, int] = {}
    source = _connect_ro(legacy)
    dest = storage.connect()
    try:
        # One pinned source snapshot owns all exact state, bounded evidence and
        # sequence frontiers.  No second migration pass is allowed to race ahead
        # of the semantic checkpoint.
        source.execute("BEGIN")
        dest.execute("BEGIN IMMEDIATE")
        _copy_current_sections(source, dest, extraction.truth, copied)
        _ensure_paper_checkpoint(source, dest, extraction.truth, copied)
        _copy_bounded_v52(source, dest, copied)
        _copy_wallet_materializations(source, dest, copied)
        _copy_active_subject_evidence(source, dest, extraction.truth, copied)
        copy_bounded_current_v52(source, dest, copy_query=_copy_query, counts=copied)
        copy_bounded_runtime_evidence(source, dest, copy_query=_copy_query, counts=copied)
        sequence_frontiers = advance_registered_sequences(source, dest)
        dest.commit()
        source.execute("COMMIT")
    except Exception:
        dest.rollback()
        try:
            source.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        dest.close()
        source.close()
    _write_logical_truth(storage, extraction.truth)
    checkpoint = build_checkpoint_payload(
        release_sha=release_sha,
        current_truth=extraction.truth,
        provenance={
            "legacy_path": str(legacy),
            "legacy_size_bytes": extraction.source_size_bytes,
            "legacy_schema_fingerprint": extraction.schema_fingerprint,
            "migration": "bounded-current-state-only",
            "history_copied_wholesale": False,
            "single_pinned_source_transaction": True,
            "registered_sequence_frontiers": sequence_frontiers,
        },
    )
    persist_verified_checkpoint(storage, checkpoint_payload=checkpoint, source_truth=extraction.truth)
    active_truth = read_logical_truth(active)
    verification = verify_semantic_equivalence(extraction.truth, active_truth)
    if not verification.equivalent:
        raise RuntimeError("shadow semantic equivalence failed:" + ",".join(verification.mismatched_sections))
    storage.prune_v52_market_validation()
    storage.prune_v52_wallet_forward_alpha()
    storage.checkpoint_wal()
    storage.assert_positive_schema()
    storage.enforce_hard_budget()
    sizes = storage.storage_bytes()
    return ShadowMigrationReport(
        release_sha=release_sha,
        source_path=str(legacy),
        source_size_bytes=extraction.source_size_bytes,
        active_path=str(active),
        active_size_bytes=sizes["main"],
        active_wal_bytes=sizes["wal"],
        source_schema_fingerprint=extraction.schema_fingerprint,
        semantic_hash=payload_hash(extraction.truth),
        equivalent=True,
        mismatched_sections=verification.mismatched_sections,
        copied_rows=copied,
    )
