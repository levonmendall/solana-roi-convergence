from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .storage_retention import RETENTION_REGISTRY, assert_registered

ACTIVE_SCHEMA_VERSION = 1


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def payload_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


_CURRENT_TABLES = (
    "system_current",
    "strategy_current",
    "wallet_current",
    "provider_current",
    "portfolio_current",
    "active_candidates",
    "active_lifecycles",
    "checkpoint_current",
    "continuity_current",
    "certification_current",
)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS system_current (
    state_key TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS strategy_current (
    state_key TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS wallet_current (
    wallet_id TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    evidence_watermark INTEGER,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS provider_current (
    provider_id TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    source_watermark TEXT,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS portfolio_current (
    state_key TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    last_engine_event_id INTEGER,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS active_candidates (
    candidate_id TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    source_event_id INTEGER,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS active_lifecycles (
    lifecycle_id TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    source_event_id INTEGER,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS checkpoint_current (
    checkpoint_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    migration_version INTEGER NOT NULL,
    release_sha TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    semantic_hash TEXT NOT NULL,
    verified INTEGER NOT NULL CHECK (verified IN (0,1))
);
CREATE TABLE IF NOT EXISTS continuity_current (
    state_key TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS certification_current (
    state_key TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bounded_market_evidence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_at TEXT NOT NULL,
    evidence_kind TEXT NOT NULL,
    subject_id TEXT,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    decision_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_bounded_market_evidence_observed_at
    ON bounded_market_evidence(observed_at, id);
CREATE TABLE IF NOT EXISTS bounded_forward_deltas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_event_id INTEGER,
    delta_kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    checkpointed INTEGER NOT NULL DEFAULT 0 CHECK (checkpointed IN (0,1))
);
CREATE INDEX IF NOT EXISTS idx_bounded_forward_deltas_checkpointed_id
    ON bounded_forward_deltas(checkpointed, id);
CREATE TABLE IF NOT EXISTS bounded_transport_state (
    change_id INTEGER PRIMARY KEY,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    acknowledged_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_bounded_transport_state_ack_change
    ON bounded_transport_state(acknowledged_at, change_id);
CREATE TABLE IF NOT EXISTS bounded_recent_diagnostics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    incident_key TEXT,
    stage TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bounded_recent_diagnostics_expires
    ON bounded_recent_diagnostics(expires_at, id);
CREATE TABLE IF NOT EXISTS storage_epoch_state (
    singleton_key INTEGER PRIMARY KEY CHECK (singleton_key = 1),
    epoch_id TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    warning_bytes INTEGER NOT NULL,
    hard_bytes INTEGER NOT NULL,
    status TEXT NOT NULL,
    last_checkpoint_id TEXT,
    updated_at TEXT NOT NULL
);
"""


@dataclass(frozen=True)
class ActiveStorageBudget:
    warning_bytes: int = 805_306_368
    hard_bytes: int = 1_073_741_824
    max_wal_bytes: int = 268_435_456


class ActiveStorage:
    def __init__(self, path: Path | str, *, budget: ActiveStorageBudget | None = None) -> None:
        self.path = Path(path)
        self.budget = budget or ActiveStorageBudget()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def initialize(self, *, epoch_id: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(_SCHEMA_SQL)
            actual = {
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            assert_registered(actual)
            now = _utc_now()
            conn.execute(
                """
                INSERT INTO storage_epoch_state(
                    singleton_key, epoch_id, opened_at, warning_bytes, hard_bytes,
                    status, last_checkpoint_id, updated_at
                ) VALUES(1, ?, ?, ?, ?, 'OPEN', NULL, ?)
                ON CONFLICT(singleton_key) DO NOTHING
                """,
                (epoch_id, now, self.budget.warning_bytes, self.budget.hard_bytes, now),
            )
            conn.commit()

    def replace_current(self, table: str, key_column: str, key: str, payload: Any, **extra: Any) -> None:
        if table not in _CURRENT_TABLES or table == "checkpoint_current":
            raise ValueError(f"not a replaceable current-state table: {table}")
        if table not in RETENTION_REGISTRY:
            raise ValueError(f"unregistered persistent dataset: {table}")
        allowed_extras = {
            "wallet_current": {"evidence_watermark"},
            "provider_current": {"source_watermark"},
            "portfolio_current": {"last_engine_event_id"},
            "active_candidates": {"source_event_id"},
            "active_lifecycles": {"source_event_id"},
        }.get(table, set())
        if set(extra) - allowed_extras:
            raise ValueError(f"unsupported columns for {table}: {sorted(set(extra) - allowed_extras)}")
        body = canonical_json(payload)
        digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
        columns = [key_column, "payload_json", "payload_hash", *extra.keys(), "updated_at"]
        values = [key, body, digest, *extra.values(), _utc_now()]
        placeholders = ",".join("?" for _ in columns)
        updates = ",".join(f"{name}=excluded.{name}" for name in columns if name != key_column)
        with self.connect() as conn:
            conn.execute(
                f"INSERT INTO {table}({','.join(columns)}) VALUES({placeholders}) "
                f"ON CONFLICT({key_column}) DO UPDATE SET {updates}",
                values,
            )
            conn.commit()

    def prune_expired_diagnostics(self, *, now_iso: str | None = None, batch_size: int = 5000) -> int:
        now_iso = now_iso or _utc_now()
        with self.connect() as conn:
            ids = [
                int(row[0])
                for row in conn.execute(
                    "SELECT id FROM bounded_recent_diagnostics WHERE expires_at <= ? ORDER BY id LIMIT ?",
                    (now_iso, batch_size),
                )
            ]
            if not ids:
                return 0
            conn.executemany("DELETE FROM bounded_recent_diagnostics WHERE id = ?", ((i,) for i in ids))
            conn.commit()
            return len(ids)

    def prune_acknowledged_transport(self, *, batch_size: int = 5000) -> int:
        with self.connect() as conn:
            ids = [
                int(row[0])
                for row in conn.execute(
                    "SELECT change_id FROM bounded_transport_state WHERE acknowledged_at IS NOT NULL ORDER BY change_id LIMIT ?",
                    (batch_size,),
                )
            ]
            if not ids:
                return 0
            conn.executemany("DELETE FROM bounded_transport_state WHERE change_id = ?", ((i,) for i in ids))
            conn.commit()
            return len(ids)

    def storage_bytes(self) -> dict[str, int]:
        main = self.path.stat().st_size if self.path.exists() else 0
        wal_path = Path(str(self.path) + "-wal")
        shm_path = Path(str(self.path) + "-shm")
        return {
            "main": main,
            "wal": wal_path.stat().st_size if wal_path.exists() else 0,
            "shm": shm_path.stat().st_size if shm_path.exists() else 0,
        }

    def enforce_hard_budget(self) -> None:
        sizes = self.storage_bytes()
        if sizes["main"] >= self.budget.hard_bytes:
            raise RuntimeError(
                f"active storage hard boundary exceeded: {sizes['main']} >= {self.budget.hard_bytes}"
            )
        if sizes["wal"] >= self.budget.max_wal_bytes:
            raise RuntimeError(
                f"active WAL hard boundary exceeded: {sizes['wal']} >= {self.budget.max_wal_bytes}"
            )
