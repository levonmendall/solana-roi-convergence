from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .storage_retention import RETENTION_REGISTRY, assert_registered, contract_for

ACTIVE_SCHEMA_VERSION = 2


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


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
CREATE TABLE IF NOT EXISTS system_current (state_key TEXT PRIMARY KEY,payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS strategy_current (state_key TEXT PRIMARY KEY,payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS wallet_current (wallet_id TEXT PRIMARY KEY,payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,evidence_watermark INTEGER,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS provider_current (provider_id TEXT PRIMARY KEY,payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,source_watermark TEXT,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS portfolio_current (state_key TEXT PRIMARY KEY,payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,last_engine_event_id INTEGER,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS active_candidates (candidate_id TEXT PRIMARY KEY,payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,source_event_id INTEGER,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS active_lifecycles (lifecycle_id TEXT PRIMARY KEY,payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,source_event_id INTEGER,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS checkpoint_current (checkpoint_id TEXT PRIMARY KEY,created_at TEXT NOT NULL,schema_version INTEGER NOT NULL,migration_version INTEGER NOT NULL,release_sha TEXT NOT NULL,payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,semantic_hash TEXT NOT NULL,verified INTEGER NOT NULL CHECK(verified IN (0,1)));
CREATE TABLE IF NOT EXISTS continuity_current (state_key TEXT PRIMARY KEY,payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS certification_current (state_key TEXT PRIMARY KEY,payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS bounded_market_evidence (id INTEGER PRIMARY KEY AUTOINCREMENT,observed_at TEXT NOT NULL,evidence_kind TEXT NOT NULL,subject_id TEXT,payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,decision_id TEXT);
CREATE INDEX IF NOT EXISTS idx_bounded_market_evidence_observed_at ON bounded_market_evidence(observed_at,id);
CREATE TABLE IF NOT EXISTS bounded_forward_deltas (id INTEGER PRIMARY KEY AUTOINCREMENT,source_event_id INTEGER,delta_kind TEXT NOT NULL,payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,created_at TEXT NOT NULL,checkpointed INTEGER NOT NULL DEFAULT 0 CHECK(checkpointed IN (0,1)));
CREATE INDEX IF NOT EXISTS idx_bounded_forward_deltas_checkpointed_id ON bounded_forward_deltas(checkpointed,id);
CREATE TABLE IF NOT EXISTS bounded_transport_state (change_id INTEGER PRIMARY KEY,table_name TEXT NOT NULL,operation TEXT NOT NULL,payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,created_at TEXT NOT NULL,acknowledged_at TEXT);
CREATE INDEX IF NOT EXISTS idx_bounded_transport_state_ack_change ON bounded_transport_state(acknowledged_at,change_id);
CREATE TABLE IF NOT EXISTS bounded_recent_diagnostics (id INTEGER PRIMARY KEY AUTOINCREMENT,created_at TEXT NOT NULL,expires_at TEXT NOT NULL,incident_key TEXT,stage TEXT NOT NULL,payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_bounded_recent_diagnostics_expires ON bounded_recent_diagnostics(expires_at,id);
CREATE TABLE IF NOT EXISTS storage_epoch_state (singleton_key INTEGER PRIMARY KEY CHECK(singleton_key=1),epoch_id TEXT NOT NULL,opened_at TEXT NOT NULL,warning_bytes INTEGER NOT NULL,hard_bytes INTEGER NOT NULL,status TEXT NOT NULL,last_checkpoint_id TEXT,updated_at TEXT NOT NULL);
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

    def _install_page_boundary(self, conn: sqlite3.Connection) -> None:
        """Install the connection-local SQLite page ceiling on every writer.

        SQLite's max_page_count is not durable across new connections.  Treating
        initialization as a persistent physical quota therefore leaves later
        connections uncapped.  ActiveStorage owns this connection factory, so
        every connection must re-install and verify the ceiling before use.
        """
        page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
        hard_pages = max(1, int(self.budget.hard_bytes) // page_size)
        effective_max = int(conn.execute(f"PRAGMA max_page_count={hard_pages}").fetchone()[0])
        if effective_max > hard_pages:
            conn.close()
            raise RuntimeError(
                f"active storage page boundary could not be installed: {effective_max} > {hard_pages}"
            )

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        self._install_page_boundary(conn)
        return conn

    def initialize(self, *, epoch_id: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            # New epochs reclaim deleted pages incrementally instead of repeating
            # the legacy pattern where logical pruning leaves an indefinitely
            # growing physical file. auto_vacuum must be selected before schema
            # objects exist, so never rewrite this setting on an existing DB.
            if not self.ordinary_tables(conn):
                conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
            conn.executescript(_SCHEMA_SQL)
            actual = self.ordinary_tables(conn)
            assert_registered(actual)
            self._install_page_boundary(conn)
            now = _utc_now()
            conn.execute(
                "INSERT INTO storage_epoch_state(singleton_key,epoch_id,opened_at,warning_bytes,hard_bytes,status,last_checkpoint_id,updated_at) "
                "VALUES(1,?,?,?,?, 'OPEN',NULL,?) ON CONFLICT(singleton_key) DO NOTHING",
                (epoch_id, now, self.budget.warning_bytes, self.budget.hard_bytes, now),
            )
            conn.commit()

    @staticmethod
    def ordinary_tables(conn: sqlite3.Connection) -> set[str]:
        return {str(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}

    def assert_positive_schema(self) -> None:
        with self.connect() as conn:
            assert_registered(self.ordinary_tables(conn))

    def replace_current(self, table: str, key_column: str, key: str, payload: Any, **extra: Any) -> None:
        if table not in _CURRENT_TABLES or table == "checkpoint_current":
            raise ValueError(f"not a replaceable current-state table: {table}")
        contract_for(table)
        allowed_extras = {
            "wallet_current": {"evidence_watermark"},
            "provider_current": {"source_watermark"},
            "portfolio_current": {"last_engine_event_id"},
            "active_candidates": {"source_event_id"},
            "active_lifecycles": {"source_event_id"},
        }.get(table, set())
        if set(extra) - allowed_extras:
            raise ValueError(f"unsupported columns for {table}: {sorted(set(extra)-allowed_extras)}")
        body = canonical_json(payload)
        digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
        columns = [key_column,"payload_json","payload_hash",*extra.keys(),"updated_at"]
        values = [key,body,digest,*extra.values(),_utc_now()]
        placeholders = ",".join("?" for _ in columns)
        updates = ",".join(f"{name}=excluded.{name}" for name in columns if name != key_column)
        with self.connect() as conn:
            conn.execute(f"INSERT INTO {table}({','.join(columns)}) VALUES({placeholders}) ON CONFLICT({key_column}) DO UPDATE SET {updates}", values)
            conn.commit()

    def prune_expired_diagnostics(self, *, now_iso: str | None = None, batch_size: int = 5000) -> int:
        now_iso = now_iso or _utc_now()
        return self._delete_ids("bounded_recent_diagnostics", "id", "expires_at<=?", (now_iso,), batch_size)

    def prune_acknowledged_transport(self, *, batch_size: int = 5000) -> int:
        return self._delete_ids("bounded_transport_state", "change_id", "acknowledged_at IS NOT NULL", (), batch_size)

    def _delete_ids(self, table: str, id_col: str, where: str, args: tuple[Any,...], batch_size: int) -> int:
        contract_for(table)
        with self.connect() as conn:
            ids = [int(row[0]) for row in conn.execute(f"SELECT {id_col} FROM {table} WHERE {where} ORDER BY {id_col} LIMIT ?", (*args,int(batch_size)))]
            if not ids:
                return 0
            conn.executemany(f"DELETE FROM {table} WHERE {id_col}=?", ((i,) for i in ids))
            conn.commit()
            return len(ids)

    def prune_v52_market_validation(self, *, now: datetime | None = None) -> dict[str,int]:
        """Bound validation persistence without changing the 250-sample decision window."""
        cutoff = ((now or datetime.now(timezone.utc)) - timedelta(days=31)).isoformat()
        deleted = {"features":0,"shadow":0,"point_in_time":0}
        with self.connect() as conn:
            tables = self.ordinary_tables(conn)
            if "v52_market_validation_features" in tables:
                # Keep the newest 250 values for every lane+feature irrespective of age;
                # delete only rows outside that exact decision window.
                cur = conn.execute(
                    "DELETE FROM v52_market_validation_features WHERE id IN ("
                    "SELECT id FROM (SELECT id,ROW_NUMBER() OVER(PARTITION BY lane,feature ORDER BY id DESC) rn "
                    "FROM v52_market_validation_features) WHERE rn>250)"
                )
                deleted["features"] = max(0,int(cur.rowcount))
            if "v52_market_validation_shadow_outcomes" in tables:
                cur = conn.execute("DELETE FROM v52_market_validation_shadow_outcomes WHERE resolved_at IS NOT NULL AND resolved_at<?", (cutoff,))
                deleted["shadow"] = max(0,int(cur.rowcount))
            if "v52_market_validation_point_in_time" in tables:
                cur = conn.execute("DELETE FROM v52_market_validation_point_in_time WHERE observed_at<? AND future_outcome_json IS NOT NULL", (cutoff,))
                deleted["point_in_time"] = max(0,int(cur.rowcount))
            conn.commit()
        return deleted

    def prune_v52_wallet_forward_alpha(self, *, now: datetime | None = None) -> dict[str,int]:
        cutoff = ((now or datetime.now(timezone.utc)) - timedelta(days=31)).isoformat()
        deleted = {"observations":0,"outcomes":0,"integrity":0,"validation":0}
        with self.connect() as conn:
            tables = self.ordinary_tables(conn)
            if "v52_wallet_forward_outcomes" in tables:
                cur = conn.execute("DELETE FROM v52_wallet_forward_outcomes WHERE available_at<?", (cutoff,))
                deleted["outcomes"] = max(0,int(cur.rowcount))
            if "v52_wallet_point_in_time_observations" in tables:
                # An observation can be pruned only when no retained outcome references it.
                cur = conn.execute(
                    "DELETE FROM v52_wallet_point_in_time_observations AS p WHERE p.detected_at<? AND NOT EXISTS ("
                    "SELECT 1 FROM v52_wallet_forward_outcomes o WHERE o.wallet=p.wallet AND o.context_key=p.context_key AND o.candidate_id=p.candidate_id)",
                    (cutoff,),
                )
                deleted["observations"] = max(0,int(cur.rowcount))
            if "v52_wallet_integrity_snapshots" in tables:
                cur = conn.execute(
                    "DELETE FROM v52_wallet_integrity_snapshots WHERE observed_at<? AND id NOT IN ("
                    "SELECT MAX(id) FROM v52_wallet_integrity_snapshots GROUP BY wallet)", (cutoff,))
                deleted["integrity"] = max(0,int(cur.rowcount))
            if "v52_wallet_forward_validation" in tables:
                cur = conn.execute(
                    "DELETE FROM v52_wallet_forward_validation WHERE evaluated_at<? AND id<>(SELECT MAX(id) FROM v52_wallet_forward_validation)",
                    (cutoff,),
                )
                deleted["validation"] = max(0,int(cur.rowcount))
            conn.commit()
        return deleted

    def storage_bytes(self) -> dict[str,int]:
        main = self.path.stat().st_size if self.path.exists() else 0
        wal = Path(str(self.path)+"-wal")
        shm = Path(str(self.path)+"-shm")
        return {"main":main,"wal":wal.stat().st_size if wal.exists() else 0,"shm":shm.stat().st_size if shm.exists() else 0}

    def page_budget_status(self) -> dict[str, int]:
        with self.connect() as conn:
            page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
            page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
            freelist_count = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
            max_page_count = int(conn.execute("PRAGMA max_page_count").fetchone()[0])
            auto_vacuum = int(conn.execute("PRAGMA auto_vacuum").fetchone()[0])
        return {
            "page_size": page_size,
            "page_count": page_count,
            "freelist_count": freelist_count,
            "max_page_count": max_page_count,
            "auto_vacuum": auto_vacuum,
            "allocated_bytes": page_size * page_count,
            "free_bytes": page_size * freelist_count,
            "configured_hard_bytes": int(self.budget.hard_bytes),
        }

    def reclaim_free_pages(self, *, max_pages: int = 16_384) -> dict[str, int]:
        """Bounded incremental physical reclaim for new active epochs.

        This is deliberately a no-op on pre-repair databases that were not born
        with incremental auto-vacuum. Those stores are compacted by epoch rollover
        instead of invoking an unbounded in-place VACUUM under production load.
        """
        with self.connect() as conn:
            mode = int(conn.execute("PRAGMA auto_vacuum").fetchone()[0])
            before = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
            reclaimed_request = min(max(0, int(max_pages)), before) if mode == 2 else 0
            if reclaimed_request:
                conn.execute(f"PRAGMA incremental_vacuum({reclaimed_request})")
                conn.commit()
            after = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        return {
            "auto_vacuum": mode,
            "freelist_before": before,
            "freelist_after": after,
            "pages_requested": reclaimed_request,
            "pages_reclaimed": max(0, before - after),
        }

    def warning_boundary_exceeded(self) -> bool:
        sizes = self.storage_bytes()
        return sizes["main"] >= self.budget.warning_bytes or sizes["wal"] >= self.budget.max_wal_bytes

    def enforce_hard_budget(self) -> None:
        sizes = self.storage_bytes()
        if sizes["main"] >= self.budget.hard_bytes:
            raise RuntimeError(f"active storage hard boundary exceeded: {sizes['main']} >= {self.budget.hard_bytes}")
        if sizes["wal"] >= self.budget.max_wal_bytes:
            raise RuntimeError(f"active WAL hard boundary exceeded: {sizes['wal']} >= {self.budget.max_wal_bytes}")

    def checkpoint_wal(self) -> tuple[int,int,int]:
        with self.connect() as conn:
            row = conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        result = tuple(int(v) for v in row)
        self.enforce_hard_budget()
        return result  # type: ignore[return-value]
