from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from pathlib import Path
from typing import Any


class AppendOnlyEventStore:
    """Hash-chained SQLite event ledger plus normalized point-in-time evidence indexes."""

    def __init__(self, path: str | Path = "data/solana-roi.sqlite3"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS events ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "event_type TEXT NOT NULL, observed_at TEXT NOT NULL, payload_json TEXT NOT NULL, "
            "previous_hash TEXT, lineage_hash TEXT NOT NULL UNIQUE)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS wallet_profiles ("
            "wallet TEXT PRIMARY KEY, entity_id TEXT NOT NULL, tier TEXT NOT NULL, "
            "first_touch_sample_size INTEGER NOT NULL, historically_eligible INTEGER NOT NULL, "
            "updated_at TEXT NOT NULL)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS normalized_swaps ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, signature TEXT NOT NULL, slot INTEGER NOT NULL, "
            "observed_at TEXT NOT NULL, received_at TEXT NOT NULL, wallet TEXT NOT NULL, "
            "token_mint TEXT NOT NULL, side TEXT NOT NULL, token_amount REAL NOT NULL, "
            "native_amount_sol REAL NOT NULL, reference_price_sol REAL NOT NULL, "
            "ingestion_latency_ms REAL NOT NULL, source TEXT NOT NULL, "
            "UNIQUE(signature, wallet, token_mint, side))"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS token_first_touches ("
            "token_mint TEXT PRIMARY KEY, signature TEXT NOT NULL, wallet TEXT NOT NULL, "
            "entity_id TEXT NOT NULL, tier TEXT NOT NULL, observed_at TEXT NOT NULL, "
            "reference_price_sol REAL NOT NULL)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS risk_evidence ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, token_mint TEXT NOT NULL, "
            "dimension TEXT NOT NULL, observed_at TEXT NOT NULL, received_at TEXT NOT NULL, "
            "source TEXT NOT NULL, payload_json TEXT NOT NULL, "
            "UNIQUE(token_mint, dimension, observed_at, source))"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS entity_links ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, wallet_a TEXT NOT NULL, wallet_b TEXT NOT NULL, "
            "relationship TEXT NOT NULL, confidence REAL NOT NULL, observed_at TEXT NOT NULL, "
            "received_at TEXT NOT NULL, source TEXT NOT NULL, "
            "UNIQUE(wallet_a, wallet_b, relationship, observed_at, source))"
        )
        self.db.execute("CREATE INDEX IF NOT EXISTS ix_swaps_token_time ON normalized_swaps(token_mint, observed_at)")
        self.db.execute("CREATE INDEX IF NOT EXISTS ix_swaps_wallet_time ON normalized_swaps(wallet, observed_at)")
        self.db.execute("CREATE INDEX IF NOT EXISTS ix_risk_token_dimension_time ON risk_evidence(token_mint, dimension, received_at, observed_at)")
        self.db.execute("CREATE INDEX IF NOT EXISTS ix_entity_links_wallet_a_time ON entity_links(wallet_a, received_at)")
        self.db.execute("CREATE INDEX IF NOT EXISTS ix_entity_links_wallet_b_time ON entity_links(wallet_b, received_at)")
        self.db.commit()

    def append(self, event_type: str, observed_at: str, payload: dict[str, Any]) -> str:
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        with self._lock, self.db:
            row = self.db.execute("SELECT lineage_hash FROM events ORDER BY id DESC LIMIT 1").fetchone()
            previous = row[0] if row else None
            lineage = hashlib.sha256(f"{previous or ''}|{event_type}|{observed_at}|{raw}".encode()).hexdigest()
            self.db.execute(
                "INSERT INTO events(event_type, observed_at, payload_json, previous_hash, lineage_hash) VALUES (?, ?, ?, ?, ?)",
                (event_type, observed_at, raw, previous, lineage),
            )
        return lineage

    def verify(self) -> bool:
        previous: str | None = None
        with self._lock:
            rows = self.db.execute(
                "SELECT event_type, observed_at, payload_json, previous_hash, lineage_hash FROM events ORDER BY id"
            ).fetchall()
        for event_type, observed_at, raw, recorded_previous, lineage in rows:
            if recorded_previous != previous:
                return False
            expected = hashlib.sha256(f"{previous or ''}|{event_type}|{observed_at}|{raw}".encode()).hexdigest()
            if expected != lineage:
                return False
            previous = lineage
        return True

    def upsert_wallet_profile(self, *, wallet: str, entity_id: str, tier: str, first_touch_sample_size: int, historically_eligible: bool, updated_at: str) -> None:
        with self._lock, self.db:
            self.db.execute(
                "INSERT INTO wallet_profiles(wallet, entity_id, tier, first_touch_sample_size, historically_eligible, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(wallet) DO UPDATE SET "
                "entity_id=excluded.entity_id, tier=excluded.tier, first_touch_sample_size=excluded.first_touch_sample_size, "
                "historically_eligible=excluded.historically_eligible, updated_at=excluded.updated_at",
                (wallet, entity_id, tier, int(first_touch_sample_size), 1 if historically_eligible else 0, updated_at),
            )

    def wallet_profile(self, wallet: str) -> dict[str, Any] | None:
        with self._lock:
            row = self.db.execute(
                "SELECT wallet, entity_id, tier, first_touch_sample_size, historically_eligible, updated_at FROM wallet_profiles WHERE wallet=?",
                (wallet,),
            ).fetchone()
        return dict(row) if row is not None else None

    def record_swap(self, *, signature: str, slot: int, observed_at: str, received_at: str, wallet: str, token_mint: str, side: str, token_amount: float, native_amount_sol: float, reference_price_sol: float, ingestion_latency_ms: float, source: str) -> bool:
        with self._lock, self.db:
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO normalized_swaps(signature, slot, observed_at, received_at, wallet, token_mint, side, token_amount, native_amount_sol, reference_price_sol, ingestion_latency_ms, source) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (signature, int(slot), observed_at, received_at, wallet, token_mint, side, float(token_amount), float(native_amount_sol), float(reference_price_sol), float(ingestion_latency_ms), source),
            )
            return cursor.rowcount == 1

    def claim_first_touch(self, *, token_mint: str, signature: str, wallet: str, entity_id: str, tier: str, observed_at: str, reference_price_sol: float) -> bool:
        with self._lock, self.db:
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO token_first_touches(token_mint, signature, wallet, entity_id, tier, observed_at, reference_price_sol) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (token_mint, signature, wallet, entity_id, tier, observed_at, float(reference_price_sol)),
            )
            return cursor.rowcount == 1

    def first_touch(self, token_mint: str) -> dict[str, Any] | None:
        with self._lock:
            row = self.db.execute(
                "SELECT token_mint, signature, wallet, entity_id, tier, observed_at, reference_price_sol FROM token_first_touches WHERE token_mint=?",
                (token_mint,),
            ).fetchone()
        return dict(row) if row is not None else None

    def record_risk_evidence(self, *, token_mint: str, dimension: str, observed_at: str, received_at: str, source: str, payload: dict[str, Any]) -> bool:
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        with self._lock, self.db:
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO risk_evidence(token_mint, dimension, observed_at, received_at, source, payload_json) VALUES (?, ?, ?, ?, ?, ?)",
                (token_mint, dimension, observed_at, received_at, source, raw),
            )
            return cursor.rowcount == 1

    def latest_risk_evidence(self, token_mint: str, dimension: str, *, as_of_received_at: str) -> dict[str, Any] | None:
        with self._lock:
            row = self.db.execute(
                "SELECT token_mint, dimension, observed_at, received_at, source, payload_json FROM risk_evidence "
                "WHERE token_mint=? AND dimension=? AND received_at<=? ORDER BY observed_at DESC, id DESC LIMIT 1",
                (token_mint, dimension, as_of_received_at),
            ).fetchone()
        if row is None:
            return None
        payload = dict(row)
        payload["payload"] = json.loads(str(payload.pop("payload_json")))
        return payload

    def record_entity_link(self, *, wallet_a: str, wallet_b: str, relationship: str, confidence: float, observed_at: str, received_at: str, source: str) -> bool:
        left, right = sorted((wallet_a, wallet_b))
        with self._lock, self.db:
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO entity_links(wallet_a, wallet_b, relationship, confidence, observed_at, received_at, source) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (left, right, relationship, float(confidence), observed_at, received_at, source),
            )
            return cursor.rowcount == 1

    def entity_neighbors(self, wallet: str, *, as_of_received_at: str, min_confidence: float) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.db.execute(
                "SELECT wallet_a, wallet_b, relationship, confidence, observed_at, received_at, source FROM entity_links "
                "WHERE received_at<=? AND confidence>=? AND (wallet_a=? OR wallet_b=?) ORDER BY received_at, id",
                (as_of_received_at, float(min_confidence), wallet, wallet),
            ).fetchall()
        return [dict(row) for row in rows]

    def evidence_counts(self) -> dict[str, int]:
        names = ("events", "wallet_profiles", "normalized_swaps", "token_first_touches", "risk_evidence", "entity_links")
        with self._lock:
            return {name: int(self.db.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]) for name in names}

    def close(self) -> None:
        with self._lock:
            self.db.close()
