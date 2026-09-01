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
            "CREATE INDEX IF NOT EXISTS ix_swaps_token_time "
            "ON normalized_swaps(token_mint, observed_at)"
        )
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_swaps_wallet_time "
            "ON normalized_swaps(wallet, observed_at)"
        )
        self.db.commit()

    def append(self, event_type: str, observed_at: str, payload: dict[str, Any]) -> str:
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        with self._lock, self.db:
            row = self.db.execute(
                "SELECT lineage_hash FROM events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            previous = row[0] if row else None
            lineage = hashlib.sha256(
                f"{previous or ''}|{event_type}|{observed_at}|{raw}".encode()
            ).hexdigest()
            self.db.execute(
                "INSERT INTO events(event_type, observed_at, payload_json, previous_hash, lineage_hash) "
                "VALUES (?, ?, ?, ?, ?)",
                (event_type, observed_at, raw, previous, lineage),
            )
        return lineage

    def verify(self) -> bool:
        previous: str | None = None
        with self._lock:
            rows = self.db.execute(
                "SELECT event_type, observed_at, payload_json, previous_hash, lineage_hash "
                "FROM events ORDER BY id"
            ).fetchall()
        for event_type, observed_at, raw, recorded_previous, lineage in rows:
            if recorded_previous != previous:
                return False
            expected = hashlib.sha256(
                f"{previous or ''}|{event_type}|{observed_at}|{raw}".encode()
            ).hexdigest()
            if expected != lineage:
                return False
            previous = lineage
        return True

    def upsert_wallet_profile(
        self,
        *,
        wallet: str,
        entity_id: str,
        tier: str,
        first_touch_sample_size: int,
        historically_eligible: bool,
        updated_at: str,
    ) -> None:
        with self._lock, self.db:
            self.db.execute(
                "INSERT INTO wallet_profiles("
                "wallet, entity_id, tier, first_touch_sample_size, historically_eligible, updated_at"
                ") VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(wallet) DO UPDATE SET "
                "entity_id=excluded.entity_id, tier=excluded.tier, "
                "first_touch_sample_size=excluded.first_touch_sample_size, "
                "historically_eligible=excluded.historically_eligible, updated_at=excluded.updated_at",
                (
                    wallet,
                    entity_id,
                    tier,
                    int(first_touch_sample_size),
                    1 if historically_eligible else 0,
                    updated_at,
                ),
            )

    def wallet_profile(self, wallet: str) -> dict[str, Any] | None:
        with self._lock:
            row = self.db.execute(
                "SELECT wallet, entity_id, tier, first_touch_sample_size, "
                "historically_eligible, updated_at FROM wallet_profiles WHERE wallet=?",
                (wallet,),
            ).fetchone()
        return dict(row) if row is not None else None

    def record_swap(
        self,
        *,
        signature: str,
        slot: int,
        observed_at: str,
        received_at: str,
        wallet: str,
        token_mint: str,
        side: str,
        token_amount: float,
        native_amount_sol: float,
        reference_price_sol: float,
        ingestion_latency_ms: float,
        source: str,
    ) -> bool:
        with self._lock, self.db:
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO normalized_swaps("
                "signature, slot, observed_at, received_at, wallet, token_mint, side, "
                "token_amount, native_amount_sol, reference_price_sol, ingestion_latency_ms, source"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    signature,
                    int(slot),
                    observed_at,
                    received_at,
                    wallet,
                    token_mint,
                    side,
                    float(token_amount),
                    float(native_amount_sol),
                    float(reference_price_sol),
                    float(ingestion_latency_ms),
                    source,
                ),
            )
            return cursor.rowcount == 1

    def claim_first_touch(
        self,
        *,
        token_mint: str,
        signature: str,
        wallet: str,
        entity_id: str,
        tier: str,
        observed_at: str,
        reference_price_sol: float,
    ) -> bool:
        with self._lock, self.db:
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO token_first_touches("
                "token_mint, signature, wallet, entity_id, tier, observed_at, reference_price_sol"
                ") VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    token_mint,
                    signature,
                    wallet,
                    entity_id,
                    tier,
                    observed_at,
                    float(reference_price_sol),
                ),
            )
            return cursor.rowcount == 1

    def first_touch(self, token_mint: str) -> dict[str, Any] | None:
        with self._lock:
            row = self.db.execute(
                "SELECT token_mint, signature, wallet, entity_id, tier, observed_at, "
                "reference_price_sol FROM token_first_touches WHERE token_mint=?",
                (token_mint,),
            ).fetchone()
        return dict(row) if row is not None else None

    def evidence_counts(self) -> dict[str, int]:
        names = ("events", "wallet_profiles", "normalized_swaps", "token_first_touches")
        with self._lock:
            return {
                name: int(self.db.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])
                for name in names
            }

    def close(self) -> None:
        with self._lock:
            self.db.close()
