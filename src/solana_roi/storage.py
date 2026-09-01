from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any


class AppendOnlyEventStore:
    """Tiny hash-chained SQLite event ledger for point-in-time forward evidence."""

    def __init__(self, path: str | Path = "data/solana-roi.sqlite3"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT NOT NULL, observed_at TEXT NOT NULL, payload_json TEXT NOT NULL, previous_hash TEXT, lineage_hash TEXT NOT NULL UNIQUE)")
        self.db.commit()

    def append(self, event_type: str, observed_at: str, payload: dict[str, Any]) -> str:
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        row = self.db.execute("SELECT lineage_hash FROM events ORDER BY id DESC LIMIT 1").fetchone()
        previous = row[0] if row else None
        lineage = hashlib.sha256(f"{previous or ''}|{event_type}|{observed_at}|{raw}".encode()).hexdigest()
        with self.db:
            self.db.execute("INSERT INTO events(event_type, observed_at, payload_json, previous_hash, lineage_hash) VALUES (?, ?, ?, ?, ?)", (event_type, observed_at, raw, previous, lineage))
        return lineage

    def verify(self) -> bool:
        previous: str | None = None
        rows = self.db.execute("SELECT event_type, observed_at, payload_json, previous_hash, lineage_hash FROM events ORDER BY id").fetchall()
        for event_type, observed_at, raw, recorded_previous, lineage in rows:
            if recorded_previous != previous: return False
            expected = hashlib.sha256(f"{previous or ''}|{event_type}|{observed_at}|{raw}".encode()).hexdigest()
            if expected != lineage: return False
            previous = lineage
        return True
