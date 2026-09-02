from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from .storage import AppendOnlyEventStore


class ObservationEventStore(AppendOnlyEventStore):
    """Append-only evidence store extended with latency, coverage, and price measurements."""

    def __init__(self, path="data/solana-roi.sqlite3"):
        super().__init__(path)
        with self._lock, self.db:
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS risk_refresh_measurements ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, token_mint TEXT NOT NULL, "
                "trigger_observed_at TEXT NOT NULL, trigger_received_at TEXT NOT NULL, "
                "started_at TEXT NOT NULL, completed_at TEXT NOT NULL, elapsed_ms REAL NOT NULL, "
                "ingestion_latency_ms REAL NOT NULL, end_to_end_ms REAL NOT NULL, "
                "complete INTEGER NOT NULL, fresh INTEGER NOT NULL, readiness_json TEXT NOT NULL)"
            )
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS price_marks ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, token_mint TEXT NOT NULL, "
                "observed_at TEXT NOT NULL, received_at TEXT NOT NULL, price_sol REAL NOT NULL, "
                "source TEXT NOT NULL, source_ref TEXT, UNIQUE(token_mint, observed_at, source, source_ref))"
            )
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS program_coverage_observations ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, token_mint TEXT NOT NULL UNIQUE, "
                "pair_created_at TEXT NOT NULL, assessed_at TEXT NOT NULL, launch_lag_ms REAL, "
                "launch_near_creation INTEGER NOT NULL, early_buy_count INTEGER NOT NULL, "
                "early_buyer_count INTEGER NOT NULL, early_buyers_complete INTEGER NOT NULL, "
                "funding_complete INTEGER NOT NULL DEFAULT 0)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_risk_refresh_completed ON risk_refresh_measurements(completed_at)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_risk_refresh_token ON risk_refresh_measurements(token_mint, completed_at)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_price_marks_token_time ON price_marks(token_mint, received_at)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_program_coverage_assessed ON program_coverage_observations(assessed_at)"
            )

    def record_risk_refresh(
        self,
        *,
        token_mint: str,
        trigger_observed_at: str,
        trigger_received_at: str,
        started_at: str,
        completed_at: str,
        elapsed_ms: float,
        ingestion_latency_ms: float,
        end_to_end_ms: float,
        complete: bool,
        fresh: bool,
        readiness: dict[str, Any],
    ) -> None:
        raw = json.dumps(readiness, sort_keys=True, separators=(",", ":"), default=str)
        with self._lock, self.db:
            self.db.execute(
                "INSERT INTO risk_refresh_measurements("
                "token_mint, trigger_observed_at, trigger_received_at, started_at, completed_at, elapsed_ms, "
                "ingestion_latency_ms, end_to_end_ms, complete, fresh, readiness_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    token_mint,
                    trigger_observed_at,
                    trigger_received_at,
                    started_at,
                    completed_at,
                    float(elapsed_ms),
                    float(ingestion_latency_ms),
                    float(end_to_end_ms),
                    1 if complete else 0,
                    1 if fresh else 0,
                    raw,
                ),
            )
        self.append(
            "risk_refresh_measurement",
            completed_at,
            {
                "token_mint": token_mint,
                "trigger_observed_at": trigger_observed_at,
                "trigger_received_at": trigger_received_at,
                "started_at": started_at,
                "completed_at": completed_at,
                "elapsed_ms": elapsed_ms,
                "ingestion_latency_ms": ingestion_latency_ms,
                "end_to_end_ms": end_to_end_ms,
                "complete": complete,
                "fresh": fresh,
                "readiness": readiness,
            },
        )

    def recent_risk_refreshes(self, limit: int = 500) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.db.execute(
                "SELECT token_mint, trigger_observed_at, trigger_received_at, started_at, completed_at, "
                "elapsed_ms, ingestion_latency_ms, end_to_end_ms, complete, fresh, readiness_json "
                "FROM risk_refresh_measurements ORDER BY id DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["complete"] = bool(item["complete"])
            item["fresh"] = bool(item["fresh"])
            item["readiness"] = json.loads(str(item.pop("readiness_json")))
            result.append(item)
        return result

    def record_program_coverage(
        self,
        *,
        token_mint: str,
        pair_created_at: str,
        assessed_at: str,
        launch_lag_ms: float | None,
        launch_near_creation: bool,
        early_buy_count: int,
        early_buyer_count: int,
        early_buyers_complete: bool,
    ) -> None:
        with self._lock, self.db:
            self.db.execute(
                "INSERT INTO program_coverage_observations("
                "token_mint, pair_created_at, assessed_at, launch_lag_ms, launch_near_creation, "
                "early_buy_count, early_buyer_count, early_buyers_complete, funding_complete) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0) "
                "ON CONFLICT(token_mint) DO UPDATE SET "
                "pair_created_at=excluded.pair_created_at, assessed_at=excluded.assessed_at, "
                "launch_lag_ms=excluded.launch_lag_ms, launch_near_creation=excluded.launch_near_creation, "
                "early_buy_count=excluded.early_buy_count, early_buyer_count=excluded.early_buyer_count, "
                "early_buyers_complete=excluded.early_buyers_complete",
                (
                    token_mint,
                    pair_created_at,
                    assessed_at,
                    float(launch_lag_ms) if launch_lag_ms is not None else None,
                    1 if launch_near_creation else 0,
                    int(early_buy_count),
                    int(early_buyer_count),
                    1 if early_buyers_complete else 0,
                ),
            )
        self.append(
            "program_coverage_observation",
            assessed_at,
            {
                "token_mint": token_mint,
                "pair_created_at": pair_created_at,
                "launch_lag_ms": launch_lag_ms,
                "launch_near_creation": launch_near_creation,
                "early_buy_count": early_buy_count,
                "early_buyer_count": early_buyer_count,
                "early_buyers_complete": early_buyers_complete,
            },
        )

    def mark_program_coverage_funding_complete(self, token_mint: str, *, assessed_at: str) -> None:
        with self._lock, self.db:
            self.db.execute(
                "UPDATE program_coverage_observations SET funding_complete=1, assessed_at=? WHERE token_mint=?",
                (assessed_at, token_mint),
            )
        self.append(
            "program_coverage_funding_complete",
            assessed_at,
            {"token_mint": token_mint, "funding_complete": True},
        )

    def recent_program_coverage(self, limit: int = 500) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.db.execute(
                "SELECT token_mint, pair_created_at, assessed_at, launch_lag_ms, launch_near_creation, "
                "early_buy_count, early_buyer_count, early_buyers_complete, funding_complete "
                "FROM program_coverage_observations ORDER BY assessed_at DESC, id DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["launch_near_creation"] = bool(item["launch_near_creation"])
            item["early_buyers_complete"] = bool(item["early_buyers_complete"])
            item["funding_complete"] = bool(item["funding_complete"])
            result.append(item)
        return result

    def first_touch_chronology_conflicts(self) -> int:
        with self._lock:
            row = self.db.execute(
                "SELECT COUNT(*) FROM token_first_touches t WHERE EXISTS ("
                "SELECT 1 FROM normalized_swaps s JOIN wallet_profiles w ON w.wallet=s.wallet "
                "WHERE s.token_mint=t.token_mint AND s.side='buy' AND w.historically_eligible=1 "
                "AND w.tier IN ('S','A') AND julianday(s.observed_at) < julianday(t.observed_at))"
            ).fetchone()
        return int(row[0]) if row else 0

    def token_first_touch_has_earlier_eligible_swap(self, token_mint: str) -> bool:
        with self._lock:
            row = self.db.execute(
                "SELECT 1 FROM token_first_touches t WHERE t.token_mint=? AND EXISTS ("
                "SELECT 1 FROM normalized_swaps s JOIN wallet_profiles w ON w.wallet=s.wallet "
                "WHERE s.token_mint=t.token_mint AND s.side='buy' AND w.historically_eligible=1 "
                "AND w.tier IN ('S','A') AND julianday(s.observed_at) < julianday(t.observed_at)) LIMIT 1",
                (token_mint,),
            ).fetchone()
        return row is not None

    def paper_entry_authorization_count(self) -> int:
        with self._lock:
            row = self.db.execute(
                "SELECT COUNT(*) FROM events WHERE event_type='candidate_activation_decision' "
                "AND payload_json LIKE '%\"code\":\"PAPER_ENTRY_AUTHORIZED\"%'"
            ).fetchone()
        return int(row[0]) if row else 0

    def record_price_mark(
        self,
        *,
        token_mint: str,
        observed_at: str,
        received_at: str,
        price_sol: float,
        source: str,
        source_ref: str | None = None,
    ) -> bool:
        if price_sol <= 0:
            return False
        with self._lock, self.db:
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO price_marks(token_mint, observed_at, received_at, price_sol, source, source_ref) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (token_mint, observed_at, received_at, float(price_sol), source, source_ref),
            )
        if cursor.rowcount == 1:
            self.append(
                "price_mark",
                received_at,
                {
                    "token_mint": token_mint,
                    "observed_at": observed_at,
                    "received_at": received_at,
                    "price_sol": price_sol,
                    "source": source,
                    "source_ref": source_ref,
                },
            )
            return True
        return False

    def latest_price_mark(self, token_mint: str, *, as_of_received_at: str | None = None) -> dict[str, Any] | None:
        sql = (
            "SELECT token_mint, observed_at, received_at, price_sol, source, source_ref "
            "FROM price_marks WHERE token_mint=?"
        )
        args: list[Any] = [token_mint]
        if as_of_received_at is not None:
            sql += " AND received_at<=?"
            args.append(as_of_received_at)
        sql += " ORDER BY received_at DESC, id DESC LIMIT 1"
        with self._lock:
            row = self.db.execute(sql, tuple(args)).fetchone()
        return dict(row) if row is not None else None

    def recent_price_marks(self, token_mint: str, *, since_received_at: str, limit: int = 2000) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.db.execute(
                "SELECT token_mint, observed_at, received_at, price_sol, source, source_ref FROM price_marks "
                "WHERE token_mint=? AND received_at>=? ORDER BY received_at, id LIMIT ?",
                (token_mint, since_received_at, int(limit)),
            ).fetchall()
        return [dict(row) for row in rows]

    def tracked_mints(self, *, as_of: datetime, horizon_seconds: float = 300.0, limit: int = 100) -> list[str]:
        cutoff = (as_of - timedelta(seconds=horizon_seconds)).isoformat()
        with self._lock:
            rows = self.db.execute(
                "SELECT token_mint, MAX(observed_at) AS last_touch FROM token_first_touches "
                "WHERE observed_at>=? GROUP BY token_mint ORDER BY last_touch DESC LIMIT ?",
                (cutoff, int(limit)),
            ).fetchall()
        return [str(row["token_mint"]) for row in rows]

    def evidence_counts(self) -> dict[str, int]:
        counts = super().evidence_counts()
        with self._lock:
            counts["risk_refresh_measurements"] = int(self.db.execute("SELECT COUNT(*) FROM risk_refresh_measurements").fetchone()[0])
            counts["price_marks"] = int(self.db.execute("SELECT COUNT(*) FROM price_marks").fetchone()[0])
            counts["program_coverage_observations"] = int(self.db.execute("SELECT COUNT(*) FROM program_coverage_observations").fetchone()[0])
        return counts
