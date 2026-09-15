from __future__ import annotations

import json
import math
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any


ATOMIC_CAPITAL_VERSION = "v51-atomic-paper-capital-v2-canonical-portfolio"
CANONICAL_PORTFOLIO_ID = "roi-convergence-paper-500-v1"
DEFAULT_CAPACITY_FRACTION = 1.0
BUSY_RETRY_LIMIT = 8
BUSY_RETRY_SLEEP_SECONDS = 0.01
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _finite_fraction(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(number):
        return 0.0
    return max(0.0, number)


def _columns(store: Any, table: str) -> set[str]:
    rows = store.db.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row["name"] if hasattr(row, "keys") else row[1]) for row in rows}


def ensure_atomic_capital_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_paper_capital_reservations ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, "
            "reservation_id TEXT NOT NULL, lane TEXT NOT NULL, candidate_id TEXT NOT NULL, "
            "requested_fraction REAL NOT NULL, reserved_fraction REAL NOT NULL, "
            "capacity_fraction REAL NOT NULL, status TEXT NOT NULL, reason TEXT NOT NULL, "
            "net_return REAL, realized_contribution REAL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "portfolio_id TEXT NOT NULL DEFAULT 'roi-convergence-paper-500-v1', "
            "UNIQUE(release_commit,reservation_id))"
        )
        if "portfolio_id" not in _columns(store, "v51_paper_capital_reservations"):
            store.db.execute(
                "ALTER TABLE v51_paper_capital_reservations ADD COLUMN portfolio_id TEXT NOT NULL "
                f"DEFAULT '{CANONICAL_PORTFOLIO_ID}'"
            )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_paper_capital_active "
            "ON v51_paper_capital_reservations(release_commit,status,id)"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_paper_capital_portfolio_active "
            "ON v51_paper_capital_reservations(portfolio_id,status,id)"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_paper_capital_portfolio_reservation "
            "ON v51_paper_capital_reservations(portfolio_id,reservation_id,id)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_paper_capital_settlements ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, "
            "settlement_id TEXT NOT NULL, reservation_id TEXT NOT NULL, net_return REAL NOT NULL, "
            "realized_contribution REAL NOT NULL, settled_at TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "portfolio_id TEXT NOT NULL DEFAULT 'roi-convergence-paper-500-v1', "
            "UNIQUE(release_commit,settlement_id), UNIQUE(release_commit,reservation_id))"
        )
        if "portfolio_id" not in _columns(store, "v51_paper_capital_settlements"):
            store.db.execute(
                "ALTER TABLE v51_paper_capital_settlements ADD COLUMN portfolio_id TEXT NOT NULL "
                f"DEFAULT '{CANONICAL_PORTFOLIO_ID}'"
            )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_paper_settlement_portfolio_reservation "
            "ON v51_paper_capital_settlements(portfolio_id,reservation_id,id)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_paper_lifecycle_events ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, "
            "candidate_id TEXT NOT NULL, event_key TEXT NOT NULL, stage TEXT NOT NULL, "
            "payload_json TEXT NOT NULL, created_at TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "UNIQUE(release_commit,candidate_id,event_key))"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_paper_lifecycle_candidate "
            "ON v51_paper_lifecycle_events(release_commit,candidate_id,id)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_paper_capital_metrics ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, "
            "operation TEXT NOT NULL, busy_retries INTEGER NOT NULL, write_latency_ms REAL NOT NULL, "
            "created_at TEXT NOT NULL)"
        )


def _begin_immediate(store: Any) -> tuple[int, float]:
    started = time.perf_counter()
    retries = 0
    while True:
        try:
            store.db.execute("BEGIN IMMEDIATE")
            return retries, started
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                raise
            retries += 1
            if retries > BUSY_RETRY_LIMIT:
                raise
            time.sleep(BUSY_RETRY_SLEEP_SECONDS * retries)


def _metric(store: Any, release_commit: str, operation: str, retries: int, started: float) -> None:
    store.db.execute(
        "INSERT INTO v51_paper_capital_metrics(release_commit,operation,busy_retries,write_latency_ms,created_at) "
        "VALUES (?,?,?,?,?)",
        (
            release_commit,
            operation,
            int(retries),
            max(0.0, (time.perf_counter() - started) * 1000.0),
            _utcnow(),
        ),
    )


def _canonical_reservations(store: Any, reservation_id: str) -> list[Any]:
    return store.db.execute(
        "SELECT * FROM v51_paper_capital_reservations "
        "WHERE portfolio_id=? AND reservation_id=? "
        "ORDER BY CASE status WHEN 'active' THEN 0 WHEN 'settled' THEN 1 WHEN 'cancelled' THEN 2 ELSE 3 END,id DESC",
        (CANONICAL_PORTFOLIO_ID, reservation_id),
    ).fetchall()


def reserve_paper_capital(
    store: Any,
    *,
    release_commit: str,
    reservation_id: str,
    lane: str,
    candidate_id: str,
    requested_fraction: float,
    capacity_fraction: float = DEFAULT_CAPACITY_FRACTION,
    allow_downsize: bool = True,
    minimum_fraction: float = 0.0,
) -> dict[str, Any]:
    """Atomically reserve one fraction of the single canonical paper portfolio.

    ``release_commit`` is immutable evidence lineage, not a capital reset boundary.
    Reservation identity and active buying power therefore survive deploy/restart
    boundaries. Replaying the same reservation id from a later release returns the
    original durable reservation rather than creating shadow capital.
    """
    ensure_atomic_capital_schema(store)
    requested = _finite_fraction(requested_fraction)
    capacity = _finite_fraction(capacity_fraction)
    minimum = _finite_fraction(minimum_fraction)
    if not reservation_id or not candidate_id or not lane:
        raise ValueError("reservation_id_lane_candidate_required")
    with store._lock:
        retries, started = _begin_immediate(store)
        try:
            existing_rows = _canonical_reservations(store, reservation_id)
            if existing_rows:
                active_rows = [row for row in existing_rows if str(row["status"]) == "active"]
                if len(active_rows) > 1:
                    store.db.rollback()
                    raise RuntimeError("paper_capital_duplicate_active_reservation_identity")
                existing = active_rows[0] if active_rows else existing_rows[0]
                _metric(store, release_commit, "reserve_replay", retries, started)
                store.db.commit()
                result = dict(existing)
                result["idempotent_replay"] = True
                result["busy_retries"] = retries
                result["replay_requested_release_commit"] = release_commit
                return result

            row = store.db.execute(
                "SELECT COALESCE(SUM(reserved_fraction),0) AS total "
                "FROM v51_paper_capital_reservations WHERE portfolio_id=? AND status='active'",
                (CANONICAL_PORTFOLIO_ID,),
            ).fetchone()
            active = max(0.0, float(row["total"] or 0.0)) if row is not None else 0.0
            available = max(0.0, capacity - active)
            reserved = min(requested, available) if allow_downsize else (requested if requested <= available else 0.0)
            if reserved <= 0.0 or reserved + 1e-12 < minimum:
                reserved = 0.0
                status = "rejected"
                reason = "paper_capital_exhausted_or_below_minimum"
            elif reserved + 1e-12 < requested:
                status = "active"
                reason = "paper_capital_downsized"
            else:
                status = "active"
                reason = "paper_capital_reserved"
            now = _utcnow()
            store.db.execute(
                "INSERT INTO v51_paper_capital_reservations("
                "release_commit,reservation_id,lane,candidate_id,requested_fraction,reserved_fraction,"
                "capacity_fraction,status,reason,created_at,updated_at,paper_only,live_money_authority,portfolio_id"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,1,0,?)",
                (
                    release_commit,
                    reservation_id,
                    lane,
                    candidate_id,
                    requested,
                    reserved,
                    capacity,
                    status,
                    reason,
                    now,
                    now,
                    CANONICAL_PORTFOLIO_ID,
                ),
            )
            _metric(store, release_commit, "reserve", retries, started)
            store.db.commit()
            result = dict(
                store.db.execute(
                    "SELECT * FROM v51_paper_capital_reservations "
                    "WHERE portfolio_id=? AND release_commit=? AND reservation_id=?",
                    (CANONICAL_PORTFOLIO_ID, release_commit, reservation_id),
                ).fetchone()
            )
            result["idempotent_replay"] = False
            result["busy_retries"] = retries
            return result
        except Exception:
            if store.db.in_transaction:
                store.db.rollback()
            raise


def cancel_paper_capital(
    store: Any,
    *,
    release_commit: str,
    reservation_id: str,
    reason: str = "entry_not_persisted",
) -> bool:
    ensure_atomic_capital_schema(store)
    with store._lock:
        retries, started = _begin_immediate(store)
        try:
            rows = _canonical_reservations(store, reservation_id)
            active_rows = [row for row in rows if str(row["status"]) == "active"]
            if len(active_rows) > 1:
                store.db.rollback()
                raise RuntimeError("paper_capital_duplicate_active_reservation_identity")
            if not active_rows:
                _metric(store, release_commit, "cancel_replay", retries, started)
                store.db.commit()
                return False
            target = active_rows[0]
            cursor = store.db.execute(
                "UPDATE v51_paper_capital_reservations SET status='cancelled',reason=?,updated_at=? WHERE id=? AND status='active'",
                (reason, _utcnow(), int(target["id"])),
            )
            _metric(store, release_commit, "cancel", retries, started)
            store.db.commit()
            return int(cursor.rowcount or 0) == 1
        except Exception:
            if store.db.in_transaction:
                store.db.rollback()
            raise


def settle_paper_capital(
    store: Any,
    *,
    release_commit: str,
    reservation_id: str,
    settlement_id: str,
    net_return: float,
) -> dict[str, Any]:
    """Settle one canonical reservation exactly once across release boundaries."""
    ensure_atomic_capital_schema(store)
    value = float(net_return)
    if not math.isfinite(value):
        raise ValueError("finite_net_return_required")
    with store._lock:
        retries, started = _begin_immediate(store)
        try:
            rows = _canonical_reservations(store, reservation_id)
            active_rows = [row for row in rows if str(row["status"]) == "active"]
            if len(active_rows) > 1:
                store.db.rollback()
                raise RuntimeError("paper_capital_duplicate_active_reservation_identity")
            existing_settlements = store.db.execute(
                "SELECT * FROM v51_paper_capital_settlements "
                "WHERE portfolio_id=? AND reservation_id=? ORDER BY id DESC",
                (CANONICAL_PORTFOLIO_ID, reservation_id),
            ).fetchall()
            if not active_rows:
                if existing_settlements:
                    _metric(store, release_commit, "settle_replay", retries, started)
                    store.db.commit()
                    result = dict(existing_settlements[0])
                    result["idempotent_replay"] = True
                    result["busy_retries"] = retries
                    result["replay_requested_release_commit"] = release_commit
                    return result
                if not rows:
                    store.db.rollback()
                    raise KeyError("paper_capital_reservation_missing")
                store.db.rollback()
                raise RuntimeError(f"paper_capital_reservation_not_active:{rows[0]['status']}")
            if existing_settlements:
                store.db.rollback()
                raise RuntimeError("paper_capital_active_reservation_already_has_settlement")

            reservation = active_rows[0]
            origin_release = str(reservation["release_commit"])
            fraction = max(0.0, float(reservation["reserved_fraction"] or 0.0))
            contribution = fraction * value
            now = _utcnow()
            store.db.execute(
                "INSERT INTO v51_paper_capital_settlements("
                "release_commit,settlement_id,reservation_id,net_return,realized_contribution,settled_at,"
                "paper_only,live_money_authority,portfolio_id) VALUES (?,?,?,?,?,?,1,0,?)",
                (
                    origin_release,
                    settlement_id,
                    reservation_id,
                    value,
                    contribution,
                    now,
                    CANONICAL_PORTFOLIO_ID,
                ),
            )
            store.db.execute(
                "UPDATE v51_paper_capital_reservations SET status='settled',reason='paper_capital_settled',"
                "net_return=?,realized_contribution=?,updated_at=? WHERE id=? AND status='active'",
                (value, contribution, now, int(reservation["id"])),
            )
            _metric(store, release_commit, "settle", retries, started)
            store.db.commit()
            result = dict(
                store.db.execute(
                    "SELECT * FROM v51_paper_capital_settlements "
                    "WHERE portfolio_id=? AND reservation_id=? ORDER BY id DESC LIMIT 1",
                    (CANONICAL_PORTFOLIO_ID, reservation_id),
                ).fetchone()
            )
            result["idempotent_replay"] = False
            result["busy_retries"] = retries
            result["settled_by_release_commit"] = release_commit
            return result
        except Exception:
            if store.db.in_transaction:
                store.db.rollback()
            raise


def record_lifecycle_event(
    store: Any,
    *,
    release_commit: str,
    candidate_id: str,
    event_key: str,
    stage: str,
    payload: dict[str, Any] | None = None,
) -> bool:
    ensure_atomic_capital_schema(store)
    with store._lock, store.db:
        cursor = store.db.execute(
            "INSERT OR IGNORE INTO v51_paper_lifecycle_events("
            "release_commit,candidate_id,event_key,stage,payload_json,created_at,paper_only,live_money_authority"
            ") VALUES (?,?,?,?,?,?,1,0)",
            (
                release_commit,
                candidate_id,
                event_key,
                stage,
                json.dumps(payload or {}, sort_keys=True, separators=(",", ":")),
                _utcnow(),
            ),
        )
    return int(cursor.rowcount or 0) == 1


def lifecycle_events(store: Any, *, release_commit: str, candidate_id: str) -> list[dict[str, Any]]:
    ensure_atomic_capital_schema(store)
    with store._lock:
        rows = store.db.execute(
            "SELECT * FROM v51_paper_lifecycle_events WHERE release_commit=? AND candidate_id=? ORDER BY id",
            (release_commit, candidate_id),
        ).fetchall()
    return [dict(row) for row in rows]


def capital_reconciliation(
    store: Any,
    *,
    release_commit: str,
    capacity_fraction: float = DEFAULT_CAPACITY_FRACTION,
) -> dict[str, Any]:
    """Return canonical portfolio accounting while retaining reporter release lineage."""
    ensure_atomic_capital_schema(store)
    capacity = _finite_fraction(capacity_fraction)
    with store._lock:
        rows = store.db.execute(
            "SELECT status,COUNT(*) AS n,COALESCE(SUM(reserved_fraction),0) AS fraction "
            "FROM v51_paper_capital_reservations WHERE portfolio_id=? GROUP BY status",
            (CANONICAL_PORTFOLIO_ID,),
        ).fetchall()
        settlement = store.db.execute(
            "SELECT COUNT(*) AS n,COALESCE(SUM(realized_contribution),0) AS contribution "
            "FROM v51_paper_capital_settlements WHERE portfolio_id=?",
            (CANONICAL_PORTFOLIO_ID,),
        ).fetchone()
        active_release = store.db.execute(
            "SELECT COUNT(DISTINCT release_commit) AS n FROM v51_paper_capital_reservations "
            "WHERE portfolio_id=? AND status='active'",
            (CANONICAL_PORTFOLIO_ID,),
        ).fetchone()
        duplicate_active = store.db.execute(
            "SELECT COUNT(*) AS n FROM ("
            "SELECT reservation_id FROM v51_paper_capital_reservations "
            "WHERE portfolio_id=? AND status='active' GROUP BY reservation_id HAVING COUNT(*)>1)",
            (CANONICAL_PORTFOLIO_ID,),
        ).fetchone()
        metrics = store.db.execute(
            "SELECT COALESCE(SUM(busy_retries),0) AS retries,COALESCE(MAX(write_latency_ms),0) AS max_ms,"
            "COALESCE(AVG(write_latency_ms),0) AS avg_ms FROM v51_paper_capital_metrics WHERE release_commit=?",
            (release_commit,),
        ).fetchone()
    by_status = {
        str(row["status"]): {"count": int(row["n"]), "fraction": float(row["fraction"] or 0.0)}
        for row in rows
    }
    active = float(by_status.get("active", {}).get("fraction", 0.0))
    realized = float(settlement["contribution"] or 0.0) if settlement is not None else 0.0
    duplicate_count = int(duplicate_active["n"] or 0) if duplicate_active is not None else 0
    return {
        "version": ATOMIC_CAPITAL_VERSION,
        "release_commit": release_commit,
        "reporting_release_commit": release_commit,
        "portfolio_id": CANONICAL_PORTFOLIO_ID,
        "release_sha_is_capital_reset_boundary": False,
        "capacity_fraction": capacity,
        "active_reserved_fraction": active,
        "available_fraction": max(0.0, capacity - active),
        "reservation_status": by_status,
        "active_release_count": int(active_release["n"] or 0) if active_release is not None else 0,
        "duplicate_active_reservation_identity_count": duplicate_count,
        "settlement_count": int(settlement["n"] or 0) if settlement is not None else 0,
        "realized_return_contribution": realized,
        "paper_nav_multiplier": 1.0 + realized,
        "capital_conserved": bool(active <= capacity + 1e-12 and duplicate_count == 0),
        "sqlite_busy_retries": int(metrics["retries"] or 0) if metrics is not None else 0,
        "max_write_latency_ms": float(metrics["max_ms"] or 0.0) if metrics is not None else 0.0,
        "avg_write_latency_ms": float(metrics["avg_ms"] or 0.0) if metrics is not None else 0.0,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "ATOMIC_CAPITAL_VERSION",
    "CANONICAL_PORTFOLIO_ID",
    "DEFAULT_CAPACITY_FRACTION",
    "cancel_paper_capital",
    "capital_reconciliation",
    "ensure_atomic_capital_schema",
    "lifecycle_events",
    "record_lifecycle_event",
    "reserve_paper_capital",
    "settle_paper_capital",
]
