from __future__ import annotations

import json
import math
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any


ATOMIC_CAPITAL_VERSION = "v51-atomic-paper-capital-v1"
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
            "UNIQUE(release_commit,reservation_id))"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_paper_capital_active "
            "ON v51_paper_capital_reservations(release_commit,status,id)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_paper_capital_settlements ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, "
            "settlement_id TEXT NOT NULL, reservation_id TEXT NOT NULL, net_return REAL NOT NULL, "
            "realized_contribution REAL NOT NULL, settled_at TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "UNIQUE(release_commit,settlement_id), UNIQUE(release_commit,reservation_id))"
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
    """Atomically reserve one shared paper-capital fraction.

    Replaying the same reservation id is idempotent.  All lane identities share the
    same release-scoped active-reservation sum, so concurrent callers cannot
    double-spend the configured capacity.
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
            existing = store.db.execute(
                "SELECT * FROM v51_paper_capital_reservations WHERE release_commit=? AND reservation_id=?",
                (release_commit, reservation_id),
            ).fetchone()
            if existing is not None:
                _metric(store, release_commit, "reserve_replay", retries, started)
                store.db.commit()
                result = dict(existing)
                result["idempotent_replay"] = True
                result["busy_retries"] = retries
                return result

            row = store.db.execute(
                "SELECT COALESCE(SUM(reserved_fraction),0) AS total "
                "FROM v51_paper_capital_reservations WHERE release_commit=? AND status='active'",
                (release_commit,),
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
                "capacity_fraction,status,reason,created_at,updated_at,paper_only,live_money_authority"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,1,0)",
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
                ),
            )
            _metric(store, release_commit, "reserve", retries, started)
            store.db.commit()
            result = dict(
                store.db.execute(
                    "SELECT * FROM v51_paper_capital_reservations WHERE release_commit=? AND reservation_id=?",
                    (release_commit, reservation_id),
                ).fetchone()
            )
            result["idempotent_replay"] = False
            result["busy_retries"] = retries
            return result
        except Exception:
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
            cursor = store.db.execute(
                "UPDATE v51_paper_capital_reservations SET status='cancelled',reason=?,updated_at=? "
                "WHERE release_commit=? AND reservation_id=? AND status='active'",
                (reason, _utcnow(), release_commit, reservation_id),
            )
            _metric(store, release_commit, "cancel", retries, started)
            store.db.commit()
            return int(cursor.rowcount or 0) == 1
        except Exception:
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
    """Settle one reservation exactly once and release its active capacity."""
    ensure_atomic_capital_schema(store)
    value = float(net_return)
    if not math.isfinite(value):
        raise ValueError("finite_net_return_required")
    with store._lock:
        retries, started = _begin_immediate(store)
        try:
            existing = store.db.execute(
                "SELECT * FROM v51_paper_capital_settlements WHERE release_commit=? AND reservation_id=?",
                (release_commit, reservation_id),
            ).fetchone()
            if existing is not None:
                _metric(store, release_commit, "settle_replay", retries, started)
                store.db.commit()
                result = dict(existing)
                result["idempotent_replay"] = True
                result["busy_retries"] = retries
                return result
            reservation = store.db.execute(
                "SELECT * FROM v51_paper_capital_reservations WHERE release_commit=? AND reservation_id=?",
                (release_commit, reservation_id),
            ).fetchone()
            if reservation is None:
                store.db.rollback()
                raise KeyError("paper_capital_reservation_missing")
            if str(reservation["status"]) != "active":
                store.db.rollback()
                raise RuntimeError(f"paper_capital_reservation_not_active:{reservation['status']}")
            fraction = max(0.0, float(reservation["reserved_fraction"] or 0.0))
            contribution = fraction * value
            now = _utcnow()
            store.db.execute(
                "INSERT INTO v51_paper_capital_settlements("
                "release_commit,settlement_id,reservation_id,net_return,realized_contribution,settled_at,"
                "paper_only,live_money_authority) VALUES (?,?,?,?,?,?,1,0)",
                (release_commit, settlement_id, reservation_id, value, contribution, now),
            )
            store.db.execute(
                "UPDATE v51_paper_capital_reservations SET status='settled',reason='paper_capital_settled',"
                "net_return=?,realized_contribution=?,updated_at=? "
                "WHERE release_commit=? AND reservation_id=?",
                (value, contribution, now, release_commit, reservation_id),
            )
            _metric(store, release_commit, "settle", retries, started)
            store.db.commit()
            result = dict(
                store.db.execute(
                    "SELECT * FROM v51_paper_capital_settlements WHERE release_commit=? AND reservation_id=?",
                    (release_commit, reservation_id),
                ).fetchone()
            )
            result["idempotent_replay"] = False
            result["busy_retries"] = retries
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
    ensure_atomic_capital_schema(store)
    capacity = _finite_fraction(capacity_fraction)
    with store._lock:
        rows = store.db.execute(
            "SELECT status,COUNT(*) AS n,COALESCE(SUM(reserved_fraction),0) AS fraction "
            "FROM v51_paper_capital_reservations WHERE release_commit=? GROUP BY status",
            (release_commit,),
        ).fetchall()
        settlement = store.db.execute(
            "SELECT COUNT(*) AS n,COALESCE(SUM(realized_contribution),0) AS contribution "
            "FROM v51_paper_capital_settlements WHERE release_commit=?",
            (release_commit,),
        ).fetchone()
        metrics = store.db.execute(
            "SELECT COALESCE(SUM(busy_retries),0) AS retries,COALESCE(MAX(write_latency_ms),0) AS max_ms,"
            "COALESCE(AVG(write_latency_ms),0) AS avg_ms FROM v51_paper_capital_metrics WHERE release_commit=?",
            (release_commit,),
        ).fetchone()
    by_status = {str(row["status"]): {"count": int(row["n"]), "fraction": float(row["fraction"] or 0.0)} for row in rows}
    active = float(by_status.get("active", {}).get("fraction", 0.0))
    realized = float(settlement["contribution"] or 0.0) if settlement is not None else 0.0
    return {
        "version": ATOMIC_CAPITAL_VERSION,
        "release_commit": release_commit,
        "capacity_fraction": capacity,
        "active_reserved_fraction": active,
        "available_fraction": max(0.0, capacity - active),
        "reservation_status": by_status,
        "settlement_count": int(settlement["n"] or 0) if settlement is not None else 0,
        "realized_return_contribution": realized,
        "paper_nav_multiplier": 1.0 + realized,
        "capital_conserved": active <= capacity + 1e-12,
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
    "DEFAULT_CAPACITY_FRACTION",
    "cancel_paper_capital",
    "capital_reconciliation",
    "ensure_atomic_capital_schema",
    "lifecycle_events",
    "record_lifecycle_event",
    "reserve_paper_capital",
    "settle_paper_capital",
]
