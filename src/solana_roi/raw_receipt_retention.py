from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any


# Launch-context reconstruction needs at most the first eight seconds and the
# exact-boundary publisher reads receipts immediately after commit.  Keep a
# deliberately wider two-minute floor after the wallet-discovery consumer has
# durably advanced; unresolved continuity gaps retain the configured expiry.
CONSUMED_RECEIPT_FLOOR_SECONDS = 120
DEFAULT_PRUNE_BATCH_ROWS = 20_000
RECENT_RECEIPT_LIVE_BYTE_BUDGET = 134_217_728


class RawReceiptCapacityBlocked(RuntimeError):
    """Protected receipt dependencies exhausted their physical byte budget."""


def recent_receipt_acknowledgement_boundary(
    connection: sqlite3.Connection,
    *,
    now: datetime,
) -> dict[str, Any]:
    """Return the exact SQL predicate for receipts durably safe to retire.

    The predicate uses ``r`` as the receipt-table alias. Time is only a safety
    floor: it never acknowledges work. In v5.2 normalized-consumer mode the
    hydration queue and metric must be terminal, and a normalized row must also
    be below the durable wallet-discovery cursor. Missing schema, interrupted
    writes, and continuity gaps therefore retain the raw receipt.
    """

    tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }
    safety_floor = (
        now - timedelta(seconds=CONSUMED_RECEIPT_FLOOR_SECONDS)
    ).isoformat()
    blockers: list[str] = []
    raw_cursor: int | None = None
    normalized_cursor: int | None = None
    normalized_consumer = False

    if "wallet_discovery_state" not in tables:
        blockers.append("wallet_discovery_state_missing")
    else:
        state_columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info('wallet_discovery_state')")
        }
        if "last_raw_receipt_id" not in state_columns:
            blockers.append("last_raw_receipt_id_missing")
        else:
            row = connection.execute(
                "SELECT last_raw_receipt_id FROM wallet_discovery_state WHERE id=1"
            ).fetchone()
            if row is None:
                blockers.append("wallet_discovery_state_row_missing")
            else:
                raw_cursor = max(0, int(row[0] or 0))
        normalized_consumer = "last_normalized_swap_id" in state_columns
        if normalized_consumer:
            row = connection.execute(
                "SELECT last_normalized_swap_id FROM wallet_discovery_state WHERE id=1"
            ).fetchone()
            if row is None:
                blockers.append("wallet_discovery_state_row_missing")
            else:
                normalized_cursor = max(0, int(row[0] or 0))

    unresolved_gap = False
    if "direct_solana_global_state" in tables:
        row = connection.execute(
            "SELECT unresolved_gap FROM direct_solana_global_state WHERE id=1"
        ).fetchone()
        if row is None:
            blockers.append("direct_solana_global_state_row_missing")
        else:
            unresolved_gap = bool(int(row[0] or 0))
    else:
        blockers.append("direct_solana_global_state_missing")
    if unresolved_gap:
        blockers.append("unresolved_continuity_gap")

    if normalized_consumer:
        required = {
            "direct_solana_hydration_queue",
            "direct_solana_hydration_metrics",
            "normalized_swaps",
        }
        blockers.extend(f"{name}_missing" for name in sorted(required - tables))
        if normalized_cursor is None:
            blockers.append("normalized_consumer_cursor_missing")
        if blockers:
            predicate = "0"
            args: tuple[Any, ...] = ()
        else:
            predicate = (
                "r.received_at<? "
                "AND EXISTS (SELECT 1 FROM direct_solana_hydration_queue q "
                "WHERE q.signature=r.signature AND q.status='complete') "
                "AND EXISTS (SELECT 1 FROM direct_solana_hydration_metrics m "
                "WHERE m.signature=r.signature AND (m.normalized=0 OR (m.normalized=1 "
                "AND EXISTS (SELECT 1 FROM normalized_swaps s "
                "WHERE s.signature=r.signature AND s.id<=?))))"
            )
            args = (safety_floor, normalized_cursor)
        mode = "normalized_swaps"
    else:
        if blockers or raw_cursor is None or unresolved_gap:
            predicate = "0"
            args = ()
        else:
            predicate = "r.id<=? AND r.received_at<?"
            args = (raw_cursor, safety_floor)
        mode = "raw_receipt_cursor"

    return {
        "eligible_predicate": predicate,
        "eligible_args": args,
        "consumer_cursor": raw_cursor,
        "normalized_consumer_cursor": normalized_cursor,
        "consumer_mode": mode,
        "unresolved_gap": unresolved_gap,
        "acknowledgement_blockers": sorted(set(blockers)),
        "consumed_floor_seconds": CONSUMED_RECEIPT_FLOOR_SECONDS,
    }


def _live_bytes(connection: sqlite3.Connection) -> int | None:
    try:
        indexes = [
            str(row[1])
            for row in connection.execute("PRAGMA index_list('direct_solana_recent_receipts')")
        ]
        names = ["direct_solana_recent_receipts", *indexes]
        placeholders = ",".join("?" for _ in names)
        row = connection.execute(
            f"SELECT COALESCE(SUM(pgsize),0) FROM dbstat WHERE name IN ({placeholders})",
            tuple(names),
        ).fetchone()
        return int(row[0] or 0) if row is not None else 0
    except sqlite3.Error:
        return None


def prune_recent_receipts(
    connection: sqlite3.Connection,
    *,
    now: datetime | None = None,
    batch_rows: int = DEFAULT_PRUNE_BATCH_ROWS,
) -> dict[str, Any]:
    """Retire raw receipt detail only after every durable dependency permits it.

    Age and configured expiry never acknowledge processing.  The applicable
    canonical consumer cursor, terminal hydration evidence, the launch/exact-
    frontier safety floor, and continuity state must all permit retirement.  A
    bounded id subquery prevents an unbounded rollback journal/WAL.
    """

    tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }
    if "direct_solana_recent_receipts" not in tables:
        return {"deleted": 0, "consumer_cursor": None, "unresolved_gap": False}

    instant = now or datetime.now(timezone.utc)
    boundary = recent_receipt_acknowledgement_boundary(connection, now=instant)
    limit = max(1, int(batch_rows))
    cursor_result = connection.execute(
        "DELETE FROM direct_solana_recent_receipts WHERE id IN ("
        "SELECT r.id FROM direct_solana_recent_receipts r WHERE "
        + str(boundary["eligible_predicate"])
        + " ORDER BY r.id LIMIT ?)",
        (*tuple(boundary["eligible_args"]), limit),
    )
    deleted = max(0, int(cursor_result.rowcount))
    live_bytes = _live_bytes(connection)
    over_budget = bool(
        live_bytes is not None and live_bytes > RECENT_RECEIPT_LIVE_BYTE_BUDGET
    )
    if over_budget and deleted == 0:
        raise RawReceiptCapacityBlocked(
            "raw receipt dependencies exceed physical byte budget:"
            f"live_bytes={live_bytes}:budget={RECENT_RECEIPT_LIVE_BYTE_BUDGET}:"
            f"consumer_cursor={boundary['consumer_cursor']}:"
            f"unresolved_gap={int(bool(boundary['unresolved_gap']))}:"
            f"acknowledgement_blockers={','.join(boundary['acknowledgement_blockers'])}"
        )
    return {
        "deleted": deleted,
        **{key: value for key, value in boundary.items() if not key.startswith("eligible_")},
        "batch_rows": limit,
        "live_bytes": live_bytes,
        "live_byte_budget": RECENT_RECEIPT_LIVE_BYTE_BUDGET,
        "over_live_byte_budget": over_budget,
    }


__all__ = [
    "CONSUMED_RECEIPT_FLOOR_SECONDS",
    "DEFAULT_PRUNE_BATCH_ROWS",
    "RECENT_RECEIPT_LIVE_BYTE_BUDGET",
    "RawReceiptCapacityBlocked",
    "prune_recent_receipts",
    "recent_receipt_acknowledgement_boundary",
]
