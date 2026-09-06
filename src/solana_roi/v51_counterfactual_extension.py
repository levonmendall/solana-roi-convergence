from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from .robinhood_chain_core import MAX_HOLD_SECONDS
from .v51_evidence_analytics import ensure_counterfactual_schema, refresh_rejected_counterfactuals


EXTENSION_VERSION = "v51-rejected-counterfactual-robinhood-v3-forward-market-resolution"
RESOLUTION_BATCH = 128
REFERENCE_STALENESS_SECONDS = 120.0


def _utcnow_dt() -> datetime:
    return datetime.now(timezone.utc)


def _utcnow() -> str:
    return _utcnow_dt().isoformat()


def _parse_dt(value: Any) -> datetime | None:
    try:
        result = datetime.fromisoformat(str(value))
    except Exception:
        return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def _table_exists(store: Any, table: str) -> bool:
    try:
        with store._lock:
            return store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,)
            ).fetchone() is not None
    except Exception:
        return False


def _columns(store: Any, table: str) -> set[str]:
    if not _table_exists(store, table):
        return set()
    with store._lock:
        return {str(row["name"]) for row in store.db.execute(f"PRAGMA table_info({table})").fetchall()}


def _ensure_resolution_columns(store: Any) -> None:
    ensure_counterfactual_schema(store)
    wanted = {
        "forward_gross_return": "REAL",
        "resolution_semantics": "TEXT",
        "resolution_horizon_seconds": "REAL",
        "entry_reference_price": "REAL",
        "forward_reference_price": "REAL",
    }
    columns = _columns(store, "v51_rejected_counterfactuals")
    with store._lock, store.db:
        for name, sql_type in wanted.items():
            if name not in columns:
                store.db.execute(
                    f"ALTER TABLE v51_rejected_counterfactuals ADD COLUMN {name} {sql_type}"
                )


def _price_row(
    store: Any,
    *,
    release_commit: str,
    market: str,
    before_or_at: str,
    after: str | None = None,
) -> dict[str, Any] | None:
    if not _table_exists(store, "robinhood_swaps"):
        return None
    where = [
        "release_commit=?",
        "market=?",
        "price_eth IS NOT NULL",
        "price_eth>0",
        "observed_at<=?",
    ]
    values: list[Any] = [release_commit, market, before_or_at]
    if after is not None:
        where.append("observed_at>?")
        values.append(after)
    with store._lock:
        row = store.db.execute(
            "SELECT price_eth,observed_at,tx_hash,log_index FROM robinhood_swaps WHERE "
            + " AND ".join(where)
            + " ORDER BY observed_at DESC,id DESC LIMIT 1",
            tuple(values),
        ).fetchone()
    return dict(row) if row is not None else None


def _resolve_robinhood_forward_market_returns(store: Any, *, limit: int = RESOLUTION_BATCH) -> dict[str, int]:
    """Resolve rejected Robinhood opportunities at the strategy's max-hold horizon.

    This is research-only counterfactual measurement. It never creates a paper trial
    or entry.  The outcome is a gross observed market-price return, not executable PnL;
    exact execution proof remains a separate promotion/settlement requirement.
    """
    _ensure_resolution_columns(store)
    if not _table_exists(store, "v51_robinhood_candidate_ledger"):
        return {"examined": 0, "resolved_market_return": 0, "resolved_no_observation": 0}
    cutoff = _utcnow_dt() - timedelta(seconds=float(MAX_HOLD_SECONDS))
    with store._lock:
        rows = [dict(row) for row in store.db.execute(
            "SELECT c.candidate_id,c.release_commit,c.decision_observed_at,c.counterfactual_state,"
            "l.market,l.token,l.venue,l.lifecycle "
            "FROM v51_rejected_counterfactuals c "
            "JOIN v51_robinhood_candidate_ledger l ON l.candidate_id=c.candidate_id "
            "WHERE c.surface='ROBINHOOD_CHAIN' AND c.counterfactual_state='pending_forward_resolution' "
            "ORDER BY c.decision_observed_at LIMIT ?",
            (max(1, int(limit)),),
        ).fetchall()]
    resolved_market = 0
    resolved_none = 0
    examined = 0
    now = _utcnow()
    for row in rows:
        observed = _parse_dt(row.get("decision_observed_at"))
        if observed is None or observed > cutoff:
            continue
        examined += 1
        release = str(row.get("release_commit") or "")
        market = str(row.get("market") or "")
        if not release or not market:
            continue
        target = observed + timedelta(seconds=float(MAX_HOLD_SECONDS))
        entry = _price_row(
            store,
            release_commit=release,
            market=market,
            before_or_at=observed.isoformat(),
        )
        exit_row = _price_row(
            store,
            release_commit=release,
            market=market,
            before_or_at=target.isoformat(),
            after=observed.isoformat(),
        )
        if entry is None:
            # A missing decision-time reference is measurement debt, not a resolved
            # loss. Leave it pending rather than inventing a price.
            continue
        exit_at = _parse_dt(exit_row.get("observed_at")) if exit_row else None
        fresh_exit = bool(
            exit_row is not None
            and exit_at is not None
            and abs((target - exit_at).total_seconds()) <= REFERENCE_STALENESS_SECONDS
        )
        if fresh_exit:
            entry_price = float(entry["price_eth"])
            exit_price = float(exit_row["price_eth"])
            gross = exit_price / entry_price - 1.0
            with store._lock, store.db:
                store.db.execute(
                    "UPDATE v51_rejected_counterfactuals SET forward_gross_return=?,resolution_source=?,"
                    "counterfactual_state='resolved_forward_market_return',resolution_semantics=?,"
                    "resolution_horizon_seconds=?,entry_reference_price=?,forward_reference_price=?,updated_at=? "
                    "WHERE surface='ROBINHOOD_CHAIN' AND candidate_id=?",
                    (
                        gross,
                        "robinhood_swaps_observed_market_price",
                        "gross_market_price_return_research_only_not_executable_trade_pnl",
                        float(MAX_HOLD_SECONDS),
                        entry_price,
                        exit_price,
                        now,
                        row["candidate_id"],
                    ),
                )
            resolved_market += 1
        else:
            # If the market has no sufficiently fresh observation around the existing
            # max-hold horizon, that itself is a resolved research outcome: the missed
            # opportunity could not be evaluated from a continuing observed market.
            with store._lock, store.db:
                store.db.execute(
                    "UPDATE v51_rejected_counterfactuals SET resolution_source=?,"
                    "counterfactual_state='resolved_no_forward_market_observation',resolution_semantics=?,"
                    "resolution_horizon_seconds=?,entry_reference_price=?,updated_at=? "
                    "WHERE surface='ROBINHOOD_CHAIN' AND candidate_id=?",
                    (
                        "robinhood_swaps_no_fresh_horizon_observation",
                        "no_fresh_observed_market_price_within_120s_of_max_hold_horizon",
                        float(MAX_HOLD_SECONDS),
                        float(entry["price_eth"]),
                        now,
                        row["candidate_id"],
                    ),
                )
            resolved_none += 1
    return {
        "examined": examined,
        "resolved_market_return": resolved_market,
        "resolved_no_observation": resolved_none,
    }


def refresh_all_rejected_counterfactuals(store: Any) -> dict[str, Any]:
    """Register new rejects and resolve mature Robinhood counterfactuals incrementally."""
    base = refresh_rejected_counterfactuals(store)
    _ensure_resolution_columns(store)
    inserted = 0
    examined_new = 0
    if _table_exists(store, "v51_robinhood_candidate_ledger"):
        with store._lock:
            rows = [dict(row) for row in store.db.execute(
                "SELECT l.* FROM v51_robinhood_candidate_ledger l "
                "LEFT JOIN v51_rejected_counterfactuals c "
                "ON c.surface='ROBINHOOD_CHAIN' AND c.candidate_id=l.candidate_id "
                "WHERE l.decision='paper_reject' AND c.candidate_id IS NULL ORDER BY l.rowid"
            ).fetchall()]
        examined_new = len(rows)
        now = _utcnow()
        values: list[tuple[Any, ...]] = []
        for row in rows:
            candidate_id = str(row.get("candidate_id") or "")
            if not candidate_id:
                continue
            payload = {
                key: row.get(key)
                for key in ("market", "venue", "lifecycle", "selected_lane", "position_fraction")
                if key in row
            }
            values.append(
                (
                    candidate_id,
                    row.get("release_commit"),
                    row.get("token"),
                    str(row.get("decision_reason") or "robinhood_forward_reject"),
                    row.get("observed_at") or row.get("updated_at") or now,
                    json.dumps(payload, sort_keys=True, default=str),
                    now,
                )
            )
        if values:
            with store._lock, store.db:
                before = store.db.total_changes
                store.db.executemany(
                    "INSERT OR IGNORE INTO v51_rejected_counterfactuals("
                    "surface,candidate_id,release_commit,token_mint,decision_reason,decision_observed_at,"
                    "forward_net_return,resolution_source,counterfactual_state,hazard_signature,hazard_severity,"
                    "payload_json,updated_at,retrospective_entry_authority,paper_only,live_money_authority) "
                    "VALUES ('ROBINHOOD_CHAIN',?,?,?,?,?,NULL,NULL,'pending_forward_resolution',NULL,NULL,?,?,0,1,0)",
                    values,
                )
                inserted = max(0, store.db.total_changes - before)
    resolution = _resolve_robinhood_forward_market_returns(store)
    with store._lock:
        totals = store.db.execute(
            "SELECT COUNT(*) AS total,"
            "SUM(CASE WHEN counterfactual_state LIKE 'resolved_%' OR forward_net_return IS NOT NULL THEN 1 ELSE 0 END) AS resolved,"
            "SUM(CASE WHEN COALESCE(forward_net_return,forward_gross_return)>0 THEN 1 ELSE 0 END) AS positive,"
            "SUM(CASE WHEN counterfactual_state='resolved_forward_market_return' THEN 1 ELSE 0 END) AS gross_resolved,"
            "SUM(CASE WHEN counterfactual_state='resolved_no_forward_market_observation' THEN 1 ELSE 0 END) AS no_observation "
            "FROM v51_rejected_counterfactuals"
        ).fetchone()
    total = int(totals["total"] or 0) if totals else 0
    resolved = int(totals["resolved"] or 0) if totals else 0
    positive = int(totals["positive"] or 0) if totals else 0
    return {
        **base,
        "counterfactual_extension_version": EXTENSION_VERSION,
        "rejected_candidate_count": total,
        "resolved_count": resolved,
        "pending_count": max(0, total - resolved),
        "resolved_positive_count": positive,
        "resolved_gross_market_return_count": int(totals["gross_resolved"] or 0) if totals else 0,
        "resolved_no_forward_market_observation_count": int(totals["no_observation"] or 0) if totals else 0,
        "resolution_horizon_seconds": float(MAX_HOLD_SECONDS),
        "resolution_batch": RESOLUTION_BATCH,
        "latest_resolution_batch": resolution,
        "new_robinhood_rejections_examined": examined_new,
        "robinhood_rejections_materialized": inserted,
        "existing_counterfactual_rows_rewritten": False,
        "gross_market_return_has_promotion_authority": False,
        "retrospective_entry_authority": False,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "EXTENSION_VERSION",
    "REFERENCE_STALENESS_SECONDS",
    "RESOLUTION_BATCH",
    "_resolve_robinhood_forward_market_returns",
    "refresh_all_rejected_counterfactuals",
]
