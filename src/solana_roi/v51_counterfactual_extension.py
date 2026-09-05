from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .v51_evidence_analytics import ensure_counterfactual_schema, refresh_rejected_counterfactuals


EXTENSION_VERSION = "v51-rejected-counterfactual-robinhood-v2-incremental"


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _table_exists(store: Any, table: str) -> bool:
    try:
        with store._lock:
            return store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,)
            ).fetchone() is not None
    except Exception:
        return False


def refresh_all_rejected_counterfactuals(store: Any) -> dict[str, Any]:
    """Incrementally register newly rejected Robinhood candidates for future resolution.

    Existing counterfactual rows are deliberately not rewritten on every proof refresh.
    Besides avoiding O(N) repeated SQLite work, this preserves any forward resolution
    already attached to a rejected candidate instead of touching its audit timestamp.
    """
    base = refresh_rejected_counterfactuals(store)
    ensure_counterfactual_schema(store)
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
    with store._lock:
        totals = store.db.execute(
            "SELECT COUNT(*) AS total,SUM(CASE WHEN forward_net_return IS NOT NULL THEN 1 ELSE 0 END) AS resolved,"
            "SUM(CASE WHEN forward_net_return>0 THEN 1 ELSE 0 END) AS positive "
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
        "new_robinhood_rejections_examined": examined_new,
        "robinhood_rejections_materialized": inserted,
        "existing_counterfactual_rows_rewritten": False,
        "retrospective_entry_authority": False,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = ["EXTENSION_VERSION", "refresh_all_rejected_counterfactuals"]
