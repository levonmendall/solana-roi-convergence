from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .v51_evidence_analytics import ensure_counterfactual_schema, refresh_rejected_counterfactuals


EXTENSION_VERSION = "v51-rejected-counterfactual-robinhood-v1"


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
    base = refresh_rejected_counterfactuals(store)
    ensure_counterfactual_schema(store)
    inserted = 0
    if _table_exists(store, "v51_robinhood_candidate_ledger"):
        with store._lock:
            rows = [dict(row) for row in store.db.execute(
                "SELECT * FROM v51_robinhood_candidate_ledger WHERE decision='paper_reject' ORDER BY rowid"
            ).fetchall()]
        now = _utcnow()
        for row in rows:
            candidate_id = str(row.get("candidate_id") or "")
            if not candidate_id:
                continue
            payload = {
                key: row.get(key)
                for key in ("market", "venue", "lifecycle", "selected_lane", "position_fraction")
                if key in row
            }
            with store._lock, store.db:
                cursor = store.db.execute(
                    "INSERT INTO v51_rejected_counterfactuals(surface,candidate_id,release_commit,token_mint,decision_reason,"
                    "decision_observed_at,forward_net_return,resolution_source,counterfactual_state,hazard_signature,hazard_severity,"
                    "payload_json,updated_at,retrospective_entry_authority,paper_only,live_money_authority) "
                    "VALUES ('ROBINHOOD_CHAIN',?,?,?,?,?,NULL,NULL,'pending_forward_resolution',NULL,NULL,?,?,0,1,0) "
                    "ON CONFLICT(surface,candidate_id) DO UPDATE SET release_commit=excluded.release_commit,"
                    "token_mint=excluded.token_mint,decision_reason=excluded.decision_reason,"
                    "decision_observed_at=excluded.decision_observed_at,payload_json=excluded.payload_json,"
                    "updated_at=excluded.updated_at,retrospective_entry_authority=0,paper_only=1,live_money_authority=0",
                    (
                        candidate_id,
                        row.get("release_commit"),
                        row.get("token"),
                        str(row.get("decision_reason") or "robinhood_forward_reject"),
                        row.get("observed_at") or row.get("updated_at") or now,
                        json.dumps(payload, sort_keys=True, default=str),
                        now,
                    ),
                )
                inserted += int(cursor.rowcount > 0)
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
        "robinhood_rejections_materialized": inserted,
        "retrospective_entry_authority": False,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = ["EXTENSION_VERSION", "refresh_all_rejected_counterfactuals"]
