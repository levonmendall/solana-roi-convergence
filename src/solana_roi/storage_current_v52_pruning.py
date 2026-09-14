from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def prune_current_v52_database(path: Path | str, *, now: datetime | None = None) -> dict[str, int]:
    """Prune only resolved/expired current-v5.2 evidence in dependency-safe order.

    Resume-critical wallet-forward runtime state is never deleted. Unresolved
    decisions, variants, horizons, ablations and their entry anchors survive
    regardless of age. Parent decision rows are removed before old outcome rows
    so outcome existence can prove resolution instead of being erased first.
    """
    instant = now or datetime.now(timezone.utc)
    cutoff31 = (instant - timedelta(days=31)).isoformat()
    cutoff7 = (instant - timedelta(days=7)).isoformat()
    connection = sqlite3.connect(Path(path), timeout=30.0)
    try:
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        deleted: dict[str, int] = {}

        def execute(table: str, sql: str, args: tuple[Any, ...]) -> None:
            if table not in tables:
                return
            cursor = connection.execute(sql, args)
            deleted[table] = max(0, int(cursor.rowcount))

        execute(
            "v52_wallet_forward_integrity_seen",
            "DELETE FROM v52_wallet_forward_integrity_seen WHERE recorded_at<?",
            (cutoff31,),
        )
        # Decision first: its matching old outcome is the proof that the decision
        # is resolved. Only after that parent has been evaluated may the old
        # outcome itself be removed.
        execute(
            "v52_wallet_forward_shadow_decisions",
            "DELETE FROM v52_wallet_forward_shadow_decisions WHERE observed_at<? AND EXISTS ("
            "SELECT 1 FROM v52_wallet_forward_shadow_outcomes o "
            "WHERE o.decision_id=v52_wallet_forward_shadow_decisions.id)",
            (cutoff31,),
        )
        execute(
            "v52_wallet_forward_shadow_outcomes",
            "DELETE FROM v52_wallet_forward_shadow_outcomes WHERE resolved_at<?",
            (cutoff31,),
        )
        if "v52_wallet_forward_replay_runs" in tables:
            cursor = connection.execute(
                "DELETE FROM v52_wallet_forward_replay_runs WHERE evaluated_at<? "
                "AND id<>(SELECT MAX(id) FROM v52_wallet_forward_replay_runs)",
                (cutoff31,),
            )
            deleted["v52_wallet_forward_replay_runs"] = max(0, int(cursor.rowcount))

        # Entries must be tested while unresolved variants still exist. Then the
        # resolved old variants and other resolved old evidence can be removed.
        execute(
            "v52_market_validation_shadow_entries",
            "DELETE FROM v52_market_validation_shadow_entries WHERE observed_at<? AND NOT EXISTS ("
            "SELECT 1 FROM v52_market_validation_shadow_variants v "
            "WHERE v.candidate_key=v52_market_validation_shadow_entries.candidate_key "
            "AND v.observed_at=v52_market_validation_shadow_entries.observed_at "
            "AND v.variant_id=v52_market_validation_shadow_entries.variant_id AND v.net_return IS NULL)",
            (cutoff31,),
        )
        execute(
            "v52_market_validation_shadow_variants",
            "DELETE FROM v52_market_validation_shadow_variants WHERE observed_at<? AND net_return IS NOT NULL",
            (cutoff31,),
        )
        execute(
            "v52_market_validation_lane_events",
            "DELETE FROM v52_market_validation_lane_events WHERE observed_at<? AND net_return IS NOT NULL",
            (cutoff31,),
        )
        execute(
            "v52_market_validation_continuation_horizons",
            "DELETE FROM v52_market_validation_continuation_horizons WHERE observed_at<? AND resolved_at IS NOT NULL",
            (cutoff31,),
        )
        execute(
            "v52_market_validation_component_ablation",
            "DELETE FROM v52_market_validation_component_ablation WHERE observed_at<? AND resolved_at IS NOT NULL",
            (cutoff31,),
        )
        execute(
            "v52_market_validation_completion_evaluations",
            "DELETE FROM v52_market_validation_completion_evaluations WHERE observed_at<?",
            (cutoff31,),
        )
        execute(
            "v52_market_validation_hardening_audit",
            "DELETE FROM v52_market_validation_hardening_audit WHERE observed_at<?",
            (cutoff7,),
        )
        connection.commit()
        return deleted
    finally:
        connection.close()


__all__ = ["prune_current_v52_database"]
