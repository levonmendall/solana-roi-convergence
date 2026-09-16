from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .raw_receipt_retention import prune_recent_receipts


def prune_active_compatibility_database(
    path: Path | str,
    *,
    now: datetime | None = None,
) -> dict[str, int]:
    """Prune only evidence/transport whose runtime dependency is explicitly bounded.

    This is active-store maintenance, not a legacy purge. It never deletes
    pending/processing transport, paper/cohort authority, certification epochs,
    current provider/gap state, wallet-forward runtime epochs, or event lineage.
    It also preserves the exact latest-sample surfaces consumed by certification
    gates and any old normalized swap still required to prove first-touch
    chronology conflicts.
    """
    instant = now or datetime.now(timezone.utc)
    cutoff31 = (instant - timedelta(days=31)).isoformat()
    cutoff7 = (instant - timedelta(days=7)).isoformat()
    now_iso = instant.isoformat()
    connection = sqlite3.connect(Path(path), timeout=30.0)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        deleted: dict[str, int] = {}

        def columns(table: str) -> set[str]:
            if table not in tables:
                return set()
            return {
                str(row[1])
                for row in connection.execute(f'PRAGMA table_info("{table}")').fetchall()
            }

        def run(table: str, sql: str, args: tuple[Any, ...]) -> None:
            if table not in tables:
                return
            cursor = connection.execute(sql, args)
            deleted[table] = deleted.get(table, 0) + max(0, int(cursor.rowcount))

        # Durable transport: unresolved work always survives. Current Helius
        # materializes terminal time in updated_at; older supported databases
        # used completed_at. Detect the source schema rather than assuming one
        # generation, and never delete a row unless it is explicitly complete.
        webhook_columns = columns("helius_webhook_inbox")
        if "updated_at" in webhook_columns:
            run(
                "helius_webhook_inbox",
                "DELETE FROM helius_webhook_inbox WHERE state='complete' AND updated_at IS NOT NULL AND updated_at<?",
                (cutoff7,),
            )
        elif "completed_at" in webhook_columns:
            run(
                "helius_webhook_inbox",
                "DELETE FROM helius_webhook_inbox WHERE state='complete' AND completed_at IS NOT NULL AND completed_at<?",
                (cutoff7,),
            )
        run(
            "direct_solana_hydration_queue",
            "DELETE FROM direct_solana_hydration_queue WHERE status NOT IN ('pending','processing') "
            "AND updated_at<? AND NOT EXISTS (SELECT 1 FROM direct_solana_recent_receipts r "
            "WHERE r.signature=direct_solana_hydration_queue.signature)",
            (cutoff7,),
        )
        run(
            "wallet_realtime_receipts",
            "DELETE FROM wallet_realtime_receipts WHERE status NOT IN ('pending','processing') AND updated_at<?",
            (cutoff7,),
        )

        # Direct-Solana raw transport and operational measurements.
        if "direct_solana_recent_receipts" in tables:
            receipt_result = prune_recent_receipts(connection, now=instant)
            deleted["direct_solana_recent_receipts"] = int(receipt_result["deleted"])
        run(
            "direct_solana_minute_receipts",
            "DELETE FROM direct_solana_minute_receipts WHERE bucket<?",
            (cutoff31,),
        )
        run(
            "direct_solana_hydration_metrics",
            "DELETE FROM direct_solana_hydration_metrics WHERE hydrated_at<? "
            "AND NOT EXISTS (SELECT 1 FROM direct_solana_recent_receipts r "
            "WHERE r.signature=direct_solana_hydration_metrics.signature)",
            (cutoff31,),
        )

        # Wallet discovery and forward-alpha source evidence. Source observations
        # go first; only then may stale de-dup markers with no retained source row
        # disappear. This prevents historical observations from being replayed.
        run(
            "wallet_discovery_forward_observations",
            "DELETE FROM wallet_discovery_forward_observations WHERE received_at<?",
            (cutoff31,),
        )
        if "v52_wallet_forward_integrity_seen" in tables:
            if "wallet_discovery_forward_observations" in tables:
                run(
                    "v52_wallet_forward_integrity_seen",
                    "DELETE FROM v52_wallet_forward_integrity_seen WHERE recorded_at<? AND NOT EXISTS ("
                    "SELECT 1 FROM wallet_discovery_forward_observations o "
                    "WHERE o.signature=v52_wallet_forward_integrity_seen.signature)",
                    (cutoff31,),
                )
            else:
                run(
                    "v52_wallet_forward_integrity_seen",
                    "DELETE FROM v52_wallet_forward_integrity_seen WHERE recorded_at<?",
                    (cutoff31,),
                )
        run(
            "wallet_discovery_broad_samples",
            "DELETE FROM wallet_discovery_broad_samples WHERE received_at<?",
            (cutoff31,),
        )
        run(
            "wallet_discovery_candidates",
            "DELETE FROM wallet_discovery_candidates WHERE state='screen_rejected' AND last_seen_at<?",
            (cutoff31,),
        )

        # Semantic-candidate persistence is watch-state only and explicitly has
        # entry_authority=0. Its immediate deadline is seconds, so a 31-day hot
        # window is deliberately conservative while still bounding unique-token
        # accumulation.
        run(
            "semantic_candidate_events",
            "DELETE FROM semantic_candidate_events WHERE received_at<?",
            (cutoff31,),
        )
        run(
            "semantic_candidate_opportunities",
            "DELETE FROM semantic_candidate_opportunities WHERE last_seen<?",
            (cutoff31,),
        )
        run(
            "semantic_candidate_risk_state",
            "DELETE FROM semantic_candidate_risk_state WHERE assessed_at<?",
            (cutoff31,),
        )

        # Prospective certification/research windows. Keep the exact latest-500
        # surface used by the gates even if activity has been sparse for >31d.
        run(
            "execution_quote_observations",
            "DELETE FROM execution_quote_observations WHERE received_at<? AND id NOT IN ("
            "SELECT id FROM execution_quote_observations ORDER BY id DESC LIMIT 500)",
            (cutoff31,),
        )
        run(
            "shadow_execution_observations",
            "DELETE FROM shadow_execution_observations WHERE completed_at<? AND id NOT IN ("
            "SELECT id FROM shadow_execution_observations ORDER BY id DESC LIMIT 500)",
            (cutoff31,),
        )
        run(
            "risk_refresh_measurements",
            "DELETE FROM risk_refresh_measurements WHERE completed_at<? AND id NOT IN ("
            "SELECT id FROM risk_refresh_measurements ORDER BY id DESC LIMIT 500)",
            (cutoff7,),
        )
        run(
            "program_coverage_observations",
            "DELETE FROM program_coverage_observations WHERE assessed_at<? AND id NOT IN ("
            "SELECT id FROM program_coverage_observations ORDER BY assessed_at DESC,id DESC LIMIT 500)",
            (cutoff31,),
        )

        # First-touch certification queries the historical existence of an earlier
        # eligible S/A buy. Preserve every row that proves such a conflict even
        # outside the hot window; all unrelated old swaps are bounded away.
        if {"normalized_swaps", "token_first_touches", "wallet_profiles"}.issubset(tables):
            run(
                "normalized_swaps",
                "DELETE FROM normalized_swaps WHERE received_at<? AND NOT EXISTS ("
                "SELECT 1 FROM token_first_touches t JOIN wallet_profiles w ON w.wallet=normalized_swaps.wallet "
                "WHERE t.token_mint=normalized_swaps.token_mint AND normalized_swaps.side='buy' "
                "AND w.historically_eligible=1 AND w.tier IN ('S','A') "
                "AND julianday(normalized_swaps.observed_at)<julianday(t.observed_at))",
                (cutoff31,),
            )

        # Keep current stale-vs-missing risk semantics by retaining the newest
        # evidence per token/dimension even if older than the hot window.
        run(
            "risk_evidence",
            "DELETE FROM risk_evidence WHERE received_at<? AND id NOT IN ("
            "SELECT MAX(id) FROM risk_evidence GROUP BY token_mint,dimension)",
            (cutoff31,),
        )

        # Preserve the latest relationship/mark per subject even when older than
        # the hot window so dormant current identity/valuation does not silently
        # vanish. Everything else outside the 31-day decision window is bounded.
        run(
            "entity_links",
            "DELETE FROM entity_links WHERE received_at<? AND id NOT IN ("
            "SELECT MAX(id) FROM entity_links GROUP BY wallet_a,wallet_b,relationship)",
            (cutoff31,),
        )
        run(
            "price_marks",
            "DELETE FROM price_marks WHERE received_at<? AND id NOT IN ("
            "SELECT MAX(id) FROM price_marks GROUP BY token_mint)",
            (cutoff31,),
        )
        run(
            "wallet_intelligence_snapshots",
            "DELETE FROM wallet_intelligence_snapshots WHERE observed_at<? AND id NOT IN ("
            "SELECT MAX(id) FROM wallet_intelligence_snapshots GROUP BY wallet)",
            (cutoff31,),
        )

        connection.commit()
        return deleted
    finally:
        connection.close()


__all__ = ["prune_active_compatibility_database"]
