from __future__ import annotations

from typing import Any

from . import v51_paper_lifecycle_runtime as lifecycle
from .v51_atomic_paper_capital import CANONICAL_PORTFOLIO_ID, ensure_atomic_capital_schema


REPAIR_VERSION = "v52-canonical-portfolio-restart-reconciliation-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_INSTALLED = False
_SETTLEMENT_SCAN_COUNT = 0
_CROSS_RELEASE_SETTLEMENT_COUNT = 0


def _table_exists(store: Any, table: str) -> bool:
    with store._lock:
        row = store.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table,),
        ).fetchone()
    return row is not None


def _active_outcomes(store: Any, *, table: str, reservation_prefix: str) -> list[dict[str, Any]]:
    if not _table_exists(store, table):
        return []
    ensure_atomic_capital_schema(store)
    with store._lock:
        rows = store.db.execute(
            f"SELECT o.source_signature,o.exit_signature,o.net_return,o.settled_at,o.release_commit AS outcome_release," 
            "r.release_commit AS reservation_release,r.reservation_id "
            f"FROM {table} o JOIN v51_paper_capital_reservations r "
            "ON r.portfolio_id=? AND r.status='active' AND r.reservation_id=(? || o.source_signature) "
            "ORDER BY o.id DESC LIMIT 4096",
            (CANONICAL_PORTFOLIO_ID, reservation_prefix),
        ).fetchall()
    # Preserve the pre-repair rule of using only the latest outcome for one source.
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in rows:
        item = dict(raw)
        signature = str(item.get("source_signature") or "")
        if not signature or signature in seen:
            continue
        seen.add(signature)
        result.append(item)
    return result


def sync_settlements_across_releases(adapter: Any) -> int:
    """Settle only still-active canonical positions, regardless of release lineage.

    A deploy/restart does not create a new portfolio. The reservation's original
    release remains immutable lineage, while a later runtime may observe the exit and
    close that same reservation. Settled historical rows are excluded by the active
    join, so repeated ticks are idempotent and bounded by currently open positions.
    """
    global _SETTLEMENT_SCAN_COUNT, _CROSS_RELEASE_SETTLEMENT_COUNT
    ensure_atomic_capital_schema(adapter.store)
    changed = 0
    current_release = str(getattr(adapter, "release_commit", "") or "")
    surfaces = (
        ("SOLANA", "risk_conditioned_alpha_v5_outcomes", "solana:"),
        ("FOMO", "fomo_paper_outcomes", "fomo:"),
    )
    for surface, table, prefix in surfaces:
        for row in _active_outcomes(adapter.store, table=table, reservation_prefix=prefix):
            did_settle = lifecycle._settle_one(
                adapter,
                surface=surface,
                source_signature=str(row["source_signature"]),
                exit_signature=str(row.get("exit_signature") or "paper-exit"),
                net_return=float(row["net_return"]),
                settled_at=str(row.get("settled_at") or "") or None,
            )
            changed += int(did_settle)
            if did_settle and str(row.get("reservation_release") or "") != current_release:
                _CROSS_RELEASE_SETTLEMENT_COUNT += 1
    _SETTLEMENT_SCAN_COUNT += 1
    # Preserve the module-level runtime counter used by existing operational status.
    lifecycle._SETTLEMENT_SYNC_COUNT += 1
    return changed


def install_v52_canonical_portfolio_restart_repair() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    lifecycle.sync_settlements = sync_settlements_across_releases  # type: ignore[assignment]
    setattr(lifecycle.sync_settlements, "_roi_v52_canonical_portfolio_restart", True)
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "portfolio_id": CANONICAL_PORTFOLIO_ID,
        "release_sha_is_capital_reset_boundary": False,
        "active_reservation_join_only": True,
        "settlement_scan_count": _SETTLEMENT_SCAN_COUNT,
        "cross_release_settlement_count": _CROSS_RELEASE_SETTLEMENT_COUNT,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = [
    "REPAIR_VERSION",
    "install_v52_canonical_portfolio_restart_repair",
    "status",
    "sync_settlements_across_releases",
]
