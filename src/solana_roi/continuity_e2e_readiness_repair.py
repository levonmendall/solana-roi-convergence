from __future__ import annotations

import os
from functools import wraps
from typing import Any, Callable

from . import direct_solana as direct_module
from . import unified_strategy_status as unified_status


REPAIR_VERSION = "continuity-e2e-readiness-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_ORIGINAL_CLOSE_OUTAGE: Callable[..., Any] | None = None
_ORIGINAL_CONNECTION_STATE: Callable[..., Any] | None = None
_ORIGINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_UNIFIED_STATUS: Callable[..., dict[str, Any]] | None = None


def _release_commit() -> str:
    for key in ("RENDER_GIT_COMMIT", "GITHUB_SHA", "SOLANA_ROI_RELEASE_COMMIT"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return "unbound-local-release"


def _ensure_epoch_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS direct_solana_release_continuity_epoch ("
            "release_commit TEXT PRIMARY KEY, started_at TEXT NOT NULL, "
            "inherited_orphaned_gap INTEGER NOT NULL, inherited_backfill_error TEXT, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
        )


def _close_outage_preserving_failed_boundary(
    self: Any,
    *,
    complete: bool,
    error: str | None = None,
) -> None:
    """Never erase the recovery boundary while the gap is unresolved."""
    if complete:
        if _ORIGINAL_CLOSE_OUTAGE is None:
            raise RuntimeError("continuity repair missing original close_outage")
        _ORIGINAL_CLOSE_OUTAGE(self, complete=True, error=error)
        return
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "UPDATE direct_solana_global_state SET unresolved_gap=1, "
            "last_backfill_complete_at=NULL, last_backfill_error=? WHERE id=1",
            (error,),
        )


def _start_release_epoch_if_safe(plane: Any) -> None:
    """Archive only a prior-process orphaned gap when a new release is live."""
    commit = _release_commit()
    if commit == "unbound-local-release":
        return
    store = plane.store
    _ensure_epoch_schema(store)
    now = direct_module.utcnow().isoformat()
    with store._lock, store.db:
        existing = store.db.execute(
            "SELECT release_commit FROM direct_solana_release_continuity_epoch WHERE release_commit=?",
            (commit,),
        ).fetchone()
        if existing is not None:
            return
        state = store.db.execute(
            "SELECT outage_started_at,unresolved_gap,last_backfill_error "
            "FROM direct_solana_global_state WHERE id=1"
        ).fetchone()
        unresolved = bool(state["unresolved_gap"]) if state is not None else False
        boundary = str(state["outage_started_at"] or "") if state is not None else ""
        inherited_error = str(state["last_backfill_error"] or "") if state is not None else ""
        inherited_orphan = bool(unresolved and not boundary)
        store.db.execute(
            "INSERT INTO direct_solana_release_continuity_epoch("
            "release_commit,started_at,inherited_orphaned_gap,inherited_backfill_error,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,1,0)",
            (commit, now, 1 if inherited_orphan else 0, inherited_error or None),
        )
        if inherited_orphan:
            store.db.execute(
                "UPDATE direct_solana_global_state SET unresolved_gap=0,last_backfill_error=NULL WHERE id=1"
            )
    if inherited_orphan:
        try:
            store.append(
                "direct_solana_release_continuity_epoch_started",
                now,
                {
                    "release_commit": commit,
                    "inherited_orphaned_gap": True,
                    "inherited_backfill_error": inherited_error or None,
                    "semantics": "new_release_forward_epoch_not_historical_gap_recovery",
                    "paper_only": True,
                    "live_money_authority": False,
                },
            )
        except Exception:
            pass


async def _connection_state_with_release_epoch(
    self: Any,
    provider: str,
    connected: bool,
    error_type: str | None = None,
) -> None:
    if _ORIGINAL_CONNECTION_STATE is None:
        raise RuntimeError("continuity repair missing original connection state")
    await _ORIGINAL_CONNECTION_STATE(self, provider, connected, error_type)
    if connected:
        _start_release_epoch_if_safe(self)


def _direct_status_with_release_epoch(self: Any) -> dict[str, Any]:
    if _ORIGINAL_DIRECT_STATUS is None:
        raise RuntimeError("continuity repair missing original direct status")
    payload = _ORIGINAL_DIRECT_STATUS(self)
    commit = _release_commit()
    _ensure_epoch_schema(self.store)
    with self.store._lock:
        row = self.store.db.execute(
            "SELECT started_at,inherited_orphaned_gap,inherited_backfill_error "
            "FROM direct_solana_release_continuity_epoch WHERE release_commit=?",
            (commit,),
        ).fetchone()
    payload["release_continuity_epoch"] = {
        "repair_version": REPAIR_VERSION,
        "release_commit": commit,
        "started": row is not None,
        "started_at": str(row["started_at"]) if row is not None else None,
        "inherited_orphaned_gap_archived": bool(row["inherited_orphaned_gap"]) if row is not None else False,
        "inherited_backfill_error": str(row["inherited_backfill_error"] or "") if row is not None else None,
        "failed_same_release_gap_boundary_preserved": True,
        "same_release_restart_cannot_clear_gap": True,
        "paper_only": True,
        "live_money_authority": False,
    }
    return payload


def _unified_status_with_strict_transport(
    base_status: dict[str, Any],
    runtime: Any,
    robinhood_status: dict[str, Any],
) -> dict[str, Any]:
    if _ORIGINAL_UNIFIED_STATUS is None:
        raise RuntimeError("continuity repair missing original unified status")
    payload = _ORIGINAL_UNIFIED_STATUS(base_status, runtime, robinhood_status)

    direct = base_status.get("direct_solana") if isinstance(base_status.get("direct_solana"), dict) else {}
    solana = payload.get("solana") if isinstance(payload.get("solana"), dict) else {}
    solana["transport_diagnostics"] = {
        "enabled": bool(direct.get("enabled")),
        "continuity_ok": bool(direct.get("continuity_ok")),
        "connected_provider_count": int(direct.get("connected_provider_count") or 0),
        "unresolved_gap": bool(direct.get("unresolved_gap")),
        "outage_started_at": direct.get("outage_started_at"),
        "last_backfill_complete_at": direct.get("last_backfill_complete_at"),
        "last_backfill_error": direct.get("last_backfill_error"),
        "release_continuity_epoch": direct.get("release_continuity_epoch"),
    }
    blockers = list(solana.get("blockers") or [])
    if bool(direct.get("enabled")) and int(direct.get("connected_provider_count") or 0) < 1:
        blockers.append("direct_solana_no_connected_provider")
    if bool(direct.get("unresolved_gap")):
        blockers.append("direct_solana_unresolved_gap")
    solana["blockers"] = list(dict.fromkeys(blockers))
    payload["solana"] = solana

    fomo = payload.get("fomo") if isinstance(payload.get("fomo"), dict) else {}
    fomo_blockers = list(fomo.get("blockers") or [])
    for blocker in solana["blockers"]:
        if blocker.startswith("direct_solana_"):
            fomo_blockers.append(blocker)
    fomo["blockers"] = list(dict.fromkeys(fomo_blockers))
    payload["fomo"] = fomo

    robinhood = payload.get("robinhood") if isinstance(payload.get("robinhood"), dict) else {}
    caught_up = bool(robinhood_status.get("caught_up_for_paper_decisions"))
    robinhood["paper_decision_transport_ready"] = bool(
        robinhood_status.get("runtime_ready")
        and caught_up
        and robinhood_status.get("paper_trading_authority", True)
        and not robinhood_status.get("failed_closed", False)
    )
    if not caught_up:
        rh_blockers = list(robinhood.get("blockers") or [])
        rh_blockers.append("robinhood_not_caught_up_for_paper_decisions")
        robinhood["blockers"] = list(dict.fromkeys(rh_blockers))
        regimes = robinhood.get("regimes") if isinstance(robinhood.get("regimes"), dict) else {}
        for regime in regimes.values():
            if not isinstance(regime, dict):
                continue
            regime["e2e_achievable"] = False
            regime_blockers = list(regime.get("blockers") or [])
            regime_blockers.append("robinhood_not_caught_up_for_paper_decisions")
            regime["blockers"] = list(dict.fromkeys(regime_blockers))
        robinhood["all_regimes_e2e_achievable"] = False
    payload["robinhood"] = robinhood

    overall = payload.get("overall") if isinstance(payload.get("overall"), dict) else {}
    overall["all_paper_planes_e2e_achievable"] = bool(
        payload.get("solana", {}).get("all_regimes_e2e_achievable")
        and payload.get("fomo", {}).get("all_regimes_e2e_achievable")
        and payload.get("robinhood", {}).get("all_regimes_e2e_achievable")
    )
    all_blockers = list(overall.get("blocking_components") or [])
    all_blockers.extend(payload.get("solana", {}).get("blockers") or [])
    all_blockers.extend(payload.get("fomo", {}).get("blockers") or [])
    all_blockers.extend(payload.get("robinhood", {}).get("blockers") or [])
    overall["blocking_components"] = list(dict.fromkeys(all_blockers))
    overall["continuity_e2e_readiness_repair"] = REPAIR_VERSION
    payload["overall"] = overall
    return payload


def install_continuity_e2e_readiness_repair() -> None:
    global _ORIGINAL_CLOSE_OUTAGE, _ORIGINAL_CONNECTION_STATE, _ORIGINAL_DIRECT_STATUS, _ORIGINAL_UNIFIED_STATUS

    current_close = direct_module.DirectSolanaJournal.close_outage
    if not bool(getattr(current_close, "_roi_continuity_e2e_repair", False)):
        _ORIGINAL_CLOSE_OUTAGE = current_close
        wrapped_close = wraps(current_close)(_close_outage_preserving_failed_boundary)
        setattr(wrapped_close, "_roi_continuity_e2e_repair", True)
        direct_module.DirectSolanaJournal.close_outage = wrapped_close  # type: ignore[method-assign]

    current_connection = direct_module.DirectSolanaIngestionPlane._connection_state
    if not bool(getattr(current_connection, "_roi_continuity_e2e_repair", False)):
        _ORIGINAL_CONNECTION_STATE = current_connection
        wrapped_connection = wraps(current_connection)(_connection_state_with_release_epoch)
        setattr(wrapped_connection, "_roi_continuity_e2e_repair", True)
        direct_module.DirectSolanaIngestionPlane._connection_state = wrapped_connection  # type: ignore[method-assign]

    current_direct_status = direct_module.DirectSolanaIngestionPlane.status
    if not bool(getattr(current_direct_status, "_roi_continuity_e2e_repair", False)):
        _ORIGINAL_DIRECT_STATUS = current_direct_status
        wrapped_direct_status = wraps(current_direct_status)(_direct_status_with_release_epoch)
        setattr(wrapped_direct_status, "_roi_continuity_e2e_repair", True)
        direct_module.DirectSolanaIngestionPlane.status = wrapped_direct_status  # type: ignore[method-assign]

    current_unified = unified_status.build_unified_strategy_status
    if not bool(getattr(current_unified, "_roi_continuity_e2e_repair", False)):
        _ORIGINAL_UNIFIED_STATUS = current_unified
        wrapped_unified = wraps(current_unified)(_unified_status_with_strict_transport)
        setattr(wrapped_unified, "_roi_continuity_e2e_repair", True)
        unified_status.build_unified_strategy_status = wrapped_unified


__all__ = ["REPAIR_VERSION", "install_continuity_e2e_readiness_repair"]
