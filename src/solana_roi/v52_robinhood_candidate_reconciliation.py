from __future__ import annotations

from typing import Any, Awaitable, Callable

from . import v52_robinhood_position_lifecycle as lifecycle
from .robinhood_chain_core import _clean_address
from .strategy_v52_authority import (
    AUTHORITY_ID,
    ECONOMIC_FREEZE_EPOCH,
    LIVE_MONEY_AUTHORITY,
    PAPER_ONLY,
    STRATEGY_VERSION,
)
from .v51_robinhood_candidate_coverage import _reconcile_durable_entry
from .v51_robinhood_consolidation import _candidate_id

RECONCILIATION_VERSION = "v52-robinhood-post-validation-candidate-reconciliation-1"
_INSTALLED = False
_BASE_VALIDATE: Callable[..., Awaitable[bool]] | None = None


def _latest_committed_trial_id(owner: Any, payload: dict[str, Any]) -> int | None:
    token = _clean_address(payload.get("token"))
    market = _clean_address(payload.get("market"))
    release = str(getattr(owner, "release_commit", "") or "")
    lane = str(payload.get("lane") or "")
    trigger_entity = _clean_address(payload.get("trigger_entity"))
    if not token or not market or not release or not lane or not trigger_entity:
        return None
    with owner.store._lock:
        row = owner.store.db.execute(
            "SELECT l.trial_id FROM v52_robinhood_position_lots l "
            "JOIN robinhood_paper_trials t ON t.id=l.trial_id "
            "JOIN robinhood_v5_trial_context c ON c.trial_id=t.id "
            "WHERE t.release_commit=? AND t.token=? AND t.market=? "
            "AND t.trigger_entity=? AND c.lane=? "
            "ORDER BY l.id DESC LIMIT 1",
            (release, token, market, trigger_entity, lane),
        ).fetchone()
    return int(row["trial_id"]) if row is not None else None


def _ledger_already_reconciled(owner: Any, candidate_id: str, trial_id: int) -> bool:
    try:
        with owner.store._lock:
            row = owner.store.db.execute(
                "SELECT decision,trial_id FROM v51_robinhood_candidate_ledger "
                "WHERE candidate_id=? LIMIT 1",
                (candidate_id,),
            ).fetchone()
    except Exception:
        return False
    return bool(
        row is not None
        and str(row["decision"] or "") == "paper_enter"
        and int(row["trial_id"] or 0) == int(trial_id)
    )


async def _validate_and_reconcile(
    owner: Any,
    payload: dict[str, Any],
    *,
    venue_object: Any,
) -> bool:
    """Reconcile the pre-lane ledger only after a validated v5.2 trial exists.

    The mature Robinhood candidate-coverage wrapper observes the original decision
    function before this lifecycle layer performs aggregate/stressed exit checks.
    It therefore cannot see a durable trial during its own after/before comparison.
    Keep that wrapper unchanged and reuse its existing durable-entry reconciliation
    immediately after the v5.2 lifecycle commits the trial.
    """
    if _BASE_VALIDATE is None:
        raise RuntimeError("v52 Robinhood candidate reconciliation base validator missing")

    committed = await _BASE_VALIDATE(owner, payload, venue_object=venue_object)
    if not committed:
        return False

    token = _clean_address(payload.get("token"))
    market = _clean_address(payload.get("market"))
    if not token or not market:
        raise RuntimeError("validated Robinhood lifecycle trial missing token or market")

    recent = getattr(venue_object, "recent_swaps", ())
    candidate = _candidate_id(token, market, recent)
    trial_id = _latest_committed_trial_id(owner, payload)
    if trial_id is None:
        raise RuntimeError("validated Robinhood lifecycle trial not recoverable for candidate reconciliation")
    if _ledger_already_reconciled(owner, candidate, trial_id):
        return True

    trace = {
        "candidate_id": candidate,
        "token": token,
        "market": market,
        "venue": str(payload.get("venue") or "UNKNOWN"),
        "lifecycle": str(payload.get("lifecycle") or "unknown"),
        "selected_lane": str(payload.get("lane") or "") or None,
        "position_fraction": float(payload.get("fraction") or 0.0),
    }
    release = str(getattr(owner, "release_commit", "") or "") or None
    reconciled = _reconcile_durable_entry(
        owner,
        candidate=candidate,
        release=release,
        trace=trace,
        new_trial_ids={trial_id},
    )
    if not reconciled:
        raise RuntimeError("validated Robinhood lifecycle trial failed candidate reconciliation")
    return True


def status() -> dict[str, Any]:
    return {
        "version": RECONCILIATION_VERSION,
        "installed": _INSTALLED,
        "authority_id": AUTHORITY_ID,
        "strategy_version": STRATEGY_VERSION,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "reconciliation_trigger": "successful_v52_lifecycle_commit_only",
        "changes_economic_decision": False,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def install_v52_robinhood_candidate_reconciliation() -> None:
    global _INSTALLED, _BASE_VALIDATE
    if _INSTALLED:
        return
    _BASE_VALIDATE = lifecycle._validate_and_commit
    lifecycle._validate_and_commit = _validate_and_reconcile
    setattr(lifecycle._validate_and_commit, "_roi_v52_candidate_reconciliation", True)
    _INSTALLED = True


__all__ = [
    "RECONCILIATION_VERSION",
    "install_v52_robinhood_candidate_reconciliation",
    "status",
]
