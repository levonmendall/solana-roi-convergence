from __future__ import annotations

from typing import Any, Callable

from . import continuation_market_recalibration as continuation
from . import fomo_paper_strategy as fomo_paper
from . import risk_conditioned_alpha_v51 as v51

REPAIR_VERSION = "v52-cross-lane-paper-certification-v4"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_INSTALLED = False
_ORIGINAL_RECORD_PAPER_TRIAL: Callable[[Any, str], bool] | None = None
_ORIGINAL_SET_V5_ROWS_OBSERVE: Callable[[Any, str, str], None] | None = None
_ORIGINAL_V52_BUILD_PRE_FROM_TRIALS: Callable[..., Any] | None = None


def _qualified_fomo_open_fraction(adapter: Any) -> float:
    """Return open FOMO exposure without relying on an ambiguous joined column."""
    continuation._ensure_fomo_schema(adapter)
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT t.position_fraction AS position_fraction FROM fomo_paper_trials t "
            "LEFT JOIN fomo_paper_outcomes o "
            "ON o.release_commit=t.release_commit AND o.source_signature=t.source_signature "
            "WHERE t.release_commit=? AND t.decision LIKE 'paper_enter%' AND o.id IS NULL",
            (adapter.release_commit,),
        ).fetchall()
    return min(1.0, sum(float(row["position_fraction"] or 0.0) for row in rows))


def _same_opportunity_solana_entry(adapter: Any, signature: str) -> bool:
    """Detect only the exact same authoritative SOLANA opportunity identity."""
    try:
        with adapter.store._lock:
            row = adapter.store.db.execute(
                "SELECT 1 FROM risk_conditioned_alpha_v5_trials "
                "WHERE release_commit=? AND source_signature=? AND selected=1 "
                "AND decision LIKE 'paper_enter%' LIMIT 1",
                (adapter.release_commit, signature),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _terminal_v5_rejection_exists(adapter: Any, signature: str) -> bool:
    """Return whether upstream strategy evaluation already issued a terminal reject.

    v5.1 exact-sizing reconciliation and v5.2 profit-confidence completion are both
    downstream of the strategy safety decision. They may refine sizing for an
    actionable candidate, but they must never erase an established ``reject_*``
    result or turn it back into a paper entry.
    """
    try:
        with adapter.store._lock:
            row = adapter.store.db.execute(
                "SELECT 1 FROM risk_conditioned_alpha_v5_trials "
                "WHERE release_commit=? AND source_signature=? "
                "AND decision LIKE 'reject_%' LIMIT 1",
                (adapter.release_commit, signature),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _set_v5_rows_observe_preserving_terminal_rejections(adapter: Any, signature: str, reason: str) -> None:
    if _ORIGINAL_SET_V5_ROWS_OBSERVE is None:
        raise RuntimeError("v52 v5.1 terminal-decision precedence repair not installed")
    if _terminal_v5_rejection_exists(adapter, signature):
        return
    _ORIGINAL_SET_V5_ROWS_OBSERVE(adapter, signature, reason)


def _build_pre_from_trials_preserving_terminal_rejections(adapter: Any, row: dict[str, Any]) -> Any:
    """Stop v5.2 profit-confidence recomposition at an upstream terminal reject."""
    if _ORIGINAL_V52_BUILD_PRE_FROM_TRIALS is None:
        raise RuntimeError("v52 profit-confidence terminal-decision precedence repair not installed")
    signature = str(row.get("signature") or "")
    if signature and _terminal_v5_rejection_exists(adapter, signature):
        return None
    return _ORIGINAL_V52_BUILD_PRE_FROM_TRIALS(adapter, row)


def _suppress_fomo_entry(
    adapter: Any,
    signature: str,
    *,
    decision: str,
    reason: str,
    event_type: str,
) -> bool:
    """Make a same-signature FOMO paper entry explicitly non-reservable."""
    with adapter.store._lock, adapter.store.db:
        row = adapter.store.db.execute(
            "SELECT id,token_mint,decision,decision_reason,position_fraction "
            "FROM fomo_paper_trials WHERE release_commit=? AND source_signature=? LIMIT 1",
            (adapter.release_commit, signature),
        ).fetchone()
        if row is None or not str(row["decision"] or "").startswith("paper_enter"):
            return False
        prior_decision = str(row["decision"] or "")
        prior_fraction = float(row["position_fraction"] or 0.0)
        adapter.store.db.execute(
            "UPDATE fomo_paper_trials SET decision=?,decision_reason=?,position_fraction=0.0 "
            "WHERE id=? AND decision LIKE 'paper_enter%'",
            (decision, reason, int(row["id"])),
        )
        token_mint = str(row["token_mint"] or "")

    try:
        adapter.store.append(
            event_type,
            continuation._utcnow(),
            {
                "source_signature": signature,
                "token_mint": token_mint,
                "suppressed_lane": "fomo",
                "authoritative_existing_surface": "SOLANA",
                "prior_fomo_decision": prior_decision,
                "prior_fomo_position_fraction": prior_fraction,
                "final_fomo_decision": decision,
                "reason": reason,
                "strategy_thresholds_changed": False,
                "qualification_changed": False,
                "paper_only": True,
                "live_money_authority": False,
            },
        )
    except Exception:
        pass
    return True


def _record_paper_trial_without_same_opportunity_double_allocation(adapter: Any, signature: str) -> bool:
    if _ORIGINAL_RECORD_PAPER_TRIAL is None:
        raise RuntimeError("v52 cross-lane paper certification repair not installed")

    inserted = bool(_ORIGINAL_RECORD_PAPER_TRIAL(adapter, signature))

    # A terminal authoritative rejection is stronger than any same-event advisory
    # FOMO preference. Keep independent market-flow FOMO untouched because those
    # rows use distinct market-flow signatures and have no matching v5 reject.
    if _terminal_v5_rejection_exists(adapter, signature):
        _suppress_fomo_entry(
            adapter,
            signature,
            decision="no_entry_terminal_authoritative_solana_rejection",
            reason="same_source_signature_has_terminal_authoritative_solana_rejection",
            event_type="v52_cross_lane_terminal_rejection",
        )
        return inserted

    if not _same_opportunity_solana_entry(adapter, signature):
        return inserted

    # The FOMO strategy layer may independently prefer the same source event after
    # the authoritative SOLANA lane already accepted it. Preserve that analytical
    # intent in the append-only event history, but make the durable paper-trial
    # disposition explicit and non-reservable so one economic opportunity cannot
    # consume portfolio capital twice.
    _suppress_fomo_entry(
        adapter,
        signature,
        decision="no_entry_duplicate_authoritative_solana_opportunity",
        reason="same_source_signature_already_has_authoritative_solana_paper_entry",
        event_type="v52_cross_lane_paper_dedup",
    )
    return inserted


def configure_v52_cross_lane_paper_certification_repair() -> None:
    global _INSTALLED, _ORIGINAL_RECORD_PAPER_TRIAL, _ORIGINAL_SET_V5_ROWS_OBSERVE
    global _ORIGINAL_V52_BUILD_PRE_FROM_TRIALS
    if _INSTALLED:
        return

    # Import lazily so the certification repair can be configured before the full
    # production composition without changing installer order.
    from . import v52_profit_confidence_completion as profit_completion

    continuation._fomo_open_fraction = _qualified_fomo_open_fraction
    _ORIGINAL_RECORD_PAPER_TRIAL = fomo_paper._record_paper_trial
    fomo_paper._record_paper_trial = _record_paper_trial_without_same_opportunity_double_allocation
    _ORIGINAL_SET_V5_ROWS_OBSERVE = v51._set_v5_rows_observe
    v51._set_v5_rows_observe = _set_v5_rows_observe_preserving_terminal_rejections
    _ORIGINAL_V52_BUILD_PRE_FROM_TRIALS = profit_completion._build_pre_from_trials
    profit_completion._build_pre_from_trials = _build_pre_from_trials_preserving_terminal_rejections
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "fomo_open_fraction_sql_qualified": True,
        "same_source_signature_cross_lane_double_allocation_blocked": True,
        "same_source_signature_terminal_rejection_blocks_fomo_reservation": True,
        "independent_market_flow_fomo_preserved": True,
        "v51_downstream_sizing_preserves_terminal_v5_rejections": True,
        "v52_profit_confidence_preserves_terminal_v5_rejections_before_requote": True,
        "mechanical_hard_stop_precedence_preserved": True,
        "strategy_thresholds_changed": False,
        "qualification_changed": False,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = [
    "REPAIR_VERSION",
    "configure_v52_cross_lane_paper_certification_repair",
    "status",
]
