from __future__ import annotations

from functools import wraps
from typing import Any, Awaitable, Callable

from .v51_candidate_ledger import record_stage_event as _record_stage
from .v51_robinhood_consolidation import _candidate_id, _upsert_ledger

COVERAGE_VERSION = "v51-robinhood-prelane-coverage-v2-append-only-stages"
_INSTALLED = False


def _ledger_row(self: Any, candidate_id: str) -> Any | None:
    try:
        with self.store._lock:
            return self.store.db.execute(
                "SELECT decision,decision_reason,trial_id FROM v51_robinhood_candidate_ledger "
                "WHERE candidate_id=? LIMIT 1",
                (candidate_id,),
            ).fetchone()
    except Exception:
        return None


def _trial_ids(self: Any, token: str, market: str) -> set[int]:
    try:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT id FROM robinhood_paper_trials WHERE release_commit=? AND token=? AND market=? ORDER BY id",
                (self.release_commit, token, market),
            ).fetchall()
        return {int(row["id"]) for row in rows}
    except Exception:
        return set()


def _reconcile_durable_entry(
    self: Any,
    *,
    candidate: str,
    release: str | None,
    trace: dict[str, Any],
    new_trial_ids: set[int],
) -> bool:
    """Make the final durable paper action authoritative over provisional rejects."""
    if not new_trial_ids:
        return False
    trial_id = max(new_trial_ids)
    with self.store._lock:
        trial_row = self.store.db.execute(
            "SELECT * FROM robinhood_paper_trials WHERE id=? LIMIT 1", (trial_id,)
        ).fetchone()
        context_row = self.store.db.execute(
            "SELECT * FROM robinhood_v5_trial_context WHERE trial_id=? LIMIT 1", (trial_id,)
        ).fetchone()
    if trial_row is None:
        return False
    trial = dict(trial_row)
    context = dict(context_row) if context_row is not None else {}
    trace["selected_lane"] = str(context.get("lane") or trial.get("context_state") or "") or None
    trace["position_fraction"] = float(trial.get("position_fraction") or 0.0)
    reason = str(trial.get("decision_reason") or "canonical_v51_paper_entry")
    _record_stage(self.store, surface="ROBINHOOD_CHAIN", candidate_id=candidate, release_commit=release,
                  stage="context", status="complete", reason="durable_v51_trial_context_persisted",
                  payload={"trial_id": trial_id, "lane": context.get("lane"), "regime": context.get("regime"),
                           "flow_state": context.get("flow_state"), "risk_signature": context.get("risk_signature"),
                           "risk_severity": context.get("risk_severity")})
    _record_stage(self.store, surface="ROBINHOOD_CHAIN", candidate_id=candidate, release_commit=release,
                  stage="execution_evidence", status="complete",
                  reason="durable_trial_confirms_exact_entry_and_exit_evidence_passed",
                  payload={"trial_id": trial_id, "round_trip_cost_fraction": trial.get("entry_round_trip_cost_fraction")})
    _record_stage(self.store, surface="ROBINHOOD_CHAIN", candidate_id=candidate, release_commit=release,
                  stage="decision", status="complete", reason=reason,
                  payload={"decision": "paper_enter", "trial_id": trial_id, "lane": context.get("lane"),
                           "position_fraction": trial.get("position_fraction"), "durable_action_is_authoritative": True})
    _record_stage(self.store, surface="ROBINHOOD_CHAIN", candidate_id=candidate, release_commit=release,
                  stage="position", status="paper_position_authorized", reason="durable_robinhood_paper_trial_created",
                  payload={"trial_id": trial_id})
    _upsert_ledger(self, trace, decision="paper_enter", reason=reason, trial_id=trial_id)
    return True


def _local_preselection_reason(self: Any, *, kind: str, token: str, subject: Any,
                               current_block: int | None) -> tuple[str, str]:
    if not bool(getattr(self, "_caught_up", False)):
        return "runtime_not_ready_for_paper_decision", "exact_local_state"
    try:
        if bool(self._token_open(token)):
            return "token_already_has_open_paper_position", "exact_local_state"
    except Exception:
        pass
    if kind == "v3":
        restrictions = getattr(subject, "restrictions_end_block", None)
        if restrictions is not None and current_block is not None and int(current_block) <= int(restrictions):
            return "launch_protection_window_active", "exact_local_state"
    return "preselection_policy_or_evidence_failed_closed_before_lane", "coarse_no_duplicate_rpc"


def _trace_seed(kind: str, subject: Any) -> tuple[str, str, str, str, Any]:
    if kind == "v3":
        token, market, venue = str(subject.token), str(subject.pool), str(subject.venue)
        lifecycle = "post_protection_v3" if venue == "PONS_V1_UNISWAP_V3" else "new_weth_pool"
    else:
        token, market, venue, lifecycle = str(subject.token), str(subject.curve), "PONS_V2_CURVE", "bonding_curve"
    return token, market, venue, lifecycle, subject.recent_swaps


async def _observe_and_delegate(self: Any, *, original: Callable[..., Awaitable[Any]], kind: str,
                                subject: Any, current_block: int | None = None) -> None:
    token, market, venue, lifecycle, recent = _trace_seed(kind, subject)
    candidate = _candidate_id(token, market, recent)
    trace = {"candidate_id": candidate, "token": token, "market": market, "venue": venue,
             "lifecycle": lifecycle, "selected_lane": None, "position_fraction": 0.0}
    release = str(getattr(self, "release_commit", "") or "") or None
    before_trials = _trial_ids(self, token, market)
    _record_stage(self.store, surface="ROBINHOOD_CHAIN", candidate_id=candidate, release_commit=release,
                  stage="ingestion", status="complete", reason="forward_opportunity_delivered_to_canonical_preselection",
                  payload={"token": token, "market": market, "venue": venue, "lifecycle": lifecycle})
    _record_stage(self.store, surface="ROBINHOOD_CHAIN", candidate_id=candidate, release_commit=release,
                  stage="candidate", status="complete", reason="pre_lane_candidate_registered_before_any_strategy_early_return",
                  payload={"coverage_version": COVERAGE_VERSION})
    try:
        if kind == "v3":
            await original(self, subject, current_block=int(current_block or 0))
        else:
            await original(self, subject)
    except Exception as exc:
        reason = f"preselection_exception_failed_closed:{type(exc).__name__}"
        _record_stage(self.store, surface="ROBINHOOD_CHAIN", candidate_id=candidate, release_commit=release,
                      stage="context", status="failed_closed", reason=reason,
                      payload={"attribution_precision": "exact_exception_type"})
        _record_stage(self.store, surface="ROBINHOOD_CHAIN", candidate_id=candidate, release_commit=release,
                      stage="decision", status="complete", reason=reason, payload={"decision": "paper_reject"})
        _record_stage(self.store, surface="ROBINHOOD_CHAIN", candidate_id=candidate, release_commit=release,
                      stage="position", status="not_opened", reason=reason)
        _upsert_ledger(self, trace, decision="paper_reject", reason=reason)
        raise
    after_trials = _trial_ids(self, token, market)
    if _reconcile_durable_entry(self, candidate=candidate, release=release, trace=trace,
                                new_trial_ids=after_trials - before_trials):
        return
    if _ledger_row(self, candidate) is not None:
        return
    reason, precision = _local_preselection_reason(self, kind=kind, token=token, subject=subject,
                                                   current_block=current_block)
    _record_stage(self.store, surface="ROBINHOOD_CHAIN", candidate_id=candidate, release_commit=release,
                  stage="context", status="failed_closed", reason=reason,
                  payload={"attribution_precision": precision, "duplicate_provider_work_used": False})
    _record_stage(self.store, surface="ROBINHOOD_CHAIN", candidate_id=candidate, release_commit=release,
                  stage="execution_evidence", status="not_requested", reason=reason)
    _record_stage(self.store, surface="ROBINHOOD_CHAIN", candidate_id=candidate, release_commit=release,
                  stage="decision", status="complete", reason=reason,
                  payload={"decision": "paper_reject", "attribution_precision": precision})
    _record_stage(self.store, surface="ROBINHOOD_CHAIN", candidate_id=candidate, release_commit=release,
                  stage="position", status="not_opened", reason=reason)
    _upsert_ledger(self, trace, decision="paper_reject", reason=reason)


def _wrap_v3(original: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(self: Any, pool: Any, *, current_block: int) -> None:
        await _observe_and_delegate(self, original=original, kind="v3", subject=pool, current_block=current_block)
    setattr(wrapped, "_roi_v51_prelane_coverage", True)
    return wrapped


def _wrap_v2(original: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(self: Any, curve: Any) -> None:
        await _observe_and_delegate(self, original=original, kind="v2", subject=curve)
    setattr(wrapped, "_roi_v51_prelane_coverage", True)
    return wrapped


def install_v51_robinhood_candidate_coverage(plane_cls: type[Any]) -> None:
    global _INSTALLED
    current_v2, current_v3 = plane_cls._maybe_open_v2, plane_cls._maybe_open_v3
    if not bool(getattr(current_v2, "_roi_v51_prelane_coverage", False)):
        plane_cls._maybe_open_v2 = _wrap_v2(current_v2)
    if not bool(getattr(current_v3, "_roi_v51_prelane_coverage", False)):
        plane_cls._maybe_open_v3 = _wrap_v3(current_v3)
    _INSTALLED = True


__all__ = ["COVERAGE_VERSION", "install_v51_robinhood_candidate_coverage"]
