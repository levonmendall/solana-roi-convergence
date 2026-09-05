from __future__ import annotations

from functools import wraps
from typing import Any, Awaitable, Callable

from .v51_candidate_pipeline import _record as _record_stage
from .v51_robinhood_consolidation import _candidate_id, _upsert_ledger

COVERAGE_VERSION = "v51-robinhood-prelane-coverage-v1"
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


def _local_preselection_reason(
    self: Any,
    *,
    kind: str,
    token: str,
    subject: Any,
    current_block: int | None,
) -> tuple[str, str]:
    """Classify cheap fail-closed causes without issuing duplicate provider work."""
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
        token = str(subject.token)
        market = str(subject.pool)
        venue = str(subject.venue)
        lifecycle = "post_protection_v3" if venue == "PONS_V1_UNISWAP_V3" else "new_weth_pool"
    else:
        token = str(subject.token)
        market = str(subject.curve)
        venue = "PONS_V2_CURVE"
        lifecycle = "bonding_curve"
    return token, market, venue, lifecycle, subject.recent_swaps


async def _observe_and_delegate(
    self: Any,
    *,
    original: Callable[..., Awaitable[Any]],
    kind: str,
    subject: Any,
    current_block: int | None = None,
) -> None:
    token, market, venue, lifecycle, recent = _trace_seed(kind, subject)
    candidate = _candidate_id(token, market, recent)
    trace = {
        "candidate_id": candidate,
        "token": token,
        "market": market,
        "venue": venue,
        "lifecycle": lifecycle,
        "selected_lane": None,
        "position_fraction": 0.0,
    }
    release = str(getattr(self, "release_commit", "") or "") or None

    _record_stage(
        self.store,
        surface="ROBINHOOD_CHAIN",
        candidate_id=candidate,
        release_commit=release,
        stage="ingestion",
        status="complete",
        reason="forward_opportunity_delivered_to_canonical_preselection",
        payload={"token": token, "market": market, "venue": venue, "lifecycle": lifecycle},
    )
    _record_stage(
        self.store,
        surface="ROBINHOOD_CHAIN",
        candidate_id=candidate,
        release_commit=release,
        stage="candidate",
        status="complete",
        reason="pre_lane_candidate_registered_before_any_strategy_early_return",
        payload={"coverage_version": COVERAGE_VERSION},
    )

    try:
        if kind == "v3":
            await original(self, subject, current_block=int(current_block or 0))
        else:
            await original(self, subject)
    except Exception as exc:
        reason = f"preselection_exception_failed_closed:{type(exc).__name__}"
        _record_stage(
            self.store,
            surface="ROBINHOOD_CHAIN",
            candidate_id=candidate,
            release_commit=release,
            stage="context",
            status="failed_closed",
            reason=reason,
            payload={"attribution_precision": "exact_exception_type"},
        )
        _record_stage(
            self.store,
            surface="ROBINHOOD_CHAIN",
            candidate_id=candidate,
            release_commit=release,
            stage="decision",
            status="complete",
            reason=reason,
            payload={"decision": "paper_reject"},
        )
        _record_stage(
            self.store,
            surface="ROBINHOOD_CHAIN",
            candidate_id=candidate,
            release_commit=release,
            stage="position",
            status="not_opened",
            reason=reason,
        )
        _upsert_ledger(self, trace, decision="paper_reject", reason=reason)
        raise

    if _ledger_row(self, candidate) is not None:
        return

    reason, precision = _local_preselection_reason(
        self,
        kind=kind,
        token=token,
        subject=subject,
        current_block=current_block,
    )
    _record_stage(
        self.store,
        surface="ROBINHOOD_CHAIN",
        candidate_id=candidate,
        release_commit=release,
        stage="context",
        status="failed_closed",
        reason=reason,
        payload={"attribution_precision": precision, "duplicate_provider_work_used": False},
    )
    _record_stage(
        self.store,
        surface="ROBINHOOD_CHAIN",
        candidate_id=candidate,
        release_commit=release,
        stage="execution_evidence",
        status="not_requested",
        reason=reason,
    )
    _record_stage(
        self.store,
        surface="ROBINHOOD_CHAIN",
        candidate_id=candidate,
        release_commit=release,
        stage="decision",
        status="complete",
        reason=reason,
        payload={"decision": "paper_reject", "attribution_precision": precision},
    )
    _record_stage(
        self.store,
        surface="ROBINHOOD_CHAIN",
        candidate_id=candidate,
        release_commit=release,
        stage="position",
        status="not_opened",
        reason=reason,
    )
    _upsert_ledger(self, trace, decision="paper_reject", reason=reason)


def _wrap_v3(original: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(self: Any, pool: Any, *, current_block: int) -> None:
        await _observe_and_delegate(
            self,
            original=original,
            kind="v3",
            subject=pool,
            current_block=current_block,
        )

    # functools.wraps deliberately preserves all existing composition metadata,
    # including the verified-live-frontier guard marker and original module lineage.
    setattr(wrapped, "_roi_v51_prelane_coverage", True)
    return wrapped


def _wrap_v2(original: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(self: Any, curve: Any) -> None:
        await _observe_and_delegate(self, original=original, kind="v2", subject=curve)

    setattr(wrapped, "_roi_v51_prelane_coverage", True)
    return wrapped


def install_v51_robinhood_candidate_coverage(plane_cls: type[Any]) -> None:
    """Make every delivered Robinhood opportunity auditable before lane selection."""
    global _INSTALLED
    current_v2 = plane_cls._maybe_open_v2
    current_v3 = plane_cls._maybe_open_v3
    if not bool(getattr(current_v2, "_roi_v51_prelane_coverage", False)):
        plane_cls._maybe_open_v2 = _wrap_v2(current_v2)
    if not bool(getattr(current_v3, "_roi_v51_prelane_coverage", False)):
        plane_cls._maybe_open_v3 = _wrap_v3(current_v3)
    _INSTALLED = True


__all__ = ["COVERAGE_VERSION", "install_v51_robinhood_candidate_coverage"]
