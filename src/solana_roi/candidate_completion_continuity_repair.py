from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from . import candidate_certification_hotpath_repair as candidate_hotpath
from . import candidate_rpc_priority_repair as candidate_priority
from . import continuity_high_volume_checkpoint_architecture as checkpoint
from . import continuity_standby_rpc_priority_repair as standby_priority
from . import forward_evidence_runtime_repair as forward
from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from . import rpc_workload_governor as governor
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


# Processed logsSubscribe can precede confirmed getTransaction availability. Keep
# the first read immediate for the repository's canonical claim contract, but do
# not let the same null-result row spin continuously: queue re-admission supplies
# retries and a short retry backoff lets untouched scout triggers make progress.
CANDIDATE_FIRST_FETCH_GRACE_SECONDS = 0.0
CANDIDATE_RETRY_BACKOFF_SECONDS = 0.20
CANDIDATE_URGENT_REMAINING_SECONDS = 5.0
CANDIDATE_FRESH_RPC_SLICE_SECONDS = 1.25
CANDIDATE_LATE_RPC_SLICE_SECONDS = 0.75

_ORIGINAL_TRANSACTION_READY: Callable[..., Any] | None = None
_ORIGINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_CHECKPOINT_FETCH: Callable[..., Any] | None = None
_ORIGINAL_ALLOWED: Callable[..., tuple[bool, float]] | None = None

_FAIRNESS_COUNTS: dict[str, int] = {
    "candidate_yields_to_waiting_standby": 0,
    "standby_yields_to_waiting_candidate": 0,
    "background_yields_to_reserved_forward_slots": 0,
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _inc(obj: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_candidate_completion_{name}"
    setattr(obj, attr, int(getattr(obj, attr, 0) or 0) + int(amount))


def _deadline_aware_claim_candidate(journal: Any) -> dict[str, Any] | None:
    """Claim scout work without letting null-result retries starve new triggers.

    Every first attempt remains immediately claimable. A requeued row must wait a
    short interval before another confirmed transaction read, except once it enters
    the final five seconds of the unchanged 20-second entry window. Urgent rows are
    then ordered first. The existing fail-closed reaper remains the hard deadline
    owner and no candidate is made authoritative by this scheduler.
    """

    now = _utcnow()
    urgent_age = max(
        0.0,
        float(forward.ENTRY_WINDOW_SECONDS) - CANDIDATE_URGENT_REMAINING_SECONDS,
    )
    urgent_cutoff = (now - timedelta(seconds=urgent_age)).isoformat()
    retry_ready_cutoff = (now - timedelta(seconds=CANDIDATE_RETRY_BACKOFF_SECONDS)).isoformat()
    reasons = tuple(sorted(forward.SCOUT_REASONS))

    sql = (
        "SELECT signature, slot, trigger_received_at, source_hint, priority, reason, attempts "
        "FROM direct_solana_hydration_queue "
        "WHERE status='pending' AND priority<=2 AND reason IN (?,?) AND ("
        "attempts=0 OR trigger_received_at<=? OR updated_at<=?) "
        "ORDER BY CASE WHEN trigger_received_at<=? THEN 0 WHEN attempts=0 THEN 1 ELSE 2 END, "
        "trigger_received_at, updated_at, signature LIMIT 1"
    )
    args = (*reasons, urgent_cutoff, retry_ready_cutoff, urgent_cutoff)
    with journal.store._lock, journal.store.db:
        row = journal.store.db.execute(sql, args).fetchone()
        if row is None:
            return None
        signature = str(row["signature"])
        updated = journal.store.db.execute(
            "UPDATE direct_solana_hydration_queue "
            "SET status='processing', attempts=attempts+1, updated_at=? "
            "WHERE signature=? AND status='pending'",
            (now.isoformat(), signature),
        )
        if updated.rowcount != 1:
            return None
        return dict(row)


async def _single_attempt_candidate_transaction_ready(
    self: DirectSolanaIngestionPlane,
    signature: str,
    *,
    hedge: bool,
    attempts: int,
) -> tuple[Any, str | None, float | None]:
    """Use one confirmed transaction read per scout queue claim.

    The canonical hydrator already requeues a null confirmed result. Repeating the
    read multiple times inside one queue claim needlessly holds a scarce candidate
    RPC slot while other frozen scout triggers age. One attempt per claim plus the
    queue backoff above is work-conserving and retains the same 20-second fail-closed
    boundary.
    """

    reason = candidate_hotpath._CURRENT_HYDRATION_REASON.get()
    trigger = forward._CURRENT_TRIGGER_AT.get()
    if reason not in forward.SCOUT_REASONS or trigger is None:
        if _ORIGINAL_TRANSACTION_READY is None:
            raise RuntimeError("candidate completion repair is not installed")
        return await _ORIGINAL_TRANSACTION_READY(
            self, signature, hedge=hedge, attempts=attempts
        )

    age = max(0.0, (_utcnow() - trigger).total_seconds())
    remaining_entry = max(0.0, float(forward.ENTRY_WINDOW_SECONDS) - age)
    if remaining_entry <= 0.0:
        _inc(self, "rpc_skipped_after_entry_window")
        return None, None, None

    remaining_fresh = float(forward.LATENCY_BUDGET_SECONDS) - age
    slice_seconds = (
        min(CANDIDATE_FRESH_RPC_SLICE_SECONDS, max(0.15, remaining_fresh))
        if remaining_fresh > 0.0
        else min(CANDIDATE_LATE_RPC_SLICE_SECONDS, remaining_entry)
    )
    slice_seconds = max(0.10, min(slice_seconds, remaining_entry))

    # Bypass PR #98's two-inner-attempt wrapper only for the frozen scout path.
    # Its captured delegate is the canonical pre-PR98 getTransaction readiness
    # method; all RPC governor, cooldown and hedge wrappers remain underneath it.
    delegate = forward._ORIGINAL_GET_TRANSACTION_READY or _ORIGINAL_TRANSACTION_READY
    if delegate is None:
        raise RuntimeError("candidate transaction delegate is unavailable")

    try:
        result = await asyncio.wait_for(
            delegate(self, signature, hedge=True, attempts=1),
            timeout=slice_seconds,
        )
    except asyncio.TimeoutError:
        _inc(self, "rpc_slice_timeouts")
        return None, None, None
    except asyncio.CancelledError:
        raise
    except Exception:
        _inc(self, "rpc_errors")
        raise

    tx = result[0] if isinstance(result, tuple) and result else None
    _inc(self, "transaction_ready" if tx is not None else "transaction_unavailable")
    _inc(self, "rpc_claims_completed")
    return result


setattr(
    _single_attempt_candidate_transaction_ready,
    "_roi_candidate_completion_single_attempt",
    True,
)


def _fair_noncritical_allowed(
    state: Any,
    workload: str,
    policy: dict[str, float | int],
) -> tuple[bool, float]:
    """Guarantee progress for candidate and high-volume standby lanes.

    Candidate keeps first claim on noncritical capacity. Once one candidate owns a
    slot, a waiting standby read gets the second slot before candidate #2. The
    symmetric rule prevents standby from taking both slots while a candidate waits.
    Background certification/research uses only capacity not currently promised to
    those forward lanes. Total endpoint concurrency and the independent critical
    continuity reservation are unchanged.
    """

    total = int(policy["total_per_endpoint"])
    noncritical_ceiling = int(policy["noncritical_ceiling_per_endpoint"])
    research_max = int(policy["research_max_per_endpoint"])
    now = time.monotonic()

    if workload == governor.WORKLOAD_CRITICAL:
        return state.active_total < total, 0.0

    noncritical_active = standby_priority._noncritical_active(state)
    if state.active_total >= total or noncritical_active >= noncritical_ceiling:
        return False, 0.01

    candidate_waiters = candidate_priority._candidate_waiters(state)
    standby_waiters = standby_priority._standby_waiters(state)
    candidate_active = int(
        state.active_by_workload.get(candidate_priority.WORKLOAD_CANDIDATE, 0) or 0
    )
    standby_active = int(
        state.active_by_workload.get(standby_priority.WORKLOAD_STANDBY, 0) or 0
    )

    if workload == candidate_priority.WORKLOAD_CANDIDATE:
        if standby_waiters > 0 and standby_active == 0 and candidate_active >= 1:
            _FAIRNESS_COUNTS["candidate_yields_to_waiting_standby"] += 1
            return False, 0.005
    elif workload == standby_priority.WORKLOAD_STANDBY:
        if candidate_waiters > 0 and candidate_active == 0:
            _FAIRNESS_COUNTS["standby_yields_to_waiting_candidate"] += 1
            return False, 0.005
    else:
        reserved = 0
        if candidate_waiters > 0 and candidate_active == 0:
            reserved += 1
        if standby_waiters > 0 and standby_active == 0:
            reserved += 1
        background_ceiling = max(0, noncritical_ceiling - reserved)
        if noncritical_active >= background_ceiling:
            _FAIRNESS_COUNTS["background_yields_to_reserved_forward_slots"] += 1
            return False, 0.01

    if workload == governor.WORKLOAD_RESEARCH:
        if int(state.active_by_workload.get(governor.WORKLOAD_RESEARCH, 0) or 0) >= research_max:
            return False, 0.05
        interval = float(policy["research_min_interval_seconds"])
        remaining = max(
            0.0,
            state.last_research_started_monotonic + interval - now,
        )
        if remaining > 0.0:
            return False, min(0.10, remaining)

    return True, 0.0


setattr(_fair_noncritical_allowed, "_roi_candidate_standby_fairness", True)


async def _generation_safe_checkpoint_fetch(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    """Never healthy-checkpoint across a known unrecovered WS generation.

    Compatibility callers that do not expose the leased cursor-generation runtime
    retain the exact existing universal-checkpoint delegate. Production leased
    workers do expose it; only there do we compare cursor generation with the live
    generation and force canonical bounded recovery before a post-gap healthy
    frontier can advance the standby watermark.
    """

    if _ORIGINAL_CHECKPOINT_FETCH is None:
        raise RuntimeError("generation-safe checkpoint repair is not installed")

    key = live_poll._poll_target_key(target)
    runtime = lease._runtime(self).get(key, {})
    if not isinstance(runtime, dict) or "cursor_ws_generation" not in runtime:
        return await _ORIGINAL_CHECKPOINT_FETCH(self, target, cursor_slot)

    generation = int(lease._current_ws_generation(self, target))
    try:
        cursor_generation = int(runtime.get("cursor_ws_generation") or 0)
    except (TypeError, ValueError):
        cursor_generation = generation

    if cursor_generation != generation:
        _inc(self, "checkpoint_blocked_unrecovered_generation")
        fallback = checkpoint._ORIGINAL_SLOT_FETCH
        if fallback is None:
            return await _ORIGINAL_CHECKPOINT_FETCH(self, target, cursor_slot)
        return await fallback(self, target, cursor_slot)

    return await _ORIGINAL_CHECKPOINT_FETCH(self, target, cursor_slot)


setattr(_generation_safe_checkpoint_fetch, "_roi_generation_safe_checkpoint", True)


def _candidate_queue_snapshot(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    result: dict[str, Any] = {
        "pending": 0,
        "processing": 0,
        "oldest_pending_age_ms": None,
        "max_attempts_pending": 0,
    }
    try:
        reasons = tuple(sorted(forward.SCOUT_REASONS))
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT status, COUNT(*) AS n, MIN(trigger_received_at) AS oldest, "
                "MAX(attempts) AS max_attempts FROM direct_solana_hydration_queue "
                "WHERE reason IN (?,?) AND status IN ('pending','processing') "
                "GROUP BY status",
                reasons,
            ).fetchall()
        now = _utcnow()
        for row in rows:
            status = str(row["status"] or "")
            if status in {"pending", "processing"}:
                result[status] = int(row["n"] or 0)
            if status == "pending":
                result["max_attempts_pending"] = int(row["max_attempts"] or 0)
                oldest = str(row["oldest"] or "")
                if oldest:
                    result["oldest_pending_age_ms"] = max(
                        0.0,
                        (now - _parse_dt(oldest)).total_seconds() * 1000.0,
                    )
    except Exception:
        result["available"] = False
    else:
        result["available"] = True
    return result


def _status_with_candidate_completion_and_fairness(
    self: DirectSolanaIngestionPlane,
) -> dict[str, Any]:
    if _ORIGINAL_DIRECT_STATUS is None:
        raise RuntimeError("candidate completion status is not installed")
    payload = _ORIGINAL_DIRECT_STATUS(self)
    payload["candidate_completion_continuity_repair"] = {
        "installed": True,
        "candidate_scheduler": {
            "first_fetch_immediate": True,
            "retry_backoff_ms": CANDIDATE_RETRY_BACKOFF_SECONDS * 1000.0,
            "urgent_remaining_seconds": CANDIDATE_URGENT_REMAINING_SECONDS,
            "single_get_transaction_attempt_per_queue_claim": True,
            "fresh_rpc_slice_seconds": CANDIDATE_FRESH_RPC_SLICE_SECONDS,
            "late_rpc_slice_seconds": CANDIDATE_LATE_RPC_SLICE_SECONDS,
            "rpc_claims_completed": int(getattr(self, "_roi_candidate_completion_rpc_claims_completed", 0) or 0),
            "transaction_ready": int(getattr(self, "_roi_candidate_completion_transaction_ready", 0) or 0),
            "transaction_unavailable": int(getattr(self, "_roi_candidate_completion_transaction_unavailable", 0) or 0),
            "rpc_slice_timeouts": int(getattr(self, "_roi_candidate_completion_rpc_slice_timeouts", 0) or 0),
            "rpc_errors": int(getattr(self, "_roi_candidate_completion_rpc_errors", 0) or 0),
            "rpc_skipped_after_entry_window": int(getattr(self, "_roi_candidate_completion_rpc_skipped_after_entry_window", 0) or 0),
            "queue": _candidate_queue_snapshot(self),
        },
        "candidate_standby_fairness": {
            "candidate_keeps_first_noncritical_slot": True,
            "waiting_standby_gets_second_noncritical_slot": True,
            "critical_reserve_unchanged": True,
            **dict(_FAIRNESS_COUNTS),
        },
        "continuity_checkpoint_guard": {
            "healthy_frontier_cannot_cross_known_unrecovered_generation": True,
            "blocked_checkpoint_count": int(getattr(self, "_roi_candidate_completion_checkpoint_blocked_unrecovered_generation", 0) or 0),
            "canonical_immediate_recovery_required_first": True,
            "recoverability_lease_seconds": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
            "hard_page_limit": live_poll.POLL_CURSOR_MAX_PAGES,
            "hard_page_size": live_poll.POLL_LIMIT,
        },
        "candidate_latency_target_seconds_unchanged": float(forward.LATENCY_BUDGET_SECONDS),
        "candidate_entry_window_seconds_unchanged": float(forward.ENTRY_WINDOW_SECONDS),
        "paper_only_authority_unchanged": True,
        "signing_or_submission_available": False,
    }
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "candidate_retry_backoff_scheduling": True,
                "candidate_single_transaction_read_per_claim": True,
                "candidate_deadline_urgent_lane": True,
                "candidate_and_standby_noncritical_slots_make_forward_progress": True,
                "healthy_frontier_checkpoint_requires_recovered_ws_generation_when_generation_state_is_known": True,
                "continuity_lease_unchanged": True,
                "recovery_bound_unchanged": True,
                "candidate_latency_threshold_unchanged": True,
                "entry_window_seconds_unchanged": True,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    return payload


setattr(
    _status_with_candidate_completion_and_fairness,
    "_roi_candidate_completion_continuity_repair",
    True,
)


def install_candidate_completion_continuity_repair() -> None:
    """Install the post-PR98 candidate/continuity repair exactly once."""

    global _ORIGINAL_TRANSACTION_READY, _ORIGINAL_DIRECT_STATUS
    global _ORIGINAL_CHECKPOINT_FETCH, _ORIGINAL_ALLOWED

    forward._claim_candidate = _deadline_aware_claim_candidate  # type: ignore[assignment]

    current_transaction_ready = DirectSolanaIngestionPlane._get_transaction_ready
    if not bool(getattr(current_transaction_ready, "_roi_candidate_completion_single_attempt", False)):
        _ORIGINAL_TRANSACTION_READY = current_transaction_ready
        try:
            _single_attempt_candidate_transaction_ready.__dict__.update(
                getattr(current_transaction_ready, "__dict__", {})
            )
        except Exception:
            pass
        DirectSolanaIngestionPlane._get_transaction_ready = _single_attempt_candidate_transaction_ready  # type: ignore[method-assign]

    current_allowed = standby_priority._allowed_with_standby_priority
    if not bool(getattr(current_allowed, "_roi_candidate_standby_fairness", False)):
        _ORIGINAL_ALLOWED = current_allowed
        standby_priority._allowed_with_standby_priority = _fair_noncritical_allowed  # type: ignore[assignment]
        governor._allowed = _fair_noncritical_allowed  # type: ignore[assignment]

    current_checkpoint = checkpoint._checkpointed_slot_fetch_delta
    if not bool(getattr(current_checkpoint, "_roi_generation_safe_checkpoint", False)):
        _ORIGINAL_CHECKPOINT_FETCH = current_checkpoint
        try:
            _generation_safe_checkpoint_fetch.__dict__.update(
                getattr(current_checkpoint, "__dict__", {})
            )
        except Exception:
            pass
        checkpoint._checkpointed_slot_fetch_delta = _generation_safe_checkpoint_fetch  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_candidate_completion_continuity_repair", False)):
        _ORIGINAL_DIRECT_STATUS = current_status
        try:
            _status_with_candidate_completion_and_fairness.__dict__.update(
                getattr(current_status, "__dict__", {})
            )
        except Exception:
            pass
        DirectSolanaIngestionPlane.status = _status_with_candidate_completion_and_fairness  # type: ignore[method-assign]


__all__ = [
    "CANDIDATE_FIRST_FETCH_GRACE_SECONDS",
    "CANDIDATE_RETRY_BACKOFF_SECONDS",
    "CANDIDATE_URGENT_REMAINING_SECONDS",
    "_deadline_aware_claim_candidate",
    "_fair_noncritical_allowed",
    "_generation_safe_checkpoint_fetch",
    "_single_attempt_candidate_transaction_ready",
    "install_candidate_completion_continuity_repair",
]
