from __future__ import annotations

import asyncio
import contextvars
from datetime import datetime, timezone
from typing import Any, Callable

from . import candidate_certification_hotpath_repair as candidate_hotpath
from . import candidate_hydration_work_conserving_repair as hydration
from . import certification_runtime_architecture_repair as runtime_arch
from . import launch_coverage_bridge as bridge
from . import rpc_workload_governor as governor
from . import runtime_guards as guards
from . import target_stream_fanout as fanout
from .config import BASELINE
from .direct_solana import DirectSolanaIngestionPlane
from .wallet_discovery import ContinuousWalletDiscovery
from .wallet_realtime_tracking_repair import RealtimeWalletTracker


# The public RPC workload governor exposes two non-critical slots per endpoint.
# Do not let nine hydration workers claim scout rows and then sit in RPC waiters;
# admit only the amount of candidate work that can actually make forward progress.
CANDIDATE_MAX_INFLIGHT = 2
CANDIDATE_RPC_SLICE_SECONDS = 2.0
CANDIDATE_LATE_RPC_SLICE_SECONDS = 1.0
ENTRY_WINDOW_SECONDS = float(BASELINE.confirmation_window_seconds)
LATENCY_BUDGET_SECONDS = 5.0
PREWARM_TASK_LIMIT = 64
PREWARM_FUNDING_RETRIES = 3
PREWARM_CONTEXT_WAIT_SECONDS = 40.0

SCOUT_REASONS = frozenset(
    {"frozen_scout_processed_trigger", "frozen_scout_live_poll_trigger"}
)

_ORIGINAL_GET_TRANSACTION_READY: Callable[..., Any] | None = None
_ORIGINAL_HYDRATE_ONE: Callable[..., Any] | None = None
_ORIGINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_SEED_CREATED_AT: Callable[..., bool] | None = None

_CURRENT_TRIGGER_AT: contextvars.ContextVar[datetime | None] = contextvars.ContextVar(
    "roi_forward_candidate_trigger_at", default=None
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _inc(obj: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_forward_evidence_{name}"
    setattr(obj, attr, int(getattr(obj, attr, 0) or 0) + int(amount))


def _active_candidates(self: Any) -> int:
    return int(getattr(self, "_roi_forward_evidence_active_candidates", 0) or 0)


def _set_active_candidates(self: Any, value: int) -> None:
    normalized = max(0, int(value))
    setattr(self, "_roi_forward_evidence_active_candidates", normalized)
    setattr(
        self,
        "_roi_forward_evidence_max_active_candidates",
        max(
            normalized,
            int(getattr(self, "_roi_forward_evidence_max_active_candidates", 0) or 0),
        ),
    )


def _claim_filtered(journal: Any, *, clause: str, args: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    now = _utcnow().isoformat()
    sql = (
        "SELECT signature, slot, trigger_received_at, source_hint, priority, reason, attempts "
        "FROM direct_solana_hydration_queue WHERE status='pending' AND "
        + clause
        + " ORDER BY priority, updated_at, signature LIMIT 1"
    )
    with journal.store._lock, journal.store.db:
        row = journal.store.db.execute(sql, args).fetchone()
        if row is None:
            return None
        signature = str(row["signature"])
        updated = journal.store.db.execute(
            "UPDATE direct_solana_hydration_queue SET status='processing', attempts=attempts+1, updated_at=? "
            "WHERE signature=? AND status='pending'",
            (now, signature),
        )
        if updated.rowcount != 1:
            return None
        return dict(row)


def _claim_candidate(journal: Any) -> dict[str, Any] | None:
    reasons = tuple(sorted(SCOUT_REASONS))
    return _claim_filtered(
        journal,
        clause="priority<=2 AND reason IN (?,?)",
        args=reasons,
    )


def _claim_non_candidate_fast(journal: Any) -> dict[str, Any] | None:
    reasons = tuple(sorted(SCOUT_REASONS))
    return _claim_filtered(
        journal,
        clause="priority<=2 AND reason NOT IN (?,?)",
        args=reasons,
    )


def _claim_background(journal: Any) -> dict[str, Any] | None:
    return _claim_filtered(journal, clause="priority>2")


def _claim_forward_work(self: Any, *, fast_only: bool) -> tuple[dict[str, Any] | None, str]:
    candidate_capacity = _active_candidates(self) < CANDIDATE_MAX_INFLIGHT

    if fast_only:
        if candidate_capacity:
            row = _claim_candidate(self.journal)
            if row is not None:
                return row, "candidate_reserved"
        else:
            _inc(self, "candidate_admission_deferrals")
        row = _claim_non_candidate_fast(self.journal)
        return (row, "candidate_reserved") if row is not None else (None, "none")

    if hydration._background_worker_can_flex(self):
        if candidate_capacity:
            row = _claim_candidate(self.journal)
            if row is not None:
                return row, "candidate_flex"
        else:
            _inc(self, "candidate_admission_deferrals")
        row = _claim_non_candidate_fast(self.journal)
        if row is not None:
            return row, "candidate_flex"

    row = _claim_background(self.journal)
    return (row, "background") if row is not None else (None, "none")


async def _bounded_candidate_worker(
    self: Any,
    stop: asyncio.Event,
    *,
    fast_only: bool,
) -> None:
    """Keep candidate queue ownership aligned with real RPC capacity.

    Rows are claimed only when a candidate admission slot is available. This keeps
    unclaimed scout triggers in the pending queue, where the existing entry-window
    reaper can classify them deterministically, rather than stranding hundreds of
    rows in `processing` while workers wait behind the same two RPC slots.
    """

    next_cleanup = 0.0
    loop = asyncio.get_running_loop()
    while not stop.is_set():
        if not fast_only and loop.time() >= next_cleanup:
            guards._expire_stale_background(self)
            next_cleanup = loop.time() + 5.0

        row, lane = _claim_forward_work(self, fast_only=fast_only)
        if row is None:
            await asyncio.sleep(0.01 if fast_only else 0.025)
            continue

        if lane == "candidate_reserved":
            hydration._increment(self, "reserved_candidate_claims")
        elif lane == "candidate_flex":
            hydration._increment(self, "flex_candidate_claims")
        elif lane == "background":
            hydration._increment(self, "background_claims")

        is_candidate = str(row.get("reason") or "") in SCOUT_REASONS
        if is_candidate:
            _set_active_candidates(self, _active_candidates(self) + 1)
            _inc(self, "candidate_claims")
        try:
            await self._hydrate_one(row)
        finally:
            if is_candidate:
                _set_active_candidates(self, _active_candidates(self) - 1)


setattr(_bounded_candidate_worker, "_roi_candidate_work_conserving", True)
setattr(_bounded_candidate_worker, "_roi_forward_evidence_bounded_admission", True)


async def _candidate_transaction_ready(
    self: DirectSolanaIngestionPlane,
    signature: str,
    *,
    hedge: bool,
    attempts: int,
) -> tuple[Any, str | None, float | None]:
    if _ORIGINAL_GET_TRANSACTION_READY is None:
        raise RuntimeError("forward evidence runtime repair is not installed")
    reason = candidate_hotpath._CURRENT_HYDRATION_REASON.get()
    trigger = _CURRENT_TRIGGER_AT.get()
    if reason not in SCOUT_REASONS or trigger is None:
        return await _ORIGINAL_GET_TRANSACTION_READY(
            self, signature, hedge=hedge, attempts=attempts
        )

    age = max(0.0, (_utcnow() - trigger).total_seconds())
    remaining_entry = max(0.0, ENTRY_WINDOW_SECONDS - age)
    if remaining_entry <= 0.0:
        _inc(self, "candidate_rpc_skipped_after_entry_window")
        return None, None, None

    remaining_latency = LATENCY_BUDGET_SECONDS - age
    slice_seconds = (
        min(CANDIDATE_RPC_SLICE_SECONDS, max(0.20, remaining_latency))
        if remaining_latency > 0.0
        else min(CANDIDATE_LATE_RPC_SLICE_SECONDS, remaining_entry)
    )
    slice_seconds = max(0.10, min(slice_seconds, remaining_entry))
    try:
        result = await asyncio.wait_for(
            _ORIGINAL_GET_TRANSACTION_READY(
                self,
                signature,
                hedge=True,
                attempts=min(max(1, int(attempts)), 2),
            ),
            timeout=slice_seconds,
        )
        _inc(self, "candidate_rpc_slices_completed")
        return result
    except asyncio.TimeoutError:
        _inc(self, "candidate_rpc_slice_timeouts")
        return None, None, None


async def _candidate_hydrate_with_terminal_accounting(
    self: DirectSolanaIngestionPlane,
    row: dict[str, Any],
) -> None:
    if _ORIGINAL_HYDRATE_ONE is None:
        raise RuntimeError("forward evidence runtime repair is not installed")
    reason = str(row.get("reason") or "")
    token = _CURRENT_TRIGGER_AT.set(_parse_dt(row["trigger_received_at"]))
    try:
        await _ORIGINAL_HYDRATE_ONE(self, row)
    finally:
        _CURRENT_TRIGGER_AT.reset(token)

    if reason not in SCOUT_REASONS:
        return
    signature = str(row.get("signature") or "")
    if not signature:
        return
    try:
        with self.store._lock:
            state = self.store.db.execute(
                "SELECT status FROM direct_solana_hydration_queue WHERE signature=?",
                (signature,),
            ).fetchone()
        status = str(state["status"] or "") if state is not None else ""
    except Exception:
        return
    if status != "failed":
        return

    # A row that reaches its canonical terminal hydration failure before the reaper
    # must still be represented in the unchanged certification denominator. Reuse
    # the existing failure-accounting primitive; it records a concrete failed risk
    # sample when normalization succeeded and an anonymous unresolved trigger when
    # identity never became knowable.
    try:
        from . import certification_failure_accounting_repair as accounting

        accounting._account_scout_expiry(
            self.store,
            row,
            outcome="terminal_hydration_failed_before_entry",
        )
        _inc(self, "terminal_candidate_failures_accounted")
    except Exception:
        _inc(self, "terminal_candidate_accounting_errors")


def _prewarm_tasks(self: Any) -> dict[str, asyncio.Task[Any]]:
    value = getattr(self, "_roi_forward_evidence_prewarm_tasks", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_forward_evidence_prewarm_tasks", value)
    return value


def _funding_complete(self: Any, mint: str) -> bool:
    try:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT funding_complete FROM program_coverage_observations "
                "WHERE token_mint=? ORDER BY assessed_at DESC LIMIT 1",
                (mint,),
            ).fetchone()
        return bool(row is not None and row["funding_complete"])
    except Exception:
        return False


async def _prospective_evidence_prewarm(
    self: Any,
    *,
    mint: str,
    created_at: datetime,
) -> None:
    raw = bridge._raw_collectors(self)
    refresh_candidate = getattr(raw, "refresh_candidate", None)
    if callable(refresh_candidate):
        _inc(self, "dynamic_prewarm_attempted")
        try:
            with governor.rpc_workload("certification"):
                await refresh_candidate(mint, _utcnow(), current_swap=None)
            _inc(self, "dynamic_prewarm_complete")
        except asyncio.CancelledError:
            raise
        except Exception:
            _inc(self, "dynamic_prewarm_failed")

    # Funding depends on the immutable launch-window buyer set. Wait for that exact
    # existing attestation, then start provenance immediately instead of waiting for
    # a later candidate to pay the full history cost. The collection timestamp is
    # always the real time of acquisition; nothing is backdated into a prior trigger.
    window_end = created_at + bridge.timedelta(seconds=bridge.LAUNCH_WINDOW_SECONDS)
    delay = max(0.0, (window_end - _utcnow()).total_seconds())
    if delay > 0.0:
        await asyncio.sleep(delay)

    deadline = asyncio.get_running_loop().time() + PREWARM_CONTEXT_WAIT_SECONDS
    while asyncio.get_running_loop().time() < deadline:
        if candidate_hotpath._attested_launch_context(self, mint):
            break
        await asyncio.sleep(0.25)
    else:
        _inc(self, "funding_prewarm_context_timeout")
        return

    funding = getattr(raw, "funding", None)
    collect = getattr(funding, "collect", None)
    if not callable(collect):
        return
    if _funding_complete(self, mint):
        _inc(self, "funding_prewarm_already_complete")
        return

    for attempt in range(PREWARM_FUNDING_RETRIES):
        _inc(self, "funding_prewarm_attempted")
        try:
            with governor.rpc_workload("certification"):
                complete = bool(await collect(mint, _utcnow()))
        except asyncio.CancelledError:
            raise
        except Exception:
            complete = False
            _inc(self, "funding_prewarm_errors")
        if complete or _funding_complete(self, mint):
            _inc(self, "funding_prewarm_complete")
            return
        if attempt + 1 < PREWARM_FUNDING_RETRIES:
            await asyncio.sleep(0.50 * (attempt + 1))
    _inc(self, "funding_prewarm_exhausted")


def _seed_launch_and_schedule_prewarm(self: Any, mint: str, created_at: datetime) -> bool:
    if _ORIGINAL_SEED_CREATED_AT is None:
        raise RuntimeError("prospective evidence prewarm is not installed")
    seeded = bool(_ORIGINAL_SEED_CREATED_AT(self, mint, created_at))
    if not seeded:
        return False

    tasks = _prewarm_tasks(self)
    current = tasks.get(mint)
    if isinstance(current, asyncio.Task) and not current.done():
        return True
    # Remove completed entries before applying the bound.
    for key, task in list(tasks.items()):
        if task.done():
            tasks.pop(key, None)
    if len(tasks) >= PREWARM_TASK_LIMIT:
        _inc(self, "prewarm_task_bound_skips")
        return True

    task = asyncio.create_task(
        _prospective_evidence_prewarm(self, mint=mint, created_at=created_at),
        name=f"prospective-evidence-prewarm:{mint[:8]}",
    )
    tasks[mint] = task
    _inc(self, "prewarm_tasks_started")

    def done(completed: asyncio.Task[Any]) -> None:
        tasks.pop(mint, None)
        try:
            exc = completed.exception()
        except asyncio.CancelledError:
            _inc(self, "prewarm_tasks_cancelled")
            return
        except BaseException:
            _inc(self, "prewarm_tasks_failed")
            return
        if exc is None:
            _inc(self, "prewarm_tasks_completed")
        else:
            _inc(self, "prewarm_tasks_failed")

    task.add_done_callback(done)
    return True


def _safe_wallet_status(self: ContinuousWalletDiscovery) -> dict[str, Any]:
    """Publish wallet status without traversing mutable module-global delegates.

    Several historical installers stored their previous `status` function in a
    mutable module global. Re-installing them in a later production composition can
    make one global point back at its own wrapper and recurse forever. This final
    status is deliberately self-contained and adds the v4/final overlays directly.
    """

    with self.store._lock:
        state_rows = self.store.db.execute(
            "SELECT state, COUNT(*) AS n FROM wallet_discovery_candidates GROUP BY state"
        ).fetchall()
        broad_count = int(
            self.store.db.execute("SELECT COUNT(*) FROM wallet_discovery_broad_samples").fetchone()[0]
        )
        forward_count = int(
            self.store.db.execute("SELECT COUNT(*) FROM wallet_discovery_forward_observations").fetchone()[0]
        )
        copyable_count = int(
            self.store.db.execute(
                "SELECT COUNT(*) FROM wallet_discovery_forward_observations WHERE copyable=1"
            ).fetchone()[0]
        )
        control = self.store.db.execute(
            "SELECT last_raw_receipt_id, last_cycle_at, last_broad_scan_at, last_error "
            "FROM wallet_discovery_state WHERE id=1"
        ).fetchone()
        leaders = [
            dict(row)
            for row in self.store.db.execute(
                "SELECT wallet, state, broad_sample_count, distinct_token_count, historical_closed_episodes, "
                "historical_return_on_capital, historical_profit_factor, forward_started_at, last_polled_at, "
                "forward_epoch_resets, last_error FROM wallet_discovery_candidates "
                "ORDER BY CASE state WHEN 'tracking' THEN 0 WHEN 'incumbent_tracking' THEN 1 ELSE 2 END, "
                "historical_return_on_capital DESC LIMIT 20"
            ).fetchall()
        ]

    states = {str(row["state"]): int(row["n"]) for row in state_rows}
    payload: dict[str, Any] = {
        "enabled": self.enabled,
        "operational": True,
        "paper_only": True,
        "live_money_authority": False,
        "signing_or_submission_available": False,
        "research_lane": True,
        "broad_program_receipt_sampling": True,
        "broad_sample_modulus": self.policy.broad_sample_modulus,
        "approximate_broad_sample_fraction": 1.0 / max(1, self.policy.broad_sample_modulus),
        "ecosystem_wide_exhaustive": False,
        "historical_screen_has_promotion_authority": False,
        "promotion_evidence_boundary": "forward_started_at only",
        "active_strategy_mutation_allowed": False,
        "future_cohort_proposal_enabled": True,
        "candidate_states": states,
        "broad_samples": broad_count,
        "forward_observations": forward_count,
        "copyable_forward_observations": copyable_count,
        "copyable_forward_fraction": copyable_count / forward_count if forward_count else 0.0,
        "tracked_wallet_limit": self.policy.max_tracked_challengers,
        "max_chase_fraction": self.policy.max_chase_fraction,
        "max_observation_lag_seconds": self.policy.max_observation_lag_seconds,
        "last_raw_receipt_id": int(control["last_raw_receipt_id"]) if control is not None else 0,
        "last_cycle_at": (str(control["last_cycle_at"] or "") or None) if control is not None else None,
        "last_broad_scan_at": (str(control["last_broad_scan_at"] or "") or None) if control is not None else None,
        "last_error": (str(control["last_error"] or "") or None) if control is not None else None,
        "tracked_wallets": leaders,
        "wallet_intelligence": self.intelligence.status(),
        "event_loop_cpu_backpressure": {
            **runtime_arch._loop_lag_snapshot(),
            "broad_discovery_skips": int(getattr(self, "_roi_cpu_backpressure_broad_skips", 0) or 0),
            "historical_screen_skips": int(getattr(self, "_roi_cpu_backpressure_screen_skips", 0) or 0),
            "last_reason": getattr(self, "_roi_cpu_backpressure_last_reason", None),
            "historical_research_yields": True,
            "live_forward_tracking_yields": False,
        },
    }

    try:
        from .profit_first_entity_final_research import _adapter

        payload["profit_first_entity_strategy"] = _adapter(self).status()
    except Exception as exc:
        payload["profit_first_entity_strategy"] = {
            "integration_installed": True,
            "failed_closed": True,
            "error_type": type(exc).__name__,
            "paper_only": True,
            "live_money_authority": False,
        }
    try:
        from .wallet_entity_universe_v4 import _universe

        payload["wallet_entity_intelligence_v4"] = _universe(self).status()
    except Exception as exc:
        payload["wallet_entity_intelligence_v4"] = {
            "failed_closed": True,
            "error_type": type(exc).__name__,
            "active_strategy_mutation_allowed": False,
            "historical_promotion_authority": False,
            "paper_only": True,
            "live_money_authority": False,
        }

    realtime_record = RealtimeWalletTracker._record_quick_forward_swap
    discovery_record = ContinuousWalletDiscovery._record_forward_swap
    payload["forward_pipeline_architecture"] = {
        "installed": True,
        "broad_universe_role": "cheap-discovery-and-screening",
        "deep_evaluation_scope": "bounded-dynamic-tracked-wallet-entity-set",
        "historical_screen_has_promotion_authority": False,
        "realtime_forward_evidence_required_for_strategy_influence": True,
        "realtime_v4_handoff_installed": bool(
            getattr(realtime_record, "_roi_profit_first_entity_final", False)
        ),
        "discovery_v4_handoff_installed": bool(
            getattr(discovery_record, "_roi_profit_first_entity_final", False)
        ),
        "v4_research_is_release_bound": True,
        "old_release_forward_rows_replayed_into_new_release": False,
        "active_v3_1_cohort_mutation_allowed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_or_submission_available": False,
    }
    payload["wallet_discovery_final_composition"] = {
        "installed": True,
        "acyclic_status_path": True,
        "acyclic_run_once_path": True,
        "mutable_global_status_delegate_bypassed": True,
        "historical_promotion_authority": False,
    }
    return payload


async def _safe_wallet_run_once(self: ContinuousWalletDiscovery) -> dict[str, Any]:
    if not self.enabled:
        return self.status()
    from .wallet_entity_universe_v4 import _universe

    _universe(self).ensure_seed_candidates()
    await self.ensure_incumbents()
    broad = await self.discover_from_raw_receipts()
    await self.screen_one_candidate()
    tracked = self._tracked_wallets()
    forward = 0
    if tracked:
        results = await asyncio.gather(*(self.poll_wallet(wallet) for wallet in tracked))
        forward = sum(int(value) for value in results)
    proposal = self.maybe_propose_adaptive_cohort()
    now = self.now_fn()
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "UPDATE wallet_discovery_state SET last_cycle_at=?, last_error=NULL WHERE id=1",
            (now.isoformat(),),
        )
    payload = self.status()
    payload["cycle"] = {
        "broad_samples_added": broad,
        "tracked_wallets_polled": len(tracked),
        "forward_observations_added": forward,
        "adaptive_proposal_evaluated": proposal is not None,
        "adaptive_proposal": proposal,
    }
    return payload


def _direct_status_with_forward_evidence(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _ORIGINAL_DIRECT_STATUS is None:
        raise RuntimeError("forward evidence status is not installed")
    payload = _ORIGINAL_DIRECT_STATUS(self)
    tasks = _prewarm_tasks(self)
    raw = bridge._raw_collectors(self)
    funding = getattr(raw, "funding", None)
    payload["forward_evidence_runtime"] = {
        "installed": True,
        "candidate_max_inflight": CANDIDATE_MAX_INFLIGHT,
        "candidate_active": _active_candidates(self),
        "candidate_max_active_observed": int(
            getattr(self, "_roi_forward_evidence_max_active_candidates", 0) or 0
        ),
        "candidate_claims": int(getattr(self, "_roi_forward_evidence_candidate_claims", 0) or 0),
        "candidate_admission_deferrals": int(
            getattr(self, "_roi_forward_evidence_candidate_admission_deferrals", 0) or 0
        ),
        "candidate_rpc_slice_timeouts": int(
            getattr(self, "_roi_forward_evidence_candidate_rpc_slice_timeouts", 0) or 0
        ),
        "candidate_rpc_slices_completed": int(
            getattr(self, "_roi_forward_evidence_candidate_rpc_slices_completed", 0) or 0
        ),
        "terminal_candidate_failures_accounted": int(
            getattr(self, "_roi_forward_evidence_terminal_candidate_failures_accounted", 0) or 0
        ),
        "prewarm_tasks_active": sum(1 for task in tasks.values() if not task.done()),
        "prewarm_tasks_started": int(getattr(self, "_roi_forward_evidence_prewarm_tasks_started", 0) or 0),
        "dynamic_prewarm_complete": int(getattr(self, "_roi_forward_evidence_dynamic_prewarm_complete", 0) or 0),
        "funding_prewarm_complete": int(getattr(self, "_roi_forward_evidence_funding_prewarm_complete", 0) or 0),
        "funding_prewarm_exhausted": int(getattr(self, "_roi_forward_evidence_funding_prewarm_exhausted", 0) or 0),
        "funding_failure_reasons": dict(
            getattr(funding, "_roi_funding_provenance_failure_counts", {}) or {}
        ) if funding is not None else {},
        "point_in_time_prewarm_only": True,
        "evidence_backdating_allowed": False,
        "candidate_latency_threshold_unchanged": True,
        "candidate_entry_window_unchanged": True,
        "coverage_thresholds_unchanged": True,
        "paper_only_authority_unchanged": True,
        "signing_or_submission_available": False,
    }
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "candidate_claims_bounded_to_real_rpc_capacity": True,
                "candidate_processing_rows_do_not_wait_unbounded_behind_rpc_slots": True,
                "prospective_dynamic_risk_prewarmed_at_actual_collection_time": True,
                "funding_provenance_scheduled_from_launch_attestation": True,
                "historical_promotion_authority": False,
                "candidate_latency_threshold_unchanged": True,
                "entry_window_seconds_unchanged": True,
                "paper_only_authority_unchanged": True,
            }
        )
    return payload


def install_forward_evidence_runtime_repair() -> None:
    global _ORIGINAL_GET_TRANSACTION_READY, _ORIGINAL_HYDRATE_ONE
    global _ORIGINAL_DIRECT_STATUS, _ORIGINAL_SEED_CREATED_AT

    # Candidate hydration scheduler: patch both symbols that are captured before
    # runtime task creation. This leaves the total 12-worker pool unchanged.
    hydration._work_conserving_reserved_worker = _bounded_candidate_worker  # type: ignore[assignment]
    guards._reserved_worker = _bounded_candidate_worker  # type: ignore[assignment]
    fanout._reserved_worker = _bounded_candidate_worker  # type: ignore[assignment]

    current_get = DirectSolanaIngestionPlane._get_transaction_ready
    if not bool(getattr(current_get, "_roi_forward_evidence_rpc_slice", False)):
        _ORIGINAL_GET_TRANSACTION_READY = current_get
        try:
            _candidate_transaction_ready.__dict__.update(getattr(current_get, "__dict__", {}))
        except Exception:
            pass
        setattr(_candidate_transaction_ready, "_roi_forward_evidence_rpc_slice", True)
        DirectSolanaIngestionPlane._get_transaction_ready = _candidate_transaction_ready  # type: ignore[method-assign]

    current_hydrate = DirectSolanaIngestionPlane._hydrate_one
    if not bool(getattr(current_hydrate, "_roi_forward_evidence_terminal_accounting", False)):
        _ORIGINAL_HYDRATE_ONE = current_hydrate
        try:
            _candidate_hydrate_with_terminal_accounting.__dict__.update(
                getattr(current_hydrate, "__dict__", {})
            )
        except Exception:
            pass
        setattr(
            _candidate_hydrate_with_terminal_accounting,
            "_roi_forward_evidence_terminal_accounting",
            True,
        )
        DirectSolanaIngestionPlane._hydrate_one = _candidate_hydrate_with_terminal_accounting  # type: ignore[method-assign]

    current_seed = bridge._seed_launch_created_at
    if not bool(getattr(current_seed, "_roi_forward_evidence_prewarm", False)):
        _ORIGINAL_SEED_CREATED_AT = current_seed
        try:
            _seed_launch_and_schedule_prewarm.__dict__.update(getattr(current_seed, "__dict__", {}))
        except Exception:
            pass
        setattr(_seed_launch_and_schedule_prewarm, "_roi_forward_evidence_prewarm", True)
        bridge._seed_launch_created_at = _seed_launch_and_schedule_prewarm  # type: ignore[assignment]

    # Replace the recursive-prone wallet status/run_once chain with one final,
    # explicit composition. Realtime record adapters remain untouched and outermost.
    setattr(_safe_wallet_status, "_roi_forward_evidence_final_wallet_status", True)
    setattr(_safe_wallet_run_once, "_roi_forward_evidence_final_wallet_run_once", True)
    ContinuousWalletDiscovery.status = _safe_wallet_status  # type: ignore[method-assign]
    ContinuousWalletDiscovery.run_once = _safe_wallet_run_once  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_forward_evidence_status", False)):
        _ORIGINAL_DIRECT_STATUS = current_status
        try:
            _direct_status_with_forward_evidence.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_direct_status_with_forward_evidence, "_roi_forward_evidence_status", True)
        DirectSolanaIngestionPlane.status = _direct_status_with_forward_evidence  # type: ignore[method-assign]


__all__ = [
    "CANDIDATE_MAX_INFLIGHT",
    "CANDIDATE_RPC_SLICE_SECONDS",
    "ENTRY_WINDOW_SECONDS",
    "_claim_forward_work",
    "_safe_wallet_run_once",
    "_safe_wallet_status",
    "install_forward_evidence_runtime_repair",
]
