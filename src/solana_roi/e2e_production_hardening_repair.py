from __future__ import annotations

import asyncio
import contextvars
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

from . import candidate_certification_hotpath_repair as candidate_hotpath
from . import certification_failure_accounting_repair as failure_accounting
from . import certification_hotpath_repair as certification_hotpath
from . import forward_evidence_runtime_repair as forward
from . import funding_provenance_repair as funding
from . import launch_chain_timing_repair as legacy_launch_timing
from . import launch_coverage_bridge as launch_bridge
from . import launch_ws_frontier_timing_repair as frontier
from . import live_poll_redundancy as live_poll
from . import render_runtime_bootstrap_repair as render_bootstrap
from . import risk_conditioned_alpha_v51 as v51
from . import robinhood_runtime_install as robinhood_runtime
from . import robinhood_worker_isolation_repair as robinhood_isolation
from . import rpc_workload_governor as governor
from .direct_solana import DirectSolanaIngestionPlane, DirectSolanaJournal
from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter


REPAIR_VERSION = "e2e-production-hardening-v1-116-123"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
CERTIFICATION_THRESHOLDS_CHANGED = False
STRATEGY_THRESHOLDS_CHANGED = False
ENTRY_WINDOW_SECONDS = float(forward.ENTRY_WINDOW_SECONDS)
ROBINHOOD_SUPERVISOR_POLL_SECONDS = 0.25
ROBINHOOD_RESTART_INITIAL_SECONDS = 0.50
ROBINHOOD_RESTART_MAX_SECONDS = 30.0
ROBINHOOD_RESTART_STABLE_SECONDS = 30.0
FRONTIER_BLOCK_TIME_RETRIES = 2
FRONTIER_BLOCK_TIME_RETRY_SECONDS = 0.10

_INSTALLED = False
_ORIGINAL_FINAL_HYDRATE: Callable[..., Awaitable[None]] | None = None
_ORIGINAL_JOURNAL_FINISH: Callable[..., None] | None = None
_ORIGINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_V51_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_FUNDING_TX_CACHED: Callable[..., Awaitable[dict[str, Any]]] | None = None
_ORIGINAL_SIGNATURE_PAGE: Callable[..., Awaitable[list[dict[str, Any]]]] | None = None
_ORIGINAL_FRONTIER_HYDRATE: Callable[..., Awaitable[tuple[int, bool, int]]] | None = None
_ORIGINAL_FRONTIER_LAG: Callable[..., tuple[float | None, str]] | None = None
_ORIGINAL_ROBINHOOD_METADATA: Callable[..., dict[str, Any]] | None = None

_CURRENT_SCOUT_ROW: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "roi_e2e_hardening_current_scout_row", default=None
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _inc(obj: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_e2e_hardening_{name}"
    setattr(obj, attr, int(getattr(obj, attr, 0) or 0) + int(amount))


def _candidate_row(row: dict[str, Any]) -> bool:
    try:
        priority = int(row.get("priority") or 0)
    except (TypeError, ValueError):
        priority = 0
    return priority <= 2 and str(row.get("reason") or "") in forward.SCOUT_REASONS


def _candidate_remaining(row: dict[str, Any], *, now: datetime | None = None) -> float:
    try:
        trigger = _parse_dt(row["trigger_received_at"])
    except Exception:
        return 0.0
    current = now or _utcnow()
    age = max(0.0, (current - trigger).total_seconds())
    return max(0.0, ENTRY_WINDOW_SECONDS - age)


def _account_candidate_terminal(plane: Any, row: dict[str, Any], outcome: str) -> None:
    try:
        failure_accounting._account_scout_expiry(plane.store, row, outcome=outcome)
        _inc(plane, "candidate_terminal_accounted")
    except Exception:
        _inc(plane, "candidate_terminal_accounting_errors")


def _journal_finish_with_absolute_candidate_deadline(
    self: DirectSolanaJournal,
    signature: str,
    *,
    error: str | None = None,
    retry: bool = False,
) -> None:
    """Let real scout transaction availability use the full existing 20-second window.

    Older hydration code could mark a scout terminal after five queue claims even
    though the already-governed entry clock still had time remaining. The claim
    count is an operational retry detail, not an economic threshold. Keep failures
    pending only while the original trigger is still inside the existing entry
    window; once that absolute deadline is reached, fail closed exactly as before.
    """
    if _ORIGINAL_JOURNAL_FINISH is None:
        raise RuntimeError("E2E journal deadline repair is not installed")

    row = _CURRENT_SCOUT_ROW.get()
    effective_retry = bool(retry)
    if isinstance(row, dict) and error and not effective_retry and _candidate_row(row):
        remaining = _candidate_remaining(row)
        if remaining > 0.0:
            effective_retry = True
            _inc(self, "candidate_retry_reopened_inside_entry_window")
        else:
            _inc(self, "candidate_terminal_at_entry_deadline")

    _ORIGINAL_JOURNAL_FINISH(
        self,
        signature,
        error=error,
        retry=effective_retry,
    )


setattr(
    _journal_finish_with_absolute_candidate_deadline,
    "_roi_e2e_absolute_candidate_deadline",
    True,
)


async def _hydrate_with_absolute_candidate_deadline(
    self: DirectSolanaIngestionPlane,
    row: dict[str, Any],
) -> None:
    """Bound the complete hydration call to the original trigger's 20-second clock."""
    if _ORIGINAL_FINAL_HYDRATE is None or _ORIGINAL_JOURNAL_FINISH is None:
        raise RuntimeError("E2E candidate hydration repair is not installed")
    if not _candidate_row(row):
        await _ORIGINAL_FINAL_HYDRATE(self, row)
        return

    token = _CURRENT_SCOUT_ROW.set(dict(row))
    signature = str(row.get("signature") or "")
    try:
        remaining = _candidate_remaining(row)
        if remaining <= 0.0:
            if signature:
                _ORIGINAL_JOURNAL_FINISH(
                    self.journal,
                    signature,
                    error="candidate absolute entry window expired before hydration",
                    retry=False,
                )
            _inc(self, "candidate_expired_before_hydration")
            _account_candidate_terminal(self, row, "expired_before_entry")
            return

        try:
            await asyncio.wait_for(
                _ORIGINAL_FINAL_HYDRATE(self, row),
                timeout=max(0.001, remaining),
            )
        except asyncio.TimeoutError:
            if signature:
                _ORIGINAL_JOURNAL_FINISH(
                    self.journal,
                    signature,
                    error="candidate absolute entry window expired during hydration",
                    retry=False,
                )
            _inc(self, "candidate_absolute_deadline_timeouts")
            _account_candidate_terminal(self, row, "terminal_hydration_failed_before_entry")
    finally:
        _CURRENT_SCOUT_ROW.reset(token)


setattr(_hydrate_with_absolute_candidate_deadline, "_roi_e2e_absolute_candidate_deadline", True)


def _singleflight_map(self: Any, name: str) -> dict[str, asyncio.Task[Any]]:
    value = getattr(self, name, None)
    if isinstance(value, dict):
        return value
    value = {}
    setattr(self, name, value)
    return value


async def _funding_transaction_singleflight(self: Any, signature: str) -> dict[str, Any]:
    if _ORIGINAL_FUNDING_TX_CACHED is None:
        raise RuntimeError("funding transaction singleflight is not installed")
    inflight = _singleflight_map(self, "_roi_e2e_funding_tx_inflight")
    existing = inflight.get(signature)
    if isinstance(existing, asyncio.Task):
        _inc(self, "funding_tx_singleflight_joins")
        return await asyncio.shield(existing)

    task = asyncio.create_task(
        _ORIGINAL_FUNDING_TX_CACHED(self, signature),
        name=f"funding-tx-singleflight:{signature[:10]}",
    )
    inflight[signature] = task
    _inc(self, "funding_tx_singleflight_starts")
    try:
        return await asyncio.shield(task)
    finally:
        if task.done() and inflight.get(signature) is task:
            inflight.pop(signature, None)


async def _funding_signature_page_singleflight(
    self: Any,
    wallet: str,
    *,
    before: str | None,
) -> list[dict[str, Any]]:
    if _ORIGINAL_SIGNATURE_PAGE is None:
        raise RuntimeError("funding signature singleflight is not installed")
    key = f"{wallet}|{before or ''}"
    inflight = _singleflight_map(self, "_roi_e2e_funding_signature_inflight")
    existing = inflight.get(key)
    if isinstance(existing, asyncio.Task):
        _inc(self, "funding_signature_singleflight_joins")
        return await asyncio.shield(existing)

    task = asyncio.create_task(
        _ORIGINAL_SIGNATURE_PAGE(self, wallet, before=before),
        name=f"funding-signature-singleflight:{wallet[:8]}",
    )
    inflight[key] = task
    _inc(self, "funding_signature_singleflight_starts")
    try:
        return await asyncio.shield(task)
    finally:
        if task.done() and inflight.get(key) is task:
            inflight.pop(key, None)


def _frontier_reason_counts(store: Any) -> dict[str, int]:
    value = getattr(store, "_roi_e2e_frontier_reason_counts", None)
    if isinstance(value, dict):
        return value
    value = {}
    try:
        setattr(store, "_roi_e2e_frontier_reason_counts", value)
    except Exception:
        pass
    return value


def _frontier_lag_with_reason_accounting(
    store: Any,
    *,
    signature: str,
    created_at: datetime,
    max_age_seconds: float,
) -> tuple[float | None, str]:
    if _ORIGINAL_FRONTIER_LAG is None:
        raise RuntimeError("frontier reason accounting is not installed")
    lag, reason = _ORIGINAL_FRONTIER_LAG(
        store,
        signature=signature,
        created_at=created_at,
        max_age_seconds=max_age_seconds,
    )
    counts = _frontier_reason_counts(store)
    key = str(reason or "unknown")
    counts[key] = int(counts.get(key, 0) or 0) + 1
    return lag, reason


async def _frontier_hydrate_with_block_time_retry(
    self: Any,
    *,
    mint: str,
    source: str,
    launch_signature: str,
    created_at: datetime,
) -> tuple[int, bool, int]:
    if _ORIGINAL_FRONTIER_HYDRATE is None:
        raise RuntimeError("frontier retry repair is not installed")
    result = await _ORIGINAL_FRONTIER_HYDRATE(
        self,
        mint=mint,
        source=source,
        launch_signature=launch_signature,
        created_at=created_at,
    )

    row = frontier._frontier_row(self.store, launch_signature)
    if not isinstance(row, dict):
        return result
    try:
        launch_slot = int(row.get("launch_slot") or 0)
        frontier_slot = int(row.get("frontier_slot") or 0)
    except (TypeError, ValueError):
        return result
    if frontier_slot <= launch_slot or launch_slot <= 0:
        return result
    if row.get("frontier_block_time") is not None:
        return result

    for attempt in range(FRONTIER_BLOCK_TIME_RETRIES):
        _inc(self, "frontier_block_time_retry_attempts")
        try:
            with governor.rpc_workload("certification"):
                value, _provider, _latency = await live_poll._poll_rpc(self).call_with_meta(
                    "getBlockTime",
                    [frontier_slot],
                    hedge=True,
                )
            if value is None:
                raise RuntimeError("preexisting WebSocket frontier blockTime unavailable")
            frontier._set_frontier_block_time(
                self.store,
                launch_signature,
                block_time=float(value),
            )
            _inc(self, "frontier_block_time_retry_recoveries")
            return result
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if attempt + 1 >= FRONTIER_BLOCK_TIME_RETRIES:
                frontier._set_frontier_block_time(
                    self.store,
                    launch_signature,
                    block_time=None,
                    error_type=type(exc).__name__,
                )
                _inc(self, "frontier_block_time_retry_exhausted")
            else:
                await asyncio.sleep(FRONTIER_BLOCK_TIME_RETRY_SECONDS)
    return result


def _v51_sizing_component(self: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    try:
        with self.store._lock:
            table = self.store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='risk_conditioned_alpha_v51_sizing_audit' LIMIT 1"
            ).fetchone()
            if table is None:
                return {"available": True, "converged_rows": 0, "failed_closed_rows": 0}
            converged = int(
                self.store.db.execute(
                    "SELECT COUNT(*) FROM risk_conditioned_alpha_v51_sizing_audit "
                    "WHERE release_commit=? AND converged=1",
                    (self.release_commit,),
                ).fetchone()[0]
            )
            failed = int(
                self.store.db.execute(
                    "SELECT COUNT(*) FROM risk_conditioned_alpha_v51_sizing_audit "
                    "WHERE release_commit=? AND converged=0",
                    (self.release_commit,),
                ).fetchone()[0]
            )
        return {
            "available": True,
            "converged_rows": converged,
            "failed_closed_rows": failed,
        }
    except Exception as exc:
        return {
            "available": False,
            "error_type": type(exc).__name__,
            "error": str(exc)[:300],
        }


def _v51_allocator_component(self: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    try:
        return {"available": True, "value": v51._allocator_cached(self)}
    except Exception as exc:
        return {
            "available": False,
            "error_type": type(exc).__name__,
            "error": str(exc)[:300],
        }


def _status_with_componentized_v51(self: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    if _ORIGINAL_V51_STATUS is None:
        raise RuntimeError("componentized v5.1 status is not installed")
    payload = _ORIGINAL_V51_STATUS(self)
    sizing = _v51_sizing_component(self)
    allocator = _v51_allocator_component(self)
    section = payload.get("risk_conditioned_alpha_v51")
    existing = dict(section) if isinstance(section, dict) else {}

    core = {
        "strategy_version": v51.V51_VERSION,
        "active_context_key": (
            "entity_x_lane_x_venue_x_lifecycle_x_regime_x_role_x_risk_signature_"
            "x_flow_x_chase_x_latency_x_execution_cost"
        ),
        "cross_entity_promotion_transfer_allowed": False,
        "context_backoff": "same_entity_only_with_risk_preservation",
        "amount_specific_sizing_convergence": True,
        "max_selective_sizing_requotes": v51.MAX_SOLANA_SIZING_REQUOTES,
        "unknown_hard_flags_fail_closed": True,
        "fomo_exact_hazard_signature": True,
        "robinhood_high_snipe_tax_reselects_and_requotes": True,
        "robinhood_exit_learning_regime_risk_conditioned": True,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    core.update({key: value for key, value in existing.items() if key not in {"failed_closed", "error"}})
    core["sizing_audit"] = sizing
    core["cross_regime_allocator_status"] = {
        key: value for key, value in allocator.items() if key != "value"
    }
    if sizing.get("available"):
        core["sizing_converged_rows"] = int(sizing.get("converged_rows") or 0)
        core["sizing_failed_closed_rows"] = int(sizing.get("failed_closed_rows") or 0)
    if allocator.get("available"):
        core["cross_regime_allocator"] = allocator.get("value")
    degraded = not bool(sizing.get("available")) or not bool(allocator.get("available"))
    core["status_degraded"] = degraded
    core["status_failure_components"] = [
        name
        for name, component in (("sizing_audit", sizing), ("cross_regime_allocator", allocator))
        if not bool(component.get("available"))
    ]
    core["generic_status_exception_collapsed"] = False
    payload["risk_conditioned_alpha_v51"] = core
    return payload


setattr(_status_with_componentized_v51, "_roi_e2e_componentized_v51_status", True)


def _prune_legacy_launch_tasks(self: Any) -> int:
    tasks = getattr(self, "_roi_launch_timing_tasks", None)
    if not isinstance(tasks, dict):
        return 0
    removed = 0
    for signature, task in list(tasks.items()):
        if isinstance(task, asyncio.Task) and task.done():
            tasks.pop(signature, None)
            removed += 1
    if removed:
        _inc(self, "legacy_launch_timing_tasks_pruned", removed)
    return removed


def _funding_collector(self: Any) -> Any | None:
    try:
        raw = launch_bridge._raw_collectors(self)
        return getattr(raw, "funding", None)
    except Exception:
        return None


def _status_with_e2e_hardening(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _ORIGINAL_DIRECT_STATUS is None:
        raise RuntimeError("E2E hardening status is not installed")
    _prune_legacy_launch_tasks(self)
    payload = _ORIGINAL_DIRECT_STATUS(self)

    journal = getattr(self, "journal", None)
    funding_collector = _funding_collector(self)
    legacy_tasks = getattr(self, "_roi_launch_timing_tasks", None)
    frontier_counts = dict(_frontier_reason_counts(self.store))
    dispatch = payload.get("raw_receipt_dispatch")
    if isinstance(dispatch, dict):
        dispatch.update(
            {
                "authoritative_implementation": "full_scope_set_based_dispatch",
                "legacy_worker_health_is_readiness_authority": False,
                "legacy_zero_counters_do_not_override_durable_receipt_evidence": True,
                "full_scope_writer_status_is_authoritative": True,
            }
        )

    payload["e2e_production_hardening"] = {
        "repair_version": REPAIR_VERSION,
        "candidate_absolute_entry_deadline_seconds": ENTRY_WINDOW_SECONDS,
        "legacy_five_claim_terminal_limit_removed_for_scouts": True,
        "candidate_retry_reopened_inside_entry_window": int(
            getattr(journal, "_roi_e2e_hardening_candidate_retry_reopened_inside_entry_window", 0) or 0
        ) if journal is not None else 0,
        "candidate_absolute_deadline_timeouts": int(
            getattr(self, "_roi_e2e_hardening_candidate_absolute_deadline_timeouts", 0) or 0
        ),
        "candidate_expired_before_hydration": int(
            getattr(self, "_roi_e2e_hardening_candidate_expired_before_hydration", 0) or 0
        ),
        "funding_transaction_singleflight": True,
        "funding_signature_page_singleflight": True,
        "funding_tx_singleflight_joins": int(
            getattr(funding_collector, "_roi_e2e_hardening_funding_tx_singleflight_joins", 0) or 0
        ) if funding_collector is not None else 0,
        "funding_signature_singleflight_joins": int(
            getattr(funding_collector, "_roi_e2e_hardening_funding_signature_singleflight_joins", 0) or 0
        ) if funding_collector is not None else 0,
        "funding_tx_inflight": len(
            getattr(funding_collector, "_roi_e2e_funding_tx_inflight", {}) or {}
        ) if funding_collector is not None else 0,
        "funding_signature_inflight": len(
            getattr(funding_collector, "_roi_e2e_funding_signature_inflight", {}) or {}
        ) if funding_collector is not None else 0,
        "frontier_block_time_retries": FRONTIER_BLOCK_TIME_RETRIES,
        "frontier_block_time_retry_attempts": int(
            getattr(self, "_roi_e2e_hardening_frontier_block_time_retry_attempts", 0) or 0
        ),
        "frontier_block_time_retry_recoveries": int(
            getattr(self, "_roi_e2e_hardening_frontier_block_time_retry_recoveries", 0) or 0
        ),
        "near_creation_timing_outcomes": frontier_counts,
        "legacy_launch_timing_task_cache_size": len(legacy_tasks) if isinstance(legacy_tasks, dict) else 0,
        "legacy_launch_timing_tasks_pruned": int(
            getattr(self, "_roi_e2e_hardening_legacy_launch_timing_tasks_pruned", 0) or 0
        ),
        "raw_dispatch_authority": "full_scope_set_based_dispatch",
        "strategy_thresholds_changed": False,
        "certification_thresholds_changed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "scout_terminal_retry_boundary_is_absolute_entry_clock": True,
                "funding_duplicate_rpc_work_singleflight": True,
                "launch_frontier_failure_reason_accounting": True,
                "launch_frontier_block_time_bounded_retry": True,
                "legacy_launch_task_cache_pruned": True,
                "raw_dispatch_legacy_worker_status_non_authoritative": True,
                "strategy_thresholds_unchanged": True,
                "certification_thresholds_unchanged": True,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    return payload


setattr(_status_with_e2e_hardening, "_roi_e2e_production_hardening", True)


def _robinhood_supervision_metadata(*, store_path: str | None = None) -> dict[str, Any]:
    if _ORIGINAL_ROBINHOOD_METADATA is None:
        return {}
    payload = _ORIGINAL_ROBINHOOD_METADATA(store_path=store_path)
    payload.update(
        {
            "supervised_restart_enabled": True,
            "supervisor_version": REPAIR_VERSION,
            "supervisor_poll_seconds": ROBINHOOD_SUPERVISOR_POLL_SECONDS,
            "restart_initial_seconds": ROBINHOOD_RESTART_INITIAL_SECONDS,
            "restart_max_seconds": ROBINHOOD_RESTART_MAX_SECONDS,
            "restart_count": int(robinhood_runtime._STATE.get("supervisor_restart_count", 0) or 0),
            "worker_generation": int(robinhood_runtime._STATE.get("supervisor_generation", 0) or 0),
            "last_restart_at": robinhood_runtime._STATE.get("supervisor_last_restart_at"),
            "last_terminal_error": robinhood_runtime._STATE.get("supervisor_last_terminal_error"),
        }
    )
    return payload


async def _supervised_robinhood_runtime_workers(runtime: Any, stop: asyncio.Event) -> None:
    """Run canonical workers while independently restarting a failed Robinhood thread."""
    canonical_workers = getattr(robinhood_runtime, "_ORIGINAL_RUNTIME_WORKERS", None)
    if canonical_workers is None:
        raise RuntimeError("Robinhood canonical worker composition unavailable")

    canonical_store = getattr(runtime, "store", None)
    if canonical_store is None or not hasattr(canonical_store, "path"):
        robinhood_runtime._STATE["worker_isolation_skipped_no_store"] = True
        await canonical_workers(runtime, stop)
        return

    robinhood_runtime._STATE["worker_isolation_skipped_no_store"] = False
    supervisor_stop = asyncio.Event()
    holder: dict[str, Any] = {}

    def start_generation(*, restart: bool) -> None:
        thread, thread_stop = robinhood_isolation._start_worker_thread(canonical_store)
        holder["thread"] = thread
        holder["thread_stop"] = thread_stop
        generation = int(robinhood_runtime._STATE.get("supervisor_generation", 0) or 0) + 1
        robinhood_runtime._STATE["supervisor_generation"] = generation
        robinhood_runtime._STATE["supervisor_enabled"] = True
        robinhood_runtime._STATE["supervisor_version"] = REPAIR_VERSION
        if restart:
            robinhood_runtime._STATE["supervisor_restart_count"] = int(
                robinhood_runtime._STATE.get("supervisor_restart_count", 0) or 0
            ) + 1
            robinhood_runtime._STATE["supervisor_last_restart_at"] = _utcnow().isoformat()

    start_generation(restart=False)

    async def supervise() -> None:
        backoff = ROBINHOOD_RESTART_INITIAL_SECONDS
        alive_since = time.monotonic()
        while not stop.is_set() and not supervisor_stop.is_set():
            thread = holder.get("thread")
            if thread is not None and thread.is_alive():
                if time.monotonic() - alive_since >= ROBINHOOD_RESTART_STABLE_SECONDS:
                    backoff = ROBINHOOD_RESTART_INITIAL_SECONDS
                await asyncio.sleep(ROBINHOOD_SUPERVISOR_POLL_SECONDS)
                continue

            state = str(robinhood_runtime._STATE.get("state") or "")
            if state == "disabled":
                robinhood_runtime._STATE["supervisor_disabled_worker_not_restarted"] = True
                await asyncio.sleep(ROBINHOOD_SUPERVISOR_POLL_SECONDS)
                continue
            if stop.is_set() or supervisor_stop.is_set():
                return

            robinhood_runtime._STATE["supervisor_last_terminal_error"] = (
                robinhood_runtime._STARTUP_ERROR or "robinhood_worker_exited_without_terminal_error"
            )
            robinhood_runtime._STATE["state"] = "restarting_failed_closed"
            deadline = time.monotonic() + backoff
            while time.monotonic() < deadline:
                if stop.is_set() or supervisor_stop.is_set():
                    return
                await asyncio.sleep(min(ROBINHOOD_SUPERVISOR_POLL_SECONDS, deadline - time.monotonic()))
            if stop.is_set() or supervisor_stop.is_set():
                return
            start_generation(restart=True)
            alive_since = time.monotonic()
            backoff = min(ROBINHOOD_RESTART_MAX_SECONDS, backoff * 2.0)

    supervisor_task = asyncio.create_task(supervise(), name="robinhood-isolated-worker-supervisor")
    try:
        await canonical_workers(runtime, stop)
    finally:
        supervisor_stop.set()
        supervisor_task.cancel()
        await asyncio.gather(supervisor_task, return_exceptions=True)
        thread_stop = holder.get("thread_stop")
        thread = holder.get("thread")
        if thread_stop is not None:
            thread_stop.set()
        if thread is not None:
            await asyncio.to_thread(thread.join, robinhood_isolation.THREAD_JOIN_TIMEOUT_SECONDS)
            if thread.is_alive():
                robinhood_runtime._STATE["state"] = "shutdown_timeout_daemon_thread"
            elif stop.is_set() and robinhood_runtime._STATE.get("state") != "failed_closed":
                robinhood_runtime._STATE["state"] = "stopped"


setattr(_supervised_robinhood_runtime_workers, "_roi_e2e_robinhood_supervisor", True)


def _install_candidate_deadline() -> None:
    global _ORIGINAL_FINAL_HYDRATE, _ORIGINAL_JOURNAL_FINISH
    current_finish = DirectSolanaJournal.finish
    if not bool(getattr(current_finish, "_roi_e2e_absolute_candidate_deadline", False)):
        _ORIGINAL_JOURNAL_FINISH = current_finish
        DirectSolanaJournal.finish = _journal_finish_with_absolute_candidate_deadline  # type: ignore[method-assign]

    current_hydrate = DirectSolanaIngestionPlane._hydrate_one
    if not bool(getattr(current_hydrate, "_roi_e2e_absolute_candidate_deadline", False)):
        _ORIGINAL_FINAL_HYDRATE = current_hydrate
        try:
            _hydrate_with_absolute_candidate_deadline.__dict__.update(
                getattr(current_hydrate, "__dict__", {})
            )
        except Exception:
            pass
        setattr(_hydrate_with_absolute_candidate_deadline, "_roi_e2e_absolute_candidate_deadline", True)
        DirectSolanaIngestionPlane._hydrate_one = _hydrate_with_absolute_candidate_deadline  # type: ignore[method-assign]


def _install_funding_singleflight() -> None:
    global _ORIGINAL_FUNDING_TX_CACHED, _ORIGINAL_SIGNATURE_PAGE
    current_tx = certification_hotpath._transaction_cached
    if not bool(getattr(current_tx, "_roi_e2e_funding_singleflight", False)):
        _ORIGINAL_FUNDING_TX_CACHED = current_tx
        setattr(_funding_transaction_singleflight, "_roi_e2e_funding_singleflight", True)
        certification_hotpath._transaction_cached = _funding_transaction_singleflight  # type: ignore[assignment]

    current_signature = funding._signature_page_with_retry
    if not bool(getattr(current_signature, "_roi_e2e_funding_singleflight", False)):
        _ORIGINAL_SIGNATURE_PAGE = current_signature
        setattr(_funding_signature_page_singleflight, "_roi_e2e_funding_singleflight", True)
        funding._signature_page_with_retry = _funding_signature_page_singleflight  # type: ignore[assignment]


def _install_frontier_repairs() -> None:
    global _ORIGINAL_FRONTIER_HYDRATE, _ORIGINAL_FRONTIER_LAG
    current_hydrate = certification_hotpath._PRE_REPAIR_HYDRATE_MINT_LAUNCH_CONTEXT
    if not bool(getattr(current_hydrate, "_roi_e2e_frontier_retry", False)):
        _ORIGINAL_FRONTIER_HYDRATE = current_hydrate
        setattr(_frontier_hydrate_with_block_time_retry, "_roi_e2e_frontier_retry", True)
        certification_hotpath._PRE_REPAIR_HYDRATE_MINT_LAUNCH_CONTEXT = (  # type: ignore[assignment]
            _frontier_hydrate_with_block_time_retry
        )

    current_lag = frontier._ws_frontier_lag_seconds
    if not bool(getattr(current_lag, "_roi_e2e_frontier_reason_accounting", False)):
        _ORIGINAL_FRONTIER_LAG = current_lag
        setattr(_frontier_lag_with_reason_accounting, "_roi_e2e_frontier_reason_accounting", True)
        frontier._ws_frontier_lag_seconds = _frontier_lag_with_reason_accounting  # type: ignore[assignment]


def _install_v51_status() -> None:
    global _ORIGINAL_V51_STATUS
    current = FinalProfitFirstResearchAdapter.status
    if not bool(getattr(current, "_roi_e2e_componentized_v51_status", False)):
        _ORIGINAL_V51_STATUS = current
        try:
            _status_with_componentized_v51.__dict__.update(getattr(current, "__dict__", {}))
        except Exception:
            pass
        FinalProfitFirstResearchAdapter.status = _status_with_componentized_v51  # type: ignore[method-assign]


def _install_direct_status() -> None:
    global _ORIGINAL_DIRECT_STATUS
    current = DirectSolanaIngestionPlane.status
    if not bool(getattr(current, "_roi_e2e_production_hardening", False)):
        _ORIGINAL_DIRECT_STATUS = current
        try:
            _status_with_e2e_hardening.__dict__.update(getattr(current, "__dict__", {}))
        except Exception:
            pass
        DirectSolanaIngestionPlane.status = _status_with_e2e_hardening  # type: ignore[method-assign]


def _install_robinhood_supervisor() -> None:
    global _ORIGINAL_ROBINHOOD_METADATA
    current_workers = render_bootstrap._run_runtime_workers
    if not bool(getattr(current_workers, "_roi_e2e_robinhood_supervisor", False)):
        render_bootstrap._run_runtime_workers = _supervised_robinhood_runtime_workers  # type: ignore[assignment]

    current_metadata = robinhood_isolation._worker_isolation_metadata
    if not bool(getattr(current_metadata, "_roi_e2e_robinhood_supervisor", False)):
        _ORIGINAL_ROBINHOOD_METADATA = current_metadata
        setattr(_robinhood_supervision_metadata, "_roi_e2e_robinhood_supervisor", True)
        robinhood_isolation._worker_isolation_metadata = _robinhood_supervision_metadata  # type: ignore[assignment]

    robinhood_runtime._STATE.update(
        {
            "supervisor_enabled": True,
            "supervisor_version": REPAIR_VERSION,
            "supervisor_restart_count": int(
                robinhood_runtime._STATE.get("supervisor_restart_count", 0) or 0
            ),
            "supervisor_generation": int(robinhood_runtime._STATE.get("supervisor_generation", 0) or 0),
        }
    )


def install_e2e_production_hardening() -> None:
    """Install repairs 116-123 after the final production graph has composed."""
    global _INSTALLED
    if _INSTALLED:
        return
    _install_candidate_deadline()
    _install_funding_singleflight()
    _install_frontier_repairs()
    _install_v51_status()
    _install_direct_status()
    _install_robinhood_supervisor()
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "candidate_absolute_entry_deadline_seconds": ENTRY_WINDOW_SECONDS,
        "legacy_five_claim_terminal_limit_removed_for_scouts": True,
        "funding_transaction_singleflight": True,
        "funding_signature_page_singleflight": True,
        "frontier_block_time_bounded_retry": True,
        "frontier_failure_reason_accounting": True,
        "v51_status_componentized": True,
        "legacy_launch_task_cache_pruned_from_status_path": True,
        "raw_dispatch_status_authority_disambiguated": True,
        "robinhood_worker_supervised_restart": True,
        "strategy_thresholds_changed": False,
        "certification_thresholds_changed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "REPAIR_VERSION",
    "install_e2e_production_hardening",
    "status",
]
