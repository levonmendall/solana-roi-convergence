from __future__ import annotations

import asyncio
import contextvars
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from . import candidate_certification_hotpath_repair as candidate_hotpath
from . import candidate_rpc_priority_repair as candidate_priority
from . import forward_evidence_runtime_repair as forward
from . import full_scope_dispatch_capacity_repair as full_scope
from . import production_capacity_repair as capacity
from . import rpc_workload_governor as governor
from .collecting_ingestion import CollectingLiveEvidenceIngestionService
from .direct_solana import DirectSolanaIngestionPlane
from .ingestion import IngestionDecision, NormalizedSwap
from .launch_funding import CompleteLiveRiskCollectors
from .observation_store import ObservationEventStore


ARCHITECTURE_VERSION = "candidate-execution-evidence-plane-v1"
CANDIDATE_EXECUTION_WORKERS = 2
CANDIDATE_EXECUTION_QUEUE_MAX = 128
CANDIDATE_PROCESSING_TARGET_SECONDS = float(forward.LATENCY_BUDGET_SECONDS)
CANDIDATE_ENTRY_WINDOW_SECONDS = float(forward.ENTRY_WINDOW_SECONDS)
BACKGROUND_SQLITE_SLICE_ROWS = 16
CANDIDATE_SQLITE_SLICE_ROWS = 4
CANDIDATE_STORAGE_YIELD_SECONDS = 0.002

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
FULL_MARKET_OBSERVATION_REDUCED = False
CERTIFICATION_THRESHOLDS_CHANGED = False

_ORIGINAL_DIRECT_INIT: Callable[..., Any] | None = None
_ORIGINAL_DIRECT_RUN: Callable[..., Any] | None = None
_ORIGINAL_DIRECT_HYDRATE: Callable[..., Any] | None = None
_ORIGINAL_SERVICE_INGEST: Callable[..., Any] | None = None
_ORIGINAL_FULL_SCOPE_BATCH: Callable[..., int] | None = None
_ORIGINAL_REFRESH_COVERAGE: Callable[..., Any] | None = None
_ORIGINAL_MARK_FUNDING: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None

_CANDIDATE_EXECUTION_CONTEXT: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "roi_candidate_execution_evidence_plane", default=False
)
_DEFERRED_FUNDING_MARK: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "roi_candidate_execution_deferred_funding_mark", default=None
)
_STORAGE_PRESSURE = threading.Event()

_SNAPSHOT_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS candidate_execution_plane_snapshots ("
    "signature TEXT PRIMARY KEY, token_mint TEXT NOT NULL, wallet TEXT NOT NULL, "
    "trigger_observed_at TEXT NOT NULL, trigger_received_at TEXT NOT NULL, "
    "queued_at TEXT NOT NULL, started_at TEXT NOT NULL, completed_at TEXT NOT NULL, "
    "queue_wait_ms REAL NOT NULL, end_to_end_ms REAL NOT NULL, decision TEXT, reason TEXT, "
    "risk_complete INTEGER NOT NULL, risk_fresh INTEGER NOT NULL, timed_out INTEGER NOT NULL, "
    "error_type TEXT, architecture_version TEXT NOT NULL, paper_only INTEGER NOT NULL, "
    "live_money_authority INTEGER NOT NULL)"
)


@dataclass(slots=True)
class CandidateExecutionJob:
    swap: NormalizedSwap
    reason: str
    queued_at: datetime
    queued_monotonic: float


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _inc(obj: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_candidate_execution_plane_{name}"
    setattr(obj, attr, int(getattr(obj, attr, 0) or 0) + int(amount))


def _set_max(obj: Any, name: str, value: float) -> None:
    attr = f"_roi_candidate_execution_plane_{name}"
    setattr(obj, attr, max(float(getattr(obj, attr, 0.0) or 0.0), float(value)))


def _candidate_queue(plane: Any) -> asyncio.PriorityQueue[tuple[float, int, CandidateExecutionJob]]:
    queue = getattr(plane, "_roi_candidate_execution_plane_queue", None)
    if isinstance(queue, asyncio.PriorityQueue):
        return queue
    queue = asyncio.PriorityQueue(maxsize=CANDIDATE_EXECUTION_QUEUE_MAX)
    setattr(plane, "_roi_candidate_execution_plane_queue", queue)
    return queue


def _next_sequence(plane: Any) -> int:
    value = int(getattr(plane, "_roi_candidate_execution_plane_sequence", 0) or 0) + 1
    setattr(plane, "_roi_candidate_execution_plane_sequence", value)
    return value


def _active_workers(plane: Any) -> int:
    return int(getattr(plane, "_roi_candidate_execution_plane_active", 0) or 0)


def _set_active_workers(plane: Any, value: int) -> None:
    normalized = max(0, int(value))
    setattr(plane, "_roi_candidate_execution_plane_active", normalized)
    _set_max(plane, "max_active", float(normalized))


def _hydration_active(plane: Any) -> int:
    return int(getattr(plane, "_roi_candidate_execution_plane_hydration_active", 0) or 0)


def _set_hydration_active(plane: Any, value: int) -> None:
    setattr(plane, "_roi_candidate_execution_plane_hydration_active", max(0, int(value)))


def _maybe_clear_storage_pressure(plane: Any) -> None:
    queue = getattr(plane, "_roi_candidate_execution_plane_queue", None)
    queued = int(queue.qsize()) if isinstance(queue, asyncio.PriorityQueue) else 0
    if queued == 0 and _active_workers(plane) == 0 and _hydration_active(plane) == 0:
        _STORAGE_PRESSURE.clear()


def _is_direct_candidate_call(service: Any, swap: NormalizedSwap) -> tuple[bool, Any | None, str]:
    plane = getattr(service, "_roi_candidate_execution_plane", None)
    if plane is None:
        return False, None, ""
    reason = str(candidate_hotpath._CURRENT_HYDRATION_REASON.get() or "")
    if reason not in forward.SCOUT_REASONS:
        return False, plane, reason
    if str(getattr(swap, "side", "") or "").lower() != "buy":
        return False, plane, reason
    wallet = str(getattr(swap, "wallet", "") or "")
    if not wallet or wallet not in set(getattr(plane, "scout_wallets", ()) or ()):
        return False, plane, reason
    return True, plane, reason


async def _route_candidate_ingest(
    self: CollectingLiveEvidenceIngestionService,
    swap: NormalizedSwap,
) -> IngestionDecision:
    if _ORIGINAL_SERVICE_INGEST is None:
        raise RuntimeError("candidate execution-evidence plane is not installed")
    eligible, plane, reason = _is_direct_candidate_call(self, swap)
    if not eligible or plane is None:
        return await _ORIGINAL_SERVICE_INGEST(self, swap)

    queue = _candidate_queue(plane)
    now = _utcnow()
    job = CandidateExecutionJob(
        swap=swap,
        reason=reason,
        queued_at=now,
        queued_monotonic=time.monotonic(),
    )
    sequence = _next_sequence(plane)
    _STORAGE_PRESSURE.set()
    await queue.put((swap.observed_at.timestamp(), sequence, job))
    _inc(plane, "queued")
    _set_max(plane, "max_queue_depth", float(queue.qsize()))

    # DirectSolanaIngestionPlane ignores the returned decision. Do not synchronously
    # append an interim decision here: the dedicated worker owns the one real
    # ingestion/risk/quote decision and its durable evidence.
    return IngestionDecision(
        signature=swap.signature,
        token_mint=swap.token_mint,
        wallet=swap.wallet,
        decision="candidate_execution_queued",
        reason="normalized frozen-scout candidate handed to isolated execution-evidence plane",
        observed_at=swap.observed_at,
        ingestion_latency_ms=swap.ingestion_latency_ms,
    )


setattr(_route_candidate_ingest, "_roi_candidate_execution_evidence_plane", True)


def _direct_init_with_candidate_plane(
    self: DirectSolanaIngestionPlane,
    *args: Any,
    **kwargs: Any,
) -> None:
    if _ORIGINAL_DIRECT_INIT is None:
        raise RuntimeError("candidate execution-evidence plane init is not installed")
    _ORIGINAL_DIRECT_INIT(self, *args, **kwargs)
    # Production uses a mutable CollectingLiveEvidenceIngestionService. A few
    # low-level transport tests intentionally pass a bare object() because they do
    # not exercise ingestion. Keep those immutable test doubles on the original
    # path instead of making candidate-plane attachment a constructor requirement.
    try:
        setattr(self.service, "_roi_candidate_execution_plane", self)
    except (AttributeError, TypeError):
        pass


setattr(_direct_init_with_candidate_plane, "_roi_candidate_execution_evidence_plane", True)


async def _hydrate_with_candidate_storage_priority(
    self: DirectSolanaIngestionPlane,
    row: dict[str, Any],
) -> None:
    if _ORIGINAL_DIRECT_HYDRATE is None:
        raise RuntimeError("candidate execution-evidence hydration wrapper is not installed")
    reason = str(row.get("reason") or "")
    try:
        priority = int(row.get("priority") or 0)
    except (TypeError, ValueError):
        priority = 0
    candidate = reason in forward.SCOUT_REASONS and priority <= 2
    if candidate:
        _STORAGE_PRESSURE.set()
        _set_hydration_active(self, _hydration_active(self) + 1)
        _inc(self, "candidate_hydrations_prioritized")
    try:
        await _ORIGINAL_DIRECT_HYDRATE(self, row)
    finally:
        if candidate:
            _set_hydration_active(self, _hydration_active(self) - 1)
            _maybe_clear_storage_pressure(self)


setattr(_hydrate_with_candidate_storage_priority, "_roi_candidate_execution_evidence_plane", True)


def _risk_refresh_exists(store: Any, job: CandidateExecutionJob) -> bool:
    try:
        with store._lock:
            row = store.db.execute(
                "SELECT 1 FROM risk_refresh_measurements WHERE token_mint=? AND trigger_observed_at=? LIMIT 1",
                (job.swap.token_mint, job.swap.observed_at.isoformat()),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _record_terminal_failure_sync(
    plane: Any,
    job: CandidateExecutionJob,
    *,
    started_at: datetime,
    completed_at: datetime,
    failure_type: str,
) -> None:
    store = plane.store
    if _risk_refresh_exists(store, job):
        return
    elapsed_ms = max(0.0, (completed_at - started_at).total_seconds() * 1000.0)
    store.record_risk_refresh(
        token_mint=job.swap.token_mint,
        trigger_observed_at=job.swap.observed_at.isoformat(),
        trigger_received_at=job.swap.received_at.isoformat(),
        started_at=started_at.isoformat(),
        completed_at=completed_at.isoformat(),
        elapsed_ms=elapsed_ms,
        ingestion_latency_ms=job.swap.ingestion_latency_ms,
        end_to_end_ms=max(
            0.0,
            (completed_at - job.swap.observed_at).total_seconds() * 1000.0,
        ),
        complete=False,
        fresh=False,
        readiness={
            "complete": False,
            "fresh": False,
            "candidate_execution_plane_terminal_failure": True,
            "failure_type": failure_type,
            "candidate_processing_target_seconds": CANDIDATE_PROCESSING_TARGET_SECONDS,
            "candidate_entry_window_seconds": CANDIDATE_ENTRY_WINDOW_SECONDS,
            "certification_thresholds_unchanged": True,
            "paper_only": True,
            "live_money_authority": False,
        },
    )


def _persist_snapshot_sync(
    plane: Any,
    job: CandidateExecutionJob,
    *,
    started_at: datetime,
    completed_at: datetime,
    queue_wait_ms: float,
    decision: IngestionDecision | None,
    timed_out: bool,
    error_type: str | None,
) -> None:
    readiness: dict[str, Any] = {"complete": False, "fresh": False}
    readiness_fn = getattr(getattr(plane.service, "risk_provider", None), "readiness", None)
    if callable(readiness_fn):
        try:
            raw = readiness_fn(job.swap.token_mint, as_of=completed_at)
            if isinstance(raw, dict):
                readiness = raw
        except Exception:
            pass

    with plane.store._lock, plane.store.db:
        plane.store.db.execute(_SNAPSHOT_TABLE_SQL)
        plane.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_candidate_execution_plane_completed "
            "ON candidate_execution_plane_snapshots(completed_at)"
        )
        plane.store.db.execute(
            "INSERT OR IGNORE INTO candidate_execution_plane_snapshots("
            "signature,token_mint,wallet,trigger_observed_at,trigger_received_at,queued_at,"
            "started_at,completed_at,queue_wait_ms,end_to_end_ms,decision,reason,risk_complete,"
            "risk_fresh,timed_out,error_type,architecture_version,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                job.swap.signature,
                job.swap.token_mint,
                job.swap.wallet,
                job.swap.observed_at.isoformat(),
                job.swap.received_at.isoformat(),
                job.queued_at.isoformat(),
                started_at.isoformat(),
                completed_at.isoformat(),
                float(queue_wait_ms),
                max(0.0, (completed_at - job.swap.observed_at).total_seconds() * 1000.0),
                str(getattr(decision, "decision", "") or "") or None,
                str(getattr(decision, "reason", "") or "")[:1000] or None,
                1 if bool(readiness.get("complete")) else 0,
                1 if bool(readiness.get("fresh")) else 0,
                1 if timed_out else 0,
                error_type,
                ARCHITECTURE_VERSION,
                1,
                0,
            ),
        )


def _snapshot_tasks(plane: Any) -> set[asyncio.Task[Any]]:
    value = getattr(plane, "_roi_candidate_execution_plane_snapshot_tasks", None)
    if isinstance(value, set):
        return value
    value = set()
    setattr(plane, "_roi_candidate_execution_plane_snapshot_tasks", value)
    return value


def _schedule_snapshot(
    plane: Any,
    job: CandidateExecutionJob,
    *,
    started_at: datetime,
    completed_at: datetime,
    queue_wait_ms: float,
    decision: IngestionDecision | None,
    timed_out: bool,
    error_type: str | None,
) -> None:
    task = asyncio.create_task(
        asyncio.to_thread(
            _persist_snapshot_sync,
            plane,
            job,
            started_at=started_at,
            completed_at=completed_at,
            queue_wait_ms=queue_wait_ms,
            decision=decision,
            timed_out=timed_out,
            error_type=error_type,
        ),
        name=f"candidate-snapshot:{job.swap.signature[:10]}",
    )
    tasks = _snapshot_tasks(plane)
    tasks.add(task)
    _inc(plane, "snapshots_scheduled")

    def done(completed: asyncio.Task[Any]) -> None:
        tasks.discard(completed)
        try:
            exc = completed.exception()
        except asyncio.CancelledError:
            _inc(plane, "snapshot_cancelled")
            return
        except BaseException:
            _inc(plane, "snapshot_errors")
            return
        if exc is None:
            _inc(plane, "snapshots_persisted")
        else:
            _inc(plane, "snapshot_errors")

    task.add_done_callback(done)


async def _candidate_execution_worker(
    plane: DirectSolanaIngestionPlane,
    stop: asyncio.Event,
    worker_index: int,
) -> None:
    queue = _candidate_queue(plane)
    while not stop.is_set():
        try:
            _priority, _sequence, job = await asyncio.wait_for(queue.get(), timeout=0.25)
        except asyncio.TimeoutError:
            continue

        started_at = _utcnow()
        queue_wait_ms = max(0.0, (time.monotonic() - job.queued_monotonic) * 1000.0)
        _set_max(plane, "max_queue_wait_ms", queue_wait_ms)
        _set_active_workers(plane, _active_workers(plane) + 1)
        _inc(plane, "started")
        _STORAGE_PRESSURE.set()

        reason_token = candidate_hotpath._CURRENT_HYDRATION_REASON.set(job.reason)
        trigger_token = forward._CURRENT_TRIGGER_AT.set(job.swap.received_at)
        execution_token = _CANDIDATE_EXECUTION_CONTEXT.set(True)
        decision: IngestionDecision | None = None
        timed_out = False
        error_type: str | None = None
        try:
            age_seconds = max(0.0, (started_at - job.swap.observed_at).total_seconds())
            remaining = CANDIDATE_ENTRY_WINDOW_SECONDS - age_seconds
            if remaining <= 0.0:
                timed_out = True
                error_type = "candidate_entry_window_expired_before_execution"
                _inc(plane, "expired_before_start")
            elif _ORIGINAL_SERVICE_INGEST is None:
                error_type = "candidate_execution_delegate_unavailable"
                _inc(plane, "errors")
            else:
                try:
                    with governor.rpc_workload(candidate_priority.WORKLOAD_CANDIDATE):
                        decision = await asyncio.wait_for(
                            _ORIGINAL_SERVICE_INGEST(plane.service, job.swap),
                            timeout=remaining,
                        )
                    _inc(plane, "completed")
                except asyncio.TimeoutError:
                    timed_out = True
                    error_type = "candidate_entry_window_hard_timeout"
                    _inc(plane, "hard_timeouts")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    error_type = type(exc).__name__
                    _inc(plane, "errors")

            completed_at = _utcnow()
            _set_max(
                plane,
                "max_end_to_end_ms",
                max(0.0, (completed_at - job.swap.observed_at).total_seconds() * 1000.0),
            )
            if timed_out or error_type is not None:
                try:
                    await asyncio.to_thread(
                        _record_terminal_failure_sync,
                        plane,
                        job,
                        started_at=started_at,
                        completed_at=completed_at,
                        failure_type=error_type or "candidate_execution_failed",
                    )
                    _inc(plane, "terminal_failures_accounted")
                except asyncio.CancelledError:
                    raise
                except Exception:
                    _inc(plane, "terminal_failure_accounting_errors")

            _schedule_snapshot(
                plane,
                job,
                started_at=started_at,
                completed_at=completed_at,
                queue_wait_ms=queue_wait_ms,
                decision=decision,
                timed_out=timed_out,
                error_type=error_type,
            )
        finally:
            _CANDIDATE_EXECUTION_CONTEXT.reset(execution_token)
            forward._CURRENT_TRIGGER_AT.reset(trigger_token)
            candidate_hotpath._CURRENT_HYDRATION_REASON.reset(reason_token)
            _set_active_workers(plane, _active_workers(plane) - 1)
            queue.task_done()
            _maybe_clear_storage_pressure(plane)


async def _run_with_candidate_execution_plane(
    self: DirectSolanaIngestionPlane,
    stop: asyncio.Event,
) -> None:
    if _ORIGINAL_DIRECT_RUN is None:
        raise RuntimeError("candidate execution-evidence run wrapper is not installed")
    _candidate_queue(self)
    workers = [
        asyncio.create_task(
            _candidate_execution_worker(self, stop, index),
            name=f"candidate-execution-evidence:{index}",
        )
        for index in range(CANDIDATE_EXECUTION_WORKERS)
    ]
    try:
        await _ORIGINAL_DIRECT_RUN(self, stop)
    finally:
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        _maybe_clear_storage_pressure(self)


setattr(_run_with_candidate_execution_plane, "_roi_candidate_execution_evidence_plane", True)


def _batch_contains_scout(items: list[Any]) -> bool:
    for item in items:
        try:
            fields = capacity._dispatch_fields(item)
        except Exception:
            continue
        if fields is None:
            continue
        target = fields[0]
        if str(getattr(target, "kind", "") or "") == "scout":
            return True
    return False


def _persist_full_scope_with_storage_slices(self: Any, items: list[Any]) -> int:
    """Bound one SQLite writer hold while preserving receipt order and durability.

    The dispatcher still drains at the unchanged 128-receipt bound. Only the
    persistence transaction is divided into ordered durable slices so a candidate
    can acquire the shared store lock between bulk commits. If a scout is already in
    the batch, use the candidate-sized slice from the first row so it cannot sit
    behind a long bulk transaction.
    """

    if _ORIGINAL_FULL_SCOPE_BATCH is None:
        raise RuntimeError("candidate storage slicing is not installed")
    if not items:
        return 0

    candidate_pressure = bool(_STORAGE_PRESSURE.is_set() or _batch_contains_scout(items))
    slice_rows = CANDIDATE_SQLITE_SLICE_ROWS if candidate_pressure else BACKGROUND_SQLITE_SLICE_ROWS
    inserted = 0
    for start in range(0, len(items), slice_rows):
        chunk = items[start : start + slice_rows]
        started = time.perf_counter()
        inserted += int(_ORIGINAL_FULL_SCOPE_BATCH(self, chunk))
        elapsed_ms = max(0.0, (time.perf_counter() - started) * 1000.0)
        _inc(self, "storage_slices")
        if candidate_pressure:
            _inc(self, "candidate_storage_slices")
        _set_max(self, "max_storage_slice_ms", elapsed_ms)
        _set_max(self, "max_storage_slice_rows", float(len(chunk)))
        if candidate_pressure and start + slice_rows < len(items):
            time.sleep(CANDIDATE_STORAGE_YIELD_SECONDS)
    return inserted


setattr(_persist_full_scope_with_storage_slices, "_roi_candidate_execution_evidence_plane", True)


def _mark_funding_with_candidate_defer(
    self: ObservationEventStore,
    token_mint: str,
    *,
    assessed_at: str,
) -> None:
    state = _DEFERRED_FUNDING_MARK.get()
    if state is not None:
        state["token_mint"] = token_mint
        state["assessed_at"] = assessed_at
        state["requested"] = True
        return
    if _ORIGINAL_MARK_FUNDING is None:
        raise RuntimeError("candidate funding mark wrapper is not installed")
    _ORIGINAL_MARK_FUNDING(self, token_mint, assessed_at=assessed_at)


setattr(_mark_funding_with_candidate_defer, "_roi_candidate_execution_evidence_plane", True)


async def _refresh_coverage_with_candidate_fanout(
    self: CompleteLiveRiskCollectors,
    mint: str,
    at: datetime,
    *,
    current_swap: Any = None,
) -> None:
    if _ORIGINAL_REFRESH_COVERAGE is None:
        raise RuntimeError("candidate six-dimension fanout is not installed")
    if not _CANDIDATE_EXECUTION_CONTEXT.get() or current_swap is None:
        await _ORIGINAL_REFRESH_COVERAGE(self, mint, at, current_swap=current_swap)
        return
    if not bool(getattr(self, "coverage_asserted", False)) or getattr(self, "launch", None) is None:
        await _ORIGINAL_REFRESH_COVERAGE(self, mint, at, current_swap=current_swap)
        return

    state: dict[str, Any] = {}
    token = _DEFERRED_FUNDING_MARK.set(state)
    try:
        launch_call = self._safe_bool("launch", mint, at, self.launch.collect(mint, at))
        funding = getattr(self, "funding", None)
        if funding is None:
            launch_ok = bool(await launch_call)
            funding_ok = False
        else:
            launch_ok, funding_ok = await asyncio.gather(
                launch_call,
                self._safe_bool("funding", mint, at, funding.collect(mint, at)),
            )
            launch_ok = bool(launch_ok)
            funding_ok = bool(funding_ok)
    finally:
        _DEFERRED_FUNDING_MARK.reset(token)

    _inc(self, "candidate_coverage_fanouts")
    if launch_ok:
        _inc(self, "candidate_launch_complete")
    if funding_ok:
        _inc(self, "candidate_funding_complete")

    # Funding is independent RPC work, but its coverage-complete marker must remain
    # ordered after the launch coverage row. The wrapper above suppresses the mark
    # inside the concurrent funding task; publish it exactly once after both tasks
    # finish and only when launch coverage also succeeded.
    if launch_ok and funding_ok and _ORIGINAL_MARK_FUNDING is not None:
        _ORIGINAL_MARK_FUNDING(self.risk.store, mint, assessed_at=at.isoformat())
        _inc(self, "candidate_funding_marks_finalized")


setattr(_refresh_coverage_with_candidate_fanout, "_roi_candidate_execution_evidence_plane", True)


def _status_with_candidate_execution_plane(
    self: DirectSolanaIngestionPlane,
) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("candidate execution-evidence status is not installed")
    payload = _ORIGINAL_STATUS(self)
    queue = getattr(self, "_roi_candidate_execution_plane_queue", None)
    raw = getattr(getattr(getattr(self, "service", None), "collectors", None), "inner", None)
    payload["candidate_execution_evidence_plane"] = {
        "installed": True,
        "version": ARCHITECTURE_VERSION,
        "architecture": "hydrate->isolated-candidate-queue->six-dimension-risk->quote->unsigned-simulation",
        "candidate_execution_workers": CANDIDATE_EXECUTION_WORKERS,
        "queue_max": CANDIDATE_EXECUTION_QUEUE_MAX,
        "queue_depth": int(queue.qsize()) if isinstance(queue, asyncio.PriorityQueue) else 0,
        "queued_session": int(getattr(self, "_roi_candidate_execution_plane_queued", 0) or 0),
        "started_session": int(getattr(self, "_roi_candidate_execution_plane_started", 0) or 0),
        "completed_session": int(getattr(self, "_roi_candidate_execution_plane_completed", 0) or 0),
        "hard_timeouts_session": int(getattr(self, "_roi_candidate_execution_plane_hard_timeouts", 0) or 0),
        "errors_session": int(getattr(self, "_roi_candidate_execution_plane_errors", 0) or 0),
        "active": _active_workers(self),
        "max_active": int(getattr(self, "_roi_candidate_execution_plane_max_active", 0) or 0),
        "max_queue_depth": int(getattr(self, "_roi_candidate_execution_plane_max_queue_depth", 0) or 0),
        "max_queue_wait_ms": float(getattr(self, "_roi_candidate_execution_plane_max_queue_wait_ms", 0.0) or 0.0),
        "max_end_to_end_ms": float(getattr(self, "_roi_candidate_execution_plane_max_end_to_end_ms", 0.0) or 0.0),
        "candidate_hydration_released_before_risk_quote": True,
        "candidate_rpc_workload": candidate_priority.WORKLOAD_CANDIDATE,
        "candidate_rpc_cannot_consume_continuity_reserve": True,
        "six_dimension_candidate_fanout": True,
        "launch_and_funding_overlap_on_candidate_path": True,
        "dynamic_authority_liquidity_flow_deployer_parallelism_preserved": True,
        "candidate_coverage_fanouts_session": int(
            getattr(raw, "_roi_candidate_execution_plane_candidate_coverage_fanouts", 0) or 0
        ),
        "candidate_launch_complete_session": int(
            getattr(raw, "_roi_candidate_execution_plane_candidate_launch_complete", 0) or 0
        ),
        "candidate_funding_complete_session": int(
            getattr(raw, "_roi_candidate_execution_plane_candidate_funding_complete", 0) or 0
        ),
        "candidate_processing_target_seconds_unchanged": CANDIDATE_PROCESSING_TARGET_SECONDS,
        "candidate_entry_window_seconds_unchanged": CANDIDATE_ENTRY_WINDOW_SECONDS,
        "snapshot_persistence_off_event_loop": True,
        "snapshots_scheduled_session": int(getattr(self, "_roi_candidate_execution_plane_snapshots_scheduled", 0) or 0),
        "snapshots_persisted_session": int(getattr(self, "_roi_candidate_execution_plane_snapshots_persisted", 0) or 0),
        "background_sqlite_slice_rows": BACKGROUND_SQLITE_SLICE_ROWS,
        "candidate_sqlite_slice_rows": CANDIDATE_SQLITE_SLICE_ROWS,
        "storage_pressure_active": bool(_STORAGE_PRESSURE.is_set()),
        "storage_slices_session": int(getattr(self, "_roi_candidate_execution_plane_storage_slices", 0) or 0),
        "candidate_storage_slices_session": int(
            getattr(self, "_roi_candidate_execution_plane_candidate_storage_slices", 0) or 0
        ),
        "max_storage_slice_ms": float(getattr(self, "_roi_candidate_execution_plane_max_storage_slice_ms", 0.0) or 0.0),
        "full_scope_dispatch_batch_max_unchanged": int(capacity.RAW_RECEIPT_BATCH_MAX),
        "full_scope_receipt_order_preserved": True,
        "raw_receipt_drops_allowed": False,
        "full_market_observation_reduced": False,
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
                "candidate_execution_evidence_plane_isolated": True,
                "candidate_hydration_released_before_risk_quote": True,
                "candidate_six_dimension_deadline_fanout": True,
                "bulk_sqlite_writer_hold_sliced_for_candidate_fairness": True,
                "candidate_snapshot_persistence_off_event_loop": True,
                "candidate_rpc_priority_preserved": True,
                "candidate_latency_threshold_unchanged": True,
                "entry_window_seconds_unchanged": True,
                "full_raw_market_scope_preserved": True,
                "raw_receipt_drops_allowed": False,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    return payload


setattr(_status_with_candidate_execution_plane, "_roi_candidate_execution_evidence_plane", True)


def install_candidate_execution_evidence_plane() -> None:
    """Install the final production candidate plane after all earlier repairs.

    This is a scheduling/storage architecture change only. It does not alter wallet
    authority, venue/lifecycle routing, chase/entry thresholds, certification gates,
    full-market observation, signing/submission capability, or paper-only authority.
    """

    global _ORIGINAL_DIRECT_INIT, _ORIGINAL_DIRECT_RUN, _ORIGINAL_DIRECT_HYDRATE
    global _ORIGINAL_SERVICE_INGEST, _ORIGINAL_FULL_SCOPE_BATCH
    global _ORIGINAL_REFRESH_COVERAGE, _ORIGINAL_MARK_FUNDING, _ORIGINAL_STATUS

    current_init = DirectSolanaIngestionPlane.__init__
    if not bool(getattr(current_init, "_roi_candidate_execution_evidence_plane", False)):
        _ORIGINAL_DIRECT_INIT = current_init
        try:
            _direct_init_with_candidate_plane.__dict__.update(getattr(current_init, "__dict__", {}))
        except Exception:
            pass
        DirectSolanaIngestionPlane.__init__ = _direct_init_with_candidate_plane  # type: ignore[method-assign]

    current_ingest = CollectingLiveEvidenceIngestionService.ingest_swap
    if not bool(getattr(current_ingest, "_roi_candidate_execution_evidence_plane", False)):
        _ORIGINAL_SERVICE_INGEST = current_ingest
        try:
            _route_candidate_ingest.__dict__.update(getattr(current_ingest, "__dict__", {}))
        except Exception:
            pass
        CollectingLiveEvidenceIngestionService.ingest_swap = _route_candidate_ingest  # type: ignore[method-assign]

    current_hydrate = DirectSolanaIngestionPlane._hydrate_one
    if not bool(getattr(current_hydrate, "_roi_candidate_execution_evidence_plane", False)):
        _ORIGINAL_DIRECT_HYDRATE = current_hydrate
        try:
            _hydrate_with_candidate_storage_priority.__dict__.update(
                getattr(current_hydrate, "__dict__", {})
            )
        except Exception:
            pass
        DirectSolanaIngestionPlane._hydrate_one = _hydrate_with_candidate_storage_priority  # type: ignore[method-assign]

    current_run = DirectSolanaIngestionPlane.run
    if not bool(getattr(current_run, "_roi_candidate_execution_evidence_plane", False)):
        _ORIGINAL_DIRECT_RUN = current_run
        try:
            _run_with_candidate_execution_plane.__dict__.update(getattr(current_run, "__dict__", {}))
        except Exception:
            pass
        DirectSolanaIngestionPlane.run = _run_with_candidate_execution_plane  # type: ignore[method-assign]

    current_batch = full_scope._persist_full_scope_batch
    if not bool(getattr(current_batch, "_roi_candidate_execution_evidence_plane", False)):
        _ORIGINAL_FULL_SCOPE_BATCH = current_batch
        try:
            _persist_full_scope_with_storage_slices.__dict__.update(
                getattr(current_batch, "__dict__", {})
            )
        except Exception:
            pass
        full_scope._persist_full_scope_batch = _persist_full_scope_with_storage_slices  # type: ignore[assignment]

    current_mark = ObservationEventStore.mark_program_coverage_funding_complete
    if not bool(getattr(current_mark, "_roi_candidate_execution_evidence_plane", False)):
        _ORIGINAL_MARK_FUNDING = current_mark
        try:
            _mark_funding_with_candidate_defer.__dict__.update(getattr(current_mark, "__dict__", {}))
        except Exception:
            pass
        ObservationEventStore.mark_program_coverage_funding_complete = _mark_funding_with_candidate_defer  # type: ignore[method-assign]

    current_coverage = CompleteLiveRiskCollectors.refresh_coverage
    if not bool(getattr(current_coverage, "_roi_candidate_execution_evidence_plane", False)):
        _ORIGINAL_REFRESH_COVERAGE = current_coverage
        try:
            _refresh_coverage_with_candidate_fanout.__dict__.update(
                getattr(current_coverage, "__dict__", {})
            )
        except Exception:
            pass
        CompleteLiveRiskCollectors.refresh_coverage = _refresh_coverage_with_candidate_fanout  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_candidate_execution_evidence_plane", False)):
        _ORIGINAL_STATUS = current_status
        try:
            _status_with_candidate_execution_plane.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        DirectSolanaIngestionPlane.status = _status_with_candidate_execution_plane  # type: ignore[method-assign]


__all__ = [
    "ARCHITECTURE_VERSION",
    "BACKGROUND_SQLITE_SLICE_ROWS",
    "CANDIDATE_ENTRY_WINDOW_SECONDS",
    "CANDIDATE_EXECUTION_QUEUE_MAX",
    "CANDIDATE_EXECUTION_WORKERS",
    "CANDIDATE_PROCESSING_TARGET_SECONDS",
    "CANDIDATE_SQLITE_SLICE_ROWS",
    "_candidate_execution_worker",
    "_persist_full_scope_with_storage_slices",
    "_refresh_coverage_with_candidate_fanout",
    "_route_candidate_ingest",
    "install_candidate_execution_evidence_plane",
]
