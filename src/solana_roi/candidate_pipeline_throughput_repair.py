from __future__ import annotations

import asyncio
import math
import time
from collections import Counter, deque
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Callable

from . import candidate_certification_hotpath_repair as candidate_hotpath
from . import candidate_completion_continuity_repair as completion
from . import candidate_risk_quote_v4_handoff as handoff
from . import candidate_rpc_priority_repair as candidate_priority
from . import continuity_standby_rpc_priority_repair as standby_priority
from . import forward_evidence_runtime_repair as forward
from . import rpc_workload_governor as governor
from . import semantic_candidate_attribution_architecture as semantic
from . import venue_native_candidate_graph_repair as venue
from .direct_solana import DirectSolanaIngestionPlane
from .solana_rpc import RpcEndpoint

REPAIR_VERSION = "candidate-pipeline-throughput-v1"
SAMPLE_LIMIT = 512
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
STRATEGY_THRESHOLDS_CHANGED = False
CERTIFICATION_THRESHOLDS_CHANGED = False

_PROVIDER_TIMEOUT_SECONDS: ContextVar[float | None] = ContextVar(
    "roi_candidate_provider_timeout_seconds",
    default=None,
)

_ORIGINAL_GOVERNOR_ENDPOINT_DELEGATE: Callable[..., Any] | None = None
_ORIGINAL_GET_TRANSACTION_READY: Callable[..., Any] | None = None
_ORIGINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_HANDOFF: Callable[..., Any] | None = None
_INSTALLED = False

_WAKE_EVENTS: dict[tuple[int, str], asyncio.Event] = {}
_SLOT_WAIT_SAMPLES: dict[tuple[str, str], deque[float]] = {}
_PROVIDER_SAMPLES: dict[tuple[str, str], deque[float]] = {}
_PROVIDER_TIMEOUTS: Counter[tuple[str, str]] = Counter()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _sample(
    mapping: dict[tuple[str, str], deque[float]],
    endpoint_key: str,
    workload: str,
    value_ms: float,
) -> None:
    key = (str(endpoint_key), str(workload))
    bucket = mapping.get(key)
    if bucket is None:
        bucket = deque(maxlen=SAMPLE_LIMIT)
        mapping[key] = bucket
    bucket.append(max(0.0, float(value_ms)))


def _samples_for(
    mapping: dict[tuple[str, str], deque[float]],
    endpoint_key: str,
    workload: str,
) -> dict[str, Any]:
    values = list(mapping.get((str(endpoint_key), str(workload)), ()))
    return {
        "sample_count": len(values),
        "p50_ms": _percentile(values, 0.50),
        "p95_ms": _percentile(values, 0.95),
        "p99_ms": _percentile(values, 0.99),
        "max_ms": max(values) if values else None,
    }


def _plane_samples(plane: Any) -> deque[float]:
    value = getattr(plane, "_roi_candidate_pipeline_total_samples_ms", None)
    if isinstance(value, deque):
        return value
    value = deque(maxlen=SAMPLE_LIMIT)
    setattr(plane, "_roi_candidate_pipeline_total_samples_ms", value)
    return value


def _inc(plane: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_candidate_pipeline_{name}"
    setattr(plane, attr, int(getattr(plane, attr, 0) or 0) + int(amount))


def _wake_event(state: Any) -> asyncio.Event:
    key = (int(state.loop_id), str(state.endpoint_key))
    event = _WAKE_EVENTS.get(key)
    if event is None:
        event = asyncio.Event()
        _WAKE_EVENTS[key] = event
    return event


def _noncritical_active(state: Any) -> int:
    return sum(
        int(state.active_by_workload.get(name, 0) or 0)
        for name in (
            governor.WORKLOAD_CERTIFICATION,
            governor.WORKLOAD_RESEARCH,
            candidate_priority.WORKLOAD_CANDIDATE,
            standby_priority.WORKLOAD_STANDBY,
        )
    )


def _candidate_first_allowed(
    state: Any,
    workload: str,
    policy: dict[str, float | int],
) -> tuple[bool, float]:
    """Give live candidates first claim on existing noncritical RPC capacity.

    The one-slot continuity-critical reservation and the configured endpoint ceiling
    remain unchanged. Candidate work may use either existing noncritical slot when
    it is actually waiting; standby then outranks certification/research, and
    research remains rate-limited.
    """

    total = int(policy["total_per_endpoint"])
    noncritical_ceiling = int(policy["noncritical_ceiling_per_endpoint"])
    research_max = int(policy["research_max_per_endpoint"])
    now = time.monotonic()

    if workload == governor.WORKLOAD_CRITICAL:
        return state.active_total < total, 0.0

    noncritical_active = _noncritical_active(state)
    if state.active_total >= total or noncritical_active >= noncritical_ceiling:
        return False, 0.05

    candidate_waiters = candidate_priority._candidate_waiters(state)
    standby_waiters = standby_priority._standby_waiters(state)
    standby_active = int(
        state.active_by_workload.get(standby_priority.WORKLOAD_STANDBY, 0) or 0
    )

    if workload == candidate_priority.WORKLOAD_CANDIDATE:
        return True, 0.0

    # Do not let background work reserve the second noncritical slot while another
    # live candidate is waiting. This is scheduling only; total RPC capacity is
    # unchanged and the continuity-critical reservation remains unavailable.
    if candidate_waiters > 0:
        return False, 0.01

    if workload == standby_priority.WORKLOAD_STANDBY:
        return True, 0.0

    # When no candidate is waiting, keep one slot available to a waiting standby
    # cursor before ordinary certification/research work.
    if standby_waiters > 0 and standby_active == 0:
        background_ceiling = max(0, noncritical_ceiling - 1)
        if noncritical_active >= background_ceiling:
            return False, 0.02

    if workload == governor.WORKLOAD_RESEARCH:
        if (
            int(state.active_by_workload.get(governor.WORKLOAD_RESEARCH, 0) or 0)
            >= research_max
        ):
            return False, 0.05
        interval = float(policy["research_min_interval_seconds"])
        remaining = max(
            0.0,
            state.last_research_started_monotonic + interval - now,
        )
        if remaining > 0.0:
            return False, min(0.10, remaining)

    return True, 0.0


async def _event_driven_acquire(endpoint: RpcEndpoint, workload: str) -> Any:
    state = governor._state_for(endpoint)
    policy = governor._policy()
    candidate_registered = workload == candidate_priority.WORKLOAD_CANDIDATE
    standby_registered = workload == standby_priority.WORKLOAD_STANDBY
    if candidate_registered:
        candidate_priority._change_candidate_waiters(state, 1)
    if standby_registered:
        standby_priority._change_standby_waiters(state, 1)

    started = time.perf_counter()
    event = _wake_event(state)
    try:
        while True:
            suggested = 0.05
            async with state.lock:
                allowed, wait_hint = _candidate_first_allowed(state, workload, policy)
                if allowed:
                    if candidate_registered:
                        candidate_priority._change_candidate_waiters(state, -1)
                        candidate_registered = False
                    if standby_registered:
                        standby_priority._change_standby_waiters(state, -1)
                        standby_registered = False
                    state.active_total += 1
                    state.active_by_workload[workload] = int(
                        state.active_by_workload.get(workload, 0) or 0
                    ) + 1
                    state.requests_by_workload[workload] = int(
                        state.requests_by_workload.get(workload, 0) or 0
                    ) + 1
                    state.max_active_total = max(
                        state.max_active_total,
                        state.active_total,
                    )
                    if workload == governor.WORKLOAD_RESEARCH:
                        state.last_research_started_monotonic = time.monotonic()
                    _sample(
                        _SLOT_WAIT_SAMPLES,
                        state.endpoint_key,
                        workload,
                        (time.perf_counter() - started) * 1000.0,
                    )
                    return state

                state.waits_by_workload[workload] = int(
                    state.waits_by_workload.get(workload, 0) or 0
                ) + 1
                event.clear()
                if wait_hint > 0.0:
                    suggested = wait_hint

            try:
                await asyncio.wait_for(
                    event.wait(),
                    timeout=max(0.005, min(0.10, suggested)),
                )
            except asyncio.TimeoutError:
                # A timeout is only a scheduler wake-up fallback (for example the
                # research cadence); it is not a candidate RPC/provider timeout.
                pass
    finally:
        if candidate_registered:
            candidate_priority._change_candidate_waiters(state, -1)
        if standby_registered:
            standby_priority._change_standby_waiters(state, -1)


async def _release_and_wake(state: Any, workload: str) -> None:
    async with state.lock:
        state.active_total = max(0, int(state.active_total) - 1)
        state.active_by_workload[workload] = max(
            0,
            int(state.active_by_workload.get(workload, 0) or 0) - 1,
        )
        _wake_event(state).set()


async def _provider_delegate_with_after_slot_timeout(
    self: Any,
    endpoint: RpcEndpoint,
    method: str,
    params: list[Any],
) -> Any:
    if _ORIGINAL_GOVERNOR_ENDPOINT_DELEGATE is None:
        raise RuntimeError("candidate provider-timeout repair is not installed")

    workload = governor._effective_workload()
    timeout = _PROVIDER_TIMEOUT_SECONDS.get()
    started = time.perf_counter()
    try:
        if timeout is not None and timeout > 0.0:
            return await asyncio.wait_for(
                _ORIGINAL_GOVERNOR_ENDPOINT_DELEGATE(
                    self,
                    endpoint,
                    method,
                    params,
                ),
                timeout=float(timeout),
            )
        return await _ORIGINAL_GOVERNOR_ENDPOINT_DELEGATE(
            self,
            endpoint,
            method,
            params,
        )
    except asyncio.TimeoutError:
        _PROVIDER_TIMEOUTS[(str(endpoint.http_url), str(workload))] += 1
        raise
    finally:
        _sample(
            _PROVIDER_SAMPLES,
            str(endpoint.http_url),
            str(workload),
            (time.perf_counter() - started) * 1000.0,
        )


setattr(
    _provider_delegate_with_after_slot_timeout,
    "_roi_candidate_provider_after_slot_timeout",
    True,
)


async def _transaction_ready_after_slot_timeout(
    self: DirectSolanaIngestionPlane,
    signature: str,
    *,
    hedge: bool,
    attempts: int,
) -> tuple[Any, str | None, float | None]:
    if _ORIGINAL_GET_TRANSACTION_READY is None:
        raise RuntimeError("candidate transaction-ready repair is not installed")

    reason = candidate_hotpath._CURRENT_HYDRATION_REASON.get()
    trigger = forward._CURRENT_TRIGGER_AT.get()
    if reason not in forward.SCOUT_REASONS or trigger is None:
        return await _ORIGINAL_GET_TRANSACTION_READY(
            self,
            signature,
            hedge=hedge,
            attempts=attempts,
        )

    age = max(0.0, (_utcnow() - trigger).total_seconds())
    remaining_entry = max(0.0, float(forward.ENTRY_WINDOW_SECONDS) - age)
    if remaining_entry <= 0.0:
        completion._inc(self, "rpc_skipped_after_entry_window")
        _inc(self, "rpc_skipped_after_entry_window")
        return None, None, None

    remaining_fresh = float(forward.LATENCY_BUDGET_SECONDS) - age
    slice_seconds = (
        min(completion.CANDIDATE_FRESH_RPC_SLICE_SECONDS, max(0.15, remaining_fresh))
        if remaining_fresh > 0.0
        else min(completion.CANDIDATE_LATE_RPC_SLICE_SECONDS, remaining_entry)
    )
    slice_seconds = max(0.10, min(slice_seconds, remaining_entry))

    # Use the same canonical delegate selected by the prior repair, but move the
    # timeout into governor._ORIGINAL_CALL_ENDPOINT. That delegate is invoked only
    # after the RPC governor has granted an endpoint slot.
    delegate = (
        forward._ORIGINAL_GET_TRANSACTION_READY
        or completion._ORIGINAL_TRANSACTION_READY
    )
    if delegate is None:
        # Compatibility fallback only; production composition supplies one of the
        # canonical delegates above.
        delegate = _ORIGINAL_GET_TRANSACTION_READY

    started = time.perf_counter()
    token = _PROVIDER_TIMEOUT_SECONDS.set(slice_seconds)
    _inc(self, "rpc_attempts")
    try:
        result = await delegate(
            self,
            signature,
            hedge=True,
            attempts=1,
        )
    except asyncio.TimeoutError:
        completion._inc(self, "rpc_slice_timeouts")
        _inc(self, "provider_timeouts")
        return None, None, None
    except asyncio.CancelledError:
        raise
    except Exception:
        completion._inc(self, "rpc_errors")
        _inc(self, "rpc_errors")
        raise
    finally:
        _PROVIDER_TIMEOUT_SECONDS.reset(token)
        _plane_samples(self).append(
            max(0.0, (time.perf_counter() - started) * 1000.0)
        )

    tx = result[0] if isinstance(result, tuple) and result else None
    completion._inc(
        self,
        "transaction_ready" if tx is not None else "transaction_unavailable",
    )
    completion._inc(self, "rpc_claims_completed")
    _inc(self, "transaction_ready" if tx is not None else "transaction_unavailable")
    _inc(self, "rpc_claims_completed")
    return result


setattr(
    _transaction_ready_after_slot_timeout,
    "_roi_candidate_pipeline_throughput",
    True,
)


async def _prewarm_durable_opportunity_immediately(
    plane: Any,
    swap: Any,
    key: str,
) -> None:
    """Refresh current risk evidence immediately with zero entry authority.

    The prior immediate-prewarm composition called _persist_risk_readthrough with
    an unsupported `as_of` argument after its collectors completed. That converted
    otherwise useful prewarm work into a TypeError/error counter. This corrected
    path keeps prewarm research-only and lower priority than a live candidate.
    """

    async with venue._prewarm_sem(plane):
        collectors = getattr(getattr(plane, "service", None), "collectors", None)
        inner = getattr(collectors, "inner", None)
        coverage = getattr(inner, "refresh_coverage", None)
        candidate = getattr(inner, "refresh_candidate", None)
        try:
            now = _utcnow()
            calls = []
            if callable(coverage):
                calls.append(coverage(swap.token_mint, now, current_swap=swap))
            if callable(candidate):
                calls.append(candidate(swap.token_mint, now, current_swap=swap))
            if calls:
                with governor.rpc_workload(governor.WORKLOAD_RESEARCH):
                    await asyncio.wait_for(
                        asyncio.gather(*calls),
                        timeout=venue.PREWARM_TIMEOUT_SECONDS,
                    )
            semantic._persist_risk_readthrough(plane, swap)
            venue._inc(plane, "prewarm_completed")
            _inc(plane, "prewarm_completed")
        except asyncio.TimeoutError:
            venue._inc(plane, "prewarm_timeouts")
            _inc(plane, "prewarm_timeouts")
        except asyncio.CancelledError:
            raise
        except Exception:
            venue._inc(plane, "prewarm_errors")
            _inc(plane, "prewarm_errors")
        finally:
            venue._prewarm_last(plane)[key] = time.monotonic()


setattr(
    _prewarm_durable_opportunity_immediately,
    "_roi_candidate_pipeline_throughput",
    True,
)


def _handoff_outcome(obj: Any, before: dict[str, int]) -> str:
    keys = (
        "quote_usable",
        "quote_unusable",
        "quote_missing",
        "risk_incomplete",
        "risk_complete",
        "normalized_swap_missing",
        "runtime_unattached",
        "non_trade_side",
    )
    for key in keys:
        current = int(getattr(obj, f"_roi_candidate_v4_handoff_{key}", 0) or 0)
        if current > int(before.get(key, 0)):
            return key
    blocker = str(
        getattr(obj, "_roi_candidate_v4_handoff_last_blocker", "") or ""
    )
    return blocker or "completed_without_specific_blocker"


async def _handoff_with_terminal_telemetry(obj: Any, signature: str) -> None:
    if _ORIGINAL_HANDOFF is None:
        raise RuntimeError("candidate handoff telemetry repair is not installed")
    watched = (
        "quote_usable",
        "quote_unusable",
        "quote_missing",
        "risk_incomplete",
        "risk_complete",
        "normalized_swap_missing",
        "runtime_unattached",
        "non_trade_side",
    )
    before = {
        key: int(getattr(obj, f"_roi_candidate_v4_handoff_{key}", 0) or 0)
        for key in watched
    }
    started = time.perf_counter()
    try:
        await _ORIGINAL_HANDOFF(obj, signature)
    except asyncio.CancelledError:
        raise
    except Exception:
        outcomes = getattr(obj, "_roi_candidate_pipeline_handoff_outcomes", None)
        if not isinstance(outcomes, Counter):
            outcomes = Counter()
            setattr(obj, "_roi_candidate_pipeline_handoff_outcomes", outcomes)
        outcomes["operational_exception"] += 1
        _inc(obj, "handoff_operational_errors")
        raise
    else:
        outcome = _handoff_outcome(obj, before)
        outcomes = getattr(obj, "_roi_candidate_pipeline_handoff_outcomes", None)
        if not isinstance(outcomes, Counter):
            outcomes = Counter()
            setattr(obj, "_roi_candidate_pipeline_handoff_outcomes", outcomes)
        outcomes[outcome] += 1
    finally:
        values = getattr(obj, "_roi_candidate_pipeline_handoff_samples_ms", None)
        if not isinstance(values, deque):
            values = deque(maxlen=SAMPLE_LIMIT)
            setattr(obj, "_roi_candidate_pipeline_handoff_samples_ms", values)
        values.append(max(0.0, (time.perf_counter() - started) * 1000.0))
        _inc(obj, "handoff_terminal_accounted")


setattr(
    _handoff_with_terminal_telemetry,
    "_roi_candidate_pipeline_throughput",
    True,
)


def _latency_summary(values: Any) -> dict[str, Any]:
    rows = list(values) if isinstance(values, deque) else []
    return {
        "sample_count": len(rows),
        "p50_ms": _percentile(rows, 0.50),
        "p95_ms": _percentile(rows, 0.95),
        "p99_ms": _percentile(rows, 0.99),
        "max_ms": max(rows) if rows else None,
    }


def _status_with_candidate_pipeline_throughput(
    self: DirectSolanaIngestionPlane,
) -> dict[str, Any]:
    if _ORIGINAL_DIRECT_STATUS is None:
        raise RuntimeError("candidate pipeline throughput status is not installed")
    payload = _ORIGINAL_DIRECT_STATUS(self)

    endpoints: list[dict[str, Any]] = []
    pools = []
    for candidate_pool in (
        getattr(self, "rpc", None),
        getattr(self, "rpc_pool", None),
    ):
        if candidate_pool is not None and candidate_pool not in pools:
            pools.append(candidate_pool)
    seen: set[str] = set()
    for pool in pools:
        for endpoint in getattr(pool, "endpoints", ()) or ():
            key = str(getattr(endpoint, "http_url", "") or "")
            if not key or key in seen:
                continue
            seen.add(key)
            endpoints.append(
                {
                    "name": str(getattr(endpoint, "name", "") or ""),
                    "http_host": key.split("/", 3)[2] if "://" in key else key,
                    "candidate_slot_wait": _samples_for(
                        _SLOT_WAIT_SAMPLES,
                        key,
                        candidate_priority.WORKLOAD_CANDIDATE,
                    ),
                    "candidate_provider_response": _samples_for(
                        _PROVIDER_SAMPLES,
                        key,
                        candidate_priority.WORKLOAD_CANDIDATE,
                    ),
                    "candidate_provider_timeouts": int(
                        _PROVIDER_TIMEOUTS.get(
                            (key, candidate_priority.WORKLOAD_CANDIDATE),
                            0,
                        )
                    ),
                }
            )

    handoff_outcomes = getattr(
        self,
        "_roi_candidate_pipeline_handoff_outcomes",
        Counter(),
    )
    payload["candidate_pipeline_throughput_repair"] = {
        "installed": True,
        "repair_version": REPAIR_VERSION,
        "rpc_priority_order": [
            governor.WORKLOAD_CRITICAL,
            candidate_priority.WORKLOAD_CANDIDATE,
            standby_priority.WORKLOAD_STANDBY,
            governor.WORKLOAD_CERTIFICATION,
            governor.WORKLOAD_RESEARCH,
        ],
        "candidate_can_borrow_all_existing_noncritical_slots_while_waiting": True,
        "critical_continuity_reserve_unchanged": True,
        "endpoint_total_capacity_unchanged": True,
        "event_driven_rpc_capacity_wakeup": True,
        "provider_timeout_starts_after_rpc_slot_acquisition": True,
        "provider_timeout_excludes_governor_slot_wait": True,
        "candidate_total_rpc_call": _latency_summary(_plane_samples(self)),
        "endpoints": endpoints,
        "rpc_attempts": int(
            getattr(self, "_roi_candidate_pipeline_rpc_attempts", 0) or 0
        ),
        "rpc_claims_completed": int(
            getattr(self, "_roi_candidate_pipeline_rpc_claims_completed", 0) or 0
        ),
        "provider_timeouts": int(
            getattr(self, "_roi_candidate_pipeline_provider_timeouts", 0) or 0
        ),
        "transaction_ready": int(
            getattr(self, "_roi_candidate_pipeline_transaction_ready", 0) or 0
        ),
        "transaction_unavailable": int(
            getattr(self, "_roi_candidate_pipeline_transaction_unavailable", 0)
            or 0
        ),
        "prewarm": {
            "immediate_after_durable_opportunity": True,
            "research_workload_only": True,
            "entry_authority": False,
            "persist_risk_readthrough_signature_corrected": True,
            "completed": int(
                getattr(self, "_roi_candidate_pipeline_prewarm_completed", 0) or 0
            ),
            "timeouts": int(
                getattr(self, "_roi_candidate_pipeline_prewarm_timeouts", 0) or 0
            ),
            "errors": int(
                getattr(self, "_roi_candidate_pipeline_prewarm_errors", 0) or 0
            ),
        },
        "handoff": {
            "every_started_handoff_gets_terminal_telemetry": True,
            "outcomes": dict(handoff_outcomes)
            if isinstance(handoff_outcomes, Counter)
            else {},
            "latency": _latency_summary(
                getattr(
                    self,
                    "_roi_candidate_pipeline_handoff_samples_ms",
                    deque(),
                )
            ),
            "terminal_accounted": int(
                getattr(
                    self,
                    "_roi_candidate_pipeline_handoff_terminal_accounted",
                    0,
                )
                or 0
            ),
            "operational_errors": int(
                getattr(
                    self,
                    "_roi_candidate_pipeline_handoff_operational_errors",
                    0,
                )
                or 0
            ),
        },
        "candidate_processing_target_seconds_unchanged": float(
            forward.LATENCY_BUDGET_SECONDS
        ),
        "candidate_entry_window_seconds_unchanged": float(
            forward.ENTRY_WINDOW_SECONDS
        ),
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
                "candidate_rpc_timeout_starts_after_slot_acquisition": True,
                "candidate_rpc_capacity_wakeup_event_driven": True,
                "candidate_can_borrow_all_existing_noncritical_capacity": True,
                "candidate_priority_over_standby_certification_research": True,
                "critical_rpc_capacity_reserved": True,
                "durable_opportunity_risk_prewarm_immediate": True,
                "prewarm_has_entry_authority": False,
                "candidate_handoff_terminal_accounting": True,
                "candidate_latency_threshold_unchanged": True,
                "entry_window_seconds_unchanged": True,
                "strategy_thresholds_unchanged": True,
                "certification_thresholds_unchanged": True,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    return payload


setattr(
    _status_with_candidate_pipeline_throughput,
    "_roi_candidate_pipeline_throughput",
    True,
)


def install_candidate_pipeline_throughput_repair() -> None:
    """Install the post-223 candidate throughput repair at the final composition edge."""

    global _INSTALLED
    global _ORIGINAL_GOVERNOR_ENDPOINT_DELEGATE
    global _ORIGINAL_GET_TRANSACTION_READY
    global _ORIGINAL_DIRECT_STATUS
    global _ORIGINAL_HANDOFF

    if _INSTALLED:
        return

    # The governor's wrapper resolves these globals dynamically on every endpoint
    # request, so this changes scheduling without replacing the already-composed
    # SolanaRpcPool._call_endpoint wrapper chain.
    governor._allowed = _candidate_first_allowed  # type: ignore[assignment]
    governor._acquire = _event_driven_acquire  # type: ignore[assignment]
    governor._release = _release_and_wake  # type: ignore[assignment]

    delegate = governor._ORIGINAL_CALL_ENDPOINT
    if delegate is None:
        raise RuntimeError("RPC workload governor endpoint delegate is unavailable")
    if not bool(
        getattr(
            delegate,
            "_roi_candidate_provider_after_slot_timeout",
            False,
        )
    ):
        _ORIGINAL_GOVERNOR_ENDPOINT_DELEGATE = delegate
        governor._ORIGINAL_CALL_ENDPOINT = (  # type: ignore[assignment]
            _provider_delegate_with_after_slot_timeout
        )

    current_get = DirectSolanaIngestionPlane._get_transaction_ready
    if not bool(
        getattr(
            current_get,
            "_roi_candidate_pipeline_throughput",
            False,
        )
    ):
        _ORIGINAL_GET_TRANSACTION_READY = current_get
        try:
            _transaction_ready_after_slot_timeout.__dict__.update(
                getattr(current_get, "__dict__", {})
            )
        except Exception:
            pass
        setattr(
            _transaction_ready_after_slot_timeout,
            "_roi_candidate_pipeline_throughput",
            True,
        )
        DirectSolanaIngestionPlane._get_transaction_ready = (  # type: ignore[method-assign]
            _transaction_ready_after_slot_timeout
        )

    # post177 already moved prewarm to immediate collection, but its terminal call
    # uses an unsupported `as_of` keyword. Replace only that research-only helper.
    venue._prewarm_after_immediate_window = (  # type: ignore[assignment]
        _prewarm_durable_opportunity_immediately
    )

    current_handoff = handoff._process_candidate_handoff
    if not bool(
        getattr(
            current_handoff,
            "_roi_candidate_pipeline_throughput",
            False,
        )
    ):
        _ORIGINAL_HANDOFF = current_handoff
        handoff._process_candidate_handoff = (  # type: ignore[assignment]
            _handoff_with_terminal_telemetry
        )

    current_status = DirectSolanaIngestionPlane.status
    if not bool(
        getattr(
            current_status,
            "_roi_candidate_pipeline_throughput",
            False,
        )
    ):
        _ORIGINAL_DIRECT_STATUS = current_status
        try:
            _status_with_candidate_pipeline_throughput.__dict__.update(
                getattr(current_status, "__dict__", {})
            )
        except Exception:
            pass
        DirectSolanaIngestionPlane.status = (  # type: ignore[method-assign]
            _status_with_candidate_pipeline_throughput
        )

    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "provider_timeout_after_slot_acquisition": True,
        "event_driven_capacity_wakeup": True,
        "candidate_priority_over_noncritical_background": True,
        "immediate_prewarm_signature_repaired": True,
        "handoff_terminal_telemetry": True,
        "candidate_processing_target_seconds_unchanged": float(
            forward.LATENCY_BUDGET_SECONDS
        ),
        "candidate_entry_window_seconds_unchanged": float(
            forward.ENTRY_WINDOW_SECONDS
        ),
        "strategy_thresholds_changed": False,
        "certification_thresholds_changed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "REPAIR_VERSION",
    "_candidate_first_allowed",
    "_event_driven_acquire",
    "_handoff_with_terminal_telemetry",
    "_prewarm_durable_opportunity_immediately",
    "_provider_delegate_with_after_slot_timeout",
    "_transaction_ready_after_slot_timeout",
    "install_candidate_pipeline_throughput_repair",
    "status",
]
