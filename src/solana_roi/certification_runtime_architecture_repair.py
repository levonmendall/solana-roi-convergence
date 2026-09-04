from __future__ import annotations

import asyncio
import math
import time
from collections import deque
from collections.abc import Callable
from typing import Any

from . import candidate_rpc_priority_repair as candidate_priority
from . import continuity_high_volume_checkpoint_architecture as checkpoint
from . import continuity_recovery_isolation_repair as isolation
from . import continuity_standby_rpc_priority_repair as standby_priority
from . import continuity_target_frontier_repair as frontier
from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from . import production_capacity_repair as capacity
from . import rpc_workload_governor as governor
from . import continuity_immediate_recovery_repair as immediate
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget
from .solana_rpc import SolanaRpcPool
from .wallet_discovery import ContinuousWalletDiscovery


EVENT_LOOP_SAMPLE_SECONDS = 0.10
EVENT_LOOP_HISTORY_SAMPLES = 600
RESEARCH_P95_YIELD_MS = 25.0
RESEARCH_CURRENT_YIELD_MS = 50.0

_ORIGINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_RPC_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_RPC_CALL_WITH_META: Callable[..., Any] | None = None
_ORIGINAL_DISCOVERY_RUN: Callable[..., Any] | None = None
_ORIGINAL_DISCOVER_RAW: Callable[..., Any] | None = None
_ORIGINAL_SCREEN_ONE: Callable[..., Any] | None = None
_ORIGINAL_NOTIFICATION: Callable[..., Any] | None = None
_ORIGINAL_KICK: Callable[..., Any] | None = None
_ORIGINAL_INTERVAL_FETCH: Callable[..., Any] | None = None

_LOOP_LAG_MS: deque[float] = deque(maxlen=EVENT_LOOP_HISTORY_SAMPLES)
_LOOP_LAG_CURRENT_MS = 0.0
_LOOP_LAG_MAX_MS = 0.0
_ACTIVE_RECOVERY_TASKS: set[int] = set()


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    index = max(0, min(len(ordered) - 1, math.ceil(float(quantile) * len(ordered)) - 1))
    return float(ordered[index])


def _loop_lag_snapshot() -> dict[str, Any]:
    values = list(_LOOP_LAG_MS)
    return {
        "sample_count": len(values),
        "current_ms": float(_LOOP_LAG_CURRENT_MS),
        "p50_ms": _percentile(values, 0.50),
        "p95_ms": _percentile(values, 0.95),
        "p99_ms": _percentile(values, 0.99),
        "max_ms": float(_LOOP_LAG_MAX_MS),
        "sample_interval_seconds": EVENT_LOOP_SAMPLE_SECONDS,
        "research_p95_yield_ms": RESEARCH_P95_YIELD_MS,
        "research_current_yield_ms": RESEARCH_CURRENT_YIELD_MS,
        "active_continuity_recovery_tasks": len(_ACTIVE_RECOVERY_TASKS),
    }


async def _event_loop_lag_monitor(stop: asyncio.Event) -> None:
    global _LOOP_LAG_CURRENT_MS, _LOOP_LAG_MAX_MS
    loop = asyncio.get_running_loop()
    expected = loop.time() + EVENT_LOOP_SAMPLE_SECONDS
    while not stop.is_set():
        await asyncio.sleep(EVENT_LOOP_SAMPLE_SECONDS)
        now = loop.time()
        lag_ms = max(0.0, (now - expected) * 1000.0)
        _LOOP_LAG_CURRENT_MS = lag_ms
        _LOOP_LAG_MAX_MS = max(_LOOP_LAG_MAX_MS, lag_ms)
        _LOOP_LAG_MS.append(lag_ms)
        expected = now + EVENT_LOOP_SAMPLE_SECONDS


def _research_pressure_reason() -> str | None:
    if _ACTIVE_RECOVERY_TASKS:
        return "continuity_recovery_active"
    snapshot = _loop_lag_snapshot()
    if float(snapshot["current_ms"]) >= RESEARCH_CURRENT_YIELD_MS:
        return "event_loop_current_lag"
    if int(snapshot["sample_count"]) >= 10 and float(snapshot["p95_ms"]) >= RESEARCH_P95_YIELD_MS:
        return "event_loop_p95_lag"
    return None


async def _discovery_run_with_loop_monitor(self: ContinuousWalletDiscovery, stop: asyncio.Event) -> None:
    if _ORIGINAL_DISCOVERY_RUN is None:
        raise RuntimeError("event-loop scheduling architecture is not installed")
    monitor = asyncio.create_task(_event_loop_lag_monitor(stop), name="roi-event-loop-lag-monitor")
    try:
        await _ORIGINAL_DISCOVERY_RUN(self, stop)
    finally:
        if not monitor.done():
            monitor.cancel()
        await asyncio.gather(monitor, return_exceptions=True)


async def _discover_raw_with_cpu_backpressure(self: ContinuousWalletDiscovery) -> int:
    if _ORIGINAL_DISCOVER_RAW is None:
        raise RuntimeError("research CPU backpressure architecture is not installed")
    reason = _research_pressure_reason()
    if reason is not None:
        setattr(self, "_roi_cpu_backpressure_last_reason", reason)
        setattr(
            self,
            "_roi_cpu_backpressure_broad_skips",
            int(getattr(self, "_roi_cpu_backpressure_broad_skips", 0) or 0) + 1,
        )
        await asyncio.sleep(0)
        return 0
    return int(await _ORIGINAL_DISCOVER_RAW(self))


async def _screen_one_with_cpu_backpressure(self: ContinuousWalletDiscovery) -> bool:
    if _ORIGINAL_SCREEN_ONE is None:
        raise RuntimeError("research CPU backpressure architecture is not installed")
    reason = _research_pressure_reason()
    if reason is not None:
        setattr(self, "_roi_cpu_backpressure_last_reason", reason)
        setattr(
            self,
            "_roi_cpu_backpressure_screen_skips",
            int(getattr(self, "_roi_cpu_backpressure_screen_skips", 0) or 0) + 1,
        )
        await asyncio.sleep(0)
        return False
    return bool(await _ORIGINAL_SCREEN_ONE(self))


def _recovery_upper_boundaries(self: Any) -> dict[str, dict[str, Any]]:
    value = getattr(self, "_roi_gap_recovery_upper_boundaries", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_gap_recovery_upper_boundaries", value)
    return value


async def _notification_with_recovery_upper_boundary(
    self: Any,
    provider: str,
    subscription_targets: dict[int, WatchTarget],
    message: dict[str, Any],
) -> None:
    if _ORIGINAL_NOTIFICATION is None:
        raise RuntimeError("generation interval architecture is not installed")
    parsed = frontier._parse_target_notification(subscription_targets, message)
    await _ORIGINAL_NOTIFICATION(self, provider, subscription_targets, message)
    if parsed is None:
        return
    target, signature, slot = parsed
    key = live_poll._poll_target_key(target)
    pending = immediate._recovery_tasks(self).get(key)
    if not isinstance(pending, dict):
        return
    try:
        generation = int(pending.get("generation", -1))
    except (TypeError, ValueError):
        return
    if generation < 0 or generation != immediate._generation(self, target):
        return
    boundaries = _recovery_upper_boundaries(self)
    current = boundaries.get(key)
    if isinstance(current, dict) and int(current.get("generation", -1)) == generation:
        return
    boundaries[key] = {
        "generation": generation,
        "signature": str(signature),
        "slot": int(slot),
        "provider": str(provider),
        "captured_monotonic": time.monotonic(),
        "source": "first-successfully-recorded-post-gap-websocket-receipt",
        "exclusive_before_signature": True,
    }
    setattr(
        self,
        "_roi_gap_recovery_upper_boundary_count",
        int(getattr(self, "_roi_gap_recovery_upper_boundary_count", 0) or 0) + 1,
    )


async def _interval_bounded_gap_fetch_delta(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None, dict[str, Any]]:
    pages: list[list[dict[str, Any]]] = []
    page_providers: list[str | None] = []
    page_latencies: list[float | None] = []
    key = live_poll._poll_target_key(target)
    generation = immediate._generation(self, target)
    boundary = _recovery_upper_boundaries(self).get(key)
    before: str | None = None
    upper_slot = 0
    if isinstance(boundary, dict) and int(boundary.get("generation", -1)) == generation:
        before = str(boundary.get("signature") or "") or None
        try:
            upper_slot = int(boundary.get("slot") or 0)
        except (TypeError, ValueError):
            upper_slot = 0

    provider: str | None = None
    latency: float | None = None
    complete = False
    context_floor = max(0, int(cursor_slot))
    cursor_reached = False

    for _page_index in range(live_poll.POLL_CURSOR_MAX_PAGES):
        config: dict[str, Any] = {
            "commitment": "confirmed",
            "limit": live_poll.POLL_LIMIT,
        }
        if before:
            config["before"] = before
        if context_floor > 0:
            config["minContextSlot"] = context_floor
        result, provider, latency = await isolation._recovery_rpc(self).call_with_meta(
            "getSignaturesForAddress",
            [target.address, config],
            hedge=True,
        )
        page = [row for row in result if isinstance(row, dict)] if isinstance(result, list) else []
        pages.append(page)
        page_providers.append(provider)
        page_latencies.append(float(latency) if latency is not None else None)
        if not page:
            complete = True
            break
        slots = [isolation.watermark._row_slot(row) for row in page]
        newest_page_slot = max(slots, default=0)
        context_floor = max(context_floor, newest_page_slot)
        if cursor_slot > 0 and any(slot <= cursor_slot for slot in slots):
            cursor_reached = True
            complete = True
            break
        if len(page) < live_poll.POLL_LIMIT:
            complete = True
            break
        before = str(page[-1].get("signature") or "")
        if not before:
            complete = True
            break

    rows: list[dict[str, Any]] = []
    if complete:
        seen: set[str] = set()
        for page in reversed(pages):
            for row in reversed(page):
                signature = str(row.get("signature") or "")
                slot = isolation.watermark._row_slot(row)
                if not signature or signature in seen or slot <= cursor_slot:
                    continue
                seen.add(signature)
                rows.append(row)

    all_slots = [isolation.watermark._row_slot(row) for page in pages for row in page]
    meta = {
        "page_count": len(pages),
        "page_sizes": [len(page) for page in pages],
        "page_providers": page_providers,
        "page_latencies_ms": page_latencies,
        "newest_slot_seen": max(all_slots, default=0),
        "oldest_slot_seen": min((slot for slot in all_slots if slot > 0), default=0),
        "cursor_slot": int(cursor_slot),
        "cursor_reached": bool(cursor_reached),
        "complete": bool(complete),
        "recovered_row_count": len(rows) if complete else 0,
        "hard_page_limit": live_poll.POLL_CURSOR_MAX_PAGES,
        "hard_page_size": live_poll.POLL_LIMIT,
        "generation_upper_boundary_applied": bool(boundary and upper_slot > 0),
        "generation_upper_boundary_slot": upper_slot or None,
        "generation_upper_boundary_source": (
            str(boundary.get("source")) if isinstance(boundary, dict) and upper_slot > 0 else None
        ),
    }
    return rows, complete, provider, latency, meta


async def _universal_checkpointed_slot_fetch_delta(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    if checkpoint._ORIGINAL_SLOT_FETCH is None:
        raise RuntimeError("universal standby checkpoint architecture is not installed")
    if not live_poll._ws_target_covered(self, target):
        return await checkpoint._ORIGINAL_SLOT_FETCH(self, target, cursor_slot)

    latest_observed_slot = checkpoint._latest_observed_target_slot(self, target)
    if latest_observed_slot <= 0:
        return await checkpoint._ORIGINAL_SLOT_FETCH(self, target, cursor_slot)
    if latest_observed_slot <= int(cursor_slot) + 1:
        return [], True, None, None

    generation = int(lease._current_ws_generation(self, target))
    try:
        with governor.rpc_workload(standby_priority.WORKLOAD_STANDBY):
            effective_cursor, anchor = await frontier._confirmed_target_frontier_cursor(
                self,
                target,
                int(cursor_slot),
                generation,
            )
    except Exception:
        return await checkpoint._ORIGINAL_SLOT_FETCH(self, target, cursor_slot)

    if (
        not live_poll._ws_target_covered(self, target)
        or int(lease._current_ws_generation(self, target)) != generation
        or not isinstance(anchor, dict)
        or str(anchor.get("source") or "") != "confirmed-target-websocket-frontier"
        or int(effective_cursor) <= int(cursor_slot)
    ):
        return await checkpoint._ORIGINAL_SLOT_FETCH(self, target, cursor_slot)

    key = live_poll._poll_target_key(target)
    counts = checkpoint._checkpoint_counts(self)
    counts[key] = int(counts.get(key, 0) or 0) + 1
    checkpoint._last_checkpoints(self)[key] = {
        "target": key,
        "source": str(target.source_hint or target.kind),
        "generation": generation,
        "prior_cursor_slot": int(cursor_slot),
        "checkpoint_cursor_slot": int(effective_cursor),
        "confirmed_frontier_slot": int(anchor.get("confirmed_frontier_slot") or 0),
        "confirmation_provider": anchor.get("confirmation_provider"),
        "confirmation_latency_ms": anchor.get("confirmation_latency_ms"),
        "same_slot_replay_required": True,
        "universal_frozen_target_checkpoint": True,
    }
    return (
        [
            {
                "signature": "",
                "slot": int(effective_cursor),
                "err": None,
                "_roi_standby_checkpoint": True,
            }
        ],
        True,
        str(anchor.get("confirmation_provider") or "") or None,
        float(anchor["confirmation_latency_ms"])
        if anchor.get("confirmation_latency_ms") is not None
        else None,
    )


def _managed_recovery_kick(self: Any, target: WatchTarget, generation: int) -> None:
    if _ORIGINAL_KICK is None:
        raise RuntimeError("recovery task ownership architecture is not installed")
    _ORIGINAL_KICK(self, target, generation)
    key = live_poll._poll_target_key(target)
    row = immediate._recovery_tasks(self).get(key)
    if not isinstance(row, dict):
        return
    task = row.get("task")
    if not isinstance(task, asyncio.Task) or bool(getattr(task, "_roi_owned_recovery_task", False)):
        return
    setattr(task, "_roi_owned_recovery_task", True)
    _ACTIVE_RECOVERY_TASKS.add(id(task))

    def done(completed: asyncio.Task[Any]) -> None:
        _ACTIVE_RECOVERY_TASKS.discard(id(completed))
        outcome = "completed"
        error_type: str | None = None
        try:
            exc = completed.exception()
            if exc is not None:
                outcome = "failed"
                error_type = type(exc).__name__
        except asyncio.CancelledError:
            outcome = "cancelled"
        except BaseException as exc:
            outcome = "failed"
            error_type = type(exc).__name__
        setattr(
            self,
            "_roi_owned_recovery_task_outcomes",
            int(getattr(self, "_roi_owned_recovery_task_outcomes", 0) or 0) + 1,
        )
        setattr(
            self,
            "_roi_owned_recovery_last_outcome",
            {
                "target": key,
                "generation": int(generation),
                "outcome": outcome,
                "error_type": error_type,
            },
        )

    task.add_done_callback(done)


async def _safe_hedged_call_with_meta(
    self: SolanaRpcPool,
    method: str,
    params: list[Any],
    *,
    hedge: bool = False,
) -> tuple[Any, str, float]:
    if _ORIGINAL_RPC_CALL_WITH_META is None:
        raise RuntimeError("RPC task cleanup architecture is not installed")
    if not hedge:
        return await _ORIGINAL_RPC_CALL_WITH_META(self, method, params, hedge=False)

    ordered = list(self._ordered(method))
    usable = [endpoint for endpoint in ordered if capacity._cooldown_remaining(self, endpoint) <= 0.0]
    if not usable:
        remaining = min((capacity._cooldown_remaining(self, endpoint) for endpoint in ordered), default=0.0)
        setattr(self, "_roi_cooling_fast_fails", int(getattr(self, "_roi_cooling_fast_fails", 0) or 0) + 1)
        raise capacity.RpcEndpointCoolingDown(
            f"all RPC endpoints cooling down; earliest retry in {remaining:.3f}s"
        )
    if len(usable) < len(ordered):
        setattr(
            self,
            "_roi_cooling_endpoints_bypassed",
            int(getattr(self, "_roi_cooling_endpoints_bypassed", 0) or 0) + (len(ordered) - len(usable)),
        )

    # Keep the intentionally sequential official-public fallback for routine and
    # continuity work. Candidate work retains its already-approved proactive hedge.
    candidate_mode = governor.current_rpc_workload() == candidate_priority.WORKLOAD_CANDIDATE
    if capacity._official_pair_requires_sequential_fallback(self) and not candidate_mode:
        errors: list[Exception] = []
        for endpoint in usable:
            try:
                return await self._call_endpoint(endpoint, method, params)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                errors.append(exc)
        raise RuntimeError(f"all usable Solana RPC endpoints failed for {method}") from (
            errors[-1] if errors else None
        )

    if len(usable) == 1:
        return await self._call_endpoint(usable[0], method, params)

    tasks: list[asyncio.Task[tuple[Any, str, float]]] = []
    errors: list[Exception] = []
    try:
        primary = asyncio.create_task(self._call_endpoint(usable[0], method, params))
        tasks.append(primary)
        done, _ = await asyncio.wait({primary}, timeout=self.hedge_delay_seconds)
        if primary in done:
            try:
                return primary.result()
            except Exception as exc:
                errors.append(exc)
                for endpoint in usable[1:]:
                    try:
                        return await self._call_endpoint(endpoint, method, params)
                    except asyncio.CancelledError:
                        raise
                    except Exception as fallback_exc:
                        errors.append(fallback_exc)
                raise RuntimeError(f"all usable Solana RPC endpoints failed for {method}") from errors[-1]

        hedge_task = asyncio.create_task(self._call_endpoint(usable[1], method, params))
        tasks.append(hedge_task)
        pending: set[asyncio.Task[tuple[Any, str, float]]] = {primary, hedge_task}
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                try:
                    result = task.result()
                except Exception as exc:
                    errors.append(exc)
                    continue
                return result
        for endpoint in usable[2:]:
            try:
                return await self._call_endpoint(endpoint, method, params)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                errors.append(exc)
        raise RuntimeError(f"all usable Solana RPC endpoints failed for {method}") from (
            errors[-1] if errors else None
        )
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


def _direct_status_with_runtime_architecture(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _ORIGINAL_DIRECT_STATUS is None:
        raise RuntimeError("runtime architecture status is not installed")
    payload = _ORIGINAL_DIRECT_STATUS(self)
    counts = checkpoint._checkpoint_counts(self)
    payload["universal_target_frontier_checkpoint_architecture"] = {
        "installed": True,
        "target_scope": "all-frozen-program-and-scout-targets",
        "checkpoint_count": sum(int(value) for value in counts.values()),
        "checkpoint_counts_by_target": dict(sorted(counts.items())),
        "confirmed_target_receipt_required": True,
        "continuous_real_websocket_generation_required": True,
        "same_slot_replay_preserved": True,
        "canonical_poll_helpers_preserved": True,
        "recoverability_lease_seconds": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
        "hard_page_limit": live_poll.POLL_CURSOR_MAX_PAGES,
        "hard_page_size": live_poll.POLL_LIMIT,
    }
    payload["generation_bounded_recovery_interval"] = {
        "installed": True,
        "upper_boundary_count": int(getattr(self, "_roi_gap_recovery_upper_boundary_count", 0) or 0),
        "upper_boundaries": dict(sorted(_recovery_upper_boundaries(self).items())),
        "upper_boundary_source": "first-successfully-recorded-post-gap-websocket-receipt",
        "upper_boundary_is_exclusive": True,
        "lower_confirmed_generation_floor_preserved": True,
        "recoverability_lease_seconds_unchanged": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
        "hard_delta_bound_unchanged": True,
    }
    payload["event_loop_pressure"] = {
        "installed": True,
        **_loop_lag_snapshot(),
        "background_historical_discovery_yields": True,
        "live_wallet_forward_tracking_yields": False,
        "continuity_recovery_priority_over_research": True,
    }
    payload["recovery_task_ownership"] = {
        "installed": True,
        "exceptions_retrieved_once_by_done_callback": True,
        "task_outcomes": int(getattr(self, "_roi_owned_recovery_task_outcomes", 0) or 0),
        "last_outcome": getattr(self, "_roi_owned_recovery_last_outcome", None),
        "active_tasks": len(_ACTIVE_RECOVERY_TASKS),
        "failure_still_fails_closed": True,
    }
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "standby_frontier_checkpoint_scope": "all-frozen-targets",
                "recovery_interval_has_post_gap_exact_upper_boundary": True,
                "background_research_yields_under_event_loop_pressure": True,
                "recovery_task_exceptions_are_owned": True,
                "continuity_lease_unchanged": True,
                "recovery_bound_unchanged": True,
                "full_raw_market_scope_preserved": True,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    return payload


def _rpc_status_with_runtime_architecture(self: SolanaRpcPool) -> dict[str, Any]:
    if _ORIGINAL_RPC_STATUS is None:
        raise RuntimeError("RPC runtime architecture status is not installed")
    payload = _ORIGINAL_RPC_STATUS(self)
    payload["rpc_task_cleanup_architecture"] = {
        "installed": True,
        "hedge_tasks_always_gathered": True,
        "cooling_endpoints_bypassed_before_task_creation": True,
        "cooling_endpoints_bypassed": int(getattr(self, "_roi_cooling_endpoints_bypassed", 0) or 0),
        "cooling_fast_fails": int(getattr(self, "_roi_cooling_fast_fails", 0) or 0),
        "provider_scope_unchanged": True,
        "read_only_only": True,
        "signing_or_submission_available": False,
    }
    return payload


def _discovery_status_with_cpu_backpressure(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: ContinuousWalletDiscovery) -> dict[str, Any]:
        payload = original(self)
        payload["cpu_event_loop_backpressure"] = {
            "installed": True,
            "broad_historical_skips": int(getattr(self, "_roi_cpu_backpressure_broad_skips", 0) or 0),
            "historical_screen_skips": int(getattr(self, "_roi_cpu_backpressure_screen_skips", 0) or 0),
            "last_reason": getattr(self, "_roi_cpu_backpressure_last_reason", None),
            "current_pressure_reason": _research_pressure_reason(),
            **_loop_lag_snapshot(),
            "forward_wallet_polling_preserved": True,
            "realtime_wallet_tracking_preserved": True,
            "historical_screen_has_promotion_authority": False,
        }
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_cpu_event_loop_backpressure", True)
    return status


def install_certification_runtime_architecture_repair() -> None:
    global _ORIGINAL_DIRECT_STATUS, _ORIGINAL_RPC_STATUS, _ORIGINAL_RPC_CALL_WITH_META
    global _ORIGINAL_DISCOVERY_RUN, _ORIGINAL_DISCOVER_RAW, _ORIGINAL_SCREEN_ONE
    global _ORIGINAL_NOTIFICATION, _ORIGINAL_KICK, _ORIGINAL_INTERVAL_FETCH

    # Generalize PR #96's confirmed-frontier standby checkpoint from only the two
    # high-volume sources to every frozen program/scout target. The proxy object and
    # every canonical inner poll helper remain untouched.
    checkpoint._checkpointed_slot_fetch_delta = _universal_checkpointed_slot_fetch_delta  # type: ignore[assignment]

    # Freeze an exact exclusive upper edge when the first post-gap receipt has been
    # successfully recorded, then let the existing isolated recovery use it on any
    # subsequent bounded attempt for the same gap generation.
    if not bool(getattr(DirectSolanaIngestionPlane._handle_notification, "_roi_generation_interval", False)):
        _ORIGINAL_NOTIFICATION = DirectSolanaIngestionPlane._handle_notification
        try:
            _notification_with_recovery_upper_boundary.__dict__.update(
                getattr(_ORIGINAL_NOTIFICATION, "__dict__", {})
            )
        except Exception:
            pass
        setattr(_notification_with_recovery_upper_boundary, "_roi_generation_interval", True)
        DirectSolanaIngestionPlane._handle_notification = _notification_with_recovery_upper_boundary  # type: ignore[method-assign]

    if not bool(getattr(isolation._isolated_gap_fetch_delta, "_roi_generation_interval", False)):
        _ORIGINAL_INTERVAL_FETCH = isolation._isolated_gap_fetch_delta
        setattr(_interval_bounded_gap_fetch_delta, "_roi_generation_interval", True)
        isolation._isolated_gap_fetch_delta = _interval_bounded_gap_fetch_delta  # type: ignore[assignment]

    # Own every immediate recovery task's terminal exception without removing it
    # from the generation map. The canonical lease worker can still await the same
    # task and receives the same failure, so fail-closed semantics are unchanged.
    if not bool(getattr(immediate._kick_immediate_recovery, "_roi_owned_recovery_task", False)):
        _ORIGINAL_KICK = immediate._kick_immediate_recovery
        try:
            _managed_recovery_kick.__dict__.update(getattr(_ORIGINAL_KICK, "__dict__", {}))
        except Exception:
            pass
        setattr(_managed_recovery_kick, "_roi_owned_recovery_task", True)
        immediate._kick_immediate_recovery = _managed_recovery_kick  # type: ignore[assignment]

    # Background historical discovery/screening may skip a cycle under measurable
    # event-loop pressure. Forward wallet polling remains in the same run_once and
    # is intentionally not gated.
    if not bool(getattr(ContinuousWalletDiscovery.run, "_roi_cpu_event_loop_backpressure", False)):
        _ORIGINAL_DISCOVERY_RUN = ContinuousWalletDiscovery.run
        setattr(_discovery_run_with_loop_monitor, "_roi_cpu_event_loop_backpressure", True)
        ContinuousWalletDiscovery.run = _discovery_run_with_loop_monitor  # type: ignore[method-assign]
    if not bool(getattr(ContinuousWalletDiscovery.discover_from_raw_receipts, "_roi_cpu_event_loop_backpressure", False)):
        _ORIGINAL_DISCOVER_RAW = ContinuousWalletDiscovery.discover_from_raw_receipts
        setattr(_discover_raw_with_cpu_backpressure, "_roi_cpu_event_loop_backpressure", True)
        ContinuousWalletDiscovery.discover_from_raw_receipts = _discover_raw_with_cpu_backpressure  # type: ignore[method-assign]
    if not bool(getattr(ContinuousWalletDiscovery.screen_one_candidate, "_roi_cpu_event_loop_backpressure", False)):
        _ORIGINAL_SCREEN_ONE = ContinuousWalletDiscovery.screen_one_candidate
        setattr(_screen_one_with_cpu_backpressure, "_roi_cpu_event_loop_backpressure", True)
        ContinuousWalletDiscovery.screen_one_candidate = _screen_one_with_cpu_backpressure  # type: ignore[method-assign]

    discovery_status = ContinuousWalletDiscovery.status
    if not bool(getattr(discovery_status, "_roi_cpu_event_loop_backpressure", False)):
        ContinuousWalletDiscovery.status = _discovery_status_with_cpu_backpressure(discovery_status)  # type: ignore[method-assign]

    # Replace only the outer hedge orchestration. Endpoint calls still traverse the
    # existing governor/capacity wrappers; known cooling endpoints are filtered out
    # before tasks are spawned and every created task is gathered in finally.
    current_rpc_call = SolanaRpcPool.call_with_meta
    if not bool(getattr(current_rpc_call, "_roi_safe_rpc_task_cleanup", False)):
        _ORIGINAL_RPC_CALL_WITH_META = current_rpc_call
        try:
            _safe_hedged_call_with_meta.__dict__.update(getattr(current_rpc_call, "__dict__", {}))
        except Exception:
            pass
        setattr(_safe_hedged_call_with_meta, "_roi_safe_rpc_task_cleanup", True)
        SolanaRpcPool.call_with_meta = _safe_hedged_call_with_meta  # type: ignore[method-assign]

    current_rpc_status = SolanaRpcPool.status
    if not bool(getattr(current_rpc_status, "_roi_safe_rpc_task_cleanup", False)):
        _ORIGINAL_RPC_STATUS = current_rpc_status
        try:
            _rpc_status_with_runtime_architecture.__dict__.update(getattr(current_rpc_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_rpc_status_with_runtime_architecture, "_roi_safe_rpc_task_cleanup", True)
        SolanaRpcPool.status = _rpc_status_with_runtime_architecture  # type: ignore[method-assign]

    current_direct_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_direct_status, "_roi_certification_runtime_architecture", False)):
        _ORIGINAL_DIRECT_STATUS = current_direct_status
        try:
            _direct_status_with_runtime_architecture.__dict__.update(
                getattr(current_direct_status, "__dict__", {})
            )
        except Exception:
            pass
        setattr(_direct_status_with_runtime_architecture, "_roi_certification_runtime_architecture", True)
        DirectSolanaIngestionPlane.status = _direct_status_with_runtime_architecture  # type: ignore[method-assign]


__all__ = [
    "EVENT_LOOP_SAMPLE_SECONDS",
    "RESEARCH_CURRENT_YIELD_MS",
    "RESEARCH_P95_YIELD_MS",
    "_interval_bounded_gap_fetch_delta",
    "_loop_lag_snapshot",
    "_managed_recovery_kick",
    "_research_pressure_reason",
    "_safe_hedged_call_with_meta",
    "_universal_checkpointed_slot_fetch_delta",
    "install_certification_runtime_architecture_repair",
]
