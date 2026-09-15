from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any

from . import robinhood_chain_runtime as runtime
from . import robinhood_v2_v4_observation as observation
from . import robinhood_v2_v4_pause_guard as pause_guard


REPAIR_VERSION = "robinhood-v2-v4-efficiency-v2"
_INSTALLED = False
_ORIGINAL_FETCH_FACTORY = observation._fetch_with_observation
_ORIGINAL_STATUS_FACTORY = observation._status_with_observation


async def _bounded_observe_range(self: Any, *, from_block: int, to_block: int) -> None:
    """Process one explicitly requested V2/V4 range without owning schedule authority.

    Production enable/disable belongs at the fetch/scheduling boundary. Keeping this
    primitive independent of the feature flag preserves exact cursor/resume behavior
    for recovery and deterministic tests while still allowing production to fail
    closed before the primitive is invoked.
    """
    observation._ensure_state(self)
    if from_block > to_block:
        return

    cursor = self._roi_v2v4_observation_cursor
    # Once the observer owns a persisted cursor, resume from that cursor rather than
    # clamping recovery to a newer caller range. Recovery remains bounded below.
    start = int(from_block) if cursor is None else int(cursor) + 1
    if start > to_block:
        return
    max_blocks = observation._max_recovery_blocks()
    span = to_block - start + 1
    if span > max_blocks:
        skipped = span - max_blocks
        start = to_block - max_blocks + 1
        observation._metric(self, "ranges_reanchored")
        observation._metric(self, "blocks_intentionally_not_backfilled", skipped)

    observed_at = runtime._utcnow()
    discovery = await observation.catchup._logs_with_resilient_range(
        self,
        from_block=start,
        to_block=to_block,
        addresses=[observation.UNISWAP_V2_FACTORY, runtime.UNISWAP_V4_POOL_MANAGER],
        topics=[[observation.UNISWAP_V2_PAIR_CREATED_TOPIC, observation.UNISWAP_V4_INITIALIZE_TOPIC]],
    )
    observation._metric(self, "discovery_requests")
    changed = False
    for log in sorted(discovery, key=observation._event_key):
        try:
            changed = observation._register_discovery(self, log, observed_at=observed_at) or changed
        except Exception:
            observation._metric(self, "parse_failures")
    if changed:
        observation._trim_registries(self, persist=True)

    rows: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    pairs = list(self._roi_v2_observation_pairs.values())
    v4_pools = list(self._roi_v4_observation_pools.values())
    batch_size = observation._v2_address_batch()
    jobs: list[Awaitable[tuple[str, list[dict[str, Any]], list[dict[str, Any]]]]] = []

    async def fetch_v2(batch: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
        result = await observation.catchup._logs_with_resilient_range(
            self,
            from_block=start,
            to_block=to_block,
            addresses=[str(item["pair"]) for item in batch],
            topics=[observation.UNISWAP_V2_SWAP_TOPIC],
        )
        return "v2", batch, result

    for index in range(0, len(pairs), batch_size):
        batch = pairs[index : index + batch_size]
        if batch:
            jobs.append(fetch_v2(batch))

    async def fetch_v4(pools: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
        pool_ids = [str(item["pool_id"]) for item in pools if item.get("pool_id")]
        result = await observation.catchup._logs_with_resilient_range(
            self,
            from_block=start,
            to_block=to_block,
            addresses=[runtime.UNISWAP_V4_POOL_MANAGER],
            # Ethereum JSON-RPC topic-array semantics are OR within one topic slot.
            # Restrict topic1 to the exact tracked pool IDs so unrelated PoolManager
            # swaps never enter provider response buffers or local decode/filter work.
            topics=[runtime.V4_SWAP_TOPIC, pool_ids],
        )
        return "v4", pools, result

    if v4_pools:
        jobs.append(fetch_v4(v4_pools))
        observation._metric(self, "v4_activity_requests")
        observation._metric(self, "v4_pool_filters_submitted", len(v4_pools))
    else:
        observation._metric(self, "v4_activity_requests_suppressed_empty")

    expected_market_requests = int(math.ceil(len(pairs) / batch_size)) + (1 if v4_pools else 0)
    observation._metric(self, "expected_market_requests", expected_market_requests)

    gate = asyncio.Semaphore(2)

    async def gated(job: Awaitable[tuple[str, list[dict[str, Any]], list[dict[str, Any]]]]) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
        async with gate:
            return await job

    results = await asyncio.gather(*(gated(job) for job in jobs)) if jobs else []
    observation._metric(self, "market_requests", len(results))
    for kind, batch, logs in results:
        if kind == "v2":
            by_pair = {str(item["pair"]): item for item in batch}
            for log in logs:
                meta = by_pair.get(runtime._clean_address(log.get("address")))
                if meta is not None:
                    rows.append(("v2", meta, log))
        else:
            by_pool = {str(item["pool_id"]): item for item in batch}
            for log in logs:
                topics = [str(topic).lower() for topic in (log.get("topics") or [])]
                pool = observation._pool_id(topics[1]) if len(topics) > 1 else ""
                meta = by_pool.get(pool)
                if meta is None:
                    observation._metric(self, "untracked_v4_swaps_ignored")
                    continue
                rows.append(("v4", meta, log))

    rows.sort(key=lambda item: observation._event_key(item[2]))
    for kind, meta, log in rows:
        try:
            payload = observation._v2_swap_payload(meta, log) if kind == "v2" else observation._v4_swap_payload(meta, log)
            if payload is not None:
                observation._persist_swap(self, payload, observed_at=observed_at)
        except Exception:
            observation._metric(self, "parse_failures")

    self._roi_v2v4_observation_cursor = int(to_block)
    observation._set_state_json(self, observation._STATE_CURSOR, int(to_block))
    self._roi_v2v4_observation_last_success_at = runtime._utcnow()
    self._roi_v2v4_observation_last_error = None
    self._roi_v2v4_observation_last_range = {
        "from_block": int(start),
        "to_block": int(to_block),
        "discovery_logs": len(discovery),
        "activity_logs": len(rows),
        "expected_market_requests": expected_market_requests,
        "actual_market_requests": len(results),
        "tracked_v2_pairs": len(pairs),
        "tracked_v4_pools": len(v4_pools),
        "v4_pool_filtering": bool(v4_pools),
        "bounded_recovery": True,
    }
    observation._metric(self, "ranges_completed")


setattr(_bounded_observe_range, "_roi_v2_v4_efficiency_repair", True)
setattr(_bounded_observe_range, "_roi_v2_v4_observation_resume", True)


def _gated_fetch_factory(
    original: Callable[..., Awaitable[list[tuple[str, Any, dict[str, Any]]]]],
) -> Callable[..., Awaitable[list[tuple[str, Any, dict[str, Any]]]]]:
    @wraps(original)
    async def wrapped(self: Any, *, from_block: int, to_block: int) -> list[tuple[str, Any, dict[str, Any]]]:
        canonical = await original(self, from_block=from_block, to_block=to_block)
        if not pause_guard.schedule_enabled(self):
            return canonical
        try:
            await observation._observe_range(self, from_block=from_block, to_block=to_block)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            observation._ensure_state(self)
            observation._metric(self, "ranges_failed")
            self._roi_v2v4_observation_last_error = f"{type(exc).__name__}: {exc}"
        return canonical

    setattr(wrapped, "_roi_v2_v4_observation_fetch", True)
    setattr(wrapped, "_roi_v2_v4_efficiency_gate", True)
    return wrapped


def _status_factory(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    base = _ORIGINAL_STATUS_FACTORY(original)

    @wraps(base)
    def wrapped(self: Any) -> dict[str, Any]:
        payload = base(self)
        status = payload.get("v2_v4_observation")
        if isinstance(status, dict):
            guarded = pause_guard._is_schedule_guarded(self)
            if guarded:
                status["enabled"] = pause_guard._explicit_enable_only()
                status["production_default_enabled"] = False
                status["reactivation_requires_explicit_true"] = True
            status.update(
                {
                    "schedule_gate_separate_from_observer_primitive": True,
                    "production_schedule_guarded": guarded,
                    "zero_pool_v4_activity_suppressed": True,
                    "v4_tracked_pool_topic_filtering": True,
                    "efficiency_repair_version": REPAIR_VERSION,
                }
            )
        return payload

    setattr(wrapped, "_roi_v2_v4_observation_status", True)
    setattr(wrapped, "_roi_v2_v4_efficiency_status", True)
    return wrapped


def install_robinhood_v2_v4_efficiency_repair() -> None:
    global _INSTALLED
    observation._observe_range = _bounded_observe_range
    observation._fetch_with_observation = _gated_fetch_factory
    observation._status_with_observation = _status_factory
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "production_schedule_fail_closed": True,
        "observer_primitive_remains_resumable": True,
        "zero_pool_v4_activity_suppressed": True,
        "v4_poolmanager_topic1_filtered_to_tracked_pools": True,
        "expected_vs_actual_market_requests_recorded": True,
        "strategy_thresholds_changed": False,
        "candidate_universe_reduced": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "REPAIR_VERSION",
    "_bounded_observe_range",
    "install_robinhood_v2_v4_efficiency_repair",
    "status",
]
