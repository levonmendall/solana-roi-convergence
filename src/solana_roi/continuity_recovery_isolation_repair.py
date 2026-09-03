from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from . import continuity_gap_clock_repair as gap_clock
from . import continuity_immediate_recovery_repair as immediate
from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from . import poll_watermark_repair as watermark
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget
from .solana_rpc import SolanaRpcPool


RECOVERY_RETRY_YIELD_SECONDS = 0.05
_ATTRIBUTION_HISTORY_LIMIT = 8
_monotonic = time.monotonic


def _recovery_rpc(self: Any) -> SolanaRpcPool:
    pool = getattr(self, "_roi_gap_recovery_rpc_pool", None)
    if isinstance(pool, SolanaRpcPool):
        return pool
    endpoints = tuple(getattr(self.rpc, "endpoints", ()) or ())
    pool = SolanaRpcPool(
        endpoints,
        timeout_seconds=2.5,
        hedge_delay_seconds=0.15,
    )
    setattr(self, "_roi_gap_recovery_rpc_pool", pool)
    return pool


def _attribution_state(self: Any) -> dict[str, Any]:
    value = getattr(self, "_roi_gap_recovery_attribution", None)
    if not isinstance(value, dict):
        value = {
            "last_success": None,
            "last_failure": None,
            "failure_counts": {},
            "failure_history": [],
        }
        setattr(self, "_roi_gap_recovery_attribution", value)
    return value


def _gap_age(self: Any, target: WatchTarget) -> float | None:
    key = live_poll._poll_target_key(target)
    row = gap_clock._gap_clocks(self).get(key)
    if not isinstance(row, dict):
        return None
    try:
        return max(0.0, _monotonic() - float(row.get("started_monotonic")))
    except (TypeError, ValueError):
        return None


def _record_success(self: Any, payload: dict[str, Any]) -> None:
    state = _attribution_state(self)
    state["last_success"] = dict(payload)


def _record_failure(self: Any, payload: dict[str, Any]) -> None:
    state = _attribution_state(self)
    row = dict(payload)
    reason = str(row.get("reason") or "unknown")
    counts = state.setdefault("failure_counts", {})
    if isinstance(counts, dict):
        counts[reason] = int(counts.get(reason, 0) or 0) + 1
    state["last_failure"] = row
    history = state.setdefault("failure_history", [])
    if isinstance(history, list):
        history.append(row)
        del history[:-_ATTRIBUTION_HISTORY_LIMIT]


async def _isolated_gap_fetch_delta(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None, dict[str, Any]]:
    pages: list[list[dict[str, Any]]] = []
    page_providers: list[str | None] = []
    page_latencies: list[float | None] = []
    before: str | None = None
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
        result, provider, latency = await _recovery_rpc(self).call_with_meta(
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
        slots = [watermark._row_slot(row) for row in page]
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
                slot = watermark._row_slot(row)
                if not signature or signature in seen or slot <= cursor_slot:
                    continue
                seen.add(signature)
                rows.append(row)

    all_slots = [watermark._row_slot(row) for page in pages for row in page]
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
    }
    return rows, complete, provider, latency, meta


async def _recover_with_isolated_rpc(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
    generation: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    key = live_poll._poll_target_key(target)
    clock = gap_clock._gap_clocks(self).get(key)
    if isinstance(clock, dict):
        try:
            gap_started = float(clock.get("started_monotonic"))
        except (TypeError, ValueError):
            gap_started = _monotonic()
    else:
        gap_started = _monotonic()
    deadline = gap_started + lease.POLL_RECOVERABILITY_LEASE_SECONDS

    attempts = 0
    last_error: Exception | None = None
    last_meta: dict[str, Any] | None = None
    last_reason = "unknown"
    while True:
        attempt_started = _monotonic()
        if attempts > 0 and attempt_started > deadline:
            last_reason = "lease_expired_before_retry"
            break
        if immediate._generation(self, target) != int(generation):
            last_reason = "gap_generation_superseded"
            last_error = RuntimeError("real websocket gap generation superseded")
            break

        attempts += 1
        immediate._increment(self, "attempts")
        try:
            rows, complete, provider, latency, meta = await _isolated_gap_fetch_delta(
                self,
                target,
                cursor_slot,
            )
            last_meta = meta
            if complete:
                immediate._increment(self, "completed")
                if attempts > 1:
                    immediate._increment(self, "completed_after_retry")
                _record_success(
                    self,
                    {
                        "target": key,
                        "generation": int(generation),
                        "cursor_slot": int(cursor_slot),
                        "attempts": attempts,
                        "gap_age_seconds": _gap_age(self, target),
                        "provider": provider,
                        "latency_ms": latency,
                        **meta,
                    },
                )
                return rows, True, provider, latency
            last_reason = "bounded_page_limit_exhausted"
            last_error = RuntimeError("isolated bounded recovery remained incomplete")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_error = exc
            last_reason = f"rpc_exception:{type(exc).__name__}"
            last_meta = {
                "page_count": 0,
                "page_sizes": [],
                "page_providers": [],
                "page_latencies_ms": [],
                "cursor_slot": int(cursor_slot),
                "complete": False,
                "recovered_row_count": 0,
                "hard_page_limit": live_poll.POLL_CURSOR_MAX_PAGES,
                "hard_page_size": live_poll.POLL_LIMIT,
            }

        if _monotonic() <= deadline:
            immediate._increment(self, "retries")
            await asyncio.sleep(RECOVERY_RETRY_YIELD_SECONDS)
            continue
        last_reason = (
            "bounded_page_limit_exhausted_at_lease"
            if last_reason == "bounded_page_limit_exhausted"
            else f"{last_reason}:lease_exhausted"
        )
        break

    immediate._increment(self, "failed")
    failure = {
        "target": key,
        "generation": int(generation),
        "cursor_slot": int(cursor_slot),
        "attempts": attempts,
        "gap_age_seconds": _gap_age(self, target),
        "reason": last_reason,
        "error_type": type(last_error).__name__ if last_error is not None else None,
        "lease_seconds": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
    }
    if isinstance(last_meta, dict):
        failure.update(last_meta)
    _record_failure(self, failure)
    if last_error is not None:
        raise last_error
    raise RuntimeError("isolated real-gap recovery exhausted fixed recoverability lease")


def _kick_with_isolated_recovery(self: Any, target: WatchTarget, generation: int) -> None:
    key = live_poll._poll_target_key(target)
    state = live_poll._poll_state(self).get(key)
    if not isinstance(state, dict) or not bool(state.get("baseline_established")):
        immediate._increment(self, "kick_skipped_no_baseline")
        return
    try:
        cursor_slot = int(state.get("cursor_slot") or 0)
    except (TypeError, ValueError):
        cursor_slot = 0
    if cursor_slot <= 0:
        immediate._increment(self, "kick_skipped_no_cursor")
        return

    tasks = immediate._recovery_tasks(self)
    previous = tasks.get(key)
    if isinstance(previous, dict):
        previous_task = previous.get("task")
        if (
            int(previous.get("generation", -1)) == int(generation)
            and int(previous.get("cursor_slot", -1)) == cursor_slot
            and isinstance(previous_task, asyncio.Task)
            and not previous_task.done()
        ):
            return
        if isinstance(previous_task, asyncio.Task) and not previous_task.done():
            previous_task.cancel()

    task = asyncio.create_task(
        _recover_with_isolated_rpc(self, target, cursor_slot, generation),
        name=f"isolated-immediate-gap-recovery:{target.kind}:{target.address[:8]}",
    )
    tasks[key] = {
        "generation": int(generation),
        "cursor_slot": cursor_slot,
        "task": task,
        "started_monotonic": _monotonic(),
    }
    immediate._increment(self, "kicked")


def _status_with_recovery_isolation(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        poll = payload.get("live_poll_redundancy")
        attribution = _attribution_state(self)
        if isinstance(poll, dict):
            poll.update(
                {
                    "real_gap_recovery_dedicated_rpc_pool": True,
                    "real_gap_recovery_failure_attribution": True,
                    "real_gap_recovery_retry_yield_seconds": RECOVERY_RETRY_YIELD_SECONDS,
                    "real_gap_recovery_last_success": attribution.get("last_success"),
                    "real_gap_recovery_last_failure": attribution.get("last_failure"),
                    "real_gap_recovery_failure_counts": dict(attribution.get("failure_counts") or {}),
                    "real_gap_recovery_failure_history": list(attribution.get("failure_history") or []),
                    "real_gap_recovery_lease_seconds": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
                    "real_gap_recovery_hard_delta_bound_unchanged": True,
                }
            )
            pool = getattr(self, "_roi_gap_recovery_rpc_pool", None)
            if isinstance(pool, SolanaRpcPool):
                poll["real_gap_recovery_rpc_pool"] = pool.status()
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "live_poll_real_gap_recovery_dedicated_rpc_pool": True,
                    "live_poll_real_gap_recovery_failure_attribution": True,
                    "live_poll_recoverability_lease_seconds": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
                    "live_poll_hard_delta_bound_unchanged": True,
                    "live_poll_irrecoverable_interval_fails_release_closed": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_continuity_recovery_isolation", True)
    return status


def install_continuity_recovery_isolation_repair() -> None:
    # Preserve the published immediate recovery helper for compatibility tests.
    # The tracked state setter resolves its kick function dynamically, so production
    # can use the isolated recovery path without replacing the canonical lease worker
    # or the established helper contract.
    immediate._kick_immediate_recovery = _kick_with_isolated_recovery  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_continuity_recovery_isolation", False)):
        DirectSolanaIngestionPlane.status = _status_with_recovery_isolation(current_status)  # type: ignore[method-assign]


__all__ = [
    "RECOVERY_RETRY_YIELD_SECONDS",
    "install_continuity_recovery_isolation_repair",
    "_isolated_gap_fetch_delta",
    "_kick_with_isolated_recovery",
    "_recover_with_isolated_rpc",
]
