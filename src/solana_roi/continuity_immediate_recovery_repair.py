from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from . import continuity_durability_repair as durability
from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from . import target_quorum
from . import target_stream_fanout as fanout
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


_PREVIOUS_SET_TARGET_STATE = target_quorum._quorum_set_target_state
_PREVIOUS_PROXY_FETCH = durability._LeaseWatermarkProxy._slot_fetch_delta
_monotonic = time.monotonic


def _generation(self: Any, target: WatchTarget) -> int:
    return int(lease._ws_gap_generations(self).get(live_poll._poll_target_key(target), 0) or 0)


def _recovery_tasks(self: Any) -> dict[str, dict[str, Any]]:
    value = getattr(self, "_roi_immediate_gap_recovery_tasks", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_immediate_gap_recovery_tasks", value)
    return value


def _increment(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_immediate_gap_recovery_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


async def _recover_until_lease_boundary(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
    generation: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    """Spend the existing fixed lease on useful bounded recovery attempts.

    Each attempt preserves the established 3x1000 confirmed-slot delta and read-only
    hedging. The only scheduling change is that the first recovery begins at the
    actual real-WebSocket zero-coverage transition rather than waiting for the next
    four-second poll tick. An attempt that starts inside the unchanged 12-second
    lease may finish after it, matching the canonical worker's attempt-start grace.
    """

    deadline = _monotonic() + lease.POLL_RECOVERABILITY_LEASE_SECONDS
    attempts = 0
    last_error: Exception | None = None
    while True:
        if attempts > 0 and _monotonic() > deadline:
            break
        if _generation(self, target) != int(generation):
            raise RuntimeError("real websocket gap generation superseded")
        attempts += 1
        _increment(self, "attempts")
        try:
            rows, complete, provider, latency = await durability._hedged_gap_fetch_delta(
                self,
                target,
                cursor_slot,
            )
            if complete:
                _increment(self, "completed")
                if attempts > 1:
                    _increment(self, "completed_after_retry")
                return rows, True, provider, latency
            last_error = durability.RecoverableLivePollDeltaIncomplete(
                "immediate bounded recovery remained incomplete"
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_error = exc

        if _monotonic() <= deadline:
            _increment(self, "retries")
            await asyncio.sleep(0)
            continue
        break

    _increment(self, "failed")
    if last_error is not None:
        raise last_error
    raise durability.RecoverableLivePollDeltaIncomplete(
        "immediate bounded recovery exhausted fixed recoverability lease"
    )


def _kick_immediate_recovery(self: Any, target: WatchTarget, generation: int) -> None:
    key = live_poll._poll_target_key(target)
    state = live_poll._poll_state(self).get(key)
    if not isinstance(state, dict) or not bool(state.get("baseline_established")):
        _increment(self, "kick_skipped_no_baseline")
        return
    try:
        cursor_slot = int(state.get("cursor_slot") or 0)
    except (TypeError, ValueError):
        cursor_slot = 0
    if cursor_slot <= 0:
        _increment(self, "kick_skipped_no_cursor")
        return

    tasks = _recovery_tasks(self)
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
        _recover_until_lease_boundary(self, target, cursor_slot, generation),
        name=f"immediate-gap-recovery:{target.kind}:{target.address[:8]}",
    )
    tasks[key] = {
        "generation": int(generation),
        "cursor_slot": cursor_slot,
        "task": task,
        "started_monotonic": _monotonic(),
    }
    _increment(self, "kicked")


async def _set_target_state_with_immediate_recovery(
    self: Any,
    endpoint: Any,
    target: WatchTarget,
    *,
    connected: bool,
    error_type: str | None = None,
    error_code: int | None = None,
    error_message: str | None = None,
) -> None:
    is_real_ws = str(getattr(endpoint, "name", "")) != live_poll.POLL_PROVIDER_NAME
    before_generation = _generation(self, target) if is_real_ws else 0
    await _PREVIOUS_SET_TARGET_STATE(
        self,
        endpoint,
        target,
        connected=connected,
        error_type=error_type,
        error_code=error_code,
        error_message=error_message,
    )
    if not is_real_ws:
        return
    after_generation = _generation(self, target)
    if after_generation > before_generation:
        _kick_immediate_recovery(self, target, after_generation)


async def _immediate_aware_proxy_fetch(
    proxy: durability._LeaseWatermarkProxy,
    plane: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    """Consume recovery already started at gap onset without changing proxy identity."""

    key = live_poll._poll_target_key(target)
    runtime = lease._runtime(plane).get(key, {})
    current_generation = _generation(plane, target)
    cursor_generation = int(runtime.get("cursor_ws_generation", current_generation) or 0)
    if current_generation != cursor_generation:
        pending = _recovery_tasks(plane).get(key)
        if isinstance(pending, dict):
            task = pending.get("task")
            if (
                int(pending.get("generation", -1)) == current_generation
                and int(pending.get("cursor_slot", -1)) == int(cursor_slot)
                and isinstance(task, asyncio.Task)
            ):
                try:
                    result = await asyncio.shield(task)
                    _increment(plane, "consumed")
                    _recovery_tasks(plane).pop(key, None)
                    return result
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    _recovery_tasks(plane).pop(key, None)
                    raise durability.RecoverableLivePollDeltaIncomplete(
                        f"immediate real-gap recovery failed closed: {type(exc).__name__}"
                    ) from exc
    return await _PREVIOUS_PROXY_FETCH(proxy, plane, target, cursor_slot)


setattr(_immediate_aware_proxy_fetch, "_roi_immediate_recovery_fetch", True)
_set_target_state_with_immediate_recovery.__name__ = "_tracked_quorum_set_target_state"


def _status_with_immediate_recovery(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        poll = payload.get("live_poll_redundancy")
        if isinstance(poll, dict):
            poll.update(
                {
                    "real_gap_recovery_kicks_immediately": True,
                    "real_gap_recovery_waits_for_poll_interval": False,
                    "real_gap_recovery_lease_seconds": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
                    "real_gap_recovery_hard_delta_bound_unchanged": True,
                    "real_gap_recovery_reads_hedged": True,
                    "real_gap_recovery_attempt_start_grace_preserved": True,
                    "immediate_recovery_kicked": int(getattr(self, "_roi_immediate_gap_recovery_kicked", 0) or 0),
                    "immediate_recovery_attempts": int(getattr(self, "_roi_immediate_gap_recovery_attempts", 0) or 0),
                    "immediate_recovery_retries": int(getattr(self, "_roi_immediate_gap_recovery_retries", 0) or 0),
                    "immediate_recovery_completed": int(getattr(self, "_roi_immediate_gap_recovery_completed", 0) or 0),
                    "immediate_recovery_completed_after_retry": int(getattr(self, "_roi_immediate_gap_recovery_completed_after_retry", 0) or 0),
                    "immediate_recovery_failed": int(getattr(self, "_roi_immediate_gap_recovery_failed", 0) or 0),
                    "immediate_recovery_consumed": int(getattr(self, "_roi_immediate_gap_recovery_consumed", 0) or 0),
                }
            )
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "live_poll_real_gap_recovery_kickoff": "actual-zero-websocket-coverage-transition",
                    "live_poll_real_gap_recovery_waits_for_next_tick": False,
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
    setattr(status, "_roi_continuity_immediate_recovery", True)
    return status


def install_continuity_immediate_recovery_repair() -> None:
    # Preserve the established helper identity: callers and regressions expect the
    # lease module's tracked setter to be the exact object installed into target
    # quorum/fanout.
    lease._tracked_quorum_set_target_state = _set_target_state_with_immediate_recovery  # type: ignore[assignment]
    target_quorum._quorum_set_target_state = lease._tracked_quorum_set_target_state  # type: ignore[assignment]
    fanout._set_target_state = lease._tracked_quorum_set_target_state  # type: ignore[assignment]

    # Preserve the durability proxy itself and its `_base is watermark` contract.
    # Extend the class method in place rather than stacking another proxy object.
    current_fetch = durability._LeaseWatermarkProxy._slot_fetch_delta
    if not bool(getattr(current_fetch, "_roi_immediate_recovery_fetch", False)):
        durability._LeaseWatermarkProxy._slot_fetch_delta = _immediate_aware_proxy_fetch  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_continuity_immediate_recovery", False)):
        DirectSolanaIngestionPlane.status = _status_with_immediate_recovery(current_status)  # type: ignore[method-assign]


__all__ = [
    "install_continuity_immediate_recovery_repair",
    "_immediate_aware_proxy_fetch",
    "_kick_immediate_recovery",
    "_recover_until_lease_boundary",
]
