from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from . import continuity_durability_repair as durability
from . import continuity_early_loss_detection_repair as early_loss
from . import continuity_immediate_recovery_repair as immediate
from . import continuity_recovery_isolation_repair as isolation
from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


_ORIGINAL_CONFIRMED_SNAPSHOT: Callable[..., Any] | None = None
_ORIGINAL_ISOLATED_RECOVERY: Callable[..., Any] | None = None
_ORIGINAL_PROXY_FETCH: Callable[..., Any] | None = None


def _increment(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_generation_floor_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


def _floors(self: Any) -> dict[str, dict[str, Any]]:
    value = getattr(self, "_roi_confirmed_generation_recovery_floors", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_confirmed_generation_recovery_floors", value)
    return value


def _remember_confirmed_floor(
    self: Any,
    target: WatchTarget,
    generation: int,
    effective_cursor: int,
    anchor: dict[str, Any],
) -> None:
    """Persist only a confirmed/finalized, same-slot-replay-safe recovery floor."""

    if str(anchor.get("source") or "") not in {
        "confirmed-target-websocket-frontier-at-gap",
        "confirmed-target-websocket-frontier",
    }:
        return
    if not bool(anchor.get("same_slot_replay_required")):
        return
    confirmed_slot = int(anchor.get("confirmed_frontier_slot") or 0)
    safe_floor = int(effective_cursor)
    if confirmed_slot <= 0 or safe_floor <= 0 or safe_floor > confirmed_slot - 1:
        return
    key = live_poll._poll_target_key(target)
    current = _floors(self).get(key)
    if (
        isinstance(current, dict)
        and int(current.get("generation", -1)) == int(generation)
        and int(current.get("safe_cursor_slot", 0) or 0) >= safe_floor
    ):
        return
    _floors(self)[key] = {
        "generation": int(generation),
        "safe_cursor_slot": safe_floor,
        "confirmed_frontier_slot": confirmed_slot,
        "source": str(anchor.get("source") or ""),
        "same_slot_replay_required": True,
    }
    _increment(self, "confirmed_floors")


def _confirmed_floor(self: Any, target: WatchTarget, generation: int) -> int | None:
    row = _floors(self).get(live_poll._poll_target_key(target))
    if not isinstance(row, dict) or int(row.get("generation", -1)) != int(generation):
        return None
    try:
        value = int(row.get("safe_cursor_slot") or 0)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


async def _confirmed_snapshot_with_generation_floor(
    self: Any,
    target: WatchTarget,
    routine_cursor_slot: int,
    generation: int,
    candidates: list[dict[str, Any]],
) -> tuple[int, dict[str, Any] | None]:
    if _ORIGINAL_CONFIRMED_SNAPSHOT is None:
        raise RuntimeError("continuity generation-floor repair is not installed")
    effective_cursor, anchor = await _ORIGINAL_CONFIRMED_SNAPSHOT(
        self,
        target,
        routine_cursor_slot,
        generation,
        candidates,
    )
    if isinstance(anchor, dict):
        _remember_confirmed_floor(
            self,
            target,
            generation,
            int(effective_cursor),
            anchor,
        )
    return int(effective_cursor), anchor


async def _isolated_recovery_with_generation_floor(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
    generation: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    """Never let a concurrent recovery regress below a confirmed gap frontier."""

    if _ORIGINAL_ISOLATED_RECOVERY is None:
        raise RuntimeError("continuity generation-floor repair is not installed")
    requested_cursor = int(cursor_slot)
    safe_floor = _confirmed_floor(self, target, generation)
    effective_cursor = max(requested_cursor, int(safe_floor or 0))
    if effective_cursor > requested_cursor:
        _increment(self, "stale_cursor_advances")
        setattr(
            self,
            "_roi_generation_floor_last_advance",
            {
                "target": live_poll._poll_target_key(target),
                "generation": int(generation),
                "requested_cursor_slot": requested_cursor,
                "effective_cursor_slot": effective_cursor,
                "same_slot_replay_preserved": True,
            },
        )
    return await _ORIGINAL_ISOLATED_RECOVERY(
        self,
        target,
        effective_cursor,
        generation,
    )


async def _proxy_fetch_with_generation_task_authority(
    proxy: durability._LeaseWatermarkProxy,
    plane: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    """Consume the gap-onset recovery for its generation even if the poll cursor moved.

    The prior exact-cursor equality check allowed the routine poll watermark to move
    while the confirmed snapshot task was running. The canonical worker would then
    ignore that safer in-flight task and start a second recovery from the older/newer
    routine watermark. Generation identity is the correct ownership boundary: the
    snapshot was frozen at that exact zero-WebSocket transition and its frontier is
    separately confirmed/finalized before use.
    """

    if _ORIGINAL_PROXY_FETCH is None:
        raise RuntimeError("continuity generation-floor repair is not installed")
    key = live_poll._poll_target_key(target)
    runtime = lease._runtime(plane).get(key, {})
    current_generation = immediate._generation(plane, target)
    cursor_generation = int(runtime.get("cursor_ws_generation", current_generation) or 0)
    if current_generation != cursor_generation:
        pending = immediate._recovery_tasks(plane).get(key)
        if isinstance(pending, dict):
            task = pending.get("task")
            if (
                int(pending.get("generation", -1)) == current_generation
                and isinstance(task, asyncio.Task)
            ):
                pending_cursor = int(pending.get("cursor_slot", 0) or 0)
                if pending_cursor != int(cursor_slot):
                    _increment(plane, "cursor_mismatch_tasks_consumed")
                    setattr(
                        plane,
                        "_roi_generation_floor_last_task_cursor_mismatch",
                        {
                            "target": key,
                            "generation": current_generation,
                            "task_routine_cursor_slot": pending_cursor,
                            "caller_cursor_slot": int(cursor_slot),
                        },
                    )
                try:
                    result = await asyncio.shield(task)
                    current = immediate._recovery_tasks(plane).get(key)
                    if isinstance(current, dict) and current.get("task") is task:
                        immediate._recovery_tasks(plane).pop(key, None)
                    immediate._increment(plane, "consumed")
                    _increment(plane, "generation_tasks_consumed")
                    return result
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    current = immediate._recovery_tasks(plane).get(key)
                    if isinstance(current, dict) and current.get("task") is task:
                        immediate._recovery_tasks(plane).pop(key, None)
                    _increment(plane, "generation_task_failures")
                    raise durability.RecoverableLivePollDeltaIncomplete(
                        f"authoritative same-generation gap recovery failed closed: {type(exc).__name__}"
                    ) from exc
    return await _ORIGINAL_PROXY_FETCH(proxy, plane, target, cursor_slot)


setattr(_confirmed_snapshot_with_generation_floor, "_roi_confirmed_generation_floor", True)
setattr(_isolated_recovery_with_generation_floor, "_roi_confirmed_generation_floor", True)
setattr(_proxy_fetch_with_generation_task_authority, "_roi_confirmed_generation_floor", True)


def _status_with_generation_floor(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        poll = payload.get("live_poll_redundancy")
        if isinstance(poll, dict):
            poll.update(
                {
                    "confirmed_generation_recovery_floor": True,
                    "same_generation_gap_task_authoritative_across_poll_cursor_motion": True,
                    "generation_floor_confirmed_count": int(getattr(self, "_roi_generation_floor_confirmed_floors", 0) or 0),
                    "generation_floor_stale_cursor_advances": int(getattr(self, "_roi_generation_floor_stale_cursor_advances", 0) or 0),
                    "generation_floor_cursor_mismatch_tasks_consumed": int(getattr(self, "_roi_generation_floor_cursor_mismatch_tasks_consumed", 0) or 0),
                    "generation_floor_tasks_consumed": int(getattr(self, "_roi_generation_floor_generation_tasks_consumed", 0) or 0),
                    "generation_floor_task_failures": int(getattr(self, "_roi_generation_floor_generation_task_failures", 0) or 0),
                    "generation_floor_last_advance": getattr(self, "_roi_generation_floor_last_advance", None),
                    "generation_floor_last_task_cursor_mismatch": getattr(self, "_roi_generation_floor_last_task_cursor_mismatch", None),
                    "recoverability_lease_seconds": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
                    "hard_page_limit": live_poll.POLL_CURSOR_MAX_PAGES,
                    "hard_page_size": live_poll.POLL_LIMIT,
                }
            )
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "confirmed_gap_frontier_is_minimum_recovery_cursor_for_generation": True,
                    "confirmed_gap_frontier_same_slot_replay_preserved": True,
                    "live_poll_recoverability_lease_seconds": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
                    "live_poll_hard_delta_bound_unchanged": True,
                    "paper_only_authority_unchanged": True,
                    "signing_or_submission_available": False,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_continuity_generation_floor", True)
    return status


def install_continuity_generation_floor_repair() -> None:
    global _ORIGINAL_CONFIRMED_SNAPSHOT, _ORIGINAL_ISOLATED_RECOVERY, _ORIGINAL_PROXY_FETCH

    current_confirmed = early_loss._confirmed_snapshot_cursor
    if not bool(getattr(current_confirmed, "_roi_confirmed_generation_floor", False)):
        _ORIGINAL_CONFIRMED_SNAPSHOT = current_confirmed
        early_loss._confirmed_snapshot_cursor = _confirmed_snapshot_with_generation_floor  # type: ignore[assignment]

    current_isolated = isolation._recover_with_isolated_rpc
    if not bool(getattr(current_isolated, "_roi_confirmed_generation_floor", False)):
        _ORIGINAL_ISOLATED_RECOVERY = current_isolated
        isolation._recover_with_isolated_rpc = _isolated_recovery_with_generation_floor  # type: ignore[assignment]

    current_proxy = durability._LeaseWatermarkProxy._slot_fetch_delta
    if not bool(getattr(current_proxy, "_roi_confirmed_generation_floor", False)):
        _ORIGINAL_PROXY_FETCH = current_proxy
        durability._LeaseWatermarkProxy._slot_fetch_delta = _proxy_fetch_with_generation_task_authority  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_continuity_generation_floor", False)):
        DirectSolanaIngestionPlane.status = _status_with_generation_floor(current_status)  # type: ignore[method-assign]


__all__ = [
    "_confirmed_floor",
    "_confirmed_snapshot_with_generation_floor",
    "_isolated_recovery_with_generation_floor",
    "_proxy_fetch_with_generation_task_authority",
    "install_continuity_generation_floor_repair",
]
