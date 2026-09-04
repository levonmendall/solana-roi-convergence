from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from . import continuity_high_volume_checkpoint_architecture as checkpoint
from . import continuity_high_volume_poll_affinity_repair as affinity
from . import continuity_immediate_recovery_repair as immediate
from . import continuity_standby_rpc_priority_repair as standby_priority
from . import continuity_target_frontier_repair as frontier
from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from . import rpc_workload_governor as governor
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


# Confirmed pre-gap checkpoints are continuity metadata only. They never create
# market observations and never repair a recorded gap. A short cadence keeps the
# safe lower recovery cursor close enough to the live Pump frontier that the
# unchanged 3x1000 recovery window remains useful during bursts.
PROACTIVE_CHECKPOINT_MIN_INTERVAL_SECONDS = 0.50
PROACTIVE_CHECKPOINT_MAX_AGE_SECONDS = 2.50

_ORIGINAL_NOTIFICATION: Callable[..., Any] | None = None
_ORIGINAL_CHECKPOINT_FETCH: Callable[..., Any] | None = None
_ORIGINAL_KICK: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None


def _increment(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_pre_gap_frontier_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


def _cache(self: Any) -> dict[str, dict[str, Any]]:
    value = getattr(self, "_roi_pre_gap_frontier_cache", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_pre_gap_frontier_cache", value)
    return value


def _tasks(self: Any) -> dict[str, asyncio.Task[Any]]:
    value = getattr(self, "_roi_pre_gap_frontier_tasks", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_pre_gap_frontier_tasks", value)
    return value


def _last_scheduled(self: Any) -> dict[str, float]:
    value = getattr(self, "_roi_pre_gap_frontier_last_scheduled", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_pre_gap_frontier_last_scheduled", value)
    return value


def _runtime_generation(self: Any, target: WatchTarget, default: int) -> int:
    key = live_poll._poll_target_key(target)
    row = lease._runtime(self).get(key, {})
    try:
        return int(row.get("cursor_ws_generation", default) or 0)
    except (TypeError, ValueError):
        return int(default)


def _state_cursor(self: Any, target: WatchTarget) -> int:
    row = live_poll._poll_state(self).get(live_poll._poll_target_key(target))
    if not isinstance(row, dict) or not bool(row.get("baseline_established")):
        return 0
    try:
        return int(row.get("cursor_slot") or 0)
    except (TypeError, ValueError):
        return 0


def _cache_row_is_fresh(row: Any, *, now: float | None = None) -> bool:
    if not isinstance(row, dict):
        return False
    try:
        captured = float(row.get("captured_monotonic") or 0.0)
    except (TypeError, ValueError):
        return False
    age = (time.monotonic() if now is None else float(now)) - captured
    return 0.0 <= age <= PROACTIVE_CHECKPOINT_MAX_AGE_SECONDS


async def _confirm_pre_gap_frontier(self: Any, target: WatchTarget) -> None:
    """Publish one same-generation confirmed cursor without changing evidence."""

    key = live_poll._poll_target_key(target)
    generation = int(lease._current_ws_generation(self, target))
    cursor_slot = _state_cursor(self, target)
    if cursor_slot <= 0:
        _increment(self, "skipped_no_baseline")
        return
    if not live_poll._ws_target_covered(self, target):
        _increment(self, "skipped_no_websocket")
        return
    if _runtime_generation(self, target, generation) != generation:
        _increment(self, "blocked_unrecovered_generation")
        return

    _increment(self, "confirmation_attempts")
    try:
        with governor.rpc_workload(standby_priority.WORKLOAD_STANDBY):
            effective_cursor, anchor = await frontier._confirmed_target_frontier_cursor(
                self,
                target,
                cursor_slot,
                generation,
            )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        setattr(self, "_roi_pre_gap_frontier_last_error_type", type(exc).__name__)
        _increment(self, "confirmation_errors")
        return

    if (
        not live_poll._ws_target_covered(self, target)
        or int(lease._current_ws_generation(self, target)) != generation
        or _runtime_generation(self, target, generation) != generation
        or not isinstance(anchor, dict)
        or str(anchor.get("source") or "") != "confirmed-target-websocket-frontier"
        or int(effective_cursor) <= cursor_slot
    ):
        _increment(self, "confirmation_rejected")
        return

    row = {
        "target": key,
        "generation": generation,
        "prior_cursor_slot": cursor_slot,
        "checkpoint_cursor_slot": int(effective_cursor),
        "confirmed_frontier_slot": int(anchor.get("confirmed_frontier_slot") or 0),
        "confirmation_provider": anchor.get("confirmation_provider"),
        "confirmation_latency_ms": anchor.get("confirmation_latency_ms"),
        "captured_monotonic": time.monotonic(),
        "same_slot_replay_required": True,
        "source": "proactive-confirmed-target-websocket-frontier",
    }
    existing = _cache(self).get(key)
    if not isinstance(existing, dict) or int(existing.get("checkpoint_cursor_slot") or 0) < int(effective_cursor):
        _cache(self)[key] = row
        _increment(self, "confirmed_checkpoints")
        setattr(self, "_roi_pre_gap_frontier_last_checkpoint", row)


def _schedule_pre_gap_checkpoint(self: Any, target: WatchTarget) -> None:
    if not affinity._is_high_volume_target(target):
        return
    key = live_poll._poll_target_key(target)
    now = time.monotonic()
    last = float(_last_scheduled(self).get(key, 0.0) or 0.0)
    if now - last < PROACTIVE_CHECKPOINT_MIN_INTERVAL_SECONDS:
        return
    current = _tasks(self).get(key)
    if isinstance(current, asyncio.Task) and not current.done():
        return

    _last_scheduled(self)[key] = now
    task = asyncio.create_task(
        _confirm_pre_gap_frontier(self, target),
        name=f"pre-gap-frontier:{target.kind}:{target.address[:8]}",
    )
    _tasks(self)[key] = task
    _increment(self, "tasks_started")

    def done(completed: asyncio.Task[Any]) -> None:
        if _tasks(self).get(key) is completed:
            _tasks(self).pop(key, None)
        try:
            completed.exception()
        except asyncio.CancelledError:
            pass
        except BaseException as exc:
            setattr(self, "_roi_pre_gap_frontier_last_task_error_type", type(exc).__name__)
            _increment(self, "task_errors")

    task.add_done_callback(done)


async def _notification_with_pre_gap_checkpoint(
    self: Any,
    provider: str,
    subscription_targets: dict[int, WatchTarget],
    message: dict[str, Any],
) -> None:
    if _ORIGINAL_NOTIFICATION is None:
        raise RuntimeError("high-volume pre-gap frontier repair is not installed")
    parsed = frontier._parse_target_notification(subscription_targets, message)
    if parsed is not None:
        target, signature, slot = parsed
        if affinity._is_high_volume_target(target):
            frontier._observe_target_frontier(
                self,
                provider,
                target,
                signature,
                slot,
                observed_monotonic=time.monotonic(),
            )
    await _ORIGINAL_NOTIFICATION(self, provider, subscription_targets, message)
    if parsed is not None:
        target, _signature, _slot = parsed
        _schedule_pre_gap_checkpoint(self, target)


setattr(_notification_with_pre_gap_checkpoint, "_roi_high_volume_pre_gap_frontier", True)


async def _checkpoint_fetch_with_pre_gap_cache(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    """Let the unchanged four-second poll consume a recent confirmed checkpoint."""

    if _ORIGINAL_CHECKPOINT_FETCH is None:
        raise RuntimeError("high-volume pre-gap frontier repair is not installed")
    if not affinity._is_high_volume_target(target):
        return await _ORIGINAL_CHECKPOINT_FETCH(self, target, cursor_slot)

    key = live_poll._poll_target_key(target)
    row = _cache(self).get(key)
    # Preserve legacy/test call shape: when no usable proactive proof exists, do
    # not read generation state before delegating to PR #99's canonical guard.
    if not _cache_row_is_fresh(row):
        return await _ORIGINAL_CHECKPOINT_FETCH(self, target, cursor_slot)
    if int(row.get("checkpoint_cursor_slot") or 0) <= int(cursor_slot):
        return await _ORIGINAL_CHECKPOINT_FETCH(self, target, cursor_slot)

    generation = int(lease._current_ws_generation(self, target))
    if (
        live_poll._ws_target_covered(self, target)
        and _runtime_generation(self, target, generation) == generation
        and int(row.get("generation", -1)) == generation
    ):
        effective_cursor = int(row["checkpoint_cursor_slot"])
        _increment(self, "cache_hits")
        counts = checkpoint._checkpoint_counts(self)
        counts[key] = int(counts.get(key, 0) or 0) + 1
        checkpoint._last_checkpoints(self)[key] = {
            "target": key,
            "source": str(target.source_hint or target.kind),
            "generation": generation,
            "prior_cursor_slot": int(cursor_slot),
            "checkpoint_cursor_slot": effective_cursor,
            "confirmed_frontier_slot": int(row.get("confirmed_frontier_slot") or 0),
            "confirmation_provider": row.get("confirmation_provider"),
            "confirmation_latency_ms": row.get("confirmation_latency_ms"),
            "same_slot_replay_required": True,
            "universal_frozen_target_checkpoint": True,
            "proactive_pre_gap_cache_hit": True,
        }
        latency = row.get("confirmation_latency_ms")
        return (
            [{"signature": "", "slot": effective_cursor, "err": None, "_roi_standby_checkpoint": True}],
            True,
            str(row.get("confirmation_provider") or "") or None,
            float(latency) if latency is not None else None,
        )

    return await _ORIGINAL_CHECKPOINT_FETCH(self, target, cursor_slot)


setattr(_checkpoint_fetch_with_pre_gap_cache, "_roi_high_volume_pre_gap_frontier", True)


def _kick_with_pre_gap_checkpoint(self: Any, target: WatchTarget, generation: int) -> None:
    """Start real-gap recovery from the freshest confirmed pre-gap lower cursor."""

    if _ORIGINAL_KICK is None:
        raise RuntimeError("high-volume pre-gap frontier repair is not installed")
    if affinity._is_high_volume_target(target):
        key = live_poll._poll_target_key(target)
        row = _cache(self).get(key)
        state = live_poll._poll_state(self).get(key)
        if (
            isinstance(state, dict)
            and bool(state.get("baseline_established"))
            and _cache_row_is_fresh(row)
            and int(row.get("generation", -2)) + 1 == int(generation)
        ):
            try:
                cached_cursor = int(row.get("checkpoint_cursor_slot") or 0)
                state_cursor = int(state.get("cursor_slot") or 0)
            except (TypeError, ValueError):
                cached_cursor = 0
                state_cursor = 0
            if cached_cursor > state_cursor:
                state["cursor_slot"] = cached_cursor
                state["pre_gap_checkpoint_applied"] = True
                state["pre_gap_checkpoint_generation"] = int(row.get("generation", -1))
                state["pre_gap_checkpoint_age_ms"] = round(
                    max(0.0, (time.monotonic() - float(row.get("captured_monotonic") or 0.0)) * 1000.0),
                    3,
                )
                _increment(self, "recovery_cursor_upgrades")
    _ORIGINAL_KICK(self, target, generation)


setattr(_kick_with_pre_gap_checkpoint, "_roi_high_volume_pre_gap_frontier", True)


def _status_with_pre_gap_frontier(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("high-volume pre-gap frontier repair is not installed")
    payload = _ORIGINAL_STATUS(self)
    now = time.monotonic()
    cache_rows: dict[str, Any] = {}
    for key, row in _cache(self).items():
        if not isinstance(row, dict):
            continue
        captured = float(row.get("captured_monotonic") or 0.0)
        cache_rows[key] = {
            "generation": int(row.get("generation", 0) or 0),
            "checkpoint_cursor_slot": int(row.get("checkpoint_cursor_slot", 0) or 0),
            "confirmed_frontier_slot": int(row.get("confirmed_frontier_slot", 0) or 0),
            "age_ms": round(max(0.0, (now - captured) * 1000.0), 3),
            "fresh": _cache_row_is_fresh(row, now=now),
            "confirmation_provider": row.get("confirmation_provider"),
            "confirmation_latency_ms": row.get("confirmation_latency_ms"),
        }

    payload["high_volume_pre_gap_frontier"] = {
        "installed": True,
        "scope": sorted(affinity.HIGH_VOLUME_ROUTINE_SOURCES),
        "proactive_confirmation_interval_seconds": PROACTIVE_CHECKPOINT_MIN_INTERVAL_SECONDS,
        "max_cache_age_seconds": PROACTIVE_CHECKPOINT_MAX_AGE_SECONDS,
        "tasks_active": sum(1 for task in _tasks(self).values() if not task.done()),
        "tasks_started": int(getattr(self, "_roi_pre_gap_frontier_tasks_started", 0) or 0),
        "confirmation_attempts": int(getattr(self, "_roi_pre_gap_frontier_confirmation_attempts", 0) or 0),
        "confirmed_checkpoints": int(getattr(self, "_roi_pre_gap_frontier_confirmed_checkpoints", 0) or 0),
        "confirmation_rejected": int(getattr(self, "_roi_pre_gap_frontier_confirmation_rejected", 0) or 0),
        "confirmation_errors": int(getattr(self, "_roi_pre_gap_frontier_confirmation_errors", 0) or 0),
        "blocked_unrecovered_generation": int(getattr(self, "_roi_pre_gap_frontier_blocked_unrecovered_generation", 0) or 0),
        "cache_hits": int(getattr(self, "_roi_pre_gap_frontier_cache_hits", 0) or 0),
        "recovery_cursor_upgrades": int(getattr(self, "_roi_pre_gap_frontier_recovery_cursor_upgrades", 0) or 0),
        "last_error_type": getattr(self, "_roi_pre_gap_frontier_last_error_type", None),
        "cache": cache_rows,
        "same_slot_replay_preserved": True,
        "recorded_gap_can_be_repaired_by_checkpoint": False,
        "websocket_transport_memory_boundary_unchanged": True,
        "poll_interval_seconds_unchanged": live_poll.POLL_INTERVAL_SECONDS,
        "recoverability_lease_seconds_unchanged": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
        "hard_page_limit_unchanged": live_poll.POLL_CURSOR_MAX_PAGES,
        "hard_page_size_unchanged": live_poll.POLL_LIMIT,
    }

    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "high_volume_pre_gap_confirmed_frontier_enabled": True,
                "pre_gap_checkpoint_is_not_market_evidence": True,
                "pre_gap_checkpoint_cannot_restore_recorded_gap": True,
                "pre_gap_checkpoint_same_slot_replay_preserved": True,
                "pre_gap_checkpoint_requires_same_generation": True,
                "websocket_transport_memory_boundary_unchanged": True,
                "routine_poll_interval_unchanged": True,
                "continuity_lease_unchanged": True,
                "recovery_bound_unchanged": True,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    return payload


setattr(_status_with_pre_gap_frontier, "_roi_high_volume_pre_gap_frontier", True)


def install_high_volume_pre_gap_frontier_repair() -> None:
    """Keep high-volume standby recovery close to confirmed chain head, safely."""

    global _ORIGINAL_NOTIFICATION, _ORIGINAL_CHECKPOINT_FETCH, _ORIGINAL_KICK, _ORIGINAL_STATUS

    current_notification = DirectSolanaIngestionPlane._handle_notification
    if not bool(getattr(current_notification, "_roi_high_volume_pre_gap_frontier", False)):
        _ORIGINAL_NOTIFICATION = current_notification
        try:
            _notification_with_pre_gap_checkpoint.__dict__.update(getattr(current_notification, "__dict__", {}))
        except Exception:
            pass
        setattr(_notification_with_pre_gap_checkpoint, "_roi_high_volume_pre_gap_frontier", True)
        DirectSolanaIngestionPlane._handle_notification = _notification_with_pre_gap_checkpoint  # type: ignore[method-assign]

    current_checkpoint = checkpoint._checkpointed_slot_fetch_delta
    if not bool(getattr(current_checkpoint, "_roi_high_volume_pre_gap_frontier", False)):
        _ORIGINAL_CHECKPOINT_FETCH = current_checkpoint
        try:
            _checkpoint_fetch_with_pre_gap_cache.__dict__.update(getattr(current_checkpoint, "__dict__", {}))
        except Exception:
            pass
        setattr(_checkpoint_fetch_with_pre_gap_cache, "_roi_high_volume_pre_gap_frontier", True)
        checkpoint._checkpointed_slot_fetch_delta = _checkpoint_fetch_with_pre_gap_cache  # type: ignore[assignment]

    current_kick = immediate._kick_immediate_recovery
    if not bool(getattr(current_kick, "_roi_high_volume_pre_gap_frontier", False)):
        _ORIGINAL_KICK = current_kick
        immediate._kick_immediate_recovery = _kick_with_pre_gap_checkpoint  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_high_volume_pre_gap_frontier", False)):
        _ORIGINAL_STATUS = current_status
        try:
            _status_with_pre_gap_frontier.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_status_with_pre_gap_frontier, "_roi_high_volume_pre_gap_frontier", True)
        DirectSolanaIngestionPlane.status = _status_with_pre_gap_frontier  # type: ignore[method-assign]


__all__ = [
    "PROACTIVE_CHECKPOINT_MIN_INTERVAL_SECONDS",
    "PROACTIVE_CHECKPOINT_MAX_AGE_SECONDS",
    "install_high_volume_pre_gap_frontier_repair",
]
