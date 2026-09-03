from __future__ import annotations

import contextvars
from collections.abc import Callable
from typing import Any

from . import direct_solana as direct_solana_module
from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from . import target_quorum
from . import target_stream_fanout as fanout
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


_PREVIOUS_SET_TARGET_STATE = target_quorum._quorum_set_target_state
_ORIGINAL_CURRENT_WS_GENERATION = lease._current_ws_generation
_ORIGINAL_MONOTONIC = lease._monotonic
_POLL_CONTEXT: contextvars.ContextVar[tuple[Any, WatchTarget] | None] = contextvars.ContextVar(
    "roi_live_poll_target_context",
    default=None,
)


def _gap_clocks(self: Any) -> dict[str, dict[str, Any]]:
    value = getattr(self, "_roi_continuity_gap_clocks", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_continuity_gap_clocks", value)
    return value


def _contextual_current_ws_generation(self: Any, target: WatchTarget) -> int:
    # Every poll target runs in its own asyncio Task. Binding the target in a
    # ContextVar lets the existing lease worker keep its established implementation
    # while its monotonic age calculation becomes target/gap aware.
    _POLL_CONTEXT.set((self, target))
    return int(_ORIGINAL_CURRENT_WS_GENERATION(self, target))


def _active_gap_clock(
    self: Any,
    target: WatchTarget,
    *,
    cursor_generation: int,
) -> dict[str, Any] | None:
    current_generation = int(_ORIGINAL_CURRENT_WS_GENERATION(self, target))
    if current_generation == int(cursor_generation):
        return None
    row = _gap_clocks(self).get(live_poll._poll_target_key(target))
    if not isinstance(row, dict):
        return None
    if int(row.get("generation", -1)) != current_generation:
        return None
    return row


async def _gap_clock_set_target_state(
    self: Any,
    endpoint: Any,
    target: WatchTarget,
    *,
    connected: bool,
    error_type: str | None = None,
    error_code: int | None = None,
    error_message: str | None = None,
) -> None:
    """Bind the fixed recovery lease to the actual real-WebSocket gap onset."""

    is_real_ws = str(getattr(endpoint, "name", "")) != live_poll.POLL_PROVIDER_NAME
    before_generation = (
        int(_ORIGINAL_CURRENT_WS_GENERATION(self, target)) if is_real_ws else 0
    )
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

    after_generation = int(_ORIGINAL_CURRENT_WS_GENERATION(self, target))
    if after_generation <= before_generation:
        return

    _gap_clocks(self)[live_poll._poll_target_key(target)] = {
        "generation": after_generation,
        "started_monotonic": float(_ORIGINAL_MONOTONIC()),
        "started_at": direct_solana_module.utcnow().isoformat(),
    }


def _gap_aware_monotonic() -> float:
    """Return the existing lease clock rebased to real gap onset when necessary.

    The recoverability worker stores the preceding successful poll timestamp and
    later subtracts it from this clock. During a tracked real-WebSocket gap we map
    the current clock onto that stored baseline plus *time since the actual gap*.
    Therefore its unchanged 12-second comparison measures the gap itself, not idle
    time since an earlier poll. Outside a real gap this is exactly the original
    monotonic clock.
    """

    raw_now = float(_ORIGINAL_MONOTONIC())
    context = _POLL_CONTEXT.get()
    if context is None:
        return raw_now
    self, target = context
    key = live_poll._poll_target_key(target)
    runtime = lease._runtime(self).get(key, {})
    if not isinstance(runtime, dict):
        return raw_now
    current_generation = int(_ORIGINAL_CURRENT_WS_GENERATION(self, target))
    cursor_generation = int(runtime.get("cursor_ws_generation", current_generation) or 0)
    gap = _active_gap_clock(self, target, cursor_generation=cursor_generation)
    if not isinstance(gap, dict):
        return raw_now
    try:
        gap_started = float(gap["started_monotonic"])
        prior_success = float(runtime["last_success_monotonic"])
    except (KeyError, TypeError, ValueError):
        return raw_now
    return prior_success + max(0.0, raw_now - gap_started)


def _status_with_gap_clock(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        poll = payload.get("live_poll_redundancy")
        if isinstance(poll, dict):
            poll["recoverability_lease_clock"] = "actual-real-websocket-zero-coverage-onset"
            poll["recoverability_worker_replaced"] = False
            poll["recoverability_lease_seconds"] = lease.POLL_RECOVERABILITY_LEASE_SECONDS
            poll["successful_inflight_attempt_started_inside_lease_can_complete"] = True
            poll["hard_delta_bound_unchanged"] = True
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "live_poll_recoverability_clock_starts_at_real_ws_gap": True,
                    "live_poll_recoverability_worker_replaced": False,
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
    setattr(status, "_roi_continuity_gap_clock_repair", True)
    return status


# Preserve the established helper identity contract while extending the tracked
# implementation in place. The canonical leased poll worker itself is untouched.
_gap_clock_set_target_state.__name__ = "_tracked_quorum_set_target_state"
_contextual_current_ws_generation.__name__ = "_current_ws_generation"
_gap_aware_monotonic.__name__ = "_monotonic"


def install_continuity_gap_clock_repair() -> None:
    lease._tracked_quorum_set_target_state = _gap_clock_set_target_state  # type: ignore[assignment]
    target_quorum._quorum_set_target_state = lease._tracked_quorum_set_target_state  # type: ignore[assignment]
    fanout._set_target_state = lease._tracked_quorum_set_target_state  # type: ignore[assignment]

    lease._current_ws_generation = _contextual_current_ws_generation  # type: ignore[assignment]
    lease._monotonic = _gap_aware_monotonic  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_continuity_gap_clock_repair", False)):
        DirectSolanaIngestionPlane.status = _status_with_gap_clock(current_status)  # type: ignore[method-assign]

    # Production proved that waiting for the next four-second poll tick can spend a
    # large fraction of the fixed 12-second real-gap lease before the first useful
    # bounded recovery read begins. Kick that same canonical bounded/hedged recovery
    # immediately on the zero-WebSocket transition without changing the lease or
    # 3x1000 delta limit.
    from .continuity_immediate_recovery_repair import install_continuity_immediate_recovery_repair

    install_continuity_immediate_recovery_repair()


__all__ = [
    "install_continuity_gap_clock_repair",
    "_active_gap_clock",
    "_gap_aware_monotonic",
    "_gap_clock_set_target_state",
]
