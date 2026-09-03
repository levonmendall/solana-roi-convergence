from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from . import direct_solana as direct_solana_module
from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from . import target_quorum
from . import target_stream_fanout as fanout
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


_PREVIOUS_SET_TARGET_STATE = target_quorum._quorum_set_target_state


def _gap_clocks(self: Any) -> dict[str, dict[str, Any]]:
    value = getattr(self, "_roi_continuity_gap_clocks", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_continuity_gap_clocks", value)
    return value


def _gap_wakes(self: Any) -> dict[str, asyncio.Event]:
    value = getattr(self, "_roi_continuity_gap_wakes", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_continuity_gap_wakes", value)
    return value


def _wake_for(self: Any, target: WatchTarget) -> asyncio.Event:
    key = live_poll._poll_target_key(target)
    events = _gap_wakes(self)
    event = events.get(key)
    if not isinstance(event, asyncio.Event):
        event = asyncio.Event()
        events[key] = event
    return event


def _active_gap_clock(
    self: Any,
    target: WatchTarget,
    *,
    cursor_generation: int,
) -> dict[str, Any] | None:
    current_generation = lease._current_ws_generation(self, target)
    if current_generation == int(cursor_generation):
        return None
    row = _gap_clocks(self).get(live_poll._poll_target_key(target))
    if not isinstance(row, dict):
        return None
    if int(row.get("generation", -1)) != int(current_generation):
        return None
    resolved_generation = row.get("resolved_generation")
    if resolved_generation is not None and int(resolved_generation) == int(current_generation):
        return None
    return row


def _mark_gap_resolved(self: Any, target: WatchTarget, generation: int) -> None:
    row = _gap_clocks(self).get(live_poll._poll_target_key(target))
    if isinstance(row, dict) and int(row.get("generation", -1)) == int(generation):
        row["resolved_generation"] = int(generation)
        row["resolved_at"] = direct_solana_module.utcnow().isoformat()


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
    """Timestamp the actual loss of the real-WebSocket target union.

    The existing lease tracker remains authoritative for generation creation. This
    wrapper only binds a monotonic timestamp to that newly-created generation and
    wakes the already-running bounded poll worker. Synthetic polling never creates
    a gap clock.
    """

    is_real_ws = str(getattr(endpoint, "name", "")) != live_poll.POLL_PROVIDER_NAME
    before_generation = lease._current_ws_generation(self, target) if is_real_ws else 0
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

    after_generation = lease._current_ws_generation(self, target)
    if after_generation <= before_generation:
        return

    key = live_poll._poll_target_key(target)
    now = time.monotonic()
    clocks = _gap_clocks(self)
    clocks[key] = {
        "generation": int(after_generation),
        "started_monotonic": now,
        "started_at": direct_solana_module.utcnow().isoformat(),
        "resolved_generation": None,
        "recovery_attempts": 0,
    }
    _wake_for(self, target).set()


def _lease_ages(
    self: Any,
    target: WatchTarget,
    *,
    cursor_generation: int,
    last_success_monotonic: float | None,
    attempt_started_monotonic: float,
    now_monotonic: float,
) -> tuple[float, float, str]:
    """Measure a real-gap lease from gap onset, never from an older poll success."""

    gap = _active_gap_clock(self, target, cursor_generation=cursor_generation)
    if isinstance(gap, dict):
        try:
            origin = float(gap["started_monotonic"])
        except (KeyError, TypeError, ValueError):
            origin = now_monotonic
        return (
            max(0.0, now_monotonic - origin),
            max(0.0, attempt_started_monotonic - origin),
            "real_websocket_gap_onset",
        )

    if last_success_monotonic is None:
        return (
            lease.POLL_RECOVERABILITY_LEASE_SECONDS + 1.0,
            lease.POLL_RECOVERABILITY_LEASE_SECONDS + 1.0,
            "no_proven_poll_baseline",
        )
    return (
        max(0.0, now_monotonic - last_success_monotonic),
        max(0.0, attempt_started_monotonic - last_success_monotonic),
        "last_successful_poll",
    )


def _state_row(*, lease_clock_source: str, **kwargs: Any) -> dict[str, Any]:
    row = lease._state_row(**kwargs)
    row["lease_clock_source"] = lease_clock_source
    return row


async def _wait_for_poll_or_gap(
    self: Any,
    target: WatchTarget,
    stop: asyncio.Event,
    delay: float,
) -> None:
    wake = _wake_for(self, target)
    if wake.is_set():
        wake.clear()
        return
    stop_task = asyncio.create_task(stop.wait())
    wake_task = asyncio.create_task(wake.wait())
    try:
        done, pending = await asyncio.wait(
            {stop_task, wake_task},
            timeout=max(0.0, delay),
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if wake_task in done and wake.is_set():
            wake.clear()
    finally:
        for task in (stop_task, wake_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(stop_task, wake_task, return_exceptions=True)


async def _gap_clock_poll_target(self: Any, target: WatchTarget, stop: asyncio.Event) -> None:
    """Existing fail-closed poll worker with the fixed lease bound to real gap time."""

    state = live_poll._poll_state(self)
    key = live_poll._poll_target_key(target)
    runtime = lease._runtime(self).setdefault(key, {})
    cursor_slot = 0
    initialized = False
    failures = 0
    poll_only_total = 0
    suppressed_total = 0
    last_success_monotonic: float | None = None
    last_success_at: str | None = None
    cursor_ws_generation = lease._current_ws_generation(self, target)

    while not stop.is_set():
        started = time.monotonic()
        try:
            if not initialized:
                baseline, provider, latency = await lease.watermark._slot_poll_page(
                    self, target, limit=1
                )
                cursor_slot = lease.watermark._row_slot(baseline[0]) if baseline else 0
                initialized = True
                failures = 0
                last_success_monotonic = time.monotonic()
                last_success_at = direct_solana_module.utcnow().isoformat()
                cursor_ws_generation = lease._current_ws_generation(self, target)
                runtime.update(
                    last_success_monotonic=last_success_monotonic,
                    degraded_started_at=None,
                    cursor_ws_generation=cursor_ws_generation,
                )
                state[key] = _state_row(
                    connected=True,
                    cursor_slot=cursor_slot,
                    provider=provider,
                    latency=latency,
                    last_success_at=last_success_at,
                    poll_only_total=poll_only_total,
                    suppressed_total=suppressed_total,
                    failures=0,
                    ws_generation=cursor_ws_generation,
                    degraded=False,
                    lease_age_seconds=0.0,
                    lease_clock_source="baseline",
                )
                await target_quorum._quorum_set_target_state(
                    self, live_poll._POLL_ENDPOINT, target, connected=True
                )
            else:
                gap = _active_gap_clock(
                    self, target, cursor_generation=cursor_ws_generation
                )
                if isinstance(gap, dict):
                    gap["recovery_attempts"] = int(gap.get("recovery_attempts", 0) or 0) + 1

                new_rows, complete, provider, latency = await lease.watermark._slot_fetch_delta(
                    self, target, cursor_slot
                )
                if complete:
                    newest_slot = max(
                        (lease.watermark._row_slot(row) for row in new_rows),
                        default=cursor_slot,
                    )
                    if new_rows:
                        if live_poll._ws_target_covered(self, target):
                            suppressed_total += len(new_rows)
                        else:
                            inserted = await live_poll._record_poll_rows(self, target, new_rows)
                            poll_only_total += inserted
                    cursor_slot = max(cursor_slot, newest_slot)
                    failures = 0
                    last_success_monotonic = time.monotonic()
                    last_success_at = direct_solana_module.utcnow().isoformat()
                    current_generation = lease._current_ws_generation(self, target)
                    _mark_gap_resolved(self, target, current_generation)
                    cursor_ws_generation = current_generation
                    runtime.update(
                        last_success_monotonic=last_success_monotonic,
                        degraded_started_at=None,
                        cursor_ws_generation=cursor_ws_generation,
                    )
                    state[key] = _state_row(
                        connected=True,
                        cursor_slot=cursor_slot,
                        provider=provider,
                        latency=latency,
                        last_success_at=last_success_at,
                        poll_only_total=poll_only_total,
                        suppressed_total=suppressed_total,
                        failures=0,
                        ws_generation=cursor_ws_generation,
                        degraded=False,
                        lease_age_seconds=0.0,
                        lease_clock_source="real_websocket_gap_onset" if gap else "last_successful_poll",
                    )
                    await target_quorum._quorum_set_target_state(
                        self, live_poll._POLL_ENDPOINT, target, connected=True
                    )
                else:
                    current_generation = lease._current_ws_generation(self, target)
                    websocket_continuous = (
                        live_poll._ws_target_covered(self, target)
                        and current_generation == cursor_ws_generation
                    )
                    rearmed = None
                    if websocket_continuous:
                        rearmed = await lease._try_rearm_with_stable_websocket(
                            self, target, cursor_slot, current_generation
                        )

                    current_generation = lease._current_ws_generation(self, target)
                    gap_recorded = current_generation != cursor_ws_generation
                    if rearmed is None and gap_recorded:
                        # A real gap must first consume its actual fixed recovery
                        # window. The continuity-durability proxy normally converts
                        # this condition into an exception before reaching here;
                        # this branch remains conservative for compatibility.
                        now_mono = time.monotonic()
                        age, _attempt_age, clock_source = _lease_ages(
                            self,
                            target,
                            cursor_generation=cursor_ws_generation,
                            last_success_monotonic=last_success_monotonic,
                            attempt_started_monotonic=started,
                            now_monotonic=now_mono,
                        )
                        if age >= lease.POLL_RECOVERABILITY_LEASE_SECONDS:
                            degraded_started_at = runtime.get("degraded_started_at")
                            if not degraded_started_at:
                                degraded_started_at = direct_solana_module.utcnow().isoformat()
                                runtime["degraded_started_at"] = degraded_started_at
                            lease._latch_irrecoverable_generation_once(
                                self,
                                target,
                                current_generation,
                                degraded_started_at,
                            )
                            if live_poll._ws_target_covered(self, target):
                                rearmed = await lease._try_rearm_with_stable_websocket(
                                    self, target, cursor_slot, current_generation
                                )
                        else:
                            failures += 1
                            state[key] = _state_row(
                                connected=True,
                                cursor_slot=cursor_slot,
                                provider=provider,
                                latency=latency,
                                last_success_at=last_success_at,
                                poll_only_total=poll_only_total,
                                suppressed_total=suppressed_total,
                                failures=failures,
                                ws_generation=cursor_ws_generation,
                                degraded=True,
                                lease_age_seconds=age,
                                cursor_overflow=True,
                                last_error_type="RecoverableLivePollDeltaIncomplete",
                                lease_clock_source=clock_source,
                            )
                            await target_quorum._quorum_set_target_state(
                                self,
                                live_poll._POLL_ENDPOINT,
                                target,
                                connected=True,
                                error_type="RecoverableLivePollDeltaIncomplete",
                                error_message="bounded recovery remains inside fixed real-gap lease",
                            )
                            elapsed = max(0.0, time.monotonic() - started)
                            await _wait_for_poll_or_gap(
                                self,
                                target,
                                stop,
                                max(0.05, live_poll.POLL_INTERVAL_SECONDS - elapsed),
                            )
                            continue

                    if rearmed is not None:
                        cursor_slot, provider, latency = rearmed
                        failures = 0
                        last_success_monotonic = time.monotonic()
                        last_success_at = direct_solana_module.utcnow().isoformat()
                        cursor_ws_generation = current_generation
                        runtime.update(
                            last_success_monotonic=last_success_monotonic,
                            degraded_started_at=None,
                            cursor_ws_generation=cursor_ws_generation,
                        )
                        state[key] = _state_row(
                            connected=True,
                            cursor_slot=cursor_slot,
                            provider=provider,
                            latency=latency,
                            last_success_at=last_success_at,
                            poll_only_total=poll_only_total,
                            suppressed_total=suppressed_total,
                            failures=0,
                            ws_generation=cursor_ws_generation,
                            degraded=False,
                            lease_age_seconds=0.0,
                            overflow_rearmed=True,
                            recorded_gap_standby_rearmed=gap_recorded,
                            lease_clock_source="post_irrecoverable_gap_standby" if gap_recorded else "continuous_websocket_standby",
                        )
                        await target_quorum._quorum_set_target_state(
                            self, live_poll._POLL_ENDPOINT, target, connected=True
                        )
                    else:
                        failures += 1
                        age, _attempt_age, clock_source = _lease_ages(
                            self,
                            target,
                            cursor_generation=cursor_ws_generation,
                            last_success_monotonic=last_success_monotonic,
                            attempt_started_monotonic=started,
                            now_monotonic=time.monotonic(),
                        )
                        state[key] = _state_row(
                            connected=False,
                            cursor_slot=cursor_slot,
                            provider=provider,
                            latency=latency,
                            last_success_at=last_success_at,
                            poll_only_total=poll_only_total,
                            suppressed_total=suppressed_total,
                            failures=failures,
                            ws_generation=cursor_ws_generation,
                            degraded=False,
                            lease_age_seconds=age,
                            cursor_overflow=True,
                            last_error_type="LivePollCursorOverflow",
                            lease_clock_source=clock_source,
                        )
                        await target_quorum._quorum_set_target_state(
                            self,
                            live_poll._POLL_ENDPOINT,
                            target,
                            connected=False,
                            error_type="LivePollCursorOverflow",
                            error_message=(
                                "confirmed-slot live polling exceeded bounded "
                                f"{live_poll.POLL_CURSOR_MAX_PAGES}x{live_poll.POLL_LIMIT} delta window"
                            ),
                        )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failures += 1
            now_mono = time.monotonic()
            age, attempt_started_age, clock_source = _lease_ages(
                self,
                target,
                cursor_generation=cursor_ws_generation,
                last_success_monotonic=last_success_monotonic,
                attempt_started_monotonic=started,
                now_monotonic=now_mono,
            )
            degraded_started_at = runtime.get("degraded_started_at")
            if not degraded_started_at:
                degraded_started_at = direct_solana_module.utcnow().isoformat()
                runtime["degraded_started_at"] = degraded_started_at

            current_generation = lease._current_ws_generation(self, target)
            websocket_covered = live_poll._ws_target_covered(self, target)
            gap_recorded = current_generation != cursor_ws_generation or not websocket_covered

            # A recovery call that starts inside the fixed gap lease may finish
            # after the wall-clock deadline and still succeed (handled above). If
            # it finishes unsuccessfully after the deadline, however, that attempt
            # has exhausted the lease; a new post-deadline attempt cannot erase the
            # missing prospective interval.
            within_lease = initialized and age <= lease.POLL_RECOVERABILITY_LEASE_SECONDS
            if within_lease:
                state[key] = _state_row(
                    connected=True,
                    cursor_slot=cursor_slot,
                    provider=None,
                    latency=None,
                    last_success_at=last_success_at,
                    poll_only_total=poll_only_total,
                    suppressed_total=suppressed_total,
                    failures=failures,
                    ws_generation=cursor_ws_generation,
                    degraded=True,
                    lease_age_seconds=age,
                    inflight_attempt_grace=False,
                    attempt_started_lease_age_seconds=attempt_started_age,
                    last_error_type=type(exc).__name__,
                    lease_clock_source=clock_source,
                )
                await target_quorum._quorum_set_target_state(
                    self,
                    live_poll._POLL_ENDPOINT,
                    target,
                    connected=True,
                    error_type=type(exc).__name__,
                    error_message="transient live-poll recovery failure inside fixed lease",
                )
            else:
                if gap_recorded:
                    lease._latch_irrecoverable_generation_once(
                        self,
                        target,
                        current_generation,
                        degraded_started_at,
                    )
                rearmed = None
                if websocket_covered and current_generation != cursor_ws_generation:
                    try:
                        rearmed = await lease._try_rearm_with_stable_websocket(
                            self, target, cursor_slot, current_generation
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        rearmed = None
                if rearmed is not None:
                    cursor_slot, provider, latency = rearmed
                    failures = 0
                    last_success_monotonic = time.monotonic()
                    last_success_at = direct_solana_module.utcnow().isoformat()
                    cursor_ws_generation = current_generation
                    runtime.update(
                        last_success_monotonic=last_success_monotonic,
                        degraded_started_at=None,
                        cursor_ws_generation=cursor_ws_generation,
                    )
                    state[key] = _state_row(
                        connected=True,
                        cursor_slot=cursor_slot,
                        provider=provider,
                        latency=latency,
                        last_success_at=last_success_at,
                        poll_only_total=poll_only_total,
                        suppressed_total=suppressed_total,
                        failures=0,
                        ws_generation=cursor_ws_generation,
                        degraded=False,
                        lease_age_seconds=0.0,
                        overflow_rearmed=True,
                        recorded_gap_standby_rearmed=True,
                        lease_clock_source="post_irrecoverable_gap_standby",
                    )
                    await target_quorum._quorum_set_target_state(
                        self, live_poll._POLL_ENDPOINT, target, connected=True
                    )
                else:
                    state[key] = _state_row(
                        connected=False,
                        cursor_slot=cursor_slot,
                        provider=None,
                        latency=None,
                        last_success_at=last_success_at,
                        poll_only_total=poll_only_total,
                        suppressed_total=suppressed_total,
                        failures=failures,
                        ws_generation=cursor_ws_generation,
                        degraded=False,
                        lease_age_seconds=age,
                        attempt_started_lease_age_seconds=attempt_started_age,
                        last_error_type="LivePollFreshnessLeaseExpired",
                        lease_clock_source=clock_source,
                    )
                    await target_quorum._quorum_set_target_state(
                        self,
                        live_poll._POLL_ENDPOINT,
                        target,
                        connected=False,
                        error_type="LivePollFreshnessLeaseExpired",
                        error_message=(
                            "live-poll recoverability lease exceeded "
                            f"{lease.POLL_RECOVERABILITY_LEASE_SECONDS:.1f}s from actual real-WebSocket gap onset"
                        ),
                    )

        elapsed = max(0.0, time.monotonic() - started)
        await _wait_for_poll_or_gap(
            self,
            target,
            stop,
            max(0.05, live_poll.POLL_INTERVAL_SECONDS - elapsed),
        )


def _status_with_gap_clock(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        poll = payload.get("live_poll_redundancy")
        if isinstance(poll, dict):
            poll["recoverability_lease_clock"] = "actual-real-websocket-zero-coverage-onset"
            poll["real_gap_wakes_poll_immediately"] = True
            poll["post_deadline_failed_attempt_can_retry"] = False
            poll["successful_inflight_attempt_started_inside_lease_can_complete"] = True
            poll["recoverability_lease_seconds"] = lease.POLL_RECOVERABILITY_LEASE_SECONDS
            poll["hard_delta_bound_unchanged"] = True
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "live_poll_recoverability_clock_starts_at_real_ws_gap": True,
                    "live_poll_gap_wakes_bounded_recovery": True,
                    "live_poll_failed_inflight_attempt_after_deadline_exhausts_lease": True,
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


# Preserve the long-standing module/function contracts used by regressions and by
# later repair layers while replacing their implementations in place.
_gap_clock_poll_target.__name__ = "_leased_poll_target"
_gap_clock_set_target_state.__name__ = "_tracked_quorum_set_target_state"


def install_continuity_gap_clock_repair() -> None:
    lease._tracked_quorum_set_target_state = _gap_clock_set_target_state  # type: ignore[assignment]
    target_quorum._quorum_set_target_state = lease._tracked_quorum_set_target_state  # type: ignore[assignment]
    fanout._set_target_state = lease._tracked_quorum_set_target_state  # type: ignore[assignment]

    lease._leased_poll_target = _gap_clock_poll_target  # type: ignore[assignment]
    live_poll._poll_target = lease._leased_poll_target  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_continuity_gap_clock_repair", False)):
        DirectSolanaIngestionPlane.status = _status_with_gap_clock(current_status)  # type: ignore[method-assign]


__all__ = [
    "install_continuity_gap_clock_repair",
    "_active_gap_clock",
    "_gap_clock_set_target_state",
    "_lease_ages",
]
