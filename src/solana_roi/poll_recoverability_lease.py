from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from . import direct_solana as direct_solana_module
from . import live_poll_redundancy as live_poll
from . import poll_standby_rearm as standby
from . import poll_watermark_repair as watermark
from . import target_quorum
from . import target_stream_fanout as fanout
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


POLL_RECOVERABILITY_LEASE_SECONDS = 12.0
IRRECOVERABLE_POLL_GAP_ERROR = (
    "prospective target interval became unrecoverable: live-poll freshness lease expired "
    "or bounded poll delta could not be recovered after a real WebSocket coverage loss"
)

_ORIGINAL_QUORUM_SET_TARGET_STATE = target_quorum._quorum_set_target_state


def _target_key(target: WatchTarget) -> str:
    return live_poll._poll_target_key(target)


def _ws_gap_generations(self: Any) -> dict[str, int]:
    value = getattr(self, "_roi_real_ws_gap_generations", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_real_ws_gap_generations", value)
    return value


def _runtime(self: Any) -> dict[str, dict[str, Any]]:
    value = getattr(self, "_roi_poll_recoverability_runtime", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_poll_recoverability_runtime", value)
    return value


def _current_ws_generation(self: Any, target: WatchTarget) -> int:
    return int(_ws_gap_generations(self).get(_target_key(target), 0) or 0)


def _latch_irrecoverable_gap(self: Any, started_at_iso: str | None = None) -> None:
    try:
        started_at = (
            direct_solana_module.datetime.fromisoformat(started_at_iso)
            if started_at_iso
            else direct_solana_module.utcnow()
        )
    except Exception:
        started_at = direct_solana_module.utcnow()
    self.journal.mark_outage(started_at)
    self.journal.close_outage(complete=False, error=IRRECOVERABLE_POLL_GAP_ERROR)


async def _tracked_quorum_set_target_state(
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
    before_ws = live_poll._ws_target_covered(self, target) if is_real_ws else False
    await _ORIGINAL_QUORUM_SET_TARGET_STATE(
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
    after_ws = live_poll._ws_target_covered(self, target)
    if before_ws and not after_ws:
        key = _target_key(target)
        generations = _ws_gap_generations(self)
        generations[key] = int(generations.get(key, 0) or 0) + 1


def _state_row(
    *,
    connected: bool,
    cursor_slot: int,
    provider: str | None,
    latency: float | None,
    last_success_at: str | None,
    poll_only_total: int,
    suppressed_total: int,
    failures: int,
    ws_generation: int,
    degraded: bool,
    lease_age_seconds: float,
    cursor_overflow: bool = False,
    overflow_rearmed: bool = False,
    last_error_type: str | None = None,
) -> dict[str, Any]:
    return {
        "connected": connected,
        "baseline_established": True,
        "cursor_slot": cursor_slot,
        "cursor_model": "confirmed-slot-watermark",
        "last_provider": provider,
        "last_latency_ms": latency,
        "last_success_at": last_success_at,
        "poll_only_receipts_total": poll_only_total,
        "suppressed_while_websocket_covered_total": suppressed_total,
        "cursor_overflow": cursor_overflow,
        "overflow_rearmed_under_websocket": overflow_rearmed,
        "failures": failures,
        "degraded_recoverable": degraded,
        "recoverability_lease_seconds": POLL_RECOVERABILITY_LEASE_SECONDS,
        "lease_age_seconds": round(max(0.0, lease_age_seconds), 3),
        "ws_gap_generation_at_cursor": ws_generation,
        "last_error_type": last_error_type,
    }


async def _leased_poll_target(self: Any, target: WatchTarget, stop: asyncio.Event) -> None:
    state = live_poll._poll_state(self)
    key = _target_key(target)
    runtime = _runtime(self).setdefault(key, {})
    cursor_slot = 0
    initialized = False
    failures = 0
    poll_only_total = 0
    suppressed_total = 0
    last_success_monotonic: float | None = None
    last_success_at: str | None = None
    cursor_ws_generation = _current_ws_generation(self, target)

    while not stop.is_set():
        started = time.monotonic()
        try:
            if not initialized:
                baseline, provider, latency = await watermark._slot_poll_page(self, target, limit=1)
                cursor_slot = watermark._row_slot(baseline[0]) if baseline else 0
                initialized = True
                failures = 0
                last_success_monotonic = time.monotonic()
                last_success_at = direct_solana_module.utcnow().isoformat()
                cursor_ws_generation = _current_ws_generation(self, target)
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
                )
                await target_quorum._quorum_set_target_state(
                    self, live_poll._POLL_ENDPOINT, target, connected=True
                )
            else:
                new_rows, complete, provider, latency = await watermark._slot_fetch_delta(
                    self, target, cursor_slot
                )
                if complete:
                    newest_slot = max(
                        (watermark._row_slot(row) for row in new_rows),
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
                    cursor_ws_generation = _current_ws_generation(self, target)
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
                    )
                    await target_quorum._quorum_set_target_state(
                        self, live_poll._POLL_ENDPOINT, target, connected=True
                    )
                else:
                    current_generation = _current_ws_generation(self, target)
                    websocket_continuous = (
                        live_poll._ws_target_covered(self, target)
                        and current_generation == cursor_ws_generation
                    )
                    rearmed = None
                    if websocket_continuous:
                        rearmed = await standby._try_rearm_under_websocket(
                            self, target, cursor_slot
                        )
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
                        )
                        await target_quorum._quorum_set_target_state(
                            self, live_poll._POLL_ENDPOINT, target, connected=True
                        )
                    else:
                        degraded_started_at = runtime.get("degraded_started_at")
                        if not degraded_started_at:
                            degraded_started_at = direct_solana_module.utcnow().isoformat()
                            runtime["degraded_started_at"] = degraded_started_at
                        # A bounded delta that cannot be recovered after any real
                        # WebSocket zero-coverage transition proves a prospective
                        # observation gap even if WebSocket coverage has since returned.
                        if current_generation != cursor_ws_generation:
                            _latch_irrecoverable_gap(self, degraded_started_at)
                        failures += 1
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
                            lease_age_seconds=(
                                time.monotonic() - last_success_monotonic
                                if last_success_monotonic is not None
                                else POLL_RECOVERABILITY_LEASE_SECONDS
                            ),
                            cursor_overflow=True,
                            last_error_type="LivePollCursorOverflow",
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
            age = (
                now_mono - last_success_monotonic
                if last_success_monotonic is not None
                else POLL_RECOVERABILITY_LEASE_SECONDS + 1.0
            )
            degraded_started_at = runtime.get("degraded_started_at")
            if not degraded_started_at:
                degraded_started_at = direct_solana_module.utcnow().isoformat()
                runtime["degraded_started_at"] = degraded_started_at
            within_lease = initialized and age <= POLL_RECOVERABILITY_LEASE_SECONDS
            current_generation = _current_ws_generation(self, target)
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
                    last_error_type=type(exc).__name__,
                )
                await target_quorum._quorum_set_target_state(
                    self,
                    live_poll._POLL_ENDPOINT,
                    target,
                    connected=True,
                    error_type=type(exc).__name__,
                    error_message="transient live-poll RPC failure inside recoverability lease",
                )
            else:
                # If real WebSocket coverage dropped at any point since the last
                # proven poll watermark, expiring the recoverability lease makes
                # that interval irrecoverable and invalidates the exact release.
                if current_generation != cursor_ws_generation or not live_poll._ws_target_covered(self, target):
                    _latch_irrecoverable_gap(self, degraded_started_at)
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
                    last_error_type="LivePollFreshnessLeaseExpired",
                )
                await target_quorum._quorum_set_target_state(
                    self,
                    live_poll._POLL_ENDPOINT,
                    target,
                    connected=False,
                    error_type="LivePollFreshnessLeaseExpired",
                    error_message=f"live-poll recoverability lease exceeded {POLL_RECOVERABILITY_LEASE_SECONDS:.1f}s",
                )

        elapsed = max(0.0, time.monotonic() - started)
        await asyncio.sleep(max(0.05, live_poll.POLL_INTERVAL_SECONDS - elapsed))


def _status_with_recoverability(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        poll = payload.get("live_poll_redundancy")
        if isinstance(poll, dict):
            targets = poll.get("targets")
            degraded_count = 0
            if isinstance(targets, dict):
                degraded_count = sum(
                    1
                    for row in targets.values()
                    if isinstance(row, dict) and bool(row.get("degraded_recoverable"))
                )
            poll["recoverability_lease_enabled"] = True
            poll["recoverability_lease_seconds"] = POLL_RECOVERABILITY_LEASE_SECONDS
            poll["degraded_recoverable_target_count"] = degraded_count
            poll["lease_can_restore_irrecoverable_gap"] = False
            poll["real_websocket_gap_generation_tracked_per_target"] = True
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "live_poll_transient_rpc_failure_immediately_withdraws_quorum": False,
                    "live_poll_recoverability_lease_seconds": POLL_RECOVERABILITY_LEASE_SECONDS,
                    "live_poll_bounded_recovery_required_before_lease_expiry": True,
                    "live_poll_real_ws_gap_generation_tracked": True,
                    "live_poll_irrecoverable_interval_fails_release_closed": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_poll_recoverability_lease", True)
    return status


def install_poll_recoverability_lease() -> None:
    target_quorum._quorum_set_target_state = _tracked_quorum_set_target_state  # type: ignore[assignment]
    fanout._set_target_state = _tracked_quorum_set_target_state  # type: ignore[assignment]
    live_poll._poll_target = _leased_poll_target  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_poll_recoverability_lease", False)):
        DirectSolanaIngestionPlane.status = _status_with_recoverability(current_status)  # type: ignore[method-assign]


__all__ = [
    "POLL_RECOVERABILITY_LEASE_SECONDS",
    "IRRECOVERABLE_POLL_GAP_ERROR",
    "install_poll_recoverability_lease",
]
