from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from . import direct_solana as direct_solana_module
from . import live_poll_redundancy as live_poll
from . import poll_watermark_repair as watermark
from . import target_quorum
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


def _rearm_stats(self: Any) -> dict[str, int]:
    stats = getattr(self, "_roi_poll_overflow_rearms", None)
    if not isinstance(stats, dict):
        stats = {}
        setattr(self, "_roi_poll_overflow_rearms", stats)
    return stats


async def _try_rearm_under_websocket(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[int, str, float] | None:
    """Re-baseline a failed standby only when the same target is live on WebSocket.

    The bounded polling delta remains strict. An overflow is never converted into
    fallback evidence and never repairs a prospective target gap. When a real
    WebSocket is currently authoritative for the same target, however, leaving the
    polling watermark frozen forever only destroys future redundancy. In that case
    read the current confirmed head and re-arm the polling lane prospectively from
    that point forward. If WebSocket coverage is absent, return ``None`` and keep
    the target failed closed.
    """

    if not live_poll._ws_target_covered(self, target):
        return None

    head, provider, latency = await watermark._slot_poll_page(
        self,
        target,
        min_context_slot=cursor_slot if cursor_slot > 0 else None,
        limit=1,
    )
    if not head:
        return None
    head_slot = watermark._row_slot(head[0])
    if head_slot <= cursor_slot:
        return None

    key = live_poll._poll_target_key(target)
    stats = _rearm_stats(self)
    stats[key] = int(stats.get(key, 0) or 0) + 1
    return head_slot, provider, latency


async def _standby_poll_target(self: Any, target: WatchTarget, stop: asyncio.Event) -> None:
    state = live_poll._poll_state(self)
    key = live_poll._poll_target_key(target)
    cursor_slot = 0
    initialized = False
    failures = 0
    poll_only_total = 0
    suppressed_total = 0

    while not stop.is_set():
        started = time.monotonic()
        try:
            if not initialized:
                baseline, provider, latency = await watermark._slot_poll_page(self, target, limit=1)
                cursor_slot = watermark._row_slot(baseline[0]) if baseline else 0
                initialized = True
                failures = 0
                state[key] = {
                    "connected": True,
                    "baseline_established": True,
                    "cursor_slot": cursor_slot,
                    "cursor_model": "confirmed-slot-watermark",
                    "last_provider": provider,
                    "last_latency_ms": latency,
                    "last_success_at": direct_solana_module.utcnow().isoformat(),
                    "poll_only_receipts_total": poll_only_total,
                    "suppressed_while_websocket_covered_total": suppressed_total,
                    "cursor_overflow": False,
                    "overflow_rearmed_under_websocket": False,
                    "failures": failures,
                }
                await target_quorum._quorum_set_target_state(
                    self, live_poll._POLL_ENDPOINT, target, connected=True
                )
            else:
                new_rows, complete, provider, latency = await watermark._slot_fetch_delta(
                    self, target, cursor_slot
                )
                if not complete:
                    rearmed = await _try_rearm_under_websocket(self, target, cursor_slot)
                    if rearmed is not None:
                        cursor_slot, provider, latency = rearmed
                        failures = 0
                        state[key] = {
                            "connected": True,
                            "baseline_established": True,
                            "cursor_slot": cursor_slot,
                            "cursor_model": "confirmed-slot-watermark",
                            "last_provider": provider,
                            "last_latency_ms": latency,
                            "last_success_at": direct_solana_module.utcnow().isoformat(),
                            "poll_only_receipts_total": poll_only_total,
                            "suppressed_while_websocket_covered_total": suppressed_total,
                            "cursor_overflow": False,
                            "overflow_rearmed_under_websocket": True,
                            "failures": failures,
                        }
                        await target_quorum._quorum_set_target_state(
                            self, live_poll._POLL_ENDPOINT, target, connected=True
                        )
                    else:
                        failures += 1
                        state[key] = {
                            "connected": False,
                            "baseline_established": True,
                            "cursor_slot": cursor_slot,
                            "cursor_model": "confirmed-slot-watermark",
                            "last_provider": provider,
                            "last_latency_ms": latency,
                            "last_success_at": direct_solana_module.utcnow().isoformat(),
                            "poll_only_receipts_total": poll_only_total,
                            "suppressed_while_websocket_covered_total": suppressed_total,
                            "cursor_overflow": True,
                            "overflow_rearmed_under_websocket": False,
                            "failures": failures,
                        }
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
                else:
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
                    state[key] = {
                        "connected": True,
                        "baseline_established": True,
                        "cursor_slot": cursor_slot,
                        "cursor_model": "confirmed-slot-watermark",
                        "last_provider": provider,
                        "last_latency_ms": latency,
                        "last_success_at": direct_solana_module.utcnow().isoformat(),
                        "poll_only_receipts_total": poll_only_total,
                        "suppressed_while_websocket_covered_total": suppressed_total,
                        "cursor_overflow": False,
                        "overflow_rearmed_under_websocket": False,
                        "failures": failures,
                    }
                    await target_quorum._quorum_set_target_state(
                        self, live_poll._POLL_ENDPOINT, target, connected=True
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failures += 1
            previous = state.get(key)
            previous_success = previous.get("last_success_at") if isinstance(previous, dict) else None
            state[key] = {
                "connected": False,
                "baseline_established": initialized,
                "cursor_slot": cursor_slot,
                "cursor_model": "confirmed-slot-watermark",
                "last_provider": None,
                "last_latency_ms": None,
                "last_success_at": previous_success,
                "poll_only_receipts_total": poll_only_total,
                "suppressed_while_websocket_covered_total": suppressed_total,
                "cursor_overflow": False,
                "overflow_rearmed_under_websocket": False,
                "failures": failures,
                "last_error_type": type(exc).__name__,
            }
            await target_quorum._quorum_set_target_state(
                self,
                live_poll._POLL_ENDPOINT,
                target,
                connected=False,
                error_type=type(exc).__name__,
            )

        elapsed = max(0.0, time.monotonic() - started)
        await asyncio.sleep(max(0.05, live_poll.POLL_INTERVAL_SECONDS - elapsed))


def _status_with_standby_rearm(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        stats = dict(_rearm_stats(self))
        poll = payload.get("live_poll_redundancy")
        if isinstance(poll, dict):
            poll["overflow_standby_rearm_enabled"] = True
            poll["overflow_rearm_requires_same_target_websocket_coverage"] = True
            poll["overflow_rearm_restores_prior_prospective_evidence"] = False
            poll["overflow_rearms_total"] = sum(int(value or 0) for value in stats.values())
            poll["overflow_rearms_by_target"] = stats
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "live_poll_overflow_standby_rearm": True,
                    "live_poll_overflow_rearm_requires_websocket_coverage": True,
                    "live_poll_overflow_rearm_can_restore_gap": False,
                    "live_poll_delta_bound_unchanged": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_poll_standby_rearm", True)
    return status


def install_poll_standby_rearm() -> None:
    # live_poll._wrap_run resolves this module global at execution time, so this is
    # entrypoint-independent and does not add another worker or polling fanout.
    live_poll._poll_target = _standby_poll_target  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_poll_standby_rearm", False)):
        DirectSolanaIngestionPlane.status = _status_with_standby_rearm(current_status)  # type: ignore[method-assign]


__all__ = [
    "install_poll_standby_rearm",
    "_try_rearm_under_websocket",
    "_standby_poll_target",
]
