from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from . import poll_watermark_repair as watermark
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


class RecoverableLivePollDeltaIncomplete(RuntimeError):
    """A bounded poll delta was not proven complete after a real WS gap."""


async def _lease_slot_poll_page(
    self: Any,
    target: WatchTarget,
    *,
    before: str | None = None,
    min_context_slot: int | None = None,
    limit: int | None = None,
) -> tuple[list[dict[str, Any]], str, float]:
    """Hedge only the time-critical poll reads used by the recoverability worker."""

    page_limit = live_poll.POLL_LIMIT if limit is None else int(limit)
    config: dict[str, Any] = {
        "commitment": "confirmed",
        "limit": max(1, min(1000, page_limit)),
    }
    if before:
        config["before"] = before
    if min_context_slot is not None and int(min_context_slot) > 0:
        config["minContextSlot"] = int(min_context_slot)
    result, provider, latency = await live_poll._poll_rpc(self).call_with_meta(
        "getSignaturesForAddress",
        [target.address, config],
        hedge=True,
    )
    rows = [row for row in result if isinstance(row, dict)] if isinstance(result, list) else []
    return rows, provider, latency


def _websocket_continuous(self: Any, target: WatchTarget) -> bool:
    key = live_poll._poll_target_key(target)
    runtime = lease._runtime(self).get(key, {})
    current_generation = lease._current_ws_generation(self, target)
    cursor_generation = int(runtime.get("cursor_ws_generation", current_generation) or 0)
    return bool(
        live_poll._ws_target_covered(self, target)
        and current_generation == cursor_generation
    )


async def _lease_slot_fetch_delta(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    """Fetch the final bounded delta with hedging and correct lease classification.

    The hard page/row bound and confirmed-slot watermark are unchanged. A bounded
    overflow while the same target has uninterrupted real-WebSocket coverage keeps
    the established standby re-arm path. If a real WebSocket zero-coverage
    generation occurred, however, an incomplete bounded read is still recoverable
    until the existing fixed lease expires, so raise into the lease exception path
    instead of immediately latching the release failed.
    """

    pages: list[list[dict[str, Any]]] = []
    before: str | None = None
    provider: str | None = None
    latency: float | None = None
    complete = False
    context_floor = max(0, int(cursor_slot))

    try:
        for _page_index in range(live_poll.POLL_CURSOR_MAX_PAGES):
            page, provider, latency = await _lease_slot_poll_page(
                self,
                target,
                before=before,
                min_context_slot=context_floor if context_floor > 0 else None,
                limit=live_poll.POLL_LIMIT,
            )
            pages.append(page)
            if not page:
                complete = True
                break

            page_slots = [watermark._row_slot(row) for row in page]
            newest_page_slot = max(page_slots, default=0)
            context_floor = max(context_floor, newest_page_slot)

            if cursor_slot > 0 and any(slot <= cursor_slot for slot in page_slots):
                complete = True
                break
            if len(page) < live_poll.POLL_LIMIT:
                complete = True
                break

            before = str(page[-1].get("signature") or "")
            if not before:
                complete = True
                break
    except asyncio.CancelledError:
        raise
    except Exception:
        # Preserve the existing exception-rearm contract when the real WebSocket
        # union never lost this target. A real WS gap instead reaches the normal
        # lease exception path, which already honors attempt-start grace.
        if _websocket_continuous(self, target):
            return [], False, None, None
        raise

    if not complete:
        if _websocket_continuous(self, target):
            return [], False, provider, latency
        raise RecoverableLivePollDeltaIncomplete(
            "bounded confirmed-slot delta incomplete after real WebSocket gap; retry from unchanged watermark"
        )

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for page in reversed(pages):
        for row in reversed(page):
            signature = str(row.get("signature") or "")
            slot = watermark._row_slot(row)
            if not signature or signature in seen or slot <= cursor_slot:
                continue
            seen.add(signature)
            rows.append(row)
    return rows, True, provider, latency


class _LeaseWatermarkProxy:
    """Give only the recoverability worker repaired poll semantics.

    Other modules keep the established watermark/exception-rearm function objects,
    so their contracts and offline regression helpers remain unchanged.
    """

    def __init__(self, base: Any):
        self._base = base

    def __getattr__(self, name: str) -> Any:
        return getattr(self._base, name)

    _slot_poll_page = staticmethod(_lease_slot_poll_page)
    _slot_fetch_delta = staticmethod(_lease_slot_fetch_delta)


def _status_with_continuity_durability(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        poll = payload.get("live_poll_redundancy")
        if isinstance(poll, dict):
            poll["bounded_delta_after_real_ws_gap_uses_recoverability_lease"] = True
            poll["continuous_ws_standby_rearm_preserved"] = True
            poll["recoverability_worker_poll_reads_hedged"] = True
            poll["poll_watermark_abandoned_before_irrecoverable_gap"] = False
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "live_poll_gap_overflow_immediately_latches_gap": False,
                    "live_poll_gap_overflow_uses_fixed_recoverability_lease": True,
                    "live_poll_continuous_ws_standby_rearm_preserved": True,
                    "live_poll_recovery_reads_hedged": True,
                    "live_poll_hard_delta_bound_unchanged": True,
                    "live_poll_irrecoverable_interval_fails_release_closed": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_continuity_durability_repair", True)
    return status


def install_continuity_durability_repair() -> None:
    current = getattr(lease, "watermark", None)
    if not isinstance(current, _LeaseWatermarkProxy):
        lease.watermark = _LeaseWatermarkProxy(current)  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_continuity_durability_repair", False)):
        DirectSolanaIngestionPlane.status = _status_with_continuity_durability(current_status)  # type: ignore[method-assign]


__all__ = [
    "RecoverableLivePollDeltaIncomplete",
    "install_continuity_durability_repair",
    "_lease_slot_fetch_delta",
    "_lease_slot_poll_page",
]
