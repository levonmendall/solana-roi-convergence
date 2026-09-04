from __future__ import annotations

from collections.abc import Callable
from typing import Any

from . import live_poll_redundancy as live_poll
from . import poll_exception_rearm as exception_rearm
from . import poll_recoverability_lease as lease
from . import poll_watermark_repair as watermark
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


class RecoverableLivePollDeltaIncomplete(RuntimeError):
    """A bounded poll delta is retryable inside the existing fixed lease."""


async def _hedged_gap_poll_page(
    self: Any,
    target: WatchTarget,
    *,
    before: str | None = None,
    min_context_slot: int | None = None,
    limit: int | None = None,
) -> tuple[list[dict[str, Any]], str, float]:
    """Hedge only recovery reads after a tracked real-WebSocket gap."""

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


async def _hedged_gap_fetch_delta(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    """Repeat the unchanged bounded watermark delta with read-only hedging."""

    pages: list[list[dict[str, Any]]] = []
    before: str | None = None
    provider: str | None = None
    latency: float | None = None
    complete = False
    context_floor = max(0, int(cursor_slot))

    for _page_index in range(live_poll.POLL_CURSOR_MAX_PAGES):
        page, provider, latency = await _hedged_gap_poll_page(
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

    if not complete:
        return [], False, provider, latency

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


def _requires_hedged_real_gap_fetch(base_fetch: Callable[..., Any]) -> bool:
    """Recognize wrappers that intentionally change only healthy standby upkeep.

    The durability proxy historically identified the canonical exception-rearm
    function by object identity. The high-volume checkpoint architecture wraps the
    same base function so healthy WebSocket periods can advance the standby cursor
    from confirmed live receipts. A real zero-WebSocket generation must still bypass
    that healthy-path wrapper and retain the existing dedicated hedged 3x1000 read.
    """

    return bool(
        base_fetch is exception_rearm._exception_rearm_fetch_delta
        or getattr(base_fetch, "_roi_high_volume_standby_checkpoint", False)
    )


class _LeaseWatermarkProxy:
    """Change only the recoverability worker's delta classification.

    The underlying watermark module remains the canonical object. Baseline reads,
    monkeypatched regression helpers, and all other callers therefore continue to
    see exactly the established functions. Only a production real-gap recovery
    attempt uses the hedged bounded reader.
    """

    def __init__(self, base: Any):
        self._base = base

    def __getattr__(self, name: str) -> Any:
        return getattr(self._base, name)

    async def _slot_fetch_delta(
        self,
        plane: Any,
        target: WatchTarget,
        cursor_slot: int,
    ) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
        key = live_poll._poll_target_key(target)
        runtime = lease._runtime(plane).get(key, {})
        current_generation = lease._current_ws_generation(plane, target)
        cursor_generation = int(runtime.get("cursor_ws_generation", current_generation) or 0)
        gap_before_attempt = current_generation != cursor_generation

        base_fetch = self._base._slot_fetch_delta
        # Once a real zero-coverage generation is known, always retain the dedicated
        # hedged bounded recovery path. Healthy-path wrappers may change standby
        # maintenance, but they cannot intercept or weaken real-gap recovery.
        if gap_before_attempt and _requires_hedged_real_gap_fetch(base_fetch):
            rows, complete, provider, latency = await _hedged_gap_fetch_delta(
                plane, target, cursor_slot
            )
        else:
            rows, complete, provider, latency = await base_fetch(
                plane, target, cursor_slot
            )

        current_generation = lease._current_ws_generation(plane, target)
        gap_after_attempt = current_generation != cursor_generation
        if not complete and gap_after_attempt:
            # The interval is not yet proven lost. Raising routes this bounded
            # incomplete result through the existing exception branch, which
            # honors the unchanged 12-second lease and attempt-start grace. Only
            # expiry after the tracked real gap latches the exact release failed.
            raise RecoverableLivePollDeltaIncomplete(
                "bounded confirmed-slot recovery incomplete after real WebSocket gap"
            )
        return rows, complete, provider, latency


def _status_with_continuity_durability(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        poll = payload.get("live_poll_redundancy")
        if isinstance(poll, dict):
            poll["bounded_delta_after_real_ws_gap_uses_recoverability_lease"] = True
            poll["continuous_ws_standby_rearm_preserved"] = True
            poll["real_gap_recovery_reads_hedged"] = True
            poll["poll_watermark_abandoned_before_irrecoverable_gap"] = False
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "live_poll_gap_overflow_immediately_latches_gap": False,
                    "live_poll_gap_overflow_uses_fixed_recoverability_lease": True,
                    "live_poll_continuous_ws_standby_rearm_preserved": True,
                    "live_poll_real_gap_recovery_reads_hedged": True,
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
    "_hedged_gap_fetch_delta",
    "_hedged_gap_poll_page",
    "_requires_hedged_real_gap_fetch",
]
