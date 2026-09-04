from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from . import live_poll_redundancy as live_poll
from . import poll_watermark_repair as watermark
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


DeltaResult = tuple[list[dict[str, Any]], bool, str | None, float | None]
DeltaHook = Callable[[Any, WatchTarget, int], Awaitable[DeltaResult | None]]

# Optional lower-layer specialization point. Keeping this hook inside the canonical
# pagination function preserves the function-object identities relied on by the
# existing pagination -> exception-rearm -> recoverability composition. The default
# remains None and therefore changes no behavior unless a later production repair
# explicitly installs a narrowly-scoped delegate.
_HIGH_VOLUME_DELTA_HOOK: DeltaHook | None = None


async def _context_fresh_fetch_delta(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> DeltaResult:
    """Fetch one bounded live delta without allowing stale pagination backends.

    ``before`` is still needed to paginate a single ``getSignaturesForAddress``
    delta. Public RPC hosts can route consecutive requests to different backend
    nodes, however. A backend that is only as fresh as the old cursor slot may not
    yet know the signature used as the previous page's ``before`` boundary. If it
    answers anyway, pages can overlap/repeat until the hard page bound is exhausted
    and create a false overflow.

    Keep the provider-agnostic confirmed-slot watermark, but after every page raise
    ``minContextSlot`` to the newest slot observed in this delta. Every backend used
    by a later page must therefore be caught up far enough to know the preceding
    page's confirmed pagination boundary. If no backend can satisfy that context,
    the RPC call fails transiently and the recoverability lease retries from the
    unchanged watermark; it is never mislabeled as a bounded-delta overflow.
    """

    if _HIGH_VOLUME_DELTA_HOOK is not None:
        hooked = await _HIGH_VOLUME_DELTA_HOOK(self, target, cursor_slot)
        if hooked is not None:
            return hooked

    pages: list[list[dict[str, Any]]] = []
    before: str | None = None
    provider: str | None = None
    latency: float | None = None
    complete = False
    context_floor = max(0, int(cursor_slot))

    for _page_index in range(live_poll.POLL_CURSOR_MAX_PAGES):
        page, provider, latency = await watermark._slot_poll_page(
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


def _status_with_pagination_context(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        poll = payload.get("live_poll_redundancy")
        if isinstance(poll, dict):
            poll["pagination_context_floor"] = "max(cursor_slot,newest_slot_seen_in_delta)"
            poll["pagination_context_advances_after_each_page"] = True
            poll["stale_backend_before_boundary_allowed"] = False
            poll["bounded_specialization_hook_available"] = True
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "live_poll_multi_page_context_advances_to_page_head": True,
                    "live_poll_before_boundary_requires_fresh_backend_context": True,
                    "live_poll_true_delta_bound_unchanged": True,
                    "live_poll_canonical_pagination_identity_preserved": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_poll_pagination_context", True)
    return status


def install_poll_pagination_context() -> None:
    # The final recoverability worker resolves watermark._slot_fetch_delta at run
    # time, so this changes only page-fetch semantics and leaves that worker/lease
    # intact. Keep the legacy live-poll alias aligned for compatibility callers.
    watermark._slot_fetch_delta = _context_fresh_fetch_delta  # type: ignore[assignment]
    live_poll._fetch_delta = _context_fresh_fetch_delta  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_poll_pagination_context", False)):
        DirectSolanaIngestionPlane.status = _status_with_pagination_context(current_status)  # type: ignore[method-assign]


__all__ = [
    "DeltaHook",
    "DeltaResult",
    "_HIGH_VOLUME_DELTA_HOOK",
    "install_poll_pagination_context",
    "_context_fresh_fetch_delta",
]
