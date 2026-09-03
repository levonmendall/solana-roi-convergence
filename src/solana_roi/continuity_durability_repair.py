from __future__ import annotations

from collections.abc import Callable
from typing import Any

from . import live_poll_redundancy as live_poll
from . import poll_watermark_repair as watermark
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


class RecoverableLivePollDeltaIncomplete(RuntimeError):
    """A bounded poll delta was not proven complete on this attempt.

    This is deliberately an exception rather than an immediate terminal overflow:
    the existing recoverability worker already gives exceptions the fixed 12-second
    lease, including attempt-start grace, and only invalidates the release after a
    real WebSocket zero-coverage generation becomes genuinely unrecoverable.
    """


_ORIGINAL_SLOT_FETCH_DELTA = watermark._slot_fetch_delta


async def _hedged_slot_poll_page(
    self: Any,
    target: WatchTarget,
    *,
    before: str | None = None,
    min_context_slot: int | None = None,
    limit: int | None = None,
) -> tuple[list[dict[str, Any]], str, float]:
    """Use the existing read-only RPC hedge for time-critical live polling."""

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


async def _lease_aware_slot_fetch_delta(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    """Keep the hard delta bound while letting the fixed recovery lease do its job.

    The final pagination implementation still decides whether the bounded delta is
    complete. A genuine bounded overflow is *not* proof that the prospective
    interval is already lost: while the prior watermark remains intact, another
    bounded attempt can still recover it. Raising here routes that state through
    the existing lease logic instead of the older immediate-latch branch.

    The preceding exception-rearm repair intentionally represents a transient RPC
    exception under uninterrupted real-WebSocket coverage as ``([], False, None,
    None)``. Preserve that sentinel so its established same-target standby re-arm
    remains available; only a real bounded overflow with provider evidence is
    converted to the lease-routed exception.
    """

    rows, complete, provider, latency = await _ORIGINAL_SLOT_FETCH_DELTA(
        self, target, cursor_slot
    )
    if not complete:
        if provider is None and latency is None:
            return rows, False, provider, latency
        raise RecoverableLivePollDeltaIncomplete(
            "bounded confirmed-slot delta incomplete; retry from unchanged watermark"
        )
    return rows, True, provider, latency


def _status_with_continuity_durability(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        poll = payload.get("live_poll_redundancy")
        if isinstance(poll, dict):
            poll["bounded_delta_incomplete_uses_recoverability_lease"] = True
            poll["continuous_ws_exception_rearm_preserved"] = True
            poll["poll_reads_hedged"] = True
            poll["poll_watermark_abandoned_before_irrecoverable_gap"] = False
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "live_poll_incomplete_delta_immediately_latches_gap": False,
                    "live_poll_incomplete_delta_uses_fixed_recoverability_lease": True,
                    "live_poll_continuous_ws_exception_rearm_preserved": True,
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
    # poll_recoverability_lease resolves watermark._slot_fetch_delta at run time.
    # Install after all pagination/re-arm repairs so the wrapper delegates to the
    # final bounded-delta implementation and changes only failure classification.
    watermark._slot_poll_page = _hedged_slot_poll_page  # type: ignore[assignment]
    watermark._slot_fetch_delta = _lease_aware_slot_fetch_delta  # type: ignore[assignment]
    live_poll._poll_page = _hedged_slot_poll_page  # type: ignore[assignment]
    live_poll._fetch_delta = _lease_aware_slot_fetch_delta  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_continuity_durability_repair", False)):
        DirectSolanaIngestionPlane.status = _status_with_continuity_durability(current_status)  # type: ignore[method-assign]


__all__ = [
    "RecoverableLivePollDeltaIncomplete",
    "install_continuity_durability_repair",
    "_hedged_slot_poll_page",
    "_lease_aware_slot_fetch_delta",
]
