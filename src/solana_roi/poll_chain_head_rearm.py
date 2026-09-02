from __future__ import annotations

from collections.abc import Callable
from typing import Any

from . import live_poll_redundancy as live_poll
from . import poll_standby_rearm as standby
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


async def _try_rearm_from_confirmed_chain_head(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[int, str, float] | None:
    """Re-arm standby at the confirmed chain head under live WebSocket coverage.

    Re-arm never reconstructs or certifies the skipped interval. The caller already
    proves that the same target remained continuously authoritative on the real
    WebSocket union. In that case the poll lane only needs a prospective baseline
    for future fallback observation. A target-specific signature-head request can
    be served by a public backend that is merely as fresh as the old target cursor,
    which can leave a high-volume standby frozen forever. `getSlot(confirmed)` is a
    cheaper provider-independent chain watermark and, when hedged, forces the
    standby baseline to advance to a current confirmed point before future polling.
    """

    if not live_poll._ws_target_covered(self, target):
        return None

    result, provider, latency = await live_poll._poll_rpc(self).call_with_meta(
        "getSlot",
        [{"commitment": "confirmed"}],
        hedge=True,
    )
    try:
        head_slot = int(result or 0)
    except (TypeError, ValueError):
        return None
    if head_slot <= int(cursor_slot):
        return None

    key = live_poll._poll_target_key(target)
    stats = standby._rearm_stats(self)
    stats[key] = int(stats.get(key, 0) or 0) + 1
    return head_slot, provider, latency


def _status_with_chain_head_rearm(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        poll = payload.get("live_poll_redundancy")
        if isinstance(poll, dict):
            poll["standby_rearm_baseline"] = "confirmed-chain-slot-head"
            poll["standby_rearm_target_signature_head_required"] = False
            poll["standby_rearm_getslot_hedged"] = True
            poll["standby_rearm_restores_prior_prospective_evidence"] = False
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "live_poll_standby_rearm_uses_confirmed_chain_head": True,
                    "live_poll_standby_rearm_requires_target_signature_head": False,
                    "live_poll_standby_rearm_getslot_hedged": True,
                    "live_poll_standby_rearm_can_restore_gap": False,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_poll_chain_head_rearm", True)
    return status


def install_poll_chain_head_rearm() -> None:
    standby._try_rearm_under_websocket = _try_rearm_from_confirmed_chain_head  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_poll_chain_head_rearm", False)):
        DirectSolanaIngestionPlane.status = _status_with_chain_head_rearm(current_status)  # type: ignore[method-assign]


__all__ = [
    "install_poll_chain_head_rearm",
    "_try_rearm_from_confirmed_chain_head",
]
