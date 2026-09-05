from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from . import continuity_exact_durable_signature_repair as exact
from . import continuity_recovery_isolation_repair as isolation
from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from . import poll_watermark_repair as watermark
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget
from . import robinhood_chain_runtime as robinhood_runtime


REPAIR_VERSION = "all-regime-runtime-boundaries-v1"
# This is deliberately scoped to urgent recovery after a real WebSocket coverage
# loss. Routine four-second live polling keeps its existing 3x1000 bound. Twelve
# pages covers the dense scout interval seen in production while the unchanged
# twelve-second lease still fails the release closed if proof cannot finish.
URGENT_GAP_RECOVERY_MAX_PAGES = 12

_ORIGINAL_SOLANA_STATUS: Callable[[Any], dict[str, Any]] | None = None


def _paper_transport_ready(self: Any) -> bool:
    cursor = getattr(self, "_cursor", None)
    latest = getattr(self, "_latest_block", None)
    if cursor is None or latest is None:
        return False
    try:
        lag = max(0, int(latest) - int(cursor))
    except (TypeError, ValueError):
        return False
    return bool(getattr(self, "_caught_up", False)) and lag <= robinhood_runtime.LIVE_LAG_BLOCKS


async def _expanded_gap_fetch_delta(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None, dict[str, Any]]:
    """Recover a dense real-WebSocket gap without changing routine poll capacity.

    The previous urgent helper retried the same first three pages until its lease
    expired. On a high-velocity scout address that can make a recoverable interval
    mathematically impossible to prove even when the RPC is healthy. This helper
    allows up to twelve pages only inside the existing dedicated urgent recovery
    call. Commitment, minContextSlot freshness, ordering, and the fixed twelve-second
    recoverability lease remain unchanged.
    """

    pages: list[list[dict[str, Any]]] = []
    page_providers: list[str | None] = []
    page_latencies: list[float | None] = []
    before: str | None = None
    provider: str | None = None
    latency: float | None = None
    complete = False
    context_floor = max(0, int(cursor_slot))
    cursor_reached = False

    for _page_index in range(URGENT_GAP_RECOVERY_MAX_PAGES):
        config: dict[str, Any] = {
            "commitment": "confirmed",
            "limit": live_poll.POLL_LIMIT,
        }
        if before:
            config["before"] = before
        if context_floor > 0:
            config["minContextSlot"] = context_floor
        result, provider, latency = await isolation._recovery_rpc(self).call_with_meta(
            "getSignaturesForAddress",
            [target.address, config],
            hedge=True,
        )
        page = [row for row in result if isinstance(row, dict)] if isinstance(result, list) else []
        pages.append(page)
        page_providers.append(provider)
        page_latencies.append(float(latency) if latency is not None else None)

        if not page:
            complete = True
            break
        slots = [watermark._row_slot(row) for row in page]
        newest_page_slot = max(slots, default=0)
        context_floor = max(context_floor, newest_page_slot)
        if cursor_slot > 0 and any(slot <= cursor_slot for slot in slots):
            cursor_reached = True
            complete = True
            break
        if len(page) < live_poll.POLL_LIMIT:
            complete = True
            break
        before = str(page[-1].get("signature") or "")
        if not before:
            complete = True
            break

    rows: list[dict[str, Any]] = []
    if complete:
        seen: set[str] = set()
        for page in reversed(pages):
            for row in reversed(page):
                signature = str(row.get("signature") or "")
                slot = watermark._row_slot(row)
                if not signature or signature in seen or slot <= cursor_slot:
                    continue
                seen.add(signature)
                rows.append(row)

    all_slots = [watermark._row_slot(row) for page in pages for row in page]
    meta = {
        "page_count": len(pages),
        "page_sizes": [len(page) for page in pages],
        "page_providers": page_providers,
        "page_latencies_ms": page_latencies,
        "newest_slot_seen": max(all_slots, default=0),
        "oldest_slot_seen": min((slot for slot in all_slots if slot > 0), default=0),
        "cursor_slot": int(cursor_slot),
        "cursor_reached": bool(cursor_reached),
        "complete": bool(complete),
        "recovered_row_count": len(rows) if complete else 0,
        "urgent_page_limit": URGENT_GAP_RECOVERY_MAX_PAGES,
        "routine_page_limit_unchanged": live_poll.POLL_CURSOR_MAX_PAGES,
        "fixed_recoverability_lease_seconds": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
    }
    return rows, complete, provider, latency, meta


def _solana_status_with_dense_recovery(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        poll = payload.get("live_poll_redundancy")
        if isinstance(poll, dict):
            poll.update(
                {
                    "urgent_real_gap_recovery_page_limit": URGENT_GAP_RECOVERY_MAX_PAGES,
                    "routine_live_poll_page_limit": live_poll.POLL_CURSOR_MAX_PAGES,
                    "routine_live_poll_bound_unchanged": True,
                    "urgent_recovery_dense_interval_capacity_expanded": True,
                    "real_gap_recovery_lease_seconds": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
                    "real_gap_recovery_still_fixed_lease_bounded": True,
                }
            )
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "urgent_real_gap_recovery_page_limit": URGENT_GAP_RECOVERY_MAX_PAGES,
                    "routine_live_poll_page_limit_unchanged": live_poll.POLL_CURSOR_MAX_PAGES,
                    "urgent_recovery_only_capacity_expansion": True,
                    "strategy_thresholds_unchanged": True,
                    "provider_scope_unchanged": True,
                    "paper_only_authority_unchanged": True,
                }
            )
        payload["all_regime_runtime_boundary_repair"] = {
            "repair_version": REPAIR_VERSION,
            "solana_dense_gap_recovery_repaired": True,
            "routine_poll_bound_unchanged": True,
            "recoverability_lease_seconds": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
            "strategy_thresholds_changed": False,
            "market_scope_reduced": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_all_regime_runtime_boundary_repair", True)
    return status


def _robinhood_status_fail_closed(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        cursor = getattr(self, "_cursor", None)
        latest = getattr(self, "_latest_block", None)
        try:
            lag = max(0, int(latest) - int(cursor)) if cursor is not None and latest is not None else None
        except (TypeError, ValueError):
            lag = None
        ready = _paper_transport_ready(self)
        payload["block_lag"] = lag
        payload["caught_up_for_paper_decisions"] = ready
        payload["paper_decision_transport_ready"] = ready
        repair = payload.setdefault("all_regime_runtime_boundary_repair", {})
        if isinstance(repair, dict):
            repair.update(
                {
                    "repair_version": REPAIR_VERSION,
                    "robinhood_stale_catchup_authority_repaired": True,
                    "paper_entry_requires_current_lag_within_live_boundary": True,
                    "live_lag_blocks": robinhood_runtime.LIVE_LAG_BLOCKS,
                    "strategy_thresholds_changed": False,
                    "paper_only": True,
                    "live_money_authority": False,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_all_regime_robinhood_status", True)
    return status


def _entry_guard(original: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    async def guarded(self: Any, *args: Any, **kwargs: Any) -> Any:
        if not _paper_transport_ready(self):
            # Clear stale authority immediately. The underlying poller will set it
            # true again only after the persisted cursor reaches the live boundary.
            self._caught_up = False
            return None
        return await original(self, *args, **kwargs)

    try:
        guarded.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(guarded, "_roi_current_lag_entry_guard", True)
    return guarded


def install_all_regime_runtime_boundary_repair(plane_cls: type[Any]) -> None:
    """Install the shared Solana/FOMO readiness repair and Robinhood fail-closed guard."""

    global _ORIGINAL_SOLANA_STATUS

    # Preserve the exact-durable wrapper when it is already installed. Its fallback
    # acquisition function is the correct seam to expand for dense scout intervals.
    # If exact durability has not yet composed, patch the base helper and let the
    # later exact installer capture this repaired function normally.
    current_fetch = isolation._isolated_gap_fetch_delta
    if bool(getattr(current_fetch, "_roi_exact_durable_signature", False)):
        exact._ORIGINAL_INTERVAL_FETCH = _expanded_gap_fetch_delta  # type: ignore[assignment]
    else:
        isolation._isolated_gap_fetch_delta = _expanded_gap_fetch_delta  # type: ignore[assignment]

    current_solana_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_solana_status, "_roi_all_regime_runtime_boundary_repair", False)):
        _ORIGINAL_SOLANA_STATUS = current_solana_status
        DirectSolanaIngestionPlane.status = _solana_status_with_dense_recovery(current_solana_status)  # type: ignore[method-assign]

    current_status = plane_cls.status
    if not bool(getattr(current_status, "_roi_all_regime_robinhood_status", False)):
        plane_cls.status = _robinhood_status_fail_closed(current_status)  # type: ignore[method-assign]

    for name in ("_maybe_open_v3", "_maybe_open_v2"):
        current = getattr(plane_cls, name)
        if not bool(getattr(current, "_roi_current_lag_entry_guard", False)):
            setattr(plane_cls, name, _entry_guard(current))

    setattr(plane_cls, "_roi_all_regime_runtime_boundary_repair_installed", True)
    setattr(plane_cls, "_roi_all_regime_runtime_boundary_repair_version", REPAIR_VERSION)


__all__ = [
    "REPAIR_VERSION",
    "URGENT_GAP_RECOVERY_MAX_PAGES",
    "_expanded_gap_fetch_delta",
    "_paper_transport_ready",
    "install_all_regime_runtime_boundary_repair",
]
