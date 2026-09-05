from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from . import continuity_exact_durable_signature_repair as exact
from . import continuity_recovery_isolation_repair as isolation
from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from . import poll_watermark_repair as watermark
from . import robinhood_chain_runtime as robinhood_runtime
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


REPAIR_VERSION = "all-regime-runtime-boundaries-v1"
URGENT_SCOUT_GAP_RECOVERY_MAX_PAGES = 12
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
    """Recover a dense strategy-scout WebSocket gap under the existing fixed lease."""
    pages: list[list[dict[str, Any]]] = []
    providers: list[str | None] = []
    latencies: list[float | None] = []
    before: str | None = None
    provider: str | None = None
    latency: float | None = None
    complete = False
    context_floor = max(0, int(cursor_slot))
    cursor_reached = False

    for _ in range(URGENT_SCOUT_GAP_RECOVERY_MAX_PAGES):
        config: dict[str, Any] = {"commitment": "confirmed", "limit": live_poll.POLL_LIMIT}
        if before:
            config["before"] = before
        if context_floor > 0:
            config["minContextSlot"] = context_floor
        result, provider, latency = await isolation._recovery_rpc(self).call_with_meta(
            "getSignaturesForAddress", [target.address, config], hedge=True
        )
        page = [row for row in result if isinstance(row, dict)] if isinstance(result, list) else []
        pages.append(page)
        providers.append(provider)
        latencies.append(float(latency) if latency is not None else None)
        if not page:
            complete = True
            break
        slots = [watermark._row_slot(row) for row in page]
        context_floor = max(context_floor, max(slots, default=0))
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
        "page_providers": providers,
        "page_latencies_ms": latencies,
        "newest_slot_seen": max(all_slots, default=0),
        "oldest_slot_seen": min((slot for slot in all_slots if slot > 0), default=0),
        "cursor_slot": int(cursor_slot),
        "cursor_reached": bool(cursor_reached),
        "complete": bool(complete),
        "recovered_row_count": len(rows) if complete else 0,
        "hard_page_limit": URGENT_SCOUT_GAP_RECOVERY_MAX_PAGES,
        "hard_page_size": live_poll.POLL_LIMIT,
        "routine_page_limit_unchanged": live_poll.POLL_CURSOR_MAX_PAGES,
        "fixed_recoverability_lease_seconds": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
    }
    return rows, complete, provider, latency, meta


def _scout_routed_fetch(base: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    async def routed(self: Any, target: WatchTarget, cursor_slot: int):
        if str(getattr(target, "kind", "")) == "scout":
            return await _expanded_gap_fetch_delta(self, target, cursor_slot)
        return await base(self, target, cursor_slot)

    setattr(routed, "_roi_dense_scout_gap_recovery", True)
    return routed


def _solana_status_with_repair(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        poll = payload.get("live_poll_redundancy")
        if isinstance(poll, dict):
            poll.update({
                "urgent_scout_gap_recovery_page_limit": URGENT_SCOUT_GAP_RECOVERY_MAX_PAGES,
                "routine_live_poll_page_limit": live_poll.POLL_CURSOR_MAX_PAGES,
                "program_gap_recovery_page_limit_unchanged": live_poll.POLL_CURSOR_MAX_PAGES,
                "urgent_scout_recovery_dense_interval_capacity_expanded": True,
                "real_gap_recovery_lease_seconds": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
                "real_gap_recovery_still_fixed_lease_bounded": True,
            })
        payload["all_regime_runtime_boundary_repair"] = {
            "repair_version": REPAIR_VERSION,
            "solana_strategy_scout_dense_gap_recovery_repaired": True,
            "program_and_routine_poll_bounds_unchanged": True,
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


def _robinhood_status_fail_closed(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
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
            self._caught_up = False
            return None
        return await original(self, *args, **kwargs)

    setattr(guarded, "_roi_current_lag_entry_guard", True)
    return guarded


def install_all_regime_runtime_boundary_repair(plane_cls: type[Any]) -> None:
    global _ORIGINAL_SOLANA_STATUS

    current_fetch = isolation._isolated_gap_fetch_delta
    if bool(getattr(current_fetch, "_roi_exact_durable_signature", False)):
        base = getattr(exact, "_ORIGINAL_INTERVAL_FETCH", None)
        if callable(base) and not bool(getattr(base, "_roi_dense_scout_gap_recovery", False)):
            exact._ORIGINAL_INTERVAL_FETCH = _scout_routed_fetch(base)  # type: ignore[assignment]
    elif not bool(getattr(current_fetch, "_roi_dense_scout_gap_recovery", False)):
        isolation._isolated_gap_fetch_delta = _scout_routed_fetch(current_fetch)  # type: ignore[assignment]

    current_solana_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_solana_status, "_roi_all_regime_runtime_boundary_repair", False)):
        _ORIGINAL_SOLANA_STATUS = current_solana_status
        DirectSolanaIngestionPlane.status = _solana_status_with_repair(current_solana_status)  # type: ignore[method-assign]

    current_status = plane_cls.status
    if not bool(getattr(current_status, "_roi_all_regime_robinhood_status", False)):
        plane_cls.status = _robinhood_status_fail_closed(current_status)  # type: ignore[method-assign]
    for name in ("_maybe_open_v3", "_maybe_open_v2"):
        current = getattr(plane_cls, name, None)
        if callable(current) and not bool(getattr(current, "_roi_current_lag_entry_guard", False)):
            setattr(plane_cls, name, _entry_guard(current))

    setattr(plane_cls, "_roi_all_regime_runtime_boundary_repair_installed", True)
    setattr(plane_cls, "_roi_all_regime_runtime_boundary_repair_version", REPAIR_VERSION)


__all__ = [
    "REPAIR_VERSION",
    "URGENT_SCOUT_GAP_RECOVERY_MAX_PAGES",
    "_expanded_gap_fetch_delta",
    "_paper_transport_ready",
    "install_all_regime_runtime_boundary_repair",
]
