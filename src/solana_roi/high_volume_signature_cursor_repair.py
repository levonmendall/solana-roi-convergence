from __future__ import annotations

from collections.abc import Callable
from typing import Any

from . import continuity_storage_capacity_repair as storage
from . import live_poll_redundancy as live_poll
from . import poll_watermark_repair as watermark
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


REPAIR_VERSION = "high-volume-exact-signature-poll-v1"
HIGH_VOLUME_SOURCES = frozenset({"PUMP_AMM", "PUMP_FUN"})

_ORIGINAL_SLOT_POLL_PAGE: Callable[..., Any] | None = None
_ORIGINAL_SLOT_FETCH_DELTA: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None


def _target_key(target: WatchTarget) -> str:
    return live_poll._poll_target_key(target)


def _is_high_volume(target: WatchTarget) -> bool:
    return str(getattr(target, "source_hint", "") or "").upper() in HIGH_VOLUME_SOURCES


def _cursor_state(self: Any) -> dict[str, dict[str, Any]]:
    value = getattr(self, "_roi_high_volume_exact_poll_cursors", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_high_volume_exact_poll_cursors", value)
    return value


def _inc(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_high_volume_exact_poll_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


def _signature(row: dict[str, Any]) -> str:
    return str(row.get("signature") or "")


def _save_cursor(
    self: Any,
    target: WatchTarget,
    *,
    signature: str,
    slot: int,
    provider: str | None,
    source: str,
) -> None:
    if not signature or int(slot) <= 0:
        return
    _cursor_state(self)[_target_key(target)] = {
        "signature": signature,
        "slot": int(slot),
        "provider": provider,
        "source": source,
    }


async def _poll_page_with_exact_baseline(
    self: Any,
    target: WatchTarget,
    *,
    before: str | None = None,
    min_context_slot: int | None = None,
    limit: int | None = None,
) -> tuple[list[dict[str, Any]], str, float]:
    """Keep the exact signature that established a high-volume slot baseline.

    The existing confirmed-slot watermark remains the audit/fallback cursor. For
    Pump.fun and Pump AMM only, the exact confirmed signature is retained beside it
    so subsequent four-second deltas can ask the assigned RPC backend to stop at the
    prior boundary instead of walking arbitrary older address history.
    """

    if _ORIGINAL_SLOT_POLL_PAGE is None:
        raise RuntimeError("high-volume exact signature cursor repair is not installed")
    rows, provider, latency = await _ORIGINAL_SLOT_POLL_PAGE(
        self,
        target,
        before=before,
        min_context_slot=min_context_slot,
        limit=limit,
    )
    if _is_high_volume(target) and before is None and int(limit or 0) == 1 and rows:
        row = rows[0]
        signature = _signature(row)
        slot = watermark._row_slot(row)
        if signature and slot > 0:
            _save_cursor(
                self,
                target,
                signature=signature,
                slot=slot,
                provider=provider,
                source="confirmed-head-baseline",
            )
            _inc(self, "baseline_updates")
    return rows, provider, latency


async def _exact_page(
    self: Any,
    target: WatchTarget,
    *,
    until: str,
    min_context_slot: int,
    before: str | None = None,
) -> tuple[list[dict[str, Any]], str, float]:
    """Read one routine page from the target's already-assigned public RPC.

    This does not add providers, hedging or fanout. It preserves the provider shard
    and existing process-wide governor while adding only the server-side `until`
    boundary for the two burst-heavy Pump targets.
    """

    config: dict[str, Any] = {
        "commitment": "confirmed",
        "limit": live_poll.POLL_LIMIT,
        "until": until,
    }
    if before:
        config["before"] = before
    if min_context_slot > 0:
        config["minContextSlot"] = int(min_context_slot)
    result, provider, latency = await storage._routine_poll_pool(self, target).call_with_meta(
        "getSignaturesForAddress",
        [target.address, config],
        hedge=False,
    )
    rows = [row for row in result if isinstance(row, dict)] if isinstance(result, list) else []
    return rows, provider, latency


async def _fetch_delta_with_high_volume_exact_cursor(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    """Use an exact confirmed lower boundary only for Pump.fun/Pump AMM.

    The previous slot-only routine poll could consume 3x1000 signatures without
    ever reaching a slot boundary on burst-heavy program addresses. That left the
    routine standby cursor stale, so a later WebSocket gap inherited thousands of
    unrelated signatures even when its actual interval was small.

    The hard 3x1000 bound is unchanged. If the exact signature is unavailable on the
    assigned backend, three full pages still fail closed; no cursor advances and no
    evidence is silently skipped. Same-slot signatures are retained because the
    exact signature, not `slot <= cursor_slot`, proves completion.
    """

    if _ORIGINAL_SLOT_FETCH_DELTA is None:
        raise RuntimeError("high-volume exact signature cursor repair is not installed")
    if not _is_high_volume(target):
        return await _ORIGINAL_SLOT_FETCH_DELTA(self, target, cursor_slot)

    key = _target_key(target)
    cursor = _cursor_state(self).get(key)
    exact_signature = str(cursor.get("signature") or "") if isinstance(cursor, dict) else ""
    exact_slot = int(cursor.get("slot") or 0) if isinstance(cursor, dict) else 0
    if not exact_signature or exact_slot < int(cursor_slot):
        _inc(self, "fallback_slot_fetches")
        rows, complete, provider, latency = await _ORIGINAL_SLOT_FETCH_DELTA(
            self, target, cursor_slot
        )
        if complete and rows:
            newest = rows[-1]
            newest_signature = _signature(newest)
            newest_slot = watermark._row_slot(newest)
            if newest_signature and newest_slot > 0:
                _save_cursor(
                    self,
                    target,
                    signature=newest_signature,
                    slot=newest_slot,
                    provider=provider,
                    source="slot-fallback-complete",
                )
                _inc(self, "fallback_cursor_updates")
        return rows, complete, provider, latency

    pages: list[list[dict[str, Any]]] = []
    before: str | None = None
    provider: str | None = None
    latency: float | None = None
    complete = False

    for _page_index in range(live_poll.POLL_CURSOR_MAX_PAGES):
        page, provider, latency = await _exact_page(
            self,
            target,
            until=exact_signature,
            min_context_slot=max(int(cursor_slot), exact_slot),
            before=before,
        )
        pages.append(page)
        if not page or len(page) < live_poll.POLL_LIMIT:
            complete = True
            break
        before = _signature(page[-1])
        if not before:
            complete = True
            break

    if not complete:
        _inc(self, "bounded_overflows")
        return [], False, provider, latency

    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for page in reversed(pages):
        for row in reversed(page):
            signature = _signature(row)
            if not signature or signature == exact_signature or signature in seen:
                continue
            seen.add(signature)
            ordered.append(row)

    if pages and pages[0]:
        newest = pages[0][0]
        newest_signature = _signature(newest)
        newest_slot = watermark._row_slot(newest)
        if newest_signature and newest_slot > 0:
            _save_cursor(
                self,
                target,
                signature=newest_signature,
                slot=newest_slot,
                provider=provider,
                source="exact-until-complete",
            )
            _inc(self, "cursor_advances")
    _inc(self, "completed_deltas")
    _inc(self, "rows", len(ordered))
    return ordered, True, provider, latency


setattr(_poll_page_with_exact_baseline, "_roi_high_volume_signature_cursor", True)
setattr(_fetch_delta_with_high_volume_exact_cursor, "_roi_high_volume_signature_cursor", True)


def _status_with_high_volume_exact_cursor(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("high-volume exact signature cursor status is not installed")
    payload = _ORIGINAL_STATUS(self)
    cursors = _cursor_state(self)
    payload["high_volume_exact_signature_live_poll"] = {
        "installed": True,
        "version": REPAIR_VERSION,
        "scope": sorted(HIGH_VOLUME_SOURCES),
        "cursor_count": len(cursors),
        "cursor_keys": sorted(cursors),
        "baseline_updates_session": int(getattr(self, "_roi_high_volume_exact_poll_baseline_updates", 0) or 0),
        "cursor_advances_session": int(getattr(self, "_roi_high_volume_exact_poll_cursor_advances", 0) or 0),
        "completed_deltas_session": int(getattr(self, "_roi_high_volume_exact_poll_completed_deltas", 0) or 0),
        "rows_session": int(getattr(self, "_roi_high_volume_exact_poll_rows", 0) or 0),
        "bounded_overflows_session": int(getattr(self, "_roi_high_volume_exact_poll_bounded_overflows", 0) or 0),
        "fallback_slot_fetches_session": int(getattr(self, "_roi_high_volume_exact_poll_fallback_slot_fetches", 0) or 0),
        "server_side_until_cursor": True,
        "same_slot_rows_preserved": True,
        "assigned_provider_shard_preserved": True,
        "poll_interval_seconds_unchanged": live_poll.POLL_INTERVAL_SECONDS,
        "hard_page_count_unchanged": live_poll.POLL_CURSOR_MAX_PAGES,
        "hard_page_size_unchanged": live_poll.POLL_LIMIT,
        "provider_scope_changed": False,
        "additional_rpc_fanout": False,
        "historical_backfill_allowed": False,
        "paper_only": True,
        "signing_available": False,
        "transaction_submission_available": False,
        "live_money_authority": False,
    }
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "high_volume_live_poll_exact_signature_cursor": True,
                "high_volume_live_poll_server_side_until_cursor": True,
                "high_volume_live_poll_same_slot_rows_preserved": True,
                "routine_poll_interval_unchanged": True,
                "recovery_bound_unchanged": True,
                "continuity_lease_unchanged": True,
                "provider_scope_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    return payload


setattr(_status_with_high_volume_exact_cursor, "_roi_high_volume_signature_cursor", True)


def install_high_volume_signature_cursor_repair() -> None:
    global _ORIGINAL_SLOT_POLL_PAGE, _ORIGINAL_SLOT_FETCH_DELTA, _ORIGINAL_STATUS

    if bool(getattr(watermark._slot_fetch_delta, "_roi_high_volume_signature_cursor", False)):
        return
    _ORIGINAL_SLOT_POLL_PAGE = watermark._slot_poll_page
    _ORIGINAL_SLOT_FETCH_DELTA = watermark._slot_fetch_delta
    watermark._slot_poll_page = _poll_page_with_exact_baseline  # type: ignore[assignment]
    watermark._slot_fetch_delta = _fetch_delta_with_high_volume_exact_cursor  # type: ignore[assignment]
    # Some compatibility paths resolve the live-poll globals directly.
    live_poll._poll_page = _poll_page_with_exact_baseline  # type: ignore[assignment]
    live_poll._fetch_delta = _fetch_delta_with_high_volume_exact_cursor  # type: ignore[assignment]

    _ORIGINAL_STATUS = DirectSolanaIngestionPlane.status
    try:
        _status_with_high_volume_exact_cursor.__dict__.update(getattr(_ORIGINAL_STATUS, "__dict__", {}))
    except Exception:
        pass
    DirectSolanaIngestionPlane.status = _status_with_high_volume_exact_cursor  # type: ignore[method-assign]


__all__ = [
    "HIGH_VOLUME_SOURCES",
    "REPAIR_VERSION",
    "_fetch_delta_with_high_volume_exact_cursor",
    "install_high_volume_signature_cursor_repair",
]
