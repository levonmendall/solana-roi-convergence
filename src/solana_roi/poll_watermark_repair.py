from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from . import direct_solana as direct_solana_module
from . import live_poll_redundancy as live_poll
from . import target_quorum
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


def _row_slot(row: dict[str, Any]) -> int:
    try:
        return int(row.get("slot") or 0)
    except (TypeError, ValueError):
        return 0


async def _slot_poll_page(
    self: Any,
    target: WatchTarget,
    *,
    before: str | None = None,
    min_context_slot: int | None = None,
    limit: int | None = None,
) -> tuple[list[dict[str, Any]], str, float]:
    """Read one confirmed signature page without relying on a signature cursor.

    Public RPC hosts may route sequential requests to different backends. A
    signature returned by one backend therefore is not a safe cross-request
    server-side ``until`` boundary. A confirmed slot watermark is stable across
    providers and ``minContextSlot`` prevents a lagging backend from silently
    answering from a context older than the prospective watermark.
    """

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
        hedge=False,
    )
    rows = [row for row in result if isinstance(row, dict)] if isinstance(result, list) else []
    return rows, provider, latency


async def _slot_fetch_delta(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    """Fetch all signatures in confirmed slots newer than ``cursor_slot``.

    Completion is proved by reaching a row at or below the prior confirmed slot,
    an empty/short page, or the bounded end of history. The prior signature never
    has to be returned by the current RPC backend. This avoids false cursor
    overflows when a public load-balanced RPC changes backend between polls.
    """

    pages: list[list[dict[str, Any]]] = []
    before: str | None = None
    provider: str | None = None
    latency: float | None = None
    complete = False

    for _page_index in range(live_poll.POLL_CURSOR_MAX_PAGES):
        page, provider, latency = await _slot_poll_page(
            self,
            target,
            before=before,
            min_context_slot=cursor_slot if cursor_slot > 0 else None,
            limit=live_poll.POLL_LIMIT,
        )
        pages.append(page)
        if not page:
            complete = True
            break

        if cursor_slot > 0 and any(_row_slot(row) <= cursor_slot for row in page):
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
            slot = _row_slot(row)
            if not signature or signature in seen or slot <= cursor_slot:
                continue
            seen.add(signature)
            rows.append(row)
    return rows, True, provider, latency


async def _slot_poll_target(self: Any, target: WatchTarget, stop: asyncio.Event) -> None:
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
                baseline, provider, latency = await _slot_poll_page(self, target, limit=1)
                cursor_slot = _row_slot(baseline[0]) if baseline else 0
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
                    "failures": failures,
                }
                await target_quorum._quorum_set_target_state(
                    self, live_poll._POLL_ENDPOINT, target, connected=True
                )
            else:
                new_rows, complete, provider, latency = await _slot_fetch_delta(
                    self, target, cursor_slot
                )
                if not complete:
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
                    newest_slot = max((_row_slot(row) for row in new_rows), default=cursor_slot)
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


def _status_with_slot_watermark(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        poll = payload.get("live_poll_redundancy")
        if isinstance(poll, dict):
            poll["cursor_model"] = "confirmed-slot-watermark"
            poll["server_side_until_cursor"] = False
            poll["min_context_slot_enforced"] = True
            poll["signature_cursor_required_for_completion"] = False
            poll["provider_agnostic_watermark"] = True
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "live_poll_server_side_cursor_bounding": False,
                    "live_poll_confirmed_slot_watermark": True,
                    "live_poll_min_context_slot_enforced": True,
                    "live_poll_signature_cursor_required": False,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_slot_watermark_poll", True)
    return status


def install_poll_watermark_repair() -> None:
    # The existing run wrapper resolves these names from the live-poll module at
    # execution time, so replacing the globals repairs every entrypoint without
    # adding another worker/fanout layer.
    live_poll._poll_page = _slot_poll_page  # type: ignore[assignment]
    live_poll._fetch_delta = _slot_fetch_delta  # type: ignore[assignment]
    live_poll._poll_target = _slot_poll_target  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_slot_watermark_poll", False)):
        DirectSolanaIngestionPlane.status = _status_with_slot_watermark(current_status)  # type: ignore[method-assign]


__all__ = [
    "install_poll_watermark_repair",
    "_slot_poll_page",
    "_slot_fetch_delta",
    "_slot_poll_target",
]
