from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Callable
from typing import Any

from . import continuity_gap_clock_repair as gap_clock
from . import continuity_immediate_recovery_repair as immediate
from . import continuity_recovery_isolation_repair as isolation
from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


TARGET_FRONTIER_HISTORY = 16
FRONTIER_CONFIRMATION_ATTEMPTS = 2
FRONTIER_CONFIRMATION_RETRY_SECONDS = 0.10


def _increment(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_target_frontier_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


def _frontiers(self: Any) -> dict[str, deque[dict[str, Any]]]:
    value = getattr(self, "_roi_target_ws_frontiers", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_target_ws_frontiers", value)
    return value


def _target_history(self: Any, target: WatchTarget) -> deque[dict[str, Any]]:
    key = live_poll._poll_target_key(target)
    rows = _frontiers(self).get(key)
    if not isinstance(rows, deque):
        rows = deque(maxlen=TARGET_FRONTIER_HISTORY)
        _frontiers(self)[key] = rows
    return rows


def _observe_target_frontier(
    self: Any,
    provider: str,
    target: WatchTarget,
    signature: str,
    slot: int,
    *,
    observed_monotonic: float | None = None,
) -> None:
    """Remember exact target receipts before durable/background processing.

    The routine poll watermark is only a standby cursor and can be stale when its
    assigned public RPC endpoint is cooling down. A real WebSocket receipt is not
    itself used as a recovery boundary: it is retained only as a candidate anchor
    and must later be proved confirmed by standard read-only RPC.
    """

    signature = str(signature or "")
    try:
        slot = int(slot)
    except (TypeError, ValueError):
        slot = 0
    if not signature or slot <= 0:
        return

    rows = _target_history(self, target)
    if any(str(row.get("signature") or "") == signature for row in rows):
        return
    rows.append(
        {
            "signature": signature,
            "slot": slot,
            "provider": str(provider),
            "observed_monotonic": float(
                time.monotonic() if observed_monotonic is None else observed_monotonic
            ),
        }
    )
    _increment(self, "observations")


def _parse_target_notification(
    subscription_targets: dict[int, WatchTarget],
    message: dict[str, Any],
) -> tuple[WatchTarget, str, int] | None:
    if message.get("method") != "logsNotification":
        return None
    params = message.get("params")
    if not isinstance(params, dict):
        return None
    try:
        subscription = int(params["subscription"])
        result = params["result"]
        value = result["value"]
        signature = str(value["signature"])
        slot = int(result["context"]["slot"])
    except (KeyError, TypeError, ValueError):
        return None
    target = subscription_targets.get(subscription)
    if target is None or not signature or slot <= 0:
        return None
    return target, signature, slot


def _handler_with_target_frontier(
    original: Callable[[Any, str, dict[int, WatchTarget], dict[str, Any]], Any],
) -> Callable[[Any, str, dict[int, WatchTarget], dict[str, Any]], Any]:
    async def handle(
        self: Any,
        provider: str,
        subscription_targets: dict[int, WatchTarget],
        message: dict[str, Any],
    ) -> None:
        parsed = _parse_target_notification(subscription_targets, message)
        if parsed is not None:
            target, signature, slot = parsed
            _observe_target_frontier(
                self,
                provider,
                target,
                signature,
                slot,
                observed_monotonic=time.monotonic(),
            )
        await original(self, provider, subscription_targets, message)

    try:
        handle.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(handle, "_roi_target_frontier_recovery", True)
    return handle


def _gap_deadline(self: Any, target: WatchTarget) -> float:
    key = live_poll._poll_target_key(target)
    row = gap_clock._gap_clocks(self).get(key)
    if isinstance(row, dict):
        try:
            return float(row["started_monotonic"]) + lease.POLL_RECOVERABILITY_LEASE_SECONDS
        except (KeyError, TypeError, ValueError):
            pass
    return time.monotonic() + lease.POLL_RECOVERABILITY_LEASE_SECONDS


def _confirmation_rows(result: Any) -> list[Any]:
    value = result.get("value") if isinstance(result, dict) else None
    return list(value) if isinstance(value, list) else []


async def _confirmed_target_frontier_cursor(
    self: Any,
    target: WatchTarget,
    routine_cursor_slot: int,
    generation: int,
) -> tuple[int, dict[str, Any] | None]:
    """Return a safe slot cursor derived from a confirmed target receipt.

    WebSocket observations use processed commitment, so they are never trusted as
    recovery boundaries directly. We batch-confirm recent exact target signatures
    with standard RPC. Once a signature is confirmed/finalized, recovery starts at
    one slot *before* that signature so every same-slot target transaction is read
    again. Duplicate durable receipts are harmless; skipping same-slot evidence is
    not allowed.
    """

    candidates = list(_target_history(self, target))
    if not candidates:
        _increment(self, "anchor_fallbacks")
        return int(routine_cursor_slot), None

    # Highest observed slots first; retain only unique signatures within the small
    # bounded history. One RPC call confirms the whole candidate set.
    candidates.sort(key=lambda row: (int(row.get("slot") or 0), float(row.get("observed_monotonic") or 0.0)), reverse=True)
    candidates = candidates[:TARGET_FRONTIER_HISTORY]
    signatures = [str(row.get("signature") or "") for row in candidates]
    deadline = _gap_deadline(self, target)
    last_provider: str | None = None
    last_latency: float | None = None

    for attempt in range(FRONTIER_CONFIRMATION_ATTEMPTS):
        if immediate._generation(self, target) != int(generation):
            raise RuntimeError("real websocket gap generation superseded")
        if attempt > 0 and time.monotonic() >= deadline:
            break
        _increment(self, "confirmation_attempts")
        try:
            result, last_provider, last_latency = await isolation._recovery_rpc(self).call_with_meta(
                "getSignatureStatuses",
                [signatures, {"searchTransactionHistory": True}],
                hedge=True,
            )
            statuses = _confirmation_rows(result)
            confirmed: list[tuple[int, dict[str, Any]]] = []
            for candidate, status in zip(candidates, statuses):
                if not isinstance(status, dict):
                    continue
                confirmation = str(status.get("confirmationStatus") or "").lower()
                if confirmation not in {"confirmed", "finalized"}:
                    continue
                try:
                    status_slot = int(status.get("slot") or 0)
                    observed_slot = int(candidate.get("slot") or 0)
                except (TypeError, ValueError):
                    continue
                if status_slot <= 0 or status_slot != observed_slot:
                    continue
                confirmed.append((status_slot, candidate))
            if confirmed:
                confirmed_slot, candidate = max(confirmed, key=lambda item: item[0])
                # Read the whole confirmed frontier slot again. The existing
                # recovery path filters only slots <= cursor, so slot-1 is the
                # strongest safe bound that cannot omit another same-slot receipt.
                effective_cursor = max(int(routine_cursor_slot), confirmed_slot - 1)
                anchor = {
                    "source": "confirmed-target-websocket-frontier",
                    "target": live_poll._poll_target_key(target),
                    "generation": int(generation),
                    "signature": str(candidate.get("signature") or ""),
                    "websocket_provider": str(candidate.get("provider") or ""),
                    "confirmation_provider": last_provider,
                    "confirmation_latency_ms": last_latency,
                    "routine_cursor_slot": int(routine_cursor_slot),
                    "confirmed_frontier_slot": confirmed_slot,
                    "effective_cursor_slot": effective_cursor,
                    "same_slot_replay_required": True,
                }
                setattr(self, "_roi_target_frontier_last_anchor", anchor)
                _increment(self, "confirmed_anchors")
                return effective_cursor, anchor
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            setattr(self, "_roi_target_frontier_last_confirmation_error", type(exc).__name__)
            _increment(self, "confirmation_errors")

        if attempt + 1 < FRONTIER_CONFIRMATION_ATTEMPTS:
            remaining = deadline - time.monotonic()
            if remaining > 0:
                await asyncio.sleep(min(FRONTIER_CONFIRMATION_RETRY_SECONDS, remaining))

    _increment(self, "anchor_fallbacks")
    setattr(
        self,
        "_roi_target_frontier_last_anchor",
        {
            "source": "routine-poll-slot-fallback",
            "target": live_poll._poll_target_key(target),
            "generation": int(generation),
            "routine_cursor_slot": int(routine_cursor_slot),
            "effective_cursor_slot": int(routine_cursor_slot),
            "same_slot_replay_required": False,
        },
    )
    return int(routine_cursor_slot), None


def _annotate_attribution(self: Any, anchor: dict[str, Any] | None, routine_cursor_slot: int) -> None:
    state = isolation._attribution_state(self)
    row = state.get("last_success") or state.get("last_failure")
    if not isinstance(row, dict):
        return
    row["routine_cursor_slot"] = int(routine_cursor_slot)
    row["recovery_anchor_source"] = (
        str(anchor.get("source")) if isinstance(anchor, dict) else "routine-poll-slot-fallback"
    )
    if isinstance(anchor, dict):
        row["confirmed_frontier_slot"] = int(anchor.get("confirmed_frontier_slot") or 0)
        row["same_slot_replay_required"] = True


async def _recover_with_target_frontier(
    self: Any,
    target: WatchTarget,
    routine_cursor_slot: int,
    generation: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    effective_cursor, anchor = await _confirmed_target_frontier_cursor(
        self,
        target,
        routine_cursor_slot,
        generation,
    )
    try:
        result = await isolation._recover_with_isolated_rpc(
            self,
            target,
            effective_cursor,
            generation,
        )
    except BaseException:
        _annotate_attribution(self, anchor, routine_cursor_slot)
        raise
    _annotate_attribution(self, anchor, routine_cursor_slot)
    return result


def _kick_with_target_frontier(self: Any, target: WatchTarget, generation: int) -> None:
    """Kick immediate recovery using a confirmed target frontier when available."""

    key = live_poll._poll_target_key(target)
    state = live_poll._poll_state(self).get(key)
    if not isinstance(state, dict) or not bool(state.get("baseline_established")):
        immediate._increment(self, "kick_skipped_no_baseline")
        return
    try:
        routine_cursor_slot = int(state.get("cursor_slot") or 0)
    except (TypeError, ValueError):
        routine_cursor_slot = 0
    if routine_cursor_slot <= 0:
        immediate._increment(self, "kick_skipped_no_cursor")
        return

    tasks = immediate._recovery_tasks(self)
    previous = tasks.get(key)
    if isinstance(previous, dict):
        previous_task = previous.get("task")
        if (
            int(previous.get("generation", -1)) == int(generation)
            and int(previous.get("cursor_slot", -1)) == routine_cursor_slot
            and isinstance(previous_task, asyncio.Task)
            and not previous_task.done()
        ):
            return
        if isinstance(previous_task, asyncio.Task) and not previous_task.done():
            previous_task.cancel()

    task = asyncio.create_task(
        _recover_with_target_frontier(self, target, routine_cursor_slot, generation),
        name=f"target-frontier-gap-recovery:{target.kind}:{target.address[:8]}",
    )
    # Keep the canonical routine cursor in task metadata. The existing lease proxy
    # matches this value before consuming the already-running recovery task.
    tasks[key] = {
        "generation": int(generation),
        "cursor_slot": routine_cursor_slot,
        "task": task,
        "started_monotonic": time.monotonic(),
    }
    immediate._increment(self, "kicked")


def _status_with_target_frontier(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        histories = _frontiers(self)
        poll = payload.get("live_poll_redundancy")
        if isinstance(poll, dict):
            poll.update(
                {
                    "real_gap_recovery_target_frontier_anchor": True,
                    "target_frontier_history_per_target": TARGET_FRONTIER_HISTORY,
                    "target_frontier_target_count": sum(1 for rows in histories.values() if rows),
                    "target_frontier_observations": int(getattr(self, "_roi_target_frontier_observations", 0) or 0),
                    "target_frontier_confirmation_attempts": int(getattr(self, "_roi_target_frontier_confirmation_attempts", 0) or 0),
                    "target_frontier_confirmation_errors": int(getattr(self, "_roi_target_frontier_confirmation_errors", 0) or 0),
                    "target_frontier_confirmed_anchors": int(getattr(self, "_roi_target_frontier_confirmed_anchors", 0) or 0),
                    "target_frontier_anchor_fallbacks": int(getattr(self, "_roi_target_frontier_anchor_fallbacks", 0) or 0),
                    "target_frontier_last_anchor": getattr(self, "_roi_target_frontier_last_anchor", None),
                    "target_frontier_last_confirmation_error": getattr(self, "_roi_target_frontier_last_confirmation_error", None),
                    "target_frontier_requires_confirmed_signature": True,
                    "target_frontier_replays_entire_anchor_slot": True,
                    "real_gap_recovery_lease_seconds": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
                    "real_gap_recovery_hard_delta_bound_unchanged": True,
                }
            )
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "real_gap_recovery_uses_confirmed_target_websocket_frontier": True,
                    "processed_websocket_receipt_never_trusted_without_confirmation": True,
                    "confirmed_frontier_same_slot_is_replayed": True,
                    "live_poll_recoverability_lease_seconds": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
                    "live_poll_hard_delta_bound_unchanged": True,
                    "paper_only_authority_unchanged": True,
                    "signing_or_submission_available": False,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_target_frontier_recovery", True)
    return status


def install_continuity_target_frontier_repair() -> None:
    """Repair stale recovery anchors without changing any certification boundary."""

    current_handler = DirectSolanaIngestionPlane._handle_notification
    if not bool(getattr(current_handler, "_roi_target_frontier_recovery", False)):
        DirectSolanaIngestionPlane._handle_notification = _handler_with_target_frontier(current_handler)  # type: ignore[method-assign]

    # The tracked quorum setter resolves this global dynamically at the actual
    # zero-WebSocket transition, so replacing only the kick function preserves the
    # canonical lease worker and all existing state transitions.
    immediate._kick_immediate_recovery = _kick_with_target_frontier  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_target_frontier_recovery", False)):
        DirectSolanaIngestionPlane.status = _status_with_target_frontier(current_status)  # type: ignore[method-assign]


__all__ = [
    "FRONTIER_CONFIRMATION_ATTEMPTS",
    "TARGET_FRONTIER_HISTORY",
    "_confirmed_target_frontier_cursor",
    "_kick_with_target_frontier",
    "_observe_target_frontier",
    "_recover_with_target_frontier",
    "install_continuity_target_frontier_repair",
]
