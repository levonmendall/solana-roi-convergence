from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from typing import Any

from . import continuity_immediate_recovery_repair as immediate
from . import continuity_recovery_isolation_repair as isolation
from . import continuity_target_frontier_repair as frontier
from . import direct_solana as direct_solana_module
from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from . import target_quorum
from . import target_stream_fanout as fanout
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget
from .solana_rpc import RpcEndpoint
from .stream_resilience import (
    STREAM_RECONNECT_INITIAL_SECONDS,
    STREAM_RECONNECT_MAX_SECONDS,
    _error_parts,
    _subscription_key,
)


# Transport-liveness bounds only. They do not change the fixed recoverability
# lease, the 3x1000 delta limit, strategy scope, certification thresholds, or any
# execution authority. Pump AMM can exceed 3000 signatures while a half-open
# socket waits through the former 15s ping + 15s timeout, so loss must be detected
# before the immutable recovery window is already exhausted.
TARGET_WS_PING_INTERVAL_SECONDS = 3.0
TARGET_WS_PING_TIMEOUT_SECONDS = 2.0
TARGET_WS_RECEIVE_PROBE_SECONDS = 8.0
TARGET_WS_PROBE_TIMEOUT_SECONDS = 2.0


def _increment(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_early_loss_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


def _snapshot_target_frontier(self: Any, target: WatchTarget) -> list[dict[str, Any]]:
    """Freeze exact target WebSocket candidates at the zero-coverage transition."""

    rows = [dict(row) for row in list(frontier._target_history(self, target))]
    _increment(self, "frontier_snapshots")
    setattr(
        self,
        "_roi_early_loss_last_frontier_snapshot",
        {
            "target": live_poll._poll_target_key(target),
            "candidate_count": len(rows),
            "highest_slot": max((int(row.get("slot") or 0) for row in rows), default=0),
            "captured_monotonic": time.monotonic(),
        },
    )
    return rows


def _confirmation_rows(result: Any) -> list[Any]:
    value = result.get("value") if isinstance(result, dict) else None
    return list(value) if isinstance(value, list) else []


async def _confirmed_snapshot_cursor(
    self: Any,
    target: WatchTarget,
    routine_cursor_slot: int,
    generation: int,
    candidates: list[dict[str, Any]],
) -> tuple[int, dict[str, Any] | None]:
    """Confirm the at-gap frontier snapshot and replay its complete slot.

    Processed WebSocket evidence is never trusted by itself. Recent exact-target
    signatures captured synchronously when coverage reaches zero are batch-checked
    at confirmed/finalized commitment. A proven slot becomes ``slot - 1`` so every
    same-slot transaction is intentionally replayed by the existing recovery path.
    """

    candidates = [dict(row) for row in candidates]
    if not candidates:
        frontier._increment(self, "anchor_fallbacks")
        return int(routine_cursor_slot), None

    candidates.sort(
        key=lambda row: (
            int(row.get("slot") or 0),
            float(row.get("observed_monotonic") or 0.0),
        ),
        reverse=True,
    )
    candidates = candidates[: frontier.TARGET_FRONTIER_HISTORY]
    signatures = [str(row.get("signature") or "") for row in candidates]
    deadline = frontier._gap_deadline(self, target)
    last_provider: str | None = None
    last_latency: float | None = None

    for attempt in range(frontier.FRONTIER_CONFIRMATION_ATTEMPTS):
        if immediate._generation(self, target) != int(generation):
            raise RuntimeError("real websocket gap generation superseded")
        if attempt > 0 and time.monotonic() >= deadline:
            break
        frontier._increment(self, "confirmation_attempts")
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
                effective_cursor = max(int(routine_cursor_slot), confirmed_slot - 1)
                anchor = {
                    "source": "confirmed-target-websocket-frontier-at-gap",
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
                    "snapshot_candidate_count": len(candidates),
                    "captured_at_zero_websocket_coverage": True,
                }
                setattr(self, "_roi_target_frontier_last_anchor", anchor)
                frontier._increment(self, "confirmed_anchors")
                _increment(self, "confirmed_gap_snapshots")
                return effective_cursor, anchor
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            setattr(self, "_roi_target_frontier_last_confirmation_error", type(exc).__name__)
            frontier._increment(self, "confirmation_errors")

        if attempt + 1 < frontier.FRONTIER_CONFIRMATION_ATTEMPTS:
            remaining = deadline - time.monotonic()
            if remaining > 0.0:
                await asyncio.sleep(min(frontier.FRONTIER_CONFIRMATION_RETRY_SECONDS, remaining))

    frontier._increment(self, "anchor_fallbacks")
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
            "snapshot_candidate_count": len(candidates),
            "captured_at_zero_websocket_coverage": True,
        },
    )
    return int(routine_cursor_slot), None


def _annotate_attribution_row(
    self: Any,
    *,
    succeeded: bool,
    anchor: dict[str, Any] | None,
    routine_cursor_slot: int,
) -> None:
    """Annotate the current result, never a stale success from an earlier gap."""

    state = isolation._attribution_state(self)
    key = "last_success" if succeeded else "last_failure"
    row = state.get(key)
    if not isinstance(row, dict):
        return
    row["routine_cursor_slot"] = int(routine_cursor_slot)
    row["recovery_anchor_source"] = (
        str(anchor.get("source")) if isinstance(anchor, dict) else "routine-poll-slot-fallback"
    )
    row["frontier_snapshot_at_gap_onset"] = True
    if isinstance(anchor, dict):
        row["confirmed_frontier_slot"] = int(anchor.get("confirmed_frontier_slot") or 0)
        row["same_slot_replay_required"] = True
        row["snapshot_candidate_count"] = int(anchor.get("snapshot_candidate_count") or 0)


async def _recover_from_gap_snapshot(
    self: Any,
    target: WatchTarget,
    routine_cursor_slot: int,
    generation: int,
    candidates: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    effective_cursor, anchor = await _confirmed_snapshot_cursor(
        self,
        target,
        routine_cursor_slot,
        generation,
        candidates,
    )
    try:
        result = await isolation._recover_with_isolated_rpc(
            self,
            target,
            effective_cursor,
            generation,
        )
    except BaseException:
        _annotate_attribution_row(
            self,
            succeeded=False,
            anchor=anchor,
            routine_cursor_slot=routine_cursor_slot,
        )
        raise
    _annotate_attribution_row(
        self,
        succeeded=True,
        anchor=anchor,
        routine_cursor_slot=routine_cursor_slot,
    )
    return result


def _kick_recovery_with_gap_snapshot(self: Any, target: WatchTarget, generation: int) -> None:
    """Start critical recovery from the exact frontier visible at gap onset."""

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

    candidates = _snapshot_target_frontier(self, target)
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

    # rpc_workload_governor reserves endpoint capacity for this established prefix.
    # PR #84's target-frontier wrapper used a new task name and unintentionally
    # lost that critical classification; restore it without increasing capacity.
    task = asyncio.create_task(
        _recover_from_gap_snapshot(
            self,
            target,
            routine_cursor_slot,
            generation,
            candidates,
        ),
        name=f"isolated-immediate-gap-recovery:{target.kind}:{target.address[:8]}:frontier",
    )
    tasks[key] = {
        "generation": int(generation),
        "cursor_slot": routine_cursor_slot,
        "task": task,
        "started_monotonic": time.monotonic(),
        "frontier_snapshot_count": len(candidates),
    }
    immediate._increment(self, "kicked")
    _increment(self, "critical_recovery_kicks")


async def _early_loss_quorum_single_target_stream(
    self: Any,
    endpoint: RpcEndpoint,
    target: WatchTarget,
    stop: asyncio.Event,
) -> None:
    """Detect dead target transports before the fixed recovery delta can overflow."""

    backoff = STREAM_RECONNECT_INITIAL_SECONDS
    while not stop.is_set():
        declared = False
        try:
            async with direct_solana_module.websockets.connect(
                endpoint.ws_url,
                ping_interval=TARGET_WS_PING_INTERVAL_SECONDS,
                ping_timeout=TARGET_WS_PING_TIMEOUT_SECONDS,
                close_timeout=2,
                max_queue=fanout.TARGET_WS_MAX_QUEUE,
                max_size=fanout.TARGET_WS_MAX_SIZE_BYTES,
                compression=None,
            ) as ws:
                await ws.send(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "logsSubscribe",
                            "params": [{"mentions": [target.address]}, {"commitment": "processed"}],
                        }
                    )
                )
                deadline = asyncio.get_running_loop().time() + fanout.TARGET_ACK_TIMEOUT_SECONDS
                external_subscription: str | None = None
                while not stop.is_set() and asyncio.get_running_loop().time() < deadline:
                    remaining = max(0.05, deadline - asyncio.get_running_loop().time())
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                    message = json.loads(raw)
                    if not isinstance(message, dict) or message.get("id") not in (1, "1"):
                        continue
                    if message.get("error") is not None:
                        code, provider_message = _error_parts(message.get("error"))
                        await target_quorum._quorum_set_target_state(
                            self,
                            endpoint,
                            target,
                            connected=False,
                            error_type="SubscriptionRejected",
                            error_code=code,
                            error_message=provider_message,
                        )
                        raise RuntimeError(f"logsSubscribe rejected code={code}: {provider_message}")
                    external_subscription = _subscription_key(message.get("result"))
                    break
                if not external_subscription:
                    raise TimeoutError("single-target Solana logsSubscribe acknowledgement timed out")

                await target_quorum._quorum_set_target_state(self, endpoint, target, connected=True)
                declared = True
                backoff = STREAM_RECONNECT_INITIAL_SECONDS
                subscription_targets = {1: target}

                while not stop.is_set():
                    try:
                        raw = await asyncio.wait_for(
                            ws.recv(),
                            timeout=TARGET_WS_RECEIVE_PROBE_SECONDS,
                        )
                    except asyncio.TimeoutError:
                        _increment(self, "explicit_transport_probes")
                        pong = await ws.ping()
                        await asyncio.wait_for(pong, timeout=TARGET_WS_PROBE_TIMEOUT_SECONDS)
                        continue
                    message = json.loads(raw)
                    if not isinstance(message, dict) or message.get("method") != "logsNotification":
                        continue
                    params = message.get("params")
                    if not isinstance(params, dict):
                        continue
                    try:
                        if _subscription_key(params.get("subscription")) != external_subscription:
                            continue
                    except Exception:
                        continue
                    mapped = dict(message)
                    mapped_params = dict(params)
                    mapped_params["subscription"] = 1
                    mapped["params"] = mapped_params
                    await self._handle_notification(endpoint.name, subscription_targets, mapped)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _increment(self, "transport_disconnects")
            await target_quorum._quorum_set_target_state(
                self,
                endpoint,
                target,
                connected=False,
                error_type=type(exc).__name__,
            )
            if not stop.is_set():
                await asyncio.sleep(backoff)
                backoff = min(STREAM_RECONNECT_MAX_SECONDS, backoff * 2.0)
        else:
            if declared:
                await target_quorum._quorum_set_target_state(
                    self,
                    endpoint,
                    target,
                    connected=False,
                )


try:
    _early_loss_quorum_single_target_stream.__dict__.update(
        getattr(target_quorum._quorum_single_target_stream, "__dict__", {})
    )
except Exception:
    pass
setattr(_early_loss_quorum_single_target_stream, "_roi_target_quorum_stream", True)
setattr(_early_loss_quorum_single_target_stream, "_roi_early_loss_detection", True)


def _status_with_early_loss_detection(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        poll = payload.get("live_poll_redundancy")
        if isinstance(poll, dict):
            poll.update(
                {
                    "target_transport_early_loss_detection": True,
                    "target_ws_ping_interval_seconds": TARGET_WS_PING_INTERVAL_SECONDS,
                    "target_ws_ping_timeout_seconds": TARGET_WS_PING_TIMEOUT_SECONDS,
                    "target_ws_receive_probe_seconds": TARGET_WS_RECEIVE_PROBE_SECONDS,
                    "target_ws_probe_timeout_seconds": TARGET_WS_PROBE_TIMEOUT_SECONDS,
                    "target_ws_keepalive_failure_bound_seconds": (
                        TARGET_WS_PING_INTERVAL_SECONDS + TARGET_WS_PING_TIMEOUT_SECONDS
                    ),
                    "frontier_snapshot_at_zero_websocket_coverage": True,
                    "frontier_snapshot_count": int(
                        getattr(self, "_roi_early_loss_frontier_snapshots", 0) or 0
                    ),
                    "confirmed_gap_snapshot_count": int(
                        getattr(self, "_roi_early_loss_confirmed_gap_snapshots", 0) or 0
                    ),
                    "critical_recovery_kicks": int(
                        getattr(self, "_roi_early_loss_critical_recovery_kicks", 0) or 0
                    ),
                    "transport_disconnects_detected": int(
                        getattr(self, "_roi_early_loss_transport_disconnects", 0) or 0
                    ),
                    "explicit_transport_probes": int(
                        getattr(self, "_roi_early_loss_explicit_transport_probes", 0) or 0
                    ),
                    "last_frontier_snapshot": getattr(
                        self, "_roi_early_loss_last_frontier_snapshot", None
                    ),
                    "real_gap_recovery_task_uses_critical_rpc_reservation": True,
                    "real_gap_recovery_lease_seconds": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
                    "real_gap_recovery_hard_delta_bound_unchanged": True,
                }
            )
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "target_transport_failure_detection_hardened": True,
                    "target_transport_keepalive_only_not_application_idle_timeout": True,
                    "real_gap_frontier_snapshotted_at_zero_coverage": True,
                    "real_gap_recovery_critical_rpc_priority_restored": True,
                    "live_poll_recoverability_lease_seconds": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
                    "live_poll_hard_delta_bound_unchanged": True,
                    "full_target_count_unchanged": len(tuple(self.watch_targets)),
                    "certification_thresholds_unchanged": True,
                    "paper_only_authority_unchanged": True,
                    "signing_or_submission_available": False,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_early_loss_detection", True)
    return status


def install_continuity_early_loss_detection_repair() -> None:
    """Install early transport loss detection after the target-frontier repair."""

    # fanout._provider_fanout resolves this global when runtime tasks start. Patch
    # the active function after all older quorum/stream installers have composed.
    target_quorum._quorum_single_target_stream = _early_loss_quorum_single_target_stream  # type: ignore[assignment]
    fanout._single_target_stream = _early_loss_quorum_single_target_stream  # type: ignore[assignment]

    # The tracked quorum state setter resolves this global at the exact transition
    # to zero real-WebSocket coverage. Snapshot synchronously before reconnect
    # traffic can rotate the small target-frontier history.
    immediate._kick_immediate_recovery = _kick_recovery_with_gap_snapshot  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_early_loss_detection", False)):
        DirectSolanaIngestionPlane.status = _status_with_early_loss_detection(current_status)  # type: ignore[method-assign]


__all__ = [
    "TARGET_WS_PING_INTERVAL_SECONDS",
    "TARGET_WS_PING_TIMEOUT_SECONDS",
    "TARGET_WS_RECEIVE_PROBE_SECONDS",
    "TARGET_WS_PROBE_TIMEOUT_SECONDS",
    "_annotate_attribution_row",
    "_confirmed_snapshot_cursor",
    "_early_loss_quorum_single_target_stream",
    "_kick_recovery_with_gap_snapshot",
    "_recover_from_gap_snapshot",
    "_snapshot_target_frontier",
    "install_continuity_early_loss_detection_repair",
]
