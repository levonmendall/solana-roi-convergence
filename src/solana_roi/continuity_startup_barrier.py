from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any

from . import direct_solana as direct_solana_module
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


POLL_PROVIDER_NAME = "rpc-live-poll"


def _all_target_keys(self: Any) -> set[str]:
    return {fanout._target_key(target) for target in self.watch_targets}


def _transport_covered(provider_targets: dict[str, set[str]]) -> set[str]:
    covered: set[str] = set()
    for rows in provider_targets.values():
        covered.update(rows)
    return covered


def _websocket_covered(provider_targets: dict[str, set[str]]) -> set[str]:
    covered: set[str] = set()
    for provider, rows in provider_targets.items():
        if provider == POLL_PROVIDER_NAME:
            continue
        covered.update(rows)
    return covered


def _minimum_count(all_keys: set[str], provider_targets: dict[str, set[str]], *, include_poll: bool) -> int:
    if not all_keys:
        return 0
    providers = [
        rows
        for provider, rows in provider_targets.items()
        if include_poll or provider != POLL_PROVIDER_NAME
    ]
    return min((sum(1 for rows in providers if key in rows) for key in all_keys), default=0)


def _barrier_snapshot(self: Any) -> dict[str, Any]:
    _lock, provider_targets, _ready_events, _states = fanout._state_maps(self)
    all_keys = _all_target_keys(self)
    transport = _transport_covered(provider_targets) & all_keys
    websocket = _websocket_covered(provider_targets) & all_keys
    poll = set(provider_targets.get(POLL_PROVIDER_NAME, set())) & all_keys
    poll_ready = bool(all_keys) and poll == all_keys
    websocket_ready = bool(all_keys) and websocket == all_keys
    return {
        "target_count": len(all_keys),
        "transport_covered_target_count": len(transport),
        "websocket_covered_target_count": len(websocket),
        "poll_baselined_target_count": len(poll),
        "transport_coverage_ok": bool(all_keys) and transport == all_keys,
        "websocket_coverage_ok": websocket_ready,
        "poll_baseline_ok": poll_ready,
        "ready": bool(poll_ready and websocket_ready),
        "minimum_live_transport_count_per_target": _minimum_count(all_keys, provider_targets, include_poll=True),
        "minimum_live_websocket_provider_count_per_target": _minimum_count(
            all_keys, provider_targets, include_poll=False
        ),
    }


def _wrap_quorum_set_target_state(original: Callable[..., Any]) -> Callable[..., Any]:
    async def set_state(
        self: Any,
        endpoint: RpcEndpoint,
        target: WatchTarget,
        *,
        connected: bool,
        error_type: str | None = None,
        error_code: int | None = None,
        error_message: str | None = None,
    ) -> None:
        lock = getattr(self, "_roi_continuity_startup_barrier_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            setattr(self, "_roi_continuity_startup_barrier_lock", lock)

        async with lock:
            armed = bool(getattr(self, "_roi_continuity_startup_barrier_armed", False))
            if not armed:
                # Full polling coverage may arrive before the real WebSocket union.
                # During that startup phase no transition is allowed to become the
                # first prospective outage boundary. The exact-release reset has
                # already archived any prior gap without relabeling it as live.
                setattr(self, "_roi_global_coverage_observed", False)

            await original(
                self,
                endpoint,
                target,
                connected=connected,
                error_type=error_type,
                error_code=error_code,
                error_message=error_message,
            )

            if armed:
                return

            snapshot = _barrier_snapshot(self)
            if snapshot["ready"]:
                setattr(self, "_roi_continuity_startup_barrier_armed", True)
                setattr(self, "_roi_continuity_startup_barrier_armed_at", direct_solana_module.utcnow().isoformat())
                setattr(self, "_roi_global_coverage_observed", True)
            else:
                # target_quorum may set this when polling alone first spans all
                # targets; keep it false until the startup barrier is truly met.
                setattr(self, "_roi_global_coverage_observed", False)

    try:
        set_state.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(set_state, "_roi_continuity_startup_barrier", True)
    return set_state


def _handshake_status_code(exc: BaseException) -> int | None:
    """Return only a sanitized HTTP status code from a failed WS handshake."""

    for candidate in (getattr(exc, "response", None), exc):
        if candidate is None:
            continue
        for attribute in ("status_code", "status"):
            value = getattr(candidate, attribute, None)
            if isinstance(value, bool):
                continue
            try:
                code = int(value)
            except (TypeError, ValueError):
                continue
            if 100 <= code <= 599:
                return code
    return None


async def _diagnostic_single_target_stream(
    self: Any,
    endpoint: RpcEndpoint,
    target: WatchTarget,
    stop: asyncio.Event,
) -> None:
    """Preserve target isolation while exposing only sanitized handshake codes."""

    backoff = STREAM_RECONNECT_INITIAL_SECONDS
    while not stop.is_set():
        declared = False
        rejection_reported = False
        try:
            async with direct_solana_module.websockets.connect(
                endpoint.ws_url,
                ping_interval=15,
                ping_timeout=15,
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
                        rejection_reported = True
                        raise RuntimeError("logsSubscribe rejected")
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
                        raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                    except asyncio.TimeoutError:
                        pong = await ws.ping()
                        await asyncio.wait_for(pong, timeout=5.0)
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
            if not rejection_reported:
                await target_quorum._quorum_set_target_state(
                    self,
                    endpoint,
                    target,
                    connected=False,
                    error_type=type(exc).__name__,
                    error_code=_handshake_status_code(exc),
                    error_message=None,
                )
            if not stop.is_set():
                await asyncio.sleep(backoff)
                backoff = min(STREAM_RECONNECT_MAX_SECONDS, backoff * 2.0)
        else:
            if declared:
                await target_quorum._quorum_set_target_state(self, endpoint, target, connected=False)


setattr(_diagnostic_single_target_stream, "_roi_target_quorum_stream", True)
setattr(_diagnostic_single_target_stream, "_roi_sanitized_handshake_status", True)


def _status_with_startup_barrier(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        snapshot = _barrier_snapshot(self)
        armed = bool(getattr(self, "_roi_continuity_startup_barrier_armed", False))
        unresolved = bool(payload.get("unresolved_gap", True))
        payload["continuity_ok"] = bool(
            armed and snapshot["transport_coverage_ok"] and not unresolved
        )

        quorum = payload.setdefault("full_scope_target_quorum", {})
        if isinstance(quorum, dict):
            quorum.update(
                {
                    "covered_target_count": snapshot["transport_covered_target_count"],
                    "target_count": snapshot["target_count"],
                    "coverage_ok": bool(armed and snapshot["transport_coverage_ok"]),
                    "transport_coverage_ok": snapshot["transport_coverage_ok"],
                    "websocket_covered_target_count": snapshot["websocket_covered_target_count"],
                    "websocket_coverage_ok": snapshot["websocket_coverage_ok"],
                    "poll_baselined_target_count": snapshot["poll_baselined_target_count"],
                    "poll_baseline_ok": snapshot["poll_baseline_ok"],
                    "startup_barrier_armed": armed,
                    "minimum_live_transport_count_per_target": snapshot[
                        "minimum_live_transport_count_per_target"
                    ],
                    # Backward-compatible field now means real independent WS
                    # providers only; the synthetic poll identity is excluded.
                    "minimum_live_provider_count_per_target": snapshot[
                        "minimum_live_websocket_provider_count_per_target"
                    ],
                    "minimum_live_websocket_provider_count_per_target": snapshot[
                        "minimum_live_websocket_provider_count_per_target"
                    ],
                    "synthetic_poll_counted_as_independent_provider": False,
                    "historical_backfill_can_restore_prospective_continuity": False,
                }
            )

        payload["continuity_startup_barrier"] = {
            "required": True,
            "armed": armed,
            "armed_at": getattr(self, "_roi_continuity_startup_barrier_armed_at", None),
            "requirements": {
                "all_live_poll_targets_baselined": snapshot["poll_baseline_ok"],
                "all_targets_real_websocket_covered": snapshot["websocket_coverage_ok"],
            },
            "startup_transitions_can_create_prospective_gap": False,
            "post_arm_fail_closed_semantics_unchanged": True,
        }

        epoch = payload.get("continuity_epoch")
        if isinstance(epoch, dict):
            epoch["startup_barrier_armed"] = armed
            epoch["startup_barrier_armed_at"] = getattr(
                self, "_roi_continuity_startup_barrier_armed_at", None
            )

        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "continuity_startup_barrier_required": True,
                    "live_poll_baseline_required_before_continuity_arm": True,
                    "real_websocket_coverage_required_before_continuity_arm": True,
                    "synthetic_poll_excluded_from_provider_independence_count": True,
                    "websocket_handshake_http_status_sanitized": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_continuity_startup_barrier", True)
    return status


def install_continuity_startup_barrier() -> None:
    current_setter = target_quorum._quorum_set_target_state
    if not bool(getattr(current_setter, "_roi_continuity_startup_barrier", False)):
        wrapped = _wrap_quorum_set_target_state(current_setter)
        target_quorum._quorum_set_target_state = wrapped  # type: ignore[assignment]
        fanout._set_target_state = wrapped  # type: ignore[assignment]

    # target_quorum installs its target stream into fanout. Replace that final
    # runtime function with an equivalent implementation that preserves only the
    # HTTP status code from failed WebSocket handshakes; endpoint URLs and API keys
    # never enter telemetry.
    fanout._single_target_stream = _diagnostic_single_target_stream  # type: ignore[assignment]
    target_quorum._quorum_single_target_stream = _diagnostic_single_target_stream  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_continuity_startup_barrier", False)):
        DirectSolanaIngestionPlane.status = _status_with_startup_barrier(current_status)  # type: ignore[method-assign]


__all__ = [
    "POLL_PROVIDER_NAME",
    "_barrier_snapshot",
    "_handshake_status_code",
    "install_continuity_startup_barrier",
]
