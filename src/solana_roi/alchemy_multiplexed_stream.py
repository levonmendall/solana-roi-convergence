from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import urlsplit

from . import direct_solana as direct_solana_module
from . import target_quorum
from . import target_stream_fanout as fanout
from .continuity_startup_barrier import _handshake_status_code
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget
from .solana_rpc import RpcEndpoint
from .stream_resilience import (
    STREAM_RECONNECT_INITIAL_SECONDS,
    STREAM_RECONNECT_MAX_SECONDS,
    STREAM_STABLE_RESET_SECONDS,
    SUBSCRIPTION_INTER_TARGET_DELAY_SECONDS,
    SUBSCRIPTION_RETRIES_PER_TARGET,
    SUBSCRIPTION_RETRY_INITIAL_SECONDS,
    SUBSCRIPTION_RETRY_MAX_SECONDS,
    SubscriptionSetupError,
    _error_parts,
    _retryable_subscription_error,
    _subscription_key,
)


ALCHEMY_PROVIDER_NAME = "alchemy"
ALCHEMY_TOPOLOGY = "single-websocket-multiplexed-logsSubscribe"
ISOLATED_TOPOLOGY = "one-target-per-websocket"


def _host(url: str) -> str:
    try:
        return str(urlsplit(url).hostname or "").lower()
    except Exception:
        return ""


def _is_alchemy_endpoint(endpoint: RpcEndpoint) -> bool:
    return bool(
        str(endpoint.name).strip().lower() == ALCHEMY_PROVIDER_NAME
        or _host(endpoint.http_url).endswith(".alchemy.com")
        or _host(endpoint.ws_url).endswith(".alchemy.com")
    )


def _alchemy_max_queue(target_count: int) -> int:
    # One physical Alchemy socket replaces N isolated target sockets. Preserve the
    # existing aggregate per-provider queue ceiling exactly: N * 8 frames at 1 MiB.
    return max(1, int(target_count)) * fanout.TARGET_WS_MAX_QUEUE


def _annotate_setup(self: Any, endpoint: RpcEndpoint, *, setup_seconds: float | None = None) -> None:
    setup = getattr(self, "_roi_subscription_setup", None)
    if not isinstance(setup, dict):
        setup = {}
        setattr(self, "_roi_subscription_setup", setup)
    row = setup.get(endpoint.name)
    if not isinstance(row, dict):
        row = {}
        setup[endpoint.name] = row
    row["topology"] = ALCHEMY_TOPOLOGY
    row["physical_websocket_count"] = 1
    row["logs_subscription_count"] = len(tuple(self.watch_targets))
    if setup_seconds is not None:
        row["setup_seconds"] = max(0.0, float(setup_seconds))


async def _set_target_state(
    self: Any,
    endpoint: RpcEndpoint,
    target: WatchTarget,
    *,
    connected: bool,
    error_type: str | None = None,
    error_code: int | None = None,
) -> None:
    await target_quorum._quorum_set_target_state(
        self,
        endpoint,
        target,
        connected=connected,
        error_type=error_type,
        error_code=error_code,
        error_message=None,
    )
    _annotate_setup(self, endpoint)


async def _disconnect_targets(
    self: Any,
    endpoint: RpcEndpoint,
    targets: tuple[WatchTarget, ...],
    *,
    error_type: str | None,
    error_code: int | None,
) -> None:
    for target in targets:
        await _set_target_state(
            self,
            endpoint,
            target,
            connected=False,
            error_type=error_type,
            error_code=error_code,
        )


async def _alchemy_multiplexed_stream(self: Any, endpoint: RpcEndpoint, stop: asyncio.Event) -> None:
    """Run all frozen Alchemy targets over one bounded WebSocket.

    Alchemy production telemetry proved that opening ten physical WebSockets at once
    can trigger HTTP 429 responses. Solana PubSub supports multiple logsSubscribe
    requests on one connection, so Alchemy uses one connection with sequential
    acknowledgement while the two public providers retain target-isolated sockets.

    Target-level state is still published immediately after each successful ACK,
    so the existing union quorum, startup barrier, 12-second recoverability lease,
    and fail-closed gap semantics remain authoritative and unchanged.
    """

    targets = tuple(self.watch_targets)
    reconnect_backoff = STREAM_RECONNECT_INITIAL_SECONDS

    while not stop.is_set():
        setup_started = time.monotonic()
        disconnect_error_type: str | None = None
        disconnect_error_code: int | None = None
        try:
            async with direct_solana_module.websockets.connect(
                endpoint.ws_url,
                ping_interval=15,
                ping_timeout=15,
                close_timeout=2,
                max_queue=_alchemy_max_queue(len(targets)),
                max_size=fanout.TARGET_WS_MAX_SIZE_BYTES,
                compression=None,
            ) as ws:
                subscription_targets: dict[int, WatchTarget] = {}
                external_to_internal: dict[str, int] = {}

                async def dispatch(message: dict[str, Any]) -> None:
                    if message.get("method") != "logsNotification":
                        return
                    params = message.get("params")
                    if not isinstance(params, dict):
                        return
                    try:
                        external = _subscription_key(params.get("subscription"))
                    except ValueError:
                        return
                    internal = external_to_internal.get(external)
                    if internal is None:
                        return
                    mapped = dict(message)
                    mapped_params = dict(params)
                    mapped_params["subscription"] = internal
                    mapped["params"] = mapped_params
                    await self._handle_notification(endpoint.name, subscription_targets, mapped)

                request_id = 0
                for internal_id, target in enumerate(targets, start=1):
                    retry_delay = SUBSCRIPTION_RETRY_INITIAL_SECONDS
                    acknowledged = False
                    for attempt in range(1, SUBSCRIPTION_RETRIES_PER_TARGET + 1):
                        if stop.is_set():
                            break
                        request_id += 1
                        current_id = request_id
                        await ws.send(
                            json.dumps(
                                {
                                    "jsonrpc": "2.0",
                                    "id": current_id,
                                    "method": "logsSubscribe",
                                    "params": [
                                        {"mentions": [target.address]},
                                        {"commitment": "processed"},
                                    ],
                                }
                            )
                        )
                        retry_this_target = False
                        deadline = asyncio.get_running_loop().time() + fanout.TARGET_ACK_TIMEOUT_SECONDS
                        while not stop.is_set() and asyncio.get_running_loop().time() < deadline:
                            remaining = max(0.05, deadline - asyncio.get_running_loop().time())
                            raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                            message = json.loads(raw)
                            if not isinstance(message, dict):
                                continue
                            if message.get("id") not in (current_id, str(current_id)):
                                await dispatch(message)
                                continue
                            error = message.get("error")
                            if error is not None:
                                code, provider_message = _error_parts(error)
                                await _set_target_state(
                                    self,
                                    endpoint,
                                    target,
                                    connected=False,
                                    error_type="SubscriptionRejected",
                                    error_code=code,
                                )
                                if (
                                    attempt < SUBSCRIPTION_RETRIES_PER_TARGET
                                    and _retryable_subscription_error(code, provider_message)
                                ):
                                    await asyncio.sleep(retry_delay)
                                    retry_delay = min(SUBSCRIPTION_RETRY_MAX_SECONDS, retry_delay * 2.0)
                                    retry_this_target = True
                                    break
                                raise SubscriptionSetupError(
                                    code=code,
                                    message=provider_message,
                                    target=target.address,
                                )
                            external_key = _subscription_key(message.get("result"))
                            external_to_internal[external_key] = internal_id
                            subscription_targets[internal_id] = target
                            await _set_target_state(self, endpoint, target, connected=True)
                            acknowledged = True
                            break

                        if acknowledged or stop.is_set():
                            break
                        if retry_this_target:
                            continue
                        raise TimeoutError("multiplexed Alchemy logsSubscribe acknowledgement timed out")

                    if stop.is_set():
                        break
                    if not acknowledged:
                        raise SubscriptionSetupError(
                            code=None,
                            message="subscription acknowledgement retry budget exhausted",
                            target=target.address,
                        )
                    await asyncio.sleep(SUBSCRIPTION_INTER_TARGET_DELAY_SECONDS)

                if stop.is_set():
                    continue
                if len(subscription_targets) != len(targets):
                    raise RuntimeError("Alchemy multiplexed subscription set incomplete")

                _annotate_setup(self, endpoint, setup_seconds=time.monotonic() - setup_started)
                stable_since = time.monotonic()
                while not stop.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                    except asyncio.TimeoutError:
                        pong = await ws.ping()
                        await asyncio.wait_for(pong, timeout=5.0)
                        continue
                    message = json.loads(raw)
                    if isinstance(message, dict):
                        await dispatch(message)
                    if time.monotonic() - stable_since >= STREAM_STABLE_RESET_SECONDS:
                        reconnect_backoff = STREAM_RECONNECT_INITIAL_SECONDS
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            disconnect_error_type = type(exc).__name__
            if isinstance(exc, SubscriptionSetupError):
                disconnect_error_code = exc.code
            else:
                disconnect_error_code = _handshake_status_code(exc)
            await _disconnect_targets(
                self,
                endpoint,
                targets,
                error_type=disconnect_error_type,
                error_code=disconnect_error_code,
            )
            if not stop.is_set():
                await asyncio.sleep(reconnect_backoff)
                reconnect_backoff = min(STREAM_RECONNECT_MAX_SECONDS, reconnect_backoff * 2.0)
        else:
            await _disconnect_targets(
                self,
                endpoint,
                targets,
                error_type=None,
                error_code=None,
            )


setattr(_alchemy_multiplexed_stream, "_roi_alchemy_multiplexed", True)


_ORIGINAL_PROVIDER_FANOUT = fanout._provider_fanout


async def _provider_specific_fanout(self: Any, endpoint: RpcEndpoint, stop: asyncio.Event) -> None:
    if _is_alchemy_endpoint(endpoint):
        await _alchemy_multiplexed_stream(self, endpoint, stop)
        return
    await _ORIGINAL_PROVIDER_FANOUT(self, endpoint, stop)


setattr(_provider_specific_fanout, "_roi_alchemy_provider_specific", True)


def _status_with_alchemy_multiplexing(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        targets = tuple(self.watch_targets)
        endpoints = tuple(self.endpoints)
        target_count = len(targets)
        alchemy_count = sum(1 for endpoint in endpoints if _is_alchemy_endpoint(endpoint))
        isolated_count = len(endpoints) - alchemy_count
        physical_connections = isolated_count * target_count + alchemy_count
        logical_subscriptions = len(endpoints) * target_count
        per_provider_ceiling = target_count * fanout.TARGET_WS_MAX_QUEUE * fanout.TARGET_WS_MAX_SIZE_BYTES

        stream = payload.setdefault("target_stream_fanout", {})
        if isinstance(stream, dict):
            stream.update(
                {
                    "enabled": True,
                    "provider_specific_topology": True,
                    "total_websocket_connections": physical_connections,
                    "total_logs_subscriptions": logical_subscriptions,
                    # Preserve this legacy field as logical target streams; physical
                    # connection count is now reported separately above.
                    "total_websocket_target_streams": logical_subscriptions,
                }
            )
            providers = stream.get("providers")
            if isinstance(providers, dict):
                for endpoint in endpoints:
                    row = providers.get(endpoint.name)
                    if not isinstance(row, dict):
                        continue
                    multiplexed = _is_alchemy_endpoint(endpoint)
                    row["topology"] = ALCHEMY_TOPOLOGY if multiplexed else ISOLATED_TOPOLOGY
                    row["websocket_connection_count"] = 1 if multiplexed else target_count
                    row["logs_subscription_count"] = target_count

        setup = payload.get("subscription_setup")
        if isinstance(setup, dict):
            for endpoint in endpoints:
                if not _is_alchemy_endpoint(endpoint):
                    continue
                row = setup.get(endpoint.name)
                if isinstance(row, dict):
                    row["topology"] = ALCHEMY_TOPOLOGY
                    row["physical_websocket_count"] = 1
                    row["logs_subscription_count"] = target_count

        boundary = payload.setdefault("production_memory_boundary", {})
        if isinstance(boundary, dict):
            boundary.update(
                {
                    "websocket_topology": "provider-specific-public-isolated-alchemy-multiplexed",
                    "physical_websocket_connection_count": physical_connections,
                    "logical_logs_subscription_count": logical_subscriptions,
                    "alchemy_websocket_max_queue": _alchemy_max_queue(target_count),
                    "alchemy_websocket_max_size_bytes": fanout.TARGET_WS_MAX_SIZE_BYTES,
                    "receive_payload_ceiling_bytes_per_provider": per_provider_ceiling,
                    "receive_payload_ceiling_bytes_all_providers": per_provider_ceiling * len(endpoints),
                    "memory_ceiling_increased_by_alchemy_multiplexing": False,
                }
            )

        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "subscription_topology": "provider-specific",
                    "public_provider_subscription_topology": ISOLATED_TOPOLOGY,
                    "alchemy_subscription_topology": ALCHEMY_TOPOLOGY,
                    "alchemy_physical_websocket_count": 1 if alchemy_count else 0,
                    "alchemy_logs_subscriptions_per_websocket": target_count if alchemy_count else 0,
                    "alchemy_sequential_subscription_ack": True,
                    "alchemy_connection_429_pressure_reduced": True,
                    "alchemy_target_quorum_semantics_unchanged": True,
                    "live_poll_recoverability_lease_seconds_unchanged": 12.0,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_alchemy_multiplexed", True)
    return status


def install_alchemy_multiplexed_stream() -> None:
    current_provider_fanout = fanout._provider_fanout
    if not bool(getattr(current_provider_fanout, "_roi_alchemy_provider_specific", False)):
        fanout._provider_fanout = _provider_specific_fanout  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_alchemy_multiplexed", False)):
        DirectSolanaIngestionPlane.status = _status_with_alchemy_multiplexing(current_status)  # type: ignore[method-assign]


__all__ = [
    "ALCHEMY_PROVIDER_NAME",
    "ALCHEMY_TOPOLOGY",
    "_alchemy_max_queue",
    "_alchemy_multiplexed_stream",
    "_is_alchemy_endpoint",
    "install_alchemy_multiplexed_stream",
]
