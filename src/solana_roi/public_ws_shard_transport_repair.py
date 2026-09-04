from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import time
from collections.abc import Callable
from contextlib import suppress
from typing import Any

from . import alchemy_multiplexed_stream as alchemy
from . import direct_solana as direct_solana_module
from . import target_quorum
from . import target_stream_fanout as fanout
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget
from .handshake_pump import MAX_INFLIGHT_NOTIFICATION_HANDLERS, NotificationDispatchBackpressureError
from .solana_rpc import RpcEndpoint
from .stream_resilience import (
    STREAM_RECONNECT_INITIAL_SECONDS,
    STREAM_RECONNECT_MAX_SECONDS,
    STREAM_STABLE_RESET_SECONDS,
    SUBSCRIPTION_RETRIES_PER_TARGET,
    SUBSCRIPTION_RETRY_INITIAL_SECONDS,
    SUBSCRIPTION_RETRY_MAX_SECONDS,
    SubscriptionSetupError,
    _error_parts,
    _retryable_subscription_error,
    _subscription_key,
)


REPAIR_VERSION = "public-ws-shard-transport-v1"
DEFAULT_TARGETS_PER_SOCKET = 4
MIN_TARGETS_PER_SOCKET = 2
MAX_TARGETS_PER_SOCKET = 5
PUBLIC_TOPOLOGY = "sharded-multiplexed-logsSubscribe"

_ORIGINAL_PROVIDER_FANOUT: Callable[[Any, RpcEndpoint, asyncio.Event], Any] | None = None


def _targets_per_socket() -> int:
    raw = os.getenv("SOLANA_ROI_PUBLIC_WS_TARGETS_PER_SOCKET", str(DEFAULT_TARGETS_PER_SOCKET))
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = DEFAULT_TARGETS_PER_SOCKET
    return max(MIN_TARGETS_PER_SOCKET, min(MAX_TARGETS_PER_SOCKET, value))


def _rotated_targets(targets: tuple[WatchTarget, ...], provider: str) -> tuple[WatchTarget, ...]:
    if len(targets) <= 1:
        return targets
    digest = hashlib.sha256(provider.encode("utf-8")).digest()
    shift = int.from_bytes(digest[:2], "big") % len(targets)
    return targets[shift:] + targets[:shift]


def _target_shards(
    targets: tuple[WatchTarget, ...], provider: str, targets_per_socket: int | None = None
) -> tuple[tuple[WatchTarget, ...], ...]:
    size = targets_per_socket or _targets_per_socket()
    ordered = _rotated_targets(targets, provider)
    return tuple(tuple(ordered[index : index + size]) for index in range(0, len(ordered), size))


def _annotate_setup(self: Any, endpoint: RpcEndpoint, *, fallback_count: int = 0) -> None:
    setup = getattr(self, "_roi_subscription_setup", None)
    if not isinstance(setup, dict):
        setup = {}
        setattr(self, "_roi_subscription_setup", setup)
    row = setup.get(endpoint.name)
    if not isinstance(row, dict):
        row = {}
        setup[endpoint.name] = row
    targets = tuple(self.watch_targets)
    shards = _target_shards(targets, endpoint.name)
    row.update(
        {
            "topology": PUBLIC_TOPOLOGY,
            "physical_websocket_count": len(shards) + max(0, int(fallback_count)),
            "planned_shard_websocket_count": len(shards),
            "fallback_single_target_websocket_count": max(0, int(fallback_count)),
            "logs_subscription_count": len(targets),
            "targets_per_shard_socket": _targets_per_socket(),
            "shard_sizes": [len(shard) for shard in shards],
        }
    )


async def _set_target_state(
    self: Any,
    endpoint: RpcEndpoint,
    target: WatchTarget,
    *,
    connected: bool,
    error_type: str | None = None,
    error_code: int | None = None,
    error_message: str | None = None,
) -> None:
    # Dynamic lookup is intentional. poll_recoverability_lease replaces this
    # function later in package composition and must remain the final authority.
    await target_quorum._quorum_set_target_state(
        self,
        endpoint,
        target,
        connected=connected,
        error_type=error_type,
        error_code=error_code,
        error_message=error_message,
    )


async def _mark_disconnected(
    self: Any,
    endpoint: RpcEndpoint,
    targets: tuple[WatchTarget, ...],
    *,
    error_type: str | None,
    error_code: int | None = None,
    error_message: str | None = None,
) -> None:
    for target in targets:
        await _set_target_state(
            self,
            endpoint,
            target,
            connected=False,
            error_type=error_type,
            error_code=error_code,
            error_message=error_message,
        )


async def _cooperative_dispatch_capacity(tasks: set[asyncio.Task[Any]]) -> None:
    if len(tasks) < MAX_INFLIGHT_NOTIFICATION_HANDLERS:
        return
    await asyncio.sleep(0)
    for task in tuple(tasks):
        if task.done():
            tasks.discard(task)
    if len(tasks) >= MAX_INFLIGHT_NOTIFICATION_HANDLERS:
        raise NotificationDispatchBackpressureError(
            "public Solana shard notification handlers exceeded bounded in-flight capacity"
        )


async def _public_shard_stream(
    self: Any,
    endpoint: RpcEndpoint,
    targets: tuple[WatchTarget, ...],
    shard_index: int,
    stop: asyncio.Event,
) -> None:
    reconnect_backoff = STREAM_RECONNECT_INITIAL_SECONDS
    while not stop.is_set():
        acknowledged: dict[int, WatchTarget] = {}
        external_to_internal: dict[str, int] = {}
        reader_task: asyncio.Task[Any] | None = None
        dispatch_failure: asyncio.Future[Any] | None = None
        dispatch_tasks: set[asyncio.Task[Any]] = set()
        ack_waiters: dict[str, asyncio.Future[dict[str, Any]]] = {}
        fallback_tasks: dict[str, asyncio.Task[Any]] = {}
        setup_started = time.monotonic()
        error_code: int | None = None
        try:
            max_queue = fanout.TARGET_WS_MAX_QUEUE * max(1, len(targets))
            async with direct_solana_module.websockets.connect(
                endpoint.ws_url,
                ping_interval=15,
                ping_timeout=15,
                close_timeout=2,
                max_queue=max_queue,
                max_size=fanout.TARGET_WS_MAX_SIZE_BYTES,
                compression=None,
            ) as ws:
                loop = asyncio.get_running_loop()
                dispatch_failure = loop.create_future()

                async def dispatch_notification(message: dict[str, Any]) -> None:
                    params = message.get("params")
                    if not isinstance(params, dict):
                        return
                    try:
                        external = _subscription_key(params.get("subscription"))
                    except Exception:
                        return
                    internal = external_to_internal.get(external)
                    target = acknowledged.get(internal or -1)
                    if internal is None or target is None:
                        return
                    mapped = dict(message)
                    mapped_params = dict(params)
                    mapped_params["subscription"] = internal
                    mapped["params"] = mapped_params
                    await self._handle_notification(endpoint.name, acknowledged, mapped)

                def dispatch_done(task: asyncio.Task[Any]) -> None:
                    dispatch_tasks.discard(task)
                    if task.cancelled():
                        return
                    exc = task.exception()
                    if exc is not None and dispatch_failure is not None and not dispatch_failure.done():
                        dispatch_failure.set_exception(exc)

                async def reader() -> None:
                    while not stop.is_set():
                        raw = await ws.recv()
                        message = json.loads(raw)
                        if not isinstance(message, dict):
                            continue
                        request_id = message.get("id")
                        if request_id is not None:
                            waiter = ack_waiters.get(str(request_id))
                            if waiter is not None and not waiter.done():
                                waiter.set_result(message)
                                continue
                        if message.get("method") != "logsNotification":
                            continue
                        await _cooperative_dispatch_capacity(dispatch_tasks)
                        task = asyncio.create_task(
                            dispatch_notification(message),
                            name=f"public-shard-notification:{endpoint.name}:{shard_index}",
                        )
                        dispatch_tasks.add(task)
                        task.add_done_callback(dispatch_done)

                reader_task = asyncio.create_task(
                    reader(), name=f"public-shard-reader:{endpoint.name}:{shard_index}"
                )

                async def await_ack(waiter: asyncio.Future[dict[str, Any]]) -> dict[str, Any]:
                    assert reader_task is not None
                    wait_set: set[asyncio.Future[Any] | asyncio.Task[Any]] = {waiter, reader_task}
                    if dispatch_failure is not None:
                        wait_set.add(dispatch_failure)
                    done, _pending = await asyncio.wait(
                        wait_set,
                        timeout=fanout.TARGET_ACK_TIMEOUT_SECONDS,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if dispatch_failure is not None and dispatch_failure in done:
                        await dispatch_failure
                    if reader_task in done:
                        await reader_task
                    if waiter not in done:
                        raise TimeoutError("public shard logsSubscribe acknowledgement timed out")
                    return waiter.result()

                request_id = shard_index * 1000
                for internal_id, target in enumerate(targets, start=1):
                    retry_delay = SUBSCRIPTION_RETRY_INITIAL_SECONDS
                    target_accepted = False
                    last_error_type: str | None = None
                    last_error_message: str | None = None
                    for attempt in range(1, SUBSCRIPTION_RETRIES_PER_TARGET + 1):
                        if stop.is_set():
                            break
                        request_id += 1
                        waiter: asyncio.Future[dict[str, Any]] = loop.create_future()
                        ack_waiters[str(request_id)] = waiter
                        await ws.send(
                            json.dumps(
                                {
                                    "jsonrpc": "2.0",
                                    "id": request_id,
                                    "method": "logsSubscribe",
                                    "params": [
                                        {"mentions": [target.address]},
                                        {"commitment": "processed"},
                                    ],
                                }
                            )
                        )
                        try:
                            message = await await_ack(waiter)
                        except TimeoutError as exc:
                            last_error_type = type(exc).__name__
                            last_error_message = str(exc)
                            if attempt < SUBSCRIPTION_RETRIES_PER_TARGET:
                                await asyncio.sleep(retry_delay)
                                retry_delay = min(SUBSCRIPTION_RETRY_MAX_SECONDS, retry_delay * 2.0)
                                continue
                            break
                        finally:
                            ack_waiters.pop(str(request_id), None)

                        error = message.get("error")
                        if error is not None:
                            error_code, provider_message = _error_parts(error)
                            last_error_type = "SubscriptionRejected"
                            last_error_message = provider_message
                            if (
                                attempt < SUBSCRIPTION_RETRIES_PER_TARGET
                                and _retryable_subscription_error(error_code, provider_message)
                            ):
                                await asyncio.sleep(retry_delay)
                                retry_delay = min(SUBSCRIPTION_RETRY_MAX_SECONDS, retry_delay * 2.0)
                                continue
                            break

                        external = _subscription_key(message.get("result"))
                        acknowledged[internal_id] = target
                        external_to_internal[external] = internal_id
                        await _set_target_state(self, endpoint, target, connected=True)
                        target_accepted = True
                        break

                    if stop.is_set():
                        break
                    if not target_accepted:
                        await _set_target_state(
                            self,
                            endpoint,
                            target,
                            connected=False,
                            error_type=last_error_type or "SubscriptionSetupError",
                            error_code=error_code,
                            error_message=last_error_message,
                        )
                        # One rejected subscription must not tear down the accepted
                        # subscriptions sharing this shard. Keep only that target on
                        # the already-proven final single-target fallback path.
                        fallback = asyncio.create_task(
                            fanout._single_target_stream(self, endpoint, target, stop),
                            name=f"public-shard-fallback:{endpoint.name}:{target.kind}:{target.address[:8]}",
                        )
                        fallback_tasks[fanout._target_key(target)] = fallback

                _annotate_setup(self, endpoint, fallback_count=len(fallback_tasks))
                stable_since = time.monotonic()
                stop_task = asyncio.create_task(stop.wait(), name=f"public-shard-stop:{endpoint.name}:{shard_index}")
                wait_set: set[asyncio.Future[Any] | asyncio.Task[Any]] = {stop_task, reader_task}
                if dispatch_failure is not None:
                    wait_set.add(dispatch_failure)
                done, _pending = await asyncio.wait(wait_set, return_when=asyncio.FIRST_COMPLETED)
                if dispatch_failure is not None and dispatch_failure in done:
                    await dispatch_failure
                if reader_task in done:
                    await reader_task
                if time.monotonic() - stable_since >= STREAM_STABLE_RESET_SECONDS:
                    reconnect_backoff = STREAM_RECONNECT_INITIAL_SECONDS
                if not stop_task.done():
                    stop_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await stop_task
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await _mark_disconnected(
                self,
                endpoint,
                tuple(acknowledged.values()),
                error_type=type(exc).__name__,
                error_code=error_code,
                error_message=str(exc),
            )
            if not stop.is_set():
                await asyncio.sleep(reconnect_backoff)
                reconnect_backoff = min(STREAM_RECONNECT_MAX_SECONDS, reconnect_backoff * 2.0)
        else:
            await _mark_disconnected(
                self,
                endpoint,
                tuple(acknowledged.values()),
                error_type=None,
            )
        finally:
            if reader_task is not None and not reader_task.done():
                reader_task.cancel()
            for task in tuple(dispatch_tasks):
                if not task.done():
                    task.cancel()
            for task in fallback_tasks.values():
                if not task.done():
                    task.cancel()
            awaitables: list[asyncio.Task[Any]] = []
            if reader_task is not None:
                awaitables.append(reader_task)
            awaitables.extend(tuple(dispatch_tasks))
            awaitables.extend(fallback_tasks.values())
            for task in awaitables:
                with suppress(asyncio.CancelledError, Exception):
                    await task
            if dispatch_failure is not None:
                if not dispatch_failure.done():
                    dispatch_failure.cancel()
                elif not dispatch_failure.cancelled():
                    with suppress(Exception):
                        dispatch_failure.exception()
            _annotate_setup(self, endpoint, fallback_count=0)


async def _public_sharded_provider_fanout(
    self: Any, endpoint: RpcEndpoint, stop: asyncio.Event
) -> None:
    assert _ORIGINAL_PROVIDER_FANOUT is not None
    if alchemy._is_alchemy_endpoint(endpoint):
        await _ORIGINAL_PROVIDER_FANOUT(self, endpoint, stop)
        return

    targets = tuple(self.watch_targets)
    shards = _target_shards(targets, endpoint.name)
    tasks: list[asyncio.Task[Any]] = []
    setattr(self, "_roi_public_ws_shard_plan", getattr(self, "_roi_public_ws_shard_plan", {}) or {})
    plan = getattr(self, "_roi_public_ws_shard_plan")
    if isinstance(plan, dict):
        plan[endpoint.name] = [[fanout._target_key(target) for target in shard] for shard in shards]
    _annotate_setup(self, endpoint)
    try:
        for index, shard in enumerate(shards):
            tasks.append(
                asyncio.create_task(
                    _public_shard_stream(self, endpoint, shard, index, stop),
                    name=f"direct-solana-public-shard:{endpoint.name}:{index}",
                )
            )
            await asyncio.sleep(fanout.TARGET_START_STAGGER_SECONDS)
        await stop.wait()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


setattr(_public_sharded_provider_fanout, "_roi_public_ws_sharded", True)


def _status_with_public_ws_shards(
    original: Callable[[Any], dict[str, Any]]
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        targets = tuple(self.watch_targets)
        public_endpoints = tuple(endpoint for endpoint in self.endpoints if not alchemy._is_alchemy_endpoint(endpoint))
        target_count = len(targets)
        shard_size = _targets_per_socket()
        planned_per_provider = math.ceil(target_count / shard_size) if target_count else 0
        plans = getattr(self, "_roi_public_ws_shard_plan", {})

        stream = payload.setdefault("target_stream_fanout", {})
        if isinstance(stream, dict):
            providers = stream.get("providers")
            if isinstance(providers, dict):
                for endpoint in public_endpoints:
                    row = providers.get(endpoint.name)
                    if not isinstance(row, dict):
                        continue
                    row.update(
                        {
                            "topology": PUBLIC_TOPOLOGY,
                            "websocket_connection_count": planned_per_provider,
                            "logs_subscription_count": target_count,
                            "target_shards": plans.get(endpoint.name) if isinstance(plans, dict) else None,
                        }
                    )
            stream["public_provider_topology"] = PUBLIC_TOPOLOGY
            stream["public_targets_per_socket"] = shard_size
            stream["public_planned_websocket_connections_per_provider"] = planned_per_provider
            stream["public_physical_connections_planned"] = planned_per_provider * len(public_endpoints)
            stream["public_connection_count_before_repair"] = target_count * len(public_endpoints)

        boundary = payload.setdefault("production_memory_boundary", {})
        if isinstance(boundary, dict):
            per_provider = target_count * fanout.TARGET_WS_MAX_QUEUE * fanout.TARGET_WS_MAX_SIZE_BYTES
            boundary.update(
                {
                    "public_websocket_topology": PUBLIC_TOPOLOGY,
                    "public_targets_per_socket": shard_size,
                    "public_planned_websocket_connections_per_provider": planned_per_provider,
                    "public_shard_max_queue_frames": fanout.TARGET_WS_MAX_QUEUE * shard_size,
                    "receive_payload_ceiling_bytes_per_provider": per_provider,
                    "public_transport_memory_ceiling_increased": False,
                    "strategy_scope_reduced": False,
                }
            )

        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "public_provider_subscription_topology": PUBLIC_TOPOLOGY,
                    "public_target_shards_are_provider_rotated": True,
                    "public_subscription_ack_receive_path": "dedicated-websocket-reader",
                    "public_notification_dispatch_path": "bounded-concurrent-handlers",
                    "public_single_target_fallback_only_on_subscription_failure": True,
                    "public_connection_churn_reduced": True,
                    "full_target_count_unchanged": target_count,
                    "target_quorum_semantics_unchanged": True,
                    "live_poll_recoverability_lease_seconds_unchanged": 12.0,
                    "bounded_poll_delta_pages_unchanged": 3,
                }
            )

        payload["public_ws_shard_transport"] = {
            "repair_version": REPAIR_VERSION,
            "enabled": True,
            "public_provider_count": len(public_endpoints),
            "target_count": target_count,
            "targets_per_socket": shard_size,
            "planned_connections_per_public_provider": planned_per_provider,
            "planned_public_connections_total": planned_per_provider * len(public_endpoints),
            "previous_public_connections_total": target_count * len(public_endpoints),
            "logical_subscriptions_unchanged": target_count * len(public_endpoints),
            "provider_rotated_shards": plans if isinstance(plans, dict) else {},
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
    setattr(status, "_roi_public_ws_sharded", True)
    return status


def install_public_ws_shard_transport_repair() -> None:
    global _ORIGINAL_PROVIDER_FANOUT
    current_provider = fanout._provider_fanout
    if not bool(getattr(current_provider, "_roi_public_ws_sharded", False)):
        _ORIGINAL_PROVIDER_FANOUT = current_provider
        fanout._provider_fanout = _public_sharded_provider_fanout  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_public_ws_sharded", False)):
        DirectSolanaIngestionPlane.status = _status_with_public_ws_shards(current_status)  # type: ignore[method-assign]


__all__ = [
    "DEFAULT_TARGETS_PER_SOCKET",
    "PUBLIC_TOPOLOGY",
    "REPAIR_VERSION",
    "_rotated_targets",
    "_target_shards",
    "_targets_per_socket",
    "install_public_ws_shard_transport_repair",
]
