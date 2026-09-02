from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from contextlib import suppress
from typing import Any

from . import alchemy_multiplexed_stream as multiplex
from . import direct_solana as direct_solana_module
from .continuity_startup_barrier import _handshake_status_code
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget
from .handshake_pump import (
    MAX_INFLIGHT_NOTIFICATION_HANDLERS,
    NotificationDispatchBackpressureError,
    _request_id_key,
)
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


def _setup_state(self: Any) -> dict[str, Any]:
    state = getattr(self, "_roi_subscription_setup", None)
    if not isinstance(state, dict):
        state = {}
        setattr(self, "_roi_subscription_setup", state)
    return state


def _record_setup(
    self: Any,
    endpoint: RpcEndpoint,
    *,
    phase: str,
    target_count: int,
    acknowledged_count: int,
    setup_started: float,
    current_target: WatchTarget | None = None,
    attempt: int | None = None,
    error_type: str | None = None,
    error_code: int | None = None,
) -> None:
    state = _setup_state(self)
    state[endpoint.name] = {
        "ready": phase == "live",
        "phase": phase,
        "target_count": int(target_count),
        "acknowledged_count": int(acknowledged_count),
        "current_target": getattr(current_target, "address", None),
        "current_target_kind": getattr(current_target, "kind", None),
        "attempt": attempt,
        "error_code": error_code,
        "error_message": None,
        "error_type": error_type,
        "topology": multiplex.ALCHEMY_TOPOLOGY,
        "physical_websocket_count": 1,
        "logs_subscription_count": int(target_count),
        "ack_receive_path": "dedicated-websocket-reader",
        "notification_dispatch_path": "bounded-concurrent-handlers",
        "max_inflight_notification_handlers": MAX_INFLIGHT_NOTIFICATION_HANDLERS,
        "setup_seconds": max(0.0, time.monotonic() - setup_started),
    }


async def _pumped_alchemy_multiplexed_stream(
    self: Any,
    endpoint: RpcEndpoint,
    stop: asyncio.Event,
) -> None:
    """Run one Alchemy socket with ACK handling isolated from live notifications.

    The previous multiplexed implementation read acknowledgements and dispatched
    notifications in the same inline loop. Under high-volume Solana traffic, a
    notification handler could therefore delay the remaining subscription ACKs
    until their setup timeout expired. This implementation gives one dedicated
    reader exclusive ownership of ``ws.recv()``. ACK responses resolve futures
    immediately, while notifications are dispatched through a separately bounded
    task set. Target quorum updates, the startup barrier, polling lease, and all
    fail-closed continuity semantics remain unchanged.
    """

    targets = tuple(self.watch_targets)
    reconnect_backoff = STREAM_RECONNECT_INITIAL_SECONDS

    while not stop.is_set():
        setup_started = time.monotonic()
        acknowledged_count = 0
        current_target: WatchTarget | None = None
        current_attempt: int | None = None
        reader_task: asyncio.Task[Any] | None = None
        stop_task: asyncio.Task[Any] | None = None
        dispatch_failure: asyncio.Future[Any] | None = None
        dispatch_tasks: set[asyncio.Task[Any]] = set()

        _record_setup(
            self,
            endpoint,
            phase="connecting",
            target_count=len(targets),
            acknowledged_count=0,
            setup_started=setup_started,
        )

        try:
            async with direct_solana_module.websockets.connect(
                endpoint.ws_url,
                ping_interval=15,
                ping_timeout=15,
                close_timeout=2,
                max_queue=multiplex._alchemy_max_queue(len(targets)),
                max_size=multiplex.fanout.TARGET_WS_MAX_SIZE_BYTES,
                compression=None,
            ) as ws:
                loop = asyncio.get_running_loop()
                dispatch_failure = loop.create_future()
                ack_waiters: dict[str, asyncio.Future[dict[str, Any]]] = {}
                request_targets: dict[str, tuple[int, WatchTarget]] = {}
                subscription_targets: dict[int, WatchTarget] = {}
                external_to_internal: dict[str, int] = {}

                async def dispatch_notification(message: dict[str, Any]) -> None:
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

                        raw_request_id = message.get("id")
                        if raw_request_id is not None:
                            try:
                                request_key = _request_id_key(raw_request_id)
                            except Exception:
                                request_key = ""
                            waiter = ack_waiters.get(request_key)
                            if waiter is not None and not waiter.done():
                                meta = request_targets.get(request_key)
                                if message.get("error") is None and meta is not None:
                                    external = _subscription_key(message.get("result"))
                                    internal_id, target = meta
                                    external_to_internal[external] = internal_id
                                    subscription_targets[internal_id] = target
                                waiter.set_result(message)
                                continue

                        if message.get("method") != "logsNotification":
                            continue
                        if len(dispatch_tasks) >= MAX_INFLIGHT_NOTIFICATION_HANDLERS:
                            raise NotificationDispatchBackpressureError(
                                "Alchemy notification handlers exceeded bounded in-flight capacity"
                            )
                        task = asyncio.create_task(
                            dispatch_notification(message),
                            name=f"alchemy-notification:{endpoint.name}",
                        )
                        dispatch_tasks.add(task)
                        task.add_done_callback(dispatch_done)

                reader_task = asyncio.create_task(reader(), name=f"alchemy-reader:{endpoint.name}")

                async def await_ack(waiter: asyncio.Future[dict[str, Any]]) -> dict[str, Any]:
                    assert reader_task is not None
                    wait_set: set[asyncio.Future[Any] | asyncio.Task[Any]] = {waiter, reader_task}
                    if dispatch_failure is not None:
                        wait_set.add(dispatch_failure)
                    done, _pending = await asyncio.wait(
                        wait_set,
                        timeout=multiplex.fanout.TARGET_ACK_TIMEOUT_SECONDS,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if dispatch_failure is not None and dispatch_failure in done:
                        await dispatch_failure
                    if reader_task in done:
                        await reader_task
                    if waiter not in done:
                        raise TimeoutError("multiplexed Alchemy logsSubscribe acknowledgement timed out")
                    return waiter.result()

                request_id = 0
                for internal_id, target in enumerate(targets, start=1):
                    current_target = target
                    retry_delay = SUBSCRIPTION_RETRY_INITIAL_SECONDS
                    acknowledged = False

                    for attempt in range(1, SUBSCRIPTION_RETRIES_PER_TARGET + 1):
                        if stop.is_set():
                            break
                        current_attempt = attempt
                        request_id += 1
                        request_key = _request_id_key(request_id)
                        waiter: asyncio.Future[dict[str, Any]] = loop.create_future()
                        ack_waiters[request_key] = waiter
                        request_targets[request_key] = (internal_id, target)

                        _record_setup(
                            self,
                            endpoint,
                            phase="awaiting_ack",
                            target_count=len(targets),
                            acknowledged_count=acknowledged_count,
                            setup_started=setup_started,
                            current_target=target,
                            attempt=attempt,
                        )

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
                        finally:
                            ack_waiters.pop(request_key, None)
                            request_targets.pop(request_key, None)

                        error = message.get("error")
                        if error is not None:
                            code, provider_message = _error_parts(error)
                            await multiplex._set_target_state(
                                self,
                                endpoint,
                                target,
                                connected=False,
                                error_type="SubscriptionRejected",
                                error_code=code,
                            )
                            _record_setup(
                                self,
                                endpoint,
                                phase="provider_rejected_subscription",
                                target_count=len(targets),
                                acknowledged_count=acknowledged_count,
                                setup_started=setup_started,
                                current_target=target,
                                attempt=attempt,
                                error_type="SubscriptionRejected",
                                error_code=code,
                            )
                            if (
                                attempt < SUBSCRIPTION_RETRIES_PER_TARGET
                                and _retryable_subscription_error(code, provider_message)
                            ):
                                await asyncio.sleep(retry_delay)
                                retry_delay = min(SUBSCRIPTION_RETRY_MAX_SECONDS, retry_delay * 2.0)
                                continue
                            raise SubscriptionSetupError(
                                code=code,
                                message=provider_message,
                                target=target.address,
                            )

                        await multiplex._set_target_state(self, endpoint, target, connected=True)
                        acknowledged = True
                        acknowledged_count += 1
                        _record_setup(
                            self,
                            endpoint,
                            phase="acknowledged",
                            target_count=len(targets),
                            acknowledged_count=acknowledged_count,
                            setup_started=setup_started,
                            current_target=target,
                            attempt=attempt,
                        )
                        break

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

                _record_setup(
                    self,
                    endpoint,
                    phase="live",
                    target_count=len(targets),
                    acknowledged_count=acknowledged_count,
                    setup_started=setup_started,
                )

                stable_since = time.monotonic()
                stop_task = asyncio.create_task(stop.wait(), name=f"alchemy-stop:{endpoint.name}")
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

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            error_type = type(exc).__name__
            error_code = exc.code if isinstance(exc, SubscriptionSetupError) else _handshake_status_code(exc)
            await multiplex._disconnect_targets(
                self,
                endpoint,
                targets,
                error_type=error_type,
                error_code=error_code,
            )
            _record_setup(
                self,
                endpoint,
                phase="reconnecting",
                target_count=len(targets),
                acknowledged_count=acknowledged_count,
                setup_started=setup_started,
                current_target=current_target,
                attempt=current_attempt,
                error_type=error_type,
                error_code=error_code,
            )
            if not stop.is_set():
                await asyncio.sleep(reconnect_backoff)
                reconnect_backoff = min(STREAM_RECONNECT_MAX_SECONDS, reconnect_backoff * 2.0)
        else:
            await multiplex._disconnect_targets(
                self,
                endpoint,
                targets,
                error_type=None,
                error_code=None,
            )
        finally:
            if stop_task is not None and not stop_task.done():
                stop_task.cancel()
            if reader_task is not None and not reader_task.done():
                reader_task.cancel()
            for task in tuple(dispatch_tasks):
                if not task.done():
                    task.cancel()
            awaitables = [task for task in (stop_task, reader_task) if task is not None]
            awaitables.extend(tuple(dispatch_tasks))
            for task in awaitables:
                with suppress(asyncio.CancelledError, Exception):
                    await task
            if dispatch_failure is not None:
                if not dispatch_failure.done():
                    dispatch_failure.cancel()
                elif not dispatch_failure.cancelled():
                    with suppress(Exception):
                        dispatch_failure.exception()


setattr(_pumped_alchemy_multiplexed_stream, "_roi_alchemy_multiplexed", True)
setattr(_pumped_alchemy_multiplexed_stream, "_roi_alchemy_handshake_pumped", True)


def _status_with_alchemy_handshake_pump(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        endpoints = tuple(getattr(self, "endpoints", ()) or ())
        if not any(multiplex._is_alchemy_endpoint(endpoint) for endpoint in endpoints):
            return payload
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "alchemy_ack_receive_path": "dedicated-websocket-reader",
                    "alchemy_notification_dispatch_path": "bounded-concurrent-handlers",
                    "alchemy_max_inflight_notification_handlers": MAX_INFLIGHT_NOTIFICATION_HANDLERS,
                    "alchemy_inline_ack_receive_removed": True,
                    "alchemy_target_quorum_semantics_unchanged": True,
                    "live_poll_recoverability_lease_seconds_unchanged": 12.0,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_alchemy_handshake_pumped", True)
    return status


def install_alchemy_handshake_pump() -> None:
    current = multiplex._alchemy_multiplexed_stream
    if not bool(getattr(current, "_roi_alchemy_handshake_pumped", False)):
        multiplex._alchemy_multiplexed_stream = _pumped_alchemy_multiplexed_stream  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_alchemy_handshake_pumped", False)):
        DirectSolanaIngestionPlane.status = _status_with_alchemy_handshake_pump(current_status)  # type: ignore[method-assign]


__all__ = [
    "_pumped_alchemy_multiplexed_stream",
    "install_alchemy_handshake_pump",
]
