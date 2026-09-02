from __future__ import annotations

import asyncio
import json
import time
from contextlib import suppress
from typing import Any, Callable

from . import direct_solana as direct_solana_module
from .direct_solana import DirectSolanaIngestionPlane
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


ACK_TIMEOUT_SECONDS = 20.0
MAX_INFLIGHT_NOTIFICATION_HANDLERS = 32


class NotificationDispatchBackpressureError(RuntimeError):
    pass


class RequestIdentifierError(RuntimeError):
    pass


def _request_id_key(value: Any) -> str:
    """Normalize JSON-RPC acknowledgement request IDs across providers."""

    if isinstance(value, bool) or value is None:
        raise RequestIdentifierError("Solana subscription acknowledgement has no usable request id")
    if isinstance(value, (int, str)):
        key = str(value).strip()
        if key:
            return key
    raise RequestIdentifierError("Solana subscription acknowledgement has an unsupported request id")


def _close_details(exc: BaseException) -> tuple[int | None, str | None]:
    """Extract bounded WebSocket close telemetry without exposing provider payloads."""

    for side_name in ("rcvd", "sent"):
        side = getattr(exc, side_name, None)
        if side is None:
            continue
        raw_code = getattr(side, "code", None)
        raw_reason = getattr(side, "reason", None)
        try:
            code = int(raw_code) if raw_code is not None else None
        except (TypeError, ValueError):
            code = None
        reason = " ".join(str(raw_reason or "").split())[:160] or None
        if code is not None or reason is not None:
            return code, reason
    return None, None


def _setup_state(self: Any) -> dict[str, Any]:
    state = getattr(self, "_roi_subscription_setup", None)
    if not isinstance(state, dict):
        state = {}
        setattr(self, "_roi_subscription_setup", state)
    return state


async def _pumped_stream_endpoint(self: Any, endpoint: RpcEndpoint, stop: asyncio.Event) -> None:
    """Keep acknowledgement handling independent from live notification work.

    A dedicated reader owns ``ws.recv()`` for the lifetime of the connection. JSON-
    RPC acknowledgements resolve futures immediately, while live notifications are
    dispatched into a tightly bounded set of handler tasks. This prevents high-
    volume program traffic from starving the remaining subscription handshakes.
    """

    reconnect_backoff = STREAM_RECONNECT_INITIAL_SECONDS
    while not stop.is_set():
        declared_connected = False
        setup_started = time.monotonic()
        reader_task: asyncio.Task[Any] | None = None
        stop_task: asyncio.Task[Any] | None = None
        dispatch_failure: asyncio.Future[Any] | None = None
        dispatch_tasks: set[asyncio.Task[Any]] = set()
        acknowledged_count = 0
        current_target: Any | None = None
        try:
            targets = tuple(self.watch_targets)
            state = _setup_state(self)
            state[endpoint.name] = {
                "ready": False,
                "phase": "connecting",
                "target_count": len(targets),
                "acknowledged_count": 0,
                "current_target": None,
                "current_target_kind": None,
                "attempt": 0,
                "error_code": None,
                "error_message": None,
                "close_code": None,
                "close_reason": None,
            }

            async with direct_solana_module.websockets.connect(
                endpoint.ws_url,
                ping_interval=15,
                ping_timeout=15,
                close_timeout=2,
                max_queue=64,
                max_size=1024 * 1024,
            ) as ws:
                loop = asyncio.get_running_loop()
                dispatch_failure = loop.create_future()
                ack_waiters: dict[str, asyncio.Future[dict[str, Any]]] = {}
                request_targets: dict[str, tuple[int, Any]] = {}
                subscription_targets: dict[int, Any] = {}
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
                        raise RuntimeError("Solana notification arrived before its subscription acknowledgement")
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
                            except RequestIdentifierError:
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
                                "direct Solana notification handlers exceeded bounded in-flight capacity"
                            )
                        task = asyncio.create_task(
                            dispatch_notification(message),
                            name=f"direct-solana-notification-{endpoint.name}",
                        )
                        dispatch_tasks.add(task)
                        task.add_done_callback(dispatch_done)

                reader_task = asyncio.create_task(reader(), name=f"direct-solana-reader-{endpoint.name}")

                async def await_ack(waiter: asyncio.Future[dict[str, Any]]) -> dict[str, Any]:
                    assert reader_task is not None
                    wait_set: set[asyncio.Future[Any] | asyncio.Task[Any]] = {waiter, reader_task}
                    if dispatch_failure is not None:
                        wait_set.add(dispatch_failure)
                    done, _pending = await asyncio.wait(
                        wait_set,
                        timeout=ACK_TIMEOUT_SECONDS,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if dispatch_failure is not None and dispatch_failure in done:
                        await dispatch_failure
                    if reader_task in done:
                        await reader_task
                    if waiter not in done:
                        raise TimeoutError("Solana logsSubscribe acknowledgement timed out")
                    return waiter.result()

                request_id = 0
                for internal_id, target in enumerate(targets, start=1):
                    current_target = target
                    retry_delay = SUBSCRIPTION_RETRY_INITIAL_SECONDS
                    acknowledged = False
                    for attempt in range(1, SUBSCRIPTION_RETRIES_PER_TARGET + 1):
                        request_id += 1
                        request_key = _request_id_key(request_id)
                        waiter: asyncio.Future[dict[str, Any]] = loop.create_future()
                        ack_waiters[request_key] = waiter
                        request_targets[request_key] = (internal_id, target)
                        state[endpoint.name] = {
                            "ready": False,
                            "phase": "awaiting_ack",
                            "target_count": len(targets),
                            "acknowledged_count": acknowledged_count,
                            "current_target": target.address,
                            "current_target_kind": target.kind,
                            "attempt": attempt,
                            "error_code": None,
                            "error_message": None,
                            "close_code": None,
                            "close_reason": None,
                            "setup_seconds": max(0.0, time.monotonic() - setup_started),
                        }
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
                            state[endpoint.name] = {
                                "ready": False,
                                "phase": "provider_rejected_subscription",
                                "target_count": len(targets),
                                "acknowledged_count": acknowledged_count,
                                "current_target": target.address,
                                "current_target_kind": target.kind,
                                "attempt": attempt,
                                "error_code": code,
                                "error_message": provider_message,
                                "close_code": None,
                                "close_reason": None,
                                "setup_seconds": max(0.0, time.monotonic() - setup_started),
                            }
                            if attempt < SUBSCRIPTION_RETRIES_PER_TARGET and _retryable_subscription_error(
                                code, provider_message
                            ):
                                await asyncio.sleep(retry_delay)
                                retry_delay = min(SUBSCRIPTION_RETRY_MAX_SECONDS, retry_delay * 2.0)
                                continue
                            raise SubscriptionSetupError(code=code, message=provider_message, target=target.address)

                        acknowledged = True
                        acknowledged_count += 1
                        state[endpoint.name] = {
                            "ready": False,
                            "phase": "acknowledged",
                            "target_count": len(targets),
                            "acknowledged_count": acknowledged_count,
                            "current_target": target.address,
                            "current_target_kind": target.kind,
                            "attempt": attempt,
                            "error_code": None,
                            "error_message": None,
                            "close_code": None,
                            "close_reason": None,
                            "setup_seconds": max(0.0, time.monotonic() - setup_started),
                        }
                        break

                    if not acknowledged:
                        if stop.is_set():
                            return
                        raise SubscriptionSetupError(
                            code=None,
                            message="subscription acknowledgement retry budget exhausted",
                            target=target.address,
                        )
                    await asyncio.sleep(SUBSCRIPTION_INTER_TARGET_DELAY_SECONDS)

                await self._connection_state(endpoint.name, True)
                declared_connected = True
                state[endpoint.name] = {
                    "ready": True,
                    "phase": "live",
                    "target_count": len(targets),
                    "acknowledged_count": acknowledged_count,
                    "current_target": None,
                    "current_target_kind": None,
                    "attempt": 0,
                    "error_code": None,
                    "error_message": None,
                    "close_code": None,
                    "close_reason": None,
                    "setup_seconds": max(0.0, time.monotonic() - setup_started),
                }

                stable_since = time.monotonic()
                stop_task = asyncio.create_task(stop.wait(), name=f"direct-solana-stop-{endpoint.name}")
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
            code, reason = _close_details(exc)
            state = _setup_state(self)
            previous = state.get(endpoint.name)
            row = dict(previous) if isinstance(previous, dict) else {}
            row.update(
                {
                    "ready": False,
                    "phase": "reconnecting",
                    "target_count": int(row.get("target_count") or len(tuple(self.watch_targets))),
                    "acknowledged_count": int(row.get("acknowledged_count") or acknowledged_count),
                    "current_target": row.get("current_target") or getattr(current_target, "address", None),
                    "current_target_kind": row.get("current_target_kind") or getattr(current_target, "kind", None),
                    "error_type": type(exc).__name__,
                    "close_code": code,
                    "close_reason": reason,
                    "setup_seconds": max(0.0, time.monotonic() - setup_started),
                }
            )
            state[endpoint.name] = row
            if declared_connected:
                await self._connection_state(endpoint.name, False, type(exc).__name__)
            else:
                self.journal.set_provider(endpoint.name, connected=False, error_type=type(exc).__name__)
            if not stop.is_set():
                await asyncio.sleep(reconnect_backoff)
                reconnect_backoff = min(STREAM_RECONNECT_MAX_SECONDS, reconnect_backoff * 2.0)
        else:
            if declared_connected:
                await self._connection_state(endpoint.name, False, None)
        finally:
            if stop_task is not None and not stop_task.done():
                stop_task.cancel()
            if reader_task is not None and not reader_task.done():
                reader_task.cancel()
            for task in tuple(dispatch_tasks):
                if not task.done():
                    task.cancel()
            awaitables = [task for task in (stop_task, reader_task) if task is not None]
            awaitables.extend(dispatch_tasks)
            for task in awaitables:
                with suppress(asyncio.CancelledError, Exception):
                    await task
            if dispatch_failure is not None and not dispatch_failure.done():
                dispatch_failure.cancel()


setattr(_pumped_stream_endpoint, "_roi_handshake_pumped", True)
setattr(_pumped_stream_endpoint, "_roi_sequential_subscription_setup", True)
setattr(_pumped_stream_endpoint, "_roi_stream_guarded", True)


def _status_with_handshake_policy(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "ack_receive_path": "dedicated-websocket-reader",
                    "notification_dispatch_path": "bounded-concurrent-handlers",
                    "max_inflight_notification_handlers": MAX_INFLIGHT_NOTIFICATION_HANDLERS,
                    "request_id_type_agnostic": True,
                    "subscription_setup_progress_telemetry": True,
                    "connection_close_code_telemetry": True,
                }
            )
        return payload

    setattr(status, "_roi_handshake_pumped", True)
    setattr(status, "_roi_transport_hardened", True)
    setattr(status, "_roi_memory_bounded", True)
    setattr(status, "_roi_subscription_telemetry", True)
    return status


def install_handshake_pump() -> None:
    current_stream = DirectSolanaIngestionPlane._stream_endpoint
    if not bool(getattr(current_stream, "_roi_handshake_pumped", False)):
        DirectSolanaIngestionPlane._stream_endpoint = _pumped_stream_endpoint  # type: ignore[method-assign]
    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_handshake_pumped", False)):
        DirectSolanaIngestionPlane.status = _status_with_handshake_policy(current_status)  # type: ignore[method-assign]


__all__ = [
    "ACK_TIMEOUT_SECONDS",
    "MAX_INFLIGHT_NOTIFICATION_HANDLERS",
    "install_handshake_pump",
]
