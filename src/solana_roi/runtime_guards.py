from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

from . import direct_solana as direct_solana_module
from .direct_solana import DirectSolanaIngestionPlane
from .solana_rpc import RpcEndpoint


NotificationHandler = Callable[[Any, str, dict[int, Any], dict[str, Any]], Awaitable[None]]
ContextPrefill = Callable[[Any, Any], Awaitable[bool]]
EndpointFactory = Callable[..., tuple[RpcEndpoint, ...]]

# Hard production memory ceilings. These constrain buffering/fanout only; they do
# not reduce the seven-program strategy scope, scout cohort, evidence depth, or
# certification thresholds.
DIRECT_WS_MAX_QUEUE = 64
DIRECT_WS_MAX_SIZE_BYTES = 256 * 1024
DIRECT_CANDIDATE_CONTEXT_SLOTS = 3
DIRECT_BACKGROUND_CONTEXT_SLOTS = 1
DIRECT_RECONNECT_INITIAL_SECONDS = 0.5
DIRECT_RECONNECT_MAX_SECONDS = 30.0

_ONFINALITY_PUBLIC_HTTP = "https://solana.api.onfinality.io/public"
_ONFINALITY_PUBLIC_WS = "wss://solana.api.onfinality.io/public-ws"
_DRPC_PUBLIC = RpcEndpoint(
    name="drpc",
    http_url="https://solana.drpc.org/",
    ws_url="wss://solana.drpc.org",
)


def _cooperative_handler(original: NotificationHandler) -> NotificationHandler:
    """Force a scheduler handoff after every raw Solana notification."""

    async def handle(
        self: Any,
        provider: str,
        subscription_targets: dict[int, Any],
        message: dict[str, Any],
    ) -> None:
        await original(self, provider, subscription_targets, message)
        await asyncio.sleep(0)

    setattr(handle, "_roi_cooperative_yield", True)
    return handle


def _bounded_ws_connect(original: Callable[..., Any]) -> Callable[..., Any]:
    """Clamp receive buffering while preserving TCP/WebSocket backpressure."""

    def connect(*args: Any, **kwargs: Any) -> Any:
        requested_queue = kwargs.get("max_queue")
        requested_size = kwargs.get("max_size")
        kwargs["max_queue"] = DIRECT_WS_MAX_QUEUE if requested_queue is None else min(
            int(requested_queue), DIRECT_WS_MAX_QUEUE
        )
        kwargs["max_size"] = DIRECT_WS_MAX_SIZE_BYTES if requested_size is None else min(
            int(requested_size), DIRECT_WS_MAX_SIZE_BYTES
        )
        return original(*args, **kwargs)

    setattr(connect, "_roi_memory_bounded", True)
    return connect


def _bounded_context_prefill(original: ContextPrefill) -> ContextPrefill:
    """Bound overlapping 600-signature expansions and reserve candidate capacity."""

    async def prefill(self: Any, candidate: Any) -> bool:
        critical = False
        try:
            profile = self.service.registry.get(candidate.wallet)
            critical = bool(profile is not None and str(candidate.side).lower() == "buy")
        except Exception:
            critical = False

        attribute = "_roi_candidate_context_gate" if critical else "_roi_background_context_gate"
        slots = DIRECT_CANDIDATE_CONTEXT_SLOTS if critical else DIRECT_BACKGROUND_CONTEXT_SLOTS
        gate = getattr(self, attribute, None)
        if gate is None:
            gate = asyncio.Semaphore(slots)
            setattr(self, attribute, gate)
        async with gate:
            return await original(self, candidate)

    setattr(prefill, "_roi_memory_bounded", True)
    return prefill


def _replace_unusable_public_onfinality(original: EndpointFactory) -> EndpointFactory:
    """Replace only the known shared OnFinality public endpoint with dRPC public.

    Production telemetry proved the configured OnFinality public endpoint had zero
    successful HTTP reads and persistent WebSocket InvalidStatus reconnects. This
    transformation is intentionally narrow: private/authenticated OnFinality URLs
    and all other operator-provided endpoints are left untouched.
    """

    def endpoints(*args: Any, **kwargs: Any) -> tuple[RpcEndpoint, ...]:
        configured = original(*args, **kwargs)
        rows: list[RpcEndpoint] = []
        for endpoint in configured:
            if endpoint.http_url.rstrip("/") == _ONFINALITY_PUBLIC_HTTP.rstrip("/") and endpoint.ws_url.rstrip("/") == _ONFINALITY_PUBLIC_WS.rstrip("/"):
                rows.append(_DRPC_PUBLIC)
            else:
                rows.append(endpoint)

        deduped: list[RpcEndpoint] = []
        seen: set[tuple[str, str]] = set()
        for endpoint in rows:
            key = (endpoint.http_url.rstrip("/"), endpoint.ws_url.rstrip("/"))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(endpoint)
        return tuple(deduped)

    setattr(endpoints, "_roi_provider_repair", True)
    return endpoints


async def _guarded_stream_endpoint(self: Any, endpoint: RpcEndpoint, stop: asyncio.Event) -> None:
    """Stream one provider with bounded memory and truthful connection state.

    A provider is not marked connected until every frozen program/scout
    subscription is acknowledged. Subscription-setup failures therefore cannot
    transiently satisfy continuity. Backoff is reset only after that full setup,
    preventing a server that accepts TCP/WebSocket but rejects subscriptions from
    spinning in a tight reconnect loop.
    """

    backoff = DIRECT_RECONNECT_INITIAL_SECONDS
    while not stop.is_set():
        declared_connected = False
        try:
            async with direct_solana_module.websockets.connect(
                endpoint.ws_url,
                ping_interval=15,
                ping_timeout=15,
                close_timeout=2,
                max_queue=DIRECT_WS_MAX_QUEUE,
                max_size=DIRECT_WS_MAX_SIZE_BYTES,
            ) as ws:
                request_targets: dict[int, Any] = {}
                subscription_targets: dict[int, Any] = {}
                for request_id, target in enumerate(self.watch_targets, start=1):
                    request_targets[request_id] = target
                    await ws.send(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": request_id,
                                "method": "logsSubscribe",
                                "params": [{"mentions": [target.address]}, {"commitment": "processed"}],
                            }
                        )
                    )

                pending_acks = set(request_targets)
                while pending_acks and not stop.is_set():
                    message = json.loads(await asyncio.wait_for(ws.recv(), timeout=20.0))
                    if isinstance(message, dict) and message.get("id") in pending_acks:
                        request_id = int(message["id"])
                        if message.get("error") is not None:
                            raise RuntimeError("Solana logsSubscribe acknowledgement returned an error")
                        subscription = message.get("result")
                        if not isinstance(subscription, int):
                            raise RuntimeError("Solana logsSubscribe acknowledgement is invalid")
                        subscription_targets[subscription] = request_targets[request_id]
                        pending_acks.discard(request_id)
                    elif isinstance(message, dict):
                        # Process notifications for subscriptions already acknowledged
                        # rather than accumulating an unbounded startup buffer.
                        await self._handle_notification(endpoint.name, subscription_targets, message)

                if pending_acks or stop.is_set():
                    continue

                await self._connection_state(endpoint.name, True)
                declared_connected = True
                backoff = DIRECT_RECONNECT_INITIAL_SECONDS

                while not stop.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                    except asyncio.TimeoutError:
                        pong = await ws.ping()
                        await asyncio.wait_for(pong, timeout=5.0)
                        continue
                    await self._handle_notification(endpoint.name, subscription_targets, json.loads(raw))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if declared_connected:
                await self._connection_state(endpoint.name, False, type(exc).__name__)
            else:
                self.journal.set_provider(endpoint.name, connected=False, error_type=type(exc).__name__)
            if not stop.is_set():
                await asyncio.sleep(backoff)
                backoff = min(DIRECT_RECONNECT_MAX_SECONDS, backoff * 2.0)
        else:
            if declared_connected:
                await self._connection_state(endpoint.name, False, None)


setattr(_guarded_stream_endpoint, "_roi_stream_guarded", True)


def _bounded_status(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    """Expose active intrinsic protections in direct-Solana telemetry."""

    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        payload["production_memory_boundary"] = {
            "installed_intrinsically": True,
            "websocket_max_queue": DIRECT_WS_MAX_QUEUE,
            "websocket_max_size_bytes": DIRECT_WS_MAX_SIZE_BYTES,
            "candidate_context_slots": DIRECT_CANDIDATE_CONTEXT_SLOTS,
            "background_context_slots": DIRECT_BACKGROUND_CONTEXT_SLOTS,
            "strategy_scope_reduced": False,
            "context_signature_limit_unchanged": int(self.candidate_context_max_signatures),
            "hydration_worker_count_unchanged": int(self.worker_count),
        }
        payload["provider_runtime_policy"] = {
            "subscription_ack_required_before_connected": True,
            "reconnect_initial_seconds": DIRECT_RECONNECT_INITIAL_SECONDS,
            "reconnect_max_seconds": DIRECT_RECONNECT_MAX_SECONDS,
            "known_unusable_public_onfinality_replaced_with_drpc": True,
        }
        return payload

    setattr(status, "_roi_memory_bounded", True)
    return status


def install_runtime_guards() -> None:
    """Install all production protections independently of the Render entrypoint."""

    current_handler = DirectSolanaIngestionPlane._handle_notification
    if not bool(getattr(current_handler, "_roi_cooperative_yield", False)):
        DirectSolanaIngestionPlane._handle_notification = _cooperative_handler(current_handler)  # type: ignore[method-assign]

    current_connect = direct_solana_module.websockets.connect
    if not bool(getattr(current_connect, "_roi_memory_bounded", False)):
        direct_solana_module.websockets.connect = _bounded_ws_connect(current_connect)  # type: ignore[assignment]

    current_prefill = DirectSolanaIngestionPlane._prefill_launch_context
    if not bool(getattr(current_prefill, "_roi_memory_bounded", False)):
        DirectSolanaIngestionPlane._prefill_launch_context = _bounded_context_prefill(current_prefill)  # type: ignore[method-assign]

    current_endpoint_factory = direct_solana_module.rpc_endpoints_from_env
    if not bool(getattr(current_endpoint_factory, "_roi_provider_repair", False)):
        direct_solana_module.rpc_endpoints_from_env = _replace_unusable_public_onfinality(current_endpoint_factory)  # type: ignore[assignment]

    current_stream = DirectSolanaIngestionPlane._stream_endpoint
    if not bool(getattr(current_stream, "_roi_stream_guarded", False)):
        DirectSolanaIngestionPlane._stream_endpoint = _guarded_stream_endpoint  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_memory_bounded", False)):
        DirectSolanaIngestionPlane.status = _bounded_status(current_status)  # type: ignore[method-assign]


__all__ = [
    "DIRECT_WS_MAX_QUEUE",
    "DIRECT_WS_MAX_SIZE_BYTES",
    "DIRECT_CANDIDATE_CONTEXT_SLOTS",
    "DIRECT_BACKGROUND_CONTEXT_SLOTS",
    "install_runtime_guards",
]
