from __future__ import annotations

import asyncio
import json
import time
from datetime import timedelta
from typing import Any, Callable

from . import direct_solana as direct_solana_module
from . import solana_rpc as solana_rpc_module
from .direct_solana import DirectSolanaIngestionPlane
from .solana_rpc import RpcEndpoint


EndpointFactory = Callable[..., tuple[RpcEndpoint, ...]]

SUBSCRIPTION_RETRIES_PER_TARGET = 5
SUBSCRIPTION_INTER_TARGET_DELAY_SECONDS = 0.05
SUBSCRIPTION_RETRY_INITIAL_SECONDS = 0.5
SUBSCRIPTION_RETRY_MAX_SECONDS = 8.0
STREAM_RECONNECT_INITIAL_SECONDS = 1.0
STREAM_RECONNECT_MAX_SECONDS = 30.0
STREAM_STABLE_RESET_SECONDS = 60.0

_OFFICIAL_SOLANA = RpcEndpoint(
    name="solana-mainnet",
    http_url="https://api.mainnet.solana.com",
    ws_url="wss://api.mainnet.solana.com",
)
_KNOWN_SHARED_SECONDARIES = {
    (
        "https://solana.api.onfinality.io/public",
        "wss://solana.api.onfinality.io/public-ws",
    ),
    (
        "https://solana.drpc.org",
        "wss://solana.drpc.org",
    ),
}


class SubscriptionSetupError(RuntimeError):
    def __init__(self, *, code: int | None, message: str, target: str):
        super().__init__("Solana logsSubscribe setup failed")
        self.code = code
        self.provider_message = message[:180]
        self.target = target


def _official_secondary(original: EndpointFactory) -> EndpointFactory:
    def endpoints(*args: Any, **kwargs: Any) -> tuple[RpcEndpoint, ...]:
        configured = original(*args, **kwargs)
        rows: list[RpcEndpoint] = []
        for endpoint in configured:
            key = (endpoint.http_url.rstrip("/"), endpoint.ws_url.rstrip("/"))
            rows.append(_OFFICIAL_SOLANA if key in _KNOWN_SHARED_SECONDARIES else endpoint)
        deduped: list[RpcEndpoint] = []
        seen: set[tuple[str, str]] = set()
        for endpoint in rows:
            key = (endpoint.http_url.rstrip("/"), endpoint.ws_url.rstrip("/"))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(endpoint)
        return tuple(deduped)

    setattr(endpoints, "_roi_official_secondary", True)
    return endpoints


def _subscription_key(value: Any) -> str:
    if isinstance(value, bool) or value is None:
        raise ValueError("missing subscription id")
    if isinstance(value, (int, str)):
        key = str(value).strip()
        if key:
            return key
    raise ValueError("unsupported subscription id")


def _error_parts(error: Any) -> tuple[int | None, str]:
    if not isinstance(error, dict):
        return None, "provider returned a subscription error"
    raw_code = error.get("code")
    try:
        code = int(raw_code) if raw_code is not None else None
    except (TypeError, ValueError):
        code = None
    message = " ".join(str(error.get("message") or "provider returned a subscription error").split())[:180]
    return code, message


def _retryable_subscription_error(code: int | None, message: str) -> bool:
    lower = message.lower()
    if code in {-32601, -32602}:
        return False
    return bool(
        code in {-32005, -32004, -32603}
        or any(token in lower for token in ("rate", "limit", "busy", "tempor", "try again", "capacity", "too many"))
    )


async def _resilient_stream_endpoint(self: Any, endpoint: RpcEndpoint, stop: asyncio.Event) -> None:
    reconnect_backoff = STREAM_RECONNECT_INITIAL_SECONDS
    while not stop.is_set():
        declared_connected = False
        setup_started = time.monotonic()
        try:
            async with direct_solana_module.websockets.connect(
                endpoint.ws_url,
                ping_interval=15,
                ping_timeout=15,
                close_timeout=2,
                max_queue=64,
                max_size=256 * 1024,
            ) as ws:
                subscription_targets: dict[int, Any] = {}
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
                for internal_id, target in enumerate(self.watch_targets, start=1):
                    retry_delay = SUBSCRIPTION_RETRY_INITIAL_SECONDS
                    acknowledged = False
                    for attempt in range(1, SUBSCRIPTION_RETRIES_PER_TARGET + 1):
                        request_id += 1
                        current_id = request_id
                        await ws.send(json.dumps({
                            "jsonrpc": "2.0",
                            "id": current_id,
                            "method": "logsSubscribe",
                            "params": [{"mentions": [target.address]}, {"commitment": "processed"}],
                        }))
                        retry_this_target = False
                        while not stop.is_set():
                            message = json.loads(await asyncio.wait_for(ws.recv(), timeout=20.0))
                            if not isinstance(message, dict):
                                continue
                            if message.get("id") != current_id:
                                await dispatch(message)
                                continue
                            error = message.get("error")
                            if error is not None:
                                code, provider_message = _error_parts(error)
                                state = getattr(self, "_roi_subscription_setup", None)
                                if not isinstance(state, dict):
                                    state = {}
                                    setattr(self, "_roi_subscription_setup", state)
                                state[endpoint.name] = {
                                    "ready": False,
                                    "target": target.address,
                                    "target_kind": target.kind,
                                    "attempt": attempt,
                                    "error_code": code,
                                    "error_message": provider_message,
                                }
                                if attempt < SUBSCRIPTION_RETRIES_PER_TARGET and _retryable_subscription_error(code, provider_message):
                                    await asyncio.sleep(retry_delay)
                                    retry_delay = min(SUBSCRIPTION_RETRY_MAX_SECONDS, retry_delay * 2.0)
                                    retry_this_target = True
                                    break
                                raise SubscriptionSetupError(code=code, message=provider_message, target=target.address)
                            external_key = _subscription_key(message.get("result"))
                            external_to_internal[external_key] = internal_id
                            subscription_targets[internal_id] = target
                            acknowledged = True
                            break
                        if acknowledged or stop.is_set():
                            break
                        if retry_this_target:
                            continue
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
                state = getattr(self, "_roi_subscription_setup", None)
                if not isinstance(state, dict):
                    state = {}
                    setattr(self, "_roi_subscription_setup", state)
                state[endpoint.name] = {
                    "ready": True,
                    "target_count": len(subscription_targets),
                    "setup_seconds": max(0.0, time.monotonic() - setup_started),
                    "error_code": None,
                    "error_message": None,
                }
                stable_since = time.monotonic()
                while not stop.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                    except asyncio.TimeoutError:
                        pong = await ws.ping()
                        await asyncio.wait_for(pong, timeout=5.0)
                        continue
                    await dispatch(json.loads(raw))
                    if time.monotonic() - stable_since >= STREAM_STABLE_RESET_SECONDS:
                        reconnect_backoff = STREAM_RECONNECT_INITIAL_SECONDS
        except asyncio.CancelledError:
            raise
        except Exception as exc:
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


setattr(_resilient_stream_endpoint, "_roi_sequential_subscription_setup", True)


def _status_with_subscription_telemetry(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        payload["subscription_setup"] = dict(getattr(self, "_roi_subscription_setup", {}) or {})
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update({
                "subscription_setup_mode": "sequential_ack_with_bounded_retry",
                "subscription_retries_per_target": SUBSCRIPTION_RETRIES_PER_TARGET,
                "subscription_inter_target_delay_ms": SUBSCRIPTION_INTER_TARGET_DELAY_SECONDS * 1000.0,
                "reconnect_stable_reset_seconds": STREAM_STABLE_RESET_SECONDS,
                "no_cost_secondary": "solana-mainnet-public",
                "public_endpoint_has_no_sla": True,
            })
        cutoff = (direct_solana_module.utcnow() - timedelta(minutes=5)).isoformat()
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT total_hydration_ms, normalized FROM direct_solana_hydration_metrics "
                "WHERE historical_recovery=0 AND hydrated_at>=? ORDER BY hydrated_at DESC LIMIT 1000",
                (cutoff,),
            ).fetchall()
        values = sorted(float(row["total_hydration_ms"]) for row in rows)
        p95 = values[min(len(values) - 1, int((len(values) - 1) * 0.95))] if values else None
        payload["recent_hydration_5m"] = {
            "sample_count": len(rows),
            "normalized_count": sum(1 for row in rows if bool(row["normalized"])),
            "p95_ms": p95,
        }
        return payload

    setattr(status, "_roi_subscription_telemetry", True)
    return status


def install_stream_resilience() -> None:
    current_factory = solana_rpc_module.rpc_endpoints_from_env
    if not bool(getattr(current_factory, "_roi_official_secondary", False)):
        solana_rpc_module.rpc_endpoints_from_env = _official_secondary(current_factory)  # type: ignore[assignment]
    direct_solana_module.rpc_endpoints_from_env = solana_rpc_module.rpc_endpoints_from_env
    current_stream = DirectSolanaIngestionPlane._stream_endpoint
    if not bool(getattr(current_stream, "_roi_sequential_subscription_setup", False)):
        DirectSolanaIngestionPlane._stream_endpoint = _resilient_stream_endpoint  # type: ignore[method-assign]
    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_subscription_telemetry", False)):
        DirectSolanaIngestionPlane.status = _status_with_subscription_telemetry(current_status)  # type: ignore[method-assign]


__all__ = ["install_stream_resilience"]
