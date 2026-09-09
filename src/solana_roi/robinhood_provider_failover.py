from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from dataclasses import dataclass
from functools import wraps
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse

import httpx

from . import robinhood_chain_runtime as runtime
from . import robinhood_production_ws_transport as transport


FAILOVER_VERSION = "robinhood-provider-failover-v1"
DEFAULT_FAILURE_THRESHOLD = 2
DEFAULT_COOLDOWN_SECONDS = 30.0

_INSTALLED = False
_LOCK = threading.RLock()
_ACTIVE_NAME: str | None = None
_GENERATION = 0
_PROVIDER_STATE: dict[str, dict[str, Any]] = {}
_ORIGINAL_RPC: Callable[..., Awaitable[Any]] | None = None
_ORIGINAL_INIT: Callable[..., None] | None = None
_ORIGINAL_READER_ASYNC: Callable[..., Awaitable[None]] | None = None
_ORIGINAL_READER_READY: Callable[[Any], bool] | None = None
_ORIGINAL_READER_GENERATION_START: Callable[[Any], int] | None = None
_ORIGINAL_UPDATE_STATE: Callable[..., dict[str, Any]] | None = None
_LEGACY_WS_RESOLVER: Callable[[], str] | None = None


@dataclass(frozen=True, slots=True)
class ProviderEndpoint:
    name: str
    http: str
    ws: str


class RobinhoodProviderPoolUnavailable(RuntimeError):
    """No private Robinhood provider pair is currently healthy."""


class _ProviderGenerationStop:
    def __init__(self, base: threading.Event, generation: int) -> None:
        self._base = base
        self._generation = int(generation)

    def is_set(self) -> bool:
        return self._base.is_set() or generation() != self._generation


def _normalized(value: str) -> str:
    return str(value or "").strip().rstrip("/").lower()


def _positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw is not None else int(default)
    except (TypeError, ValueError):
        value = int(default)
    return max(1, value)


def _positive_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    try:
        value = float(raw) if raw is not None else float(default)
    except (TypeError, ValueError):
        value = float(default)
    return max(0.0, value)


def _failure_threshold() -> int:
    return _positive_int_env("ROBINHOOD_PROVIDER_FAILOVER_ERROR_THRESHOLD", DEFAULT_FAILURE_THRESHOLD)


def _cooldown_seconds() -> float:
    return _positive_float_env("ROBINHOOD_PROVIDER_FAILOVER_COOLDOWN_SECONDS", DEFAULT_COOLDOWN_SECONDS)


def _private_pair(name: str, http_url: str, ws_url: str) -> ProviderEndpoint | None:
    http_url = str(http_url or "").strip()
    ws_url = str(ws_url or "").strip()
    if not http_url or not ws_url:
        return None
    if _normalized(http_url) == _normalized(runtime.ROBINHOOD_PUBLIC_RPC):
        return None
    if _normalized(ws_url) == _normalized(transport.PUBLIC_SEQUENCER_FEED):
        return None
    try:
        http_parts = urlparse(http_url)
        ws_parts = urlparse(ws_url)
    except Exception:
        return None
    if http_parts.scheme.lower() != "https" or not http_parts.netloc:
        return None
    if ws_parts.scheme.lower() != "wss" or not ws_parts.netloc:
        return None
    clean_name = str(name or "").strip() or "provider"
    return ProviderEndpoint(name=clean_name, http=http_url, ws=ws_url)


def _json_providers() -> list[ProviderEndpoint]:
    raw = (os.getenv("ROBINHOOD_RPC_ENDPOINTS_JSON") or "").strip()
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    providers: list[ProviderEndpoint] = []
    names: set[str] = set()
    endpoints: set[tuple[str, str]] = set()
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            continue
        provider = _private_pair(
            str(item.get("name") or f"provider-{index + 1}"),
            str(item.get("http") or ""),
            str(item.get("ws") or ""),
        )
        if provider is None or provider.name in names:
            continue
        key = (_normalized(provider.http), _normalized(provider.ws))
        if key in endpoints:
            continue
        providers.append(provider)
        names.add(provider.name)
        endpoints.add(key)
    return providers


def _legacy_primary_ws() -> str:
    explicit = (os.getenv("ROBINHOOD_WS_URL") or "").strip()
    if explicit:
        return explicit
    resolver = _LEGACY_WS_RESOLVER
    if resolver is not None and resolver is not ws_url:
        try:
            return str(resolver() or "").strip()
        except Exception:
            return ""
    configured_rpc = (os.getenv("ROBINHOOD_RPC_URL") or "").strip()
    try:
        parsed = urlparse(configured_rpc)
    except Exception:
        return ""
    if parsed.scheme.lower() != "https" or not parsed.netloc:
        return ""
    if _normalized(configured_rpc) == _normalized(runtime.ROBINHOOD_PUBLIC_RPC):
        return ""
    return parsed._replace(scheme="wss").geturl()


def _legacy_providers() -> list[ProviderEndpoint]:
    providers: list[ProviderEndpoint] = []
    primary = _private_pair(
        "primary",
        (os.getenv("ROBINHOOD_RPC_URL") or "").strip(),
        _legacy_primary_ws(),
    )
    if primary is not None:
        providers.append(primary)
    backup = _private_pair(
        "backup",
        (os.getenv("ROBINHOOD_BACKUP_RPC_URL") or "").strip(),
        (os.getenv("ROBINHOOD_BACKUP_WS_URL") or "").strip(),
    )
    if backup is not None and all(
        (_normalized(backup.http), _normalized(backup.ws))
        != (_normalized(item.http), _normalized(item.ws))
        for item in providers
    ):
        providers.append(backup)
    return providers


def providers() -> tuple[ProviderEndpoint, ...]:
    configured = _json_providers()
    if configured:
        return tuple(configured)
    return tuple(_legacy_providers())


def _provider_by_name(name: str | None) -> ProviderEndpoint | None:
    if not name:
        return None
    for item in providers():
        if item.name == name:
            return item
    return None


def _state_for_locked(name: str) -> dict[str, Any]:
    state = _PROVIDER_STATE.get(name)
    if state is None:
        state = {
            "http_failures": 0,
            "ws_failures": 0,
            "failovers_from": 0,
            "last_failure_type": None,
            "last_failure_at": None,
            "cooldown_until": 0.0,
            "chain_verified": False,
        }
        _PROVIDER_STATE[name] = state
    return state


def _eligible_locked(item: ProviderEndpoint, now: float) -> bool:
    state = _state_for_locked(item.name)
    return float(state.get("cooldown_until", 0.0) or 0.0) <= now


def _ensure_active_locked() -> ProviderEndpoint | None:
    global _ACTIVE_NAME
    items = providers()
    if not items:
        _ACTIVE_NAME = None
        return None
    now = time.monotonic()
    active = _provider_by_name(_ACTIVE_NAME)
    if active is not None and _eligible_locked(active, now):
        return active

    preferred = (os.getenv("ROBINHOOD_PROVIDER_PRIMARY") or "").strip()
    ordered = list(items)
    if preferred:
        preferred_item = next((item for item in ordered if item.name == preferred), None)
        if preferred_item is not None:
            ordered = [preferred_item] + [item for item in ordered if item.name != preferred_item.name]
    candidate = next((item for item in ordered if _eligible_locked(item, now)), None)
    if candidate is None:
        return None
    if _ACTIVE_NAME != candidate.name:
        _ACTIVE_NAME = candidate.name
    return candidate


def active_provider() -> ProviderEndpoint | None:
    with _LOCK:
        return _ensure_active_locked()


def active_name() -> str | None:
    provider = active_provider()
    return provider.name if provider is not None else None


def generation() -> int:
    with _LOCK:
        return int(_GENERATION)


def rpc_url() -> str:
    provider = active_provider()
    if provider is not None:
        return provider.http
    return (os.getenv("ROBINHOOD_RPC_URL") or runtime.ROBINHOOD_PUBLIC_RPC).strip()


def ws_url() -> str:
    provider = active_provider()
    if provider is not None:
        return provider.ws
    return ""


def production_provider_configured() -> bool:
    return active_provider() is not None


def endpoint_kind() -> str:
    count = len(providers())
    if count >= 2:
        return "configured_production_provider_pool"
    if count == 1:
        return "configured_production_rpc_and_websocket"
    if _normalized((os.getenv("ROBINHOOD_RPC_URL") or runtime.ROBINHOOD_PUBLIC_RPC)) == _normalized(
        runtime.ROBINHOOD_PUBLIC_RPC
    ):
        return "official_public_rate_limited_research_only"
    return "production_provider_configuration_incomplete"


def _mark_success(name: str, *, transport_kind: str) -> None:
    with _LOCK:
        state = _state_for_locked(name)
        state[f"{transport_kind}_failures"] = 0
        if transport_kind == "http":
            state["chain_verified"] = True


def _switch_from_locked(
    failed_name: str,
    *,
    failure_type: str,
    immediate: bool,
    transport_kind: str,
) -> ProviderEndpoint | None:
    global _ACTIVE_NAME, _GENERATION
    now = time.monotonic()
    state = _state_for_locked(failed_name)
    key = f"{transport_kind}_failures"
    state[key] = int(state.get(key, 0) or 0) + 1
    state["last_failure_type"] = str(failure_type)
    state["last_failure_at"] = time.time()

    if not immediate and int(state[key]) < _failure_threshold():
        return _provider_by_name(_ACTIVE_NAME)

    state["cooldown_until"] = now + _cooldown_seconds()
    items = list(providers())
    if not items:
        _ACTIVE_NAME = None
        return None
    try:
        start = next(i for i, item in enumerate(items) if item.name == failed_name)
    except StopIteration:
        start = -1
    for offset in range(1, len(items) + 1):
        candidate = items[(start + offset) % len(items)]
        if candidate.name == failed_name:
            continue
        if _eligible_locked(candidate, now):
            if _ACTIVE_NAME != candidate.name:
                _ACTIVE_NAME = candidate.name
                _GENERATION += 1
                state["failovers_from"] = int(state.get("failovers_from", 0) or 0) + 1
            return candidate
    return None


def _switch_from(
    failed_name: str,
    *,
    failure_type: str,
    immediate: bool,
    transport_kind: str,
) -> ProviderEndpoint | None:
    with _LOCK:
        return _switch_from_locked(
            failed_name,
            failure_type=failure_type,
            immediate=immediate,
            transport_kind=transport_kind,
        )


def _is_pool_http(value: str) -> bool:
    candidate = _normalized(value)
    return any(candidate == _normalized(item.http) for item in providers())


def _is_retriable_http_error(exc: BaseException) -> tuple[bool, bool]:
    if type(exc).__name__ == "RobinhoodAlchemyBudgetExceeded":
        return True, True
    if isinstance(exc, httpx.HTTPStatusError):
        status = int(exc.response.status_code)
        if status == 429:
            return True, True
        if status >= 500:
            return True, False
        return False, False
    if isinstance(exc, (httpx.TimeoutException, httpx.TransportError, httpx.NetworkError)):
        return True, False
    return False, False


def _critical_priority() -> bool:
    try:
        from . import robinhood_alchemy_budget_guard as guard
        return guard._PRIORITY.get() == "critical"
    except Exception:
        return False


async def _verify_candidate_chain(
    original: Callable[..., Awaitable[Any]],
    rpc_self: Any,
    provider: ProviderEndpoint,
) -> bool:
    rpc_self.rpc_url = provider.http
    try:
        raw = await original(rpc_self, "eth_chainId", [])
        chain_id = int(str(raw), 16)
    except Exception as exc:
        _switch_from(
            provider.name,
            failure_type=type(exc).__name__,
            immediate=True,
            transport_kind="http",
        )
        return False
    if chain_id != runtime.ROBINHOOD_CHAIN_ID:
        _switch_from(
            provider.name,
            failure_type="WrongRobinhoodChainId",
            immediate=True,
            transport_kind="http",
        )
        return False
    _mark_success(provider.name, transport_kind="http")
    return True


def _rpc_wrapper(original: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(rpc_self: Any, method: str, params: list[Any]) -> Any:
        current = str(getattr(rpc_self, "rpc_url", "") or "")
        items = providers()
        if not items or _normalized(current) == _normalized(runtime.ROBINHOOD_PUBLIC_RPC):
            return await original(rpc_self, method, params)
        if current and not _is_pool_http(current):
            return await original(rpc_self, method, params)

        attempts = max(1, len(items))
        last_error: Exception | None = None
        tried: set[str] = set()
        for _ in range(attempts):
            provider = active_provider()
            if provider is None or provider.name in tried:
                break
            tried.add(provider.name)
            rpc_self.rpc_url = provider.http
            try:
                result = await original(rpc_self, method, params)
                _mark_success(provider.name, transport_kind="http")
                return result
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                retriable, immediate = _is_retriable_http_error(exc)
                if not retriable:
                    raise
                last_error = exc
                next_provider = _switch_from(
                    provider.name,
                    failure_type=type(exc).__name__,
                    immediate=bool(immediate or _critical_priority()),
                    transport_kind="http",
                )
                if next_provider is None or next_provider.name == provider.name:
                    raise
                if not await _verify_candidate_chain(original, rpc_self, next_provider):
                    continue

        if last_error is not None:
            raise last_error
        raise RobinhoodProviderPoolUnavailable("robinhood_private_provider_pool_unavailable")

    setattr(wrapped, "_roi_robinhood_provider_failover_rpc", True)
    return wrapped


def _init_wrapper(original: Callable[..., None]) -> Callable[..., None]:
    @wraps(original)
    def wrapped(self: Any, rpc_url_arg: str | None = None, *, timeout_seconds: float = 4.0) -> None:
        original(self, rpc_url_arg, timeout_seconds=timeout_seconds)
        if rpc_url_arg is None:
            provider = active_provider()
            if provider is not None:
                self.rpc_url = provider.http

    setattr(wrapped, "_roi_robinhood_provider_failover_init", True)
    return wrapped


def _reader_generation_start_wrapper(original: Callable[[Any], int]) -> Callable[[Any], int]:
    @wraps(original)
    def wrapped(self: Any) -> int:
        result = original(self)
        if _ORIGINAL_UPDATE_STATE is not None:
            provider = active_provider()
            _ORIGINAL_UPDATE_STATE(
                self,
                provider_pool_generation=generation(),
                provider_name=provider.name if provider is not None else None,
            )
        return result

    return wrapped


def _reader_ready_wrapper(original: Callable[[Any], bool]) -> Callable[[Any], bool]:
    @wraps(original)
    def wrapped(self: Any) -> bool:
        if not original(self):
            return False
        state = transport._state(self)
        provider = active_provider()
        return bool(
            provider is not None
            and int(state.get("provider_pool_generation", -1)) == generation()
            and state.get("provider_name") == provider.name
        )

    return wrapped


def _update_state_wrapper(original: Callable[..., dict[str, Any]]) -> Callable[..., dict[str, Any]]:
    @wraps(original)
    def wrapped(self: Any, **updates: Any) -> dict[str, Any]:
        result = original(self, **updates)
        provider = active_provider()
        if provider is None:
            return result
        if updates.get("connected") is True and updates.get("synchronized") is True:
            _mark_success(provider.name, transport_kind="ws")
            return result
        error_type = updates.get("last_error_type")
        if error_type:
            _switch_from(
                provider.name,
                failure_type=str(error_type),
                immediate=False,
                transport_kind="ws",
            )
        return result

    return wrapped


def _reader_async_wrapper(
    original: Callable[[Any, threading.Event], Awaitable[None]],
) -> Callable[[Any, threading.Event], Awaitable[None]]:
    @wraps(original)
    async def wrapped(self: Any, stop: threading.Event) -> None:
        while not stop.is_set():
            provider = active_provider()
            if provider is None:
                transport._update_state(
                    self,
                    connected=False,
                    synchronized=False,
                    last_error_type="RobinhoodProviderPoolUnavailable",
                )
                await asyncio.sleep(transport.RECONNECT_SECONDS)
                continue
            current_generation = generation()
            proxy = _ProviderGenerationStop(stop, current_generation)
            await original(self, proxy)
            if not stop.is_set() and generation() == current_generation:
                await asyncio.sleep(transport.RECONNECT_SECONDS)

    setattr(wrapped, "_roi_robinhood_provider_failover_reader", True)
    return wrapped


def install_robinhood_provider_failover() -> None:
    global _INSTALLED
    global _ORIGINAL_RPC, _ORIGINAL_INIT, _ORIGINAL_READER_ASYNC
    global _ORIGINAL_READER_READY, _ORIGINAL_READER_GENERATION_START, _ORIGINAL_UPDATE_STATE
    global _LEGACY_WS_RESOLVER

    if _INSTALLED:
        return

    _ORIGINAL_INIT = runtime.RobinhoodRpc.__init__
    if not bool(getattr(runtime.RobinhoodRpc.__init__, "_roi_robinhood_provider_failover_init", False)):
        runtime.RobinhoodRpc.__init__ = _init_wrapper(runtime.RobinhoodRpc.__init__)  # type: ignore[method-assign]

    _ORIGINAL_RPC = runtime.RobinhoodRpc.rpc
    if not bool(getattr(runtime.RobinhoodRpc.rpc, "_roi_robinhood_provider_failover_rpc", False)):
        runtime.RobinhoodRpc.rpc = _rpc_wrapper(runtime.RobinhoodRpc.rpc)  # type: ignore[method-assign]

    _ORIGINAL_READER_ASYNC = transport._reader_async
    _ORIGINAL_READER_READY = transport._reader_ready
    _LEGACY_WS_RESOLVER = transport._ws_url
    _ORIGINAL_READER_GENERATION_START = transport._reader_generation_start
    _ORIGINAL_UPDATE_STATE = transport._update_state

    transport._rpc_url = rpc_url
    transport._ws_url = ws_url
    transport.production_provider_configured = production_provider_configured
    transport.endpoint_kind = endpoint_kind
    transport._reader_generation_start = _reader_generation_start_wrapper(transport._reader_generation_start)
    transport._reader_ready = _reader_ready_wrapper(transport._reader_ready)
    transport._update_state = _update_state_wrapper(transport._update_state)
    transport._reader_async = _reader_async_wrapper(transport._reader_async)

    _INSTALLED = True


def status() -> dict[str, Any]:
    items = providers()
    provider = active_provider()
    with _LOCK:
        states = {
            item.name: {
                "http_failures": int(_state_for_locked(item.name).get("http_failures", 0) or 0),
                "ws_failures": int(_state_for_locked(item.name).get("ws_failures", 0) or 0),
                "failovers_from": int(_state_for_locked(item.name).get("failovers_from", 0) or 0),
                "last_failure_type": _state_for_locked(item.name).get("last_failure_type"),
                "last_failure_at": _state_for_locked(item.name).get("last_failure_at"),
                "chain_verified": bool(_state_for_locked(item.name).get("chain_verified", False)),
            }
            for item in items
        }
    return {
        "version": FAILOVER_VERSION,
        "installed": _INSTALLED,
        "provider_count": len(items),
        "provider_names": [item.name for item in items],
        "active_provider": provider.name if provider is not None else None,
        "provider_generation": generation(),
        "automatic_http_failover": len(items) >= 2,
        "automatic_websocket_failover": len(items) >= 2,
        "paired_http_websocket_switching": True,
        "chain_id_required": runtime.ROBINHOOD_CHAIN_ID,
        "public_rpc_can_be_decision_authoritative": False,
        "public_sequencer_can_be_decision_authoritative": False,
        "provider_endpoints_exposed_in_status": False,
        "failure_threshold": _failure_threshold(),
        "cooldown_seconds": _cooldown_seconds(),
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
        "provider_state": states,
    }


def reset_for_tests() -> None:
    global _ACTIVE_NAME, _GENERATION
    with _LOCK:
        _ACTIVE_NAME = None
        _GENERATION = 0
        _PROVIDER_STATE.clear()


__all__ = [
    "FAILOVER_VERSION",
    "ProviderEndpoint",
    "RobinhoodProviderPoolUnavailable",
    "active_name",
    "active_provider",
    "endpoint_kind",
    "generation",
    "install_robinhood_provider_failover",
    "production_provider_configured",
    "providers",
    "reset_for_tests",
    "rpc_url",
    "status",
    "ws_url",
]
