from __future__ import annotations

import os
import threading
import time
from functools import wraps
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse

from . import robinhood_chain_runtime as runtime
from . import robinhood_provider_failover as failover


RUNTIME_PROOF_VERSION = "robinhood-provider-runtime-proof-v1"
_INSTALLED = False
_PROBE_LOCK = threading.Lock()
_ORIGINAL_MARK_SUCCESS: Callable[..., None] | None = None
_ORIGINAL_SWITCH_FROM: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[[], dict[str, Any]] | None = None
_ORIGINAL_RUNTIME_RPC: Callable[..., Awaitable[Any]] | None = None


def _provider_kind(provider: failover.ProviderEndpoint | None) -> str:
    if provider is None:
        return "none"
    try:
        host = (urlparse(provider.http).hostname or "").lower()
    except Exception:
        return "private_rpc"
    if "drpc" in host:
        return "drpc"
    if "alchemy" in host:
        return "alchemy"
    return "private_rpc"


def _preferred_provider() -> failover.ProviderEndpoint | None:
    preferred = (os.getenv("ROBINHOOD_PROVIDER_PRIMARY") or "").strip()
    if not preferred:
        return None
    return next((item for item in failover.providers() if item.name == preferred), None)


def _mark_success_with_telemetry(name: str, *, transport_kind: str) -> None:
    assert _ORIGINAL_MARK_SUCCESS is not None
    _ORIGINAL_MARK_SUCCESS(name, transport_kind=transport_kind)
    provider = next((item for item in failover.providers() if item.name == name), None)
    with failover._LOCK:
        state = failover._state_for_locked(name)
        key = f"{transport_kind}_successes"
        state[key] = int(state.get(key, 0) or 0) + 1
        state[f"last_{transport_kind}_success_at"] = time.time()
        count = int(state[key])
        chain_verified = bool(state.get("chain_verified", False))
    if count == 1 or count % 100 == 0:
        print(
            "ROBINHOOD_PROVIDER_TRAFFIC "
            f"provider={name} provider_kind={_provider_kind(provider)} "
            f"transport={transport_kind} successes={count} "
            f"generation={failover.generation()} chain_verified={str(chain_verified).lower()}",
            flush=True,
        )


def _switch_from_with_verification_reset(
    failed_name: str,
    *,
    failure_type: str,
    immediate: bool,
    transport_kind: str,
) -> failover.ProviderEndpoint | None:
    assert _ORIGINAL_SWITCH_FROM is not None
    result = _ORIGINAL_SWITCH_FROM(
        failed_name,
        failure_type=failure_type,
        immediate=immediate,
        transport_kind=transport_kind,
    )
    if result is None or result.name != failed_name:
        with failover._LOCK:
            failover._state_for_locked(failed_name)["chain_verified"] = False
    return result


def _record_probe_failure(provider: failover.ProviderEndpoint, exc_name: str) -> None:
    with failover._LOCK:
        state = failover._state_for_locked(provider.name)
        state["http_failures"] = int(state.get("http_failures", 0) or 0) + 1
        state["last_failure_type"] = str(exc_name)
        state["last_failure_at"] = time.time()
        state["cooldown_until"] = time.monotonic() + failover._cooldown_seconds()
        state["chain_verified"] = False


def _record_chain_verified(
    provider: failover.ProviderEndpoint,
    *,
    previous_name: str | None,
) -> None:
    changed = previous_name is not None and previous_name != provider.name
    with failover._LOCK:
        state = failover._state_for_locked(provider.name)
        state["http_failures"] = 0
        state["chain_verified"] = True
        state["chain_verifications"] = int(state.get("chain_verifications", 0) or 0) + 1
        state["last_chain_verified_at"] = time.time()
        if changed:
            failover._ACTIVE_NAME = provider.name
            failover._GENERATION += 1
            state["failbacks_to"] = int(state.get("failbacks_to", 0) or 0) + 1
        current_generation = int(failover._GENERATION)
    print(
        "ROBINHOOD_PROVIDER_CHAIN_VERIFIED "
        f"provider={provider.name} provider_kind={_provider_kind(provider)} "
        f"generation={current_generation} failback={str(changed).lower()} "
        f"chain_id={runtime.ROBINHOOD_CHAIN_ID}",
        flush=True,
    )


async def _verify_preferred_if_needed(rpc_self: Any) -> bool:
    preferred = _preferred_provider()
    active = failover.active_provider()
    if preferred is None or active is None:
        return False
    with failover._LOCK:
        state = failover._state_for_locked(preferred.name)
        eligible = float(state.get("cooldown_until", 0.0) or 0.0) <= time.monotonic()
        already_verified = bool(state.get("chain_verified", False))
    if not eligible:
        return False
    if active.name == preferred.name and already_verified:
        return True
    if not _PROBE_LOCK.acquire(blocking=False):
        return False
    try:
        # Re-evaluate after acquiring the singleton probe guard.
        preferred = _preferred_provider()
        active = failover.active_provider()
        if preferred is None or active is None:
            return False
        with failover._LOCK:
            state = failover._state_for_locked(preferred.name)
            if float(state.get("cooldown_until", 0.0) or 0.0) > time.monotonic():
                return False
            if active.name == preferred.name and bool(state.get("chain_verified", False)):
                return True
        original = failover._ORIGINAL_RPC
        if original is None:
            return False
        old_url = str(getattr(rpc_self, "rpc_url", "") or "")
        rpc_self.rpc_url = preferred.http
        try:
            raw = await original(rpc_self, "eth_chainId", [])
            text = str(raw).strip().lower()
            chain_id = int(text, 16) if text.startswith("0x") else int(text)
            if chain_id != runtime.ROBINHOOD_CHAIN_ID:
                raise RuntimeError("WrongRobinhoodChainId")
        except Exception as exc:
            _record_probe_failure(preferred, type(exc).__name__)
            rpc_self.rpc_url = active.http if active is not None else old_url
            print(
                "ROBINHOOD_PROVIDER_CHAIN_VERIFY_FAILED "
                f"provider={preferred.name} provider_kind={_provider_kind(preferred)} "
                f"error_type={type(exc).__name__}",
                flush=True,
            )
            return False
        _record_chain_verified(preferred, previous_name=active.name)
        rpc_self.rpc_url = preferred.http
        return True
    finally:
        _PROBE_LOCK.release()


def _runtime_rpc_with_preferred_verification(
    original: Callable[..., Awaitable[Any]],
) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(rpc_self: Any, method: str, params: list[Any]) -> Any:
        current = str(getattr(rpc_self, "rpc_url", "") or "")
        if current and failover._is_pool_http(current):
            await _verify_preferred_if_needed(rpc_self)
        return await original(rpc_self, method, params)

    setattr(wrapped, "_roi_robinhood_provider_runtime_proof", True)
    return wrapped


def _status_with_runtime_proof() -> dict[str, Any]:
    assert _ORIGINAL_STATUS is not None
    result = dict(_ORIGINAL_STATUS())
    active = failover.active_provider()
    with failover._LOCK:
        traffic = {
            item.name: {
                "provider_kind": _provider_kind(item),
                "http_successes": int(failover._state_for_locked(item.name).get("http_successes", 0) or 0),
                "ws_successes": int(failover._state_for_locked(item.name).get("ws_successes", 0) or 0),
                "last_http_success_at": failover._state_for_locked(item.name).get("last_http_success_at"),
                "last_ws_success_at": failover._state_for_locked(item.name).get("last_ws_success_at"),
                "chain_verifications": int(failover._state_for_locked(item.name).get("chain_verifications", 0) or 0),
                "last_chain_verified_at": failover._state_for_locked(item.name).get("last_chain_verified_at"),
                "failbacks_to": int(failover._state_for_locked(item.name).get("failbacks_to", 0) or 0),
            }
            for item in failover.providers()
        }
    result.update(
        {
            "runtime_proof_version": RUNTIME_PROOF_VERSION,
            "active_provider_semantic": _provider_kind(active),
            "preferred_provider": (_preferred_provider().name if _preferred_provider() is not None else None),
            "provider_traffic": traffic,
            "provider_traffic_observed": any(
                state["http_successes"] > 0 or state["ws_successes"] > 0 for state in traffic.values()
            ),
        }
    )
    return result


def install_robinhood_provider_runtime_proof() -> None:
    global _INSTALLED
    global _ORIGINAL_MARK_SUCCESS, _ORIGINAL_SWITCH_FROM, _ORIGINAL_STATUS, _ORIGINAL_RUNTIME_RPC
    if _INSTALLED:
        return

    _ORIGINAL_MARK_SUCCESS = failover._mark_success
    _ORIGINAL_SWITCH_FROM = failover._switch_from
    _ORIGINAL_STATUS = failover.status
    _ORIGINAL_RUNTIME_RPC = runtime.RobinhoodRpc.rpc

    failover._mark_success = _mark_success_with_telemetry
    failover._switch_from = _switch_from_with_verification_reset
    failover.status = _status_with_runtime_proof
    if not bool(getattr(runtime.RobinhoodRpc.rpc, "_roi_robinhood_provider_runtime_proof", False)):
        runtime.RobinhoodRpc.rpc = _runtime_rpc_with_preferred_verification(runtime.RobinhoodRpc.rpc)  # type: ignore[method-assign]

    active = failover.active_provider()
    preferred = _preferred_provider()
    print(
        "ROBINHOOD_PROVIDER_CONFIGURED "
        f"active_provider={(active.name if active is not None else 'none')} "
        f"active_provider_kind={_provider_kind(active)} "
        f"preferred_provider={(preferred.name if preferred is not None else 'none')} "
        f"provider_count={len(failover.providers())} generation={failover.generation()}",
        flush=True,
    )
    _INSTALLED = True


__all__ = [
    "RUNTIME_PROOF_VERSION",
    "install_robinhood_provider_runtime_proof",
]
