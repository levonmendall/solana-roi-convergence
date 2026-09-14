from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse

import httpx

from . import robinhood_chain_runtime as runtime
from . import robinhood_provider_failover as failover


RUNTIME_PROOF_VERSION = "robinhood-provider-runtime-proof-v4-alchemy-monthly-quota-recovery"
DEFAULT_DRPC_METHOD_UNAVAILABLE_COOLDOWN_SECONDS = 900.0
_INSTALLED = False
_PROBE_LOCK = threading.Lock()
_FAILURE_LOCK = threading.Lock()
_REQUEST_FAILURE_COUNTS: dict[tuple[str, str], int] = {}
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


def _drpc_method_unavailable_cooldown_seconds() -> float:
    raw = os.getenv("ROBINHOOD_DRPC_METHOD_UNAVAILABLE_COOLDOWN_SECONDS")
    try:
        value = float(raw) if raw is not None else DEFAULT_DRPC_METHOD_UNAVAILABLE_COOLDOWN_SECONDS
    except (TypeError, ValueError):
        value = DEFAULT_DRPC_METHOD_UNAVAILABLE_COOLDOWN_SECONDS
    return max(60.0, value)


def _jsonrpc_error_code(exc: BaseException) -> int | None:
    if not isinstance(exc, httpx.HTTPStatusError):
        return None
    try:
        body = exc.response.json()
    except Exception:
        return None
    if not isinstance(body, dict):
        return None
    error = body.get("error")
    if not isinstance(error, dict):
        return None
    try:
        return int(error.get("code"))
    except (TypeError, ValueError):
        return None


def _drpc_method_unavailable(provider: failover.ProviderEndpoint, exc: BaseException) -> bool:
    return _provider_kind(provider) == "drpc" and _jsonrpc_error_code(exc) == -32601


def _next_utc_month_epoch(now_epoch: float | None = None) -> float:
    current = datetime.fromtimestamp(float(now_epoch if now_epoch is not None else time.time()), tz=timezone.utc)
    if current.month == 12:
        nxt = datetime(current.year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        nxt = datetime(current.year, current.month + 1, 1, tzinfo=timezone.utc)
    return nxt.timestamp()


def _iso_utc(epoch: Any) -> str | None:
    try:
        value = float(epoch)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _alchemy_monthly_quota_exhausted(
    provider: failover.ProviderEndpoint,
    exc: BaseException,
) -> bool:
    """Classify only durable/monthly Alchemy exhaustion, not ordinary burst 429s."""
    if _provider_kind(provider) != "alchemy":
        return False
    if type(exc).__name__ == "RobinhoodProviderMonthlyBudgetExceeded":
        return True
    message = str(exc).lower()
    strong_markers = (
        "monthly quota",
        "monthly request",
        "monthly capacity",
        "billing limit",
        "billing capacity",
        "compute unit limit",
        "compute units limit",
        "credit limit",
        "usage limit",
        "plan limit",
    )
    if any(marker in message for marker in strong_markers):
        return True
    if not isinstance(exc, httpx.HTTPStatusError) or int(exc.response.status_code) != 429:
        return False
    try:
        body = str(exc.response.text or "").lower()
    except Exception:
        body = ""
    try:
        payload = exc.response.json()
        body = f"{body} {payload!r}".lower()
    except Exception:
        pass
    return any(marker in body for marker in strong_markers)


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
        read_capability_verified = bool(state.get("read_capability_verified", False))
    if count == 1 or count % 100 == 0:
        print(
            "ROBINHOOD_PROVIDER_TRAFFIC "
            f"provider={name} provider_kind={_provider_kind(provider)} "
            f"transport={transport_kind} successes={count} "
            f"generation={failover.generation()} chain_verified={str(chain_verified).lower()} "
            f"read_capability_verified={str(read_capability_verified).lower()}",
            flush=True,
        )


def _record_request_failure(provider: failover.ProviderEndpoint | None, method: str, exc_name: str) -> None:
    kind = _provider_kind(provider)
    key = (kind, str(method))
    with _FAILURE_LOCK:
        count = int(_REQUEST_FAILURE_COUNTS.get(key, 0)) + 1
        _REQUEST_FAILURE_COUNTS[key] = count
    if count == 1 or count % 20 == 0:
        print(
            "ROBINHOOD_PROVIDER_REQUEST_FAILED "
            f"provider_kind={kind} method={method} error_type={exc_name} failures={count} "
            f"generation={failover.generation()}",
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
            state = failover._state_for_locked(failed_name)
            state["chain_verified"] = False
            state["read_capability_verified"] = False
    return result


def _record_probe_failure(provider: failover.ProviderEndpoint, exc: BaseException) -> None:
    jsonrpc_code = _jsonrpc_error_code(exc)
    method_unavailable = _drpc_method_unavailable(provider, exc)
    monthly_quota = _alchemy_monthly_quota_exhausted(provider, exc)
    now_wall = time.time()
    quota_until = _next_utc_month_epoch(now_wall) if monthly_quota else None
    if monthly_quota:
        cooldown_seconds = max(60.0, float(quota_until or now_wall) - now_wall)
        reason = "alchemy_monthly_quota_exhausted"
    elif method_unavailable:
        cooldown_seconds = _drpc_method_unavailable_cooldown_seconds()
        reason = "jsonrpc_method_not_found"
    else:
        cooldown_seconds = failover._cooldown_seconds()
        reason = None
    with failover._LOCK:
        state = failover._state_for_locked(provider.name)
        state["http_failures"] = int(state.get("http_failures", 0) or 0) + 1
        state["last_failure_type"] = type(exc).__name__
        state["last_failure_at"] = now_wall
        state["last_capability_jsonrpc_code"] = jsonrpc_code
        state["cooldown_until"] = time.monotonic() + cooldown_seconds
        state["capability_quarantine_reason"] = reason
        state["capability_quarantine_until"] = now_wall + cooldown_seconds if reason is not None else None
        state["chain_verified"] = False
        state["read_capability_verified"] = False
        if monthly_quota:
            state["quota_exhausted_until"] = quota_until
            state["quota_recovery_probe_required"] = True
            state["last_quota_exhausted_at"] = now_wall
    if method_unavailable:
        print(
            "ROBINHOOD_PROVIDER_CAPABILITY_QUARANTINED "
            f"provider={provider.name} provider_kind=drpc "
            f"reason={reason} jsonrpc_code=-32601 "
            f"cooldown_seconds={cooldown_seconds:.0f}",
            flush=True,
        )
    if monthly_quota:
        print(
            "ROBINHOOD_PROVIDER_QUOTA_QUARANTINED "
            f"provider={provider.name} provider_kind=alchemy "
            f"reason={reason} reenable_after_utc={_iso_utc(quota_until)} "
            "recovery_requires_chain_id_and_block_head=true",
            flush=True,
        )


def _record_provider_verified(
    provider: failover.ProviderEndpoint,
    *,
    previous_name: str | None,
    latest_block: int,
) -> None:
    changed = previous_name is not None and previous_name != provider.name
    with failover._LOCK:
        state = failover._state_for_locked(provider.name)
        state["http_failures"] = 0
        state["chain_verified"] = True
        state["read_capability_verified"] = True
        state["chain_verifications"] = int(state.get("chain_verifications", 0) or 0) + 1
        state["read_capability_verifications"] = int(state.get("read_capability_verifications", 0) or 0) + 1
        state["last_chain_verified_at"] = time.time()
        state["last_read_capability_verified_at"] = time.time()
        state["last_verified_block"] = int(latest_block)
        state["last_capability_jsonrpc_code"] = None
        state["capability_quarantine_reason"] = None
        state["capability_quarantine_until"] = None
        if _provider_kind(provider) == "alchemy":
            state["quota_exhausted_until"] = None
            state["quota_recovery_probe_required"] = False
            state["last_quota_recovery_at"] = time.time()
        if changed:
            failover._ACTIVE_NAME = provider.name
            failover._GENERATION += 1
            state["failbacks_to"] = int(state.get("failbacks_to", 0) or 0) + 1
        current_generation = int(failover._GENERATION)
    print(
        "ROBINHOOD_PROVIDER_CAPABILITY_VERIFIED "
        f"provider={provider.name} provider_kind={_provider_kind(provider)} "
        f"generation={current_generation} failback={str(changed).lower()} "
        f"chain_id={runtime.ROBINHOOD_CHAIN_ID} eth_block_number=true latest_block={int(latest_block)}",
        flush=True,
    )


async def _probe_provider(
    rpc_self: Any,
    provider: failover.ProviderEndpoint,
) -> int:
    original = failover._ORIGINAL_RPC
    if original is None:
        raise RuntimeError("provider_inner_rpc_unavailable")
    rpc_self.rpc_url = provider.http
    raw = await original(rpc_self, "eth_chainId", [])
    text = str(raw).strip().lower()
    chain_id = int(text, 16) if text.startswith("0x") else int(text)
    if chain_id != runtime.ROBINHOOD_CHAIN_ID:
        raise RuntimeError("WrongRobinhoodChainId")
    head_raw = await original(rpc_self, "eth_blockNumber", [])
    head_text = str(head_raw).strip().lower()
    latest_block = int(head_text, 16) if head_text.startswith("0x") else int(head_text)
    if latest_block < 0:
        raise RuntimeError("InvalidRobinhoodBlockNumber")
    return latest_block


async def _verify_quota_recovery_if_needed(rpc_self: Any) -> bool:
    """Re-admit an exhausted Alchemy endpoint only after its month boundary and live proof."""
    active = failover.active_provider()
    now_monotonic = time.monotonic()
    candidate: failover.ProviderEndpoint | None = None
    with failover._LOCK:
        for item in failover.providers():
            state = failover._state_for_locked(item.name)
            if not bool(state.get("quota_recovery_probe_required", False)):
                continue
            if float(state.get("cooldown_until", 0.0) or 0.0) > now_monotonic:
                continue
            candidate = item
            break
    if candidate is None:
        return False
    if not _PROBE_LOCK.acquire(blocking=False):
        return False
    try:
        active = failover.active_provider()
        if active is None:
            return False
        with failover._LOCK:
            state = failover._state_for_locked(candidate.name)
            if not bool(state.get("quota_recovery_probe_required", False)):
                return False
            if float(state.get("cooldown_until", 0.0) or 0.0) > time.monotonic():
                return False
        old_url = str(getattr(rpc_self, "rpc_url", "") or "")
        try:
            latest_block = await _probe_provider(rpc_self, candidate)
        except Exception as exc:
            _record_probe_failure(candidate, exc)
            rpc_self.rpc_url = active.http if active is not None else old_url
            print(
                "ROBINHOOD_PROVIDER_QUOTA_RECOVERY_FAILED "
                f"provider={candidate.name} provider_kind={_provider_kind(candidate)} "
                f"error_type={type(exc).__name__}",
                flush=True,
            )
            return False
        preferred = _preferred_provider()
        previous_name = (
            active.name
            if preferred is not None
            and preferred.name == candidate.name
            and active.name != candidate.name
            else None
        )
        _record_provider_verified(candidate, previous_name=previous_name, latest_block=latest_block)
        restored = failover.active_provider()
        rpc_self.rpc_url = restored.http if restored is not None else old_url
        print(
            "ROBINHOOD_PROVIDER_QUOTA_RECOVERED "
            f"provider={candidate.name} provider_kind={_provider_kind(candidate)} "
            f"chain_id={runtime.ROBINHOOD_CHAIN_ID} latest_block={latest_block} "
            f"returned_to_pool=true",
            flush=True,
        )
        return True
    finally:
        _PROBE_LOCK.release()


async def _verify_preferred_if_needed(rpc_self: Any) -> bool:
    preferred = _preferred_provider()
    active = failover.active_provider()
    if preferred is None or active is None:
        return False
    with failover._LOCK:
        state = failover._state_for_locked(preferred.name)
        eligible = float(state.get("cooldown_until", 0.0) or 0.0) <= time.monotonic()
        already_verified = bool(state.get("chain_verified", False)) and bool(
            state.get("read_capability_verified", False)
        )
        quota_probe_required = bool(state.get("quota_recovery_probe_required", False))
    if not eligible or quota_probe_required:
        return False
    if active.name == preferred.name and already_verified:
        return True
    if not _PROBE_LOCK.acquire(blocking=False):
        return False
    try:
        preferred = _preferred_provider()
        active = failover.active_provider()
        if preferred is None or active is None:
            return False
        with failover._LOCK:
            state = failover._state_for_locked(preferred.name)
            if float(state.get("cooldown_until", 0.0) or 0.0) > time.monotonic():
                return False
            if bool(state.get("quota_recovery_probe_required", False)):
                return False
            if active.name == preferred.name and bool(state.get("chain_verified", False)) and bool(
                state.get("read_capability_verified", False)
            ):
                return True
        old_url = str(getattr(rpc_self, "rpc_url", "") or "")
        try:
            latest_block = await _probe_provider(rpc_self, preferred)
        except Exception as exc:
            _record_probe_failure(preferred, exc)
            rpc_self.rpc_url = active.http if active is not None else old_url
            print(
                "ROBINHOOD_PROVIDER_CAPABILITY_VERIFY_FAILED "
                f"provider={preferred.name} provider_kind={_provider_kind(preferred)} "
                f"error_type={type(exc).__name__}",
                flush=True,
            )
            return False
        _record_provider_verified(preferred, previous_name=active.name, latest_block=latest_block)
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
            await _verify_quota_recovery_if_needed(rpc_self)
            await _verify_preferred_if_needed(rpc_self)
        try:
            return await original(rpc_self, method, params)
        except Exception as exc:
            provider = failover.active_provider() if failover._is_pool_http(str(getattr(rpc_self, "rpc_url", "") or "")) else None
            if provider is not None:
                _record_request_failure(provider, method, type(exc).__name__)
            raise

    setattr(wrapped, "_roi_robinhood_provider_runtime_proof", True)
    return wrapped


def _status_with_runtime_proof() -> dict[str, Any]:
    assert _ORIGINAL_STATUS is not None
    result = dict(_ORIGINAL_STATUS())
    active = failover.active_provider()
    now_monotonic = time.monotonic()
    with failover._LOCK:
        traffic: dict[str, dict[str, Any]] = {}
        for item in failover.providers():
            state = failover._state_for_locked(item.name)
            quarantine_reason = state.get("capability_quarantine_reason")
            cooldown_until = float(state.get("cooldown_until", 0.0) or 0.0)
            quarantined = bool(quarantine_reason) and cooldown_until > now_monotonic
            quota_recovery_required = bool(state.get("quota_recovery_probe_required", False))
            traffic[item.name] = {
                "provider_kind": _provider_kind(item),
                "http_successes": int(state.get("http_successes", 0) or 0),
                "ws_successes": int(state.get("ws_successes", 0) or 0),
                "last_http_success_at": state.get("last_http_success_at"),
                "last_ws_success_at": state.get("last_ws_success_at"),
                "chain_verified": bool(state.get("chain_verified", False)),
                "read_capability_verified": bool(state.get("read_capability_verified", False)),
                "chain_verifications": int(state.get("chain_verifications", 0) or 0),
                "read_capability_verifications": int(state.get("read_capability_verifications", 0) or 0),
                "last_chain_verified_at": state.get("last_chain_verified_at"),
                "last_read_capability_verified_at": state.get("last_read_capability_verified_at"),
                "last_verified_block": state.get("last_verified_block"),
                "failbacks_to": int(state.get("failbacks_to", 0) or 0),
                "capability_quarantined": quarantined,
                "capability_quarantine_reason": quarantine_reason if quarantined else None,
                "capability_quarantine_remaining_seconds": (
                    max(0.0, round(cooldown_until - now_monotonic, 3)) if quarantined else 0.0
                ),
                "last_capability_jsonrpc_code": state.get("last_capability_jsonrpc_code"),
                "quota_recovery_probe_required": quota_recovery_required,
                "quota_exhausted_until_utc": _iso_utc(state.get("quota_exhausted_until")),
                "quota_recovery_due": quota_recovery_required and cooldown_until <= now_monotonic,
                "last_quota_exhausted_at": state.get("last_quota_exhausted_at"),
                "last_quota_recovery_at": state.get("last_quota_recovery_at"),
            }
    with _FAILURE_LOCK:
        request_failures = {
            f"{kind}:{method}": count for (kind, method), count in sorted(_REQUEST_FAILURE_COUNTS.items())
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
            "provider_request_failures": request_failures,
            "drpc_method_unavailable_cooldown_seconds": _drpc_method_unavailable_cooldown_seconds(),
            "alchemy_monthly_quota_recovery": {
                "automatic": True,
                "reenable_boundary": "next_utc_month",
                "generic_transient_429_uses_monthly_quarantine": False,
                "requires_chain_id_4663_probe": True,
                "requires_block_head_probe": True,
                "backup_provider_can_rejoin_without_becoming_primary": True,
            },
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
