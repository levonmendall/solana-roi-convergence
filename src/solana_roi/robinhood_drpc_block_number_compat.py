from __future__ import annotations

import threading
from functools import wraps
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse

import httpx


COMPAT_VERSION = "robinhood-drpc-block-number-compat-v1"
_INSTALLED = False
_LOCK = threading.Lock()
_FALLBACK_ACTIVE = False
_FALLBACK_SUCCESSES = 0
_FALLBACK_FAILURES = 0


def _is_drpc_url(value: str) -> bool:
    try:
        host = (urlparse(str(value or "").strip()).hostname or "").lower()
    except Exception:
        return False
    return host == "lb.drpc.live" or host.endswith(".drpc.live")


def _http_status(exc: BaseException) -> int | None:
    if not isinstance(exc, httpx.HTTPStatusError):
        return None
    try:
        return int(exc.response.status_code)
    except (TypeError, ValueError):
        return None


def _extract_block_number(block: Any) -> str:
    if not isinstance(block, dict):
        raise RuntimeError("InvalidDrpcLatestBlockResponse")
    raw = block.get("number")
    text = str(raw or "").strip().lower()
    if not text:
        raise RuntimeError("MissingDrpcLatestBlockNumber")
    value = int(text, 16) if text.startswith("0x") else int(text)
    if value < 0:
        raise RuntimeError("InvalidDrpcLatestBlockNumber")
    return hex(value)


def _fallback_active() -> bool:
    with _LOCK:
        return bool(_FALLBACK_ACTIVE)


def _record_success(*, activated: bool) -> None:
    global _FALLBACK_ACTIVE, _FALLBACK_SUCCESSES
    with _LOCK:
        _FALLBACK_ACTIVE = True
        _FALLBACK_SUCCESSES += 1
        successes = int(_FALLBACK_SUCCESSES)
    if activated or successes == 1 or successes % 100 == 0:
        print(
            "ROBINHOOD_DRPC_BLOCK_NUMBER_COMPATIBILITY "
            f"fallback=eth_getBlockByNumber active=true successes={successes}",
            flush=True,
        )


def _record_failure(exc: BaseException) -> None:
    global _FALLBACK_FAILURES
    with _LOCK:
        _FALLBACK_FAILURES += 1
        failures = int(_FALLBACK_FAILURES)
    if failures == 1 or failures % 20 == 0:
        print(
            "ROBINHOOD_DRPC_BLOCK_NUMBER_COMPATIBILITY_FAILED "
            f"fallback=eth_getBlockByNumber error_type={type(exc).__name__} failures={failures}",
            flush=True,
        )


async def _equivalent_latest_block(
    original: Callable[..., Awaitable[Any]],
    rpc_self: Any,
) -> str:
    block = await original(rpc_self, "eth_getBlockByNumber", ["latest", False])
    return _extract_block_number(block)


def _compat_rpc_wrapper(
    original: Callable[..., Awaitable[Any]],
) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(rpc_self: Any, method: str, params: list[Any]) -> Any:
        drpc = _is_drpc_url(str(getattr(rpc_self, "rpc_url", "") or ""))
        if drpc and str(method) == "eth_blockNumber" and _fallback_active():
            try:
                result = await _equivalent_latest_block(original, rpc_self)
            except Exception as exc:
                _record_failure(exc)
                raise
            _record_success(activated=False)
            return result

        try:
            return await original(rpc_self, method, params)
        except Exception as exc:
            if not (drpc and str(method) == "eth_blockNumber" and _http_status(exc) == 400):
                raise
            try:
                result = await _equivalent_latest_block(original, rpc_self)
            except Exception as fallback_exc:
                _record_failure(fallback_exc)
                raise exc from fallback_exc
            _record_success(activated=True)
            return result

    setattr(wrapped, "_roi_robinhood_drpc_block_number_compat", True)
    return wrapped


def install_robinhood_drpc_block_number_compat(runtime_rpc_owner: type[Any]) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    current = runtime_rpc_owner.rpc
    if not bool(getattr(current, "_roi_robinhood_drpc_block_number_compat", False)):
        runtime_rpc_owner.rpc = _compat_rpc_wrapper(current)  # type: ignore[method-assign]
    _INSTALLED = True


def status() -> dict[str, Any]:
    with _LOCK:
        return {
            "version": COMPAT_VERSION,
            "installed": _INSTALLED,
            "fallback_active": bool(_FALLBACK_ACTIVE),
            "fallback_method": "eth_getBlockByNumber_latest_false",
            "fallback_successes": int(_FALLBACK_SUCCESSES),
            "fallback_failures": int(_FALLBACK_FAILURES),
            "activation_condition": "drpc_eth_blockNumber_http_400_only",
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }


__all__ = [
    "COMPAT_VERSION",
    "install_robinhood_drpc_block_number_compat",
    "status",
]
