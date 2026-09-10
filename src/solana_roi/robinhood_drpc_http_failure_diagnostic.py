from __future__ import annotations

from functools import wraps
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse

from . import robinhood_provider_failover as failover


DIAGNOSTIC_VERSION = "robinhood-drpc-http-failure-proof-v1"
_INSTALLED = False
_ALLOWED_PROBE_METHODS = {"eth_chainId", "eth_blockNumber"}


def _is_drpc_url(value: str) -> bool:
    try:
        host = (urlparse(str(value or "").strip()).hostname or "").lower()
    except Exception:
        return False
    return host == "lb.drpc.live" or host.endswith(".drpc.live")


def _safe_method(method: str) -> str:
    value = str(method or "")
    return value if value in _ALLOWED_PROBE_METHODS else "other"


def _safe_http_status(exc: BaseException) -> int | None:
    response = getattr(exc, "response", None)
    value = getattr(response, "status_code", None)
    try:
        status = int(value)
    except (TypeError, ValueError):
        return None
    return status if 100 <= status <= 599 else None


def _diagnostic_rpc_wrapper(
    original: Callable[..., Awaitable[Any]],
) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(rpc_self: Any, method: str, params: list[Any]) -> Any:
        try:
            return await original(rpc_self, method, params)
        except Exception as exc:
            if _is_drpc_url(str(getattr(rpc_self, "rpc_url", "") or "")):
                status = _safe_http_status(exc)
                print(
                    "ROBINHOOD_DRPC_HTTP_FAILURE "
                    f"method={_safe_method(method)} error_type={type(exc).__name__} "
                    f"http_status={status if status is not None else 'none'}",
                    flush=True,
                )
            raise

    setattr(wrapped, "_roi_robinhood_drpc_http_failure_diagnostic", True)
    return wrapped


def install_robinhood_drpc_http_failure_diagnostic() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    current = failover._ORIGINAL_RPC
    if current is None:
        return
    if not bool(getattr(current, "_roi_robinhood_drpc_http_failure_diagnostic", False)):
        failover._ORIGINAL_RPC = _diagnostic_rpc_wrapper(current)
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": DIAGNOSTIC_VERSION,
        "installed": _INSTALLED,
        "logs_endpoint": False,
        "logs_credentials": False,
        "logs_headers": False,
        "logs_response_body": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "DIAGNOSTIC_VERSION",
    "install_robinhood_drpc_http_failure_diagnostic",
    "status",
]
