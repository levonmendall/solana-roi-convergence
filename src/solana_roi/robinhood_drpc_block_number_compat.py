from __future__ import annotations

import threading
from functools import wraps
from typing import Any, Awaitable, Callable
from urllib.parse import unquote, urlparse

import httpx


COMPAT_VERSION = "robinhood-drpc-block-number-compat-v3-ogrpc-routing"
_INSTALLED = False
_LOCK = threading.Lock()
_MODE: str | None = None
_FALLBACK_SUCCESSES = 0
_FALLBACK_FAILURES = 0

_MODE_PARAMLESS = "eth_blockNumber_without_params"
_MODE_FINALIZED = "eth_getBlockByNumber_finalized_false"
_MODE_OGRPC = "ogrpc_header_robinhood"
_OGRPC_URL = "https://lb.drpc.org/ogrpc?network=robinhood"


def _is_drpc_url(value: str) -> bool:
    try:
        host = (urlparse(str(value or "").strip()).hostname or "").lower()
    except Exception:
        return False
    return host in {"lb.drpc.live", "lb.drpc.org"} or host.endswith(".drpc.live") or host.endswith(".drpc.org")


def _path_key(value: str) -> str:
    try:
        parsed = urlparse(str(value or "").strip())
        host = (parsed.hostname or "").lower()
        if host != "lb.drpc.live":
            return ""
        parts = [unquote(item) for item in parsed.path.split("/") if item]
    except Exception:
        return ""
    if len(parts) < 2 or parts[0].lower() != "robinhood":
        return ""
    return str(parts[-1]).strip()


def _http_status(exc: BaseException) -> int | None:
    if not isinstance(exc, httpx.HTTPStatusError):
        return None
    try:
        return int(exc.response.status_code)
    except (TypeError, ValueError):
        return None


def _drpc_code(exc: BaseException) -> int | None:
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


def _extract_quantity(raw: Any, *, missing_error: str, invalid_error: str) -> str:
    text = str(raw or "").strip().lower()
    if not text:
        raise RuntimeError(missing_error)
    try:
        value = int(text, 16) if text.startswith("0x") else int(text)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(invalid_error) from exc
    if value < 0:
        raise RuntimeError(invalid_error)
    return hex(value)


def _extract_block_number(block: Any) -> str:
    if not isinstance(block, dict):
        raise RuntimeError("InvalidDrpcFinalizedBlockResponse")
    return _extract_quantity(
        block.get("number"),
        missing_error="MissingDrpcFinalizedBlockNumber",
        invalid_error="InvalidDrpcFinalizedBlockNumber",
    )


def _mode() -> str | None:
    with _LOCK:
        return _MODE


def _record_success(*, mode: str, activated: bool) -> None:
    global _MODE, _FALLBACK_SUCCESSES
    with _LOCK:
        _MODE = str(mode)
        _FALLBACK_SUCCESSES += 1
        successes = int(_FALLBACK_SUCCESSES)
    if activated or successes == 1 or successes % 100 == 0:
        print(
            "ROBINHOOD_DRPC_BLOCK_NUMBER_COMPATIBILITY "
            f"mode={mode} active=true successes={successes}",
            flush=True,
        )


def _record_failure(*, stage: str, exc: BaseException) -> None:
    global _FALLBACK_FAILURES
    with _LOCK:
        _FALLBACK_FAILURES += 1
        failures = int(_FALLBACK_FAILURES)
    if failures <= 6 or failures % 20 == 0:
        status = _http_status(exc)
        code = _drpc_code(exc)
        safe_status = str(status) if status is not None else "none"
        safe_code = str(code) if code is not None else "none"
        print(
            "ROBINHOOD_DRPC_BLOCK_NUMBER_COMPATIBILITY_FAILED "
            f"stage={stage} error_type={type(exc).__name__} "
            f"http_status={safe_status} drpc_code={safe_code} failures={failures}",
            flush=True,
        )


async def _decode_response(response: httpx.Response) -> Any:
    response.raise_for_status()
    body = response.json()
    if not isinstance(body, dict):
        raise RuntimeError("InvalidDrpcCompatibilityResponse")
    error = body.get("error")
    if error is not None:
        raise RuntimeError("DrpcCompatibilityJsonRpcError")
    return body.get("result")


async def _direct_json_rpc(
    rpc_self: Any,
    *,
    method: str,
    params: list[Any] | None,
) -> Any:
    payload: dict[str, Any] = {
        "jsonrpc": "2.0",
        "id": int(getattr(rpc_self, "_request_id", 0) or 0) + 1,
        "method": method,
    }
    if params is not None:
        payload["params"] = params
    setattr(rpc_self, "_request_id", int(payload["id"]))
    response = await rpc_self.client.post(str(getattr(rpc_self, "rpc_url", "") or ""), json=payload)
    return await _decode_response(response)


async def _ogrpc_json_rpc(
    rpc_self: Any,
    *,
    method: str,
    params: list[Any],
) -> Any:
    key = _path_key(str(getattr(rpc_self, "rpc_url", "") or ""))
    if not key:
        raise RuntimeError("MissingDrpcPathKeyForOgrpc")
    payload: dict[str, Any] = {
        "jsonrpc": "2.0",
        "id": int(getattr(rpc_self, "_request_id", 0) or 0) + 1,
        "method": str(method),
        "params": list(params),
    }
    setattr(rpc_self, "_request_id", int(payload["id"]))
    response = await rpc_self.client.post(
        _OGRPC_URL,
        json=payload,
        headers={"Drpc-Key": key, "Content-Type": "application/json"},
    )
    return await _decode_response(response)


async def _paramless_block_number(rpc_self: Any) -> str:
    raw = await _direct_json_rpc(rpc_self, method="eth_blockNumber", params=None)
    return _extract_quantity(
        raw,
        missing_error="MissingDrpcParamlessBlockNumber",
        invalid_error="InvalidDrpcParamlessBlockNumber",
    )


async def _finalized_block_number(rpc_self: Any) -> str:
    block = await _direct_json_rpc(
        rpc_self,
        method="eth_getBlockByNumber",
        params=["finalized", False],
    )
    return _extract_block_number(block)


async def _ogrpc_block_number(rpc_self: Any) -> str:
    raw = await _ogrpc_json_rpc(rpc_self, method="eth_blockNumber", params=[])
    return _extract_quantity(
        raw,
        missing_error="MissingDrpcOgrpcBlockNumber",
        invalid_error="InvalidDrpcOgrpcBlockNumber",
    )


async def _read_compatible_block_number(rpc_self: Any, *, preferred_mode: str | None) -> tuple[str, str]:
    if preferred_mode == _MODE_PARAMLESS:
        return await _paramless_block_number(rpc_self), _MODE_PARAMLESS
    if preferred_mode == _MODE_FINALIZED:
        return await _finalized_block_number(rpc_self), _MODE_FINALIZED
    if preferred_mode == _MODE_OGRPC:
        return await _ogrpc_block_number(rpc_self), _MODE_OGRPC

    try:
        return await _paramless_block_number(rpc_self), _MODE_PARAMLESS
    except Exception as exc:
        _record_failure(stage=_MODE_PARAMLESS, exc=exc)

    try:
        return await _finalized_block_number(rpc_self), _MODE_FINALIZED
    except Exception as exc:
        _record_failure(stage=_MODE_FINALIZED, exc=exc)

    try:
        return await _ogrpc_block_number(rpc_self), _MODE_OGRPC
    except Exception as exc:
        _record_failure(stage=_MODE_OGRPC, exc=exc)
        raise


def _compat_rpc_wrapper(
    original: Callable[..., Awaitable[Any]],
) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(rpc_self: Any, method: str, params: list[Any]) -> Any:
        drpc = _is_drpc_url(str(getattr(rpc_self, "rpc_url", "") or ""))
        active_mode = _mode()

        if drpc and active_mode == _MODE_OGRPC:
            try:
                result = await _ogrpc_json_rpc(rpc_self, method=str(method), params=list(params))
                if str(method) == "eth_blockNumber":
                    result = _extract_quantity(
                        result,
                        missing_error="MissingDrpcOgrpcBlockNumber",
                        invalid_error="InvalidDrpcOgrpcBlockNumber",
                    )
                    _record_success(mode=_MODE_OGRPC, activated=False)
                return result
            except Exception as exc:
                _record_failure(stage=_MODE_OGRPC, exc=exc)
                raise

        if drpc and str(method) == "eth_blockNumber" and active_mode is not None:
            try:
                result, mode = await _read_compatible_block_number(rpc_self, preferred_mode=active_mode)
            except Exception as exc:
                _record_failure(stage=str(active_mode), exc=exc)
                raise
            _record_success(mode=mode, activated=False)
            return result

        try:
            return await original(rpc_self, method, params)
        except Exception as exc:
            if not (drpc and str(method) == "eth_blockNumber" and _http_status(exc) == 400):
                raise
            try:
                result, mode = await _read_compatible_block_number(rpc_self, preferred_mode=None)
            except Exception as fallback_exc:
                raise exc from fallback_exc
            _record_success(mode=mode, activated=True)
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
            "fallback_active": _MODE is not None,
            "fallback_mode": _MODE,
            "fallback_successes": int(_FALLBACK_SUCCESSES),
            "fallback_failures": int(_FALLBACK_FAILURES),
            "activation_condition": "drpc_eth_blockNumber_http_400_only",
            "documented_request_forms": [
                _MODE_PARAMLESS,
                _MODE_FINALIZED,
                _MODE_OGRPC,
            ],
            "ogrpc_routes_all_drpc_http_after_proof": True,
            "logs_only_numeric_drpc_error_code": True,
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
