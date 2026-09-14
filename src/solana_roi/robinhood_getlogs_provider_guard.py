from __future__ import annotations

import asyncio
import os
import threading
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from functools import wraps
from typing import Any
from urllib.parse import urlparse

import httpx

from . import robinhood_chain_core as core


REPAIR_VERSION = "robinhood-getlogs-provider-guard-v6-validation-cloud-production-proof"
ALCHEMY_SAFE_MAX_BLOCKS = 10
MAX_CONFIGURED_BLOCKS = 10_000
DEFAULT_VALIDATION_CLOUD_TIMEOUT_SECONDS = 12.0
DEFAULT_VALIDATION_CLOUD_RETRIES = 1
MAX_VALIDATION_CLOUD_RETRIES = 3
MAX_VALIDATION_CLOUD_SPLIT_DEPTH = 3
ENV_MAX_BLOCKS = "ROBINHOOD_ETH_GET_LOGS_MAX_BLOCKS"
ENV_VALIDATION_CLOUD_RPC_URL = "ROBINHOOD_VALIDATION_CLOUD_RPC_URL"
ENV_VALIDATION_CLOUD_MAX_BLOCKS = "ROBINHOOD_VALIDATION_CLOUD_MAX_BLOCKS"
ENV_VALIDATION_CLOUD_TIMEOUT_SECONDS = "ROBINHOOD_VALIDATION_CLOUD_TIMEOUT_SECONDS"
ENV_VALIDATION_CLOUD_RETRIES = "ROBINHOOD_VALIDATION_CLOUD_RETRIES"

_INSTALLED = False
_ORIGINAL_GET_LOGS: Callable[..., Awaitable[list[dict[str, Any]]]] | None = None
_VALIDATION_CLOUD_VERIFIED_ENDPOINT: str | None = None
_PROOF_LOCK = threading.RLock()
_PROOF: dict[str, Any] = {
    "ranges_attempted": 0,
    "ranges_succeeded": 0,
    "ranges_failed": 0,
    "rpc_retries": 0,
    "range_splits": 0,
    "fallback_ranges": 0,
    "last_success_at": None,
    "last_failure_at": None,
    "last_failure_type": None,
    "last_failure_http_status": None,
    "last_failure_rpc_code": None,
    "last_failure_from_block": None,
    "last_failure_to_block": None,
}


class ValidationCloudRequestError(RuntimeError):
    """Sanitized Validation Cloud failure that never contains endpoint credentials."""

    def __init__(
        self,
        error_type: str,
        *,
        http_status: int | None = None,
        rpc_code: int | None = None,
        retryable: bool = False,
        split_worthy: bool = False,
    ) -> None:
        super().__init__(error_type)
        self.error_type = str(error_type or "ValidationCloudRequestError")
        self.http_status = int(http_status) if http_status is not None else None
        self.rpc_code = int(rpc_code) if rpc_code is not None else None
        self.retryable = bool(retryable)
        self.split_worthy = bool(split_worthy)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalized_endpoint(value: str) -> str:
    return str(value or "").strip().rstrip("/").lower()


def _is_alchemy_endpoint(rpc_url: str) -> bool:
    try:
        host = (urlparse(str(rpc_url)).hostname or "").lower()
    except Exception:
        return False
    return bool(
        host == "alchemy.com"
        or host.endswith(".alchemy.com")
        or host == "alchemyapi.io"
        or host.endswith(".alchemyapi.io")
    )


def _positive_block_limit(raw: str | None) -> int | None:
    value_raw = str(raw or "").strip()
    if not value_raw:
        return None
    try:
        value = int(value_raw)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    return min(MAX_CONFIGURED_BLOCKS, value)


def _positive_float(raw: str | None, default: float, *, maximum: float) -> float:
    try:
        value = float(raw) if raw is not None and str(raw).strip() else float(default)
    except (TypeError, ValueError):
        value = float(default)
    if value <= 0:
        value = float(default)
    return min(float(maximum), value)


def _bounded_int(raw: str | None, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(raw) if raw is not None and str(raw).strip() else int(default)
    except (TypeError, ValueError):
        value = int(default)
    return max(int(minimum), min(int(maximum), value))


def _explicit_max_blocks() -> int | None:
    return _positive_block_limit(os.getenv(ENV_MAX_BLOCKS))


def _validation_cloud_max_blocks() -> int | None:
    return _positive_block_limit(os.getenv(ENV_VALIDATION_CLOUD_MAX_BLOCKS))


def _validation_cloud_timeout_seconds() -> float:
    return _positive_float(
        os.getenv(ENV_VALIDATION_CLOUD_TIMEOUT_SECONDS),
        DEFAULT_VALIDATION_CLOUD_TIMEOUT_SECONDS,
        maximum=30.0,
    )


def _validation_cloud_retries() -> int:
    return _bounded_int(
        os.getenv(ENV_VALIDATION_CLOUD_RETRIES),
        DEFAULT_VALIDATION_CLOUD_RETRIES,
        minimum=0,
        maximum=MAX_VALIDATION_CLOUD_RETRIES,
    )


def _validation_cloud_rpc_url() -> str:
    raw = str(os.getenv(ENV_VALIDATION_CLOUD_RPC_URL) or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
    except Exception as exc:
        raise RuntimeError("invalid Validation Cloud Robinhood RPC URL") from exc
    if parsed.scheme.lower() != "https" or not parsed.netloc:
        raise RuntimeError("Validation Cloud Robinhood RPC URL must use https")
    return raw


def _validation_cloud_configured() -> bool:
    return bool(str(os.getenv(ENV_VALIDATION_CLOUD_RPC_URL) or "").strip())


def _primary_max_blocks(self: Any) -> int | None:
    explicit = _explicit_max_blocks()
    if explicit is not None:
        return explicit
    if _validation_cloud_configured():
        return _validation_cloud_max_blocks()
    return _fallback_provider_max_blocks(self)


def _fallback_provider_max_blocks(self: Any) -> int | None:
    """Return limits for the active governed fallback, independent of VC config.

    This separation is critical: a configured Validation Cloud endpoint must not hide
    the stricter limit of a fallback provider after Validation Cloud has already
    failed the exact range.
    """
    explicit = _explicit_max_blocks()
    if explicit is not None:
        return explicit
    if _is_alchemy_endpoint(str(getattr(self, "rpc_url", "") or "")):
        return ALCHEMY_SAFE_MAX_BLOCKS
    return None


def _provider_max_blocks(self: Any) -> int | None:
    """Compatibility alias for the first-choice provider limit."""
    return _primary_max_blocks(self)


def _inc(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_getlogs_guard_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


def _set_max(self: Any, name: str, value: int) -> None:
    attr = f"_roi_getlogs_guard_{name}"
    setattr(self, attr, max(int(getattr(self, attr, 0) or 0), int(value)))


def _proof_inc(name: str, amount: int = 1) -> int:
    with _PROOF_LOCK:
        _PROOF[name] = int(_PROOF.get(name, 0) or 0) + int(amount)
        return int(_PROOF[name])


def _proof_set(**values: Any) -> None:
    with _PROOF_LOCK:
        _PROOF.update(values)


def _proof_snapshot() -> dict[str, Any]:
    with _PROOF_LOCK:
        return dict(_PROOF)


def _reset_proof_state_for_tests() -> None:
    with _PROOF_LOCK:
        _PROOF.update(
            {
                "ranges_attempted": 0,
                "ranges_succeeded": 0,
                "ranges_failed": 0,
                "rpc_retries": 0,
                "range_splits": 0,
                "fallback_ranges": 0,
                "last_success_at": None,
                "last_failure_at": None,
                "last_failure_type": None,
                "last_failure_http_status": None,
                "last_failure_rpc_code": None,
                "last_failure_from_block": None,
                "last_failure_to_block": None,
            }
        )


def _is_getlogs_http_403(exc: BaseException) -> bool:
    return isinstance(exc, httpx.HTTPStatusError) and int(exc.response.status_code) == 403


def _next_request_id(self: Any) -> int:
    value = int(getattr(self, "_request_id", 0) or 0) + 1
    setattr(self, "_request_id", value)
    return value


def _validation_failure_fields(exc: BaseException) -> tuple[str, int | None, int | None]:
    if isinstance(exc, ValidationCloudRequestError):
        return exc.error_type, exc.http_status, exc.rpc_code
    if isinstance(exc, httpx.HTTPStatusError):
        return type(exc).__name__, int(exc.response.status_code), None
    return type(exc).__name__, None, None


def _log_validation_cloud(event: str, **fields: Any) -> None:
    # Never include the endpoint URL or raw exception text: both may contain secrets.
    pieces = [f"{key}={value}" for key, value in fields.items() if value is not None]
    print(f"ROBINHOOD_VALIDATION_CLOUD_{event} {' '.join(pieces)}".rstrip(), flush=True)


def _retryable_http_status(status: int) -> bool:
    return int(status) in {408, 425, 429, 500, 502, 503, 504}


async def _validation_cloud_rpc(self: Any, method: str, params: list[Any]) -> Any:
    url = _validation_cloud_rpc_url()
    if not url:
        raise ValidationCloudRequestError("NotConfigured")
    client = getattr(self, "client", None)
    if client is None:
        raise ValidationCloudRequestError("RpcClientUnavailable")

    retries = _validation_cloud_retries()
    timeout_seconds = _validation_cloud_timeout_seconds()
    last_error: ValidationCloudRequestError | None = None

    for attempt in range(retries + 1):
        try:
            response = await client.post(
                url,
                json={
                    "jsonrpc": "2.0",
                    "id": _next_request_id(self),
                    "method": method,
                    "params": params,
                },
                timeout=timeout_seconds,
            )
            status = int(response.status_code)
            if status >= 400:
                retryable = _retryable_http_status(status)
                raise ValidationCloudRequestError(
                    "HTTPStatusError",
                    http_status=status,
                    retryable=retryable,
                    split_worthy=status in {500, 502, 503, 504},
                )
            try:
                payload = response.json()
            except Exception as exc:
                raise ValidationCloudRequestError(
                    "InvalidJSON",
                    retryable=True,
                    split_worthy=True,
                ) from exc
            if not isinstance(payload, dict):
                raise ValidationCloudRequestError(
                    "InvalidJSONRPCPayload",
                    retryable=True,
                    split_worthy=True,
                )
            error = payload.get("error")
            if error is not None:
                code_raw = error.get("code") if isinstance(error, dict) else None
                try:
                    code = int(code_raw) if code_raw is not None else None
                except (TypeError, ValueError):
                    code = None
                # Provider-side capacity/range failures are safe to retry/split;
                # authentication, wrong-method, and malformed-request failures are not.
                retryable = code in {-32000, -32001, -32005, -32603}
                split_worthy = code in {-32000, -32005, -32603}
                raise ValidationCloudRequestError(
                    "JSONRPCError",
                    rpc_code=code,
                    retryable=retryable,
                    split_worthy=split_worthy,
                )
            return payload.get("result")
        except asyncio.CancelledError:
            raise
        except ValidationCloudRequestError as exc:
            last_error = exc
        except httpx.TimeoutException as exc:
            last_error = ValidationCloudRequestError(
                type(exc).__name__, retryable=True, split_worthy=True
            )
        except httpx.TransportError as exc:
            last_error = ValidationCloudRequestError(
                type(exc).__name__, retryable=True, split_worthy=True
            )
        except Exception as exc:
            last_error = ValidationCloudRequestError(type(exc).__name__)

        if last_error is None:
            break
        if attempt >= retries or not last_error.retryable:
            raise last_error

        _inc(self, "validation_cloud_rpc_retries")
        retry_count = _proof_inc("rpc_retries")
        _log_validation_cloud(
            "RETRY",
            method=method,
            attempt=attempt + 1,
            error_type=last_error.error_type,
            http_status=last_error.http_status,
            rpc_code=last_error.rpc_code,
            cumulative_retries=retry_count,
        )
        await asyncio.sleep(min(1.0, 0.25 * (2**attempt)))

    raise last_error or ValidationCloudRequestError("UnknownFailure")


async def _verify_validation_cloud_chain(self: Any) -> bool:
    global _VALIDATION_CLOUD_VERIFIED_ENDPOINT

    url = _validation_cloud_rpc_url()
    if not url:
        return False
    normalized = _normalized_endpoint(url)
    if _VALIDATION_CLOUD_VERIFIED_ENDPOINT == normalized:
        return True

    raw_chain_id = await _validation_cloud_rpc(self, "eth_chainId", [])
    try:
        chain_id = int(str(raw_chain_id), 16)
    except (TypeError, ValueError) as exc:
        raise ValidationCloudRequestError("InvalidChainId") from exc
    if chain_id != core.ROBINHOOD_CHAIN_ID:
        raise ValidationCloudRequestError("WrongChainId")

    raw_head = await _validation_cloud_rpc(self, "eth_blockNumber", [])
    try:
        latest_block = int(str(raw_head), 16)
    except (TypeError, ValueError) as exc:
        raise ValidationCloudRequestError("BlockHeadUnavailable") from exc

    _VALIDATION_CLOUD_VERIFIED_ENDPOINT = normalized
    _log_validation_cloud(
        "CAPABILITY_VERIFIED",
        chain_id=chain_id,
        latest_block=latest_block,
        timeout_seconds=_validation_cloud_timeout_seconds(),
        retries=_validation_cloud_retries(),
    )
    return True


async def _validation_cloud_get_logs(
    self: Any,
    *,
    from_block: int,
    to_block: int,
    addresses: list[str] | tuple[str, ...] | None,
    topics: list[Any] | None,
    _split_depth: int = 0,
) -> list[dict[str, Any]]:
    if not await _verify_validation_cloud_chain(self):
        raise ValidationCloudRequestError("NotConfigured")

    query: dict[str, Any] = {
        "fromBlock": hex(max(0, int(from_block))),
        "toBlock": hex(max(0, int(to_block))),
    }
    if addresses:
        query["address"] = list(addresses) if len(addresses) > 1 else addresses[0]
    if topics is not None:
        query["topics"] = topics

    try:
        result = await _validation_cloud_rpc(self, "eth_getLogs", [query])
    except ValidationCloudRequestError as exc:
        start = int(from_block)
        end = int(to_block)
        if exc.split_worthy and start < end and _split_depth < MAX_VALIDATION_CLOUD_SPLIT_DEPTH:
            midpoint = start + ((end - start) // 2)
            _inc(self, "validation_cloud_range_splits")
            split_count = _proof_inc("range_splits")
            _log_validation_cloud(
                "RANGE_SPLIT",
                from_block=start,
                to_block=end,
                span=end - start + 1,
                left_to_block=midpoint,
                right_from_block=midpoint + 1,
                error_type=exc.error_type,
                http_status=exc.http_status,
                rpc_code=exc.rpc_code,
                cumulative_splits=split_count,
            )
            left = await _validation_cloud_get_logs(
                self,
                from_block=start,
                to_block=midpoint,
                addresses=addresses,
                topics=topics,
                _split_depth=_split_depth + 1,
            )
            right = await _validation_cloud_get_logs(
                self,
                from_block=midpoint + 1,
                to_block=end,
                addresses=addresses,
                topics=topics,
                _split_depth=_split_depth + 1,
            )
            return left + right
        raise

    if result is None:
        return []
    if not isinstance(result, list):
        raise ValidationCloudRequestError("NonListGetLogsResult")
    return list(result)


async def _failover_from_getlogs_403(self: Any) -> bool:
    """Quarantine a refusing provider and require fresh Robinhood chain proof on its peer."""
    try:
        from . import robinhood_provider_failover as failover

        current_url = _normalized_endpoint(str(getattr(self, "rpc_url", "") or ""))
        provider = failover.active_provider()
        if provider is None or _normalized_endpoint(provider.http) != current_url:
            provider = next(
                (
                    item
                    for item in failover.providers()
                    if _normalized_endpoint(item.http) == current_url
                ),
                None,
            )
        if provider is None:
            return False

        replacement = failover._switch_from(
            provider.name,
            failure_type="EthGetLogsHTTP403",
            immediate=True,
            transport_kind="http",
        )
        if replacement is None or replacement.name == provider.name:
            return False

        original_rpc = failover._ORIGINAL_RPC
        if original_rpc is None:
            failover._switch_from(
                replacement.name,
                failure_type="MissingRobinhoodChainVerifier",
                immediate=True,
                transport_kind="http",
            )
            return False

        if not await failover._verify_candidate_chain(original_rpc, self, replacement):
            return False
        return True
    except Exception:
        return False


async def _request_range(
    self: Any,
    *,
    from_block: int,
    to_block: int,
    addresses: list[str] | tuple[str, ...] | None,
    topics: list[Any] | None,
) -> list[dict[str, Any]]:
    """Serve one exact range through the existing governed provider path."""
    if _ORIGINAL_GET_LOGS is None:
        raise RuntimeError("Robinhood eth_getLogs provider guard is not installed")

    try:
        return await _ORIGINAL_GET_LOGS(
            self,
            from_block=from_block,
            to_block=to_block,
            addresses=addresses,
            topics=topics,
        )
    except Exception as exc:
        if not _is_getlogs_http_403(exc):
            raise
        _inc(self, "http_403s")
        if not await _failover_from_getlogs_403(self):
            _inc(self, "http_403_fail_closed")
            raise

        _inc(self, "http_403_failovers")
        # Validation Cloud has already failed this exact range if we arrived here
        # through _dispatch_range. Do not recursively re-enter it. Apply the newly
        # active fallback provider's own range constraints (notably Alchemy's ten).
        return await _fallback_bounded_get_logs(
            self,
            from_block=from_block,
            to_block=to_block,
            addresses=addresses,
            topics=topics,
        )


async def _fallback_bounded_get_logs(
    self: Any,
    *,
    from_block: int,
    to_block: int,
    addresses: list[str] | tuple[str, ...] | None,
    topics: list[Any] | None,
) -> list[dict[str, Any]]:
    start = int(from_block)
    end = int(to_block)
    limit = _fallback_provider_max_blocks(self)
    if end < start or limit is None or (end - start + 1) <= limit:
        return await _request_range(
            self,
            from_block=start,
            to_block=end,
            addresses=addresses,
            topics=topics,
        )

    _inc(self, "fallback_ranges_chunked")
    rows: list[dict[str, Any]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + limit - 1)
        rows.extend(
            await _request_range(
                self,
                from_block=cursor,
                to_block=chunk_end,
                addresses=addresses,
                topics=topics,
            )
        )
        cursor = chunk_end + 1
    return rows


async def _dispatch_range(
    self: Any,
    *,
    from_block: int,
    to_block: int,
    addresses: list[str] | tuple[str, ...] | None,
    topics: list[Any] | None,
) -> list[dict[str, Any]]:
    """Prefer Validation Cloud, then one exact-range governed fallback path."""
    start = int(from_block)
    end = int(to_block)
    if _validation_cloud_configured():
        _inc(self, "validation_cloud_requests")
        attempted = _proof_inc("ranges_attempted")
        started = time.monotonic()
        try:
            rows = await _validation_cloud_get_logs(
                self,
                from_block=start,
                to_block=end,
                addresses=addresses,
                topics=topics,
            )
            _inc(self, "validation_cloud_successes")
            succeeded = _proof_inc("ranges_succeeded")
            _proof_set(
                last_success_at=_utcnow(),
                last_failure_type=None,
                last_failure_http_status=None,
                last_failure_rpc_code=None,
            )
            _log_validation_cloud(
                "GETLOGS_SUCCESS",
                from_block=start,
                to_block=end,
                span=max(0, end - start + 1),
                result_count=len(rows),
                latency_ms=round((time.monotonic() - started) * 1000.0, 1),
                ranges_attempted=attempted,
                ranges_succeeded=succeeded,
                ranges_failed=_proof_snapshot().get("ranges_failed", 0),
            )
            return rows
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _inc(self, "validation_cloud_failures")
            failed = _proof_inc("ranges_failed")
            error_type, http_status, rpc_code = _validation_failure_fields(exc)
            _proof_set(
                last_failure_at=_utcnow(),
                last_failure_type=error_type,
                last_failure_http_status=http_status,
                last_failure_rpc_code=rpc_code,
                last_failure_from_block=start,
                last_failure_to_block=end,
            )
            _log_validation_cloud(
                "GETLOGS_FAILED",
                from_block=start,
                to_block=end,
                span=max(0, end - start + 1),
                latency_ms=round((time.monotonic() - started) * 1000.0, 1),
                error_type=error_type,
                http_status=http_status,
                rpc_code=rpc_code,
                ranges_attempted=attempted,
                ranges_succeeded=_proof_snapshot().get("ranges_succeeded", 0),
                ranges_failed=failed,
            )

    _inc(self, "validation_cloud_fallback_ranges")
    fallback_count = _proof_inc("fallback_ranges")
    _log_validation_cloud(
        "FALLBACK",
        from_block=start,
        to_block=end,
        span=max(0, end - start + 1),
        cumulative_fallback_ranges=fallback_count,
    )
    return await _fallback_bounded_get_logs(
        self,
        from_block=start,
        to_block=end,
        addresses=addresses,
        topics=topics,
    )


async def _provider_bounded_get_logs(
    self: Any,
    *,
    from_block: int,
    to_block: int,
    addresses: list[str] | tuple[str, ...] | None = None,
    topics: list[Any] | None = None,
) -> list[dict[str, Any]]:
    if _ORIGINAL_GET_LOGS is None:
        raise RuntimeError("Robinhood eth_getLogs provider guard is not installed")

    start = int(from_block)
    end = int(to_block)
    if end < start:
        return await _dispatch_range(
            self,
            from_block=start,
            to_block=end,
            addresses=addresses,
            topics=topics,
        )

    requested_blocks = end - start + 1
    _set_max(self, "max_requested_blocks", requested_blocks)
    limit = _primary_max_blocks(self)
    if limit is None or requested_blocks <= limit:
        _set_max(self, "max_sent_blocks", requested_blocks)
        return await _dispatch_range(
            self,
            from_block=start,
            to_block=end,
            addresses=addresses,
            topics=topics,
        )

    _inc(self, "ranges_chunked")
    rows: list[dict[str, Any]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + limit - 1)
        chunk_blocks = chunk_end - cursor + 1
        _inc(self, "provider_requests")
        _set_max(self, "max_sent_blocks", chunk_blocks)
        rows.extend(
            await _dispatch_range(
                self,
                from_block=cursor,
                to_block=chunk_end,
                addresses=addresses,
                topics=topics,
            )
        )
        cursor = chunk_end + 1
    return rows


setattr(_provider_bounded_get_logs, "_roi_robinhood_getlogs_provider_guard", True)


def install_robinhood_getlogs_provider_guard() -> None:
    global _INSTALLED, _ORIGINAL_GET_LOGS
    current = core.RobinhoodRpc.get_logs
    if bool(getattr(current, "_roi_robinhood_getlogs_provider_guard", False)):
        _INSTALLED = True
        return
    _ORIGINAL_GET_LOGS = current
    wrapped = wraps(current)(_provider_bounded_get_logs)
    setattr(wrapped, "_roi_robinhood_getlogs_provider_guard", True)
    core.RobinhoodRpc.get_logs = wrapped  # type: ignore[method-assign]
    _INSTALLED = True


def status() -> dict[str, Any]:
    proof = _proof_snapshot()
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "alchemy_detected_max_blocks": ALCHEMY_SAFE_MAX_BLOCKS,
        "configured_max_blocks_env": ENV_MAX_BLOCKS,
        "validation_cloud_rpc_env": ENV_VALIDATION_CLOUD_RPC_URL,
        "validation_cloud_max_blocks_env": ENV_VALIDATION_CLOUD_MAX_BLOCKS,
        "validation_cloud_timeout_env": ENV_VALIDATION_CLOUD_TIMEOUT_SECONDS,
        "validation_cloud_retries_env": ENV_VALIDATION_CLOUD_RETRIES,
        "validation_cloud_timeout_seconds": _validation_cloud_timeout_seconds(),
        "validation_cloud_retries": _validation_cloud_retries(),
        "validation_cloud_configured": _validation_cloud_configured(),
        "validation_cloud_chain_verified": _VALIDATION_CLOUD_VERIFIED_ENDPOINT is not None,
        "validation_cloud_getlogs_only": True,
        "validation_cloud_preserves_primary_ws": True,
        "validation_cloud_dispatch_above_capability_repair": True,
        "validation_cloud_fallback_reentry_prevented": True,
        "fallback_provider_limits_reapplied": True,
        "validation_cloud_proof": proof,
        "prevents_oversized_provider_requests": True,
        "inclusive_block_range_accounting": True,
        "http_403_same_range_failover": True,
        "replacement_provider_limits_reapplied": True,
        "replacement_chain_id_verified": True,
        "contiguous_frontier_fail_closed": True,
        "changes_strategy_thresholds": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "ALCHEMY_SAFE_MAX_BLOCKS",
    "DEFAULT_VALIDATION_CLOUD_RETRIES",
    "DEFAULT_VALIDATION_CLOUD_TIMEOUT_SECONDS",
    "ENV_MAX_BLOCKS",
    "ENV_VALIDATION_CLOUD_MAX_BLOCKS",
    "ENV_VALIDATION_CLOUD_RETRIES",
    "ENV_VALIDATION_CLOUD_RPC_URL",
    "ENV_VALIDATION_CLOUD_TIMEOUT_SECONDS",
    "REPAIR_VERSION",
    "ValidationCloudRequestError",
    "_dispatch_range",
    "_failover_from_getlogs_403",
    "_fallback_bounded_get_logs",
    "_fallback_provider_max_blocks",
    "_is_alchemy_endpoint",
    "_is_getlogs_http_403",
    "_provider_bounded_get_logs",
    "_provider_max_blocks",
    "_request_range",
    "_validation_cloud_get_logs",
    "_validation_cloud_rpc",
    "_verify_validation_cloud_chain",
    "install_robinhood_getlogs_provider_guard",
    "status",
]
