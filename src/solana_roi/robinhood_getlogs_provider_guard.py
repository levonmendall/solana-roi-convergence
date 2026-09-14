from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any
from urllib.parse import urlparse

import httpx

from . import robinhood_chain_core as core


REPAIR_VERSION = "robinhood-getlogs-provider-guard-v5-validation-cloud-composed-dispatch"
ALCHEMY_SAFE_MAX_BLOCKS = 10
MAX_CONFIGURED_BLOCKS = 10_000
ENV_MAX_BLOCKS = "ROBINHOOD_ETH_GET_LOGS_MAX_BLOCKS"
ENV_VALIDATION_CLOUD_RPC_URL = "ROBINHOOD_VALIDATION_CLOUD_RPC_URL"
ENV_VALIDATION_CLOUD_MAX_BLOCKS = "ROBINHOOD_VALIDATION_CLOUD_MAX_BLOCKS"

_INSTALLED = False
_ORIGINAL_GET_LOGS: Callable[..., Awaitable[list[dict[str, Any]]]] | None = None
_VALIDATION_CLOUD_VERIFIED_ENDPOINT: str | None = None


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


def _explicit_max_blocks() -> int | None:
    return _positive_block_limit(os.getenv(ENV_MAX_BLOCKS))


def _validation_cloud_max_blocks() -> int | None:
    return _positive_block_limit(os.getenv(ENV_VALIDATION_CLOUD_MAX_BLOCKS))


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


def _provider_max_blocks(self: Any) -> int | None:
    explicit = _explicit_max_blocks()
    if explicit is not None:
        return explicit
    if str(os.getenv(ENV_VALIDATION_CLOUD_RPC_URL) or "").strip():
        validation_limit = _validation_cloud_max_blocks()
        if validation_limit is not None:
            return validation_limit
    if _is_alchemy_endpoint(str(getattr(self, "rpc_url", "") or "")):
        # The connected Robinhood production app is on the Alchemy Free tier, whose
        # eth_getLogs range is capped at ten inclusive blocks. Keep ten as the safe
        # automatic default even if the account is later upgraded; operators can
        # raise the cap explicitly through ENV_MAX_BLOCKS after validating the plan.
        return ALCHEMY_SAFE_MAX_BLOCKS
    return None


def _inc(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_getlogs_guard_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


def _set_max(self: Any, name: str, value: int) -> None:
    attr = f"_roi_getlogs_guard_{name}"
    setattr(self, attr, max(int(getattr(self, attr, 0) or 0), int(value)))


def _is_getlogs_http_403(exc: BaseException) -> bool:
    return isinstance(exc, httpx.HTTPStatusError) and int(exc.response.status_code) == 403


def _next_request_id(self: Any) -> int:
    value = int(getattr(self, "_request_id", 0) or 0) + 1
    setattr(self, "_request_id", value)
    return value


async def _validation_cloud_rpc(self: Any, method: str, params: list[Any]) -> Any:
    url = _validation_cloud_rpc_url()
    if not url:
        raise RuntimeError("Validation Cloud Robinhood RPC is not configured")
    client = getattr(self, "client", None)
    if client is None:
        raise RuntimeError("Robinhood RPC client unavailable")
    response = await client.post(
        url,
        json={
            "jsonrpc": "2.0",
            "id": _next_request_id(self),
            "method": method,
            "params": params,
        },
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError(f"Validation Cloud {method} returned invalid JSON-RPC payload")
    error = payload.get("error")
    if error is not None:
        code = error.get("code") if isinstance(error, dict) else None
        raise RuntimeError(f"Validation Cloud {method} RPC error code={code}")
    return payload.get("result")


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
        raise RuntimeError("Validation Cloud returned invalid Robinhood chain id") from exc
    if chain_id != core.ROBINHOOD_CHAIN_ID:
        raise RuntimeError(
            f"Validation Cloud Robinhood chain mismatch: expected {core.ROBINHOOD_CHAIN_ID}, got {chain_id}"
        )

    raw_head = await _validation_cloud_rpc(self, "eth_blockNumber", [])
    try:
        int(str(raw_head), 16)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Validation Cloud Robinhood block head unavailable") from exc

    _VALIDATION_CLOUD_VERIFIED_ENDPOINT = normalized
    return True


async def _validation_cloud_get_logs(
    self: Any,
    *,
    from_block: int,
    to_block: int,
    addresses: list[str] | tuple[str, ...] | None,
    topics: list[Any] | None,
) -> list[dict[str, Any]]:
    if not await _verify_validation_cloud_chain(self):
        raise RuntimeError("Validation Cloud Robinhood RPC is not configured")

    query: dict[str, Any] = {
        "fromBlock": hex(max(0, int(from_block))),
        "toBlock": hex(max(0, int(to_block))),
    }
    if addresses:
        query["address"] = list(addresses) if len(addresses) > 1 else addresses[0]
    if topics is not None:
        query["topics"] = topics

    result = await _validation_cloud_rpc(self, "eth_getLogs", [query])
    if result is None:
        return []
    if not isinstance(result, list):
        raise RuntimeError("Validation Cloud eth_getLogs returned non-list result")
    return list(result)


async def _failover_from_getlogs_403(self: Any) -> bool:
    """Quarantine a refusing provider and require fresh Robinhood chain proof on its peer.

    The provider failover module imports this guard, so the import remains local to
    avoid a module-initialization cycle. Recovery deliberately reuses the canonical
    provider pool, cooldown, and chain-id verifier rather than creating second truths.
    """
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
            # A replacement may not remain active without a canonical chain verifier.
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
        # Recovery itself must never convert a provider denial into apparent success.
        return False


async def _request_range(
    self: Any,
    *,
    from_block: int,
    to_block: int,
    addresses: list[str] | tuple[str, ...] | None,
    topics: list[Any] | None,
) -> list[dict[str, Any]]:
    """Serve one exact range through the existing governed provider path.

    Validation Cloud dispatch intentionally lives one level above this function in
    `_dispatch_range`. Production installs the capability repair by replacing this
    function, so keeping the dedicated archive provider above that replacement makes
    the production composition deterministic instead of import-order dependent.
    """
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

        # Retry the exact same contiguous range. Re-entering the provider guard is
        # intentional: the verified replacement can have a stricter range limit
        # (Alchemy is ten blocks), so that limit is applied before any retry is sent.
        # No caller cursor/watermark can observe success until the full range succeeds.
        _inc(self, "http_403_failovers")
        return await _provider_bounded_get_logs(
            self,
            from_block=from_block,
            to_block=to_block,
            addresses=addresses,
            topics=topics,
        )


async def _dispatch_range(
    self: Any,
    *,
    from_block: int,
    to_block: int,
    addresses: list[str] | tuple[str, ...] | None,
    topics: list[Any] | None,
) -> list[dict[str, Any]]:
    """Prefer the dedicated Validation Cloud log plane, then exact-range fallback.

    This function is never replaced by the later capability-repair installer. That
    keeps Validation Cloud log routing active in the fully composed production path
    while preserving the capability layer as the governed fallback authority.
    """
    if str(os.getenv(ENV_VALIDATION_CLOUD_RPC_URL) or "").strip():
        _inc(self, "validation_cloud_requests")
        try:
            rows = await _validation_cloud_get_logs(
                self,
                from_block=from_block,
                to_block=to_block,
                addresses=addresses,
                topics=topics,
            )
            _inc(self, "validation_cloud_successes")
            return rows
        except asyncio.CancelledError:
            raise
        except Exception:
            # Do not advance the caller frontier. The exact same interval is handed
            # to the composed governed provider path below; if that path cannot serve
            # it, the request remains fail-closed.
            _inc(self, "validation_cloud_failures")

    return await _request_range(
        self,
        from_block=from_block,
        to_block=to_block,
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
    limit = _provider_max_blocks(self)
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
        # Advance only after the entire current chunk returned successfully. Any
        # refusal by every verified provider raises above and leaves this frontier
        # unadvanced, preserving fail-closed contiguous event coverage.
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
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "alchemy_detected_max_blocks": ALCHEMY_SAFE_MAX_BLOCKS,
        "configured_max_blocks_env": ENV_MAX_BLOCKS,
        "validation_cloud_rpc_env": ENV_VALIDATION_CLOUD_RPC_URL,
        "validation_cloud_max_blocks_env": ENV_VALIDATION_CLOUD_MAX_BLOCKS,
        "validation_cloud_configured": bool(
            str(os.getenv(ENV_VALIDATION_CLOUD_RPC_URL) or "").strip()
        ),
        "validation_cloud_chain_verified": _VALIDATION_CLOUD_VERIFIED_ENDPOINT is not None,
        "validation_cloud_getlogs_only": True,
        "validation_cloud_preserves_primary_ws": True,
        "validation_cloud_dispatch_above_capability_repair": True,
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
    "ENV_MAX_BLOCKS",
    "ENV_VALIDATION_CLOUD_MAX_BLOCKS",
    "ENV_VALIDATION_CLOUD_RPC_URL",
    "REPAIR_VERSION",
    "_dispatch_range",
    "_failover_from_getlogs_403",
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
