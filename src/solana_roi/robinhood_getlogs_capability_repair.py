from __future__ import annotations

import json
import re
import time
from typing import Any
from urllib.parse import urlparse

import httpx

from . import robinhood_getlogs_provider_guard as guard
from . import robinhood_provider_failover as failover


REPAIR_VERSION = "robinhood-getlogs-capability-repair-v1"
RECOVERY_SUCCESS_STREAK = 3
CHAINSTACK_DISCOVERY_MAX_BLOCKS = 200
_MAX_SAFE_BODY_CHARS = 320
_INSTALLED = False
_ORIGINAL_REQUEST_RANGE: Any = None


def _normalized(value: str) -> str:
    return str(value or "").strip().rstrip("/").lower()


def _provider_for_url(value: str) -> failover.ProviderEndpoint | None:
    target = _normalized(value)
    if not target:
        return None
    return next((item for item in failover.providers() if _normalized(item.http) == target), None)


def _provider_kind(provider: failover.ProviderEndpoint | None) -> str:
    if provider is None:
        return "unknown"
    name = str(provider.name or "").lower()
    try:
        host = (urlparse(provider.http).hostname or "").lower()
    except Exception:
        host = ""
    if "chainstack" in name or host.endswith(".chainstack.com") or host.endswith(".chainstacklabs.com") or host.endswith(".p2pify.com"):
        return "chainstack"
    if "alchemy" in name or "alchemy" in host:
        return "alchemy"
    if "drpc" in name or "drpc" in host:
        return "drpc"
    return "private_rpc"


def _safe_provider(provider: failover.ProviderEndpoint | None) -> str:
    if provider is None:
        return "unknown"
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(provider.name or "provider"))[:48]
    return value or _provider_kind(provider)


def _safe_error_body(exc: BaseException) -> str:
    response = getattr(exc, "response", None)
    if response is None:
        return "unavailable"
    try:
        payload = response.json()
    except Exception:
        try:
            text = str(response.text or "")
        except Exception:
            text = ""
    else:
        error = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(error, dict):
            safe = {"code": error.get("code"), "message": error.get("message")}
            text = json.dumps(safe, sort_keys=True, separators=(",", ":"))
        elif error is not None:
            text = str(error)
        else:
            text = "http_error"
    text = re.sub(r"(?i)(https?|wss?)://[^\s\"']+", "[redacted_url]", text)
    text = re.sub(r"(?i)(authorization|bearer|api[_ -]?key|token|secret)\s*[:=]\s*[^\s,;}]+", r"\1=[redacted]", text)
    text = re.sub(r"\b[A-Za-z0-9_-]{40,}\b", "[redacted_token]", text)
    return text[:_MAX_SAFE_BODY_CHARS] or "empty"


def _http_status(exc: BaseException) -> int | None:
    response = getattr(exc, "response", None)
    try:
        value = int(getattr(response, "status_code", 0))
    except (TypeError, ValueError):
        return None
    return value if 100 <= value <= 599 else None


def _is_http_403(exc: BaseException) -> bool:
    return isinstance(exc, httpx.HTTPStatusError) and _http_status(exc) == 403


def _state(provider: failover.ProviderEndpoint) -> dict[str, Any]:
    with failover._LOCK:
        state = failover._state_for_locked(provider.name)
        state.setdefault("getlogs_capability", "unknown")
        state.setdefault("getlogs_success_streak", 0)
        state.setdefault("getlogs_successes", 0)
        state.setdefault("getlogs_failures", 0)
        state.setdefault("getlogs_safe_max_blocks", None)
        state.setdefault("getlogs_span1_proven", False)
        state.setdefault("getlogs_last_http_status", None)
        state.setdefault("getlogs_last_failure_at", None)
        state.setdefault("getlogs_last_success_at", None)
        return state


def _record_success(provider: failover.ProviderEndpoint, *, span: int) -> None:
    now = time.time()
    with failover._LOCK:
        state = failover._state_for_locked(provider.name)
        state["getlogs_successes"] = int(state.get("getlogs_successes", 0) or 0) + 1
        streak = int(state.get("getlogs_success_streak", 0) or 0) + 1
        state["getlogs_success_streak"] = streak
        state["getlogs_last_success_at"] = now
        state["getlogs_last_http_status"] = 200
        if int(span) == 1:
            state["getlogs_span1_proven"] = True
        if streak >= RECOVERY_SUCCESS_STREAK:
            state["getlogs_capability"] = "healthy"
        elif state.get("getlogs_capability") != "basic_evm_rpc_only":
            state["getlogs_capability"] = "recovering"


def _record_failure(
    provider: failover.ProviderEndpoint,
    exc: BaseException,
    *,
    span: int,
    basic_only: bool = False,
) -> None:
    now = time.time()
    with failover._LOCK:
        state = failover._state_for_locked(provider.name)
        state["getlogs_failures"] = int(state.get("getlogs_failures", 0) or 0) + 1
        state["getlogs_success_streak"] = 0
        state["getlogs_last_failure_at"] = now
        state["getlogs_last_http_status"] = _http_status(exc)
        if basic_only:
            state["getlogs_capability"] = "basic_evm_rpc_only"
            state["getlogs_safe_max_blocks"] = 0
            state["getlogs_span1_proven"] = False
        else:
            state["getlogs_capability"] = "degraded"


def _set_safe_max(provider: failover.ProviderEndpoint, blocks: int) -> None:
    with failover._LOCK:
        state = failover._state_for_locked(provider.name)
        state["getlogs_safe_max_blocks"] = max(1, int(blocks))
        if state.get("getlogs_capability") != "healthy":
            state["getlogs_capability"] = "bounded"


def _capability(provider: failover.ProviderEndpoint) -> str:
    with failover._LOCK:
        return str(failover._state_for_locked(provider.name).get("getlogs_capability") or "unknown")


def _safe_max(provider: failover.ProviderEndpoint) -> int | None:
    with failover._LOCK:
        value = failover._state_for_locked(provider.name).get("getlogs_safe_max_blocks")
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _log_result(
    provider: failover.ProviderEndpoint,
    *,
    from_block: int,
    to_block: int,
    status: int | None,
    body: str,
) -> None:
    span = max(0, int(to_block) - int(from_block) + 1)
    print(
        "ROBINHOOD_GETLOGS_RESULT "
        f"provider={_safe_provider(provider)} provider_kind={_provider_kind(provider)} "
        f"from_block={int(from_block)} to_block={int(to_block)} span={span} "
        f"http_status={status if status is not None else 'none'} response_body={body}",
        flush=True,
    )


def _raw_query(
    *,
    from_block: int,
    to_block: int,
    addresses: list[str] | tuple[str, ...] | None,
    topics: list[Any] | None,
) -> dict[str, Any]:
    query: dict[str, Any] = {"fromBlock": hex(max(0, int(from_block))), "toBlock": hex(max(0, int(to_block)))}
    if addresses:
        query["address"] = list(addresses) if len(addresses) > 1 else addresses[0]
    if topics is not None:
        query["topics"] = topics
    return query


async def _direct_getlogs(
    self: Any,
    provider: failover.ProviderEndpoint,
    *,
    from_block: int,
    to_block: int,
    addresses: list[str] | tuple[str, ...] | None,
    topics: list[Any] | None,
) -> list[dict[str, Any]]:
    original_rpc = failover._ORIGINAL_RPC
    if original_rpc is None:
        raise RuntimeError("Robinhood provider RPC base is unavailable")
    previous_url = str(getattr(self, "rpc_url", "") or "")
    self.rpc_url = provider.http
    try:
        result = await original_rpc(
            self,
            "eth_getLogs",
            [_raw_query(from_block=from_block, to_block=to_block, addresses=addresses, topics=topics)],
        )
        rows = list(result or [])
    except Exception as exc:
        _log_result(
            provider,
            from_block=from_block,
            to_block=to_block,
            status=_http_status(exc),
            body=_safe_error_body(exc),
        )
        raise
    finally:
        self.rpc_url = previous_url
    _log_result(
        provider,
        from_block=from_block,
        to_block=to_block,
        status=200,
        body=f"result_count:{len(rows)}",
    )
    _record_success(provider, span=int(to_block) - int(from_block) + 1)
    return rows


async def _ensure_chain(provider: failover.ProviderEndpoint, self: Any) -> bool:
    with failover._LOCK:
        verified = bool(failover._state_for_locked(provider.name).get("chain_verified", False))
    if verified:
        return True
    original = failover._ORIGINAL_RPC
    if original is None:
        return False
    return bool(await failover._verify_candidate_chain(original, self, provider))


def _candidate_providers(current: failover.ProviderEndpoint | None, excluded: set[str]) -> list[failover.ProviderEndpoint]:
    items = list(failover.providers())
    ordered: list[failover.ProviderEndpoint] = []
    if current is not None:
        ordered.append(current)
    active = failover.active_provider()
    if active is not None and all(item.name != active.name for item in ordered):
        ordered.append(active)
    rank = {"healthy": 0, "bounded": 1, "recovering": 2, "unknown": 3, "degraded": 4}
    rest = [item for item in items if all(item.name != seen.name for seen in ordered)]
    rest.sort(key=lambda item: rank.get(_capability(item), 5))
    ordered.extend(rest)
    return [
        item
        for item in ordered
        if item.name not in excluded and _capability(item) != "basic_evm_rpc_only"
    ]


async def _probe_span_one(
    self: Any,
    provider: failover.ProviderEndpoint,
    *,
    block: int,
    addresses: list[str] | tuple[str, ...] | None,
    topics: list[Any] | None,
) -> bool:
    try:
        await _direct_getlogs(
            self,
            provider,
            from_block=block,
            to_block=block,
            addresses=addresses,
            topics=topics,
        )
        return True
    except Exception as exc:
        _record_failure(provider, exc, span=1, basic_only=_is_http_403(exc))
        return False


async def _discover_chainstack_max(
    self: Any,
    provider: failover.ProviderEndpoint,
    *,
    from_block: int,
    to_block: int,
    addresses: list[str] | tuple[str, ...] | None,
    topics: list[Any] | None,
) -> int:
    requested = max(1, int(to_block) - int(from_block) + 1)
    ceiling = min(requested, CHAINSTACK_DISCOVERY_MAX_BLOCKS)
    accepted = 1
    rejected: int | None = None
    probe = 2
    while probe <= ceiling:
        end = int(from_block) + probe - 1
        try:
            await _direct_getlogs(self, provider, from_block=from_block, to_block=end, addresses=addresses, topics=topics)
            accepted = probe
            if probe == ceiling:
                break
            probe = min(ceiling, probe * 2)
            if probe == accepted:
                break
        except Exception as exc:
            _record_failure(provider, exc, span=probe)
            if not _is_http_403(exc):
                raise
            rejected = probe
            break
    if rejected is not None and rejected - accepted > 1:
        low, high = accepted + 1, rejected - 1
        while low <= high:
            mid = (low + high) // 2
            end = int(from_block) + mid - 1
            try:
                await _direct_getlogs(self, provider, from_block=from_block, to_block=end, addresses=addresses, topics=topics)
                accepted = mid
                low = mid + 1
            except Exception as exc:
                _record_failure(provider, exc, span=mid)
                if not _is_http_403(exc):
                    raise
                high = mid - 1
    _set_safe_max(provider, accepted)
    print(
        "ROBINHOOD_GETLOGS_BOUNDARY "
        f"provider={_safe_provider(provider)} provider_kind={_provider_kind(provider)} "
        f"safe_max_blocks={accepted} requested_blocks={requested}",
        flush=True,
    )
    return accepted


async def _retrieve_from_provider(
    self: Any,
    provider: failover.ProviderEndpoint,
    *,
    from_block: int,
    to_block: int,
    addresses: list[str] | tuple[str, ...] | None,
    topics: list[Any] | None,
    excluded: set[str],
) -> list[dict[str, Any]]:
    span = int(to_block) - int(from_block) + 1
    limit = _safe_max(provider)
    if limit is None and guard._is_alchemy_endpoint(provider.http):
        limit = guard.ALCHEMY_SAFE_MAX_BLOCKS
    if limit is not None and span > limit:
        rows: list[dict[str, Any]] = []
        cursor = int(from_block)
        while cursor <= int(to_block):
            end = min(int(to_block), cursor + limit - 1)
            rows.extend(
                await _retrieve_range(
                    self,
                    from_block=cursor,
                    to_block=end,
                    addresses=addresses,
                    topics=topics,
                    excluded=set(excluded),
                    preferred=provider,
                )
            )
            cursor = end + 1
        return rows
    try:
        return await _direct_getlogs(
            self,
            provider,
            from_block=from_block,
            to_block=to_block,
            addresses=addresses,
            topics=topics,
        )
    except Exception as exc:
        _record_failure(provider, exc, span=span, basic_only=bool(_is_http_403(exc) and span == 1))
        if _provider_kind(provider) == "chainstack" and _is_http_403(exc):
            if span == 1:
                print(
                    "ROBINHOOD_GETLOGS_CAPABILITY provider="
                    f"{_safe_provider(provider)} provider_kind=chainstack capability=basic_evm_rpc_only reason=span1_http_403",
                    flush=True,
                )
            elif await _probe_span_one(
                self,
                provider,
                block=int(from_block),
                addresses=addresses,
                topics=topics,
            ):
                limit = await _discover_chainstack_max(
                    self,
                    provider,
                    from_block=from_block,
                    to_block=to_block,
                    addresses=addresses,
                    topics=topics,
                )
                return await _retrieve_from_provider(
                    self,
                    provider,
                    from_block=from_block,
                    to_block=to_block,
                    addresses=addresses,
                    topics=topics,
                    excluded=excluded,
                )
            else:
                with failover._LOCK:
                    state = failover._state_for_locked(provider.name)
                    if state.get("getlogs_capability") != "basic_evm_rpc_only":
                        state["getlogs_capability"] = "degraded"
        return await _retrieve_range(
            self,
            from_block=from_block,
            to_block=to_block,
            addresses=addresses,
            topics=topics,
            excluded=set(excluded) | {provider.name},
            preferred=None,
        )


async def _retrieve_range(
    self: Any,
    *,
    from_block: int,
    to_block: int,
    addresses: list[str] | tuple[str, ...] | None,
    topics: list[Any] | None,
    excluded: set[str],
    preferred: failover.ProviderEndpoint | None = None,
) -> list[dict[str, Any]]:
    current = preferred or _provider_for_url(str(getattr(self, "rpc_url", "") or ""))
    candidates = _candidate_providers(current, excluded)
    last_error: BaseException | None = None
    for provider in candidates:
        if not await _ensure_chain(provider, self):
            excluded.add(provider.name)
            continue
        try:
            return await _retrieve_from_provider(
                self,
                provider,
                from_block=from_block,
                to_block=to_block,
                addresses=addresses,
                topics=topics,
                excluded=excluded,
            )
        except Exception as exc:
            last_error = exc
            excluded.add(provider.name)
            continue
    if last_error is not None:
        raise last_error
    raise failover.RobinhoodProviderPoolUnavailable("robinhood_getlogs_capable_provider_unavailable")


async def _capability_request_range(
    self: Any,
    *,
    from_block: int,
    to_block: int,
    addresses: list[str] | tuple[str, ...] | None,
    topics: list[Any] | None,
) -> list[dict[str, Any]]:
    if int(to_block) < int(from_block) or not failover.providers() or failover._ORIGINAL_RPC is None:
        if _ORIGINAL_REQUEST_RANGE is None:
            raise RuntimeError("Robinhood getLogs capability repair is not installed")
        return await _ORIGINAL_REQUEST_RANGE(
            self,
            from_block=from_block,
            to_block=to_block,
            addresses=addresses,
            topics=topics,
        )
    return await _retrieve_range(
        self,
        from_block=int(from_block),
        to_block=int(to_block),
        addresses=addresses,
        topics=topics,
        excluded=set(),
    )


def status() -> dict[str, Any]:
    providers: dict[str, dict[str, Any]] = {}
    with failover._LOCK:
        for item in failover.providers():
            state = failover._state_for_locked(item.name)
            chain_verified = bool(state.get("chain_verified", False))
            read_verified = bool(state.get("read_capability_verified", False))
            streak = int(state.get("getlogs_success_streak", 0) or 0)
            capability = str(state.get("getlogs_capability") or "unknown")
            providers[_safe_provider(item)] = {
                "provider_kind": _provider_kind(item),
                "chain_verified": chain_verified,
                "read_capability_verified": read_verified,
                "eth_getlogs_capability": capability,
                "eth_getlogs_success_streak": streak,
                "eth_getlogs_required_success_streak": RECOVERY_SUCCESS_STREAK,
                "eth_getlogs_safe_max_blocks": state.get("getlogs_safe_max_blocks"),
                "eth_getlogs_span1_proven": bool(state.get("getlogs_span1_proven", False)),
                "eth_getlogs_last_http_status": state.get("getlogs_last_http_status"),
                "fully_healthy": bool(chain_verified and read_verified and capability == "healthy" and streak >= RECOVERY_SUCCESS_STREAK),
            }
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "mandatory_capabilities": ["eth_chainId", "eth_blockNumber", "eth_getLogs"],
        "basic_rpc_success_does_not_imply_full_health": True,
        "span1_403_classifies_basic_evm_rpc_only": True,
        "bounded_adaptive_pagination": True,
        "failed_range_cursor_authority": False,
        "exact_range_fallback": True,
        "recovery_success_streak_required": RECOVERY_SUCCESS_STREAK,
        "diagnostics_expose_endpoint": False,
        "diagnostics_expose_credentials": False,
        "providers": providers,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def install_robinhood_getlogs_capability_repair() -> None:
    global _INSTALLED, _ORIGINAL_REQUEST_RANGE
    if _INSTALLED:
        return
    _ORIGINAL_REQUEST_RANGE = guard._request_range
    guard._request_range = _capability_request_range
    _INSTALLED = True


__all__ = [
    "CHAINSTACK_DISCOVERY_MAX_BLOCKS",
    "RECOVERY_SUCCESS_STREAK",
    "REPAIR_VERSION",
    "_capability_request_range",
    "_provider_kind",
    "_safe_error_body",
    "install_robinhood_getlogs_capability_repair",
    "status",
]
