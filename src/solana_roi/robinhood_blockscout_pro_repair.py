from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Callable

from . import robinhood_entity_resolution_repair as entity_repair
from .robinhood_chain_core import KNOWN_NON_ACTORS, ROBINHOOD_CHAIN_ID, _clean_address


REPAIR_VERSION = "robinhood-blockscout-pro-entity-v1"
DEFAULT_PRO_API_URL = "https://api.blockscout.com/v2/api"
MISSING_KEY_BACKOFF_SECONDS = 300.0
TXLIST_OFFSET = 50
_ORIGINAL_STATUS: Callable[..., Any] | None = None


def _required() -> bool:
    return os.getenv("ROBINHOOD_ENTITY_RESOLUTION_REQUIRED", "true").strip().lower() not in {
        "0",
        "false",
        "no",
    }


def _api_key() -> str:
    return os.getenv("BLOCKSCOUT_API_KEY", "").strip()


def _api_url() -> str:
    return os.getenv("ROBINHOOD_BLOCKSCOUT_PRO_API_URL", DEFAULT_PRO_API_URL).strip() or DEFAULT_PRO_API_URL


async def _entity_anchor_fetch_pro(self: Any, actor: str) -> str | None:
    """Resolve the earliest native funder with one ascending Pro API query.

    Blockscout retired unauthenticated per-instance API traffic on 2026-07-01.
    Use the universal chain-scoped Etherscan-compatible endpoint instead. Asking for
    inbound transactions in ascending order makes the first valid positive-value
    transfer the authoritative funding anchor and avoids the former three-page
    newest-first approximation.
    """

    stats = entity_repair._stats(self)
    stats.setdefault("pro_api_requests", 0)
    stats.setdefault("missing_api_key_failures", 0)
    stats.setdefault("pro_api_parse_failures", 0)
    negative = entity_repair._negative_cache(self)
    prior = negative.get(actor)
    prior_count = int(prior[2]) if prior is not None else 0
    key = _api_key()

    if not key:
        self._entity_resolution_failures += 1
        stats["missing_api_key_failures"] = int(stats["missing_api_key_failures"]) + 1
        stats["last_error_type"] = "BlockscoutApiKeyMissing"
        stats["last_error_status"] = None
        negative[actor] = (
            time.monotonic() + MISSING_KEY_BACKOFF_SECONDS,
            "BlockscoutApiKeyMissing",
            prior_count + 1,
            None,
        )
        if _required():
            return None
        self._entity_cache[actor] = (actor, time.monotonic())
        return actor

    params: dict[str, Any] = {
        "chain_id": ROBINHOOD_CHAIN_ID,
        "module": "account",
        "action": "txlist",
        "address": actor,
        "startblock": 0,
        "endblock": 999999999,
        "page": 1,
        "offset": TXLIST_OFFSET,
        "sort": "asc",
        "filterby": "to",
        "apikey": key,
    }
    try:
        async with entity_repair._entity_semaphore(self):
            stats["http_requests"] = int(stats["http_requests"]) + 1
            stats["pro_api_requests"] = int(stats["pro_api_requests"]) + 1
            response = await self.rpc.client.get(
                _api_url(),
                params=params,
                timeout=3.5,
                headers={"Accept": "application/json"},
            )
            response.raise_for_status()
            payload = response.json()

        result = payload.get("result") if isinstance(payload, dict) else None
        status = str(payload.get("status") or "") if isinstance(payload, dict) else ""
        message = str(payload.get("message") or "") if isinstance(payload, dict) else ""
        if isinstance(result, str):
            lowered = f"{message} {result}".lower()
            if "no transactions" in lowered:
                result = []
            else:
                stats["pro_api_parse_failures"] = int(stats["pro_api_parse_failures"]) + 1
                raise RuntimeError("Blockscout Pro txlist returned a non-list result")
        if status not in {"", "1"} and not isinstance(result, list):
            stats["pro_api_parse_failures"] = int(stats["pro_api_parse_failures"]) + 1
            raise RuntimeError("Blockscout Pro txlist request was not successful")
        if not isinstance(result, list):
            stats["pro_api_parse_failures"] = int(stats["pro_api_parse_failures"]) + 1
            raise RuntimeError("Blockscout Pro txlist returned no transaction list")

        anchor: str | None = None
        for tx in result:
            if not isinstance(tx, dict):
                continue
            to_addr = _clean_address(str(tx.get("to") or ""))
            from_addr = _clean_address(str(tx.get("from") or ""))
            try:
                value = int(str(tx.get("value") or "0"))
            except (TypeError, ValueError):
                continue
            if (
                to_addr == actor
                and from_addr
                and from_addr not in KNOWN_NON_ACTORS
                and value > 0
            ):
                anchor = from_addr
                break

        resolved = anchor or actor
        self._entity_cache[actor] = (resolved, time.monotonic())
        negative.pop(actor, None)
        stats["last_error_type"] = None
        stats["last_error_status"] = None
        if anchor is None:
            stats["resolved_singletons"] = int(stats["resolved_singletons"]) + 1
        else:
            stats["resolved_funding_anchors"] = int(stats["resolved_funding_anchors"]) + 1
        return resolved
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        self._entity_resolution_failures += 1
        stats["external_failures"] = int(stats["external_failures"]) + 1
        delay, http_status = entity_repair._failure_backoff(exc, prior_count + 1)
        if http_status == 429:
            stats["rate_limit_failures"] = int(stats["rate_limit_failures"]) + 1
        stats["last_error_type"] = type(exc).__name__
        stats["last_error_status"] = http_status
        negative[actor] = (
            time.monotonic() + delay,
            type(exc).__name__,
            prior_count + 1,
            http_status,
        )
        if _required():
            return None
        self._entity_cache[actor] = (actor, time.monotonic())
        return actor


def _status_with_pro_api(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        stats = entity_repair._stats(self)
        entity = payload.setdefault("entity_resolution", {})
        if isinstance(entity, dict):
            entity.update(
                {
                    "provider_api": "blockscout-pro-universal",
                    "provider_chain_id": ROBINHOOD_CHAIN_ID,
                    "provider_api_url": _api_url(),
                    "api_key_configured": bool(_api_key()),
                    "api_key_value_exposed": False,
                    "query_shape": "account.txlist inbound sort=asc single-request",
                    "max_provider_calls_per_uncached_actor": 1,
                    "legacy_per_instance_endpoint_used": False,
                    "pro_api_requests_session": int(stats.get("pro_api_requests") or 0),
                    "missing_api_key_failures_session": int(stats.get("missing_api_key_failures") or 0),
                    "pro_api_parse_failures_session": int(stats.get("pro_api_parse_failures") or 0),
                    "missing_key_fails_closed": True,
                }
            )
        payload["blockscout_pro_entity_repair"] = {
            "repair_version": REPAIR_VERSION,
            "enabled": True,
            "legacy_403_path_removed": True,
            "single_request_ascending_funder_resolution": True,
            "strategy_thresholds_changed": False,
            "entity_independence_rules_changed": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_blockscout_pro_entity_repair", True)
    return status


def install_robinhood_blockscout_pro_repair(plane_cls: type[Any]) -> None:
    global _ORIGINAL_STATUS
    entity_repair._entity_anchor_fetch = _entity_anchor_fetch_pro
    current_status = plane_cls.status
    if not bool(getattr(current_status, "_roi_blockscout_pro_entity_repair", False)):
        _ORIGINAL_STATUS = current_status
        plane_cls.status = _status_with_pro_api(current_status)  # type: ignore[method-assign]
    setattr(plane_cls, "_roi_blockscout_pro_entity_repair_installed", True)
    setattr(plane_cls, "_roi_blockscout_pro_entity_repair_version", REPAIR_VERSION)


__all__ = [
    "REPAIR_VERSION",
    "DEFAULT_PRO_API_URL",
    "install_robinhood_blockscout_pro_repair",
]
