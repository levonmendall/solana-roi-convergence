from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

import httpx

from .deployment import FROZEN_PROGRAM_ADDRESSES, PUMP_PROGRAM_ID

ENHANCED_PROGRAM_ADDRESSES = tuple(address for address in FROZEN_PROGRAM_ADDRESSES if address != PUMP_PROGRAM_ID)
ENHANCED_TRANSACTION_TYPES = ("BUY", "SELL", "SWAP")


class SplitHeliusWebhookManager:
    """Maintain a low-noise enhanced feed plus raw Pump.fun feed.

    Helius Enhanced exposes PUMP_AMM BUY/SELL and RAYDIUM SWAP, but does not
    expose a PUMP_FUN source parser. Pump bonding-curve traffic therefore uses
    a raw webhook and is parsed locally from official Pump discriminators.
    """

    API_ROOT = "https://api-mainnet.helius-rpc.com/v0/webhooks"

    def __init__(self, *, api_key: str, auth_header: str, client: Any | None = None):
        if not api_key or not auth_header:
            raise ValueError("HELIUS_API_KEY and HELIUS_WEBHOOK_AUTH are required")
        self.api_key = api_key
        self.auth_header = auth_header
        self.client = client or httpx.AsyncClient(timeout=10.0)

    def _params(self) -> dict[str, str]:
        return {"api-key": self.api_key}

    @staticmethod
    def _target(service_url: str, path: str) -> str:
        return urljoin(service_url.rstrip("/") + "/", path.lstrip("/"))

    def _desired(self, service_url: str) -> tuple[dict[str, Any], dict[str, Any]]:
        enhanced = {
            "webhookURL": self._target(service_url, "/v1/ingestion/helius"),
            "transactionTypes": list(ENHANCED_TRANSACTION_TYPES),
            "accountAddresses": list(ENHANCED_PROGRAM_ADDRESSES),
            "webhookType": "enhanced",
            "authHeader": self.auth_header,
            "active": True,
        }
        pump_raw = {
            "webhookURL": self._target(service_url, "/v1/ingestion/helius/pump-raw"),
            "transactionTypes": ["ANY"],
            "accountAddresses": [PUMP_PROGRAM_ID],
            "webhookType": "raw",
            "authHeader": self.auth_header,
            "active": True,
        }
        return enhanced, pump_raw

    @staticmethod
    def _same(current: dict[str, Any], desired: dict[str, Any]) -> bool:
        common = bool(
            current.get("webhookURL") == desired["webhookURL"]
            and set(current.get("accountAddresses") or []) == set(desired["accountAddresses"])
            and current.get("webhookType") == desired["webhookType"]
            and current.get("authHeader") == desired["authHeader"]
            and current.get("active", True)
        )
        if not common:
            return False
        if desired["webhookType"] == "raw":
            return True
        return set(current.get("transactionTypes") or []) == set(desired["transactionTypes"])

    async def _upsert(self, webhooks: list[Any], desired: dict[str, Any], *, label: str) -> dict[str, Any]:
        target = desired["webhookURL"]
        matching = [row for row in webhooks if isinstance(row, dict) and row.get("webhookURL") == target]
        if len(matching) > 1:
            raise RuntimeError(f"multiple Helius webhooks target {label}; refusing ambiguous mutation")
        if matching:
            current = matching[0]
            webhook_id = str(current.get("webhookID") or "")
            if not webhook_id:
                raise RuntimeError(f"matching {label} webhook has no webhookID")
            if self._same(current, desired):
                return {"feed": label, "action": "unchanged", "webhook_id": webhook_id, "active": True}
            response = await self.client.put(f"{self.API_ROOT}/{webhook_id}", params=self._params(), json=desired)
            response.raise_for_status()
            payload = response.json()
            return {
                "feed": label,
                "action": "updated",
                "webhook_id": str(payload.get("webhookID") or webhook_id) if isinstance(payload, dict) else webhook_id,
                "active": bool(payload.get("active", True)) if isinstance(payload, dict) else True,
            }
        response = await self.client.post(self.API_ROOT, params=self._params(), json=desired)
        response.raise_for_status()
        payload = response.json()
        return {
            "feed": label,
            "action": "created",
            "webhook_id": str(payload.get("webhookID") or "") if isinstance(payload, dict) else "",
            "active": bool(payload.get("active", True)) if isinstance(payload, dict) else True,
        }

    async def sync(self, service_url: str) -> dict[str, Any]:
        if not service_url.startswith("https://"):
            raise ValueError("service URL must be public HTTPS")
        response = await self.client.get(self.API_ROOT, params=self._params())
        response.raise_for_status()
        webhooks = response.json()
        if not isinstance(webhooks, list):
            raise RuntimeError("Helius webhook list response is invalid")
        enhanced, pump_raw = self._desired(service_url)
        enhanced_result = await self._upsert(webhooks, enhanced, label="enhanced_swap_feed")
        # Include a synthetic representation of the first mutation so a just-
        # created/updated enhanced endpoint cannot be mistaken for the raw URL.
        raw_result = await self._upsert(webhooks, pump_raw, label="pump_fun_raw_feed")
        return {
            "action": "split_webhooks_synced",
            "feeds": [enhanced_result, raw_result],
            "enhanced_program_count": len(ENHANCED_PROGRAM_ADDRESSES),
            "pump_raw_program_count": 1,
            "enhanced_transaction_types": list(ENHANCED_TRANSACTION_TYPES),
        }


async def auto_sync_split_helius_from_env(*, store: Any | None = None, delay_seconds: float = 2.0) -> dict[str, Any]:
    await asyncio.sleep(max(0.0, delay_seconds))
    api_key = os.getenv("HELIUS_API_KEY", "").strip()
    auth_header = os.getenv("HELIUS_WEBHOOK_AUTH", "").strip()
    service_url = os.getenv("RENDER_EXTERNAL_URL", "").strip()
    if not api_key or not auth_header or not service_url:
        return {"action": "skipped", "reason": "Helius credentials or Render external URL unavailable"}
    manager = SplitHeliusWebhookManager(api_key=api_key, auth_header=auth_header)
    observed_at = datetime.now(timezone.utc).isoformat()
    try:
        result = await manager.sync(service_url)
        if store is not None:
            store.append("helius_split_webhook_bootstrap", observed_at, result)
        return result
    except Exception as exc:
        result = {
            "action": "failed",
            "error_type": type(exc).__name__,
            "error": "Helius split-webhook bootstrap failed; provider exception text suppressed to protect query-string credentials",
        }
        if store is not None:
            store.append("helius_split_webhook_bootstrap_failed", observed_at, result)
        return result
