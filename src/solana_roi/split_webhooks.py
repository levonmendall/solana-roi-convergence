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


def render_service_url_from_env(env: dict[str, str] | None = None) -> str:
    """Resolve Render's public HTTPS URL without requiring one specific default variable.

    Render documents both RENDER_EXTERNAL_URL and RENDER_EXTERNAL_HOSTNAME for web
    services. Prefer the full URL, but derive it from the hostname when needed.
    This keeps Helius bootstrap fail-closed while avoiding a silent skip when one
    Render-provided alias is unexpectedly absent.
    """

    values = env if env is not None else os.environ
    external_url = str(values.get("RENDER_EXTERNAL_URL") or "").strip()
    if external_url:
        return external_url
    hostname = str(values.get("RENDER_EXTERNAL_HOSTNAME") or "").strip()
    if hostname:
        return "https://" + hostname.lstrip("/")
    return ""


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
        self.stage = "initialized"

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
        }
        pump_raw = {
            "webhookURL": self._target(service_url, "/v1/ingestion/helius/pump-raw"),
            # Helius documents transactionTypes as enhanced-only; ANY is kept in
            # the request schema while raw delivery is governed by the program
            # address and webhookType=raw.
            "transactionTypes": ["ANY"],
            "accountAddresses": [PUMP_PROGRAM_ID],
            "webhookType": "raw",
            "authHeader": self.auth_header,
        }
        return enhanced, pump_raw

    @staticmethod
    def _same_config(current: dict[str, Any], desired: dict[str, Any]) -> bool:
        common = bool(
            current.get("webhookURL") == desired["webhookURL"]
            and set(current.get("accountAddresses") or []) == set(desired["accountAddresses"])
            and current.get("webhookType") == desired["webhookType"]
            and current.get("authHeader") == desired["authHeader"]
        )
        if not common:
            return False
        if desired["webhookType"] == "raw":
            return True
        return set(current.get("transactionTypes") or []) == set(desired["transactionTypes"])

    async def _enable(self, webhook_id: str, *, label: str) -> None:
        self.stage = f"reenable:{label}"
        response = await self.client.patch(
            f"{self.API_ROOT}/{webhook_id}",
            params=self._params(),
            json={"active": True},
        )
        response.raise_for_status()

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
            action = "unchanged"
            active = bool(current.get("active", True))
            if not self._same_config(current, desired):
                self.stage = f"update:{label}"
                response = await self.client.put(f"{self.API_ROOT}/{webhook_id}", params=self._params(), json=desired)
                response.raise_for_status()
                payload = response.json()
                active = bool(payload.get("active", True)) if isinstance(payload, dict) else active
                action = "updated"
            if not active:
                await self._enable(webhook_id, label=label)
                active = True
                action = "updated_and_reenabled" if action == "updated" else "reenabled"
            return {"feed": label, "action": action, "webhook_id": webhook_id, "active": active}
        self.stage = f"create:{label}"
        response = await self.client.post(self.API_ROOT, params=self._params(), json=desired)
        response.raise_for_status()
        payload = response.json()
        webhook_id = str(payload.get("webhookID") or "") if isinstance(payload, dict) else ""
        active = bool(payload.get("active", True)) if isinstance(payload, dict) else True
        if webhook_id and not active:
            await self._enable(webhook_id, label=label)
            active = True
        return {
            "feed": label,
            "action": "created" if active else "created_inactive",
            "webhook_id": webhook_id,
            "active": active,
        }

    async def sync(self, service_url: str) -> dict[str, Any]:
        if not service_url.startswith("https://"):
            raise ValueError("service URL must be public HTTPS")
        self.stage = "list_webhooks"
        response = await self.client.get(self.API_ROOT, params=self._params())
        response.raise_for_status()
        webhooks = response.json()
        if not isinstance(webhooks, list):
            raise RuntimeError("Helius webhook list response is invalid")
        enhanced, pump_raw = self._desired(service_url)
        enhanced_result = await self._upsert(webhooks, enhanced, label="enhanced_swap_feed")
        raw_result = await self._upsert(webhooks, pump_raw, label="pump_fun_raw_feed")
        self.stage = "complete"
        return {
            "action": "split_webhooks_synced",
            "feeds": [enhanced_result, raw_result],
            "enhanced_program_count": len(ENHANCED_PROGRAM_ADDRESSES),
            "pump_raw_program_count": 1,
            "enhanced_transaction_types": list(ENHANCED_TRANSACTION_TYPES),
        }


async def auto_sync_split_helius_from_env(*, store: Any | None = None, delay_seconds: float = 2.0) -> dict[str, Any]:
    await asyncio.sleep(max(0.0, delay_seconds))
    observed_at = datetime.now(timezone.utc).isoformat()
    api_key = os.getenv("HELIUS_API_KEY", "").strip()
    auth_header = os.getenv("HELIUS_WEBHOOK_AUTH", "").strip()
    service_url = render_service_url_from_env()
    if not api_key or not auth_header or not service_url:
        result = {
            "action": "skipped",
            "reason": "required Helius credentials or Render public service URL unavailable",
            "helius_api_key_configured": bool(api_key),
            "webhook_auth_configured": bool(auth_header),
            "render_external_url_configured": bool(os.getenv("RENDER_EXTERNAL_URL", "").strip()),
            "render_external_hostname_configured": bool(os.getenv("RENDER_EXTERNAL_HOSTNAME", "").strip()),
        }
        if store is not None:
            store.append("helius_split_webhook_bootstrap_skipped", observed_at, result)
        return result
    manager = SplitHeliusWebhookManager(api_key=api_key, auth_header=auth_header)
    try:
        result = await manager.sync(service_url)
        if store is not None:
            store.append("helius_split_webhook_bootstrap", observed_at, result)
        return result
    except Exception as exc:
        status_code = None
        if isinstance(exc, httpx.HTTPStatusError):
            status_code = int(exc.response.status_code)
        result = {
            "action": "failed",
            "stage": manager.stage,
            "error_type": type(exc).__name__,
            "http_status_code": status_code,
            "error": "Helius split-webhook bootstrap failed; provider exception text suppressed to protect query-string credentials",
        }
        if store is not None:
            store.append("helius_split_webhook_bootstrap_failed", observed_at, result)
        return result
