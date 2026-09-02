from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from typing import Any, Mapping
from urllib.parse import urljoin

import httpx

from .shadow_execution import validate_solana_public_key

PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMP_AMM_PROGRAM_ID = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
RAYDIUM_AMM_V4_PROGRAM_ID = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
RAYDIUM_CPMM_PROGRAM_ID = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"
RAYDIUM_CLMM_PROGRAM_ID = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"
RAYDIUM_STABLE_AMM_PROGRAM_ID = "5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h"
RAYDIUM_LAUNCHLAB_PROGRAM_ID = "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj"

FROZEN_PROGRAM_ADDRESSES = (
    PUMP_PROGRAM_ID,
    PUMP_AMM_PROGRAM_ID,
    RAYDIUM_AMM_V4_PROGRAM_ID,
    RAYDIUM_CPMM_PROGRAM_ID,
    RAYDIUM_CLMM_PROGRAM_ID,
    RAYDIUM_STABLE_AMM_PROGRAM_ID,
    RAYDIUM_LAUNCHLAB_PROGRAM_ID,
)

# Public identities selected by the existing ROI Convergence research. The live
# deployment freezes the exact wallet addresses through SOLANA_ROI_WALLET_PROFILES_JSON.
DEFAULT_SCOUT_PROFILES = (
    {
        "wallet": "4BdKaxN8G6ka4GYtQQWk4G4dZRUTX2vQH9GcXdBREFUk",
        "entity_id": "kol:jijo",
        "tier": "S",
        "first_touch_sample_size": 191,
        "historically_eligible": True,
    },
    {
        "wallet": "862TYSvRYoiHAK3F3WwTRYAfuGiQaGdxedN9AGvRGWo2",
        "entity_id": "kol:wugi",
        "tier": "S",
        "first_touch_sample_size": 52,
        "historically_eligible": True,
    },
    {
        "wallet": "DYAn4XpAkN5mhiXkRB7dGq4Jadnx6XYgu8L5b3WGhbrt",
        "entity_id": "kol:the-doc",
        "tier": "S",
        "first_touch_sample_size": 121,
        "historically_eligible": True,
    },
)

FORBIDDEN_SECRET_ENV_NAMES = (
    "SOLANA_ROI_PRIVATE_KEY",
    "SOLANA_ROI_WALLET_PRIVATE_KEY",
    "SOLANA_ROI_SECRET_KEY",
    "SOLANA_ROI_SEED_PHRASE",
    "SOLANA_ROI_MNEMONIC",
)


def default_scout_profiles_json() -> str:
    return json.dumps(DEFAULT_SCOUT_PROFILES, separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class PreflightCheck:
    name: str
    ok: bool
    detail: str


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes"}


def _profiles_check(raw: str) -> tuple[bool, str]:
    try:
        rows = json.loads(raw)
    except json.JSONDecodeError:
        return False, "wallet profile JSON is invalid"
    if not isinstance(rows, list) or not rows:
        return False, "at least one frozen S/A scout profile is required"
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            return False, "wallet profile entries must be objects"
        wallet = str(row.get("wallet") or "")
        try:
            validate_solana_public_key(wallet)
        except ValueError:
            return False, "wallet profile contains an invalid Solana public key"
        if wallet in seen:
            return False, "wallet profile contains a duplicate wallet"
        seen.add(wallet)
        if str(row.get("tier") or "").upper() not in {"S", "A"}:
            return False, "deployment scout cohort may contain only frozen S/A profiles"
        if int(row.get("first_touch_sample_size") or 0) < 30:
            return False, "deployment scout cohort requires >=30 historical first touches"
        if not bool(row.get("historically_eligible")):
            return False, "deployment scout cohort includes a non-eligible profile"
        if not str(row.get("entity_id") or "").strip():
            return False, "every deployment scout requires an explicit entity id"
    return True, f"{len(rows)} frozen scout profiles configured"


def deployment_preflight(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    env = env or os.environ
    checks: list[PreflightCheck] = []
    checks.append(PreflightCheck("paper_only", _truthy(env.get("PAPER_ONLY")), "PAPER_ONLY must be true"))
    checks.append(PreflightCheck("mainnet", env.get("SOLANA_NETWORK") == "mainnet-beta", "SOLANA_NETWORK must be mainnet-beta"))
    checks.append(PreflightCheck("program_coverage_enabled", _truthy(env.get("SOLANA_ROI_PROGRAM_WIDE_SWAP_COVERAGE")), "program-wide collection must be enabled"))
    checks.append(PreflightCheck("continuous_clock_enabled", _truthy(env.get("SOLANA_ROI_SHADOW_CLOCK_ENABLED")), "continuous paper price clock must be enabled"))

    if _truthy(env.get("RENDER")):
        persistent = str(env.get("SOLANA_ROI_DB_PATH") or "").startswith("/var/data/")
        checks.append(PreflightCheck("persistent_sqlite", persistent, "Render deployment must use /var/data persistent disk"))
        commit = str(env.get("RENDER_GIT_COMMIT") or "")
        checks.append(PreflightCheck("release_commit", len(commit) == 40, "RENDER_GIT_COMMIT must bind the exact deployed release"))

    for name in ("HELIUS_API_KEY", "HELIUS_WEBHOOK_AUTH", "JUPITER_API_KEY", "SOLANA_ROI_COHORT_ARM_AUTH"):
        checks.append(PreflightCheck(name.lower(), bool(str(env.get(name) or "").strip()), f"{name} must be configured as a secret"))

    shadow_wallet = str(env.get("SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY") or "").strip()
    try:
        validate_solana_public_key(shadow_wallet)
        wallet_ok = True
    except ValueError:
        wallet_ok = False
    checks.append(PreflightCheck("shadow_wallet_public_key", wallet_ok, "a valid public Solana address is required; no private key"))

    forbidden = [name for name in FORBIDDEN_SECRET_ENV_NAMES if str(env.get(name) or "").strip()]
    checks.append(PreflightCheck("no_private_key_material", not forbidden, "application environment must contain no ROI wallet private key, seed phrase, or mnemonic"))

    profiles_ok, profiles_detail = _profiles_check(str(env.get("SOLANA_ROI_WALLET_PROFILES_JSON") or ""))
    checks.append(PreflightCheck("frozen_scout_cohort", profiles_ok, profiles_detail))

    return {
        "ready_for_live_shadow_collection": all(check.ok for check in checks),
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
        "checks": [asdict(check) for check in checks],
        "program_addresses": list(FROZEN_PROGRAM_ADDRESSES),
    }


class HeliusWebhookManager:
    """Idempotently install the program-wide enhanced webhook without exposing secrets."""

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
    def _target_url(service_url: str) -> str:
        base = service_url.rstrip("/") + "/"
        return urljoin(base, "v1/ingestion/helius")

    def _desired(self, service_url: str) -> dict[str, Any]:
        return {
            "webhookURL": self._target_url(service_url),
            "transactionTypes": ["ANY"],
            "accountAddresses": list(FROZEN_PROGRAM_ADDRESSES),
            "webhookType": "enhanced",
            "authHeader": self.auth_header,
        }

    @staticmethod
    def _same_public_config(current: dict[str, Any], desired: dict[str, Any]) -> bool:
        return bool(
            current.get("webhookURL") == desired["webhookURL"]
            and set(current.get("transactionTypes") or []) == {"ANY"}
            and set(current.get("accountAddresses") or []) == set(desired["accountAddresses"])
            and current.get("webhookType", "enhanced") == "enhanced"
            and current.get("active", True)
        )

    async def sync(self, service_url: str) -> dict[str, Any]:
        if not service_url.startswith("https://"):
            raise ValueError("service URL must be public HTTPS")
        desired = self._desired(service_url)
        response = await self.client.get(self.API_ROOT, params=self._params())
        response.raise_for_status()
        webhooks = response.json()
        if not isinstance(webhooks, list):
            raise RuntimeError("Helius webhook list response is invalid")
        target = desired["webhookURL"]
        matching = [row for row in webhooks if isinstance(row, dict) and row.get("webhookURL") == target]
        if len(matching) > 1:
            raise RuntimeError("multiple Helius webhooks target the ROI endpoint; refusing ambiguous mutation")
        if matching:
            current = matching[0]
            webhook_id = str(current.get("webhookID") or "")
            if not webhook_id:
                raise RuntimeError("matching Helius webhook has no webhookID")
            if self._same_public_config(current, desired):
                return {"action": "unchanged", "webhook_id": webhook_id, "active": True, "webhook_url": target, "program_count": len(FROZEN_PROGRAM_ADDRESSES)}
            update = await self.client.put(f"{self.API_ROOT}/{webhook_id}", params=self._params(), json=desired)
            update.raise_for_status()
            result = update.json()
            return {"action": "updated", "webhook_id": str(result.get("webhookID") or webhook_id), "active": bool(result.get("active", True)), "webhook_url": target, "program_count": len(FROZEN_PROGRAM_ADDRESSES)}
        create = await self.client.post(self.API_ROOT, params=self._params(), json=desired)
        create.raise_for_status()
        result = create.json()
        return {"action": "created", "webhook_id": str(result.get("webhookID") or ""), "active": bool(result.get("active", True)), "webhook_url": target, "program_count": len(FROZEN_PROGRAM_ADDRESSES)}
