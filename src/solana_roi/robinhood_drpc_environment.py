from __future__ import annotations

import json
import os
from urllib.parse import quote, urlparse


DRPC_NETWORK = "robinhood"
DRPC_HTTP_BASE = f"https://lb.drpc.live/{DRPC_NETWORK}"
DRPC_WS_BASE = f"wss://lb.drpc.live/{DRPC_NETWORK}"
DRPC_KEY_ENV_NAMES = (
    "ROBINHOOD_DRPC_API_KEY",
    "DRPC_API_KEY",
    "DRPC_KEY",
    "SOLANA_ROI_DRPC_API_KEY",
)


def _api_key() -> str:
    for name in DRPC_KEY_ENV_NAMES:
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return ""


def _endpoint_pair(key: str) -> tuple[str, str]:
    encoded = quote(str(key).strip(), safe="")
    return f"{DRPC_HTTP_BASE}/{encoded}", f"{DRPC_WS_BASE}/{encoded}"


def _provider_name(http_url: str, fallback: str) -> str:
    try:
        host = (urlparse(str(http_url or "").strip()).hostname or "").lower()
    except Exception:
        return fallback
    if host == "lb.drpc.live" or host.endswith(".drpc.live"):
        return "drpc"
    if host.endswith("alchemy.com"):
        return "alchemy"
    return fallback


def _legacy_primary_pair() -> tuple[str, str] | None:
    http_url = (os.getenv("ROBINHOOD_RPC_URL") or "").strip()
    if not http_url:
        return None
    ws_url = (os.getenv("ROBINHOOD_WS_URL") or "").strip()
    if not ws_url:
        try:
            parsed = urlparse(http_url)
        except Exception:
            return None
        if parsed.scheme.lower() != "https" or not parsed.netloc:
            return None
        ws_url = parsed._replace(scheme="wss").geturl()
    return http_url, ws_url


def _append_json_provider(http_url: str, ws_url: str) -> bool:
    raw = (os.getenv("ROBINHOOD_RPC_ENDPOINTS_JSON") or "").strip()
    if not raw:
        return False
    try:
        payload = json.loads(raw)
    except Exception:
        return False
    if not isinstance(payload, list):
        return False

    normalized_http = http_url.rstrip("/").lower()
    normalized_ws = ws_url.rstrip("/").lower()
    for item in payload:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip().lower()
        existing_http = str(item.get("http") or "").strip().rstrip("/").lower()
        existing_ws = str(item.get("ws") or "").strip().rstrip("/").lower()
        if name == "drpc" or (existing_http == normalized_http and existing_ws == normalized_ws):
            return True

    payload.append({"name": "drpc", "http": http_url, "ws": ws_url})
    os.environ["ROBINHOOD_RPC_ENDPOINTS_JSON"] = json.dumps(payload, separators=(",", ":"))
    return True


def _materialize_semantic_pool(http_url: str, ws_url: str) -> bool:
    primary = _legacy_primary_pair()
    if primary is None:
        return False
    primary_http, primary_ws = primary
    if not primary_http or not primary_ws:
        return False
    primary_name = _provider_name(primary_http, "primary")
    payload = [
        {"name": primary_name, "http": primary_http, "ws": primary_ws},
        {"name": "drpc", "http": http_url, "ws": ws_url},
    ]
    os.environ["ROBINHOOD_RPC_ENDPOINTS_JSON"] = json.dumps(payload, separators=(",", ":"))
    return True


def configure_robinhood_drpc_backup() -> bool:
    """Materialize the Render-held dRPC key into the private provider-pool contract.

    Explicit JSON or backup configuration retains precedence. When production is
    still using the legacy primary-only variables, promote that pair plus dRPC into
    a semantically named JSON provider pool. This lets ROBINHOOD_PROVIDER_PRIMARY
    select `drpc` deterministically while preserving the original private provider
    (normally Alchemy) as failover capacity. No endpoint or key is returned, logged,
    or exposed through status telemetry.
    """

    key = _api_key()
    if not key:
        return False

    http_url, ws_url = _endpoint_pair(key)
    if _append_json_provider(http_url, ws_url):
        return True

    explicit_http = (os.getenv("ROBINHOOD_BACKUP_RPC_URL") or "").strip()
    explicit_ws = (os.getenv("ROBINHOOD_BACKUP_WS_URL") or "").strip()
    if explicit_http or explicit_ws:
        return bool(explicit_http and explicit_ws)

    if _materialize_semantic_pool(http_url, ws_url):
        return True

    os.environ["ROBINHOOD_BACKUP_RPC_URL"] = http_url
    os.environ["ROBINHOOD_BACKUP_WS_URL"] = ws_url
    return True


__all__ = [
    "DRPC_KEY_ENV_NAMES",
    "DRPC_NETWORK",
    "configure_robinhood_drpc_backup",
]
