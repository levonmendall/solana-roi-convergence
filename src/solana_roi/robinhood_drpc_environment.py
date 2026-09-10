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


def _is_drpc_endpoint(value: str) -> bool:
    try:
        host = (urlparse(str(value or "").strip()).hostname or "").lower()
    except Exception:
        return False
    return host == "lb.drpc.live" or host.endswith(".drpc.live")


def _resolve_legacy_drpc_preference(http_url: str, ws_url: str) -> None:
    """Map the semantic dRPC preference onto the legacy internal backup name.

    The failover pool historically names legacy pairs ``primary`` and ``backup``.
    Keep that compatibility contract intact, but when the configured preferred
    provider is ``drpc`` and the backup pair is verifiably dRPC, resolve the
    process-local preference to ``backup`` before production composition imports
    the failover layer. This avoids synthesizing a stale JSON provider pool while
    still making dRPC the actual active provider.
    """

    preferred = (os.getenv("ROBINHOOD_PROVIDER_PRIMARY") or "").strip().lower()
    if preferred != "drpc":
        return
    if _is_drpc_endpoint(http_url) and _is_drpc_endpoint(ws_url):
        os.environ["ROBINHOOD_PROVIDER_PRIMARY"] = "backup"


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


def configure_robinhood_drpc_backup() -> bool:
    """Materialize the Render-held dRPC key into the existing provider-pair contract.

    Explicit backup URLs retain precedence. If the runtime already uses the JSON
    provider pool, dRPC is appended there with its semantic name. Otherwise the
    existing legacy backup variables are populated and a semantic ``drpc`` primary
    preference is resolved to that legacy backup pair before production composition.
    No endpoint or key is returned, logged, or exposed through status telemetry.
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
        if not (explicit_http and explicit_ws):
            return False
        _resolve_legacy_drpc_preference(explicit_http, explicit_ws)
        return True

    os.environ["ROBINHOOD_BACKUP_RPC_URL"] = http_url
    os.environ["ROBINHOOD_BACKUP_WS_URL"] = ws_url
    _resolve_legacy_drpc_preference(http_url, ws_url)
    return True


__all__ = [
    "DRPC_KEY_ENV_NAMES",
    "DRPC_NETWORK",
    "configure_robinhood_drpc_backup",
]
