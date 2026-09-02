from __future__ import annotations

import json
import os
from collections.abc import Callable
from typing import Any
from urllib.parse import urlsplit

from .direct_solana import DirectSolanaIngestionPlane
from .solana_rpc import RpcEndpoint


# No unauthenticated tertiary WSS is trusted by default. Production proved the
# shared dRPC public WSS could not establish any of the ten required subscriptions.
# A future managed/free WSS endpoint can still be supplied explicitly through the
# environment without putting it into the HTTP hydration/risk/quote pool.
_DEFAULT_STREAM_ONLY_ENDPOINTS: tuple[RpcEndpoint, ...] = ()


def stream_only_endpoints_from_env(env: dict[str, str] | None = None) -> tuple[RpcEndpoint, ...]:
    values = env if env is not None else os.environ
    raw = str(values.get("SOLANA_ROI_STREAM_ONLY_ENDPOINTS_JSON") or "").strip()
    if not raw:
        return _DEFAULT_STREAM_ONLY_ENDPOINTS

    payload = json.loads(raw)
    if not isinstance(payload, list):
        raise ValueError("SOLANA_ROI_STREAM_ONLY_ENDPOINTS_JSON must be a JSON array")

    rows: list[RpcEndpoint] = []
    seen_names: set[str] = set()
    seen_ws: set[str] = set()
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("stream-only RPC endpoint entries must be objects")
        name = str(item.get("name") or "").strip()
        http_url = str(item.get("http") or item.get("http_url") or "").strip()
        ws_url = str(item.get("ws") or item.get("ws_url") or "").strip()
        if not name or not http_url.startswith("https://") or not ws_url.startswith("wss://"):
            raise ValueError("stream-only endpoint requires name, https URL, and wss URL")
        if name in seen_names or ws_url.rstrip("/") in seen_ws:
            raise ValueError("stream-only endpoints must be distinct")
        seen_names.add(name)
        seen_ws.add(ws_url.rstrip("/"))
        rows.append(RpcEndpoint(name=name, http_url=http_url, ws_url=ws_url))
    return tuple(rows)


def _append_stream_only_endpoints(original: Callable[..., None]) -> Callable[..., None]:
    def init(self: Any, *args: Any, **kwargs: Any) -> None:
        original(self, *args, **kwargs)
        base = tuple(self.endpoints)
        base_ws = {endpoint.ws_url.rstrip("/") for endpoint in base}
        base_names = {endpoint.name for endpoint in base}
        extras: list[RpcEndpoint] = []
        for endpoint in stream_only_endpoints_from_env():
            if endpoint.name in base_names or endpoint.ws_url.rstrip("/") in base_ws:
                continue
            extras.append(endpoint)
            base_names.add(endpoint.name)
            base_ws.add(endpoint.ws_url.rstrip("/"))

        self.endpoints = base + tuple(extras)
        self._roi_base_stream_provider_names = tuple(endpoint.name for endpoint in base)
        self._roi_stream_only_endpoint_names = tuple(endpoint.name for endpoint in extras)

    setattr(init, "_roi_stream_tertiary", True)
    return init


def _status_with_stream_redundancy(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        stream_only_names = tuple(getattr(self, "_roi_stream_only_endpoint_names", ()) or ())
        base_names = tuple(getattr(self, "_roi_base_stream_provider_names", ()) or ())
        endpoint_by_name = {endpoint.name: endpoint for endpoint in tuple(self.endpoints)}

        rpc_status: dict[str, Any] = {}
        try:
            candidate = self.rpc.status()
            if isinstance(candidate, dict):
                rpc_status = candidate
        except Exception:
            rpc_status = {}

        payload["stream_redundancy"] = {
            "stream_provider_count": len(tuple(self.endpoints)),
            "base_stream_providers": list(base_names),
            "stream_only_providers": [
                {
                    "name": name,
                    "ws_host": urlsplit(endpoint_by_name[name].ws_url).netloc,
                    "http_hydration_enabled": False,
                }
                for name in stream_only_names
                if name in endpoint_by_name
            ],
            "hydration_rpc_provider_count": int(rpc_status.get("endpoint_count") or 0),
            "drpc_public_http_hydration_retired": True,
            "drpc_public_wss_default_retired": True,
            "explicit_stream_only_override_supported": True,
            "strategy_scope_reduced": False,
        }
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "stream_provider_count": len(tuple(self.endpoints)),
                    "stream_only_provider_count": len(stream_only_names),
                    "stream_and_hydration_provider_sets_decoupled": True,
                    "tertiary_stream_can_authorize_hydration": False,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_stream_tertiary", True)
    return status


def install_stream_redundancy() -> None:
    current_init = DirectSolanaIngestionPlane.__init__
    if not bool(getattr(current_init, "_roi_stream_tertiary", False)):
        DirectSolanaIngestionPlane.__init__ = _append_stream_only_endpoints(current_init)  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_stream_tertiary", False)):
        DirectSolanaIngestionPlane.status = _status_with_stream_redundancy(current_status)  # type: ignore[method-assign]


__all__ = ["install_stream_redundancy", "stream_only_endpoints_from_env"]
