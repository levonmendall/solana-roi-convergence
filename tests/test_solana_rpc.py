from __future__ import annotations

import asyncio
import json

import pytest

from solana_roi.solana_rpc import RpcEndpoint, SolanaRpcPool, rpc_endpoints_from_env


class FakeResponse:
    def __init__(self, result, *, status_code=200):
        self.status_code = status_code
        self._result = result

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")

    def json(self):
        return {"jsonrpc": "2.0", "id": 1, "result": self._result}


class FakeClient:
    def __init__(self, result, *, delay=0.0, error=None):
        self.result = result
        self.delay = delay
        self.error = error
        self.calls = 0

    async def post(self, _url, *, json):
        self.calls += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.error is not None:
            raise self.error
        return FakeResponse(self.result)


def _endpoints():
    return (
        RpcEndpoint("slow", "https://slow.example", "wss://slow.example"),
        RpcEndpoint("fast", "https://fast.example", "wss://fast.example"),
    )


def test_latency_hedge_accepts_first_valid_secondary_response():
    slow = FakeClient({"slot": 1}, delay=0.05)
    fast = FakeClient({"slot": 2})
    pool = SolanaRpcPool(
        _endpoints(),
        hedge_delay_seconds=0.001,
        clients={"slow": slow, "fast": fast},
    )
    result, provider, _latency = asyncio.run(pool.call_with_meta("getSlot", [], hedge=True))
    assert result == {"slot": 2}
    assert provider == "fast"
    assert slow.calls == 1
    assert fast.calls == 1


def test_nonhedged_request_falls_back_after_primary_failure():
    failed = FakeClient(None, error=RuntimeError("down"))
    good = FakeClient(123)
    pool = SolanaRpcPool(
        _endpoints(),
        clients={"slow": failed, "fast": good},
    )
    result, provider, _latency = asyncio.run(pool.call_with_meta("getSlot", [], hedge=False))
    assert result == 123
    assert provider == "fast"
    assert failed.calls == 1
    assert good.calls == 1


def test_endpoint_configuration_rejects_duplicates_and_requires_secure_transports():
    duplicate = json.dumps([
        {"name": "a", "http": "https://same.example", "ws": "wss://a.example"},
        {"name": "b", "http": "https://same.example", "ws": "wss://b.example"},
    ])
    with pytest.raises(ValueError):
        rpc_endpoints_from_env({"SOLANA_ROI_RPC_ENDPOINTS_JSON": duplicate})

    insecure = json.dumps([
        {"name": "a", "http": "http://a.example", "ws": "ws://a.example"},
    ])
    with pytest.raises(ValueError):
        rpc_endpoints_from_env({"SOLANA_ROI_RPC_ENDPOINTS_JSON": insecure})


def test_alchemy_secret_is_idle_by_default_without_changing_public_endpoints():
    base = json.dumps([
        {"name": "a", "http": "https://a.example", "ws": "wss://a.example"},
        {"name": "b", "http": "https://b.example", "ws": "wss://b.example"},
    ])
    endpoints = rpc_endpoints_from_env({
        "SOLANA_ROI_RPC_ENDPOINTS_JSON": base,
        "SOLANA_ROI_ALCHEMY_API_KEY": "configured-but-idle",
    })
    assert [endpoint.name for endpoint in endpoints] == ["a", "b"]


def test_alchemy_can_be_explicitly_opted_in_without_exposing_secret_in_status():
    base = json.dumps([
        {"name": "a", "http": "https://a.example", "ws": "wss://a.example"},
        {"name": "b", "http": "https://b.example", "ws": "wss://b.example"},
    ])
    secret = "test-secret"
    endpoints = rpc_endpoints_from_env({
        "SOLANA_ROI_RPC_ENDPOINTS_JSON": base,
        "SOLANA_ROI_ALCHEMY_API_KEY": secret,
        "SOLANA_ROI_ENABLE_METERED_ALCHEMY": "true",
    })
    assert [endpoint.name for endpoint in endpoints] == ["a", "b", "alchemy"]
    assert endpoints[2].http_url == f"https://solana-mainnet.g.alchemy.com/v2/{secret}"
    assert endpoints[2].ws_url == f"wss://solana-mainnet.streaming.alchemy.com/v2/{secret}"

    pool = SolanaRpcPool(
        endpoints,
        clients={endpoint.name: FakeClient(1) for endpoint in endpoints},
    )
    status = pool.status()
    assert status["endpoint_count"] == 3
    assert status["endpoints"][2]["http_host"] == "solana-mainnet.g.alchemy.com"
    assert status["endpoints"][2]["ws_host"] == "solana-mainnet.streaming.alchemy.com"
    assert secret not in repr(status)


def test_explicit_alchemy_endpoint_is_also_idle_without_metered_opt_in():
    explicit = json.dumps([
        {"name": "a", "http": "https://a.example", "ws": "wss://a.example"},
        {"name": "b", "http": "https://b.example", "ws": "wss://b.example"},
        {"name": "alchemy", "http": "https://solana-mainnet.g.alchemy.com/v2/external", "ws": "wss://solana-mainnet.streaming.alchemy.com/v2/external"},
    ])
    endpoints = rpc_endpoints_from_env({
        "SOLANA_ROI_RPC_ENDPOINTS_JSON": explicit,
        "SOLANA_ROI_ALCHEMY_API_KEY": "ignored-because-explicit",
    })
    assert [endpoint.name for endpoint in endpoints] == ["a", "b"]

    opted_in = rpc_endpoints_from_env({
        "SOLANA_ROI_RPC_ENDPOINTS_JSON": explicit,
        "SOLANA_ROI_ALCHEMY_API_KEY": "ignored-because-explicit",
        "SOLANA_ROI_ENABLE_METERED_ALCHEMY": "true",
    })
    assert len(opted_in) == 3
    assert [endpoint.name for endpoint in opted_in].count("alchemy") == 1


def test_status_exposes_hosts_not_full_endpoint_urls():
    endpoints = (
        RpcEndpoint("keyed", "https://rpc.example/path?api-key=super-secret", "wss://ws.example/path?key=hidden"),
    )
    pool = SolanaRpcPool(endpoints, clients={"keyed": FakeClient(1)})
    status = pool.status()
    assert status["endpoints"][0]["http_host"] == "rpc.example"
    assert status["endpoints"][0]["ws_host"] == "ws.example"
    assert "super-secret" not in repr(status)
    assert "hidden" not in repr(status)
