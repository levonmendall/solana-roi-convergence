from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import robinhood_entity_resolution_repair as entity_repair
from solana_roi.robinhood_blockscout_pro_repair import (
    DEFAULT_PRO_API_URL,
    _entity_anchor_fetch_pro,
    install_robinhood_blockscout_pro_repair,
)


ACTOR = "0x" + "1" * 40
FUNDER = "0x" + "2" * 40
OTHER = "0x" + "3" * 40


class Response:
    status_code = 200

    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class Client:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    async def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return Response(self.payload)


def _plane(payload=None):
    client = Client(payload or {"status": "1", "message": "OK", "result": []})
    return SimpleNamespace(
        rpc=SimpleNamespace(client=client),
        _entity_cache={},
        _entity_resolution_failures=0,
    ), client


def test_missing_api_key_fails_closed_without_external_request(monkeypatch) -> None:
    monkeypatch.delenv("BLOCKSCOUT_API_KEY", raising=False)
    plane, client = _plane()
    result = asyncio.run(_entity_anchor_fetch_pro(plane, ACTOR))
    assert result is None
    assert client.calls == []
    stats = entity_repair._stats(plane)
    assert stats["missing_api_key_failures"] == 1
    assert stats["last_error_type"] == "BlockscoutApiKeyMissing"


def test_pro_query_is_single_call_inbound_oldest_first(monkeypatch) -> None:
    monkeypatch.setenv("BLOCKSCOUT_API_KEY", "proapi_test_only")
    payload = {
        "status": "1",
        "message": "OK",
        "result": [
            {
                "blockNumber": "10",
                "from": FUNDER,
                "to": ACTOR,
                "value": "1000",
            },
            {
                "blockNumber": "20",
                "from": OTHER,
                "to": ACTOR,
                "value": "2000",
            },
        ],
    }
    plane, client = _plane(payload)
    result = asyncio.run(_entity_anchor_fetch_pro(plane, ACTOR))
    assert result == FUNDER
    assert len(client.calls) == 1
    url, kwargs = client.calls[0]
    assert url == DEFAULT_PRO_API_URL
    params = kwargs["params"]
    assert params["chain_id"] == 4663
    assert params["module"] == "account"
    assert params["action"] == "txlist"
    assert params["filterby"] == "to"
    assert params["sort"] == "asc"
    assert params["page"] == 1
    assert params["apikey"] == "proapi_test_only"
    assert entity_repair._stats(plane)["pro_api_requests"] == 1


def test_successful_empty_history_is_authoritative_singleton(monkeypatch) -> None:
    monkeypatch.setenv("BLOCKSCOUT_API_KEY", "proapi_test_only")
    plane, client = _plane({"status": "0", "message": "No transactions found", "result": []})
    result = asyncio.run(_entity_anchor_fetch_pro(plane, ACTOR))
    assert result == ACTOR
    assert len(client.calls) == 1
    assert entity_repair._stats(plane)["resolved_singletons"] == 1


def test_install_keeps_identity_rules_and_exposes_key_state(monkeypatch) -> None:
    monkeypatch.setenv("BLOCKSCOUT_API_KEY", "proapi_test_only")

    class Plane:
        def status(self):
            return {"entity_resolution": {}}

    install_robinhood_blockscout_pro_repair(Plane)
    payload = Plane().status()
    entity = payload["entity_resolution"]
    assert entity["provider_api"] == "blockscout-pro-universal"
    assert entity["api_key_configured"] is True
    assert entity["api_key_value_exposed"] is False
    assert entity["max_provider_calls_per_uncached_actor"] == 1
    assert payload["blockscout_pro_entity_repair"]["entity_independence_rules_changed"] is False
