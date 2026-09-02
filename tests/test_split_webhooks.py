from __future__ import annotations

import asyncio

from solana_roi.deployment import FROZEN_PROGRAM_ADDRESSES, PUMP_PROGRAM_ID
from solana_roi.split_webhooks import (
    ENHANCED_PROGRAM_ADDRESSES,
    ENHANCED_TRANSACTION_TYPES,
    SplitHeliusWebhookManager,
)


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeClient:
    def __init__(self, existing=None):
        self.existing = list(existing or [])
        self.posts = []
        self.puts = []
        self.patches = []

    async def get(self, url, **kwargs):
        assert "api-key" in kwargs["params"]
        return FakeResponse(self.existing)

    async def post(self, url, **kwargs):
        self.posts.append(kwargs["json"])
        payload = dict(kwargs["json"])
        payload.update({"webhookID": f"created-{len(self.posts)}", "active": True})
        return FakeResponse(payload)

    async def put(self, url, **kwargs):
        self.puts.append((url, kwargs["json"]))
        payload = dict(kwargs["json"])
        payload.update({"webhookID": url.rsplit("/", 1)[-1], "active": True})
        return FakeResponse(payload)

    async def patch(self, url, **kwargs):
        self.patches.append((url, kwargs["json"]))
        return FakeResponse({"webhookID": url.rsplit("/", 1)[-1], "active": True})


def test_split_sync_updates_old_any_feed_and_creates_raw_pump_feed():
    target = "https://roi.example/v1/ingestion/helius"
    existing = [{
        "webhookID": "old-id",
        "webhookURL": target,
        "transactionTypes": ["ANY"],
        "accountAddresses": list(FROZEN_PROGRAM_ADDRESSES),
        "webhookType": "enhanced",
        "authHeader": "secret",
        "active": True,
    }]
    client = FakeClient(existing)
    manager = SplitHeliusWebhookManager(api_key="k", auth_header="secret", client=client)
    result = asyncio.run(manager.sync("https://roi.example"))
    assert result["action"] == "split_webhooks_synced"
    assert len(client.puts) == 1
    enhanced = client.puts[0][1]
    assert enhanced["webhookURL"] == target
    assert set(enhanced["transactionTypes"]) == set(ENHANCED_TRANSACTION_TYPES)
    assert set(enhanced["accountAddresses"]) == set(ENHANCED_PROGRAM_ADDRESSES)
    assert PUMP_PROGRAM_ID not in enhanced["accountAddresses"]
    assert len(client.posts) == 1
    raw = client.posts[0]
    assert raw["webhookURL"] == "https://roi.example/v1/ingestion/helius/pump-raw"
    assert raw["webhookType"] == "raw"
    assert raw["accountAddresses"] == [PUMP_PROGRAM_ID]


def test_split_sync_is_idempotent_and_reenables_disabled_feed():
    enhanced_url = "https://roi.example/v1/ingestion/helius"
    raw_url = "https://roi.example/v1/ingestion/helius/pump-raw"
    existing = [
        {
            "webhookID": "enhanced-id",
            "webhookURL": enhanced_url,
            "transactionTypes": list(ENHANCED_TRANSACTION_TYPES),
            "accountAddresses": list(ENHANCED_PROGRAM_ADDRESSES),
            "webhookType": "enhanced",
            "authHeader": "secret",
            "active": False,
        },
        {
            "webhookID": "raw-id",
            "webhookURL": raw_url,
            "transactionTypes": [],
            "accountAddresses": [PUMP_PROGRAM_ID],
            "webhookType": "raw",
            "authHeader": "secret",
            "active": True,
        },
    ]
    client = FakeClient(existing)
    result = asyncio.run(SplitHeliusWebhookManager(api_key="k", auth_header="secret", client=client).sync("https://roi.example"))
    assert client.puts == []
    assert client.posts == []
    assert len(client.patches) == 1
    assert client.patches[0][1] == {"active": True}
    actions = {row["feed"]: row["action"] for row in result["feeds"]}
    assert actions["enhanced_swap_feed"] == "reenabled"
    assert actions["pump_fun_raw_feed"] == "unchanged"


def test_auth_rotation_updates_both_feeds_without_exposing_secret_in_result():
    existing = [
        {
            "webhookID": "enhanced-id",
            "webhookURL": "https://roi.example/v1/ingestion/helius",
            "transactionTypes": list(ENHANCED_TRANSACTION_TYPES),
            "accountAddresses": list(ENHANCED_PROGRAM_ADDRESSES),
            "webhookType": "enhanced",
            "authHeader": "old",
            "active": True,
        },
        {
            "webhookID": "raw-id",
            "webhookURL": "https://roi.example/v1/ingestion/helius/pump-raw",
            "transactionTypes": [],
            "accountAddresses": [PUMP_PROGRAM_ID],
            "webhookType": "raw",
            "authHeader": "old",
            "active": True,
        },
    ]
    client = FakeClient(existing)
    result = asyncio.run(SplitHeliusWebhookManager(api_key="k", auth_header="new-secret", client=client).sync("https://roi.example"))
    assert len(client.puts) == 2
    assert all(row[1]["authHeader"] == "new-secret" for row in client.puts)
    assert "new-secret" not in repr(result)
