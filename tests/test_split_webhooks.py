from __future__ import annotations

import asyncio

import httpx

from solana_roi.deployment import FROZEN_PROGRAM_ADDRESSES, PUMP_PROGRAM_ID
from solana_roi.split_webhooks import (
    ENHANCED_PROGRAM_ADDRESSES,
    ENHANCED_TRANSACTION_TYPES,
    SplitHeliusWebhookManager,
    render_service_url_from_env,
)


class FakeResponse:
    def __init__(self, payload, *, status_code=200, headers=None):
        self.payload = payload
        self.status_code = status_code
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            request = httpx.Request("GET", "https://api-mainnet.helius-rpc.com/v0/webhooks")
            response = httpx.Response(self.status_code, request=request)
            raise httpx.HTTPStatusError("fake Helius error", request=request, response=response)
        return None

    def json(self):
        return self.payload


class FakeClient:
    def __init__(self, existing=None):
        self.existing = list(existing or [])
        self.posts = []
        self.puts = []
        self.patches = []
        self.get_calls = 0

    async def get(self, url, **kwargs):
        self.get_calls += 1
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


class RateLimitedListClient(FakeClient):
    def __init__(self, existing=None):
        super().__init__(existing)
        self.responses = [
            FakeResponse({"error": "rate limited"}, status_code=429, headers={"retry-after": "0.25"}),
            FakeResponse({"error": "rate limited"}, status_code=429),
            FakeResponse(self.existing),
        ]

    async def get(self, url, **kwargs):
        self.get_calls += 1
        assert "api-key" in kwargs["params"]
        return self.responses.pop(0)


class RateLimitedMutationClient(FakeClient):
    def __init__(self, existing=None):
        super().__init__(existing)
        self.put_attempts = 0
        self.post_attempts = 0

    async def put(self, url, **kwargs):
        self.put_attempts += 1
        if self.put_attempts == 1:
            return FakeResponse({"error": "rate limited"}, status_code=429, headers={"retry-after": "0"})
        return await super().put(url, **kwargs)

    async def post(self, url, **kwargs):
        self.post_attempts += 1
        if self.post_attempts == 1:
            return FakeResponse({"error": "unavailable"}, status_code=503, headers={"retry-after": "0"})
        return await super().post(url, **kwargs)


def test_render_service_url_prefers_full_url_and_falls_back_to_hostname():
    assert render_service_url_from_env({
        "RENDER_EXTERNAL_URL": "https://roi.example",
        "RENDER_EXTERNAL_HOSTNAME": "ignored.example",
    }) == "https://roi.example"
    assert render_service_url_from_env({
        "RENDER_EXTERNAL_HOSTNAME": "roi-fallback.onrender.com",
    }) == "https://roi-fallback.onrender.com"
    assert render_service_url_from_env({}) == ""


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


def test_rate_limited_list_honors_retry_after_and_recovers():
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    client = RateLimitedListClient([])
    manager = SplitHeliusWebhookManager(
        api_key="k",
        auth_header="secret",
        client=client,
        sleep_fn=fake_sleep,
        initial_backoff_seconds=1.0,
    )
    result = asyncio.run(manager.sync("https://roi.example"))
    assert client.get_calls == 3
    assert sleeps == [0.25, 2.0]
    assert result["management_retry_count"] == 2
    assert result["last_retry_status_code"] == 429


def test_rate_limited_mutations_retry_before_updating_and_creating():
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

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
    client = RateLimitedMutationClient(existing)
    manager = SplitHeliusWebhookManager(
        api_key="k",
        auth_header="secret",
        client=client,
        sleep_fn=fake_sleep,
    )
    result = asyncio.run(manager.sync("https://roi.example"))
    assert client.put_attempts == 2
    assert client.post_attempts == 2
    assert sleeps == [0.0, 0.0]
    assert result["management_retry_count"] == 2
