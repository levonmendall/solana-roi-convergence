from __future__ import annotations

import asyncio
import json

import httpx

from solana_roi.deployment import DEFAULT_SCOUT_PROFILES, FROZEN_PROGRAM_ADDRESSES
from solana_roi.split_webhooks import (
    SCOUT_TRANSACTION_TYPES,
    SplitHeliusWebhookManager,
    render_service_url_from_env,
    scout_wallets_from_env,
)

SCOUT_WALLETS = tuple(str(row["wallet"]) for row in DEFAULT_SCOUT_PROFILES)


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
        return FakeResponse({"webhookID": url.rsplit("/", 1)[-1], "active": kwargs["json"]["active"]})


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

    async def put(self, url, **kwargs):
        self.put_attempts += 1
        if self.put_attempts == 1:
            return FakeResponse({"error": "rate limited"}, status_code=429, headers={"retry-after": "0"})
        return await super().put(url, **kwargs)


def test_render_service_url_prefers_full_url_and_falls_back_to_hostname():
    assert render_service_url_from_env({
        "RENDER_EXTERNAL_URL": "https://roi.example",
        "RENDER_EXTERNAL_HOSTNAME": "ignored.example",
    }) == "https://roi.example"
    assert render_service_url_from_env({
        "RENDER_EXTERNAL_HOSTNAME": "roi-fallback.onrender.com",
    }) == "https://roi-fallback.onrender.com"
    assert render_service_url_from_env({}) == ""


def test_scout_wallets_default_and_environment_override():
    assert scout_wallets_from_env({}) == SCOUT_WALLETS
    raw = json.dumps([
        {"wallet": "ScoutA", "tier": "S", "historically_eligible": True},
        {"wallet": "ScoutB", "tier": "A", "historically_eligible": True},
        {"wallet": "Ignore", "tier": "B", "historically_eligible": True},
    ])
    assert scout_wallets_from_env({"SOLANA_ROI_WALLET_PROFILES_JSON": raw}) == ("ScoutA", "ScoutB")


def test_sync_replaces_program_wide_any_feed_with_scout_only_feed():
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
    result = asyncio.run(manager.sync("https://roi.example", scout_wallets=SCOUT_WALLETS))
    assert result["action"] == "credit_efficient_webhooks_synced"
    assert result["program_wide_coverage_implied"] is False
    assert result["scout_wallet_count"] == 3
    assert len(client.puts) == 1
    desired = client.puts[0][1]
    assert desired["webhookURL"] == target
    assert desired["transactionTypes"] == list(SCOUT_TRANSACTION_TYPES)
    assert tuple(desired["accountAddresses"]) == SCOUT_WALLETS
    assert not (set(desired["accountAddresses"]) & set(FROZEN_PROGRAM_ADDRESSES))
    assert client.posts == []


def test_sync_disables_legacy_raw_program_feed():
    enhanced_url = "https://roi.example/v1/ingestion/helius"
    raw_url = "https://roi.example/v1/ingestion/helius/pump-raw"
    existing = [
        {
            "webhookID": "enhanced-id",
            "webhookURL": enhanced_url,
            "transactionTypes": list(SCOUT_TRANSACTION_TYPES),
            "accountAddresses": list(SCOUT_WALLETS),
            "webhookType": "enhanced",
            "authHeader": "secret",
            "active": True,
        },
        {
            "webhookID": "raw-id",
            "webhookURL": raw_url,
            "transactionTypes": ["ANY"],
            "accountAddresses": [FROZEN_PROGRAM_ADDRESSES[0]],
            "webhookType": "raw",
            "authHeader": "secret",
            "active": True,
        },
    ]
    client = FakeClient(existing)
    result = asyncio.run(
        SplitHeliusWebhookManager(api_key="k", auth_header="secret", client=client).sync(
            "https://roi.example", scout_wallets=SCOUT_WALLETS
        )
    )
    assert client.puts == []
    assert client.posts == []
    assert len(client.patches) == 1
    assert client.patches[0][1] == {"active": False}
    actions = {row["feed"]: row["action"] for row in result["feeds"]}
    assert actions["scout_trigger_feed"] == "unchanged"
    assert actions["legacy_pump_fun_raw_feed"] == "disabled"


def test_disabled_scout_feed_is_reenabled():
    existing = [{
        "webhookID": "enhanced-id",
        "webhookURL": "https://roi.example/v1/ingestion/helius",
        "transactionTypes": list(SCOUT_TRANSACTION_TYPES),
        "accountAddresses": list(SCOUT_WALLETS),
        "webhookType": "enhanced",
        "authHeader": "secret",
        "active": False,
    }]
    client = FakeClient(existing)
    result = asyncio.run(
        SplitHeliusWebhookManager(api_key="k", auth_header="secret", client=client).sync(
            "https://roi.example", scout_wallets=SCOUT_WALLETS
        )
    )
    assert client.puts == []
    assert len(client.patches) == 1
    assert client.patches[0][1] == {"active": True}
    assert result["feeds"][0]["action"] == "reenabled"


def test_auth_rotation_updates_scout_feed_without_exposing_secret_in_result():
    existing = [{
        "webhookID": "enhanced-id",
        "webhookURL": "https://roi.example/v1/ingestion/helius",
        "transactionTypes": list(SCOUT_TRANSACTION_TYPES),
        "accountAddresses": list(SCOUT_WALLETS),
        "webhookType": "enhanced",
        "authHeader": "old",
        "active": True,
    }]
    client = FakeClient(existing)
    result = asyncio.run(
        SplitHeliusWebhookManager(api_key="k", auth_header="new-secret", client=client).sync(
            "https://roi.example", scout_wallets=SCOUT_WALLETS
        )
    )
    assert len(client.puts) == 1
    assert client.puts[0][1]["authHeader"] == "new-secret"
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
    result = asyncio.run(manager.sync("https://roi.example", scout_wallets=SCOUT_WALLETS))
    assert client.get_calls == 3
    assert sleeps == [0.25, 2.0]
    assert result["management_retry_count"] == 2
    assert result["last_retry_status_code"] == 429


def test_rate_limited_update_retries_before_scout_containment():
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
    manager = SplitHeliusWebhookManager(api_key="k", auth_header="secret", client=client, sleep_fn=fake_sleep)
    result = asyncio.run(manager.sync("https://roi.example", scout_wallets=SCOUT_WALLETS))
    assert client.put_attempts == 2
    assert sleeps == [0.0]
    assert result["management_retry_count"] == 1
