from __future__ import annotations

import asyncio

from solana_roi.deployment import (
    DEFAULT_SCOUT_PROFILES,
    FROZEN_PROGRAM_ADDRESSES,
    HeliusWebhookManager,
    default_scout_profiles_json,
    deployment_preflight,
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

    async def get(self, url, **kwargs):
        assert "api-key" in kwargs["params"]
        return FakeResponse(self.existing)

    async def post(self, url, **kwargs):
        self.posts.append(kwargs["json"])
        payload = dict(kwargs["json"])
        payload.update({"webhookID": "created-id", "active": True})
        return FakeResponse(payload)

    async def put(self, url, **kwargs):
        self.puts.append((url, kwargs["json"]))
        payload = dict(kwargs["json"])
        payload.update({"webhookID": "updated-id", "active": True})
        return FakeResponse(payload)


def _complete_env():
    return {
        "PAPER_ONLY": "true",
        "SOLANA_NETWORK": "mainnet-beta",
        "SOLANA_ROI_PROGRAM_WIDE_SWAP_COVERAGE": "true",
        "SOLANA_ROI_SHADOW_CLOCK_ENABLED": "true",
        "HELIUS_API_KEY": "helius-secret",
        "HELIUS_WEBHOOK_AUTH": "webhook-secret",
        "JUPITER_API_KEY": "jupiter-secret",
        "SOLANA_ROI_COHORT_ARM_AUTH": "cohort-secret",
        "SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY": "11111111111111111111111111111111",
        "SOLANA_ROI_WALLET_PROFILES_JSON": default_scout_profiles_json(),
    }


def test_preflight_passes_complete_paper_only_configuration_without_exposing_secrets():
    env = _complete_env()
    status = deployment_preflight(env)
    assert status["ready_for_live_shadow_collection"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
    rendered = repr(status)
    assert "helius-secret" not in rendered
    assert "webhook-secret" not in rendered
    assert "jupiter-secret" not in rendered
    assert len(DEFAULT_SCOUT_PROFILES) == 3


def test_render_preflight_requires_persistent_disk_release_and_rejects_private_key_material():
    env = _complete_env()
    env.update({
        "RENDER": "true",
        "SOLANA_ROI_DB_PATH": "/tmp/reset.sqlite3",
        "RENDER_GIT_COMMIT": "bad",
        "SOLANA_ROI_PRIVATE_KEY": "must-never-be-here",
    })
    status = deployment_preflight(env)
    assert status["ready_for_live_shadow_collection"] is False
    failed = {row["name"] for row in status["checks"] if not row["ok"]}
    assert {"persistent_sqlite", "release_commit", "no_private_key_material"} <= failed


def test_preflight_fails_closed_on_malformed_scout_sample_size():
    env = _complete_env()
    env["SOLANA_ROI_WALLET_PROFILES_JSON"] = '[{"wallet":"11111111111111111111111111111111","entity_id":"x","tier":"S","first_touch_sample_size":"bad","historically_eligible":true}]'
    status = deployment_preflight(env)
    assert status["ready_for_live_shadow_collection"] is False
    check = next(row for row in status["checks"] if row["name"] == "frozen_scout_cohort")
    assert check["ok"] is False


def test_helius_sync_creates_exact_program_wide_enhanced_webhook():
    client = FakeClient()
    manager = HeliusWebhookManager(api_key="k", auth_header="secret", client=client)
    result = asyncio.run(manager.sync("https://roi.example"))
    assert result["action"] == "created"
    desired = client.posts[0]
    assert desired["webhookURL"] == "https://roi.example/v1/ingestion/helius"
    assert desired["transactionTypes"] == ["ANY"]
    assert set(desired["accountAddresses"]) == set(FROZEN_PROGRAM_ADDRESSES)
    assert desired["webhookType"] == "enhanced"
    assert desired["authHeader"] == "secret"


def test_helius_sync_is_idempotent_when_complete_configuration_matches():
    target = "https://roi.example/v1/ingestion/helius"
    existing = [{
        "webhookID": "existing-id",
        "webhookURL": target,
        "transactionTypes": ["ANY"],
        "accountAddresses": list(FROZEN_PROGRAM_ADDRESSES),
        "webhookType": "enhanced",
        "authHeader": "secret",
        "active": True,
    }]
    client = FakeClient(existing)
    manager = HeliusWebhookManager(api_key="k", auth_header="secret", client=client)
    result = asyncio.run(manager.sync("https://roi.example"))
    assert result["action"] == "unchanged"
    assert result["webhook_id"] == "existing-id"
    assert client.posts == []
    assert client.puts == []


def test_helius_sync_updates_when_auth_header_rotates():
    target = "https://roi.example/v1/ingestion/helius"
    existing = [{
        "webhookID": "existing-id",
        "webhookURL": target,
        "transactionTypes": ["ANY"],
        "accountAddresses": list(FROZEN_PROGRAM_ADDRESSES),
        "webhookType": "enhanced",
        "authHeader": "old-secret",
        "active": True,
    }]
    client = FakeClient(existing)
    manager = HeliusWebhookManager(api_key="k", auth_header="new-secret", client=client)
    result = asyncio.run(manager.sync("https://roi.example"))
    assert result["action"] == "updated"
    assert client.posts == []
    assert len(client.puts) == 1
    assert client.puts[0][1]["authHeader"] == "new-secret"
