from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx
import pytest

from solana_roi import robinhood_provider_capacity_budget as capacity


@pytest.fixture(autouse=True)
def _reset_capacity(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("ROBINHOOD_PROVIDER_MONTHLY_USAGE_PATH", str(tmp_path / "usage.json"))
    monkeypatch.setenv("ROBINHOOD_PROVIDER_RESTART_SAFETY_REQUESTS", "0")
    monkeypatch.setenv("ROBINHOOD_PROVIDER_USAGE_PERSIST_EVERY_REQUESTS", "1")
    monkeypatch.setenv("ROBINHOOD_PROVIDER_USAGE_PERSIST_EVERY_SECONDS", "0.1")
    monkeypatch.setenv("ROBINHOOD_CHAINSTACK_ROLLING_RPS_LIMIT", "20")
    monkeypatch.setenv("ROBINHOOD_CHAINSTACK_MONTHLY_REQUEST_LIMIT", "3000000")
    monkeypatch.setenv("ROBINHOOD_ALCHEMY_MONTHLY_REQUEST_LIMIT", "3000000")
    monkeypatch.setenv("ROBINHOOD_PROVIDER_COMBINED_MONTHLY_REQUEST_LIMIT", "6000000")
    monkeypatch.setenv("ROBINHOOD_PROVIDER_MONTHLY_CRITICAL_RESERVE_FRACTION", "0.10")
    monkeypatch.setenv("ROBINHOOD_PROVIDER_BACKGROUND_SHARE", "0.65")
    monkeypatch.delenv("ROBINHOOD_PROVIDER_MONTHLY_USAGE_BASELINES_JSON", raising=False)
    capacity.reset_for_tests()
    yield
    capacity.reset_for_tests()


def test_chainstack_is_identified_and_default_capacity_is_six_million() -> None:
    assert (
        capacity._provider_kind_from_url("https://robinhood-mainnet.core.chainstack.com/redacted")
        == "chainstack"
    )
    assert capacity._provider_kind_from_url("https://example.g.alchemy.com/v2/redacted") == "alchemy"
    status = capacity.status()
    assert status["provider_monthly_request_limits"] == {
        "chainstack": 3_000_000,
        "alchemy": 3_000_000,
    }
    assert status["combined_monthly_request_limit"] == 6_000_000
    assert status["chainstack_rolling_rps_limit"] == 20
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False


def test_current_month_baseline_and_request_counts_are_persisted(monkeypatch: pytest.MonkeyPatch) -> None:
    month = capacity._month_key()
    monkeypatch.setenv(
        "ROBINHOOD_PROVIDER_MONTHLY_USAGE_BASELINES_JSON",
        json.dumps({month: {"alchemy": 1234, "chainstack": 11}}),
    )
    capacity.reset_for_tests()

    capacity._reserve_monthly_request("chainstack", "http")
    capacity._reserve_monthly_request("alchemy", "ws_control")

    status = capacity.status()
    assert status["provider_month_to_date_requests"]["chainstack"] == 12
    assert status["provider_month_to_date_requests"]["alchemy"] == 1235
    assert status["combined_month_to_date_requests"] == 1247
    assert status["http_requests_since_accounting_start"]["chainstack"] == 1
    assert status["ws_control_requests_since_accounting_start"]["alchemy"] == 1

    persisted = json.loads(capacity._state_path().read_text(encoding="utf-8"))
    assert persisted["month"] == month
    assert persisted["providers"]["chainstack"] == 12
    assert persisted["providers"]["alchemy"] == 1235


def test_new_utc_month_does_not_carry_prior_month_usage() -> None:
    old = {
        "month": "2026-09",
        "providers": {"chainstack": 2_900_000, "alchemy": 2_500_000},
    }
    fresh = capacity._sanitize_state(old, "2026-10")
    assert fresh["month"] == "2026-10"
    assert fresh["providers"] == {"chainstack": 0, "alchemy": 0}


def test_critical_reserve_blocks_qualification_before_settlement(monkeypatch: pytest.MonkeyPatch) -> None:
    month = capacity._month_key()
    monkeypatch.setenv("ROBINHOOD_CHAINSTACK_MONTHLY_REQUEST_LIMIT", "10")
    monkeypatch.setenv("ROBINHOOD_ALCHEMY_MONTHLY_REQUEST_LIMIT", "10")
    monkeypatch.setenv("ROBINHOOD_PROVIDER_COMBINED_MONTHLY_REQUEST_LIMIT", "20")
    monkeypatch.setenv(
        "ROBINHOOD_PROVIDER_MONTHLY_USAGE_BASELINES_JSON",
        json.dumps({month: {"chainstack": 9, "alchemy": 0}}),
    )
    capacity.reset_for_tests()

    with pytest.raises(capacity.RobinhoodProviderMonthlyBudgetExceeded):
        capacity._reserve_monthly_request("chainstack", "http")

    token = capacity.set_priority("critical")
    try:
        capacity._reserve_monthly_request("chainstack", "http")
        with pytest.raises(capacity.RobinhoodProviderMonthlyBudgetExceeded):
            capacity._reserve_monthly_request("chainstack", "http")
    finally:
        capacity.reset_priority(token)

    assert capacity.status()["provider_month_to_date_requests"]["chainstack"] == 10


@pytest.mark.asyncio
async def test_chainstack_429_is_retried_once_without_immediate_failover(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ROBINHOOD_PROVIDER_429_BACKOFF_SECONDS", "0.01")
    rpc = SimpleNamespace(rpc_url="https://robinhood-mainnet.core.chainstack.com/redacted")
    calls = 0

    async def original(_rpc, _method, _params):
        nonlocal calls
        calls += 1
        if calls == 1:
            request = httpx.Request("POST", rpc.rpc_url)
            response = httpx.Response(429, headers={"Retry-After": "0.05"}, request=request)
            raise httpx.HTTPStatusError("rate limited", request=request, response=response)
        return "0x1"

    wrapped = capacity._guarded_rpc(original)
    result = await wrapped(rpc, "eth_call", [{"to": "0x0", "data": "0x"}, "latest"])

    assert result == "0x1"
    assert calls == 2
    status = capacity.status()
    assert status["provider_month_to_date_requests"]["chainstack"] == 2
    assert status["stats"]["chainstack_429s"] == 1
    assert status["stats"]["last_http_status"] == 429
    assert status["stats"]["last_http_error_provider_kind"] == "chainstack"
    assert status["stats"]["last_http_error_method"] == "eth_call"


def test_background_target_uses_remaining_month_capacity(monkeypatch: pytest.MonkeyPatch) -> None:
    month = capacity._month_key()
    monkeypatch.setenv(
        "ROBINHOOD_PROVIDER_MONTHLY_USAGE_BASELINES_JSON",
        json.dumps({month: {"chainstack": 2_000_000, "alchemy": 0}}),
    )
    capacity.reset_for_tests()
    with capacity._LOCK:
        target = capacity._background_target_rps_locked("chainstack")
    assert target > 0
    assert target < 20


def test_month_helpers_are_utc_calendar_month_based() -> None:
    now = datetime(2026, 9, 30, 23, 59, 59, tzinfo=timezone.utc)
    assert capacity._month_key(now) == "2026-09"
    assert 0 < capacity._seconds_remaining_in_month(now) <= 1.0
