from __future__ import annotations

import asyncio

import httpx

from solana_roi import robinhood_chain_core as core
from solana_roi.robinhood_rpc_rate_limit_repair import (
    REPAIR_VERSION,
    install_robinhood_rpc_rate_limit_repair,
)


class _Plane:
    def __init__(self, rpc):
        self.rpc = rpc

    def status(self):
        return {"paper_only": True, "live_money_authority": False}


def test_429_retries_exact_same_rpc_request_and_exposes_recovery_telemetry() -> None:
    calls: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.content.decode("utf-8")
        calls.append({"body": body})
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "0.01"}, request=request)
        return httpx.Response(
            200,
            json={"jsonrpc": "2.0", "id": 2, "result": "0x123"},
            request=request,
        )

    rpc = core.RobinhoodRpc("https://rpc.example.test")
    asyncio.run(rpc.client.aclose())
    rpc.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    install_robinhood_rpc_rate_limit_repair(_Plane)

    try:
        result = asyncio.run(rpc.rpc("eth_blockNumber", []))
        assert result == "0x123"
        assert len(calls) == 2
        assert '"method":"eth_blockNumber"' in calls[0]["body"].replace(" ", "")
        assert '"method":"eth_blockNumber"' in calls[1]["body"].replace(" ", "")
        status = _Plane(rpc).status()["rpc_rate_limit_recovery"]
        assert status["repair_version"] == REPAIR_VERSION
        assert status["rate_limit_events_session"] == 1
        assert status["retry_attempts_session"] == 1
        assert status["failed_ranges_skipped"] is False
        assert status["catchup_batch_limit_changed"] is False
        assert status["catchup_query_concurrency_changed"] is False
        assert status["paper_only"] is True
        assert status["live_money_authority"] is False
    finally:
        asyncio.run(rpc.client.aclose())


def test_non_rate_limit_http_error_still_fails_closed_without_retry() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500, request=request)

    rpc = core.RobinhoodRpc("https://rpc.example.test")
    asyncio.run(rpc.client.aclose())
    rpc.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    install_robinhood_rpc_rate_limit_repair(_Plane)
    try:
        try:
            asyncio.run(rpc.rpc("eth_blockNumber", []))
        except httpx.HTTPStatusError:
            pass
        else:
            raise AssertionError("500 must remain fail closed")
        assert calls == 1
    finally:
        asyncio.run(rpc.client.aclose())
