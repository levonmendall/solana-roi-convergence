from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import httpx

from solana_roi import robinhood_getlogs_provider_guard as guard


@dataclass
class _Rpc:
    rpc_url: str = "https://robinhood-mainnet.g.alchemy.com/v2/example"
    calls: list[tuple[int, int]] = field(default_factory=list)
    _request_id: int = 0
    client: Any = None


def test_validation_cloud_failure_reapplies_alchemy_limit_without_reentering_validation_cloud(
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        guard.ENV_VALIDATION_CLOUD_RPC_URL,
        "https://mainnet.robinhood.validationcloud.example/key",
    )
    monkeypatch.delenv(guard.ENV_MAX_BLOCKS, raising=False)
    monkeypatch.delenv(guard.ENV_VALIDATION_CLOUD_MAX_BLOCKS, raising=False)
    rpc = _Rpc()
    validation_calls = 0

    async def validation_failure(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        nonlocal validation_calls
        validation_calls += 1
        raise guard.ValidationCloudRequestError("ReadTimeout", retryable=True)

    async def fallback_range(
        self: _Rpc,
        *,
        from_block: int,
        to_block: int,
        addresses=None,
        topics=None,
    ) -> list[dict[str, Any]]:
        self.calls.append((from_block, to_block))
        return [{"blockNumber": hex(block)} for block in range(from_block, to_block + 1)]

    monkeypatch.setattr(guard, "_validation_cloud_get_logs", validation_failure)
    monkeypatch.setattr(guard, "_request_range", fallback_range)
    guard._reset_proof_state_for_tests()

    rows = asyncio.run(
        guard._dispatch_range(
            rpc,
            from_block=100,
            to_block=124,
            addresses=["0xmarket"],
            topics=["0xtopic"],
        )
    )

    assert validation_calls == 1
    assert rpc.calls == [(100, 109), (110, 119), (120, 124)]
    assert len(rows) == 25
    proof = guard.status()["validation_cloud_proof"]
    assert proof["ranges_attempted"] == 1
    assert proof["ranges_failed"] == 1
    assert proof["fallback_ranges"] == 1
    assert proof["last_failure_type"] == "ReadTimeout"


def test_validation_cloud_transport_timeout_gets_bounded_same_rpc_retry(monkeypatch) -> None:
    monkeypatch.setenv(
        guard.ENV_VALIDATION_CLOUD_RPC_URL,
        "https://mainnet.robinhood.validationcloud.example/key",
    )
    monkeypatch.delenv(guard.ENV_VALIDATION_CLOUD_TIMEOUT_SECONDS, raising=False)
    monkeypatch.delenv(guard.ENV_VALIDATION_CLOUD_RETRIES, raising=False)
    guard._reset_proof_state_for_tests()

    class _Response:
        status_code = 200

        def json(self) -> dict[str, Any]:
            return {"jsonrpc": "2.0", "id": 2, "result": hex(4663)}

    class _Client:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        async def post(self, url: str, **kwargs: Any) -> _Response:
            self.calls.append({"url": url, **kwargs})
            if len(self.calls) == 1:
                raise httpx.ReadTimeout("simulated timeout")
            return _Response()

    client = _Client()
    rpc = _Rpc(client=client)
    result = asyncio.run(guard._validation_cloud_rpc(rpc, "eth_chainId", []))

    assert result == hex(4663)
    assert len(client.calls) == 2
    assert all(call["timeout"] == guard.DEFAULT_VALIDATION_CLOUD_TIMEOUT_SECONDS for call in client.calls)
    assert client.calls[0]["json"]["method"] == "eth_chainId"
    assert client.calls[1]["json"]["method"] == "eth_chainId"
    assert guard.status()["validation_cloud_proof"]["rpc_retries"] == 1


def test_validation_cloud_timeout_can_split_exact_range_before_governed_fallback(
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        guard.ENV_VALIDATION_CLOUD_RPC_URL,
        "https://mainnet.robinhood.validationcloud.example/key",
    )
    monkeypatch.setattr(guard, "_VALIDATION_CLOUD_VERIFIED_ENDPOINT", "https://mainnet.robinhood.validationcloud.example/key")
    guard._reset_proof_state_for_tests()
    rpc = _Rpc()
    queried: list[tuple[int, int]] = []

    async def adaptive_rpc(self: _Rpc, method: str, params: list[Any]) -> Any:
        assert method == "eth_getLogs"
        query = params[0]
        start = int(query["fromBlock"], 16)
        end = int(query["toBlock"], 16)
        queried.append((start, end))
        if end - start + 1 > 4:
            raise guard.ValidationCloudRequestError(
                "ReadTimeout", retryable=True, split_worthy=True
            )
        return [{"blockNumber": hex(block)} for block in range(start, end + 1)]

    monkeypatch.setattr(guard, "_validation_cloud_rpc", adaptive_rpc)
    rows = asyncio.run(
        guard._validation_cloud_get_logs(
            rpc,
            from_block=200,
            to_block=207,
            addresses=["0xmarket"],
            topics=["0xtopic"],
        )
    )

    assert queried == [(200, 207), (200, 203), (204, 207)]
    assert [int(row["blockNumber"], 16) for row in rows] == list(range(200, 208))
    assert guard.status()["validation_cloud_proof"]["range_splits"] == 1


def test_status_exposes_production_proof_without_secret(monkeypatch) -> None:
    endpoint = "https://mainnet.robinhood.validationcloud.example/secret-key"
    monkeypatch.setenv(guard.ENV_VALIDATION_CLOUD_RPC_URL, endpoint)
    guard._reset_proof_state_for_tests()
    status = guard.status()

    assert status["validation_cloud_configured"] is True
    assert status["validation_cloud_timeout_seconds"] == guard.DEFAULT_VALIDATION_CLOUD_TIMEOUT_SECONDS
    assert status["validation_cloud_retries"] == guard.DEFAULT_VALIDATION_CLOUD_RETRIES
    assert status["validation_cloud_fallback_reentry_prevented"] is True
    assert status["fallback_provider_limits_reapplied"] is True
    assert "secret-key" not in repr(status)
