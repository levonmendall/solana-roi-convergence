from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from solana_roi import robinhood_getlogs_provider_guard as guard


@dataclass
class _Rpc:
    rpc_url: str = "https://robinhood-mainnet.core.chainstack.com/example"
    calls: list[tuple[int, int]] = field(default_factory=list)


def _run(rpc: _Rpc, start: int, end: int) -> list[dict[str, Any]]:
    return asyncio.run(
        guard._provider_bounded_get_logs(
            rpc,
            from_block=start,
            to_block=end,
            addresses=["0xmarket"],
            topics=["0xtopic"],
        )
    )


def test_validation_cloud_is_preferred_for_getlogs_without_moving_primary_provider(
    monkeypatch,
) -> None:
    endpoint = "https://mainnet.robinhood.validationcloud.example/key"
    monkeypatch.setenv(guard.ENV_VALIDATION_CLOUD_RPC_URL, endpoint)
    monkeypatch.delenv(guard.ENV_VALIDATION_CLOUD_MAX_BLOCKS, raising=False)
    monkeypatch.delenv(guard.ENV_MAX_BLOCKS, raising=False)
    monkeypatch.setattr(guard, "_VALIDATION_CLOUD_VERIFIED_ENDPOINT", None)

    rpc = _Rpc()
    methods: list[tuple[str, list[Any]]] = []

    async def canonical_should_not_run(
        self: _Rpc,
        *,
        from_block: int,
        to_block: int,
        addresses=None,
        topics=None,
    ) -> list[dict[str, Any]]:
        raise AssertionError("canonical provider should not run while Validation Cloud is healthy")

    async def validation_rpc(self: _Rpc, method: str, params: list[Any]) -> Any:
        methods.append((method, params))
        if method == "eth_chainId":
            return hex(4663)
        if method == "eth_blockNumber":
            return hex(63_000_000)
        if method == "eth_getLogs":
            query = params[0]
            assert query["address"] == "0xmarket"
            assert query["topics"] == ["0xtopic"]
            start = int(query["fromBlock"], 16)
            end = int(query["toBlock"], 16)
            return [{"blockNumber": hex(block)} for block in range(start, end + 1)]
        raise AssertionError(method)

    monkeypatch.setattr(guard, "_ORIGINAL_GET_LOGS", canonical_should_not_run)
    monkeypatch.setattr(guard, "_validation_cloud_rpc", validation_rpc)

    first = _run(rpc, 100, 104)
    second = _run(rpc, 105, 109)

    assert [int(row["blockNumber"], 16) for row in first + second] == list(range(100, 110))
    assert rpc.rpc_url == "https://robinhood-mainnet.core.chainstack.com/example"
    assert [method for method, _ in methods] == [
        "eth_chainId",
        "eth_blockNumber",
        "eth_getLogs",
        "eth_getLogs",
    ]
    assert rpc._roi_getlogs_guard_validation_cloud_requests == 2
    assert rpc._roi_getlogs_guard_validation_cloud_successes == 2


def test_validation_cloud_failure_falls_back_to_exact_canonical_range(monkeypatch) -> None:
    monkeypatch.setenv(
        guard.ENV_VALIDATION_CLOUD_RPC_URL,
        "https://mainnet.robinhood.validationcloud.example/key",
    )
    monkeypatch.delenv(guard.ENV_VALIDATION_CLOUD_MAX_BLOCKS, raising=False)
    monkeypatch.delenv(guard.ENV_MAX_BLOCKS, raising=False)
    monkeypatch.setattr(guard, "_VALIDATION_CLOUD_VERIFIED_ENDPOINT", None)

    rpc = _Rpc()

    async def validation_failure(self: _Rpc, method: str, params: list[Any]) -> Any:
        raise RuntimeError("simulated Validation Cloud outage")

    async def canonical(
        self: _Rpc,
        *,
        from_block: int,
        to_block: int,
        addresses=None,
        topics=None,
    ) -> list[dict[str, Any]]:
        self.calls.append((from_block, to_block))
        assert addresses == ["0xmarket"]
        assert topics == ["0xtopic"]
        return [{"blockNumber": hex(block)} for block in range(from_block, to_block + 1)]

    monkeypatch.setattr(guard, "_validation_cloud_rpc", validation_failure)
    monkeypatch.setattr(guard, "_ORIGINAL_GET_LOGS", canonical)

    rows = _run(rpc, 200, 204)

    assert rpc.calls == [(200, 204)]
    assert [int(row["blockNumber"], 16) for row in rows] == list(range(200, 205))
    assert rpc._roi_getlogs_guard_validation_cloud_requests == 1
    assert rpc._roi_getlogs_guard_validation_cloud_failures == 1


def test_wrong_validation_cloud_chain_never_admits_wrong_chain_logs(monkeypatch) -> None:
    monkeypatch.setenv(
        guard.ENV_VALIDATION_CLOUD_RPC_URL,
        "https://mainnet.robinhood.validationcloud.example/key",
    )
    monkeypatch.delenv(guard.ENV_VALIDATION_CLOUD_MAX_BLOCKS, raising=False)
    monkeypatch.delenv(guard.ENV_MAX_BLOCKS, raising=False)
    monkeypatch.setattr(guard, "_VALIDATION_CLOUD_VERIFIED_ENDPOINT", None)

    rpc = _Rpc()
    validation_methods: list[str] = []

    async def wrong_chain(self: _Rpc, method: str, params: list[Any]) -> Any:
        validation_methods.append(method)
        if method == "eth_chainId":
            return hex(1)
        if method == "eth_getLogs":
            raise AssertionError("wrong-chain endpoint must never be queried for logs")
        raise AssertionError(method)

    async def canonical(
        self: _Rpc,
        *,
        from_block: int,
        to_block: int,
        addresses=None,
        topics=None,
    ) -> list[dict[str, Any]]:
        self.calls.append((from_block, to_block))
        return [{"blockNumber": hex(block)} for block in range(from_block, to_block + 1)]

    monkeypatch.setattr(guard, "_validation_cloud_rpc", wrong_chain)
    monkeypatch.setattr(guard, "_ORIGINAL_GET_LOGS", canonical)

    rows = _run(rpc, 300, 302)

    assert validation_methods == ["eth_chainId"]
    assert rpc.calls == [(300, 302)]
    assert len(rows) == 3
    assert rpc._roi_getlogs_guard_validation_cloud_failures == 1


def test_validation_cloud_specific_block_limit_chunks_without_changing_filters(
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        guard.ENV_VALIDATION_CLOUD_RPC_URL,
        "https://mainnet.robinhood.validationcloud.example/key",
    )
    monkeypatch.setenv(guard.ENV_VALIDATION_CLOUD_MAX_BLOCKS, "5")
    monkeypatch.delenv(guard.ENV_MAX_BLOCKS, raising=False)
    monkeypatch.setattr(guard, "_VALIDATION_CLOUD_VERIFIED_ENDPOINT", None)

    rpc = _Rpc()
    getlogs_queries: list[dict[str, Any]] = []

    async def canonical_should_not_run(
        self: _Rpc,
        *,
        from_block: int,
        to_block: int,
        addresses=None,
        topics=None,
    ) -> list[dict[str, Any]]:
        raise AssertionError("canonical provider should not run")

    async def validation_rpc(self: _Rpc, method: str, params: list[Any]) -> Any:
        if method == "eth_chainId":
            return hex(4663)
        if method == "eth_blockNumber":
            return hex(63_000_000)
        if method == "eth_getLogs":
            query = dict(params[0])
            getlogs_queries.append(query)
            start = int(query["fromBlock"], 16)
            end = int(query["toBlock"], 16)
            return [{"blockNumber": hex(block)} for block in range(start, end + 1)]
        raise AssertionError(method)

    monkeypatch.setattr(guard, "_ORIGINAL_GET_LOGS", canonical_should_not_run)
    monkeypatch.setattr(guard, "_validation_cloud_rpc", validation_rpc)

    rows = _run(rpc, 400, 411)

    assert [(int(q["fromBlock"], 16), int(q["toBlock"], 16)) for q in getlogs_queries] == [
        (400, 404),
        (405, 409),
        (410, 411),
    ]
    assert all(q["address"] == "0xmarket" for q in getlogs_queries)
    assert all(q["topics"] == ["0xtopic"] for q in getlogs_queries)
    assert len(rows) == 12
    assert rpc._roi_getlogs_guard_ranges_chunked == 1
    assert rpc._roi_getlogs_guard_max_sent_blocks == 5


def test_validation_cloud_status_preserves_paper_only_authority(monkeypatch) -> None:
    monkeypatch.setenv(
        guard.ENV_VALIDATION_CLOUD_RPC_URL,
        "https://mainnet.robinhood.validationcloud.example/key",
    )
    proof = guard.status()

    assert proof["validation_cloud_rpc_env"] == "ROBINHOOD_VALIDATION_CLOUD_RPC_URL"
    assert proof["validation_cloud_max_blocks_env"] == "ROBINHOOD_VALIDATION_CLOUD_MAX_BLOCKS"
    assert proof["validation_cloud_configured"] is True
    assert proof["validation_cloud_getlogs_only"] is True
    assert proof["validation_cloud_preserves_primary_ws"] is True
    assert proof["changes_strategy_thresholds"] is False
    assert proof["paper_only"] is True
    assert proof["live_money_authority"] is False
    assert proof["signing_available"] is False
    assert proof["transaction_submission_available"] is False
