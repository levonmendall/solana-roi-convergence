from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest

from solana_roi import robinhood_getlogs_provider_guard as guard


@dataclass
class _Rpc:
    rpc_url: str
    calls: list[tuple[int, int]] = field(default_factory=list)


async def _original_get_logs(
    self: _Rpc,
    *,
    from_block: int,
    to_block: int,
    addresses: list[str] | tuple[str, ...] | None = None,
    topics: list[Any] | None = None,
) -> list[dict[str, Any]]:
    self.calls.append((from_block, to_block))
    if guard._is_alchemy_endpoint(self.rpc_url):
        assert to_block - from_block + 1 <= guard.ALCHEMY_SAFE_MAX_BLOCKS
    return [{"blockNumber": hex(block)} for block in range(from_block, to_block + 1)]


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


def _http_403(url: str) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", url)
    response = httpx.Response(403, request=request)
    return httpx.HTTPStatusError("forbidden", request=request, response=response)


def test_alchemy_25_block_range_is_partitioned_before_provider_call(monkeypatch) -> None:
    monkeypatch.setattr(guard, "_ORIGINAL_GET_LOGS", _original_get_logs)
    monkeypatch.delenv(guard.ENV_MAX_BLOCKS, raising=False)
    rpc = _Rpc("https://robinhood-mainnet.g.alchemy.com/v2/example")

    rows = _run(rpc, 100, 124)

    assert rpc.calls == [(100, 109), (110, 119), (120, 124)]
    assert [int(row["blockNumber"], 16) for row in rows] == list(range(100, 125))
    assert rpc._roi_getlogs_guard_max_requested_blocks == 25
    assert rpc._roi_getlogs_guard_max_sent_blocks == 10
    assert rpc._roi_getlogs_guard_ranges_chunked == 1


def test_alchemy_exact_ten_block_range_remains_one_request(monkeypatch) -> None:
    monkeypatch.setattr(guard, "_ORIGINAL_GET_LOGS", _original_get_logs)
    monkeypatch.delenv(guard.ENV_MAX_BLOCKS, raising=False)
    rpc = _Rpc("https://robinhood-mainnet.g.alchemy.com/v2/example")

    rows = _run(rpc, 200, 209)

    assert rpc.calls == [(200, 209)]
    assert len(rows) == 10
    assert rpc._roi_getlogs_guard_max_sent_blocks == 10


def test_public_robinhood_rpc_is_not_silently_given_alchemy_limit(monkeypatch) -> None:
    monkeypatch.setattr(guard, "_ORIGINAL_GET_LOGS", _original_get_logs)
    monkeypatch.delenv(guard.ENV_MAX_BLOCKS, raising=False)
    rpc = _Rpc("https://rpc.mainnet.chain.robinhood.com")

    rows = _run(rpc, 300, 324)

    assert rpc.calls == [(300, 324)]
    assert len(rows) == 25


def test_explicit_provider_limit_overrides_endpoint_default(monkeypatch) -> None:
    monkeypatch.setattr(guard, "_ORIGINAL_GET_LOGS", _original_get_logs)
    monkeypatch.setenv(guard.ENV_MAX_BLOCKS, "7")
    rpc = _Rpc("https://robinhood-mainnet.g.alchemy.com/v2/example")

    rows = _run(rpc, 400, 416)

    assert rpc.calls == [(400, 406), (407, 413), (414, 416)]
    assert len(rows) == 17
    assert rpc._roi_getlogs_guard_max_sent_blocks == 7


def test_chainstack_403_retries_same_range_on_alchemy_and_reapplies_limit(monkeypatch) -> None:
    monkeypatch.delenv(guard.ENV_MAX_BLOCKS, raising=False)
    rpc = _Rpc("https://robinhood-mainnet.core.chainstack.com/example")
    calls: list[tuple[str, int, int, tuple[str, ...], tuple[Any, ...]]] = []

    async def original(
        self: _Rpc,
        *,
        from_block: int,
        to_block: int,
        addresses: list[str] | tuple[str, ...] | None = None,
        topics: list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        calls.append(
            (
                self.rpc_url,
                from_block,
                to_block,
                tuple(addresses or ()),
                tuple(topics or ()),
            )
        )
        if len(calls) == 1:
            raise _http_403(self.rpc_url)
        assert guard._is_alchemy_endpoint(self.rpc_url)
        assert to_block - from_block + 1 <= guard.ALCHEMY_SAFE_MAX_BLOCKS
        return [{"blockNumber": hex(block)} for block in range(from_block, to_block + 1)]

    def failover(self: _Rpc) -> bool:
        self.rpc_url = "https://robinhood-mainnet.g.alchemy.com/v2/example"
        return True

    monkeypatch.setattr(guard, "_ORIGINAL_GET_LOGS", original)
    monkeypatch.setattr(guard, "_failover_from_getlogs_403", failover)

    rows = _run(rpc, 500, 524)

    assert [(start, end) for _, start, end, _, _ in calls] == [
        (500, 524),
        (500, 509),
        (510, 519),
        (520, 524),
    ]
    assert all(addresses == ("0xmarket",) for _, _, _, addresses, _ in calls)
    assert all(topics == ("0xtopic",) for _, _, _, _, topics in calls)
    assert [int(row["blockNumber"], 16) for row in rows] == list(range(500, 525))
    assert rpc._roi_getlogs_guard_http_403s == 1
    assert rpc._roi_getlogs_guard_http_403_failovers == 1
    assert rpc._roi_getlogs_guard_max_sent_blocks == 25


def test_403_with_no_healthy_peer_fails_closed(monkeypatch) -> None:
    rpc = _Rpc("https://robinhood-mainnet.core.chainstack.com/example")

    async def forbidden(
        self: _Rpc,
        *,
        from_block: int,
        to_block: int,
        addresses: list[str] | tuple[str, ...] | None = None,
        topics: list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append((from_block, to_block))
        raise _http_403(self.rpc_url)

    monkeypatch.setattr(guard, "_ORIGINAL_GET_LOGS", forbidden)
    monkeypatch.setattr(guard, "_failover_from_getlogs_403", lambda _self: False)

    with pytest.raises(httpx.HTTPStatusError):
        _run(rpc, 600, 609)

    assert rpc.calls == [(600, 609)]
    assert rpc._roi_getlogs_guard_http_403s == 1
    assert rpc._roi_getlogs_guard_http_403_fail_closed == 1


def test_failed_middle_chunk_never_advances_to_later_chunk(monkeypatch) -> None:
    monkeypatch.setenv(guard.ENV_MAX_BLOCKS, "5")
    rpc = _Rpc("https://robinhood-mainnet.core.chainstack.com/example")

    async def fail_middle(
        self: _Rpc,
        *,
        from_block: int,
        to_block: int,
        addresses: list[str] | tuple[str, ...] | None = None,
        topics: list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append((from_block, to_block))
        if from_block == 705:
            raise _http_403(self.rpc_url)
        return [{"blockNumber": hex(block)} for block in range(from_block, to_block + 1)]

    monkeypatch.setattr(guard, "_ORIGINAL_GET_LOGS", fail_middle)
    monkeypatch.setattr(guard, "_failover_from_getlogs_403", lambda _self: False)

    with pytest.raises(httpx.HTTPStatusError):
        _run(rpc, 700, 714)

    assert rpc.calls == [(700, 704), (705, 709)]


def test_status_preserves_paper_only_authority() -> None:
    proof = guard.status()
    assert proof["alchemy_detected_max_blocks"] == 10
    assert proof["prevents_oversized_provider_requests"] is True
    assert proof["http_403_same_range_failover"] is True
    assert proof["replacement_provider_limits_reapplied"] is True
    assert proof["contiguous_frontier_fail_closed"] is True
    assert proof["changes_strategy_thresholds"] is False
    assert proof["paper_only"] is True
    assert proof["live_money_authority"] is False
    assert proof["signing_available"] is False
    assert proof["transaction_submission_available"] is False
