from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

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


def test_status_preserves_paper_only_authority() -> None:
    proof = guard.status()
    assert proof["alchemy_detected_max_blocks"] == 10
    assert proof["prevents_oversized_provider_requests"] is True
    assert proof["changes_strategy_thresholds"] is False
    assert proof["paper_only"] is True
    assert proof["live_money_authority"] is False
    assert proof["signing_available"] is False
    assert proof["transaction_submission_available"] is False
