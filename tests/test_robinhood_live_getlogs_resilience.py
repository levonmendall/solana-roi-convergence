from __future__ import annotations

from dataclasses import dataclass

import pytest

from solana_roi import robinhood_catchup_capacity_repair as catchup
from solana_roi import robinhood_live_frontier_verification_repair as frontier
from solana_roi.robinhood_live_getlogs_resilience import (
    _resilient_live_logs,
    install_robinhood_live_getlogs_resilience,
    status,
)


@dataclass
class _RangeLimitedRPC:
    max_blocks: int = 32
    calls: list[tuple[int, int, tuple[str, ...]]] | None = None

    def __post_init__(self) -> None:
        self.calls = []

    async def get_logs(self, *, from_block: int, to_block: int, addresses: list[str], topics):
        assert self.calls is not None
        self.calls.append((from_block, to_block, tuple(addresses)))
        if to_block - from_block + 1 > self.max_blocks:
            raise TimeoutError("provider read timeout on wide live range")
        return [
            {"blockNumber": hex(block), "address": addresses[0], "logIndex": "0x0", "transactionIndex": "0x0"}
            for block in range(from_block, to_block + 1)
        ]


@dataclass
class _AddressLimitedRPC:
    max_addresses: int = 2
    calls: list[tuple[int, int, tuple[str, ...]]] | None = None

    def __post_init__(self) -> None:
        self.calls = []

    async def get_logs(self, *, from_block: int, to_block: int, addresses: list[str], topics):
        assert self.calls is not None
        self.calls.append((from_block, to_block, tuple(addresses)))
        if len(addresses) > self.max_addresses:
            raise TimeoutError("provider read timeout on compound address filter")
        return [{"blockNumber": hex(from_block), "address": address} for address in addresses]


class _Plane:
    def __init__(self, rpc) -> None:
        self.rpc = rpc


@pytest.mark.asyncio
async def test_failed_64_block_live_range_is_split_without_skipping_blocks() -> None:
    rpc = _RangeLimitedRPC(max_blocks=32)
    rows = await _resilient_live_logs(
        _Plane(rpc),
        from_block=100,
        to_block=163,
        addresses=["0xmarket"],
        topics=["0xtopic"],
    )
    blocks = [int(str(row["blockNumber"]), 16) for row in rows]
    assert blocks == list(range(100, 164))
    assert len(set(blocks)) == 64
    # The original 64-block request was actually attempted/retried, proving the
    # regression covers the production failure boundary rather than pre-splitting.
    assert rpc.calls is not None
    assert sum(1 for start, end, _ in rpc.calls if (start, end) == (100, 163)) == 3
    assert (100, 131, ("0xmarket",)) in rpc.calls
    assert (132, 163, ("0xmarket",)) in rpc.calls


@pytest.mark.asyncio
async def test_single_block_failure_falls_back_to_disjoint_address_splits() -> None:
    rpc = _AddressLimitedRPC(max_addresses=2)
    addresses = ["0xa", "0xb", "0xc", "0xd"]
    rows = await _resilient_live_logs(
        _Plane(rpc),
        from_block=200,
        to_block=200,
        addresses=addresses,
        topics=None,
    )
    assert sorted(row["address"] for row in rows) == sorted(addresses)
    assert len(rows) == 4
    assert rpc.calls is not None
    assert sum(1 for start, end, got in rpc.calls if start == end == 200 and got == tuple(addresses)) == 3


def test_installer_patches_both_dynamic_log_read_references() -> None:
    install_robinhood_live_getlogs_resilience()
    assert catchup._logs_with_resilient_range is _resilient_live_logs
    assert frontier._logs_with_resilient_range is _resilient_live_logs
    proof = status()
    assert proof["installed"] is True
    assert proof["skips_failed_blocks"] is False
    assert proof["changes_strategy_thresholds"] is False
    assert proof["paper_only"] is True
    assert proof["live_money_authority"] is False
