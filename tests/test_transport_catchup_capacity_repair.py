from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi.direct_solana import WatchTarget
from solana_roi.poll_recoverability_lease import POLL_RECOVERABILITY_LEASE_SECONDS
from solana_roi.public_ws_shard_transport_repair import (
    DEFAULT_TARGETS_PER_SOCKET,
    _target_shards,
)
from solana_roi.robinhood_catchup_capacity_repair import (
    DEFAULT_CATCHUP_MAX_BLOCKS,
    _capacity_poll_once,
    _logs_with_resilient_range,
    _select_batch_limit,
    _status_with_catchup_capacity,
)
from solana_roi import robinhood_chain_runtime as robinhood_runtime


def _targets() -> tuple[WatchTarget, ...]:
    return tuple(
        WatchTarget(
            kind="scout" if index < 3 else "program",
            address=f"target-{index}",
            source_hint=None if index < 3 else f"SOURCE_{index}",
        )
        for index in range(10)
    )


def test_public_shards_preserve_full_scope_with_fewer_physical_connections() -> None:
    targets = _targets()
    a = _target_shards(targets, "publicnode", DEFAULT_TARGETS_PER_SOCKET)
    b = _target_shards(targets, "onfinality", DEFAULT_TARGETS_PER_SOCKET)

    # The strategy-critical repair deliberately packs the three scouts onto one
    # scout-only socket and the seven high-volume programs onto two program-only
    # sockets. This preserves the existing three physical sockets/provider while
    # removing program-firehose head-of-line pressure from scout continuity.
    assert len(a) == 3
    assert len(b) == 3
    assert sorted(len(shard) for shard in a) == [3, 3, 4]
    assert sorted(len(shard) for shard in b) == [3, 3, 4]
    assert {row.address for shard in a for row in shard} == {row.address for row in targets}
    assert {row.address for shard in b for row in shard} == {row.address for row in targets}
    assert len(a) < len(targets)
    assert len(b) < len(targets)
    assert all(len({row.kind for row in shard}) == 1 for shard in a)
    assert all(len({row.kind for row in shard}) == 1 for shard in b)
    assert sum(1 for shard in a if shard and shard[0].kind == "scout") == 1
    assert sum(1 for shard in b if shard and shard[0].kind == "scout") == 1
    # Provider-specific rotation still avoids making program socket failures
    # correlate over the exact same target ordering on both public providers.
    assert [[row.address for row in shard] for shard in a] != [
        [row.address for row in shard] for shard in b
    ]


def test_public_sharding_does_not_relax_continuity_lease() -> None:
    assert POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert DEFAULT_TARGETS_PER_SOCKET == 4


def test_robinhood_deep_backlog_uses_historical_only_capacity_batch() -> None:
    assert _select_batch_limit(1569) == DEFAULT_CATCHUP_MAX_BLOCKS
    assert _select_batch_limit(robinhood_runtime.MAX_BLOCKS_PER_POLL) == robinhood_runtime.MAX_BLOCKS_PER_POLL
    assert _select_batch_limit(robinhood_runtime.LIVE_LAG_BLOCKS) == robinhood_runtime.MAX_BLOCKS_PER_POLL


def test_robinhood_large_log_range_splits_without_skipping_blocks() -> None:
    class Rpc:
        def __init__(self) -> None:
            self.ranges: list[tuple[int, int]] = []

        async def get_logs(self, *, from_block, to_block, addresses, topics):
            self.ranges.append((from_block, to_block))
            if to_block - from_block + 1 > robinhood_runtime.MAX_BLOCKS_PER_POLL:
                raise RuntimeError("provider range limit")
            return [{"from": from_block, "to": to_block}]

    plane = SimpleNamespace(rpc=Rpc())
    rows = asyncio.run(
        _logs_with_resilient_range(
            plane,
            from_block=1,
            to_block=800,
            addresses=["0x" + "1" * 40],
            topics=None,
        )
    )
    covered = sorted((int(row["from"]), int(row["to"])) for row in rows)
    assert covered == [(1, 200), (201, 400), (401, 600), (601, 800)]


def test_robinhood_poll_advances_800_blocks_but_remains_fail_closed_for_entries() -> None:
    class Rpc:
        async def chain_id(self):
            return robinhood_runtime.ROBINHOOD_CHAIN_ID

        async def block_number(self):
            return 2600

        async def get_logs(self, *, from_block, to_block, addresses, topics=None):
            return []

    class Plane:
        def __init__(self) -> None:
            self.rpc = Rpc()
            self._cursor = 1000
            self._latest_block = None
            self._caught_up = False
            self._last_error = None
            self._last_success_at = None
            self._last_poll_at = None
            self.v3_pools = {}
            self.v2_curves = {}
            self.settled = False

        def _set_cursor(self, value: int) -> None:
            self._cursor = value

        async def _process_factory_log(self, _log):
            raise AssertionError("no factory logs expected")

        async def _process_v3_swap(self, *_args, **_kwargs):
            raise AssertionError("no v3 swaps expected")

        async def _process_v2_curve_log(self, *_args, **_kwargs):
            raise AssertionError("no v2 swaps expected")

        async def _settle_open_positions(self):
            self.settled = True

    plane = Plane()
    asyncio.run(_capacity_poll_once(plane))

    assert plane._cursor == 1800
    assert plane._roi_last_batch_blocks == 800
    assert plane._caught_up is False
    assert plane.settled is False
    assert plane._roi_catchup_mode is True


def test_robinhood_status_reports_lag_and_never_equates_runtime_with_paper_readiness() -> None:
    class Plane:
        _cursor = 1000
        _latest_block = 2569
        _caught_up = False
        _roi_catchup_mode = True
        _roi_last_batch_size_limit = 800
        _roi_last_batch_blocks = 800
        _roi_last_batch_seconds = 1.0
        _roi_blocks_scanned_total = 800
        _roi_catchup_started_at = "2026-09-04T20:00:00+00:00"

    wrapped = _status_with_catchup_capacity(lambda _self: {"runtime_ready": True})
    payload = wrapped(Plane())
    assert payload["runtime_ready"] is True
    assert payload["block_lag"] == 1569
    assert payload["paper_decision_transport_ready"] is False
    assert payload["catchup_capacity"]["estimated_batches_to_live"] == 2
    assert payload["catchup_capacity"]["paper_entries_allowed_during_catchup"] is False
    assert payload["catchup_capacity"]["strategy_thresholds_changed"] is False
