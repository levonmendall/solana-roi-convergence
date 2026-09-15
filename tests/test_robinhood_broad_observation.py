from __future__ import annotations

import asyncio
import inspect
from types import SimpleNamespace

from solana_roi import robinhood_broad_observation as broad
from solana_roi import robinhood_chain_core as core


TOKEN = "0x9999999999999999999999999999999999999999"
PAIR = "0x7777777777777777777777777777777777777777"
ACTOR = "0x6666666666666666666666666666666666666666"
POOL_ID = "0x" + "ab" * 32


def _topic_address(address: str) -> str:
    return "0x" + ("0" * 24) + address.removeprefix("0x")


def _word(value: int) -> str:
    if value < 0:
        value = (1 << 256) + value
    return "0x" + int(value).to_bytes(32, "big", signed=False).hex()


def _word_address(address: str) -> str:
    return _topic_address(address)


def _log(
    *,
    address: str,
    topics: list[str],
    words: list[str],
    block: int,
    log_index: int,
    tx: str,
) -> dict[str, object]:
    return {
        "address": address,
        "topics": topics,
        "data": "0x" + "".join(item.removeprefix("0x") for item in words),
        "blockNumber": hex(block),
        "transactionIndex": "0x0",
        "logIndex": hex(log_index),
        "transactionHash": tx,
    }


class FakeRpc:
    async def token_decimals(self, _token: str) -> int:
        return 18


class FakePlane:
    def __init__(self) -> None:
        self.rpc = FakeRpc()
        self.launches: list[dict[str, object]] = []
        self.swaps: list[dict[str, object]] = []

    def _persist_launch(self, **payload):
        self.launches.append(payload)

    def _record_swap(self, **payload):
        self.swaps.append(payload)
        return True

    async def _maybe_open_v3(self, *_args, **_kwargs):
        raise AssertionError("broad observation must not call V3 paper authority")

    async def _maybe_open_v2(self, *_args, **_kwargs):
        raise AssertionError("broad observation must not call Pons V2 paper authority")


def test_observer_is_default_off_and_canonical_sync_still_runs(monkeypatch) -> None:
    monkeypatch.delenv("ROBINHOOD_BROAD_OBSERVATION_ENABLED", raising=False)
    calls: list[tuple[int, int]] = []

    async def canonical(_self, *, from_block: int, to_block: int):
        calls.append((from_block, to_block))
        return 11

    async def forbidden(*_args, **_kwargs):
        raise AssertionError("disabled broad observation performed RPC work")

    monkeypatch.setattr(broad, "_ORIGINAL_SYNC", canonical)
    monkeypatch.setattr(broad, "_observe", forbidden)
    result = asyncio.run(
        broad._sync(SimpleNamespace(), from_block=10, to_block=12)
    )

    assert result == 11
    assert calls == [(10, 12)]


def test_v2_and_v4_observations_persist_without_paper_authority(monkeypatch) -> None:
    plane = FakePlane()
    v2_pair = _log(
        address=broad.UNISWAP_V2_FACTORY,
        topics=[
            broad.V2_PAIR_CREATED_TOPIC,
            _topic_address(core.WETH),
            _topic_address(TOKEN),
        ],
        words=[_word_address(PAIR), _word(1)],
        block=101,
        log_index=0,
        tx="0x" + "11" * 32,
    )
    v2_swap = _log(
        address=PAIR,
        topics=[
            broad.V2_SWAP_TOPIC,
            _topic_address(ACTOR),
            _topic_address(ACTOR),
        ],
        words=[_word(10**18), _word(0), _word(0), _word(1000 * 10**18)],
        block=101,
        log_index=1,
        tx="0x" + "12" * 32,
    )
    v4_initialize = _log(
        address=core.UNISWAP_V4_POOL_MANAGER,
        topics=[
            broad.V4_INITIALIZE_TOPIC,
            POOL_ID,
            _topic_address(core.WETH),
            _topic_address(TOKEN),
        ],
        words=[_word(3000), _word(60), _word(0), _word(1 << 96), _word(0)],
        block=101,
        log_index=2,
        tx="0x" + "13" * 32,
    )
    v4_swap = _log(
        address=core.UNISWAP_V4_POOL_MANAGER,
        topics=[
            core.V4_SWAP_TOPIC,
            POOL_ID,
            _topic_address(ACTOR),
        ],
        words=[
            _word(2 * 10**18),
            _word(-2000 * 10**18),
            _word(1 << 96),
            _word(1),
            _word(0),
            _word(3000),
        ],
        block=101,
        log_index=3,
        tx="0x" + "14" * 32,
    )

    async def logs(
        _self,
        *,
        from_block: int,
        to_block: int,
        addresses: list[str],
        topics,
    ):
        assert (from_block, to_block) == (101, 101)
        if addresses == [broad.UNISWAP_V2_FACTORY]:
            return [v2_pair]
        if addresses == [core.UNISWAP_V4_POOL_MANAGER] and topics == [broad.V4_INITIALIZE_TOPIC]:
            return [v4_initialize]
        if addresses == [PAIR]:
            return [v2_swap]
        if (
            addresses == [core.UNISWAP_V4_POOL_MANAGER]
            and topics
            and topics[0] == core.V4_SWAP_TOPIC
        ):
            assert POOL_ID in topics[1]
            return [v4_swap]
        return []

    monkeypatch.setattr(broad, "_logs_with_resilient_range", logs)
    asyncio.run(broad._observe(plane, from_block=101, to_block=101, include_swaps=True))

    assert {row["protocol"] for row in plane.launches} == {"uniswap_v2", "uniswap_v4"}
    assert all(row["paper_eligible"] is False for row in plane.launches)
    assert {row["venue"] for row in plane.swaps} == {
        "UNISWAP_V2_OBSERVED",
        "UNISWAP_V4_OBSERVED",
    }
    assert all(row["side"] == "buy" for row in plane.swaps)
    assert plane._roi_broad_v2_swaps == 1
    assert plane._roi_broad_v4_swaps == 1


def test_metadata_recovery_discovers_pools_but_never_backfills_swaps(monkeypatch) -> None:
    plane = FakePlane()
    calls: list[tuple[tuple[str, ...], object]] = []
    pair_log = _log(
        address=broad.UNISWAP_V2_FACTORY,
        topics=[
            broad.V2_PAIR_CREATED_TOPIC,
            _topic_address(core.WETH),
            _topic_address(TOKEN),
        ],
        words=[_word_address(PAIR), _word(1)],
        block=90,
        log_index=0,
        tx="0x" + "21" * 32,
    )

    async def logs(
        _self,
        *,
        from_block: int,
        to_block: int,
        addresses: list[str],
        topics,
    ):
        calls.append((tuple(addresses), topics))
        if addresses == [broad.UNISWAP_V2_FACTORY]:
            return [pair_log]
        return []

    monkeypatch.setattr(broad, "_logs_with_resilient_range", logs)
    asyncio.run(broad._observe(plane, from_block=80, to_block=100, include_swaps=False))

    assert len(plane.launches) == 1
    assert plane.swaps == []
    assert all(tuple(addresses) in {
        (broad.UNISWAP_V2_FACTORY,),
        (core.UNISWAP_V4_POOL_MANAGER,),
    } for addresses, _topics in calls)
    assert plane._roi_broad_last_range["metadata_recovery_only"] is True


def test_broad_observer_failure_cannot_block_canonical_frontier(monkeypatch) -> None:
    monkeypatch.setenv("ROBINHOOD_BROAD_OBSERVATION_ENABLED", "true")

    async def canonical(_self, *, from_block: int, to_block: int):
        return 7

    async def broken(*_args, **_kwargs):
        raise RuntimeError("provider unavailable")

    monkeypatch.setattr(broad, "_ORIGINAL_SYNC", canonical)
    monkeypatch.setattr(broad, "_observe", broken)
    monkeypatch.setattr(broad, "_prospective_live_range", lambda *_args, **_kwargs: True)

    plane = SimpleNamespace()
    result = asyncio.run(broad._sync(plane, from_block=201, to_block=202))

    assert result == 7
    assert plane._roi_broad_failures == 1
    assert "provider unavailable" in plane._roi_broad_last_error


def test_live_range_guard_excludes_startup_and_long_gap_recovery(monkeypatch) -> None:
    plane = SimpleNamespace(_latest_block=105)
    monkeypatch.setattr(broad.frontier, "_live_epoch_active", lambda _self: True)
    monkeypatch.setattr(broad.frontier, "_live_cursor", lambda _self: 103)
    monkeypatch.setattr(broad.frontier, "MAX_LIVE_FRONTIER_GAP_BLOCKS", 8)

    assert broad._prospective_live_range(plane, from_block=104, to_block=105) is True

    plane._latest_block = 120
    assert broad._prospective_live_range(plane, from_block=104, to_block=120) is False

    monkeypatch.setattr(broad.frontier, "_live_epoch_active", lambda _self: False)
    assert broad._prospective_live_range(plane, from_block=104, to_block=120) is False


def test_status_contract_is_explicitly_observation_only(monkeypatch) -> None:
    monkeypatch.delenv("ROBINHOOD_BROAD_OBSERVATION_ENABLED", raising=False)
    wrapped = broad._status(lambda _self: {"paper_only": True})
    payload = wrapped(SimpleNamespace())

    status = payload["broad_market_observation"]
    assert status["installed"] is True
    assert status["enabled"] is False
    assert status["activation_hold"] == "storage_migration_certification"
    assert status["strategy_authority"] is False
    assert status["paper_entry_authority"] is False
    assert status["paper_exit_authority"] is False
    assert status["paper_eligible"] is False
    assert status["schema_changes"] is False
    assert status["storage_migration_touched"] is False
    assert status["canonical_storage_authority_changed"] is False
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False


def test_observer_source_contains_no_storage_schema_or_migration_mutation() -> None:
    source = inspect.getsource(broad)
    forbidden = (
        "CREATE TABLE",
        "ALTER TABLE",
        "DROP TABLE",
        "certification_release_epochs",
        "verified_genesis",
        "DELETE FROM",
    )
    assert all(token not in source for token in forbidden)


def test_production_plane_installs_observer_after_existing_policy_composition() -> None:
    from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane

    assert RobinhoodChainPaperPlane._roi_robinhood_broad_observation_installed is True
    assert getattr(
        RobinhoodChainPaperPlane.status,
        "_roi_robinhood_broad_observation_status",
        False,
    ) is True
