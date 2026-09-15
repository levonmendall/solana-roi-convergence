from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi.observation_store import ObservationEventStore
from solana_roi import robinhood_v2_v4_observation as observation


def _topic_address(address: str) -> str:
    return "0x" + ("0" * 24) + address.lower().removeprefix("0x")


def _word_address(address: str) -> str:
    return _topic_address(address)


def _word_uint(value: int) -> str:
    if value < 0:
        value = (1 << 256) + value
    return "0x" + int(value).to_bytes(32, "big", signed=False).hex()


def _data(*words: str) -> str:
    return "0x" + "".join(word.removeprefix("0x") for word in words)


def test_uniswap_v2_pair_created_decodes_as_observation_only() -> None:
    token = "0x1111111111111111111111111111111111111111"
    pair = "0x2222222222222222222222222222222222222222"
    log = {
        "address": observation.UNISWAP_V2_FACTORY,
        "topics": [
            observation.UNISWAP_V2_PAIR_CREATED_TOPIC,
            _topic_address(observation.runtime.WETH),
            _topic_address(token),
        ],
        "data": _data(_word_address(pair), _word_uint(1)),
        "blockNumber": hex(100),
        "logIndex": hex(3),
        "transactionHash": "0xabc",
    }
    decoded = observation._decode_v2_pair(log)
    assert decoded is not None
    assert decoded["venue"] == "UNISWAP_V2"
    assert decoded["token"] == token
    assert decoded["pair"] == pair
    assert decoded["paper_eligible"] is False
    assert decoded["execution_authorized"] is False


def test_uniswap_v4_initialize_uses_pool_id_then_currency_topics() -> None:
    token = "0x3333333333333333333333333333333333333333"
    hooks = "0x4444444444444444444444444444444444444444"
    pool_id = "0x" + ("55" * 32)
    log = {
        "address": observation.runtime.UNISWAP_V4_POOL_MANAGER,
        "topics": [
            observation.UNISWAP_V4_INITIALIZE_TOPIC,
            pool_id,
            _topic_address(observation.ZERO_ADDRESS),
            _topic_address(token),
        ],
        "data": _data(
            _word_uint(3000),
            _word_uint(60),
            _word_address(hooks),
            _word_uint(1 << 96),
            _word_uint(0),
        ),
        "blockNumber": hex(101),
        "logIndex": hex(4),
        "transactionHash": "0xdef",
    }
    decoded = observation._decode_v4_pool(log)
    assert decoded is not None
    assert decoded["pool_id"] == pool_id
    assert decoded["currency0"] == observation.ZERO_ADDRESS
    assert decoded["currency1"] == token
    assert decoded["token"] == token
    assert decoded["hooks"] == hooks
    assert decoded["fee"] == 3000
    assert decoded["paper_eligible"] is False
    assert decoded["execution_authorized"] is False


def test_fetch_wrapper_returns_canonical_markets_unchanged(monkeypatch) -> None:
    canonical = [("v3", object(), {"blockNumber": hex(10)})]
    observed: list[tuple[int, int]] = []

    async def base(_self, *, from_block: int, to_block: int):
        return canonical

    async def observe(_self, *, from_block: int, to_block: int) -> None:
        observed.append((from_block, to_block))

    monkeypatch.setattr(observation, "_observe_range", observe)
    wrapped = observation._fetch_with_observation(base)
    result = asyncio.run(wrapped(SimpleNamespace(), from_block=9, to_block=10))
    assert result is canonical
    assert observed == [(9, 10)]


def test_observer_resumes_from_its_own_cursor_without_unbounded_backfill(tmp_path, monkeypatch) -> None:
    store = ObservationEventStore(tmp_path / "observer.sqlite3")
    try:
        with store._lock, store.db:
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS robinhood_chain_state ("
                "key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)"
            )
            store.db.execute(
                "INSERT INTO robinhood_chain_state(key,value,updated_at) VALUES (?,?,?)",
                ("robinhood_v2_v4_observation_cursor", "100", "now"),
            )

        ranges: list[tuple[int, int]] = []

        async def logs(_self, *, from_block: int, to_block: int, addresses, topics):
            ranges.append((from_block, to_block))
            return []

        monkeypatch.setattr(observation.catchup, "_logs_with_resilient_range", logs)
        plane = SimpleNamespace(store=store)
        asyncio.run(observation._observe_range(plane, from_block=110, to_block=112))
        assert ranges
        assert ranges[0] == (101, 112)
        assert plane._roi_v2v4_observation_cursor == 112
        assert plane._roi_v2v4_observation_last_range["bounded_recovery"] is True
    finally:
        store.close()


def test_status_declares_observation_without_trade_authority(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "status.sqlite3")
    try:
        with store._lock, store.db:
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS robinhood_chain_state ("
                "key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)"
            )
        plane = SimpleNamespace(store=store, _latest_block=200)
        wrapped = observation._status_with_observation(lambda _self: {"venues": {}})
        payload = wrapped(plane)
        status = payload["v2_v4_observation"]
        assert status["production_default_enabled"] is True
        assert status["observation_only"] is True
        assert status["canonical_trade_eligible"] is False
        assert status["new_entry_authority"] is False
        assert status["execution_authorized"] is False
        assert status["strategy_thresholds_changed"] is False
        assert status["paper_only"] is True
        assert status["live_money_authority"] is False
        assert payload["venues"]["uniswap_v2_observation"]["paper_authority"] is False
        assert payload["venues"]["uniswap_v4_observation"]["new_entry_authority"] is False
    finally:
        store.close()
