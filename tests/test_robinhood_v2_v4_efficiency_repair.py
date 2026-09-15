from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi.observation_store import ObservationEventStore
from solana_roi import robinhood_v2_v4_efficiency_repair as repair
from solana_roi import robinhood_v2_v4_observation as observation
from solana_roi import robinhood_v2_v4_pause_guard as pause_guard


def _state_store(tmp_path):
    store = ObservationEventStore(tmp_path / "v2v4-efficiency.sqlite3")
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS robinhood_chain_state ("
            "key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)"
        )
    return store


def _guarded_plane(**kwargs):
    return SimpleNamespace(**{pause_guard.SCHEDULE_GUARD_ATTR: True, **kwargs})


def test_disabled_schedule_does_not_invoke_observer(monkeypatch) -> None:
    monkeypatch.delenv("ROBINHOOD_V2_V4_OBSERVATION_ENABLED", raising=False)
    observed: list[tuple[int, int]] = []

    async def base(_self, *, from_block: int, to_block: int):
        return [("canonical", None, {"blockNumber": hex(to_block)})]

    async def observe(_self, *, from_block: int, to_block: int) -> None:
        observed.append((from_block, to_block))

    monkeypatch.setattr(observation, "_observe_range", observe)
    wrapped = repair._gated_fetch_factory(base)
    result = asyncio.run(wrapped(_guarded_plane(), from_block=10, to_block=11))

    assert result[0][0] == "canonical"
    assert observed == []


def test_enabled_schedule_invokes_observer_without_changing_canonical(monkeypatch) -> None:
    monkeypatch.setenv("ROBINHOOD_V2_V4_OBSERVATION_ENABLED", "true")
    observed: list[tuple[int, int]] = []
    canonical = [("canonical", None, {"blockNumber": hex(11)})]

    async def base(_self, *, from_block: int, to_block: int):
        return canonical

    async def observe(_self, *, from_block: int, to_block: int) -> None:
        observed.append((from_block, to_block))

    monkeypatch.setattr(observation, "_observe_range", observe)
    wrapped = repair._gated_fetch_factory(base)
    result = asyncio.run(wrapped(_guarded_plane(), from_block=10, to_block=11))

    assert result is canonical
    assert observed == [(10, 11)]


def test_observer_primitive_resumes_even_when_production_schedule_is_disabled(tmp_path, monkeypatch) -> None:
    store = _state_store(tmp_path)
    try:
        with store._lock, store.db:
            store.db.execute(
                "INSERT INTO robinhood_chain_state(key,value,updated_at) VALUES (?,?,?)",
                ("robinhood_v2_v4_observation_cursor", "100", "now"),
            )
        calls: list[tuple[int, int, list[str], list[object]]] = []

        async def logs(_self, *, from_block: int, to_block: int, addresses, topics):
            calls.append((from_block, to_block, list(addresses or []), list(topics or [])))
            return []

        monkeypatch.delenv("ROBINHOOD_V2_V4_OBSERVATION_ENABLED", raising=False)
        monkeypatch.setattr(observation.catchup, "_logs_with_resilient_range", logs)
        plane = _guarded_plane(store=store)

        asyncio.run(repair._bounded_observe_range(plane, from_block=110, to_block=112))

        assert calls
        assert calls[0][0:2] == (101, 112)
        assert plane._roi_v2v4_observation_cursor == 112
        assert plane._roi_v2v4_observation_last_range["actual_market_requests"] == 0
    finally:
        store.close()


def test_zero_tracked_v4_pools_issues_zero_v4_activity_requests(tmp_path, monkeypatch) -> None:
    store = _state_store(tmp_path)
    try:
        calls: list[dict[str, object]] = []

        async def logs(_self, *, from_block: int, to_block: int, addresses, topics):
            calls.append({"addresses": list(addresses or []), "topics": list(topics or [])})
            return []

        monkeypatch.setattr(observation.catchup, "_logs_with_resilient_range", logs)
        plane = SimpleNamespace(store=store)

        asyncio.run(repair._bounded_observe_range(plane, from_block=1, to_block=2))

        assert len(calls) == 1  # discovery only
        assert calls[0]["addresses"] == [
            observation.UNISWAP_V2_FACTORY,
            observation.runtime.UNISWAP_V4_POOL_MANAGER,
        ]
        metrics = plane._roi_v2v4_observation_metrics
        assert metrics["market_requests"] == 0
        assert metrics["v4_activity_requests_suppressed_empty"] == 1
        assert plane._roi_v2v4_observation_last_range["expected_market_requests"] == 0
    finally:
        store.close()


def test_v4_activity_is_provider_filtered_to_tracked_pool_ids(tmp_path, monkeypatch) -> None:
    store = _state_store(tmp_path)
    try:
        calls: list[dict[str, object]] = []
        pool_a = "0x" + "11" * 32
        pool_b = "0x" + "22" * 32

        async def logs(_self, *, from_block: int, to_block: int, addresses, topics):
            calls.append({"addresses": list(addresses or []), "topics": list(topics or [])})
            return []

        monkeypatch.setattr(observation.catchup, "_logs_with_resilient_range", logs)
        plane = SimpleNamespace(store=store)
        observation._ensure_state(plane)
        plane._roi_v4_observation_pools = {
            pool_a: {"pool_id": pool_a, "token": "0x" + "aa" * 20},
            pool_b: {"pool_id": pool_b, "token": "0x" + "bb" * 20},
        }

        asyncio.run(repair._bounded_observe_range(plane, from_block=1, to_block=2))

        assert len(calls) == 2  # discovery + one filtered V4 activity request
        v4 = calls[1]
        assert v4["addresses"] == [observation.runtime.UNISWAP_V4_POOL_MANAGER]
        assert v4["topics"] == [observation.runtime.V4_SWAP_TOPIC, [pool_a, pool_b]]
        metrics = plane._roi_v2v4_observation_metrics
        assert metrics["v4_activity_requests"] == 1
        assert metrics["v4_pool_filters_submitted"] == 2
        assert metrics["expected_market_requests"] == metrics["market_requests"] == 1
    finally:
        store.close()


def test_status_reports_fail_closed_schedule_and_efficiency_contract(tmp_path, monkeypatch) -> None:
    store = _state_store(tmp_path)
    try:
        monkeypatch.delenv("ROBINHOOD_V2_V4_OBSERVATION_ENABLED", raising=False)
        wrapped = repair._status_factory(lambda _self: {"venues": {}})
        payload = wrapped(_guarded_plane(store=store, _latest_block=10))
        status = payload["v2_v4_observation"]

        assert status["enabled"] is False
        assert status["production_default_enabled"] is False
        assert status["reactivation_requires_explicit_true"] is True
        assert status["schedule_gate_separate_from_observer_primitive"] is True
        assert status["production_schedule_guarded"] is True
        assert status["zero_pool_v4_activity_suppressed"] is True
        assert status["v4_tracked_pool_topic_filtering"] is True
        assert status["paper_only"] is True
        assert status["live_money_authority"] is False
    finally:
        store.close()
