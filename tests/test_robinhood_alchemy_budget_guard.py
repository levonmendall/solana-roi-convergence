from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import robinhood_alchemy_budget_guard as guard


def test_emergency_load_zeroes_single_prospective_lane_without_provider_pool(monkeypatch) -> None:
    plane = SimpleNamespace()
    monkeypatch.setattr(guard.adaptive, "_rates", lambda: (700.0, 650.0, 700.0))
    monkeypatch.setattr(guard.adaptive, "_target_cu_per_minute", lambda: 600.0)
    monkeypatch.setattr(guard, "_provider_pool_capacity_mode", lambda: (False, False))
    monkeypatch.setattr(guard, "_ORIGINAL_CONTROL", lambda _self, demand, open_positions: 1)

    result = guard._guarded_control(plane, demand=1, open_positions=0)

    assert result == 0
    state = guard.adaptive._state(plane)
    assert state["prospective_lane_cap"] == 0
    assert state["last_change_reason"] == "provider_budget_emergency"


def test_emergency_does_not_depend_on_open_position_count_without_provider_pool(monkeypatch) -> None:
    plane = SimpleNamespace()
    monkeypatch.setattr(guard.adaptive, "_rates", lambda: (900.0, 900.0, 900.0))
    monkeypatch.setattr(guard.adaptive, "_target_cu_per_minute", lambda: 600.0)
    monkeypatch.setattr(guard, "_provider_pool_capacity_mode", lambda: (False, False))
    monkeypatch.setattr(guard, "_ORIGINAL_CONTROL", lambda _self, demand, open_positions: 4)

    assert guard._guarded_control(plane, demand=4, open_positions=3) == 0
    # Open positions are selected separately by the provider-budget transport and
    # therefore remain forced-live even when prospective capacity is zero.
    assert guard.adaptive._state(plane)["open_position_count"] == 3


def test_provider_pool_removes_legacy_four_lane_alchemy_bottleneck(monkeypatch) -> None:
    plane = SimpleNamespace()
    monkeypatch.setenv("ROBINHOOD_PROVIDER_POOL_LIVE_MARKET_CAP", "16")
    monkeypatch.setattr(guard.adaptive, "_rates", lambda: (100.0, 100.0, 100.0))
    monkeypatch.setattr(guard.adaptive, "_target_cu_per_minute", lambda: 600.0)
    monkeypatch.setattr(guard, "_provider_pool_capacity_mode", lambda: (True, True))
    monkeypatch.setattr(guard, "_ORIGINAL_CONTROL", lambda _self, demand, open_positions: 4)

    result = guard._guarded_control(plane, demand=12, open_positions=0)

    assert result == 12
    state = guard.adaptive._state(plane)
    assert state["prospective_lane_cap"] == 12
    assert state["provider_pool_total_live_market_cap"] == 16
    assert state["provider_pool_capacity_mode"] is True
    assert state["last_change_reason"] == "provider_pool_capacity"


def test_provider_pool_preserves_open_positions_inside_total_transport_ceiling(monkeypatch) -> None:
    plane = SimpleNamespace()
    monkeypatch.setenv("ROBINHOOD_PROVIDER_POOL_LIVE_MARKET_CAP", "16")
    monkeypatch.setattr(guard.adaptive, "_rates", lambda: (100.0, 100.0, 100.0))
    monkeypatch.setattr(guard.adaptive, "_target_cu_per_minute", lambda: 600.0)
    monkeypatch.setattr(guard, "_provider_pool_capacity_mode", lambda: (True, True))
    monkeypatch.setattr(guard, "_ORIGINAL_CONTROL", lambda _self, demand, open_positions: 4)

    assert guard._guarded_control(plane, demand=20, open_positions=3) == 13
    state = guard.adaptive._state(plane)
    assert state["open_position_count"] == 3
    assert state["prospective_lane_cap"] == 13


def test_alchemy_pressure_requests_failover_without_zeroing_candidates(monkeypatch) -> None:
    plane = SimpleNamespace()
    monkeypatch.setenv("ROBINHOOD_PROVIDER_POOL_LIVE_MARKET_CAP", "16")
    monkeypatch.setattr(guard.adaptive, "_rates", lambda: (800.0, 700.0, 800.0))
    monkeypatch.setattr(guard.adaptive, "_target_cu_per_minute", lambda: 600.0)
    monkeypatch.setattr(guard, "_provider_pool_capacity_mode", lambda: (True, False))
    monkeypatch.setattr(guard, "_request_provider_failover_for_alchemy_pressure", lambda: True)
    monkeypatch.setattr(guard, "_ORIGINAL_CONTROL", lambda _self, demand, open_positions: 0)

    result = guard._guarded_control(plane, demand=10, open_positions=2)

    assert result == 10
    state = guard.adaptive._state(plane)
    assert state["prospective_lane_cap"] == 10
    assert state["last_change_reason"] == "provider_pool_alchemy_pressure_failover"


def test_active_backup_ignores_stale_alchemy_meter_for_candidate_capacity(monkeypatch) -> None:
    plane = SimpleNamespace()
    monkeypatch.setenv("ROBINHOOD_PROVIDER_POOL_LIVE_MARKET_CAP", "16")
    monkeypatch.setattr(guard.adaptive, "_rates", lambda: (900.0, 900.0, 900.0))
    monkeypatch.setattr(guard.adaptive, "_target_cu_per_minute", lambda: 600.0)
    monkeypatch.setattr(guard, "_provider_pool_capacity_mode", lambda: (True, True))
    monkeypatch.setattr(
        guard,
        "_request_provider_failover_for_alchemy_pressure",
        lambda: (_ for _ in ()).throw(AssertionError("must not fail over an already-active backup")),
    )
    monkeypatch.setattr(guard, "_ORIGINAL_CONTROL", lambda _self, demand, open_positions: 0)

    assert guard._guarded_control(plane, demand=16, open_positions=0) == 16
    assert guard.adaptive._state(plane)["last_change_reason"] == "provider_pool_capacity"


def test_provider_pool_cap_never_exceeds_hard_transport_ceiling(monkeypatch) -> None:
    monkeypatch.setenv("ROBINHOOD_PROVIDER_POOL_LIVE_MARKET_CAP", "999")
    assert guard._provider_pool_live_market_cap() == guard.MAX_PROVIDER_POOL_LIVE_MARKET_CAP == 16


def test_duplicate_production_eth_call_is_cached(monkeypatch) -> None:
    monkeypatch.setenv("ROBINHOOD_RPC_URL", "https://robinhood-mainnet.g.alchemy.com/v2/redacted")
    monkeypatch.setenv("ROBINHOOD_ALCHEMY_HARD_BURST_CU", "10000")
    monkeypatch.setenv("ROBINHOOD_ALCHEMY_ETH_CALL_CACHE_TTL_SECONDS", "5")
    guard.reset_for_tests()
    calls = 0

    async def original(rpc_self, method, params):
        nonlocal calls
        calls += 1
        return "0x1234"

    wrapped = guard._guarded_rpc(original)
    rpc = SimpleNamespace(rpc_url="https://robinhood-mainnet.g.alchemy.com/v2/redacted")
    params = [{"to": "0x" + "1" * 40, "data": "0xabcdef"}, "latest"]

    async def run() -> None:
        assert await wrapped(rpc, "eth_call", params) == "0x1234"
        assert await wrapped(rpc, "eth_call", params) == "0x1234"

    asyncio.run(run())
    assert calls == 1
    assert guard.status()["stats"]["cache_hits"] >= 1


def test_concurrent_duplicate_eth_calls_singleflight(monkeypatch) -> None:
    monkeypatch.setenv("ROBINHOOD_RPC_URL", "https://robinhood-mainnet.g.alchemy.com/v2/redacted")
    monkeypatch.setenv("ROBINHOOD_ALCHEMY_HARD_BURST_CU", "10000")
    guard.reset_for_tests()
    calls = 0

    async def original(rpc_self, method, params):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return "0xbeef"

    wrapped = guard._guarded_rpc(original)
    rpc = SimpleNamespace(rpc_url="https://robinhood-mainnet.g.alchemy.com/v2/redacted")
    params = [{"to": "0x" + "2" * 40, "data": "0x123456"}, "latest"]

    async def run() -> None:
        first, second = await asyncio.gather(
            wrapped(rpc, "eth_call", params),
            wrapped(rpc, "eth_call", params),
        )
        assert first == second == "0xbeef"

    asyncio.run(run())
    assert calls == 1
    assert guard.status()["stats"]["singleflight_hits"] >= 1


def test_public_research_rpc_is_not_governed(monkeypatch) -> None:
    monkeypatch.setenv("ROBINHOOD_RPC_URL", "https://robinhood-mainnet.g.alchemy.com/v2/redacted")
    guard.reset_for_tests()
    calls = 0

    async def original(rpc_self, method, params):
        nonlocal calls
        calls += 1
        return "0x1"

    wrapped = guard._guarded_rpc(original)
    rpc = SimpleNamespace(rpc_url=guard.runtime.ROBINHOOD_PUBLIC_RPC)

    async def run() -> None:
        await wrapped(rpc, "eth_call", [{"to": "0x" + "3" * 40, "data": "0x"}, "latest"])

    asyncio.run(run())
    assert calls == 1
    assert guard.status()["stats"]["network_eth_calls"] == 0


def test_critical_settlement_bypasses_noncritical_budget(monkeypatch) -> None:
    observed = []

    async def settle(self, trial):
        observed.append(guard._PRIORITY.get())
        return trial

    wrapped = guard._critical_settlement_wrapper(settle)
    result = asyncio.run(wrapped(SimpleNamespace(), {"id": 1}))
    assert result == {"id": 1}
    assert observed == ["critical"]
    assert guard._PRIORITY.get() == "qualification"


def test_status_preserves_discovery_and_paper_only_boundaries(monkeypatch) -> None:
    monkeypatch.setattr(guard, "_provider_pool_capacity_mode", lambda: (True, True))
    status = guard.status()
    assert status["factory_market_discovery_constrained"] is False
    assert status["critical_open_position_settlement_bypasses_budget"] is True
    assert status["alchemy_budget_restricts_robinhood_when_pool_available"] is False
    assert status["strategy_authority_changed"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
