from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import robinhood_alchemy_budget_guard as guard


def test_emergency_load_zeroes_single_prospective_lane(monkeypatch) -> None:
    plane = SimpleNamespace()
    monkeypatch.setattr(guard.adaptive, "_rates", lambda: (700.0, 650.0, 700.0))
    monkeypatch.setattr(guard.adaptive, "_target_cu_per_minute", lambda: 600.0)
    monkeypatch.setattr(guard, "_ORIGINAL_CONTROL", lambda _self, demand, open_positions: 1)

    result = guard._guarded_control(plane, demand=1, open_positions=0)

    assert result == 0
    state = guard.adaptive._state(plane)
    assert state["prospective_lane_cap"] == 0
    assert state["last_change_reason"] == "provider_budget_emergency"


def test_emergency_does_not_depend_on_open_position_count(monkeypatch) -> None:
    plane = SimpleNamespace()
    monkeypatch.setattr(guard.adaptive, "_rates", lambda: (900.0, 900.0, 900.0))
    monkeypatch.setattr(guard.adaptive, "_target_cu_per_minute", lambda: 600.0)
    monkeypatch.setattr(guard, "_ORIGINAL_CONTROL", lambda _self, demand, open_positions: 4)

    assert guard._guarded_control(plane, demand=4, open_positions=3) == 0
    # Open positions are selected separately by the provider-budget transport and
    # therefore remain forced-live even when prospective capacity is zero.
    assert guard.adaptive._state(plane)["open_position_count"] == 3


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


def test_status_preserves_discovery_and_paper_only_boundaries() -> None:
    status = guard.status()
    assert status["factory_market_discovery_constrained"] is False
    assert status["critical_open_position_settlement_bypasses_budget"] is True
    assert status["strategy_authority_changed"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
