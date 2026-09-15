from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from solana_roi import robinhood_provider_budget_transport as provider_budget
from solana_roi import robinhood_request_budget_telemetry as telemetry


def test_public_research_budget_matches_exact_batch_formula(monkeypatch) -> None:
    universe = {
        **{f"v3-{i}": {"kind": "v3"} for i in range(65)},
        **{f"v2-{i}": {"kind": "v2"} for i in range(64)},
    }
    state = {"cursor_block": 100}
    plane = SimpleNamespace()
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    monkeypatch.setattr(provider_budget, "_candidate_universe", lambda _self: universe)
    monkeypatch.setattr(provider_budget, "_research_state", lambda _self: dict(state))

    def update(_self, **updates):
        state.update(updates)

    monkeypatch.setattr(provider_budget, "_update_research_state", update)

    async def base(_self, _rpc):
        state["cursor_block"] = 110
        await _rpc.get_logs(from_block=101, to_block=110, addresses=["a"], topics=["v3"])
        await _rpc.get_logs(from_block=101, to_block=110, addresses=["b"], topics=["v3"])
        await _rpc.get_logs(from_block=101, to_block=110, addresses=["c"], topics=["v2"])

    async def get_logs(*args, **kwargs):
        calls.append((args, kwargs))
        return []

    rpc = SimpleNamespace(get_logs=get_logs)
    monkeypatch.setattr(telemetry, "_ORIGINAL_RESEARCH_PASS", base)

    asyncio.run(telemetry._budgeted_research_pass(plane, rpc))

    assert len(calls) == 3
    assert state["expected_getlogs_last_pass"] == 3
    assert state["actual_getlogs_last_pass"] == 3
    assert state["request_budget_last_pass_explained"] is True
    assert state["request_budget_violations"] == 0


def test_public_research_budget_fails_closed_on_unexplained_amplification(monkeypatch) -> None:
    universe = {f"v3-{i}": {"kind": "v3"} for i in range(10)}
    state = {"cursor_block": 200, "ready": True, "last_error_type": None}
    plane = SimpleNamespace()

    monkeypatch.setattr(provider_budget, "_candidate_universe", lambda _self: universe)
    monkeypatch.setattr(provider_budget, "_research_state", lambda _self: dict(state))
    monkeypatch.setattr(provider_budget, "_update_research_state", lambda _self, **updates: state.update(updates))

    async def base(_self, _rpc):
        state["cursor_block"] = 201
        await _rpc.get_logs(from_block=201, to_block=201, addresses=["a"], topics=["v3"])
        await _rpc.get_logs(from_block=201, to_block=201, addresses=["a"], topics=["v3"])

    async def get_logs(*_args, **_kwargs):
        return []

    rpc = SimpleNamespace(get_logs=get_logs)
    monkeypatch.setattr(telemetry, "_ORIGINAL_RESEARCH_PASS", base)

    with pytest.raises(telemetry.RobinhoodRequestBudgetViolation):
        asyncio.run(telemetry._budgeted_research_pass(plane, rpc))

    assert state["expected_getlogs_last_pass"] == 1
    assert state["actual_getlogs_last_pass"] == 2
    assert state["request_budget_last_pass_explained"] is False
    assert state["request_budget_violations"] == 1
    assert state["ready"] is False
    assert state["last_error_type"] == "RobinhoodRequestBudgetViolation"
