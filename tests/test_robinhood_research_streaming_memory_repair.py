from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from solana_roi import robinhood_provider_budget_transport as provider_budget
from solana_roi import robinhood_research_streaming_memory_repair as repair


def _address(index: int) -> str:
    return f"0x{index:040x}"


def _universe(v3_count: int, v2_count: int) -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {}
    for index in range(1, v3_count + 1):
        address = _address(index)
        result[address] = {"address": address, "kind": "v3"}
    for index in range(v3_count + 1, v3_count + v2_count + 1):
        address = _address(index)
        result[address] = {"address": address, "kind": "v2"}
    return result


def test_streaming_pass_preserves_full_batch_coverage_and_cursor_commit(monkeypatch) -> None:
    universe = _universe(65, 64)
    state = {"cursor_block": 100, "logs_seen": 7, "passes": 2}
    plane = SimpleNamespace()
    calls: list[tuple[int, int, tuple[str, ...]]] = []
    committed: list[tuple[str, str, int, str]] = []

    monkeypatch.setattr(provider_budget, "_candidate_universe", lambda _self: universe)
    monkeypatch.setattr(provider_budget, "_research_state", lambda _self: dict(state))
    monkeypatch.setattr(
        provider_budget,
        "_update_research_state",
        lambda _self, **updates: state.update(updates),
    )
    monkeypatch.setattr(
        provider_budget,
        "_research_log_signal",
        lambda _descriptor, log: (str(log["side"]), int(log["quote"]), str(log["actor"])),
    )
    monkeypatch.setattr(
        provider_budget,
        "_record_research_event",
        lambda _self, *, address, side, quote_amount, actor: committed.append(
            (address, side, quote_amount, actor)
        ),
    )

    class Rpc:
        async def block_number(self) -> int:
            return 110

        async def get_logs(self, *, from_block, to_block, addresses, topics):
            calls.append((from_block, to_block, tuple(addresses)))
            return [
                {
                    "address": addresses[0],
                    "side": "buy",
                    "quote": len(calls),
                    "actor": _address(9000 + len(calls)),
                }
            ]

    asyncio.run(repair._streaming_research_pass(plane, Rpc()))

    assert len(calls) == 3
    assert all(from_block == 101 and to_block == 110 for from_block, to_block, _ in calls)
    assert sum(len(addresses) for _, _, addresses in calls) == 129
    assert state["cursor_block"] == 110
    assert state["logs_seen"] == 10
    assert state["passes"] == 3
    assert state["raw_logs_retained_across_batches"] is False
    assert state["max_raw_batch_logs_last_pass"] == 1
    assert state["compact_pending_signals_last_pass"] == 3
    assert state["cursor_commit_requires_complete_pass"] is True
    assert len(committed) == 3


def test_streaming_pass_does_not_commit_partial_events_or_cursor_on_mid_pass_failure(monkeypatch) -> None:
    universe = _universe(65, 0)
    state = {"cursor_block": 200, "logs_seen": 4, "passes": 1}
    plane = SimpleNamespace()
    committed: list[tuple[object, ...]] = []
    calls = 0

    monkeypatch.setattr(provider_budget, "_candidate_universe", lambda _self: universe)
    monkeypatch.setattr(provider_budget, "_research_state", lambda _self: dict(state))
    monkeypatch.setattr(
        provider_budget,
        "_update_research_state",
        lambda _self, **updates: state.update(updates),
    )
    monkeypatch.setattr(
        provider_budget,
        "_research_log_signal",
        lambda _descriptor, log: ("buy", 1, str(log["actor"])),
    )
    monkeypatch.setattr(
        provider_budget,
        "_record_research_event",
        lambda *args, **kwargs: committed.append((args, kwargs)),
    )

    class Rpc:
        async def block_number(self) -> int:
            return 210

        async def get_logs(self, *, addresses, **_kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("synthetic provider failure")
            return [{"address": addresses[0], "actor": _address(999)}]

    with pytest.raises(RuntimeError, match="synthetic provider failure"):
        asyncio.run(repair._streaming_research_pass(plane, Rpc()))

    assert calls == 2
    assert state["cursor_block"] == 200
    assert state["logs_seen"] == 4
    assert state["passes"] == 1
    assert committed == []


def test_staged_signals_are_bounded_to_existing_per_market_retention(monkeypatch) -> None:
    address = _address(1)
    universe = {address: {"address": address, "kind": "v3"}}
    pending = {}
    logs = [
        {"address": address, "sequence": sequence}
        for sequence in range(300)
    ]

    monkeypatch.setattr(
        provider_budget,
        "_research_log_signal",
        lambda _descriptor, log: ("buy", int(log["sequence"]), _address(500)),
    )

    repair._stage_logs(universe, logs, pending)

    assert len(pending[address]) == 256
    assert pending[address][0][1] == 44
    assert pending[address][-1][1] == 299


def test_installer_exposes_fail_safe_memory_contract() -> None:
    status = repair.status()
    assert status["raw_provider_batches_streamed"] is True
    assert status["pending_signals_per_market_max"] == 256
    assert status["cursor_commit_requires_complete_pass"] is True
    assert status["candidate_universe_reduced"] is False
    assert status["research_block_cap_changed"] is False
    assert status["request_budget_changed"] is False
    assert status["strategy_thresholds_changed"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
