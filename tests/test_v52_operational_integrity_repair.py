from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from solana_roi import v52_operational_integrity_repair as repair
from solana_roi import v52_robinhood_flow_cutoff_context as cutoff_context
from solana_roi import v52_robinhood_position_lifecycle as lifecycle
from solana_roi.robinhood_chain_profit_maximizer import RobinhoodProfitMaximizerMixin


A = "0x00000000000000000000000000000000000000a1"
B = "0x00000000000000000000000000000000000000b2"
C = "0x00000000000000000000000000000000000000c3"
DEPLOYER = "0x00000000000000000000000000000000000000d4"


class _Entities:
    async def _entity_anchor(self, actor: str) -> str | None:
        return actor


def _swap(ts: float, actor: str, *, side: str = "buy", quote: int = 100, price: float = 1.0) -> dict[str, object]:
    return {
        "observed_ts": ts,
        "actor": actor,
        "side": side,
        "quote_amount_wei": quote,
        "price_eth": price,
    }


def test_flow_cutoff_includes_exact_boundary_and_excludes_future_activity() -> None:
    swaps = [
        _swap(940.0, A, price=0.9),
        _swap(950.0, A, price=1.0),
        _swap(1000.0, B, price=1.1),
        _swap(1000.001, C, quote=10_000, price=9.0),
    ]
    metrics = asyncio.run(
        repair._point_in_time_flow_metrics(
            _Entities(), swaps, deployer=DEPLOYER, decision_cutoff_ts=1000.0
        )
    )
    assert metrics["decision_cutoff_ts"] == pytest.approx(1000.0)
    assert metrics["buy_count_60s"] == 3
    assert metrics["buy_quote_wei"] == 300
    assert metrics["trigger_actor"] == B
    assert metrics["price_change_60s"] == pytest.approx(1.1 / 0.9 - 1.0)


def test_out_of_order_late_event_uses_event_time_not_ingest_order_or_wall_clock() -> None:
    swaps = [
        _swap(990.0, A),
        _swap(1050.0, C, quote=50_000),
        _swap(1000.0, B),
    ]
    metrics = asyncio.run(repair._point_in_time_flow_metrics(_Entities(), swaps))
    assert metrics["decision_cutoff_ts"] == pytest.approx(1000.0)
    assert metrics["buy_count_60s"] == 2
    assert metrics["buy_quote_wei"] == 200
    assert metrics["trigger_actor"] == B


def test_late_historical_evidence_before_cutoff_is_preserved() -> None:
    swaps = [
        _swap(900.0, A),
        _swap(945.0, A),
        _swap(980.0, B),
        _swap(1000.0, C),
    ]
    metrics = asyncio.run(
        repair._point_in_time_flow_metrics(_Entities(), swaps, decision_cutoff_ts=1000.0)
    )
    assert metrics["buy_count_60s"] == 3
    assert metrics["buy_count_acceleration"] == pytest.approx(3.0)
    assert metrics["trigger_actor"] == C


def test_entry_context_uses_trigger_event_time_and_restores_owner_state(monkeypatch) -> None:
    owner = SimpleNamespace()
    pool = SimpleNamespace(recent_swaps=[_swap(900.0, A), _swap(1000.0, B)])
    observed: list[float] = []

    async def base_flow(self, swaps, *, deployer="", decision_cutoff_ts=None):
        observed.append(float(decision_cutoff_ts))
        return {"decision_cutoff_ts": decision_cutoff_ts}

    async def base_v3(self, venue, *, current_block):
        result = await cutoff_context._flow_with_decision_context(self, venue.recent_swaps)
        assert result["decision_cutoff_ts"] == pytest.approx(1000.0)

    monkeypatch.setattr(cutoff_context, "_BASE_FLOW", base_flow)
    monkeypatch.setattr(cutoff_context, "_BASE_V3", base_v3)
    asyncio.run(cutoff_context._v3_entry_with_cutoff(owner, pool, current_block=5))
    assert observed == [1000.0]
    assert not hasattr(owner, cutoff_context._CONTEXT_ATTR)


def test_position_management_without_entry_context_uses_wall_clock_aging(monkeypatch) -> None:
    owner = SimpleNamespace()
    observed: list[float] = []

    async def base_flow(self, swaps, *, deployer="", decision_cutoff_ts=None):
        observed.append(float(decision_cutoff_ts))
        return {"decision_cutoff_ts": decision_cutoff_ts}

    monkeypatch.setattr(cutoff_context, "_BASE_FLOW", base_flow)
    monkeypatch.setattr(cutoff_context.time, "time", lambda: 2000.0)
    result = asyncio.run(cutoff_context._flow_with_decision_context(owner, [_swap(1000.0, A)]))
    assert result["decision_cutoff_ts"] == pytest.approx(2000.0)
    assert observed == [2000.0]


def test_staged_exit_contributions_are_additive_within_one_position() -> None:
    rows = [
        {"position_id": 7, "position_fraction": 0.005, "net_return": 0.20},
        {"position_id": 7, "position_fraction": 0.005, "net_return": 0.20},
    ]
    reconciled = repair.managed_position_nav_multiplier(rows)
    assert reconciled == pytest.approx(1.002, abs=1e-15)
    independently_compounded = (1.0 + 0.005 * 0.20) ** 2
    assert reconciled != pytest.approx(independently_compounded, abs=1e-12)


def test_separate_positions_preserve_existing_multiplicative_portfolio_sequence() -> None:
    rows = [
        {"position_id": 7, "position_fraction": 0.005, "net_return": 0.20},
        {"position_id": 8, "position_fraction": 0.005, "net_return": 0.20},
    ]
    assert repair.managed_position_nav_multiplier(rows) == pytest.approx(1.001 * 1.001)


def test_production_import_installs_integrity_wrappers_without_live_money_authority() -> None:
    from solana_roi import v52_strategy_api  # noqa: F401

    assert getattr(RobinhoodProfitMaximizerMixin._v5_flow_metrics, "_roi_v52_point_in_time_flow", False) is True
    assert getattr(RobinhoodProfitMaximizerMixin._v5_flow_metrics, "_roi_v52_entry_cutoff_context", False) is True
    assert getattr(lifecycle._paper_nav_with_lifecycle, "_roi_v52_staged_nav_reconciliation", False) is True
    status = repair.status()
    assert status["installed"] is True
    assert status["future_flow_allowed"] is False
    assert status["changes_strategy_thresholds"] is False
    cutoff_status = cutoff_context.status()
    assert cutoff_status["entry_decisions_use_event_time_cutoff"] is True
    assert cutoff_status["position_management_uses_wall_clock_aging"] is True
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
