from __future__ import annotations

from types import SimpleNamespace

from solana_roi import robinhood_adaptive_lane_controller as adaptive
from solana_roi import robinhood_provider_meter as meter


def _plane() -> SimpleNamespace:
    return SimpleNamespace()


def test_low_provider_load_expands_one_lane_at_a_time(monkeypatch) -> None:
    plane = _plane()
    monkeypatch.setattr(adaptive, "_rates", lambda: (0.0, 0.0, 0.0))
    monkeypatch.setattr(adaptive, "_min_lanes", lambda: 1)
    monkeypatch.setattr(adaptive, "_max_lanes", lambda: 4)
    monkeypatch.setattr(adaptive, "_target_cu_per_minute", lambda: 600.0)

    state = adaptive._state(plane)
    state["prospective_lane_cap"] = 1
    state["last_control_monotonic"] = -100.0
    state["last_change_monotonic"] = -100.0
    assert adaptive._control(plane, demand=4, open_positions=0) == 2

    state["last_control_monotonic"] = -100.0
    state["last_change_monotonic"] = -100.0
    assert adaptive._control(plane, demand=4, open_positions=0) == 3

    state["last_control_monotonic"] = -100.0
    state["last_change_monotonic"] = -100.0
    assert adaptive._control(plane, demand=4, open_positions=0) == 4


def test_high_provider_load_contracts_prospecting_before_open_positions(monkeypatch) -> None:
    plane = _plane()
    monkeypatch.setattr(adaptive, "_rates", lambda: (700.0, 650.0, 700.0))
    monkeypatch.setattr(adaptive, "_min_lanes", lambda: 1)
    monkeypatch.setattr(adaptive, "_max_lanes", lambda: 4)
    monkeypatch.setattr(adaptive, "_target_cu_per_minute", lambda: 600.0)

    state = adaptive._state(plane)
    state["prospective_lane_cap"] = 4
    state["last_control_monotonic"] = -100.0
    assert adaptive._control(plane, demand=4, open_positions=2) == 0
    assert state["last_change_reason"] == "provider_budget_emergency"


def test_open_positions_are_outside_prospective_cap(monkeypatch) -> None:
    plane = _plane()
    open_address = "0x" + "1" * 40
    candidate_a = "0x" + "2" * 40
    candidate_b = "0x" + "3" * 40

    def descriptor(address: str, block: int) -> dict[str, object]:
        return {
            "address": address,
            "kind": "v3",
            "protocol": "UNISWAP_V3",
            "venue": "UNISWAP_V3_DIRECT",
            "lifecycle": "new_weth_pool",
            "token": "0x" + "4" * 40,
            "deployer": "0x" + "5" * 40,
            "pair_token": "",
            "fee": 3000,
            "launch_block": block,
            "restrictions_end_block": 0,
            "graduation_threshold": 0,
        }

    universe = {
        open_address: descriptor(open_address, 10),
        candidate_a: descriptor(candidate_a, 11),
        candidate_b: descriptor(candidate_b, 12),
    }
    monkeypatch.setattr(adaptive.budget, "_candidate_universe", lambda _self: universe)
    monkeypatch.setattr(adaptive.budget, "_open_market_addresses", lambda _self: {open_address})
    monkeypatch.setattr(
        adaptive.budget,
        "_research_rankings",
        lambda _self, _universe: [
            (candidate_a, 100.0, "prospective_buy_flow"),
            (candidate_b, 90.0, "prospective_buy_flow"),
        ],
    )
    monkeypatch.setattr(adaptive.budget, "_ensure_runtime_market", lambda _self, _descriptor: None)
    monkeypatch.setattr(adaptive.budget, "_update_research_state", lambda _self, **_updates: None)
    monkeypatch.setattr(adaptive, "_control", lambda _self, demand, open_positions: 2)

    selected, reasons = adaptive._adaptive_selected_market_targets(plane)
    assert set(selected) == {open_address, candidate_a, candidate_b}
    assert reasons[open_address] == "open_position_forced_live_outside_prospective_cap"
    assert plane._roi_adaptive_prospective_lane_count == 2
    assert plane._roi_adaptive_open_position_live_count == 1


def test_public_rpc_is_never_counted_as_production_provider(monkeypatch) -> None:
    monkeypatch.setenv("ROBINHOOD_RPC_URL", "https://robinhood-mainnet.g.alchemy.com/v2/redacted")
    assert adaptive._production_rpc_url(adaptive.runtime.ROBINHOOD_PUBLIC_RPC) is False
    assert adaptive._production_rpc_url("https://robinhood-mainnet.g.alchemy.com/v2/redacted") is True


def test_provider_meter_reports_non_authoritative_estimator() -> None:
    meter.reset_for_tests()
    meter.record_ws_log({"address": "0x" + "a" * 40, "data": "0x1234"})
    snapshot = meter.snapshot(60.0)
    assert snapshot["estimated_cu_per_minute"] > 0
    assert snapshot["estimator"]["billing_authority"] is False


def test_module_status_preserves_strategy_and_safety_boundaries(monkeypatch) -> None:
    monkeypatch.setattr(adaptive, "_min_lanes", lambda: 1)
    monkeypatch.setattr(adaptive, "_max_lanes", lambda: 4)
    monkeypatch.setattr(adaptive, "_target_cu_per_minute", lambda: 600.0)
    status = adaptive.status()
    assert status["adaptive_prospective_lanes"] is True
    assert status["open_positions_outside_prospective_cap"] is True
    assert status["candidate_discovery_constrained_by_cap"] is False
    assert status["strategy_authority_changed"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
