from __future__ import annotations

from types import SimpleNamespace

import pytest

from solana_roi import v52_authoritative_strategy as strategy
from solana_roi.strategy_v52_authority import (
    AUTHORITY_ID,
    ECONOMIC_FREEZE_EPOCH,
    STRATEGY_VERSION,
    authority,
    authority_fingerprint,
    safety_manifest,
)
from solana_roi.v52_lane_contract import CANONICAL_LANES, LANE_DESCRIPTORS


def test_v52_authority_is_frozen_paper_only_and_five_lane() -> None:
    payload = authority()
    assert AUTHORITY_ID == "roi-convergence-v5.2-authoritative-1"
    assert STRATEGY_VERSION == "roi-convergence-v5.2-continuation-capture-1"
    assert ECONOMIC_FREEZE_EPOCH == "v52-authoritative-cutover-20260908"
    assert tuple(payload["canonical_lanes"]) == (
        "pump_fun",
        "pump_amm",
        "raydium",
        "fomo",
        "robinhood",
    )
    assert tuple(LANE_DESCRIPTORS) == CANONICAL_LANES
    assert payload["policy_freeze_origin"] == "operator_directed_ex_ante_cutover"
    assert payload["economic_superiority_claim"] is False
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
    assert len(authority_fingerprint()) == 64


def test_v52_safety_and_capture_policy_are_fail_closed() -> None:
    payload = authority()
    execution = payload["execution"]
    position = payload["position_management"]
    assert execution["latency_hard_max_seconds"] == 20.0
    assert execution["chase_observe_only_above_fraction"] == 0.40
    assert execution["amount_specific_entry_and_exit_quotes_required"] is True
    assert execution["first_slot_pump_fun_sniping_allowed"] is False
    assert position == {
        "starter_fraction_of_target": 0.25,
        "max_scale_fraction_of_target_per_add": 0.25,
        "first_derisk_fraction_of_position": 0.25,
        "second_derisk_fraction_of_position": 0.50,
        "runner_fraction_of_target": 0.10,
        "minimum_exit_depth_coverage_ratio": 2.0,
        "scale_requires_new_forward_evidence": True,
        "scale_requires_price_not_below_last_add": True,
        "averaging_down_allowed": False,
        "staged_derisk_enabled": True,
        "runner_enabled": True,
        "second_leg_reentry_enabled": True,
    }
    safety = safety_manifest()
    assert safety["v51_control_has_final_decision_authority"] is False
    assert safety["averaging_down_allowed"] is False


def test_v52_solana_is_final_sizing_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    def base(adapter, pre, *, chase=None, latency=None):
        return "graduation_continuation", 0.08, {"graduation_continuation": {"state": "promoted"}}

    monkeypatch.setattr(strategy, "_BASE_SOLANA_CHOOSE", base)
    lane, fraction, profiles = strategy._v52_solana_choose(object(), {}, chase=0.10, latency=4.0)
    assert lane == "graduation_continuation"
    assert fraction == pytest.approx(0.02)
    meta = profiles[lane]["v52_authority"]
    assert meta["decision_owner"] == "v52"
    assert meta["target_fraction_before_v52_capture_policy"] == pytest.approx(0.08)
    assert meta["final_fraction"] == pytest.approx(0.02)
    assert meta["averaging_down_allowed"] is False

    lane, fraction, _ = strategy._v52_solana_choose(object(), {}, chase=0.41, latency=4.0)
    assert lane is None
    assert fraction == 0.0
    lane, fraction, _ = strategy._v52_solana_choose(object(), {}, chase=0.10, latency=20.01)
    assert lane is None
    assert fraction == 0.0


def test_v52_fomo_is_final_sizing_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    def base(adapter, *, observation, trial):
        return {
            "decision": "paper_enter_promoted_fomo_wallet",
            "reason": "base",
            "position_fraction": 0.04,
            "profile": {},
        }

    monkeypatch.setattr(strategy, "_BASE_FOMO_DECISION", base)
    result = strategy._v52_fomo_decision(
        object(),
        observation={},
        trial={"signal_to_entry_seconds": 5.0, "entry_executable": True, "exit_executable": True},
    )
    assert result["decision"] == "paper_enter_v52_starter"
    assert result["position_fraction"] == pytest.approx(0.01)
    assert result["v52_authority"]["decision_owner"] == "v52"

    blocked = strategy._v52_fomo_decision(
        object(),
        observation={},
        trial={"signal_to_entry_seconds": 20.1, "entry_executable": True, "exit_executable": True},
    )
    assert blocked["decision"] == "no_entry_v52_execution_boundary"
    assert blocked["position_fraction"] == 0.0


def test_v52_robinhood_is_final_sizing_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    def base(self, **kwargs):
        return "entity_flow_accumulation", 0.04, {"entity_flow_accumulation": {"state": "promoted"}}

    monkeypatch.setattr(strategy, "_BASE_RH_CHOOSE", base)
    lane, fraction, profiles = strategy._v52_robinhood_choose(SimpleNamespace())
    assert lane == "entity_flow_accumulation"
    assert fraction == pytest.approx(0.01)
    assert profiles[lane]["v52_authority"]["decision_owner"] == "v52"


def test_v52_authority_contract_keeps_v51_as_zero_authority_control() -> None:
    governance = authority()["governance"]
    assert governance["forward_only_measurement"] is True
    assert governance["same_candidate_stream_control_comparison"] is True
    assert governance["historical_promotion_authority"] is False
    assert governance["automatic_parameter_mutation_authority"] is False
    assert governance["automatic_signal_promotion_authority"] is False
    assert governance["v51_control_has_final_decision_authority"] is False
