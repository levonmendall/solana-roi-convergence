from __future__ import annotations

import json
import sqlite3
import threading
from types import SimpleNamespace

import pytest

from solana_roi import v52_authoritative_strategy as strategy
from solana_roi import v52_robinhood_exit_authority as robinhood_exit
from solana_roi.strategy_v52_authority import (
    AUTHORITY_ID,
    ECONOMIC_FREEZE_EPOCH,
    STRATEGY_VERSION,
    authority,
    authority_fingerprint,
    safety_manifest,
)
from solana_roi.v52_lane_contract import (
    CANONICAL_LANES,
    LANE_DESCRIPTORS,
    canonical_lane_for_surface,
)


class _Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


def test_v52_authority_keeps_baseline_paper_only_and_five_lane() -> None:
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
    assert canonical_lane_for_surface("PUMP_FUN") == "pump_fun"
    assert canonical_lane_for_surface("PUMPSWAP") == "pump_amm"
    assert canonical_lane_for_surface("PUMP_AMM") == "pump_amm"
    assert canonical_lane_for_surface("RAYDIUM") == "raydium"
    assert canonical_lane_for_surface("FOMO") == "fomo"
    assert canonical_lane_for_surface("ROBINHOOD_CHAIN") == "robinhood"
    assert payload["policy_freeze_origin"] == "legacy_baseline_epoch_identifier_continuous_evolution_enabled"
    assert payload["governance"]["continuous_strategy_evolution_enabled"] is True
    assert payload["governance"]["protected_strategy_change_requires_forward_validation"] is True
    assert payload["governance"]["prospective_tournament_promotion_authority"] is True
    assert payload["economic_superiority_claim"] is False
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
    assert len(authority_fingerprint()) == 64


def test_v52_baseline_safety_capture_detection_and_forward_evidence_policy() -> None:
    payload = authority()
    execution = payload["execution"]
    position = payload["position_management"]
    detection = payload["detection_intelligence"]
    sizing = payload["target_sizing"]
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
    assert detection["minimum_wallet_quality"] == 0.70
    assert detection["minimum_skilled_independent_clusters"] == 3
    assert detection["minimum_broad_independent_clusters"] == 5
    assert detection["minimum_comparable_peer_count"] == 20
    assert detection["anomaly_percentile_threshold"] == 0.995
    assert detection["discovered_wallet_initial_signal_weight"] == 0.0
    assert sizing["fresh_v52_forward_evidence_required_for_promotion"] is True
    assert sizing["v51_outcomes_may_seed_target_selection_prior"] is False
    assert sizing["v51_outcomes_may_grant_v52_promotion"] is False
    safety = safety_manifest()
    assert safety["v51_control_has_final_decision_authority"] is False
    assert safety["v51_control_is_read_only"] is True
    assert safety["averaging_down_allowed"] is False
    assert safety["continuous_strategy_evolution_enabled"] is True
    assert safety["protected_strategy_change_requires_forward_validation"] is True


def test_v52_solana_fractional_starter_and_execution_boundaries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(strategy, "_ensure_v52_epoch", lambda owner: None)
    monkeypatch.setattr(
        strategy,
        "_v52_solana_target",
        lambda adapter, pre, chase, latency: (
            "graduation_continuation",
            0.08,
            {"graduation_continuation": {"state": "bootstrap_forward_evidence"}},
        ),
    )
    monkeypatch.setattr(strategy, "_open_solana_rows", lambda adapter, token: [])
    monkeypatch.setattr(strategy, "_current_entry_price", lambda adapter, pre, chase: 1.10)
    monkeypatch.setattr(strategy, "_record_candidate_state", lambda *args, **kwargs: None)

    pre = {"token": "mint-1", "venue": "PUMP_AMM", "lifecycle": "early_post_graduation"}
    lane, fraction, profiles = strategy._v52_solana_choose(object(), pre, chase=0.10, latency=4.0)
    assert lane == "graduation_continuation"
    assert fraction == pytest.approx(0.02)
    meta = profiles[lane]["v52_authority"]
    assert meta["decision_owner"] == "v52"
    assert meta["target_fraction"] == pytest.approx(0.08)
    assert meta["final_fraction"] == pytest.approx(0.02)
    assert meta["capture_stage"] == "starter"
    assert meta["averaging_down_allowed"] is False

    lane, fraction, _ = strategy._v52_solana_choose(object(), pre, chase=0.41, latency=4.0)
    assert lane is None
    assert fraction == 0.0
    lane, fraction, _ = strategy._v52_solana_choose(object(), pre, chase=0.10, latency=20.01)
    assert lane is None
    assert fraction == 0.0


def test_v52_solana_scale_requires_new_evidence_and_cannot_average_down(monkeypatch: pytest.MonkeyPatch) -> None:
    prior = [{
        "position_fraction": 0.02,
        "entry_all_in_price_sol": 1.0,
        "venue": "PUMP_AMM",
        "lifecycle": "early_post_graduation",
        "lane": "graduation_continuation",
        "risk_severity": 0.0,
        "opportunity_json": json.dumps({"independent_confirmation_count": 2}),
    }]
    monkeypatch.setattr(strategy, "_ensure_v52_epoch", lambda owner: None)
    monkeypatch.setattr(
        strategy,
        "_v52_solana_target",
        lambda adapter, pre, chase, latency: (
            "graduation_continuation",
            0.08,
            {"graduation_continuation": {}},
        ),
    )
    monkeypatch.setattr(strategy, "_open_solana_rows", lambda adapter, token: prior)
    monkeypatch.setattr(strategy, "_record_candidate_state", lambda *args, **kwargs: None)

    monkeypatch.setattr(strategy, "_new_solana_scale_evidence", lambda pre, rows, lane: False)
    monkeypatch.setattr(strategy, "_current_entry_price", lambda adapter, pre, chase: 1.10)
    lane, fraction, profiles = strategy._v52_solana_choose(
        object(), {"token": "mint-1", "venue": "PUMP_AMM", "lifecycle": "early_post_graduation"},
        chase=0.10, latency=4.0,
    )
    assert lane is None
    assert fraction == 0.0
    assert profiles["graduation_continuation"]["v52_authority"]["reason"] == "scale_blocked_no_new_forward_evidence"

    monkeypatch.setattr(strategy, "_new_solana_scale_evidence", lambda pre, rows, lane: True)
    monkeypatch.setattr(strategy, "_current_entry_price", lambda adapter, pre, chase: 0.90)
    lane, fraction, profiles = strategy._v52_solana_choose(
        object(), {"token": "mint-1", "venue": "PUMP_AMM", "lifecycle": "early_post_graduation"},
        chase=0.10, latency=4.0,
    )
    assert lane is None
    assert fraction == 0.0
    assert profiles["graduation_continuation"]["v52_authority"]["reason"] == "scale_blocked_averaging_down"


def test_v52_fomo_uses_fresh_epoch_bootstrap_and_fractional_starter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(strategy, "_ensure_v52_epoch", lambda owner: None)
    monkeypatch.setattr(strategy, "_v52_fomo_values", lambda *args, **kwargs: [])
    monkeypatch.setattr(strategy, "_open_fomo_rows", lambda adapter, token: [])
    monkeypatch.setattr(strategy, "_record_candidate_state", lambda *args, **kwargs: None)
    observation = {
        "state_json": json.dumps({"state": "active_fomo", "structurally_accessible": True}),
        "venue": "PUMP_AMM",
        "lifecycle": "early_post_graduation",
        "regime": "neutral",
        "token_mint": "mint-fomo",
    }
    trial = {
        "signal_to_entry_seconds": 5.0,
        "entry_executable": True,
        "exit_executable": True,
        "trigger_wallet": "wallet-1",
        "token_mint": "mint-fomo",
        "entry_all_in_price_sol": 1.0,
    }
    result = strategy._v52_fomo_decision(object(), observation=observation, trial=trial)
    assert result["decision"] == "paper_enter_v52_starter"
    assert result["position_fraction"] == pytest.approx(0.0025)
    assert result["profile"]["evidence_source"] == "v52_authoritative_forward_epoch_only"
    assert result["profile"]["v51_promotion_evidence_used"] is False
    assert result["v52_authority"]["decision_owner"] == "v52"

    blocked = strategy._v52_fomo_decision(
        object(), observation=observation, trial={**trial, "signal_to_entry_seconds": 20.1}
    )
    assert blocked["decision"] == "no_entry_v52_execution_boundary"
    assert blocked["position_fraction"] == 0.0


def test_v52_robinhood_is_final_fractional_sizing_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    def base(self, **kwargs):
        return "entity_flow_accumulation", 0.04, {"entity_flow_accumulation": {"state": "promoted"}}

    monkeypatch.setattr(strategy, "_ensure_v52_epoch", lambda owner: None)
    monkeypatch.setattr(strategy, "_BASE_RH_CHOOSE", base)
    lane, fraction, profiles = strategy._v52_robinhood_choose(SimpleNamespace())
    assert lane == "entity_flow_accumulation"
    assert fraction == pytest.approx(0.01)
    assert profiles[lane]["v52_authority"]["decision_owner"] == "v52"
    assert profiles[lane]["v52_authority"]["capture_stage"] == "starter"


def test_robinhood_exit_policy_never_requires_v51_freeze_table() -> None:
    store = _Store()
    with store._lock, store.db:
        store.db.execute("CREATE TABLE robinhood_v5_trial_context(trial_id INTEGER PRIMARY KEY,lane TEXT NOT NULL)")
        store.db.execute("INSERT INTO robinhood_v5_trial_context(trial_id,lane) VALUES (1,'entity_flow_accumulation')")
    owner = SimpleNamespace(store=store, release_commit="v52-test-release")
    policy = robinhood_exit._v52_learned_exit_policy(
        owner,
        {"id": 1, "venue": "UNISWAP_V3", "lifecycle": "new_weth_pool"},
    )
    assert policy["source"] == "v52_frozen_bootstrap_exit"
    assert policy["authority_id"] == AUTHORITY_ID
    assert policy["economic_freeze_epoch"] == ECONOMIC_FREEZE_EPOCH
    assert policy["v51_evidence_used"] is False


def test_v52_authority_contract_keeps_v51_as_zero_authority_control() -> None:
    governance = authority()["governance"]
    assert governance["forward_only_measurement"] is True
    assert governance["same_candidate_stream_control_comparison"] is True
    assert governance["historical_promotion_authority"] is False
    assert governance["automatic_parameter_mutation_authority"] is False
    assert governance["automatic_signal_promotion_authority"] is False
    assert governance["continuous_strategy_evolution_enabled"] is True
    assert governance["protected_strategy_change_requires_forward_validation"] is True
    assert governance["prospective_tournament_promotion_authority"] is True
    assert governance["v51_control_has_final_decision_authority"] is False
    assert governance["v51_control_is_read_only"] is True
