import pytest

import solana_roi.strategy_v52_authority as authority_module
from solana_roi.strategy_v52_authority import (
    authority,
    execution_policy,
    new_strategy_evolution,
    position_policy,
    promote_prospective_tournament_winner,
    safety_manifest,
    target_sizing_policy,
)
from solana_roi.v52_continuous_evolution import PolicyOutcome, ProspectivePolicyTournament


def _winner():
    tournament = ProspectivePolicyTournament(min_paired_episodes=20, min_improvement_ratio=1.03)
    for index in range(20):
        stream = f"canonical-{index:03d}"
        tournament.record(PolicyOutcome("incumbent", stream, 0.01, 0.10, True))
        tournament.record(PolicyOutcome("challenger", stream, 0.03, 0.08, True))
    decision = tournament.compare("incumbent", ["challenger"])
    assert decision.eligible is True
    return decision


def test_authority_declares_continuous_evolution_without_widening_execution_authority():
    payload = authority()
    governance = payload["governance"]
    assert governance["continuous_strategy_evolution_enabled"] is True
    assert governance["protected_strategy_change_requires_forward_validation"] is True
    assert governance["prospective_tournament_promotion_authority"] is True
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
    assert "frozen_during_forward_authority_epoch" not in payload["change_policy"]


def test_live_policy_getters_follow_isolated_strategy_epochs():
    evolution = new_strategy_evolution()
    baseline = execution_policy(evolution)
    assert baseline["latency_hard_max_seconds"] == pytest.approx(20.0)
    assert baseline["chase_observe_only_above_fraction"] == pytest.approx(0.40)

    evolution.evolve(
        {
            "position_management.runner_fraction_of_target": 0.18,
            "target_sizing.bootstrap_fraction_clean": 0.015,
        },
        rationale="forward ordinary calibration",
        evidence_refs=("forward:ordinary-epoch",),
    )
    evolution.evolve_protected_strategy_constraint(
        {
            "absolute_latency_ceiling_seconds": 18.0,
            "chase_observe_only_fraction": 0.35,
        },
        rationale="validated protected forward improvement",
        validation_refs=("test:protected-policy", "forward:paired-epoch"),
    )

    assert execution_policy(evolution)["latency_hard_max_seconds"] == pytest.approx(18.0)
    assert execution_policy(evolution)["chase_observe_only_above_fraction"] == pytest.approx(0.35)
    assert position_policy(evolution)["runner_fraction_of_target"] == pytest.approx(0.18)
    assert target_sizing_policy(evolution)["bootstrap_fraction_clean"] == pytest.approx(0.015)

    manifest = safety_manifest(evolution)
    assert manifest["continuous_strategy_evolution_enabled"] is True
    assert manifest["active_strategy_epoch"]["sequence"] == 3
    assert manifest["paper_only"] is True
    assert manifest["live_money_authority"] is False
    assert manifest["signing_available"] is False
    assert manifest["transaction_submission_available"] is False


def test_canonical_tournament_promotion_changes_strategy_not_authority(monkeypatch):
    evolution = new_strategy_evolution()
    monkeypatch.setattr(authority_module, "_CANONICAL_EVOLUTION", evolution)

    epochs = promote_prospective_tournament_winner(
        _winner(),
        {
            "challenger": {
                "position_management.runner_fraction_of_target": 0.20,
                "absolute_latency_ceiling_seconds": 17.0,
            }
        },
    )

    assert len(epochs) == 2
    assert position_policy(evolution)["runner_fraction_of_target"] == pytest.approx(0.20)
    assert execution_policy(evolution)["latency_hard_max_seconds"] == pytest.approx(17.0)
    assert evolution.current.config["paper_only"] is True
    assert evolution.current.config["signing_enabled"] is False
    assert evolution.current.config["transaction_submission_enabled"] is False
    assert evolution.current.config["live_money_authority"] is False


def test_canonical_tournament_rejects_authority_mutation_before_epoch_write(monkeypatch):
    evolution = new_strategy_evolution()
    monkeypatch.setattr(authority_module, "_CANONICAL_EVOLUTION", evolution)

    with pytest.raises(ValueError, match="immutable authority"):
        promote_prospective_tournament_winner(
            _winner(),
            {"challenger": {"signing_enabled": True}},
        )

    assert len(evolution.history) == 1
    assert evolution.current.config["signing_enabled"] is False
