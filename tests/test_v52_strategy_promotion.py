import pytest

from solana_roi.v52_continuous_evolution import (
    ContinuousStrategyEvolution,
    PolicyOutcome,
    ProspectivePolicyTournament,
)
from solana_roi.v52_strategy_promotion import promote_tournament_winner


def _winner() -> object:
    tournament = ProspectivePolicyTournament(min_paired_episodes=20, min_improvement_ratio=1.03)
    for index in range(20):
        stream = f"episode-{index:03d}"
        tournament.record(PolicyOutcome("incumbent", stream, 0.01, 0.10, True))
        tournament.record(PolicyOutcome("challenger", stream, 0.03, 0.08, True))
    decision = tournament.compare("incumbent", ["challenger"])
    assert decision.eligible is True
    return decision


def test_tournament_can_promote_ordinary_and_protected_strategy_changes_together():
    evolution = ContinuousStrategyEvolution(
        {"runner_fraction": 0.20, "starter_fraction": 0.05},
        version="v5.2",
    )
    epochs = promote_tournament_winner(
        evolution,
        _winner(),
        {
            "challenger": {
                "runner_fraction": 0.30,
                "absolute_latency_ceiling_seconds": 18.0,
            }
        },
    )

    assert len(epochs) == 2
    assert epochs[0].protected_change is False
    assert epochs[1].protected_change is True
    assert evolution.current.config["runner_fraction"] == pytest.approx(0.30)
    assert evolution.current.config["absolute_latency_ceiling_seconds"] == pytest.approx(18.0)
    assert evolution.current.config["paper_only"] is True
    assert evolution.current.config["signing_enabled"] is False
    assert all("paired_policy:challenger:episodes=20" in epoch.evidence_refs for epoch in epochs)


def test_tournament_promotion_cannot_change_live_money_authority_boundary():
    evolution = ContinuousStrategyEvolution({"runner_fraction": 0.20}, version="v5.2")
    with pytest.raises(ValueError, match="immutable authority"):
        promote_tournament_winner(
            evolution,
            _winner(),
            {"challenger": {"signing_enabled": True}},
        )
    assert len(evolution.history) == 1
    assert evolution.current.config["signing_enabled"] is False


def test_ineligible_tournament_writes_no_strategy_epoch():
    tournament = ProspectivePolicyTournament(min_paired_episodes=20)
    decision = tournament.compare("incumbent", ["challenger"])
    evolution = ContinuousStrategyEvolution({"runner_fraction": 0.20}, version="v5.2")

    assert promote_tournament_winner(
        evolution,
        decision,
        {"challenger": {"runner_fraction": 0.30}},
    ) == ()
    assert len(evolution.history) == 1
