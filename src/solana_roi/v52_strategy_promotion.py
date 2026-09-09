from __future__ import annotations

from typing import Any, Mapping

from .v52_continuous_evolution import (
    IMMUTABLE_AUTHORITY_KEYS,
    PROTECTED_STRATEGY_KEYS,
    ContinuousStrategyEvolution,
    StrategyEpoch,
    TournamentDecision,
)


def promote_tournament_winner(
    evolution: ContinuousStrategyEvolution,
    decision: TournamentDecision,
    policy_configs: Mapping[str, Mapping[str, Any]],
    *,
    rationale_prefix: str = "prospective_policy_tournament",
) -> tuple[StrategyEpoch, ...]:
    """Promote an eligible forward tournament winner into auditable strategy epochs.

    Ordinary parameters and protected strategy constraints are both evolvable.
    Protected constraints use the explicit validated path.  The immutable
    paper/live-money authority boundary is rejected before any epoch is written.

    Mixed winner configs are applied synchronously as two append-only epochs:
    ordinary parameters first, protected constraints second.  No external
    publication occurs inside this helper, so callers observe the completed
    winner configuration after the function returns.
    """
    if not decision.eligible or decision.winner is None:
        return ()

    winner = decision.winner
    configured = policy_configs.get(winner)
    if configured is None:
        raise KeyError(f"missing policy config for winner {winner}")
    changes = dict(configured)
    if not changes:
        raise ValueError("winner policy config must contain at least one change")

    immutable = IMMUTABLE_AUTHORITY_KEYS.intersection(changes)
    if immutable:
        raise ValueError(f"immutable authority keys cannot be changed: {sorted(immutable)}")

    ordinary = {key: value for key, value in changes.items() if key not in PROTECTED_STRATEGY_KEYS}
    protected = {key: value for key, value in changes.items() if key in PROTECTED_STRATEGY_KEYS}
    paired_episodes = min((score.episodes for score in decision.scores), default=0)
    evidence_refs = (f"paired_policy:{winner}:episodes={paired_episodes}",)
    rationale = f"{rationale_prefix}:{winner}:ratio={decision.improvement_ratio}"

    epochs: list[StrategyEpoch] = []
    if ordinary:
        epochs.append(
            evolution.evolve(
                ordinary,
                rationale=rationale,
                evidence_refs=evidence_refs,
            )
        )
    if protected:
        epochs.append(
            evolution.evolve_protected_strategy_constraint(
                protected,
                rationale=rationale,
                validation_refs=evidence_refs,
            )
        )
    return tuple(epochs)
