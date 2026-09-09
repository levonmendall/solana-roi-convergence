from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Literal, Mapping, Sequence


IMMUTABLE_AUTHORITY_KEYS = frozenset(
    {
        "paper_only",
        "signing_enabled",
        "transaction_submission_enabled",
        "live_money_authority",
    }
)

PROTECTED_STRATEGY_KEYS = frozenset(
    {
        "allow_averaging_down",
        "allow_first_slot_sniping",
        "require_exact_two_sided_quote",
        "require_exact_sell_route",
        "require_structural_exitability",
        "absolute_latency_ceiling_seconds",
        "chase_observe_only_fraction",
    }
)

DEFAULT_AUTHORITY_BOUNDARY = {
    "paper_only": True,
    "signing_enabled": False,
    "transaction_submission_enabled": False,
    "live_money_authority": False,
}

CURRENT_PROTECTED_STRATEGY = {
    "allow_averaging_down": False,
    "allow_first_slot_sniping": False,
    "require_exact_two_sided_quote": True,
    "require_exact_sell_route": True,
    "require_structural_exitability": True,
    "absolute_latency_ceiling_seconds": 20.0,
    "chase_observe_only_fraction": 0.40,
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return min(high, max(low, float(value)))


@dataclass(frozen=True, slots=True)
class OutcomeContext:
    lane: str
    lifecycle: str
    regime: str
    risk_signature: str

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (self.lane, self.lifecycle, self.regime, self.risk_signature)


@dataclass(frozen=True, slots=True)
class OutcomeEpisode:
    context: OutcomeContext
    net_return: float
    structural_collapse: bool = False


@dataclass(frozen=True, slots=True)
class CalibratedOutcomeDistribution:
    sample_size: int
    p_gain_25: float
    p_gain_50: float
    p_2x: float
    p_5x: float
    p_loss_20: float
    p_loss_50: float
    p_structural_collapse: float
    expected_log_growth: float
    confidence: float


class OutcomeCalibrator:
    """Empirical context calibration with conservative shrinkage.

    Historical/backtest episodes may be used for research calibration when the
    caller has handled leakage/overfitting controls. Production promotion remains
    a separate governance concern. The calibrator never authorizes a trade.
    """

    def __init__(self, *, prior_weight: float = 8.0, confidence_sample: int = 40) -> None:
        if prior_weight <= 0 or confidence_sample <= 0:
            raise ValueError("prior_weight and confidence_sample must be positive")
        self.prior_weight = float(prior_weight)
        self.confidence_sample = int(confidence_sample)
        self._episodes: dict[tuple[str, str, str, str], list[OutcomeEpisode]] = {}

    def record(self, episode: OutcomeEpisode) -> None:
        if not math.isfinite(episode.net_return) or episode.net_return <= -1.0:
            raise ValueError("net_return must be finite and greater than -1")
        self._episodes.setdefault(episode.context.key, []).append(episode)

    def calibrate(
        self,
        context: OutcomeContext,
        *,
        base_rates: Mapping[str, float] | None = None,
    ) -> CalibratedOutcomeDistribution:
        episodes = self._episodes.get(context.key, [])
        n = len(episodes)
        defaults = {
            "p_gain_25": 0.20,
            "p_gain_50": 0.12,
            "p_2x": 0.06,
            "p_5x": 0.01,
            "p_loss_20": 0.20,
            "p_loss_50": 0.07,
            "p_structural_collapse": 0.03,
        }
        if base_rates:
            defaults.update({key: _clamp(value) for key, value in base_rates.items() if key in defaults})

        def shrunk(count: int, prior_key: str) -> float:
            return (count + self.prior_weight * defaults[prior_key]) / (n + self.prior_weight)

        returns = [episode.net_return for episode in episodes]
        counts = {
            "p_gain_25": sum(value >= 0.25 for value in returns),
            "p_gain_50": sum(value >= 0.50 for value in returns),
            "p_2x": sum(value >= 1.00 for value in returns),
            "p_5x": sum(value >= 4.00 for value in returns),
            "p_loss_20": sum(value <= -0.20 for value in returns),
            "p_loss_50": sum(value <= -0.50 for value in returns),
            "p_structural_collapse": sum(episode.structural_collapse for episode in episodes),
        }
        expected_log_growth = sum(math.log1p(value) for value in returns) / n if n else 0.0
        confidence = _clamp(n / self.confidence_sample)
        return CalibratedOutcomeDistribution(
            sample_size=n,
            p_gain_25=shrunk(counts["p_gain_25"], "p_gain_25"),
            p_gain_50=shrunk(counts["p_gain_50"], "p_gain_50"),
            p_2x=shrunk(counts["p_2x"], "p_2x"),
            p_5x=shrunk(counts["p_5x"], "p_5x"),
            p_loss_20=shrunk(counts["p_loss_20"], "p_loss_20"),
            p_loss_50=shrunk(counts["p_loss_50"], "p_loss_50"),
            p_structural_collapse=shrunk(counts["p_structural_collapse"], "p_structural_collapse"),
            expected_log_growth=expected_log_growth,
            confidence=confidence,
        )


@dataclass(frozen=True, slots=True)
class EvidenceSignal:
    name: str
    value: float
    confidence: float
    freshness: float
    provenance_complete: bool
    causal_root: str
    source_id: str

    @property
    def effective_strength(self) -> float:
        if not self.provenance_complete:
            return 0.0
        return max(0.0, float(self.value)) * _clamp(self.confidence) * _clamp(self.freshness)


@dataclass(frozen=True, slots=True)
class EvidenceDeconfliction:
    raw_signal_count: int
    independent_root_count: int
    raw_strength: float
    independent_strength: float
    diversity_factor: float
    missing_provenance: tuple[str, ...]


class EvidenceIndependenceModel:
    """Prevents correlated manifestations from multiplying confirmation."""

    @staticmethod
    def deconflict(signals: Sequence[EvidenceSignal]) -> EvidenceDeconfliction:
        roots: dict[str, float] = {}
        missing: list[str] = []
        raw_strength = 0.0
        for signal in signals:
            raw_strength += signal.effective_strength
            if not signal.provenance_complete:
                missing.append(signal.name)
                continue
            roots[signal.causal_root] = max(roots.get(signal.causal_root, 0.0), signal.effective_strength)
        independent_strength = sum(roots.values())
        diversity = 0.0 if not signals else _clamp(len(roots) / len(signals))
        return EvidenceDeconfliction(
            raw_signal_count=len(signals),
            independent_root_count=len(roots),
            raw_strength=raw_strength,
            independent_strength=independent_strength,
            diversity_factor=diversity,
            missing_provenance=tuple(sorted(missing)),
        )


@dataclass(frozen=True, slots=True)
class FailureArchetypeFeatures:
    liquidity_growth_stall: float = 0.0
    marginal_price_response_decay: float = 0.0
    buyer_quality_deterioration: float = 0.0
    top_heavy_sell_pressure: float = 0.0
    repeat_buyer_decay: float = 0.0
    early_holder_distribution: float = 0.0
    market_cap_to_exit_depth_stress: float = 0.0
    coordination_increase: float = 0.0


@dataclass(frozen=True, slots=True)
class FailureRiskAssessment:
    risk_score: float
    starter_size_multiplier: float
    exit_urgency: float
    hard_veto: bool = False


class FailureArchetypeModel:
    """Compresses clean-looking failure risk without inventing a blanket veto."""

    WEIGHTS = (0.10, 0.16, 0.14, 0.16, 0.10, 0.12, 0.14, 0.08)

    @classmethod
    def assess(cls, features: FailureArchetypeFeatures) -> FailureRiskAssessment:
        values = [_clamp(value) for value in asdict(features).values()]
        score = _clamp(sum(weight * value for weight, value in zip(cls.WEIGHTS, values)))
        starter_multiplier = max(0.20, 1.0 - 0.80 * score)
        exit_urgency = _clamp(0.15 + 0.85 * score)
        return FailureRiskAssessment(score, starter_multiplier, exit_urgency, False)


@dataclass(frozen=True, slots=True)
class ExecutionAttempt:
    quoted_price: float
    available_notional: float
    quote_fresh: bool = True
    route_available: bool = True
    adverse_move_fraction: float = 0.0
    extra_slippage_fraction: float = 0.0


@dataclass(frozen=True, slots=True)
class SimulatedExecution:
    side: str
    requested_notional: float
    filled_notional: float
    unfilled_notional: float
    average_price: float | None
    attempts_used: int
    total_cost_fraction: float
    complete: bool
    failure_reason: str | None


class RealisticPaperExecution:
    """Deterministic quote/partial-fill/requote settlement model."""

    @staticmethod
    def simulate(
        requested_notional: float,
        attempts: Sequence[ExecutionAttempt],
        *,
        side: Literal["buy", "sell"] = "buy",
        fee_fraction: float = 0.0,
        require_complete: bool = True,
    ) -> SimulatedExecution:
        if requested_notional <= 0:
            raise ValueError("requested_notional must be positive")
        if side not in ("buy", "sell"):
            raise ValueError("side must be 'buy' or 'sell'")
        remaining = float(requested_notional)
        filled = 0.0
        weighted_price = 0.0
        direct_cost = 0.0
        used = 0
        failure_reason: str | None = None
        for attempt in attempts:
            if remaining <= 1e-12:
                break
            used += 1
            if not attempt.quote_fresh:
                failure_reason = "stale_quote"
                continue
            if not attempt.route_available:
                failure_reason = "route_unavailable"
                continue
            if attempt.quoted_price <= 0 or attempt.available_notional <= 0:
                failure_reason = "invalid_execution_evidence"
                continue
            fill = min(remaining, float(attempt.available_notional))
            execution_cost = max(0.0, attempt.adverse_move_fraction) + max(0.0, attempt.extra_slippage_fraction)
            direction = 1.0 if side == "buy" else -1.0
            execution_price = attempt.quoted_price * max(0.0, 1.0 + direction * execution_cost)
            filled += fill
            remaining -= fill
            weighted_price += fill * execution_price
            direct_cost += fill * (execution_cost + max(0.0, fee_fraction))
        complete = remaining <= 1e-12
        if not complete and require_complete and failure_reason is None:
            failure_reason = "insufficient_depth_after_requotes"
        average_price = weighted_price / filled if filled > 0 else None
        total_cost_fraction = direct_cost / filled if filled > 0 else 0.0
        return SimulatedExecution(
            side=side,
            requested_notional=requested_notional,
            filled_notional=filled,
            unfilled_notional=max(0.0, remaining),
            average_price=average_price,
            attempts_used=used,
            total_cost_fraction=total_cost_fraction,
            complete=complete,
            failure_reason=failure_reason,
        )


@dataclass(frozen=True, slots=True)
class CashOptionDecision:
    opportunity_rate: float
    cash_option_rate: float
    advantage: float
    deploy: bool


class CapitalOpportunityCostModel:
    """Compares residual-return velocity with the option value of cash."""

    @staticmethod
    def evaluate(
        *,
        expected_residual_return: float,
        expected_holding_seconds: float,
        future_opportunity_arrivals_per_hour: float,
        future_opportunity_expected_return: float,
        probability_future_opportunity_qualifies: float,
        capital_reuse_fraction: float = 1.0,
    ) -> CashOptionDecision:
        if expected_holding_seconds <= 0:
            raise ValueError("expected_holding_seconds must be positive")
        holding_hours = expected_holding_seconds / 3600.0
        opportunity_rate = expected_residual_return / holding_hours
        cash_option_rate = (
            max(0.0, future_opportunity_arrivals_per_hour)
            * max(0.0, future_opportunity_expected_return)
            * _clamp(probability_future_opportunity_qualifies)
            * _clamp(capital_reuse_fraction)
        )
        advantage = opportunity_rate - cash_option_rate
        return CashOptionDecision(opportunity_rate, cash_option_rate, advantage, advantage > 0.0)


@dataclass(frozen=True, slots=True)
class WalletContextEvidence:
    context_key: str
    proven_forward_episodes: int
    copyable_return_on_capital: float
    profit_factor: float
    copyability_rate: float
    manipulation_risk: float
    side_wallet_risk: float
    confidence_delta: float = 0.0
    sizing_delta: float = 0.0
    exit_urgency_delta: float = 0.0


@dataclass(frozen=True, slots=True)
class WalletContextAdjustment:
    eligible: bool
    confidence_delta: float
    sizing_multiplier: float
    exit_urgency_delta: float
    blockers: tuple[str, ...]


class BoundedWalletContextModel:
    """Allows proven wallet context to influence, never authorize, a decision."""

    @staticmethod
    def adjust(expected_context_key: str, evidence: WalletContextEvidence | None) -> WalletContextAdjustment:
        if evidence is None:
            return WalletContextAdjustment(False, 0.0, 1.0, 0.0, ("missing_wallet_context",))
        blockers: list[str] = []
        if evidence.context_key != expected_context_key:
            blockers.append("context_mismatch")
        if evidence.proven_forward_episodes < 30:
            blockers.append("insufficient_forward_episodes")
        if evidence.copyable_return_on_capital <= 0.0:
            blockers.append("copyable_return_not_positive")
        if evidence.profit_factor <= 1.0:
            blockers.append("profit_factor_not_above_one")
        if evidence.copyability_rate < 0.80:
            blockers.append("copyability_rate_below_minimum")
        if evidence.manipulation_risk > 0.10:
            blockers.append("manipulation_risk_too_high")
        if evidence.side_wallet_risk > 0.10:
            blockers.append("side_wallet_risk_too_high")
        if blockers:
            return WalletContextAdjustment(False, 0.0, 1.0, 0.0, tuple(blockers))
        confidence_delta = max(-0.15, min(0.15, evidence.confidence_delta))
        sizing_multiplier = max(0.80, min(1.15, 1.0 + evidence.sizing_delta))
        exit_delta = max(-0.20, min(0.20, evidence.exit_urgency_delta))
        return WalletContextAdjustment(True, confidence_delta, sizing_multiplier, exit_delta, ())


@dataclass(frozen=True, slots=True)
class PolicyOutcome:
    policy_id: str
    stream_id: str
    net_return: float
    drawdown: float
    execution_complete: bool = True


@dataclass(frozen=True, slots=True)
class PolicyScore:
    policy_id: str
    episodes: int
    geometric_growth: float
    mean_return: float
    max_drawdown: float
    execution_completion_rate: float
    score: float


@dataclass(frozen=True, slots=True)
class TournamentDecision:
    winner: str | None
    incumbent: str
    eligible: bool
    improvement_ratio: float | None
    blockers: tuple[str, ...]
    scores: tuple[PolicyScore, ...]


class ProspectivePolicyTournament:
    """Paired same-stream forward comparison for automatic evolution evidence.

    Retrospective work may design challenger policies, but the automatic winner
    path requires paired observations from the same subsequent stream so a policy
    cannot promote itself using the outcome that inspired it.
    """

    def __init__(self, *, min_paired_episodes: int = 20, min_improvement_ratio: float = 1.03) -> None:
        self.min_paired_episodes = int(min_paired_episodes)
        self.min_improvement_ratio = float(min_improvement_ratio)
        self._outcomes: list[PolicyOutcome] = []

    def record(self, outcome: PolicyOutcome) -> None:
        if not math.isfinite(outcome.net_return) or outcome.net_return <= -1.0:
            raise ValueError("net_return must be finite and greater than -1")
        self._outcomes.append(outcome)

    @staticmethod
    def _score(policy_id: str, outcomes: Sequence[PolicyOutcome]) -> PolicyScore:
        returns = [item.net_return for item in outcomes]
        episodes = len(returns)
        geometric_growth = math.exp(sum(math.log1p(value) for value in returns) / episodes) - 1.0
        mean_return = sum(returns) / episodes
        max_drawdown = max((max(0.0, item.drawdown) for item in outcomes), default=0.0)
        completion = sum(item.execution_complete for item in outcomes) / episodes
        score = geometric_growth * completion / (1.0 + max_drawdown)
        return PolicyScore(policy_id, episodes, geometric_growth, mean_return, max_drawdown, completion, score)

    def compare(self, incumbent: str, challengers: Iterable[str]) -> TournamentDecision:
        policies = [incumbent, *list(challengers)]
        by_policy: dict[str, dict[str, PolicyOutcome]] = {policy: {} for policy in policies}
        for item in self._outcomes:
            if item.policy_id in by_policy:
                by_policy[item.policy_id][item.stream_id] = item
        shared = set.intersection(*(set(by_policy[policy]) for policy in policies)) if policies else set()
        blockers: list[str] = []
        if len(shared) < self.min_paired_episodes:
            blockers.append("insufficient_paired_forward_episodes")
        scores = tuple(
            self._score(policy, [by_policy[policy][stream_id] for stream_id in sorted(shared)])
            for policy in policies
            if shared
        )
        if blockers or not scores:
            return TournamentDecision(None, incumbent, False, None, tuple(blockers), scores)
        incumbent_score = next(score for score in scores if score.policy_id == incumbent)
        best = max(scores, key=lambda score: score.score)
        if best.policy_id == incumbent:
            return TournamentDecision(None, incumbent, False, 1.0, ("incumbent_not_beaten",), scores)
        if incumbent_score.score > 0.0:
            ratio = best.score / incumbent_score.score
        else:
            ratio = math.inf if best.score > 0.0 else 1.0
        if ratio < self.min_improvement_ratio:
            return TournamentDecision(None, incumbent, False, ratio, ("improvement_below_minimum",), scores)
        return TournamentDecision(best.policy_id, incumbent, True, ratio, (), scores)


@dataclass(frozen=True, slots=True)
class StrategyEpoch:
    sequence: int
    strategy_version: str
    parent_version: str | None
    created_at: str
    fingerprint: str
    rationale: str
    evidence_refs: tuple[str, ...]
    protected_change: bool
    config: Mapping[str, Any]


class ContinuousStrategyEvolution:
    """Append-only continuous refinement with immutable authority boundaries.

    Ordinary research parameters can evolve without a permanent freeze or
    cooldown. Current protected strategy constraints can also change, but only
    through the explicit protected-change path with recorded evidence/test refs.
    The paper/live-money authority boundary is never mutable here.
    """

    def __init__(self, initial_config: Mapping[str, Any], *, version: str = "v5.2") -> None:
        config = dict(DEFAULT_AUTHORITY_BOUNDARY)
        config.update(CURRENT_PROTECTED_STRATEGY)
        config.update(dict(initial_config))
        self._assert_authority(config)
        self._epochs: list[StrategyEpoch] = []
        self._append(version, None, "initial_strategy_epoch", config, evidence_refs=(), protected_change=False)

    @staticmethod
    def _assert_authority(config: Mapping[str, Any]) -> None:
        for key, expected in DEFAULT_AUTHORITY_BOUNDARY.items():
            if config.get(key) != expected:
                raise ValueError(f"immutable authority key cannot be changed: {key}")

    @staticmethod
    def _fingerprint(config: Mapping[str, Any]) -> str:
        raw = json.dumps(dict(config), sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(raw.encode()).hexdigest()

    def _append(
        self,
        version: str,
        parent_version: str | None,
        rationale: str,
        config: Mapping[str, Any],
        *,
        evidence_refs: Sequence[str],
        protected_change: bool,
    ) -> StrategyEpoch:
        epoch = StrategyEpoch(
            sequence=len(self._epochs) + 1,
            strategy_version=version,
            parent_version=parent_version,
            created_at=_utcnow().isoformat(),
            fingerprint=self._fingerprint(config),
            rationale=rationale,
            evidence_refs=tuple(evidence_refs),
            protected_change=protected_change,
            config=dict(config),
        )
        self._epochs.append(epoch)
        return epoch

    @property
    def current(self) -> StrategyEpoch:
        return self._epochs[-1]

    @property
    def history(self) -> tuple[StrategyEpoch, ...]:
        return tuple(self._epochs)

    def evolve(
        self,
        changes: Mapping[str, Any],
        *,
        rationale: str,
        version: str | None = None,
        evidence_refs: Sequence[str] = (),
    ) -> StrategyEpoch:
        if not rationale.strip():
            raise ValueError("rationale is required")
        immutable = IMMUTABLE_AUTHORITY_KEYS.intersection(changes)
        if immutable:
            raise ValueError(f"immutable authority keys are not evolvable: {sorted(immutable)}")
        protected = PROTECTED_STRATEGY_KEYS.intersection(changes)
        if protected:
            raise ValueError(
                "protected strategy keys require evolve_protected_strategy_constraint: "
                f"{sorted(protected)}"
            )
        config = dict(self.current.config)
        config.update(dict(changes))
        self._assert_authority(config)
        next_version = version or f"v5.2.e{self.current.sequence}"
        return self._append(
            next_version,
            self.current.strategy_version,
            rationale,
            config,
            evidence_refs=evidence_refs,
            protected_change=False,
        )

    def evolve_protected_strategy_constraint(
        self,
        changes: Mapping[str, Any],
        *,
        rationale: str,
        validation_refs: Sequence[str],
        version: str | None = None,
    ) -> StrategyEpoch:
        """Explicit governed path for changing a current strategy constraint."""
        if not rationale.strip():
            raise ValueError("rationale is required")
        if not validation_refs or any(not str(ref).strip() for ref in validation_refs):
            raise ValueError("at least one validation/test reference is required")
        immutable = IMMUTABLE_AUTHORITY_KEYS.intersection(changes)
        if immutable:
            raise ValueError(f"immutable authority keys cannot be changed: {sorted(immutable)}")
        if not changes:
            raise ValueError("at least one strategy change is required")
        unknown = set(changes) - PROTECTED_STRATEGY_KEYS
        if unknown:
            raise ValueError(
                "protected strategy path accepts only protected strategy keys: "
                f"{sorted(unknown)}"
            )
        config = dict(self.current.config)
        config.update(dict(changes))
        self._assert_authority(config)
        next_version = version or f"v5.2.p{self.current.sequence}"
        return self._append(
            next_version,
            self.current.strategy_version,
            rationale,
            config,
            evidence_refs=validation_refs,
            protected_change=True,
        )

    def evolve_from_tournament(
        self,
        decision: TournamentDecision,
        policy_configs: Mapping[str, Mapping[str, Any]],
        *,
        rationale_prefix: str = "prospective_policy_tournament",
    ) -> StrategyEpoch | None:
        if not decision.eligible or decision.winner is None:
            return None
        changes = policy_configs.get(decision.winner)
        if changes is None:
            raise KeyError(f"missing policy config for winner {decision.winner}")
        return self.evolve(
            changes,
            rationale=f"{rationale_prefix}:{decision.winner}:ratio={decision.improvement_ratio}",
            evidence_refs=(f"paired_policy:{decision.winner}",),
        )


@dataclass(frozen=True, slots=True)
class RefinedDecisionContext:
    calibrated_expected_log_growth: float
    calibration_confidence: float
    evidence: EvidenceDeconfliction
    failure: FailureRiskAssessment
    wallet: WalletContextAdjustment
    cash_option: CashOptionDecision


@dataclass(frozen=True, slots=True)
class RefinedDecision:
    confidence: float
    sizing_multiplier: float
    exit_urgency: float
    deploy_capital: bool
    ranking_score: float
    blockers: tuple[str, ...] = field(default_factory=tuple)


class V52ContinuousRefinementEngine:
    """Combines Batch 8 evidence without bypassing the active strategy's gates."""

    @staticmethod
    def refine(
        *,
        base_confidence: float,
        context: RefinedDecisionContext,
        hard_gate_passed: bool,
    ) -> RefinedDecision:
        blockers: list[str] = []
        if not hard_gate_passed:
            blockers.append("existing_v52_hard_gate_failed")
        if context.evidence.missing_provenance:
            blockers.append("missing_signal_provenance")
        confidence = _clamp(
            base_confidence * (0.5 + 0.5 * context.evidence.diversity_factor)
            + context.wallet.confidence_delta
        )
        sizing_multiplier = (
            context.failure.starter_size_multiplier
            * context.wallet.sizing_multiplier
            * (0.5 + 0.5 * context.calibration_confidence)
        )
        exit_urgency = _clamp(context.failure.exit_urgency + context.wallet.exit_urgency_delta)
        ranking_score = (
            context.calibrated_expected_log_growth
            * confidence
            * max(0.0, context.cash_option.advantage + 1.0)
        )
        deploy = hard_gate_passed and not blockers and context.cash_option.deploy and ranking_score > 0.0
        return RefinedDecision(
            confidence=confidence,
            sizing_multiplier=max(0.0, sizing_multiplier),
            exit_urgency=exit_urgency,
            deploy_capital=deploy,
            ranking_score=ranking_score,
            blockers=tuple(blockers),
        )
