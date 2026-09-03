from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from statistics import mean, median
from typing import Iterable, Mapping, Sequence


FINAL_STRATEGY_VERSION = "roi-convergence-v4.0-profit-first-entity-1"
PARENT_RESEARCH_VERSION = "roi-convergence-v4.0-profit-first-entity-research-1"
STARTING_PAPER_NAV_USD = 500.0
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
ACTIVE_OLD_COHORT_MUTATION_ALLOWED = False
HISTORICAL_PROMOTION_AUTHORITY = False
SIGNAL_DECAY_DELAYS_SECONDS = (1, 2, 5, 10, 20, 30, 60)
UNIFIED_LANE = "unified_profit_maximizer"


class FinalLane(str, Enum):
    CLEAN_SCOUT = "clean_scout_alpha"
    ELITE_WALLET_CONTINUATION = "elite_wallet_continuation"
    CREATOR_INSIDER_CONTINUATION = "creator_insider_continuation"
    ENTITY_FLOW_MOMENTUM = "entity_flow_momentum"


class MarketRegime(str, Enum):
    WEAK = "weak_or_deteriorating"
    NEUTRAL = "neutral"
    HIGH_SPECULATION = "high_speculation"
    BROAD_MANIA = "broad_mania"


class FinalDecision(str, Enum):
    SHADOW = "shadow"
    PAPER_ENTER = "paper_enter"
    WAIT = "wait"
    REJECT = "reject"


@dataclass(frozen=True)
class FinalOpportunity:
    token: str
    source_signature: str
    observed_at: str
    trigger_entity: str
    creator_entity: str | None
    independent_confirmation_count: int
    creator_linked_trigger: bool
    creator_flow_state: str
    chase_fraction: float
    signal_to_entry_seconds: float
    round_trip_cost_fraction: float
    entry_executable: bool
    exit_executable: bool
    regime: MarketRegime = MarketRegime.NEUTRAL
    independent_demand_strength: float = 0.0
    early_buyer_exit_fraction: float = 0.0
    soft_risk_flags: frozenset[str] = frozenset()
    hard_risk_flags: frozenset[str] = frozenset()


@dataclass(frozen=True)
class FinalLaneContext:
    lane: FinalLane
    regime: MarketRegime
    creator_flow_state: str
    confirmation_bin: str
    chase_bin: str
    latency_bin: str
    early_exit_bin: str
    soft_risk_bin: str
    creator_linked_trigger: bool

    @property
    def bucket_key(self) -> tuple[str, ...]:
        return (
            self.lane.value,
            self.regime.value,
            self.creator_flow_state,
            self.confirmation_bin,
            self.chase_bin,
            self.latency_bin,
            self.early_exit_bin,
            self.soft_risk_bin,
            "creator" if self.creator_linked_trigger else "independent",
        )


@dataclass(frozen=True)
class FinalForwardOutcome:
    context: FinalLaneContext
    net_return: float
    source_signature: str
    release_commit: str
    observed_at: str
    signal_to_entry_seconds: float
    position_fraction: float
    evidence_phase: str = "forward"
    maximum_adverse_excursion: float = 0.0
    maximum_favorable_excursion: float = 0.0
    exit_reason: str = "unknown"


@dataclass(frozen=True)
class SizingConstraints:
    liquidity_headroom_fraction: float = 1.0
    entity_concentration_headroom_fraction: float = 1.0
    correlation_headroom_fraction: float = 1.0
    confidence_multiplier: float = 1.0

    def cap(self, raw_fraction: float) -> float:
        if raw_fraction <= 0.0:
            return 0.0
        values = (
            raw_fraction,
            max(0.0, self.liquidity_headroom_fraction),
            max(0.0, self.entity_concentration_headroom_fraction),
            max(0.0, self.correlation_headroom_fraction),
            raw_fraction * max(0.0, min(1.0, self.confidence_multiplier)),
        )
        return max(0.0, min(values))


@dataclass(frozen=True)
class FinalPrediction:
    lane: FinalLane
    forward_sample_count: int
    calibration_sample_count: int
    source_level: str
    mean_net_return: float | None
    median_net_return: float | None
    hit_rate: float | None
    best_raw_position_fraction: float
    constrained_position_fraction: float
    best_expected_log_growth: float | None
    expected_log_growth_by_fraction: Mapping[float, float]


@dataclass(frozen=True)
class FinalStrategyDecision:
    strategy_version: str
    decision: FinalDecision
    selected_lane: FinalLane | None
    position_fraction: float
    predicted_mean_net_return: float | None
    predicted_log_growth: float | None
    forward_sample_count: int
    blockers: tuple[str, ...] = ()
    paper_only: bool = True
    live_money_authority: bool = False
    signing_available: bool = False
    transaction_submission_available: bool = False


@dataclass(frozen=True)
class FinalPolicy:
    max_chase_fraction: float = 0.15
    max_certified_observation_latency_seconds: float = 20.0
    min_forward_outcomes_for_selection: int = 30
    min_exact_bucket_samples: int = 20
    min_lane_regime_samples: int = 30
    creator_min_independent_confirmations: int = 1
    position_fraction_grid: tuple[float, ...] = (0.005, 0.01, 0.02, 0.05, 0.10, 0.15, 0.20)
    structural_hard_stops: frozenset[str] = frozenset(
        {
            "sell_route_unavailable",
            "transfer_restricted",
            "liquidity_unexitable",
            "entry_quote_unavailable",
            "exit_quote_unavailable",
            "authority_can_block_transfer_or_exit",
            "linked_entity_can_remove_required_liquidity",
        }
    )


class WalkForwardLedger:
    """Keeps calibration evidence separate from forward promotion authority."""

    def __init__(self, outcomes: Iterable[FinalForwardOutcome] = ()) -> None:
        self._outcomes = list(outcomes)

    def add(self, outcome: FinalForwardOutcome) -> None:
        self._outcomes.append(outcome)

    @property
    def outcomes(self) -> tuple[FinalForwardOutcome, ...]:
        return tuple(self._outcomes)

    @staticmethod
    def _log_growth(returns: Sequence[float], fraction: float) -> float:
        if not returns or fraction <= 0:
            return 0.0
        values: list[float] = []
        for result in returns:
            terminal = 1.0 + fraction * result
            if terminal <= 0:
                return float("-inf")
            values.append(math.log(terminal))
        return mean(values)

    @staticmethod
    def _matches(context: FinalLaneContext, item: FinalForwardOutcome) -> bool:
        return item.context.bucket_key == context.bucket_key

    def _forward_cohort(self, context: FinalLaneContext, policy: FinalPolicy) -> tuple[list[FinalForwardOutcome], str]:
        forward = [item for item in self._outcomes if item.evidence_phase == "forward"]
        exact = [item for item in forward if self._matches(context, item)]
        if len(exact) >= policy.min_exact_bucket_samples:
            return exact, "forward_exact_feature_bucket"
        lane_regime = [
            item for item in forward
            if item.context.lane == context.lane and item.context.regime == context.regime
        ]
        if len(lane_regime) >= policy.min_lane_regime_samples:
            return lane_regime, "forward_lane_regime"
        lane = [item for item in forward if item.context.lane == context.lane]
        return lane, "forward_lane" if lane else "none"

    def predict(
        self,
        context: FinalLaneContext,
        policy: FinalPolicy,
        constraints: SizingConstraints,
    ) -> FinalPrediction:
        cohort, source = self._forward_cohort(context, policy)
        returns = [item.net_return for item in cohort]
        calibration_count = sum(
            1 for item in self._outcomes
            if item.evidence_phase != "forward" and item.context.lane == context.lane
        )
        if not returns:
            return FinalPrediction(
                lane=context.lane,
                forward_sample_count=0,
                calibration_sample_count=calibration_count,
                source_level=source,
                mean_net_return=None,
                median_net_return=None,
                hit_rate=None,
                best_raw_position_fraction=0.0,
                constrained_position_fraction=0.0,
                best_expected_log_growth=None,
                expected_log_growth_by_fraction={},
            )
        growth = {fraction: self._log_growth(returns, fraction) for fraction in policy.position_fraction_grid}
        raw_fraction, best_growth = max(growth.items(), key=lambda item: item[1])
        if not math.isfinite(best_growth) or best_growth <= 0:
            raw_fraction = 0.0
        constrained = constraints.cap(raw_fraction)
        return FinalPrediction(
            lane=context.lane,
            forward_sample_count=len(returns),
            calibration_sample_count=calibration_count,
            source_level=source,
            mean_net_return=mean(returns),
            median_net_return=median(returns),
            hit_rate=sum(value > 0 for value in returns) / len(returns),
            best_raw_position_fraction=raw_fraction,
            constrained_position_fraction=constrained,
            best_expected_log_growth=best_growth,
            expected_log_growth_by_fraction=growth,
        )


@dataclass(frozen=True)
class ExitFeatures:
    creator_distribution: bool = False
    linked_entity_distribution: bool = False
    early_holder_exit_fraction: float = 0.0
    successful_scout_exit: bool = False
    independent_flow_decelerating: bool = False
    buy_sell_flow_reversal: bool = False
    quote_deterioration_fraction: float = 0.0
    executable_depth_deterioration_fraction: float = 0.0
    liquidity_withdrawal_fraction: float = 0.0
    exit_slippage_increase_fraction: float = 0.0
    stagnation_seconds: float = 0.0
    downside_fraction: float = 0.0


@dataclass(frozen=True)
class ExitSignal:
    should_exit: bool
    urgency_score: float
    reasons: tuple[str, ...]


class ExitAlphaModel:
    """Research exit model; emits paper signals only and never submits a transaction."""

    def evaluate(self, features: ExitFeatures) -> ExitSignal:
        score = 0.0
        reasons: list[str] = []
        if features.creator_distribution:
            score += 3.0
            reasons.append("creator_distribution")
        if features.linked_entity_distribution:
            score += 2.5
            reasons.append("linked_entity_distribution")
        if features.early_holder_exit_fraction >= 0.20:
            score += 1.5
            reasons.append("early_holder_distribution")
        if features.successful_scout_exit:
            score += 1.0
            reasons.append("successful_scout_exit")
        if features.independent_flow_decelerating:
            score += 1.0
            reasons.append("independent_flow_deceleration")
        if features.buy_sell_flow_reversal:
            score += 1.5
            reasons.append("buy_sell_flow_reversal")
        if features.quote_deterioration_fraction >= 0.05:
            score += 1.0
            reasons.append("quote_deterioration")
        if features.executable_depth_deterioration_fraction >= 0.20:
            score += 1.5
            reasons.append("depth_deterioration")
        if features.liquidity_withdrawal_fraction >= 0.20:
            score += 2.0
            reasons.append("liquidity_withdrawal")
        if features.exit_slippage_increase_fraction >= 0.05:
            score += 1.0
            reasons.append("exit_slippage_deterioration")
        if features.stagnation_seconds >= 300:
            score += 0.5
            reasons.append("stagnation")
        if features.downside_fraction >= 0.20:
            score += 2.0
            reasons.append("downside_stop")
        return ExitSignal(should_exit=score >= 2.5, urgency_score=score, reasons=tuple(reasons))


@dataclass(frozen=True)
class SignalDecayPoint:
    delay_seconds: int
    sample_count: int
    mean_net_return: float | None
    median_net_return: float | None
    expected_log_growth_at_one_percent: float | None


class SignalDecayCurve:
    @staticmethod
    def _bucket(delay: float) -> int:
        for value in SIGNAL_DECAY_DELAYS_SECONDS:
            if delay <= value:
                return value
        return SIGNAL_DECAY_DELAYS_SECONDS[-1]

    @classmethod
    def from_outcomes(cls, outcomes: Iterable[FinalForwardOutcome]) -> tuple[SignalDecayPoint, ...]:
        buckets: dict[int, list[float]] = {value: [] for value in SIGNAL_DECAY_DELAYS_SECONDS}
        for item in outcomes:
            if item.evidence_phase != "forward":
                continue
            buckets[cls._bucket(item.signal_to_entry_seconds)].append(item.net_return)
        result: list[SignalDecayPoint] = []
        for delay in SIGNAL_DECAY_DELAYS_SECONDS:
            values = buckets[delay]
            growth = None
            if values:
                growth = mean(math.log(max(1e-12, 1.0 + 0.01 * value)) for value in values)
            result.append(
                SignalDecayPoint(
                    delay_seconds=delay,
                    sample_count=len(values),
                    mean_net_return=mean(values) if values else None,
                    median_net_return=median(values) if values else None,
                    expected_log_growth_at_one_percent=growth,
                )
            )
        return tuple(result)


class FinalProfitFirstStrategy:
    def __init__(self, *, ledger: WalkForwardLedger | None = None, policy: FinalPolicy | None = None) -> None:
        self.ledger = ledger or WalkForwardLedger()
        self.policy = policy or FinalPolicy()
        self.exit_model = ExitAlphaModel()

    @staticmethod
    def _confirmation_bin(value: int) -> str:
        return "0" if value <= 0 else "1" if value == 1 else "2-3" if value <= 3 else "4+"

    @staticmethod
    def _chase_bin(value: float) -> str:
        return "<=5%" if value <= 0.05 else "5-10%" if value <= 0.10 else "10-15%" if value <= 0.15 else ">15%"

    @staticmethod
    def _latency_bin(value: float) -> str:
        return "<=2s" if value <= 2 else "2-5s" if value <= 5 else "5-10s" if value <= 10 else "10-20s" if value <= 20 else ">20s"

    @staticmethod
    def _early_exit_bin(value: float) -> str:
        return "<=5%" if value <= 0.05 else "5-20%" if value <= 0.20 else ">20%"

    @staticmethod
    def _risk_bin(flags: frozenset[str]) -> str:
        return "0" if not flags else "1" if len(flags) == 1 else "2+"

    def context(self, opportunity: FinalOpportunity, lane: FinalLane) -> FinalLaneContext:
        return FinalLaneContext(
            lane=lane,
            regime=opportunity.regime,
            creator_flow_state=opportunity.creator_flow_state,
            confirmation_bin=self._confirmation_bin(opportunity.independent_confirmation_count),
            chase_bin=self._chase_bin(opportunity.chase_fraction),
            latency_bin=self._latency_bin(opportunity.signal_to_entry_seconds),
            early_exit_bin=self._early_exit_bin(opportunity.early_buyer_exit_fraction),
            soft_risk_bin=self._risk_bin(opportunity.soft_risk_flags),
            creator_linked_trigger=opportunity.creator_linked_trigger,
        )

    def structural_blockers(self, opportunity: FinalOpportunity) -> tuple[str, ...]:
        blockers: list[str] = []
        if not opportunity.entry_executable:
            blockers.append("entry_quote_unavailable")
        if not opportunity.exit_executable:
            blockers.append("exit_quote_unavailable")
        blockers.extend(sorted(opportunity.hard_risk_flags & self.policy.structural_hard_stops))
        if opportunity.chase_fraction > self.policy.max_chase_fraction:
            blockers.append("chase_above_existing_limit")
        if opportunity.signal_to_entry_seconds > self.policy.max_certified_observation_latency_seconds:
            blockers.append("observation_or_processing_latency_above_existing_limit")
        return tuple(dict.fromkeys(blockers))

    def lane_eligibility(self, opportunity: FinalOpportunity, lane: FinalLane) -> tuple[bool, tuple[str, ...]]:
        if lane == FinalLane.CLEAN_SCOUT:
            return (not opportunity.creator_linked_trigger, () if not opportunity.creator_linked_trigger else ("creator_linked_not_clean_scout",))
        if lane == FinalLane.ELITE_WALLET_CONTINUATION:
            return True, ()
        if lane == FinalLane.CREATOR_INSIDER_CONTINUATION:
            if not opportunity.creator_linked_trigger:
                return False, ("trigger_not_creator_linked",)
            if opportunity.independent_confirmation_count < self.policy.creator_min_independent_confirmations:
                return False, ("creator_lane_requires_independent_external_demand",)
            return True, ()
        if lane == FinalLane.ENTITY_FLOW_MOMENTUM:
            if opportunity.independent_confirmation_count < 1:
                return False, ("requires_independent_economic_entity_confirmation",)
            if opportunity.independent_demand_strength <= 0:
                return False, ("independent_demand_not_accelerating",)
            if opportunity.creator_flow_state == "distributing":
                return False, ("creator_distribution_against_entity_flow",)
            return True, ()
        return False, ("unknown_lane",)

    def evaluate_lane(
        self,
        opportunity: FinalOpportunity,
        lane: FinalLane,
        constraints: SizingConstraints | None = None,
    ) -> FinalStrategyDecision:
        structural = self.structural_blockers(opportunity)
        context = self.context(opportunity, lane)
        if structural:
            return FinalStrategyDecision(
                strategy_version=FINAL_STRATEGY_VERSION,
                decision=FinalDecision.REJECT,
                selected_lane=lane,
                position_fraction=0.0,
                predicted_mean_net_return=None,
                predicted_log_growth=None,
                forward_sample_count=0,
                blockers=structural,
            )
        eligible, reasons = self.lane_eligibility(opportunity, lane)
        if not eligible:
            return FinalStrategyDecision(
                strategy_version=FINAL_STRATEGY_VERSION,
                decision=FinalDecision.WAIT,
                selected_lane=lane,
                position_fraction=0.0,
                predicted_mean_net_return=None,
                predicted_log_growth=None,
                forward_sample_count=0,
                blockers=reasons,
            )
        prediction = self.ledger.predict(context, self.policy, constraints or SizingConstraints())
        if prediction.forward_sample_count < self.policy.min_forward_outcomes_for_selection:
            return FinalStrategyDecision(
                strategy_version=FINAL_STRATEGY_VERSION,
                decision=FinalDecision.SHADOW,
                selected_lane=lane,
                position_fraction=0.0,
                predicted_mean_net_return=prediction.mean_net_return,
                predicted_log_growth=prediction.best_expected_log_growth,
                forward_sample_count=prediction.forward_sample_count,
                blockers=("insufficient_final_version_forward_evidence",),
            )
        if (
            prediction.mean_net_return is None
            or prediction.mean_net_return <= 0
            or prediction.best_expected_log_growth is None
            or prediction.best_expected_log_growth <= 0
            or prediction.constrained_position_fraction <= 0
        ):
            return FinalStrategyDecision(
                strategy_version=FINAL_STRATEGY_VERSION,
                decision=FinalDecision.WAIT,
                selected_lane=lane,
                position_fraction=0.0,
                predicted_mean_net_return=prediction.mean_net_return,
                predicted_log_growth=prediction.best_expected_log_growth,
                forward_sample_count=prediction.forward_sample_count,
                blockers=("nonpositive_forward_residual_geometric_edge",),
            )
        return FinalStrategyDecision(
            strategy_version=FINAL_STRATEGY_VERSION,
            decision=FinalDecision.PAPER_ENTER,
            selected_lane=lane,
            position_fraction=prediction.constrained_position_fraction,
            predicted_mean_net_return=prediction.mean_net_return,
            predicted_log_growth=prediction.best_expected_log_growth,
            forward_sample_count=prediction.forward_sample_count,
        )

    def evaluate_all(
        self,
        opportunity: FinalOpportunity,
        constraints: SizingConstraints | None = None,
    ) -> dict[str, FinalStrategyDecision]:
        decisions = {lane.value: self.evaluate_lane(opportunity, lane, constraints) for lane in FinalLane}
        candidates = [value for value in decisions.values() if value.decision == FinalDecision.PAPER_ENTER]
        if candidates:
            best = max(candidates, key=lambda row: (row.predicted_log_growth or float("-inf"), row.predicted_mean_net_return or float("-inf")))
            decisions[UNIFIED_LANE] = best
        else:
            forward_counts = [value.forward_sample_count for value in decisions.values()]
            decisions[UNIFIED_LANE] = FinalStrategyDecision(
                strategy_version=FINAL_STRATEGY_VERSION,
                decision=FinalDecision.SHADOW,
                selected_lane=None,
                position_fraction=0.0,
                predicted_mean_net_return=None,
                predicted_log_growth=None,
                forward_sample_count=max(forward_counts, default=0),
                blockers=("unified_profit_maximizer_has_no_authority_until_forward_gate",),
            )
        return decisions

    def status(self) -> dict[str, object]:
        return {
            "strategy_version": FINAL_STRATEGY_VERSION,
            "parent_research_version": PARENT_RESEARCH_VERSION,
            "objective": "maximize_out_of_sample_compounded_net_return_for_500_usd_paper_portfolio",
            "paper_starting_nav_usd": STARTING_PAPER_NAV_USD,
            "paper_only": PAPER_ONLY,
            "live_money_authority": LIVE_MONEY_AUTHORITY,
            "signing_available": SIGNING_AVAILABLE,
            "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
            "historical_promotion_authority": HISTORICAL_PROMOTION_AUTHORITY,
            "active_old_cohort_mutation_allowed": ACTIVE_OLD_COHORT_MUTATION_ALLOWED,
            "primary_target": "copyable_executable_residual_return",
            "creator_association_is_automatic_veto": False,
            "five_competing_lanes": [lane.value for lane in FinalLane] + [UNIFIED_LANE],
            "entity_flow_momentum_first_class": True,
            "signal_decay_delays_seconds": list(SIGNAL_DECAY_DELAYS_SECONDS),
            "exit_alpha_is_separate_predictive_model": True,
            "position_fraction_grid": list(self.policy.position_fraction_grid),
            "position_sizing_objective": "maximize_E_log_1_plus_fR_subject_to_liquidity_entity_correlation_confidence",
            "regime_conditioned_forward_evidence": True,
            "max_chase_fraction_unchanged": self.policy.max_chase_fraction,
            "latency_certification_threshold_unchanged_seconds": self.policy.max_certified_observation_latency_seconds,
            "unified_authority_requires_final_version_forward_samples": self.policy.min_forward_outcomes_for_selection,
        }


@dataclass(frozen=True)
class RobustnessReport:
    sample_count: int
    compounded_bankroll_return: float
    geometric_mean_return: float
    hit_rate: float
    profit_factor: float | None
    average_winner: float | None
    average_loser: float | None
    maximum_drawdown: float
    worst_trade: float | None
    expected_shortfall_5pct: float | None
    capital_utilization: float
    top_1pct_removed_return: float | None
    top_5pct_removed_return: float | None
    top_10pct_removed_return: float | None
    winner_concentrated: bool


def _compounded(outcomes: Sequence[FinalForwardOutcome]) -> float:
    bankroll = 1.0
    for row in outcomes:
        bankroll *= max(0.0, 1.0 + row.position_fraction * row.net_return)
    return bankroll - 1.0


def _removed(outcomes: Sequence[FinalForwardOutcome], fraction: float) -> float | None:
    if not outcomes:
        return None
    remove = max(1, math.ceil(len(outcomes) * fraction))
    kept = sorted(outcomes, key=lambda row: row.net_return, reverse=True)[remove:]
    return _compounded(kept) if kept else 0.0


def build_robustness_report(outcomes: Sequence[FinalForwardOutcome]) -> RobustnessReport:
    if not outcomes:
        return RobustnessReport(0, 0.0, 0.0, 0.0, None, None, None, 0.0, None, None, 0.0, None, None, None, False)
    returns = [row.net_return for row in outcomes]
    wins = [value for value in returns if value > 0]
    losses = [value for value in returns if value < 0]
    bankroll = 1.0
    peak = 1.0
    max_dd = 0.0
    logs: list[float] = []
    for row in outcomes:
        terminal = max(1e-12, 1.0 + row.position_fraction * row.net_return)
        bankroll *= terminal
        logs.append(math.log(terminal))
        peak = max(peak, bankroll)
        max_dd = max(max_dd, 1.0 - bankroll / peak)
    tail_count = max(1, math.ceil(len(returns) * 0.05))
    expected_shortfall = mean(sorted(returns)[:tail_count])
    top1 = _removed(outcomes, 0.01)
    top5 = _removed(outcomes, 0.05)
    top10 = _removed(outcomes, 0.10)
    full = bankroll - 1.0
    concentrated = full > 0 and top10 is not None and top10 <= 0
    return RobustnessReport(
        sample_count=len(outcomes),
        compounded_bankroll_return=full,
        geometric_mean_return=math.exp(mean(logs)) - 1.0,
        hit_rate=len(wins) / len(returns),
        profit_factor=(sum(wins) / abs(sum(losses))) if losses else None,
        average_winner=mean(wins) if wins else None,
        average_loser=mean(losses) if losses else None,
        maximum_drawdown=max_dd,
        worst_trade=min(returns),
        expected_shortfall_5pct=expected_shortfall,
        capital_utilization=mean(row.position_fraction for row in outcomes),
        top_1pct_removed_return=top1,
        top_5pct_removed_return=top5,
        top_10pct_removed_return=top10,
        winner_concentrated=concentrated,
    )


__all__ = [
    "ACTIVE_OLD_COHORT_MUTATION_ALLOWED",
    "ExitAlphaModel",
    "ExitFeatures",
    "ExitSignal",
    "FINAL_STRATEGY_VERSION",
    "FinalDecision",
    "FinalForwardOutcome",
    "FinalLane",
    "FinalLaneContext",
    "FinalOpportunity",
    "FinalPolicy",
    "FinalPrediction",
    "FinalProfitFirstStrategy",
    "FinalStrategyDecision",
    "HISTORICAL_PROMOTION_AUTHORITY",
    "LIVE_MONEY_AUTHORITY",
    "MarketRegime",
    "PAPER_ONLY",
    "PARENT_RESEARCH_VERSION",
    "RobustnessReport",
    "SIGNAL_DECAY_DELAYS_SECONDS",
    "SIGNING_AVAILABLE",
    "STARTING_PAPER_NAV_USD",
    "SignalDecayCurve",
    "SignalDecayPoint",
    "SizingConstraints",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "UNIFIED_LANE",
    "WalkForwardLedger",
    "build_robustness_report",
]
