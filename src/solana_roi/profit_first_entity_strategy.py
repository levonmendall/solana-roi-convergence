from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from statistics import mean, median
from typing import Iterable, Mapping, Sequence


STRATEGY_VERSION = "roi-convergence-v4.0-profit-first-entity-research-1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
ACTIVE_V3_1_COHORT_MUTATION_ALLOWED = False


class Lane(str, Enum):
    CLEAN_SCOUT = "clean_scout"
    CREATOR_CONTINUATION = "creator_aware_continuation"
    SMART_MONEY_SWARM = "smart_money_swarm"
    UNFILTERED_ELITE = "unfiltered_elite_wallet"


class Decision(str, Enum):
    SHADOW = "shadow"
    ENTER = "enter"
    WAIT = "wait"
    REJECT = "reject"


@dataclass(frozen=True)
class EntityLink:
    left: str
    right: str
    reason: str


class EntityGraph:
    """Deterministically collapse addresses known to share one economic entity.

    The graph never invents relationships. Only caller-supplied links are collapsed.
    This makes five funded/controlled side wallets count as one confirmation rather
    than five while preserving genuinely independent wallets.
    """

    def __init__(self, links: Iterable[EntityLink] = ()) -> None:
        self._parent: dict[str, str] = {}
        for link in links:
            self.link(link.left, link.right)

    def _ensure(self, address: str) -> None:
        if address not in self._parent:
            self._parent[address] = address

    def _find(self, address: str) -> str:
        self._ensure(address)
        parent = self._parent[address]
        if parent != address:
            self._parent[address] = self._find(parent)
        return self._parent[address]

    def link(self, left: str, right: str) -> None:
        if not left or not right:
            return
        a = self._find(left)
        b = self._find(right)
        if a == b:
            return
        # Stable root independent of link order makes persisted telemetry reproducible.
        root, child = sorted((a, b))
        self._parent[child] = root

    def entity_id(self, address: str | None) -> str | None:
        if not address:
            return None
        return f"entity:{self._find(address)}"

    def distinct_entities(self, addresses: Iterable[str]) -> tuple[str, ...]:
        values = {self.entity_id(address) for address in addresses if address}
        return tuple(sorted(value for value in values if value is not None))


@dataclass(frozen=True)
class OpportunitySnapshot:
    token: str
    trigger_wallet: str
    creator_wallet: str | None = None
    confirming_wallets: tuple[str, ...] = ()
    chase_fraction: float = 0.0
    observation_lag_seconds: float = 0.0
    round_trip_cost_fraction: float = 0.0
    entry_executable: bool = True
    exit_executable: bool = True
    creator_cluster_net_flow_fraction: float = 0.0
    early_buyer_exit_fraction: float = 0.0
    independent_buy_volume_fraction: float = 0.0
    linked_holder_concentration: float = 0.0
    hard_risk_flags: frozenset[str] = frozenset()
    soft_risk_flags: frozenset[str] = frozenset()


@dataclass(frozen=True)
class OpportunityContext:
    lane: Lane
    trigger_entity: str
    creator_entity: str | None
    confirmation_entities: tuple[str, ...]
    independent_confirmation_count: int
    creator_linked_trigger: bool
    creator_flow_state: str
    confirmation_bin: str
    chase_bin: str
    early_exit_bin: str
    soft_risk_bin: str

    @property
    def bucket_key(self) -> tuple[str, ...]:
        return (
            self.lane.value,
            self.confirmation_bin,
            self.chase_bin,
            self.creator_flow_state,
            self.early_exit_bin,
            self.soft_risk_bin,
        )


@dataclass(frozen=True)
class ForwardOutcome:
    context: OpportunityContext
    net_return: float
    maximum_adverse_excursion: float = 0.0
    maximum_favorable_excursion: float = 0.0
    exit_reason: str = "unknown"


@dataclass(frozen=True)
class LanePrediction:
    lane: Lane
    sample_count: int
    source_level: str
    mean_net_return: float | None
    median_net_return: float | None
    hit_rate: float | None
    loss_rate: float | None
    best_position_fraction: float
    best_expected_log_growth: float | None
    expected_log_growth_by_fraction: Mapping[float, float]


@dataclass(frozen=True)
class StrategyDecision:
    strategy_version: str
    decision: Decision
    selected_lane: Lane | None
    position_fraction: float
    predicted_mean_net_return: float | None
    predicted_log_growth: float | None
    independent_confirmation_count: int
    creator_linked_trigger: bool
    blockers: tuple[str, ...] = ()
    research_only: bool = True
    paper_only: bool = True
    live_money_authority: bool = False
    signing_available: bool = False
    transaction_submission_available: bool = False


@dataclass(frozen=True)
class ProfitFirstPolicy:
    # Existing research/copyability boundaries carried into the v4 research lane.
    max_chase_fraction: float = 0.15
    max_observation_lag_seconds: float = 20.0
    min_forward_outcomes_for_selection: int = 30
    # Do not pre-select a permanently conservative size. Sweep a broad paper-only grid
    # and let out-of-sample geometric growth choose the allocation.
    position_fraction_grid: tuple[float, ...] = (
        0.005,
        0.01,
        0.02,
        0.05,
        0.10,
        0.15,
        0.20,
    )
    # Exact buckets are preferred, then progressively broader empirical cohorts.
    min_exact_bucket_samples: int = 20
    min_lane_samples: int = 30
    # Structural execution failures remain hard blockers. Creator/deployer linkage,
    # bundles, snipers and related-wallet activity intentionally are NOT on this list.
    structural_hard_stops: frozenset[str] = frozenset(
        {
            "sell_route_unavailable",
            "transfer_restricted",
            "liquidity_unexitable",
            "entry_quote_unavailable",
            "exit_quote_unavailable",
            "authority_can_block_transfer_or_exit",
        }
    )


class OutcomeLedger:
    """Point-in-time forward outcome ledger used to estimate residual copyable edge."""

    def __init__(self, outcomes: Iterable[ForwardOutcome] = ()) -> None:
        self._outcomes: list[ForwardOutcome] = list(outcomes)

    def add(self, outcome: ForwardOutcome) -> None:
        self._outcomes.append(outcome)

    @staticmethod
    def _log_growth(returns: Sequence[float], fraction: float) -> float:
        if not returns or fraction <= 0.0:
            return 0.0
        values: list[float] = []
        for result in returns:
            terminal = 1.0 + fraction * result
            if terminal <= 0.0:
                return float("-inf")
            values.append(math.log(terminal))
        return mean(values)

    def _cohort(self, context: OpportunityContext, policy: ProfitFirstPolicy) -> tuple[list[ForwardOutcome], str]:
        exact = [item for item in self._outcomes if item.context.bucket_key == context.bucket_key]
        if len(exact) >= policy.min_exact_bucket_samples:
            return exact, "exact_feature_bucket"

        lane_confirmation = [
            item
            for item in self._outcomes
            if item.context.lane == context.lane
            and item.context.confirmation_bin == context.confirmation_bin
            and item.context.creator_flow_state == context.creator_flow_state
        ]
        if len(lane_confirmation) >= policy.min_lane_samples:
            return lane_confirmation, "lane_confirmation_flow"

        lane = [item for item in self._outcomes if item.context.lane == context.lane]
        if lane:
            return lane, "lane"
        return [], "none"

    def predict(self, context: OpportunityContext, policy: ProfitFirstPolicy) -> LanePrediction:
        cohort, source = self._cohort(context, policy)
        returns = [item.net_return for item in cohort]
        if not returns:
            return LanePrediction(
                lane=context.lane,
                sample_count=0,
                source_level=source,
                mean_net_return=None,
                median_net_return=None,
                hit_rate=None,
                loss_rate=None,
                best_position_fraction=0.0,
                best_expected_log_growth=None,
                expected_log_growth_by_fraction={},
            )

        growth = {
            fraction: self._log_growth(returns, fraction)
            for fraction in policy.position_fraction_grid
        }
        best_fraction, best_growth = max(growth.items(), key=lambda item: item[1])
        if not math.isfinite(best_growth) or best_growth <= 0.0:
            best_fraction = 0.0

        return LanePrediction(
            lane=context.lane,
            sample_count=len(returns),
            source_level=source,
            mean_net_return=mean(returns),
            median_net_return=median(returns),
            hit_rate=sum(value > 0.0 for value in returns) / len(returns),
            loss_rate=sum(value < 0.0 for value in returns) / len(returns),
            best_position_fraction=best_fraction,
            best_expected_log_growth=best_growth,
            expected_log_growth_by_fraction=growth,
        )


class ProfitFirstEntityStrategy:
    """Paper-only v4 research strategy.

    It deliberately permits creator/deployer-associated opportunities. Profitability is
    evaluated from the first executable observation available to this system, after
    latency/costs, rather than from the privileged creator entry. Related addresses are
    collapsed into economic entities before confirmations are counted.
    """

    def __init__(
        self,
        *,
        entity_graph: EntityGraph | None = None,
        ledger: OutcomeLedger | None = None,
        policy: ProfitFirstPolicy | None = None,
    ) -> None:
        self.entities = entity_graph or EntityGraph()
        self.ledger = ledger or OutcomeLedger()
        self.policy = policy or ProfitFirstPolicy()

    @staticmethod
    def _creator_flow_state(value: float) -> str:
        # Bucketing only; it is not a manipulation verdict or an automatic veto.
        if value >= 0.05:
            return "accumulating"
        if value <= -0.05:
            return "distributing"
        return "neutral"

    @staticmethod
    def _confirmation_bin(value: int) -> str:
        if value <= 0:
            return "0"
        if value == 1:
            return "1"
        if value <= 3:
            return "2-3"
        return "4+"

    @staticmethod
    def _chase_bin(value: float) -> str:
        if value <= 0.05:
            return "<=5%"
        if value <= 0.10:
            return "5-10%"
        if value <= 0.15:
            return "10-15%"
        return ">15%"

    @staticmethod
    def _early_exit_bin(value: float) -> str:
        if value <= 0.05:
            return "<=5%"
        if value <= 0.20:
            return "5-20%"
        return ">20%"

    @staticmethod
    def _risk_bin(flags: frozenset[str]) -> str:
        count = len(flags)
        if count == 0:
            return "0"
        if count == 1:
            return "1"
        return "2+"

    def _entity_context(self, snapshot: OpportunitySnapshot) -> tuple[str, str | None, tuple[str, ...], int, bool]:
        trigger_entity = self.entities.entity_id(snapshot.trigger_wallet)
        assert trigger_entity is not None
        creator_entity = self.entities.entity_id(snapshot.creator_wallet)
        creator_linked = creator_entity is not None and creator_entity == trigger_entity

        confirmation_entities = self.entities.distinct_entities(snapshot.confirming_wallets)
        excluded = {trigger_entity}
        if creator_entity is not None:
            # A creator-side wallet does not count as independent confirmation of its own
            # token; external entities still do.
            excluded.add(creator_entity)
        independent = tuple(entity for entity in confirmation_entities if entity not in excluded)
        return trigger_entity, creator_entity, independent, len(independent), creator_linked

    def context_for_lane(self, snapshot: OpportunitySnapshot, lane: Lane) -> OpportunityContext:
        trigger_entity, creator_entity, confirmations, count, creator_linked = self._entity_context(snapshot)
        return OpportunityContext(
            lane=lane,
            trigger_entity=trigger_entity,
            creator_entity=creator_entity,
            confirmation_entities=confirmations,
            independent_confirmation_count=count,
            creator_linked_trigger=creator_linked,
            creator_flow_state=self._creator_flow_state(snapshot.creator_cluster_net_flow_fraction),
            confirmation_bin=self._confirmation_bin(count),
            chase_bin=self._chase_bin(snapshot.chase_fraction),
            early_exit_bin=self._early_exit_bin(snapshot.early_buyer_exit_fraction),
            soft_risk_bin=self._risk_bin(snapshot.soft_risk_flags),
        )

    def eligible_lanes(self, snapshot: OpportunitySnapshot) -> tuple[Lane, ...]:
        trigger_entity, creator_entity, _confirmations, count, creator_linked = self._entity_context(snapshot)
        lanes: list[Lane] = [Lane.UNFILTERED_ELITE]
        if creator_linked:
            lanes.append(Lane.CREATOR_CONTINUATION)
        else:
            lanes.append(Lane.CLEAN_SCOUT)
        # Count genuinely independent entities, not addresses. Trigger + one external
        # confirmation is enough to create a two-entity swarm candidate.
        if count >= 1:
            lanes.append(Lane.SMART_MONEY_SWARM)
        return tuple(lanes)

    def structural_blockers(self, snapshot: OpportunitySnapshot) -> tuple[str, ...]:
        blockers: list[str] = []
        if not snapshot.entry_executable:
            blockers.append("entry_quote_unavailable")
        if not snapshot.exit_executable:
            blockers.append("exit_quote_unavailable")
        blockers.extend(sorted(snapshot.hard_risk_flags & self.policy.structural_hard_stops))
        # Existing copyability boundaries are retained. Creator association is not one.
        if snapshot.chase_fraction > self.policy.max_chase_fraction:
            blockers.append("chase_above_limit")
        if snapshot.observation_lag_seconds > self.policy.max_observation_lag_seconds:
            blockers.append("observation_lag_above_limit")
        return tuple(dict.fromkeys(blockers))

    def evaluate_lane(self, snapshot: OpportunitySnapshot, lane: Lane) -> StrategyDecision:
        blockers = self.structural_blockers(snapshot)
        context = self.context_for_lane(snapshot, lane)
        if blockers:
            return StrategyDecision(
                strategy_version=STRATEGY_VERSION,
                decision=Decision.REJECT,
                selected_lane=lane,
                position_fraction=0.0,
                predicted_mean_net_return=None,
                predicted_log_growth=None,
                independent_confirmation_count=context.independent_confirmation_count,
                creator_linked_trigger=context.creator_linked_trigger,
                blockers=blockers,
            )

        prediction = self.ledger.predict(context, self.policy)
        if prediction.sample_count < self.policy.min_forward_outcomes_for_selection:
            return StrategyDecision(
                strategy_version=STRATEGY_VERSION,
                decision=Decision.SHADOW,
                selected_lane=lane,
                position_fraction=0.0,
                predicted_mean_net_return=prediction.mean_net_return,
                predicted_log_growth=prediction.best_expected_log_growth,
                independent_confirmation_count=context.independent_confirmation_count,
                creator_linked_trigger=context.creator_linked_trigger,
                blockers=("insufficient_forward_outcomes",),
            )

        if (
            prediction.mean_net_return is None
            or prediction.mean_net_return <= 0.0
            or prediction.best_expected_log_growth is None
            or prediction.best_expected_log_growth <= 0.0
            or prediction.best_position_fraction <= 0.0
        ):
            return StrategyDecision(
                strategy_version=STRATEGY_VERSION,
                decision=Decision.WAIT,
                selected_lane=lane,
                position_fraction=0.0,
                predicted_mean_net_return=prediction.mean_net_return,
                predicted_log_growth=prediction.best_expected_log_growth,
                independent_confirmation_count=context.independent_confirmation_count,
                creator_linked_trigger=context.creator_linked_trigger,
                blockers=("nonpositive_empirical_residual_edge",),
            )

        return StrategyDecision(
            strategy_version=STRATEGY_VERSION,
            decision=Decision.ENTER,
            selected_lane=lane,
            position_fraction=prediction.best_position_fraction,
            predicted_mean_net_return=prediction.mean_net_return,
            predicted_log_growth=prediction.best_expected_log_growth,
            independent_confirmation_count=context.independent_confirmation_count,
            creator_linked_trigger=context.creator_linked_trigger,
        )

    def evaluate_all(self, snapshot: OpportunitySnapshot) -> dict[str, StrategyDecision]:
        result: dict[str, StrategyDecision] = {}
        eligible = set(self.eligible_lanes(snapshot))
        for lane in Lane:
            if lane in eligible:
                result[lane.value] = self.evaluate_lane(snapshot, lane)

        # Unified lane selects the empirically strongest eligible tradeable lane.
        entries = [decision for decision in result.values() if decision.decision == Decision.ENTER]
        if entries:
            best = max(
                entries,
                key=lambda decision: (
                    decision.predicted_log_growth if decision.predicted_log_growth is not None else float("-inf"),
                    decision.predicted_mean_net_return if decision.predicted_mean_net_return is not None else float("-inf"),
                ),
            )
            result["unified_profit_maximizer"] = best
        else:
            shadows = [decision for decision in result.values() if decision.decision == Decision.SHADOW]
            waits = [decision for decision in result.values() if decision.decision == Decision.WAIT]
            fallback = shadows[0] if shadows else (waits[0] if waits else next(iter(result.values())))
            result["unified_profit_maximizer"] = fallback
        return result

    def status(self) -> dict[str, object]:
        return {
            "strategy_version": STRATEGY_VERSION,
            "objective": "maximize_expected_compounded_paper_return_after_costs",
            "paper_only": PAPER_ONLY,
            "live_money_authority": LIVE_MONEY_AUTHORITY,
            "signing_available": SIGNING_AVAILABLE,
            "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
            "active_v3_1_cohort_mutation_allowed": ACTIVE_V3_1_COHORT_MUTATION_ALLOWED,
            "creator_association_is_automatic_veto": False,
            "side_wallets_collapsed_to_economic_entities": True,
            "independent_confirmations_count_entities_not_addresses": True,
            "profitability_measurement_start": "first_system_observable_executable_entry",
            "position_sizing_objective": "empirical_expected_log_growth",
            "paper_position_fraction_grid": list(self.policy.position_fraction_grid),
            "min_forward_outcomes_for_selection": self.policy.min_forward_outcomes_for_selection,
            "hard_stops_are_structural_execution_failures": True,
            "four_strategy_shadow_comparison": [
                Lane.CLEAN_SCOUT.value,
                Lane.UNFILTERED_ELITE.value,
                Lane.CREATOR_CONTINUATION.value,
                "unified_profit_maximizer",
            ],
        }


__all__ = [
    "ACTIVE_V3_1_COHORT_MUTATION_ALLOWED",
    "Decision",
    "EntityGraph",
    "EntityLink",
    "ForwardOutcome",
    "Lane",
    "LanePrediction",
    "LIVE_MONEY_AUTHORITY",
    "OpportunityContext",
    "OpportunitySnapshot",
    "OutcomeLedger",
    "PAPER_ONLY",
    "ProfitFirstEntityStrategy",
    "ProfitFirstPolicy",
    "SIGNING_AVAILABLE",
    "STRATEGY_VERSION",
    "StrategyDecision",
    "TRANSACTION_SUBMISSION_AVAILABLE",
]
