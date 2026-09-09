from __future__ import annotations

"""v5.2 Batch 7: final research-only refinement and prospective freeze.

Adds the last approved refinement layer to the frozen v5.2 challenger:
anti-flapping state hysteresis, expiring continuation epochs, velocity-sensitive
quote freshness, stressed exit-capacity sizing, flow-to-price response,
portfolio-level opportunity competition, confidence/provenance on derived
signals, and time-to-alpha-loss measurement.

This module is intentionally unreachable from production composition and cannot
grant entry authority, change v5.1 authority, sign, submit, or enable live money.
"""

import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from .v52_continuation_capture import (
    CHALLENGER_EPOCH,
    CHALLENGER_VERSION,
    HIGH_CHASE_OBSERVE_ONLY_THRESHOLD,
    IMMEDIATE_COPY_MAX_SECONDS,
    INCUMBENT_VERSION,
    LIVE_MONEY_AUTHORITY,
    PAPER_ONLY,
    SIGNING_AVAILABLE,
    TRANSACTION_SUBMISSION_AVAILABLE,
)
from .v52_measurement_governance import FROZEN_FEATURE_IDS as BASE_FROZEN_FEATURE_IDS

BATCH_VERSION = "v52-batch7-final-refinement-1"
FINAL_FREEZE_ID = "v52-weekend-review-20260908-final-refinement-freeze-2"
PRODUCTION_COMPOSITION_HOOK = False
CHALLENGER_ENTRY_AUTHORITY = False
INCUMBENT_AUTHORITY_CHANGED = False
HISTORICAL_PROMOTION_AUTHORITY = False
PARAMETER_MUTATION_AUTHORITY = False
OPPORTUNITY_EMERGENCE_AUTHORITY = False
AVERAGING_DOWN_PERMITTED = False
FIRST_SLOT_PUMPFUN_SNIPING_PERMITTED = False
DYNAMIC_QUOTE_CALM_MAX_SECONDS = 8.0
DYNAMIC_QUOTE_FLOOR_SECONDS = 0.75
DEFAULT_SIGNAL_HALF_LIFE_SECONDS = 300.0
DEFAULT_SIGNAL_EXPIRY_SECONDS = 900.0
DEFAULT_IMPROVEMENT_CONFIRMATIONS = 2
DEFAULT_EXIT_DEPTH_UTILIZATION = 0.25
DEFAULT_IMMATURE_FAMILY_CAP_FRACTION = 0.25
ALPHA_LOSS_DELAYS_SECONDS: tuple[int, ...] = (1, 2, 5, 10, 20, 30, 60)

FINAL_REFINEMENT_FEATURE_IDS: tuple[str, ...] = (
    "state_hysteresis_anti_flapping",
    "signal_expiration_continuation_epochs",
    "dynamic_quote_freshness",
    "exit_capacity_stress",
    "flow_to_price_response",
    "portfolio_opportunity_competition",
    "signal_confidence_freshness_provenance",
    "time_to_alpha_loss_curve",
)


def _finite(value: Any, *, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field}_invalid") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field}_invalid")
    return number


def _nonnegative(value: Any, *, field: str) -> float:
    number = _finite(value, field=field)
    if number < 0.0:
        raise ValueError(f"{field}_invalid")
    return number


def _fraction(value: Any, *, field: str) -> float:
    number = _nonnegative(value, field=field)
    if number > 1.0:
        raise ValueError(f"{field}_invalid")
    return number


def _text(value: Any, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field}_missing")
    return text


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


@dataclass(frozen=True)
class DerivedSignal:
    name: str
    value: float
    confidence: float
    freshness_seconds: float
    provenance: tuple[str, ...]
    research_only: bool = True
    trading_authority: bool = False

    def validated(self) -> "DerivedSignal":
        _text(self.name, field="signal_name")
        _finite(self.value, field="signal_value")
        _fraction(self.confidence, field="signal_confidence")
        _nonnegative(self.freshness_seconds, field="signal_freshness_seconds")
        if not self.provenance:
            raise ValueError("signal_provenance_missing")
        normalized = tuple(dict.fromkeys(_text(item, field="signal_provenance") for item in self.provenance))
        return DerivedSignal(
            self.name.strip(),
            float(self.value),
            float(self.confidence),
            float(self.freshness_seconds),
            normalized,
        )


@dataclass(frozen=True)
class ContinuationEpoch:
    candidate_id: str
    epoch_id: str
    created_at_seconds: float
    reference_price: float
    evidence: tuple[DerivedSignal, ...]
    half_life_seconds: float = DEFAULT_SIGNAL_HALF_LIFE_SECONDS
    expiry_seconds: float = DEFAULT_SIGNAL_EXPIRY_SECONDS

    def validated(self) -> "ContinuationEpoch":
        _text(self.candidate_id, field="candidate_id")
        _text(self.epoch_id, field="epoch_id")
        _nonnegative(self.created_at_seconds, field="created_at_seconds")
        price = _finite(self.reference_price, field="reference_price")
        if price <= 0.0:
            raise ValueError("reference_price_invalid")
        half_life = _nonnegative(self.half_life_seconds, field="half_life_seconds")
        expiry = _nonnegative(self.expiry_seconds, field="expiry_seconds")
        if half_life <= 0.0 or expiry <= 0.0:
            raise ValueError("epoch_decay_invalid")
        if not self.evidence:
            raise ValueError("epoch_evidence_missing")
        tuple(item.validated() for item in self.evidence)
        return self


@dataclass(frozen=True)
class ContinuationEpochAssessment:
    epoch_id: str
    age_seconds: float
    active: bool
    expired: bool
    decayed_confidence: float
    new_epoch_required: bool
    reason: str
    research_only: bool = True
    trading_authority: bool = False


def assess_continuation_epoch(epoch: ContinuationEpoch, *, now_seconds: float) -> ContinuationEpochAssessment:
    epoch.validated()
    now = _nonnegative(now_seconds, field="now_seconds")
    age = max(0.0, now - epoch.created_at_seconds)
    raw_confidence = sum(item.confidence for item in epoch.evidence) / len(epoch.evidence)
    freshness_penalty = sum(
        0.5 ** (item.freshness_seconds / epoch.half_life_seconds)
        for item in epoch.evidence
    ) / len(epoch.evidence)
    age_penalty = 0.5 ** (age / epoch.half_life_seconds)
    decayed = _clamp01(raw_confidence * freshness_penalty * age_penalty)
    expired = age > epoch.expiry_seconds
    return ContinuationEpochAssessment(
        epoch_id=epoch.epoch_id,
        age_seconds=age,
        active=not expired,
        expired=expired,
        decayed_confidence=0.0 if expired else decayed,
        new_epoch_required=expired,
        reason="continuation_epoch_expired_new_evidence_required" if expired else "continuation_epoch_active",
    )


_STATE_RANK = {
    "developing": 0,
    "pre_actionable": 1,
    "actionable": 2,
}


@dataclass(frozen=True)
class LifecycleTransition:
    previous_state: str
    proposed_state: str
    resulting_state: str
    transition_applied: bool
    entry_consideration_allowed: bool
    confirmation_count: int
    reason: str
    research_only: bool = True
    trading_authority: bool = False


def apply_state_hysteresis(
    *,
    previous_state: str,
    proposed_state: str,
    confirmation_count: int,
    signal_fingerprint: str,
    last_entry_signal_fingerprint: str | None = None,
    required_improvement_confirmations: int = DEFAULT_IMPROVEMENT_CONFIRMATIONS,
    hard_structural_deterioration: bool = False,
) -> LifecycleTransition:
    previous = _text(previous_state, field="previous_state")
    proposed = _text(proposed_state, field="proposed_state")
    if previous not in _STATE_RANK or proposed not in _STATE_RANK:
        raise ValueError("lifecycle_state_unsupported")
    if confirmation_count < 0 or required_improvement_confirmations <= 0:
        raise ValueError("confirmation_count_invalid")
    fingerprint = _text(signal_fingerprint, field="signal_fingerprint")

    if hard_structural_deterioration:
        resulting = proposed if _STATE_RANK[proposed] < _STATE_RANK[previous] else "developing"
        return LifecycleTransition(
            previous, proposed, resulting, resulting != previous, False, confirmation_count,
            "hard_structural_deterioration_immediate_demotion",
        )

    improving = _STATE_RANK[proposed] > _STATE_RANK[previous]
    deteriorating = _STATE_RANK[proposed] < _STATE_RANK[previous]
    if improving and confirmation_count < required_improvement_confirmations:
        return LifecycleTransition(
            previous, proposed, previous, False, False, confirmation_count,
            "improvement_waiting_for_corroboration",
        )

    resulting = proposed if improving or deteriorating else previous
    duplicate = resulting == "actionable" and fingerprint == str(last_entry_signal_fingerprint or "")
    allowed = resulting == "actionable" and not duplicate
    reason = (
        "duplicate_marginal_signal_suppressed"
        if duplicate
        else "transition_confirmed"
        if resulting != previous
        else "state_held"
    )
    return LifecycleTransition(
        previous, proposed, resulting, resulting != previous, allowed, confirmation_count, reason
    )


@dataclass(frozen=True)
class QuoteFreshnessAssessment:
    price_velocity_per_second: float
    dynamic_max_age_seconds: float
    signal_to_quote_seconds: float
    quote_to_decision_seconds: float
    decision_to_fill_seconds: float
    total_latency_seconds: float
    hard_max_respected: bool
    dynamically_fresh: bool
    acceptable: bool
    reason: str
    research_only: bool = True
    trading_authority: bool = False


def dynamic_quote_max_age_seconds(price_velocity_per_second: float) -> float:
    velocity = abs(_finite(price_velocity_per_second, field="price_velocity_per_second"))
    soft_limit = DYNAMIC_QUOTE_CALM_MAX_SECONDS / (1.0 + 80.0 * velocity)
    return min(IMMEDIATE_COPY_MAX_SECONDS, max(DYNAMIC_QUOTE_FLOOR_SECONDS, soft_limit))


def assess_quote_freshness(
    *,
    price_velocity_per_second: float,
    signal_to_quote_seconds: float,
    quote_to_decision_seconds: float,
    decision_to_fill_seconds: float,
) -> QuoteFreshnessAssessment:
    stages = (
        _nonnegative(signal_to_quote_seconds, field="signal_to_quote_seconds"),
        _nonnegative(quote_to_decision_seconds, field="quote_to_decision_seconds"),
        _nonnegative(decision_to_fill_seconds, field="decision_to_fill_seconds"),
    )
    total = sum(stages)
    dynamic_max = dynamic_quote_max_age_seconds(price_velocity_per_second)
    hard_ok = total <= IMMEDIATE_COPY_MAX_SECONDS
    dynamic_ok = total <= dynamic_max
    acceptable = hard_ok and dynamic_ok
    return QuoteFreshnessAssessment(
        price_velocity_per_second=float(price_velocity_per_second),
        dynamic_max_age_seconds=dynamic_max,
        signal_to_quote_seconds=stages[0],
        quote_to_decision_seconds=stages[1],
        decision_to_fill_seconds=stages[2],
        total_latency_seconds=total,
        hard_max_respected=hard_ok,
        dynamically_fresh=dynamic_ok,
        acceptable=acceptable,
        reason=(
            "quote_chain_fresh"
            if acceptable
            else "hard_20s_latency_ceiling_exceeded"
            if not hard_ok
            else "velocity_sensitive_quote_freshness_failed"
        ),
    )


@dataclass(frozen=True)
class ExitCapacityStress:
    requested_position_notional: float
    exact_sell_depth_notional: float
    worst_degraded_depth_notional: float
    maximum_position_notional: float
    depth_utilization_limit: float
    passed: bool
    scenario_fractions: tuple[float, ...]
    research_only: bool = True
    trading_authority: bool = False


def stress_exit_capacity(
    *,
    requested_position_notional: float,
    exact_sell_depth_notional: float,
    scenario_fractions: Sequence[float] = (0.75, 0.50, 0.35),
    depth_utilization_limit: float = DEFAULT_EXIT_DEPTH_UTILIZATION,
) -> ExitCapacityStress:
    requested = _nonnegative(requested_position_notional, field="requested_position_notional")
    depth = _nonnegative(exact_sell_depth_notional, field="exact_sell_depth_notional")
    utilization = _fraction(depth_utilization_limit, field="depth_utilization_limit")
    if utilization <= 0.0:
        raise ValueError("depth_utilization_limit_invalid")
    scenarios = tuple(_fraction(item, field="scenario_fraction") for item in scenario_fractions)
    if not scenarios or any(item <= 0.0 for item in scenarios):
        raise ValueError("scenario_fractions_invalid")
    worst_depth = depth * min(scenarios)
    maximum = worst_depth * utilization
    return ExitCapacityStress(
        requested_position_notional=requested,
        exact_sell_depth_notional=depth,
        worst_degraded_depth_notional=worst_depth,
        maximum_position_notional=maximum,
        depth_utilization_limit=utilization,
        passed=requested <= maximum,
        scenario_fractions=scenarios,
    )


@dataclass(frozen=True)
class FlowToPriceResponse:
    price_change_fraction: float
    independent_net_buy_notional: float
    new_independent_buyers: int
    price_change_per_net_buy_dollar: float | None
    price_change_per_new_buyer: float | None
    marginal_buy_response_ratio: float | None
    seller_absorption_fraction: float | None
    exhaustion_risk: bool
    research_only: bool = True
    trading_authority: bool = False


def flow_to_price_response(
    *,
    price_change_fraction: float,
    independent_net_buy_notional: float,
    new_independent_buyers: int,
    prior_price_change_per_net_buy_dollar: float | None = None,
) -> FlowToPriceResponse:
    price_change = _finite(price_change_fraction, field="price_change_fraction")
    net_buy = _finite(independent_net_buy_notional, field="independent_net_buy_notional")
    if new_independent_buyers < 0:
        raise ValueError("new_independent_buyers_invalid")
    per_dollar = price_change / net_buy if net_buy > 0.0 else None
    per_buyer = price_change / new_independent_buyers if new_independent_buyers > 0 else None
    ratio: float | None = None
    absorption: float | None = None
    exhaustion = False
    if prior_price_change_per_net_buy_dollar is not None:
        prior = _finite(prior_price_change_per_net_buy_dollar, field="prior_price_change_per_net_buy_dollar")
        if prior > 0.0 and per_dollar is not None:
            ratio = per_dollar / prior
            absorption = _clamp01(1.0 - max(0.0, ratio))
            exhaustion = bool(net_buy > 0.0 and ratio < 0.50)
    if net_buy > 0.0 and price_change <= 0.0:
        exhaustion = True
        absorption = 1.0
    return FlowToPriceResponse(
        price_change_fraction=price_change,
        independent_net_buy_notional=net_buy,
        new_independent_buyers=new_independent_buyers,
        price_change_per_net_buy_dollar=per_dollar,
        price_change_per_new_buyer=per_buyer,
        marginal_buy_response_ratio=ratio,
        seller_absorption_fraction=absorption,
        exhaustion_risk=exhaustion,
    )


@dataclass(frozen=True)
class OpportunityCandidate:
    candidate_id: str
    expected_residual_return_fraction: float
    confidence: float
    execution_quality: float
    tail_risk_cost: float
    requested_notional: float
    creator_cluster: str | None = None
    funder_cluster: str | None = None
    wallet_cluster: str | None = None
    graduation_cohort: str | None = None
    venue: str | None = None

    def validated(self) -> "OpportunityCandidate":
        _text(self.candidate_id, field="candidate_id")
        _finite(self.expected_residual_return_fraction, field="expected_residual_return_fraction")
        _fraction(self.confidence, field="confidence")
        _fraction(self.execution_quality, field="execution_quality")
        risk = _nonnegative(self.tail_risk_cost, field="tail_risk_cost")
        if risk <= 0.0:
            raise ValueError("tail_risk_cost_invalid")
        _nonnegative(self.requested_notional, field="requested_notional")
        return self

    @property
    def base_score(self) -> float:
        self.validated()
        return max(0.0, self.expected_residual_return_fraction) * self.confidence * self.execution_quality / self.tail_risk_cost

    @property
    def cluster_keys(self) -> frozenset[str]:
        pairs = (
            ("creator", self.creator_cluster),
            ("funder", self.funder_cluster),
            ("wallet", self.wallet_cluster),
            ("cohort", self.graduation_cohort),
            ("venue", self.venue),
        )
        return frozenset(f"{kind}:{value}" for kind, value in pairs if str(value or "").strip())


@dataclass(frozen=True)
class OpportunityAllocation:
    candidate_id: str
    base_score: float
    competition_score: float
    overlap_count: int
    allocated_notional: float
    rank: int
    research_only: bool = True
    trading_authority: bool = False


def compete_for_portfolio_capital(
    candidates: Iterable[OpportunityCandidate],
    *,
    portfolio_notional: float,
    current_immature_family_notional: float = 0.0,
    immature_family_cap_fraction: float = DEFAULT_IMMATURE_FAMILY_CAP_FRACTION,
    existing_cluster_keys: Iterable[str] = (),
) -> tuple[OpportunityAllocation, ...]:
    rows = [item.validated() for item in candidates]
    if len({item.candidate_id for item in rows}) != len(rows):
        raise ValueError("duplicate_candidate_id")
    portfolio = _nonnegative(portfolio_notional, field="portfolio_notional")
    current = _nonnegative(current_immature_family_notional, field="current_immature_family_notional")
    cap_fraction = _fraction(immature_family_cap_fraction, field="immature_family_cap_fraction")
    remaining = max(0.0, portfolio * cap_fraction - current)
    occupied = set(str(item) for item in existing_cluster_keys)

    allocations: list[OpportunityAllocation] = []
    selected_clusters = set(occupied)
    pending = list(rows)
    rank = 1
    while pending:
        scored = [
            (
                item,
                item.base_score / (1.0 + len(item.cluster_keys & selected_clusters)),
                len(item.cluster_keys & selected_clusters),
            )
            for item in pending
        ]
        item, adjusted_score, overlap = max(
            scored,
            key=lambda row: (row[1], -len(row[0].cluster_keys), row[0].candidate_id),
        )
        allocation = min(item.requested_notional, remaining) if adjusted_score > 0.0 else 0.0
        remaining -= allocation
        if allocation > 0.0:
            selected_clusters.update(item.cluster_keys)
        allocations.append(
            OpportunityAllocation(
                candidate_id=item.candidate_id,
                base_score=item.base_score,
                competition_score=adjusted_score,
                overlap_count=overlap,
                allocated_notional=allocation,
                rank=rank,
            )
        )
        pending.remove(item)
        rank += 1
    return tuple(allocations)


@dataclass(frozen=True)
class AlphaLossPoint:
    delay_seconds: int
    executable_residual_upside_fraction: float
    alpha_lost_vs_1s_fraction: float


@dataclass(frozen=True)
class AlphaLossCurve:
    points: tuple[AlphaLossPoint, ...]
    fastest_material_loss_window_seconds: int | None
    research_only: bool = True
    trading_authority: bool = False


def time_to_alpha_loss_curve(
    executable_residual_upside_by_delay: Mapping[int, float],
    *,
    material_loss_fraction: float = 0.10,
) -> AlphaLossCurve:
    threshold = _fraction(material_loss_fraction, field="material_loss_fraction")
    missing = [delay for delay in ALPHA_LOSS_DELAYS_SECONDS if delay not in executable_residual_upside_by_delay]
    if missing:
        raise ValueError(f"alpha_loss_delay_missing:{missing[0]}")
    residuals = {
        delay: _finite(executable_residual_upside_by_delay[delay], field=f"residual_upside_{delay}s")
        for delay in ALPHA_LOSS_DELAYS_SECONDS
    }
    baseline = residuals[1]
    points = tuple(
        AlphaLossPoint(
            delay_seconds=delay,
            executable_residual_upside_fraction=residuals[delay],
            alpha_lost_vs_1s_fraction=max(0.0, baseline - residuals[delay]),
        )
        for delay in ALPHA_LOSS_DELAYS_SECONDS
    )
    material_window = next(
        (
            point.delay_seconds
            for point in points[1:]
            if point.alpha_lost_vs_1s_fraction >= threshold
        ),
        None,
    )
    return AlphaLossCurve(points=points, fastest_material_loss_window_seconds=material_window)


def final_freeze_manifest() -> dict[str, Any]:
    payload = {
        "batch_version": BATCH_VERSION,
        "final_freeze_id": FINAL_FREEZE_ID,
        "challenger_version": CHALLENGER_VERSION,
        "challenger_epoch": CHALLENGER_EPOCH,
        "incumbent_version": INCUMBENT_VERSION,
        "frozen_feature_ids": list(BASE_FROZEN_FEATURE_IDS) + list(FINAL_REFINEMENT_FEATURE_IDS),
        "prospective_testing_only": True,
        "historical_weekend_evidence_can_retune": False,
        "parameter_mutation_authority": PARAMETER_MUTATION_AUTHORITY,
        "historical_promotion_authority": HISTORICAL_PROMOTION_AUTHORITY,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {**payload, "freeze_fingerprint_sha256": hashlib.sha256(encoded).hexdigest()}


def safety_manifest() -> dict[str, Any]:
    return {
        **final_freeze_manifest(),
        "incumbent_remains_authoritative": True,
        "incumbent_authority_changed": INCUMBENT_AUTHORITY_CHANGED,
        "challenger_entry_authority": CHALLENGER_ENTRY_AUTHORITY,
        "research_only": True,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        "production_composition_hook": PRODUCTION_COMPOSITION_HOOK,
        "hard_latency_ceiling_seconds": IMMEDIATE_COPY_MAX_SECONDS,
        "dynamic_quote_freshness_tighter_than_hard_ceiling": True,
        "high_chase_observe_only_threshold": HIGH_CHASE_OBSERVE_ONLY_THRESHOLD,
        "exact_two_sided_quotes_required": True,
        "exact_sell_route_required": True,
        "structural_exit_hard_stops_preserved": True,
        "first_slot_pumpfun_sniping_permitted": FIRST_SLOT_PUMPFUN_SNIPING_PERMITTED,
        "averaging_down_permitted": AVERAGING_DOWN_PERMITTED,
        "acceleration_alone_can_increase_allocation": False,
        "opportunity_emergence_trading_authority": OPPORTUNITY_EMERGENCE_AUTHORITY,
        "evidence_standards_can_be_lowered_to_force_trades": False,
        "immature_family_cap_fraction": DEFAULT_IMMATURE_FAMILY_CAP_FRACTION,
        "time_to_alpha_loss_delays_seconds": list(ALPHA_LOSS_DELAYS_SECONDS),
    }


__all__ = [
    "ALPHA_LOSS_DELAYS_SECONDS",
    "AlphaLossCurve",
    "AlphaLossPoint",
    "BATCH_VERSION",
    "ContinuationEpoch",
    "ContinuationEpochAssessment",
    "DerivedSignal",
    "ExitCapacityStress",
    "FINAL_FREEZE_ID",
    "FlowToPriceResponse",
    "LifecycleTransition",
    "OpportunityAllocation",
    "OpportunityCandidate",
    "QuoteFreshnessAssessment",
    "apply_state_hysteresis",
    "assess_continuation_epoch",
    "assess_quote_freshness",
    "compete_for_portfolio_capital",
    "dynamic_quote_max_age_seconds",
    "final_freeze_manifest",
    "flow_to_price_response",
    "safety_manifest",
    "stress_exit_capacity",
    "time_to_alpha_loss_curve",
]
