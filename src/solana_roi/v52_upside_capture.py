from __future__ import annotations

"""v5.2 Batch 4: research-only upside-capture mechanics.

This module implements the state-machine mechanics requested for the v5.2
challenger without granting production reachability or entry authority.
Economic percentages are explicit experiment inputs, not production defaults.
"""

import math
from dataclasses import asdict, dataclass
from typing import Any

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
from .v52_lossless_candidate_accounting import CANONICAL_LANES


BATCH_VERSION = "v52-batch4-upside-capture-1"
PRODUCTION_COMPOSITION_HOOK = False
CHALLENGER_ENTRY_AUTHORITY = False
INCUMBENT_AUTHORITY_CHANGED = False


def _finite_nonnegative(value: Any, *, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field}_invalid") from exc
    if not math.isfinite(number) or number < 0.0:
        raise ValueError(f"{field}_invalid")
    return number


def _fraction(value: Any, *, field: str, allow_zero: bool = False) -> float:
    number = _finite_nonnegative(value, field=field)
    if number > 1.0 or (not allow_zero and number <= 0.0):
        raise ValueError(f"{field}_invalid")
    return number


@dataclass(frozen=True)
class UpsideCapturePolicy:
    """Prospective experiment parameters; no values are promoted by this module."""

    starter_fraction_of_target: float
    max_scale_fraction_of_target: float
    first_derisk_fraction_of_position: float
    second_derisk_fraction_of_position: float
    runner_fraction_of_target: float
    minimum_exit_depth_coverage_ratio: float

    def validated(self) -> "UpsideCapturePolicy":
        _fraction(self.starter_fraction_of_target, field="starter_fraction_of_target")
        _fraction(self.max_scale_fraction_of_target, field="max_scale_fraction_of_target")
        _fraction(
            self.first_derisk_fraction_of_position,
            field="first_derisk_fraction_of_position",
        )
        _fraction(
            self.second_derisk_fraction_of_position,
            field="second_derisk_fraction_of_position",
        )
        _fraction(
            self.runner_fraction_of_target,
            field="runner_fraction_of_target",
            allow_zero=True,
        )
        coverage = _finite_nonnegative(
            self.minimum_exit_depth_coverage_ratio,
            field="minimum_exit_depth_coverage_ratio",
        )
        if coverage < 1.0:
            raise ValueError("minimum_exit_depth_coverage_ratio_invalid")
        return self


@dataclass(frozen=True)
class QuoteSnapshot:
    """Amount-specific two-sided execution evidence for one decision instant."""

    latency_seconds: float
    chase_fraction: float
    exact_buy_quote_available: bool
    exact_sell_quote_available: bool
    structurally_exitable: bool
    buy_quote_notional: float
    sell_quote_notional: float
    executable_sell_depth_notional: float


@dataclass(frozen=True)
class ForwardEvidence:
    """Upstream evidence flags; this module does not invent signal thresholds."""

    mark_price: float
    new_forward_evidence: bool = False
    independent_buying_accelerating: bool = False
    wallet_quality_improving: bool = False
    liquidity_improving: bool = False
    sell_depth_improving: bool = False
    concentration_improving: bool = False
    hazard_declining: bool = False
    buyer_breadth_expanding: bool = False
    incremental_slippage_acceptable: bool = False
    continuation_healthy: bool = True
    independent_participation_persists: bool = True
    seller_pressure_controlled: bool = True
    hazards_acceptable: bool = True
    attention_decay: bool = False
    weakening_price_structure: bool = False
    structural_hard_stop: bool = False

    def strength_evidence_count(self) -> int:
        return sum(
            bool(value)
            for value in (
                self.independent_buying_accelerating,
                self.wallet_quality_improving,
                self.liquidity_improving,
                self.sell_depth_improving,
                self.concentration_improving,
                self.hazard_declining,
                self.buyer_breadth_expanding,
                self.incremental_slippage_acceptable,
            )
        )


@dataclass(frozen=True)
class CapturePosition:
    candidate_id: str
    lane: str
    target_notional: float
    position_notional: float
    last_add_price: float
    lifecycle_state: str
    derisk_stage: int
    impulse_id: str

    def validated(self) -> "CapturePosition":
        if not str(self.candidate_id or "").strip():
            raise ValueError("candidate_id_missing")
        if str(self.lane or "").strip() not in CANONICAL_LANES:
            raise ValueError("lane_unsupported")
        _finite_nonnegative(self.target_notional, field="target_notional")
        _finite_nonnegative(self.position_notional, field="position_notional")
        _finite_nonnegative(self.last_add_price, field="last_add_price")
        if self.derisk_stage not in (0, 1, 2):
            raise ValueError("derisk_stage_invalid")
        if not str(self.impulse_id or "").strip():
            raise ValueError("impulse_id_missing")
        return self


@dataclass(frozen=True)
class CaptureAction:
    action: str
    reasons: tuple[str, ...]
    notional_change: float
    resulting_position_notional: float
    resulting_lifecycle_state: str
    resulting_derisk_stage: int
    liquidity_cap_notional: float
    research_only: bool = True
    entry_authority: bool = False
    paper_only: bool = True

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SecondLegEvidence:
    new_impulse_id: str
    consolidation_confirmed: bool
    new_independent_buyers: bool
    liquidity_expanding: bool
    renewed_acceleration: bool


def _validated_quote(quote: QuoteSnapshot) -> QuoteSnapshot:
    _finite_nonnegative(quote.latency_seconds, field="latency_seconds")
    _finite_nonnegative(quote.chase_fraction, field="chase_fraction")
    _finite_nonnegative(quote.buy_quote_notional, field="buy_quote_notional")
    _finite_nonnegative(quote.sell_quote_notional, field="sell_quote_notional")
    _finite_nonnegative(
        quote.executable_sell_depth_notional,
        field="executable_sell_depth_notional",
    )
    return quote


def liquidity_cap_notional(
    target_notional: float,
    quote: QuoteSnapshot,
    policy: UpsideCapturePolicy,
) -> float:
    """Cap position size by actual executable sell depth at the decision instant."""

    policy.validated()
    _validated_quote(quote)
    target = _finite_nonnegative(target_notional, field="target_notional")
    depth_cap = quote.executable_sell_depth_notional / policy.minimum_exit_depth_coverage_ratio
    return max(0.0, min(target, depth_cap, quote.sell_quote_notional))


def _buy_blockers(
    quote: QuoteSnapshot,
    *,
    buy_notional: float,
    resulting_position_notional: float,
) -> list[str]:
    blockers: list[str] = []
    if quote.latency_seconds > IMMEDIATE_COPY_MAX_SECONDS:
        blockers.append("latency_above_20s")
    if quote.chase_fraction > HIGH_CHASE_OBSERVE_ONLY_THRESHOLD:
        blockers.append("gt_40pct_chase_observe_only")
    if not quote.exact_buy_quote_available:
        blockers.append("exact_buy_quote_missing")
    if not quote.exact_sell_quote_available:
        blockers.append("exact_sell_quote_missing")
    if not quote.structurally_exitable:
        blockers.append("structurally_unexitable")
    if quote.buy_quote_notional + 1e-12 < buy_notional:
        blockers.append("buy_quote_notional_insufficient")
    if quote.sell_quote_notional + 1e-12 < resulting_position_notional:
        blockers.append("sell_quote_notional_insufficient")
    return blockers


def _sell_blockers(quote: QuoteSnapshot, *, sell_notional: float) -> list[str]:
    blockers: list[str] = []
    if quote.latency_seconds > IMMEDIATE_COPY_MAX_SECONDS:
        blockers.append("latency_above_20s")
    if not quote.exact_sell_quote_available:
        blockers.append("exact_sell_quote_missing")
    if not quote.structurally_exitable:
        blockers.append("structurally_unexitable")
    if quote.sell_quote_notional + 1e-12 < sell_notional:
        blockers.append("sell_quote_notional_insufficient")
    if quote.executable_sell_depth_notional + 1e-12 < sell_notional:
        blockers.append("sell_depth_insufficient")
    return blockers


def _action(
    *,
    action: str,
    reasons: list[str] | tuple[str, ...],
    change: float,
    resulting_position: float,
    lifecycle: str,
    derisk_stage: int,
    liquidity_cap: float,
) -> CaptureAction:
    return CaptureAction(
        action=action,
        reasons=tuple(reasons),
        notional_change=float(change),
        resulting_position_notional=max(0.0, float(resulting_position)),
        resulting_lifecycle_state=lifecycle,
        resulting_derisk_stage=derisk_stage,
        liquidity_cap_notional=max(0.0, float(liquidity_cap)),
    )


def plan_starter(
    *,
    candidate_id: str,
    lane: str,
    target_notional: float,
    mark_price: float,
    impulse_id: str,
    quote: QuoteSnapshot,
    policy: UpsideCapturePolicy,
) -> CaptureAction:
    """Plan a small initial position while enforcing two-sided execution evidence."""

    policy.validated()
    _validated_quote(quote)
    if not str(candidate_id or "").strip():
        raise ValueError("candidate_id_missing")
    if str(lane or "").strip() not in CANONICAL_LANES:
        raise ValueError("lane_unsupported")
    target = _finite_nonnegative(target_notional, field="target_notional")
    _finite_nonnegative(mark_price, field="mark_price")
    if not str(impulse_id or "").strip():
        raise ValueError("impulse_id_missing")

    cap = liquidity_cap_notional(target, quote, policy)
    requested = target * policy.starter_fraction_of_target
    starter = min(requested, cap, quote.buy_quote_notional)
    blockers = _buy_blockers(
        quote,
        buy_notional=starter,
        resulting_position_notional=starter,
    )
    if starter <= 0.0:
        blockers.append("no_executable_starter_size")
    if blockers:
        action = "observe_only" if blockers == ["gt_40pct_chase_observe_only"] else "starter_blocked"
        return _action(
            action=action,
            reasons=blockers,
            change=0.0,
            resulting_position=0.0,
            lifecycle="pre_actionable",
            derisk_stage=0,
            liquidity_cap=cap,
        )

    return _action(
        action="starter",
        reasons=["starter_fractional_entry"],
        change=starter,
        resulting_position=starter,
        lifecycle="entered",
        derisk_stage=0,
        liquidity_cap=cap,
    )


def plan_scale_in(
    position: CapturePosition,
    *,
    quote: QuoteSnapshot,
    evidence: ForwardEvidence,
    policy: UpsideCapturePolicy,
) -> CaptureAction:
    """Scale only into new demonstrated strength and never average down."""

    position.validated()
    policy.validated()
    _validated_quote(quote)
    mark_price = _finite_nonnegative(evidence.mark_price, field="mark_price")
    cap = liquidity_cap_notional(position.target_notional, quote, policy)

    reasons: list[str] = []
    if position.position_notional <= 0.0:
        reasons.append("no_open_position")
    if evidence.structural_hard_stop:
        reasons.append("structural_hard_stop")
    if not evidence.new_forward_evidence:
        reasons.append("no_new_forward_evidence")
    if evidence.strength_evidence_count() <= 0:
        reasons.append("no_strength_evidence")
    if not evidence.continuation_healthy:
        reasons.append("continuation_not_healthy")
    if not evidence.hazards_acceptable:
        reasons.append("hazards_not_acceptable_for_scale")
    if mark_price + 1e-12 < position.last_add_price:
        reasons.append("averaging_down_prohibited")

    room = max(0.0, cap - position.position_notional)
    planned = min(
        position.target_notional * policy.max_scale_fraction_of_target,
        room,
        quote.buy_quote_notional,
        max(0.0, quote.sell_quote_notional - position.position_notional),
    )
    projected = position.position_notional + planned
    reasons.extend(
        _buy_blockers(
            quote,
            buy_notional=planned,
            resulting_position_notional=projected,
        )
    )
    if planned <= 0.0:
        reasons.append("no_liquidity_adjusted_scale_room")

    if reasons:
        return _action(
            action="scale_blocked",
            reasons=list(dict.fromkeys(reasons)),
            change=0.0,
            resulting_position=position.position_notional,
            lifecycle=position.lifecycle_state,
            derisk_stage=position.derisk_stage,
            liquidity_cap=cap,
        )

    return _action(
        action="scale_in",
        reasons=["new_forward_evidence_confirmed", "two_sided_requote_confirmed"],
        change=planned,
        resulting_position=projected,
        lifecycle="scaling",
        derisk_stage=position.derisk_stage,
        liquidity_cap=cap,
    )


def _runner_conditions(evidence: ForwardEvidence) -> bool:
    return bool(
        evidence.continuation_healthy
        and evidence.independent_participation_persists
        and evidence.seller_pressure_controlled
        and evidence.hazards_acceptable
        and not evidence.attention_decay
        and not evidence.structural_hard_stop
    )


def _sell_action(
    position: CapturePosition,
    *,
    quote: QuoteSnapshot,
    sell_notional: float,
    action: str,
    reasons: list[str],
    lifecycle: str,
    derisk_stage: int,
    cap: float,
) -> CaptureAction:
    blockers = _sell_blockers(quote, sell_notional=sell_notional)
    if blockers:
        return _action(
            action="exit_blocked",
            reasons=blockers,
            change=0.0,
            resulting_position=position.position_notional,
            lifecycle=position.lifecycle_state,
            derisk_stage=position.derisk_stage,
            liquidity_cap=cap,
        )
    resulting = max(0.0, position.position_notional - sell_notional)
    if resulting <= 1e-12:
        resulting = 0.0
        lifecycle = "reentry_watch"
    return _action(
        action=action,
        reasons=reasons,
        change=-sell_notional,
        resulting_position=resulting,
        lifecycle=lifecycle,
        derisk_stage=derisk_stage,
        liquidity_cap=cap,
    )


def plan_position_management(
    position: CapturePosition,
    *,
    quote: QuoteSnapshot,
    evidence: ForwardEvidence,
    policy: UpsideCapturePolicy,
) -> CaptureAction:
    """Plan staged de-risking, runner retention, or a full paper exit."""

    position.validated()
    policy.validated()
    _validated_quote(quote)
    _finite_nonnegative(evidence.mark_price, field="mark_price")
    cap = liquidity_cap_notional(position.target_notional, quote, policy)

    if position.position_notional <= 0.0:
        return _action(
            action="hold_reentry_watch",
            reasons=["no_open_position"],
            change=0.0,
            resulting_position=0.0,
            lifecycle="reentry_watch",
            derisk_stage=position.derisk_stage,
            liquidity_cap=cap,
        )

    if position.lifecycle_state == "runner":
        if _runner_conditions(evidence):
            return _action(
                action="hold_runner",
                reasons=["runner_conditions_healthy"],
                change=0.0,
                resulting_position=position.position_notional,
                lifecycle="runner",
                derisk_stage=position.derisk_stage,
                liquidity_cap=cap,
            )
        return _sell_action(
            position,
            quote=quote,
            sell_notional=position.position_notional,
            action="exit_runner",
            reasons=["runner_conditions_failed"],
            lifecycle="reentry_watch",
            derisk_stage=position.derisk_stage,
            cap=cap,
        )

    if evidence.structural_hard_stop:
        return _sell_action(
            position,
            quote=quote,
            sell_notional=position.position_notional,
            action="full_exit",
            reasons=["structural_hard_stop"],
            lifecycle="reentry_watch",
            derisk_stage=position.derisk_stage,
            cap=cap,
        )

    deterioration = bool(
        (evidence.attention_decay and evidence.weakening_price_structure)
        or not evidence.seller_pressure_controlled
        or not evidence.hazards_acceptable
    )
    if not deterioration:
        if position.derisk_stage >= 2 and _runner_conditions(evidence):
            runner_target = min(
                position.position_notional,
                position.target_notional * policy.runner_fraction_of_target,
            )
            sell_amount = max(0.0, position.position_notional - runner_target)
            if sell_amount <= 1e-12:
                return _action(
                    action="enter_runner",
                    reasons=["runner_conditions_healthy"],
                    change=0.0,
                    resulting_position=position.position_notional,
                    lifecycle="runner",
                    derisk_stage=2,
                    liquidity_cap=cap,
                )
            return _sell_action(
                position,
                quote=quote,
                sell_notional=sell_amount,
                action="enter_runner",
                reasons=["runner_conditions_healthy", "runner_fraction_retained"],
                lifecycle="runner",
                derisk_stage=2,
                cap=cap,
            )
        return _action(
            action="hold",
            reasons=["no_exit_trigger"],
            change=0.0,
            resulting_position=position.position_notional,
            lifecycle=position.lifecycle_state,
            derisk_stage=position.derisk_stage,
            liquidity_cap=cap,
        )

    if position.derisk_stage == 0:
        sell_amount = position.position_notional * policy.first_derisk_fraction_of_position
        return _sell_action(
            position,
            quote=quote,
            sell_notional=sell_amount,
            action="stage_derisk_1",
            reasons=["deterioration_detected"],
            lifecycle="de_risking",
            derisk_stage=1,
            cap=cap,
        )

    if position.derisk_stage == 1:
        sell_amount = position.position_notional * policy.second_derisk_fraction_of_position
        return _sell_action(
            position,
            quote=quote,
            sell_notional=sell_amount,
            action="stage_derisk_2",
            reasons=["persistent_deterioration"],
            lifecycle="de_risking",
            derisk_stage=2,
            cap=cap,
        )

    if _runner_conditions(evidence):
        runner_target = min(
            position.position_notional,
            position.target_notional * policy.runner_fraction_of_target,
        )
        sell_amount = max(0.0, position.position_notional - runner_target)
        if sell_amount <= 1e-12:
            return _action(
                action="enter_runner",
                reasons=["runner_conditions_healthy"],
                change=0.0,
                resulting_position=position.position_notional,
                lifecycle="runner",
                derisk_stage=2,
                liquidity_cap=cap,
            )
        return _sell_action(
            position,
            quote=quote,
            sell_notional=sell_amount,
            action="enter_runner",
            reasons=["runner_conditions_healthy", "runner_fraction_retained"],
            lifecycle="runner",
            derisk_stage=2,
            cap=cap,
        )

    return _sell_action(
        position,
        quote=quote,
        sell_notional=position.position_notional,
        action="full_exit",
        reasons=["deterioration_persisted_after_staged_derisk"],
        lifecycle="reentry_watch",
        derisk_stage=2,
        cap=cap,
    )


def plan_second_leg_reentry(
    exited_position: CapturePosition,
    *,
    quote: QuoteSnapshot,
    evidence: ForwardEvidence,
    second_leg: SecondLegEvidence,
    policy: UpsideCapturePolicy,
) -> CaptureAction:
    """Create a fresh starter only after a genuinely new continuation event."""

    exited_position.validated()
    policy.validated()
    _validated_quote(quote)
    if exited_position.position_notional > 1e-12:
        return _action(
            action="reentry_blocked",
            reasons=["prior_position_still_open"],
            change=0.0,
            resulting_position=exited_position.position_notional,
            lifecycle=exited_position.lifecycle_state,
            derisk_stage=exited_position.derisk_stage,
            liquidity_cap=liquidity_cap_notional(
                exited_position.target_notional, quote, policy
            ),
        )

    reasons: list[str] = []
    new_impulse = str(second_leg.new_impulse_id or "").strip()
    if not new_impulse:
        reasons.append("new_impulse_id_missing")
    elif new_impulse == exited_position.impulse_id:
        reasons.append("second_leg_must_use_new_impulse")
    if not second_leg.consolidation_confirmed:
        reasons.append("consolidation_not_confirmed")
    if not second_leg.new_independent_buyers:
        reasons.append("new_independent_buyers_missing")
    if not second_leg.liquidity_expanding:
        reasons.append("liquidity_expansion_missing")
    if not second_leg.renewed_acceleration:
        reasons.append("renewed_acceleration_missing")
    if not evidence.continuation_healthy:
        reasons.append("continuation_not_healthy")
    if not evidence.hazards_acceptable:
        reasons.append("hazards_not_acceptable_for_reentry")
    if evidence.structural_hard_stop:
        reasons.append("structural_hard_stop")

    cap = liquidity_cap_notional(exited_position.target_notional, quote, policy)
    requested = exited_position.target_notional * policy.starter_fraction_of_target
    starter = min(requested, cap, quote.buy_quote_notional)
    reasons.extend(
        _buy_blockers(
            quote,
            buy_notional=starter,
            resulting_position_notional=starter,
        )
    )
    if starter <= 0.0:
        reasons.append("no_executable_reentry_size")

    deduped = list(dict.fromkeys(reasons))
    if deduped:
        action = "observe_only" if deduped == ["gt_40pct_chase_observe_only"] else "reentry_blocked"
        return _action(
            action=action,
            reasons=deduped,
            change=0.0,
            resulting_position=0.0,
            lifecycle="reentry_watch",
            derisk_stage=0,
            liquidity_cap=cap,
        )

    return _action(
        action="reentry_starter",
        reasons=["fresh_second_leg_confirmed", "two_sided_requote_confirmed"],
        change=starter,
        resulting_position=starter,
        lifecycle="entered",
        derisk_stage=0,
        liquidity_cap=cap,
    )


def safety_manifest() -> dict[str, Any]:
    return {
        "batch_version": BATCH_VERSION,
        "challenger_version": CHALLENGER_VERSION,
        "challenger_epoch": CHALLENGER_EPOCH,
        "incumbent_version": INCUMBENT_VERSION,
        "incumbent_remains_authoritative": True,
        "incumbent_authority_changed": INCUMBENT_AUTHORITY_CHANGED,
        "challenger_entry_authority": CHALLENGER_ENTRY_AUTHORITY,
        "research_only": True,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        "production_composition_hook": PRODUCTION_COMPOSITION_HOOK,
        "production_economic_parameters_frozen_by_batch4": False,
        "starter_positions_supported": True,
        "scale_requires_new_forward_evidence": True,
        "averaging_down_prohibited": True,
        "scale_requires_fresh_two_sided_amount_specific_quotes": True,
        "liquidity_adjusted_position_cap": True,
        "staged_derisk_supported": True,
        "persistent_runner_supported": True,
        "second_leg_reentry_supported": True,
        "second_leg_requires_new_impulse": True,
        "gt_40pct_chase_observe_only": True,
        "immediate_copy_max_seconds": IMMEDIATE_COPY_MAX_SECONDS,
        "high_chase_observe_only_threshold": HIGH_CHASE_OBSERVE_ONLY_THRESHOLD,
        "canonical_lanes": list(CANONICAL_LANES),
    }


__all__ = [
    "BATCH_VERSION",
    "CaptureAction",
    "CapturePosition",
    "ForwardEvidence",
    "QuoteSnapshot",
    "SecondLegEvidence",
    "UpsideCapturePolicy",
    "liquidity_cap_notional",
    "plan_position_management",
    "plan_scale_in",
    "plan_second_leg_reentry",
    "plan_starter",
    "safety_manifest",
]
