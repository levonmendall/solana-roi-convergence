from __future__ import annotations

"""v5.2 Batch 6: research-only measurement and frozen challenger governance.

This module measures executable opportunity capture and compares the frozen v5.2
challenger against the authoritative v5.1 incumbent on an identical forward
candidate stream. It cannot tune either strategy, grant entry authority, or
promote historical/weekend evidence.
"""

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from statistics import mean, median
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
from .v52_lossless_candidate_accounting import CANONICAL_LANES


BATCH_VERSION = "v52-batch6-measurement-governance-1"
FREEZE_ID = "v52-weekend-review-20260908-frozen-challenger-1"
PRODUCTION_COMPOSITION_HOOK = False
CHALLENGER_ENTRY_AUTHORITY = False
INCUMBENT_AUTHORITY_CHANGED = False
HISTORICAL_PROMOTION_AUTHORITY = False
PARAMETER_MUTATION_AUTHORITY = False
FORWARD_ONLY_COMPARISON = True
SAME_CANDIDATE_STREAM_REQUIRED = True
EXPECTED_SHORTFALL_TAIL_FRACTION = 0.20

FROZEN_FEATURE_IDS: tuple[str, ...] = (
    "candidate_continuity",
    "early_detection",
    "graduation_execution",
    "upside_capture",
    "detection_intelligence",
    "measurement_governance",
)

REGRET_CAUSES: frozenset[str] = frozenset(
    {
        "not_discovered",
        "discovered_too_late",
        "candidate_continuity_failure",
        "graduation_handoff_failure",
        "insufficient_monitoring_priority",
        "quote_failure",
        "chase_restriction",
        "evidence_maturity",
        "structural_hard_stop",
        "hazard_sizing",
        "undersizing",
        "failed_pyramid",
        "premature_exit",
        "runner_too_small",
        "failed_reentry",
        "correct_avoidance",
    }
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


def _return_fraction(value: Any, *, field: str) -> float:
    number = _finite(value, field=field)
    if number < -1.0:
        raise ValueError(f"{field}_below_total_loss")
    return number


def _text(value: Any, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field}_missing")
    return text


def _lane(value: Any) -> str:
    lane = _text(value, field="lane")
    if lane not in CANONICAL_LANES:
        raise ValueError("lane_unsupported")
    return lane


@dataclass(frozen=True)
class ExecutablePathPoint:
    """After-cost return snapshot with exact execution evidence."""

    timestamp_index: int
    net_return_fraction_after_costs: float
    exact_buy_quote_available: bool = True
    exact_sell_quote_available: bool = True
    structurally_exitable: bool = True
    latency_seconds: float = 0.0
    chase_fraction: float = 0.0

    def validated(self) -> "ExecutablePathPoint":
        if self.timestamp_index < 0:
            raise ValueError("timestamp_index_invalid")
        _return_fraction(
            self.net_return_fraction_after_costs,
            field="net_return_fraction_after_costs",
        )
        _nonnegative(self.latency_seconds, field="latency_seconds")
        _nonnegative(self.chase_fraction, field="chase_fraction")
        return self

    @property
    def legitimate_entry(self) -> bool:
        self.validated()
        return bool(
            self.exact_buy_quote_available
            and self.exact_sell_quote_available
            and self.structurally_exitable
            and self.latency_seconds <= IMMEDIATE_COPY_MAX_SECONDS
            and self.chase_fraction <= HIGH_CHASE_OBSERVE_ONLY_THRESHOLD
        )

    @property
    def executable_exit(self) -> bool:
        self.validated()
        return bool(
            self.exact_sell_quote_available
            and self.structurally_exitable
            and self.latency_seconds <= IMMEDIATE_COPY_MAX_SECONDS
        )


@dataclass(frozen=True)
class ExecutableExcursion:
    first_legitimate_executable_index: int
    last_executable_index: int
    executable_point_count: int
    nonexecutable_point_count_after_entry: int
    executable_mfe_fraction: float
    executable_mae_fraction: float
    realized_net_return_fraction: float | None
    capture_ratio: float | None
    measurement_complete: bool
    research_only: bool = True
    trading_authority: bool = False

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def measure_executable_excursion(
    points: Iterable[ExecutablePathPoint],
    *,
    realized_net_return_fraction: float | None = None,
) -> ExecutableExcursion:
    rows = sorted((item.validated() for item in points), key=lambda item: item.timestamp_index)
    if not rows:
        raise ValueError("executable_path_empty")
    if len({item.timestamp_index for item in rows}) != len(rows):
        raise ValueError("duplicate_timestamp_index")

    first = next((item for item in rows if item.legitimate_entry), None)
    if first is None:
        raise ValueError("no_legitimate_executable_opportunity")

    tail = [item for item in rows if item.timestamp_index >= first.timestamp_index]
    executable = [item for item in tail if item.executable_exit]
    if not executable:
        raise ValueError("no_executable_exit_observation")

    returns = [item.net_return_fraction_after_costs for item in executable]
    mfe = max(0.0, max(returns))
    mae = min(0.0, min(returns))
    realized: float | None = None
    capture: float | None = None
    if realized_net_return_fraction is not None:
        realized = _return_fraction(realized_net_return_fraction, field="realized_net_return_fraction")
        capture = realized / mfe if mfe > 0.0 else None

    return ExecutableExcursion(
        first_legitimate_executable_index=first.timestamp_index,
        last_executable_index=executable[-1].timestamp_index,
        executable_point_count=len(executable),
        nonexecutable_point_count_after_entry=len(tail) - len(executable),
        executable_mfe_fraction=mfe,
        executable_mae_fraction=mae,
        realized_net_return_fraction=realized,
        capture_ratio=capture,
        measurement_complete=True,
    )


@dataclass(frozen=True)
class DetectionCapture:
    first_legitimate_executable_index: int
    detection_index: int
    peak_executable_index: int
    total_executable_forward_upside_fraction: float
    executable_return_at_detection_fraction: float
    remaining_upside_at_detection_fraction: float
    detection_capture_ratio: float | None
    research_only: bool = True

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def measure_detection_capture(
    points: Iterable[ExecutablePathPoint],
    *,
    detection_index: int,
) -> DetectionCapture:
    if detection_index < 0:
        raise ValueError("detection_index_invalid")
    rows = sorted((item.validated() for item in points), key=lambda item: item.timestamp_index)
    first = next((item for item in rows if item.legitimate_entry), None)
    if first is None:
        raise ValueError("no_legitimate_executable_opportunity")
    executable = [
        item for item in rows
        if item.timestamp_index >= first.timestamp_index and item.executable_exit
    ]
    if not executable:
        raise ValueError("no_executable_exit_observation")
    observed_at_detection = [item for item in executable if item.timestamp_index <= detection_index]
    if not observed_at_detection:
        raise ValueError("detection_precedes_legitimate_executable_opportunity")
    detection_point = observed_at_detection[-1]
    peak = max(executable, key=lambda item: item.net_return_fraction_after_costs)
    total = max(0.0, peak.net_return_fraction_after_costs)
    remaining = max(0.0, peak.net_return_fraction_after_costs - detection_point.net_return_fraction_after_costs)
    ratio = remaining / total if total > 0.0 else None
    return DetectionCapture(
        first_legitimate_executable_index=first.timestamp_index,
        detection_index=detection_index,
        peak_executable_index=peak.timestamp_index,
        total_executable_forward_upside_fraction=total,
        executable_return_at_detection_fraction=detection_point.net_return_fraction_after_costs,
        remaining_upside_at_detection_fraction=remaining,
        detection_capture_ratio=ratio,
    )


@dataclass(frozen=True)
class LifecycleOutcome:
    candidate_id: str
    lane: str
    discovered: bool = True
    developing: bool = False
    pre_actionable: bool = False
    temporary_reject: bool = False
    reactivated: bool = False
    actionable: bool = False
    entered: bool = False
    successful_position: bool = False
    temporary_reject_later_profitable_actionable: bool = False

    def validated(self) -> "LifecycleOutcome":
        _text(self.candidate_id, field="candidate_id")
        _lane(self.lane)
        if self.temporary_reject_later_profitable_actionable and not self.temporary_reject:
            raise ValueError("profitable_reactivation_without_temporary_reject")
        if self.entered and not self.actionable:
            raise ValueError("entered_without_actionable")
        if self.successful_position and not self.entered:
            raise ValueError("successful_without_entry")
        return self


@dataclass(frozen=True)
class ActionabilityConversion:
    candidates_discovered: int
    developing_candidates: int
    pre_actionable_candidates: int
    temporary_rejects: int
    reactivations: int
    actionable_opportunities: int
    entered_opportunities: int
    successful_positions: int
    temporary_rejects_later_profitable_actionable: int
    temporary_reject_profitable_reactivation_rate: float | None
    research_only: bool = True

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def actionability_conversion(outcomes: Iterable[LifecycleOutcome]) -> ActionabilityConversion:
    rows = [item.validated() for item in outcomes]
    if len({item.candidate_id for item in rows}) != len(rows):
        raise ValueError("duplicate_candidate_id")
    temporary_rejects = sum(item.temporary_reject for item in rows)
    profitable = sum(item.temporary_reject_later_profitable_actionable for item in rows)
    return ActionabilityConversion(
        candidates_discovered=sum(item.discovered for item in rows),
        developing_candidates=sum(item.developing for item in rows),
        pre_actionable_candidates=sum(item.pre_actionable for item in rows),
        temporary_rejects=temporary_rejects,
        reactivations=sum(item.reactivated for item in rows),
        actionable_opportunities=sum(item.actionable for item in rows),
        entered_opportunities=sum(item.entered for item in rows),
        successful_positions=sum(item.successful_position for item in rows),
        temporary_rejects_later_profitable_actionable=profitable,
        temporary_reject_profitable_reactivation_rate=(
            profitable / temporary_rejects if temporary_rejects else None
        ),
    )


@dataclass(frozen=True)
class RegretRecord:
    candidate_id: str
    lane: str
    missed_executable_alpha_fraction: float
    causes: tuple[str, ...]

    def validated(self) -> "RegretRecord":
        _text(self.candidate_id, field="candidate_id")
        _lane(self.lane)
        missed = _nonnegative(
            self.missed_executable_alpha_fraction,
            field="missed_executable_alpha_fraction",
        )
        if not self.causes:
            raise ValueError("regret_causes_missing")
        normalized = tuple(dict.fromkeys(_text(cause, field="regret_cause") for cause in self.causes))
        unknown = [cause for cause in normalized if cause not in REGRET_CAUSES]
        if unknown:
            raise ValueError(f"unknown_regret_cause:{unknown[0]}")
        if "correct_avoidance" in normalized and (missed > 0.0 or len(normalized) > 1):
            raise ValueError("correct_avoidance_cannot_carry_regret")
        return RegretRecord(self.candidate_id, self.lane, missed, normalized)


@dataclass(frozen=True)
class RegretDecomposition:
    record_count: int
    total_missed_executable_alpha_fraction: float
    alpha_by_cause: Mapping[str, float]
    candidate_count_by_cause: Mapping[str, int]
    correct_avoidance_count: int
    research_only: bool = True

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["alpha_by_cause"] = dict(self.alpha_by_cause)
        payload["candidate_count_by_cause"] = dict(self.candidate_count_by_cause)
        return payload


def regret_decomposition(records: Iterable[RegretRecord]) -> RegretDecomposition:
    rows = [item.validated() for item in records]
    if len({item.candidate_id for item in rows}) != len(rows):
        raise ValueError("duplicate_candidate_id")
    alpha: dict[str, float] = {cause: 0.0 for cause in sorted(REGRET_CAUSES)}
    counts: dict[str, int] = {cause: 0 for cause in sorted(REGRET_CAUSES)}
    for item in rows:
        for cause in item.causes:
            alpha[cause] += item.missed_executable_alpha_fraction
            counts[cause] += 1
    return RegretDecomposition(
        record_count=len(rows),
        total_missed_executable_alpha_fraction=sum(item.missed_executable_alpha_fraction for item in rows),
        alpha_by_cause=alpha,
        candidate_count_by_cause=counts,
        correct_avoidance_count=counts["correct_avoidance"],
    )


@dataclass(frozen=True)
class StrategyCandidateOutcome:
    candidate_id: str
    lane: str
    stream_index: int
    strategy_version: str
    ground_truth_actionable: bool
    detected_actionable: bool
    entered: bool
    realized_net_return_fraction: float
    executable_mfe_fraction: float
    detection_latency_seconds: float | None
    slippage_fraction: float
    reject_regret_fraction: float
    exit_regret_fraction: float
    capital_utilization_fraction: float

    def validated(self) -> "StrategyCandidateOutcome":
        _text(self.candidate_id, field="candidate_id")
        _lane(self.lane)
        if self.stream_index < 0:
            raise ValueError("stream_index_invalid")
        _text(self.strategy_version, field="strategy_version")
        _return_fraction(self.realized_net_return_fraction, field="realized_net_return_fraction")
        _nonnegative(self.executable_mfe_fraction, field="executable_mfe_fraction")
        if self.detection_latency_seconds is not None:
            _nonnegative(self.detection_latency_seconds, field="detection_latency_seconds")
        _fraction(self.slippage_fraction, field="slippage_fraction")
        _nonnegative(self.reject_regret_fraction, field="reject_regret_fraction")
        _nonnegative(self.exit_regret_fraction, field="exit_regret_fraction")
        _fraction(self.capital_utilization_fraction, field="capital_utilization_fraction")
        if self.detected_actionable and self.detection_latency_seconds is None:
            raise ValueError("detected_actionable_missing_latency")
        if self.entered and not self.detected_actionable:
            raise ValueError("entered_without_detected_actionable")
        return self


@dataclass(frozen=True)
class StrategyMetrics:
    strategy_version: str
    candidate_count: int
    compounded_return_after_costs: float
    geometric_growth: float
    mean_detection_latency_seconds: float | None
    actionable_opportunity_recall: float | None
    aggregate_capture_ratio: float | None
    executable_mfe_captured_fraction: float
    max_drawdown_fraction: float
    expected_shortfall_fraction: float | None
    mean_slippage_fraction: float
    losing_trade_frequency: float | None
    reject_regret_fraction: float
    exit_regret_fraction: float
    mean_capital_utilization_fraction: float

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _max_drawdown(returns: Sequence[float]) -> float:
    equity = 1.0
    peak = 1.0
    worst = 0.0
    for ret in returns:
        equity *= 1.0 + ret
        peak = max(peak, equity)
        if peak > 0.0:
            worst = max(worst, (peak - equity) / peak)
    return worst


def _expected_shortfall(returns: Sequence[float]) -> float | None:
    if not returns:
        return None
    ordered = sorted(returns)
    tail_count = max(1, math.ceil(len(ordered) * EXPECTED_SHORTFALL_TAIL_FRACTION))
    return mean(ordered[:tail_count])


def summarize_strategy(outcomes: Iterable[StrategyCandidateOutcome]) -> StrategyMetrics:
    rows = sorted((item.validated() for item in outcomes), key=lambda item: item.stream_index)
    if not rows:
        raise ValueError("strategy_outcomes_empty")
    if len({item.candidate_id for item in rows}) != len(rows):
        raise ValueError("duplicate_candidate_id")
    versions = {item.strategy_version for item in rows}
    if len(versions) != 1:
        raise ValueError("mixed_strategy_versions")
    returns = [item.realized_net_return_fraction for item in rows]
    wealth = math.prod(1.0 + ret for ret in returns)
    compounded = wealth - 1.0
    geometric = wealth ** (1.0 / len(rows)) - 1.0 if wealth > 0.0 else -1.0
    latencies = [
        item.detection_latency_seconds
        for item in rows
        if item.detected_actionable and item.detection_latency_seconds is not None
    ]
    actionable = sum(item.ground_truth_actionable for item in rows)
    detected = sum(item.ground_truth_actionable and item.detected_actionable for item in rows)
    entered = [item for item in rows if item.entered]
    total_mfe = sum(item.executable_mfe_fraction for item in entered)
    realized_for_capture = sum(item.realized_net_return_fraction for item in entered)
    captured_mfe = sum(
        min(max(item.realized_net_return_fraction, 0.0), item.executable_mfe_fraction)
        for item in entered
    )
    losses = sum(item.realized_net_return_fraction < 0.0 for item in entered)
    return StrategyMetrics(
        strategy_version=next(iter(versions)),
        candidate_count=len(rows),
        compounded_return_after_costs=compounded,
        geometric_growth=geometric,
        mean_detection_latency_seconds=mean(latencies) if latencies else None,
        actionable_opportunity_recall=detected / actionable if actionable else None,
        aggregate_capture_ratio=realized_for_capture / total_mfe if total_mfe > 0.0 else None,
        executable_mfe_captured_fraction=captured_mfe,
        max_drawdown_fraction=_max_drawdown(returns),
        expected_shortfall_fraction=_expected_shortfall(returns),
        mean_slippage_fraction=mean(item.slippage_fraction for item in rows),
        losing_trade_frequency=losses / len(entered) if entered else None,
        reject_regret_fraction=sum(item.reject_regret_fraction for item in rows),
        exit_regret_fraction=sum(item.exit_regret_fraction for item in rows),
        mean_capital_utilization_fraction=mean(item.capital_utilization_fraction for item in rows),
    )


@dataclass(frozen=True)
class FrozenChallengerComparison:
    incumbent: StrategyMetrics
    challenger: StrategyMetrics
    identical_candidate_stream: bool
    identical_stream_order: bool
    frozen_hypotheses: bool
    forward_only: bool
    promotion_authority: bool = False
    parameter_mutation_authority: bool = False
    research_only: bool = True

    def as_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "incumbent": self.incumbent.as_dict(),
            "challenger": self.challenger.as_dict(),
        }


def compare_frozen_challenger(
    incumbent_outcomes: Iterable[StrategyCandidateOutcome],
    challenger_outcomes: Iterable[StrategyCandidateOutcome],
) -> FrozenChallengerComparison:
    incumbent_rows = [item.validated() for item in incumbent_outcomes]
    challenger_rows = [item.validated() for item in challenger_outcomes]
    if {item.strategy_version for item in incumbent_rows} != {INCUMBENT_VERSION}:
        raise ValueError("incumbent_version_mismatch")
    if {item.strategy_version for item in challenger_rows} != {CHALLENGER_VERSION}:
        raise ValueError("challenger_version_mismatch")
    inc_ids = {item.candidate_id for item in incumbent_rows}
    chal_ids = {item.candidate_id for item in challenger_rows}
    if inc_ids != chal_ids:
        raise ValueError("candidate_stream_mismatch")
    inc_index = {item.candidate_id: (item.stream_index, item.lane) for item in incumbent_rows}
    chal_index = {item.candidate_id: (item.stream_index, item.lane) for item in challenger_rows}
    if inc_index != chal_index:
        raise ValueError("candidate_stream_order_or_lane_mismatch")
    return FrozenChallengerComparison(
        incumbent=summarize_strategy(incumbent_rows),
        challenger=summarize_strategy(challenger_rows),
        identical_candidate_stream=True,
        identical_stream_order=True,
        frozen_hypotheses=True,
        forward_only=FORWARD_ONLY_COMPARISON,
    )


@dataclass(frozen=True)
class IncrementalSignalPair:
    candidate_id: str
    baseline_return_fraction: float
    augmented_return_fraction: float

    def validated(self) -> "IncrementalSignalPair":
        _text(self.candidate_id, field="candidate_id")
        _return_fraction(self.baseline_return_fraction, field="baseline_return_fraction")
        _return_fraction(self.augmented_return_fraction, field="augmented_return_fraction")
        return self


@dataclass(frozen=True)
class IncrementalSignalAlpha:
    signal_name: str
    sample_count: int
    minimum_sample_count: int
    eligible: bool
    mean_incremental_return_fraction: float | None
    median_incremental_return_fraction: float | None
    positive_delta_frequency: float | None
    positive_incremental_forward_value_observed: bool
    reason: str
    promotion_authority: bool = False
    research_only: bool = True

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def assess_incremental_signal_alpha(
    signal_name: str,
    pairs: Iterable[IncrementalSignalPair],
    *,
    minimum_sample_count: int,
) -> IncrementalSignalAlpha:
    name = _text(signal_name, field="signal_name")
    if minimum_sample_count <= 0:
        raise ValueError("minimum_sample_count_invalid")
    rows = [item.validated() for item in pairs]
    if len({item.candidate_id for item in rows}) != len(rows):
        raise ValueError("duplicate_candidate_id")
    if len(rows) < minimum_sample_count:
        return IncrementalSignalAlpha(
            signal_name=name,
            sample_count=len(rows),
            minimum_sample_count=minimum_sample_count,
            eligible=False,
            mean_incremental_return_fraction=None,
            median_incremental_return_fraction=None,
            positive_delta_frequency=None,
            positive_incremental_forward_value_observed=False,
            reason="insufficient_forward_sample",
        )
    deltas = [item.augmented_return_fraction - item.baseline_return_fraction for item in rows]
    mean_delta = mean(deltas)
    median_delta = median(deltas)
    positive_frequency = sum(delta > 0.0 for delta in deltas) / len(deltas)
    positive = mean_delta > 0.0 and median_delta >= 0.0 and positive_frequency > 0.5
    return IncrementalSignalAlpha(
        signal_name=name,
        sample_count=len(rows),
        minimum_sample_count=minimum_sample_count,
        eligible=True,
        mean_incremental_return_fraction=mean_delta,
        median_incremental_return_fraction=median_delta,
        positive_delta_frequency=positive_frequency,
        positive_incremental_forward_value_observed=positive,
        reason="measured_forward_incremental_value" if positive else "incremental_forward_value_not_demonstrated",
    )


def frozen_challenger_manifest() -> dict[str, Any]:
    payload = {
        "batch_version": BATCH_VERSION,
        "freeze_id": FREEZE_ID,
        "challenger_version": CHALLENGER_VERSION,
        "challenger_epoch": CHALLENGER_EPOCH,
        "incumbent_version": INCUMBENT_VERSION,
        "frozen_feature_ids": list(FROZEN_FEATURE_IDS),
        "forward_only_comparison": FORWARD_ONLY_COMPARISON,
        "same_candidate_stream_required": SAME_CANDIDATE_STREAM_REQUIRED,
        "parameter_mutation_authority": PARAMETER_MUTATION_AUTHORITY,
        "historical_promotion_authority": HISTORICAL_PROMOTION_AUTHORITY,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {**payload, "freeze_fingerprint_sha256": hashlib.sha256(encoded).hexdigest()}


def safety_manifest() -> dict[str, Any]:
    return {
        **frozen_challenger_manifest(),
        "incumbent_remains_authoritative": True,
        "incumbent_authority_changed": INCUMBENT_AUTHORITY_CHANGED,
        "challenger_entry_authority": CHALLENGER_ENTRY_AUTHORITY,
        "research_only": True,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        "production_composition_hook": PRODUCTION_COMPOSITION_HOOK,
        "immediate_copy_max_seconds": IMMEDIATE_COPY_MAX_SECONDS,
        "high_chase_observe_only_threshold": HIGH_CHASE_OBSERVE_ONLY_THRESHOLD,
        "exact_amount_specific_entry_quotes_preserved": True,
        "exact_amount_specific_exit_quotes_preserved": True,
        "structural_exit_hard_stops_preserved": True,
        "hazards_remain_evidence_or_sizing_modifiers": True,
        "averaging_down_permitted": False,
        "measurement_can_change_strategy_parameters": False,
        "comparison_can_promote_challenger": False,
        "regret_causes": sorted(REGRET_CAUSES),
        "canonical_lanes": list(CANONICAL_LANES),
    }


__all__ = [
    "ActionabilityConversion",
    "BATCH_VERSION",
    "DetectionCapture",
    "ExecutableExcursion",
    "ExecutablePathPoint",
    "FREEZE_ID",
    "FrozenChallengerComparison",
    "IncrementalSignalAlpha",
    "IncrementalSignalPair",
    "LifecycleOutcome",
    "REGRET_CAUSES",
    "RegretDecomposition",
    "RegretRecord",
    "StrategyCandidateOutcome",
    "StrategyMetrics",
    "actionability_conversion",
    "assess_incremental_signal_alpha",
    "compare_frozen_challenger",
    "frozen_challenger_manifest",
    "measure_detection_capture",
    "measure_executable_excursion",
    "regret_decomposition",
    "safety_manifest",
    "summarize_strategy",
]
