from __future__ import annotations

"""v5.2 Batch 5: research-only detection intelligence.

Measures independent flow, wallet cascades, liquidity/concentration trajectories,
seller pressure, hazard direction, and exact-cohort anomalies. This module has no
production composition hook or entry authority.
"""

import math
from dataclasses import asdict, dataclass
from statistics import median
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

BATCH_VERSION = "v52-batch5-detection-intelligence-1"
PRODUCTION_COMPOSITION_HOOK = False
CHALLENGER_ENTRY_AUTHORITY = False
INCUMBENT_AUTHORITY_CHANGED = False
CREATOR_FUNDER_TRADING_AUTHORITY = False
DISCOVERED_WALLET_INITIAL_SIGNAL_WEIGHT = 0.0


def _finite(value: Any, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field}_invalid") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field}_invalid")
    return number


def _nonnegative(value: Any, field: str) -> float:
    number = _finite(value, field)
    if number < 0:
        raise ValueError(f"{field}_invalid")
    return number


def _fraction(value: Any, field: str) -> float:
    number = _nonnegative(value, field)
    if number > 1:
        raise ValueError(f"{field}_invalid")
    return number


def _text(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field}_missing")
    return text


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _growth(start: float, end: float) -> float:
    return 0.0 if start <= 0 and end <= 0 else (1.0 if start <= 0 else (end - start) / start)


def _percentile(values: Sequence[float], p: float) -> float:
    if not values:
        return 0.0
    p = _fraction(p, "percentile")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    index = p * (len(ordered) - 1)
    lo, hi = math.floor(index), math.ceil(index)
    if lo == hi:
        return ordered[lo]
    w = index - lo
    return ordered[lo] * (1 - w) + ordered[hi] * w


def _empirical(value: float, population: Sequence[float]) -> float:
    if not population:
        return 0.0
    less = sum(item < value for item in population)
    equal = sum(item == value for item in population)
    return (less + 0.5 * equal) / len(population)


@dataclass(frozen=True)
class FlowPrint:
    wallet_id: str
    funding_cluster_id: str | None
    side: str
    notional: float
    historical_alpha: float = 0.0
    timestamp_index: int = 0

    def validated(self) -> "FlowPrint":
        _text(self.wallet_id, "wallet_id")
        if str(self.side).lower() not in {"buy", "sell"}:
            raise ValueError("side_invalid")
        _nonnegative(self.notional, "notional")
        _finite(self.historical_alpha, "historical_alpha")
        if self.timestamp_index < 0:
            raise ValueError("timestamp_index_invalid")
        return self

    @property
    def cluster(self) -> str:
        return str(self.funding_cluster_id or "").strip() or f"wallet:{self.wallet_id.strip()}"


@dataclass(frozen=True)
class FlowMetrics:
    buy_transaction_count: int
    sell_transaction_count: int
    unique_buyer_count: int
    independent_buyer_count: int
    repeat_buyer_count: int
    buyer_breadth: float
    common_funding_ratio: float
    buy_notional: float
    sell_notional: float
    dollar_buy_sell_imbalance: float
    weighted_wallet_quality: float
    independent_buy_notional_fraction: float

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def independent_flow_metrics(prints: Iterable[FlowPrint]) -> FlowMetrics:
    rows = [item.validated() for item in prints]
    buys = [item for item in rows if item.side.lower() == "buy"]
    sells = [item for item in rows if item.side.lower() == "sell"]
    wallet_counts: dict[str, int] = {}
    cluster_max: dict[str, float] = {}
    for item in buys:
        wallet_counts[item.wallet_id] = wallet_counts.get(item.wallet_id, 0) + 1
        cluster_max[item.cluster] = max(cluster_max.get(item.cluster, 0.0), item.notional)
    unique = len(wallet_counts)
    independent = len(cluster_max)
    breadth = independent / unique if unique else 0.0
    buy_notional = sum(item.notional for item in buys)
    sell_notional = sum(item.notional for item in sells)
    total = buy_notional + sell_notional
    return FlowMetrics(
        buy_transaction_count=len(buys),
        sell_transaction_count=len(sells),
        unique_buyer_count=unique,
        independent_buyer_count=independent,
        repeat_buyer_count=sum(count > 1 for count in wallet_counts.values()),
        buyer_breadth=_clamp01(breadth),
        common_funding_ratio=_clamp01(1 - breadth) if unique else 0.0,
        buy_notional=buy_notional,
        sell_notional=sell_notional,
        dollar_buy_sell_imbalance=(buy_notional - sell_notional) / total if total else 0.0,
        weighted_wallet_quality=(
            sum(item.notional * item.historical_alpha for item in buys) / buy_notional
            if buy_notional else 0.0
        ),
        independent_buy_notional_fraction=(
            _clamp01(sum(cluster_max.values()) / buy_notional) if buy_notional else 0.0
        ),
    )


@dataclass(frozen=True)
class ParticipationQuality:
    score: float
    components: Mapping[str, float]
    research_only: bool = True
    observation_authority: bool = True
    trading_authority: bool = False

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["components"] = dict(self.components)
        return payload


def quality_of_participation(
    flow: FlowMetrics,
    *,
    holder_growth_fraction: float,
    concentration_dispersion: float,
    concentration_improving: bool,
) -> ParticipationQuality:
    holder_growth = _finite(holder_growth_fraction, "holder_growth_fraction")
    repeat_rate = flow.repeat_buyer_count / flow.unique_buyer_count if flow.unique_buyer_count else 0.0
    parts = {
        "independent_buyer_breadth": flow.buyer_breadth,
        "independent_buy_notional": flow.independent_buy_notional_fraction,
        "funding_independence": 1 - flow.common_funding_ratio,
        "repeat_buyer_rate": _clamp01(repeat_rate),
        "wallet_historical_alpha": _clamp01(flow.weighted_wallet_quality),
        "holder_growth": _clamp01(max(0.0, holder_growth)),
        "concentration_dispersion": _fraction(concentration_dispersion, "concentration_dispersion"),
        "concentration_direction": 1.0 if concentration_improving else 0.0,
    }
    return ParticipationQuality(sum(parts.values()) / len(parts), parts)


@dataclass(frozen=True)
class WalletCascade:
    detected: bool
    skilled_independent_cluster_count: int
    broad_independent_cluster_count: int
    ordered_skilled_clusters: tuple[str, ...]
    reasons: tuple[str, ...]
    research_only: bool = True
    trading_authority: bool = False

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def detect_wallet_cascade(
    prints: Iterable[FlowPrint],
    *,
    minimum_wallet_quality: float,
    minimum_skilled_independent_clusters: int,
    minimum_broad_independent_clusters: int,
) -> WalletCascade:
    quality = _finite(minimum_wallet_quality, "minimum_wallet_quality")
    if minimum_skilled_independent_clusters < 2:
        raise ValueError("minimum_skilled_independent_clusters_invalid")
    if minimum_broad_independent_clusters < minimum_skilled_independent_clusters:
        raise ValueError("minimum_broad_independent_clusters_invalid")
    buys = sorted(
        [item.validated() for item in prints if item.side.lower() == "buy"],
        key=lambda item: item.timestamp_index,
    )
    broad: list[str] = []
    skilled: list[str] = []
    for item in buys:
        if item.cluster not in broad:
            broad.append(item.cluster)
        if item.historical_alpha >= quality and item.cluster not in skilled:
            skilled.append(item.cluster)
    reasons: list[str] = []
    if len(skilled) < minimum_skilled_independent_clusters:
        reasons.append("insufficient_independent_skilled_wallet_sequence")
    if len(broad) < minimum_broad_independent_clusters:
        reasons.append("insufficient_broad_independent_follow_through")
    detected = not reasons
    if detected:
        reasons.append("independent_wallet_cascade_observed")
    return WalletCascade(detected, len(skilled), len(broad), tuple(skilled), tuple(reasons))


@dataclass(frozen=True)
class DiscoveredWallet:
    wallet_id: str
    discovery_source: str
    initial_signal_weight: float
    prospective_validation_required: bool
    observation_authority: bool
    trading_authority: bool


def discover_wallets_from_successful_candidate(
    prints: Iterable[FlowPrint],
    *,
    candidate_id: str,
    candidate_success_confirmed: bool,
    known_wallet_ids: Iterable[str] = (),
) -> tuple[DiscoveredWallet, ...]:
    candidate = _text(candidate_id, "candidate_id")
    if not candidate_success_confirmed:
        return ()
    known = {str(item).strip() for item in known_wallet_ids}
    found: dict[str, DiscoveredWallet] = {}
    for item in prints:
        item.validated()
        if item.side.lower() != "buy" or item.wallet_id in known or item.wallet_id in found:
            continue
        found[item.wallet_id] = DiscoveredWallet(
            item.wallet_id,
            f"successful_candidate:{candidate}",
            DISCOVERED_WALLET_INITIAL_SIGNAL_WEIGHT,
            True,
            True,
            False,
        )
    return tuple(found[key] for key in sorted(found))


@dataclass(frozen=True)
class CreatorFunderPriority:
    base_priority: float
    propagated_priority: float
    decayed_increment: float
    incremental_alpha_validated: bool
    observation_authority: bool = True
    trading_authority: bool = False


def creator_funder_priority(
    *,
    base_priority: float,
    validated_increment: float,
    age_minutes: float,
    half_life_minutes: float,
    incremental_alpha_validated: bool,
) -> CreatorFunderPriority:
    base = _nonnegative(base_priority, "base_priority")
    increment = _nonnegative(validated_increment, "validated_increment")
    age = _nonnegative(age_minutes, "age_minutes")
    half_life = _nonnegative(half_life_minutes, "half_life_minutes")
    if half_life <= 0:
        raise ValueError("half_life_minutes_invalid")
    decayed = increment * 0.5 ** (age / half_life) if incremental_alpha_validated else 0.0
    return CreatorFunderPriority(base, base + decayed, decayed, bool(incremental_alpha_validated))


@dataclass(frozen=True)
class LiquidityPoint:
    liquidity_notional: float
    executable_sell_depth_notional: float
    market_cap_notional: float
    constant_notional_slippage_fraction: float

    def validated(self) -> "LiquidityPoint":
        for name in (
            "liquidity_notional",
            "executable_sell_depth_notional",
            "market_cap_notional",
            "constant_notional_slippage_fraction",
        ):
            _nonnegative(getattr(self, name), name)
        return self


@dataclass(frozen=True)
class LiquidityTrajectory:
    liquidity_growth_fraction: float
    sell_depth_growth_fraction: float
    liquidity_market_cap_ratio_start: float
    liquidity_market_cap_ratio_end: float
    liquidity_market_cap_ratio_change: float
    sell_depth_position_size_ratio: float
    constant_notional_slippage_improvement: float
    direction: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def liquidity_trajectory(
    start: LiquidityPoint, end: LiquidityPoint, *, position_notional: float
) -> LiquidityTrajectory:
    start.validated()
    end.validated()
    position = _nonnegative(position_notional, "position_notional")
    lg = _growth(start.liquidity_notional, end.liquidity_notional)
    dg = _growth(start.executable_sell_depth_notional, end.executable_sell_depth_notional)
    sr = start.liquidity_notional / start.market_cap_notional if start.market_cap_notional else 0.0
    er = end.liquidity_notional / end.market_cap_notional if end.market_cap_notional else 0.0
    slip = start.constant_notional_slippage_fraction - end.constant_notional_slippage_fraction
    positives = sum(value > 0 for value in (lg, dg, slip))
    negatives = sum(value < 0 for value in (lg, dg, slip))
    direction = (
        "supportive" if positives >= 2 and not negatives
        else "deteriorating" if negatives >= 2 and not positives
        else "stable" if not positives and not negatives
        else "mixed"
    )
    return LiquidityTrajectory(
        lg, dg, sr, er, er - sr,
        end.executable_sell_depth_notional / position if position else math.inf,
        slip, direction,
    )


@dataclass(frozen=True)
class ConcentrationTrajectory:
    start_fraction: float
    end_fraction: float
    delta_fraction: float
    direction: str
    research_only: bool = True

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def concentration_trajectory(values: Sequence[float]) -> ConcentrationTrajectory:
    if len(values) < 2:
        raise ValueError("concentration_series_too_short")
    vals = [_fraction(value, "concentration_fraction") for value in values]
    delta = vals[-1] - vals[0]
    direction = "healthy_distribution" if delta < -1e-9 else "increasing_concentration" if delta > 1e-9 else "stable"
    return ConcentrationTrajectory(vals[0], vals[-1], delta, direction)


@dataclass(frozen=True)
class SellerPrint:
    wallet_id: str
    side: str
    notional: float
    price_response_fraction: float = 0.0
    early_wallet: bool = False

    def validated(self) -> "SellerPrint":
        _text(self.wallet_id, "wallet_id")
        if self.side.lower() not in {"buy", "sell"}:
            raise ValueError("side_invalid")
        _nonnegative(self.notional, "notional")
        _finite(self.price_response_fraction, "price_response_fraction")
        return self


@dataclass(frozen=True)
class SellerPressure:
    buy_notional: float
    sell_notional: float
    median_buy_notional: float
    median_sell_notional: float
    p90_sell_notional: float
    largest_seller_notional: float
    largest_seller_share: float
    repeat_seller_count: int
    early_wallet_sell_fraction: float
    lp_withdrawal_fraction: float
    price_response_per_dollar_sold: float
    pressure_score: float
    classification: str
    research_only: bool = True
    trading_authority: bool = False

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def seller_pressure(
    prints: Iterable[SellerPrint], *, lp_withdrawal_fraction: float
) -> SellerPressure:
    rows = [item.validated() for item in prints]
    lp = _fraction(lp_withdrawal_fraction, "lp_withdrawal_fraction")
    buys = [item for item in rows if item.side.lower() == "buy"]
    sells = [item for item in rows if item.side.lower() == "sell"]
    bv = [item.notional for item in buys]
    sv = [item.notional for item in sells]
    bn, sn = sum(bv), sum(sv)
    totals: dict[str, float] = {}
    counts: dict[str, int] = {}
    for item in sells:
        totals[item.wallet_id] = totals.get(item.wallet_id, 0.0) + item.notional
        counts[item.wallet_id] = counts.get(item.wallet_id, 0) + 1
    largest = max(totals.values(), default=0.0)
    early = sum(item.notional for item in sells if item.early_wallet)
    flow_share = sn / (bn + sn) if bn + sn else 0.0
    repeat_share = sum(count > 1 for count in counts.values()) / max(1, len(counts))
    size_ratio = (median(sv) if sv else 0.0) / max(median(bv) if bv else 0.0, 1e-12)
    components = (
        flow_share,
        _clamp01(largest / sn) if sn else 0.0,
        _clamp01(repeat_share),
        _clamp01(early / sn) if sn else 0.0,
        lp,
        _clamp01(size_ratio),
    )
    score = sum(components) / len(components)
    classification = "high_distribution_pressure" if score >= 0.67 else "elevated_distribution_pressure" if score >= 0.40 else "controlled_distribution_pressure"
    response = (
        sum(abs(min(0.0, item.price_response_fraction)) * item.notional for item in sells) / sn
        if sn else 0.0
    )
    return SellerPressure(
        bn, sn, median(bv) if bv else 0.0, median(sv) if sv else 0.0,
        _percentile(sv, 0.90), largest, _clamp01(largest / sn) if sn else 0.0,
        sum(count > 1 for count in counts.values()), _clamp01(early / sn) if sn else 0.0,
        lp, response, score, classification,
    )


@dataclass(frozen=True)
class HazardPoint:
    severity: float
    liquidity_notional: float
    participation_quality: float

    def validated(self) -> "HazardPoint":
        _nonnegative(self.severity, "severity")
        _nonnegative(self.liquidity_notional, "liquidity_notional")
        _fraction(self.participation_quality, "participation_quality")
        return self


@dataclass(frozen=True)
class HazardDirection:
    current_severity: float
    severity_slope: float
    worsening_interval_count: int
    liquidity_change_fraction: float
    participation_quality_change: float
    direction: str
    can_override_structural_hard_stop: bool = False
    research_only: bool = True

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def dynamic_hazard_direction(points: Sequence[HazardPoint]) -> HazardDirection:
    if len(points) < 2:
        raise ValueError("hazard_series_too_short")
    rows = [item.validated() for item in points]
    slope = (rows[-1].severity - rows[0].severity) / (len(rows) - 1)
    worsening = sum(cur.severity > prev.severity for prev, cur in zip(rows, rows[1:]))
    liquidity = _growth(rows[0].liquidity_notional, rows[-1].liquidity_notional)
    participation = rows[-1].participation_quality - rows[0].participation_quality
    direction = "improving" if slope < 0 and liquidity >= 0 and participation >= 0 else "deteriorating" if slope > 0 and (liquidity < 0 or participation < 0) else "mixed_or_stable"
    return HazardDirection(rows[-1].severity, slope, worsening, liquidity, participation, direction)


@dataclass(frozen=True)
class CohortObservation:
    candidate_id: str
    token_age_bucket: str
    venue: str
    lifecycle_stage: str
    liquidity_bucket: str
    market_cap_bucket: str
    launch_mechanism: str
    hazard_class: str
    market_regime: str
    independent_buyer_acceleration: float
    liquidity_growth_fraction: float
    participation_quality: float
    seller_pressure_score: float

    def validated(self) -> "CohortObservation":
        _text(self.candidate_id, "candidate_id")
        for field in (
            "token_age_bucket", "venue", "lifecycle_stage", "liquidity_bucket",
            "market_cap_bucket", "launch_mechanism", "hazard_class", "market_regime",
        ):
            _text(getattr(self, field), field)
        _finite(self.independent_buyer_acceleration, "independent_buyer_acceleration")
        _finite(self.liquidity_growth_fraction, "liquidity_growth_fraction")
        _fraction(self.participation_quality, "participation_quality")
        _fraction(self.seller_pressure_score, "seller_pressure_score")
        return self

    @property
    def cohort_key(self) -> tuple[str, ...]:
        return (
            self.token_age_bucket, self.venue, self.lifecycle_stage, self.liquidity_bucket,
            self.market_cap_bucket, self.launch_mechanism, self.hazard_class, self.market_regime,
        )


@dataclass(frozen=True)
class CohortAnomaly:
    candidate_id: str
    comparable_peer_count: int
    independent_buyer_acceleration_percentile: float
    liquidity_growth_percentile: float
    participation_quality_percentile: float
    low_seller_pressure_percentile: float
    anomaly_threshold: float
    anomaly_detected: bool
    eligible: bool
    reason: str
    research_only: bool = True
    trading_authority: bool = False

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def cohort_relative_anomaly(
    candidate: CohortObservation,
    peers: Iterable[CohortObservation],
    *,
    minimum_peer_count: int,
    anomaly_percentile_threshold: float,
) -> CohortAnomaly:
    candidate.validated()
    if minimum_peer_count < 1:
        raise ValueError("minimum_peer_count_invalid")
    threshold = _fraction(anomaly_percentile_threshold, "anomaly_percentile_threshold")
    matching: list[CohortObservation] = []
    for peer in peers:
        peer.validated()
        if peer.cohort_key == candidate.cohort_key and peer.candidate_id != candidate.candidate_id:
            matching.append(peer)
    if len(matching) < minimum_peer_count:
        return CohortAnomaly(candidate.candidate_id, len(matching), 0, 0, 0, 0, threshold, False, False, "insufficient_comparable_cohort")
    flow = _empirical(candidate.independent_buyer_acceleration, [p.independent_buyer_acceleration for p in matching])
    liquidity = _empirical(candidate.liquidity_growth_fraction, [p.liquidity_growth_fraction for p in matching])
    participation = _empirical(candidate.participation_quality, [p.participation_quality for p in matching])
    low_seller = 1 - _empirical(candidate.seller_pressure_score, [p.seller_pressure_score for p in matching])
    detected = flow >= threshold
    return CohortAnomaly(candidate.candidate_id, len(matching), flow, liquidity, participation, low_seller, threshold, detected, True, "cohort_relative_anomaly_observed" if detected else "within_cohort_distribution")


@dataclass(frozen=True)
class DetectionIntelligenceSnapshot:
    candidate_id: str
    lane: str
    independent_flow: FlowMetrics
    participation_quality: ParticipationQuality
    wallet_cascade: WalletCascade
    liquidity: LiquidityTrajectory
    concentration: ConcentrationTrajectory
    seller_pressure: SellerPressure
    hazard_direction: HazardDirection
    cohort_anomaly: CohortAnomaly
    research_only: bool = True
    trading_authority: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "lane": self.lane,
            "independent_flow": self.independent_flow.as_dict(),
            "participation_quality": self.participation_quality.as_dict(),
            "wallet_cascade": self.wallet_cascade.as_dict(),
            "liquidity": self.liquidity.as_dict(),
            "concentration": self.concentration.as_dict(),
            "seller_pressure": self.seller_pressure.as_dict(),
            "hazard_direction": self.hazard_direction.as_dict(),
            "cohort_anomaly": self.cohort_anomaly.as_dict(),
            "research_only": True,
            "trading_authority": False,
        }


def build_detection_intelligence_snapshot(
    *,
    candidate_id: str,
    lane: str,
    flow_prints: Iterable[FlowPrint],
    holder_growth_fraction: float,
    concentration_dispersion: float,
    concentration_values: Sequence[float],
    liquidity_start: LiquidityPoint,
    liquidity_end: LiquidityPoint,
    position_notional: float,
    seller_prints: Iterable[SellerPrint],
    lp_withdrawal_fraction: float,
    hazard_points: Sequence[HazardPoint],
    cohort_candidate: CohortObservation,
    cohort_peers: Iterable[CohortObservation],
    minimum_wallet_quality: float,
    minimum_skilled_independent_clusters: int,
    minimum_broad_independent_clusters: int,
    minimum_peer_count: int,
    anomaly_percentile_threshold: float,
) -> DetectionIntelligenceSnapshot:
    candidate = _text(candidate_id, "candidate_id")
    if lane not in CANONICAL_LANES:
        raise ValueError("lane_unsupported")
    flow_rows = tuple(flow_prints)
    concentration = concentration_trajectory(concentration_values)
    flow = independent_flow_metrics(flow_rows)
    return DetectionIntelligenceSnapshot(
        candidate,
        lane,
        flow,
        quality_of_participation(
            flow,
            holder_growth_fraction=holder_growth_fraction,
            concentration_dispersion=concentration_dispersion,
            concentration_improving=concentration.direction == "healthy_distribution",
        ),
        detect_wallet_cascade(
            flow_rows,
            minimum_wallet_quality=minimum_wallet_quality,
            minimum_skilled_independent_clusters=minimum_skilled_independent_clusters,
            minimum_broad_independent_clusters=minimum_broad_independent_clusters,
        ),
        liquidity_trajectory(liquidity_start, liquidity_end, position_notional=position_notional),
        concentration,
        seller_pressure(seller_prints, lp_withdrawal_fraction=lp_withdrawal_fraction),
        dynamic_hazard_direction(hazard_points),
        cohort_relative_anomaly(
            cohort_candidate,
            cohort_peers,
            minimum_peer_count=minimum_peer_count,
            anomaly_percentile_threshold=anomaly_percentile_threshold,
        ),
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
        "canonical_lanes": list(CANONICAL_LANES),
        "immediate_copy_max_seconds": IMMEDIATE_COPY_MAX_SECONDS,
        "high_chase_observe_only_threshold": HIGH_CHASE_OBSERVE_ONLY_THRESHOLD,
        "independent_flow_is_observation_only": True,
        "wallet_cascades_are_observation_only": True,
        "discovered_wallet_initial_signal_weight": DISCOVERED_WALLET_INITIAL_SIGNAL_WEIGHT,
        "discovered_wallet_prospective_validation_required": True,
        "creator_funder_trading_authority": CREATOR_FUNDER_TRADING_AUTHORITY,
        "creator_funder_incremental_alpha_required": True,
        "creator_funder_priority_decay_required": True,
        "liquidity_trajectory_is_evidence_modifier": True,
        "concentration_trajectory_is_evidence_modifier": True,
        "seller_pressure_is_exit_and_sizing_evidence": True,
        "hazard_direction_cannot_override_structural_hard_stops": True,
        "cohort_exact_comparability_required": True,
        "cohort_insufficient_sample_fails_closed": True,
        "economic_thresholds_changed": False,
        "no_averaging_down_preserved": True,
        "exact_two_sided_quotes_preserved": True,
        "structural_exit_hard_stops_preserved": True,
    }


__all__ = [
    "BATCH_VERSION", "CohortAnomaly", "CohortObservation", "ConcentrationTrajectory",
    "CreatorFunderPriority", "DetectionIntelligenceSnapshot", "DiscoveredWallet",
    "FlowMetrics", "FlowPrint", "HazardDirection", "HazardPoint", "LiquidityPoint",
    "LiquidityTrajectory", "ParticipationQuality", "SellerPressure", "SellerPrint",
    "WalletCascade", "build_detection_intelligence_snapshot", "cohort_relative_anomaly",
    "concentration_trajectory", "creator_funder_priority", "detect_wallet_cascade",
    "discover_wallets_from_successful_candidate", "dynamic_hazard_direction",
    "independent_flow_metrics", "liquidity_trajectory", "quality_of_participation",
    "safety_manifest", "seller_pressure",
]
