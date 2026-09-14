from __future__ import annotations

import math
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from types import MethodType
from typing import Any, Iterable, Mapping, Sequence

from . import v52_market_validation_controls as base
from .strategy_v52_authority import target_sizing_policy

VERSION = "v52-market-validation-governance-v1"

WINDOWS = {
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
}
TRANSITION_BUCKET_MINUTES = 15
DEACTIVATION_PERSISTENCE_BUCKETS = 3
REACTIVATION_PERSISTENCE_BUCKETS = 3
POSTERIOR_PRIOR_STRENGTH = 8.0
POSITIVE_PROBABILITY_ACTIVATE = 0.80
POSITIVE_PROBABILITY_DEACTIVATE = 0.45

GRADUATION_FEATURE_FAMILIES: dict[str, tuple[str, ...]] = {
    "timing": (
        "graduation_speed_seconds",
        "acceleration_into_graduation",
    ),
    "participation": (
        "independent_buyer_breadth",
        "independent_buyer_growth",
        "transaction_acceleration",
        "high_forward_alpha_wallet_participation",
    ),
    "flow": (
        "buy_sell_imbalance",
        "price_behavior_into_graduation",
    ),
    "liquidity": (
        "liquidity_formation",
        "liquidity_depth",
    ),
    "integrity": (
        "concentration",
        "creator_associated_activity",
        "funder_associated_activity",
        "linked_wallet_clustering",
        "wallet_integrity",
        "repeated_entity_activity",
        "abnormal_coordinated_buying",
        "suspicious_liquidity_behavior",
    ),
}

GRADUATION_LOWER_IS_BETTER = {
    "graduation_speed_seconds",
    "concentration",
    "creator_associated_activity",
    "funder_associated_activity",
    "linked_wallet_clustering",
    "repeated_entity_activity",
    "abnormal_coordinated_buying",
    "suspicious_liquidity_behavior",
}

CONTINUATION_FEATURE_FAMILIES: dict[str, tuple[str, ...]] = {
    "price": (
        "price_velocity_since_graduation",
        "price_velocity_since_entry",
        "maximum_favorable_excursion",
    ),
    "participation": (
        "buyer_acceleration",
        "buyer_breadth_growth",
        "new_wallet_participation",
        "quality_wallet_participation",
    ),
    "flow": (
        "transaction_velocity",
        "buy_sell_imbalance",
        "volume_persistence",
    ),
    "liquidity": (
        "liquidity_growth",
    ),
    "risk": (
        "maximum_adverse_excursion",
    ),
}

CONTINUATION_LOWER_IS_BETTER = {"maximum_adverse_excursion"}


@dataclass(frozen=True)
class IndependentActorMetrics:
    unique_wallets: int
    independent_economic_actors: int
    independent_buyer_breadth: float
    creator_or_funder_associated_actors: int
    creator_or_funder_contamination: float
    repeated_entity_activity: float
    linked_wallet_clustering: float
    independent_notional_fraction: float


@dataclass(frozen=True)
class PointInTimeComposite:
    score: float | None
    calibrated: bool
    family_scores: dict[str, float]
    component_percentiles: dict[str, float]
    sample_counts: dict[str, int]
    decision_time: str
    future_observations_used: int = 0


@dataclass(frozen=True)
class ContinuationPersistence:
    score: float | None
    calibrated: bool
    expected_continuation: float | None
    actual_continuation: float | None
    no_continuation_negative_evidence: float
    evidence_multiplier: float
    family_scores: dict[str, float]
    component_percentiles: dict[str, float]
    time_since_graduation_seconds: float | None
    time_since_entry_seconds: float | None
    hard_exit_seconds: float | None = None


@dataclass(frozen=True)
class WindowAlpha:
    window: str
    sample_count: int
    mean_return: float | None
    posterior_mean: float | None
    posterior_lower_90: float | None
    probability_positive: float | None
    positive_evidence: bool
    negative_evidence: bool


@dataclass(frozen=True)
class GovernedLaneState:
    lane: str
    mode: str
    capital_allowed: bool
    positive_streak: int
    negative_streak: int
    evidence_windows: dict[str, WindowAlpha]
    reason: str
    evaluated_at: str


@dataclass(frozen=True)
class AttributionResult:
    component: str
    incremental_return: float | None
    comparison: str
    causal_claim: bool = False


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _safe_mapping(item: Any) -> Mapping[str, Any]:
    return item if isinstance(item, Mapping) else {}


def independent_actor_metrics(participants: Iterable[Mapping[str, Any]]) -> IndependentActorMetrics:
    wallets: set[str] = set()
    actors: dict[str, dict[str, Any]] = {}
    total_notional = 0.0
    independent_notional = 0.0

    for raw in participants:
        row = _safe_mapping(raw)
        side = str(row.get("side") or "buy").lower()
        if side != "buy":
            continue
        wallet = str(row.get("wallet") or row.get("wallet_id") or "").strip()
        if not wallet:
            continue
        wallets.add(wallet)
        actor = (
            str(row.get("linked_entity_id") or "").strip()
            or str(row.get("funding_cluster_id") or "").strip()
            or str(row.get("funder_cluster_id") or "").strip()
            or f"wallet:{wallet}"
        )
        notional = max(0.0, _finite(row.get("notional")) or 0.0)
        associated = bool(row.get("creator_associated") or row.get("funder_associated"))
        bucket = actors.setdefault(actor, {"wallets": set(), "notional": 0.0, "associated": False})
        bucket["wallets"].add(wallet)
        bucket["notional"] += notional
        bucket["associated"] = bool(bucket["associated"] or associated)
        total_notional += notional

    for bucket in actors.values():
        if not bool(bucket["associated"]):
            independent_notional += float(bucket["notional"])

    unique = len(wallets)
    actor_count = len(actors)
    contaminated = sum(bool(bucket["associated"]) for bucket in actors.values())
    repeated = sum(max(0, len(bucket["wallets"]) - 1) for bucket in actors.values())
    repeated_ratio = repeated / max(1, unique)
    clustering = 1.0 - (actor_count / max(1, unique)) if unique else 0.0
    return IndependentActorMetrics(
        unique_wallets=unique,
        independent_economic_actors=actor_count,
        independent_buyer_breadth=actor_count / max(1, unique) if unique else 0.0,
        creator_or_funder_associated_actors=contaminated,
        creator_or_funder_contamination=contaminated / max(1, actor_count) if actor_count else 0.0,
        repeated_entity_activity=_clamp01(repeated_ratio),
        linked_wallet_clustering=_clamp01(clustering),
        independent_notional_fraction=(independent_notional / total_notional if total_notional > 0 else 0.0),
    )


def graduation_metrics_with_actor_integrity(
    metrics: Mapping[str, Any],
    participants: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    actors = independent_actor_metrics(participants)
    enriched = dict(metrics)
    enriched.setdefault("independent_buyer_breadth", actors.independent_buyer_breadth)
    enriched.setdefault("creator_associated_activity", actors.creator_or_funder_contamination)
    enriched.setdefault("funder_associated_activity", actors.creator_or_funder_contamination)
    enriched.setdefault("linked_wallet_clustering", actors.linked_wallet_clustering)
    enriched.setdefault("repeated_entity_activity", actors.repeated_entity_activity)
    enriched.setdefault("wallet_integrity", actors.independent_notional_fraction)
    return enriched


def _family_composite(
    metrics: Mapping[str, Any],
    history: Mapping[str, Sequence[Any]],
    families: Mapping[str, Sequence[str]],
    lower_is_better: set[str],
    *,
    minimum_samples: int,
    decision_time: str,
) -> PointInTimeComposite:
    components: dict[str, float] = {}
    counts: dict[str, int] = {}
    family_scores: dict[str, float] = {}
    for family, keys in families.items():
        family_values: list[float] = []
        for key in keys:
            population = [value for value in (_finite(item) for item in history.get(key, ())) if value is not None]
            counts[key] = len(population)
            percentile = base.empirical_percentile(
                metrics.get(key),
                population,
                higher_is_better=key not in lower_is_better,
            )
            if percentile is None:
                continue
            components[key] = percentile
            if len(population) >= minimum_samples:
                family_values.append(percentile)
        if family_values:
            family_scores[family] = statistics.fmean(family_values)
    calibrated = bool(family_scores) and len(family_scores) >= max(2, len(families) // 2)
    score = statistics.fmean(family_scores.values()) if family_scores else None
    return PointInTimeComposite(
        score=score,
        calibrated=calibrated,
        family_scores=family_scores,
        component_percentiles=components,
        sample_counts=counts,
        decision_time=decision_time,
        future_observations_used=0,
    )


def _normal_probability_positive(mean: float, se: float) -> float:
    if se <= 1e-12:
        return 1.0 if mean > 0 else (0.0 if mean < 0 else 0.5)
    z = mean / se
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def _posterior_window(values: Sequence[float], label: str, minimum_samples: int) -> WindowAlpha:
    clean = [float(item) for item in values if math.isfinite(float(item))]
    n = len(clean)
    if not clean:
        return WindowAlpha(label, 0, None, None, None, None, False, False)
    mean = statistics.fmean(clean)
    variance = statistics.pvariance(clean) if n > 1 else 0.25
    posterior_mean = (n * mean) / (n + POSTERIOR_PRIOR_STRENGTH)
    posterior_variance = (variance + 0.25) / max(1.0, n + POSTERIOR_PRIOR_STRENGTH)
    se = math.sqrt(max(1e-12, posterior_variance))
    lower = posterior_mean - 1.645 * se
    probability = _normal_probability_positive(posterior_mean, se)
    enough = n >= minimum_samples
    positive = bool(enough and posterior_mean > 0.0 and probability >= POSITIVE_PROBABILITY_ACTIVATE)
    negative = bool(enough and posterior_mean <= 0.0 and probability <= POSITIVE_PROBABILITY_DEACTIVATE)
    return WindowAlpha(
        window=label,
        sample_count=n,
        mean_return=mean,
        posterior_mean=posterior_mean,
        posterior_lower_90=lower,
        probability_positive=probability,
        positive_evidence=positive,
        negative_evidence=negative,
    )


def component_attribution(variant_returns: Mapping[str, Any]) -> dict[str, AttributionResult]:
    clean = {key: _finite(value) for key, value in variant_returns.items()}
    comparisons = {
        "wallet_intelligence": ("v52_full", "v52_no_wallet"),
        "pre_graduation_entry_bundle": ("v52_no_wallet", "graduation_only_continuation"),
        "graduation_quality": ("full_proposed", "no_graduation_quality"),
        "post_graduation_decay": ("full_proposed", "no_decay"),
        "lane_relative_calibration": ("full_proposed", "no_lane_calibration"),
        "staged_derisking": ("full_proposed", "no_staged_derisking"),
        "runners": ("full_proposed", "no_runners"),
        "reentry": ("full_proposed", "no_reentry"),
        "lane_gating": ("full_proposed", "no_lane_gating"),
    }
    result: dict[str, AttributionResult] = {}
    for component, (with_key, without_key) in comparisons.items():
        with_value = clean.get(with_key)
        without_value = clean.get(without_key)
        delta = None if with_value is None or without_value is None else with_value - without_value
        result[component] = AttributionResult(
            component=component,
            incremental_return=delta,
            comparison=f"{with_key} minus {without_key}",
            causal_claim=False,
        )
    return result


class MarketValidationGovernance:
    """Point-in-time research and hysteretic lane governance layered over v5.2.

    It never originates a trade. The only production influence is a conservative
    lane eligibility result consumed by the existing market-validation wrapper.
    """

    def __init__(self, controller: base.MarketValidationController) -> None:
        self.controller = controller
        self.store = controller.store
        self._schema()

    def _schema(self) -> None:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_lane_gate_state ("
                "lane TEXT PRIMARY KEY, mode TEXT NOT NULL, positive_streak INTEGER NOT NULL, "
                "negative_streak INTEGER NOT NULL, last_bucket TEXT, last_evaluated_at TEXT NOT NULL, "
                "reason TEXT NOT NULL, evidence_json TEXT NOT NULL, paper_only INTEGER NOT NULL, "
                "live_money_authority INTEGER NOT NULL)"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_market_validation_point_in_time ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, candidate_key TEXT NOT NULL, lane TEXT NOT NULL, "
                "observed_at TEXT NOT NULL, lifecycle_state TEXT, graduation_state TEXT, raw_features_json TEXT NOT NULL, "
                "relative_features_json TEXT NOT NULL, graduation_quality REAL, continuation_persistence REAL, "
                "lane_alpha_mode TEXT, wallet_evidence_json TEXT NOT NULL, v52_decision_json TEXT NOT NULL, "
                "shadow_decision_json TEXT NOT NULL, earliest_executable_price REAL, future_outcome_json TEXT, "
                "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, UNIQUE(candidate_key,observed_at))"
            )

    def history_before(
        self,
        lane: str,
        features: Sequence[str],
        decision_at: datetime | str,
        *,
        limit_per_feature: int = 250,
    ) -> dict[str, list[float]]:
        cutoff = _iso(_parse_time(decision_at))
        result: dict[str, list[float]] = {}
        with self.store._lock:
            for feature in features:
                rows = self.store.db.execute(
                    "SELECT value FROM v52_market_validation_features "
                    "WHERE lane=? AND feature=? AND observed_at<? ORDER BY observed_at DESC,id DESC LIMIT ?",
                    (str(lane), str(feature), cutoff, max(1, int(limit_per_feature))),
                ).fetchall()
                result[str(feature)] = [float(row["value"]) for row in rows]
        return result

    def graduation_quality_at(
        self,
        lane: str,
        metrics: Mapping[str, Any],
        decision_at: datetime | str,
    ) -> PointInTimeComposite:
        minimum = max(1, int(target_sizing_policy().get("minimum_forward_samples", 30)))
        features = tuple(key for keys in GRADUATION_FEATURE_FAMILIES.values() for key in keys)
        at = _iso(_parse_time(decision_at))
        history = self.history_before(lane, features, at)
        return _family_composite(
            metrics,
            history,
            GRADUATION_FEATURE_FAMILIES,
            GRADUATION_LOWER_IS_BETTER,
            minimum_samples=minimum,
            decision_time=at,
        )

    def lane_relative_score_at(
        self,
        lane: str,
        metrics: Mapping[str, Any],
        decision_at: datetime | str,
    ) -> PointInTimeComposite:
        minimum = max(1, int(target_sizing_policy().get("minimum_forward_samples", 30)))
        families = {
            "momentum": ("acceleration", "velocity"),
            "participation": ("independent_buyer_breadth", "buyer_breadth_growth"),
            "flow": ("buy_sell_imbalance", "transaction_velocity"),
            "liquidity": ("liquidity_formation", "liquidity_growth", "liquidity"),
            "execution": ("slippage", "execution_success"),
        }
        lower = {"slippage"}
        features = tuple(key for keys in families.values() for key in keys)
        at = _iso(_parse_time(decision_at))
        history = self.history_before(lane, features, at)
        return _family_composite(
            metrics,
            history,
            families,
            lower,
            minimum_samples=minimum,
            decision_time=at,
        )

    def continuation_at(
        self,
        lane: str,
        metrics: Mapping[str, Any],
        decision_at: datetime | str,
        *,
        expected_continuation: float | None,
    ) -> ContinuationPersistence:
        minimum = max(1, int(target_sizing_policy().get("minimum_forward_samples", 30)))
        features = tuple(key for keys in CONTINUATION_FEATURE_FAMILIES.values() for key in keys)
        at = _iso(_parse_time(decision_at))
        history = self.history_before(lane, features, at)
        composite = _family_composite(
            metrics,
            history,
            CONTINUATION_FEATURE_FAMILIES,
            CONTINUATION_LOWER_IS_BETTER,
            minimum_samples=minimum,
            decision_time=at,
        )
        expected = None if expected_continuation is None else _clamp01(float(expected_continuation))
        actual = composite.score
        negative = 0.0
        if composite.calibrated and expected is not None and actual is not None:
            negative = _clamp01(max(0.0, expected - actual) * expected)
        return ContinuationPersistence(
            score=actual,
            calibrated=composite.calibrated,
            expected_continuation=expected,
            actual_continuation=actual,
            no_continuation_negative_evidence=negative,
            evidence_multiplier=1.0 - negative,
            family_scores=composite.family_scores,
            component_percentiles=composite.component_percentiles,
            time_since_graduation_seconds=_finite(metrics.get("seconds_since_graduation")),
            time_since_entry_seconds=_finite(metrics.get("seconds_since_entry")),
            hard_exit_seconds=None,
        )

    def _aliases(self, lane: str) -> tuple[str, ...]:
        canonical = base.canonical_alpha_lane(lane)
        return base.LANE_ALIASES.get(canonical, (canonical,))

    def _returns_before(self, lane: str, decision_at: datetime, window: timedelta) -> list[float]:
        aliases = self._aliases(lane)
        placeholders = ",".join("?" for _ in aliases)
        start = _iso(decision_at - window)
        cutoff = _iso(decision_at)
        params = tuple(str(value) for value in aliases)
        values: list[float] = []
        if base._table_exists(self.store, "v52_profit_signal_events"):
            with self.store._lock:
                rows = self.store.db.execute(
                    "SELECT realized_net_return FROM v52_profit_signal_events "
                    f"WHERE lane IN ({placeholders}) AND observed_at>=? AND observed_at<? "
                    "AND realized_net_return IS NOT NULL ORDER BY observed_at,id",
                    (*params, start, cutoff),
                ).fetchall()
            values.extend(float(row["realized_net_return"]) for row in rows)
        return values

    def alpha_windows_at(self, lane: str, decision_at: datetime | str) -> dict[str, WindowAlpha]:
        at = _parse_time(decision_at)
        minimum = max(1, int(target_sizing_policy().get("minimum_forward_samples", 30)))
        minimum_by_window = {
            "24h": max(8, minimum // 3),
            "7d": max(15, minimum // 2),
            "30d": minimum,
        }
        return {
            label: _posterior_window(
                self._returns_before(lane, at, delta),
                label,
                minimum_by_window[label],
            )
            for label, delta in WINDOWS.items()
        }

    def _bucket(self, at: datetime) -> str:
        minute = (at.minute // TRANSITION_BUCKET_MINUTES) * TRANSITION_BUCKET_MINUTES
        bucket = at.replace(minute=minute, second=0, microsecond=0)
        return _iso(bucket)

    def _load_state(self, lane: str) -> dict[str, Any]:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT * FROM v52_lane_gate_state WHERE lane=?",
                (lane,),
            ).fetchone()
        if row is None:
            return {
                "mode": "insufficient_evidence",
                "positive_streak": 0,
                "negative_streak": 0,
                "last_bucket": None,
                "reason": "cold_start",
            }
        return dict(row)

    def governed_lane_state_at(self, lane: str, decision_at: datetime | str) -> GovernedLaneState:
        canonical = base.canonical_alpha_lane(lane)
        at = _parse_time(decision_at)
        bucket = self._bucket(at)
        windows = self.alpha_windows_at(canonical, at)
        state = self._load_state(canonical)
        prior_mode = str(state.get("mode") or "insufficient_evidence")
        positive_streak = int(state.get("positive_streak") or 0)
        negative_streak = int(state.get("negative_streak") or 0)

        long_windows = [windows["7d"], windows["30d"]]
        enough = all(item.sample_count > 0 for item in long_windows) and windows["30d"].sample_count >= max(
            1, int(target_sizing_policy().get("minimum_forward_samples", 30))
        )
        positive = bool(enough and all(item.positive_evidence for item in long_windows))
        negative = bool(enough and all(item.negative_evidence for item in long_windows))

        if state.get("last_bucket") != bucket:
            if positive:
                positive_streak += 1
                negative_streak = 0
            elif negative:
                negative_streak += 1
                positive_streak = 0
            else:
                positive_streak = max(0, positive_streak - 1)
                negative_streak = max(0, negative_streak - 1)

        mode = prior_mode
        reason = "hysteresis_hold"
        if not enough:
            if prior_mode == "observe_only":
                mode = "observe_only"
                reason = "insufficient_new_evidence_preserve_observe_only"
            else:
                mode = "insufficient_evidence"
                reason = "insufficient_multi_horizon_evidence"
        elif prior_mode == "observe_only":
            if positive_streak >= REACTIVATION_PERSISTENCE_BUCKETS:
                mode = "active"
                reason = "persistent_positive_multi_horizon_reactivation"
            else:
                mode = "observe_only"
                reason = "reactivation_hysteresis_not_satisfied"
        elif negative_streak >= DEACTIVATION_PERSISTENCE_BUCKETS:
            mode = "observe_only"
            reason = "persistent_negative_multi_horizon_expectancy"
        elif positive_streak >= REACTIVATION_PERSISTENCE_BUCKETS:
            mode = "active"
            reason = "persistent_positive_multi_horizon_expectancy"
        else:
            mode = "active" if prior_mode == "active" else "insufficient_evidence"
            reason = "transition_hysteresis_not_satisfied"

        evidence_payload = {key: asdict(value) for key, value in windows.items()}
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT INTO v52_lane_gate_state("
                "lane,mode,positive_streak,negative_streak,last_bucket,last_evaluated_at,reason,evidence_json,paper_only,live_money_authority"
                ") VALUES (?,?,?,?,?,?,?,?,1,0) ON CONFLICT(lane) DO UPDATE SET "
                "mode=excluded.mode,positive_streak=excluded.positive_streak,negative_streak=excluded.negative_streak,"
                "last_bucket=excluded.last_bucket,last_evaluated_at=excluded.last_evaluated_at,reason=excluded.reason,"
                "evidence_json=excluded.evidence_json,paper_only=1,live_money_authority=0",
                (
                    canonical,
                    mode,
                    positive_streak,
                    negative_streak,
                    bucket,
                    _iso(at),
                    reason,
                    __import__("json").dumps(evidence_payload, sort_keys=True),
                ),
            )
        return GovernedLaneState(
            lane=canonical,
            mode=mode,
            capital_allowed=mode != "observe_only",
            positive_streak=positive_streak,
            negative_streak=negative_streak,
            evidence_windows=windows,
            reason=reason,
            evaluated_at=_iso(at),
        )

    def alpha_gate_at(self, lane: str, decision_at: datetime | str) -> base.LaneAlphaGate:
        governed = self.governed_lane_state_at(lane, decision_at)
        thirty = governed.evidence_windows["30d"]
        return base.LaneAlphaGate(
            lane=governed.lane,
            mode=(
                "observe_only"
                if governed.mode == "observe_only"
                else ("active" if governed.mode == "active" else "preserve_v52")
            ),
            capital_allowed=governed.capital_allowed,
            realized_expectancy=thirty.posterior_mean,
            realized_samples=thirty.sample_count,
            counterfactual_expectancy=None,
            counterfactual_samples=0,
            strategy_specific_underperformance=False,
            reason=governed.reason,
        )

    def record_point_in_time_decision(
        self,
        *,
        candidate_key: str,
        lane: str,
        observed_at: datetime | str,
        lifecycle_state: str | None,
        graduation_state: str | None,
        raw_features: Mapping[str, Any],
        relative_features: Mapping[str, Any],
        graduation_quality: float | None,
        continuation_persistence: float | None,
        lane_alpha_mode: str | None,
        wallet_evidence: Mapping[str, Any],
        v52_decision: Mapping[str, Any],
        shadow_decision: Mapping[str, Any],
        earliest_executable_price: float | None,
    ) -> None:
        import json

        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT OR REPLACE INTO v52_market_validation_point_in_time("
                "candidate_key,lane,observed_at,lifecycle_state,graduation_state,raw_features_json,relative_features_json,"
                "graduation_quality,continuation_persistence,lane_alpha_mode,wallet_evidence_json,v52_decision_json,"
                "shadow_decision_json,earliest_executable_price,future_outcome_json,paper_only,live_money_authority"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,1,0)",
                (
                    str(candidate_key),
                    base.canonical_alpha_lane(lane),
                    _iso(_parse_time(observed_at)),
                    lifecycle_state,
                    graduation_state,
                    json.dumps(dict(raw_features), sort_keys=True),
                    json.dumps(dict(relative_features), sort_keys=True),
                    graduation_quality,
                    continuation_persistence,
                    lane_alpha_mode,
                    json.dumps(dict(wallet_evidence), sort_keys=True),
                    json.dumps(dict(v52_decision), sort_keys=True),
                    json.dumps(dict(shadow_decision), sort_keys=True),
                    earliest_executable_price,
                ),
            )

    def resolve_future_outcome(self, candidate_key: str, observed_at: datetime | str, outcome: Mapping[str, Any]) -> None:
        import json

        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE v52_market_validation_point_in_time SET future_outcome_json=? "
                "WHERE candidate_key=? AND observed_at=?",
                (
                    json.dumps(dict(outcome), sort_keys=True),
                    str(candidate_key),
                    _iso(_parse_time(observed_at)),
                ),
            )


_GOVERNANCE: MarketValidationGovernance | None = None
_BASE_ALPHA_GATE: Any = None
_INSTALLED = False


def governance() -> MarketValidationGovernance:
    if _GOVERNANCE is None:
        raise RuntimeError("v5.2 market-validation governance not installed")
    return _GOVERNANCE


def _governed_alpha_gate(self: base.MarketValidationController, lane: str) -> base.LaneAlphaGate:
    if _GOVERNANCE is None:
        if _BASE_ALPHA_GATE is None:
            raise RuntimeError("v5.2 market-validation alpha predecessor unavailable")
        return _BASE_ALPHA_GATE(lane)
    return _GOVERNANCE.alpha_gate_at(lane, _utcnow())


def install_v52_market_validation_governance(controller: base.MarketValidationController) -> MarketValidationGovernance:
    global _GOVERNANCE, _BASE_ALPHA_GATE, _INSTALLED
    if _GOVERNANCE is None or _GOVERNANCE.controller is not controller:
        _GOVERNANCE = MarketValidationGovernance(controller)
    if not _INSTALLED:
        _BASE_ALPHA_GATE = controller.alpha_gate
        controller.alpha_gate = MethodType(_governed_alpha_gate, controller)
        _INSTALLED = True
    return _GOVERNANCE


def status() -> dict[str, Any]:
    return {
        "version": VERSION,
        "installed": _INSTALLED,
        "point_in_time_history_cutoff": True,
        "independent_economic_actor_counting": True,
        "creator_funder_contamination": True,
        "graduation_evidence_family_decorrelation": True,
        "continuation_evidence_family_decorrelation": True,
        "multi_horizon_lane_alpha": list(WINDOWS),
        "bayesian_shrinkage": True,
        "minimum_effective_samples": True,
        "activation_deactivation_hysteresis": True,
        "persistent_observe_only": True,
        "automatic_reactivation": True,
        "future_leakage_allowed": False,
        "shadow_strategies_control_trading": False,
        "hard_post_graduation_exit_seconds": None,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "VERSION",
    "AttributionResult",
    "ContinuationPersistence",
    "GovernedLaneState",
    "IndependentActorMetrics",
    "MarketValidationGovernance",
    "PointInTimeComposite",
    "WindowAlpha",
    "component_attribution",
    "governance",
    "graduation_metrics_with_actor_integrity",
    "independent_actor_metrics",
    "install_v52_market_validation_governance",
    "status",
]
