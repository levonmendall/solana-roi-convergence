from __future__ import annotations

import json
import math
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

from .strategy_v52_authority import (
    LIVE_MONEY_AUTHORITY,
    PAPER_ONLY,
    SIGNING_AVAILABLE,
    TRANSACTION_SUBMISSION_AVAILABLE,
    detection_policy,
    target_sizing_policy,
)

VERSION = "v52-market-validation-controls-v1"

GRADUATION_COMPONENTS = (
    "graduation_speed_seconds",
    "independent_buyer_breadth",
    "buyer_acceleration",
    "buy_sell_imbalance",
    "concentration",
    "liquidity_formation",
    "creator_associated_activity",
)
LANE_RELATIVE_COMPONENTS = (
    "acceleration",
    "independent_buyer_breadth",
    "liquidity_formation",
    "buy_sell_imbalance",
    "velocity",
)
LOWER_IS_BETTER = {
    "graduation_speed_seconds",
    "concentration",
    "creator_associated_activity",
    "seconds_since_graduation",
    "seconds_since_entry",
}
FOMO_MARKET_STATES = {
    "pre_fomo",
    "active_fomo",
    "fomo",
    "fomo_acceleration",
    "fomo_exhaustion",
    "exhaustion",
}
SHADOW_STRATEGIES = (
    "graduation_only_continuation",
    "v52_no_wallet",
    "v52_full",
)

LANE_ALIASES: dict[str, tuple[str, ...]] = {
    "pump_fun": ("pump_fun", "elite_wallet_continuation", "PUMP_FUN"),
    "pump_amm": ("pump_amm", "pumpswap", "graduation_continuation", "PUMP_AMM", "PUMPSWAP"),
    "raydium": ("raydium", "raydium_cross_venue_persistence", "RAYDIUM"),
    "fomo": ("fomo", "fomo_continuation", "FOMO"),
    "robinhood": ("robinhood", "robinhood_entity_continuation", "UNISWAP_V3"),
}


def canonical_alpha_lane(value: Any) -> str:
    raw = str(value or "").strip()
    lowered = raw.lower()
    for canonical, aliases in LANE_ALIASES.items():
        if lowered == canonical or lowered in {str(alias).lower() for alias in aliases}:
            return canonical
    if "robinhood" in lowered:
        return "robinhood"
    if "fomo" in lowered:
        return "fomo"
    if "raydium" in lowered:
        return "raydium"
    if "graduation" in lowered or "pumpswap" in lowered or "pump_amm" in lowered:
        return "pump_amm"
    if "pump" in lowered or "wallet_continuation" in lowered:
        return "pump_fun"
    return raw or "unknown"


@dataclass(frozen=True)
class MarketContext:
    execution_lane: str
    discovery_route: str | None
    market_state: str | None
    market_archetype: str | None


@dataclass(frozen=True)
class RelativeScore:
    score: float | None
    calibrated: bool
    components: dict[str, float]
    sample_counts: dict[str, int]


@dataclass(frozen=True)
class GraduationQuality:
    score: float | None
    calibrated: bool
    organic_participation_evidence: float | None
    components: dict[str, float]
    sample_counts: dict[str, int]
    fixed_seconds_threshold_used: bool = False
    fixed_buyer_threshold_used: bool = False


@dataclass(frozen=True)
class ContinuationDecay:
    continuation_score: float | None
    evidence_multiplier: float
    calibrated: bool
    no_continuation_negative_evidence: float
    components: dict[str, float]
    hard_wait_seconds: float | None = None
    hard_stop_seconds: float | None = None


@dataclass(frozen=True)
class LaneAlphaGate:
    lane: str
    mode: str
    capital_allowed: bool
    realized_expectancy: float | None
    realized_samples: int
    counterfactual_expectancy: float | None
    counterfactual_samples: int
    strategy_specific_underperformance: bool
    reason: str


def _finite(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _clean(values: Iterable[Any]) -> list[float]:
    cleaned: list[float] = []
    for value in values:
        parsed = _finite(value)
        if parsed is not None:
            cleaned.append(parsed)
    return cleaned


def empirical_percentile(
    value: Any,
    population: Sequence[Any],
    *,
    higher_is_better: bool = True,
) -> float | None:
    observed = _finite(value)
    values = _clean(population)
    if observed is None or not values:
        return None
    below = sum(item < observed for item in values)
    equal = sum(item == observed for item in values)
    percentile = (below + 0.5 * equal) / len(values)
    if not higher_is_better:
        percentile = 1.0 - percentile
    return max(0.0, min(1.0, percentile))


def _minimum_calibration_samples() -> int:
    return max(1, int(detection_policy().get("minimum_comparable_peer_count", 20)))


def _minimum_alpha_samples() -> int:
    return max(1, int(target_sizing_policy().get("minimum_forward_samples", 30)))


def classify_market_context(
    execution_lane: str,
    *,
    discovery_route: str | None = None,
    market_state: str | None = None,
) -> MarketContext:
    state = str(market_state or "").strip().lower() or None
    archetype = "fomo" if state in FOMO_MARKET_STATES else None
    return MarketContext(
        execution_lane=str(execution_lane or "unknown"),
        discovery_route=str(discovery_route) if discovery_route else None,
        market_state=state,
        market_archetype=archetype,
    )


def lane_relative_opportunity_score(
    metrics: Mapping[str, Any],
    lane_history: Mapping[str, Sequence[Any]],
    *,
    minimum_samples: int | None = None,
) -> RelativeScore:
    minimum = _minimum_calibration_samples() if minimum_samples is None else max(1, int(minimum_samples))
    components: dict[str, float] = {}
    sample_counts: dict[str, int] = {}
    for key in LANE_RELATIVE_COMPONENTS:
        history = _clean(lane_history.get(key, ()))
        sample_counts[key] = len(history)
        percentile = empirical_percentile(
            metrics.get(key),
            history,
            higher_is_better=key not in LOWER_IS_BETTER,
        )
        if percentile is not None:
            components[key] = percentile
    score = statistics.fmean(components.values()) if components else None
    calibrated = bool(components) and all(
        sample_counts.get(key, 0) >= minimum for key in components
    )
    return RelativeScore(
        score=score,
        calibrated=calibrated,
        components=components,
        sample_counts=sample_counts,
    )


def graduation_quality_score(
    metrics: Mapping[str, Any],
    lane_history: Mapping[str, Sequence[Any]],
    *,
    minimum_samples: int | None = None,
) -> GraduationQuality:
    minimum = _minimum_calibration_samples() if minimum_samples is None else max(1, int(minimum_samples))
    components: dict[str, float] = {}
    sample_counts: dict[str, int] = {}
    for key in GRADUATION_COMPONENTS:
        history = _clean(lane_history.get(key, ()))
        sample_counts[key] = len(history)
        percentile = empirical_percentile(
            metrics.get(key),
            history,
            higher_is_better=key not in LOWER_IS_BETTER,
        )
        if percentile is not None:
            components[key] = percentile
    score = statistics.fmean(components.values()) if components else None
    calibrated = bool(components) and all(
        sample_counts.get(key, 0) >= minimum for key in components
    )
    organic = score if calibrated else None
    return GraduationQuality(
        score=score,
        calibrated=calibrated,
        organic_participation_evidence=organic,
        components=components,
        sample_counts=sample_counts,
    )


def post_graduation_decay_clock(
    *,
    seconds_since_graduation: Any,
    seconds_since_entry: Any,
    continuation_persistence: Any,
    history: Mapping[str, Sequence[Any]],
    minimum_samples: int | None = None,
) -> ContinuationDecay:
    minimum = _minimum_calibration_samples() if minimum_samples is None else max(1, int(minimum_samples))
    raw = {
        "seconds_since_graduation": seconds_since_graduation,
        "seconds_since_entry": seconds_since_entry,
        "continuation_persistence": continuation_persistence,
    }
    components: dict[str, float] = {}
    counts: dict[str, int] = {}
    for key, value in raw.items():
        population = _clean(history.get(key, ()))
        counts[key] = len(population)
        percentile = empirical_percentile(
            value,
            population,
            higher_is_better=key not in LOWER_IS_BETTER,
        )
        if percentile is not None:
            components[key] = percentile

    score = statistics.fmean(components.values()) if components else None
    calibrated = bool(components) and all(counts.get(key, 0) >= minimum for key in components)
    if not calibrated:
        return ContinuationDecay(
            continuation_score=score,
            evidence_multiplier=1.0,
            calibrated=False,
            no_continuation_negative_evidence=0.0,
            components=components,
        )

    persistence = components.get("continuation_persistence", 0.5)
    time_components = [
        value
        for key, value in components.items()
        if key in {"seconds_since_graduation", "seconds_since_entry"}
    ]
    time_quality = statistics.fmean(time_components) if time_components else 0.5
    negative = max(0.0, min(1.0, (1.0 - persistence) * (1.0 - time_quality)))
    multiplier = max(0.0, min(1.0, 1.0 - negative))
    return ContinuationDecay(
        continuation_score=score,
        evidence_multiplier=multiplier,
        calibrated=True,
        no_continuation_negative_evidence=negative,
        components=components,
    )


def _trimmed_expectancy(values: Sequence[Any]) -> tuple[float | None, int]:
    cleaned = sorted(_clean(values))
    if not cleaned:
        return None, 0
    count = len(cleaned)
    if count >= 10:
        trim = max(1, int(count * 0.10))
        if count > trim * 2:
            cleaned = cleaned[trim:-trim]
    return statistics.fmean(cleaned), count


def lane_alpha_gate(
    lane: str,
    *,
    realized_returns: Sequence[Any],
    counterfactual_returns: Sequence[Any],
    minimum_samples: int | None = None,
) -> LaneAlphaGate:
    minimum = _minimum_alpha_samples() if minimum_samples is None else max(1, int(minimum_samples))
    realized, realized_n = _trimmed_expectancy(realized_returns)
    counterfactual, counterfactual_n = _trimmed_expectancy(counterfactual_returns)

    if realized_n < minimum:
        return LaneAlphaGate(
            lane=str(lane),
            mode="preserve_v52",
            capital_allowed=True,
            realized_expectancy=realized,
            realized_samples=realized_n,
            counterfactual_expectancy=counterfactual,
            counterfactual_samples=counterfactual_n,
            strategy_specific_underperformance=False,
            reason="insufficient_realized_forward_samples",
        )

    if realized is not None and realized <= 0.0:
        positive_control = bool(
            counterfactual_n >= minimum
            and counterfactual is not None
            and counterfactual > 0.0
        )
        return LaneAlphaGate(
            lane=str(lane),
            mode="observe_only",
            capital_allowed=False,
            realized_expectancy=realized,
            realized_samples=realized_n,
            counterfactual_expectancy=counterfactual,
            counterfactual_samples=counterfactual_n,
            strategy_specific_underperformance=positive_control,
            reason=(
                "v52_negative_while_counterfactual_positive"
                if positive_control
                else "realized_lane_expectancy_nonpositive"
            ),
        )

    return LaneAlphaGate(
        lane=str(lane),
        mode="active",
        capital_allowed=True,
        realized_expectancy=realized,
        realized_samples=realized_n,
        counterfactual_expectancy=counterfactual,
        counterfactual_samples=counterfactual_n,
        strategy_specific_underperformance=False,
        reason="realized_lane_expectancy_positive",
    )


def shadow_strategy_definitions() -> dict[str, dict[str, Any]]:
    return {
        "graduation_only_continuation": {
            "description": "post-graduation continuation control using no pre-graduation entry advantage",
            "controls_trading": False,
            "wallet_intelligence": False,
            "pre_graduation_entry": False,
        },
        "v52_no_wallet": {
            "description": "v5.2 mechanics with wallet intelligence neutralized for paired counterfactual attribution",
            "controls_trading": False,
            "wallet_intelligence": False,
            "pre_graduation_entry": True,
        },
        "v52_full": {
            "description": "full v5.2 incumbent including wallet intelligence",
            "controls_trading": False,
            "wallet_intelligence": True,
            "pre_graduation_entry": True,
        },
    }


def _table_exists(store: Any, name: str) -> bool:
    try:
        with store._lock:
            row = store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (name,),
            ).fetchone()
        return row is not None
    except Exception:
        return False


class MarketValidationController:
    """Durable validation/control layer that can only reduce v5.2 paper authority."""

    def __init__(self, store: Any) -> None:
        self.store = store
        self._schema()

    def _schema(self) -> None:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_market_validation_features ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, lane TEXT NOT NULL, observed_at TEXT NOT NULL, "
                "discovery_route TEXT, market_state TEXT, market_archetype TEXT, feature TEXT NOT NULL, "
                "value REAL NOT NULL, paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
            )
            self.store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_v52_market_validation_features_lane "
                "ON v52_market_validation_features(lane,feature,id)"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_market_validation_shadow_outcomes ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, candidate_key TEXT NOT NULL, lane TEXT NOT NULL, "
                "strategy_id TEXT NOT NULL, observed_at TEXT NOT NULL, net_return REAL, resolved_at TEXT, "
                "evidence_json TEXT NOT NULL, controls_trading INTEGER NOT NULL, paper_only INTEGER NOT NULL, "
                "live_money_authority INTEGER NOT NULL, UNIQUE(candidate_key,strategy_id))"
            )
            self.store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_v52_market_validation_shadow_lane "
                "ON v52_market_validation_shadow_outcomes(lane,strategy_id,resolved_at)"
            )

    def observe_features(
        self,
        lane: str,
        metrics: Mapping[str, Any],
        *,
        observed_at: str | None = None,
        discovery_route: str | None = None,
        market_state: str | None = None,
    ) -> int:
        context = classify_market_context(
            lane,
            discovery_route=discovery_route,
            market_state=market_state,
        )
        at = observed_at or datetime.now(timezone.utc).isoformat()
        rows: list[tuple[Any, ...]] = []
        for feature, raw in metrics.items():
            value = _finite(raw)
            if value is None:
                continue
            rows.append(
                (
                    str(lane),
                    at,
                    context.discovery_route,
                    context.market_state,
                    context.market_archetype,
                    str(feature),
                    value,
                )
            )
        if not rows:
            return 0
        with self.store._lock, self.store.db:
            self.store.db.executemany(
                "INSERT INTO v52_market_validation_features("
                "lane,observed_at,discovery_route,market_state,market_archetype,feature,value,"
                "paper_only,live_money_authority) VALUES (?,?,?,?,?,?,?,1,0)",
                rows,
            )
        return len(rows)

    def lane_history(
        self,
        lane: str,
        features: Sequence[str],
        *,
        limit_per_feature: int = 250,
    ) -> dict[str, list[float]]:
        result: dict[str, list[float]] = {}
        with self.store._lock:
            for feature in features:
                rows = self.store.db.execute(
                    "SELECT value FROM v52_market_validation_features "
                    "WHERE lane=? AND feature=? ORDER BY id DESC LIMIT ?",
                    (str(lane), str(feature), max(1, int(limit_per_feature))),
                ).fetchall()
                result[str(feature)] = [float(row["value"]) for row in rows]
        return result

    def graduation_quality(self, lane: str, metrics: Mapping[str, Any]) -> GraduationQuality:
        return graduation_quality_score(
            metrics,
            self.lane_history(lane, GRADUATION_COMPONENTS),
        )

    def opportunity_score(self, lane: str, metrics: Mapping[str, Any]) -> RelativeScore:
        return lane_relative_opportunity_score(
            metrics,
            self.lane_history(lane, LANE_RELATIVE_COMPONENTS),
        )

    def decay_clock(
        self,
        lane: str,
        *,
        seconds_since_graduation: Any,
        seconds_since_entry: Any,
        continuation_persistence: Any,
    ) -> ContinuationDecay:
        features = ("seconds_since_graduation", "seconds_since_entry", "continuation_persistence")
        return post_graduation_decay_clock(
            seconds_since_graduation=seconds_since_graduation,
            seconds_since_entry=seconds_since_entry,
            continuation_persistence=continuation_persistence,
            history=self.lane_history(lane, features),
        )

    def record_shadow_outcome(
        self,
        candidate_key: str,
        lane: str,
        strategy_id: str,
        *,
        observed_at: str,
        net_return: float | None = None,
        resolved_at: str | None = None,
        evidence: Mapping[str, Any] | None = None,
    ) -> None:
        if strategy_id not in SHADOW_STRATEGIES:
            raise ValueError(f"unknown market-validation shadow strategy: {strategy_id}")
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT INTO v52_market_validation_shadow_outcomes("
                "candidate_key,lane,strategy_id,observed_at,net_return,resolved_at,evidence_json,"
                "controls_trading,paper_only,live_money_authority) VALUES (?,?,?,?,?,?,?,0,1,0) "
                "ON CONFLICT(candidate_key,strategy_id) DO UPDATE SET "
                "net_return=excluded.net_return,resolved_at=excluded.resolved_at,evidence_json=excluded.evidence_json",
                (
                    str(candidate_key),
                    str(lane),
                    str(strategy_id),
                    str(observed_at),
                    None if net_return is None else float(net_return),
                    resolved_at,
                    json.dumps(dict(evidence or {}), sort_keys=True),
                ),
            )

    def _existing_lane_returns(self, lane: str) -> tuple[list[float], list[float]]:
        realized: list[float] = []
        counterfactual: list[float] = []
        canonical = canonical_alpha_lane(lane)
        aliases = LANE_ALIASES.get(canonical, (canonical,))
        placeholders = ",".join("?" for _ in aliases)
        params = tuple(str(value) for value in aliases)
        if _table_exists(self.store, "v52_profit_signal_events"):
            with self.store._lock:
                rows = self.store.db.execute(
                    "SELECT realized_net_return FROM v52_profit_signal_events "
                    f"WHERE lane IN ({placeholders}) AND realized_net_return IS NOT NULL "
                    "ORDER BY id DESC LIMIT 250",
                    params,
                ).fetchall()
            realized = [float(row["realized_net_return"]) for row in rows]
        if _table_exists(self.store, "v52_counterfactual_decisions"):
            with self.store._lock:
                rows = self.store.db.execute(
                    "SELECT net_return FROM v52_counterfactual_decisions "
                    f"WHERE lane IN ({placeholders}) AND resolved_at IS NOT NULL AND net_return IS NOT NULL "
                    "ORDER BY id DESC LIMIT 250",
                    params,
                ).fetchall()
            counterfactual.extend(float(row["net_return"]) for row in rows)
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT net_return FROM v52_market_validation_shadow_outcomes "
                f"WHERE lane IN ({placeholders}) AND resolved_at IS NOT NULL AND net_return IS NOT NULL "
                "ORDER BY id DESC LIMIT 250",
                params,
            ).fetchall()
        counterfactual.extend(float(row["net_return"]) for row in rows)
        return realized, counterfactual

    def alpha_gate(self, lane: str) -> LaneAlphaGate:
        realized, counterfactual = self._existing_lane_returns(lane)
        return lane_alpha_gate(
            canonical_alpha_lane(lane),
            realized_returns=realized,
            counterfactual_returns=counterfactual,
        )

    def status(self) -> dict[str, Any]:
        return status(self)


_CONTROLLER: MarketValidationController | None = None
_INSTALLED = False
_BASE_SOLANA_CHOOSE: Any = None
_BASE_FOMO_DECISION: Any = None
_BASE_ROBINHOOD_CHOOSE: Any = None


def controller() -> MarketValidationController:
    if _CONTROLLER is None:
        raise RuntimeError("v5.2 market-validation controller not installed")
    return _CONTROLLER


def _validation_metadata(gate: LaneAlphaGate) -> dict[str, Any]:
    return {
        "version": VERSION,
        "canonical_alpha_lane": gate.lane,
        "lane_alpha_gate": asdict(gate),
        "graduation_quality_available": True,
        "post_graduation_decay_available": True,
        "lane_relative_calibration_available": True,
        "shadow_controls": list(SHADOW_STRATEGIES),
        "shadow_controls_authoritative": False,
        "fomo_state_separate_from_discovery_route": True,
        "paper_only": True,
        "live_money_authority": False,
    }


def _copy_profiles(profiles: Any) -> dict[str, Any]:
    return {
        key: dict(value) if isinstance(value, dict) else value
        for key, value in dict(profiles or {}).items()
    }


def _validated_solana_choose(
    adapter: Any,
    pre: dict[str, Any],
    *,
    chase: float | None = None,
    latency: float | None = None,
) -> tuple[str | None, float, dict[str, Any]]:
    if _BASE_SOLANA_CHOOSE is None:
        raise RuntimeError("v52 market-validation Solana predecessor unavailable")
    lane, fraction, profiles = _BASE_SOLANA_CHOOSE(adapter, pre, chase=chase, latency=latency)
    copied = _copy_profiles(profiles)
    if not lane or float(fraction or 0.0) <= 0.0:
        return lane, max(0.0, float(fraction or 0.0)), copied
    gate = controller().alpha_gate(canonical_alpha_lane(lane))
    profile = dict(copied.get(lane) or {})
    profile["v52_market_validation"] = _validation_metadata(gate)
    copied[lane] = profile
    if not gate.capital_allowed:
        auth = dict(profile.get("v52_authority") or {})
        auth["market_validation_lane_gate"] = "observe_only"
        auth["market_validation_reason"] = gate.reason
        auth["final_fraction"] = 0.0
        profile["v52_authority"] = auth
        copied[lane] = profile
        return None, 0.0, copied
    return lane, max(0.0, float(fraction or 0.0)), copied


def _validated_fomo_decision(
    adapter: Any,
    *,
    observation: dict[str, Any],
    trial: dict[str, Any],
) -> dict[str, Any]:
    if _BASE_FOMO_DECISION is None:
        raise RuntimeError("v52 market-validation FOMO predecessor unavailable")
    result = dict(_BASE_FOMO_DECISION(adapter, observation=observation, trial=trial))
    gate = controller().alpha_gate("fomo")
    result["v52_market_validation"] = _validation_metadata(gate)
    context = classify_market_context(
        str(observation.get("execution_lane") or "pump_amm"),
        discovery_route=str(observation.get("discovery_route") or "fomo"),
        market_state=str(observation.get("state") or observation.get("fomo_state") or ""),
    )
    result["market_context"] = asdict(context)
    if (
        not gate.capital_allowed
        and float(result.get("position_fraction") or 0.0) > 0.0
    ):
        result["position_fraction"] = 0.0
        result["decision"] = "no_entry_v52_lane_alpha_observe_only"
        result["reason"] = gate.reason
        auth = dict(result.get("v52_authority") or {})
        auth["market_validation_lane_gate"] = "observe_only"
        auth["market_validation_reason"] = gate.reason
        auth["final_fraction"] = 0.0
        result["v52_authority"] = auth
    return result


def _validated_robinhood_choose(self: Any, **kwargs: Any) -> tuple[str | None, float, dict[str, Any]]:
    if _BASE_ROBINHOOD_CHOOSE is None:
        raise RuntimeError("v52 market-validation Robinhood predecessor unavailable")
    lane, fraction, profiles = _BASE_ROBINHOOD_CHOOSE(self, **kwargs)
    copied = _copy_profiles(profiles)
    if not lane or float(fraction or 0.0) <= 0.0:
        return lane, max(0.0, float(fraction or 0.0)), copied
    gate = controller().alpha_gate("robinhood")
    profile = dict(copied.get(lane) or {})
    profile["v52_market_validation"] = _validation_metadata(gate)
    copied[lane] = profile
    if not gate.capital_allowed:
        auth = dict(profile.get("v52_authority") or {})
        auth["market_validation_lane_gate"] = "observe_only"
        auth["market_validation_reason"] = gate.reason
        auth["final_fraction"] = 0.0
        profile["v52_authority"] = auth
        copied[lane] = profile
        return None, 0.0, copied
    return lane, max(0.0, float(fraction or 0.0)), copied


def _preserve_lineage(wrapper: Any, predecessor: Any) -> None:
    if not callable(predecessor):
        raise RuntimeError("v52 market-validation predecessor unavailable")
    setattr(wrapper, "__wrapped__", predecessor)
    for name, value in vars(predecessor).items():
        if name.startswith("_roi_") and not hasattr(wrapper, name):
            setattr(wrapper, name, value)


def install_v52_market_validation_controls(store: Any) -> MarketValidationController:
    """Install additive controls after v5.2 finalization.

    This layer can only preserve or reduce an already-authorized paper fraction.
    It cannot originate entries, raise sizing, relax thresholds, sign, or submit.
    """
    global _CONTROLLER, _INSTALLED, _BASE_SOLANA_CHOOSE, _BASE_FOMO_DECISION, _BASE_ROBINHOOD_CHOOSE
    from . import fomo_paper_strategy as fomo_paper
    from . import risk_conditioned_alpha_v5 as solana_strategy
    from . import v52_authoritative_strategy as authoritative
    from . import v52_robinhood_position_lifecycle as robinhood_lifecycle
    from .robinhood_chain_paper import RobinhoodChainPaperPlane

    if _CONTROLLER is None or _CONTROLLER.store is not store:
        _CONTROLLER = MarketValidationController(store)
    if _INSTALLED:
        return _CONTROLLER

    _BASE_SOLANA_CHOOSE = solana_strategy._choose_lane_and_fraction
    _BASE_FOMO_DECISION = fomo_paper._paper_decision
    _BASE_ROBINHOOD_CHOOSE = RobinhoodChainPaperPlane._v5_choose_lane_fraction
    _preserve_lineage(_validated_solana_choose, _BASE_SOLANA_CHOOSE)
    _preserve_lineage(_validated_fomo_decision, _BASE_FOMO_DECISION)
    _preserve_lineage(_validated_robinhood_choose, _BASE_ROBINHOOD_CHOOSE)

    _validated_solana_choose.__module__ = authoritative.__name__
    _validated_fomo_decision.__module__ = authoritative.__name__
    _validated_robinhood_choose.__module__ = robinhood_lifecycle.__name__

    solana_strategy._choose_lane_and_fraction = _validated_solana_choose
    fomo_paper._paper_decision = _validated_fomo_decision
    RobinhoodChainPaperPlane._v5_choose_lane_fraction = _validated_robinhood_choose  # type: ignore[method-assign]

    for wrapper in (
        solana_strategy._choose_lane_and_fraction,
        fomo_paper._paper_decision,
        RobinhoodChainPaperPlane._v5_choose_lane_fraction,
    ):
        setattr(wrapper, "_roi_v52_final_authority", True)
        setattr(wrapper, "_roi_v52_market_validation_controls", True)
        setattr(wrapper, "_roi_v52_profit_confidence_finalization", True)

    _INSTALLED = True
    return _CONTROLLER


def status(controller: MarketValidationController | None = None) -> dict[str, Any]:
    return {
        "version": VERSION,
        "installed": bool(_INSTALLED and (controller is not None or _CONTROLLER is not None)),
        "graduation_quality_score": True,
        "graduation_quality_fixed_thresholds": False,
        "post_graduation_decay_clock": True,
        "post_graduation_hard_wait_seconds": None,
        "post_graduation_hard_stop_seconds": None,
        "no_continuation_is_negative_evidence": True,
        "lane_relative_percentile_calibration": True,
        "permanent_shadow_strategies": shadow_strategy_definitions(),
        "shadow_strategies_control_trading": False,
        "lane_level_alpha_gating": True,
        "cold_start_lane_behavior": "preserve_v52",
        "fomo_state_separate_from_discovery_route": True,
        "ordinary_starter_fraction_unchanged": 0.25,
        "fixed_minus_12_stop_added": False,
        "fixed_plus_30_principal_recovery_added": False,
        "pre_graduation_entries_abandoned": False,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = [
    "VERSION",
    "ContinuationDecay",
    "GraduationQuality",
    "LaneAlphaGate",
    "MarketContext",
    "MarketValidationController",
    "RelativeScore",
    "SHADOW_STRATEGIES",
    "canonical_alpha_lane",
    "classify_market_context",
    "controller",
    "empirical_percentile",
    "graduation_quality_score",
    "install_v52_market_validation_controls",
    "lane_alpha_gate",
    "lane_relative_opportunity_score",
    "post_graduation_decay_clock",
    "shadow_strategy_definitions",
    "status",
]
