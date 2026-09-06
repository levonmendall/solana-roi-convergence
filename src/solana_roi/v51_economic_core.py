from __future__ import annotations

import math
import random
from statistics import mean, median, stdev
from typing import Any, Iterable, Sequence

from .strategy_v51_authority import authority, hazard_requirements
from .v51_return_validation import STATISTICS_VERSION, valid_return_values

POSITION_GRID = (0.0025, 0.005, 0.01, 0.02, 0.05, 0.10, 0.20)
BOOTSTRAP_SAMPLES = 800
BOOTSTRAP_SEED = 5107108


def _finite_values(values: Iterable[float]) -> list[float]:
    """Canonical v5.1 economic returns, including exact total losses."""
    return valid_return_values(values)


def _expected_log_growth(values: Sequence[float], fraction: float) -> float | None:
    if not values:
        return None
    logs: list[float] = []
    for value in values:
        terminal = 1.0 + fraction * value
        if terminal <= 0.0:
            return float("-inf")
        logs.append(math.log(terminal))
    return mean(logs)


def _drawdown(values: Sequence[float], fraction: float) -> float:
    nav = 1.0
    peak = 1.0
    worst = 0.0
    for value in values:
        nav *= max(0.0, 1.0 + fraction * value)
        peak = max(peak, nav)
        worst = max(worst, 1.0 - nav / peak if peak > 0.0 else 1.0)
    return worst


def _confidence(values: Sequence[float]) -> tuple[float | None, float | None]:
    """Legacy normal interval retained for side-by-side migration visibility."""
    if not values:
        return None, None
    center = mean(values)
    if len(values) < 2:
        return center, center
    se = stdev(values) / math.sqrt(len(values))
    return center - 1.96 * se, center + 1.96 * se


def _quantile(values: Sequence[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = max(0.0, min(1.0, q)) * (len(ordered) - 1)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _expected_shortfall(values: Sequence[float], tail_fraction: float = 0.20) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    tail_n = max(1, int(math.ceil(len(ordered) * tail_fraction)))
    return mean(ordered[:tail_n])


def _cluster_groups(values: Sequence[float], cluster_ids: Sequence[str] | None) -> list[list[float]]:
    if cluster_ids is None or len(cluster_ids) != len(values):
        return [[float(value)] for value in values]
    grouped: dict[str, list[float]] = {}
    for value, cluster_id in zip(values, cluster_ids):
        grouped.setdefault(str(cluster_id), []).append(float(value))
    return list(grouped.values())


def _bootstrap_distributions(
    values: Sequence[float],
    *,
    fraction: float,
    cluster_ids: Sequence[str] | None = None,
    samples: int = BOOTSTRAP_SAMPLES,
) -> dict[str, list[float]]:
    groups = _cluster_groups(values, cluster_ids)
    if not groups:
        return {"mean": [], "median": [], "log_growth": [], "expected_shortfall_20": []}
    rng = random.Random(BOOTSTRAP_SEED + len(values) * 131 + len(groups) * 17)
    distributions: dict[str, list[float]] = {
        "mean": [],
        "median": [],
        "log_growth": [],
        "expected_shortfall_20": [],
    }
    count = max(1, int(samples))
    for _ in range(count):
        draw: list[float] = []
        for _index in range(len(groups)):
            draw.extend(groups[rng.randrange(len(groups))])
        distributions["mean"].append(mean(draw))
        distributions["median"].append(median(draw))
        growth = _expected_log_growth(draw, fraction)
        if growth is not None:
            distributions["log_growth"].append(growth)
        shortfall = _expected_shortfall(draw)
        if shortfall is not None:
            distributions["expected_shortfall_20"].append(shortfall)
    return distributions


def _bootstrap_interval(values: Sequence[float]) -> tuple[float | None, float | None]:
    return _quantile(values, 0.025), _quantile(values, 0.975)


def robust_profile(
    values: Iterable[float],
    *,
    max_fraction: float = 0.20,
    fixed_fraction: float | None = None,
    cluster_ids: Sequence[str] | None = None,
    bootstrap_samples: int = BOOTSTRAP_SAMPLES,
) -> dict[str, Any]:
    series = _finite_values(values)
    n = len(series)
    empty = {
        "statistics_version": STATISTICS_VERSION,
        "sample_count": 0,
        "mean_return": None,
        "median_return": None,
        "hit_rate": None,
        "expected_shortfall_20": None,
        "leave_best_trade_out_mean": None,
        "remove_top_3_mean": None,
        "remove_top_5_mean": None,
        "winner_concentration": None,
        "mean_return_ci95_lower": None,
        "mean_return_ci95_upper": None,
        "mean_return_bootstrap_ci95_lower": None,
        "mean_return_bootstrap_ci95_upper": None,
        "median_return_bootstrap_ci95_lower": None,
        "median_return_bootstrap_ci95_upper": None,
        "best_fraction": 0.0 if fixed_fraction is None else float(fixed_fraction),
        "fraction_selection_mode": "optimized_on_sample" if fixed_fraction is None else "preselected_fixed_fraction",
        "best_expected_log_growth": None,
        "expected_log_growth_ci95_lower": None,
        "expected_log_growth_ci95_upper": None,
        "expected_log_growth_bootstrap_ci95_lower": None,
        "expected_log_growth_bootstrap_ci95_upper": None,
        "lower_confidence_expected_log_growth": None,
        "expected_shortfall_20_bootstrap_ci95_lower": None,
        "expected_shortfall_20_bootstrap_ci95_upper": None,
        "remove_top_1_expected_log_growth_bootstrap_ci95_lower": None,
        "remove_top_3_expected_log_growth_bootstrap_ci95_lower": None,
        "max_drawdown_at_best_fraction": None,
        "compounded_nav_at_best_fraction": 1.0,
        "bootstrap_unit": "token_event_cluster",
        "bootstrap_samples": int(bootstrap_samples),
        "legacy_normal_confidence_retained_for_migration": True,
    }
    if not series:
        return empty

    ordered = sorted(series)
    positive = [max(0.0, value) for value in series]
    total_positive = sum(positive)
    best_positive = max(positive) if positive else 0.0
    ci_lower, ci_upper = _confidence(series)

    if fixed_fraction is None:
        fractions = [value for value in POSITION_GRID if value <= max_fraction + 1e-12]
        if max_fraction > 0.0 and not fractions:
            fractions = [max_fraction]
        scored = [(fraction, _expected_log_growth(series, fraction)) for fraction in fractions]
        best_fraction, best_growth = max(
            scored,
            key=lambda pair: pair[1] if pair[1] is not None else float("-inf"),
        )
        selection_mode = "optimized_on_sample"
    else:
        best_fraction = max(0.0, min(float(fixed_fraction), max_fraction))
        best_growth = _expected_log_growth(series, best_fraction)
        selection_mode = "preselected_fixed_fraction"

    log_series = [math.log(1.0 + best_fraction * value) for value in series]
    growth_ci_lower, growth_ci_upper = _confidence(log_series)
    distributions = _bootstrap_distributions(
        series,
        fraction=best_fraction,
        cluster_ids=cluster_ids,
        samples=bootstrap_samples,
    )
    mean_bootstrap = _bootstrap_interval(distributions["mean"])
    median_bootstrap = _bootstrap_interval(distributions["median"])
    growth_bootstrap = _bootstrap_interval(distributions["log_growth"])
    shortfall_bootstrap = _bootstrap_interval(distributions["expected_shortfall_20"])

    nav = 1.0
    for value in series:
        nav *= max(0.0, 1.0 + best_fraction * value)

    def removed(k: int) -> list[float]:
        if n <= k:
            return []
        return sorted(series, reverse=True)[k:]

    def removed_mean(k: int) -> float | None:
        remaining = removed(k)
        return mean(remaining) if remaining else None

    def removed_growth_lower(k: int) -> float | None:
        remaining = removed(k)
        if not remaining:
            return None
        distribution = _bootstrap_distributions(
            remaining,
            fraction=best_fraction,
            samples=bootstrap_samples,
        )["log_growth"]
        return _quantile(distribution, 0.025)

    return {
        "statistics_version": STATISTICS_VERSION,
        "sample_count": n,
        "mean_return": mean(series),
        "median_return": median(series),
        "hit_rate": sum(1 for value in series if value > 0.0) / n,
        "expected_shortfall_20": _expected_shortfall(ordered),
        "leave_best_trade_out_mean": removed_mean(1),
        "remove_top_3_mean": removed_mean(3),
        "remove_top_5_mean": removed_mean(5),
        "winner_concentration": (best_positive / total_positive) if total_positive > 0.0 else 0.0,
        "mean_return_ci95_lower": ci_lower,
        "mean_return_ci95_upper": ci_upper,
        "mean_return_bootstrap_ci95_lower": mean_bootstrap[0],
        "mean_return_bootstrap_ci95_upper": mean_bootstrap[1],
        "median_return_bootstrap_ci95_lower": median_bootstrap[0],
        "median_return_bootstrap_ci95_upper": median_bootstrap[1],
        "best_fraction": best_fraction,
        "fraction_selection_mode": selection_mode,
        "best_expected_log_growth": best_growth,
        "expected_log_growth_ci95_lower": growth_ci_lower,
        "expected_log_growth_ci95_upper": growth_ci_upper,
        "expected_log_growth_bootstrap_ci95_lower": growth_bootstrap[0],
        "expected_log_growth_bootstrap_ci95_upper": growth_bootstrap[1],
        "lower_confidence_expected_log_growth": growth_bootstrap[0],
        "expected_shortfall_20_bootstrap_ci95_lower": shortfall_bootstrap[0],
        "expected_shortfall_20_bootstrap_ci95_upper": shortfall_bootstrap[1],
        "remove_top_1_expected_log_growth_bootstrap_ci95_lower": removed_growth_lower(1),
        "remove_top_3_expected_log_growth_bootstrap_ci95_lower": removed_growth_lower(3),
        "max_drawdown_at_best_fraction": _drawdown(series, best_fraction),
        "compounded_nav_at_best_fraction": nav,
        "bootstrap_unit": "token_event_cluster",
        "bootstrap_samples": int(bootstrap_samples),
        "legacy_normal_confidence_retained_for_migration": True,
    }


def _weighted_stat(
    exact: list[float],
    parent: list[float],
    family: list[float],
    transform,
) -> float | None:
    cfg = authority()["hierarchical_pooling"]
    pieces: list[tuple[float, float]] = []
    if exact:
        pieces.append((float(len(exact)), mean(transform(value) for value in exact)))
    if parent:
        pieces.append((float(min(len(parent), int(cfg["parent_effective_sample_cap"]))), mean(transform(value) for value in parent)))
    if family:
        pieces.append((float(min(len(family), int(cfg["family_effective_sample_cap"]))), mean(transform(value) for value in family)))
    denominator = sum(weight for weight, _ in pieces)
    if denominator <= 0.0:
        return None
    return sum(weight * value for weight, value in pieces) / denominator


def hierarchical_profile(
    exact_values: Iterable[float],
    parent_values: Iterable[float],
    family_values: Iterable[float],
    *,
    risk_severity: float = 0.0,
    risk_signature: str = "clean",
    max_fraction: float = 0.20,
) -> dict[str, Any]:
    exact = _finite_values(exact_values)
    parent = _finite_values(parent_values)
    family = _finite_values(family_values)
    requirements = hazard_requirements(risk_severity, risk_signature)
    base = robust_profile(exact, max_fraction=max_fraction)
    fractions = [value for value in POSITION_GRID if value <= max_fraction + 1e-12] or [max_fraction]
    log_scores: list[tuple[float, float | None]] = []
    for fraction in fractions:
        value = _weighted_stat(
            exact,
            parent,
            family,
            lambda item, selected=fraction: math.log(1.0 + selected * item),
        )
        log_scores.append((fraction, value))
    best_fraction, shrunk_growth = max(
        log_scores,
        key=lambda pair: pair[1] if pair[1] is not None else float("-inf"),
    )
    shrunk_mean = _weighted_stat(exact, parent, family, lambda value: value)
    unique_evidence_n = len(exact) + len(parent) + len(family)
    exact_min = int(requirements["minimum_exact_outcomes"])
    independent_min = int(requirements["minimum_independent_outcomes"])
    hurdle = float(requirements["minimum_expected_log_growth"])
    robust_positive = (
        base["leave_best_trade_out_mean"] is not None
        and float(base["leave_best_trade_out_mean"]) > 0.0
    )
    mature = len(exact) >= exact_min and unique_evidence_n >= independent_min
    promoted = bool(mature and shrunk_growth is not None and shrunk_growth > hurdle and robust_positive)
    kill_min = max(int(authority()["kill_policy"]["minimum_independent_outcomes"]), independent_min)
    killed = bool(
        len(exact) >= exact_min
        and unique_evidence_n >= kill_min
        and shrunk_growth is not None
        and shrunk_growth <= 0.0
        and base["leave_best_trade_out_mean"] is not None
        and float(base["leave_best_trade_out_mean"]) <= 0.0
        and base["mean_return_ci95_upper"] is not None
        and float(base["mean_return_ci95_upper"]) <= 0.0
    )
    state = "killed_negative_robust_edge" if killed else "promoted_positive_hierarchical_edge" if promoted else "mature_unproven" if mature else "bootstrap_hierarchical_evidence"
    return {
        **base,
        "best_fraction": best_fraction,
        "best_expected_log_growth": shrunk_growth,
        "shrunk_mean_return": shrunk_mean,
        "exact_sample_count": len(exact),
        "parent_sample_count": len(parent),
        "family_sample_count": len(family),
        "independent_evidence_count": unique_evidence_n,
        "hazard_bin": requirements["hazard_bin"],
        "minimum_exact_outcomes": exact_min,
        "minimum_independent_outcomes": independent_min,
        "minimum_expected_log_growth": hurdle,
        "state": state,
        "promoted": promoted,
        "killed": killed,
    }


def bootstrap_execution_multiplier(
    *,
    latency_seconds: float | None,
    chase_fraction: float | None,
    round_trip_cost_fraction: float | None,
    risk_severity: float = 0.0,
    risk_signature: str = "clean",
) -> float:
    cfg = authority()["execution"]
    latency = 0.0 if latency_seconds is None else max(0.0, float(latency_seconds))
    chase = 0.0 if chase_fraction is None else max(0.0, float(chase_fraction))
    cost = 0.0 if round_trip_cost_fraction is None else max(0.0, float(round_trip_cost_fraction))
    if latency_seconds is None or latency > float(cfg["latency_hard_max_seconds"]):
        return 0.0
    if chase > float(cfg["chase_observe_only_above_fraction"]):
        return 0.0
    latency_factor = math.exp(-max(0.0, latency - 2.0) / 18.0)
    chase_factor = math.exp(-2.5 * max(0.0, chase - 0.05))
    cost_factor = math.exp(-2.0 * max(0.0, cost - 0.03))
    hazard_factor = float(hazard_requirements(risk_severity, risk_signature)["bootstrap_size_multiplier"])
    return max(0.0, min(1.0, latency_factor * chase_factor * cost_factor * hazard_factor))


def stress_returns(values: Iterable[float], scenario: dict[str, Any]) -> list[float]:
    series = _finite_values(values)
    extra_latency = max(0.0, float(scenario.get("extra_latency_seconds") or 0.0))
    cost = max(0.0, float(scenario.get("extra_round_trip_cost_fraction") or 0.0))
    adverse = max(0.0, float(scenario.get("adverse_selection_fraction") or 0.0))
    failure = max(0.0, min(1.0, float(scenario.get("failure_probability") or 0.0)))
    latency_decay = math.exp(-extra_latency / 20.0)
    failed_return = -min(0.25, cost + adverse)
    stressed: list[float] = []
    for value in series:
        decayed = value * latency_decay if value > 0.0 else value
        executed = (1.0 + decayed) * (1.0 - cost) * (1.0 - adverse) - 1.0
        stressed.append((1.0 - failure) * executed + failure * failed_return)
    return stressed


def execution_stress_profiles(
    values: Iterable[float],
    *,
    max_fraction: float = 0.20,
    fixed_fraction: float | None = None,
) -> dict[str, Any]:
    series = list(values)
    return {
        str(scenario["name"]): robust_profile(
            stress_returns(series, scenario),
            max_fraction=max_fraction,
            fixed_fraction=fixed_fraction,
        )
        for scenario in authority()["execution_stress"]
    }


def incremental_alpha_profile(wallet_values: Iterable[float], identity_free_values: Iterable[float]) -> dict[str, Any]:
    wallet = _finite_values(wallet_values)
    baseline = _finite_values(identity_free_values)
    paired_n = min(len(wallet), len(baseline))
    if paired_n == 0:
        return {"paired_sample_count": 0, "residual_profile": robust_profile(())}
    residuals = [wallet[index] - baseline[index] for index in range(paired_n)]
    profile = robust_profile(residuals)
    return {
        "paired_sample_count": paired_n,
        "residual_profile": profile,
        "wallet_identity_adds_forward_edge": bool(
            paired_n >= 20
            and profile["leave_best_trade_out_mean"] is not None
            and float(profile["leave_best_trade_out_mean"]) > 0.0
            and profile["lower_confidence_expected_log_growth"] is not None
            and float(profile["lower_confidence_expected_log_growth"]) > 0.0
        ),
    }


__all__ = [
    "BOOTSTRAP_SAMPLES",
    "bootstrap_execution_multiplier",
    "execution_stress_profiles",
    "hierarchical_profile",
    "incremental_alpha_profile",
    "robust_profile",
    "stress_returns",
]
