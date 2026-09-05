from __future__ import annotations

import math
from statistics import mean, median, stdev
from typing import Any, Iterable

from .strategy_v51_authority import authority, hazard_requirements

POSITION_GRID = (0.0025, 0.005, 0.01, 0.02, 0.05, 0.10, 0.20)


def _finite_values(values: Iterable[float]) -> list[float]:
    result: list[float] = []
    for raw in values:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if math.isfinite(value) and value > -1.0:
            result.append(value)
    return result


def _expected_log_growth(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    logs = [math.log1p(max(-0.999999, fraction * value)) for value in values]
    return mean(logs)


def _drawdown(values: list[float], fraction: float) -> float:
    nav = 1.0
    peak = 1.0
    worst = 0.0
    for value in values:
        nav *= max(1e-9, 1.0 + fraction * value)
        peak = max(peak, nav)
        worst = max(worst, 1.0 - nav / peak)
    return worst


def _confidence(values: list[float]) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    center = mean(values)
    if len(values) < 2:
        return center, center
    se = stdev(values) / math.sqrt(len(values))
    return center - 1.96 * se, center + 1.96 * se


def robust_profile(values: Iterable[float], *, max_fraction: float = 0.20) -> dict[str, Any]:
    series = _finite_values(values)
    n = len(series)
    if not series:
        return {
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
            "best_fraction": 0.0,
            "best_expected_log_growth": None,
            "max_drawdown_at_best_fraction": None,
            "compounded_nav_at_best_fraction": 1.0,
        }
    ordered = sorted(series)
    tail_n = max(1, int(math.ceil(n * 0.20)))
    positive = [max(0.0, x) for x in series]
    total_positive = sum(positive)
    best_positive = max(positive) if positive else 0.0
    ci_lower, ci_upper = _confidence(series)
    fractions = [x for x in POSITION_GRID if x <= max_fraction + 1e-12]
    if max_fraction > 0 and not fractions:
        fractions = [max_fraction]
    scored = [(fraction, _expected_log_growth(series, fraction)) for fraction in fractions]
    best_fraction, best_growth = max(scored, key=lambda pair: pair[1] if pair[1] is not None else float("-inf"))
    nav = 1.0
    for value in series:
        nav *= max(1e-9, 1.0 + best_fraction * value)
    def removed(k: int) -> float | None:
        if n <= k:
            return None
        return mean(sorted(series, reverse=True)[k:])
    return {
        "sample_count": n,
        "mean_return": mean(series),
        "median_return": median(series),
        "hit_rate": sum(1 for x in series if x > 0.0) / n,
        "expected_shortfall_20": mean(ordered[:tail_n]),
        "leave_best_trade_out_mean": removed(1),
        "remove_top_3_mean": removed(3),
        "remove_top_5_mean": removed(5),
        "winner_concentration": (best_positive / total_positive) if total_positive > 0.0 else 0.0,
        "mean_return_ci95_lower": ci_lower,
        "mean_return_ci95_upper": ci_upper,
        "best_fraction": best_fraction,
        "best_expected_log_growth": best_growth,
        "max_drawdown_at_best_fraction": _drawdown(series, best_fraction),
        "compounded_nav_at_best_fraction": nav,
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
        pieces.append((float(len(exact)), mean(transform(x) for x in exact)))
    if parent:
        pieces.append((float(min(len(parent), int(cfg["parent_effective_sample_cap"]))), mean(transform(x) for x in parent)))
    if family:
        pieces.append((float(min(len(family), int(cfg["family_effective_sample_cap"]))), mean(transform(x) for x in family)))
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
    fractions = [x for x in POSITION_GRID if x <= max_fraction + 1e-12] or [max_fraction]
    log_scores: list[tuple[float, float | None]] = []
    for fraction in fractions:
        value = _weighted_stat(
            exact,
            parent,
            family,
            lambda x, f=fraction: math.log1p(max(-0.999999, f * x)),
        )
        log_scores.append((fraction, value))
    best_fraction, shrunk_growth = max(
        log_scores,
        key=lambda pair: pair[1] if pair[1] is not None else float("-inf"),
    )
    shrunk_mean = _weighted_stat(exact, parent, family, lambda x: x)
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


def execution_stress_profiles(values: Iterable[float], *, max_fraction: float = 0.20) -> dict[str, Any]:
    series = list(values)
    return {
        str(scenario["name"]): robust_profile(stress_returns(series, scenario), max_fraction=max_fraction)
        for scenario in authority()["execution_stress"]
    }


def incremental_alpha_profile(wallet_values: Iterable[float], identity_free_values: Iterable[float]) -> dict[str, Any]:
    wallet = _finite_values(wallet_values)
    baseline = _finite_values(identity_free_values)
    paired_n = min(len(wallet), len(baseline))
    if paired_n == 0:
        return {"paired_sample_count": 0, "residual_profile": robust_profile(())}
    residuals = [wallet[i] - baseline[i] for i in range(paired_n)]
    profile = robust_profile(residuals)
    return {
        "paired_sample_count": paired_n,
        "residual_profile": profile,
        "wallet_identity_adds_forward_edge": bool(
            paired_n >= 20
            and profile["leave_best_trade_out_mean"] is not None
            and float(profile["leave_best_trade_out_mean"]) > 0.0
            and profile["best_expected_log_growth"] is not None
            and float(profile["best_expected_log_growth"]) > 0.0
        ),
    }


__all__ = [
    "robust_profile",
    "hierarchical_profile",
    "bootstrap_execution_multiplier",
    "stress_returns",
    "execution_stress_profiles",
    "incremental_alpha_profile",
]
