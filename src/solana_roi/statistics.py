from __future__ import annotations

import math
from statistics import mean, stdev
from typing import Any, Iterable, Mapping, Sequence

from .v51_economic_core import (
    BOOTSTRAP_SAMPLES,
    bootstrap_execution_multiplier,
    execution_stress_profiles,
    hierarchical_profile,
    incremental_alpha_profile,
    robust_profile,
    stress_returns,
)
from .v51_return_validation import (
    STATISTICS_VERSION,
    return_integrity_summary,
    valid_return_values,
    validate_return,
)

STATISTICAL_CORE_VERSION = "v51-canonical-statistical-core-128-v2"
DEFAULT_FDR_Q = 0.10
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False


def _valid(values: Iterable[float]) -> list[float]:
    """One public return-validation boundary; exact -1 remains a valid loss."""
    return valid_return_values(values)


def positive_edge_p_value(values: Iterable[float]) -> float:
    clean = _valid(values)
    if len(clean) < 2:
        return 1.0
    sigma = stdev(clean)
    if sigma <= 0.0:
        return 0.0 if mean(clean) > 0.0 else 1.0
    z = mean(clean) / (sigma / math.sqrt(len(clean)))
    return max(0.0, min(1.0, 0.5 * math.erfc(z / math.sqrt(2.0))))


def benjamini_hochberg(p_values: Mapping[str, float], *, q: float = DEFAULT_FDR_Q) -> dict[str, bool]:
    ordered = sorted((max(0.0, min(1.0, float(p))), str(key)) for key, p in p_values.items())
    cutoff_rank = 0
    total = len(ordered)
    for rank, (p_value, _key) in enumerate(ordered, start=1):
        if p_value <= float(q) * rank / max(1, total):
            cutoff_rank = rank
    accepted = {str(key): False for key in p_values}
    if cutoff_rank:
        threshold = ordered[cutoff_rank - 1][0]
        for p_value, key in ordered:
            accepted[key] = p_value <= threshold
    return accepted


def expected_log_growth(values: Iterable[float], *, fraction: float) -> float | None:
    clean = _valid(values)
    if not clean:
        return None
    selected = max(0.0, float(fraction))
    logs: list[float] = []
    for value in clean:
        terminal = 1.0 + selected * value
        if terminal <= 0.0:
            return float("-inf")
        logs.append(math.log(terminal))
    return mean(logs)


def drawdown(values: Iterable[float], *, fraction: float) -> float:
    clean = _valid(values)
    selected = max(0.0, float(fraction))
    nav = 1.0
    peak = 1.0
    worst = 0.0
    for value in clean:
        nav *= max(0.0, 1.0 + selected * value)
        peak = max(peak, nav)
        worst = max(worst, 1.0 - nav / peak if peak > 0.0 else 1.0)
    return worst


def expected_shortfall(values: Iterable[float], *, tail_fraction: float = 0.20) -> float | None:
    clean = sorted(_valid(values))
    if not clean:
        return None
    fraction = max(0.0, min(1.0, float(tail_fraction)))
    tail_n = max(1, int(math.ceil(len(clean) * fraction)))
    return mean(clean[:tail_n])


def event_cluster_profile(
    values: Iterable[float],
    *,
    cluster_ids: Sequence[str] | None,
    fixed_fraction: float | None = None,
    bootstrap_samples: int = BOOTSTRAP_SAMPLES,
) -> dict[str, Any]:
    """Canonical cluster-aware bootstrap/robustness profile."""
    return robust_profile(
        _valid(values),
        fixed_fraction=fixed_fraction,
        cluster_ids=cluster_ids,
        bootstrap_samples=bootstrap_samples,
    )


def bootstrap_ci(
    values: Iterable[float],
    *,
    cluster_ids: Sequence[str] | None = None,
    fixed_fraction: float | None = None,
    bootstrap_samples: int = BOOTSTRAP_SAMPLES,
) -> dict[str, float | None]:
    profile = event_cluster_profile(
        values,
        cluster_ids=cluster_ids,
        fixed_fraction=fixed_fraction,
        bootstrap_samples=bootstrap_samples,
    )
    return {
        "mean_lower": profile["mean_return_bootstrap_ci95_lower"],
        "mean_upper": profile["mean_return_bootstrap_ci95_upper"],
        "median_lower": profile["median_return_bootstrap_ci95_lower"],
        "median_upper": profile["median_return_bootstrap_ci95_upper"],
        "log_growth_lower": profile["expected_log_growth_bootstrap_ci95_lower"],
        "log_growth_upper": profile["expected_log_growth_bootstrap_ci95_upper"],
        "expected_shortfall_lower": profile["expected_shortfall_20_bootstrap_ci95_lower"],
        "expected_shortfall_upper": profile["expected_shortfall_20_bootstrap_ci95_upper"],
    }


def sizing_profile(
    values: Iterable[float],
    *,
    max_fraction: float = 0.20,
    preselected_fraction: float | None = None,
    cluster_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Canonical sizing output. Holdouts pass a preselected fraction explicitly."""
    profile = robust_profile(
        _valid(values),
        max_fraction=max_fraction,
        fixed_fraction=preselected_fraction,
        cluster_ids=cluster_ids,
    )
    return {
        "statistical_core_version": STATISTICAL_CORE_VERSION,
        "best_fraction": profile["best_fraction"],
        "fraction_selection_mode": profile["fraction_selection_mode"],
        "lower_confidence_expected_log_growth": profile["lower_confidence_expected_log_growth"],
        "max_drawdown_at_best_fraction": profile["max_drawdown_at_best_fraction"],
        "preselected_holdout_fraction_required": preselected_fraction is not None,
    }


def maturity_kill_profile(
    exact_values: Iterable[float],
    parent_values: Iterable[float],
    family_values: Iterable[float],
    *,
    risk_severity: float = 0.0,
    risk_signature: str = "clean",
    max_fraction: float = 0.20,
) -> dict[str, Any]:
    """Canonical v5.1 maturity/promotion/kill inference."""
    return hierarchical_profile(
        _valid(exact_values),
        _valid(parent_values),
        _valid(family_values),
        risk_severity=risk_severity,
        risk_signature=risk_signature,
        max_fraction=max_fraction,
    )


def winner_removal_profile(values: Iterable[float], *, fixed_fraction: float | None = None) -> dict[str, Any]:
    clean = _valid(values)
    ordered = sorted(clean, reverse=True)
    return {
        "statistical_core_version": STATISTICAL_CORE_VERSION,
        "full": robust_profile(clean, fixed_fraction=fixed_fraction),
        "remove_top_1": robust_profile(ordered[1:], fixed_fraction=fixed_fraction) if len(ordered) > 1 else robust_profile((), fixed_fraction=fixed_fraction),
        "remove_top_3": robust_profile(ordered[3:], fixed_fraction=fixed_fraction) if len(ordered) > 3 else robust_profile((), fixed_fraction=fixed_fraction),
        "remove_top_5": robust_profile(ordered[5:], fixed_fraction=fixed_fraction) if len(ordered) > 5 else robust_profile((), fixed_fraction=fixed_fraction),
        "exact_total_losses_remain_valid": any(value == -1.0 for value in clean),
    }


def evidence_state(rows: Iterable[Mapping[str, Any]], *, minimum_valid: int = 1) -> dict[str, Any]:
    materialized = [dict(row) for row in rows]
    integrity = return_integrity_summary(materialized)
    valid_n = int(integrity.get("valid_economic_measurement_count") or 0)
    proof_eligible = bool(integrity.get("proof_eligible")) and valid_n >= max(0, int(minimum_valid))
    return {
        "statistical_core_version": STATISTICAL_CORE_VERSION,
        "statistics_version": STATISTICS_VERSION,
        "state": "proof_eligible" if proof_eligible else "insufficient_or_invalid_evidence",
        "proof_eligible": proof_eligible,
        "valid_economic_measurement_count": valid_n,
        "minimum_valid_economic_measurements": max(0, int(minimum_valid)),
        "measurement_integrity": integrity,
        "invalid_returns_are_never_imputed": True,
        "paper_only": True,
        "live_money_authority": False,
    }


def status() -> dict[str, Any]:
    return {
        "statistical_core_version": STATISTICAL_CORE_VERSION,
        "statistics_version": STATISTICS_VERSION,
        "bootstrap_samples": BOOTSTRAP_SAMPLES,
        "owns_return_validation": True,
        "owns_event_clustering": True,
        "owns_log_growth": True,
        "owns_drawdown": True,
        "owns_expected_shortfall": True,
        "owns_bootstrap_inference": True,
        "owns_winner_removal": True,
        "owns_stress": True,
        "owns_sizing": True,
        "owns_fdr": True,
        "owns_maturity_and_kill": True,
        "owns_evidence_state": True,
        "legacy_economic_core_role": "implementation_detail_pending_safe_namespace_retirement",
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "BOOTSTRAP_SAMPLES",
    "DEFAULT_FDR_Q",
    "STATISTICAL_CORE_VERSION",
    "benjamini_hochberg",
    "bootstrap_ci",
    "bootstrap_execution_multiplier",
    "drawdown",
    "evidence_state",
    "event_cluster_profile",
    "execution_stress_profiles",
    "expected_log_growth",
    "expected_shortfall",
    "hierarchical_profile",
    "incremental_alpha_profile",
    "maturity_kill_profile",
    "positive_edge_p_value",
    "robust_profile",
    "sizing_profile",
    "status",
    "stress_returns",
    "validate_return",
    "winner_removal_profile",
]
