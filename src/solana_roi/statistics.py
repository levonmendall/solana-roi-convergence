from __future__ import annotations

import math
from statistics import mean, stdev
from typing import Any, Iterable, Mapping

from .v51_economic_core import (
    BOOTSTRAP_SAMPLES,
    bootstrap_execution_multiplier,
    execution_stress_profiles,
    hierarchical_profile,
    incremental_alpha_profile,
    robust_profile,
    stress_returns,
)
from .v51_return_validation import STATISTICS_VERSION, return_integrity_summary, valid_return_values, validate_return

STATISTICAL_CORE_VERSION = "v51-canonical-statistical-core-128-v1"
DEFAULT_FDR_Q = 0.10
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False


def positive_edge_p_value(values: Iterable[float]) -> float:
    clean = [float(value) for value in values if math.isfinite(float(value))]
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


def winner_removal_profile(values: Iterable[float], *, fixed_fraction: float | None = None) -> dict[str, Any]:
    clean = valid_return_values(values)
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
        "owns_point_estimates": True,
        "owns_bootstrap_inference": True,
        "owns_winner_removal": True,
        "owns_fdr_helpers": True,
        "owns_evidence_state": True,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "BOOTSTRAP_SAMPLES",
    "DEFAULT_FDR_Q",
    "STATISTICAL_CORE_VERSION",
    "benjamini_hochberg",
    "bootstrap_execution_multiplier",
    "evidence_state",
    "execution_stress_profiles",
    "hierarchical_profile",
    "incremental_alpha_profile",
    "positive_edge_p_value",
    "robust_profile",
    "status",
    "stress_returns",
    "validate_return",
    "winner_removal_profile",
]
