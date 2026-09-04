from __future__ import annotations

import math
from dataclasses import asdict
from typing import Any

from .risk_conditioned_alpha_v5 import robust_return_profile


ALLOCATOR_VERSION = "cross-regime-paper-allocator-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
MIN_SEGMENT_SAMPLES = 30
MAX_SEGMENT_WEIGHT = 0.50
UNKNOWN_CORRELATION_WEIGHT_CAP = 0.25


def _table_exists(store: Any, table: str) -> bool:
    with store._lock:
        row = store.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table,),
        ).fetchone()
    return row is not None


def _segment_returns(store: Any, release_commit: str) -> dict[str, list[float]]:
    grouped: dict[str, list[float]] = {}
    if _table_exists(store, "risk_conditioned_alpha_v5_outcomes"):
        with store._lock:
            rows = store.db.execute(
                "SELECT venue,net_return FROM risk_conditioned_alpha_v5_outcomes "
                "WHERE release_commit=? ORDER BY id",
                (release_commit,),
            ).fetchall()
        for row in rows:
            venue = str(row["venue"] or "UNKNOWN")
            grouped.setdefault(venue, []).append(float(row["net_return"]))
    if _table_exists(store, "fomo_paper_outcomes"):
        with store._lock:
            rows = store.db.execute(
                "SELECT net_return FROM fomo_paper_outcomes WHERE release_commit=? ORDER BY id",
                (release_commit,),
            ).fetchall()
        grouped["FOMO"] = [float(row["net_return"]) for row in rows]
    if _table_exists(store, "robinhood_paper_outcomes"):
        with store._lock:
            rows = store.db.execute(
                "SELECT net_return FROM robinhood_paper_outcomes WHERE release_commit=? ORDER BY id",
                (release_commit,),
            ).fetchall()
        grouped["ROBINHOOD_CHAIN"] = [float(row["net_return"]) for row in rows]
    return grouped


def _score(profile: Any) -> float:
    growth = profile.best_expected_log_growth
    if growth is None or not math.isfinite(growth) or growth <= 0.0:
        return 0.0
    drawdown = float(profile.max_drawdown_at_best_fraction or 0.0)
    shortfall = float(profile.expected_shortfall_20 or 0.0)
    tail_penalty = max(0.0, -shortfall)
    return max(0.0, float(growth)) / (1.0 + drawdown + tail_penalty)


def _capped_normalize(scores: dict[str, float], caps: dict[str, float]) -> dict[str, float]:
    positive = {key: value for key, value in scores.items() if value > 0.0}
    if not positive:
        return {}
    weights = {key: 0.0 for key in positive}
    remaining = 1.0
    active = set(positive)
    for _ in range(len(active) + 2):
        if not active or remaining <= 1e-12:
            break
        denom = sum(positive[key] for key in active)
        if denom <= 0.0:
            break
        saturated: list[str] = []
        for key in list(active):
            proposed = remaining * positive[key] / denom
            room = max(0.0, caps[key] - weights[key])
            if proposed >= room - 1e-12:
                weights[key] += room
                remaining -= room
                saturated.append(key)
        if saturated:
            active.difference_update(saturated)
            continue
        for key in active:
            add = remaining * positive[key] / denom
            weights[key] += add
        remaining = 0.0
        break
    return weights


def build_cross_regime_allocation(store: Any, release_commit: str) -> dict[str, Any]:
    """Allocate only mature, robust-positive paper regimes; unallocated weight is cash.

    Cross-regime correlation is deliberately fail-closed. Until sufficiently aligned
    forward observations exist to estimate it reliably, any one regime is capped at
    25% rather than assuming independence. Once correlation evidence is added, that
    cap can be relaxed up to the hard 50% per-regime ceiling.
    """
    grouped = _segment_returns(store, release_commit)
    profiles: dict[str, dict[str, Any]] = {}
    scores: dict[str, float] = {}
    caps: dict[str, float] = {}
    for segment, values in grouped.items():
        profile = robust_return_profile(values, max_fraction=0.20)
        profiles[segment] = asdict(profile)
        mature = profile.sample_count >= MIN_SEGMENT_SAMPLES
        promoted = profile.state == "promoted_positive_log_growth"
        scores[segment] = _score(profile) if mature and promoted else 0.0
        caps[segment] = min(MAX_SEGMENT_WEIGHT, UNKNOWN_CORRELATION_WEIGHT_CAP)
    weights = _capped_normalize(scores, caps)
    allocated = min(1.0, sum(weights.values()))
    return {
        "allocator_version": ALLOCATOR_VERSION,
        "release_commit": release_commit,
        "authority": "paper_only_if_forward_mature_and_robust_positive",
        "minimum_segment_samples": MIN_SEGMENT_SAMPLES,
        "correlation_policy": "unknown_correlation_is_not_zero; cap_each_segment_until_aligned_forward_evidence",
        "max_segment_weight": MAX_SEGMENT_WEIGHT,
        "unknown_correlation_weight_cap": UNKNOWN_CORRELATION_WEIGHT_CAP,
        "segment_profiles": profiles,
        "segment_scores": scores,
        "paper_allocation_weights": weights,
        "paper_cash_weight": max(0.0, 1.0 - allocated),
        "mature_promoted_segments": sum(1 for score in scores.values() if score > 0.0),
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = ["ALLOCATOR_VERSION", "build_cross_regime_allocation"]
