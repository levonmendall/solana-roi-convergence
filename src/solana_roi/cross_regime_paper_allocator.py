from __future__ import annotations

import json
import math
from dataclasses import asdict
from typing import Any

from .risk_conditioned_alpha_v5 import robust_return_profile


ALLOCATOR_VERSION = "cross-regime-paper-allocator-v2-context-isolated"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
MIN_SEGMENT_SAMPLES = 30
MAX_FAMILY_WEIGHT = 0.50
UNKNOWN_CORRELATION_FAMILY_CAP = 0.25
ALLOCATOR_EVIDENCE_POSITION_GRID = (0.005, 0.01, 0.02, 0.05)

_FOMO_NON_HAZARD_VARIANTS = frozenset(
    {
        "wallet_signal_only",
        "wallet_plus_entity_confirmation",
        "wallet_plus_fomo_acceleration",
        "pure_entity_flow_fomo",
        "hazard_fomo",
        "clean_fomo",
    }
)


def _table_exists(store: Any, table: str) -> bool:
    with store._lock:
        row = store.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table,),
        ).fetchone()
    return row is not None


def _safe_json(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if not raw:
        return {}
    try:
        value = json.loads(str(raw))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _fomo_risk_signature(raw_state_json: Any) -> str:
    payload = _safe_json(raw_state_json)
    variants = {
        str(value)
        for value in (payload.get("experiment_variants") or ())
        if str(value)
    }
    hazards = sorted(value for value in variants if value not in _FOMO_NON_HAZARD_VARIANTS)
    return "clean" if not hazards else "+".join(hazards)


def _segment_key(
    *,
    surface: str,
    lane: str,
    venue: str,
    lifecycle: str,
    regime: str,
    risk_signature: str,
) -> str:
    return "|".join(
        (
            surface,
            lane,
            venue,
            lifecycle,
            regime,
            risk_signature,
        )
    )


def _family_key(*, surface: str, venue: str) -> str:
    # Solana active-alpha and FOMO overlays can express the same underlying venue
    # exposure. Keep them in the same correlation family until aligned forward
    # evidence proves the overlap is diversifying rather than duplicative.
    if surface in {"SOLANA_ALPHA", "FOMO"}:
        return f"SOLANA_UNDERLYING:{venue}"
    return "ROBINHOOD_CHAIN"


def _append(
    grouped: dict[str, list[float]],
    metadata: dict[str, dict[str, str]],
    *,
    surface: str,
    lane: str,
    venue: str,
    lifecycle: str,
    regime: str,
    risk_signature: str,
    net_return: float,
) -> None:
    key = _segment_key(
        surface=surface,
        lane=lane,
        venue=venue,
        lifecycle=lifecycle,
        regime=regime,
        risk_signature=risk_signature,
    )
    grouped.setdefault(key, []).append(float(net_return))
    metadata[key] = {
        "surface": surface,
        "lane": lane,
        "venue": venue,
        "lifecycle": lifecycle,
        "regime": regime,
        "risk_signature": risk_signature,
        "correlation_family": _family_key(surface=surface, venue=venue),
    }


def _segment_returns(
    store: Any,
    release_commit: str,
) -> tuple[dict[str, list[float]], dict[str, dict[str, str]]]:
    grouped: dict[str, list[float]] = {}
    metadata: dict[str, dict[str, str]] = {}

    if _table_exists(store, "risk_conditioned_alpha_v5_outcomes"):
        with store._lock:
            rows = store.db.execute(
                "SELECT lane,venue,lifecycle,regime,risk_signature,net_return "
                "FROM risk_conditioned_alpha_v5_outcomes WHERE release_commit=? ORDER BY id",
                (release_commit,),
            ).fetchall()
        for row in rows:
            _append(
                grouped,
                metadata,
                surface="SOLANA_ALPHA",
                lane=str(row["lane"] or "unknown"),
                venue=str(row["venue"] or "UNKNOWN"),
                lifecycle=str(row["lifecycle"] or "unknown"),
                regime=str(row["regime"] or "unknown"),
                risk_signature=str(row["risk_signature"] or "clean"),
                net_return=float(row["net_return"]),
            )

    if _table_exists(store, "fomo_paper_outcomes"):
        shadow_exists = _table_exists(store, "fomo_shadow_observations")
        with store._lock:
            if shadow_exists:
                rows = store.db.execute(
                    "SELECT o.venue,o.lifecycle,o.regime,o.net_return,s.state_json "
                    "FROM fomo_paper_outcomes o "
                    "LEFT JOIN fomo_shadow_observations s "
                    "ON s.release_commit=o.release_commit AND s.source_signature=o.source_signature "
                    "WHERE o.release_commit=? ORDER BY o.id",
                    (release_commit,),
                ).fetchall()
            else:
                rows = store.db.execute(
                    "SELECT venue,lifecycle,regime,net_return,NULL AS state_json "
                    "FROM fomo_paper_outcomes WHERE release_commit=? ORDER BY id",
                    (release_commit,),
                ).fetchall()
        for row in rows:
            _append(
                grouped,
                metadata,
                surface="FOMO",
                lane="fomo_continuation",
                venue=str(row["venue"] or "UNKNOWN"),
                lifecycle=str(row["lifecycle"] or "unknown"),
                regime=str(row["regime"] or "unknown"),
                risk_signature=_fomo_risk_signature(row["state_json"]),
                net_return=float(row["net_return"]),
            )

    if _table_exists(store, "robinhood_paper_outcomes") and _table_exists(store, "robinhood_v5_trial_context"):
        with store._lock:
            rows = store.db.execute(
                "SELECT c.lane,t.venue,t.lifecycle,c.regime,c.risk_signature,o.net_return "
                "FROM robinhood_paper_outcomes o "
                "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
                "JOIN robinhood_paper_trials t ON t.id=o.trial_id "
                "WHERE o.release_commit=? ORDER BY o.id",
                (release_commit,),
            ).fetchall()
        for row in rows:
            _append(
                grouped,
                metadata,
                surface="ROBINHOOD_CHAIN",
                lane=str(row["lane"] or "unknown"),
                venue=str(row["venue"] or "UNKNOWN"),
                lifecycle=str(row["lifecycle"] or "unknown"),
                regime=str(row["regime"] or "unknown"),
                risk_signature=str(row["risk_signature"] or "clean"),
                net_return=float(row["net_return"]),
            )

    return grouped, metadata


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
    """Allocate paper capital by exact lifecycle/regime/risk segments.

    A weak lifecycle or hazard cohort cannot dilute or subsidize a different
    lifecycle/regime/risk signature. Correlation remains fail-closed: Solana alpha
    and FOMO exposures on the same underlying venue share one family cap until
    aligned forward evidence can support a correlation estimate.
    """
    grouped, metadata = _segment_returns(store, release_commit)
    profiles: dict[str, dict[str, Any]] = {}
    segment_scores: dict[str, float] = {}
    family_scores: dict[str, float] = {}

    for segment, values in grouped.items():
        profile = robust_return_profile(
            values,
            grid=ALLOCATOR_EVIDENCE_POSITION_GRID,
            max_fraction=0.05,
        )
        profiles[segment] = asdict(profile)
        mature = profile.sample_count >= MIN_SEGMENT_SAMPLES
        promoted = profile.state == "promoted_positive_log_growth"
        score = _score(profile) if mature and promoted else 0.0
        segment_scores[segment] = score
        if score > 0.0:
            family = metadata[segment]["correlation_family"]
            family_scores[family] = family_scores.get(family, 0.0) + score

    family_caps = {
        family: min(MAX_FAMILY_WEIGHT, UNKNOWN_CORRELATION_FAMILY_CAP)
        for family in family_scores
    }
    family_weights = _capped_normalize(family_scores, family_caps)

    segment_weights: dict[str, float] = {}
    for family, family_weight in family_weights.items():
        members = {
            segment: score
            for segment, score in segment_scores.items()
            if score > 0.0 and metadata[segment]["correlation_family"] == family
        }
        denom = sum(members.values())
        if denom <= 0.0:
            continue
        for segment, score in members.items():
            segment_weights[segment] = family_weight * score / denom

    allocated = min(1.0, sum(segment_weights.values()))
    return {
        "allocator_version": ALLOCATOR_VERSION,
        "release_commit": release_commit,
        "authority": "paper_only_if_exact_forward_segment_is_mature_and_robust_positive",
        "segmentation": "surface_x_lane_x_venue_x_lifecycle_x_regime_x_full_risk_signature",
        "minimum_segment_samples": MIN_SEGMENT_SAMPLES,
        "correlation_policy": (
            "unknown_correlation_is_not_zero; Solana alpha and FOMO on the same "
            "underlying venue share one capped family until aligned forward evidence"
        ),
        "max_family_weight": MAX_FAMILY_WEIGHT,
        "unknown_correlation_family_cap": UNKNOWN_CORRELATION_FAMILY_CAP,
        "segment_metadata": metadata,
        "segment_profiles": profiles,
        "segment_scores": segment_scores,
        "family_scores": family_scores,
        "family_weights": family_weights,
        "paper_allocation_weights": segment_weights,
        "paper_cash_weight": max(0.0, 1.0 - allocated),
        "mature_promoted_segments": sum(1 for score in segment_scores.values() if score > 0.0),
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = ["ALLOCATOR_VERSION", "build_cross_regime_allocation"]
