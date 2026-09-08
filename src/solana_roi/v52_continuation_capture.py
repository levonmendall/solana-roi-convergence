from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any


CHALLENGER_VERSION = "roi-convergence-v5.2-continuation-capture-1"
CHALLENGER_EPOCH = "v52-weekend-review-20260908"
INCUMBENT_VERSION = "roi-convergence-v5.1-context-exactness-1"

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
CHALLENGER_ENTRY_AUTHORITY = False
HISTORICAL_PROMOTION_AUTHORITY = False
INCUMBENT_AUTHORITY_CHANGED = False

IMMEDIATE_COPY_MAX_SECONDS = 20.0
HIGH_CHASE_OBSERVE_ONLY_THRESHOLD = 0.40

SUPPORTED_SURFACES = frozenset(
    {
        "PUMP_FUN",
        "PUMP_AMM",
        "PUMPSWAP",
        "RAYDIUM",
        "FOMO",
        "ROBINHOOD_CHAIN",
    }
)


@dataclass(frozen=True)
class ContinuationObservation:
    """One prospective v5.2 continuation observation.

    This is a research record only.  It deliberately cannot authorize a paper
    entry, replace the v5.1 incumbent, or promote historical/weekend evidence.
    """

    source_signature: str
    asset_id: str
    surface: str
    lifecycle: str
    latency_seconds: float
    chase_fraction: float
    exact_entry_quote_available: bool
    exact_exit_quote_available: bool
    structurally_exitable: bool
    residual_return_fraction: float | None = None


@dataclass(frozen=True)
class ContinuationCapture:
    challenger_version: str
    challenger_epoch: str
    incumbent_version: str
    classification: str
    reasons: tuple[str, ...]
    source_signature: str
    asset_id: str
    surface: str
    lifecycle: str
    latency_seconds: float
    latency_band: str
    chase_fraction: float
    chase_band: str
    exact_entry_quote_available: bool
    exact_exit_quote_available: bool
    structurally_exitable: bool
    executable_snapshot: bool
    residual_return_fraction: float | None
    research_only: bool
    entry_authority: bool
    direct_promotion_authority: bool
    incumbent_authority_changed: bool
    paper_only: bool
    live_money_authority: bool
    signing_available: bool
    transaction_submission_available: bool


def _finite_nonnegative(value: Any, *, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field}_invalid") from exc
    if not math.isfinite(number) or number < 0.0:
        raise ValueError(f"{field}_invalid")
    return number


def _finite_optional(value: Any, *, field: str) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field}_invalid") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field}_invalid")
    return number


def latency_band(seconds: float) -> str:
    value = _finite_nonnegative(seconds, field="latency_seconds")
    if value <= 5.0:
        return "le_5s"
    if value <= 10.0:
        return "5_10s"
    if value <= IMMEDIATE_COPY_MAX_SECONDS:
        return "10_20s"
    if value <= 60.0:
        return "20_60s_research_only"
    if value <= 120.0:
        return "1_2m_research_only"
    if value <= 300.0:
        return "2_5m_research_only"
    return "gt_5m_research_only"


def chase_band(fraction: float) -> str:
    value = _finite_nonnegative(fraction, field="chase_fraction")
    if value <= 0.15:
        return "le_15pct"
    if value <= 0.25:
        return "15_25pct_challenger"
    if value <= HIGH_CHASE_OBSERVE_ONLY_THRESHOLD:
        return "25_40pct_challenger"
    return "gt_40pct_observe_only"


def safety_manifest() -> dict[str, Any]:
    """Machine-readable proof of the locked v5.2/v5.1 authority boundary."""

    return {
        "challenger_version": CHALLENGER_VERSION,
        "challenger_epoch": CHALLENGER_EPOCH,
        "incumbent_version": INCUMBENT_VERSION,
        "incumbent_remains_authoritative": True,
        "incumbent_authority_changed": INCUMBENT_AUTHORITY_CHANGED,
        "challenger_entry_authority": CHALLENGER_ENTRY_AUTHORITY,
        "historical_promotion_authority": HISTORICAL_PROMOTION_AUTHORITY,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        "immediate_copy_max_seconds": IMMEDIATE_COPY_MAX_SECONDS,
        "high_chase_observe_only_threshold": HIGH_CHASE_OBSERVE_ONLY_THRESHOLD,
        "exact_entry_quote_required_for_executable_snapshot": True,
        "exact_exit_quote_required_for_executable_snapshot": True,
        "structural_exitability_required_for_executable_snapshot": True,
        "direct_promotion_from_weekend_review": False,
        "production_composition_hook": False,
        "supported_surfaces": sorted(SUPPORTED_SURFACES),
    }


def capture_continuation(observation: ContinuationObservation) -> dict[str, Any]:
    """Classify and preserve a v5.2 research observation without granting authority.

    The function intentionally distinguishes whether an observation had enough
    exact execution evidence to be *measurable* from whether it can authorize an
    incumbent trade.  v5.2 has no entry authority in this boundary, even when the
    observation is inside the v5.1 immediate-copy timing/chase envelope.
    """

    signature = str(observation.source_signature or "").strip()
    asset_id = str(observation.asset_id or "").strip()
    surface = str(observation.surface or "").strip().upper()
    lifecycle = str(observation.lifecycle or "").strip()
    if not signature:
        raise ValueError("source_signature_missing")
    if not asset_id:
        raise ValueError("asset_id_missing")
    if surface not in SUPPORTED_SURFACES:
        raise ValueError("surface_unsupported")
    if not lifecycle:
        raise ValueError("lifecycle_missing")

    latency = _finite_nonnegative(observation.latency_seconds, field="latency_seconds")
    chase = _finite_nonnegative(observation.chase_fraction, field="chase_fraction")
    residual = _finite_optional(
        observation.residual_return_fraction,
        field="residual_return_fraction",
    )

    reasons: list[str] = []
    if latency > IMMEDIATE_COPY_MAX_SECONDS:
        reasons.append("post_20s_immediate_copy_window_research_only")
    if chase > HIGH_CHASE_OBSERVE_ONLY_THRESHOLD:
        reasons.append("gt_40pct_chase_observe_only")
    if not observation.exact_entry_quote_available:
        reasons.append("exact_entry_quote_missing")
    if not observation.exact_exit_quote_available:
        reasons.append("exact_exit_quote_missing")
    if not observation.structurally_exitable:
        reasons.append("structurally_unexitable")

    executable_snapshot = bool(
        observation.exact_entry_quote_available
        and observation.exact_exit_quote_available
        and observation.structurally_exitable
    )

    if not observation.structurally_exitable:
        classification = "structurally_unexitable_research_only"
    elif not observation.exact_entry_quote_available or not observation.exact_exit_quote_available:
        classification = "non_executable_quote_research_only"
    elif latency > IMMEDIATE_COPY_MAX_SECONDS or chase > HIGH_CHASE_OBSERVE_ONLY_THRESHOLD:
        classification = "challenger_observe_only"
    else:
        classification = "continuation_candidate_observed"

    capture = ContinuationCapture(
        challenger_version=CHALLENGER_VERSION,
        challenger_epoch=CHALLENGER_EPOCH,
        incumbent_version=INCUMBENT_VERSION,
        classification=classification,
        reasons=tuple(reasons),
        source_signature=signature,
        asset_id=asset_id,
        surface=surface,
        lifecycle=lifecycle,
        latency_seconds=latency,
        latency_band=latency_band(latency),
        chase_fraction=chase,
        chase_band=chase_band(chase),
        exact_entry_quote_available=bool(observation.exact_entry_quote_available),
        exact_exit_quote_available=bool(observation.exact_exit_quote_available),
        structurally_exitable=bool(observation.structurally_exitable),
        executable_snapshot=executable_snapshot,
        residual_return_fraction=residual,
        research_only=True,
        entry_authority=CHALLENGER_ENTRY_AUTHORITY,
        direct_promotion_authority=False,
        incumbent_authority_changed=INCUMBENT_AUTHORITY_CHANGED,
        paper_only=PAPER_ONLY,
        live_money_authority=LIVE_MONEY_AUTHORITY,
        signing_available=SIGNING_AVAILABLE,
        transaction_submission_available=TRANSACTION_SUBMISSION_AVAILABLE,
    )
    return asdict(capture)


__all__ = [
    "CHALLENGER_VERSION",
    "CHALLENGER_EPOCH",
    "INCUMBENT_VERSION",
    "IMMEDIATE_COPY_MAX_SECONDS",
    "HIGH_CHASE_OBSERVE_ONLY_THRESHOLD",
    "ContinuationObservation",
    "capture_continuation",
    "chase_band",
    "latency_band",
    "safety_manifest",
]
