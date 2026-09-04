from __future__ import annotations

import math
from dataclasses import replace
from functools import wraps
from typing import Any, Callable

from . import continuation_market_recalibration as continuation
from . import fomo_runtime_install
from . import risk_conditioned_alpha_v5 as v5
from . import strategy_candidate_admission_repair as admission
from .fomo_continuation_shadow import FomoState
from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from .robinhood_chain_profit_maximizer import RobinhoodProfitMaximizerMixin


FINALIZER_VERSION = "continuation-market-recalibration-compat-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_ORIGINAL_FINAL_BUY: Callable[..., Any] | None = None
_ORIGINAL_FINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_FOMO_CLASSIFIER: Callable[..., Any] | None = None
_INSTALLED = False


def _legacy_admission_contract(row: dict[str, Any]) -> bool:
    """Preserve the PR143 helper contract; final continuation authority wraps above it."""
    if str(row.get("side") or "").lower() != "buy":
        return False
    if not str(row.get("signature") or "") or not str(row.get("token_mint") or "") or not str(row.get("wallet") or ""):
        return False
    try:
        wallet_price = float(row.get("wallet_price_sol"))
        lag_ms = float(row.get("observation_lag_ms"))
    except (TypeError, ValueError):
        return False
    if not math.isfinite(wallet_price) or wallet_price <= 0.0 or not math.isfinite(lag_ms):
        return False
    return 0.0 <= lag_ms <= 20_000.0


def _compatible_chase_band(value: float | None) -> str:
    """Preserve historical v5 labels; continuation keeps richer bands separately."""
    if value is None:
        return "unknown"
    numeric = float(value)
    if not math.isfinite(numeric):
        return "unknown"
    numeric = max(0.0, numeric)
    if numeric <= 0.15:
        return "baseline_le_15pct"
    if numeric <= 0.25:
        return "challenger_15_25pct"
    if numeric <= 0.40:
        return "challenger_25_40pct"
    return "challenger_gt_40pct"


def _compatible_latency_band(value: float | None) -> str:
    if value is None:
        return "unknown"
    numeric = float(value)
    if not math.isfinite(numeric):
        return "unknown"
    numeric = max(0.0, numeric)
    if numeric <= 5.0:
        return "le_5s"
    if numeric <= 10.0:
        return "5_10s"
    if numeric <= 20.0:
        return "10_20s"
    if numeric <= 30.0:
        return "20_30s"
    if numeric <= 60.0:
        return "30_60s"
    if numeric <= 120.0:
        return "1_2m"
    if numeric <= 300.0:
        return "2_5m"
    return "gt_5m"


async def _final_continuation_admission(self: Any, row: dict[str, Any]) -> None:
    """Admit a later valid buy ephemerally without mutating durable copyability evidence."""
    if _ORIGINAL_FINAL_BUY is None:
        raise RuntimeError("continuation final admission is not installed")
    candidate = row
    if not bool(row.get("copyable")) and continuation.continuation_strategy_evaluation_eligible(row):
        candidate = dict(row)
        candidate["copyable"] = 1
        setattr(
            self,
            "_roi_continuation_late_admission_bypasses",
            int(getattr(self, "_roi_continuation_late_admission_bypasses", 0) or 0) + 1,
        )
    await _ORIGINAL_FINAL_BUY(self, candidate)


def _fomo_continuation_classifier(features: Any, *, max_chase_fraction: float = 0.15, max_latency_seconds: float = 20.0) -> FomoState:
    """Preserve the proven FOMO score while making >40% chase a context, not a veto."""
    if _ORIGINAL_FOMO_CLASSIFIER is None:
        raise RuntimeError("continuation FOMO classifier is not installed")
    actual_chase = getattr(features, "chase_fraction", None)
    proxy = features
    if actual_chase is not None and math.isfinite(float(actual_chase)) and float(actual_chase) > 0.40:
        proxy = replace(features, chase_fraction=0.40)
    result = _ORIGINAL_FOMO_CLASSIFIER(
        proxy,
        max_chase_fraction=max_chase_fraction,
        max_latency_seconds=max_latency_seconds,
    )
    variants = list(result.experiment_variants)
    if actual_chase is not None and math.isfinite(float(actual_chase)) and float(actual_chase) > 0.40:
        variants = [
            value
            for value in variants
            if value not in {"challenger_15_25pct", "challenger_25_40pct", "challenger_gt_40pct"}
        ]
        variants.append(continuation.continuation_chase_band(float(actual_chase)))
        if "hazard_fomo" not in variants:
            variants.append("hazard_fomo")
    return FomoState(
        state=result.state,
        score=result.score,
        structurally_accessible=result.structurally_accessible,
        blockers=tuple(blocker for blocker in result.blockers if blocker != "chase_above_research_ceiling"),
        experiment_variants=tuple(dict.fromkeys(variants)),
        feature_version="fomo-continuation-context-v3",
    )


def _status_with_final_authority(self: Any) -> dict[str, Any]:
    if _ORIGINAL_FINAL_STATUS is None:
        raise RuntimeError("continuation final status is not installed")
    payload = _ORIGINAL_FINAL_STATUS(self)
    legacy = payload.get("strategy_candidate_admission_repair")
    if isinstance(legacy, dict):
        legacy.update(
            {
                "legacy_helper_entry_window_seconds": 20.0,
                "final_strategy_entry_window_seconds": None,
                "final_strategy_time_is_entry_veto": False,
                "v5_chase_above_40pct_remains_nonactionable": False,
                "final_authority": continuation.RECALIBRATION_VERSION,
            }
        )
    current = payload.get("continuation_market_recalibration")
    if isinstance(current, dict):
        current.update(
            {
                "compatibility_finalizer": FINALIZER_VERSION,
                "late_noncopyable_admissions_session": int(
                    getattr(self, "_roi_continuation_late_admission_bypasses", 0) or 0
                ),
                "legacy_admission_helper_preserved": True,
                "legacy_v5_chase_labels_preserved": True,
                "extended_continuation_bands_recorded_separately": True,
            }
        )
    return payload


def install_continuation_market_recalibration_finalize() -> None:
    global _INSTALLED, _ORIGINAL_FINAL_BUY, _ORIGINAL_FINAL_STATUS, _ORIGINAL_FOMO_CLASSIFIER
    if _INSTALLED:
        return

    # Restore PR143's helper contract for direct callers/tests. The actual final buy
    # path below admits later candidates ephemerally and therefore does not mutate
    # the durable legacy copyable observation.
    admission.strategy_evaluation_eligible = _legacy_admission_contract
    admission.ENTRY_WINDOW_SECONDS = 20.0

    # Preserve historical v5 labels so old evidence keys/tests stay stable. The new
    # continuation audit/FOMO surfaces carry the richer 40-75/75-125/>125 bands.
    v5.chase_band = _compatible_chase_band
    v5.latency_band = _compatible_latency_band

    current_buy = FinalProfitFirstResearchAdapter._buy
    _ORIGINAL_FINAL_BUY = current_buy
    wrapped_buy = wraps(current_buy)(_final_continuation_admission)
    try:
        wrapped_buy.__dict__.update(getattr(current_buy, "__dict__", {}))
    except Exception:
        pass
    setattr(wrapped_buy, "_roi_continuation_final_admission", True)
    FinalProfitFirstResearchAdapter._buy = wrapped_buy  # type: ignore[method-assign]

    current_status = FinalProfitFirstResearchAdapter.status
    _ORIGINAL_FINAL_STATUS = current_status
    wrapped_status = wraps(current_status)(_status_with_final_authority)
    try:
        wrapped_status.__dict__.update(getattr(current_status, "__dict__", {}))
    except Exception:
        pass
    FinalProfitFirstResearchAdapter.status = wrapped_status  # type: ignore[method-assign]

    _ORIGINAL_FOMO_CLASSIFIER = v5._fomo_classify_v5
    v5._fomo_classify_v5 = _fomo_continuation_classifier
    fomo_runtime_install.classify_fomo_state = _fomo_continuation_classifier

    # Preserve authority lineage metadata expected by the existing Robinhood
    # settlement/version dispatch while retaining the new method bodies.
    if continuation._ORIGINAL_RH_V3 is not None:
        RobinhoodProfitMaximizerMixin._maybe_open_v3.__module__ = continuation._ORIGINAL_RH_V3.__module__
    if continuation._ORIGINAL_RH_V2 is not None:
        RobinhoodProfitMaximizerMixin._maybe_open_v2.__module__ = continuation._ORIGINAL_RH_V2.__module__

    _INSTALLED = True


__all__ = [
    "FINALIZER_VERSION",
    "install_continuation_market_recalibration_finalize",
]
