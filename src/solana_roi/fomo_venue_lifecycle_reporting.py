from __future__ import annotations

import math
from collections import defaultdict
from statistics import mean, median
from typing import Any, Callable

from .fomo_continuation_shadow import FomoContinuationShadow, SIGNAL_DECAY_DELAYS_SECONDS


REPORT_VERSION = "fomo-venue-lifecycle-roi-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
ACTIVE_STRATEGY_MUTATION_ALLOWED = False
HISTORICAL_PROMOTION_AUTHORITY = False

_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _trimmed(values: list[float], n: int) -> float | None:
    if len(values) <= n:
        return None
    remaining = sorted(values, reverse=True)[n:]
    return mean(remaining) if remaining else None


def _delay_bucket(delay: float) -> int:
    for value in SIGNAL_DECAY_DELAYS_SECONDS:
        if delay <= value:
            return value
    return SIGNAL_DECAY_DELAYS_SECONDS[-1]


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    values = [value for row in rows if (value := _safe_float(row.get("net_return"))) is not None]
    by_state: dict[str, list[float]] = defaultdict(list)
    latency: dict[int, list[float]] = {value: [] for value in SIGNAL_DECAY_DELAYS_SECONDS}
    for row in rows:
        value = _safe_float(row.get("net_return"))
        delay = _safe_float(row.get("signal_to_entry_seconds"))
        if value is None:
            continue
        by_state[str(row.get("fomo_state") or "unknown")].append(value)
        if delay is not None:
            latency[_delay_bucket(delay)].append(value)
    return {
        "sample_count": len(values),
        "mean_residual_roi_pct": mean(values) * 100.0 if values else None,
        "median_residual_roi_pct": median(values) * 100.0 if values else None,
        "trimmed_mean_residual_roi_ex_best_1_pct": (
            _trimmed(values, 1) * 100.0 if _trimmed(values, 1) is not None else None
        ),
        "trimmed_mean_residual_roi_ex_best_3_pct": (
            _trimmed(values, 3) * 100.0 if _trimmed(values, 3) is not None else None
        ),
        "trimmed_mean_residual_roi_ex_best_5_pct": (
            _trimmed(values, 5) * 100.0 if _trimmed(values, 5) is not None else None
        ),
        "positive_rate_pct": (
            sum(value > 0.0 for value in values) / len(values) * 100.0 if values else None
        ),
        "by_fomo_state": {
            state: {
                "sample_count": len(state_values),
                "mean_residual_roi_pct": mean(state_values) * 100.0,
                "median_residual_roi_pct": median(state_values) * 100.0,
            }
            for state, state_values in sorted(by_state.items())
        },
        "signal_decay": {
            str(delay): {
                "sample_count": len(bucket),
                "mean_residual_roi_pct": mean(bucket) * 100.0 if bucket else None,
                "median_residual_roi_pct": median(bucket) * 100.0 if bucket else None,
            }
            for delay, bucket in latency.items()
        },
    }


def _venue_lifecycle_report(self: FomoContinuationShadow) -> dict[str, Any]:
    with self.store._lock:
        rows = [
            dict(row)
            for row in self.store.db.execute(
                "SELECT venue,lifecycle,regime,fomo_state,signal_to_entry_seconds,net_return "
                "FROM fomo_shadow_outcomes WHERE release_commit=? ORDER BY id",
                (self.release_commit,),
            ).fetchall()
        ]

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row.get("venue") or "UNKNOWN"), str(row.get("lifecycle") or "unknown"))].append(row)

    segments: list[dict[str, Any]] = []
    for (venue, lifecycle), segment_rows in sorted(grouped.items()):
        segments.append(
            {
                "venue": venue,
                "lifecycle": lifecycle,
                **_metrics(segment_rows),
                "strategy_authority": False,
                "historical_promotion_authority": False,
            }
        )

    return {
        "report_version": REPORT_VERSION,
        "assignment_key": "fomo_state_x_venue_x_lifecycle",
        "segments": segments,
        "segment_count": len(segments),
        "cross_venue_pooling_for_roi": False,
        "fomo_is_market_state_overlay_not_venue": True,
        "paper_only": True,
        "live_money_authority": False,
        "active_strategy_mutation_allowed": False,
        "historical_promotion_authority": False,
    }


def _status_with_venue_lifecycle_reporting(self: FomoContinuationShadow) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("FOMO venue/lifecycle reporting is not installed")
    payload = _ORIGINAL_STATUS(self)
    try:
        payload["roi_by_venue_lifecycle"] = _venue_lifecycle_report(self)
    except Exception as exc:
        payload["roi_by_venue_lifecycle"] = {
            "report_version": REPORT_VERSION,
            "failed_closed": True,
            "error": f"{type(exc).__name__}: venue/lifecycle FOMO ROI unavailable",
            "cross_venue_pooling_for_roi": False,
            "strategy_authority": False,
            "paper_only": True,
            "live_money_authority": False,
        }
    return payload


def install_fomo_venue_lifecycle_reporting() -> None:
    global _ORIGINAL_STATUS
    if _ORIGINAL_STATUS is not None:
        return
    current = FomoContinuationShadow.status
    _ORIGINAL_STATUS = current
    try:
        _status_with_venue_lifecycle_reporting.__dict__.update(getattr(current, "__dict__", {}))
    except Exception:
        pass
    setattr(_status_with_venue_lifecycle_reporting, "_roi_fomo_venue_lifecycle_reporting", True)
    FomoContinuationShadow.status = _status_with_venue_lifecycle_reporting  # type: ignore[method-assign]


__all__ = [
    "REPORT_VERSION",
    "install_fomo_venue_lifecycle_reporting",
]
