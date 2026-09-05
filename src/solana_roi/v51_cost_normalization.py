from __future__ import annotations

from collections import defaultdict
from functools import wraps
from typing import Any, Callable

from .v51_economic_core import robust_profile
from .v51_evidence_analytics import _audit_records, _surface_for_row, _safe, refresh_execution_cost_ledger


COST_NORMALIZATION_VERSION = "v51-certification-cost-normalization-v2"
_INSTALLED = False
_ORIGINAL_API_BUILD: Callable[[Any], dict[str, Any]] | None = None


def _band(value: float | None) -> str:
    if value is None or value < 0.0:
        return "unknown"
    if value <= 0.03:
        return "le_3pct"
    if value <= 0.07:
        return "3_7pct"
    if value <= 0.15:
        return "7_15pct"
    return "gt_15pct"


def normalize_certification_execution_costs(store: Any, certification: dict[str, Any]) -> dict[str, Any]:
    """Replace family cost sensitivity with normalized round-trip percentage cost.

    The frozen economic outcome itself is not recomputed here. This repairs the proof
    dimension that previously left FOMO in an `unknown` cost bucket even though the
    amount-specific unified trial already persisted round-trip cost.
    """
    refresh_execution_cost_ledger(store)
    with store._lock:
        rows = store.db.execute(
            "SELECT surface,source_signature,round_trip_cost_fraction FROM v51_execution_cost_ledger"
        ).fetchall()
    costs = {
        (str(row["surface"]), str(row["source_signature"])): float(row["round_trip_cost_fraction"])
        for row in rows
    }
    grouped: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    known = unknown = 0
    for row in _audit_records(store):
        family = str(row.get("family") or "UNKNOWN")
        surface = _surface_for_row(row)
        signature = str(row.get("source_signature") or row.get("trial_id") or row.get("id") or "")
        cost = costs.get((surface, signature))
        if cost is None:
            unknown += 1
        else:
            known += 1
        grouped[family][_band(cost)].append(_safe(row.get("net_return")))
    result = dict(certification)
    families = {key: dict(value) for key, value in dict(result.get("families") or {}).items()}
    for family, bands in grouped.items():
        if family not in families:
            continue
        families[family]["execution_cost_sensitivity"] = {
            band: robust_profile(values) for band, values in sorted(bands.items())
        }
        families[family]["execution_cost_unit"] = "round_trip_fraction_of_notional"
        families[family]["execution_cost_source"] = "v51_execution_cost_ledger"
    result["families"] = families
    result["execution_cost_normalization"] = {
        "version": COST_NORMALIZATION_VERSION,
        "known_cost_outcome_count": known,
        "unknown_cost_outcome_count": unknown,
        "fomo_cost_source": "profit_first_final_trials.round_trip_cost_fraction",
        "unit": "fraction_of_notional_round_trip",
    }
    return result


def install_api_cost_normalization() -> None:
    """Decorate the public audit-certification builder without changing economics."""
    global _INSTALLED, _ORIGINAL_API_BUILD
    from . import v51_strategy_api as api

    current = api.build_economic_certification
    if bool(getattr(current, "_roi_v51_cost_normalized", False)):
        _INSTALLED = True
        return
    _ORIGINAL_API_BUILD = current

    @wraps(current)
    def normalized(store: Any) -> dict[str, Any]:
        return normalize_certification_execution_costs(store, current(store))

    setattr(normalized, "_roi_v51_cost_normalized", True)
    api.build_economic_certification = normalized  # type: ignore[assignment]
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": COST_NORMALIZATION_VERSION,
        "installed": _INSTALLED,
        "audit_certification_uses_normalized_round_trip_cost": _INSTALLED,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "COST_NORMALIZATION_VERSION",
    "install_api_cost_normalization",
    "normalize_certification_execution_costs",
    "status",
]
