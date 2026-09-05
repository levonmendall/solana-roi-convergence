from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Iterable

from .v51_economic_certification import _records
from .v51_economic_core import robust_profile

DIAGNOSTIC_VERSION = "v51-execution-mechanism-stress-v1"

# Diagnostic-only mechanism shocks. They do not modify strategy selection, sizing,
# promotion, exit, or the frozen authority fingerprint.
MECHANISM_SCENARIOS: dict[str, tuple[dict[str, float | str], ...]] = {
    "priority_fee": (
        {"name": "mild", "fee_drag_fraction": 0.002},
        {"name": "material", "fee_drag_fraction": 0.005},
        {"name": "severe", "fee_drag_fraction": 0.010},
    ),
    "block_placement": (
        {"name": "mild", "extra_delay_seconds": 1.0},
        {"name": "material", "extra_delay_seconds": 3.0},
        {"name": "severe", "extra_delay_seconds": 7.0},
    ),
    "mev_adverse_selection": (
        {"name": "mild", "adverse_selection_fraction": 0.005},
        {"name": "material", "adverse_selection_fraction": 0.020},
        {"name": "severe", "adverse_selection_fraction": 0.050},
    ),
    "quote_deterioration": (
        {"name": "mild", "quote_deterioration_fraction": 0.010},
        {"name": "material", "quote_deterioration_fraction": 0.030},
        {"name": "severe", "quote_deterioration_fraction": 0.070},
    ),
    "transaction_failure": (
        {"name": "mild", "failure_probability": 0.02, "failed_attempt_drag_fraction": 0.01},
        {"name": "material", "failure_probability": 0.07, "failed_attempt_drag_fraction": 0.03},
        {"name": "severe", "failure_probability": 0.15, "failed_attempt_drag_fraction": 0.05},
    ),
}


def _finite(values: Iterable[float]) -> list[float]:
    result: list[float] = []
    for value in values:
        numeric = float(value)
        if math.isfinite(numeric):
            result.append(numeric)
    return result


def _apply(values: Iterable[float], mechanism: str, scenario: dict[str, float | str]) -> list[float]:
    series = _finite(values)
    if mechanism == "block_placement":
        delay = float(scenario["extra_delay_seconds"])
        decay = math.exp(-delay / 20.0)
        return [value * decay if value > 0.0 else value for value in series]
    if mechanism == "transaction_failure":
        probability = float(scenario["failure_probability"])
        failed_drag = float(scenario["failed_attempt_drag_fraction"])
        return [(1.0 - probability) * value + probability * (-failed_drag) for value in series]
    if mechanism == "priority_fee":
        drag = float(scenario["fee_drag_fraction"])
    elif mechanism == "mev_adverse_selection":
        drag = float(scenario["adverse_selection_fraction"])
    elif mechanism == "quote_deterioration":
        drag = float(scenario["quote_deterioration_fraction"])
    else:
        raise ValueError(f"unknown execution stress mechanism: {mechanism}")
    return [(1.0 + value) * (1.0 - drag) - 1.0 for value in series]


def mechanism_stress_profiles(values: Iterable[float]) -> dict[str, Any]:
    series = _finite(values)
    return {
        mechanism: {
            str(scenario["name"]): {
                "scenario": dict(scenario),
                "profile": robust_profile(_apply(series, mechanism, scenario)),
            }
            for scenario in scenarios
        }
        for mechanism, scenarios in MECHANISM_SCENARIOS.items()
    }


def build_execution_mechanism_stress(store: Any) -> dict[str, Any]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in _records(store):
        grouped[str(row.get("family") or "unknown")].append(float(row.get("net_return") or 0.0))
    return {
        "diagnostic_version": DIAGNOSTIC_VERSION,
        "diagnostic_only": True,
        "changes_frozen_strategy_economics": False,
        "paper_only": True,
        "live_money_authority": False,
        "mechanisms": list(MECHANISM_SCENARIOS),
        "families": {
            family: {
                "closed_outcome_count": len(values),
                "mechanism_stress": mechanism_stress_profiles(values),
            }
            for family, values in sorted(grouped.items())
        },
        "interpretation": (
            "These scenarios isolate paper-to-live execution mechanisms; they are sensitivity diagnostics, "
            "not calibrated claims about future live fills and do not grant live execution authority."
        ),
    }


__all__ = [
    "DIAGNOSTIC_VERSION",
    "MECHANISM_SCENARIOS",
    "build_execution_mechanism_stress",
    "mechanism_stress_profiles",
]
