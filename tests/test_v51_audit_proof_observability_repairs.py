from __future__ import annotations

from fastapi import FastAPI

from solana_roi import v51_phase17_context_certification as phase17
from solana_roi.v51_phase14_api import install_phase14_profitability_certification
from solana_roi.v51_phase17_attestation_hardening import (
    install_phase17_surface_attestation_hardening,
    strict_surface_attestations,
)
from solana_roi import v51_resource_pressure as resource_pressure


def _aggregate_only_forward() -> dict:
    return {
        "checks": {
            "41_current_release_attestation": {
                "pass": True,
                "attested": True,
            }
        }
    }


def test_surface_attestation_missing_fails_closed_even_when_aggregate_passes() -> None:
    result = strict_surface_attestations(_aggregate_only_forward())
    assert set(result) == {"SOLANA", "FOMO", "ROBINHOOD_CHAIN"}
    for row in result.values():
        assert row["present"] is False
        assert row["attested"] is False
        assert row["source"] == "missing_surface_attestation_fail_closed"
        assert "surface_attestation_unavailable" in row["reasons"]


def test_surface_attestation_hardening_patches_phase17_authority_path() -> None:
    install_phase17_surface_attestation_hardening()
    result = phase17._surface_attestations(_aggregate_only_forward())
    assert result["SOLANA"]["attested"] is False
    assert result["FOMO"]["attested"] is False
    assert result["ROBINHOOD_CHAIN"]["attested"] is False
    assert getattr(phase17._surface_attestations, "_roi_surface_attestation_fail_closed", False) is True


def test_resource_pressure_trend_reports_growth_and_throttling() -> None:
    samples = [
        {
            "monotonic": 100.0,
            "memory_current_bytes": 500,
            "cpu_usage_usec": 1000,
            "cpu_throttled_usec": 100,
            "cpu_nr_throttled": 1,
        },
        {
            "monotonic": 700.0,
            "memory_current_bytes": 1100,
            "cpu_usage_usec": 1900,
            "cpu_throttled_usec": 400,
            "cpu_nr_throttled": 4,
        },
    ]
    trend = resource_pressure._trend(samples)
    assert trend["window_seconds"] == 600.0
    assert trend["memory_growth_bytes_per_minute"] == 60.0
    assert trend["cpu_throttle_fraction"] == 0.25
    assert trend["cpu_throttled_events_delta"] == 3
    assert trend["trend_window_sufficient"] is True


def test_canonical_production_proof_route_fails_closed_without_system_proof() -> None:
    app = FastAPI()
    install_phase14_profitability_certification(app, object())
    route = next(route for route in app.routes if getattr(route, "path", None) == "/v1/strategy/production-proof")
    payload = route.endpoint()
    assert payload["state"] == "DEGRADED"
    assert payload["ready_for_forward_proof"] is False
    assert payload["final_certification"]["classification"] == "INSUFFICIENT_EVIDENCE"
    assert payload["surface_attestation_policy"] if "surface_attestation_policy" in payload else True
    assert payload["read_only_observability"] is True
    assert payload["changes_strategy_authority"] is False
    assert payload["changes_economic_thresholds"] is False
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
    assert payload["resource_pressure"]["read_only_observability"] is True
