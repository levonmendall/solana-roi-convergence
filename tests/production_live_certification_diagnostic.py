from __future__ import annotations

import json
import os
import time
import urllib.request

BASE_URL = os.getenv("SOLANA_ROI_PRODUCTION_URL", "https://solana-roi-convergence.onrender.com").rstrip("/")
TIMEOUT_SECONDS = float(os.getenv("CERTIFICATION_DIAGNOSTIC_HTTP_TIMEOUT_SECONDS", "60"))


def _get(path: str) -> dict:
    started = time.monotonic()
    request = urllib.request.Request(
        f"{BASE_URL}{path}",
        headers={"Accept": "application/json", "User-Agent": "solana-roi-live-certification-diagnostic/2"},
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError(f"{path} returned non-object JSON")
    print(f"GET {path} elapsed={time.monotonic() - started:.2f}s", flush=True)
    return payload


def main() -> None:
    health = _get("/health")
    direct = _get("/v1/direct-solana/status")
    e2e = _get("/v1/strategy/e2e-status")
    certificate = _get("/v1/strategy/forward-certification")
    production = _get("/v1/strategy/production-proof")
    composition = _get("/v1/operations/production-composition")

    checks = certificate.get("checks") or {}
    transports = {
        surface: checks.get(f"{number}_{surface}_transport") or {}
        for number, surface in ((37, "solana"), (38, "fomo"), (39, "robinhood"))
    }
    accounting = production.get("candidate_accounting") or {}
    lanes = accounting.get("lanes") or accounting.get("lane_accounting") or {}
    lane_summary = {
        lane: {
            "verified": value.get("verified"),
            "status": value.get("status"),
            "observed_candidate_count": value.get("observed_candidate_count"),
            "terminal_candidate_count": value.get("terminal_candidate_count"),
            "valid_pending_candidate_count": value.get("valid_pending_candidate_count"),
            "coverage_debt_candidate_count": value.get("coverage_debt_candidate_count"),
            "unexplained_candidate_count": value.get("unexplained_candidate_count"),
            "reconciled": value.get("reconciled"),
            "proof_state": value.get("proof_state"),
        }
        for lane, value in lanes.items()
        if isinstance(value, dict)
    }
    lifecycle = composition.get("paper_execution_lifecycle") or {}
    lifecycle_truth = composition.get("lifecycle_truth") or composition.get("paper_lifecycle_truth") or {}
    components = composition.get("components") or {}
    component_summary = {
        name: value
        for name, value in components.items()
        if name in {"ingestion", "candidate", "strategy", "execution", "settlement"}
    }
    direct_summary = {
        "enabled": direct.get("enabled"),
        "connected_provider_count": direct.get("connected_provider_count"),
        "provider_states": direct.get("provider_states"),
        "continuity_ok": direct.get("continuity_ok"),
        "unresolved_gap": direct.get("unresolved_gap"),
        "outage_started_at": direct.get("outage_started_at"),
        "last_backfill_complete_at": direct.get("last_backfill_complete_at"),
        "last_backfill_error": direct.get("last_backfill_error"),
        "hydration_queue": direct.get("hydration_queue"),
        "source_receipts_last_hour": direct.get("source_receipts_last_hour"),
        "hydration": direct.get("hydration"),
        "paper_only": direct.get("paper_only"),
    }

    payload = {
        "health": {
            "release_commit": health.get("release_commit"),
            "paper_only": health.get("paper_only"),
            "live_money_authority": health.get("live_money_authority"),
        },
        "direct_solana": direct_summary,
        "e2e_release_commit": e2e.get("release_commit"),
        "certificate_release_commit": certificate.get("release_commit"),
        "production_release_commit": (production.get("release") or {}).get("release_commit"),
        "composition_release_commit": composition.get("release_commit"),
        "transports": transports,
        "candidate_lanes": lane_summary,
        "production_blockers": production.get("blockers"),
        "production_state": production.get("state"),
        "production_proof_pass": production.get("production_proof_pass"),
        "lifecycle_truth": lifecycle_truth,
        "paper_execution_lifecycle": lifecycle,
        "selected_components": component_summary,
        "unavailable_required_components": composition.get("unavailable_required_components"),
    }
    print("LIVE_CERTIFICATION_DIAGNOSTIC=" + json.dumps(payload, sort_keys=True, default=str), flush=True)

    # This workflow is intentionally read-only and diagnostic. It must never mutate
    # paper state or loosen an economic/safety gate merely to manufacture proof.
    assert direct.get("paper_only") is True
    assert (e2e.get("overall") or {}).get("paper_only") is True
    assert (e2e.get("overall") or {}).get("live_money_authority") is False
    assert (e2e.get("overall") or {}).get("signing_available") is False
    assert (e2e.get("overall") or {}).get("transaction_submission_available") is False


if __name__ == "__main__":
    main()
