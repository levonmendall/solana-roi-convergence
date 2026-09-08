from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

BASE_URL = os.getenv("SOLANA_ROI_PRODUCTION_URL", "https://solana-roi-convergence.onrender.com").rstrip("/")
EXPECTED_SHA = os.getenv("EXPECTED_RELEASE_COMMIT", "").strip()
TIMEOUT_SECONDS = float(os.getenv("FORWARD_PROOF_HTTP_TIMEOUT_SECONDS", "60"))


def _get(path: str) -> dict:
    started = time.monotonic()
    request = urllib.request.Request(
        f"{BASE_URL}{path}",
        headers={"Accept": "application/json", "User-Agent": "solana-roi-live-e2e-diagnostic/1"},
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError(f"{path} returned non-object JSON")
    print(f"GET {path} elapsed={time.monotonic() - started:.2f}s", flush=True)
    return payload


def _safe_get(path: str) -> dict:
    try:
        return _get(path)
    except Exception as exc:
        return {"_diagnostic_error": f"{type(exc).__name__}: {exc}"}


def _sample(number: int) -> None:
    health = _safe_get("/health")
    direct = _safe_get("/v1/direct-solana/status")
    e2e_cache = _safe_get("/v1/strategy/e2e-status/cache")
    e2e = _safe_get("/v1/strategy/e2e-status")
    certificate = _safe_get("/v1/strategy/forward-certification")
    production_cache = _safe_get("/v1/strategy/production-proof/cache")
    production = _safe_get("/v1/strategy/production-proof")
    robinhood = _safe_get("/v1/robinhood-chain/status")
    composition = _safe_get("/v1/operations/production-composition")
    checks = certificate.get("checks") or {}
    accounting = production.get("candidate_accounting") or {}
    lanes = accounting.get("lanes") or accounting.get("lane_accounting") or {}
    lane_summary = {}
    if isinstance(lanes, dict):
        for lane, value in lanes.items():
            if isinstance(value, dict):
                lane_summary[lane] = {
                    "verified": value.get("verified"),
                    "status": value.get("status"),
                    "observed": value.get("observed_candidate_count"),
                    "terminal": value.get("terminal_candidate_count"),
                    "pending": value.get("valid_pending_candidate_count"),
                    "coverage_debt": value.get("coverage_debt_candidate_count"),
                    "unexplained": value.get("unexplained_candidate_count"),
                    "reconciled": value.get("reconciled"),
                    "proof_state": value.get("proof_state"),
                }
    payload = {
        "sample": number,
        "expected_release_commit": EXPECTED_SHA,
        "health": {
            "error": health.get("_diagnostic_error"),
            "release_commit": health.get("release_commit"),
            "paper_only": health.get("paper_only"),
            "live_money_authority": health.get("live_money_authority"),
        },
        "direct_solana": {
            "error": direct.get("_diagnostic_error"),
            "enabled": direct.get("enabled"),
            "paper_only": direct.get("paper_only"),
            "connected_provider_count": direct.get("connected_provider_count"),
            "continuity_ok": direct.get("continuity_ok"),
            "unresolved_gap": direct.get("unresolved_gap"),
            "strategy_relevant_continuity": direct.get("strategy_relevant_continuity"),
            "target_stream_fanout": direct.get("target_stream_fanout"),
            "full_scope_target_quorum": direct.get("full_scope_target_quorum"),
            "live_poll_redundancy": direct.get("live_poll_redundancy"),
            "continuity_epoch": direct.get("continuity_epoch"),
            "subscription_setup": direct.get("subscription_setup"),
        },
        "e2e_cache": e2e_cache,
        "e2e": {
            "error": e2e.get("_diagnostic_error"),
            "release_commit": e2e.get("release_commit"),
            "solana": e2e.get("solana"),
            "fomo": e2e.get("fomo"),
            "robinhood": e2e.get("robinhood"),
            "overall": e2e.get("overall"),
        },
        "certificate": {
            "error": certificate.get("_diagnostic_error"),
            "release_commit": certificate.get("release_commit"),
            "state": certificate.get("state"),
            "system_forward_certified": certificate.get("system_forward_certified"),
            "blockers": certificate.get("blockers"),
            "checks": {key: checks.get(key) for key in (
                "35_exact_live_release", "36_paper_only_safety_boundary",
                "37_solana_transport", "38_fomo_transport", "39_robinhood_transport"
            )},
        },
        "production_cache": production_cache,
        "production": {
            "error": production.get("_diagnostic_error"),
            "release": production.get("release"),
            "state": production.get("state"),
            "production_proof_pass": production.get("production_proof_pass"),
            "blockers": production.get("blockers"),
            "resource_pressure": production.get("resource_pressure"),
            "candidate_lanes": lane_summary,
            "candidate_conservation": accounting.get("candidate_conservation"),
            "final_certification": production.get("final_certification"),
        },
        "robinhood": {
            "error": robinhood.get("_diagnostic_error"),
            "paper_only": robinhood.get("paper_only"),
            "live_money_authority": robinhood.get("live_money_authority"),
            "status": robinhood.get("status"),
            "blockers": robinhood.get("blockers"),
            "all_regimes_e2e_achievable": robinhood.get("all_regimes_e2e_achievable"),
            "worker": robinhood.get("worker"),
            "provider": robinhood.get("provider"),
            "transport": robinhood.get("transport"),
            "catchup": robinhood.get("catchup"),
        },
        "composition": {
            "error": composition.get("_diagnostic_error"),
            "release_commit": composition.get("release_commit"),
            "paper_execution_lifecycle": composition.get("paper_execution_lifecycle"),
            "lifecycle_truth": composition.get("lifecycle_truth") or composition.get("paper_lifecycle_truth"),
            "unavailable_required_components": composition.get("unavailable_required_components"),
        },
    }
    print("LIVE_E2E_DIAGNOSTIC=" + json.dumps(payload, sort_keys=True, default=str), flush=True)


def main() -> None:
    for number in range(1, 4):
        _sample(number)
        if number < 3:
            time.sleep(16)


if __name__ == "__main__":
    main()
