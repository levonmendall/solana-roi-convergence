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
        headers={"Accept": "application/json", "User-Agent": "solana-roi-live-certification-diagnostic/5"},
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
    robinhood = _get("/v1/robinhood-chain/status")
    print("ROBINHOOD_STATUS=" + json.dumps(robinhood, sort_keys=True, default=str), flush=True)
    e2e = _get("/v1/strategy/e2e-status")
    certificate = _get("/v1/strategy/forward-certification")
    composition = _get("/v1/operations/production-composition")

    checks = certificate.get("checks") or {}
    transports = {
        surface: checks.get(f"{number}_{surface}_transport") or {}
        for number, surface in ((37, "solana"), (38, "fomo"), (39, "robinhood"))
    }
    lifecycle = composition.get("paper_execution_lifecycle") or {}
    direct_summary = {
        "enabled": direct.get("enabled"),
        "connected_provider_count": direct.get("connected_provider_count"),
        "continuity_ok": direct.get("continuity_ok"),
        "unresolved_gap": direct.get("unresolved_gap"),
        "target_stream_fanout": direct.get("target_stream_fanout"),
        "full_scope_target_quorum": direct.get("full_scope_target_quorum"),
        "live_poll_redundancy": direct.get("live_poll_redundancy"),
        "strategy_relevant_continuity": direct.get("strategy_relevant_continuity"),
        "continuity_epoch": direct.get("continuity_epoch"),
        "subscription_setup": direct.get("subscription_setup"),
        "paper_only": direct.get("paper_only"),
    }
    payload = {
        "health": health,
        "direct_solana": direct_summary,
        "robinhood_status": robinhood,
        "e2e_release_commit": e2e.get("release_commit"),
        "e2e_solana": e2e.get("solana"),
        "e2e_fomo": e2e.get("fomo"),
        "e2e_robinhood": e2e.get("robinhood"),
        "e2e_overall": e2e.get("overall"),
        "certificate_release_commit": certificate.get("release_commit"),
        "transports": transports,
        "composition_release_commit": composition.get("release_commit"),
        "paper_execution_lifecycle": lifecycle,
        "lifecycle_truth": composition.get("lifecycle_truth") or composition.get("paper_lifecycle_truth") or {},
        "unavailable_required_components": composition.get("unavailable_required_components"),
    }
    print("LIVE_CERTIFICATION_DIAGNOSTIC=" + json.dumps(payload, sort_keys=True, default=str), flush=True)

    assert direct.get("paper_only") is True
    assert robinhood.get("paper_only") is True
    assert robinhood.get("live_money_authority") is False
    assert (e2e.get("overall") or {}).get("paper_only") is True
    assert (e2e.get("overall") or {}).get("live_money_authority") is False
    assert (e2e.get("overall") or {}).get("signing_available") is False
    assert (e2e.get("overall") or {}).get("transaction_submission_available") is False


if __name__ == "__main__":
    main()
