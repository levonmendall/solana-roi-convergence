from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

BASE_URL = os.getenv("SOLANA_ROI_PRODUCTION_URL", "https://solana-roi-convergence.onrender.com").rstrip("/")
EXPECTED_SHA = os.getenv("EXPECTED_RELEASE_COMMIT", "").strip()
ATTEMPTS = int(os.getenv("FORWARD_PROOF_PROBE_ATTEMPTS", "30"))
SLEEP_SECONDS = float(os.getenv("FORWARD_PROOF_PROBE_SLEEP_SECONDS", "10"))
TIMEOUT_SECONDS = float(os.getenv("FORWARD_PROOF_HTTP_TIMEOUT_SECONDS", "10"))


def _get(path: str) -> dict:
    request = urllib.request.Request(
        f"{BASE_URL}{path}",
        headers={"Accept": "application/json", "User-Agent": "solana-roi-forward-proof-ci/1"},
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError(f"{path} returned non-object JSON")
    return payload


def _assert_safety(payload: dict, *, label: str) -> None:
    assert payload.get("paper_only") is True, f"{label}: paper_only must be true"
    assert payload.get("live_money_authority") is False, f"{label}: live money authority exposed"


def _probe_once() -> dict:
    health = _get("/health")
    _assert_safety(health, label="health")

    e2e = _get("/v1/strategy/e2e-status")
    overall = e2e.get("overall") or {}
    _assert_safety(overall, label="e2e overall")
    assert overall.get("signing_available") is False, "e2e: signing must remain unavailable"
    assert overall.get("transaction_submission_available") is False, "e2e: submission must remain unavailable"

    certificate = _get("/v1/strategy/forward-certification")
    _assert_safety(certificate, label="forward certificate")
    assert certificate.get("signing_available") is False, "certificate: signing must remain unavailable"
    assert certificate.get("transaction_submission_available") is False, "certificate: submission must remain unavailable"

    e2e_sha = str(e2e.get("release_commit") or "")
    cert_sha = str(certificate.get("release_commit") or "")
    if EXPECTED_SHA:
        assert e2e_sha == EXPECTED_SHA, f"e2e release {e2e_sha!r} != expected {EXPECTED_SHA!r}"
        assert cert_sha == EXPECTED_SHA, f"certificate release {cert_sha!r} != expected {EXPECTED_SHA!r}"
    assert e2e_sha and e2e_sha == cert_sha, "e2e/certificate release binding mismatch"

    checks = certificate.get("checks") or {}
    assert (checks.get("35_exact_live_release") or {}).get("pass") is True, "exact release gate failed"
    assert (checks.get("36_paper_only_safety_boundary") or {}).get("pass") is True, "safety gate failed"
    for number, surface in ((37, "solana"), (38, "fomo"), (39, "robinhood")):
        check = checks.get(f"{number}_{surface}_transport") or {}
        assert check.get("ready") is True, f"{surface} transport not ready: {check.get('blockers')}"
    assert (checks.get("45_correlation_and_one_capital_base") or {}).get("one_capital_base") is True, (
        "one-capital-base reconciliation invariant failed"
    )
    assert certificate.get("changes_strategy_authority") is False
    assert certificate.get("changes_economic_thresholds") is False

    return {
        "release_commit": cert_sha,
        "state": certificate.get("state"),
        "system_forward_certified": certificate.get("system_forward_certified"),
        "promotion_eligible_families": certificate.get(
            "promotion_eligible_families_under_existing_v51_claims"
        ),
        "blockers": certificate.get("blockers"),
    }


def main() -> None:
    last_error: BaseException | None = None
    for attempt in range(1, ATTEMPTS + 1):
        try:
            result = _probe_once()
            print(json.dumps({"attempt": attempt, "production_forward_probe": result}, sort_keys=True))
            return
        except (AssertionError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            print(f"attempt {attempt}/{ATTEMPTS} not ready: {type(exc).__name__}: {exc}")
            if attempt < ATTEMPTS:
                time.sleep(SLEEP_SECONDS)
    raise SystemExit(f"production forward proof did not converge: {type(last_error).__name__}: {last_error}")


if __name__ == "__main__":
    main()
