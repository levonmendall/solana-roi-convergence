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
# These endpoints intentionally aggregate durable evidence across independent paper
# planes. Production observation showed valid 200 responses can exceed 10 seconds;
# this verifier is post-deploy and must distinguish a slow deep proof from an outage.
TIMEOUT_SECONDS = float(os.getenv("FORWARD_PROOF_HTTP_TIMEOUT_SECONDS", "60"))
FIVE_LANES = ("pump_fun", "pump_amm", "raydium", "fomo", "robinhood")


def _get(path: str) -> dict:
    request = urllib.request.Request(
        f"{BASE_URL}{path}",
        headers={"Accept": "application/json", "User-Agent": "solana-roi-forward-proof-ci/4"},
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError(f"{path} returned non-object JSON")
    return payload


def _assert_safety(payload: dict, *, label: str) -> None:
    assert payload.get("paper_only") is True, f"{label}: paper_only must be true"
    assert payload.get("live_money_authority") is False, f"{label}: live money authority exposed"


def _five_lane_result(production: dict) -> dict:
    accounting = production.get("candidate_accounting") or {}
    # Canonical Batch 7 schema is `lanes`; retain the Batch 6 alias only as a
    # read-compatibility fallback for older deployed proof payloads.
    lanes = accounting.get("lanes") or accounting.get("lane_accounting") or {}
    conservation = accounting.get("candidate_conservation") or {}
    assert isinstance(lanes, dict), "production proof: lane accounting unavailable"
    missing = [lane for lane in FIVE_LANES if lane not in lanes]
    assert not missing, f"production proof: missing five-lane accounting for {missing}"

    lane_result: dict[str, dict] = {}
    for lane in FIVE_LANES:
        value = lanes.get(lane) or {}
        verified = bool(value.get("verified"))
        status = str(value.get("status") or "")
        if not verified:
            assert status == "unable_to_verify", (
                f"{lane}: unverified population must be explicit unable_to_verify, got {status!r}"
            )
        observed = value.get("observed_candidate_count")
        reconciled = bool(value.get("reconciled"))
        if verified and reconciled and int(observed or 0) > 0:
            outcome = "OBSERVED_AND_RECONCILED"
        elif verified and reconciled:
            outcome = "VERIFIED_ZERO_CANDIDATES"
        else:
            outcome = "UNABLE_TO_VERIFY"
        lane_result[lane] = {
            "outcome": outcome,
            "verified": verified,
            "status": status,
            "observed_candidate_count": observed,
            "terminal_candidate_count": value.get("terminal_candidate_count"),
            "valid_pending_candidate_count": value.get("valid_pending_candidate_count"),
            "coverage_debt_candidate_count": value.get("coverage_debt_candidate_count"),
            "unexplained_candidate_count": value.get("unexplained_candidate_count"),
            "conservation_delta": value.get("conservation_delta"),
            "conserved": value.get("conserved"),
            "reconciled": value.get("reconciled"),
            "proof_state": value.get("proof_state"),
            "coverage_complete": value.get("coverage_complete"),
        }

    population_verifiable = bool(
        conservation.get(
            "candidate_population_verifiable",
            conservation.get("population_verifiable"),
        )
    )
    unexplained = conservation.get(
        "unexplained_candidate_count",
        conservation.get("unexplained"),
    )
    coverage_debt = conservation.get(
        "coverage_debt_candidate_count",
        conservation.get("coverage_debt"),
    )
    if population_verifiable:
        assert int(unexplained or 0) == 0, "candidate reconciliation has unexplained disappearance"
        assert int(coverage_debt or 0) == 0, "candidate reconciliation has coverage debt"
        assert int(conservation.get("conservation_delta") or 0) == 0, "candidate conservation delta is nonzero"
        assert conservation.get("conserved") is True, "candidate population is not conserved"
        assert conservation.get("reconciled") is True, "candidate population is not reconciled"

    return {
        "lanes": lane_result,
        "candidate_population_verifiable": population_verifiable,
        "all_lane_sources_verified": conservation.get("all_lane_sources_verified"),
        "unverified_lanes": conservation.get("unverified_lanes"),
        "verification_blockers": conservation.get("verification_blockers"),
        "observed_candidate_count": conservation.get(
            "observed_candidate_count",
            conservation.get("observed"),
        ),
        "terminal_candidate_count": conservation.get(
            "terminal_candidate_count",
            conservation.get("terminal"),
        ),
        "valid_pending_candidate_count": conservation.get(
            "valid_pending_candidate_count",
            conservation.get("valid_pending"),
        ),
        "coverage_debt_candidate_count": coverage_debt,
        "unexplained_candidate_count": unexplained,
        "conservation_delta": conservation.get("conservation_delta"),
        "conserved": conservation.get("conserved"),
        "reconciled": conservation.get("reconciled"),
        "classification_anomalies": accounting.get("classification_anomalies"),
    }


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

    production = _get("/v1/strategy/production-proof")
    _assert_safety(production, label="production proof")
    assert production.get("signing_available") is False, "production proof: signing must remain unavailable"
    assert production.get("transaction_submission_available") is False, "production proof: submission must remain unavailable"
    assert production.get("read_only_observability") is True, "production proof must remain read-only"
    assert production.get("changes_strategy_authority") is False
    assert production.get("changes_economic_thresholds") is False

    e2e_sha = str(e2e.get("release_commit") or "")
    cert_sha = str(certificate.get("release_commit") or "")
    production_sha = str((production.get("release") or {}).get("release_commit") or "")
    if EXPECTED_SHA:
        assert e2e_sha == EXPECTED_SHA, f"e2e release {e2e_sha!r} != expected {EXPECTED_SHA!r}"
        assert cert_sha == EXPECTED_SHA, f"certificate release {cert_sha!r} != expected {EXPECTED_SHA!r}"
        assert production_sha == EXPECTED_SHA, (
            f"production proof release {production_sha!r} != expected {EXPECTED_SHA!r}"
        )
    assert e2e_sha and e2e_sha == cert_sha == production_sha, "production proof release binding mismatch"

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

    attestation_policy = production.get("surface_attestation_policy") or {}
    assert attestation_policy.get("surface_scoped_attestation_required") is True
    assert attestation_policy.get("aggregate_attestation_fallback_allowed") is False
    resource_pressure = production.get("resource_pressure") or {}
    assert resource_pressure.get("read_only_observability") is True
    assert resource_pressure.get("state") in {"healthy", "warning", "critical", "unavailable"}
    assert isinstance(production.get("candidate_accounting"), dict)
    five_lane = _five_lane_result(production)
    final = production.get("final_certification") or {}
    _assert_safety(final, label="final certification")
    assert final.get("surface_scoped_attestation_required") is True
    assert final.get("aggregate_attestation_fallback_allowed") is False

    batch6_gate = production.get("batch6_release_gate") or {}
    epochs = production.get("epochs") or {}
    return {
        "release_commit": cert_sha,
        "state": certificate.get("state"),
        "system_forward_certified": certificate.get("system_forward_certified"),
        "production_proof_state": production.get("state"),
        "production_proof_pass": production.get("production_proof_pass"),
        "batch6_release_gate_verdict": batch6_gate.get("verdict"),
        "batch6_release_gate_blockers": batch6_gate.get("blockers"),
        "economic_epoch": epochs.get("economic_epoch") or epochs.get("economic_freeze_epoch"),
        "measurement_epoch": epochs.get("measurement_epoch"),
        "final_classification": final.get("classification"),
        "coverage_debt_count": (production.get("candidate_accounting") or {}).get("coverage_debt_count"),
        "five_lane_candidate_accounting": five_lane,
        "resource_pressure_state": resource_pressure.get("state"),
        "promotion_eligible_families": certificate.get(
            "promotion_eligible_families_under_existing_v51_claims"
        ),
        "blockers": production.get("blockers"),
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
