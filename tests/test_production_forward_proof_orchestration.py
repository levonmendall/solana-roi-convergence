from pathlib import Path
import re


def test_production_forward_proof_cannot_block_render_checks_pass_deploy() -> None:
    workflow = Path(".github/workflows/production-forward-proof.yml").read_text(encoding="utf-8")
    trigger_block = workflow.split("permissions:", 1)[0]
    assert "workflow_dispatch:" in trigger_block
    assert "schedule:" in trigger_block
    assert "push:" not in trigger_block
    assert "pull_request:" not in trigger_block
    assert "workflow_run:" not in trigger_block
    assert "continue-on-error: ${{ github.event_name == 'schedule' }}" in workflow
    assert "Record scheduled retry state" in workflow


def test_post_deploy_probe_still_requires_exact_release_binding() -> None:
    probe = Path("tests/production_forward_proof_probe.py").read_text(encoding="utf-8")
    assert "EXPECTED_RELEASE_COMMIT" in probe
    assert "e2e_sha == EXPECTED_SHA" in probe
    assert "cert_sha == EXPECTED_SHA" in probe
    assert "production_sha == EXPECTED_SHA" in probe
    assert '"/v1/strategy/production-proof"' in probe
    assert "aggregate_attestation_fallback_allowed" in probe


def test_post_deploy_probe_prefers_canonical_five_lane_schema() -> None:
    probe = Path("tests/production_forward_proof_probe.py").read_text(encoding="utf-8")
    canonical = 'accounting.get("lanes")'
    legacy = 'accounting.get("lane_accounting")'
    assert canonical in probe
    assert legacy in probe
    assert probe.index(canonical) < probe.index(legacy)


def test_production_forward_proof_retry_budget_fits_job_timeout() -> None:
    workflow = Path(".github/workflows/production-forward-proof.yml").read_text(encoding="utf-8")
    probe = Path("tests/production_forward_proof_probe.py").read_text(encoding="utf-8")

    def value(pattern: str) -> int:
        match = re.search(pattern, workflow)
        assert match is not None, pattern
        return int(match.group(1))

    timeout_minutes = value(r"timeout-minutes:\s*(\d+)")
    attempts = value(r"FORWARD_PROOF_PROBE_ATTEMPTS:\s*'(\d+)'")
    sleep_seconds = value(r"FORWARD_PROOF_PROBE_SLEEP_SECONDS:\s*'(\d+)'")
    http_timeout_seconds = value(r"FORWARD_PROOF_HTTP_TIMEOUT_SECONDS:\s*'(\d+)'")

    probe_once = probe.split("def _probe_once() -> dict:", 1)[1].split("\ndef main() -> None:", 1)[0]
    request_count = probe_once.count("_get(")
    assert request_count == 4

    worst_case_seconds = (
        attempts * request_count * http_timeout_seconds
        + max(0, attempts - 1) * sleep_seconds
    )
    # Preserve one minute for checkout/setup, assertions, JSON parsing, and cleanup.
    assert worst_case_seconds <= timeout_minutes * 60 - 60
    assert "PYTHONUNBUFFERED: '1'" in workflow
    assert "flush=True" in probe
