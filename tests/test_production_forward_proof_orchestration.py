from pathlib import Path


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
