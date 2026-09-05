from __future__ import annotations

from pathlib import Path

from solana_roi import post161_candidate_attribution_repair as post161
from solana_roi import post164_invocation_source_repair as invocation
from solana_roi import robinhood_forward_only_runtime_repair as robinhood_forward
from solana_roi import venue_native_candidate_graph_repair as venue_graph
from solana_roi.v51_strategy_api import _isolated_robinhood_proof


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "solana_roi"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_11_helius_is_absent_from_canonical_environment_contract() -> None:
    env_contract = _text(ROOT / ".env.example")
    assert "HELIUS_API_KEY" not in env_contract
    assert "HELIUS_WEBHOOK_AUTH" not in env_contract
    assert "SOLANA_ROI_DIRECT_SOLANA_ENABLED=true" in env_contract
    assert "SOLANA_ROI_RPC_ENDPOINTS_JSON=" in env_contract


def test_12_root_contract_and_package_metadata_name_v51() -> None:
    integration = _text(ROOT / "INTEGRATION.md")
    pyproject = _text(ROOT / "pyproject.toml")
    assert "roi-convergence-v5.1-context-exactness-1" in integration
    assert "roi-convergence-v5.1-consolidated-proof-1" in integration
    assert "ROI Convergence v3.1 on Solana" not in pyproject
    assert (ROOT / "requirements.lock").is_file()


def test_13_workflow_invokes_real_launched_production_smoke() -> None:
    workflow = _text(ROOT / ".github" / "workflows" / "test.yml")
    assert "python tests/launched_production_smoke.py" in workflow
    smoke = _text(ROOT / "tests" / "launched_production_smoke.py")
    assert "uvicorn" in smoke
    assert "solana_roi.production:app" in smoke
    assert "/v1/strategy/authority" in smoke


def test_14_required_ci_check_name_is_stable() -> None:
    workflow = _text(ROOT / ".github" / "workflows" / "test.yml")
    assert "required-ci:" in workflow
    assert "pull_request:" in workflow
    assert "branches: [main]" in workflow


def test_15_16_venue_native_graph_contract_remains_installed() -> None:
    assert "venue-native" in venue_graph.REPAIR_VERSION
    source = _text(SRC / "venue_native_candidate_graph_repair.py")
    assert "programIdIndex" in source
    assert "loadedAddresses" in source
    assert "temporary token accounts" in source
    assert "semantic_multiple_directional_endpoints" in source
    assert "entry_authority" in source


def test_17_compiled_transfers_diagnostics_and_robinhood_429_repair_remain() -> None:
    assert post161.RAW_TRANSFER_DECODER_VERSION == "compiled-spl-system-transfer-v1"
    assert post161.DIAGNOSTIC_VERSION == "sanitized-scout-failure-shape-v1"
    assert post161.MAX_DIAGNOSTIC_ROWS == 256
    throttling = _text(SRC / "robinhood_rpc_rate_limit_repair.py")
    assert "Retry-After" in throttling
    assert "cooldown" in throttling.lower()
    assert "429" in throttling


def test_18_candidate_source_requires_actual_supported_program_invocation() -> None:
    assert invocation.SOURCE_AUTHORITY == "executed_supported_program_invocation_only"
    assert invocation._invoked_supported_sources(
        {"transaction": {"message": {"accountKeys": []}}, "meta": {"innerInstructions": []}}
    ) == set()
    source = _text(SRC / "post164_invocation_source_repair.py")
    assert "account_key_presence_has_candidate_source_authority\": False" in source


def test_19_robinhood_runtime_is_forward_only_and_historical_cursor_is_archival() -> None:
    assert robinhood_forward.REPAIR_VERSION == "robinhood-forward-only-runtime-v1"
    source = _text(SRC / "robinhood_forward_only_runtime_repair.py")
    assert '"historical_backfill_enabled"] = False' in source
    assert '"historical_swap_replay_enabled"] = False' in source
    assert '"retrospective_entry_authority": False' in source
    assert "METADATA_RECOVERY_BLOCKS = 64" in source


def test_20_missing_execution_can_only_remain_explicit_nonentry_research() -> None:
    seeded = _text(SRC / "v51_seeded_e2e.py")
    assert "exact_entry_or_exit_execution_evidence_unavailable" in seeded
    assert "paper_reject" in seeded
    authority = _text(ROOT / "strategy_v51_authority.json")
    assert '"entry_executable"' in seeded or "entry_executable" in seeded
    assert '"mechanical_hard_stops"' in authority


def test_21_robinhood_proof_api_consumes_status_cache_without_store_parameter() -> None:
    proof = {
        "available": True,
        "authority_id": "roi-convergence-v5.1-consolidated-proof-1",
        "economic_freeze_epoch": "v51-consolidated-proof-20260905",
    }
    assert _isolated_robinhood_proof(
        lambda: {"runtime_ready": True, "failed_closed": False, "v51_proof": proof}
    ) == proof
    api_source = _text(SRC / "v51_strategy_api.py")
    assert "nonblocking" in api_source.lower()
    assert "status_provider" in api_source


def test_22_production_proof_wiring_is_bound_to_live_normalizer_and_durable_frontier() -> None:
    proof_wiring = _text(SRC / "post182_production_proof_wiring_repair.py")
    production = _text(SRC / "v51_production_authority.py")
    assert "scout" in proof_wiring.lower()
    assert "normalizer" in proof_wiring.lower()
    assert "websocket" in proof_wiring.lower()
    assert "durable" in proof_wiring.lower()
    assert "install_post183_production_proof_wiring_repair" in production
    assert "paper_only" in production.lower()
    assert "live_money_authority" in production.lower()
