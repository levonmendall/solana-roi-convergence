from __future__ import annotations

import hashlib
import json
from pathlib import Path

from solana_roi.dependency_integrity import dependency_integrity_status
from solana_roi.direct_deployment import deployment_preflight
from solana_roi.strategy_v51_authority import authority


ROOT = Path(__file__).resolve().parents[1]


def test_75_archives_superseded_strategy_documents_and_keeps_active_docs_current() -> None:
    assert not (ROOT / "STRATEGY_V4.md").exists()
    assert not (ROOT / "docs" / "BASELINE_STRATEGY.md").exists()

    v3 = (ROOT / "docs" / "archive" / "v3" / "BASELINE_STRATEGY.md").read_text(encoding="utf-8")
    v4 = (ROOT / "docs" / "archive" / "v4" / "STRATEGY_V4.md").read_text(encoding="utf-8")
    architecture = (ROOT / "docs" / "ARCHITECTURE.md").read_text(encoding="utf-8")
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    assert "ARCHIVED" in v3 and "no current" in v3
    assert "ARCHIVED" in v4 and "no current" in v4
    assert "roi-convergence-v5.1-consolidated-proof-1" in architecture
    assert "active v3.1 cohort" not in architecture.lower()
    assert "Architecture package release: **0.5.1**" in readme


def test_76_77_package_metadata_is_architecture_release_not_strategy_version() -> None:
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'version = "0.5.1"' in pyproject
    assert "Paper-only forward-validation and economic-certification engine" in pyproject
    assert "strategy_v51_authority.json" not in pyproject


def test_78_79_env_example_is_current_topology_and_hides_legacy_catchup_controls() -> None:
    env = (ROOT / ".env.example").read_text(encoding="utf-8")
    for section in ("REQUIRED PRODUCTION", "OPTIONAL PRODUCTION TUNING", "RESEARCH ONLY", "LEGACY / ARCHIVE"):
        assert section in env
    assert "solana-rpc.publicnode.com" in env
    assert "solana.api.onfinality.io/public" in env
    assert "SOLANA_ROI_ALCHEMY_API_KEY=" in env
    assert "ROBINHOOD_RPC_URL=" in env
    assert "ROBINHOOD_WS_URL=" in env
    assert "ROBINHOOD_POLL_SECONDS=" not in env
    assert "ROBINHOOD_BOOTSTRAP_BLOCK_LOOKBACK=" not in env
    assert "HELIUS_WEBHOOK_AUTH=" not in env
    assert "PRIVATE_KEY" not in env
    assert "SEED_PHRASE" not in env


def test_80_lock_hash_is_recorded_and_deployment_preflight_reports_it() -> None:
    lock = (ROOT / "requirements.lock").read_bytes()
    lock_hash = hashlib.sha256(lock).hexdigest()
    manifest = json.loads((ROOT / "dependency_compatibility.json").read_text(encoding="utf-8"))
    assert manifest["requirements_lock_sha256"] == lock_hash

    status = dependency_integrity_status()
    assert status["requirements_lock_present"] is True
    assert status["requirements_lock_sha256"] == lock_hash
    assert status["compatibility_review_matches_lock"] is True
    assert status["deterministic_production_install"] is True

    profiles = [
        {
            "wallet": "4BdKaxN8G6ka4GYtQQWk4G4dZRUTX2vQH9GcXdBREFUk",
            "entity_id": "kol:jijo",
            "tier": "S",
            "first_touch_sample_size": 191,
            "historically_eligible": True,
        }
    ]
    preflight = deployment_preflight(
        {
            "PAPER_ONLY": "true",
            "SOLANA_NETWORK": "mainnet-beta",
            "SOLANA_ROI_DIRECT_SOLANA_ENABLED": "true",
            "SOLANA_ROI_PROGRAM_WIDE_SWAP_COVERAGE": "true",
            "SOLANA_ROI_SHADOW_CLOCK_ENABLED": "true",
            "SOLANA_ROI_ALCHEMY_API_KEY": "test-read-only-token",
            "JUPITER_API_KEY": "test",
            "SOLANA_ROI_COHORT_ARM_AUTH": "test",
            "SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY": "4BdKaxN8G6ka4GYtQQWk4G4dZRUTX2vQH9GcXdBREFUk",
            "SOLANA_ROI_WALLET_PROFILES_JSON": json.dumps(profiles),
        }
    )
    assert preflight["dependency_integrity"]["requirements_lock_sha256"] == lock_hash
    lock_check = next(row for row in preflight["checks"] if row["name"] == "deterministic_dependency_lock")
    assert lock_check["ok"] is True


def test_81_dependency_updates_are_review_only_and_compatibility_bound() -> None:
    dependabot = (ROOT / ".github" / "dependabot.yml").read_text(encoding="utf-8")
    policy = (ROOT / "docs" / "DEPENDENCY_UPDATE_POLICY.md").read_text(encoding="utf-8")
    manifest = json.loads((ROOT / "dependency_compatibility.json").read_text(encoding="utf-8"))

    assert 'package-ecosystem: "pip"' in dependabot
    assert 'interval: "weekly"' in dependabot
    assert "auto-merge" not in dependabot.lower()
    assert "execution compatibility review" in policy.lower()
    assert "measurement compatibility review" in policy.lower()
    assert manifest["execution_compatibility_review"] == "confirmed_no_change"
    assert manifest["measurement_compatibility_review"] == "confirmed_no_change"
    assert manifest["strategy_economic_authority_changed"] is False


def test_phase11_preserves_frozen_v51_paper_only_authority() -> None:
    spec = authority()
    assert spec["authority_id"] == "roi-convergence-v5.1-consolidated-proof-1"
    assert spec["economic_freeze_epoch"] == "v51-consolidated-proof-20260905"
    assert spec["execution"]["latency_hard_max_seconds"] == 20.0
    assert spec["paper_only"] is True
    assert spec["live_money_authority"] is False
    assert spec["signing_available"] is False
    assert spec["transaction_submission_available"] is False
