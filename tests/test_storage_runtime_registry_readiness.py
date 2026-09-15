from __future__ import annotations

from pathlib import Path

import pytest

from solana_roi import storage_retention
from solana_roi import storage_runtime_registry_readiness as readiness


EXPECTED_CURRENT_RUNTIME = {
    "anonymous_candidate_latency_failures",
    "candidate_compute_admission_decisions",
    "context_research_bandwidth_decisions",
    "continuation_market_context",
    "direct_solana_hydration_status_meta",
    "direct_solana_hydration_status_recent",
    "direct_solana_release_continuity_epoch",
    "economic_signal_shadow_audit",
    "execution_quote_failures",
    "fomo_learning_entry_windows",
    "fomo_learning_post_entry_flow",
    "fomo_shadow_observations",
    "fomo_shadow_outcomes",
    "risk_conditioned_alpha_v5_outcomes",
    "risk_conditioned_alpha_v5_trials",
    "strategy_learning_compatibility_releases",
    "strategy_learning_exit_paths",
    "strategy_learning_final_paths",
    "strategy_learning_horizon_marks",
    "strategy_learning_subjects",
    "venue_resource_governance_decisions",
}


def test_current_runtime_registry_is_finalized_before_runtime_use() -> None:
    state = readiness.registry_readiness()
    assert state.ready is True
    assert state.deferred is False
    assert set(state.registered_datasets) == EXPECTED_CURRENT_RUNTIME
    storage_retention.assert_registered(EXPECTED_CURRENT_RUNTIME)
    assert all(
        storage_retention.RETENTION_REGISTRY[name].retention_class
        is not storage_retention.RetentionClass.LEGACY_UNCLASSIFIED
        for name in EXPECTED_CURRENT_RUNTIME
    )


def test_pre_ready_maintenance_gate_defers_without_storage_mutation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = tmp_path / "must-not-be-created.sqlite3"
    monkeypatch.setattr(readiness, "_REGISTRY_READY", False)
    state = readiness.maintenance_readiness_gate()
    assert state.ready is False
    assert state.deferred is True
    assert state.reason == "storage_registry_not_finalized"
    assert not sentinel.exists()


def test_post_ready_unknown_dataset_still_fails_closed() -> None:
    readiness.finalize_storage_registry()
    with pytest.raises(ValueError, match="unregistered persistent datasets"):
        readiness.assert_registered_after_readiness(["definitely_unknown_persistent_table"])


def test_registry_finalization_is_idempotent() -> None:
    first = readiness.finalize_storage_registry()
    second = readiness.finalize_storage_registry()
    assert first == second
    assert first.ready is True
