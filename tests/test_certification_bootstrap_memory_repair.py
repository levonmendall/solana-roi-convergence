from __future__ import annotations

import inspect

from solana_roi import certification_proof_memory_repair as repair
from solana_roi import v51_economic_core as core


def test_weighted_bootstrap_is_exactly_equivalent_for_individual_resampling() -> None:
    values = [-1.0, -0.35, -0.02, 0.0, 0.08, 0.21, 0.75, 1.6]
    expected = repair._ORIGINAL_BOOTSTRAP_DISTRIBUTIONS(
        values,
        fraction=0.05,
        samples=73,
    )
    actual = repair._bounded_bootstrap_distributions(
        values,
        fraction=0.05,
        samples=73,
    )
    assert actual == expected


def test_weighted_bootstrap_is_exactly_equivalent_for_unequal_clusters() -> None:
    values = [-0.8, -0.2, 0.05, 0.12, 0.4, 1.1, -0.03, 0.22, 0.9]
    clusters = ["a", "a", "b", "c", "c", "c", "d", "e", "e"]
    expected = repair._ORIGINAL_BOOTSTRAP_DISTRIBUTIONS(
        values,
        fraction=0.1,
        cluster_ids=clusters,
        samples=97,
    )
    actual = repair._bounded_bootstrap_distributions(
        values,
        fraction=0.1,
        cluster_ids=clusters,
        samples=97,
    )
    assert actual == expected


def test_weighted_bootstrap_does_not_materialize_resampled_draw_lists() -> None:
    source = inspect.getsource(repair._bounded_bootstrap_distributions)
    assert "draw.extend" not in source
    assert "draw: list" not in source
    status = repair.status()
    assert status["bootstrap_materialized_resample_draw_lists"] is False
    assert status["bootstrap_seed_changed"] is False
    assert status["bootstrap_sample_count_changed"] is False
    assert status["bootstrap_cluster_resampling_changed"] is False


def test_install_patches_only_bootstrap_implementation_not_economic_contract(monkeypatch) -> None:
    class State:
        pass

    class App:
        def __init__(self) -> None:
            self.state = State()
            self.routes = []

        def add_api_route(self, *args, **kwargs) -> None:
            return None

    app = App()
    monkeypatch.setattr(repair, "_INSTALLED", False)
    monkeypatch.setattr(repair.proof, "_ORIGINAL_PRODUCTION_PROOF", lambda: {})
    monkeypatch.setattr(repair, "_ORIGINAL_PROOF_BUILDER", None)
    original_bootstrap = core._bootstrap_distributions
    original_records = repair.economic._records
    original_phase14 = repair.phase14.combined_promotion_records
    original_phase17 = repair.phase17.combined_promotion_records
    try:
        repair.install_certification_proof_memory_repair(app)
        assert core._bootstrap_distributions is repair._bounded_bootstrap_distributions
        status = repair.status()
        assert status["resource_guard_relaxed"] is False
        assert status["stale_gate_relaxed"] is False
        assert status["continuity_gate_relaxed"] is False
        assert status["economic_thresholds_changed"] is False
        assert status["canonical_evidence_reset"] is False
        assert status["paper_only"] is True
        assert status["live_money_authority"] is False
        assert status["signing_available"] is False
        assert status["transaction_submission_available"] is False
    finally:
        core._bootstrap_distributions = original_bootstrap
        repair.economic._records = original_records
        repair.phase14.combined_promotion_records = original_phase14
        repair.phase17.combined_promotion_records = original_phase17
        monkeypatch.setattr(repair, "_INSTALLED", False)


def test_process_thread_count_is_read_only_telemetry() -> None:
    count = repair._process_thread_count()
    assert count is None or count >= 1
