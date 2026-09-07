from __future__ import annotations

import sqlite3
import threading

from solana_roi.strategy_v51_authority import ECONOMIC_FREEZE_EPOCH, authority_fingerprint
from solana_roi.v51_batch6_production_proof_release_gate import _accounting_assertion
from solana_roi.v51_measurement_integrity import (
    MEASUREMENT_EPOCH,
    ensure_release_compatibility,
    proof_metadata,
)

PRE_BATCH7_MEASUREMENT_EPOCH = "v51-measurement-post185-20260905-1"
BATCH7_MEASUREMENT_EPOCH = "v51-measurement-batch7-20260906-1"


class Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row


def test_batch7_starts_new_measurement_epoch_without_changing_economic_epoch(monkeypatch) -> None:
    store = Store()
    release = "2" * 40
    monkeypatch.setenv("RENDER_GIT_COMMIT", release)

    row = ensure_release_compatibility(store, release)
    metadata = proof_metadata(store)

    assert MEASUREMENT_EPOCH == BATCH7_MEASUREMENT_EPOCH
    assert row is not None
    assert row["measurement_epoch"] == BATCH7_MEASUREMENT_EPOCH
    assert row["economic_epoch"] == ECONOMIC_FREEZE_EPOCH
    assert row["economic_fingerprint"] == authority_fingerprint()
    assert metadata["measurement_epoch"] == BATCH7_MEASUREMENT_EPOCH
    assert metadata["economic_freeze_epoch"] == ECONOMIC_FREEZE_EPOCH
    assert metadata["paper_only"] is True
    assert metadata["live_money_authority"] is False


def test_batch7_rotation_does_not_rewrite_historical_epoch_tags(monkeypatch) -> None:
    store = Store()
    historical_release = "1" * 40
    current_release = "2" * 40

    # Only the running release may auto-register as measurement-valid. Register the
    # first release while it is current, then preserve it as historical before
    # switching the runtime identity to the Batch 7 release.
    monkeypatch.setenv("RENDER_GIT_COMMIT", historical_release)
    historical = ensure_release_compatibility(store, historical_release)
    assert historical is not None
    with store._lock, store.db:
        store.db.execute(
            "UPDATE v51_release_compatibility SET measurement_epoch=? WHERE release_commit=?",
            (PRE_BATCH7_MEASUREMENT_EPOCH, historical_release),
        )

    monkeypatch.setenv("RENDER_GIT_COMMIT", current_release)
    current = ensure_release_compatibility(store, current_release)
    preserved = ensure_release_compatibility(store, historical_release)

    assert preserved is not None
    assert current is not None
    assert preserved["measurement_epoch"] == PRE_BATCH7_MEASUREMENT_EPOCH
    assert current["measurement_epoch"] == BATCH7_MEASUREMENT_EPOCH
    assert preserved["economic_epoch"] == ECONOMIC_FREEZE_EPOCH
    assert current["economic_epoch"] == ECONOMIC_FREEZE_EPOCH
    assert preserved["economic_fingerprint"] == current["economic_fingerprint"] == authority_fingerprint()

    metadata = proof_metadata(store)
    assert metadata["measurement_epoch"] == BATCH7_MEASUREMENT_EPOCH
    assert metadata["economic_freeze_epoch"] == ECONOMIC_FREEZE_EPOCH


def test_batch7_gate_consumes_canonical_five_lane_accounting_names() -> None:
    accounting = {
        "coverage_complete": True,
        "classification_anomalies": {
            "local_candidate_store_readable": True,
            "unclassified_solana_candidate_count": 0,
            "orphan_stage_state_count": 0,
            "robinhood_count_inconsistency": False,
        },
        "candidate_conservation": {
            "candidate_population_verifiable": True,
            "all_lane_sources_verified": True,
            "unverified_lanes": [],
            "verification_blockers": [],
            "observed_candidate_count": 9,
            "terminal_candidate_count": 6,
            "valid_pending_candidate_count": 3,
            "coverage_debt_candidate_count": 0,
            "unexplained_candidate_count": 0,
            "conservation_delta": 0,
            "conserved": True,
            "reconciled": True,
        },
    }

    result = _accounting_assertion(accounting)

    assert result["pass"] is True
    assert result["population_verifiable"] is True
    assert result["observed"] == 9
    assert result["terminal"] == 6
    assert result["valid_pending"] == 3
    assert result["coverage_debt"] == 0
    assert result["unexplained"] == 0
    assert result["no_disappeared_candidates"] is True
    assert result["verification_blockers"] == []


def test_batch7_gate_rejects_canonical_accounting_anomalies() -> None:
    accounting = {
        "coverage_complete": True,
        "classification_anomalies": {
            "local_candidate_store_readable": True,
            "unclassified_solana_candidate_count": 1,
            "orphan_stage_state_count": 0,
            "robinhood_count_inconsistency": False,
        },
        "candidate_conservation": {
            "candidate_population_verifiable": False,
            "verification_blockers": ["unclassified_solana_candidates"],
            "observed_candidate_count": None,
            "terminal_candidate_count": None,
            "valid_pending_candidate_count": None,
            "coverage_debt_candidate_count": None,
            "unexplained_candidate_count": None,
            "conservation_delta": None,
            "conserved": False,
            "reconciled": False,
        },
    }

    result = _accounting_assertion(accounting)

    assert result["pass"] is False
    assert result["population_verifiable"] is False
    assert "unclassified_solana_candidates" in result["verification_blockers"]
    assert result["no_disappeared_candidates"] is False
