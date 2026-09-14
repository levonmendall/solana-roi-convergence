from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from solana_roi.active_storage import ActiveStorage
from solana_roi.storage_retention import (
    RETENTION_REGISTRY,
    RetentionClass,
    assert_registered,
    certification_table_allowlist,
    startup_table_allowlist,
)
from solana_roi.storage_transition import (
    ACTIVE_PATH_ENV,
    ACTIVATE_ENV,
    LEGACY_PATH_ENV,
    build_checkpoint_payload,
    load_verified_checkpoint,
    persist_verified_checkpoint,
    select_runtime_database_from_environment,
    verify_semantic_equivalence,
)


REQUIRED_ACTIVE = {
    "system_current",
    "strategy_current",
    "wallet_current",
    "provider_current",
    "portfolio_current",
    "active_candidates",
    "active_lifecycles",
    "checkpoint_current",
    "continuity_current",
    "certification_current",
    "bounded_market_evidence",
    "bounded_forward_deltas",
    "bounded_transport_state",
    "bounded_recent_diagnostics",
}


def _truth() -> dict[str, object]:
    return {
        "strategy": {"version": "v5.2", "state": "paper"},
        "wallet": {"wallet-a": {"score": 0.7}},
        "wallet_evidence_watermarks": {"wallet-a": 101},
        "provider_source": {"alchemy": {"fresh": True}},
        "freshness": {"market": "2026-09-13T00:00:00+00:00"},
        "latest_event_ids": {"event": 12_491_882, "engine": 12_491_700},
        "active_candidates": {"cand-a": {"stage": "watch"}},
        "active_lifecycles": {"mint-a": {"stage": "graduated"}},
        "portfolio": {"cash": 500.0, "positions": {}},
        "replication_watermarks": {"change_id": 9001},
        "certification": {"release": "test-sha", "state": "not-certified"},
        "continuity": {"epoch": "epoch-7", "boundary": 12_491_882},
    }


def _verified_active(tmp_path: Path) -> Path:
    path = tmp_path / "active" / "epoch-0001.sqlite"
    storage = ActiveStorage(path)
    storage.initialize(epoch_id="epoch-0001")
    truth = _truth()
    checkpoint = build_checkpoint_payload(
        release_sha="92c0c1620f78116e7ecbeade039e9aedaf3a51a9",
        current_truth=truth,
        provenance={"legacy": "LEGACY_MIXED_STORE", "bounded_extraction": True},
    )
    result = persist_verified_checkpoint(storage, checkpoint_payload=checkpoint, source_truth=truth)
    assert result.equivalent
    return path


def test_retention_registry_contains_required_hot_schema_and_no_legacy_unclassified() -> None:
    assert REQUIRED_ACTIVE <= set(RETENTION_REGISTRY)
    assert all(
        contract.retention_class is not RetentionClass.LEGACY_UNCLASSIFIED
        for contract in RETENTION_REGISTRY.values()
    )
    assert set(startup_table_allowlist()) <= set(RETENTION_REGISTRY)
    assert set(certification_table_allowlist()) <= set(RETENTION_REGISTRY)


def test_unregistered_persistence_fails_validation() -> None:
    with pytest.raises(ValueError, match="unregistered persistent datasets"):
        assert_registered(["system_current", "accidental_history_dump"])


def test_active_storage_contains_only_registered_tables(tmp_path: Path) -> None:
    path = tmp_path / "active.sqlite"
    ActiveStorage(path).initialize(epoch_id="epoch-0001")
    with sqlite3.connect(path) as conn:
        tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
    assert REQUIRED_ACTIVE <= tables
    assert tables <= set(RETENTION_REGISTRY)


def test_checkpoint_semantic_equivalence_detects_section_change() -> None:
    truth = _truth()
    checkpoint = build_checkpoint_payload(release_sha="abc", current_truth=truth, provenance={})
    assert verify_semantic_equivalence(truth, checkpoint).equivalent
    altered = json.loads(json.dumps(checkpoint))
    altered["portfolio"]["cash"] = 499.0
    result = verify_semantic_equivalence(truth, altered)
    assert not result.equivalent
    assert result.mismatched_sections == ("portfolio",)


def test_verified_checkpoint_round_trip_and_corruption_fails_closed(tmp_path: Path) -> None:
    path = _verified_active(tmp_path)
    payload = load_verified_checkpoint(path)
    assert payload["latest_event_ids"]["event"] == 12_491_882

    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE checkpoint_current SET payload_json = payload_json || 'x' WHERE verified=1")
        conn.commit()
    with pytest.raises(RuntimeError, match="payload hash mismatch"):
        load_verified_checkpoint(path)


@pytest.mark.parametrize("legacy_gib", [1, 5, 10])
def test_verified_active_selection_is_independent_of_large_legacy_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, legacy_gib: int
) -> None:
    active = _verified_active(tmp_path)
    legacy = tmp_path / f"legacy-{legacy_gib}g.sqlite"
    with legacy.open("wb") as handle:
        handle.truncate(legacy_gib * 1024**3)

    monkeypatch.setenv(ACTIVATE_ENV, "true")
    monkeypatch.setenv(ACTIVE_PATH_ENV, str(active))
    monkeypatch.setenv(LEGACY_PATH_ENV, str(legacy))

    selected = select_runtime_database_from_environment()
    assert selected == active
    assert legacy.stat().st_size == legacy_gib * 1024**3


def test_missing_or_incomplete_active_checkpoint_never_falls_back_to_legacy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    active = tmp_path / "active.sqlite"
    ActiveStorage(active).initialize(epoch_id="epoch-0001")
    legacy = tmp_path / "legacy.sqlite"
    legacy.write_bytes(b"legacy must not be opened")

    monkeypatch.setenv(ACTIVATE_ENV, "true")
    monkeypatch.setenv(ACTIVE_PATH_ENV, str(active))
    monkeypatch.setenv(LEGACY_PATH_ENV, str(legacy))

    with pytest.raises(RuntimeError, match="no verified continuation checkpoint"):
        select_runtime_database_from_environment()
