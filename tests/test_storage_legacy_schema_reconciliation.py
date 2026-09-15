from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from solana_roi import storage_manifest
from solana_roi.active_storage import ActiveStorage
from solana_roi.storage_legacy_schema_reconciliation import (
    LEGACY_RETAINED_CONTRACTS,
    LEGACY_RETAINED_DATASETS,
    OWNER,
    PRUNE_CONDITION,
)
from solana_roi.storage_retention import RetentionClass, RetentionContract


def test_exact_production_observed_legacy_set_is_registered_without_authority(tmp_path: Path) -> None:
    assert len(LEGACY_RETAINED_DATASETS) == 69
    assert tuple(sorted(LEGACY_RETAINED_DATASETS)) == LEGACY_RETAINED_DATASETS
    assert "economic_current_context_probe_audit" in LEGACY_RETAINED_DATASETS
    assert "scout_economic_movement_observations" in LEGACY_RETAINED_DATASETS

    for name in LEGACY_RETAINED_DATASETS:
        contract = storage_manifest.RETENTION_REGISTRY[name]
        assert contract.retention_class is RetentionClass.LEGACY_UNCLASSIFIED
        assert contract.owner == OWNER
        assert contract.hot_or_cold == "cold"
        assert contract.startup_access is False
        assert contract.certification_access is False
        assert contract.prune_condition == PRUNE_CONDITION
        assert name not in storage_manifest.startup_table_allowlist()
        assert name not in storage_manifest.certification_table_allowlist()

    database = tmp_path / "active.sqlite3"
    with sqlite3.connect(database) as conn:
        for index, name in enumerate(LEGACY_RETAINED_DATASETS):
            conn.execute(f'CREATE TABLE "{name}" (id INTEGER PRIMARY KEY, value TEXT)')
            conn.execute(f'INSERT INTO "{name}"(id,value) VALUES(?,?)', (index + 1, name))
        conn.commit()

    # This is the exact production boundary that previously failed every 60s.
    ActiveStorage(database).assert_positive_schema()


def test_unknown_table_still_fails_positive_schema(tmp_path: Path) -> None:
    database = tmp_path / "active.sqlite3"
    with sqlite3.connect(database) as conn:
        conn.execute("CREATE TABLE future_unclassified_dataset(id INTEGER PRIMARY KEY)")
        conn.commit()

    with pytest.raises(ValueError, match="future_unclassified_dataset"):
        ActiveStorage(database).assert_positive_schema()


def test_manifest_rejects_any_legacy_contract_not_in_exact_observed_set() -> None:
    name = "future_legacy_dataset"
    contract = RetentionContract(
        dataset=name,
        owner=OWNER,
        retention_class=RetentionClass.LEGACY_UNCLASSIFIED,
        purpose="should not be admitted",
        consumer="none",
        hot_or_cold="cold",
        max_hot_age=None,
        max_hot_rows=None,
        max_hot_bytes=None,
        archive_policy="none",
        prune_condition=PRUNE_CONDITION,
        startup_access=False,
        certification_access=False,
    )
    storage_manifest.RETENTION_REGISTRY[name] = contract
    try:
        with pytest.raises(RuntimeError, match="not an exact approved retained-legacy contract"):
            storage_manifest.validate_manifest()
    finally:
        storage_manifest.RETENTION_REGISTRY.pop(name, None)
    storage_manifest.validate_manifest()


def test_manifest_rejects_authority_drift_on_exact_legacy_name() -> None:
    original = LEGACY_RETAINED_CONTRACTS[0]
    changed = RetentionContract(
        dataset=original.dataset,
        owner=original.owner,
        retention_class=original.retention_class,
        purpose=original.purpose,
        consumer=original.consumer,
        hot_or_cold=original.hot_or_cold,
        max_hot_age=original.max_hot_age,
        max_hot_rows=original.max_hot_rows,
        max_hot_bytes=original.max_hot_bytes,
        archive_policy=original.archive_policy,
        prune_condition=original.prune_condition,
        startup_access=True,
        certification_access=False,
    )
    storage_manifest.RETENTION_REGISTRY[original.dataset] = changed
    try:
        with pytest.raises(RuntimeError, match="not an exact approved retained-legacy contract"):
            storage_manifest.validate_manifest()
    finally:
        storage_manifest.RETENTION_REGISTRY[original.dataset] = original
    storage_manifest.validate_manifest()
