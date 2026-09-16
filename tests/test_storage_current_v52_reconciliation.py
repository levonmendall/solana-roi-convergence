from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from solana_roi import storage_current_v52_reconciliation as current_v52
from solana_roi import storage_manifest
from solana_roi.runtime_storage_composition import (
    LEGACY_QUARANTINE_ENV,
    _quarantine_legacy_for_independence_proof,
    _restore_legacy_from_quarantine,
)
from solana_roi.storage_current_v52_pruning import prune_current_v52_database


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def test_current_main_v52_persistence_is_explicitly_classified() -> None:
    names = set(current_v52.registered_dataset_names())
    expected = {
        "v51_release_compatibility",
        "v52_tournament_exact_evidence",
        "v52_wallet_forward_runtime_state",
        "v52_wallet_forward_integrity_seen",
        "v52_wallet_forward_shadow_decisions",
        "v52_wallet_forward_shadow_outcomes",
        "v52_wallet_forward_replay_runs",
        "v52_market_validation_lane_events",
        "v52_market_validation_shadow_variants",
        "v52_market_validation_continuation_horizons",
        "v52_market_validation_component_ablation",
        "v52_market_validation_completion_evaluations",
        "v52_market_validation_shadow_entries",
        "v52_market_validation_hardening_audit",
    }
    assert names == expected
    assert expected <= set(storage_manifest.RETENTION_REGISTRY)
    certification = set(storage_manifest.certification_table_allowlist())
    assert "v52_wallet_forward_runtime_state" in certification
    assert "v52_wallet_forward_shadow_decisions" in certification
    assert "v52_market_validation_completion_evaluations" in certification
    assert "v52_market_validation_hardening_audit" not in certification


def test_wallet_forward_runtime_epoch_is_added_to_exact_transition_truth() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        "CREATE TABLE v52_wallet_forward_runtime_state("
        "id INTEGER PRIMARY KEY CHECK(id=1),runtime_version TEXT NOT NULL,started_at TEXT NOT NULL,"
        "last_capture_at TEXT,last_validation_at TEXT,last_error TEXT,paper_only INTEGER NOT NULL,"
        "live_money_authority INTEGER NOT NULL)"
    )
    started = "2026-09-01T12:00:00+00:00"
    connection.execute(
        "INSERT INTO v52_wallet_forward_runtime_state VALUES(1,'v1',?,NULL,NULL,NULL,1,0)",
        (started,),
    )
    truth = {"strategy": {"strategy_controls": []}}
    counts: dict[str, int] = {}
    current_v52.augment_current_state_truth(
        connection,
        {"v52_wallet_forward_runtime_state"},
        truth,
        counts,
    )
    rows = truth["strategy"]["v52_wallet_forward_runtime_state"]
    assert rows == [
        {
            "id": 1,
            "runtime_version": "v1",
            "started_at": started,
            "last_capture_at": None,
            "last_validation_at": None,
            "last_error": None,
            "paper_only": 1,
            "live_money_authority": 0,
        }
    ]
    assert counts["v52_wallet_forward_runtime_state"] == 1


def test_pruning_removes_only_old_resolved_wallet_shadow_rows(tmp_path: Path) -> None:
    path = tmp_path / "active.sqlite3"
    now = datetime(2026, 9, 14, tzinfo=timezone.utc)
    old = _iso(now - timedelta(days=40))
    recent = _iso(now - timedelta(days=2))
    connection = sqlite3.connect(path)
    connection.executescript(
        "CREATE TABLE v52_wallet_forward_runtime_state("
        "id INTEGER PRIMARY KEY,runtime_version TEXT,started_at TEXT,last_capture_at TEXT,last_validation_at TEXT,"
        "last_error TEXT,paper_only INTEGER,live_money_authority INTEGER);"
        "CREATE TABLE v52_wallet_forward_shadow_decisions("
        "id INTEGER PRIMARY KEY,candidate_id TEXT,observed_at TEXT);"
        "CREATE TABLE v52_wallet_forward_shadow_outcomes("
        "id INTEGER PRIMARY KEY,decision_id INTEGER,resolved_at TEXT);"
    )
    connection.execute(
        "INSERT INTO v52_wallet_forward_runtime_state VALUES(1,'v1',?,NULL,NULL,NULL,1,0)",
        (old,),
    )
    connection.executemany(
        "INSERT INTO v52_wallet_forward_shadow_decisions VALUES(?,?,?)",
        [(1, "old-resolved", old), (2, "old-unresolved", old), (3, "recent-resolved", recent)],
    )
    connection.executemany(
        "INSERT INTO v52_wallet_forward_shadow_outcomes VALUES(?,?,?)",
        [(11, 1, old), (13, 3, recent)],
    )
    connection.commit()
    connection.close()

    prune_current_v52_database(path, now=now)

    connection = sqlite3.connect(path)
    assert connection.execute("SELECT started_at FROM v52_wallet_forward_runtime_state WHERE id=1").fetchone()[0] == old
    assert [row[0] for row in connection.execute("SELECT id FROM v52_wallet_forward_shadow_decisions ORDER BY id")] == [2, 3]
    assert [row[0] for row in connection.execute("SELECT id FROM v52_wallet_forward_shadow_outcomes ORDER BY id")] == [13]
    connection.close()


def test_pruning_preserves_old_unresolved_market_variant_and_entry(tmp_path: Path) -> None:
    path = tmp_path / "active.sqlite3"
    now = datetime(2026, 9, 14, tzinfo=timezone.utc)
    old = _iso(now - timedelta(days=40))
    connection = sqlite3.connect(path)
    connection.executescript(
        "CREATE TABLE v52_market_validation_shadow_variants("
        "candidate_key TEXT,observed_at TEXT,variant_id TEXT,net_return REAL);"
        "CREATE TABLE v52_market_validation_shadow_entries("
        "candidate_key TEXT,observed_at TEXT,variant_id TEXT);"
    )
    connection.executemany(
        "INSERT INTO v52_market_validation_shadow_variants VALUES(?,?,?,?)",
        [("keep", old, "G", None), ("drop", old, "G", 0.2)],
    )
    connection.executemany(
        "INSERT INTO v52_market_validation_shadow_entries VALUES(?,?,?)",
        [("keep", old, "G"), ("drop", old, "G")],
    )
    connection.commit()
    connection.close()

    prune_current_v52_database(path, now=now)

    connection = sqlite3.connect(path)
    assert connection.execute("SELECT candidate_key FROM v52_market_validation_shadow_variants").fetchall() == [("keep",)]
    assert connection.execute("SELECT candidate_key FROM v52_market_validation_shadow_entries").fetchall() == [("keep",)]
    connection.close()


def test_legacy_quarantine_has_in_product_byte_exact_restore(tmp_path: Path, monkeypatch) -> None:
    legacy = tmp_path / "legacy.sqlite3"
    quarantine = tmp_path / "quarantine"
    family = {
        "": b"main-database",
        "-wal": b"wal-bytes",
        "-shm": b"shm-bytes",
    }
    for suffix, payload in family.items():
        Path(str(legacy) + suffix).write_bytes(payload)
    monkeypatch.setenv(LEGACY_QUARANTINE_ENV, str(quarantine))

    quarantined = _quarantine_legacy_for_independence_proof(legacy)
    assert quarantined["deleted"] is False
    assert not legacy.exists()
    assert (quarantine / legacy.name).read_bytes() == family[""]

    restored = _restore_legacy_from_quarantine(legacy)
    assert restored["deleted"] is False
    for suffix, payload in family.items():
        assert Path(str(legacy) + suffix).read_bytes() == payload
    assert not (quarantine / legacy.name).exists()
