from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from solana_roi.storage_active_compat_pruning import prune_active_compatibility_database
from solana_roi.storage_runtime_persistence_reconciliation import (
    advance_registered_sequences,
    augment_runtime_current_state_truth,
    copy_bounded_runtime_evidence,
)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def test_runtime_resume_state_is_sealed_into_existing_truth_sections(monkeypatch) -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        "CREATE TABLE certification_release_epochs(release_commit TEXT PRIMARY KEY,started_at TEXT NOT NULL);"
        "CREATE TABLE forward_cohort_manifest(id INTEGER PRIMARY KEY,frozen_at TEXT,release_commit TEXT,manifest_json TEXT,manifest_sha256 TEXT);"
        "CREATE TABLE forward_cohort_arm_state(id INTEGER PRIMARY KEY,armed_at TEXT,manifest_sha256 TEXT);"
        "CREATE TABLE direct_solana_provider_state(provider TEXT PRIMARY KEY,connected INTEGER,connected_at TEXT,last_message_at TEXT,reconnect_count INTEGER,last_error_type TEXT);"
        "CREATE TABLE direct_solana_global_state(id INTEGER PRIMARY KEY,outage_started_at TEXT,unresolved_gap INTEGER,last_backfill_complete_at TEXT,last_backfill_error TEXT);"
        "CREATE TABLE wallet_discovery_state(id INTEGER PRIMARY KEY,last_raw_receipt_id INTEGER,last_cycle_at TEXT,last_broad_scan_at TEXT,last_error TEXT);"
        "CREATE TABLE wallet_discovery_candidates(wallet TEXT PRIMARY KEY,first_seen_at TEXT,last_seen_at TEXT,state TEXT);"
        "CREATE TABLE wallet_realtime_state(wallet TEXT PRIMARY KEY,epoch_started_at TEXT,active INTEGER);"
        "CREATE TABLE wallet_realtime_runtime(id INTEGER PRIMARY KEY,last_cycle_at TEXT,last_error TEXT,last_provider_change_at TEXT,last_recovery_at TEXT);"
        "CREATE TABLE helius_webhook_inbox(id INTEGER PRIMARY KEY,state TEXT,payload_json TEXT);"
        "CREATE TABLE direct_solana_hydration_queue(signature TEXT PRIMARY KEY,status TEXT,updated_at TEXT);"
        "CREATE TABLE wallet_realtime_receipts(id INTEGER PRIMARY KEY,status TEXT,updated_at TEXT);"
    )
    release = "a" * 40
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", release)
    connection.execute(
        "INSERT INTO certification_release_epochs VALUES(?,?)",
        (release, "2026-09-14T00:00:00+00:00"),
    )
    connection.execute("INSERT INTO forward_cohort_manifest VALUES(1,'t',?,'{}','h')", (release,))
    connection.execute("INSERT INTO forward_cohort_arm_state VALUES(1,'t','h')")
    connection.execute("INSERT INTO direct_solana_provider_state VALUES('alchemy',1,'t','t',2,NULL)")
    connection.execute("INSERT INTO direct_solana_global_state VALUES(1,'t',1,NULL,'gap')")
    connection.execute("INSERT INTO wallet_discovery_state VALUES(1,123,'t','t',NULL)")
    connection.execute("INSERT INTO wallet_discovery_candidates VALUES('w','t','t','tracking')")
    connection.execute("INSERT INTO wallet_realtime_state VALUES('w','t',1)")
    connection.execute("INSERT INTO wallet_realtime_runtime VALUES(1,'t',NULL,'t','t')")
    connection.execute("INSERT INTO helius_webhook_inbox VALUES(1,'pending','{}')")
    connection.execute("INSERT INTO direct_solana_hydration_queue VALUES('s','pending','t')")
    connection.execute("INSERT INTO wallet_realtime_receipts VALUES(1,'processing','t')")

    truth = {
        "strategy": {},
        "wallet": {},
        "provider_source": {},
        "continuity": {},
        "certification": {},
    }
    counts: dict[str, int] = {}
    tables = {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    augment_runtime_current_state_truth(connection, tables, truth, counts)

    assert truth["strategy"]["forward_cohort_manifest"][0]["manifest_sha256"] == "h"
    assert truth["strategy"]["forward_cohort_arm_state"][0]["manifest_sha256"] == "h"
    assert truth["certification"]["certification_release_epochs"][0]["release_commit"] == release
    assert truth["provider_source"]["direct_solana_provider_state"][0]["provider"] == "alchemy"
    assert truth["continuity"]["direct_solana_global_state"][0]["unresolved_gap"] == 1
    assert truth["wallet"]["wallet_discovery_candidates"][0]["state"] == "tracking"
    assert truth["continuity"]["helius_webhook_inbox"][0]["state"] == "pending"
    assert truth["continuity"]["direct_solana_hydration_queue"][0]["status"] == "pending"
    assert truth["continuity"]["wallet_realtime_receipts"][0]["status"] == "processing"


def test_certification_release_epoch_selector_binds_exact_frontier_in_large_history(monkeypatch) -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        "CREATE TABLE certification_release_epochs(release_commit TEXT PRIMARY KEY,started_at TEXT NOT NULL)"
    )
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    history = [
        (f"{index:040x}", _iso(base + timedelta(minutes=index)))
        for index in range(300)
    ]
    current_release = "f" * 40
    current = (current_release, _iso(base + timedelta(minutes=300)))
    later_nonfrontier = ("e" * 40, _iso(base + timedelta(minutes=301)))
    connection.executemany(
        "INSERT INTO certification_release_epochs(release_commit,started_at) VALUES(?,?)",
        [*history, current, later_nonfrontier],
    )
    source_count = connection.execute("SELECT COUNT(*) FROM certification_release_epochs").fetchone()[0]
    assert source_count > 256
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", current_release)

    truth = {"certification": {}}
    counts: dict[str, int] = {}
    augment_runtime_current_state_truth(
        connection,
        {"certification_release_epochs"},
        truth,
        counts,
    )

    selected = truth["certification"]["certification_release_epochs"]
    assert [row["release_commit"] for row in selected] == [history[-1][0], current_release]
    assert counts["certification_release_epochs"] == 2
    assert connection.execute("SELECT COUNT(*) FROM certification_release_epochs").fetchone()[0] == source_count
    assert connection.execute(
        "SELECT COUNT(*) FROM certification_release_epochs WHERE release_commit=?",
        (later_nonfrontier[0],),
    ).fetchone()[0] == 1


def test_registered_autoincrement_sequence_advances_without_copying_old_rows(tmp_path: Path) -> None:
    source = sqlite3.connect(tmp_path / "source.sqlite3")
    destination = sqlite3.connect(tmp_path / "destination.sqlite3")
    try:
        source.execute("CREATE TABLE execution_quote_observations(id INTEGER PRIMARY KEY AUTOINCREMENT,received_at TEXT)")
        source.execute("INSERT INTO execution_quote_observations(received_at) VALUES('old')")
        source.execute("UPDATE sqlite_sequence SET seq=987654 WHERE name='execution_quote_observations'")
        source.commit()
        destination.execute("CREATE TABLE execution_quote_observations(id INTEGER PRIMARY KEY AUTOINCREMENT,received_at TEXT)")
        destination.commit()

        advanced = advance_registered_sequences(source, destination)
        destination.commit()
        assert advanced["execution_quote_observations"] == 987654
        assert destination.execute(
            "SELECT seq FROM sqlite_sequence WHERE name='execution_quote_observations'"
        ).fetchone()[0] == 987654
        assert destination.execute("SELECT COUNT(*) FROM execution_quote_observations").fetchone()[0] == 0
    finally:
        source.close()
        destination.close()


def test_shadow_copy_supports_legacy_helius_completed_at_schema() -> None:
    source = sqlite3.connect(":memory:")
    destination = sqlite3.connect(":memory:")
    now = datetime(2026, 9, 14, tzinfo=timezone.utc)
    old = _iso(now - timedelta(days=40))
    recent = _iso(now - timedelta(days=1))
    source.execute(
        "CREATE TABLE helius_webhook_inbox(id INTEGER PRIMARY KEY,state TEXT,completed_at TEXT)"
    )
    source.executemany(
        "INSERT INTO helius_webhook_inbox VALUES(?,?,?)",
        [(1, "pending", None), (2, "complete", old), (3, "complete", recent)],
    )
    selected: list[tuple[int, ...]] = []
    selected_sql: list[str] = []

    def copy_query(
        src: sqlite3.Connection,
        _dst: sqlite3.Connection,
        table: str,
        sql: str,
        args: tuple[object, ...],
    ) -> int:
        assert table == "helius_webhook_inbox"
        selected_sql.append(sql)
        rows = src.execute(sql, args).fetchall()
        selected.append(tuple(int(row[0]) for row in rows))
        return len(rows)

    counts: dict[str, int] = {}
    copy_bounded_runtime_evidence(
        source,
        destination,
        copy_query=copy_query,
        counts=counts,
        now=now,
    )
    assert selected == [(1, 3)]
    assert "completed_at" in selected_sql[0]
    assert counts["helius_webhook_inbox"] == 2
    source.close()
    destination.close()


def test_active_pruning_keeps_pending_transport_and_pairs_wallet_source_with_dedup(tmp_path: Path) -> None:
    path = tmp_path / "active.sqlite3"
    now = datetime(2026, 9, 14, tzinfo=timezone.utc)
    old = _iso(now - timedelta(days=40))
    recent = _iso(now - timedelta(days=1))
    connection = sqlite3.connect(path)
    connection.executescript(
        "CREATE TABLE helius_webhook_inbox(id INTEGER PRIMARY KEY,state TEXT,completed_at TEXT);"
        "CREATE TABLE wallet_discovery_forward_observations(id INTEGER PRIMARY KEY,signature TEXT UNIQUE,received_at TEXT);"
        "CREATE TABLE v52_wallet_forward_integrity_seen(signature TEXT PRIMARY KEY,recorded_at TEXT);"
        "CREATE TABLE wallet_realtime_receipts(id INTEGER PRIMARY KEY,status TEXT,updated_at TEXT);"
    )
    connection.executemany(
        "INSERT INTO helius_webhook_inbox VALUES(?,?,?)",
        [(1, "pending", None), (2, "complete", old), (3, "complete", recent)],
    )
    connection.executemany(
        "INSERT INTO wallet_discovery_forward_observations VALUES(?,?,?)",
        [(1, "old", old), (2, "recent", recent)],
    )
    connection.executemany(
        "INSERT INTO v52_wallet_forward_integrity_seen VALUES(?,?)",
        [("old", old), ("recent", old), ("orphan", old)],
    )
    connection.executemany(
        "INSERT INTO wallet_realtime_receipts VALUES(?,?,?)",
        [(1, "pending", old), (2, "processing", old), (3, "complete", old)],
    )
    connection.commit()
    connection.close()

    prune_active_compatibility_database(path, now=now)

    connection = sqlite3.connect(path)
    assert connection.execute("SELECT id FROM helius_webhook_inbox ORDER BY id").fetchall() == [(1,), (3,)]
    assert connection.execute("SELECT signature FROM wallet_discovery_forward_observations").fetchall() == [("recent",)]
    # The retained recent source keeps its de-dup marker even though the marker's
    # own timestamp is old; orphan/removed-source markers may disappear.
    assert connection.execute("SELECT signature FROM v52_wallet_forward_integrity_seen").fetchall() == [("recent",)]
    assert connection.execute("SELECT id FROM wallet_realtime_receipts ORDER BY id").fetchall() == [(1,), (2,)]
    connection.close()


def test_active_certifier_child_uses_disposable_clone_as_active_database(tmp_path: Path, monkeypatch) -> None:
    import solana_roi.certifier_cleanup_service as wrapper

    snapshot = tmp_path / "cycle.sqlite3"
    snapshot.write_bytes(b"not-used-by-environment-test")
    monkeypatch.setenv("SOLANA_ROI_ACTIVE_STORAGE_ENABLED", "1")
    monkeypatch.setenv("SOLANA_ROI_ACTIVE_DB_PATH", "/var/data/wrong-service-path.sqlite3")
    monkeypatch.setenv("SOLANA_ROI_ACTIVE_STORAGE_SHADOW", "1")
    monkeypatch.setenv("SOLANA_ROI_ACTIVE_STORAGE_FINALIZE_FROM_LEGACY", "1")
    monkeypatch.setenv("SOLANA_ROI_ACTIVE_STORAGE_HIDE_LEGACY", "1")
    monkeypatch.setenv("SOLANA_ROI_ACTIVE_STORAGE_RESTORE_LEGACY", "0")

    release = "b" * 40
    env = wrapper._active_storage_child_environment(snapshot, release)
    assert env["SOLANA_ROI_DB_PATH"] == str(snapshot)
    assert env["SOLANA_ROI_ACTIVE_DB_PATH"] == str(snapshot)
    assert env["SOLANA_ROI_ACTIVE_STORAGE_ENABLED"] == "1"
    assert env["SOLANA_ROI_ACTIVE_STORAGE_SHADOW"] == "0"
    assert env["SOLANA_ROI_ACTIVE_STORAGE_FINALIZE_FROM_LEGACY"] == "0"
    assert env["SOLANA_ROI_ACTIVE_STORAGE_HIDE_LEGACY"] == "0"
    assert env["SOLANA_ROI_ACTIVE_STORAGE_RESTORE_LEGACY"] == "0"
    assert env["SOLANA_ROI_RELEASE_COMMIT"] == release
    assert env["GIT_COMMIT"] == release


def test_active_certifier_child_leaves_legacy_mode_legacy(tmp_path: Path, monkeypatch) -> None:
    import solana_roi.certifier_cleanup_service as wrapper

    snapshot = tmp_path / "cycle.sqlite3"
    monkeypatch.setenv("SOLANA_ROI_ACTIVE_STORAGE_ENABLED", "0")
    env = wrapper._active_storage_child_environment(snapshot, "c" * 40)
    assert env["SOLANA_ROI_DB_PATH"] == str(snapshot)
    assert env.get("SOLANA_ROI_ACTIVE_STORAGE_ENABLED") == "0"
