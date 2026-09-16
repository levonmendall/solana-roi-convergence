from __future__ import annotations

import os
import sqlite3
from types import SimpleNamespace
from collections import namedtuple
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from solana_roi import active_storage_epoch_rollover as rollover
from solana_roi import sealed_epoch_reclamation as reclamation
from solana_roi.active_storage import ActiveStorage
from solana_roi import raw_receipt_retention
from solana_roi import storage_maintenance_lock_isolation_repair as isolated_maintenance
from solana_roi.raw_receipt_retention import prune_recent_receipts
from solana_roi.observation_store import ObservationEventStore
from solana_roi.storage_runtime_persistence_reconciliation import (
    copy_bounded_runtime_evidence,
)


NOW = datetime(2026, 9, 16, 4, 0, tzinfo=timezone.utc)
RELEASE = "a" * 40
CHECKPOINT = "checkpoint-total-retention"
DiskUsage = namedtuple("DiskUsage", "total used free")


def _receipt_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        "CREATE TABLE direct_solana_recent_receipts("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,signature TEXT NOT NULL,source_key TEXT NOT NULL,"
        "slot INTEGER NOT NULL,received_at TEXT NOT NULL,launch_like INTEGER NOT NULL,"
        "expires_at TEXT NOT NULL,UNIQUE(signature,source_key));"
        "CREATE INDEX ix_direct_recent_source_received "
        "ON direct_solana_recent_receipts(source_key,received_at);"
        "CREATE TABLE wallet_discovery_state(id INTEGER PRIMARY KEY,last_raw_receipt_id INTEGER NOT NULL);"
        "CREATE TABLE direct_solana_global_state(id INTEGER PRIMARY KEY,unresolved_gap INTEGER NOT NULL);"
        "INSERT INTO wallet_discovery_state VALUES(1,0);"
        "INSERT INTO direct_solana_global_state VALUES(1,0);"
    )


def _receipt(connection: sqlite3.Connection, name: str, at: datetime, expires: datetime) -> int:
    cursor = connection.execute(
        "INSERT INTO direct_solana_recent_receipts("
        "signature,source_key,slot,received_at,launch_like,expires_at) VALUES(?,?,?,?,0,?)",
        (name, "PUMP_FUN", 1, at.isoformat(), expires.isoformat()),
    )
    return int(cursor.lastrowid)


def _normalized_ack_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        "ALTER TABLE wallet_discovery_state "
        "ADD COLUMN last_normalized_swap_id INTEGER NOT NULL DEFAULT 0"
    )
    connection.executescript(
        "CREATE TABLE direct_solana_hydration_queue("
        "signature TEXT PRIMARY KEY,status TEXT NOT NULL,updated_at TEXT NOT NULL);"
        "CREATE TABLE direct_solana_hydration_metrics("
        "signature TEXT PRIMARY KEY,normalized INTEGER NOT NULL,hydrated_at TEXT NOT NULL);"
        "CREATE TABLE normalized_swaps("
        "id INTEGER PRIMARY KEY,signature TEXT NOT NULL UNIQUE,received_at TEXT NOT NULL);"
    )


def _acknowledge_normalized(
    connection: sqlite3.Connection,
    signature: str,
    *,
    normalized: bool = True,
) -> None:
    connection.execute(
        "INSERT INTO direct_solana_hydration_queue(signature,status,updated_at) "
        "VALUES(?, 'complete', ?)",
        (signature, NOW.isoformat()),
    )
    connection.execute(
        "INSERT INTO direct_solana_hydration_metrics(signature,normalized,hydrated_at) "
        "VALUES(?, ?, ?)",
        (signature, 1 if normalized else 0, NOW.isoformat()),
    )
    if normalized:
        cursor = connection.execute(
            "INSERT INTO normalized_swaps(signature,received_at) VALUES(?,?)",
            (signature, NOW.isoformat()),
        )
        connection.execute(
            "UPDATE wallet_discovery_state SET last_normalized_swap_id=? WHERE id=1",
            (int(cursor.lastrowid),),
        )


def test_consumed_receipts_retire_after_safety_floor_but_dependencies_survive(tmp_path: Path) -> None:
    path = tmp_path / "receipts.sqlite3"
    with sqlite3.connect(path) as connection:
        _receipt_schema(connection)
        consumed_old = _receipt(connection, "consumed-old", NOW - timedelta(seconds=121), NOW + timedelta(minutes=13))
        consumed_recent = _receipt(connection, "consumed-recent", NOW - timedelta(seconds=119), NOW + timedelta(minutes=13))
        unconsumed_old = _receipt(connection, "unconsumed-old", NOW - timedelta(minutes=10), NOW + timedelta(minutes=5))
        expired = _receipt(connection, "expired", NOW - timedelta(minutes=20), NOW - timedelta(seconds=1))
        connection.execute(
            "UPDATE wallet_discovery_state SET last_raw_receipt_id=? WHERE id=1",
            (consumed_recent,),
        )
        result = prune_recent_receipts(connection, now=NOW)
        connection.commit()
        remaining = {
            str(row[0]) for row in connection.execute(
                "SELECT signature FROM direct_solana_recent_receipts ORDER BY id"
            )
        }

    assert result["deleted"] == 1
    assert consumed_old < consumed_recent < unconsumed_old < expired
    assert remaining == {"consumed-recent", "unconsumed-old", "expired"}


def test_unresolved_gap_protects_consumed_receipts_but_not_expired_waste(tmp_path: Path) -> None:
    path = tmp_path / "gap.sqlite3"
    with sqlite3.connect(path) as connection:
        _receipt_schema(connection)
        protected = _receipt(connection, "protected", NOW - timedelta(minutes=10), NOW + timedelta(minutes=5))
        _receipt(connection, "expired", NOW - timedelta(minutes=20), NOW - timedelta(seconds=1))
        connection.execute("UPDATE wallet_discovery_state SET last_raw_receipt_id=?", (protected,))
        connection.execute("UPDATE direct_solana_global_state SET unresolved_gap=1")
        result = prune_recent_receipts(connection, now=NOW)
        connection.commit()
        remaining = [row[0] for row in connection.execute("SELECT signature FROM direct_solana_recent_receipts")]

    assert result["unresolved_gap"] is True
    assert result["deleted"] == 0
    assert set(remaining) == {"protected", "expired"}


def test_v52_normalized_consumer_retires_raw_receipts_without_stale_raw_cursor(
    tmp_path: Path,
) -> None:
    path = tmp_path / "normalized-consumer.sqlite3"
    with sqlite3.connect(path) as connection:
        _receipt_schema(connection)
        _normalized_ack_schema(connection)
        _receipt(
            connection,
            "old-raw-no-longer-a-wallet-input",
            NOW - timedelta(seconds=121),
            NOW + timedelta(minutes=13),
        )
        _receipt(
            connection,
            "recent-exact-frontier",
            NOW - timedelta(seconds=119),
            NOW + timedelta(minutes=13),
        )
        _acknowledge_normalized(connection, "old-raw-no-longer-a-wallet-input")
        _acknowledge_normalized(connection, "recent-exact-frontier")
        result = prune_recent_receipts(connection, now=NOW)
        remaining = [
            row[0]
            for row in connection.execute(
                "SELECT signature FROM direct_solana_recent_receipts ORDER BY id"
            )
        ]

    assert result["consumer_cursor"] == 0
    assert result["consumer_mode"] == "normalized_swaps"
    assert result["deleted"] == 1
    assert remaining == ["recent-exact-frontier"]


def test_rollover_copy_uses_same_v52_raw_receipt_dependency_boundary() -> None:
    source = sqlite3.connect(":memory:")
    destination = sqlite3.connect(":memory:")
    source.executescript(
        "CREATE TABLE direct_solana_recent_receipts("
        "id INTEGER PRIMARY KEY,signature TEXT,received_at TEXT,expires_at TEXT);"
        "CREATE TABLE wallet_discovery_state("
        "id INTEGER PRIMARY KEY,last_raw_receipt_id INTEGER NOT NULL,"
        "last_normalized_swap_id INTEGER NOT NULL);"
        "CREATE TABLE direct_solana_global_state("
        "id INTEGER PRIMARY KEY,unresolved_gap INTEGER NOT NULL);"
        "CREATE TABLE direct_solana_hydration_queue("
        "signature TEXT PRIMARY KEY,status TEXT NOT NULL,updated_at TEXT NOT NULL);"
        "CREATE TABLE direct_solana_hydration_metrics("
        "signature TEXT PRIMARY KEY,normalized INTEGER NOT NULL,hydrated_at TEXT NOT NULL);"
        "CREATE TABLE normalized_swaps("
        "id INTEGER PRIMARY KEY,signature TEXT NOT NULL UNIQUE,received_at TEXT NOT NULL);"
        "INSERT INTO wallet_discovery_state VALUES(1,0,2);"
        "INSERT INTO direct_solana_global_state VALUES(1,0);"
        "INSERT INTO direct_solana_hydration_queue VALUES('old','complete','2026-09-16T04:00:00+00:00');"
        "INSERT INTO direct_solana_hydration_queue VALUES('recent','complete','2026-09-16T04:00:00+00:00');"
        "INSERT INTO direct_solana_hydration_metrics VALUES('old',1,'2026-09-16T04:00:00+00:00');"
        "INSERT INTO direct_solana_hydration_metrics VALUES('recent',1,'2026-09-16T04:00:00+00:00');"
        "INSERT INTO normalized_swaps VALUES(1,'old','2026-09-16T04:00:00+00:00');"
        "INSERT INTO normalized_swaps VALUES(2,'recent','2026-09-16T04:00:00+00:00');"
    )
    source.executemany(
        "INSERT INTO direct_solana_recent_receipts VALUES(?,?,?,?)",
        [
            (
                1,
                "old",
                (NOW - timedelta(seconds=121)).isoformat(),
                (NOW + timedelta(minutes=13)).isoformat(),
            ),
            (
                2,
                "recent",
                (NOW - timedelta(seconds=119)).isoformat(),
                (NOW + timedelta(minutes=13)).isoformat(),
            ),
        ],
    )
    selected: list[str] = []

    def copy_query(src, _dst, table, sql, args):
        if table != "direct_solana_recent_receipts":
            return 0
        selected.extend(str(row[1]) for row in src.execute(sql, args))
        return len(selected)

    counts: dict[str, int] = {}
    copy_bounded_runtime_evidence(
        source,
        destination,
        copy_query=copy_query,
        counts=counts,
        now=NOW,
    )

    assert selected == ["recent"]
    assert counts["direct_solana_recent_receipts"] == 1
    assert source.execute(
        "SELECT COUNT(*) FROM direct_solana_recent_receipts"
    ).fetchone()[0] == 2
    source.close()
    destination.close()


def test_normalized_receipts_require_durable_acknowledgement_not_age_or_expiry(
    tmp_path: Path,
) -> None:
    path = tmp_path / "acknowledgement.sqlite3"
    with sqlite3.connect(path) as connection:
        _receipt_schema(connection)
        _normalized_ack_schema(connection)
        for signature in ("delayed", "interrupted", "acknowledged", "negative"):
            _receipt(
                connection,
                signature,
                NOW - timedelta(minutes=20),
                NOW - timedelta(minutes=5),
            )

        # Processing delayed beyond both 120 seconds and expires_at remains raw.
        connection.execute(
            "INSERT INTO direct_solana_hydration_queue VALUES('delayed','pending',?)",
            (NOW.isoformat(),),
        )
        # Canonical insertion without terminal queue/metric acknowledgement is
        # an interrupted write and cannot authorize deletion.
        interrupted = connection.execute(
            "INSERT INTO normalized_swaps(signature,received_at) VALUES('interrupted',?)",
            (NOW.isoformat(),),
        )
        _acknowledge_normalized(connection, "acknowledged")
        _acknowledge_normalized(connection, "negative", normalized=False)
        connection.execute(
            "UPDATE wallet_discovery_state SET last_normalized_swap_id=MAX(last_normalized_swap_id,?) WHERE id=1",
            (int(interrupted.lastrowid),),
        )

        result = prune_recent_receipts(connection, now=NOW)
        remaining = {
            str(row[0])
            for row in connection.execute(
                "SELECT signature FROM direct_solana_recent_receipts"
            )
        }
        canonical = {
            str(row[0]) for row in connection.execute("SELECT signature FROM normalized_swaps")
        }

    assert result["deleted"] == 2
    assert remaining == {"delayed", "interrupted"}
    assert canonical == {"interrupted", "acknowledged"}


def test_late_replay_waits_for_new_safety_floor_and_gap_resolution(tmp_path: Path) -> None:
    path = tmp_path / "late-replay.sqlite3"
    with sqlite3.connect(path) as connection:
        _receipt_schema(connection)
        _normalized_ack_schema(connection)
        _receipt(connection, "replayed", NOW - timedelta(seconds=30), NOW + timedelta(minutes=14))
        _acknowledge_normalized(connection, "replayed")

        assert prune_recent_receipts(connection, now=NOW)["deleted"] == 0
        connection.execute("UPDATE direct_solana_global_state SET unresolved_gap=1")
        assert prune_recent_receipts(connection, now=NOW + timedelta(minutes=3))["deleted"] == 0
        connection.execute("UPDATE direct_solana_global_state SET unresolved_gap=0")
        assert prune_recent_receipts(connection, now=NOW + timedelta(minutes=3))["deleted"] == 1


def test_independent_maintenance_preserves_ack_until_raw_receipt_retires(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "maintenance-order.sqlite3"
    with sqlite3.connect(path) as connection:
        _receipt_schema(connection)
        _normalized_ack_schema(connection)
        connection.execute(
            "ALTER TABLE direct_solana_hydration_metrics "
            "ADD COLUMN historical_recovery INTEGER NOT NULL DEFAULT 0"
        )
        _receipt(
            connection,
            "ordered",
            NOW - timedelta(minutes=20),
            NOW - timedelta(minutes=5),
        )
        _acknowledge_normalized(connection, "ordered")

    monkeypatch.setattr(
        isolated_maintenance.direct_solana_module,
        "utcnow",
        lambda: NOW + timedelta(days=40),
    )
    plane = SimpleNamespace(store=SimpleNamespace(path=path))
    assert isolated_maintenance._prune_operational_rows_once_isolated(plane) == (0, 0)

    with sqlite3.connect(path) as connection:
        assert prune_recent_receipts(connection, now=NOW)["deleted"] == 1

    assert isolated_maintenance._prune_operational_rows_once_isolated(plane) == (1, 1)
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM direct_solana_hydration_queue"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM direct_solana_hydration_metrics"
        ).fetchone()[0] == 0


def test_protected_backlog_fails_closed_at_physical_byte_budget(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "pressure.sqlite3"
    with sqlite3.connect(path) as connection:
        _receipt_schema(connection)
        _receipt(connection, "unconsumed", NOW, NOW + timedelta(minutes=15))
        connection.commit()
        monkeypatch.setattr(raw_receipt_retention, "RECENT_RECEIPT_LIVE_BYTE_BUDGET", 1)
        with pytest.raises(raw_receipt_retention.RawReceiptCapacityBlocked, match="physical byte budget"):
            prune_recent_receipts(connection, now=NOW)
        assert connection.execute("SELECT COUNT(*) FROM direct_solana_recent_receipts").fetchone()[0] == 1


def test_repeated_identical_receipts_and_consumed_stream_remain_bounded(tmp_path: Path) -> None:
    path = tmp_path / "bounded.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA auto_vacuum=INCREMENTAL")
        connection.execute("VACUUM")
        _receipt_schema(connection)
        for cycle in range(8):
            rows = []
            for offset in range(2_000):
                name = f"{cycle}-{offset}"
                rows.append(
                    (
                        name,
                        "PUMP_FUN",
                        offset,
                        (NOW - timedelta(minutes=3)).isoformat(),
                        0,
                        (NOW + timedelta(minutes=12)).isoformat(),
                    )
                )
            connection.executemany(
                "INSERT OR IGNORE INTO direct_solana_recent_receipts("
                "signature,source_key,slot,received_at,launch_like,expires_at) VALUES(?,?,?,?,?,?)",
                rows,
            )
            # Replaying the same batch must not create a second representation.
            connection.executemany(
                "INSERT OR IGNORE INTO direct_solana_recent_receipts("
                "signature,source_key,slot,received_at,launch_like,expires_at) VALUES(?,?,?,?,?,?)",
                rows,
            )
            high = int(connection.execute("SELECT MAX(id) FROM direct_solana_recent_receipts").fetchone()[0])
            connection.execute("UPDATE wallet_discovery_state SET last_raw_receipt_id=?", (high,))
            result = prune_recent_receipts(connection, now=NOW)
            connection.commit()
            assert result["deleted"] == 2_000
            assert connection.execute("SELECT COUNT(*) FROM direct_solana_recent_receipts").fetchone()[0] == 0
        before_pages = int(connection.execute("PRAGMA page_count").fetchone()[0])

    report = ActiveStorage(path).reclaim_free_pages(max_pages=100_000)
    with sqlite3.connect(path) as connection:
        after_pages = int(connection.execute("PRAGMA page_count").fetchone()[0])

    assert report["pages_reclaimed"] > 1
    assert report["vacuum_attempts"] == report["pages_reclaimed"]
    assert after_pages < before_pages


def test_specialized_observations_do_not_duplicate_variable_payloads_in_event_ledger(tmp_path: Path) -> None:
    store = ObservationEventStore(tmp_path / "observations.sqlite3")
    store.record_risk_refresh(
        token_mint="mint",
        trigger_observed_at=NOW.isoformat(),
        trigger_received_at=NOW.isoformat(),
        started_at=NOW.isoformat(),
        completed_at=NOW.isoformat(),
        elapsed_ms=1.0,
        ingestion_latency_ms=1.0,
        end_to_end_ms=2.0,
        complete=True,
        fresh=True,
        readiness={"complete": True, "payload": "x" * 10_000},
    )
    kwargs = {
        "token_mint": "mint",
        "pair_created_at": NOW.isoformat(),
        "assessed_at": NOW.isoformat(),
        "launch_lag_ms": 1.0,
        "launch_near_creation": True,
        "early_buy_count": 1,
        "early_buyer_count": 1,
        "early_buyers_complete": True,
    }
    store.record_program_coverage(**kwargs)
    store.record_program_coverage(**kwargs)
    store.mark_program_coverage_funding_complete("mint", assessed_at=NOW.isoformat())

    counts = store.evidence_counts()
    assert counts["risk_refresh_measurements"] == 1
    assert counts["program_coverage_observations"] == 1
    assert counts["events"] == 0
    store.close()


def test_sealed_inventory_counts_hardlinked_inode_once_and_blocks_third_generation(tmp_path: Path) -> None:
    active = tmp_path / "active.sqlite3"
    active.write_bytes(b"active")
    first_dir = tmp_path / rollover.SEALED_EPOCH_DIR / "sealed-one"
    second_dir = tmp_path / rollover.SEALED_EPOCH_DIR / "sealed-two"
    first_dir.mkdir(parents=True)
    second_dir.mkdir(parents=True)
    first = first_dir / active.name
    second = second_dir / active.name
    first.write_bytes(b"one")
    os.link(first, second)
    inventory = rollover.sealed_epoch_physical_inventory(active)
    assert inventory["path_count"] == 2
    assert inventory["physical_inode_count"] == 1

    second.unlink()
    second.write_bytes(b"two")
    with pytest.raises(RuntimeError, match="unreclaimed sealed epoch bound reached"):
        rollover._enforce_sealed_epoch_bound(active)


def test_rollover_disk_measurements_distinguish_prebuild_from_postrollover(
    tmp_path: Path, monkeypatch
) -> None:
    active = tmp_path / "active.sqlite3"
    active.write_bytes(b"active")
    usage = iter(
        [DiskUsage(100_000, 70_000, 30_000), DiskUsage(100_000, 80_000, 20_000)]
    )
    monkeypatch.setattr(rollover.shutil, "disk_usage", lambda _path: next(usage))
    budget = rollover.ActiveStorageBudget(
        warning_bytes=1_000,
        hard_bytes=10_000,
        max_wal_bytes=1_000,
    )

    prebuild = rollover._rollover_headroom(active, budget)
    postrollover = rollover._disk_usage_payload(active, prefix="postrollover")

    assert prebuild == {
        "prebuild_disk_total_bytes": 100_000,
        "prebuild_disk_used_bytes": 70_000,
        "prebuild_disk_free_bytes": 30_000,
        "required_rollover_free_bytes": 20_000,
    }
    assert postrollover == {
        "postrollover_disk_total_bytes": 100_000,
        "postrollover_disk_used_bytes": 80_000,
        "postrollover_disk_free_bytes": 20_000,
    }


def _late_table(connection: sqlite3.Connection, value: str) -> None:
    connection.execute(
        "CREATE TABLE v51_release_compatibility("
        "release_commit TEXT PRIMARY KEY,measurement_epoch TEXT,promotion_eligible INTEGER)"
    )
    connection.execute(
        "INSERT INTO v51_release_compatibility VALUES('release',?,0)", (value,)
    )
    connection.commit()


def test_late_registered_evidence_must_exist_exactly_in_survivor(tmp_path: Path) -> None:
    active = tmp_path / "active.sqlite3"
    sealed = tmp_path / "sealed.sqlite3"
    with sqlite3.connect(active) as connection:
        _late_table(connection, "epoch")
    with sqlite3.connect(sealed) as connection:
        _late_table(connection, "epoch")
    assert reclamation._late_evidence_coverage(active, sealed)["covered"] is True
    with sqlite3.connect(active) as connection:
        connection.execute("UPDATE v51_release_compatibility SET measurement_epoch='changed'")
        connection.commit()
    result = reclamation._late_evidence_coverage(active, sealed)
    assert result["covered"] is False
    assert result["blockers"] == ["late_evidence_row_missing_or_changed:v51_release_compatibility"]


def test_batch_reclamation_unlinks_all_proven_inodes_and_records_physical_delta(tmp_path: Path, monkeypatch) -> None:
    active = tmp_path / "active.sqlite3"
    active.write_bytes(b"active")
    candidates = []
    for number in range(2):
        directory = tmp_path / rollover.SEALED_EPOCH_DIR / f"sealed-{number}"
        directory.mkdir(parents=True)
        candidate = directory / active.name
        candidate.write_bytes(("sealed" + str(number)).encode())
        stat = candidate.stat()
        candidates.append(
            {
                "path": str(candidate.resolve()),
                "device": int(stat.st_dev),
                "inode": int(stat.st_ino),
                "size_bytes": int(stat.st_size),
                "allocated_bytes": int(stat.st_blocks) * 512,
                "source_release_commit": "b" * 40,
                "semantic_hash": "c" * 64,
                "eligible": True,
            }
        )
    checkpoint = {"checkpoint_id": CHECKPOINT, "release_sha": RELEASE, "semantic_hash": "c" * 64}
    monkeypatch.setattr(reclamation, "load_verified_checkpoint", lambda *_args, **_kwargs: checkpoint)
    monkeypatch.setattr(
        reclamation,
        "preflight_sealed_epoch_reclamation",
        lambda *_args, **_kwargs: {
            "reclaimable": True,
            "eligible_candidate": candidates[0],
            "eligible_candidates": candidates,
            "blockers": [],
        },
    )
    usage = iter(
        [DiskUsage(100_000, 90_000, 10_000), DiskUsage(100_000, 80_000, 20_000)]
    )
    monkeypatch.setattr(reclamation.shutil, "disk_usage", lambda _path: next(usage))

    result = reclamation.execute_sealed_epoch_reclamation(
        active,
        expected_release_sha=RELEASE,
        approved_checkpoint_id=CHECKPOINT,
    )

    assert result["deleted_candidate_count"] == 2
    assert result["filesystem_free_bytes_delta"] == 10_000
    assert all(not Path(item["path"]).exists() for item in candidates)


def test_batch_reclamation_recovers_after_first_unlink_and_remains_idempotent(
    tmp_path: Path, monkeypatch
) -> None:
    active = tmp_path / "active.sqlite3"
    active.write_bytes(b"active")
    candidates = []
    for number in range(2):
        directory = tmp_path / rollover.SEALED_EPOCH_DIR / f"sealed-{number}"
        directory.mkdir(parents=True)
        candidate = directory / active.name
        candidate.write_bytes(("sealed" + str(number)).encode())
        stat = candidate.stat()
        candidates.append(
            {
                "path": str(candidate.resolve()),
                "device": int(stat.st_dev),
                "inode": int(stat.st_ino),
                "size_bytes": int(stat.st_size),
                "allocated_bytes": int(stat.st_blocks) * 512,
                "source_release_commit": "b" * 40,
                "semantic_hash": "c" * 64,
                "eligible": True,
            }
        )
    checkpoint = {
        "checkpoint_id": CHECKPOINT,
        "release_sha": RELEASE,
        "semantic_hash": "c" * 64,
    }
    monkeypatch.setattr(reclamation, "load_verified_checkpoint", lambda *_args, **_kwargs: checkpoint)
    monkeypatch.setattr(
        reclamation,
        "preflight_sealed_epoch_reclamation",
        lambda *_args, **_kwargs: {
            "reclaimable": True,
            "eligible_candidate": candidates[0],
            "eligible_candidates": candidates,
            "blockers": [],
        },
    )
    monkeypatch.setattr(
        reclamation.shutil,
        "disk_usage",
        lambda _path: DiskUsage(100_000, 90_000, 10_000),
    )
    original_write = reclamation._write_receipt
    interrupted = False

    def interrupt_after_first_unlink(path, payload):
        nonlocal interrupted
        result = original_write(path, payload)
        if len(payload.get("deleted_candidate_paths") or ()) == 1 and not interrupted:
            interrupted = True
            raise RuntimeError("synthetic interruption after first unlink")
        return result

    monkeypatch.setattr(reclamation, "_write_receipt", interrupt_after_first_unlink)
    with pytest.raises(RuntimeError, match="synthetic interruption"):
        reclamation.execute_sealed_epoch_reclamation(
            active,
            expected_release_sha=RELEASE,
            approved_checkpoint_id=CHECKPOINT,
        )
    assert not Path(candidates[0]["path"]).exists()
    assert Path(candidates[1]["path"]).exists()

    monkeypatch.setattr(reclamation, "_write_receipt", original_write)
    recovered = reclamation.execute_sealed_epoch_reclamation(
        active,
        expected_release_sha=RELEASE,
        approved_checkpoint_id=CHECKPOINT,
    )
    assert recovered["status"] == "complete"
    assert recovered["deleted_candidate_count"] == 2
    assert set(recovered["deleted_candidate_paths"]) == {
        str(Path(item["path"]).resolve(strict=False)) for item in candidates
    }
    assert recovered["recovered_missing_candidate_paths"] == [
        str(Path(candidates[0]["path"]).resolve(strict=False))
    ]
    assert all(not Path(item["path"]).exists() for item in candidates)

    replay = reclamation.execute_sealed_epoch_reclamation(
        active,
        expected_release_sha=RELEASE,
        approved_checkpoint_id=CHECKPOINT,
    )
    assert replay["idempotent_replay"] is True
    assert replay["deleted_candidate_count"] == 2
