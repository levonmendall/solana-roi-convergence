from __future__ import annotations

import sqlite3
from collections import namedtuple

import pytest

from solana_roi import active_storage_epoch_rollover as rollover
from solana_roi.active_storage import ActiveStorageBudget


DiskUsage = namedtuple("DiskUsage", "total used free")


def _seed_database(path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE truth_probe(id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        connection.execute("INSERT INTO truth_probe(value) VALUES('preserve-me')")
        connection.commit()
    finally:
        connection.close()


def _read_probe(path) -> str:
    connection = sqlite3.connect(path)
    try:
        row = connection.execute("SELECT value FROM truth_probe WHERE id=1").fetchone()
        assert row is not None
        return str(row[0])
    finally:
        connection.close()


def test_rollover_refuses_to_build_without_temporary_disk_headroom(tmp_path, monkeypatch):
    database = tmp_path / "active.sqlite3"
    _seed_database(database)
    original_bytes = database.read_bytes()
    budget = ActiveStorageBudget(
        warning_bytes=1,
        hard_bytes=8 * 1024 * 1024,
        max_wal_bytes=2 * 1024 * 1024,
    )
    required = budget.hard_bytes * 2
    monkeypatch.setattr(
        rollover.shutil,
        "disk_usage",
        lambda _path: DiskUsage(total=required * 4, used=required * 3 + 1, free=required - 1),
    )

    build_called = False

    def unexpected_build(**_kwargs):
        nonlocal build_called
        build_called = True
        raise AssertionError("shadow build must not start without disk headroom")

    monkeypatch.setattr(rollover, "build_shadow_database", unexpected_build)

    with pytest.raises(RuntimeError, match="insufficient temporary disk headroom"):
        rollover.rollover_active_epoch_if_needed(
            database,
            release_sha="test-release",
            budget=budget,
        )

    assert build_called is False
    assert database.read_bytes() == original_bytes
    assert _read_probe(database) == "preserve-me"
    assert not list(tmp_path.glob(".active.sqlite3.epoch-next-*"))


def test_failed_shadow_build_removes_partial_successor_family(tmp_path, monkeypatch):
    database = tmp_path / "active.sqlite3"
    _seed_database(database)
    budget = ActiveStorageBudget(
        warning_bytes=1,
        hard_bytes=8 * 1024 * 1024,
        max_wal_bytes=2 * 1024 * 1024,
    )
    required = budget.hard_bytes * 2
    monkeypatch.setattr(
        rollover.shutil,
        "disk_usage",
        lambda _path: DiskUsage(total=required * 4, used=required, free=required * 3),
    )

    created: list = []

    def failed_build(*, active_path, **_kwargs):
        successor = active_path
        successor.write_bytes(b"partial-successor")
        rollover.Path(str(successor) + "-wal").write_bytes(b"partial-wal")
        rollover.Path(str(successor) + "-shm").write_bytes(b"partial-shm")
        created.append(successor)
        raise RuntimeError("synthetic shadow build failure")

    monkeypatch.setattr(rollover, "build_shadow_database", failed_build)

    with pytest.raises(RuntimeError, match="synthetic shadow build failure"):
        rollover.rollover_active_epoch_if_needed(
            database,
            release_sha="test-release",
            budget=budget,
        )

    assert created
    successor = created[0]
    assert not successor.exists()
    assert not rollover.Path(str(successor) + "-wal").exists()
    assert not rollover.Path(str(successor) + "-shm").exists()
    assert database.exists()
    assert _read_probe(database) == "preserve-me"
