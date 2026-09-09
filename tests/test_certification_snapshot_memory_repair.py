from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from solana_roi import certification_service_split as split
from solana_roi import certification_snapshot_memory_repair as repair


class _Store:
    def __init__(self, path: Path) -> None:
        self.path = path


def _source_db(path: Path, rows: int = 4000) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE evidence (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
        connection.executemany(
            "INSERT INTO evidence(payload) VALUES (?)",
            [("x" * 512,) for _ in range(rows)],
        )
        connection.commit()
    finally:
        connection.close()


def test_bounded_export_preserves_exact_sqlite_snapshot_and_releases_ranges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "canonical.sqlite3"
    snapshot = tmp_path / "snapshot.sqlite3"
    _source_db(source)

    monkeypatch.setattr(split, "_snapshot_max_bytes", lambda: 128 * 1024 * 1024)
    monkeypatch.setattr(split, "_snapshot_free_reserve_bytes", lambda: 1)
    monkeypatch.setattr(split, "_snapshot_pages_per_step", lambda: 64)
    monkeypatch.setattr(split, "_snapshot_step_sleep_seconds", lambda: 0.0)
    monkeypatch.setattr(split, "_snapshot_deadline_seconds", lambda: 30.0)
    monkeypatch.setattr(repair, "_cache_release_interval_bytes", lambda: 256 * 1024)
    monkeypatch.setattr(
        repair,
        "_raw_cgroup_sample",
        lambda: {"current": 256 * 1024 * 1024, "maximum": 2 * 1024 * 1024 * 1024,
                 "headroom": 1792 * 1024 * 1024, "fraction": 0.125},
    )

    releases: list[tuple[Path, int, int, bool]] = []
    original_release = repair._flush_drop_range

    def observe_release(path: Path, offset: int, length: int, *, flush: bool) -> bool:
        releases.append((path, offset, length, flush))
        return original_release(path, offset, length, flush=flush)

    monkeypatch.setattr(repair, "_flush_drop_range", observe_release)

    size, estimated = repair._bounded_snapshot_store_to_file(_Store(source), snapshot)

    assert size == snapshot.stat().st_size
    assert estimated >= size
    with sqlite3.connect(snapshot) as connection:
        assert connection.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 4000
    assert any(path == snapshot and flush and length > 0 for path, _offset, length, flush in releases)
    assert any(path == source and not flush and length > 0 for path, _offset, length, flush in releases)


def test_raw_cgroup_guard_reclaims_then_fails_closed_before_hard_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "canonical.sqlite3"
    snapshot = tmp_path / "snapshot.sqlite3"
    source.write_bytes(b"source")
    snapshot.write_bytes(b"snapshot")

    samples = iter(
        [
            {"current": 1900, "maximum": 2000, "headroom": 100, "fraction": 0.95},
            {"current": 1880, "maximum": 2000, "headroom": 120, "fraction": 0.94},
        ]
    )
    monkeypatch.setattr(repair, "_raw_cgroup_sample", lambda: next(samples))
    monkeypatch.setattr(repair, "_raw_memory_stop_fraction", lambda: 0.86)
    monkeypatch.setattr(repair, "_raw_memory_min_headroom_bytes", lambda: 256)
    reclaimed: list[int] = []
    monkeypatch.setattr(
        repair,
        "_aggressive_cache_release",
        lambda _source, _snapshot, completed: reclaimed.append(completed),
    )
    monkeypatch.setattr(repair.time, "sleep", lambda _seconds: None)

    with pytest.raises(MemoryError, match="raw cgroup memory headroom exhausted"):
        repair._guard_raw_cgroup_headroom(source, snapshot, 1024)

    assert reclaimed == [1024]


def test_raw_cgroup_guard_allows_export_after_cache_reclaim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "canonical.sqlite3"
    snapshot = tmp_path / "snapshot.sqlite3"
    source.write_bytes(b"source")
    snapshot.write_bytes(b"snapshot")

    samples = iter(
        [
            {"current": 1900, "maximum": 2000, "headroom": 100, "fraction": 0.95},
            {"current": 1200, "maximum": 2000, "headroom": 800, "fraction": 0.60},
        ]
    )
    monkeypatch.setattr(repair, "_raw_cgroup_sample", lambda: next(samples))
    monkeypatch.setattr(repair, "_raw_memory_stop_fraction", lambda: 0.86)
    monkeypatch.setattr(repair, "_raw_memory_min_headroom_bytes", lambda: 256)
    reclaimed: list[int] = []
    monkeypatch.setattr(
        repair,
        "_aggressive_cache_release",
        lambda _source, _snapshot, completed: reclaimed.append(completed),
    )
    monkeypatch.setattr(repair.time, "sleep", lambda _seconds: None)

    repair._guard_raw_cgroup_headroom(source, snapshot, 2048)

    assert reclaimed == [2048]


def test_missing_cgroup_telemetry_preserves_non_cgroup_compatibility(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "canonical.sqlite3"
    snapshot = tmp_path / "snapshot.sqlite3"
    source.write_bytes(b"source")
    snapshot.write_bytes(b"snapshot")
    monkeypatch.setattr(
        repair,
        "_raw_cgroup_sample",
        lambda: {"current": None, "maximum": None, "headroom": None, "fraction": None},
    )

    repair._guard_raw_cgroup_headroom(source, snapshot, 0)


def test_installer_patches_split_before_snapshot_routes_mount(monkeypatch: pytest.MonkeyPatch) -> None:
    original_export = split._snapshot_store_to_file
    original_status = split.status
    try:
        repair.install_certification_snapshot_memory_repair()
        assert split._snapshot_store_to_file is repair._bounded_snapshot_store_to_file
        assert getattr(split._snapshot_store_to_file, "_roi_snapshot_cgroup_memory_repair", False)
        payload = split.status()
        boundary = payload["snapshot_cgroup_memory_repair"]
        assert boundary["installed"] is True
        assert boundary["progressive_cache_release"] is True
        assert boundary["raw_cgroup_hard_limit_guard"] is True
        assert boundary["snapshot_consistency_changed"] is False
        assert boundary["certification_thresholds_changed"] is False
        assert boundary["paper_only"] is True
        assert boundary["live_money_authority"] is False
    finally:
        split._snapshot_store_to_file = original_export
        split.status = original_status
