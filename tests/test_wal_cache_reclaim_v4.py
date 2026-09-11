from __future__ import annotations

import sqlite3

from solana_roi import durable_bootstrap_memory_repair as repair

MIB = 1024 * 1024


def test_passive_wal_checkpoint_reports_committed_frames(tmp_path):
    database = tmp_path / "certification.db"
    writer = sqlite3.connect(database)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0].lower() == "wal"
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE evidence (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        writer.executemany(
            "INSERT INTO evidence(value) VALUES (?)",
            [(f"row-{index}",) for index in range(128)],
        )
        writer.commit()

        result = repair._passive_wal_checkpoint(database)

        assert result["attempted"] is True
        assert result["error"] is None
        assert result["busy"] == 0
        assert isinstance(result["log_frames"], int)
        assert result["log_frames"] >= 1
        assert isinstance(result["checkpointed_frames"], int)
        assert result["checkpointed_frames"] >= 1
        assert isinstance(result["wal_bytes_before"], int)
        assert result["wal_bytes_before"] > 0
        assert isinstance(result["wal_bytes_after"], int)
        assert result["wal_bytes_after"] >= 0
        assert writer.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 128
    finally:
        writer.close()


def test_passive_wal_checkpoint_failure_is_nonfatal(tmp_path, monkeypatch):
    database = tmp_path / "certification.db"
    database.touch()

    def locked_connect(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(repair.sqlite3, "connect", locked_connect)

    result = repair._passive_wal_checkpoint(database)

    assert result["attempted"] is True
    assert result["busy"] is None
    assert result["checkpointed_frames"] is None
    assert result["error"] == "OperationalError: database is locked"
    assert result["wal_bytes_before"] == 0
    assert result["wal_bytes_after"] == 0


def test_cgroup_memory_reports_file_writeback(tmp_path):
    (tmp_path / "memory.current").write_text("1800000000\n", encoding="utf-8")
    (tmp_path / "memory.max").write_text("2147483648\n", encoding="utf-8")
    (tmp_path / "memory.stat").write_text(
        "anon 170000000\n"
        "file 1550000000\n"
        "file_dirty 64000000\n"
        "file_writeback 12000000\n"
        "slab_reclaimable 8000000\n",
        encoding="utf-8",
    )
    (tmp_path / "memory.events").write_text("oom 0\noom_kill 0\n", encoding="utf-8")

    state = repair._cgroup_memory(tmp_path)

    assert state["file_writeback_bytes"] == 12_000_000
    assert state["file_dirty_bytes"] == 64_000_000
    assert state["file_bytes"] == 1_550_000_000
    assert state["anon_bytes"] == 170_000_000


def test_page_finalizer_never_checkpoints_and_releases_clean_cache_first(tmp_path, monkeypatch):
    database = tmp_path / "certification.db"
    database.touch()
    calls: list[str] = []
    states = iter(
        [
            {
                "fraction": 0.90,
                "headroom_bytes": 200 * MIB,
                "file_dirty_bytes": 96 * MIB,
                "file_writeback_bytes": 0,
            },
            {
                "fraction": 0.60,
                "headroom_bytes": 800 * MIB,
                "file_dirty_bytes": 0,
                "file_writeback_bytes": 0,
            },
        ]
    )

    monkeypatch.setattr(
        repair,
        "_passive_wal_checkpoint",
        lambda path: (_ for _ in ()).throw(AssertionError("page finalizer must not checkpoint")),
    )
    monkeypatch.setattr(
        repair,
        "_release_sqlite_file_cache",
        lambda path: calls.append("release") or True,
    )
    monkeypatch.setattr(
        repair,
        "_sync_sqlite_dirty_pages",
        lambda path: calls.append("flush") or True,
    )
    monkeypatch.setattr(repair, "_cgroup_memory", lambda: next(states))
    monkeypatch.setattr(repair, "WRITEBACK_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(repair, "_trim_process_heap", lambda: calls.append("heap") or True)

    assert repair._drop_file_cache_with_sidecars(database) is True
    assert calls == ["release", "flush", "release"]


def test_page_finalizer_clean_cache_release_does_not_write(tmp_path, monkeypatch):
    database = tmp_path / "certification.db"
    database.touch()
    calls: list[str] = []

    monkeypatch.setattr(
        repair,
        "_passive_wal_checkpoint",
        lambda path: (_ for _ in ()).throw(AssertionError("page finalizer must not checkpoint")),
    )
    monkeypatch.setattr(
        repair,
        "_sync_sqlite_dirty_pages",
        lambda path: (_ for _ in ()).throw(AssertionError("clean cache must not be flushed")),
    )
    monkeypatch.setattr(
        repair,
        "_release_sqlite_file_cache",
        lambda path: calls.append("release") or True,
    )
    monkeypatch.setattr(
        repair,
        "_cgroup_memory",
        lambda: {
            "fraction": 0.1,
            "headroom_bytes": 10**9,
            "file_dirty_bytes": 0,
            "file_writeback_bytes": 0,
        },
    )

    assert repair._drop_file_cache_with_sidecars(database) is True
    assert calls == ["release"]


def test_repair_preserves_paper_only_and_fail_closed_boundaries():
    state = repair.status()

    assert state["repair_version"] == "durable-bootstrap-cgroup-memory-v7-page-finalizer-cache-release"
    assert state["logical_bootstrap_page_wide_transaction"] is False
    assert state["passive_wal_checkpoint_under_pressure"] is True
    assert state["passive_wal_checkpoint_gated"] is True
    assert state["clean_cache_eviction_precedes_checkpoint"] is True
    assert state["dirty_writeback_alone_triggers_checkpoint"] is False
    assert state["wal_checkpoint_max_attempts_per_guard"] == 1
    assert state["page_finalizer_checkpoint_enabled"] is False
    assert state["page_finalizer_clean_cache_release"] is True
    assert state["raw_critical_fraction"] == 0.94
    assert state["wal_checkpoint_busy_timeout_ms"] == 0
    assert state["wal_checkpoint_changes_logical_state"] is False
    assert state["writeback_changes_logical_state"] is False
    assert state["strategy_thresholds_changed"] is False
    assert state["certification_thresholds_changed"] is False
    assert state["continuity_semantics_changed"] is False
    assert state["canonical_evidence_reset"] is False
    assert state["paper_only"] is True
    assert state["live_money_authority"] is False
    assert state["signing_available"] is False
    assert state["transaction_submission_available"] is False
