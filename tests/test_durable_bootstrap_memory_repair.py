from __future__ import annotations

from datetime import datetime, timezone

import pytest

from solana_roi import durable_bootstrap_memory_repair as repair
from solana_roi.durable_engine import DurablePaperTradingEngine
from solana_roi.observation_store import ObservationEventStore

GIB = 1024 * 1024 * 1024
MIB = 1024 * 1024


def _healthy_memory():
    return {
        "current_bytes": 100 * MIB,
        "max_bytes": 2 * GIB,
        "headroom_bytes": 2 * GIB - 100 * MIB,
        "fraction": (100 * MIB) / (2 * GIB),
    }


def _store_with_events(tmp_path, count: int = 9) -> ObservationEventStore:
    store = ObservationEventStore(tmp_path / "events.sqlite3")
    now = datetime.now(timezone.utc).isoformat()
    for index in range(count):
        store.append("diagnostic", now, {"index": index})
    return store


def test_bounded_restore_preserves_full_hash_verification_and_reclaims_during_scan(tmp_path, monkeypatch):
    store = _store_with_events(tmp_path)
    engine = DurablePaperTradingEngine.__new__(DurablePaperTradingEngine)
    engine.store = store
    releases: list[str] = []

    monkeypatch.setattr(repair, "VERIFY_CACHE_RELEASE_ROWS", 2)
    monkeypatch.setattr(repair, "RECLAIM_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(repair, "_cgroup_memory", _healthy_memory)
    monkeypatch.setattr(repair, "_release_sqlite_file_cache", lambda path: releases.append(str(path)) or True)
    monkeypatch.setattr(repair, "_trim_process_heap", lambda: True)

    verified, through, latest_engine = repair._bounded_verify_engine_snapshot(engine)

    assert verified is True
    assert through == 9
    assert latest_engine is None
    assert len(releases) >= 5
    store.close()


def test_bounded_restore_still_fails_on_hash_chain_corruption(tmp_path, monkeypatch):
    store = _store_with_events(tmp_path, 3)
    with store._lock, store.db:
        store.db.execute("UPDATE events SET previous_hash='corrupt' WHERE id=2")
    engine = DurablePaperTradingEngine.__new__(DurablePaperTradingEngine)
    engine.store = store

    monkeypatch.setattr(repair, "RECLAIM_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(repair, "_cgroup_memory", _healthy_memory)
    monkeypatch.setattr(repair, "_trim_process_heap", lambda: True)

    assert repair._bounded_verify_engine_snapshot(engine) == (False, 0, None)
    store.close()


def test_raw_cgroup_guard_reclaims_clean_cache_before_checkpoint(tmp_path, monkeypatch):
    source = tmp_path / "state.sqlite3"
    source.write_bytes(b"sqlite")
    states = iter(
        [
            {
                "current_bytes": 1800 * MIB,
                "max_bytes": 2 * GIB,
                "headroom_bytes": 248 * MIB,
                "fraction": (1800 * MIB) / (2 * GIB),
                "file_dirty_bytes": 0,
                "file_writeback_bytes": 0,
            },
            {
                "current_bytes": 1000 * MIB,
                "max_bytes": 2 * GIB,
                "headroom_bytes": 1048 * MIB,
                "fraction": (1000 * MIB) / (2 * GIB),
                "file_dirty_bytes": 0,
                "file_writeback_bytes": 0,
            },
        ]
    )
    order: list[str] = []
    monkeypatch.setattr(repair, "RECLAIM_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(repair, "_cgroup_memory", lambda: next(states))
    monkeypatch.setattr(repair, "_release_sqlite_file_cache", lambda path: order.append("cache") or True)
    monkeypatch.setattr(repair, "_trim_process_heap", lambda: order.append("heap") or True)
    monkeypatch.setattr(repair, "_request_cgroup_file_reclaim", lambda state: order.append("cgroup") or True)
    monkeypatch.setattr(
        repair,
        "_passive_wal_checkpoint",
        lambda path: (_ for _ in ()).throw(AssertionError("checkpoint must be skipped")),
    )
    monkeypatch.setattr(repair, "_emit_reclaim_telemetry", lambda *args, **kwargs: None)

    result = repair._guard_raw_cgroup(source)

    assert result["fraction"] < 0.50
    assert order == ["cache", "heap", "cgroup"]


def test_raw_cgroup_guard_retries_reclaim_until_headroom_returns(tmp_path, monkeypatch):
    source = tmp_path / "state.sqlite3"
    source.write_bytes(b"sqlite")
    states = iter(
        [
            {
                "current_bytes": 1980 * MIB,
                "max_bytes": 2 * GIB,
                "headroom_bytes": 68 * MIB,
                "fraction": 1980 / 2048,
            },
            {
                "current_bytes": 1900 * MIB,
                "max_bytes": 2 * GIB,
                "headroom_bytes": 148 * MIB,
                "fraction": 1900 / 2048,
            },
            {
                "current_bytes": 1400 * MIB,
                "max_bytes": 2 * GIB,
                "headroom_bytes": 648 * MIB,
                "fraction": 1400 / 2048,
            },
        ]
    )
    calls = {"cache": 0, "heap": 0, "cgroup": 0, "checkpoint": 0}
    monkeypatch.setattr(repair, "RECLAIM_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(repair, "_cgroup_memory", lambda: next(states))
    monkeypatch.setattr(
        repair,
        "_release_sqlite_file_cache",
        lambda path: calls.__setitem__("cache", calls["cache"] + 1) or True,
    )
    monkeypatch.setattr(
        repair,
        "_trim_process_heap",
        lambda: calls.__setitem__("heap", calls["heap"] + 1) or True,
    )
    monkeypatch.setattr(
        repair,
        "_request_cgroup_file_reclaim",
        lambda state: calls.__setitem__("cgroup", calls["cgroup"] + 1) or True,
    )
    monkeypatch.setattr(
        repair,
        "_passive_wal_checkpoint",
        lambda path: calls.__setitem__("checkpoint", calls["checkpoint"] + 1),
    )
    monkeypatch.setattr(repair, "_emit_reclaim_telemetry", lambda *args, **kwargs: None)

    result = repair._guard_raw_cgroup(source)

    assert result["current_bytes"] == 1400 * MIB
    assert calls == {"cache": 2, "heap": 2, "cgroup": 2, "checkpoint": 0}


def test_raw_cgroup_guard_skips_checkpoint_for_small_wal_and_negligible_writeback(tmp_path, monkeypatch):
    source = tmp_path / "state.sqlite3"
    source.write_bytes(b"sqlite")
    (tmp_path / "state.sqlite3-wal").write_bytes(b"x" * 4096)
    state = {
        "current_bytes": 1750 * MIB,
        "max_bytes": 2 * GIB,
        "headroom_bytes": 298 * MIB,
        "fraction": 1750 / 2048,
        "file_dirty_bytes": 1 * MIB,
        "file_writeback_bytes": 1 * MIB,
    }
    calls = {"checkpoint": 0}
    monkeypatch.setattr(repair, "RECLAIM_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(repair, "_cgroup_memory", lambda: dict(state))
    monkeypatch.setattr(repair, "_release_sqlite_file_cache", lambda path: True)
    monkeypatch.setattr(repair, "_trim_process_heap", lambda: True)
    monkeypatch.setattr(repair, "_request_cgroup_file_reclaim", lambda state: False)
    monkeypatch.setattr(
        repair,
        "_passive_wal_checkpoint",
        lambda path: calls.__setitem__("checkpoint", calls["checkpoint"] + 1),
    )
    monkeypatch.setattr(repair, "_emit_reclaim_telemetry", lambda *args, **kwargs: None)

    result = repair._guard_raw_cgroup(source)

    assert result["fraction"] == 1750 / 2048
    assert calls["checkpoint"] == 0


def test_raw_cgroup_guard_flushes_dirty_cache_without_checkpoint_for_small_wal(tmp_path, monkeypatch):
    source = tmp_path / "state.sqlite3"
    source.write_bytes(b"sqlite")
    states = iter(
        [
            {
                "current_bytes": 1900 * MIB,
                "max_bytes": 2 * GIB,
                "headroom_bytes": 148 * MIB,
                "fraction": 1900 / 2048,
                "file_dirty_bytes": 96 * MIB,
                "file_writeback_bytes": 0,
            },
            {
                "current_bytes": 1850 * MIB,
                "max_bytes": 2 * GIB,
                "headroom_bytes": 198 * MIB,
                "fraction": 1850 / 2048,
                "file_dirty_bytes": 96 * MIB,
                "file_writeback_bytes": 8 * MIB,
            },
            {
                "current_bytes": 1300 * MIB,
                "max_bytes": 2 * GIB,
                "headroom_bytes": 748 * MIB,
                "fraction": 1300 / 2048,
                "file_dirty_bytes": 0,
                "file_writeback_bytes": 0,
            },
        ]
    )
    order: list[str] = []

    monkeypatch.setattr(repair, "RECLAIM_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(repair, "WRITEBACK_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(repair, "_cgroup_memory", lambda: next(states))
    monkeypatch.setattr(repair, "_wal_size_bytes", lambda path: 4 * MIB)
    monkeypatch.setattr(repair, "_release_sqlite_file_cache", lambda path: order.append("cache") or True)
    monkeypatch.setattr(repair, "_trim_process_heap", lambda: order.append("heap") or True)
    monkeypatch.setattr(repair, "_request_cgroup_file_reclaim", lambda state: order.append("cgroup") or False)
    monkeypatch.setattr(
        repair,
        "_passive_wal_checkpoint",
        lambda path: (_ for _ in ()).throw(AssertionError("small WAL must not checkpoint")),
    )
    monkeypatch.setattr(repair, "_sync_sqlite_dirty_pages", lambda path: order.append("writeback") or True)
    monkeypatch.setattr(repair, "_emit_reclaim_telemetry", lambda *args, **kwargs: None)

    result = repair._guard_raw_cgroup(source)

    assert result["current_bytes"] == 1300 * MIB
    assert order == ["cache", "heap", "cgroup", "writeback", "cache", "cgroup"]


def test_raw_cgroup_guard_runs_one_checkpoint_when_wal_is_large(tmp_path, monkeypatch):
    source = tmp_path / "state.sqlite3"
    source.write_bytes(b"sqlite")
    state_high = {
        "current_bytes": 1800 * MIB,
        "max_bytes": 2 * GIB,
        "headroom_bytes": 248 * MIB,
        "fraction": 1800 / 2048,
        "file_dirty_bytes": 0,
        "file_writeback_bytes": 0,
    }
    states = iter([dict(state_high), dict(state_high), {**_healthy_memory(), "file_dirty_bytes": 0, "file_writeback_bytes": 0}])
    calls = {"checkpoint": 0}

    monkeypatch.setattr(repair, "RECLAIM_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(repair, "WRITEBACK_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(repair, "_cgroup_memory", lambda: next(states))
    monkeypatch.setattr(repair, "_wal_size_bytes", lambda path: repair.WAL_CHECKPOINT_TRIGGER_BYTES)
    monkeypatch.setattr(repair, "_release_sqlite_file_cache", lambda path: True)
    monkeypatch.setattr(repair, "_trim_process_heap", lambda: True)
    monkeypatch.setattr(repair, "_request_cgroup_file_reclaim", lambda state: False)
    monkeypatch.setattr(repair, "_sync_sqlite_dirty_pages", lambda path: True)
    monkeypatch.setattr(
        repair,
        "_passive_wal_checkpoint",
        lambda path: calls.__setitem__("checkpoint", calls["checkpoint"] + 1) or {
            "attempted": True,
            "busy": 0,
            "log_frames": 1,
            "checkpointed_frames": 1,
            "error": None,
            "wal_bytes_before": repair.WAL_CHECKPOINT_TRIGGER_BYTES,
            "wal_bytes_after": repair.WAL_CHECKPOINT_TRIGGER_BYTES,
        },
    )
    monkeypatch.setattr(repair, "_emit_reclaim_telemetry", lambda *args, **kwargs: None)

    result = repair._guard_raw_cgroup(source)

    assert result["current_bytes"] == 100 * MIB
    assert calls["checkpoint"] == 1


def test_raw_cgroup_guard_fails_closed_if_reclaim_cannot_restore_headroom(tmp_path, monkeypatch):
    source = tmp_path / "state.sqlite3"
    source.write_bytes(b"sqlite")
    monkeypatch.setattr(repair, "RECLAIM_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(repair, "WRITEBACK_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(
        repair,
        "_cgroup_memory",
        lambda: {
            "current_bytes": 1950 * MIB,
            "max_bytes": 2 * GIB,
            "headroom_bytes": 98 * MIB,
            "fraction": (1950 * MIB) / (2 * GIB),
            "file_dirty_bytes": 0,
            "file_writeback_bytes": 0,
        },
    )
    monkeypatch.setattr(repair, "_release_sqlite_file_cache", lambda path: True)
    monkeypatch.setattr(repair, "_sync_sqlite_dirty_pages", lambda path: True)
    monkeypatch.setattr(repair, "_trim_process_heap", lambda: True)
    monkeypatch.setattr(repair, "_request_cgroup_file_reclaim", lambda state: False)
    monkeypatch.setattr(repair, "_emit_reclaim_telemetry", lambda *args, **kwargs: None)

    with pytest.raises(MemoryError, match="raw cgroup memory pressure"):
        repair._guard_raw_cgroup(source)


def test_cgroup_reclaim_requests_only_file_reclaim_with_zero_swappiness(tmp_path):
    reclaim = tmp_path / "memory.reclaim"
    reclaim.write_text("", encoding="ascii")
    state = {
        "current_bytes": 1900 * MIB,
        "max_bytes": 2 * GIB,
        "headroom_bytes": 148 * MIB,
        "fraction": 1900 / 2048,
    }

    assert repair._request_cgroup_file_reclaim(state, root=tmp_path) is True
    payload = reclaim.read_text(encoding="ascii")
    amount, policy = payload.split()
    assert int(amount) >= repair.MIN_CGROUP_RECLAIM_BYTES
    assert int(amount) <= repair.MAX_CGROUP_RECLAIM_BYTES
    assert policy == "swappiness=0"


def test_sqlite_cache_release_includes_wal_and_shm(tmp_path, monkeypatch):
    source = tmp_path / "state.sqlite3"
    seen: list[str] = []
    monkeypatch.setattr(repair, "_advise_dontneed", lambda path: seen.append(str(path)) or True)

    assert repair._release_sqlite_file_cache(source) is True
    assert seen == [str(source), str(source) + "-wal", str(source) + "-shm"]


def test_dirty_writeback_fsyncs_db_wal_and_shm(tmp_path, monkeypatch):
    source = tmp_path / "state.sqlite3"
    source.write_bytes(b"db")
    (tmp_path / "state.sqlite3-wal").write_bytes(b"wal")
    (tmp_path / "state.sqlite3-shm").write_bytes(b"shm")
    synced: list[int] = []
    monkeypatch.setattr(repair.os, "fdatasync", lambda fd: synced.append(int(fd)))

    assert repair._sync_sqlite_dirty_pages(source) is True
    assert len(synced) == 3


def test_dirty_writeback_gate_counts_kernel_writeback_not_only_dirty_pages():
    state = {
        "file_dirty_bytes": 8 * MIB,
        "file_writeback_bytes": repair.DIRTY_WRITEBACK_TRIGGER_BYTES,
    }
    assert repair._dirty_writeback_needed(state) is True


def test_dirty_writeback_alone_does_not_justify_wal_checkpoint(tmp_path):
    source = tmp_path / "state.sqlite3"
    source.write_bytes(b"db")
    (tmp_path / "state.sqlite3-wal").write_bytes(b"wal")
    state = {
        "file_dirty_bytes": 256 * MIB,
        "file_writeback_bytes": 32 * MIB,
    }

    assert repair._dirty_writeback_needed(state) is True
    assert repair._wal_checkpoint_needed(source, state) is False


def test_critical_fail_closed_boundary_remains_94_percent():
    assert repair.RAW_CRITICAL_FRACTION == 0.94
    assert repair._critical(
        {
            "fraction": 0.94,
            "headroom_bytes": repair.RAW_CRITICAL_RESERVE_BYTES + 1,
        }
    ) is True


def test_install_patches_only_read_paths_and_preserves_authority_contract():
    repair.install_durable_bootstrap_memory_repair()

    from solana_roi import certification_logical_bootstrap as logical
    from solana_roi import certification_service_split as split

    assert getattr(DurablePaperTradingEngine._verify_engine_snapshot, "_roi_durable_bootstrap_memory_bounded", False)
    assert getattr(logical._pinned_reader, "_roi_durable_bootstrap_memory_bounded", False)
    assert getattr(split._drop_file_cache, "_roi_sqlite_sidecar_cache_release", False)
    status = repair.status()
    assert status["repair_version"] == "durable-bootstrap-cgroup-memory-v8-bootstrap-lease-wal-ownership"
    assert status["raw_critical_fraction"] == 0.94
    assert status["full_hash_chain_verification_preserved"] is True
    assert status["logical_bootstrap_keyset_semantics_preserved"] is True
    assert status["cgroup_file_reclaim_best_effort"] is True
    assert status["cgroup_reclaim_swappiness_zero"] is True
    assert status["heap_trim_under_pressure"] is True
    assert status["targeted_sqlite_dirty_writeback"] is True
    assert status["clean_cache_eviction_precedes_checkpoint"] is True
    assert status["dirty_writeback_alone_triggers_checkpoint"] is False
    assert status["passive_wal_checkpoint_gated"] is True
    assert status["bootstrap_active_lease_owns_wal_checkpoint"] is True
    assert status["bootstrap_guard_passive_checkpoint_enabled"] is False
    assert status["wal_checkpoint_max_attempts_per_guard"] == 1
    assert status["page_finalizer_checkpoint_enabled"] is False
    assert status["page_finalizer_clean_cache_release"] is True
    assert status["writeback_changes_logical_state"] is False
    assert status["canonical_evidence_reset"] is False
    assert status["strategy_thresholds_changed"] is False
    assert status["certification_thresholds_changed"] is False
    assert status["continuity_semantics_changed"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
