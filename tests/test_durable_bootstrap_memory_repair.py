from __future__ import annotations

from datetime import datetime, timezone

import pytest

from solana_roi import durable_bootstrap_memory_repair as repair
from solana_roi.durable_engine import DurablePaperTradingEngine
from solana_roi.observation_store import ObservationEventStore


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
    monkeypatch.setattr(
        repair,
        "_cgroup_memory",
        lambda: {"current_bytes": 100, "max_bytes": 1_000, "headroom_bytes": 900, "fraction": 0.1},
    )
    monkeypatch.setattr(repair, "_release_sqlite_file_cache", lambda path: releases.append(str(path)) or True)

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
    monkeypatch.setattr(
        repair,
        "_cgroup_memory",
        lambda: {"current_bytes": 100, "max_bytes": 1_000, "headroom_bytes": 900, "fraction": 0.1},
    )

    assert repair._bounded_verify_engine_snapshot(engine) == (False, 0, None)
    store.close()


def test_raw_cgroup_guard_reclaims_before_critical_boundary(tmp_path, monkeypatch):
    source = tmp_path / "state.sqlite3"
    source.write_bytes(b"sqlite")
    states = iter(
        [
            {"current_bytes": 1_800, "max_bytes": 2_000, "headroom_bytes": 200, "fraction": 0.90},
            {"current_bytes": 1_000, "max_bytes": 2_000, "headroom_bytes": 1_000, "fraction": 0.50},
        ]
    )
    releases: list[str] = []
    monkeypatch.setattr(repair, "RECLAIM_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(repair, "_cgroup_memory", lambda: next(states))
    monkeypatch.setattr(repair, "_release_sqlite_file_cache", lambda path: releases.append(str(path)) or True)

    result = repair._guard_raw_cgroup(source)

    assert result["fraction"] == 0.50
    assert releases == [str(source)]


def test_raw_cgroup_guard_fails_closed_if_reclaim_cannot_restore_headroom(tmp_path, monkeypatch):
    source = tmp_path / "state.sqlite3"
    source.write_bytes(b"sqlite")
    monkeypatch.setattr(repair, "RECLAIM_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(
        repair,
        "_cgroup_memory",
        lambda: {"current_bytes": 1_950, "max_bytes": 2_000, "headroom_bytes": 50, "fraction": 0.975},
    )
    monkeypatch.setattr(repair, "_release_sqlite_file_cache", lambda path: True)

    with pytest.raises(MemoryError, match="raw cgroup memory pressure"):
        repair._guard_raw_cgroup(source)


def test_sqlite_cache_release_includes_wal_and_shm(tmp_path, monkeypatch):
    source = tmp_path / "state.sqlite3"
    seen: list[str] = []
    monkeypatch.setattr(repair, "_advise_dontneed", lambda path: seen.append(str(path)) or True)

    assert repair._release_sqlite_file_cache(source) is True
    assert seen == [str(source), str(source) + "-wal", str(source) + "-shm"]


def test_install_patches_only_read_paths_and_preserves_authority_contract():
    repair.install_durable_bootstrap_memory_repair()

    from solana_roi import certification_logical_bootstrap as logical
    from solana_roi import certification_service_split as split

    assert getattr(DurablePaperTradingEngine._verify_engine_snapshot, "_roi_durable_bootstrap_memory_bounded", False)
    assert getattr(logical._pinned_reader, "_roi_durable_bootstrap_memory_bounded", False)
    assert getattr(split._drop_file_cache, "_roi_sqlite_sidecar_cache_release", False)
    status = repair.status()
    assert status["full_hash_chain_verification_preserved"] is True
    assert status["logical_bootstrap_keyset_semantics_preserved"] is True
    assert status["canonical_evidence_reset"] is False
    assert status["strategy_thresholds_changed"] is False
    assert status["certification_thresholds_changed"] is False
    assert status["continuity_semantics_changed"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
