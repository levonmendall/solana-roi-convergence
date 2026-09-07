from __future__ import annotations

from datetime import datetime, timedelta, timezone

from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi import same_release_continuity_epoch_repair as repair
from solana_roi import strategy_relevant_continuity as continuity
from solana_roi import target_stream_fanout as fanout
from solana_roi.direct_solana import DirectSolanaJournal, WatchTarget
from solana_roi.observation_store import ObservationEventStore


repair.install_same_release_continuity_epoch_repair()


def _plane(tmp_path, store=None):
    store = store or ObservationEventStore(tmp_path / "same-release-continuity.sqlite3")

    class Plane:
        watch_targets = (
            WatchTarget("scout", "scout-a", None),
            WatchTarget("scout", "scout-b", None),
            WatchTarget("program", "program-a", "PUMP_AMM"),
        )

    plane = Plane()
    plane.store = store
    plane.journal = DirectSolanaJournal(store)
    return plane


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _set_strategy_evidence(plane, *, release: str, at: datetime) -> None:
    _lock, provider_targets, _events, states = fanout._state_maps(plane)
    keys = {
        continuity._target_key(target)
        for target in plane.watch_targets
        if target.kind == "scout"
    }
    provider_targets["provider-a"] = set(keys)
    provider_targets[live_poll.POLL_PROVIDER_NAME] = set(keys)
    states["provider-a"] = {
        key: {
            "connected": True,
            "kind": "scout",
            "last_change_at": _iso(at),
        }
        for key in keys
    }

    poll_state = live_poll._poll_state(plane)
    for key in keys:
        poll_state[key] = {
            "connected": True,
            "baseline_established": True,
            "last_success_at": _iso(at),
        }

    with plane.store._lock, plane.store.db:
        plane.store.db.execute(
            "CREATE TABLE IF NOT EXISTS direct_solana_strategy_poll_checkpoint ("
            "release_commit TEXT NOT NULL,target_key TEXT NOT NULL,cursor_slot INTEGER NOT NULL,"
            "ws_gap_generation INTEGER NOT NULL,last_success_at TEXT NOT NULL,updated_at TEXT NOT NULL,"
            "paper_only INTEGER NOT NULL,live_money_authority INTEGER NOT NULL,"
            "PRIMARY KEY(release_commit,target_key))"
        )
        for index, key in enumerate(sorted(keys), start=1):
            plane.store.db.execute(
                "INSERT INTO direct_solana_strategy_poll_checkpoint("
                "release_commit,target_key,cursor_slot,ws_gap_generation,last_success_at,updated_at,"
                "paper_only,live_money_authority) VALUES (?,?,?,?,?,?,1,0) "
                "ON CONFLICT(release_commit,target_key) DO UPDATE SET "
                "cursor_slot=excluded.cursor_slot,ws_gap_generation=excluded.ws_gap_generation,"
                "last_success_at=excluded.last_success_at,updated_at=excluded.updated_at",
                (
                    release,
                    key,
                    100 + index,
                    0,
                    _iso(at),
                    _iso(at),
                ),
            )


def _rows(plane, release: str):
    with plane.store._lock:
        return plane.store.db.execute(
            "SELECT epoch_id,generation,state,started_at,failed_at,failure_error,"
            "predecessor_epoch_id,evidence_floor_at,websocket_evidence_at,poll_evidence_at "
            "FROM direct_solana_strategy_continuity_epoch_v2 "
            "WHERE release_commit=? ORDER BY generation",
            (release,),
        ).fetchall()


def test_same_release_failed_epoch_stays_historical_and_fresh_evidence_starts_successor(
    tmp_path, monkeypatch
):
    release = "same-release-successor-test"
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", release)
    plane = _plane(tmp_path)

    t0 = datetime(2026, 9, 7, 20, 0, tzinfo=timezone.utc)
    _set_strategy_evidence(plane, release=release, at=t0)
    assert continuity._start_strategy_epoch_if_ready(plane) is True

    initial = _rows(plane, release)
    assert len(initial) == 1
    first_epoch_id = int(initial[0]["epoch_id"])
    assert int(initial[0]["generation"]) == 1
    assert initial[0]["state"] == "active"

    gap_started = t0 + timedelta(seconds=10)
    scout = plane.watch_targets[0]
    assert lease._latch_irrecoverable_generation_once(
        plane,
        scout,
        1,
        _iso(gap_started),
    ) is True

    failed = _rows(plane, release)
    assert len(failed) == 1
    assert int(failed[0]["epoch_id"]) == first_epoch_id
    assert failed[0]["state"] == "failed"
    failed_at = str(failed[0]["failed_at"])
    failure_error = str(failed[0]["failure_error"])
    assert plane.journal.status()["unresolved_gap"] is True

    # Current coverage plus stale pre-gap evidence is intentionally insufficient.
    assert continuity._start_strategy_epoch_if_ready(plane) is False
    assert len(_rows(plane, release)) == 1
    assert plane.journal.status()["unresolved_gap"] is True

    fresh_at = gap_started + timedelta(seconds=5)
    _set_strategy_evidence(plane, release=release, at=fresh_at)
    assert continuity._start_strategy_epoch_if_ready(plane) is True

    recovered = _rows(plane, release)
    assert len(recovered) == 2
    assert recovered[0]["state"] == "failed"
    assert str(recovered[0]["failed_at"]) == failed_at
    assert str(recovered[0]["failure_error"]) == failure_error
    assert int(recovered[1]["generation"]) == 2
    assert recovered[1]["state"] == "active"
    assert int(recovered[1]["predecessor_epoch_id"]) == first_epoch_id
    assert plane.journal.status()["unresolved_gap"] is False

    # Re-evaluation is idempotent and never rewrites the terminal historical row.
    assert continuity._start_strategy_epoch_if_ready(plane) is True
    stable = _rows(plane, release)
    assert len(stable) == 2
    assert str(stable[0]["failed_at"]) == failed_at
    assert str(stable[0]["failure_error"]) == failure_error


def test_same_release_restart_does_not_clear_gap_and_repeated_cycles_create_lineage(
    tmp_path, monkeypatch
):
    release = "same-release-restart-test"
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", release)
    plane = _plane(tmp_path)

    t0 = datetime(2026, 9, 7, 21, 0, tzinfo=timezone.utc)
    _set_strategy_evidence(plane, release=release, at=t0)
    assert continuity._start_strategy_epoch_if_ready(plane) is True

    first_gap = t0 + timedelta(seconds=10)
    assert lease._latch_irrecoverable_generation_once(
        plane,
        plane.watch_targets[0],
        11,
        _iso(first_gap),
    ) is True

    # A same-release process restart reconstructs healthy coverage but carries only
    # stale evidence. It must remain failed closed.
    restarted = _plane(tmp_path, store=plane.store)
    _set_strategy_evidence(
        restarted,
        release=release,
        at=first_gap - timedelta(seconds=1),
    )
    assert continuity._start_strategy_epoch_if_ready(restarted) is False
    assert restarted.journal.status()["unresolved_gap"] is True
    rows = _rows(restarted, release)
    assert len(rows) == 1
    assert rows[0]["state"] == "failed"

    fresh_one = first_gap + timedelta(seconds=3)
    _set_strategy_evidence(restarted, release=release, at=fresh_one)
    assert continuity._start_strategy_epoch_if_ready(restarted) is True
    rows = _rows(restarted, release)
    assert [int(row["generation"]) for row in rows] == [1, 2]
    assert [str(row["state"]) for row in rows] == ["failed", "active"]

    second_gap = fresh_one + timedelta(seconds=10)
    assert lease._latch_irrecoverable_generation_once(
        restarted,
        restarted.watch_targets[1],
        12,
        _iso(second_gap),
    ) is True
    assert continuity._start_strategy_epoch_if_ready(restarted) is False

    fresh_two = second_gap + timedelta(seconds=3)
    _set_strategy_evidence(restarted, release=release, at=fresh_two)
    assert continuity._start_strategy_epoch_if_ready(restarted) is True
    rows = _rows(restarted, release)
    assert [int(row["generation"]) for row in rows] == [1, 2, 3]
    assert [str(row["state"]) for row in rows] == ["failed", "failed", "active"]
    assert int(rows[1]["predecessor_epoch_id"]) == int(rows[0]["epoch_id"])
    assert int(rows[2]["predecessor_epoch_id"]) == int(rows[1]["epoch_id"])

    with restarted.store._lock:
        active_count = restarted.store.db.execute(
            "SELECT COUNT(*) AS n FROM direct_solana_strategy_continuity_epoch_v2 "
            "WHERE release_commit=? AND state='active'",
            (release,),
        ).fetchone()["n"]
        gap_events = restarted.store.db.execute(
            "SELECT COUNT(*) AS n FROM direct_solana_strategy_continuity_gap_event "
            "WHERE release_commit=?",
            (release,),
        ).fetchone()["n"]
    assert int(active_count) == 1
    assert int(gap_events) == 2
