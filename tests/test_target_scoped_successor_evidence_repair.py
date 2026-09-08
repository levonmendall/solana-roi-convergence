from __future__ import annotations

from datetime import datetime, timedelta, timezone

from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi import same_release_continuity_epoch_repair as successor
from solana_roi import strategy_relevant_continuity as continuity
from solana_roi import target_scoped_successor_evidence_repair as target_repair
from solana_roi import target_stream_fanout as fanout
from solana_roi.direct_solana import DirectSolanaJournal, WatchTarget
from solana_roi.observation_store import ObservationEventStore


successor.install_same_release_continuity_epoch_repair()
target_repair.install_target_scoped_successor_evidence_repair()


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _plane(tmp_path):
    store = ObservationEventStore(tmp_path / "target-scoped-successor.sqlite3")

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


def _ensure_checkpoint_schema(plane) -> None:
    with plane.store._lock, plane.store.db:
        plane.store.db.execute(
            "CREATE TABLE IF NOT EXISTS direct_solana_strategy_poll_checkpoint ("
            "release_commit TEXT NOT NULL,target_key TEXT NOT NULL,cursor_slot INTEGER NOT NULL,"
            "ws_gap_generation INTEGER NOT NULL,last_success_at TEXT NOT NULL,updated_at TEXT NOT NULL,"
            "paper_only INTEGER NOT NULL,live_money_authority INTEGER NOT NULL,"
            "PRIMARY KEY(release_commit,target_key))"
        )


def _set_evidence(
    plane,
    *,
    release: str,
    ws_times: dict[str, datetime],
    poll_time: datetime,
) -> None:
    strategy_targets = [target for target in plane.watch_targets if target.kind == "scout"]
    keys = {continuity._target_key(target) for target in strategy_targets}
    _lock, provider_targets, _events, states = fanout._state_maps(plane)
    provider_targets["provider-a"] = set(keys)
    provider_targets[live_poll.POLL_PROVIDER_NAME] = set(keys)
    states["provider-a"] = {
        continuity._target_key(target): {
            "connected": True,
            "kind": "scout",
            "last_change_at": _iso(ws_times[target.address]),
        }
        for target in strategy_targets
    }

    poll_state = live_poll._poll_state(plane)
    _ensure_checkpoint_schema(plane)
    with plane.store._lock, plane.store.db:
        for index, key in enumerate(sorted(keys), start=1):
            poll_state[key] = {
                "connected": True,
                "baseline_established": True,
                "last_success_at": _iso(poll_time),
            }
            plane.store.db.execute(
                "INSERT INTO direct_solana_strategy_poll_checkpoint("
                "release_commit,target_key,cursor_slot,ws_gap_generation,last_success_at,updated_at,"
                "paper_only,live_money_authority) VALUES (?,?,?,?,?,?,1,0) "
                "ON CONFLICT(release_commit,target_key) DO UPDATE SET "
                "last_success_at=excluded.last_success_at,updated_at=excluded.updated_at",
                (release, key, 100 + index, 0, _iso(poll_time), _iso(poll_time)),
            )


def _rows(plane, release: str):
    with plane.store._lock:
        return plane.store.db.execute(
            "SELECT epoch_id,generation,state,predecessor_epoch_id FROM "
            "direct_solana_strategy_continuity_epoch_v2 WHERE release_commit=? ORDER BY generation",
            (release,),
        ).fetchall()


def test_unaffected_scout_does_not_need_artificial_reconnect(tmp_path, monkeypatch):
    release = "target-scoped-unaffected-test"
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", release)
    plane = _plane(tmp_path)
    t0 = datetime(2026, 9, 8, 1, 0, tzinfo=timezone.utc)
    _set_evidence(
        plane,
        release=release,
        ws_times={"scout-a": t0, "scout-b": t0},
        poll_time=t0,
    )
    assert continuity._start_strategy_epoch_if_ready(plane) is True

    gap = t0 + timedelta(seconds=10)
    assert lease._latch_irrecoverable_generation_once(
        plane, plane.watch_targets[0], 1, _iso(gap)
    ) is True

    # Scout A proves a real post-gap reconnect. Scout B never lost coverage and
    # therefore retains its original connection timestamp. Both poll checkpoints
    # are freshly proven after the failure.
    fresh = gap + timedelta(seconds=4)
    _set_evidence(
        plane,
        release=release,
        ws_times={"scout-a": fresh, "scout-b": t0},
        poll_time=fresh,
    )
    assert continuity._start_strategy_epoch_if_ready(plane) is True
    rows = _rows(plane, release)
    assert [int(row["generation"]) for row in rows] == [1, 2]
    assert [str(row["state"]) for row in rows] == ["failed", "active"]
    assert plane.journal.status()["unresolved_gap"] is False


def test_each_gapped_scout_must_reconnect_after_its_own_latest_gap(tmp_path, monkeypatch):
    release = "target-scoped-mixed-gap-test"
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", release)
    plane = _plane(tmp_path)
    t0 = datetime(2026, 9, 8, 2, 0, tzinfo=timezone.utc)
    _set_evidence(
        plane,
        release=release,
        ws_times={"scout-a": t0, "scout-b": t0},
        poll_time=t0,
    )
    assert continuity._start_strategy_epoch_if_ready(plane) is True

    gap_a = t0 + timedelta(seconds=10)
    assert lease._latch_irrecoverable_generation_once(
        plane, plane.watch_targets[0], 11, _iso(gap_a)
    ) is True
    reconnect_a = gap_a + timedelta(seconds=2)

    # While the epoch is already failed, a separate scout records a later real
    # gap. This later gap must not move scout A's reconnect requirement forward.
    gap_b = t0 + timedelta(seconds=20)
    assert lease._latch_irrecoverable_generation_once(
        plane, plane.watch_targets[1], 12, _iso(gap_b)
    ) is True
    reconnect_b = gap_b + timedelta(seconds=2)
    _set_evidence(
        plane,
        release=release,
        ws_times={"scout-a": reconnect_a, "scout-b": reconnect_b},
        poll_time=reconnect_b,
    )
    assert continuity._start_strategy_epoch_if_ready(plane) is True
    rows = _rows(plane, release)
    assert [int(row["generation"]) for row in rows] == [1, 2]
    assert [str(row["state"]) for row in rows] == ["failed", "active"]


def test_gapped_scout_stale_ws_evidence_stays_failed_closed(tmp_path, monkeypatch):
    release = "target-scoped-stale-test"
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", release)
    plane = _plane(tmp_path)
    t0 = datetime(2026, 9, 8, 3, 0, tzinfo=timezone.utc)
    _set_evidence(
        plane,
        release=release,
        ws_times={"scout-a": t0, "scout-b": t0},
        poll_time=t0,
    )
    assert continuity._start_strategy_epoch_if_ready(plane) is True

    gap = t0 + timedelta(seconds=10)
    assert lease._latch_irrecoverable_generation_once(
        plane, plane.watch_targets[0], 21, _iso(gap)
    ) is True
    fresh_poll = gap + timedelta(seconds=5)
    _set_evidence(
        plane,
        release=release,
        ws_times={"scout-a": gap - timedelta(seconds=1), "scout-b": t0},
        poll_time=fresh_poll,
    )
    assert continuity._start_strategy_epoch_if_ready(plane) is False
    rows = _rows(plane, release)
    assert len(rows) == 1
    assert str(rows[0]["state"]) == "failed"
    assert plane.journal.status()["unresolved_gap"] is True

    _set_evidence(
        plane,
        release=release,
        ws_times={"scout-a": gap + timedelta(seconds=6), "scout-b": t0},
        poll_time=gap + timedelta(seconds=6),
    )
    assert continuity._start_strategy_epoch_if_ready(plane) is True
