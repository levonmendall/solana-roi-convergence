from __future__ import annotations

import asyncio
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import candidate_certification_hotpath_repair as candidate_hotpath
from solana_roi import candidate_completion_continuity_repair as repair
from solana_roi import candidate_rpc_priority_repair as candidate_priority
from solana_roi import continuity_high_volume_checkpoint_architecture as checkpoint
from solana_roi import continuity_standby_rpc_priority_repair as standby_priority
from solana_roi import forward_evidence_runtime_repair as forward
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi import rpc_workload_governor as governor
from solana_roi.direct_solana import WatchTarget


def _journal() -> SimpleNamespace:
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute(
        "CREATE TABLE direct_solana_hydration_queue ("
        "signature TEXT PRIMARY KEY, slot INTEGER NOT NULL, trigger_received_at TEXT NOT NULL, "
        "source_hint TEXT, priority INTEGER NOT NULL, reason TEXT NOT NULL, status TEXT NOT NULL, "
        "attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT, updated_at TEXT NOT NULL)"
    )
    store = SimpleNamespace(_lock=threading.RLock(), db=db)
    return SimpleNamespace(store=store)


def _insert(
    journal: SimpleNamespace,
    *,
    signature: str,
    trigger: datetime,
    attempts: int = 0,
    updated: datetime | None = None,
) -> None:
    journal.store.db.execute(
        "INSERT INTO direct_solana_hydration_queue("
        "signature, slot, trigger_received_at, source_hint, priority, reason, status, attempts, updated_at) "
        "VALUES (?, 1, ?, NULL, 0, 'frozen_scout_processed_trigger', 'pending', ?, ?)",
        (
            signature,
            trigger.isoformat(),
            attempts,
            (updated or trigger).isoformat(),
        ),
    )


def test_candidate_first_claim_is_immediate_but_retry_obeys_backoff():
    journal = _journal()
    now = datetime.now(timezone.utc)
    _insert(journal, signature="fresh-first", trigger=now, attempts=0, updated=now)
    _insert(
        journal,
        signature="fresh-retry",
        trigger=now - timedelta(seconds=1),
        attempts=1,
        updated=now,
    )

    row = repair._deadline_aware_claim_candidate(journal)
    assert row is not None
    assert row["signature"] == "fresh-first"

    # The untouched retry remains pending until its short re-admission backoff has
    # elapsed, preventing a null-result row from immediately consuming another RPC
    # slot ahead of new scout triggers.
    assert repair._deadline_aware_claim_candidate(journal) is None
    old = now - timedelta(seconds=1)
    journal.store.db.execute(
        "UPDATE direct_solana_hydration_queue SET updated_at=? WHERE signature='fresh-retry'",
        (old.isoformat(),),
    )
    row = repair._deadline_aware_claim_candidate(journal)
    assert row is not None
    assert row["signature"] == "fresh-retry"


def test_candidate_claim_prioritizes_trigger_near_entry_deadline():
    journal = _journal()
    now = datetime.now(timezone.utc)
    ready_new = now - timedelta(seconds=1)
    urgent = now - timedelta(
        seconds=forward.ENTRY_WINDOW_SECONDS - repair.CANDIDATE_URGENT_REMAINING_SECONDS + 0.5
    )
    _insert(journal, signature="new", trigger=ready_new, attempts=0, updated=ready_new)
    _insert(journal, signature="urgent", trigger=urgent, attempts=3, updated=now)

    row = repair._deadline_aware_claim_candidate(journal)
    assert row is not None
    assert row["signature"] == "urgent"


def test_candidate_transaction_uses_one_base_attempt_per_queue_claim(monkeypatch):
    plane = SimpleNamespace()
    calls: list[int] = []

    async def base(_self, _signature, *, hedge, attempts):
        calls.append(int(attempts))
        assert hedge is True
        return {"slot": 1}, "publicnode", 3.0

    monkeypatch.setattr(forward, "_ORIGINAL_GET_TRANSACTION_READY", base)
    reason_token = candidate_hotpath._CURRENT_HYDRATION_REASON.set(
        "frozen_scout_processed_trigger"
    )
    trigger_token = forward._CURRENT_TRIGGER_AT.set(datetime.now(timezone.utc))
    try:
        result = asyncio.run(
            repair._single_attempt_candidate_transaction_ready(
                plane,
                "sig",
                hedge=True,
                attempts=4,
            )
        )
    finally:
        candidate_hotpath._CURRENT_HYDRATION_REASON.reset(reason_token)
        forward._CURRENT_TRIGGER_AT.reset(trigger_token)

    assert result[0] == {"slot": 1}
    assert calls == [1]
    assert plane._roi_candidate_completion_transaction_ready == 1
    assert plane._roi_candidate_completion_rpc_claims_completed == 1


def _governor_state(*, candidate_active: int, standby_active: int) -> SimpleNamespace:
    return SimpleNamespace(
        loop_id=1,
        endpoint_key="https://rpc.example",
        active_total=candidate_active + standby_active,
        active_by_workload={
            governor.WORKLOAD_CRITICAL: 0,
            governor.WORKLOAD_CERTIFICATION: 0,
            governor.WORKLOAD_RESEARCH: 0,
            candidate_priority.WORKLOAD_CANDIDATE: candidate_active,
            standby_priority.WORKLOAD_STANDBY: standby_active,
        },
        last_research_started_monotonic=0.0,
    )


def _policy() -> dict[str, float | int]:
    return {
        "total_per_endpoint": 3,
        "noncritical_ceiling_per_endpoint": 2,
        "research_max_per_endpoint": 1,
        "research_min_interval_seconds": 1.0,
    }


def test_waiting_standby_gets_second_noncritical_slot_without_displacing_candidate():
    state = _governor_state(candidate_active=1, standby_active=0)
    key = (state.loop_id, state.endpoint_key)
    standby_priority._STANDBY_WAITERS[key] = 1
    candidate_priority._CANDIDATE_WAITERS.pop(key, None)
    try:
        candidate_allowed, _ = repair._fair_noncritical_allowed(
            state,
            candidate_priority.WORKLOAD_CANDIDATE,
            _policy(),
        )
        standby_allowed, _ = repair._fair_noncritical_allowed(
            state,
            standby_priority.WORKLOAD_STANDBY,
            _policy(),
        )
    finally:
        standby_priority._STANDBY_WAITERS.pop(key, None)

    assert candidate_allowed is False
    assert standby_allowed is True


def test_candidate_gets_first_slot_when_both_forward_lanes_are_waiting():
    state = _governor_state(candidate_active=0, standby_active=0)
    key = (state.loop_id, state.endpoint_key)
    standby_priority._STANDBY_WAITERS[key] = 1
    candidate_priority._CANDIDATE_WAITERS[key] = 1
    try:
        candidate_allowed, _ = repair._fair_noncritical_allowed(
            state,
            candidate_priority.WORKLOAD_CANDIDATE,
            _policy(),
        )
        standby_allowed, _ = repair._fair_noncritical_allowed(
            state,
            standby_priority.WORKLOAD_STANDBY,
            _policy(),
        )
    finally:
        standby_priority._STANDBY_WAITERS.pop(key, None)
        candidate_priority._CANDIDATE_WAITERS.pop(key, None)

    assert candidate_allowed is True
    assert standby_allowed is False


def test_healthy_frontier_cannot_checkpoint_across_unrecovered_generation(monkeypatch):
    target = WatchTarget(kind="program", address="pump", source_hint="PUMP_FUN")
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace()
    fallbacks: list[int] = []

    async def fallback(_self, _target, cursor):
        fallbacks.append(cursor)
        return [{"signature": "recovered", "slot": cursor + 1}], True, "publicnode", 2.0

    async def checkpoint_should_not_run(_self, _target, _cursor):
        raise AssertionError("post-gap frontier must not skip canonical recovery")

    monkeypatch.setattr(lease, "_current_ws_generation", lambda _self, _target: 8)
    monkeypatch.setattr(
        lease,
        "_runtime",
        lambda _self: {key: {"cursor_ws_generation": 7}},
    )
    monkeypatch.setattr(checkpoint, "_ORIGINAL_SLOT_FETCH", fallback)
    monkeypatch.setattr(repair, "_ORIGINAL_CHECKPOINT_FETCH", checkpoint_should_not_run)

    result = asyncio.run(repair._generation_safe_checkpoint_fetch(plane, target, 100))
    assert result[1] is True
    assert fallbacks == [100]
    assert plane._roi_candidate_completion_checkpoint_blocked_unrecovered_generation == 1


def test_checkpoint_resumes_after_cursor_generation_is_recovered(monkeypatch):
    target = WatchTarget(kind="program", address="pump", source_hint="PUMP_FUN")
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace()
    calls: list[int] = []

    async def original(_self, _target, cursor):
        calls.append(cursor)
        return [], True, "publicnode", 1.0

    monkeypatch.setattr(lease, "_current_ws_generation", lambda _self, _target: 8)
    monkeypatch.setattr(
        lease,
        "_runtime",
        lambda _self: {key: {"cursor_ws_generation": 8}},
    )
    monkeypatch.setattr(repair, "_ORIGINAL_CHECKPOINT_FETCH", original)

    result = asyncio.run(repair._generation_safe_checkpoint_fetch(plane, target, 101))
    assert result == ([], True, "publicnode", 1.0)
    assert calls == [101]


def test_checkpoint_compatibility_without_cursor_generation_is_exact_delegate(monkeypatch):
    target = WatchTarget(kind="program", address="pump", source_hint="PUMP_FUN")
    plane = SimpleNamespace()
    calls: list[int] = []

    async def original(_self, _target, cursor):
        calls.append(cursor)
        return [], True, "publicnode", 1.0

    monkeypatch.setattr(lease, "_runtime", lambda _self: {})
    monkeypatch.setattr(repair, "_ORIGINAL_CHECKPOINT_FETCH", original)
    result = asyncio.run(repair._generation_safe_checkpoint_fetch(plane, target, 55))
    assert result == ([], True, "publicnode", 1.0)
    assert calls == [55]


def test_frozen_safety_and_certification_boundaries_are_unchanged():
    assert repair.CANDIDATE_FIRST_FETCH_GRACE_SECONDS == 0.0
    assert forward.LATENCY_BUDGET_SECONDS == 5.0
    assert forward.ENTRY_WINDOW_SECONDS == 20.0
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000
