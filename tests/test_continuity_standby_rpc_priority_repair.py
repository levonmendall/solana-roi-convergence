from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import candidate_rpc_priority_repair as candidate
from solana_roi import continuity_standby_rpc_priority_repair as repair
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi import poll_watermark_repair as watermark
from solana_roi import rpc_workload_governor as governor
from solana_roi.continuity_standby_rpc_priority_repair import (
    WORKLOAD_STANDBY,
    _allowed_with_standby_priority,
    _sharded_page_with_standby_priority,
)
from solana_roi.direct_solana import WatchTarget


def _policy():
    return {
        "total_per_endpoint": 3,
        "critical_reserved_per_endpoint": 1,
        "noncritical_ceiling_per_endpoint": 2,
        "research_max_per_endpoint": 1,
        "research_min_interval_seconds": 1.0,
    }


def _state(*, active_total: int, certification: int = 0, candidate_active: int = 0, standby: int = 0):
    return SimpleNamespace(
        loop_id=123,
        endpoint_key="https://rpc.example",
        active_total=active_total,
        active_by_workload={
            governor.WORKLOAD_CRITICAL: 0,
            governor.WORKLOAD_CERTIFICATION: certification,
            governor.WORKLOAD_RESEARCH: 0,
            candidate.WORKLOAD_CANDIDATE: candidate_active,
            WORKLOAD_STANDBY: standby,
        },
        requests_by_workload={},
        waits_by_workload={},
        last_research_started_monotonic=0.0,
        max_active_total=active_total,
    )


def test_waiting_standby_reserves_one_existing_noncritical_slot_from_background():
    state = _state(active_total=1, certification=1)
    repair._STANDBY_WAITERS[(state.loop_id, state.endpoint_key)] = 1
    candidate._CANDIDATE_WAITERS.pop((state.loop_id, state.endpoint_key), None)
    try:
        allowed_background, _ = _allowed_with_standby_priority(
            state,
            governor.WORKLOAD_CERTIFICATION,
            _policy(),
        )
        allowed_standby, _ = _allowed_with_standby_priority(
            state,
            WORKLOAD_STANDBY,
            _policy(),
        )
    finally:
        repair._STANDBY_WAITERS.pop((state.loop_id, state.endpoint_key), None)

    assert allowed_background is False
    assert allowed_standby is True


def test_waiting_candidate_keeps_priority_over_standby():
    state = _state(active_total=1, certification=1)
    candidate._CANDIDATE_WAITERS[(state.loop_id, state.endpoint_key)] = 1
    repair._STANDBY_WAITERS[(state.loop_id, state.endpoint_key)] = 1
    try:
        allowed_standby, _ = _allowed_with_standby_priority(
            state,
            WORKLOAD_STANDBY,
            _policy(),
        )
        allowed_candidate, _ = _allowed_with_standby_priority(
            state,
            candidate.WORKLOAD_CANDIDATE,
            _policy(),
        )
    finally:
        candidate._CANDIDATE_WAITERS.pop((state.loop_id, state.endpoint_key), None)
        repair._STANDBY_WAITERS.pop((state.loop_id, state.endpoint_key), None)

    assert allowed_standby is False
    assert allowed_candidate is True


def test_standby_never_consumes_critical_reservation():
    state = _state(active_total=2, certification=2)
    allowed_standby, _ = _allowed_with_standby_priority(
        state,
        WORKLOAD_STANDBY,
        _policy(),
    )
    allowed_critical, _ = _allowed_with_standby_priority(
        state,
        governor.WORKLOAD_CRITICAL,
        _policy(),
    )

    assert allowed_standby is False
    assert allowed_critical is True


def test_only_high_volume_routine_pages_enter_standby_workload(monkeypatch):
    calls: list[tuple[str, str]] = []

    async def fake_page(self, target, *, before=None, min_context_slot=None, limit):
        calls.append((str(target.source_hint), governor.current_rpc_workload()))
        return [], "publicnode", 1.0

    monkeypatch.setattr(repair, "_ORIGINAL_SHARDED_PAGE", fake_page)
    pump = WatchTarget("program", "pump", "PUMP_FUN")
    raydium = WatchTarget("program", "ray", "RAYDIUM")

    async def run():
        await _sharded_page_with_standby_priority(SimpleNamespace(), pump, limit=1000)
        await _sharded_page_with_standby_priority(SimpleNamespace(), raydium, limit=1000)

    asyncio.run(run())

    assert calls == [
        ("PUMP_FUN", WORKLOAD_STANDBY),
        ("RAYDIUM", governor.WORKLOAD_CERTIFICATION),
    ]


def test_production_composition_preserves_hard_bounds_and_candidate_priority():
    from solana_roi import continuity_storage_capacity_repair as storage
    from solana_roi.production import app  # noqa: F401

    assert WORKLOAD_STANDBY in governor._VALID_WORKLOADS
    assert candidate.WORKLOAD_CANDIDATE in governor._VALID_WORKLOADS
    assert getattr(watermark._slot_poll_page, "_roi_high_volume_standby_priority", False) is True
    assert watermark._slot_poll_page is storage._sharded_slot_poll_page
    assert getattr(governor._acquire, "__name__", "") == "_acquire_with_standby_priority"
    policy = governor._policy()
    assert int(policy["total_per_endpoint"]) == 3
    assert int(policy["critical_reserved_per_endpoint"]) == 1
    assert int(policy["noncritical_ceiling_per_endpoint"]) == 2
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll.POLL_INTERVAL_SECONDS == 4.0
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000
