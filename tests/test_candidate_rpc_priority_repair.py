from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import candidate_rpc_priority_repair as repair
from solana_roi import rpc_workload_governor as governor


def _policy():
    return {
        "total_per_endpoint": 3,
        "critical_reserved_per_endpoint": 1,
        "noncritical_ceiling_per_endpoint": 2,
        "research_max_per_endpoint": 1,
        "research_min_interval_seconds": 1.0,
    }


def _state(*, critical=0, certification=0, candidate=0, research=0):
    return SimpleNamespace(
        loop_id=1,
        endpoint_key="https://rpc.invalid",
        active_total=critical + certification + candidate + research,
        active_by_workload={
            governor.WORKLOAD_CRITICAL: critical,
            governor.WORKLOAD_CERTIFICATION: certification,
            repair.WORKLOAD_CANDIDATE: candidate,
            governor.WORKLOAD_RESEARCH: research,
        },
        last_research_started_monotonic=0.0,
    )


def test_noncritical_capacity_is_not_reduced_when_critical_slot_is_active():
    state = _state(critical=1, certification=1)

    allowed, _delay = repair._allowed_with_candidate_priority(
        state,
        repair.WORKLOAD_CANDIDATE,
        _policy(),
    )

    assert allowed is True
    # This is the configured 3-slot shape: one critical + two noncritical.
    assert state.active_total == 2
    assert _policy()["total_per_endpoint"] == 3
    assert _policy()["noncritical_ceiling_per_endpoint"] == 2


def test_waiting_candidate_reserves_one_noncritical_slot_from_background():
    state = _state(critical=1, certification=1)
    repair._CANDIDATE_WAITERS.clear()
    repair._CANDIDATE_WAITERS[(state.loop_id, state.endpoint_key)] = 1
    try:
        background_allowed, _ = repair._allowed_with_candidate_priority(
            state,
            governor.WORKLOAD_CERTIFICATION,
            _policy(),
        )
        candidate_allowed, _ = repair._allowed_with_candidate_priority(
            state,
            repair.WORKLOAD_CANDIDATE,
            _policy(),
        )
    finally:
        repair._CANDIDATE_WAITERS.clear()

    assert background_allowed is False
    assert candidate_allowed is True


def test_candidate_never_uses_critical_reserve_when_two_noncritical_slots_are_full():
    state = _state(certification=1, candidate=1)
    allowed, _ = repair._allowed_with_candidate_priority(
        state,
        repair.WORKLOAD_CANDIDATE,
        _policy(),
    )
    assert allowed is False
    assert state.active_total == 2


def test_scout_hydration_runs_under_candidate_workload(monkeypatch):
    seen = []

    async def original(_self, _row):
        seen.append(governor.current_rpc_workload())

    monkeypatch.setattr(repair, "_ORIGINAL_HYDRATE", original)
    monkeypatch.setattr(
        governor,
        "_VALID_WORKLOADS",
        frozenset(set(governor._VALID_WORKLOADS) | {repair.WORKLOAD_CANDIDATE}),
    )

    async def scenario():
        await repair._hydrate_with_candidate_rpc_priority(
            SimpleNamespace(),
            {"reason": "frozen_scout_processed_trigger", "priority": 0},
        )
        await repair._hydrate_with_candidate_rpc_priority(
            SimpleNamespace(),
            {"reason": "prospective_launch", "priority": 10},
        )

    asyncio.run(scenario())

    assert seen == [repair.WORKLOAD_CANDIDATE, governor.WORKLOAD_CERTIFICATION]


def test_configured_endpoint_concurrency_and_paper_boundary_are_not_changed(monkeypatch):
    original_policy = governor._policy()
    assert int(original_policy["total_per_endpoint"]) == 3
    assert int(original_policy["critical_reserved_per_endpoint"]) == 1
    assert int(original_policy["noncritical_ceiling_per_endpoint"]) == 2
    # Repair is scheduling-only. It exposes no signing or submission API.
    assert not hasattr(repair, "sign_transaction")
    assert not hasattr(repair, "submit_transaction")
