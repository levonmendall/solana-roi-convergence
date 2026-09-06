from __future__ import annotations

import asyncio
import time
from collections import Counter
from datetime import datetime, timezone
from types import SimpleNamespace

from solana_roi import candidate_certification_hotpath_repair as candidate_hotpath
from solana_roi import candidate_completion_continuity_repair as completion
from solana_roi import candidate_pipeline_throughput_repair as repair
from solana_roi import candidate_rpc_priority_repair as candidate_priority
from solana_roi import continuity_standby_rpc_priority_repair as standby_priority
from solana_roi import forward_evidence_runtime_repair as forward
from solana_roi import rpc_workload_governor as governor
from solana_roi import semantic_candidate_attribution_architecture as semantic
from solana_roi import venue_native_candidate_graph_repair as venue


def _state(*, candidate_active: int = 0, standby_active: int = 0) -> SimpleNamespace:
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
        requests_by_workload={},
        waits_by_workload={},
        max_active_total=candidate_active + standby_active,
        last_research_started_monotonic=0.0,
        lock=asyncio.Lock(),
    )


def _policy() -> dict[str, float | int]:
    return {
        "total_per_endpoint": 3,
        "critical_reserved_per_endpoint": 1,
        "noncritical_ceiling_per_endpoint": 2,
        "research_max_per_endpoint": 1,
        "research_min_interval_seconds": 1.0,
    }


def test_waiting_candidate_can_take_second_existing_noncritical_slot():
    state = _state(candidate_active=1, standby_active=0)
    key = (state.loop_id, state.endpoint_key)
    candidate_priority._CANDIDATE_WAITERS[key] = 1
    standby_priority._STANDBY_WAITERS[key] = 1
    try:
        candidate_allowed, _ = repair._candidate_first_allowed(
            state,
            candidate_priority.WORKLOAD_CANDIDATE,
            _policy(),
        )
        standby_allowed, _ = repair._candidate_first_allowed(
            state,
            standby_priority.WORKLOAD_STANDBY,
            _policy(),
        )
        certification_allowed, _ = repair._candidate_first_allowed(
            state,
            governor.WORKLOAD_CERTIFICATION,
            _policy(),
        )
    finally:
        candidate_priority._CANDIDATE_WAITERS.pop(key, None)
        standby_priority._STANDBY_WAITERS.pop(key, None)

    assert candidate_allowed is True
    assert standby_allowed is False
    assert certification_allowed is False


def test_critical_reserve_is_never_consumed_by_candidate():
    state = _state(candidate_active=2, standby_active=0)
    allowed, _ = repair._candidate_first_allowed(
        state,
        candidate_priority.WORKLOAD_CANDIDATE,
        _policy(),
    )
    assert allowed is False
    assert state.active_total == 2


def test_provider_timeout_does_not_include_scheduler_wait(monkeypatch):
    endpoint = SimpleNamespace(http_url="https://rpc.example", name="rpc")
    calls: list[str] = []

    async def provider(_self, _endpoint, _method, _params):
        calls.append("provider")
        await asyncio.sleep(0.005)
        return {"ok": True}, "rpc", 5.0

    monkeypatch.setattr(repair, "_ORIGINAL_GOVERNOR_ENDPOINT_DELEGATE", provider)

    async def run():
        token = repair._PROVIDER_TIMEOUT_SECONDS.set(0.01)
        try:
            # This represents governor/admission wait. The timeout is intentionally
            # not active until the provider delegate below begins.
            await asyncio.sleep(0.03)
            return await repair._provider_delegate_with_after_slot_timeout(
                object(),
                endpoint,
                "getTransaction",
                [],
            )
        finally:
            repair._PROVIDER_TIMEOUT_SECONDS.reset(token)

    started = time.perf_counter()
    result = asyncio.run(run())
    elapsed = time.perf_counter() - started

    assert result[0] == {"ok": True}
    assert calls == ["provider"]
    assert elapsed >= 0.03


def test_candidate_transaction_wrapper_has_no_outer_rpc_slice_timeout(monkeypatch):
    plane = SimpleNamespace()
    calls: list[int] = []

    async def base(_self, _signature, *, hedge, attempts):
        calls.append(attempts)
        # Longer than the patched 100ms provider slice. Because this test delegate
        # contains no provider call, the wrapper itself must not time it out.
        await asyncio.sleep(0.12)
        return {"slot": 1}, "rpc", 1.0

    monkeypatch.setattr(forward, "_ORIGINAL_GET_TRANSACTION_READY", base)
    monkeypatch.setattr(completion, "CANDIDATE_FRESH_RPC_SLICE_SECONDS", 0.10)

    reason_token = candidate_hotpath._CURRENT_HYDRATION_REASON.set(
        "frozen_scout_processed_trigger"
    )
    trigger_token = forward._CURRENT_TRIGGER_AT.set(datetime.now(timezone.utc))
    try:
        result = asyncio.run(
            repair._transaction_ready_after_slot_timeout(
                plane,
                "sig",
                hedge=True,
                attempts=3,
            )
        )
    finally:
        candidate_hotpath._CURRENT_HYDRATION_REASON.reset(reason_token)
        forward._CURRENT_TRIGGER_AT.reset(trigger_token)

    assert result[0] == {"slot": 1}
    assert calls == [1]
    assert plane._roi_candidate_pipeline_transaction_ready == 1
    assert int(getattr(plane, "_roi_candidate_pipeline_provider_timeouts", 0) or 0) == 0


def test_immediate_prewarm_uses_supported_risk_readthrough_signature(monkeypatch):
    calls: list[str] = []
    persisted: list[tuple[object, object]] = []

    async def coverage(mint, _at, *, current_swap=None):
        calls.append(f"coverage:{mint}")
        assert current_swap is swap

    async def candidate(mint, _at, *, current_swap=None):
        calls.append(f"candidate:{mint}")
        assert current_swap is swap

    monkeypatch.setattr(
        semantic,
        "_persist_risk_readthrough",
        lambda plane, row: persisted.append((plane, row)),
    )
    monkeypatch.setattr(venue, "_prewarm_sem", lambda _plane: asyncio.Semaphore(1))
    monkeypatch.setattr(venue, "_prewarm_last", lambda _plane: {})

    plane = SimpleNamespace(
        service=SimpleNamespace(
            collectors=SimpleNamespace(
                inner=SimpleNamespace(
                    refresh_coverage=coverage,
                    refresh_candidate=candidate,
                )
            )
        )
    )
    swap = SimpleNamespace(token_mint="mint")
    asyncio.run(
        repair._prewarm_durable_opportunity_immediately(
            plane,
            swap,
            "mint:PUMP_AMM",
        )
    )

    assert sorted(calls) == ["candidate:mint", "coverage:mint"]
    assert persisted == [(plane, swap)]
    assert plane._roi_candidate_pipeline_prewarm_completed == 1


def test_handoff_always_records_terminal_telemetry(monkeypatch):
    obj = SimpleNamespace(
        _roi_candidate_v4_handoff_risk_incomplete=0,
        _roi_candidate_v4_handoff_last_blocker=None,
    )

    async def original(target, _signature):
        target._roi_candidate_v4_handoff_risk_incomplete = 1
        target._roi_candidate_v4_handoff_last_blocker = (
            "six_dimension_risk_incomplete_or_stale"
        )

    monkeypatch.setattr(repair, "_ORIGINAL_HANDOFF", original)
    asyncio.run(repair._handoff_with_terminal_telemetry(obj, "sig"))

    assert isinstance(obj._roi_candidate_pipeline_handoff_outcomes, Counter)
    assert obj._roi_candidate_pipeline_handoff_outcomes["risk_incomplete"] == 1
    assert obj._roi_candidate_pipeline_handoff_terminal_accounted == 1


def test_frozen_strategy_and_safety_boundaries_remain_exact():
    assert forward.LATENCY_BUDGET_SECONDS == 5.0
    assert forward.ENTRY_WINDOW_SECONDS == 20.0
    assert repair.STRATEGY_THRESHOLDS_CHANGED is False
    assert repair.CERTIFICATION_THRESHOLDS_CHANGED is False
    assert repair.PAPER_ONLY is True
    assert repair.LIVE_MONEY_AUTHORITY is False
    assert repair.SIGNING_AVAILABLE is False
    assert repair.TRANSACTION_SUBMISSION_AVAILABLE is False
