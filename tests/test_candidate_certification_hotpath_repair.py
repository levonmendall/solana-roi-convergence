from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import candidate_certification_hotpath_repair as repair
from solana_roi import launch_ws_frontier_timing_repair as frontier
from solana_roi.coverage_completeness_repair import _launch_contexts
from solana_roi.observation import TimedRiskCollectors
from solana_roi.observation_store import ObservationEventStore


def _now() -> datetime:
    return datetime.now(timezone.utc)


def test_frozen_scout_prefill_reuses_attested_mint_context_without_source_fanout(monkeypatch):
    calls: list[str] = []

    async def original(_self, candidate):
        calls.append(str(candidate.token_mint))
        return True

    monkeypatch.setattr(repair, "_ORIGINAL_PREFILL", original)
    launch = SimpleNamespace()
    _launch_contexts(launch)["MINT-A"] = {"complete": True}
    profile = SimpleNamespace(historically_eligible=True, tier=SimpleNamespace(value="S"))
    plane = SimpleNamespace(
        scout_wallets=("SCOUT-A",),
        service=SimpleNamespace(
            registry=SimpleNamespace(get=lambda wallet: profile if wallet == "SCOUT-A" else None),
            collectors=SimpleNamespace(inner=SimpleNamespace(launch=launch)),
        ),
    )
    candidate = SimpleNamespace(wallet="SCOUT-A", side="buy", token_mint="MINT-A")

    token = repair._CURRENT_HYDRATION_REASON.set("frozen_scout_processed_trigger")
    try:
        result = asyncio.run(repair._candidate_prefill_from_attested_context(plane, candidate))
    finally:
        repair._CURRENT_HYDRATION_REASON.reset(token)

    assert result is True
    assert calls == []
    assert plane._roi_candidate_hotpath_scout_source_fanout_bypassed == 1
    assert plane._roi_candidate_hotpath_attested_launch_context_reused == 1


def test_frozen_scout_without_attestation_fails_closed_without_source_fanout(monkeypatch):
    calls: list[str] = []

    async def original(_self, candidate):
        calls.append(str(candidate.token_mint))
        return True

    monkeypatch.setattr(repair, "_ORIGINAL_PREFILL", original)
    profile = SimpleNamespace(historically_eligible=True, tier=SimpleNamespace(value="A"))
    plane = SimpleNamespace(
        scout_wallets=("SCOUT-A",),
        service=SimpleNamespace(
            registry=SimpleNamespace(get=lambda wallet: profile if wallet == "SCOUT-A" else None),
            collectors=SimpleNamespace(inner=SimpleNamespace(launch=SimpleNamespace())),
        ),
    )
    candidate = SimpleNamespace(wallet="SCOUT-A", side="buy", token_mint="MINT-MISSING")

    token = repair._CURRENT_HYDRATION_REASON.set("frozen_scout_live_poll_trigger")
    try:
        result = asyncio.run(repair._candidate_prefill_from_attested_context(plane, candidate))
    finally:
        repair._CURRENT_HYDRATION_REASON.reset(token)

    assert result is False
    assert calls == []
    assert plane._roi_candidate_hotpath_missing_attested_launch_context == 1


def test_non_scout_prefill_preserves_original_path(monkeypatch):
    calls: list[str] = []

    async def original(_self, candidate):
        calls.append(str(candidate.token_mint))
        return True

    monkeypatch.setattr(repair, "_ORIGINAL_PREFILL", original)
    plane = SimpleNamespace(scout_wallets=("SCOUT-A",), service=SimpleNamespace())
    candidate = SimpleNamespace(wallet="OTHER", side="buy", token_mint="MINT-B")

    token = repair._CURRENT_HYDRATION_REASON.set("prospective_launch")
    try:
        result = asyncio.run(repair._candidate_prefill_from_attested_context(plane, candidate))
    finally:
        repair._CURRENT_HYDRATION_REASON.reset(token)

    assert result is True
    assert calls == ["MINT-B"]


class _LatencyStore:
    def __init__(self):
        self.measurements: list[dict] = []

    def wallet_profile(self, wallet: str):
        if wallet != "SCOUT-A":
            return None
        return {"historically_eligible": 1, "tier": "S"}

    def record_risk_refresh(self, **kwargs):
        self.measurements.append(kwargs)


class _Risk:
    def readiness(self, mint: str, *, as_of: datetime):
        return {
            "complete": True,
            "fresh": True,
            "fresh_dimensions": {
                "mint_authority": True,
                "liquidity": True,
                "deployer_history": True,
                "flow": True,
                "launch": True,
                "funding": True,
            },
        }


def test_candidate_risk_dimensions_refresh_concurrently_and_record_measurement(monkeypatch):
    starts: list[str] = []
    both_started = asyncio.Event()

    class Inner:
        async def refresh_coverage(self, mint, at, *, current_swap=None):
            starts.append("coverage")
            if len(starts) == 2:
                both_started.set()
            await asyncio.wait_for(both_started.wait(), timeout=0.2)

        async def refresh_candidate(self, mint, at, *, current_swap=None):
            starts.append("candidate")
            if len(starts) == 2:
                both_started.set()
            await asyncio.wait_for(both_started.wait(), timeout=0.2)

        def status(self):
            return {}

    async def original(*args, **kwargs):
        raise AssertionError("eligible production candidate must use bounded concurrent path")

    monkeypatch.setattr(repair, "_ORIGINAL_TIMED_REFRESH", original)
    store = _LatencyStore()
    timed = TimedRiskCollectors(Inner(), risk=_Risk(), store=store)
    now = _now()
    swap = SimpleNamespace(
        wallet="SCOUT-A",
        side="buy",
        observed_at=now - timedelta(milliseconds=100),
        received_at=now,
        ingestion_latency_ms=100.0,
    )

    asyncio.run(repair._timed_refresh_with_candidate_budget(timed, "MINT-C", now, current_swap=swap))

    assert sorted(starts) == ["candidate", "coverage"]
    assert len(store.measurements) == 1
    measurement = store.measurements[0]
    assert measurement["complete"] is True
    assert measurement["fresh"] is True
    assert timed._roi_candidate_hotpath_measurements_recorded == 1


def test_candidate_already_outside_five_second_clock_records_failed_sample_without_collecting(monkeypatch):
    calls: list[str] = []

    class Inner:
        async def refresh_coverage(self, mint, at, *, current_swap=None):
            calls.append("coverage")

        async def refresh_candidate(self, mint, at, *, current_swap=None):
            calls.append("candidate")

        def status(self):
            return {}

    async def original(*args, **kwargs):
        raise AssertionError("eligible production candidate must use bounded concurrent path")

    monkeypatch.setattr(repair, "_ORIGINAL_TIMED_REFRESH", original)
    store = _LatencyStore()
    trigger = _now() - timedelta(seconds=6)
    fixed_now = _now()
    timed = TimedRiskCollectors(
        Inner(),
        risk=_Risk(),
        store=store,
        now_fn=lambda: fixed_now,
        perf_fn=time.perf_counter,
    )
    swap = SimpleNamespace(
        wallet="SCOUT-A",
        side="buy",
        observed_at=trigger,
        received_at=trigger + timedelta(milliseconds=100),
        ingestion_latency_ms=100.0,
    )

    asyncio.run(repair._timed_refresh_with_candidate_budget(timed, "MINT-LATE", trigger, current_swap=swap))

    assert calls == []
    assert len(store.measurements) == 1
    measurement = store.measurements[0]
    assert measurement["complete"] is False
    assert measurement["fresh"] is False
    assert measurement["readiness"]["candidate_certification_budget_exhausted"] is True
    assert measurement["end_to_end_ms"] >= 5_000.0
    assert timed._roi_candidate_hotpath_budget_exhausted == 1


def test_near_creation_diagnostics_expose_actual_lag_without_changing_gate(tmp_path):
    store = ObservationEventStore(tmp_path / "diag.sqlite3")
    at = _now()
    created = at - timedelta(seconds=8)
    mint = "MINT-DIAG"
    signature = "SIG-DIAG"
    store.record_program_coverage(
        token_mint=mint,
        pair_created_at=created.isoformat(),
        assessed_at=at.isoformat(),
        launch_lag_ms=4_250.0,
        launch_near_creation=False,
        early_buy_count=4,
        early_buyer_count=4,
        early_buyers_complete=True,
    )
    frontier._write_frontier_row(
        store,
        signature=signature,
        launch_slot=100,
        frontier_slot=111,
        frontier_provider="publicnode",
        frontier_age_ms=125.0,
        captured_at=at.isoformat(),
        status="captured",
    )
    frontier._set_frontier_block_time(store, signature, block_time=created.timestamp() + 4.25)

    launch = SimpleNamespace(store=store, _roi_last_launch_timing_proof="preexisting-websocket-chain-frontier-lag")
    _launch_contexts(launch)[mint] = {"complete": True, "launch_signature": signature}
    repair._record_launch_diagnostic(launch, mint, at)
    status = repair._diagnostic_status(store)

    assert status["diagnostic_only"] is True
    assert status["gate_semantics_unchanged"] is True
    assert status["threshold_seconds_unchanged"] == 3.0
    assert status["sample_count"] == 1
    assert status["near_creation_count"] == 0
    assert status["p50_launch_lag_ms"] == 4_250.0
    assert status["p50_frontier_slot_delta"] == 11.0
    assert status["timing_proof_counts"] == {"preexisting-websocket-chain-frontier-lag": 1}
    store.close()


def test_frozen_limits_remain_unchanged():
    assert repair.CANDIDATE_END_TO_END_BUDGET_SECONDS == 5.0
    assert repair.CANDIDATE_RECORDING_RESERVE_SECONDS == 0.10
