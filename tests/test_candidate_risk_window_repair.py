from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import candidate_risk_window as risk_window
from solana_roi import candidate_risk_window_repair as compatibility
from solana_roi.observation import TimedRiskCollectors


class _Store:
    def __init__(self) -> None:
        self.rows: list[dict[str, object]] = []

    def record_risk_refresh(self, **kwargs) -> None:
        self.rows.append(dict(kwargs))

    def wallet_profile(self, _wallet: str):
        return {"historically_eligible": True, "tier": "S"}


class _Risk:
    def __init__(self) -> None:
        self.complete = False
        self.fresh = False

    def readiness(self, mint: str, *, as_of: datetime):
        return {
            "token_mint": mint,
            "complete": self.complete,
            "fresh": self.fresh,
            "fresh_dimensions": {
                "authority": self.fresh,
                "liquidity": self.fresh,
                "launch": self.fresh,
                "flow": self.fresh,
                "funding": self.fresh,
                "deployer": self.fresh,
            },
        }


class _Inner:
    def __init__(self, risk: _Risk, clock: list[datetime], *, complete_on_round: int = 1) -> None:
        self.risk = risk
        self.clock = clock
        self.complete_on_round = complete_on_round
        self.coverage_times: list[datetime] = []
        self.candidate_times: list[datetime] = []
        self.rounds = 0

    async def refresh_coverage(self, mint: str, at: datetime, *, current_swap=None) -> None:
        self.coverage_times.append(at)
        self.rounds += 1
        self.clock[0] = self.clock[0] + timedelta(milliseconds=100)
        if self.rounds >= self.complete_on_round:
            self.risk.complete = True
            self.risk.fresh = True

    async def refresh_candidate(self, mint: str, at: datetime, *, current_swap=None) -> None:
        self.candidate_times.append(at)

    def status(self):
        return {"inner": "ok"}


class _Clock:
    def __init__(self, now: datetime) -> None:
        self.now = [now]
        self.perf = [100.0]

    def now_fn(self) -> datetime:
        return self.now[0]

    def perf_fn(self) -> float:
        self.perf[0] += 0.1
        return self.perf[0]


def _collectors(*, now: datetime, complete_on_round: int = 1):
    clock = _Clock(now)
    risk = _Risk()
    inner = _Inner(risk, clock.now, complete_on_round=complete_on_round)
    store = _Store()
    collectors = TimedRiskCollectors(
        inner,
        risk=risk,
        store=store,
        now_fn=clock.now_fn,
        perf_fn=clock.perf_fn,
    )
    return collectors, inner, store


def _swap(observed_at: datetime, received_at: datetime):
    return SimpleNamespace(
        wallet="scout-wallet",
        side="buy",
        observed_at=observed_at,
        received_at=received_at,
        ingestion_latency_ms=max(0.0, (received_at - observed_at).total_seconds() * 1000.0),
    )


def test_candidate_after_five_seconds_can_complete_risk_inside_twenty_seconds():
    trigger = datetime.now(timezone.utc)
    collectors, _inner, store = _collectors(now=trigger + timedelta(seconds=6))
    swap = _swap(trigger, trigger + timedelta(seconds=1))

    asyncio.run(collectors.refresh("MintLateButValid", trigger + timedelta(seconds=1), current_swap=swap))

    assert len(store.rows) == 1
    row = store.rows[0]
    assert row["complete"] is True
    assert row["fresh"] is True
    assert float(row["end_to_end_ms"]) > 5000.0
    assert float(row["end_to_end_ms"]) < 20000.0
    readiness = row["readiness"]
    assert readiness["candidate_processing_target_exceeded"] is True
    assert readiness["candidate_processing_target_is_not_entry_authority"] is True
    assert "candidate_entry_window_exhausted" not in readiness
    assert int(getattr(collectors, "_roi_candidate_risk_window_late_but_complete", 0)) == 1


def test_collectors_use_actual_prospective_decision_time_not_original_receipt_time():
    trigger = datetime.now(timezone.utc)
    decision_time = trigger + timedelta(seconds=8.5)
    collectors, inner, store = _collectors(now=decision_time)
    swap = _swap(trigger, trigger + timedelta(milliseconds=500))

    asyncio.run(collectors.refresh("MintLaunchWindowMatured", swap.received_at, current_swap=swap))

    assert inner.coverage_times == [decision_time]
    assert inner.candidate_times == [decision_time]
    assert inner.coverage_times[0] > swap.received_at
    assert store.rows[0]["complete"] is True


def test_candidate_at_or_beyond_twenty_seconds_fails_closed_without_new_rpc_work():
    trigger = datetime.now(timezone.utc)
    collectors, inner, store = _collectors(now=trigger + timedelta(seconds=20.1))
    swap = _swap(trigger, trigger + timedelta(seconds=1))

    asyncio.run(collectors.refresh("MintTooLate", swap.received_at, current_swap=swap))

    assert inner.coverage_times == []
    assert inner.candidate_times == []
    assert len(store.rows) == 1
    row = store.rows[0]
    assert row["complete"] is False
    assert row["fresh"] is False
    assert row["readiness"]["candidate_entry_window_exhausted"] is True
    assert int(getattr(collectors, "_roi_candidate_risk_window_entry_window_exhausted", 0)) == 1


def test_native_status_and_compatibility_shim_have_no_installer_authority():
    trigger = datetime.now(timezone.utc)
    collectors, _inner, _store = _collectors(now=trigger)
    status = collectors.status()["candidate_risk_window"]
    assert status["implementation"] == "native_timed_risk_collectors_dependency"
    assert status["processing_target_seconds"] == 5.0
    assert status["entry_window_seconds"] == 20.0
    assert status["processing_target_is_entry_authority"] is False
    assert status["signing_or_submission_available"] is False
    assert compatibility.PRODUCTION_MONKEYPATCH_INSTALLER_RETIRED is True
    assert not hasattr(compatibility, "install_candidate_risk_window_repair")


def test_constants_preserve_existing_latency_and_strategy_boundaries():
    assert risk_window.CANDIDATE_PROCESSING_TARGET_SECONDS == 5.0
    assert risk_window.CANDIDATE_ENTRY_WINDOW_SECONDS == 20.0
    assert risk_window.CANDIDATE_RECORDING_RESERVE_SECONDS == 0.10
