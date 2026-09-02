from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi.observation import LatencyCertificationGate, TimedRiskCollectors
from solana_roi.observation_store import ObservationEventStore


class FakeInner:
    def __init__(self):
        self.coverage_calls = 0
        self.candidate_calls = 0

    async def refresh_coverage(self, mint, at, *, current_swap=None):
        self.coverage_calls += 1

    async def refresh_candidate(self, mint, at, *, current_swap=None):
        self.candidate_calls += 1

    def status(self):
        return {}


class FakeRisk:
    def readiness(self, mint, *, as_of):
        return {"complete": True, "fresh": True, "fresh_dimensions": {}}


def _swap(wallet: str, at: datetime):
    return SimpleNamespace(
        wallet=wallet,
        side="buy",
        observed_at=at - timedelta(milliseconds=50),
        received_at=at,
        ingestion_latency_ms=50.0,
    )


def test_background_program_traffic_collects_coverage_but_not_candidate_latency(tmp_path):
    store = ObservationEventStore(tmp_path / "latency.sqlite3")
    inner = FakeInner()
    at = datetime.now(timezone.utc)
    collectors = TimedRiskCollectors(inner, risk=FakeRisk(), store=store)

    asyncio.run(collectors.refresh("mint", at, current_swap=_swap("background", at)))
    assert inner.coverage_calls == 1
    assert inner.candidate_calls == 0
    assert store.recent_risk_refreshes() == []

    store.upsert_wallet_profile(
        wallet="scout",
        entity_id="kol:scout",
        tier="S",
        first_touch_sample_size=30,
        historically_eligible=True,
        updated_at=at.isoformat(),
    )
    asyncio.run(collectors.refresh("mint", at, current_swap=_swap("scout", at)))
    assert inner.coverage_calls == 2
    assert inner.candidate_calls == 1
    rows = store.recent_risk_refreshes()
    assert len(rows) == 1
    assert rows[0]["complete"] is True
    assert rows[0]["fresh"] is True


def test_latency_gate_excludes_pre_release_measurements(tmp_path):
    store = ObservationEventStore(tmp_path / "epoch-latency.sqlite3")
    epoch = datetime(2026, 9, 2, tzinfo=timezone.utc)
    for i, completed in enumerate((epoch - timedelta(seconds=1), epoch + timedelta(seconds=1))):
        store.record_risk_refresh(
            token_mint=f"mint-{i}",
            trigger_observed_at=(completed - timedelta(milliseconds=100)).isoformat(),
            trigger_received_at=(completed - timedelta(milliseconds=50)).isoformat(),
            started_at=(completed - timedelta(milliseconds=40)).isoformat(),
            completed_at=completed.isoformat(),
            elapsed_ms=40,
            ingestion_latency_ms=50,
            end_to_end_ms=100,
            complete=True,
            fresh=True,
            readiness={"complete": True, "fresh": True},
        )
    status = LatencyCertificationGate(store, prospective_start_at=epoch).status()
    assert status["sample_count"] == 1
    assert status["complete_fresh_count"] == 1
    assert status["prospective_start_at"] == epoch.isoformat()
