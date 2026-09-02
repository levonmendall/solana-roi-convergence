from __future__ import annotations

from datetime import datetime, timedelta, timezone

from solana_roi.observation_store import ObservationEventStore
from solana_roi.source_coverage import SourceAwareProgramCoverageCertificationGate


def _seed_launch_coverage(store: ObservationEventStore, now: datetime) -> None:
    for i in range(100):
        at = now + timedelta(seconds=i)
        mint = f"launch-{i}"
        store.record_program_coverage(
            token_mint=mint,
            pair_created_at=at.isoformat(),
            assessed_at=(at + timedelta(seconds=2)).isoformat(),
            launch_lag_ms=1000,
            launch_near_creation=True,
            early_buy_count=5,
            early_buyer_count=5,
            early_buyers_complete=True,
        )
        store.mark_program_coverage_funding_complete(
            mint,
            assessed_at=(at + timedelta(seconds=3)).isoformat(),
        )


def _seed_source(store: ObservationEventStore, source: str, now: datetime) -> None:
    for i in range(10):
        store.record_swap(
            signature=f"{source}-{i}",
            slot=i + 1,
            observed_at=(now + timedelta(milliseconds=i)).isoformat(),
            received_at=(now + timedelta(milliseconds=i + 1)).isoformat(),
            wallet=f"wallet-{source}-{i}",
            token_mint=f"mint-{source}-{i}",
            side="buy",
            token_amount=1000,
            native_amount_sol=1,
            reference_price_sol=.001,
            ingestion_latency_ms=1,
            source=f"helius-enhanced-webhook:{source}:buy",
        )


def test_aggregate_coverage_cannot_certify_a_missing_supported_source(tmp_path):
    store = ObservationEventStore(tmp_path / "coverage.sqlite3")
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    _seed_launch_coverage(store, now)
    gate = SourceAwareProgramCoverageCertificationGate(store, configured_fn=lambda: True)

    assert gate.status()["certified"] is False
    _seed_source(store, "PUMP_FUN", now)
    status = gate.status()
    assert status["certified"] is False
    assert set(status["missing_or_under_sampled_program_sources"]) == {"PUMP_AMM", "RAYDIUM"}

    _seed_source(store, "PUMP_AMM", now)
    assert gate.status()["certified"] is False
    _seed_source(store, "RAYDIUM", now)
    status = gate.status()
    assert status["certified"] is True
    assert status["program_source_counts"] == {"PUMP_FUN": 10, "PUMP_AMM": 10, "RAYDIUM": 10}
    assert "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P" in status["frozen_program_ids_by_source"]["PUMP_FUN"]
