from __future__ import annotations

from datetime import datetime, timedelta, timezone

from solana_roi.observation_store import ObservationEventStore
from solana_roi.source_coverage import SourceAwareProgramCoverageCertificationGate


def _seed_direct_source(store: ObservationEventStore, source: str, epoch: datetime) -> None:
    for index in range(10):
        at = epoch + timedelta(milliseconds=index)
        store.record_swap(
            signature=f"direct-{source}-{index}",
            slot=1000 + index,
            observed_at=at.isoformat(),
            received_at=(at + timedelta(milliseconds=1)).isoformat(),
            wallet=f"wallet-{source}-{index}",
            token_mint=f"mint-{source}-{index}",
            side="buy",
            token_amount=1000.0,
            native_amount_sol=1.0,
            reference_price_sol=0.001,
            ingestion_latency_ms=1.0,
            source=f"solana-direct:{source}:buy",
        )


def _seed_launches(store: ObservationEventStore, epoch: datetime) -> None:
    for index in range(100):
        created = epoch + timedelta(seconds=index)
        mint = f"launch-{index}"
        store.record_program_coverage(
            token_mint=mint,
            pair_created_at=created.isoformat(),
            assessed_at=(created + timedelta(seconds=8)).isoformat(),
            launch_lag_ms=500.0,
            launch_near_creation=True,
            early_buy_count=5,
            early_buyer_count=5,
            early_buyers_complete=True,
        )
        store.mark_program_coverage_funding_complete(
            mint, assessed_at=(created + timedelta(seconds=9)).isoformat()
        )


def test_direct_transport_counts_without_changing_existing_certification_thresholds(tmp_path):
    store = ObservationEventStore(tmp_path / "direct-coverage.sqlite3")
    epoch = datetime(2026, 9, 2, tzinfo=timezone.utc)
    _seed_launches(store, epoch)
    for source in ("PUMP_FUN", "PUMP_AMM", "RAYDIUM"):
        _seed_direct_source(store, source, epoch)

    gate = SourceAwareProgramCoverageCertificationGate(
        store,
        configured_fn=lambda: True,
        prospective_start_at=epoch,
    )
    status = gate.status()
    assert status["certified"] is True
    assert status["sample_count"] == 100
    assert status["near_creation_fraction"] == 1.0
    assert status["early_buyer_complete_fraction"] == 1.0
    assert status["funding_complete_fraction"] == 1.0
    assert status["program_source_counts"] == {"PUMP_FUN": 10, "PUMP_AMM": 10, "RAYDIUM": 10}
    assert status["requirements"]["min_samples"] == 100
    assert status["requirements"]["min_normalized_swaps_per_source"] == 10
