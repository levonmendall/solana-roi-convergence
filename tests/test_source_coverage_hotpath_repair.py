from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import public_data_economics as economics
from solana_roi.direct_solana import DirectSolanaJournal
from solana_roi.observation_store import ObservationEventStore
from solana_roi.source_coverage import SourceAwareProgramCoverageCertificationGate


def _record_swap(
    store: ObservationEventStore,
    *,
    signature: str,
    source: str,
    received_at: datetime,
) -> None:
    store.record_swap(
        signature=signature,
        slot=1,
        observed_at=received_at.isoformat(),
        received_at=received_at.isoformat(),
        wallet=f"wallet-{signature}",
        token_mint=f"mint-{signature}",
        side="buy",
        token_amount=1000,
        native_amount_sol=1,
        reference_price_sol=0.001,
        ingestion_latency_ms=1,
        source=f"solana-direct:{source}:buy",
    )


def test_production_bound_gate_probe_never_calls_full_status(monkeypatch) -> None:
    monkeypatch.setattr(economics, "COVERAGE_BOOTSTRAP_MIN_INTERVAL_SECONDS", 0.0)

    class Gate:
        def __init__(self) -> None:
            self.probes: list[str] = []

        def source_needs_bootstrap(self, source: str) -> bool:
            self.probes.append(source)
            return source == "PUMP_FUN"

        def status(self):
            raise AssertionError("full certification status must not run on the raw receipt path")

    gate = Gate()
    plane = SimpleNamespace(coverage_status_fn=gate.status)

    assert economics._source_needs_bootstrap(plane, "PUMP_FUN") is True
    assert economics._source_needs_bootstrap(plane, "RAYDIUM") is False
    assert gate.probes == ["PUMP_FUN", "RAYDIUM"]


def test_source_probe_preserves_release_threshold_and_recovery_exclusion(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "bounded-source.sqlite3")
    epoch = datetime(2026, 9, 4, 21, 0, tzinfo=timezone.utc)
    gate = SourceAwareProgramCoverageCertificationGate(
        store,
        configured_fn=lambda: True,
        prospective_start_at=epoch,
    )
    journal = DirectSolanaJournal(store)

    # Arbitrarily many pre-release rows cannot satisfy the prospective boundary.
    for index in range(12):
        _record_swap(
            store,
            signature=f"legacy-{index}",
            source="PUMP_FUN",
            received_at=epoch - timedelta(minutes=1, milliseconds=index),
        )
    assert gate.source_needs_bootstrap("PUMP_FUN") is True

    # Nine genuine live rows plus recovered history still remain under the exact 10-row minimum.
    for index in range(9):
        at = epoch + timedelta(seconds=1, milliseconds=index)
        _record_swap(store, signature=f"live-{index}", source="PUMP_FUN", received_at=at)
    for index in range(12):
        at = epoch + timedelta(seconds=2, milliseconds=index)
        signature = f"recovered-{index}"
        _record_swap(store, signature=signature, source="PUMP_FUN", received_at=at)
        journal.record_hydration(
            signature=signature,
            source="PUMP_FUN",
            trigger_received_at=at,
            hydrated_at=at,
            rpc_provider="test",
            rpc_latency_ms=0.0,
            normalized=True,
            historical_recovery=True,
        )
    assert gate.source_needs_bootstrap("PUMP_FUN") is True

    _record_swap(
        store,
        signature="live-9",
        source="PUMP_FUN",
        received_at=epoch + timedelta(seconds=3),
    )
    assert gate.source_needs_bootstrap("PUMP_FUN") is False

    with store._lock:
        indexes = {str(row["name"]) for row in store.db.execute("PRAGMA index_list(normalized_swaps)").fetchall()}
    assert "ix_swaps_received_source_signature" in indexes


def test_bootstrap_interval_short_circuits_before_sqlite() -> None:
    class Result:
        def fetchone(self):
            return {"n": 0}

    class DB:
        def __init__(self) -> None:
            self.execute_calls = 0

        def execute(self, *_args, **_kwargs):
            self.execute_calls += 1
            return Result()

    db = DB()
    plane = SimpleNamespace(store=SimpleNamespace(_lock=threading.RLock(), db=db))

    assert economics._bootstrap_capacity_available(plane, "RAYDIUM") is True
    assert economics._bootstrap_capacity_available(plane, "RAYDIUM") is False
    assert db.execute_calls == 1


def test_journal_installs_index_for_bounded_bootstrap_capacity_query(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "queue-index.sqlite3")
    DirectSolanaJournal(store)
    with store._lock:
        indexes = {
            str(row["name"])
            for row in store.db.execute("PRAGMA index_list(direct_solana_hydration_queue)").fetchall()
        }
    assert "ix_direct_hydration_source_reason_status" in indexes
