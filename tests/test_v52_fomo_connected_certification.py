from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from solana_roi.observation_store import ObservationEventStore
from solana_roi import candidate_fomo_runtime_repair as scanner
from solana_roi import continuation_market_recalibration as continuation
from solana_roi import v51_paper_lifecycle_runtime as lifecycle
from solana_roi.v51_atomic_paper_capital import capital_reconciliation
from solana_roi.v52_cross_lane_paper_certification_repair import (
    configure_v52_cross_lane_paper_certification_repair,
)


TOKEN = "FomoToken111111111111111111111111111111111"


class _Execution:
    async def _risk(self, row, at):
        _ = row, at
        return set(), set(), 0.0

    async def _route(self, token, output_mint, token_raw):
        _ = token, output_mint, token_raw
        return {"out_amount": 1_400_000_000, "fee_lamports": 0}


class _Adapter:
    def __init__(self, store: ObservationEventStore) -> None:
        self.store = store
        self.release_commit = "v52-fomo-connected-certification"
        self.epoch_id = "v52-fomo-connected-certification-epoch"
        self.execution = _Execution()

    def _market_regime(self, at):
        _ = at
        return SimpleNamespace(value="neutral")

    async def _execution(self, row, fraction):
        _ = row, fraction
        return {
            "token_raw": 1_000,
            "decimals": 6,
            "entry_cost_sol": 1.0,
            "entry_price_sol": 0.001,
            "exit_net_sol": 0.95,
            "round_trip_cost_fraction": 0.05,
            "chase_fraction": 0.02,
            "signal_to_entry_seconds": 1.0,
            "quote_latency_ms": 1.0,
        }


def _rows(now: datetime) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, wallet in enumerate(("wallet-a", "wallet-b", "wallet-c")):
        at = now - timedelta(seconds=5 - index)
        rows.append(
            {
                "signature": f"fomo-cert-{index}",
                "slot": 100 + index,
                "wallet": wallet,
                "token_mint": TOKEN,
                "side": "buy",
                "token_amount": 1_000.0,
                "native_amount_sol": 1.0,
                "reference_price_sol": 0.001,
                "observed_at": at.isoformat(),
                "received_at": at.isoformat(),
                "source": "solana-direct:PUMP_AMM:buy",
            }
        )
    return rows


def test_normalized_fomo_candidate_reaches_shared_capital_exit_and_canonical_settlement(tmp_path) -> None:
    configure_v52_cross_lane_paper_certification_repair()
    store = ObservationEventStore(tmp_path / "fomo-connected.sqlite3")
    adapter = _Adapter(store)
    now = datetime.now(timezone.utc)
    rows = _rows(now)

    for row in rows:
        assert store.record_swap(
            signature=str(row["signature"]),
            slot=int(row["slot"]),
            observed_at=str(row["observed_at"]),
            received_at=str(row["received_at"]),
            wallet=str(row["wallet"]),
            token_mint=str(row["token_mint"]),
            side=str(row["side"]),
            token_amount=float(row["token_amount"]),
            native_amount_sol=float(row["native_amount_sol"]),
            reference_price_sol=float(row["reference_price_sol"]),
            ingestion_latency_ms=0.0,
            source=str(row["source"]),
        ) is True

    candidates, diagnostics = scanner._fomo_scan_rows(rows, now=now)
    assert diagnostics["scanner_consuming_normalized_swaps"] is True
    assert diagnostics["active_fomo_candidates"] == 1
    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["state"] == "active_fomo"

    opened = asyncio.run(continuation._open_independent_fomo(adapter, candidate))
    assert opened is True
    source_signature = f"market-flow:{rows[-1]['signature']}"

    with store._lock:
        trial = dict(
            store.db.execute(
                "SELECT * FROM fomo_paper_trials WHERE release_commit=? AND source_signature=?",
                (adapter.release_commit, source_signature),
            ).fetchone()
        )
    assert trial["decision"] == "paper_enter_independent_fomo_probe"
    assert trial["entry_executable"] == 1
    assert trial["exit_executable"] == 1
    assert trial["paper_only"] == 1
    assert trial["live_money_authority"] == 0
    fraction = float(trial["position_fraction"])
    assert 0.0 < fraction <= 0.05

    assert lifecycle.sync_entry_reservations(adapter, source_signature) == 1
    reserved = capital_reconciliation(store, release_commit=adapter.release_commit)
    assert reserved["active_reserved_fraction"] == pytest.approx(fraction)
    assert reserved["available_fraction"] == pytest.approx(1.0 - fraction)

    asyncio.run(continuation._settle_independent_fomo(adapter))
    with store._lock:
        outcome = dict(
            store.db.execute(
                "SELECT * FROM fomo_paper_outcomes WHERE release_commit=? AND source_signature=?",
                (adapter.release_commit, source_signature),
            ).fetchone()
        )
    assert outcome["exit_reason"] == "independent_market_flow:harvest"
    assert float(outcome["net_return"]) == pytest.approx(0.40)
    assert outcome["paper_only"] == 1
    assert outcome["live_money_authority"] == 0

    assert lifecycle.sync_settlements(adapter) == 1
    settled = capital_reconciliation(store, release_commit=adapter.release_commit)
    assert settled["active_reserved_fraction"] == 0.0
    assert settled["available_fraction"] == pytest.approx(1.0)
    assert settled["settlement_count"] == 1
    assert settled["realized_return_contribution"] == pytest.approx(fraction * 0.40)
    assert settled["paper_nav_multiplier"] == pytest.approx(1.0 + fraction * 0.40)

    # Replays are idempotent at both reservation and settlement boundaries.
    assert lifecycle.sync_entry_reservations(adapter, source_signature) == 0
    assert lifecycle.sync_settlements(adapter) == 0
    replayed = capital_reconciliation(store, release_commit=adapter.release_commit)
    assert replayed["settlement_count"] == 1
    assert replayed["paper_nav_multiplier"] == pytest.approx(settled["paper_nav_multiplier"])
    store.close()


def test_fomo_threshold_rejection_never_reaches_paper_capital(tmp_path) -> None:
    configure_v52_cross_lane_paper_certification_repair()
    store = ObservationEventStore(tmp_path / "fomo-reject.sqlite3")
    adapter = _Adapter(store)
    now = datetime.now(timezone.utc)
    row = _rows(now)[0]

    candidates, diagnostics = scanner._fomo_scan_rows([row], now=now)
    assert candidates == []
    assert diagnostics["rejected_min_buys"] == 1
    state = capital_reconciliation(store, release_commit=adapter.release_commit)
    assert state["active_reserved_fraction"] == 0.0
    assert state["settlement_count"] == 0
    store.close()
