from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from solana_roi import certification_failure_accounting_repair as repair
from solana_roi.observation import LatencyCertificationGate
from solana_roi.observation_store import ObservationEventStore
from solana_roi.quote import ExecutableQuoteLedger, QuoteCertificationGate


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _insert_passing_latency_rows(store: ObservationEventStore, *, count: int, at: datetime) -> None:
    with store._lock, store.db:
        for index in range(count):
            observed = at + timedelta(milliseconds=index)
            received = observed + timedelta(milliseconds=100)
            completed = observed + timedelta(milliseconds=500)
            store.db.execute(
                "INSERT INTO risk_refresh_measurements("
                "token_mint, trigger_observed_at, trigger_received_at, started_at, completed_at, elapsed_ms, "
                "ingestion_latency_ms, end_to_end_ms, complete, fresh, readiness_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 1, '{}')",
                (
                    f"MINT-{index}", observed.isoformat(), received.isoformat(), received.isoformat(),
                    completed.isoformat(), 400.0, 100.0, 500.0,
                ),
            )


def test_eligible_scout_timeout_becomes_failed_latency_sample(tmp_path):
    store = ObservationEventStore(tmp_path / "eligible-timeout.sqlite3")
    now = _now()
    trigger = now - timedelta(seconds=20)
    observed = trigger - timedelta(milliseconds=250)
    store.upsert_wallet_profile(
        wallet="scout-s",
        entity_id="entity-s",
        tier="S",
        first_touch_sample_size=100,
        historically_eligible=True,
        updated_at=(trigger - timedelta(days=1)).isoformat(),
    )
    store.record_swap(
        signature="sig-timeout",
        slot=1,
        observed_at=observed.isoformat(),
        received_at=trigger.isoformat(),
        wallet="scout-s",
        token_mint="MINT-TIMEOUT",
        side="buy",
        token_amount=1.0,
        native_amount_sol=1.0,
        reference_price_sol=1.0,
        ingestion_latency_ms=250.0,
        source="solana-direct:PUMP_FUN:scout",
    )
    row = {
        "signature": "sig-timeout",
        "trigger_received_at": trigger.isoformat(),
        "reason": "frozen_scout_processed_trigger",
    }

    outcome = repair._account_scout_expiry(
        store,
        row,
        outcome="expired_in_flight_before_entry",
        failed_at=now,
    )

    assert outcome == "eligible_candidate_failed_latency"
    measurements = store.recent_risk_refreshes(10)
    assert len(measurements) == 1
    assert measurements[0]["token_mint"] == "MINT-TIMEOUT"
    assert measurements[0]["complete"] is False
    assert measurements[0]["fresh"] is False
    assert measurements[0]["end_to_end_ms"] >= 20_000.0
    assert measurements[0]["readiness"]["certification_failure_accounting"] is True
    store.close()


def test_non_candidate_scout_transaction_does_not_pollute_latency_denominator(tmp_path):
    store = ObservationEventStore(tmp_path / "sell.sqlite3")
    now = _now()
    trigger = now - timedelta(seconds=20)
    store.upsert_wallet_profile(
        wallet="scout-s",
        entity_id="entity-s",
        tier="S",
        first_touch_sample_size=100,
        historically_eligible=True,
        updated_at=(trigger - timedelta(days=1)).isoformat(),
    )
    store.record_swap(
        signature="sig-sell",
        slot=1,
        observed_at=trigger.isoformat(),
        received_at=trigger.isoformat(),
        wallet="scout-s",
        token_mint="MINT-SELL",
        side="sell",
        token_amount=1.0,
        native_amount_sol=1.0,
        reference_price_sol=1.0,
        ingestion_latency_ms=0.0,
        source="solana-direct:PUMP_FUN:scout",
    )

    outcome = repair._account_scout_expiry(
        store,
        {
            "signature": "sig-sell",
            "trigger_received_at": trigger.isoformat(),
            "reason": "frozen_scout_processed_trigger",
        },
        outcome="expired_in_flight_before_entry",
        failed_at=now,
    )

    assert outcome == "classified_non_candidate"
    assert store.recent_risk_refreshes(10) == []
    store.close()


def test_unclassified_scout_expiry_blocks_otherwise_passing_latency_gate(tmp_path):
    store = ObservationEventStore(tmp_path / "unclassified.sqlite3")
    start = _now() - timedelta(minutes=1)
    _insert_passing_latency_rows(store, count=100, at=start)
    repair._record_anonymous_candidate_failure(
        store,
        reason="frozen_scout_processed_trigger",
        outcome="expired_before_entry",
        count=1,
        max_age_ms=20_500.0,
        failed_at=_now(),
    )
    gate = LatencyCertificationGate(store, prospective_start_at=start)

    baseline = repair._ORIGINAL_LATENCY_STATUS(gate)
    assert baseline["certified"] is True
    status = repair._latency_status_with_failure_accounting(gate)

    assert status["certified"] is False
    assert status["candidate_sampling_complete"] is False
    assert status["unclassified_scout_trigger_expiry_count"] == 1
    assert status["requirements"]["all_frozen_scout_triggers_must_be_classified_within_entry_window"] is True
    store.close()


def _insert_passing_quote_rows(store: ObservationEventStore, *, count: int, at: datetime) -> None:
    with store._lock, store.db:
        for index in range(count):
            received = at + timedelta(milliseconds=index)
            store.db.execute(
                "INSERT INTO execution_quote_observations("
                "token_mint, stage, requested_notional_usd, input_sol, sol_usd, output_token_units, "
                "effective_price_sol, scout_reference_price_sol, drift_fraction, router, fee_bps, token_decimals, "
                "quoted_at, received_at, quote_latency_ms, chain_to_quote_ms, usable, reason) "
                "VALUES (?, 'starter', 10, 0.1, 100, 1000, 0.0001, 0.0001, 0, 'test', 0, 6, ?, ?, 100, 500, 1, 'ok')",
                (f"MINT-Q-{index}", received.isoformat(), received.isoformat()),
            )


def test_quote_failures_are_included_in_certification_denominator(tmp_path):
    store = ObservationEventStore(tmp_path / "quote-failures.sqlite3")
    ledger = ExecutableQuoteLedger(store)
    at = _now() - timedelta(seconds=2)
    _insert_passing_quote_rows(store, count=100, at=at)
    for index in range(6):
        failed = at + timedelta(seconds=1, milliseconds=index)
        repair._record_quote_failure(
            store,
            token_mint=f"MINT-F-{index}",
            stage="starter",
            trigger_observed_at=failed - timedelta(milliseconds=500),
            started_at=failed - timedelta(milliseconds=100),
            failed_at=failed,
            elapsed_ms=100.0,
            error_type="TimeoutError",
            error="timed out",
        )
    gate = QuoteCertificationGate(ledger)

    status = repair._quote_status_with_failure_accounting(gate)

    assert status["sample_count"] == 106
    assert status["usable_count"] == 100
    assert status["failed_attempt_count"] == 6
    assert status["usable_fraction"] == pytest.approx(100 / 106)
    assert status["certified"] is False
    assert status["failure_attempts_included_in_denominator"] is True
    store.close()


def test_cancelled_shadow_simulation_is_retained_without_suppressing_cancel(monkeypatch):
    quote = SimpleNamespace(
        token_mint="MINT-CANCEL",
        stage="starter",
        input_sol=0.1,
        router="test",
    )
    simulator = SimpleNamespace(shadow_wallet="11111111111111111111111111111111")

    async def cancelled(_self, _quote):
        raise asyncio.CancelledError

    monkeypatch.setattr(repair, "_ORIGINAL_SHADOW_OBSERVE", cancelled)

    async def scenario():
        with pytest.raises(asyncio.CancelledError):
            await repair._shadow_observe_with_cancellation_accounting(simulator, quote)

    asyncio.run(scenario())
    pending = simulator._roi_cancelled_shadow_observations
    assert len(pending) == 1
    assert pending[0].simulation_ok is False
    assert pending[0].transaction_built is False
    assert "entry window expired" in str(pending[0].error)
