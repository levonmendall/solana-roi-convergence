from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from solana_roi.models import IntentKind, TradeIntent
from solana_roi.observation_store import ObservationEventStore
from solana_roi.portfolio import PaperPortfolio
from solana_roi.quote import ExecutableQuote
from solana_roi.shadow_execution import (
    ShadowExecutionObservation,
    ShadowWalletExecutableQuoteHandoff,
)


T0 = datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc)


def _quote() -> ExecutableQuote:
    return ExecutableQuote(
        token_mint="MINT",
        stage="starter",
        requested_notional_usd=3.75,
        input_sol=0.02,
        sol_usd=200.0,
        output_token_units=20.0,
        effective_price_sol=0.001,
        scout_reference_price_sol=0.001,
        drift_fraction=0.0,
        router="iris",
        fee_bps=30,
        token_decimals=6,
        quoted_at=T0,
        received_at=T0,
        quote_latency_ms=25.0,
        chain_to_quote_ms=100.0,
        usable=True,
        reason="within chase ceiling",
    )


class FakeQuoteClient:
    async def quote_buy(self, **kwargs):
        assert kwargs["token_mint"] == "MINT"
        return _quote()


class FakeSimulator:
    def __init__(self, *, simulation_ok: bool = True):
        self.simulation_ok = simulation_ok

    async def observe(self, quote: ExecutableQuote) -> ShadowExecutionObservation:
        completed = T0 + timedelta(milliseconds=40)
        return ShadowExecutionObservation(
            token_mint=quote.token_mint,
            stage=quote.stage,
            shadow_wallet="11111111111111111111111111111111",
            observed_at=T0 + timedelta(milliseconds=30),
            completed_at=completed,
            input_lamports=20_000_000,
            transaction_built=True,
            transaction_sha256="abc",
            transaction_size_bytes=800,
            last_valid_block_height=123,
            router="iris-taker",
            order_out_token_units=20.0,
            order_effective_price_sol=0.0011,
            order_drift_fraction=0.10,
            signature_fee_lamports=5_000,
            prioritization_fee_lamports=200_000,
            rent_fee_lamports=2_039_280,
            simulation_ok=self.simulation_ok,
            units_consumed=100_000,
            simulation_slot=999,
            logs_count=1,
            total_latency_ms=40.0,
            error=None if self.simulation_ok else "simulated failure",
        )


def _handoff(tmp_path, *, simulation_ok: bool = True):
    store = ObservationEventStore(tmp_path / ("ok.sqlite3" if simulation_ok else "failed.sqlite3"))
    handoff = ShadowWalletExecutableQuoteHandoff(
        store=store,
        client=FakeQuoteClient(),
        simulator=FakeSimulator(simulation_ok=simulation_ok),
        full_position_notional_fn=lambda: 12.5,
        max_chase_fraction=0.15,
    )
    return store, handoff


def _intent() -> TradeIntent:
    return TradeIntent(IntentKind.OPEN_STARTER, "MINT", T0, fraction_of_full_position=0.30)


async def _quote_then_apply(handoff: ShadowWalletExecutableQuoteHandoff):
    quote = await handoff.observe(
        token_mint="MINT",
        stage="starter",
        fraction_of_full_position=0.30,
        scout_reference_price_sol=0.001,
        trigger_observed_at=T0,
    )
    assert quote is not None
    portfolio = PaperPortfolio()
    portfolio.apply(_intent(), scout_wallet="scout", reference_price=quote.effective_price_sol)
    return quote, portfolio


def test_successful_shadow_order_drives_exact_entry_price_and_network_cash_cost(tmp_path):
    store, handoff = _handoff(tmp_path)
    quote, portfolio = asyncio.run(_quote_then_apply(handoff))
    assert quote.usable is True
    assert quote.effective_price_sol == pytest.approx(0.0011)
    assert quote.output_token_units == pytest.approx(20.0)
    assert quote.router == "iris-taker"

    fill = portfolio.positions["MINT"].fills[-1]
    expected_network_fee_usd = (5_000 + 200_000 + 2_039_280) / 1_000_000_000 * 200.0
    assert fill.fill_price == pytest.approx(0.0011)
    assert fill.fill_price != pytest.approx(0.0011 * 1.025)
    assert fill.execution_drag_usd == pytest.approx(expected_network_fee_usd)
    assert fill.notional_usd == pytest.approx(3.75)
    assert portfolio.cash_usd == pytest.approx(500.0 - 3.75 - expected_network_fee_usd)
    assert portfolio.positions["MINT"].cost_basis_usd == pytest.approx(3.75 + expected_network_fee_usd)
    assert portfolio.positions["MINT"].entry_capital_usd == pytest.approx(3.75 + expected_network_fee_usd)

    row = store.db.execute(
        "SELECT payload_json FROM events WHERE event_type='paper_execution_cost_observation' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    raw = str(row["payload_json"])
    assert '"route_cost_embedded_in_net_order_price":true' in raw
    assert '"fixed_entry_drag_suppressed_when_exact_order_available":true' in raw
    assert '"network_fee_lamports":2244280' in raw


def test_failed_simulation_never_unlocks_exact_fill_accounting(tmp_path):
    _store, handoff = _handoff(tmp_path, simulation_ok=False)
    quote, portfolio = asyncio.run(_quote_then_apply(handoff))
    assert quote.usable is False

    # Production activation rejects this quote. Even inside the same coroutine,
    # failed simulation never publishes exact-fill context, so a forced unit-test
    # apply can only use the conservative fallback.
    fill = portfolio.positions["MINT"].fills[-1]
    assert fill.fill_price == pytest.approx(quote.effective_price_sol * 1.025)


def test_no_executable_observation_preserves_frozen_conservative_drag():
    portfolio = PaperPortfolio()
    portfolio.apply(_intent(), scout_wallet="scout", reference_price=1.0)
    fill = portfolio.positions["MINT"].fills[-1]
    assert fill.fill_price == pytest.approx(1.025)
    assert portfolio.cash_usd == pytest.approx(496.25)
