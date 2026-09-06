from __future__ import annotations

from datetime import datetime, timezone

from solana_roi import execution_realism
from solana_roi.execution_realism import ObservedEntryExecution, apply_exact_entry_if_available
from solana_roi.models import IntentKind, TradeIntent
from solana_roi.portfolio import PaperPortfolio


def test_exact_evidence_price_divergence_cannot_fall_back_to_theoretical_fill() -> None:
    at = datetime(2026, 9, 6, tzinfo=timezone.utc)
    portfolio = PaperPortfolio()
    intent = TradeIntent(IntentKind.OPEN_STARTER, "MINT", at, fraction_of_full_position=0.30)
    execution_realism._CURRENT_ENTRY_EXECUTION.set(
        ObservedEntryExecution(
            token_mint="MINT",
            stage="starter",
            completed_at=at,
            input_lamports=18_750_000,
            input_sol=0.01875,
            input_usd=3.75,
            order_effective_price_sol=0.0011,
            order_out_token_units=0.01875 / 0.0011,
            order_drift_fraction=0.10,
            router="iris-taker",
            route_fee_bps=30,
            signature_fee_lamports=5_000,
            prioritization_fee_lamports=200_000,
            rent_fee_lamports=2_039_280,
            network_fee_lamports=2_244_280,
            network_fee_usd=0.448856,
            final_quote_latency_ms=100.0,
            final_chain_to_quote_ms=200.0,
            simulation_ok=True,
        )
    )

    consumed = apply_exact_entry_if_available(
        portfolio,
        intent,
        scout_wallet="scout",
        reference_price=0.0012,
    )

    assert consumed is True
    assert "MINT" not in portfolio.positions
    assert portfolio.cash_usd == portfolio.initial_capital_usd
    assert execution_realism._CURRENT_ENTRY_EXECUTION.get() is None
