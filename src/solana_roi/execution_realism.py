from __future__ import annotations

import math
from contextvars import ContextVar
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from .models import IntentKind, PaperPosition, SimulatedFill
from .quote import ExecutableQuote, LAMPORTS_PER_SOL


_ENTRY_KINDS = {
    IntentKind.OPEN_STARTER,
    IntentKind.OPEN_FULL,
    IntentKind.ADD_CONFIRMATION,
}


@dataclass(frozen=True, slots=True)
class ObservedEntryExecution:
    token_mint: str
    stage: str
    completed_at: datetime
    input_lamports: int
    input_sol: float
    input_usd: float
    order_effective_price_sol: float
    order_out_token_units: float
    order_drift_fraction: float
    router: str
    route_fee_bps: int
    signature_fee_lamports: int
    prioritization_fee_lamports: int
    rent_fee_lamports: int
    network_fee_lamports: int
    network_fee_usd: float
    final_quote_latency_ms: float
    final_chain_to_quote_ms: float
    simulation_ok: bool


_CURRENT_ENTRY_EXECUTION: ContextVar[ObservedEntryExecution | None] = ContextVar(
    "solana_roi_observed_entry_execution",
    default=None,
)


def _fee(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _latency_hard_max_ms() -> float:
    from .strategy_v51_authority import authority

    return max(0.0, float(authority()["execution"]["latency_hard_max_seconds"]) * 1000.0)


def clear_observed_entry_execution() -> None:
    """Clear candidate-local exact-entry evidence before a new handoff begins."""

    _CURRENT_ENTRY_EXECUTION.set(None)


def enrich_exact_entry_quote(
    *,
    store: Any,
    quote: ExecutableQuote,
    shadow: Any,
) -> ExecutableQuote:
    """Attach exact unsigned-order economics and revalidate final executability.

    The final quote timestamp is the completion of unsigned transaction simulation,
    not the earlier quote-only response.  The exact input amount is retained all the
    way to the paper ledger so a quote for one size can never be booked at another.
    """

    try:
        order_price = float(getattr(shadow, "order_effective_price_sol", None) or 0.0)
        order_units = float(getattr(shadow, "order_out_token_units", None) or 0.0)
        raw_drift = getattr(shadow, "order_drift_fraction", None)
        order_drift = float(raw_drift) if raw_drift is not None else quote.drift_fraction
    except (TypeError, ValueError):
        order_price = 0.0
        order_units = 0.0
        order_drift = quote.drift_fraction

    input_lamports = _fee(getattr(shadow, "input_lamports", None))
    expected_input_lamports = max(1, int(float(quote.input_sol) * LAMPORTS_PER_SOL))
    input_amount_matches = input_lamports == expected_input_lamports
    input_sol = input_lamports / LAMPORTS_PER_SOL
    input_usd = input_sol * float(quote.sol_usd)

    signature_fee = _fee(getattr(shadow, "signature_fee_lamports", None))
    priority_fee = _fee(getattr(shadow, "prioritization_fee_lamports", None))
    rent_fee = _fee(getattr(shadow, "rent_fee_lamports", None))
    network_fee_lamports = signature_fee + priority_fee + rent_fee
    network_fee_usd = (network_fee_lamports / LAMPORTS_PER_SOL) * float(quote.sol_usd)
    simulation_ok = bool(getattr(shadow, "simulation_ok", False))
    transaction_built = bool(getattr(shadow, "transaction_built", False))
    exact_order_available = bool(order_price > 0.0 and order_units > 0.0 and input_lamports > 0)
    router = str(getattr(shadow, "router", None) or quote.router)

    completed_at = getattr(shadow, "completed_at", None)
    if not isinstance(completed_at, datetime):
        completed_at = quote.received_at
    post_quote_ms = max(0.0, (completed_at - quote.received_at).total_seconds() * 1000.0)
    final_quote_latency_ms = max(0.0, float(quote.quote_latency_ms) + post_quote_ms)
    final_chain_to_quote_ms = max(0.0, float(quote.chain_to_quote_ms) + post_quote_ms)
    stale = final_chain_to_quote_ms > _latency_hard_max_ms()

    usable = bool(
        quote.usable
        and simulation_ok
        and transaction_built
        and exact_order_available
        and input_amount_matches
        and not stale
    )
    if not quote.usable:
        reason = quote.reason
    elif not exact_order_available:
        reason = "exact_entry_amount_or_output_unavailable"
    elif not input_amount_matches:
        reason = "exact_entry_input_amount_mismatch"
    elif not transaction_built:
        reason = "exact_entry_transaction_not_built"
    elif not simulation_ok:
        reason = "exact_entry_simulation_failed"
    elif stale:
        reason = "exact_entry_stale_after_simulation"
    else:
        reason = "exact_amount_specific_entry_executable"

    exact_quote = replace(
        quote,
        input_sol=input_sol if input_lamports > 0 else quote.input_sol,
        effective_price_sol=order_price if exact_order_available else quote.effective_price_sol,
        output_token_units=order_units if exact_order_available else quote.output_token_units,
        drift_fraction=order_drift if exact_order_available else quote.drift_fraction,
        router=router,
        received_at=completed_at,
        quote_latency_ms=final_quote_latency_ms,
        chain_to_quote_ms=final_chain_to_quote_ms,
        usable=usable,
        reason=reason,
    )

    store.append(
        "paper_execution_cost_observation",
        exact_quote.received_at.isoformat(),
        {
            "token_mint": exact_quote.token_mint,
            "stage": exact_quote.stage,
            "observed_at": exact_quote.received_at.isoformat(),
            "paper_only": True,
            "live_money_authority": False,
            "simulation_ok": simulation_ok,
            "transaction_built": transaction_built,
            "assembled_taker_order_price_available": exact_order_available,
            "paper_fill_uses_assembled_taker_order_price": bool(exact_quote.usable),
            "input_lamports": input_lamports,
            "expected_input_lamports": expected_input_lamports,
            "input_amount_matches_quote": input_amount_matches,
            "input_sol": input_sol,
            "input_usd": input_usd,
            "order_effective_price_sol": order_price if exact_order_available else None,
            "order_out_token_units": order_units if exact_order_available else None,
            "order_drift_fraction": order_drift if exact_order_available else None,
            "router": router,
            "route_fee_bps": int(quote.fee_bps),
            "route_cost_embedded_in_net_order_price": True,
            "signature_fee_lamports": signature_fee,
            "prioritization_fee_lamports": priority_fee,
            "rent_fee_lamports": rent_fee,
            "rent_treated_as_conservative_cash_cost": True,
            "network_fee_lamports": network_fee_lamports,
            "network_fee_usd": network_fee_usd,
            "final_quote_latency_ms": final_quote_latency_ms,
            "final_chain_to_quote_ms": final_chain_to_quote_ms,
            "latency_hard_max_ms": _latency_hard_max_ms(),
            "stale_after_simulation": stale,
            "final_usable": usable,
            "final_reason": reason,
            "fixed_entry_drag_suppressed_when_exact_order_available": True,
            "fallback_execution_drag_per_side_fraction": 0.025,
        },
    )

    if exact_quote.usable:
        _CURRENT_ENTRY_EXECUTION.set(
            ObservedEntryExecution(
                token_mint=exact_quote.token_mint,
                stage=exact_quote.stage,
                completed_at=exact_quote.received_at,
                input_lamports=input_lamports,
                input_sol=input_sol,
                input_usd=input_usd,
                order_effective_price_sol=exact_quote.effective_price_sol,
                order_out_token_units=exact_quote.output_token_units,
                order_drift_fraction=exact_quote.drift_fraction,
                router=exact_quote.router,
                route_fee_bps=int(exact_quote.fee_bps),
                signature_fee_lamports=signature_fee,
                prioritization_fee_lamports=priority_fee,
                rent_fee_lamports=rent_fee,
                network_fee_lamports=network_fee_lamports,
                network_fee_usd=network_fee_usd,
                final_quote_latency_ms=final_quote_latency_ms,
                final_chain_to_quote_ms=final_chain_to_quote_ms,
                simulation_ok=True,
            )
        )
    return exact_quote


def apply_exact_entry_if_available(
    portfolio: Any,
    intent: Any,
    *,
    scout_wallet: str,
    reference_price: float,
    family: str = "legacy_runtime",
    context: str = "unclassified",
) -> bool:
    """Apply one exact simulated entry through the canonical portfolio ledger.

    Once exact evidence exists for this intent, any amount/capital divergence fails
    closed.  The caller must not fall back to a theoretical fill using the same quote.
    """

    evidence = _CURRENT_ENTRY_EXECUTION.get()
    use_exact = bool(
        intent.kind in _ENTRY_KINDS
        and evidence is not None
        and evidence.simulation_ok
        and evidence.token_mint == intent.token_mint
        and evidence.order_effective_price_sol > 0.0
        and evidence.order_out_token_units > 0.0
        and evidence.input_usd > 0.0
        and math.isclose(
            float(reference_price),
            evidence.order_effective_price_sol,
            rel_tol=1e-9,
            abs_tol=1e-15,
        )
    )
    if not use_exact or evidence is None:
        return False

    _CURRENT_ENTRY_EXECUTION.set(None)

    network_fee_usd = max(0.0, float(evidence.network_fee_usd))
    exact_notional = max(0.0, float(evidence.input_usd))
    exact_units = max(0.0, float(evidence.order_out_token_units))
    full = portfolio.full_position_notional({intent.token_mint: evidence.order_effective_price_sol})
    intended_notional = max(0.0, full * intent.fraction_of_full_position)
    available_for_swap = max(0.0, portfolio.cash_usd - network_fee_usd)

    numeric_tolerance = max(1e-6, exact_notional * 1e-6)
    amount_still_authorized = math.isclose(
        exact_notional,
        intended_notional,
        rel_tol=1e-6,
        abs_tol=numeric_tolerance,
    )
    capital_available = exact_notional <= available_for_swap + numeric_tolerance
    if not amount_still_authorized or not capital_available or exact_units <= 0.0:
        return True

    position = portfolio.positions.get(intent.token_mint)
    if position is None:
        position = PaperPosition(intent.token_mint, scout_wallet, intent.observed_at)
        portfolio.ledger.register_position(position, family=family, context=context)

    fill = SimulatedFill(
        token_mint=intent.token_mint,
        side="buy",
        observed_at=intent.observed_at,
        reference_price=evidence.order_effective_price_sol,
        fill_price=evidence.order_effective_price_sol,
        notional_usd=exact_notional,
        units=exact_units,
        execution_drag_usd=network_fee_usd,
        intent=intent.kind,
    )
    total_cash_cost = exact_notional + network_fee_usd
    portfolio.cash_usd -= total_cash_cost
    position.units += exact_units
    position.cost_basis_usd += total_cash_cost
    position.entry_capital_usd += total_cash_cost
    position.fills.append(fill)
    portfolio.ledger.record_equity({intent.token_mint: evidence.order_effective_price_sol})
    return True


def install_execution_realism() -> None:
    """Compatibility bridge for the pre-Phase-18 composition installer."""

    from .portfolio import PaperPortfolio
    from .shadow_execution import ShadowWalletExecutableQuoteHandoff

    setattr(ShadowWalletExecutableQuoteHandoff.observe, "_roi_execution_realism", True)
    setattr(PaperPortfolio.apply, "_roi_execution_realism", True)


__all__ = [
    "ObservedEntryExecution",
    "apply_exact_entry_if_available",
    "clear_observed_entry_execution",
    "enrich_exact_entry_quote",
    "install_execution_realism",
]
