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


def clear_observed_entry_execution() -> None:
    """Clear candidate-local exact-entry evidence before a new handoff begins."""

    _CURRENT_ENTRY_EXECUTION.set(None)


def enrich_exact_entry_quote(
    *,
    store: Any,
    quote: ExecutableQuote,
    shadow: Any,
) -> ExecutableQuote:
    """Attach exact unsigned-order economics without replacing runtime methods.

    ``shadow`` is the just-recorded unsigned Jupiter/mainnet simulation result.
    The function publishes the same paper execution-cost observation previously
    emitted by the runtime wrapper and stores one consume-once exact-entry context.
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

    signature_fee = _fee(getattr(shadow, "signature_fee_lamports", None))
    priority_fee = _fee(getattr(shadow, "prioritization_fee_lamports", None))
    rent_fee = _fee(getattr(shadow, "rent_fee_lamports", None))
    network_fee_lamports = signature_fee + priority_fee + rent_fee
    network_fee_usd = (network_fee_lamports / LAMPORTS_PER_SOL) * float(quote.sol_usd)
    simulation_ok = bool(getattr(shadow, "simulation_ok", False))
    exact_order_available = bool(order_price > 0.0 and order_units > 0.0)
    router = str(getattr(shadow, "router", None) or quote.router)

    exact_quote = replace(
        quote,
        effective_price_sol=order_price if exact_order_available else quote.effective_price_sol,
        output_token_units=order_units if exact_order_available else quote.output_token_units,
        drift_fraction=order_drift if exact_order_available else quote.drift_fraction,
        router=router,
        usable=bool(quote.usable and simulation_ok and exact_order_available),
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
            "assembled_taker_order_price_available": exact_order_available,
            "paper_fill_uses_assembled_taker_order_price": bool(exact_quote.usable),
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
    """Apply one exact simulated entry through the canonical portfolio ledger."""

    evidence = _CURRENT_ENTRY_EXECUTION.get()
    use_exact = bool(
        intent.kind in _ENTRY_KINDS
        and evidence is not None
        and evidence.simulation_ok
        and evidence.token_mint == intent.token_mint
        and evidence.order_effective_price_sol > 0.0
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

    position = portfolio.positions.get(intent.token_mint)
    if position is None:
        position = PaperPosition(intent.token_mint, scout_wallet, intent.observed_at)
        portfolio.ledger.register_position(position, family=family, context=context)

    network_fee_usd = max(0.0, float(evidence.network_fee_usd))
    full = portfolio.full_position_notional({intent.token_mint: evidence.order_effective_price_sol})
    available_for_swap = max(0.0, portfolio.cash_usd - network_fee_usd)
    notional = min(available_for_swap, full * intent.fraction_of_full_position)
    if notional <= 0.0:
        return True

    units = notional / evidence.order_effective_price_sol
    fill = SimulatedFill(
        token_mint=intent.token_mint,
        side="buy",
        observed_at=intent.observed_at,
        reference_price=evidence.order_effective_price_sol,
        fill_price=evidence.order_effective_price_sol,
        notional_usd=notional,
        units=units,
        execution_drag_usd=network_fee_usd,
        intent=intent.kind,
    )
    total_cash_cost = notional + network_fee_usd
    portfolio.cash_usd -= total_cash_cost
    position.units += units
    position.cost_basis_usd += total_cash_cost
    position.entry_capital_usd += total_cash_cost
    position.fills.append(fill)
    portfolio.ledger.record_equity({intent.token_mint: evidence.order_effective_price_sol})
    return True


__all__ = [
    "ObservedEntryExecution",
    "apply_exact_entry_if_available",
    "clear_observed_entry_execution",
    "enrich_exact_entry_quote",
]
