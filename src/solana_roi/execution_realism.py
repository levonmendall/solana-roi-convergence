from __future__ import annotations

import math
from contextvars import ContextVar
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Callable

from .models import IntentKind, PaperPosition, SimulatedFill
from .portfolio import PaperPortfolio
from .quote import ExecutableQuote, LAMPORTS_PER_SOL
from .shadow_execution import ShadowWalletExecutableQuoteHandoff
from .v51_exact_exit_execution import install_exact_exit_execution_model


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


def _matching_shadow_row(
    handoff: ShadowWalletExecutableQuoteHandoff,
    quote: ExecutableQuote,
) -> dict[str, Any] | None:
    """Find the exact shadow observation that produced this returned quote.

    The original handoff stamps the final quote's ``received_at`` with the shadow
    observation's ``completed_at``. Matching all three fields avoids accidentally
    borrowing fees from a concurrent candidate.
    """

    completed = quote.received_at.isoformat()
    for row in handoff.shadow_ledger.recent(64):
        if (
            str(row.get("token_mint") or "") == quote.token_mint
            and str(row.get("stage") or "") == quote.stage
            and str(row.get("completed_at") or "") == completed
        ):
            return row
    return None


def _fee(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _observed_handoff(
    original: Callable[..., Any],
) -> Callable[..., Any]:
    async def observe(self: ShadowWalletExecutableQuoteHandoff, *args: Any, **kwargs: Any) -> ExecutableQuote | None:
        # Never let a previous candidate's observed execution leak into a later
        # path. A fresh handoff either publishes one exact entry context or none.
        _CURRENT_ENTRY_EXECUTION.set(None)
        quote = await original(self, *args, **kwargs)
        if quote is None:
            return None

        shadow = _matching_shadow_row(self, quote)
        if shadow is None:
            return quote

        try:
            order_price = float(shadow.get("order_effective_price_sol") or 0.0)
            order_units = float(shadow.get("order_out_token_units") or 0.0)
            order_drift = float(shadow.get("order_drift_fraction"))
        except (TypeError, ValueError):
            order_price = 0.0
            order_units = 0.0
            order_drift = quote.drift_fraction

        signature_fee = _fee(shadow.get("signature_fee_lamports"))
        priority_fee = _fee(shadow.get("prioritization_fee_lamports"))
        rent_fee = _fee(shadow.get("rent_fee_lamports"))
        network_fee_lamports = signature_fee + priority_fee + rent_fee
        network_fee_usd = (network_fee_lamports / LAMPORTS_PER_SOL) * float(quote.sol_usd)
        simulation_ok = bool(shadow.get("simulation_ok"))
        exact_order_available = bool(order_price > 0.0 and order_units > 0.0)

        # Jupiter's assembled taker order ``outAmount`` is already net of route
        # execution effects. Its effective price therefore replaces the earlier
        # quote-only price for paper accounting rather than receiving another
        # fixed 2.5% entry haircut on top.
        exact_quote = replace(
            quote,
            effective_price_sol=order_price if exact_order_available else quote.effective_price_sol,
            output_token_units=order_units if exact_order_available else quote.output_token_units,
            drift_fraction=order_drift if exact_order_available else quote.drift_fraction,
            router=str(shadow.get("router") or quote.router),
            usable=bool(quote.usable and simulation_ok and exact_order_available),
            reason=(
                quote.reason
                if quote.usable and simulation_ok and exact_order_available
                else quote.reason
            ),
        )

        payload = {
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
            "router": str(shadow.get("router") or quote.router),
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
        }
        self.store.append("paper_execution_cost_observation", exact_quote.received_at.isoformat(), payload)

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

    try:
        observe.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(observe, "_roi_execution_realism", True)
    return observe


def _exact_entry_apply(
    original: Callable[..., None],
) -> Callable[..., None]:
    def apply(
        self: PaperPortfolio,
        intent: Any,
        *,
        scout_wallet: str,
        reference_price: float,
    ) -> None:
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
            return original(self, intent, scout_wallet=scout_wallet, reference_price=reference_price)

        # Consume exactly once. A confirmation add receives its own independently
        # built and simulated Jupiter order and therefore its own context.
        _CURRENT_ENTRY_EXECUTION.set(None)

        position = self.positions.get(intent.token_mint)
        if position is None:
            position = PaperPosition(intent.token_mint, scout_wallet, intent.observed_at)
            self.positions[intent.token_mint] = position
            self._trade_start_nav[intent.token_mint] = self.nav(
                {intent.token_mint: evidence.order_effective_price_sol}
            )

        network_fee_usd = max(0.0, float(evidence.network_fee_usd))
        full = self.full_position_notional({intent.token_mint: evidence.order_effective_price_sol})
        available_for_swap = max(0.0, self.cash_usd - network_fee_usd)
        notional = min(available_for_swap, full * intent.fraction_of_full_position)
        if notional <= 0.0:
            return

        units = notional / evidence.order_effective_price_sol
        fill = SimulatedFill(
            token_mint=intent.token_mint,
            side="buy",
            observed_at=intent.observed_at,
            reference_price=evidence.order_effective_price_sol,
            fill_price=evidence.order_effective_price_sol,
            notional_usd=notional,
            units=units,
            # For exact observed entries this field is the explicit cash friction
            # not already embedded in the net Jupiter order price. Existing
            # checkpoint/event schemas therefore remain backward compatible.
            execution_drag_usd=network_fee_usd,
            intent=intent.kind,
        )
        total_cash_cost = notional + network_fee_usd
        self.cash_usd -= total_cash_cost
        position.units += units
        # Fee-inclusive basis makes later paper P&L and break-even accounting
        # reflect the actual observed entry cash requirement.
        position.cost_basis_usd += total_cash_cost
        position.entry_capital_usd += total_cash_cost
        position.fills.append(fill)

    try:
        apply.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(apply, "_roi_execution_realism", True)
    return apply


def install_execution_realism() -> None:
    handoff_observe = ShadowWalletExecutableQuoteHandoff.observe
    if not bool(getattr(handoff_observe, "_roi_execution_realism", False)):
        ShadowWalletExecutableQuoteHandoff.observe = _observed_handoff(handoff_observe)  # type: ignore[method-assign]

    portfolio_apply = PaperPortfolio.apply
    if not bool(getattr(portfolio_apply, "_roi_execution_realism", False)):
        PaperPortfolio.apply = _exact_entry_apply(portfolio_apply)  # type: ignore[method-assign]

    # Repairs 109-113 extend the already-existing execution-realism composition.
    # The new final/FOMO sell path never calls the legacy quote-only settlement;
    # exact held-size unsigned simulation is now the only current-epoch outcome path.
    install_exact_exit_execution_model()


__all__ = ["ObservedEntryExecution", "install_execution_realism"]
