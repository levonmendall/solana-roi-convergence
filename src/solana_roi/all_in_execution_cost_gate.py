from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Callable

from .activation import ActivationDecision, CandidateActivationGate
from .quote import ExecutableQuote, LAMPORTS_PER_SOL
from .shadow_execution import ShadowWalletExecutableQuoteHandoff


@dataclass(frozen=True, slots=True)
class AllInExecutionEconomics:
    token_mint: str
    stage: str
    completed_at: datetime
    swap_input_lamports: int
    swap_input_sol: float
    output_token_units: float
    route_effective_price_sol: float
    route_drift_fraction: float
    signature_fee_lamports: int
    prioritization_fee_lamports: int
    rent_fee_lamports: int
    network_fee_lamports: int
    network_fee_usd: float
    network_fee_fraction_of_requested_notional: float
    all_in_effective_price_sol: float
    all_in_drift_fraction: float


_CURRENT_ALL_IN_EXECUTION: ContextVar[AllInExecutionEconomics | None] = ContextVar(
    "solana_roi_all_in_execution_economics",
    default=None,
)


def _matching_shadow_row(
    handoff: ShadowWalletExecutableQuoteHandoff,
    quote: ExecutableQuote,
) -> dict[str, Any] | None:
    completed = quote.received_at.isoformat()
    for row in handoff.shadow_ledger.recent(64):
        if (
            str(row.get("token_mint") or "") == quote.token_mint
            and str(row.get("stage") or "") == quote.stage
            and str(row.get("completed_at") or "") == completed
        ):
            return row
    return None


def _required_nonnegative_int(row: dict[str, Any], name: str) -> int | None:
    value = row.get(name)
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _positive_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0.0 else None


def _all_in_handoff(
    original: Callable[..., Any],
) -> Callable[..., Any]:
    async def observe(
        self: ShadowWalletExecutableQuoteHandoff,
        *args: Any,
        **kwargs: Any,
    ) -> ExecutableQuote | None:
        # Candidate-local state must never leak from an earlier observation.
        _CURRENT_ALL_IN_EXECUTION.set(None)
        quote = await original(self, *args, **kwargs)
        if quote is None:
            return None

        shadow = _matching_shadow_row(self, quote)
        missing: list[str] = []
        if shadow is None:
            missing.append("matching_shadow_execution")
        else:
            input_lamports = _required_nonnegative_int(shadow, "input_lamports")
            signature_fee = _required_nonnegative_int(shadow, "signature_fee_lamports")
            priority_fee = _required_nonnegative_int(shadow, "prioritization_fee_lamports")
            rent_fee = _required_nonnegative_int(shadow, "rent_fee_lamports")
            output_units = _positive_float(shadow.get("order_out_token_units"))
            route_price = _positive_float(quote.effective_price_sol)
            scout_price = _positive_float(quote.scout_reference_price_sol)
            sol_usd = _positive_float(quote.sol_usd)
            if input_lamports is None or input_lamports <= 0:
                missing.append("input_lamports")
            if signature_fee is None:
                missing.append("signature_fee_lamports")
            if priority_fee is None:
                missing.append("prioritization_fee_lamports")
            if rent_fee is None:
                missing.append("rent_fee_lamports")
            if output_units is None:
                missing.append("order_out_token_units")
            if route_price is None:
                missing.append("order_effective_price_sol")
            if scout_price is None:
                missing.append("scout_reference_price_sol")
            if sol_usd is None:
                missing.append("sol_usd")
            if not bool(shadow.get("simulation_ok")):
                missing.append("successful_simulation")

        evidence_complete = not missing
        observed_at = quote.received_at
        if not evidence_complete or shadow is None:
            self.store.append(
                "all_in_execution_cost_observation",
                observed_at.isoformat(),
                {
                    "token_mint": quote.token_mint,
                    "stage": quote.stage,
                    "paper_only": True,
                    "live_money_authority": False,
                    "evidence_complete": False,
                    "missing": missing,
                    "within_all_in_chase_ceiling": False,
                    "max_chase_fraction": self.max_chase_fraction,
                },
            )
            # Production may not treat an otherwise usable execution as admissible
            # when its exact fee burden is unknown. Zero is valid only when it was
            # actually observed as zero; absent fields fail closed.
            if quote.usable:
                return replace(
                    quote,
                    usable=False,
                    reason="all-in execution cost unavailable: " + ",".join(missing),
                )
            return quote

        assert input_lamports is not None
        assert signature_fee is not None
        assert priority_fee is not None
        assert rent_fee is not None
        assert output_units is not None
        assert route_price is not None
        assert scout_price is not None
        assert sol_usd is not None

        swap_input_sol = input_lamports / LAMPORTS_PER_SOL
        network_fee_lamports = signature_fee + priority_fee + rent_fee
        network_fee_sol = network_fee_lamports / LAMPORTS_PER_SOL
        network_fee_usd = network_fee_sol * sol_usd
        all_in_price = (swap_input_sol + network_fee_sol) / output_units
        all_in_drift = all_in_price / scout_price - 1.0
        requested = max(0.0, float(quote.requested_notional_usd))
        network_fraction = network_fee_usd / requested if requested > 0.0 else float("inf")
        route_drift = float(quote.drift_fraction)

        economics = AllInExecutionEconomics(
            token_mint=quote.token_mint,
            stage=quote.stage,
            completed_at=quote.received_at,
            swap_input_lamports=input_lamports,
            swap_input_sol=swap_input_sol,
            output_token_units=output_units,
            route_effective_price_sol=route_price,
            route_drift_fraction=route_drift,
            signature_fee_lamports=signature_fee,
            prioritization_fee_lamports=priority_fee,
            rent_fee_lamports=rent_fee,
            network_fee_lamports=network_fee_lamports,
            network_fee_usd=network_fee_usd,
            network_fee_fraction_of_requested_notional=network_fraction,
            all_in_effective_price_sol=all_in_price,
            all_in_drift_fraction=all_in_drift,
        )
        _CURRENT_ALL_IN_EXECUTION.set(economics)
        self.store.append(
            "all_in_execution_cost_observation",
            observed_at.isoformat(),
            {
                "token_mint": quote.token_mint,
                "stage": quote.stage,
                "paper_only": True,
                "live_money_authority": False,
                "evidence_complete": True,
                "requested_notional_usd": requested,
                "swap_input_lamports": input_lamports,
                "swap_input_sol": swap_input_sol,
                "output_token_units": output_units,
                "route_effective_price_sol": route_price,
                "route_drift_fraction": route_drift,
                "route_cost_embedded_in_net_order_price": True,
                "signature_fee_lamports": signature_fee,
                "prioritization_fee_lamports": priority_fee,
                "rent_fee_lamports": rent_fee,
                "rent_treated_as_conservative_cash_cost": True,
                "network_fee_lamports": network_fee_lamports,
                "network_fee_usd": network_fee_usd,
                "network_fee_fraction_of_requested_notional": network_fraction,
                "all_in_effective_price_sol": all_in_price,
                "all_in_drift_fraction": all_in_drift,
                "max_chase_fraction": self.max_chase_fraction,
                "within_all_in_chase_ceiling": all_in_drift <= self.max_chase_fraction,
            },
        )
        return quote

    try:
        observe.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(observe, "_roi_all_in_execution_cost_gate", True)
    return observe


def _matching_economics(quote: ExecutableQuote | None) -> AllInExecutionEconomics | None:
    if quote is None:
        return None
    economics = _CURRENT_ALL_IN_EXECUTION.get()
    if economics is None:
        return None
    if economics.token_mint != quote.token_mint or economics.stage != quote.stage:
        return None
    if economics.completed_at != quote.received_at:
        return None
    return economics


def _all_in_activation(
    original: Callable[..., ActivationDecision],
) -> Callable[..., ActivationDecision]:
    def evaluate(self: CandidateActivationGate, *args: Any, **kwargs: Any) -> ActivationDecision:
        quote = kwargs.get("quote")
        economics = _matching_economics(quote if isinstance(quote, ExecutableQuote) else None)
        custom_blockers: list[str] = []
        required_cash_usd: float | None = None

        if economics is not None and isinstance(quote, ExecutableQuote):
            if economics.all_in_drift_fraction > self.engine.config.max_chase_fraction:
                custom_blockers.append("all_in_execution_cost_exceeds_chase_ceiling")
            required_cash_usd = float(quote.requested_notional_usd) + economics.network_fee_usd
            if required_cash_usd > self.engine.portfolio.cash_usd + 1e-9:
                custom_blockers.append("insufficient_paper_buying_power_including_network_cost")

        call_kwargs = dict(kwargs)
        if custom_blockers and isinstance(quote, ExecutableQuote):
            # Feed a fail-closed quote into the existing sole authorization gate.
            # This guarantees the underlying gate itself cannot emit an authorized
            # decision before the wrapper adds the more specific economic reason.
            call_kwargs["quote"] = replace(
                quote,
                usable=False,
                reason="all-in execution economics rejected candidate",
            )

        decision = original(self, *args, **call_kwargs)
        if custom_blockers:
            blockers = tuple(dict.fromkeys((*decision.blockers, *custom_blockers)))
            decision = ActivationDecision(
                False,
                "record_only",
                decision.token_mint,
                decision.stage,
                decision.decision_at,
                blockers,
            )

        if economics is not None and isinstance(quote, ExecutableQuote):
            self.store.append(
                "all_in_execution_cost_decision",
                decision.decision_at.isoformat(),
                {
                    "authorized": decision.authorized,
                    "code": decision.code,
                    "token_mint": decision.token_mint,
                    "stage": decision.stage,
                    "paper_only": True,
                    "live_money_authority": False,
                    "blockers": list(decision.blockers),
                    "requested_notional_usd": quote.requested_notional_usd,
                    "route_effective_price_sol": economics.route_effective_price_sol,
                    "route_drift_fraction": economics.route_drift_fraction,
                    "signature_fee_lamports": economics.signature_fee_lamports,
                    "prioritization_fee_lamports": economics.prioritization_fee_lamports,
                    "rent_fee_lamports": economics.rent_fee_lamports,
                    "network_fee_lamports": economics.network_fee_lamports,
                    "network_fee_usd": economics.network_fee_usd,
                    "network_fee_fraction_of_requested_notional": economics.network_fee_fraction_of_requested_notional,
                    "all_in_effective_price_sol": economics.all_in_effective_price_sol,
                    "all_in_drift_fraction": economics.all_in_drift_fraction,
                    "max_chase_fraction": self.engine.config.max_chase_fraction,
                    "required_cash_including_network_cost_usd": required_cash_usd,
                    "paper_cash_usd": self.engine.portfolio.cash_usd,
                    "all_in_gate_passed": not custom_blockers,
                },
            )
        return decision

    try:
        evaluate.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(evaluate, "_roi_all_in_execution_cost_gate", True)
    return evaluate


def install_all_in_execution_cost_gate() -> None:
    observe = ShadowWalletExecutableQuoteHandoff.observe
    if not bool(getattr(observe, "_roi_all_in_execution_cost_gate", False)):
        ShadowWalletExecutableQuoteHandoff.observe = _all_in_handoff(observe)  # type: ignore[method-assign]

    evaluate = CandidateActivationGate.evaluate
    if not bool(getattr(evaluate, "_roi_all_in_execution_cost_gate", False)):
        CandidateActivationGate.evaluate = _all_in_activation(evaluate)  # type: ignore[method-assign]


__all__ = ["AllInExecutionEconomics", "install_all_in_execution_cost_gate"]
