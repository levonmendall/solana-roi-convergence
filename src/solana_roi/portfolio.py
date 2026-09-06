from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Mapping

from .config import BASELINE, StrategyConfig
from .execution import ExecutionSimulator
from .models import IntentKind, PaperPosition, SimulatedFill, TradeIntent, TradeOutcome

PORTFOLIO_CORE_VERSION = "v51-canonical-paper-capital-ledger-127-v3-native-execution-realism"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False


def max_drawdown(equity_curve: Iterable[float]) -> float:
    peak = 0.0
    worst = 0.0
    for raw in equity_curve:
        value = max(0.0, float(raw))
        peak = max(peak, value)
        if peak > 0.0:
            worst = max(worst, 1.0 - value / peak)
    return worst


def aggregate_exposure(weights: Mapping[str, float]) -> dict[str, Any]:
    normalized = {str(name): max(0.0, float(value)) for name, value in weights.items()}
    invested = min(1.0, sum(normalized.values()))
    return {
        "family_weights": normalized,
        "invested_weight": invested,
        "cash_weight": max(0.0, 1.0 - invested),
        "family_count": len(normalized),
        "paper_only": True,
        "live_money_authority": False,
    }


def allocate_family_capital(
    scores: Mapping[str, float],
    *,
    priority: Iterable[str] | None = None,
    family_cap: float | None = None,
) -> dict[str, Any]:
    """Canonical frozen-v5.1 family allocation policy.

    This is a pure consolidation of the allocation algorithm previously embedded in
    proof merging. It does not change the active cap, ranking, or economics.
    """
    from .strategy_v51_authority import authority

    spec = authority()
    ordered_priority = list(priority or spec["research_family_priority"])
    cap = float(spec["allocation"]["immature_family_max_weight"] if family_cap is None else family_cap)
    if cap < 0.0 or cap > 1.0:
        raise ValueError("family_cap must be within [0, 1]")
    clean_scores = {str(name): float(value) for name, value in scores.items()}
    ordered = sorted(
        set(ordered_priority) | set(clean_scores),
        key=lambda family: (
            -clean_scores.get(family, 0.0),
            ordered_priority.index(family) if family in ordered_priority else len(ordered_priority),
            family,
        ),
    )
    positive = [family for family in ordered if clean_scores.get(family, 0.0) > 0.0]
    total = sum(clean_scores[family] for family in positive)
    weights: dict[str, float] = {}
    remaining = 1.0
    for family in positive:
        raw = clean_scores[family] / total if total > 0.0 else 0.0
        weight = min(cap, raw, remaining)
        if weight > 0.0:
            weights[family] = weight
        remaining = max(0.0, remaining - weight)
    exposure = aggregate_exposure(weights)
    return {
        "portfolio_core_version": PORTFOLIO_CORE_VERSION,
        "research_family_ranking": ordered,
        "paper_allocation_weights": weights,
        "paper_cash_weight": exposure["cash_weight"],
        "invested_weight": exposure["invested_weight"],
        "active_family_cap": cap,
        "allocation_policy_changed": False,
        "paper_only": True,
        "live_money_authority": False,
    }


def correlation_cap_diagnostic(
    weights: Mapping[str, float],
    correlations: Mapping[tuple[str, str], float | None],
    *,
    maximum_abs_correlation: float = 0.50,
) -> dict[str, Any]:
    """Read-only concentration diagnostic; it never changes active weights."""
    active = {name for name, weight in weights.items() if float(weight) > 0.0}
    mature_pairs = 0
    excessive: list[dict[str, Any]] = []
    unknown: list[tuple[str, str]] = []
    for (left, right), raw in correlations.items():
        if left not in active or right not in active or left == right:
            continue
        if raw is None:
            unknown.append((left, right))
            continue
        value = float(raw)
        mature_pairs += 1
        if abs(value) > maximum_abs_correlation:
            excessive.append({"left": left, "right": right, "correlation": value})
    return {
        "portfolio_core_version": PORTFOLIO_CORE_VERSION,
        "mature_pair_count": mature_pairs,
        "unknown_pairs": unknown,
        "excessive_pairs": excessive,
        "maximum_abs_correlation_reference": maximum_abs_correlation,
        "changes_active_allocation": False,
        "unknown_correlation_assumed_zero": False,
        "paper_only": True,
        "live_money_authority": False,
    }


@dataclass
class CanonicalPaperCapitalLedger:
    """Single in-memory owner for paper capital and position accounting.

    Economic outcomes are attributed to the capital actually committed to the
    position. Whole-portfolio NAV is never used as a trade-return denominator.
    This object is deliberately paper-only and contains no signing or submission
    capability.
    """

    initial_capital_usd: float
    cash_usd: float = field(init=False)
    positions: dict[str, PaperPosition] = field(default_factory=dict)
    closed: list[TradeOutcome] = field(default_factory=list)
    pending_exits: set[str] = field(default_factory=set)
    family_by_mint: dict[str, str] = field(default_factory=dict)
    context_by_mint: dict[str, str] = field(default_factory=dict)
    equity_history: list[float] = field(default_factory=list)

    def __post_init__(self) -> None:
        value = float(self.initial_capital_usd)
        if value <= 0.0:
            raise ValueError("initial paper capital must be positive")
        self.initial_capital_usd = value
        self.cash_usd = value
        self.equity_history.append(value)

    def nav(self, marks: Mapping[str, float] | None = None) -> float:
        marks = marks or {}
        value = float(self.cash_usd)
        for mint, position in self.positions.items():
            if position.is_open:
                price = float(marks.get(mint, position.average_entry_price or 0.0))
                value += position.units * price
        return value

    def capital_committed_usd(self) -> float:
        return sum(max(0.0, float(position.cost_basis_usd)) for position in self.positions.values() if position.is_open)

    def realized_pnl_usd(self) -> float:
        return sum(float(position.realized_pnl_usd) for position in self.positions.values())

    def unrealized_pnl_usd(self, marks: Mapping[str, float] | None = None) -> float:
        marks = marks or {}
        total = 0.0
        for mint, position in self.positions.items():
            if not position.is_open:
                continue
            mark = float(marks.get(mint, position.average_entry_price or 0.0))
            total += position.units * mark - position.cost_basis_usd
        return total

    def all_fills(self) -> tuple[SimulatedFill, ...]:
        return tuple(fill for position in self.positions.values() for fill in position.fills)

    def register_position(self, position: PaperPosition, *, family: str, context: str) -> None:
        self.positions[position.token_mint] = position
        self.family_by_mint[position.token_mint] = str(family)
        self.context_by_mint[position.token_mint] = str(context)

    def mark_exit_pending(self, mint: str) -> None:
        if mint in self.positions and self.positions[mint].is_open:
            self.pending_exits.add(mint)

    def clear_exit_pending(self, mint: str) -> None:
        self.pending_exits.discard(mint)

    def record_equity(self, marks: Mapping[str, float] | None = None) -> float:
        value = self.nav(marks)
        self.equity_history.append(value)
        return value

    def snapshot(self, marks: Mapping[str, float] | None = None) -> dict[str, Any]:
        nav = self.nav(marks)
        open_positions = [position for position in self.positions.values() if position.is_open]
        allocation: dict[str, float] = {}
        for position in open_positions:
            family = self.family_by_mint.get(position.token_mint, "unclassified")
            allocation[family] = allocation.get(family, 0.0) + max(0.0, float(position.cost_basis_usd))
        return {
            "portfolio_core_version": PORTFOLIO_CORE_VERSION,
            "initial_capital_usd": self.initial_capital_usd,
            "cash_usd": self.cash_usd,
            "strategy_nav_usd": nav,
            "capital_committed_usd": self.capital_committed_usd(),
            "realized_pnl_usd": self.realized_pnl_usd(),
            "unrealized_pnl_usd": self.unrealized_pnl_usd(marks),
            "open_paper_position_count": len(open_positions),
            "simulated_fill_count": len(self.all_fills()),
            "pending_exits": sorted(self.pending_exits),
            "family_capital_committed_usd": allocation,
            "max_drawdown": max_drawdown(self.equity_history),
            "trade_return_denominator": "actual_position_entry_capital",
            "whole_nav_trade_attribution": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }


class PaperPortfolio:
    """Compatibility facade over the canonical paper capital ledger."""

    def __init__(self, config: StrategyConfig = BASELINE, execution: ExecutionSimulator | None = None):
        if not config.paper_only:
            raise ValueError("paper_only must remain true")
        self.config = config
        self.execution = execution or ExecutionSimulator(config)
        self.ledger = CanonicalPaperCapitalLedger(float(config.initial_capital_usd))
        self.initial_capital_usd = self.ledger.initial_capital_usd
        self.positions = self.ledger.positions
        self.closed = self.ledger.closed

    @property
    def cash_usd(self) -> float:
        return self.ledger.cash_usd

    @cash_usd.setter
    def cash_usd(self, value: float) -> None:
        self.ledger.cash_usd = float(value)

    def nav(self, marks: dict[str, float] | None = None) -> float:
        return self.ledger.nav(marks)

    def full_position_notional(self, marks: dict[str, float] | None = None) -> float:
        return self.nav(marks) * self.config.full_position_fraction_of_nav

    def apply(
        self,
        intent: TradeIntent,
        *,
        scout_wallet: str,
        reference_price: float,
        family: str = "legacy_runtime",
        context: str = "unclassified",
    ) -> None:
        position = self.positions.get(intent.token_mint)
        if intent.kind in {IntentKind.OPEN_STARTER, IntentKind.OPEN_FULL, IntentKind.ADD_CONFIRMATION}:
            # Repair 126 migration: exact entry realism is an ordinary dependency,
            # not a runtime replacement of this method.
            from .execution_realism import apply_exact_entry_if_available

            if apply_exact_entry_if_available(
                self,
                intent,
                scout_wallet=scout_wallet,
                reference_price=reference_price,
                family=family,
                context=context,
            ):
                return
            if position is None:
                position = PaperPosition(intent.token_mint, scout_wallet, intent.observed_at)
                self.ledger.register_position(position, family=family, context=context)
            full = self.full_position_notional({intent.token_mint: reference_price})
            notional = min(self.cash_usd, full * intent.fraction_of_full_position)
            if notional <= 0:
                return
            fill = self.execution.buy(
                token_mint=intent.token_mint,
                observed_at=intent.observed_at,
                reference_price=reference_price,
                notional_usd=notional,
                intent=intent.kind,
            )
            self.cash_usd -= notional
            position.units += fill.units
            position.cost_basis_usd += notional
            position.entry_capital_usd += notional
            position.fills.append(fill)
            self.ledger.record_equity({intent.token_mint: reference_price})
            return
        if position is None or not position.is_open:
            return
        if intent.kind is IntentKind.HARVEST:
            units_to_sell = position.units * self.config.harvest_fraction
            position.harvest_hit = True
            self._sell(position, intent, reference_price, units_to_sell)
            position.runner_units = position.units
            position.high_water_price = reference_price
            self.ledger.record_equity({intent.token_mint: reference_price})
            return
        if intent.kind in {IntentKind.EXIT_STARTER, IntentKind.EXIT_THESIS, IntentKind.EXIT_STOP, IntentKind.EXIT_RUNNER}:
            self.ledger.mark_exit_pending(position.token_mint)
            try:
                self._sell(position, intent, reference_price, position.units)
                self._close_outcome(position, intent.observed_at, intent.reason or intent.kind.value)
            finally:
                self.ledger.clear_exit_pending(position.token_mint)
            self.ledger.record_equity({intent.token_mint: reference_price})

    def _sell(self, position: PaperPosition, intent: TradeIntent, reference_price: float, units: float) -> None:
        units = max(0.0, min(units, position.units))
        if units <= 0:
            return
        pre_units = position.units
        basis_fraction = units / pre_units
        basis_released = position.cost_basis_usd * basis_fraction
        fill = self.execution.sell(
            token_mint=position.token_mint,
            observed_at=intent.observed_at,
            reference_price=reference_price,
            units=units,
            intent=intent.kind,
        )
        self.cash_usd += fill.notional_usd
        position.realized_pnl_usd += fill.notional_usd - basis_released
        position.units -= units
        position.cost_basis_usd -= basis_released
        position.fills.append(fill)
        if position.units <= 1e-15:
            position.units = 0.0
            position.cost_basis_usd = 0.0

    def _close_outcome(self, position: PaperPosition, observed_at: datetime, reason: str) -> None:
        position.closed_at = observed_at
        position.closed_reason = reason
        committed = float(position.entry_capital_usd)
        pnl = float(position.realized_pnl_usd)
        ending_capital = committed + pnl
        self.closed.append(
            TradeOutcome(
                position.token_mint,
                position.scout_wallet,
                position.opened_at,
                observed_at,
                committed,
                ending_capital,
                pnl,
                (pnl / committed) if committed else 0.0,
                position.harvest_hit,
                reason,
            )
        )

    def accounting_status(self, marks: dict[str, float] | None = None) -> dict[str, Any]:
        return self.ledger.snapshot(marks)


__all__ = [
    "PORTFOLIO_CORE_VERSION",
    "CanonicalPaperCapitalLedger",
    "PaperPortfolio",
    "aggregate_exposure",
    "allocate_family_capital",
    "correlation_cap_diagnostic",
    "max_drawdown",
]
