from __future__ import annotations

from datetime import datetime

from .config import BASELINE, StrategyConfig
from .execution import ExecutionSimulator
from .models import IntentKind, PaperPosition, TradeIntent, TradeOutcome


class PaperPortfolio:
    """A continuously compounding, spot-only, paper ledger with a fixed $500 genesis."""

    def __init__(self, config: StrategyConfig = BASELINE, execution: ExecutionSimulator | None = None):
        if not config.paper_only:
            raise ValueError("paper_only must remain true")
        self.config = config
        self.execution = execution or ExecutionSimulator(config)
        self.initial_capital_usd = config.initial_capital_usd
        self.cash_usd = config.initial_capital_usd
        self.positions: dict[str, PaperPosition] = {}
        self.closed: list[TradeOutcome] = []
        self._trade_start_nav: dict[str, float] = {}

    def nav(self, marks: dict[str, float] | None = None) -> float:
        marks = marks or {}
        value = self.cash_usd
        for mint, position in self.positions.items():
            if position.is_open:
                price = marks.get(mint, position.average_entry_price or 0.0)
                value += position.units * price
        return value

    def full_position_notional(self, marks: dict[str, float] | None = None) -> float:
        return self.nav(marks) * self.config.full_position_fraction_of_nav

    def apply(self, intent: TradeIntent, *, scout_wallet: str, reference_price: float) -> None:
        position = self.positions.get(intent.token_mint)
        if intent.kind in {IntentKind.OPEN_STARTER, IntentKind.OPEN_FULL, IntentKind.ADD_CONFIRMATION}:
            if position is None:
                position = PaperPosition(intent.token_mint, scout_wallet, intent.observed_at)
                self.positions[intent.token_mint] = position
                self._trade_start_nav[intent.token_mint] = self.nav({intent.token_mint: reference_price})
            full = self.full_position_notional({intent.token_mint: reference_price})
            notional = min(self.cash_usd, full * intent.fraction_of_full_position)
            if notional <= 0:
                return
            fill = self.execution.buy(token_mint=intent.token_mint, observed_at=intent.observed_at, reference_price=reference_price, notional_usd=notional, intent=intent.kind)
            self.cash_usd -= notional
            position.units += fill.units
            position.cost_basis_usd += notional
            position.entry_capital_usd += notional
            position.fills.append(fill)
            return
        if position is None or not position.is_open:
            return
        if intent.kind is IntentKind.HARVEST:
            units_to_sell = position.units * self.config.harvest_fraction
            position.harvest_hit = True
            self._sell(position, intent, reference_price, units_to_sell)
            position.runner_units = position.units
            position.high_water_price = reference_price
            return
        if intent.kind in {IntentKind.EXIT_STARTER, IntentKind.EXIT_THESIS, IntentKind.EXIT_STOP, IntentKind.EXIT_RUNNER}:
            self._sell(position, intent, reference_price, position.units)
            self._close_outcome(position, intent.observed_at, intent.reason or intent.kind.value, reference_price)

    def _sell(self, position: PaperPosition, intent: TradeIntent, reference_price: float, units: float) -> None:
        units = max(0.0, min(units, position.units))
        if units <= 0:
            return
        pre_units = position.units
        basis_fraction = units / pre_units
        basis_released = position.cost_basis_usd * basis_fraction
        fill = self.execution.sell(token_mint=position.token_mint, observed_at=intent.observed_at, reference_price=reference_price, units=units, intent=intent.kind)
        self.cash_usd += fill.notional_usd
        position.realized_pnl_usd += fill.notional_usd - basis_released
        position.units -= units
        position.cost_basis_usd -= basis_released
        position.fills.append(fill)
        if position.units <= 1e-15:
            position.units = 0.0
            position.cost_basis_usd = 0.0

    def _close_outcome(self, position: PaperPosition, observed_at: datetime, reason: str, reference_price: float) -> None:
        position.closed_at = observed_at
        position.closed_reason = reason
        start_nav = self._trade_start_nav.pop(position.token_mint, self.initial_capital_usd)
        end_nav = self.nav({position.token_mint: reference_price})
        pnl = end_nav - start_nav
        self.closed.append(TradeOutcome(position.token_mint, position.scout_wallet, position.opened_at, observed_at, start_nav, end_nav, pnl, (pnl / start_nav) if start_nav else 0.0, position.harvest_hit, reason))
