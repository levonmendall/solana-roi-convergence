from __future__ import annotations

from datetime import datetime

from .config import BASELINE, StrategyConfig
from .models import IntentKind, SimulatedFill


class ExecutionSimulator:
    """Conservative spot-fill simulator; never constructs or submits a transaction."""

    def __init__(self, config: StrategyConfig = BASELINE):
        self.config = config

    def buy(self, *, token_mint: str, observed_at: datetime, reference_price: float, notional_usd: float, intent: IntentKind) -> SimulatedFill:
        drag = self.config.execution_drag_per_side_fraction
        fill_price = reference_price * (1.0 + drag)
        units = notional_usd / fill_price
        ideal_units = notional_usd / reference_price
        execution_drag_usd = max(0.0, (ideal_units - units) * reference_price)
        return SimulatedFill(token_mint, "buy", observed_at, reference_price, fill_price, notional_usd, units, execution_drag_usd, intent)

    def sell(self, *, token_mint: str, observed_at: datetime, reference_price: float, units: float, intent: IntentKind) -> SimulatedFill:
        drag = self.config.execution_drag_per_side_fraction
        fill_price = reference_price * (1.0 - drag)
        notional_usd = units * fill_price
        execution_drag_usd = max(0.0, units * reference_price - notional_usd)
        return SimulatedFill(token_mint, "sell", observed_at, reference_price, fill_price, notional_usd, units, execution_drag_usd, intent)
