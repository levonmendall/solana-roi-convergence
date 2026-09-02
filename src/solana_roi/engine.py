from __future__ import annotations

from dataclasses import asdict
from datetime import datetime

from .config import BASELINE, StrategyConfig
from .models import Confirmation, RiskSnapshot, WalletTouch
from .portfolio import PaperPortfolio
from .storage import AppendOnlyEventStore
from .strategy import RoiConvergenceStrategy


class PaperTradingEngine:
    """Coordinates point-in-time signals, strategy decisions, paper fills, and evidence."""

    def __init__(self, *, config: StrategyConfig = BASELINE, strategy: RoiConvergenceStrategy | None = None, portfolio: PaperPortfolio | None = None, store: AppendOnlyEventStore | None = None):
        self.config = config
        self.strategy = strategy or RoiConvergenceStrategy(config)
        self.portfolio = portfolio or PaperPortfolio(config)
        self.store = store
        self.marks: dict[str, float] = {}

    def _append(self, event_type: str, observed_at: datetime, payload: dict[str, object]) -> None:
        if self.store is not None:
            self.store.append(event_type, observed_at.isoformat(), payload)

    def on_first_touch(self, touch: WalletTouch, risk: RiskSnapshot, *, execution_price: float | None = None) -> None:
        fill_reference = execution_price if execution_price is not None else touch.reference_price
        self.marks[touch.token_mint] = fill_reference
        self._append("first_touch", touch.observed_at, {"touch": asdict(touch), "risk": asdict(risk), "strategy_version": self.config.version})
        for intent in self.strategy.first_touch(touch, risk):
            self.portfolio.apply(intent, scout_wallet=touch.wallet, reference_price=fill_reference)
            self._append("trade_intent", intent.observed_at, {**asdict(intent), "execution_reference_price": fill_reference})

    def on_confirmation(self, confirmation: Confirmation, risk: RiskSnapshot, *, execution_price: float | None = None) -> None:
        fill_reference = execution_price if execution_price is not None else confirmation.reference_price
        self.marks[confirmation.token_mint] = fill_reference
        self._append("confirmation", confirmation.observed_at, {"confirmation": asdict(confirmation), "risk": asdict(risk)})
        candidate = self.strategy.candidates.get(confirmation.token_mint)
        scout_wallet = candidate.scout_wallet if candidate else confirmation.wallet
        for intent in self.strategy.confirm(confirmation, risk):
            self.portfolio.apply(intent, scout_wallet=scout_wallet, reference_price=fill_reference)
            self._append("trade_intent", intent.observed_at, {**asdict(intent), "execution_reference_price": fill_reference})

    def on_price(self, token_mint: str, observed_at: datetime, reference_price: float) -> None:
        self.marks[token_mint] = reference_price
        self._append("price", observed_at, {"token_mint": token_mint, "reference_price": reference_price})
        candidate = self.strategy.candidates.get(token_mint)
        scout_wallet = candidate.scout_wallet if candidate else "unknown"
        before_closed = len(self.portfolio.closed)
        for intent in self.strategy.on_clock(token_mint, observed_at, reference_price):
            self.portfolio.apply(intent, scout_wallet=scout_wallet, reference_price=reference_price)
            self._append("trade_intent", intent.observed_at, {**asdict(intent), "execution_reference_price": reference_price})
        for outcome in self.portfolio.closed[before_closed:]:
            self._append("trade_outcome", outcome.closed_at, asdict(outcome))

    @property
    def nav_usd(self) -> float:
        return self.portfolio.nav(self.marks)
