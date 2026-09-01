from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass

from .config import BASELINE, StrategyConfig
from .models import TradeOutcome


@dataclass(frozen=True, slots=True)
class CertificationReport:
    status: str
    closed_trades: int
    unique_tokens: int
    hit_rate: float
    hit_rate_wilson_lower: float
    total_pnl_usd: float
    profit_factor: float
    geometric_growth: float
    pnl_ex_best_trade_usd: float
    pnl_ex_best_scout_usd: float
    blockers: tuple[str, ...]


def wilson_lower(successes: int, n: int, z: float = 1.959963984540054) -> float:
    if n <= 0:
        return 0.0
    p = successes / n
    denom = 1.0 + z * z / n
    centre = p + z * z / (2.0 * n)
    margin = z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * n)) / n)
    return max(0.0, (centre - margin) / denom)


class ProfitabilityCertifier:
    def __init__(self, config: StrategyConfig = BASELINE):
        self.config = config

    def evaluate(self, outcomes: list[TradeOutcome]) -> CertificationReport:
        n = len(outcomes)
        hits = sum(outcome.harvest_hit for outcome in outcomes)
        hit_rate = hits / n if n else 0.0
        lower = wilson_lower(hits, n)
        gains = sum(max(0.0, o.net_pnl_usd) for o in outcomes)
        losses = -sum(min(0.0, o.net_pnl_usd) for o in outcomes)
        profit_factor = gains / losses if losses > 0 else (math.inf if gains > 0 else 0.0)
        total_pnl = sum(o.net_pnl_usd for o in outcomes)
        growth = math.prod(1.0 + o.return_on_starting_nav for o in outcomes) - 1.0 if outcomes else 0.0
        best_trade = max((o.net_pnl_usd for o in outcomes), default=0.0)
        pnl_ex_best_trade = total_pnl - max(0.0, best_trade)
        by_scout: dict[str, float] = defaultdict(float)
        for outcome in outcomes:
            by_scout[outcome.scout_wallet] += outcome.net_pnl_usd
        best_scout = max(by_scout.values(), default=0.0)
        pnl_ex_best_scout = total_pnl - max(0.0, best_scout)
        blockers: list[str] = []
        if n < self.config.certification_min_closed_trades: blockers.append("minimum_300_closed_trades_not_met")
        if len({o.token_mint for o in outcomes}) < self.config.certification_min_closed_trades: blockers.append("minimum_300_independent_tokens_not_met")
        if total_pnl <= 0: blockers.append("aggregate_net_pnl_not_positive")
        if growth <= 0: blockers.append("geometric_growth_not_positive")
        if profit_factor <= 1.0: blockers.append("profit_factor_not_above_one")
        if lower <= self.config.certification_break_even_hit_rate: blockers.append("wilson_lower_hit_rate_not_above_break_even")
        if pnl_ex_best_trade <= 0: blockers.append("profitability_depends_on_best_trade")
        if pnl_ex_best_scout <= 0: blockers.append("profitability_depends_on_best_scout")
        return CertificationReport("certified" if not blockers else "collecting_or_failed", n, len({o.token_mint for o in outcomes}), hit_rate, lower, total_pnl, profit_factor, growth, pnl_ex_best_trade, pnl_ex_best_scout, tuple(blockers))
