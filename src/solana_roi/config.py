from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    """Frozen ROI Convergence v3.1 baseline used for forward certification."""

    version: str = "roi-convergence-v3.1-forward-1"
    paper_only: bool = True
    initial_capital_usd: float = 500.0
    bankroll_risk_fraction: float = 0.0075
    catastrophic_stop_fraction: float = 0.30
    starter_fraction_of_full_position: float = 0.30
    confirmation_window_seconds: float = 20.0
    max_chase_fraction: float = 0.15
    stagnation_check_seconds: float = 90.0
    thesis_timeout_seconds: float = 180.0
    harvest_gain_fraction: float = 0.50
    harvest_fraction: float = 0.70
    runner_fraction: float = 0.30
    runner_trailing_drawdown_fraction: float = 0.40
    execution_drag_per_side_fraction: float = 0.025
    certification_min_closed_trades: int = 300
    certification_confidence: float = 0.95
    certification_break_even_hit_rate: float = 0.5749

    @property
    def full_position_fraction_of_nav(self) -> float:
        return self.bankroll_risk_fraction / self.catastrophic_stop_fraction


BASELINE = StrategyConfig()
