"""Solana ROI Convergence paper-trading research engine.

Package import is intentionally passive. Production runtime composition is owned by
``solana_roi.production_system`` and reached through ``solana_roi.production:app``.
Importing ``solana_roi`` alone must not install transport, strategy, execution, or
certification adapters.
"""

from .config import BASELINE, StrategyConfig

__all__ = ["BASELINE", "StrategyConfig"]
