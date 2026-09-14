"""Solana ROI Convergence paper-trading research engine.

Package import is passive by default. Production runtime composition is owned by
``solana_roi.production_system`` and reached through ``solana_roi.production:app``.
The isolated certifier may explicitly opt into its replica-continuity bootstrap via
a Render-only environment flag; that bootstrap changes only certifier-owned replica
storage/compatibility and grants no trading authority.
"""

import os

from .config import BASELINE, StrategyConfig

if os.getenv("SOLANA_ROI_CERTIFIER_REPLICA_CONTINUITY", "").strip().lower() in {"1", "true", "yes", "on"}:
    from .certifier_replica_continuity_repair import configure_certifier_replica_continuity_repair

    configure_certifier_replica_continuity_repair()

__all__ = ["BASELINE", "StrategyConfig"]
