"""Solana ROI Convergence paper-trading research engine.

Package import is passive with respect to external I/O and production storage.
The complete positive storage registry is finalized in memory before any runtime
submodule can validate or maintain active storage, eliminating import-order
ambiguity while preserving fail-closed unknown-table handling.

Production runtime composition is owned by ``solana_roi.production_system`` and
reached through ``solana_roi.production:app``.  The isolated certifier may
explicitly opt into its replica-continuity bootstrap via a Render-only environment
flag; that bootstrap changes only certifier-owned replica storage/compatibility and
grants no trading authority.
"""

import os

from .config import BASELINE, StrategyConfig
from .storage_runtime_registry_readiness import finalize_storage_registry

# In-memory only.  No database/filesystem mutation occurs here.  This must happen
# before callers can import runtime maintenance/rollover modules from the package.
finalize_storage_registry()

if os.getenv("SOLANA_ROI_CERTIFIER_REPLICA_CONTINUITY", "").strip().lower() in {"1", "true", "yes", "on"}:
    from .certifier_replica_continuity_repair import configure_certifier_replica_continuity_repair

    configure_certifier_replica_continuity_repair()

__all__ = ["BASELINE", "StrategyConfig"]
