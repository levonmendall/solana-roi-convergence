"""Solana ROI Convergence paper-trading research engine.

Package import is passive by default. Production runtime composition is owned by
``solana_roi.production_system`` and reached through ``solana_roi.production:app``.
The isolated certifier may explicitly opt into its replica-continuity bootstrap via
a Render-only environment flag; that bootstrap changes only certifier-owned replica
storage/compatibility and grants no trading authority.

Active-storage cutover is also explicit and fail-closed.  When enabled, package
composition first validates a release-bound transition checkpoint, redirects the
runtime DB path to that active database, replaces only the storage/engine adapters,
and scopes certification to the positive active manifest.  There is no fallback to
legacy storage after the flag is enabled.
"""

import os

from .config import BASELINE, StrategyConfig


def _truthy(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


if _truthy("SOLANA_ROI_ACTIVE_STORAGE_ENABLED"):
    release_sha = (os.getenv("RENDER_GIT_COMMIT") or os.getenv("GIT_COMMIT") or "").strip() or None
    from .storage_transition import activate_runtime_database_environment

    activate_runtime_database_environment(expected_release_sha=release_sha)

    # Patch the two composition symbols before runtime.py imports them. Existing
    # strategy/risk/wallet/execution classes are untouched.
    from . import durable_engine as _durable_engine
    from . import observation_store as _observation_store
    from .active_runtime import ActiveDurablePaperTradingEngine, ActiveObservationEventStore
    from .certification_active_manifest import install_active_certification_manifest

    _observation_store.ObservationEventStore = ActiveObservationEventStore
    _durable_engine.DurablePaperTradingEngine = ActiveDurablePaperTradingEngine
    install_active_certification_manifest()

if _truthy("SOLANA_ROI_CERTIFIER_REPLICA_CONTINUITY"):
    from .certifier_replica_continuity_repair import configure_certifier_replica_continuity_repair

    configure_certifier_replica_continuity_repair()

__all__ = ["BASELINE", "StrategyConfig"]
