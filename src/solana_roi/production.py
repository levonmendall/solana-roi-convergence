from __future__ import annotations

"""Canonical Render production entrypoint.

All production composition is owned by :mod:`solana_roi.production_system`. This
module remains deliberately thin so ``uvicorn solana_roi.production:app`` has one
unambiguous construction path.

Compatibility audit markers for authority/architecture bindings now consumed by
the explicit composition root (not called from this facade):
``install_v51_production_authority(app, ingestion_runtime)`` and
``install_post104_production_architecture_repair``.
"""

import asyncio

from .certification_delta_production_bounds import configure_production_certification_delta_bound
from .incremental_event_integrity_repair import configure_incremental_event_integrity_repair
from .robinhood_drpc_environment import configure_robinhood_drpc_backup

# Keep authoritative certification-replica requests below the certifier's bounded
# HTTP deadline. This changes only transport pagination; evidence, continuity, and
# every strategy/certification threshold remain unchanged.
configure_production_certification_delta_bound()

# Replace ordinary full-history event-ledger verification with a validated durable
# integrity anchor plus append-only tail verification before production composition
# installs the cgroup-bounded verifier. Missing/corrupt anchors still fall back to
# complete hash-chain verification; no integrity or authority gate is weakened.
configure_incremental_event_integrity_repair()

# Provider credentials must be materialized before importing the production
# composition root because the legacy-compatible Robinhood ingestion substrate
# reads its provider environment during construction. This is environment-only
# bootstrap: it grants no strategy, signing, submission, or live-money authority.
configure_robinhood_drpc_backup()

from .production_system import (
    COMPOSITION_STATUS_PATH,
    COMPOSITION_VERSION,
    ProductionSystem,
    app,
    build_production_system,
    ingestion_runtime,
    production_system,
)
from .startup_retention_cleanup import register_startup_retention_cleanup
from . import legacy_production_composition as _legacy_production

# Register one bounded cleanup on the canonical FastAPI lifespan. Registration is
# storage-non-mutating; actual cleanup executes only at real application startup.
register_startup_retention_cleanup(app, ingestion_runtime)

# Backward-compatible observability constants; these are resource ceilings only.
DIRECT_WS_MAX_QUEUE = 64
DIRECT_WS_MAX_SIZE_BYTES = 256 * 1024
DIRECT_CANDIDATE_CONTEXT_SLOTS = 3
DIRECT_BACKGROUND_CONTEXT_SLOTS = 1

# Test/replay compatibility for pre-Phase-18 callers.  These helpers are exported
# from the canonical facade, but their installers are not invoked here: the single
# production composition root has already constructed the runtime before this
# module finishes importing.
_cooperative_handler = _legacy_production._cooperative_handler
_bounded_ws_connect = _legacy_production._bounded_ws_connect
_bounded_context_prefill = _legacy_production._bounded_context_prefill
install_direct_stream_fairness = _legacy_production.install_direct_stream_fairness
install_direct_stream_memory_bounds = _legacy_production.install_direct_stream_memory_bounds

__all__ = [
    "COMPOSITION_STATUS_PATH",
    "COMPOSITION_VERSION",
    "DIRECT_BACKGROUND_CONTEXT_SLOTS",
    "DIRECT_CANDIDATE_CONTEXT_SLOTS",
    "DIRECT_WS_MAX_QUEUE",
    "DIRECT_WS_MAX_SIZE_BYTES",
    "ProductionSystem",
    "app",
    "build_production_system",
    "ingestion_runtime",
    "production_system",
]
