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
from .logical_bootstrap_page_cache_repair import configure_logical_bootstrap_page_cache_repair
from .robinhood_drpc_environment import configure_robinhood_drpc_backup
from .wallet_discovery_background_status_repair import configure_wallet_discovery_background_status_repair

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

# The autonomous wallet discovery worker does not consume the status payload returned
# by run_once(). Keep those history-scaled research/status reads out of the background
# hot path while preserving explicit status endpoints, proposal selection, forward
# evidence, strategy thresholds, and all paper-only authority boundaries.
configure_wallet_discovery_background_status_repair()

from .production_system import (
    COMPOSITION_STATUS_PATH,
    COMPOSITION_VERSION,
    ProductionSystem,
    app,
    build_production_system,
    ingestion_runtime,
    production_system,
)
from .sqlite_phase_observability import install_sqlite_phase_observability
from .startup_retention_cleanup import run_safe_retention_cleanup
from .storage_maintenance_bounded_io_repair import install_storage_maintenance_bounded_io_repair
from . import legacy_production_composition as _legacy_production

# The production storage-maintenance worker already exists in the composed runtime.
# Replace only its history-scaled hydration-metric selector with a durable rowid
# cursor that inspects at most one existing maintenance batch per pass. The deletion
# predicate, retention horizon, worker, guard, historical recovery protection, and
# all economic/certification authority remain unchanged.
install_storage_maintenance_bounded_io_repair()

# Attribute startup cgroup file cache, dirty/writeback, WAL growth and process I/O to
# the exact long-lived worker/SQLite phase before ASGI lifespan starts those workers.
# Install after the bounded-I/O repair so telemetry surrounds the production function
# that will actually execute.
install_sqlite_phase_observability()

# The production system has now composed the complete logical-bootstrap route stack.
# When service splitting is enabled, configure one lifecycle gate around that final
# page endpoint so the next page waits for the prior post-send cache cleanup. When
# splitting is intentionally disabled the route is absent and configuration is a
# documented no-op. No authority or resource threshold changes here.
configure_logical_bootstrap_page_cache_repair(app)

# This call only registers the bounded stale-export cleanup on the already-canonical
# FastAPI lifespan. Registration is storage-non-mutating; actual cleanup executes at
# real application startup before entering the guarded Render handoff lifespan.
run_safe_retention_cleanup(app, ingestion_runtime)

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
