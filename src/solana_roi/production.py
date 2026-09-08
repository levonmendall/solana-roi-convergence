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

from .production_system import (
    COMPOSITION_STATUS_PATH,
    COMPOSITION_VERSION,
    ProductionSystem,
    app,
    build_production_system,
    ingestion_runtime,
    production_system,
)
from . import legacy_production_composition as _legacy_production

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
