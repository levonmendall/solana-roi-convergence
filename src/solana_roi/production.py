from __future__ import annotations

"""Canonical Render production entrypoint.

All production composition is owned by :mod:`solana_roi.production_system`. This
module remains deliberately thin so ``uvicorn solana_roi.production:app`` has one
unambiguous construction path.
"""

from .production_system import (
    COMPOSITION_STATUS_PATH,
    COMPOSITION_VERSION,
    ProductionSystem,
    app,
    build_production_system,
    ingestion_runtime,
    production_system,
)

# Backward-compatible observability constants; these are resource ceilings only.
DIRECT_WS_MAX_QUEUE = 64
DIRECT_WS_MAX_SIZE_BYTES = 256 * 1024
DIRECT_CANDIDATE_CONTEXT_SLOTS = 3
DIRECT_BACKGROUND_CONTEXT_SLOTS = 1

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
