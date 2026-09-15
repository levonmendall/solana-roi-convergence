from __future__ import annotations

import os

from causal_overlay import NORMAL_MODE, apply_hydration_causal_overlay, write_causal_evidence

# Import canonical production first so its normal composition/configuration is retained.
# The ASGI lifespan has not started yet when this module is imported by uvicorn, so the
# harness overlay is installed before long-lived production workers begin.
from solana_roi.production import app  # noqa: E402
from solana_roi import direct_solana_hydration_status_repair as hydration_repair  # noqa: E402

mode = os.environ.get("PORTABLE_REPRO_CAUSAL_MODE", NORMAL_MODE)
evidence = apply_hydration_causal_overlay(hydration_repair, mode=mode)
write_causal_evidence(evidence)

__all__ = ["app"]
