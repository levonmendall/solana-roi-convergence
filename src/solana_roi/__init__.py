"""Solana ROI Convergence paper-trading research engine."""

# Import-time installation is intentional: Render may launch either the legacy
# ``solana_roi.api:app`` entrypoint or the guarded ``solana_roi.production:app``
# entrypoint depending on whether Blueprint settings have been synchronized.
# The direct-Solana safety envelope must therefore be active before either API
# module constructs the runtime.
from .runtime_guards import install_runtime_guards

install_runtime_guards()

# Provider/subscription resilience is also entrypoint-independent. It is applied
# after the generic guards so the final runtime uses sequential subscription
# acknowledgement and the repaired zero-cost secondary provider everywhere.
from .stream_resilience import install_stream_resilience

install_stream_resilience()

# Preserve the legacy marker contract used by production.py so that importing the
# compatibility entrypoint later cannot wrap over the richer intrinsic status or
# stream implementation and hide the active safety/telemetry envelope.
from .direct_solana import DirectSolanaIngestionPlane

setattr(DirectSolanaIngestionPlane._stream_endpoint, "_roi_stream_guarded", True)
setattr(DirectSolanaIngestionPlane.status, "_roi_memory_bounded", True)

from .config import BASELINE, StrategyConfig

__all__ = ["BASELINE", "StrategyConfig"]
