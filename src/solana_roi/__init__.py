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

# The transport layer preserves every frozen program/scout subscription while
# establishing low-volume targets first and permitting large-yet-bounded Solana
# log frames.
from .transport_hardening import install_transport_hardening

install_transport_hardening()

# Keep the multiplexed handshake implementation available as a compatibility
# fallback, but the final production run topology below does not depend on a
# provider accepting ten subscriptions on one public WebSocket.
from .handshake_pump import install_handshake_pump

install_handshake_pump()

# Free public Solana endpoints proved that one-socket multiplexing is not robust
# enough for the frozen ten-target feed. Isolate every target on its own bounded
# WebSocket.
from .target_stream_fanout import install_target_stream_fanout

install_target_stream_fanout()

# Continuity is a property of full target coverage, not whether one flaky public
# provider happens to hold all ten streams at the same instant. Use the redundant
# provider union per target, and never bulk-backfill a prospective timing gap into
# the hydration/candidate lanes.
from .target_quorum import install_target_quorum

install_target_quorum()

# Preserve the legacy marker contract used by production.py so that importing the
# compatibility entrypoint later cannot wrap over the richer intrinsic status or
# stream implementation and hide the active safety/telemetry envelope.
from .direct_solana import DirectSolanaIngestionPlane

setattr(DirectSolanaIngestionPlane._stream_endpoint, "_roi_stream_guarded", True)
setattr(DirectSolanaIngestionPlane.status, "_roi_memory_bounded", True)

from .config import BASELINE, StrategyConfig

__all__ = ["BASELINE", "StrategyConfig"]
