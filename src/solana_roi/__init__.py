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

# Optional managed/free WSS observers remain supported, but no unauthenticated
# third-party shared endpoint is trusted by default after production proved the
# dRPC public WSS could not establish even one frozen subscription.
from .stream_redundancy import install_stream_redundancy

install_stream_redundancy()

# WebSocket-only failures must not invalidate a release when the read-only HTTP RPC
# plane is still healthy. Continuously poll every frozen target prospectively and
# admit that transport into the same target-quorum state machine. This is not
# historical backfill: the cursor is maintained continuously from release startup.
from .live_poll_redundancy import install_live_poll_redundancy

install_live_poll_redundancy()

# Public load-balanced RPC backends cannot be assumed to recognize the exact same
# signature cursor on every request. Use a confirmed-slot watermark instead: it is
# provider-agnostic, bounded, and still fails closed if more than the allowed live
# delta accumulates between observations.
from .poll_watermark_repair import install_poll_watermark_repair

install_poll_watermark_repair()

# If the bounded polling delta overflows while the same frozen target is still
# covered by a real WebSocket, re-baseline only the standby poll watermark from
# the current confirmed head. This never restores an invalid prospective gap; it
# only prevents a safely redundant polling lane from remaining frozen forever.
from .poll_standby_rearm import install_poll_standby_rearm

install_poll_standby_rearm()

# A failed HTTP poll does not itself prove a data gap: the next bounded read can
# still recover every signature since the last confirmed watermark. Keep that
# target provisionally covered for a short fixed lease, but fail the exact release
# closed if the bounded delta becomes unrecoverable or the lease expires after a
# real WebSocket zero-coverage interval.
from .poll_recoverability_lease import install_poll_recoverability_lease

install_poll_recoverability_lease()

# Multi-page getSignaturesForAddress requests can be routed to different backend
# nodes even on the same public RPC hostname. Raise minContextSlot to the newest
# slot already observed in the current delta before requesting each later page so
# the next backend must be fresh enough to understand the preceding `before`
# signature. This preserves the hard bounded delta while preventing false overflow.
from .poll_pagination_context import install_poll_pagination_context

install_poll_pagination_context()

# Preserve the legacy marker contract used by production.py so that importing the
# compatibility entrypoint later cannot wrap over the richer intrinsic status or
# stream implementation and hide the active safety/telemetry envelope.
from .direct_solana import DirectSolanaIngestionPlane

setattr(DirectSolanaIngestionPlane._stream_endpoint, "_roi_stream_guarded", True)
setattr(DirectSolanaIngestionPlane.status, "_roi_memory_bounded", True)

from .config import BASELINE, StrategyConfig

__all__ = ["BASELINE", "StrategyConfig"]
