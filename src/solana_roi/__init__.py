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

# A transient multi-page poll exception is also recoverable when the same target
# has remained continuously observed by the real WebSocket union. Route that case
# through the existing same-target standby re-arm path instead of letting the poll
# watermark freeze after the 12-second lease. Any real zero-coverage generation
# still bypasses this repair and preserves the exact-release fail-closed boundary.
from .poll_exception_rearm import install_poll_exception_rearm

install_poll_exception_rearm()

# The same-target re-arm itself must not depend on a possibly stale target-specific
# signature head from a load-balanced public backend. Once uninterrupted WebSocket
# authority is proven, move only the standby baseline to the hedged confirmed chain
# slot so future fallback observation starts prospectively from a current point.
from .poll_chain_head_rearm import install_poll_chain_head_rearm

install_poll_chain_head_rearm()

# Polling baselines can become ready before the real WebSocket fanout during process
# startup. Do not let that warm-up ordering arm the prospective outage clock. The
# final continuity barrier requires all poll baselines plus real WebSocket coverage
# of every frozen target, keeps synthetic polling out of provider-independence
# counts, and exposes only sanitized HTTP status codes for failed WS handshakes.
from .continuity_startup_barrier import install_continuity_startup_barrier

install_continuity_startup_barrier()

# Production telemetry proved the managed Alchemy endpoint is connection-rate
# limited when all ten frozen targets open separate sockets. Keep PublicNode and
# Solana mainnet target-isolated, but carry all ten Alchemy logsSubscribe requests
# on one sequentially acknowledged socket without changing target quorum or memory
# ceilings.
from .alchemy_multiplexed_stream import install_alchemy_multiplexed_stream

install_alchemy_multiplexed_stream()

# Production then proved that multiplexing alone is insufficient when live Solana
# notifications share the same inline receive/dispatch loop as the remaining
# subscription acknowledgements. Give the single Alchemy socket a dedicated reader
# so ACK futures resolve independently while notification handlers remain bounded.
from .alchemy_handshake_pump import install_alchemy_handshake_pump

install_alchemy_handshake_pump()

# Commercial RPC capacity is optional, not a prerequisite for observing a public
# blockchain. By default leave a configured Alchemy key completely idle, preserve
# the two public full-scope WebSocket providers plus continuous bounded polling,
# and hydrate ordinary program traffic only while a source is below the unchanged
# empirical delivery minimum. Launches and scout activity remain fully hydrated.
from .public_data_economics import install_public_data_economics

install_public_data_economics()

# A precise launch transaction is usually a creation transaction rather than a
# simple swap, so it cannot pass the normal swap parser. Resolve its mint directly
# from standard transaction metadata, hydrate only that mint's launch-window
# signatures, and publish coverage evidence through the existing launch/funding
# collectors without creating candidate, latency, quote, or paper authority.
from .launch_coverage_bridge import install_launch_coverage_bridge

install_launch_coverage_bridge()

# Successful paper entries use the exact assembled Jupiter taker-order price and
# debit observed signature, priority and rent lamports separately. Because the net
# order output already embeds route/price impact, do not layer the fixed 2.5% entry
# haircut on top; retain that drag only as the conservative fallback for paths with
# no amount-specific executable observation (including current exits/offline use).
from .execution_realism import install_execution_realism

install_execution_realism()

# Preserve the legacy marker contract used by production.py so that importing the
# compatibility entrypoint later cannot wrap over the richer intrinsic status or
# stream implementation and hide the active safety/telemetry envelope.
from .direct_solana import DirectSolanaIngestionPlane

setattr(DirectSolanaIngestionPlane._stream_endpoint, "_roi_stream_guarded", True)
setattr(DirectSolanaIngestionPlane.status, "_roi_memory_bounded", True)

from .config import BASELINE, StrategyConfig

__all__ = ["BASELINE", "StrategyConfig"]
