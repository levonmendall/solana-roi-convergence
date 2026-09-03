from __future__ import annotations

from .direct_solana import DirectSolanaIngestionPlane
from . import wallet_realtime_tracking_repair as realtime


def install_wallet_realtime_status_compatibility() -> None:
    """Carry forward every intrinsic status guard marker through telemetry wrapping.

    The realtime repair only adds fields to ``status()``. Repository guard markers
    are part of the production composition contract and must remain visible on the
    outer callable after wrapping.
    """

    current = DirectSolanaIngestionPlane.status
    original = realtime._ORIGINAL_DIRECT_STATUS
    try:
        current.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(current, "_roi_wallet_realtime_tracking_repair", True)

    # v4 is research-only and intentionally installs after the realtime wallet
    # transport exists, so both polling and low-latency observation writers can be
    # shadowed without touching critical continuity/hydration authority.
    from .profit_first_entity_research import install_profit_first_entity_research

    install_profit_first_entity_research()


__all__ = ["install_wallet_realtime_status_compatibility"]
