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

    # Keep the parent research implementation importable and its historical rows
    # intact, then install the final governed strategy as the only new sampler.
    # The final wallet/entity universe is a bounded research-priority and scoring
    # layer downstream of the same realtime/continuity authority.
    from .profit_first_entity_research import install_profit_first_entity_research
    from .profit_first_entity_final_research import install_final_profit_first_entity_research
    from .wallet_entity_universe_v4 import install_v4_wallet_entity_universe

    install_profit_first_entity_research()
    install_final_profit_first_entity_research()
    install_v4_wallet_entity_universe()


__all__ = ["install_wallet_realtime_status_compatibility"]
