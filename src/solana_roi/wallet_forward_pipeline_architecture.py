from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .wallet_discovery import ContinuousWalletDiscovery
from .wallet_realtime_tracking_repair import RealtimeWalletTracker


_ORIGINAL_DISCOVERY_STATUS: Callable[..., dict[str, Any]] | None = None


def _status_with_forward_pipeline(self: ContinuousWalletDiscovery) -> dict[str, Any]:
    if _ORIGINAL_DISCOVERY_STATUS is None:
        raise RuntimeError("wallet forward pipeline architecture is not installed")
    payload = _ORIGINAL_DISCOVERY_STATUS(self)
    realtime_record = RealtimeWalletTracker._record_quick_forward_swap
    discovery_record = ContinuousWalletDiscovery._record_forward_swap
    payload["forward_pipeline_architecture"] = {
        "installed": True,
        "broad_universe_role": "cheap-discovery-and-screening",
        "deep_evaluation_scope": "bounded-dynamic-tracked-wallet-entity-set",
        "historical_screen_has_promotion_authority": False,
        "realtime_forward_evidence_required_for_strategy_influence": True,
        "realtime_v4_handoff_installed": bool(
            getattr(realtime_record, "_roi_profit_first_entity_final", False)
        ),
        "discovery_v4_handoff_installed": bool(
            getattr(discovery_record, "_roi_profit_first_entity_final", False)
        ),
        "v4_research_is_release_bound": True,
        "old_release_forward_rows_replayed_into_new_release": False,
        "active_v3_1_cohort_mutation_allowed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_or_submission_available": False,
    }
    return payload


setattr(_status_with_forward_pipeline, "_roi_wallet_forward_pipeline_architecture", True)


def install_wallet_forward_pipeline_architecture() -> None:
    """Make the realtime-wallet -> final-v4 handoff the last wallet composition.

    The wallet evidence repair owns point-in-time copyability/risk semantics and the
    final profit-first adapter owns release-bound v4 shadow sampling. Production has
    accumulated several installers that can wrap the same realtime record method in
    different import orders. Re-running the idempotent installers in authority order
    here guarantees that the final adapter is outermost after evidence semantics are
    installed, so every newly inserted realtime forward observation is offered to v4.

    Historical rows are intentionally not replayed. A newly deployed release gets a
    clean v4 evidence epoch and only observations first seen by that release can
    create final shadow trials or forward outcomes.
    """

    global _ORIGINAL_DISCOVERY_STATUS

    from .profit_first_entity_final_research import install_final_profit_first_entity_research
    from .wallet_entity_universe_v4 import install_v4_wallet_entity_universe
    from .wallet_evidence_rpc_repair import install_wallet_evidence_rpc_repair

    # Evidence semantics first, final v4 sampler second, dynamic entity universe
    # third. All three installers are idempotent and retain paper-only authority.
    install_wallet_evidence_rpc_repair()
    install_final_profit_first_entity_research()
    install_v4_wallet_entity_universe()

    realtime_record = RealtimeWalletTracker._record_quick_forward_swap
    discovery_record = ContinuousWalletDiscovery._record_forward_swap
    if not bool(getattr(realtime_record, "_roi_profit_first_entity_final", False)):
        raise RuntimeError("final v4 realtime wallet handoff is not installed")
    if not bool(getattr(discovery_record, "_roi_profit_first_entity_final", False)):
        raise RuntimeError("final v4 discovery wallet handoff is not installed")

    current_status = ContinuousWalletDiscovery.status
    if not bool(getattr(current_status, "_roi_wallet_forward_pipeline_architecture", False)):
        _ORIGINAL_DISCOVERY_STATUS = current_status
        try:
            _status_with_forward_pipeline.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_status_with_forward_pipeline, "_roi_wallet_forward_pipeline_architecture", True)
        ContinuousWalletDiscovery.status = _status_with_forward_pipeline  # type: ignore[method-assign]


__all__ = ["install_wallet_forward_pipeline_architecture"]
