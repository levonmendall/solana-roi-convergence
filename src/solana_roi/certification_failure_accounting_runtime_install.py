from __future__ import annotations

from . import certification_failure_accounting_repair as accounting
from . import ephemeral_candidate_retention as retention
from .observation import LatencyCertificationGate
from .quote import QuoteCertificationGate
from .shadow_execution import JupiterShadowTransactionSimulator, ShadowWalletExecutableQuoteHandoff


def install_final_certification_failure_accounting() -> None:
    """Capture the final production composition before installing accounting.

    The accounting module is also imported directly by regressions. Capturing its
    delegates here, at production install time, prevents test import order from
    causing it to wrap stale pre-production methods and preserves every existing
    continuity/research/quote wrapper marker.

    This is also the final post-frontier composition point in Render bootstrap.
    Install the exact-release transport, generation-safe recovery, standby
    affinity/priority, confirmed-WebSocket checkpoint architecture, hydration
    scheduling, candidate RPC-priority/hedging, universal continuity scheduling,
    the candidate-completion/standby-fairness repair, high-volume pre-gap frontier
    maintenance, exact durable-signature gap boundary, and the final realtime
    wallet-to-v4 handoff here so no older installer can replace them before runtime
    tasks are created.
    """

    from .candidate_completion_continuity_repair import (
        install_candidate_completion_continuity_repair,
    )
    from .candidate_compute_admission import install_candidate_compute_admission
    from .candidate_hydration_work_conserving_repair import (
        install_candidate_hydration_work_conserving_repair,
    )
    from .candidate_risk_quote_v4_handoff import install_candidate_risk_quote_v4_handoff
    from .candidate_rpc_hedge_repair import install_candidate_rpc_hedge_repair
    from .candidate_rpc_priority_repair import install_candidate_rpc_priority_repair
    from .candidate_v4_runtime_wiring import install_candidate_v4_runtime_wiring
    from .certification_runtime_architecture_repair import (
        install_certification_runtime_architecture_repair,
    )
    from .continuity_early_loss_detection_repair import (
        install_continuity_early_loss_detection_repair,
    )
    from .continuity_exact_durable_signature_repair import (
        install_exact_durable_signature_continuity_repair,
    )
    from .continuity_generation_floor_repair import (
        install_continuity_generation_floor_repair,
    )
    from .continuity_high_volume_checkpoint_architecture import (
        install_high_volume_standby_checkpoint_architecture,
    )
    from .continuity_high_volume_poll_affinity_repair import (
        install_continuity_high_volume_poll_affinity_repair,
    )
    from .continuity_high_volume_pre_gap_repair import (
        install_high_volume_pre_gap_frontier_repair,
    )
    from .continuity_standby_rpc_priority_repair import (
        install_continuity_standby_rpc_priority_repair,
    )
    from .wallet_forward_pipeline_architecture import (
        install_wallet_forward_pipeline_architecture,
    )

    install_continuity_early_loss_detection_repair()
    install_continuity_generation_floor_repair()
    install_continuity_high_volume_poll_affinity_repair()
    install_candidate_hydration_work_conserving_repair()
    install_candidate_rpc_priority_repair()
    install_continuity_standby_rpc_priority_repair()
    install_candidate_rpc_hedge_repair()
    install_high_volume_standby_checkpoint_architecture()
    install_certification_runtime_architecture_repair()
    install_wallet_forward_pipeline_architecture()
    install_candidate_completion_continuity_repair()
    install_high_volume_pre_gap_frontier_repair()
    install_exact_durable_signature_continuity_repair()

    current_discard = retention._discard_hydration_row
    if not bool(getattr(current_discard, "_roi_failure_accounting", False)):
        accounting._ORIGINAL_DISCARD = current_discard
        setattr(accounting._discard_with_failure_accounting, "_roi_failure_accounting", True)

    current_reap = retention._reap_sqlite
    if not bool(getattr(current_reap, "_roi_failure_accounting", False)):
        accounting._ORIGINAL_REAP = current_reap
        setattr(accounting._reap_with_failure_accounting, "_roi_failure_accounting", True)

    current_latency = LatencyCertificationGate.status
    if not bool(getattr(current_latency, "_roi_failure_accounting", False)):
        accounting._ORIGINAL_LATENCY_STATUS = current_latency

    current_quote = QuoteCertificationGate.status
    if not bool(getattr(current_quote, "_roi_failure_accounting", False)):
        accounting._ORIGINAL_QUOTE_STATUS = current_quote

    current_shadow = JupiterShadowTransactionSimulator.observe
    if not bool(getattr(current_shadow, "_roi_failure_accounting", False)):
        accounting._ORIGINAL_SHADOW_OBSERVE = current_shadow

    current_handoff = ShadowWalletExecutableQuoteHandoff.observe
    if not bool(getattr(current_handoff, "_roi_failure_accounting", False)):
        accounting._ORIGINAL_QUOTE_OBSERVE = current_handoff

    accounting.install_certification_failure_accounting_repair()
    install_candidate_v4_runtime_wiring()
    install_candidate_risk_quote_v4_handoff()
    install_candidate_compute_admission()

    from .release_bound_scout_classification_repair import (
        install_release_bound_scout_classification_repair,
    )
    from .release_bound_scout_reaper_atomicity import (
        install_release_bound_scout_reaper_atomicity,
    )

    install_release_bound_scout_classification_repair()
    install_release_bound_scout_reaper_atomicity()

    from .websocket_frontier_provenance_repair import (
        install_websocket_frontier_provenance_repair,
    )

    install_websocket_frontier_provenance_repair()

    # PR #224 proved candidate RPC admission is no longer the dominant delay. The
    # next exact-release telemetry showed high-volume public websocket shards could
    # deliver launch receipts well behind a fresh head, while v7 had parked the
    # independent confirmed-head sampler. Recompose the transport/timing and
    # multi-scout normalizer at the final worker-creation boundary.
    from .post224_frontier_candidate_hardening import (
        install_post224_frontier_candidate_hardening,
    )

    install_post224_frontier_candidate_hardening()

    # Finalize without changing any economic/certification boundary: retain the
    # established websocket proof vocabulary and the existing three physical
    # sockets/provider topology (one scout-only plus two balanced program shards).
    from .post226_compatibility_finalize import (
        install_post226_compatibility_finalize,
    )

    install_post226_compatibility_finalize()


__all__ = ["install_final_certification_failure_accounting"]
