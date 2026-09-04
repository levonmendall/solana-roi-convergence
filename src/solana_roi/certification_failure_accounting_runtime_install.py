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
    Install the exact-release transport, generation-safe recovery, hydration
    scheduling, candidate RPC-priority, and candidate-only hedge repairs here so no
    older installer can replace them before runtime tasks are created.
    """

    from .candidate_hydration_work_conserving_repair import (
        install_candidate_hydration_work_conserving_repair,
    )
    from .candidate_rpc_hedge_repair import install_candidate_rpc_hedge_repair
    from .candidate_rpc_priority_repair import install_candidate_rpc_priority_repair
    from .continuity_early_loss_detection_repair import (
        install_continuity_early_loss_detection_repair,
    )
    from .continuity_generation_floor_repair import (
        install_continuity_generation_floor_repair,
    )

    install_continuity_early_loss_detection_repair()
    install_continuity_generation_floor_repair()
    install_candidate_hydration_work_conserving_repair()
    install_candidate_rpc_priority_repair()
    install_candidate_rpc_hedge_repair()

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


__all__ = ["install_final_certification_failure_accounting"]
