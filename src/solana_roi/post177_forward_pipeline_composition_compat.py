from __future__ import annotations

from typing import Any

from . import continuity_standby_rpc_priority_repair as standby
from . import ephemeral_candidate_retention as ephemeral
from . import post104_production_architecture_repair as post104
from . import post177_forward_pipeline_bottleneck_repair as repair
from . import unified_strategy_status as unified_status
from .config import BASELINE


COMPAT_VERSION = "post177-forward-pipeline-composition-compat-v1"


def _inherit_markers(target: Any, source: Any) -> None:
    if target is None or source is None:
        return
    try:
        target.__dict__.update(getattr(source, "__dict__", {}))
    except Exception:
        pass


def install_post177_forward_pipeline_composition_compat(plane_cls: type[Any]) -> None:
    """Restore established composition identities without undoing the repair.

    PR177's follow-up needs new forward semantics, not new strategy or scheduler
    authority. Keep the canonical 20-second candidate-state lifetime, restore the
    final standby-over-background RPC governor, preserve wrapper lineage markers,
    and leave unified-status composition to the repository's existing readiness
    installer. Robinhood's legacy `caught_up_for_paper_decisions` field is already a
    forward-frontier alias, so readiness behavior remains corrected without adding a
    second wrapper cycle around unified status.
    """

    # The candidate-state lifetime is a frozen strategy/certification invariant.
    # Post-177 fixes candidate path latency by removing unnecessary work and starting
    # risk prewarm earlier; it does not enlarge this state window.
    ephemeral.ENTRY_WINDOW_SECONDS = float(BASELINE.confirmation_window_seconds)
    post104.CANDIDATE_ENTRY_WINDOW_SECONDS = float(BASELINE.confirmation_window_seconds)

    # candidate_rpc's installer writes the lower-level governor function directly.
    # Reassert the existing final composition where candidate has priority over
    # standby, and standby has priority over ordinary certification/research.
    standby.install_continuity_standby_rpc_priority_repair()

    # Do not add a second unified-status wrapper: continuity_e2e owns that composition
    # and can consume the corrected Robinhood compatibility alias directly.
    original_unified = repair._ORIGINAL_UNIFIED_STATUS
    if callable(original_unified):
        unified_status.build_unified_strategy_status = original_unified

    # New wrappers must preserve all prior composition markers because those markers
    # are repository architecture proofs used by tests and diagnostics.
    _inherit_markers(plane_cls.run, repair._ORIGINAL_ROBINHOOD_RUN)
    _inherit_markers(plane_cls.status, repair._ORIGINAL_ROBINHOOD_STATUS)
    setattr(plane_cls.run, "_roi_post177_forward_pipeline", True)
    setattr(plane_cls.status, "_roi_post177_forward_pipeline", True)
    setattr(plane_cls, "_roi_post177_forward_pipeline_composition_compat_installed", True)
    setattr(plane_cls, "_roi_post177_forward_pipeline_composition_compat_version", COMPAT_VERSION)


__all__ = ["COMPAT_VERSION", "install_post177_forward_pipeline_composition_compat"]
