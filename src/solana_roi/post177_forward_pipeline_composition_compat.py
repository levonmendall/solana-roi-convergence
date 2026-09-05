from __future__ import annotations

from typing import Any, Callable

from . import continuity_standby_rpc_priority_repair as standby
from . import ephemeral_candidate_retention as ephemeral
from . import post104_production_architecture_repair as post104
from . import post177_forward_pipeline_bottleneck_repair as repair
from . import post178_e2e_residual_repair as post178
from . import post178_scout_terminal_classification_fix as post178_scout
from . import unified_strategy_status as unified_status
from .config import BASELINE
from .direct_solana import DirectSolanaIngestionPlane


COMPAT_VERSION = "post177-forward-pipeline-composition-compat-v3"
_FINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None


def _inherit_markers(target: Any, source: Any) -> None:
    if target is None or source is None:
        return
    try:
        target.__dict__.update(getattr(source, "__dict__", {}))
    except Exception:
        pass


def _truthful_direct_status(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _FINAL_DIRECT_STATUS is None:
        raise RuntimeError("post-177 composition compatibility is not installed")
    payload = _FINAL_DIRECT_STATUS(self)

    retention = payload.get("ephemeral_candidate_retention")
    if isinstance(retention, dict):
        retention.update(
            {
                "entry_window_seconds": float(BASELINE.confirmation_window_seconds),
                "immediate_copy_window_seconds": float(BASELINE.confirmation_window_seconds),
                "scout_hydration_retention_seconds": float(ephemeral.SCOUT_HYDRATION_RETENTION_SECONDS),
                "twenty_seconds_is_strategy_candidate_state_boundary": True,
                "twenty_seconds_is_universal_hydration_prune_boundary": False,
                "expired_candidate_hydration_work_pruned": True,
                "scout_hydration_pruned_after_seconds": float(ephemeral.SCOUT_HYDRATION_RETENTION_SECONDS),
                "non_scout_ephemeral_hydration_pruned_after_seconds": float(BASELINE.confirmation_window_seconds),
                "late_scout_hydration_has_retrospective_entry_authority": False,
                "retention_semantics_version": COMPAT_VERSION,
            }
        )
        retention.pop("operational_hydration_retention_seconds", None)
        retention.pop("twenty_seconds_is_hydration_prune_boundary", None)
        retention.pop("hydration_work_pruned_after_operational_timeout", None)

    post = payload.get("post104_architecture_repair")
    if isinstance(post, dict):
        post.update(
            {
                "candidate_entry_window_seconds_unchanged": float(BASELINE.confirmation_window_seconds),
                "candidate_state_lifetime_seconds": float(BASELINE.confirmation_window_seconds),
                "scout_hydration_retention_seconds": float(ephemeral.SCOUT_HYDRATION_RETENTION_SECONDS),
                "candidate_context_20s_hard_cutoff_active": True,
                "scout_hydration_can_complete_after_20s": True,
                "late_scout_hydration_retrospective_entry_authority": False,
            }
        )
        post.pop("candidate_context_operational_timeout_seconds", None)
        post.pop("continuation_context_collection_after_20s", None)

    policy = payload.get("provider_runtime_policy")
    if isinstance(policy, dict):
        policy.update(
            {
                "candidate_state_entry_window_seconds": float(BASELINE.confirmation_window_seconds),
                "scout_hydration_retention_seconds": float(ephemeral.SCOUT_HYDRATION_RETENTION_SECONDS),
                "scout_hydration_uses_extended_operational_retention": True,
                "all_candidate_hydration_uses_operational_timeout": False,
                "candidate_20s_is_immediate_copy_context_only": True,
                "late_scout_hydration_is_continuation_research_only": True,
            }
        )
        policy.pop("candidate_hydration_retention_uses_operational_timeout", None)

    post177 = payload.get("post177_forward_pipeline_bottleneck_repair")
    if isinstance(post177, dict):
        post177.update(
            {
                "candidate_state_lifetime_seconds": float(BASELINE.confirmation_window_seconds),
                "scout_hydration_retention_seconds": float(ephemeral.SCOUT_HYDRATION_RETENTION_SECONDS),
                "late_scout_hydration_retrospective_entry_authority": False,
            }
        )
        post177.pop("candidate_operational_retention_seconds", None)
    return payload


setattr(_truthful_direct_status, "_roi_post177_forward_pipeline_composition_compat", True)


def install_post177_forward_pipeline_composition_compat(plane_cls: type[Any]) -> None:
    """Restore established composition identities, then install final E2E residuals.

    PR177's follow-up needs new forward semantics, not new strategy or scheduler
    authority. Keep the canonical 20-second candidate-state lifetime, restore the
    final standby-over-background RPC governor, preserve wrapper lineage markers,
    and leave unified-status composition to the repository's existing readiness
    installer before the post-178 residual repair applies its final current-frontier
    semantics.
    """

    global _FINAL_DIRECT_STATUS

    ephemeral.ENTRY_WINDOW_SECONDS = float(BASELINE.confirmation_window_seconds)
    post104.CANDIDATE_ENTRY_WINDOW_SECONDS = float(BASELINE.confirmation_window_seconds)

    standby.install_continuity_standby_rpc_priority_repair()

    original_unified = repair._ORIGINAL_UNIFIED_STATUS
    if callable(original_unified):
        unified_status.build_unified_strategy_status = original_unified

    current_direct_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_direct_status, "_roi_post177_forward_pipeline_composition_compat", False)):
        _FINAL_DIRECT_STATUS = current_direct_status
        _inherit_markers(_truthful_direct_status, current_direct_status)
        setattr(_truthful_direct_status, "_roi_post177_forward_pipeline_composition_compat", True)
        DirectSolanaIngestionPlane.status = _truthful_direct_status  # type: ignore[method-assign]

    _inherit_markers(plane_cls.run, repair._ORIGINAL_ROBINHOOD_RUN)
    _inherit_markers(plane_cls.status, repair._ORIGINAL_ROBINHOOD_STATUS)
    setattr(plane_cls.run, "_roi_post177_forward_pipeline", True)
    setattr(plane_cls.status, "_roi_post177_forward_pipeline", True)

    post178.install_post178_e2e_residual_repair(plane_cls)
    post178_scout.install_post178_scout_terminal_classification_fix()

    setattr(plane_cls, "_roi_post177_forward_pipeline_composition_compat_installed", True)
    setattr(plane_cls, "_roi_post177_forward_pipeline_composition_compat_version", COMPAT_VERSION)


__all__ = ["COMPAT_VERSION", "install_post177_forward_pipeline_composition_compat"]
