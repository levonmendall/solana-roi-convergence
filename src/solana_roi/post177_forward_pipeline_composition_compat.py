from __future__ import annotations

from typing import Any, Callable

from . import continuity_standby_rpc_priority_repair as standby
from . import ephemeral_candidate_retention as ephemeral
from . import post104_production_architecture_repair as post104
from . import post177_forward_pipeline_bottleneck_repair as repair
from . import post178_e2e_residual_repair as post178
from . import post178_scout_terminal_classification_fix as post178_scout
from . import robinhood_research_getlogs_isolation as robinhood_research_getlogs_isolation
from . import robinhood_research_streaming_memory_repair as robinhood_research_streaming_memory
from . import robinhood_research_universe_cache as robinhood_research_universe_cache
from . import robinhood_request_budget_telemetry as robinhood_request_budget
from . import robinhood_v2_v4_efficiency_repair as robinhood_v2_v4_efficiency
from . import robinhood_v2_v4_observation as robinhood_v2_v4
from . import robinhood_v2_v4_observation_resume as robinhood_v2_v4_resume
from . import robinhood_v2_v4_pause_guard as robinhood_v2_v4_pause_guard
from . import unified_strategy_status as unified_status
from .config import BASELINE
from .direct_solana import DirectSolanaIngestionPlane


COMPAT_VERSION = "post177-forward-pipeline-composition-compat-v4"
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
    """Restore established composition identities, then install final E2E residuals."""

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

    # The broad provider-budget research screener explicitly constructs a public
    # Robinhood RPC client. Keep its getLogs traffic on that public endpoint instead
    # of allowing the globally-installed Validation Cloud guard to redirect the whole
    # research universe through a private provider. Private/production acquisition
    # remains on the governed Validation Cloud/failover path unchanged.
    robinhood_research_getlogs_isolation.install_robinhood_research_getlogs_isolation()

    # robinhood_launches is append-only within a release. Build the complete broad
    # research universe once, then load only newly inserted launch rows. This preserves
    # every paper-eligible market while preventing the five-second research cadence
    # from rescanning thousands of immutable SQLite rows.
    robinhood_research_universe_cache.install_robinhood_research_universe_cache()

    # Stream each public research provider response through the existing signal
    # decoder immediately instead of retaining every raw response for the whole
    # all-market pass. The compact pending surface preserves the existing 256-event
    # per-market retention and commits only after the complete pass succeeds.
    robinhood_research_streaming_memory.install_robinhood_research_streaming_memory_repair(plane_cls)

    # V2/V4 remains fail-closed until the production reactivation gate passes. Attach
    # that rule to the production plane, not the observer's global enable predicate,
    # so direct recovery primitives stay deterministic and independently resumable.
    robinhood_v2_v4_pause_guard.install_robinhood_v2_v4_pause_guard(plane_cls)

    # Install the legacy resume shim first, then make the bounded efficiency observer
    # the final observer primitive. The repaired primitive owns the same cursor-resume
    # semantics itself, so no later installer may replace it with the pre-repair path.
    robinhood_v2_v4_resume.install_robinhood_v2_v4_observation_resume()
    robinhood_v2_v4_efficiency.install_robinhood_v2_v4_efficiency_repair()

    # Permanent V2/V4 observation is composed only after the final current-frontier
    # repairs. Its fetch wrapper returns the original canonical market list unchanged,
    # so observation can collect forward evidence without becoming alternate paper
    # entry authority. Storage remains the existing Robinhood state/event/swap path.
    robinhood_v2_v4.install_robinhood_v2_v4_observation(plane_cls)

    # The budget wrapper imported before composition captured the historical research
    # primitive. Point it at the streaming primitive before installing the final
    # expected-versus-actual counter wrapper so request accounting and memory repair
    # compose rather than one silently restoring the old whole-pass accumulator.
    robinhood_request_budget._ORIGINAL_RESEARCH_PASS = robinhood_research_streaming_memory._streaming_research_pass
    robinhood_request_budget.install_robinhood_request_budget_telemetry(plane_cls)

    setattr(plane_cls, "_roi_post177_forward_pipeline_composition_compat_installed", True)
    setattr(plane_cls, "_roi_post177_forward_pipeline_composition_compat_version", COMPAT_VERSION)


__all__ = ["COMPAT_VERSION", "install_post177_forward_pipeline_composition_compat"]
