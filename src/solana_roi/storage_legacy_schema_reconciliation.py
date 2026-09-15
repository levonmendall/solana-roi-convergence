from __future__ import annotations

"""Exact retention classification for pre-compact tables still carried in active storage.

These tables were observed in the verified production successor after the storage
rollover.  They are retained only because the semantic transition preserved them;
none is granted startup or certification authority by this module.  The set is
intentionally exact: a future unknown table must continue to fail the positive-schema
check rather than being admitted by a prefix or wildcard.
"""

from . import storage_retention as base


OWNER = "storage-legacy-reconciliation"
PRUNE_CONDITION = "operator-approved exact deletion proof only"
ARCHIVE_POLICY = "retained in verified successor pending independent deletion proof"

# Exact set reported by production ROI_ACTIVE_STORAGE_MAINTENANCE_FAILED after the
# verified rollover/reclamation sequence.  Keep alphabetized for reviewability.
LEGACY_RETAINED_DATASETS: tuple[str, ...] = (
    "anonymous_certification_outcomes",
    "continuation_recalibration_audit",
    "direct_solana_continuity_epoch",
    "direct_solana_discovery_gap_event",
    "direct_solana_launch_reference_samples",
    "direct_solana_launch_ws_frontier",
    "direct_solana_storage_maintenance",
    "direct_solana_storage_maintenance_cursor",
    "direct_solana_strategy_continuity_epoch",
    "direct_solana_strategy_continuity_epoch_v2",
    "direct_solana_strategy_continuity_gap_event",
    "direct_solana_strategy_poll_checkpoint",
    "economic_current_context_probe_audit",
    "ephemeral_candidate_state",
    "fomo_paper_outcome_execution_models",
    "fomo_paper_outcomes",
    "fomo_paper_trials",
    "fomo_wallet_cohort",
    "independent_fomo_runtime",
    "later_activity_strategy_handoff",
    "launch_near_creation_diagnostics",
    "profit_first_entity_forward_outcomes",
    "profit_first_entity_shadow_trials",
    "profit_first_final_epochs",
    "profit_first_final_exit_execution_attempts",
    "profit_first_final_exit_liquidations",
    "profit_first_final_exit_signals",
    "profit_first_final_outcome_execution_models",
    "profit_first_final_outcomes",
    "profit_first_final_trials",
    "release_bound_candidate_failures",
    "risk_conditioned_alpha_v5_outcome_execution_models",
    "scout_attribution_failure_diagnostics",
    "scout_economic_movement_observations",
    "scout_trigger_terminal_classification",
    "shadow_price_tracked_mints_meta",
    "shadow_price_tracked_mints_state",
    "v4_entity_signal_context",
    "v4_token_entity_links",
    "v51_candidate_current_state",
    "v51_candidate_pipeline_audit",
    "v51_candidate_stage_events",
    "v51_candidates",
    "v51_execution_cost_ledger",
    "v51_invalid_economic_measurements",
    "v51_paper_capital_metrics",
    "v51_paper_capital_reservations",
    "v51_paper_capital_settlements",
    "v51_paper_execution_balance_artifacts",
    "v51_paper_lifecycle_events",
    "v51_paper_lifecycle_runtime_state",
    "v51_rejected_counterfactuals",
    "v51_release_attestation",
    "v52_counterfactual_decisions",
    "v52_governed_challengers",
    "v52_lane_decay_profiles",
    "v52_portfolio_rotation_requests",
    "v52_profit_signal_events",
    "v52_provider_economics",
    "v52_reliability_economics",
    "v52_staged_exit_fills",
    "v52_staged_position_lifecycle",
    "v52_strategy_governance_history",
    "v52_tournament_decisions",
    "v52_tournament_outcomes",
    "v52_wallet_lead_outcomes",
    "v52_wallet_marginal_alpha",
    "v52_wallet_missed_opportunities",
    "wallet_realtime_risk_work",
)


def _legacy_contract(dataset: str) -> base.RetentionContract:
    return base.RetentionContract(
        dataset=dataset,
        owner=OWNER,
        retention_class=base.RetentionClass.LEGACY_UNCLASSIFIED,
        purpose="Retained pre-compact table preserved by verified semantic transition; no current authority granted",
        consumer="storage maintenance / explicit future deletion proof only",
        hot_or_cold="cold",
        max_hot_age=None,
        max_hot_rows=None,
        max_hot_bytes=None,
        archive_policy=ARCHIVE_POLICY,
        prune_condition=PRUNE_CONDITION,
        startup_access=False,
        certification_access=False,
    )


LEGACY_RETAINED_CONTRACTS: tuple[base.RetentionContract, ...] = tuple(
    _legacy_contract(name) for name in LEGACY_RETAINED_DATASETS
)

for contract in LEGACY_RETAINED_CONTRACTS:
    existing = base.RETENTION_REGISTRY.get(contract.dataset)
    if existing is not None and existing != contract:
        raise RuntimeError(f"conflicting retained-legacy retention contract:{contract.dataset}")
    base.RETENTION_REGISTRY[contract.dataset] = contract


__all__ = [
    "ARCHIVE_POLICY",
    "LEGACY_RETAINED_CONTRACTS",
    "LEGACY_RETAINED_DATASETS",
    "OWNER",
    "PRUNE_CONDITION",
]
