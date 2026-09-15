from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    table: str
    mode: str
    value: int | str | None
    enforcement: str
    rationale: str


RETENTION_POLICIES: tuple[RetentionPolicy, ...] = (
    RetentionPolicy(
        "certification_release_epochs",
        "latest_n",
        2,
        "existing",
        "Active certification projection retains the current frontier and predecessor; provenance remains separately protected.",
    ),
    RetentionPolicy(
        "direct_solana_hydration_metrics",
        "time_window",
        "ordinary=6h; historical_recovery=protected",
        "existing",
        "Ordinary telemetry is bounded while historical-recovery evidence is excluded from that deletion boundary.",
    ),
    RetentionPolicy(
        "direct_solana_hydration_queue",
        "bounded_history",
        "terminal-only; pending/processing protected",
        "existing",
        "Only terminal queue history is eligible for existing bounded maintenance.",
    ),
    RetentionPolicy(
        "v52_wallet_forward_replay_runs",
        "latest_n",
        5,
        "writer",
        "Operational replay status consumes only the latest result; stale diagnostics drain in bounded batches.",
    ),
    RetentionPolicy(
        "v52_wallet_forward_validation",
        "semantic_milestones",
        3600,
        "writer",
        "Persist semantic authority transitions immediately and unchanged complete validation as an hourly milestone.",
    ),
    RetentionPolicy(
        "v52_wallet_forward_runtime_state",
        "singleton",
        1,
        "existing",
        "Runtime status is current-state authority and is maintained by singleton update.",
    ),
    RetentionPolicy(
        "certification_replication_changes",
        "frontier_bounded",
        "certifier-acknowledged frontier required",
        "policy_only",
        "Physical pruning requires durable certifier watermark and restart/equivalence proof.",
    ),
    RetentionPolicy(
        "direct_solana_recent_receipts",
        "time_window",
        "pending exact recovery-horizon proof",
        "policy_only",
        "Receipt history may be bounded only after recovery and reconciliation dependencies are proven.",
    ),
    RetentionPolicy(
        "direct_solana_minute_receipts",
        "aggregate_then_discard",
        "pending exact replay/recovery-horizon proof",
        "policy_only",
        "Older minute detail may be aggregated only after point-in-time and recovery dependencies are proven.",
    ),
    RetentionPolicy(
        "wallet_discovery_broad_samples",
        "aggregate_then_discard",
        "pending dependency proof",
        "policy_only",
        "Broad scouting samples are a future aggregation candidate, not a current deletion target.",
    ),
    RetentionPolicy(
        "wallet_discovery_candidates",
        "bounded_history",
        "active/recent plus forward-referenced; exact retirement boundary pending",
        "policy_only",
        "Candidate retirement must preserve anything referenced by forward evidence or point-in-time replay.",
    ),
    RetentionPolicy(
        "wallet_discovery_forward_observations",
        "retain_forever",
        None,
        "protected",
        "Canonical prospective wallet evidence is high-value lineage and is not pruned by this repair.",
    ),
    RetentionPolicy(
        "wallet_intelligence_snapshots",
        "pending_proof",
        "point-in-time replay dependencies unresolved",
        "protected",
        "Do not prune until historical wallet-state reconstruction is independently proven safe.",
    ),
    RetentionPolicy(
        "risk_evidence",
        "retain_forever",
        None,
        "protected",
        "Canonical decision and replay evidence remains protected.",
    ),
    RetentionPolicy(
        "risk_refresh_measurements",
        "pending_proof",
        "dependency boundary unresolved",
        "policy_only",
        "Potentially bounded telemetry remains untouched until its consumers are proven.",
    ),
    RetentionPolicy(
        "normalized_swaps",
        "retain_forever",
        None,
        "protected",
        "Core market, replay, and wallet evidence remains protected.",
    ),
    RetentionPolicy(
        "program_coverage_observations",
        "aggregate_then_discard",
        "pending dependency proof",
        "policy_only",
        "Coverage observations may be summarized later, but physical disposal is not authorized here.",
    ),
    RetentionPolicy(
        "helius_webhook_inbox",
        "bounded_history",
        "acknowledged plus bounded recovery window; exact boundary pending",
        "policy_only",
        "Webhook input must remain until acknowledgement and recovery semantics establish a safe deletion boundary.",
    ),
)

POLICIES_BY_TABLE = {policy.table: policy for policy in RETENTION_POLICIES}

if len(POLICIES_BY_TABLE) != len(RETENTION_POLICIES):
    raise RuntimeError("duplicate storage retention policy table")


__all__ = ["POLICIES_BY_TABLE", "RETENTION_POLICIES", "RetentionPolicy"]
