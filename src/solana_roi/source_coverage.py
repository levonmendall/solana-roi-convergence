from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from .activation import CoverageCertificationPolicy, ProgramCoverageCertificationGate
from .observation_store import ObservationEventStore

# Frozen v3.1 mainnet launch/swap program set. The transport is intentionally
# independent of these IDs: direct Solana RPC observes and normalizes the exact
# same immutable Pump.fun, Pump AMM and Raydium scope locally.
FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "PUMP_FUN",
        ("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",),
    ),
    (
        "PUMP_AMM",
        ("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",),
    ),
    (
        "RAYDIUM",
        (
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
            "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
            "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
            "5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h",
            "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj",
        ),
    ),
)


@dataclass(frozen=True, slots=True)
class SourceAwareCoverageCertificationPolicy(CoverageCertificationPolicy):
    min_normalized_swaps_per_source: int = 10
    required_program_sources: tuple[str, ...] = tuple(
        source for source, _program_ids in FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE
    )
    frozen_program_ids_by_source: tuple[tuple[str, tuple[str, ...]], ...] = FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE


class SourceAwareProgramCoverageCertificationGate(ProgramCoverageCertificationGate):
    """Require prospective launch evidence and empirical live delivery per source."""

    def __init__(
        self,
        store: ObservationEventStore,
        *,
        configured_fn,
        policy: SourceAwareCoverageCertificationPolicy | None = None,
        prospective_start_at: datetime | None = None,
    ):
        super().__init__(store, configured_fn=configured_fn, policy=policy or SourceAwareCoverageCertificationPolicy())
        self.prospective_start_at = prospective_start_at

    @property
    def source_policy(self) -> SourceAwareCoverageCertificationPolicy:
        return self.policy  # type: ignore[return-value]

    def _has_direct_recovery_ledger(self) -> bool:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='direct_solana_hydration_metrics'"
            ).fetchone()
        return row is not None

    def _source_counts(self) -> dict[str, int]:
        counts = {source: 0 for source in self.source_policy.required_program_sources}
        sql = (
            "SELECT source, COUNT(*) AS n FROM normalized_swaps WHERE "
            "(source LIKE 'solana-direct:%' OR source LIKE 'helius-enhanced-webhook:%' "
            "OR source LIKE 'helius-raw-webhook:%')"
        )
        args: list[Any] = []
        if self.prospective_start_at is not None:
            sql += " AND received_at>=?"
            args.append(self.prospective_start_at.isoformat())
        # Gap recovery is authoritative history used to restore chronology, but
        # it is not proof that the live stream delivered that transaction. Never
        # allow recovered rows to satisfy empirical per-source delivery.
        if self._has_direct_recovery_ledger():
            sql += (
                " AND NOT EXISTS (SELECT 1 FROM direct_solana_hydration_metrics recovery "
                "WHERE recovery.signature=normalized_swaps.signature AND recovery.historical_recovery=1)"
            )
        sql += " GROUP BY source"
        with self.store._lock:
            rows = self.store.db.execute(sql, tuple(args)).fetchall()
        for row in rows:
            raw = str(row["source"])
            parts = raw.split(":")
            if len(parts) < 2:
                continue
            source = parts[1].upper()
            if source in counts:
                counts[source] += int(row["n"])
        return counts

    def _prospective_rows(self, limit: int) -> list[dict[str, Any]]:
        rows = self.store.recent_program_coverage(limit)
        if self.prospective_start_at is None:
            return rows
        result: list[dict[str, Any]] = []
        for row in rows:
            try:
                created_at = datetime.fromisoformat(str(row["pair_created_at"]))
            except (KeyError, TypeError, ValueError):
                continue
            if created_at >= self.prospective_start_at:
                result.append(row)
        return result

    def status(self, *, limit: int = 500) -> dict[str, object]:
        rows = self._prospective_rows(limit)
        configured = bool(self.configured_fn())
        near = [row for row in rows if row["launch_near_creation"]]
        early = [row for row in rows if row["early_buyers_complete"]]
        funded = [row for row in rows if row["funding_complete"]]
        chronology_conflicts = self.store.first_touch_chronology_conflicts()
        total = len(rows)
        near_fraction = len(near) / total if total else 0.0
        early_fraction = len(early) / total if total else 0.0
        funding_fraction = len(funded) / total if total else 0.0
        counts = self._source_counts()
        missing = [
            source
            for source in self.source_policy.required_program_sources
            if counts.get(source, 0) < self.source_policy.min_normalized_swaps_per_source
        ]
        certified = bool(
            configured
            and total >= self.source_policy.min_samples
            and near_fraction >= self.source_policy.min_near_creation_fraction
            and early_fraction >= self.source_policy.min_early_buyer_complete_fraction
            and funding_fraction >= self.source_policy.min_funding_complete_fraction
            and chronology_conflicts == 0
            and not missing
        )
        requirements = asdict(self.source_policy)
        requirements["empirical_per_source_delivery_required"] = True
        requirements["prospective_release_boundary_required"] = True
        requirements["historical_gap_recovery_excluded_from_live_delivery"] = True
        return {
            "certified": certified,
            "configured": configured,
            "configuration_is_not_certification": True,
            "sample_count": total,
            "near_creation_count": len(near),
            "near_creation_fraction": near_fraction,
            "early_buyer_complete_count": len(early),
            "early_buyer_complete_fraction": early_fraction,
            "funding_complete_count": len(funded),
            "funding_complete_fraction": funding_fraction,
            "first_touch_chronology_conflicts": chronology_conflicts,
            "program_source_counts": counts,
            "missing_or_under_sampled_program_sources": missing,
            "required_program_sources": list(self.source_policy.required_program_sources),
            "frozen_program_ids_by_source": {
                source: list(program_ids)
                for source, program_ids in self.source_policy.frozen_program_ids_by_source
            },
            "prospective_start_at": self.prospective_start_at.isoformat() if self.prospective_start_at else None,
            "requirements": requirements,
        }
