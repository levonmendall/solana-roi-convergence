from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .activation import CoverageCertificationPolicy, ProgramCoverageCertificationGate
from .observation_store import ObservationEventStore

# Frozen v3.1 mainnet launch/swap program set. Helius source labels collapse the
# Raydium programs into RAYDIUM, so certification is source-aware while the
# exact program IDs remain frozen into the manifest via the policy.
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
    """Require empirical delivery from every frozen supported source.

    Aggregate launch/funding counts alone are insufficient because one healthy
    venue must never certify a missing venue. Source evidence is derived only
    from normalized swaps that actually traversed the Helius webhook parser.
    """

    def __init__(
        self,
        store: ObservationEventStore,
        *,
        configured_fn,
        policy: SourceAwareCoverageCertificationPolicy | None = None,
    ):
        super().__init__(store, configured_fn=configured_fn, policy=policy or SourceAwareCoverageCertificationPolicy())

    @property
    def source_policy(self) -> SourceAwareCoverageCertificationPolicy:
        return self.policy  # type: ignore[return-value]

    def _source_counts(self) -> dict[str, int]:
        counts = {source: 0 for source in self.source_policy.required_program_sources}
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT source, COUNT(*) AS n FROM normalized_swaps "
                "WHERE source LIKE 'helius-enhanced-webhook:%' GROUP BY source"
            ).fetchall()
        for row in rows:
            raw = str(row["source"])
            parts = raw.split(":")
            if len(parts) < 2:
                continue
            source = parts[1].upper()
            if source in counts:
                counts[source] += int(row["n"])
        return counts

    def status(self, *, limit: int = 500) -> dict[str, object]:
        base = super().status(limit=limit)
        counts = self._source_counts()
        missing = [
            source
            for source in self.source_policy.required_program_sources
            if counts.get(source, 0) < self.source_policy.min_normalized_swaps_per_source
        ]
        requirements = dict(base.get("requirements") or {})
        requirements.update(asdict(self.source_policy))
        requirements["empirical_per_source_delivery_required"] = True
        base.update(
            {
                "certified": bool(base["certified"] and not missing),
                "program_source_counts": counts,
                "missing_or_under_sampled_program_sources": missing,
                "required_program_sources": list(self.source_policy.required_program_sources),
                "frozen_program_ids_by_source": {
                    source: list(program_ids)
                    for source, program_ids in self.source_policy.frozen_program_ids_by_source
                },
                "requirements": requirements,
            }
        )
        return base
