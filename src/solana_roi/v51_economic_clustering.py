from __future__ import annotations

from collections import defaultdict
from statistics import mean
from typing import Any, Iterable

from .v51_promotion_proof import evidence_partition, event_cluster_id
from .v51_return_validation import validate_row_return


def cluster_economic_rows(
    rows: Iterable[dict[str, Any]],
    *,
    family: str | None = None,
    excluded_cluster_ids: set[str] | None = None,
    promotion_only: bool = False,
) -> list[dict[str, Any]]:
    """Cluster immutable outcomes using the canonical return-validation contract.

    Invalid rows are not imputed into cluster returns. They remain in the underlying
    audit evidence and are accounted for by the measurement-integrity/debt gate.
    Exact -100% outcomes are valid and participate in cluster means.
    """
    excluded = excluded_cluster_ids or set()
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in rows:
        row = dict(raw)
        cluster_id = event_cluster_id(row, family=family)
        if cluster_id in excluded:
            continue
        grouped[cluster_id].append(row)

    result: list[dict[str, Any]] = []
    for cluster_id, group in grouped.items():
        partition = evidence_partition(cluster_id)
        if promotion_only and partition == "discovery":
            continue
        values: list[float] = []
        for row in group:
            validated = validate_row_return(row)
            if validated.validity and validated.normalized_fraction is not None:
                values.append(validated.normalized_fraction)
        if not values:
            continue
        settled_values = [str(row.get("settled_at") or "") for row in group if row.get("settled_at")]
        representative = dict(group[-1])
        representative.update(
            {
                "event_cluster_id": cluster_id,
                "evidence_partition": partition,
                "cluster_observation_count": len(group),
                "cluster_valid_economic_measurement_count": len(values),
                "cluster_invalid_economic_measurement_count": len(group) - len(values),
                "net_return": mean(values),
                "settled_at": max(settled_values) if settled_values else representative.get("settled_at"),
            }
        )
        result.append(representative)
    result.sort(key=lambda row: (str(row.get("settled_at") or ""), str(row.get("event_cluster_id") or "")))
    return result


__all__ = ["cluster_economic_rows"]
