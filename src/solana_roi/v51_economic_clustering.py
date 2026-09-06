from __future__ import annotations

import math
from collections import defaultdict
from statistics import mean
from typing import Any, Iterable

from .v51_promotion_proof import evidence_partition, event_cluster_id
from .v51_return_validation import validate_row_return


def _group_rows(
    rows: Iterable[dict[str, Any]],
    *,
    family: str | None,
    excluded_cluster_ids: set[str] | None,
) -> dict[str, list[dict[str, Any]]]:
    excluded = excluded_cluster_ids or set()
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in rows:
        row = dict(raw)
        cluster_id = event_cluster_id(row, family=family)
        if cluster_id not in excluded:
            grouped[cluster_id].append(row)
    return grouped


def _representative(
    cluster_id: str,
    group: list[dict[str, Any]],
    values: list[float],
    *,
    promotion_only: bool,
) -> dict[str, Any] | None:
    partition = evidence_partition(cluster_id)
    if promotion_only and partition == "discovery":
        return None
    if not values:
        return None
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
    return representative


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
    grouped = _group_rows(rows, family=family, excluded_cluster_ids=excluded_cluster_ids)
    result: list[dict[str, Any]] = []
    for cluster_id, group in grouped.items():
        values: list[float] = []
        for row in group:
            validated = validate_row_return(row)
            if validated.validity and validated.normalized_fraction is not None:
                values.append(validated.normalized_fraction)
        representative = _representative(cluster_id, group, values, promotion_only=promotion_only)
        if representative is not None:
            result.append(representative)
    result.sort(key=lambda row: (str(row.get("settled_at") or ""), str(row.get("event_cluster_id") or "")))
    return result


def cluster_economic_rows_legacy_pre103(
    rows: Iterable[dict[str, Any]],
    *,
    family: str | None = None,
    excluded_cluster_ids: set[str] | None = None,
    promotion_only: bool = False,
) -> list[dict[str, Any]]:
    """Audit-only reproduction of the pre-103 `finite and > -1.0` filter.

    This must never be used for current promotion authority. It exists only so the
    v2 proof can publish a reproducible before/after migration reconciliation.
    """
    grouped = _group_rows(rows, family=family, excluded_cluster_ids=excluded_cluster_ids)
    result: list[dict[str, Any]] = []
    for cluster_id, group in grouped.items():
        values: list[float] = []
        for row in group:
            try:
                value = float(row.get("net_return"))
            except (TypeError, ValueError, OverflowError):
                continue
            if math.isfinite(value) and value > -1.0:
                values.append(value)
        representative = _representative(cluster_id, group, values, promotion_only=promotion_only)
        if representative is not None:
            representative["legacy_pre103_audit_only"] = True
            result.append(representative)
    result.sort(key=lambda row: (str(row.get("settled_at") or ""), str(row.get("event_cluster_id") or "")))
    return result


__all__ = ["cluster_economic_rows", "cluster_economic_rows_legacy_pre103"]
