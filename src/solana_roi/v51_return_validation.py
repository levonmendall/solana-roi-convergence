from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping


STATISTICS_VERSION = "v51-economic-statistics-v2"
INVALID_ECONOMIC_MEASUREMENT = "invalid_economic_measurement"
VALID_ECONOMIC_MEASUREMENT = "valid_economic_measurement"
MAX_INVALID_ECONOMIC_MEASUREMENT_RATE = 0.0


@dataclass(frozen=True)
class ValidatedReturn:
    raw_value: Any
    normalized_fraction: float | None
    validity: bool
    invalid_reason: str | None
    source_surface: str
    source_signature: str
    measurement_epoch: str
    execution_model_epoch: str

    @property
    def state(self) -> str:
        return VALID_ECONOMIC_MEASUREMENT if self.validity else INVALID_ECONOMIC_MEASUREMENT

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["state"] = self.state
        return payload


def validate_return(
    raw_value: Any,
    *,
    source_surface: str = "UNKNOWN",
    source_signature: str = "",
    measurement_epoch: str = "",
    execution_model_epoch: str = "",
) -> ValidatedReturn:
    reason: str | None = None
    value: float | None = None
    if raw_value is None:
        reason = "missing_return"
    elif isinstance(raw_value, bool):
        reason = "malformed_return"
    else:
        try:
            value = float(raw_value)
        except (TypeError, ValueError, OverflowError):
            reason = "malformed_return"
    if reason is None and value is not None and not math.isfinite(value):
        reason = "non_finite_return"
    if reason is None and value is not None and value < -1.0:
        reason = "return_below_total_loss_bound"
    valid = reason is None and value is not None
    return ValidatedReturn(
        raw_value=raw_value,
        normalized_fraction=value if valid else None,
        validity=valid,
        invalid_reason=reason,
        source_surface=str(source_surface or "UNKNOWN"),
        source_signature=str(source_signature or ""),
        measurement_epoch=str(measurement_epoch or ""),
        execution_model_epoch=str(execution_model_epoch or ""),
    )


def validate_row_return(
    row: Mapping[str, Any],
    *,
    value_key: str = "net_return",
    source_surface: str | None = None,
) -> ValidatedReturn:
    surface = str(source_surface or row.get("surface") or "UNKNOWN")
    signature = str(
        row.get("source_signature")
        or row.get("trial_id")
        or row.get("candidate_id")
        or row.get("id")
        or ""
    )
    return validate_return(
        row.get(value_key),
        source_surface=surface,
        source_signature=signature,
        measurement_epoch=str(row.get("measurement_epoch") or ""),
        execution_model_epoch=str(row.get("execution_model_epoch") or ""),
    )


def valid_return_values(values: Iterable[Any]) -> list[float]:
    result: list[float] = []
    for raw in values:
        validated = validate_return(raw)
        if validated.validity and validated.normalized_fraction is not None:
            result.append(validated.normalized_fraction)
    return result


def return_integrity_summary(
    rows: Iterable[Mapping[str, Any]],
    *,
    value_key: str = "net_return",
    invalid_rate_threshold: float = MAX_INVALID_ECONOMIC_MEASUREMENT_RATE,
) -> dict[str, Any]:
    total = 0
    valid = 0
    total_losses = 0
    reasons: dict[str, int] = {}
    invalid_rows: list[dict[str, Any]] = []
    for row in rows:
        total += 1
        result = validate_row_return(row, value_key=value_key)
        if result.validity:
            valid += 1
            if result.normalized_fraction == -1.0:
                total_losses += 1
            continue
        reason = str(result.invalid_reason or "unknown_invalid_return")
        reasons[reason] = reasons.get(reason, 0) + 1
        invalid_rows.append(result.to_dict())
    invalid = total - valid
    invalid_rate = invalid / total if total else 0.0
    threshold = max(0.0, float(invalid_rate_threshold))
    proof_eligible = invalid_rate <= threshold
    return {
        "statistics_version": STATISTICS_VERSION,
        "state": VALID_ECONOMIC_MEASUREMENT if proof_eligible else INVALID_ECONOMIC_MEASUREMENT,
        "raw_measurement_count": total,
        "valid_economic_measurement_count": valid,
        "measurement_debt_count": invalid,
        "invalid_economic_measurement_rate": invalid_rate,
        "invalid_rate_integrity_threshold": threshold,
        "proof_eligible": proof_eligible,
        "exact_total_loss_count": total_losses,
        "invalid_reason_counts": reasons,
        "invalid_measurements": invalid_rows,
        "no_imputation": True,
    }


def _debt_key(row: Mapping[str, Any], validated: ValidatedReturn) -> str:
    payload = {
        "surface": validated.source_surface,
        "signature": validated.source_signature,
        "family": str(row.get("family") or "UNKNOWN"),
        "raw": repr(validated.raw_value),
        "settled_at": str(row.get("settled_at") or ""),
        "reason": validated.invalid_reason,
        "measurement_epoch": validated.measurement_epoch,
        "execution_model_epoch": validated.execution_model_epoch,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


def persist_invalid_measurement_debt(store: Any, rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    materialized = [dict(row) for row in rows]
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_invalid_economic_measurements ("
            "debt_key TEXT PRIMARY KEY,statistics_version TEXT NOT NULL,source_surface TEXT NOT NULL,"
            "source_signature TEXT,family TEXT,raw_value TEXT,invalid_reason TEXT NOT NULL,"
            "measurement_epoch TEXT,execution_model_epoch TEXT,settled_at TEXT,"
            "paper_only INTEGER NOT NULL,live_money_authority INTEGER NOT NULL)"
        )
        persisted = 0
        for row in materialized:
            validated = validate_row_return(row)
            if validated.validity:
                continue
            store.db.execute(
                "INSERT OR IGNORE INTO v51_invalid_economic_measurements("
                "debt_key,statistics_version,source_surface,source_signature,family,raw_value,invalid_reason,"
                "measurement_epoch,execution_model_epoch,settled_at,paper_only,live_money_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    _debt_key(row, validated),
                    STATISTICS_VERSION,
                    validated.source_surface,
                    validated.source_signature or None,
                    str(row.get("family") or "UNKNOWN"),
                    repr(validated.raw_value),
                    str(validated.invalid_reason or "unknown_invalid_return"),
                    validated.measurement_epoch or None,
                    validated.execution_model_epoch or None,
                    str(row.get("settled_at") or "") or None,
                ),
            )
            persisted += 1
    summary = return_integrity_summary(materialized)
    summary["persisted_measurement_debt_count"] = persisted
    summary["debt_table"] = "v51_invalid_economic_measurements"
    return summary


__all__ = [
    "INVALID_ECONOMIC_MEASUREMENT",
    "MAX_INVALID_ECONOMIC_MEASUREMENT_RATE",
    "STATISTICS_VERSION",
    "VALID_ECONOMIC_MEASUREMENT",
    "ValidatedReturn",
    "persist_invalid_measurement_debt",
    "return_integrity_summary",
    "valid_return_values",
    "validate_return",
    "validate_row_return",
]
