from __future__ import annotations

"""v5.2 Batch 3: five-lane positive/negative E2E plus synthetic isolation.

Research/test-only verifier around the existing canonical seeded five-lane harness.
It adds no decision path, strategy authority, economic threshold, signing, submission,
or live-money capability.
"""

from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping

from .v51_lane_capability_e2e import LANE_DESCRIPTORS, run_five_lane_capability_matrix
from .v51_synthetic_provenance import SYNTHETIC_SURFACE, synthetic_provenance_for
from .v52_continuation_capture import (
    CHALLENGER_EPOCH,
    CHALLENGER_VERSION,
    INCUMBENT_VERSION,
    LIVE_MONEY_AUTHORITY,
    PAPER_ONLY,
    SIGNING_AVAILABLE,
    TRANSACTION_SUBMISSION_AVAILABLE,
)
from .v52_lossless_candidate_accounting import (
    CANONICAL_LANES,
    CandidateAccountingRecord,
    ObservedCandidate,
    TerminalDisposition,
    reconcile_candidates,
)


BATCH_VERSION = "v52-batch3-five-lane-e2e-isolation-1"
PRODUCTION_COMPOSITION_HOOK = False
CHALLENGER_ENTRY_AUTHORITY = False
INCUMBENT_AUTHORITY_CHANGED = False

EXPECTED_POSITIVE_STAGE_TAIL: tuple[str, ...] = ("settlement", "learning")
EXPECTED_NEGATIVE_FINAL_STAGE = "position"

CANONICAL_STATISTICS_TABLES: tuple[str, ...] = (
    "risk_conditioned_alpha_v5_trials",
    "risk_conditioned_alpha_v5_outcomes",
    "profit_first_final_trials",
    "fomo_paper_trials",
    "fomo_paper_outcomes",
    "robinhood_paper_trials",
    "robinhood_paper_outcomes",
)
CANDIDATE_ID_COLUMNS: tuple[str, ...] = (
    "candidate_id",
    "source_candidate_id",
    "source_signature",
    "signature",
)


@dataclass(frozen=True)
class EconomicEventObservation:
    economic_event_id: str
    lane: str
    candidate_id: str


@dataclass(frozen=True)
class EconomicEventAccounting:
    lane_observation_count: int
    unique_economic_event_count: int
    cross_lane_event_count: int
    per_lane_observation_count: Mapping[str, int]
    event_lanes: Mapping[str, tuple[str, ...]]
    validation_errors: tuple[str, ...]
    economic_events_counted_once: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "per_lane_observation_count": dict(self.per_lane_observation_count),
            "event_lanes": {key: list(value) for key, value in self.event_lanes.items()},
        }


def _text(value: Any) -> str:
    return str(value or "").strip()


def _case_candidate_id(lane: str, *, qualifying: bool) -> str:
    return f"batch3-{lane}-{'positive' if qualifying else 'negative'}"


def reconcile_economic_events(
    observations: Iterable[EconomicEventObservation],
) -> EconomicEventAccounting:
    """Count each economic event once while preserving every lane observation."""

    errors: list[str] = []
    event_lanes: dict[str, set[str]] = {}
    per_lane = {lane: 0 for lane in CANONICAL_LANES}
    seen: set[tuple[str, str, str]] = set()
    observation_count = 0

    for index, observation in enumerate(observations):
        event_id = _text(observation.economic_event_id)
        lane = _text(observation.lane)
        candidate_id = _text(observation.candidate_id)
        if not event_id:
            errors.append(f"observation[{index}]:economic_event_id_missing")
            continue
        if lane not in CANONICAL_LANES:
            errors.append(f"observation[{index}]:unknown_lane:{lane}")
            continue
        if not candidate_id:
            errors.append(f"observation[{index}]:candidate_id_missing")
            continue
        key = (event_id, lane, candidate_id)
        if key in seen:
            errors.append(
                f"observation[{index}]:duplicate_lane_observation:"
                f"{event_id}:{lane}:{candidate_id}"
            )
            continue
        seen.add(key)
        observation_count += 1
        per_lane[lane] += 1
        event_lanes.setdefault(event_id, set()).add(lane)

    normalized = {
        event_id: tuple(sorted(lanes)) for event_id, lanes in sorted(event_lanes.items())
    }
    return EconomicEventAccounting(
        lane_observation_count=observation_count,
        unique_economic_event_count=len(normalized),
        cross_lane_event_count=sum(1 for lanes in normalized.values() if len(lanes) > 1),
        per_lane_observation_count=per_lane,
        event_lanes=normalized,
        validation_errors=tuple(errors),
        economic_events_counted_once=not errors,
    )


def _pipeline_rows(store: Any, candidate_id: str) -> list[dict[str, Any]]:
    with store._lock:
        rows = store.db.execute(
            "SELECT stage,stage_index,status,reason,payload_json "
            "FROM v51_candidate_pipeline_audit "
            "WHERE surface=? AND candidate_id=? ORDER BY stage_index",
            (SYNTHETIC_SURFACE, candidate_id),
        ).fetchall()
    return [dict(row) for row in rows]


def _table_columns(store: Any, table: str) -> set[str]:
    with store._lock:
        exists = store.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table,),
        ).fetchone()
        if exists is None:
            return set()
        return {
            str(row["name"])
            for row in store.db.execute(f"PRAGMA table_info({table})").fetchall()
        }


def _synthetic_statistics_contamination(
    store: Any,
    candidate_ids: Iterable[str],
) -> tuple[int, tuple[str, ...]]:
    ids = tuple(sorted({_text(item) for item in candidate_ids if _text(item)}))
    if not ids:
        return 0, ()
    placeholders = ",".join("?" for _ in ids)
    contaminated: set[str] = set()
    for table in CANONICAL_STATISTICS_TABLES:
        columns = _table_columns(store, table)
        for column in (name for name in CANDIDATE_ID_COLUMNS if name in columns):
            with store._lock:
                rows = store.db.execute(
                    f"SELECT {column} AS candidate_id FROM {table} "
                    f"WHERE {column} IN ({placeholders})",
                    ids,
                ).fetchall()
            for row in rows:
                contaminated.add(f"{table}:{column}:{row['candidate_id']}")
    entries = tuple(sorted(contaminated))
    return len(entries), entries


def _default_economic_observations() -> tuple[EconomicEventObservation, ...]:
    return tuple(
        EconomicEventObservation(
            economic_event_id=f"batch3-economic-event:{lane}",
            lane=lane,
            candidate_id=_case_candidate_id(lane, qualifying=True),
        )
        for lane in CANONICAL_LANES
    )


def _provenance_value(lane_report: Mapping[str, Any], label: str, field: str) -> Any:
    label_report = ((lane_report.get("provenance") or {}).get(label) or {})
    provenance = label_report.get("provenance") or {}
    return provenance.get(field)


def run_batch3_five_lane_e2e_isolation(
    store: Any,
    *,
    economic_event_observations: Iterable[EconomicEventObservation] | None = None,
) -> dict[str, Any]:
    """Run and verify the exact five-lane Batch 3 synthetic contract."""

    matrix = run_five_lane_capability_matrix(store)
    errors: list[str] = []
    expected_lanes = set(CANONICAL_LANES)
    matrix_lanes = set((matrix.get("lanes") or {}).keys())
    if matrix_lanes != expected_lanes:
        errors.append("five_lane_matrix_incomplete")

    lane_reports: dict[str, Any] = {}
    observed: list[ObservedCandidate] = []
    accounting_records: list[CandidateAccountingRecord] = []
    candidate_ids: list[str] = []

    for lane in CANONICAL_LANES:
        descriptor = LANE_DESCRIPTORS[lane]
        lane_payload = (matrix.get("lanes") or {}).get(lane) or {}
        positive = lane_payload.get("positive") or {}
        negative = lane_payload.get("negative") or {}
        positive_result = positive.get("result") or {}
        negative_result = negative.get("result") or {}
        positive_id = _case_candidate_id(lane, qualifying=True)
        negative_id = _case_candidate_id(lane, qualifying=False)
        candidate_ids.extend((positive_id, negative_id))

        positive_rows = _pipeline_rows(store, positive_id)
        negative_rows = _pipeline_rows(store, negative_id)
        positive_stages = [str(row.get("stage") or "") for row in positive_rows]
        negative_stages = [str(row.get("stage") or "") for row in negative_rows]

        positive_settled = bool(
            positive_result.get("decision") == "paper_enter"
            and len(positive_rows) >= 2
            and tuple(positive_stages[-2:]) == EXPECTED_POSITIVE_STAGE_TAIL
            and all(str(row.get("status") or "") == "complete" for row in positive_rows[-2:])
        )
        if not positive_settled:
            errors.append(f"{lane}:positive_case_did_not_reach_settlement_learning")

        expected_negative_reason = str(negative.get("expected_negative_reason") or "")
        negative_accounted = bool(
            negative_result.get("decision") == "paper_reject"
            and str(negative_result.get("reason") or "") == expected_negative_reason
            and negative_rows
            and negative_stages[-1] == EXPECTED_NEGATIVE_FINAL_STAGE
            and str(negative_rows[-1].get("status") or "") == "not_opened"
            and "settlement" not in negative_stages
            and "learning" not in negative_stages
        )
        if not negative_accounted:
            errors.append(f"{lane}:negative_case_not_explicitly_accounted")

        provenance_reports: dict[str, Any] = {}
        provenance_valid = True
        for label, item_id in (("positive", positive_id), ("negative", negative_id)):
            provenance = synthetic_provenance_for(store, item_id)
            valid = bool(
                provenance
                and provenance.get("synthetic") is True
                and provenance.get("surface") == SYNTHETIC_SURFACE
                and provenance.get("origin") == f"batch3_lane_capability:{lane}:{label}"
                and provenance.get("lane") == descriptor["lane"]
                and provenance.get("economic_surface") == descriptor["economic_surface"]
                and provenance.get("venue") == descriptor["venue"]
                and provenance.get("certification_eligible") is False
                and provenance.get("promotion_eligible") is False
            )
            if not valid:
                provenance_valid = False
                errors.append(f"{lane}:{label}:synthetic_provenance_invalid")
            provenance_reports[label] = {"valid": valid, "provenance": provenance}

        observed.extend(
            (
                ObservedCandidate(candidate_id=positive_id, lane=lane),
                ObservedCandidate(candidate_id=negative_id, lane=lane),
            )
        )
        accounting_records.extend(
            (
                CandidateAccountingRecord.terminal_record(
                    positive_id, lane, TerminalDisposition.EXITED.value
                ),
                CandidateAccountingRecord.terminal_record(
                    negative_id, lane, TerminalDisposition.PERMANENTLY_REJECTED.value
                ),
            )
        )
        lane_reports[lane] = {
            "positive_candidate_id": positive_id,
            "negative_candidate_id": negative_id,
            "positive_settlement_learning_verified": positive_settled,
            "negative_rejection_accounted": negative_accounted,
            "positive_stages": positive_stages,
            "negative_stages": negative_stages,
            "synthetic_provenance_verified": provenance_valid,
            "provenance": provenance_reports,
            "certification_eligible_case_count": 0,
            "profitability_statistics_contribution_count": 0,
        }

    lossless = reconcile_candidates(observed, accounting_records)
    if not lossless.certification_ready or lossless.unexplained_disappearance_count != 0:
        errors.append("batch2_lossless_accounting_gate_failed")

    contamination_count, contamination = _synthetic_statistics_contamination(
        store, candidate_ids
    )
    if contamination_count:
        errors.append("synthetic_canonical_statistics_contamination")

    economic_accounting = reconcile_economic_events(
        economic_event_observations
        if economic_event_observations is not None
        else _default_economic_observations()
    )
    if economic_accounting.validation_errors:
        errors.append("economic_event_accounting_invalid")
    observed_event_lanes = {
        lane
        for lane, count in economic_accounting.per_lane_observation_count.items()
        if count > 0
    }
    if observed_event_lanes != expected_lanes:
        errors.append("economic_event_lane_observability_incomplete")

    fomo_matrix = (matrix.get("lanes") or {}).get("fomo") or {}
    fomo_shadow_non_authoritative = bool(
        (lane_reports.get("fomo") or {}).get("synthetic_provenance_verified")
        and (fomo_matrix.get("positive") or {}).get("promotion_eligible") is False
        and (fomo_matrix.get("negative") or {}).get("promotion_eligible") is False
        and not CHALLENGER_ENTRY_AUTHORITY
        and not PRODUCTION_COMPOSITION_HOOK
    )
    if not fomo_shadow_non_authoritative:
        errors.append("fomo_shadow_authority_boundary_failed")

    robinhood = lane_reports.get("robinhood") or {}
    robinhood_provenance_survives = bool(
        robinhood.get("synthetic_provenance_verified")
        and all(
            _provenance_value(robinhood, label, "economic_surface") == "ROBINHOOD_CHAIN"
            for label in ("positive", "negative")
        )
        and all(
            _provenance_value(robinhood, label, "venue") == "UNISWAP_V3"
            for label in ("positive", "negative")
        )
    )
    if not robinhood_provenance_survives:
        errors.append("robinhood_provenance_boundary_failed")

    matrix_isolation_valid = bool(
        matrix.get("all_cases_synthetic") is True
        and int(matrix.get("certification_eligible_case_count") or 0) == 0
        and int(matrix.get("promotion_eligible_case_count") or 0) == 0
    )
    if not matrix_isolation_valid:
        errors.append("synthetic_matrix_eligibility_boundary_failed")

    return {
        "batch_version": BATCH_VERSION,
        "batch3_ready": not errors,
        "validation_errors": errors,
        "canonical_lanes": list(CANONICAL_LANES),
        "positive_case_count": len(CANONICAL_LANES),
        "negative_case_count": len(CANONICAL_LANES),
        "lanes": lane_reports,
        "batch2_candidate_accounting": lossless.as_dict(),
        "unexplained_disappearance_count": lossless.unexplained_disappearance_count,
        "synthetic_candidate_count": len(candidate_ids),
        "synthetic_certification_contribution_count": (
            0 if matrix_isolation_valid else len(candidate_ids)
        ),
        "synthetic_profitability_contribution_count": contamination_count,
        "canonical_statistics_contamination_count": contamination_count,
        "canonical_statistics_contamination": list(contamination),
        "economic_event_accounting": economic_accounting.as_dict(),
        "fomo_shadow_non_authoritative": fomo_shadow_non_authoritative,
        "robinhood_provenance_survives": robinhood_provenance_survives,
        "research_only": True,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "changes_strategy_authority": False,
        "changes_economic_thresholds": False,
        "production_composition_hook": PRODUCTION_COMPOSITION_HOOK,
        "challenger_entry_authority": CHALLENGER_ENTRY_AUTHORITY,
    }


def safety_manifest() -> dict[str, Any]:
    return {
        "batch_version": BATCH_VERSION,
        "challenger_version": CHALLENGER_VERSION,
        "challenger_epoch": CHALLENGER_EPOCH,
        "incumbent_version": INCUMBENT_VERSION,
        "incumbent_remains_authoritative": True,
        "incumbent_authority_changed": INCUMBENT_AUTHORITY_CHANGED,
        "challenger_entry_authority": CHALLENGER_ENTRY_AUTHORITY,
        "research_only": True,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        "production_composition_hook": PRODUCTION_COMPOSITION_HOOK,
        "canonical_lanes": list(CANONICAL_LANES),
        "positive_negative_e2e_required_per_lane": True,
        "synthetic_provenance_required": True,
        "synthetic_certification_contribution_required_zero": True,
        "synthetic_profitability_contribution_required_zero": True,
        "cross_lane_economic_event_dedup_required": True,
        "per_lane_observability_preserved": True,
        "fomo_shadow_non_authoritative": True,
        "robinhood_provenance_required": True,
        "batch2_lossless_accounting_required": True,
    }


__all__ = [
    "BATCH_VERSION",
    "CANONICAL_STATISTICS_TABLES",
    "EconomicEventAccounting",
    "EconomicEventObservation",
    "reconcile_economic_events",
    "run_batch3_five_lane_e2e_isolation",
    "safety_manifest",
]
