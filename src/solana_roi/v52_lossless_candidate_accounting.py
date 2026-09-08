from __future__ import annotations

"""v5.2 Batch 2: lossless candidate accounting for the canonical five-lane seam.

This module is deliberately research/test-only.  It reconciles every observed
candidate against an explicit current accounting record and fails certification
closed when any candidate silently disappears or when the accounting ledger is
malformed.  It does not create entry authority, alter economics, or install a
production composition hook.
"""

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Iterable, Mapping

from .v51_lane_capability_e2e import LANE_DESCRIPTORS, run_five_lane_capability_matrix
from .v52_continuation_capture import (
    CHALLENGER_EPOCH,
    CHALLENGER_VERSION,
    INCUMBENT_VERSION,
    LIVE_MONEY_AUTHORITY,
    PAPER_ONLY,
    SIGNING_AVAILABLE,
    TRANSACTION_SUBMISSION_AVAILABLE,
)


BATCH_VERSION = "v52-batch2-lossless-candidate-accounting-1"
PRODUCTION_COMPOSITION_HOOK = False
CHALLENGER_ENTRY_AUTHORITY = False
INCUMBENT_AUTHORITY_CHANGED = False

CANONICAL_LANES: tuple[str, ...] = tuple(LANE_DESCRIPTORS)


class TerminalDisposition(str, Enum):
    EXITED = "exited"
    PERMANENTLY_REJECTED = "permanently_rejected"
    EXPIRED = "expired"
    INVALIDATED = "invalidated"


@dataclass(frozen=True)
class ObservedCandidate:
    candidate_id: str
    lane: str


@dataclass(frozen=True)
class CandidateAccountingRecord:
    candidate_id: str
    lane: str
    active: bool
    terminal_disposition: str | None = None

    @classmethod
    def active_record(cls, candidate_id: str, lane: str) -> "CandidateAccountingRecord":
        return cls(candidate_id=candidate_id, lane=lane, active=True)

    @classmethod
    def terminal_record(
        cls,
        candidate_id: str,
        lane: str,
        terminal_disposition: str,
    ) -> "CandidateAccountingRecord":
        return cls(
            candidate_id=candidate_id,
            lane=lane,
            active=False,
            terminal_disposition=terminal_disposition,
        )


@dataclass(frozen=True)
class LaneAccounting:
    lane: str
    observed_candidate_count: int
    active_candidate_count: int
    terminal_candidate_count: int
    accounted_candidate_count: int
    unexplained_disappearance_count: int
    unexplained_candidate_ids: tuple[str, ...]
    unexpected_candidate_count: int
    unexpected_candidate_ids: tuple[str, ...]


@dataclass(frozen=True)
class LosslessCandidateAccounting:
    batch_version: str
    lanes: Mapping[str, LaneAccounting]
    observed_candidate_count: int
    active_candidate_count: int
    terminal_candidate_count: int
    accounted_candidate_count: int
    unexplained_disappearance_count: int
    unexplained_candidate_ids: tuple[str, ...]
    unexpected_candidate_count: int
    unexpected_candidate_ids: tuple[str, ...]
    validation_errors: tuple[str, ...]
    accounting_valid: bool
    certification_ready: bool
    research_only: bool = True
    paper_only: bool = True
    live_money_authority: bool = False
    changes_strategy_authority: bool = False
    changes_economic_thresholds: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "lanes": {lane: asdict(value) for lane, value in self.lanes.items()},
        }


def _text(value: Any, *, field: str, errors: list[str], context: str) -> str | None:
    text = str(value or "").strip()
    if not text:
        errors.append(f"{context}:{field}_missing")
        return None
    return text


def _lane(value: Any, *, errors: list[str], context: str) -> str | None:
    lane = _text(value, field="lane", errors=errors, context=context)
    if lane is None:
        return None
    if lane not in CANONICAL_LANES:
        errors.append(f"{context}:unknown_lane:{lane}")
        return None
    return lane


def _normalize_observed(
    candidates: Iterable[ObservedCandidate],
    *,
    errors: list[str],
) -> dict[str, str]:
    observed: dict[str, str] = {}
    for index, candidate in enumerate(candidates):
        context = f"observed[{index}]"
        candidate_id = _text(
            candidate.candidate_id,
            field="candidate_id",
            errors=errors,
            context=context,
        )
        lane = _lane(candidate.lane, errors=errors, context=context)
        if candidate_id is None or lane is None:
            continue
        if candidate_id in observed:
            if observed[candidate_id] != lane:
                errors.append(
                    f"{context}:candidate_lane_conflict:{candidate_id}:"
                    f"{observed[candidate_id]}!={lane}"
                )
            else:
                errors.append(f"{context}:duplicate_candidate_id:{candidate_id}")
            continue
        observed[candidate_id] = lane
    return observed


def _normalize_records(
    records: Iterable[CandidateAccountingRecord],
    *,
    errors: list[str],
) -> dict[str, CandidateAccountingRecord]:
    normalized: dict[str, CandidateAccountingRecord] = {}
    for index, record in enumerate(records):
        context = f"record[{index}]"
        candidate_id = _text(
            record.candidate_id,
            field="candidate_id",
            errors=errors,
            context=context,
        )
        lane = _lane(record.lane, errors=errors, context=context)
        if candidate_id is None or lane is None:
            continue

        disposition: str | None = None
        if record.active:
            if record.terminal_disposition not in (None, ""):
                errors.append(f"{context}:active_record_has_terminal_disposition:{candidate_id}")
                continue
        else:
            raw = _text(
                record.terminal_disposition,
                field="terminal_disposition",
                errors=errors,
                context=context,
            )
            if raw is None:
                continue
            try:
                disposition = TerminalDisposition(raw).value
            except ValueError:
                errors.append(f"{context}:unknown_terminal_disposition:{raw}")
                continue

        current = CandidateAccountingRecord(
            candidate_id=candidate_id,
            lane=lane,
            active=bool(record.active),
            terminal_disposition=disposition,
        )
        if candidate_id in normalized:
            previous = normalized[candidate_id]
            if previous != current:
                errors.append(f"{context}:conflicting_accounting_record:{candidate_id}")
            else:
                errors.append(f"{context}:duplicate_accounting_record:{candidate_id}")
            continue
        normalized[candidate_id] = current
    return normalized


def reconcile_candidates(
    observed_candidates: Iterable[ObservedCandidate],
    accounting_records: Iterable[CandidateAccountingRecord],
) -> LosslessCandidateAccounting:
    """Reconcile the candidate population without allowing silent loss.

    `observed_candidates` is the immutable population that entered the Batch 2
    accounting boundary.  Every candidate in that set must have exactly one
    current record proving either continued active tracking or a valid terminal
    disposition.  Any missing record is an unexplained disappearance.
    """

    errors: list[str] = []
    observed = _normalize_observed(observed_candidates, errors=errors)
    records = _normalize_records(accounting_records, errors=errors)

    unexplained: list[str] = []
    unexpected: list[str] = []
    active_count = 0
    terminal_count = 0

    lane_unexplained: dict[str, list[str]] = {lane: [] for lane in CANONICAL_LANES}
    lane_unexpected: dict[str, list[str]] = {lane: [] for lane in CANONICAL_LANES}
    lane_active: dict[str, int] = {lane: 0 for lane in CANONICAL_LANES}
    lane_terminal: dict[str, int] = {lane: 0 for lane in CANONICAL_LANES}

    for candidate_id, lane in observed.items():
        record = records.get(candidate_id)
        if record is None:
            unexplained.append(candidate_id)
            lane_unexplained[lane].append(candidate_id)
            continue
        if record.lane != lane:
            errors.append(
                f"candidate_lane_conflict:{candidate_id}:{lane}!={record.lane}"
            )
            unexplained.append(candidate_id)
            lane_unexplained[lane].append(candidate_id)
            continue
        if record.active:
            active_count += 1
            lane_active[lane] += 1
        else:
            terminal_count += 1
            lane_terminal[lane] += 1

    for candidate_id, record in records.items():
        if candidate_id in observed:
            continue
        unexpected.append(candidate_id)
        lane_unexpected[record.lane].append(candidate_id)

    lanes: dict[str, LaneAccounting] = {}
    for lane in CANONICAL_LANES:
        observed_count = sum(1 for value in observed.values() if value == lane)
        active = lane_active[lane]
        terminal = lane_terminal[lane]
        unexplained_ids = tuple(sorted(lane_unexplained[lane]))
        unexpected_ids = tuple(sorted(lane_unexpected[lane]))
        lanes[lane] = LaneAccounting(
            lane=lane,
            observed_candidate_count=observed_count,
            active_candidate_count=active,
            terminal_candidate_count=terminal,
            accounted_candidate_count=active + terminal,
            unexplained_disappearance_count=len(unexplained_ids),
            unexplained_candidate_ids=unexplained_ids,
            unexpected_candidate_count=len(unexpected_ids),
            unexpected_candidate_ids=unexpected_ids,
        )

    unexplained_ids = tuple(sorted(unexplained))
    unexpected_ids = tuple(sorted(unexpected))
    unexplained_count = len(unexplained_ids)

    # Internal conservation invariants are explicit and fail closed.
    per_lane_unexplained = sum(
        lane.unexplained_disappearance_count for lane in lanes.values()
    )
    if per_lane_unexplained != unexplained_count:
        errors.append("aggregate_unexplained_count_mismatch")
    per_lane_observed = sum(lane.observed_candidate_count for lane in lanes.values())
    if per_lane_observed != len(observed):
        errors.append("aggregate_observed_count_mismatch")
    if active_count + terminal_count + unexplained_count != len(observed):
        errors.append("candidate_conservation_mismatch")

    accounting_valid = not errors and not unexpected_ids
    certification_ready = accounting_valid and unexplained_count == 0

    return LosslessCandidateAccounting(
        batch_version=BATCH_VERSION,
        lanes=lanes,
        observed_candidate_count=len(observed),
        active_candidate_count=active_count,
        terminal_candidate_count=terminal_count,
        accounted_candidate_count=active_count + terminal_count,
        unexplained_disappearance_count=unexplained_count,
        unexplained_candidate_ids=unexplained_ids,
        unexpected_candidate_count=len(unexpected_ids),
        unexpected_candidate_ids=unexpected_ids,
        validation_errors=tuple(errors),
        accounting_valid=accounting_valid,
        certification_ready=certification_ready,
        research_only=True,
        paper_only=PAPER_ONLY,
        live_money_authority=LIVE_MONEY_AUTHORITY,
        changes_strategy_authority=False,
        changes_economic_thresholds=False,
    )


def attach_lossless_accounting(
    five_lane_matrix: Mapping[str, Any],
    accounting: LosslessCandidateAccounting,
) -> dict[str, Any]:
    """Attach the Batch 2 hard gate without mutating v5.1 capability semantics."""

    expected_lanes = set(CANONICAL_LANES)
    matrix_lanes = set((five_lane_matrix.get("lanes") or {}).keys())
    matrix_valid = matrix_lanes == expected_lanes
    reasons: list[str] = []
    if not matrix_valid:
        reasons.append("five_lane_matrix_incomplete")
    if accounting.validation_errors:
        reasons.append("candidate_accounting_invalid")
    if accounting.unexpected_candidate_count:
        reasons.append("unexpected_candidate_records")
    if accounting.unexplained_disappearance_count:
        reasons.append("unexplained_candidate_disappearance")

    certification_ready = (
        matrix_valid
        and accounting.certification_ready
        and accounting.unexplained_disappearance_count == 0
    )

    return {
        **dict(five_lane_matrix),
        "v52_batch2": {
            "batch_version": BATCH_VERSION,
            "candidate_accounting": accounting.as_dict(),
            "unexplained_disappearance_count": accounting.unexplained_disappearance_count,
            "hard_readiness_gate_passed": certification_ready,
            "readiness_blockers": reasons,
            "research_only": True,
            "production_composition_hook": PRODUCTION_COMPOSITION_HOOK,
            "challenger_entry_authority": CHALLENGER_ENTRY_AUTHORITY,
            "incumbent_authority_changed": INCUMBENT_AUTHORITY_CHANGED,
        },
        "certification_ready": certification_ready,
        "unexplained_disappearance_count": accounting.unexplained_disappearance_count,
        "paper_only": True,
        "live_money_authority": False,
        "changes_strategy_authority": False,
        "changes_economic_thresholds": False,
    }


def run_five_lane_accounting_matrix(
    store: Any,
    observed_candidates: Iterable[ObservedCandidate],
    accounting_records: Iterable[CandidateAccountingRecord],
) -> dict[str, Any]:
    """Run the existing synthetic five-lane matrix and attach Batch 2 accounting."""

    matrix = run_five_lane_capability_matrix(store)
    accounting = reconcile_candidates(observed_candidates, accounting_records)
    return attach_lossless_accounting(matrix, accounting)


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
        "unexplained_disappearance_hard_gate": True,
        "duplicate_identity_fails_closed": True,
        "conflicting_identity_fails_closed": True,
        "malformed_identity_fails_closed": True,
        "unexpected_record_fails_closed": True,
    }


__all__ = [
    "BATCH_VERSION",
    "CANONICAL_LANES",
    "CandidateAccountingRecord",
    "LaneAccounting",
    "LosslessCandidateAccounting",
    "ObservedCandidate",
    "TerminalDisposition",
    "attach_lossless_accounting",
    "reconcile_candidates",
    "run_five_lane_accounting_matrix",
    "safety_manifest",
]
