from __future__ import annotations

"""v5.2 Batch 1: durable candidate continuity and event-driven reactivation.

This module is deliberately research-only.  It provides the canonical lifecycle
identity/state machinery required by the v5.2 challenger without installing a
production composition hook or changing v5.1 incumbent authority.
"""

import json
import math
import sqlite3
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterable

from .v52_continuation_capture import (
    CHALLENGER_EPOCH,
    CHALLENGER_VERSION,
    INCUMBENT_VERSION,
    LIVE_MONEY_AUTHORITY,
    PAPER_ONLY,
    SIGNING_AVAILABLE,
    TRANSACTION_SUBMISSION_AVAILABLE,
)


BATCH_VERSION = "v52-batch1-candidate-continuity-1"
PRODUCTION_COMPOSITION_HOOK = False
CHALLENGER_ENTRY_AUTHORITY = False
INCUMBENT_AUTHORITY_CHANGED = False


class CandidateLifecycleState(str, Enum):
    DISCOVERED = "discovered"
    DEVELOPING = "developing"
    PRE_BREAKOUT = "pre_breakout"
    ACTIONABLE = "actionable"
    ENTERED = "entered"
    SCALING = "scaling"
    PARTIAL_EXIT = "partial_exit"
    RUNNER = "runner"
    EXITED = "exited"
    REENTRY_WATCH = "reentry_watch"


class RejectionKind(str, Enum):
    TEMPORARY = "temporary"
    PERMANENT = "permanent"


class LifecycleEventType(str, Enum):
    PUMP_FUN_DETECTED = "pump_fun_detected"
    CURVE_PROGRESS_CHANGED = "curve_progress_changed"
    BUYER_ACCELERATION_CHANGED = "buyer_acceleration_changed"
    WALLET_CASCADE_CHANGED = "wallet_cascade_changed"
    LIQUIDITY_CHANGED = "liquidity_changed"
    QUOTE_REFRESHED = "quote_refreshed"
    GRADUATION_IMMINENT = "graduation_imminent"
    GRADUATED = "graduated"
    PUMPSWAP_ROUTE_AVAILABLE = "pumpswap_route_available"
    SECONDARY_POOL_ROUTE_AVAILABLE = "secondary_pool_route_available"
    CHASE_RESET = "chase_reset"
    BLOCKER_RESOLVED = "blocker_resolved"
    ENTRY_RECORDED = "entry_recorded"
    SCALE_RECORDED = "scale_recorded"
    PARTIAL_EXIT_RECORDED = "partial_exit_recorded"
    RUNNER_RECORDED = "runner_recorded"
    EXIT_RECORDED = "exit_recorded"
    REENTRY_SIGNAL = "reentry_signal"


class Surface(str, Enum):
    PUMP_FUN = "PUMP_FUN"
    PUMP_AMM = "PUMP_AMM"
    PUMPSWAP = "PUMPSWAP"
    RAYDIUM = "RAYDIUM"
    FOMO = "FOMO"
    ROBINHOOD_CHAIN = "ROBINHOOD_CHAIN"
    SECONDARY_POOL = "SECONDARY_POOL"


_ALLOWED_TRANSITIONS: dict[CandidateLifecycleState, frozenset[CandidateLifecycleState]] = {
    CandidateLifecycleState.DISCOVERED: frozenset({CandidateLifecycleState.DEVELOPING}),
    CandidateLifecycleState.DEVELOPING: frozenset({CandidateLifecycleState.PRE_BREAKOUT}),
    CandidateLifecycleState.PRE_BREAKOUT: frozenset({CandidateLifecycleState.ACTIONABLE}),
    CandidateLifecycleState.ACTIONABLE: frozenset({CandidateLifecycleState.ENTERED}),
    CandidateLifecycleState.ENTERED: frozenset(
        {
            CandidateLifecycleState.SCALING,
            CandidateLifecycleState.PARTIAL_EXIT,
            CandidateLifecycleState.RUNNER,
            CandidateLifecycleState.EXITED,
        }
    ),
    CandidateLifecycleState.SCALING: frozenset(
        {
            CandidateLifecycleState.PARTIAL_EXIT,
            CandidateLifecycleState.RUNNER,
            CandidateLifecycleState.EXITED,
        }
    ),
    CandidateLifecycleState.PARTIAL_EXIT: frozenset(
        {CandidateLifecycleState.RUNNER, CandidateLifecycleState.EXITED}
    ),
    CandidateLifecycleState.RUNNER: frozenset({CandidateLifecycleState.EXITED}),
    CandidateLifecycleState.EXITED: frozenset({CandidateLifecycleState.REENTRY_WATCH}),
    CandidateLifecycleState.REENTRY_WATCH: frozenset({CandidateLifecycleState.ACTIONABLE}),
}


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_text(value: Any, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field}_missing")
    return text


def _finite_nonnegative(value: Any, *, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field}_invalid") from exc
    if not math.isfinite(number) or number < 0.0:
        raise ValueError(f"{field}_invalid")
    return number


def _normalize_timestamp(value: Any, *, field: str) -> str:
    text = _require_text(value, field=field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field}_invalid") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field}_timezone_required")
    return parsed.astimezone(timezone.utc).isoformat()


def _enum_value(enum_type: type[Enum], value: Any, *, field: str):
    try:
        return enum_type(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field}_invalid") from exc


@dataclass(frozen=True)
class ExecutableQuoteSnapshot:
    """Exact two-sided executable evidence retained across temporary rejection."""

    entry_price: float
    exit_price: float
    quote_timestamp: str
    entry_exact: bool = True
    exit_exact: bool = True
    structurally_exitable: bool = True

    def validated(self) -> "ExecutableQuoteSnapshot":
        entry = _finite_nonnegative(self.entry_price, field="entry_price")
        exit_ = _finite_nonnegative(self.exit_price, field="exit_price")
        if entry <= 0.0:
            raise ValueError("entry_price_invalid")
        if exit_ <= 0.0:
            raise ValueError("exit_price_invalid")
        timestamp = _normalize_timestamp(self.quote_timestamp, field="quote_timestamp")
        if not self.entry_exact:
            raise ValueError("exact_entry_quote_required")
        if not self.exit_exact:
            raise ValueError("exact_exit_quote_required")
        if not self.structurally_exitable:
            raise ValueError("structural_exitability_required")
        return replace(self, entry_price=entry, exit_price=exit_, quote_timestamp=timestamp)


@dataclass(frozen=True)
class SurfaceObservation:
    surface: str
    route_id: str | None
    observed_at: str
    event_type: str

    def validated(self) -> "SurfaceObservation":
        surface = _enum_value(Surface, str(self.surface or "").upper(), field="surface")
        event = _enum_value(LifecycleEventType, self.event_type, field="event_type")
        route = str(self.route_id).strip() if self.route_id is not None else None
        return replace(
            self,
            surface=surface.value,
            route_id=route or None,
            observed_at=_normalize_timestamp(self.observed_at, field="observed_at"),
            event_type=event.value,
        )


@dataclass(frozen=True)
class RejectionContext:
    kind: str
    last_valid_state: str
    blocker_reason: str
    latest_exact_executable_quote: ExecutableQuoteSnapshot | None
    quote_timestamp: str | None
    distance_to_actionable: float | None
    active_event_subscriptions: tuple[str, ...]
    rejected_at: str

    @property
    def temporary(self) -> bool:
        return self.kind == RejectionKind.TEMPORARY.value

    @property
    def permanent(self) -> bool:
        return self.kind == RejectionKind.PERMANENT.value


@dataclass(frozen=True)
class CandidateLifecycle:
    candidate_id: str
    asset_id: str
    state: str
    last_valid_state: str
    current_surface: str
    created_at: str
    updated_at: str
    source_signatures: tuple[str, ...]
    surface_history: tuple[SurfaceObservation, ...]
    rejection: RejectionContext | None = None
    lifecycle_sequence: int = 0
    research_only: bool = True
    entry_authority: bool = False
    incumbent_authority_changed: bool = False

    @property
    def permanently_rejected(self) -> bool:
        return bool(self.rejection and self.rejection.permanent)

    @property
    def temporarily_rejected(self) -> bool:
        return bool(self.rejection and self.rejection.temporary)


@dataclass(frozen=True)
class LifecycleEvent:
    candidate_id: str
    event_type: str
    observed_at: str
    surface: str | None = None
    route_id: str | None = None
    source_signature: str | None = None
    target_state: str | None = None
    blocker_resolved: bool = False

    def validated(self) -> "LifecycleEvent":
        candidate_id = _require_text(self.candidate_id, field="candidate_id")
        event = _enum_value(LifecycleEventType, self.event_type, field="event_type")
        surface = None
        if self.surface is not None:
            surface = _enum_value(Surface, str(self.surface).upper(), field="surface").value
        target = None
        if self.target_state is not None:
            target = _enum_value(CandidateLifecycleState, self.target_state, field="target_state").value
        signature = str(self.source_signature).strip() if self.source_signature is not None else None
        route = str(self.route_id).strip() if self.route_id is not None else None
        return replace(
            self,
            candidate_id=candidate_id,
            event_type=event.value,
            observed_at=_normalize_timestamp(self.observed_at, field="observed_at"),
            surface=surface,
            route_id=route or None,
            source_signature=signature or None,
            target_state=target,
        )


@dataclass(frozen=True)
class EventProcessingResult:
    candidate: CandidateLifecycle
    reevaluation_triggered: bool
    rejection_cleared: bool
    state_changed: bool
    event_consumed: bool
    reason: str


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
        "temporary_rejection_preserves_state": True,
        "temporary_rejection_requires_event_subscriptions": True,
        "temporary_rejection_requires_exact_quote": True,
        "permanent_rejection_is_terminal": True,
        "canonical_lifecycle_states": [state.value for state in CandidateLifecycleState],
    }


def new_candidate(
    *,
    candidate_id: str,
    asset_id: str,
    surface: str,
    observed_at: str,
    source_signature: str,
    route_id: str | None = None,
) -> CandidateLifecycle:
    candidate_id = _require_text(candidate_id, field="candidate_id")
    asset_id = _require_text(asset_id, field="asset_id")
    source_signature = _require_text(source_signature, field="source_signature")
    surface_value = _enum_value(Surface, str(surface or "").upper(), field="surface").value
    timestamp = _normalize_timestamp(observed_at, field="observed_at")
    first_surface = SurfaceObservation(
        surface=surface_value,
        route_id=route_id,
        observed_at=timestamp,
        event_type=LifecycleEventType.PUMP_FUN_DETECTED.value,
    ).validated()
    return CandidateLifecycle(
        candidate_id=candidate_id,
        asset_id=asset_id,
        state=CandidateLifecycleState.DISCOVERED.value,
        last_valid_state=CandidateLifecycleState.DISCOVERED.value,
        current_surface=surface_value,
        created_at=timestamp,
        updated_at=timestamp,
        source_signatures=(source_signature,),
        surface_history=(first_surface,),
        lifecycle_sequence=0,
        research_only=True,
        entry_authority=False,
        incumbent_authority_changed=False,
    )


def transition(
    candidate: CandidateLifecycle,
    target_state: str,
    *,
    observed_at: str,
) -> CandidateLifecycle:
    if candidate.permanently_rejected:
        raise ValueError("permanent_rejection_terminal")
    if candidate.temporarily_rejected:
        raise ValueError("temporary_rejection_must_be_reactivated_by_subscribed_event")
    current = _enum_value(CandidateLifecycleState, candidate.state, field="state")
    target = _enum_value(CandidateLifecycleState, target_state, field="target_state")
    if target not in _ALLOWED_TRANSITIONS[current]:
        raise ValueError(f"invalid_lifecycle_transition:{current.value}->{target.value}")
    timestamp = _normalize_timestamp(observed_at, field="observed_at")
    return replace(
        candidate,
        state=target.value,
        last_valid_state=target.value,
        updated_at=timestamp,
        lifecycle_sequence=candidate.lifecycle_sequence + 1,
    )


def record_surface_transition(
    candidate: CandidateLifecycle,
    *,
    surface: str,
    event_type: str,
    observed_at: str,
    route_id: str | None = None,
    source_signature: str | None = None,
) -> CandidateLifecycle:
    if candidate.permanently_rejected:
        raise ValueError("permanent_rejection_terminal")
    observation = SurfaceObservation(
        surface=surface,
        route_id=route_id,
        observed_at=observed_at,
        event_type=event_type,
    ).validated()
    signatures = candidate.source_signatures
    if source_signature is not None:
        signature = _require_text(source_signature, field="source_signature")
        if signature not in signatures:
            signatures = signatures + (signature,)
    return replace(
        candidate,
        current_surface=observation.surface,
        updated_at=observation.observed_at,
        source_signatures=signatures,
        surface_history=candidate.surface_history + (observation,),
        lifecycle_sequence=candidate.lifecycle_sequence + 1,
    )


def reject_temporarily(
    candidate: CandidateLifecycle,
    *,
    blocker_reason: str,
    latest_exact_executable_quote: ExecutableQuoteSnapshot,
    distance_to_actionable: float,
    active_event_subscriptions: Iterable[str],
    rejected_at: str,
) -> CandidateLifecycle:
    if candidate.permanently_rejected:
        raise ValueError("permanent_rejection_terminal")
    reason = _require_text(blocker_reason, field="blocker_reason")
    quote = latest_exact_executable_quote.validated()
    distance = _finite_nonnegative(distance_to_actionable, field="distance_to_actionable")
    subscriptions: list[str] = []
    for raw in active_event_subscriptions:
        event = _enum_value(LifecycleEventType, raw, field="event_subscription").value
        if event not in subscriptions:
            subscriptions.append(event)
    if not subscriptions:
        raise ValueError("temporary_rejection_event_subscriptions_required")
    timestamp = _normalize_timestamp(rejected_at, field="rejected_at")
    context = RejectionContext(
        kind=RejectionKind.TEMPORARY.value,
        last_valid_state=candidate.last_valid_state,
        blocker_reason=reason,
        latest_exact_executable_quote=quote,
        quote_timestamp=quote.quote_timestamp,
        distance_to_actionable=distance,
        active_event_subscriptions=tuple(subscriptions),
        rejected_at=timestamp,
    )
    # Crucially, state and last_valid_state are retained.  A temporary rejection
    # pauses advancement; it never sends the candidate back to discovery.
    return replace(candidate, rejection=context, updated_at=timestamp)


def reject_permanently(
    candidate: CandidateLifecycle,
    *,
    blocker_reason: str,
    rejected_at: str,
) -> CandidateLifecycle:
    reason = _require_text(blocker_reason, field="blocker_reason")
    timestamp = _normalize_timestamp(rejected_at, field="rejected_at")
    context = RejectionContext(
        kind=RejectionKind.PERMANENT.value,
        last_valid_state=candidate.last_valid_state,
        blocker_reason=reason,
        latest_exact_executable_quote=None,
        quote_timestamp=None,
        distance_to_actionable=None,
        active_event_subscriptions=(),
        rejected_at=timestamp,
    )
    return replace(candidate, rejection=context, updated_at=timestamp)


def process_event(candidate: CandidateLifecycle, event: LifecycleEvent) -> EventProcessingResult:
    event = event.validated()
    if event.candidate_id != candidate.candidate_id:
        raise ValueError("event_candidate_identity_mismatch")
    if candidate.permanently_rejected:
        return EventProcessingResult(
            candidate=candidate,
            reevaluation_triggered=False,
            rejection_cleared=False,
            state_changed=False,
            event_consumed=False,
            reason="permanent_rejection_terminal",
        )

    updated = candidate
    if event.surface is not None:
        updated = record_surface_transition(
            updated,
            surface=event.surface,
            event_type=event.event_type,
            observed_at=event.observed_at,
            route_id=event.route_id,
            source_signature=event.source_signature,
        )
    elif event.source_signature is not None and event.source_signature not in updated.source_signatures:
        updated = replace(
            updated,
            source_signatures=updated.source_signatures + (event.source_signature,),
            updated_at=event.observed_at,
            lifecycle_sequence=updated.lifecycle_sequence + 1,
        )

    if updated.temporarily_rejected:
        rejection = updated.rejection
        assert rejection is not None
        if event.event_type not in rejection.active_event_subscriptions:
            return EventProcessingResult(
                candidate=updated,
                reevaluation_triggered=False,
                rejection_cleared=False,
                state_changed=False,
                event_consumed=True,
                reason="event_not_subscribed_for_reevaluation",
            )
        if not event.blocker_resolved:
            return EventProcessingResult(
                candidate=updated,
                reevaluation_triggered=True,
                rejection_cleared=False,
                state_changed=False,
                event_consumed=True,
                reason="reevaluation_triggered_blocker_still_active",
            )
        # The subscribed event is the reactivation boundary.  Clear only the
        # temporary blocker; retain canonical state, history and identity.
        updated = replace(updated, rejection=None, updated_at=event.observed_at)
        state_changed = False
        if event.target_state is not None:
            before = updated.state
            updated = transition(updated, event.target_state, observed_at=event.observed_at)
            state_changed = before != updated.state
        return EventProcessingResult(
            candidate=updated,
            reevaluation_triggered=True,
            rejection_cleared=True,
            state_changed=state_changed,
            event_consumed=True,
            reason="temporary_rejection_reactivated",
        )

    state_changed = False
    if event.target_state is not None:
        before = updated.state
        updated = transition(updated, event.target_state, observed_at=event.observed_at)
        state_changed = before != updated.state
    return EventProcessingResult(
        candidate=updated,
        reevaluation_triggered=False,
        rejection_cleared=False,
        state_changed=state_changed,
        event_consumed=True,
        reason="event_applied",
    )


def _quote_to_dict(quote: ExecutableQuoteSnapshot | None) -> dict[str, Any] | None:
    return asdict(quote) if quote is not None else None


def _surface_to_dict(item: SurfaceObservation) -> dict[str, Any]:
    return asdict(item)


def _candidate_to_payload(candidate: CandidateLifecycle) -> dict[str, Any]:
    rejection = None
    if candidate.rejection is not None:
        rejection = asdict(candidate.rejection)
        rejection["latest_exact_executable_quote"] = _quote_to_dict(
            candidate.rejection.latest_exact_executable_quote
        )
    return {
        "candidate_id": candidate.candidate_id,
        "asset_id": candidate.asset_id,
        "state": candidate.state,
        "last_valid_state": candidate.last_valid_state,
        "current_surface": candidate.current_surface,
        "created_at": candidate.created_at,
        "updated_at": candidate.updated_at,
        "source_signatures": list(candidate.source_signatures),
        "surface_history": [_surface_to_dict(item) for item in candidate.surface_history],
        "rejection": rejection,
        "lifecycle_sequence": candidate.lifecycle_sequence,
        "research_only": candidate.research_only,
        "entry_authority": candidate.entry_authority,
        "incumbent_authority_changed": candidate.incumbent_authority_changed,
    }


def _candidate_from_payload(payload: dict[str, Any]) -> CandidateLifecycle:
    rejection_payload = payload.get("rejection")
    rejection = None
    if isinstance(rejection_payload, dict):
        quote_payload = rejection_payload.get("latest_exact_executable_quote")
        quote = ExecutableQuoteSnapshot(**quote_payload) if isinstance(quote_payload, dict) else None
        rejection = RejectionContext(
            kind=str(rejection_payload["kind"]),
            last_valid_state=str(rejection_payload["last_valid_state"]),
            blocker_reason=str(rejection_payload["blocker_reason"]),
            latest_exact_executable_quote=quote,
            quote_timestamp=rejection_payload.get("quote_timestamp"),
            distance_to_actionable=rejection_payload.get("distance_to_actionable"),
            active_event_subscriptions=tuple(rejection_payload.get("active_event_subscriptions") or ()),
            rejected_at=str(rejection_payload["rejected_at"]),
        )
    return CandidateLifecycle(
        candidate_id=str(payload["candidate_id"]),
        asset_id=str(payload["asset_id"]),
        state=str(payload["state"]),
        last_valid_state=str(payload["last_valid_state"]),
        current_surface=str(payload["current_surface"]),
        created_at=str(payload["created_at"]),
        updated_at=str(payload["updated_at"]),
        source_signatures=tuple(payload.get("source_signatures") or ()),
        surface_history=tuple(SurfaceObservation(**row) for row in payload.get("surface_history") or ()),
        rejection=rejection,
        lifecycle_sequence=int(payload.get("lifecycle_sequence") or 0),
        research_only=bool(payload.get("research_only", True)),
        entry_authority=bool(payload.get("entry_authority", False)),
        incumbent_authority_changed=bool(payload.get("incumbent_authority_changed", False)),
    )


class CandidateContinuityStore:
    """Minimal durable canonical store for Batch-1 lifecycle records.

    The caller owns the SQLite connection.  The module does not open production
    storage, alter retention, or install itself into the production runtime.
    """

    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self.db:
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_candidate_lifecycle ("
                "candidate_id TEXT PRIMARY KEY, asset_id TEXT NOT NULL, state TEXT NOT NULL, "
                "last_valid_state TEXT NOT NULL, current_surface TEXT NOT NULL, "
                "payload_json TEXT NOT NULL, lifecycle_sequence INTEGER NOT NULL, "
                "updated_at TEXT NOT NULL)"
            )
            self.db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_v52_candidate_lifecycle_asset "
                "ON v52_candidate_lifecycle(asset_id)"
            )
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_candidate_lifecycle_events ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, candidate_id TEXT NOT NULL, "
                "event_type TEXT NOT NULL, observed_at TEXT NOT NULL, state_before TEXT NOT NULL, "
                "state_after TEXT NOT NULL, reevaluation_triggered INTEGER NOT NULL, "
                "rejection_cleared INTEGER NOT NULL, reason TEXT NOT NULL, payload_json TEXT NOT NULL)"
            )

    def put(self, candidate: CandidateLifecycle) -> CandidateLifecycle:
        payload = json.dumps(_candidate_to_payload(candidate), sort_keys=True, separators=(",", ":"))
        with self.db:
            self.db.execute(
                "INSERT INTO v52_candidate_lifecycle("
                "candidate_id,asset_id,state,last_valid_state,current_surface,payload_json,lifecycle_sequence,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?) "
                "ON CONFLICT(candidate_id) DO UPDATE SET "
                "asset_id=excluded.asset_id,state=excluded.state,last_valid_state=excluded.last_valid_state,"
                "current_surface=excluded.current_surface,payload_json=excluded.payload_json,"
                "lifecycle_sequence=excluded.lifecycle_sequence,updated_at=excluded.updated_at",
                (
                    candidate.candidate_id,
                    candidate.asset_id,
                    candidate.state,
                    candidate.last_valid_state,
                    candidate.current_surface,
                    payload,
                    candidate.lifecycle_sequence,
                    candidate.updated_at,
                ),
            )
        return candidate

    def create(self, candidate: CandidateLifecycle) -> CandidateLifecycle:
        if self.get(candidate.candidate_id) is not None:
            raise ValueError("candidate_id_already_exists")
        row = self.db.execute(
            "SELECT candidate_id FROM v52_candidate_lifecycle WHERE asset_id=? LIMIT 1",
            (candidate.asset_id,),
        ).fetchone()
        if row is not None:
            raise ValueError("asset_id_already_has_canonical_candidate")
        return self.put(candidate)

    def get(self, candidate_id: str) -> CandidateLifecycle | None:
        candidate_id = _require_text(candidate_id, field="candidate_id")
        row = self.db.execute(
            "SELECT payload_json FROM v52_candidate_lifecycle WHERE candidate_id=? LIMIT 1",
            (candidate_id,),
        ).fetchone()
        if row is None:
            return None
        return _candidate_from_payload(json.loads(str(row[0])))

    def get_by_asset(self, asset_id: str) -> CandidateLifecycle | None:
        asset_id = _require_text(asset_id, field="asset_id")
        row = self.db.execute(
            "SELECT payload_json FROM v52_candidate_lifecycle WHERE asset_id=? LIMIT 1",
            (asset_id,),
        ).fetchone()
        if row is None:
            return None
        return _candidate_from_payload(json.loads(str(row[0])))

    def process(self, event: LifecycleEvent) -> EventProcessingResult:
        event = event.validated()
        candidate = self.get(event.candidate_id)
        if candidate is None:
            raise ValueError("candidate_not_found")
        before = candidate.state
        result = process_event(candidate, event)
        self.put(result.candidate)
        with self.db:
            self.db.execute(
                "INSERT INTO v52_candidate_lifecycle_events("
                "candidate_id,event_type,observed_at,state_before,state_after,"
                "reevaluation_triggered,rejection_cleared,reason,payload_json) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    event.candidate_id,
                    event.event_type,
                    event.observed_at,
                    before,
                    result.candidate.state,
                    int(result.reevaluation_triggered),
                    int(result.rejection_cleared),
                    result.reason,
                    json.dumps(asdict(event), sort_keys=True, separators=(",", ":")),
                ),
            )
        return result

    def event_count(self, candidate_id: str) -> int:
        row = self.db.execute(
            "SELECT COUNT(*) FROM v52_candidate_lifecycle_events WHERE candidate_id=?",
            (_require_text(candidate_id, field="candidate_id"),),
        ).fetchone()
        return int(row[0] if row is not None else 0)


__all__ = [
    "BATCH_VERSION",
    "CandidateContinuityStore",
    "CandidateLifecycle",
    "CandidateLifecycleState",
    "EventProcessingResult",
    "ExecutableQuoteSnapshot",
    "LifecycleEvent",
    "LifecycleEventType",
    "RejectionContext",
    "RejectionKind",
    "Surface",
    "new_candidate",
    "process_event",
    "record_surface_transition",
    "reject_permanently",
    "reject_temporarily",
    "safety_manifest",
    "transition",
]
