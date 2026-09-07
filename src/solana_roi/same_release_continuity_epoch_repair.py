from __future__ import annotations

from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable

from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from . import strategy_relevant_continuity as continuity
from . import target_stream_fanout as fanout
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


REPAIR_VERSION = "same-release-continuity-successor-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_ORIGINAL_START: Callable[[Any], bool] | None = None
_ORIGINAL_EPOCH_ROW: Callable[[Any], Any | None] | None = None
_ORIGINAL_LATCH: Callable[..., bool] | None = None
_ORIGINAL_STATUS: Callable[[Any], dict[str, Any]] | None = None
_INSTALLED = False


def _release_commit() -> str:
    return continuity._release_commit()


def _iso(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _after(value: Any, boundary: Any) -> bool:
    left = _iso(value)
    right = _iso(boundary)
    if not left or not right:
        return False
    return datetime.fromisoformat(left) > datetime.fromisoformat(right)


def _now() -> str:
    return continuity.direct_module.utcnow().isoformat()


def _store(self: Any) -> Any | None:
    store = getattr(self, "store", None)
    if store is None or not hasattr(store, "db") or not hasattr(store, "_lock"):
        return None
    return store


def _ensure_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS direct_solana_strategy_continuity_epoch_v2 ("
            "epoch_id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, "
            "generation INTEGER NOT NULL, started_at TEXT NOT NULL, state TEXT NOT NULL, "
            "failed_at TEXT, failure_error TEXT, predecessor_epoch_id INTEGER, "
            "evidence_floor_at TEXT, websocket_evidence_at TEXT, poll_evidence_at TEXT, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "UNIQUE(release_commit,generation))"
        )
        store.db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_direct_strategy_continuity_v2_one_active "
            "ON direct_solana_strategy_continuity_epoch_v2(release_commit) WHERE state='active'"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS direct_solana_strategy_continuity_gap_event ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, "
            "epoch_id INTEGER, target_key TEXT NOT NULL, ws_gap_generation INTEGER NOT NULL, "
            "gap_started_at TEXT NOT NULL, observed_at TEXT NOT NULL, error TEXT NOT NULL, "
            "UNIQUE(release_commit,target_key,ws_gap_generation,gap_started_at))"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_direct_strategy_gap_event_epoch "
            "ON direct_solana_strategy_continuity_gap_event(release_commit,epoch_id,observed_at)"
        )


def _epoch_rows(self: Any) -> list[Any]:
    store = _store(self)
    if store is None:
        return []
    _ensure_schema(store)
    with store._lock:
        return list(
            store.db.execute(
                "SELECT epoch_id,release_commit,generation,started_at,state,failed_at,failure_error,"
                "predecessor_epoch_id,evidence_floor_at,websocket_evidence_at,poll_evidence_at,"
                "paper_only,live_money_authority "
                "FROM direct_solana_strategy_continuity_epoch_v2 "
                "WHERE release_commit=? ORDER BY generation",
                (_release_commit(),),
            ).fetchall()
        )


def _active_epoch(self: Any) -> Any | None:
    rows = _epoch_rows(self)
    for row in reversed(rows):
        if str(row["state"]) == "active":
            return row
    return None


def _latest_epoch(self: Any) -> Any | None:
    rows = _epoch_rows(self)
    return rows[-1] if rows else None


def _global_state(self: Any) -> Any | None:
    store = _store(self)
    if store is None:
        return None
    try:
        with store._lock:
            return store.db.execute(
                "SELECT outage_started_at,unresolved_gap,last_backfill_complete_at,last_backfill_error "
                "FROM direct_solana_global_state WHERE id=1"
            ).fetchone()
    except Exception:
        return None


def _bootstrap_from_legacy(self: Any) -> Any | None:
    if _latest_epoch(self) is not None:
        return _latest_epoch(self)
    if _ORIGINAL_EPOCH_ROW is None:
        return None
    legacy = _ORIGINAL_EPOCH_ROW(self)
    if legacy is None:
        return None

    store = _store(self)
    if store is None:
        return None
    state = _global_state(self)
    unresolved = bool(state["unresolved_gap"]) if state is not None else False
    error = str(state["last_backfill_error"] or "") if state is not None else ""
    started_at = _iso(legacy["started_at"]) or _now()
    failed_at = _now() if unresolved else None
    epoch_state = "failed" if unresolved else "active"
    with store._lock, store.db:
        store.db.execute(
            "INSERT OR IGNORE INTO direct_solana_strategy_continuity_epoch_v2("
            "release_commit,generation,started_at,state,failed_at,failure_error,predecessor_epoch_id,"
            "evidence_floor_at,websocket_evidence_at,poll_evidence_at,paper_only,live_money_authority) "
            "VALUES (?,1,?,?,?,?,NULL,?,NULL,NULL,1,0)",
            (
                _release_commit(),
                started_at,
                epoch_state,
                failed_at,
                error or None,
                failed_at,
            ),
        )
        row = store.db.execute(
            "SELECT epoch_id,release_commit,generation,started_at,state,failed_at,failure_error,"
            "predecessor_epoch_id,evidence_floor_at,websocket_evidence_at,poll_evidence_at,"
            "paper_only,live_money_authority FROM direct_solana_strategy_continuity_epoch_v2 "
            "WHERE release_commit=? AND generation=1",
            (_release_commit(),),
        ).fetchone()
        if unresolved and row is not None:
            store.db.execute(
                "INSERT OR IGNORE INTO direct_solana_strategy_continuity_gap_event("
                "release_commit,epoch_id,target_key,ws_gap_generation,gap_started_at,observed_at,error) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    _release_commit(),
                    int(row["epoch_id"]),
                    "strategy:legacy-unresolved-gap",
                    -1,
                    failed_at,
                    failed_at,
                    error or "legacy unresolved strategy continuity gap observed during v2 upgrade",
                ),
            )
    return row


def _record_gap_event(
    self: Any,
    target: WatchTarget,
    generation: int,
    started_at_iso: str | None,
) -> None:
    store = _store(self)
    if store is None or target.kind != "scout":
        return
    _ensure_schema(store)
    if _latest_epoch(self) is None and _ORIGINAL_START is not None:
        _ORIGINAL_START(self)
        _bootstrap_from_legacy(self)

    active = _active_epoch(self)
    latest = active or _latest_epoch(self)
    boundary = _iso(started_at_iso) or _now()
    observed = _now()
    epoch_id = int(latest["epoch_id"]) if latest is not None else None
    error = lease.IRRECOVERABLE_POLL_GAP_ERROR
    with store._lock, store.db:
        store.db.execute(
            "INSERT OR IGNORE INTO direct_solana_strategy_continuity_gap_event("
            "release_commit,epoch_id,target_key,ws_gap_generation,gap_started_at,observed_at,error) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                _release_commit(),
                epoch_id,
                continuity._target_key(target),
                int(generation),
                boundary,
                observed,
                error,
            ),
        )
        if active is not None:
            store.db.execute(
                "UPDATE direct_solana_strategy_continuity_epoch_v2 "
                "SET state='failed',failed_at=?,failure_error=?,evidence_floor_at=? "
                "WHERE epoch_id=? AND state='active'",
                (boundary, error, boundary, int(active["epoch_id"])),
            )
    setattr(self, "_roi_strategy_continuity_observed", False)


def _failure_floor(self: Any, epoch: Any) -> str | None:
    store = _store(self)
    if store is None:
        return _iso(epoch["failed_at"])
    _ensure_schema(store)
    with store._lock:
        row = store.db.execute(
            "SELECT MAX(gap_started_at) AS boundary FROM direct_solana_strategy_continuity_gap_event "
            "WHERE release_commit=? AND epoch_id=?",
            (_release_commit(), int(epoch["epoch_id"])),
        ).fetchone()
    event_floor = _iso(row["boundary"]) if row is not None else None
    failed_at = _iso(epoch["failed_at"])
    candidates = [value for value in (event_floor, failed_at) if value]
    if not candidates:
        return None
    return max(candidates, key=lambda value: datetime.fromisoformat(value))


def _fresh_websocket_evidence(self: Any, boundary: str) -> tuple[bool, str | None]:
    strategy_targets = tuple(
        target for target in (getattr(self, "watch_targets", ()) or ()) if target.kind == "scout"
    )
    if not strategy_targets:
        return False, None
    _lock, provider_targets, _events, states = fanout._state_maps(self)
    satisfied_at: list[str] = []
    for target in strategy_targets:
        key = continuity._target_key(target)
        target_times: list[str] = []
        for provider, live_targets in provider_targets.items():
            if provider == live_poll.POLL_PROVIDER_NAME or key not in set(live_targets):
                continue
            provider_state = states.get(provider, {})
            row = provider_state.get(key) if isinstance(provider_state, dict) else None
            if not isinstance(row, dict) or not bool(row.get("connected")):
                continue
            changed = _iso(row.get("last_change_at"))
            if changed and _after(changed, boundary):
                target_times.append(changed)
        if not target_times:
            return False, None
        satisfied_at.append(max(target_times, key=lambda value: datetime.fromisoformat(value)))
    return True, max(satisfied_at, key=lambda value: datetime.fromisoformat(value))


def _fresh_poll_evidence(self: Any, boundary: str) -> tuple[bool, str | None]:
    store = _store(self)
    strategy_targets = tuple(
        target for target in (getattr(self, "watch_targets", ()) or ()) if target.kind == "scout"
    )
    if store is None or not strategy_targets:
        return False, None
    state = live_poll._poll_state(self)
    satisfied_at: list[str] = []
    try:
        with store._lock:
            for target in strategy_targets:
                key = continuity._target_key(target)
                runtime_row = state.get(key)
                runtime_at = _iso(runtime_row.get("last_success_at")) if isinstance(runtime_row, dict) else None
                checkpoint = store.db.execute(
                    "SELECT last_success_at FROM direct_solana_strategy_poll_checkpoint "
                    "WHERE release_commit=? AND target_key=?",
                    (_release_commit(), key),
                ).fetchone()
                durable_at = _iso(checkpoint["last_success_at"]) if checkpoint is not None else None
                if (
                    not runtime_at
                    or not durable_at
                    or not _after(runtime_at, boundary)
                    or not _after(durable_at, boundary)
                ):
                    return False, None
                satisfied_at.append(
                    min(
                        (runtime_at, durable_at),
                        key=lambda value: datetime.fromisoformat(value),
                    )
                )
    except Exception:
        return False, None
    return True, max(satisfied_at, key=lambda value: datetime.fromisoformat(value))


def _terminalize_untracked_global_gap(self: Any, active: Any) -> None:
    store = _store(self)
    if store is None:
        return
    state = _global_state(self)
    if state is None or not bool(state["unresolved_gap"]):
        return
    boundary = _now()
    error = str(state["last_backfill_error"] or "") or lease.IRRECOVERABLE_POLL_GAP_ERROR
    with store._lock, store.db:
        store.db.execute(
            "UPDATE direct_solana_strategy_continuity_epoch_v2 "
            "SET state='failed',failed_at=?,failure_error=?,evidence_floor_at=? "
            "WHERE epoch_id=? AND state='active'",
            (boundary, error, boundary, int(active["epoch_id"])),
        )
        store.db.execute(
            "INSERT OR IGNORE INTO direct_solana_strategy_continuity_gap_event("
            "release_commit,epoch_id,target_key,ws_gap_generation,gap_started_at,observed_at,error) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                _release_commit(),
                int(active["epoch_id"]),
                "strategy:untracked-global-gap",
                -1,
                boundary,
                boundary,
                error,
            ),
        )
    setattr(self, "_roi_strategy_continuity_observed", False)


def _start_strategy_epoch_if_ready(self: Any) -> bool:
    if _ORIGINAL_START is None:
        return False
    if _release_commit() == "unbound-local-release":
        return bool(_ORIGINAL_START(self))
    if not getattr(self, "watch_targets", None) or _store(self) is None:
        return False

    snapshot = continuity._coverage_snapshot(self)
    if not snapshot["strategy_startup_ready"]:
        return False

    if _latest_epoch(self) is None:
        _ORIGINAL_START(self)
        _bootstrap_from_legacy(self)

    active = _active_epoch(self)
    global_state = _global_state(self)
    if active is not None:
        if global_state is not None and bool(global_state["unresolved_gap"]):
            _terminalize_untracked_global_gap(self, active)
            return False
        setattr(self, "_roi_strategy_continuity_observed", True)
        return True

    failed = _latest_epoch(self)
    if failed is None or str(failed["state"]) != "failed":
        return False
    boundary = _failure_floor(self, failed)
    if not boundary:
        return False

    websocket_ok, websocket_at = _fresh_websocket_evidence(self, boundary)
    poll_ok, poll_at = _fresh_poll_evidence(self, boundary)
    if not websocket_ok or not poll_ok or websocket_at is None or poll_at is None:
        setattr(self, "_roi_strategy_continuity_observed", False)
        return False

    store = _store(self)
    if store is None:
        return False
    now = _now()
    with store._lock, store.db:
        existing = store.db.execute(
            "SELECT epoch_id FROM direct_solana_strategy_continuity_epoch_v2 "
            "WHERE release_commit=? AND state='active'",
            (_release_commit(),),
        ).fetchone()
        if existing is None:
            generation = int(failed["generation"]) + 1
            store.db.execute(
                "INSERT INTO direct_solana_strategy_continuity_epoch_v2("
                "release_commit,generation,started_at,state,failed_at,failure_error,predecessor_epoch_id,"
                "evidence_floor_at,websocket_evidence_at,poll_evidence_at,paper_only,live_money_authority) "
                "VALUES (?,?,?,'active',NULL,NULL,?,?,?,?,1,0)",
                (
                    _release_commit(),
                    generation,
                    now,
                    int(failed["epoch_id"]),
                    boundary,
                    websocket_at,
                    poll_at,
                ),
            )
            store.db.execute(
                "UPDATE direct_solana_global_state SET outage_started_at=NULL,unresolved_gap=0,"
                "last_backfill_complete_at=NULL,last_backfill_error=NULL WHERE id=1"
            )
    setattr(self, "_roi_strategy_continuity_observed", True)
    return True


def _strategy_epoch_row(self: Any) -> Any | None:
    if _release_commit() == "unbound-local-release":
        return _ORIGINAL_EPOCH_ROW(self) if _ORIGINAL_EPOCH_ROW is not None else None
    active = _active_epoch(self)
    if active is not None:
        return active
    return None


def _latch_with_epoch_history(
    self: Any,
    target: WatchTarget,
    generation: int,
    started_at_iso: str | None,
) -> bool:
    if _ORIGINAL_LATCH is None:
        raise RuntimeError("same-release continuity repair missing original latch")
    latched = bool(_ORIGINAL_LATCH(self, target, generation, started_at_iso))
    if latched and target.kind == "scout":
        _record_gap_event(self, target, generation, started_at_iso)
    return latched


def _status_with_successor_epochs(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("same-release continuity repair missing original status")
    payload = _ORIGINAL_STATUS(self)
    if not getattr(self, "watch_targets", None) or _store(self) is None:
        return payload

    rows = _epoch_rows(self)
    current = next((row for row in reversed(rows) if str(row["state"]) == "active"), None)
    latest = rows[-1] if rows else None
    failed = [row for row in rows if str(row["state"]) == "failed"]
    store = _store(self)
    gap_count = 0
    if store is not None:
        try:
            with store._lock:
                row = store.db.execute(
                    "SELECT COUNT(*) AS n FROM direct_solana_strategy_continuity_gap_event "
                    "WHERE release_commit=?",
                    (_release_commit(),),
                ).fetchone()
            gap_count = int(row["n"]) if row is not None else 0
        except Exception:
            gap_count = 0

    details = payload.get("strategy_relevant_continuity")
    if isinstance(details, dict):
        details.update(
            {
                "epoch_model": "independent-same-release-successor",
                "epoch_id": int(current["epoch_id"]) if current is not None else None,
                "epoch_generation": int(current["generation"]) if current is not None else None,
                "latest_epoch_state": str(latest["state"]) if latest is not None else None,
                "latest_epoch_id": int(latest["epoch_id"]) if latest is not None else None,
                "failed_epoch_count_same_release": len(failed),
                "gap_event_count_same_release": gap_count,
                "same_release_successor_supported": True,
                "failed_history_preserved": True,
                "stale_pre_gap_evidence_can_rearm": False,
                "fresh_post_gap_websocket_and_poll_required": True,
            }
        )

    barrier = payload.get("continuity_startup_barrier")
    if isinstance(barrier, dict):
        barrier.update(
            {
                "epoch_model": "independent-same-release-successor",
                "same_release_successor_supported": True,
                "restart_alone_can_clear_failed_gap": False,
                "fresh_post_gap_websocket_and_poll_required": True,
            }
        )

    payload["same_release_continuity_epoch"] = {
        "repair_version": REPAIR_VERSION,
        "release_commit": _release_commit(),
        "current_epoch_id": int(current["epoch_id"]) if current is not None else None,
        "current_generation": int(current["generation"]) if current is not None else None,
        "latest_epoch_state": str(latest["state"]) if latest is not None else None,
        "latest_failed_at": _iso(latest["failed_at"]) if latest is not None else None,
        "failed_epoch_count": len(failed),
        "gap_event_count": gap_count,
        "same_release_successor_supported": True,
        "failed_history_preserved": True,
        "restart_alone_can_clear_failed_gap": False,
        "fresh_post_gap_websocket_required": True,
        "fresh_post_gap_poll_required": True,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "same_release_continuity_successor_epoch": True,
                "failed_strategy_epoch_history_immutable_after_terminalization": True,
                "same_release_successor_requires_fresh_post_gap_ws_and_poll": True,
                "same_release_restart_does_not_clear_gap": True,
            }
        )
    return payload


def install_same_release_continuity_epoch_repair() -> None:
    global _ORIGINAL_START, _ORIGINAL_EPOCH_ROW, _ORIGINAL_LATCH, _ORIGINAL_STATUS, _INSTALLED
    if _INSTALLED:
        return

    continuity.install_strategy_relevant_continuity()

    _ORIGINAL_START = continuity._start_strategy_epoch_if_ready
    _ORIGINAL_EPOCH_ROW = continuity._strategy_epoch_row
    continuity._start_strategy_epoch_if_ready = _start_strategy_epoch_if_ready
    continuity._strategy_epoch_row = _strategy_epoch_row

    current_latch = lease._latch_irrecoverable_generation_once
    if not bool(getattr(current_latch, "_roi_same_release_continuity_successor", False)):
        _ORIGINAL_LATCH = current_latch
        wrapped_latch = wraps(current_latch)(_latch_with_epoch_history)
        setattr(wrapped_latch, "_roi_same_release_continuity_successor", True)
        lease._latch_irrecoverable_generation_once = wrapped_latch

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_same_release_continuity_successor", False)):
        _ORIGINAL_STATUS = current_status
        wrapped_status = wraps(current_status)(_status_with_successor_epochs)
        setattr(wrapped_status, "_roi_same_release_continuity_successor", True)
        DirectSolanaIngestionPlane.status = wrapped_status

    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "install_same_release_continuity_epoch_repair",
]
