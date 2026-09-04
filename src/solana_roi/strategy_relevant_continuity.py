from __future__ import annotations

import os
from contextvars import ContextVar
from functools import wraps
from typing import Any, Callable

from . import continuity_immediate_recovery_repair as immediate
from . import direct_solana as direct_module
from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from . import target_quorum
from . import target_stream_fanout as fanout
from .direct_solana import DirectSolanaIngestionPlane, DirectSolanaJournal, WatchTarget


REPAIR_VERSION = "strategy-relevant-continuity-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_SCOPE: ContextVar[str] = ContextVar("roi_continuity_scope", default="strategy")

_ORIGINAL_MARK_OUTAGE: Callable[..., Any] | None = None
_ORIGINAL_CLOSE_OUTAGE: Callable[..., Any] | None = None
_ORIGINAL_SET_TARGET_STATE: Callable[..., Any] | None = None
_ORIGINAL_LATCH_GENERATION: Callable[..., Any] | None = None
_ORIGINAL_KICK_RECOVERY: Callable[..., Any] | None = None
_ORIGINAL_RECORD_POLL_ROWS: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None


def _release_commit() -> str:
    for key in ("RENDER_GIT_COMMIT", "GITHUB_SHA", "SOLANA_ROI_RELEASE_COMMIT"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return "unbound-local-release"


def _target_key(target: WatchTarget) -> str:
    return fanout._target_key(target)


def _target_groups(self: Any) -> tuple[set[str], set[str]]:
    strategy: set[str] = set()
    discovery: set[str] = set()
    for target in self.watch_targets:
        key = _target_key(target)
        if target.kind == "scout":
            strategy.add(key)
        else:
            discovery.add(key)
    return strategy, discovery


def _coverage_snapshot(self: Any) -> dict[str, Any]:
    _lock, provider_targets, _ready_events, _states = fanout._state_maps(self)
    strategy_keys, discovery_keys = _target_groups(self)
    websocket_sets = {
        provider: set(rows)
        for provider, rows in provider_targets.items()
        if provider != live_poll.POLL_PROVIDER_NAME
    }
    websocket_union: set[str] = set()
    for rows in websocket_sets.values():
        websocket_union.update(rows)
    poll_rows = set(provider_targets.get(live_poll.POLL_PROVIDER_NAME, set()))

    strategy_ws = websocket_union & strategy_keys
    discovery_ws = websocket_union & discovery_keys
    strategy_poll = poll_rows & strategy_keys
    discovery_poll = poll_rows & discovery_keys
    contributing_strategy_providers = sum(
        1 for rows in websocket_sets.values() if bool(rows & strategy_keys)
    )

    return {
        "strategy_target_count": len(strategy_keys),
        "strategy_websocket_covered_target_count": len(strategy_ws),
        "strategy_poll_baselined_target_count": len(strategy_poll),
        "strategy_websocket_coverage_ok": bool(strategy_keys) and strategy_ws == strategy_keys,
        "strategy_poll_baseline_ok": bool(strategy_keys) and strategy_poll == strategy_keys,
        "strategy_startup_ready": bool(strategy_keys)
        and strategy_ws == strategy_keys
        and strategy_poll == strategy_keys,
        "strategy_connected_provider_count": contributing_strategy_providers,
        "discovery_target_count": len(discovery_keys),
        "discovery_websocket_covered_target_count": len(discovery_ws),
        "discovery_poll_baselined_target_count": len(discovery_poll),
        "discovery_websocket_coverage_ok": bool(discovery_keys) and discovery_ws == discovery_keys,
        "discovery_poll_baseline_ok": bool(discovery_keys) and discovery_poll == discovery_keys,
    }


def _ensure_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS direct_solana_strategy_continuity_epoch ("
            "release_commit TEXT PRIMARY KEY, started_at TEXT NOT NULL, "
            "archived_outage_started_at TEXT, archived_unresolved_gap INTEGER NOT NULL, "
            "archived_backfill_error TEXT, paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS direct_solana_discovery_gap_event ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, "
            "target_key TEXT NOT NULL, generation INTEGER NOT NULL, started_at TEXT, "
            "observed_at TEXT NOT NULL, reason TEXT NOT NULL, "
            "UNIQUE(release_commit,target_key,generation,reason))"
        )


def _record_discovery_gap(
    store: Any,
    *,
    target: WatchTarget | None,
    generation: int = -1,
    started_at: str | None = None,
    reason: str,
) -> None:
    try:
        _ensure_schema(store)
        key = _target_key(target) if target is not None else "program:unknown"
        with store._lock, store.db:
            store.db.execute(
                "INSERT OR IGNORE INTO direct_solana_discovery_gap_event("
                "release_commit,target_key,generation,started_at,observed_at,reason) "
                "VALUES (?,?,?,?,?,?)",
                (
                    _release_commit(),
                    key,
                    int(generation),
                    started_at,
                    direct_module.utcnow().isoformat(),
                    reason,
                ),
            )
    except Exception:
        # Discovery telemetry must never be able to terminate the strategy hot path.
        pass


def _strategy_epoch_row(self: Any) -> Any:
    _ensure_schema(self.store)
    with self.store._lock:
        return self.store.db.execute(
            "SELECT started_at,archived_outage_started_at,archived_unresolved_gap,"
            "archived_backfill_error FROM direct_solana_strategy_continuity_epoch "
            "WHERE release_commit=?",
            (_release_commit(),),
        ).fetchone()


def _start_strategy_epoch_if_ready(self: Any) -> bool:
    snapshot = _coverage_snapshot(self)
    if not snapshot["strategy_startup_ready"]:
        return False
    commit = _release_commit()
    if commit == "unbound-local-release":
        setattr(self, "_roi_strategy_continuity_observed", True)
        return True

    _ensure_schema(self.store)
    now = direct_module.utcnow().isoformat()
    with self.store._lock, self.store.db:
        existing = self.store.db.execute(
            "SELECT release_commit FROM direct_solana_strategy_continuity_epoch WHERE release_commit=?",
            (commit,),
        ).fetchone()
        if existing is None:
            state = self.store.db.execute(
                "SELECT outage_started_at,unresolved_gap,last_backfill_error "
                "FROM direct_solana_global_state WHERE id=1"
            ).fetchone()
            archived_outage = str(state["outage_started_at"] or "") if state is not None else ""
            archived_unresolved = bool(state["unresolved_gap"]) if state is not None else False
            archived_error = str(state["last_backfill_error"] or "") if state is not None else ""
            self.store.db.execute(
                "INSERT INTO direct_solana_strategy_continuity_epoch("
                "release_commit,started_at,archived_outage_started_at,archived_unresolved_gap,"
                "archived_backfill_error,paper_only,live_money_authority) VALUES (?,?,?,?,?,1,0)",
                (
                    commit,
                    now,
                    archived_outage or None,
                    1 if archived_unresolved else 0,
                    archived_error or None,
                ),
            )
            # This is a new release and a new prospective strategy-continuity epoch.
            # Prior full-firehose gaps remain archived evidence; they are not relabeled
            # as recovered and cannot authorize historical promotion.
            self.store.db.execute(
                "UPDATE direct_solana_global_state SET outage_started_at=NULL,unresolved_gap=0,"
                "last_backfill_complete_at=NULL,last_backfill_error=NULL WHERE id=1"
            )
    setattr(self, "_roi_strategy_continuity_observed", True)
    return True


def _mark_outage_scoped(self: Any, started_at: Any) -> None:
    if _SCOPE.get() == "discovery":
        _record_discovery_gap(
            self.store,
            target=None,
            started_at=getattr(started_at, "isoformat", lambda: None)(),
            reason="program_target_transport_gap",
        )
        return
    if _ORIGINAL_MARK_OUTAGE is None:
        raise RuntimeError("strategy continuity repair missing original mark_outage")
    _ORIGINAL_MARK_OUTAGE(self, started_at)


def _close_outage_scoped(
    self: Any,
    *,
    complete: bool,
    error: str | None = None,
) -> None:
    if _SCOPE.get() == "discovery":
        _record_discovery_gap(
            self.store,
            target=None,
            reason=error or ("program_target_transport_recovered" if complete else "program_target_transport_gap"),
        )
        return
    if _ORIGINAL_CLOSE_OUTAGE is None:
        raise RuntimeError("strategy continuity repair missing original close_outage")
    _ORIGINAL_CLOSE_OUTAGE(self, complete=complete, error=error)


async def _set_target_state_scoped(
    self: Any,
    endpoint: Any,
    target: WatchTarget,
    *,
    connected: bool,
    error_type: str | None = None,
    error_code: int | None = None,
    error_message: str | None = None,
) -> None:
    if _ORIGINAL_SET_TARGET_STATE is None:
        raise RuntimeError("strategy continuity repair missing original target setter")

    before = _coverage_snapshot(self)
    scope = "strategy" if target.kind == "scout" else "discovery"
    token = _SCOPE.set(scope)
    try:
        await _ORIGINAL_SET_TARGET_STATE(
            self,
            endpoint,
            target,
            connected=connected,
            error_type=error_type,
            error_code=error_code,
            error_message=error_message,
        )
    finally:
        _SCOPE.reset(token)

    after = _coverage_snapshot(self)
    if target.kind != "scout":
        if before["discovery_websocket_coverage_ok"] and not after["discovery_websocket_coverage_ok"]:
            _record_discovery_gap(
                self.store,
                target=target,
                reason=error_type or "program_websocket_coverage_degraded",
            )
        return

    # Strategy continuity has its own startup boundary. Once all scout targets have
    # a live WS copy plus a confirmed poll baseline, program-stream health can no
    # longer prevent the strategy epoch from becoming observable.
    _start_strategy_epoch_if_ready(self)
    observed = bool(getattr(self, "_roi_strategy_continuity_observed", False))
    if (
        observed
        and before["strategy_websocket_coverage_ok"]
        and not after["strategy_websocket_coverage_ok"]
    ):
        self.journal.mark_outage(direct_module.utcnow())


def _latch_generation_scoped(
    self: Any,
    target: WatchTarget,
    generation: int,
    started_at_iso: str | None,
) -> bool:
    if target.kind != "scout":
        runtime = lease._runtime(self).setdefault(live_poll._poll_target_key(target), {})
        if runtime.get("discovery_degraded_ws_generation") == int(generation):
            return False
        runtime["discovery_degraded_ws_generation"] = int(generation)
        _record_discovery_gap(
            self.store,
            target=target,
            generation=int(generation),
            started_at=started_at_iso,
            reason="bounded_program_poll_delta_not_recovered",
        )
        return True
    if _ORIGINAL_LATCH_GENERATION is None:
        raise RuntimeError("strategy continuity repair missing original irrecoverable latch")
    return bool(
        _ORIGINAL_LATCH_GENERATION(
            self,
            target,
            generation,
            started_at_iso,
        )
    )


def _kick_recovery_scoped(self: Any, target: WatchTarget, generation: int) -> None:
    if target.kind != "scout":
        immediate._increment(self, "program_discovery_kick_skipped")
        _record_discovery_gap(
            self.store,
            target=target,
            generation=int(generation),
            reason="program_gap_not_sent_to_critical_recovery_lane",
        )
        return
    if _ORIGINAL_KICK_RECOVERY is None:
        raise RuntimeError("strategy continuity repair missing original recovery kick")
    _ORIGINAL_KICK_RECOVERY(self, target, generation)


async def _record_poll_rows_scoped(
    self: Any,
    target: WatchTarget,
    rows: list[dict[str, Any]],
) -> int:
    if target.kind == "scout":
        if _ORIGINAL_RECORD_POLL_ROWS is None:
            raise RuntimeError("strategy continuity repair missing original poll recorder")
        return int(await _ORIGINAL_RECORD_POLL_ROWS(self, target, rows))

    inserted_count = 0
    source_key = target.source_hint or f"PROGRAM:{target.address}"
    for row in rows:
        signature = str(row.get("signature") or "")
        if not signature:
            continue
        try:
            slot = int(row.get("slot") or 0)
        except (TypeError, ValueError):
            continue
        if slot <= 0:
            continue
        received_at = direct_module.utcnow()
        inserted = self.journal.record_receipt(
            signature=signature,
            source_key=source_key,
            slot=slot,
            received_at=received_at,
            launch_like=False,
        )
        if not inserted or row.get("err") is not None:
            continue
        inserted_count += 1

    setattr(
        self,
        "_roi_program_poll_rows_raw_only_total",
        int(getattr(self, "_roi_program_poll_rows_raw_only_total", 0) or 0) + inserted_count,
    )
    return inserted_count


def _discovery_gap_summary(self: Any) -> dict[str, Any]:
    _ensure_schema(self.store)
    commit = _release_commit()
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT target_key,MAX(observed_at) AS last_observed_at,COUNT(*) AS n "
            "FROM direct_solana_discovery_gap_event WHERE release_commit=? "
            "GROUP BY target_key ORDER BY target_key",
            (commit,),
        ).fetchall()
    return {
        "degraded_target_count": len(rows),
        "gap_event_count": sum(int(row["n"]) for row in rows),
        "targets": [
            {
                "target": str(row["target_key"]),
                "gap_events": int(row["n"]),
                "last_observed_at": str(row["last_observed_at"] or "") or None,
            }
            for row in rows
        ],
    }


def _status_with_strategy_continuity(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("strategy continuity repair missing original status")
    payload = _ORIGINAL_STATUS(self)
    snapshot = _coverage_snapshot(self)
    _start_strategy_epoch_if_ready(self)
    epoch = _strategy_epoch_row(self)

    # Preserve the pre-repair all-target view as discovery/raw-market telemetry.
    raw_connected = int(payload.get("connected_provider_count") or 0)
    raw_barrier = payload.get("continuity_startup_barrier")
    if isinstance(raw_barrier, dict):
        payload["raw_discovery_startup_barrier"] = dict(raw_barrier)

    with self.store._lock:
        global_row = self.store.db.execute(
            "SELECT outage_started_at,unresolved_gap,last_backfill_complete_at,last_backfill_error "
            "FROM direct_solana_global_state WHERE id=1"
        ).fetchone()
    unresolved = bool(global_row["unresolved_gap"]) if global_row is not None else True
    outage_started = str(global_row["outage_started_at"] or "") if global_row is not None else ""
    backfill_at = str(global_row["last_backfill_complete_at"] or "") if global_row is not None else ""
    backfill_error = str(global_row["last_backfill_error"] or "") if global_row is not None else ""

    strategy_started = epoch is not None or (
        _release_commit() == "unbound-local-release"
        and bool(getattr(self, "_roi_strategy_continuity_observed", False))
    )
    strategy_ok = bool(
        strategy_started
        and snapshot["strategy_websocket_coverage_ok"]
        and not unresolved
    )

    payload["raw_full_scope_connected_provider_count"] = raw_connected
    payload["strategy_connected_provider_count"] = snapshot["strategy_connected_provider_count"]
    # Compatibility field consumed by the strict E2E status wrapper. Its authority
    # is now strategy transport, while raw provider completeness remains above.
    payload["connected_provider_count"] = snapshot["strategy_connected_provider_count"]
    payload["continuity_ok"] = strategy_ok
    payload["unresolved_gap"] = unresolved
    payload["outage_started_at"] = outage_started or None
    payload["last_backfill_complete_at"] = backfill_at or None
    payload["last_backfill_error"] = backfill_error or None

    payload["strategy_relevant_continuity"] = {
        "repair_version": REPAIR_VERSION,
        "release_commit": _release_commit(),
        "epoch_started": strategy_started,
        "epoch_started_at": str(epoch["started_at"]) if epoch is not None else None,
        "target_scope": "frozen-scout-and-strategy-trigger-transport",
        "target_count": snapshot["strategy_target_count"],
        "websocket_covered_target_count": snapshot["strategy_websocket_covered_target_count"],
        "websocket_coverage_ok": snapshot["strategy_websocket_coverage_ok"],
        "poll_baselined_target_count": snapshot["strategy_poll_baselined_target_count"],
        "poll_baseline_ok": snapshot["strategy_poll_baseline_ok"],
        "startup_ready": snapshot["strategy_startup_ready"],
        "continuity_ok": strategy_ok,
        "unresolved_gap": unresolved,
        "lossless_authority": True,
        "program_firehose_gap_can_invalidate": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    discovery = _discovery_gap_summary(self)
    payload["discovery_continuity"] = {
        "scope": "frozen-program-launch-and-lifecycle-discovery",
        "target_count": snapshot["discovery_target_count"],
        "websocket_covered_target_count": snapshot["discovery_websocket_covered_target_count"],
        "websocket_coverage_ok": snapshot["discovery_websocket_coverage_ok"],
        "poll_baselined_target_count": snapshot["discovery_poll_baselined_target_count"],
        "poll_baseline_ok": snapshot["discovery_poll_baseline_ok"],
        "best_effort_raw_observation_preserved": True,
        "program_poll_fallback_rows_raw_only_total": int(
            getattr(self, "_roi_program_poll_rows_raw_only_total", 0) or 0
        ),
        "critical_recovery_skipped_for_program_gaps": int(
            getattr(self, "_roi_immediate_gap_recovery_program_discovery_kick_skipped", 0) or 0
        ),
        "blocks_strategy_execution_continuity": False,
        "blocks_program_wide_coverage_certification_when_evidence_is_insufficient": True,
        **discovery,
    }
    payload["continuity_startup_barrier"] = {
        "required": True,
        "armed": strategy_started,
        "armed_at": str(epoch["started_at"]) if epoch is not None else None,
        "requirements": {
            "all_strategy_scouts_real_websocket_covered": snapshot["strategy_websocket_coverage_ok"],
            "all_strategy_scouts_live_poll_baselined": snapshot["strategy_poll_baseline_ok"],
        },
        "startup_transitions_can_create_prospective_gap": False,
        "post_arm_scout_gap_fails_closed": True,
        "program_target_health_is_discovery_not_execution_authority": True,
    }

    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "continuity_model": "strategy-relevant-scout-provider-union",
                "raw_discovery_model": "all-frozen-program-targets-best-effort-plus-coverage-certification",
                "program_target_gap_blocks_strategy_continuity": False,
                "program_target_gap_blocks_program_coverage_certification_when_evidence_missing": True,
                "program_gap_critical_recovery_disabled": True,
                "program_gap_poll_rows_hydrated": False,
                "scout_gap_fail_closed_semantics_unchanged": True,
                "scout_immediate_recovery_lease_unchanged": True,
                "scout_recovery_bound_unchanged": True,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )

    throughput = payload.setdefault("throughput_policy", {})
    if isinstance(throughput, dict):
        throughput.update(
            {
                "full_raw_market_scope_preserved": True,
                "raw_full_scope_observation_preserved": True,
                "strategy_scope_reduced": False,
                "program_gap_fallback_hydration": False,
                "program_gap_critical_recovery": False,
                "strategy_hot_path_lossless": True,
                "discovery_firehose_can_block_candidate_lane": False,
                "program_wide_certification_gate_unchanged": True,
            }
        )
    return payload


def install_strategy_relevant_continuity() -> None:
    global _ORIGINAL_MARK_OUTAGE, _ORIGINAL_CLOSE_OUTAGE, _ORIGINAL_SET_TARGET_STATE
    global _ORIGINAL_LATCH_GENERATION, _ORIGINAL_KICK_RECOVERY
    global _ORIGINAL_RECORD_POLL_ROWS, _ORIGINAL_STATUS

    current_mark = DirectSolanaJournal.mark_outage
    if not bool(getattr(current_mark, "_roi_strategy_relevant_continuity", False)):
        _ORIGINAL_MARK_OUTAGE = current_mark
        wrapped_mark = wraps(current_mark)(_mark_outage_scoped)
        setattr(wrapped_mark, "_roi_strategy_relevant_continuity", True)
        DirectSolanaJournal.mark_outage = wrapped_mark  # type: ignore[method-assign]

    current_close = DirectSolanaJournal.close_outage
    if not bool(getattr(current_close, "_roi_strategy_relevant_continuity", False)):
        _ORIGINAL_CLOSE_OUTAGE = current_close
        wrapped_close = wraps(current_close)(_close_outage_scoped)
        setattr(wrapped_close, "_roi_strategy_relevant_continuity", True)
        DirectSolanaJournal.close_outage = wrapped_close  # type: ignore[method-assign]

    current_setter = target_quorum._quorum_set_target_state
    if not bool(getattr(current_setter, "_roi_strategy_relevant_continuity", False)):
        _ORIGINAL_SET_TARGET_STATE = current_setter
        wrapped_setter = wraps(current_setter)(_set_target_state_scoped)
        setattr(wrapped_setter, "_roi_strategy_relevant_continuity", True)
        target_quorum._quorum_set_target_state = wrapped_setter  # type: ignore[assignment]
        fanout._set_target_state = wrapped_setter  # type: ignore[assignment]
        lease._tracked_quorum_set_target_state = wrapped_setter  # type: ignore[assignment]

    current_latch = lease._latch_irrecoverable_generation_once
    if not bool(getattr(current_latch, "_roi_strategy_relevant_continuity", False)):
        _ORIGINAL_LATCH_GENERATION = current_latch
        wrapped_latch = wraps(current_latch)(_latch_generation_scoped)
        setattr(wrapped_latch, "_roi_strategy_relevant_continuity", True)
        lease._latch_irrecoverable_generation_once = wrapped_latch  # type: ignore[assignment]

    current_kick = immediate._kick_immediate_recovery
    if not bool(getattr(current_kick, "_roi_strategy_relevant_continuity", False)):
        _ORIGINAL_KICK_RECOVERY = current_kick
        wrapped_kick = wraps(current_kick)(_kick_recovery_scoped)
        setattr(wrapped_kick, "_roi_strategy_relevant_continuity", True)
        immediate._kick_immediate_recovery = wrapped_kick  # type: ignore[assignment]

    current_poll_recorder = live_poll._record_poll_rows
    if not bool(getattr(current_poll_recorder, "_roi_strategy_relevant_continuity", False)):
        _ORIGINAL_RECORD_POLL_ROWS = current_poll_recorder
        wrapped_poll = wraps(current_poll_recorder)(_record_poll_rows_scoped)
        setattr(wrapped_poll, "_roi_strategy_relevant_continuity", True)
        live_poll._record_poll_rows = wrapped_poll  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_strategy_relevant_continuity", False)):
        _ORIGINAL_STATUS = current_status
        wrapped_status = wraps(current_status)(_status_with_strategy_continuity)
        setattr(wrapped_status, "_roi_strategy_relevant_continuity", True)
        DirectSolanaIngestionPlane.status = wrapped_status  # type: ignore[method-assign]


__all__ = [
    "REPAIR_VERSION",
    "install_strategy_relevant_continuity",
]
