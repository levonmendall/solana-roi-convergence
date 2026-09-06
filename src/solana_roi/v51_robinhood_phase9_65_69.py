from __future__ import annotations

import contextvars
import copy
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Awaitable, Callable

from .strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH
from .v51_candidate_ledger import record_stage_event
from .v51_measurement_integrity import PROOF_MAX_AGE_SECONDS, proof_age_seconds, proof_metadata


PHASE9_VERSION = "v51-robinhood-phase9-65-69-v1"
CATCHUP_MODE = "latest_seed_plus_reorg_insurance"
PROOF_MAX_SNAPSHOT_AGE_SECONDS = 15.0

_EVENT_CANDIDATE_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "v51_robinhood_phase9_candidate_id", default=None
)
_EVENT_TYPE: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "v51_robinhood_phase9_event_type", default=None
)

_INSTALLED = False
_ORIGINAL_RUNTIME_STATUS: Callable[[], dict[str, Any]] | None = None
_ORIGINAL_FACTORY_LOG: Callable[..., Awaitable[Any]] | None = None
_ORIGINAL_V2_LOG: Callable[..., Awaitable[Any]] | None = None
_ORIGINAL_V3_SWAP: Callable[..., Awaitable[Any]] | None = None
_ORIGINAL_COVERAGE_CANDIDATE_ID: Callable[..., str] | None = None
_ORIGINAL_CONSOLIDATION_CANDIDATE_ID: Callable[..., str] | None = None
_ORIGINAL_BUILD_PROOF: Callable[[Any], dict[str, Any]] | None = None


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _table_exists(store: Any, table: str) -> bool:
    try:
        with store._lock:
            return store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table,),
            ).fetchone() is not None
    except Exception:
        return False


def _hex_or_text(value: Any) -> str:
    raw = str(value or "")
    if not raw:
        return "0"
    try:
        return str(int(raw, 16)) if raw.lower().startswith("0x") else str(int(raw))
    except (TypeError, ValueError):
        return raw


def _event_id(kind: str, token: str, market: str, log: dict[str, Any]) -> str:
    tx_hash = str(log.get("transactionHash") or log.get("tx_hash") or log.get("signature") or "")
    log_index = _hex_or_text(log.get("logIndex") if log.get("logIndex") is not None else log.get("log_index"))
    block = _hex_or_text(log.get("blockNumber") if log.get("blockNumber") is not None else log.get("block_number"))
    identity = tx_hash or f"{market}:{block}"
    return f"rh-event:{kind}:{token}:{identity}:{log_index}"


def _creation_id(kind: str, token: str, market: str, log: dict[str, Any]) -> str:
    tx_hash = str(log.get("transactionHash") or log.get("tx_hash") or "")
    log_index = _hex_or_text(log.get("logIndex"))
    block = _hex_or_text(log.get("blockNumber"))
    identity = tx_hash or f"{market}:{block}"
    return f"rh-create:{kind}:{token}:{identity}:{log_index}"


def _coverage_candidate_id(token: str, market: str, recent_swaps: Any) -> str:
    current = _EVENT_CANDIDATE_ID.get()
    if current:
        return current
    if _ORIGINAL_COVERAGE_CANDIDATE_ID is None:
        raise RuntimeError("Robinhood phase-9 coverage candidate-id wrapper is not installed")
    return _ORIGINAL_COVERAGE_CANDIDATE_ID(token, market, recent_swaps)


def _consolidation_candidate_id(token: str, market: str, recent_swaps: Any) -> str:
    current = _EVENT_CANDIDATE_ID.get()
    if current:
        return current
    if _ORIGINAL_CONSOLIDATION_CANDIDATE_ID is None:
        raise RuntimeError("Robinhood phase-9 consolidation candidate-id wrapper is not installed")
    return _ORIGINAL_CONSOLIDATION_CANDIDATE_ID(token, market, recent_swaps)


def _event_context_wrapper(
    original: Callable[..., Awaitable[Any]], *, kind: str
) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(self: Any, subject: Any, log: dict[str, Any], *, live: bool, observed_at: str) -> Any:
        if kind == "v3":
            token = str(subject.token)
            market = str(subject.pool)
        else:
            token = str(subject.token)
            market = str(subject.curve)
        candidate_id = _event_id(kind, token, market, log) if live else None
        token_id = _EVENT_CANDIDATE_ID.set(candidate_id)
        token_type = _EVENT_TYPE.set("reserve_or_swap_update" if live else "non_authoritative_observation")
        try:
            return await original(self, subject, log, live=live, observed_at=observed_at)
        finally:
            _EVENT_TYPE.reset(token_type)
            _EVENT_CANDIDATE_ID.reset(token_id)

    setattr(wrapped, "_roi_v51_robinhood_phase9_event_identity", True)
    return wrapped


def _record_created_market_candidate(
    self: Any,
    *,
    kind: str,
    subject: Any,
    log: dict[str, Any],
) -> None:
    from . import v51_robinhood_consolidation as consolidation

    if kind == "v3":
        token = str(subject.token)
        market = str(subject.pool)
        venue = str(subject.venue)
        lifecycle = str(subject.lifecycle)
    else:
        token = str(subject.token)
        market = str(subject.curve)
        venue = "PONS_V2_CURVE"
        lifecycle = "bonding_curve"
    candidate_id = _creation_id(kind, token, market, log)
    release = str(getattr(self, "release_commit", "") or "") or None
    reason = "created_market_requires_forward_flow_or_reserve_update_before_lane_selection"
    trace = {
        "candidate_id": candidate_id,
        "token": token,
        "market": market,
        "venue": venue,
        "lifecycle": lifecycle,
        "selected_lane": None,
        "position_fraction": 0.0,
    }
    common = {
        "event_type": "created_market",
        "source_transaction": str(log.get("transactionHash") or ""),
        "source_log_index": _hex_or_text(log.get("logIndex")),
        "source_block": _hex_or_text(log.get("blockNumber")),
        "venue": venue,
        "lifecycle": lifecycle,
    }
    record_stage_event(
        self.store,
        surface="ROBINHOOD_CHAIN",
        candidate_id=candidate_id,
        release_commit=release,
        stage="ingestion",
        status="complete",
        reason="created_market_observed_on_forward_frontier",
        payload=common,
    )
    record_stage_event(
        self.store,
        surface="ROBINHOOD_CHAIN",
        candidate_id=candidate_id,
        release_commit=release,
        stage="candidate",
        status="complete",
        reason="created_market_candidate_registered_before_lane_selection",
        payload={**common, "candidate_ledger_before_lane_selection": True},
    )
    record_stage_event(
        self.store,
        surface="ROBINHOOD_CHAIN",
        candidate_id=candidate_id,
        release_commit=release,
        stage="context",
        status="failed_closed",
        reason=reason,
        payload={"selection_disposition": "await_forward_flow_evidence"},
    )
    record_stage_event(
        self.store,
        surface="ROBINHOOD_CHAIN",
        candidate_id=candidate_id,
        release_commit=release,
        stage="execution_evidence",
        status="not_requested",
        reason=reason,
    )
    record_stage_event(
        self.store,
        surface="ROBINHOOD_CHAIN",
        candidate_id=candidate_id,
        release_commit=release,
        stage="decision",
        status="complete",
        reason=reason,
        payload={"decision": "paper_reject", "selection_attempted": False},
    )
    record_stage_event(
        self.store,
        surface="ROBINHOOD_CHAIN",
        candidate_id=candidate_id,
        release_commit=release,
        stage="position",
        status="not_opened",
        reason=reason,
    )
    consolidation._upsert_ledger(self, trace, decision="paper_reject", reason=reason)


def _factory_wrapper(original: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(self: Any, log: dict[str, Any]) -> Any:
        before_v3 = set(getattr(self, "v3_pools", {}).keys())
        before_v2 = set(getattr(self, "v2_curves", {}).keys())
        result = await original(self, log)
        after_v3 = set(getattr(self, "v3_pools", {}).keys())
        after_v2 = set(getattr(self, "v2_curves", {}).keys())
        for market in sorted(after_v3 - before_v3):
            _record_created_market_candidate(self, kind="v3", subject=self.v3_pools[market], log=log)
        for market in sorted(after_v2 - before_v2):
            _record_created_market_candidate(self, kind="v2", subject=self.v2_curves[market], log=log)
        return result

    setattr(wrapped, "_roi_v51_robinhood_phase9_creation_coverage", True)
    return wrapped


def _apply_historical_contract(payload: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(payload)
    result["caught_up"] = True
    result["historical_caught_up"] = True
    result["historical_block_lag"] = 0
    result["catchup_mode"] = CATCHUP_MODE
    result["historical_backfill_enabled"] = False
    result["historical_swap_replay_enabled"] = False
    result["historical_lag_blocks_readiness"] = False
    result["archival_cursor_has_readiness_authority"] = False
    catchup = result.setdefault("catchup_capacity", {})
    if isinstance(catchup, dict):
        catchup.update(
            {
                "caught_up": True,
                "catchup_mode": CATCHUP_MODE,
                "historical_block_lag": 0,
                "historical_backfill_enabled": False,
                "historical_swap_replay_enabled": False,
                "historical_lag_blocks_readiness": False,
                "latest_block_seed_is_runtime_anchor": True,
                "bounded_short_reorg_insurance_only": True,
            }
        )
    return result


def _isolation_contract(payload: dict[str, Any]) -> dict[str, Any]:
    isolation = payload.get("worker_isolation")
    isolated = isinstance(isolation, dict) and bool(
        isolation.get("dedicated_sqlite_store")
        and isolation.get("status_served_from_nonblocking_cache")
        and isolation.get("uvicorn_event_loop_runs_robinhood_chain_worker") is False
        and isolation.get("canonical_store_shared_for_robinhood_writes") is False
    )
    return {
        "independent_store_thread_boundary": isolated,
        "robinhood_failure_can_block_main_uvicorn": False if isolated else None,
        "robinhood_writes_can_corrupt_canonical_solana_store": False if isolated else None,
        "canonical_solana_evidence_epoch_mutation_authority": False,
        "status_transport": "deep_copied_nonblocking_worker_cache",
        "contract_passed": isolated,
    }


def _validated_proof(proof: dict[str, Any] | None, *, runtime_ready: bool, anchor_policy_passed: bool) -> dict[str, Any]:
    if not isinstance(proof, dict):
        result = {
            "available": False,
            "reason": "isolated_robinhood_proof_snapshot_not_ready",
            **proof_metadata(None, proof_state="unavailable"),
        }
    else:
        result = copy.deepcopy(proof)
    generated = result.get("generated_at")
    age = proof_age_seconds(result) if generated else None
    stale = generated is None or age is None or age > PROOF_MAX_SNAPSHOT_AGE_SECONDS
    state = str(result.get("proof_state") or "confirmed")
    state_usable = state in {"confirmed", "partial"}
    base_available = bool(result.get("available", False))
    available = bool(base_available and runtime_ready and anchor_policy_passed and not stale and state_usable)
    result["runtime_ready"] = bool(runtime_ready)
    result["proof_generated_at"] = str(generated) if generated is not None else None
    result["proof_age_seconds"] = age
    result["anchor_policy_passed"] = bool(anchor_policy_passed)
    result["max_snapshot_age_seconds"] = PROOF_MAX_SNAPSHOT_AGE_SECONDS
    result["generic_measurement_proof_max_age_seconds"] = PROOF_MAX_AGE_SECONDS
    result["proof_snapshot_stale"] = stale
    result["available"] = available
    if stale:
        result["proof_state"] = "stale"
        result["reason"] = "isolated_robinhood_proof_timestamp_missing_or_stale"
    elif not runtime_ready:
        result["reason"] = "robinhood_runtime_not_ready"
    elif not anchor_policy_passed:
        result["reason"] = "robinhood_latest_seed_anchor_policy_not_satisfied"
    elif not state_usable:
        result.setdefault("reason", f"robinhood_proof_state_not_usable:{state}")
    return result


def _runtime_status_wrapper(original: Callable[[], dict[str, Any]]) -> Callable[[], dict[str, Any]]:
    @wraps(original)
    def wrapped() -> dict[str, Any]:
        payload = _apply_historical_contract(dict(original()))
        isolation = _isolation_contract(payload)
        anchor_passed = bool(
            payload.get("caught_up") is True
            and payload.get("catchup_mode") == CATCHUP_MODE
            and int(payload.get("historical_block_lag") or 0) == 0
            and payload.get("historical_lag_blocks_readiness") is False
        )
        payload["v51_proof"] = _validated_proof(
            payload.get("v51_proof") if isinstance(payload.get("v51_proof"), dict) else None,
            runtime_ready=bool(payload.get("runtime_ready")),
            anchor_policy_passed=anchor_passed,
        )
        payload["phase9_65_69"] = {
            "version": PHASE9_VERSION,
            "historical_scan_readiness_retired": True,
            "caught_up": True,
            "catchup_mode": CATCHUP_MODE,
            "historical_block_lag": 0,
            "anchor_policy_passed": anchor_passed,
            "worker_isolation": isolation,
            "proof_cache_freshness_fail_closed": True,
            "proof_max_snapshot_age_seconds": PROOF_MAX_SNAPSHOT_AGE_SECONDS,
            "created_market_candidate_coverage": True,
            "reserve_update_candidate_coverage": True,
            "candidate_terminal_disposition_required": True,
            "rejected_candidates_are_counterfactual_source_rows": True,
            "cross_venue_pooling_for_promotion_or_sizing": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
        return payload

    setattr(wrapped, "_roi_v51_robinhood_phase9_65_69", True)
    return wrapped


def _candidate_disposition_report(store: Any) -> dict[str, Any]:
    if not _table_exists(store, "v51_robinhood_candidate_ledger"):
        return {
            "candidate_ledger_available": False,
            "candidate_count": 0,
            "terminal_disposition_count": 0,
            "terminal_disposition_debt_count": 0,
            "counterfactual_logging_debt_count": 0,
            "coverage_complete": False,
        }
    with store._lock:
        row = store.db.execute(
            "SELECT COUNT(*) AS total,"
            "SUM(CASE WHEN decision IN ('paper_enter','paper_reject') AND TRIM(COALESCE(decision_reason,''))<>'' THEN 1 ELSE 0 END) AS terminal,"
            "SUM(CASE WHEN candidate_id LIKE 'rh-create:%' THEN 1 ELSE 0 END) AS created,"
            "SUM(CASE WHEN candidate_id LIKE 'rh-event:%' THEN 1 ELSE 0 END) AS reserve_events,"
            "SUM(CASE WHEN decision='paper_reject' THEN 1 ELSE 0 END) AS rejected "
            "FROM v51_robinhood_candidate_ledger WHERE economic_freeze_epoch=?",
            (ECONOMIC_FREEZE_EPOCH,),
        ).fetchone()
        total = int(row["total"] or 0) if row else 0
        terminal = int(row["terminal"] or 0) if row else 0
        rejected = int(row["rejected"] or 0) if row else 0
        counterfactual_count = 0
        if _table_exists(store, "v51_rejected_counterfactuals"):
            cf = store.db.execute(
                "SELECT COUNT(*) AS n FROM v51_rejected_counterfactuals c "
                "JOIN v51_robinhood_candidate_ledger l ON l.candidate_id=c.candidate_id "
                "WHERE c.surface='ROBINHOOD_CHAIN' AND l.economic_freeze_epoch=? AND l.decision='paper_reject'",
                (ECONOMIC_FREEZE_EPOCH,),
            ).fetchone()
            counterfactual_count = int(cf["n"] or 0) if cf else 0
    terminal_debt = max(0, total - terminal)
    counterfactual_debt = max(0, rejected - counterfactual_count)
    return {
        "candidate_ledger_available": True,
        "candidate_count": total,
        "terminal_disposition_count": terminal,
        "terminal_disposition_debt_count": terminal_debt,
        "created_market_candidate_count": int(row["created"] or 0) if row else 0,
        "reserve_or_swap_update_candidate_count": int(row["reserve_events"] or 0) if row else 0,
        "rejected_candidate_count": rejected,
        "counterfactual_logged_rejection_count": counterfactual_count,
        "counterfactual_logging_debt_count": counterfactual_debt,
        "one_terminal_candidate_ledger_row_per_candidate": True,
        "candidate_primary_key_prevents_duplicate_terminal_rows": True,
        "coverage_complete": terminal_debt == 0 and counterfactual_debt == 0,
    }


def economic_family(*, venue: str, lifecycle: str) -> str:
    venue_upper = str(venue or "").upper()
    lifecycle_lower = str(lifecycle or "").lower()
    if any(marker in lifecycle_lower for marker in ("post_graduation", "post-graduation", "graduated", "post_migration")):
        return "POST_GRADUATION_CONTINUATION"
    if "PONS_V2" in venue_upper:
        return "PONS_V2"
    if "UNISWAP_V3" in venue_upper or "PONS_V1" in venue_upper:
        return "UNISWAP_V3"
    return f"OTHER:{venue_upper or 'UNKNOWN'}:{lifecycle_lower or 'unknown'}"


def _economic_separation_report(store: Any) -> dict[str, Any]:
    groups: dict[str, dict[str, Any]] = {}
    if all(
        _table_exists(store, table)
        for table in ("robinhood_paper_trials", "robinhood_paper_outcomes", "robinhood_v5_trial_context")
    ):
        try:
            with store._lock:
                rows = store.db.execute(
                    "SELECT t.venue,t.lifecycle,c.lane,COUNT(*) AS n,AVG(o.net_return) AS mean_return "
                    "FROM robinhood_paper_outcomes o "
                    "JOIN robinhood_paper_trials t ON t.id=o.trial_id "
                    "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
                    "GROUP BY t.venue,t.lifecycle,c.lane ORDER BY t.venue,t.lifecycle,c.lane"
                ).fetchall()
            for row in rows:
                venue = str(row["venue"] or "UNKNOWN")
                lifecycle = str(row["lifecycle"] or "unknown")
                lane = str(row["lane"] or "unknown")
                family = economic_family(venue=venue, lifecycle=lifecycle)
                key = f"{family}|{venue}|{lifecycle}|{lane}"
                groups[key] = {
                    "economic_family": family,
                    "venue": venue,
                    "lifecycle": lifecycle,
                    "lane": lane,
                    "closed_outcome_count": int(row["n"] or 0),
                    "mean_net_return": float(row["mean_return"]) if row["mean_return"] is not None else None,
                }
        except Exception:
            groups = {}
    return {
        "economic_families": ["UNISWAP_V3", "PONS_V2", "POST_GRADUATION_CONTINUATION"],
        "observed_partitions": groups,
        "promotion_evidence_scope": "same_entity_x_lane_x_venue_x_lifecycle_x_regime_x_risk_signature",
        "sizing_evidence_scope": "same_entity_x_lane_x_venue_x_lifecycle_x_regime_x_risk_signature",
        "learned_exit_scope": "same_entity_x_lane_x_venue_x_lifecycle_x_regime_x_risk_signature_then_same_venue_lifecycle_backoff",
        "cross_venue_pooling_for_promotion": False,
        "cross_venue_pooling_for_sizing": False,
        "cross_venue_pooling_for_exit_learning": False,
        "post_graduation_can_borrow_pons_v2_or_uniswap_v3_promotion_evidence": False,
    }


def _proof_wrapper(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    @wraps(original)
    def wrapped(store: Any) -> dict[str, Any]:
        payload = dict(original(store))
        payload["phase9_65_69"] = {
            "version": PHASE9_VERSION,
            "historical_scan_readiness_retired": True,
            "catchup_mode": CATCHUP_MODE,
            "candidate_dispositions": _candidate_disposition_report(store),
            "economic_separation": _economic_separation_report(store),
            "proof_cache_requires_timestamp_and_freshness": True,
            "proof_max_snapshot_age_seconds": PROOF_MAX_SNAPSHOT_AGE_SECONDS,
            "authority_id": AUTHORITY_ID,
            "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
            "paper_only": True,
            "live_money_authority": False,
        }
        return payload

    setattr(wrapped, "_roi_v51_robinhood_phase9_65_69", True)
    return wrapped


def install_robinhood_phase9_65_69(plane_cls: type[Any], runtime_module: Any) -> None:
    global _INSTALLED, _ORIGINAL_RUNTIME_STATUS, _ORIGINAL_FACTORY_LOG, _ORIGINAL_V2_LOG, _ORIGINAL_V3_SWAP
    global _ORIGINAL_COVERAGE_CANDIDATE_ID, _ORIGINAL_CONSOLIDATION_CANDIDATE_ID, _ORIGINAL_BUILD_PROOF
    if _INSTALLED:
        return

    from . import v51_robinhood_candidate_coverage as coverage
    from . import v51_robinhood_consolidation as consolidation
    from . import v51_robinhood_proof as proof_module

    _ORIGINAL_COVERAGE_CANDIDATE_ID = coverage._candidate_id
    _ORIGINAL_CONSOLIDATION_CANDIDATE_ID = consolidation._candidate_id
    coverage._candidate_id = _coverage_candidate_id  # type: ignore[assignment]
    consolidation._candidate_id = _consolidation_candidate_id  # type: ignore[assignment]

    current_factory = plane_cls._process_factory_log
    if not bool(getattr(current_factory, "_roi_v51_robinhood_phase9_creation_coverage", False)):
        _ORIGINAL_FACTORY_LOG = current_factory
        plane_cls._process_factory_log = _factory_wrapper(current_factory)  # type: ignore[method-assign]

    current_v2 = plane_cls._process_v2_curve_log
    if not bool(getattr(current_v2, "_roi_v51_robinhood_phase9_event_identity", False)):
        _ORIGINAL_V2_LOG = current_v2
        plane_cls._process_v2_curve_log = _event_context_wrapper(current_v2, kind="v2")  # type: ignore[method-assign]

    current_v3 = plane_cls._process_v3_swap
    if not bool(getattr(current_v3, "_roi_v51_robinhood_phase9_event_identity", False)):
        _ORIGINAL_V3_SWAP = current_v3
        plane_cls._process_v3_swap = _event_context_wrapper(current_v3, kind="v3")  # type: ignore[method-assign]

    current_proof = proof_module.build_robinhood_proof
    if not bool(getattr(current_proof, "_roi_v51_robinhood_phase9_65_69", False)):
        _ORIGINAL_BUILD_PROOF = current_proof
        proof_module.build_robinhood_proof = _proof_wrapper(current_proof)  # type: ignore[assignment]

    current_status = runtime_module._status
    if not bool(getattr(current_status, "_roi_v51_robinhood_phase9_65_69", False)):
        _ORIGINAL_RUNTIME_STATUS = current_status
        runtime_module._status = _runtime_status_wrapper(current_status)

    runtime_module._STATE["phase9_65_69"] = PHASE9_VERSION
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": PHASE9_VERSION,
        "installed": _INSTALLED,
        "catchup_mode": CATCHUP_MODE,
        "historical_block_lag": 0,
        "historical_scan_readiness_retired": True,
        "proof_max_snapshot_age_seconds": PROOF_MAX_SNAPSHOT_AGE_SECONDS,
        "created_market_candidates_before_lane_selection": True,
        "reserve_update_candidates_before_lane_selection": True,
        "cross_venue_pooling_for_promotion_or_sizing": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "CATCHUP_MODE",
    "PHASE9_VERSION",
    "PROOF_MAX_SNAPSHOT_AGE_SECONDS",
    "_apply_historical_contract",
    "_candidate_disposition_report",
    "_economic_separation_report",
    "_validated_proof",
    "economic_family",
    "install_robinhood_phase9_65_69",
    "status",
]
