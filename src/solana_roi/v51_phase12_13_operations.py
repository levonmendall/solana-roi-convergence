from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

from .strategy_v51_authority import ECONOMIC_FREEZE_EPOCH

OPERATIONS_VERSION = "v51-phase12-13-operations-83-94-v2"
PROCESS_STARTED_AT = datetime.now(timezone.utc).isoformat()
BACKPRESSURE_AGE_LIMIT_SECONDS = 120.0
OUTPACING_AGE_LIMIT_SECONDS = 60.0


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _number(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _release() -> str | None:
    for key in ("RENDER_GIT_COMMIT", "SOLANA_ROI_RELEASE_COMMIT", "GIT_COMMIT"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return None


def _nested_first(payload: dict[str, Any], names: tuple[str, ...]) -> Any:
    stack: list[Any] = [payload]
    while stack:
        current = stack.pop()
        if not isinstance(current, dict):
            continue
        for name in names:
            if name in current and current[name] is not None:
                return current[name]
        stack.extend(value for value in current.values() if isinstance(value, dict))
    return None


def _work_counter(payload: dict[str, Any]) -> int:
    preferred = (
        "processed_count", "event_count", "raw_event_count", "normalized_swap_count",
        "candidate_count", "evaluation_count", "cycle_count", "request_count",
    )
    values: list[int] = []
    for key in preferred:
        raw = _nested_first(payload, (key,))
        if raw is not None:
            values.append(_int(raw))
    return max(values, default=0)


def normalize_subsystem(name: str, payload: dict[str, Any] | None) -> dict[str, Any]:
    source = _dict(payload)
    queue_depth = _number(_nested_first(source, ("queue_depth", "pending_count", "pending", "backlog")))
    oldest = _number(_nested_first(source, (
        "oldest_pending_age_seconds", "oldest_unprocessed_raw_event_age_seconds",
        "oldest_coverage_debt_age_seconds", "oldest_pending_settlement_age_seconds",
    )))
    producer = _number(_nested_first(source, ("events_per_second", "producer_rate", "ingress_per_second")))
    consumer = _number(_nested_first(source, ("processing_per_second", "consumer_rate", "processed_per_second")))
    lag = _number(_nested_first(source, (
        "lag", "lag_seconds", "chain_block_lag", "historical_block_lag", "queue_delay_seconds",
    )))
    dropped = _int(_nested_first(source, ("dropped_count", "drop_count", "dropped_events")))
    retries = _int(_nested_first(source, ("retry_count", "retries", "provider_retry_count")))
    cycle = _number(_nested_first(source, (
        "cycle_duration_seconds", "last_cycle_duration_seconds", "last_poll_duration_seconds",
    )))
    worker_started_at = _nested_first(source, ("worker_started_at", "started_at", "process_started_at"))
    cursor_restore = _nested_first(source, ("cursor_restore_status", "restore_status", "checkpoint_restore_status"))
    persistent_outpace = bool(
        producer is not None and consumer is not None and producer > consumer
        and oldest is not None and oldest > OUTPACING_AGE_LIMIT_SECONDS
    )
    aged_backlog = bool(
        queue_depth is not None and queue_depth > 0
        and oldest is not None and oldest > BACKPRESSURE_AGE_LIMIT_SECONDS
    )
    healthy = not persistent_outpace and not aged_backlog and dropped == 0
    return {
        "subsystem": name,
        "queue_depth": int(queue_depth) if queue_depth is not None else None,
        "oldest_pending_age_seconds": oldest,
        "events_per_second": producer,
        "processing_per_second": consumer,
        "lag": lag,
        "dropped_count": dropped,
        "retry_count": retries,
        "cycle_duration_seconds": cycle,
        "work_counter": _work_counter(source),
        "worker_started_at": worker_started_at,
        "cursor_restore_status": cursor_restore,
        "producer_persistently_outpacing_consumer": persistent_outpace,
        "aged_backlog": aged_backlog,
        "backpressure_healthy": healthy,
    }


def _safe_status(obj: Any) -> tuple[dict[str, Any], float]:
    start = time.perf_counter()
    try:
        value = obj.status() if obj is not None else {}
        payload = dict(value) if isinstance(value, dict) else {}
    except Exception as exc:
        payload = {"runtime_ready": False, "status_error_type": type(exc).__name__}
    return payload, max(0.0, time.perf_counter() - start)


def _ensure_continuity_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_runtime_continuity ("
            "subsystem TEXT PRIMARY KEY,continuity_epoch TEXT NOT NULL,first_observed_at TEXT NOT NULL,"
            "last_observed_at TEXT NOT NULL,last_process_started_at TEXT,restart_count INTEGER NOT NULL DEFAULT 0,"
            "last_restart_reason TEXT,cursor_restore_status TEXT NOT NULL,current_release TEXT,previous_release TEXT,"
            "paper_only INTEGER NOT NULL,live_money_authority INTEGER NOT NULL)"
        )


def _continuity_rows(store: Any, subsystems: dict[str, dict[str, Any]]) -> dict[str, Any]:
    _ensure_continuity_schema(store)
    now = datetime.now(timezone.utc).isoformat()
    reason = os.getenv("SOLANA_ROI_RESTART_REASON", "process_start_or_platform_restart").strip() or "process_start_or_platform_restart"
    current_release = _release()
    rows: dict[str, Any] = {}
    for name, status in subsystems.items():
        restore = str(status.get("cursor_restore_status") or "restored_or_forward_fresh_boundary")
        with store._lock, store.db:
            existing = store.db.execute("SELECT * FROM v51_runtime_continuity WHERE subsystem=?", (name,)).fetchone()
            if existing is None:
                previous_release = None
                store.db.execute(
                    "INSERT INTO v51_runtime_continuity(subsystem,continuity_epoch,first_observed_at,last_observed_at,"
                    "last_process_started_at,restart_count,last_restart_reason,cursor_restore_status,current_release,previous_release,"
                    "paper_only,live_money_authority) VALUES (?,?,?,?,?,0,?,?,?,?,1,0)",
                    (name, ECONOMIC_FREEZE_EPOCH, now, now, PROCESS_STARTED_AT, reason, restore, current_release, previous_release),
                )
                restart_count = 0
                first = now
            else:
                changed = str(existing["last_process_started_at"] or "") != PROCESS_STARTED_AT
                restart_count = int(existing["restart_count"] or 0) + int(changed)
                first = str(existing["first_observed_at"])
                prior_current = existing["current_release"]
                previous_release = prior_current if current_release and prior_current and str(prior_current) != current_release else existing["previous_release"]
                store.db.execute(
                    "UPDATE v51_runtime_continuity SET continuity_epoch=?,last_observed_at=?,last_process_started_at=?,"
                    "restart_count=?,last_restart_reason=?,cursor_restore_status=?,current_release=?,previous_release=?,"
                    "paper_only=1,live_money_authority=0 WHERE subsystem=?",
                    (ECONOMIC_FREEZE_EPOCH, now, PROCESS_STARTED_AT, restart_count, reason, restore,
                     current_release, previous_release, name),
                )
        rows[name] = {
            "continuity_epoch": ECONOMIC_FREEZE_EPOCH,
            "first_observed_at": first,
            "process_started_at": PROCESS_STARTED_AT,
            "worker_started_at": status.get("worker_started_at") or PROCESS_STARTED_AT,
            "restart_count": restart_count,
            "last_restart_reason": reason,
            "current_release": current_release,
            "previous_release": previous_release,
            "cursor_restore_status": restore,
            "restart_changes_economic_epoch": False,
        }
    return rows


def build_operations_proof(
    runtime: Any,
    *,
    unified_status: dict[str, Any] | None = None,
    robinhood_status: dict[str, Any] | None = None,
    legacy_webhook_worker_enabled: bool = False,
    proof_cycle_duration_seconds: float | None = None,
    http_request_count: int = 0,
    http_total_duration_seconds: float = 0.0,
) -> dict[str, Any]:
    unified = _dict(unified_status)
    direct_raw, direct_duration = _safe_status(getattr(runtime, "direct_ingestion", None))
    wallet_raw, wallet_duration = _safe_status(getattr(runtime, "wallet_discovery", None))
    collectors_raw, collectors_duration = _safe_status(getattr(runtime, "collectors", None))
    fomo_raw = _dict(unified.get("fomo"))
    robinhood_raw = _dict(robinhood_status) or _dict(unified.get("robinhood"))
    raw = {
        "solana_ingestion": direct_raw,
        "wallet_discovery": wallet_raw,
        "risk_enrichment": collectors_raw,
        "fomo": fomo_raw,
        "robinhood": robinhood_raw,
        "proof_publication": {"cycle_duration_seconds": proof_cycle_duration_seconds, "cycle_count": 1},
        "http": {"cycle_duration_seconds": http_total_duration_seconds, "request_count": http_request_count},
    }
    subsystems = {name: normalize_subsystem(name, value) for name, value in raw.items()}
    subsystems["solana_ingestion"]["status_refresh_duration_seconds"] = direct_duration
    subsystems["wallet_discovery"]["status_refresh_duration_seconds"] = wallet_duration
    subsystems["risk_enrichment"]["status_refresh_duration_seconds"] = collectors_duration
    for name in ("fomo", "robinhood", "proof_publication", "http"):
        subsystems[name]["status_refresh_duration_seconds"] = None
    continuity = _continuity_rows(runtime.store, subsystems)
    worker_names = ("solana_ingestion", "wallet_discovery", "risk_enrichment", "fomo", "robinhood")
    unhealthy = sorted(name for name in worker_names if not bool(subsystems[name]["backpressure_healthy"]))
    return {
        "operations_version": OPERATIONS_VERSION,
        "backpressure": {
            "healthy": not unhealthy,
            "unhealthy_subsystems": unhealthy,
            "producer_outpaces_consumer_is_healthy": False,
            "subsystems": subsystems,
        },
        "resource_attribution": {
            name: {
                "cycle_duration_seconds": row.get("cycle_duration_seconds"),
                "status_refresh_duration_seconds": row.get("status_refresh_duration_seconds"),
                "work_counter": row.get("work_counter"),
                "queue_depth": row.get("queue_depth"),
                "lag": row.get("lag"),
            }
            for name, row in subsystems.items()
        },
        "continuity": {
            "continuity_epoch": ECONOMIC_FREEZE_EPOCH,
            "process_started_at": PROCESS_STARTED_AT,
            "current_release": current_release if (current_release := _release()) else None,
            "subsystems": continuity,
            "restart_changes_economic_epoch": False,
            "candidate_ledger_is_persistent": True,
        },
        "background_work": {
            "direct_solana_is_canonical": True,
            "legacy_helius_webhook_worker_enabled": bool(legacy_webhook_worker_enabled),
            "unused_helius_worker_disabled_when_direct_solana_canonical": not bool(legacy_webhook_worker_enabled),
            "robinhood_historical_catchup_has_readiness_authority": False,
            "robinhood_historical_swap_replay_active": False,
            "robinhood_authoritative_rest_plus_ws_duplicate_ingestion": False,
            "proof_precomputation_supported": True,
        },
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "BACKPRESSURE_AGE_LIMIT_SECONDS",
    "OPERATIONS_VERSION",
    "PROCESS_STARTED_AT",
    "build_operations_proof",
    "normalize_subsystem",
]
