from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Callable


REPAIR_VERSION = "later-activity-execution-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
DURABLE_HANDOFF_WORKERS = 4
MAX_HANDOFF_ATTEMPTS = 3

_ORIGINAL_LIFECYCLE: Callable[..., str] | None = None
_ORIGINAL_SCHEDULE: Callable[..., Any] | None = None
_ORIGINAL_OBSERVE: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_DISCOVERY_RUN: Callable[..., Any] | None = None
_INSTALLED = False


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_time(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _same_venue_pattern(venue: str) -> str | None:
    return {
        "PUMP_FUN": "%PUMP_FUN%",
        "PUMP_AMM": "%PUMP_AMM%",
        "RAYDIUM": "%RAYDIUM%",
    }.get(str(venue or "").upper())


def _prior_same_venue_age_seconds(adapter: Any, row: dict[str, Any], venue: str) -> float | None:
    pattern = _same_venue_pattern(venue)
    token = str(row.get("token_mint") or "")
    current_raw = str(row.get("received_at") or "")
    current = _parse_time(current_raw)
    if not pattern or not token or current is None:
        return None
    try:
        with adapter.store._lock:
            prior = adapter.store.db.execute(
                "SELECT MIN(received_at) AS first_at FROM wallet_discovery_forward_observations "
                "WHERE token_mint=? AND received_at<? AND UPPER(source) LIKE ?",
                (token, current_raw, pattern),
            ).fetchone()
    except Exception:
        return None
    first_raw = str(prior["first_at"] or "") if prior is not None else ""
    first = _parse_time(first_raw)
    if first is None:
        return None
    return max(0.0, (current - first).total_seconds())


def _pump_fun_lifecycle(age: float) -> str:
    if age <= 120.0:
        return "pump_bonding_curve_observed_0_2m"
    if age <= 300.0:
        return "pump_bonding_curve_observed_2_5m"
    if age <= 900.0:
        return "pump_bonding_curve_observed_5_15m"
    if age <= 3600.0:
        return "pump_bonding_curve_observed_15_60m"
    if age <= 21600.0:
        return "pump_bonding_curve_observed_1_6h"
    return "pump_bonding_curve_observed_6h_plus"


def _pump_amm_lifecycle(age: float) -> str:
    if age <= 30.0:
        return "pump_amm_immediate_graduation_0_30s"
    if age <= 120.0:
        return "pump_amm_early_post_graduation_30_120s"
    if age <= 300.0:
        return "pump_amm_established_continuation_2_5m"
    if age <= 900.0:
        return "pump_amm_mature_intraday_5_15m"
    if age <= 3600.0:
        return "pump_amm_mature_intraday_15_60m"
    if age <= 21600.0:
        return "pump_amm_mature_intraday_1_6h"
    return "pump_amm_mature_intraday_6h_plus"


def _raydium_age_suffix(age: float) -> str:
    if age <= 300.0:
        return "observed_0_5m"
    if age <= 1800.0:
        return "observed_5_30m"
    if age <= 7200.0:
        return "observed_30m_2h"
    if age <= 21600.0:
        return "observed_2_6h"
    return "observed_6h_plus"


def _lifecycle_with_observed_age(adapter: Any, row: dict[str, Any], venue: str) -> str:
    if _ORIGINAL_LIFECYCLE is None:
        raise RuntimeError("later-activity lifecycle repair is not installed")
    base = _ORIGINAL_LIFECYCLE(adapter, row, venue)
    age = _prior_same_venue_age_seconds(adapter, row, venue)
    if age is None:
        # Do not invent token/pool age when this is the first same-venue observation.
        return base
    normalized = str(venue or "").upper()
    if normalized == "PUMP_FUN":
        return _pump_fun_lifecycle(age)
    if normalized == "PUMP_AMM":
        return _pump_amm_lifecycle(age)
    if normalized == "RAYDIUM":
        return f"{base}_{_raydium_age_suffix(age)}"
    return base


def _ensure_handoff_schema(adapter: Any) -> None:
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS later_activity_strategy_handoff ("
            "release_commit TEXT NOT NULL, signature TEXT NOT NULL, token_mint TEXT, wallet TEXT, side TEXT, "
            "tracking_transport TEXT, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, "
            "enqueued_at TEXT NOT NULL, updated_at TEXT NOT NULL, terminal_outcome TEXT, last_error TEXT, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "PRIMARY KEY(release_commit,signature))"
        )
        adapter.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_later_activity_handoff_state ON "
            "later_activity_strategy_handoff(release_commit,state,enqueued_at)"
        )


def _observation_metadata(adapter: Any, signature: str) -> dict[str, Any]:
    with adapter.store._lock:
        try:
            row = adapter.store.db.execute(
                "SELECT token_mint,wallet,side,copyable,risk_complete,tracking_transport "
                "FROM wallet_discovery_forward_observations WHERE signature=? LIMIT 1",
                (signature,),
            ).fetchone()
        except Exception:
            row = adapter.store.db.execute(
                "SELECT token_mint,wallet,side,copyable,risk_complete "
                "FROM wallet_discovery_forward_observations WHERE signature=? LIMIT 1",
                (signature,),
            ).fetchone()
    return dict(row) if row is not None else {}


def _enqueue_handoff(adapter: Any, signature: str) -> bool:
    _ensure_handoff_schema(adapter)
    metadata = _observation_metadata(adapter, signature)
    now = _utcnow()
    with adapter.store._lock, adapter.store.db:
        cursor = adapter.store.db.execute(
            "INSERT OR IGNORE INTO later_activity_strategy_handoff("
            "release_commit,signature,token_mint,wallet,side,tracking_transport,state,attempts,enqueued_at,updated_at,"
            "terminal_outcome,last_error,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,'pending',0,?,?,NULL,NULL,1,0)",
            (
                adapter.release_commit,
                signature,
                str(metadata.get("token_mint") or "") or None,
                str(metadata.get("wallet") or "") or None,
                str(metadata.get("side") or "") or None,
                str(metadata.get("tracking_transport") or "") or None,
                now,
                now,
            ),
        )
    return cursor.rowcount == 1


def _pending_count(adapter: Any) -> int:
    _ensure_handoff_schema(adapter)
    with adapter.store._lock:
        return int(
            adapter.store.db.execute(
                "SELECT COUNT(*) FROM later_activity_strategy_handoff WHERE release_commit=? AND state='pending'",
                (adapter.release_commit,),
            ).fetchone()[0]
        )


def _claim_handoff(adapter: Any) -> dict[str, Any] | None:
    _ensure_handoff_schema(adapter)
    now = _utcnow()
    with adapter.store._lock, adapter.store.db:
        row = adapter.store.db.execute(
            "SELECT signature,attempts FROM later_activity_strategy_handoff "
            "WHERE release_commit=? AND state='pending' ORDER BY enqueued_at,signature LIMIT 1",
            (adapter.release_commit,),
        ).fetchone()
        if row is None:
            return None
        cursor = adapter.store.db.execute(
            "UPDATE later_activity_strategy_handoff SET state='processing',attempts=attempts+1,updated_at=? "
            "WHERE release_commit=? AND signature=? AND state='pending'",
            (now, adapter.release_commit, str(row["signature"])),
        )
        if cursor.rowcount != 1:
            return None
    return {"signature": str(row["signature"]), "attempts": int(row["attempts"] or 0) + 1}


def _terminal_outcome(adapter: Any, signature: str) -> tuple[str | None, str | None]:
    metadata = _observation_metadata(adapter, signature)
    if not metadata:
        return None, "forward_observation_missing"
    side = str(metadata.get("side") or "").lower()
    if side == "buy":
        try:
            with adapter.store._lock:
                selected = adapter.store.db.execute(
                    "SELECT decision FROM risk_conditioned_alpha_v5_trials "
                    "WHERE release_commit=? AND source_signature=? AND selected=1 ORDER BY id DESC LIMIT 1",
                    (adapter.release_commit, signature),
                ).fetchone()
                any_trial = adapter.store.db.execute(
                    "SELECT decision FROM risk_conditioned_alpha_v5_trials "
                    "WHERE release_commit=? AND source_signature=? ORDER BY id DESC LIMIT 1",
                    (adapter.release_commit, signature),
                ).fetchone()
        except Exception:
            selected = any_trial = None
        if selected is not None:
            return str(selected["decision"]), None
        if any_trial is not None:
            return str(any_trial["decision"]), None
        if not bool(metadata.get("copyable")):
            return "reject_not_copyable_at_observation", None
        return None, "copyable_buy_missing_canonical_terminal_trial"
    if side == "sell":
        return "sell_exit_evaluation_complete", None
    return "ignore_non_swap_or_unknown_side", None


def _finish_handoff(
    adapter: Any,
    signature: str,
    *,
    state: str,
    terminal_outcome: str | None = None,
    error: str | None = None,
) -> None:
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "UPDATE later_activity_strategy_handoff SET state=?,terminal_outcome=?,last_error=?,updated_at=? "
            "WHERE release_commit=? AND signature=?",
            (state, terminal_outcome, error, _utcnow(), adapter.release_commit, signature),
        )


async def _handoff_worker(adapter: Any) -> None:
    if _ORIGINAL_OBSERVE is None:
        raise RuntimeError("later-activity handoff repair is not installed")
    try:
        while True:
            claimed = _claim_handoff(adapter)
            if claimed is None:
                return
            signature = str(claimed["signature"])
            attempts = int(claimed["attempts"])
            try:
                await _ORIGINAL_OBSERVE(adapter, signature)
                terminal, error = _terminal_outcome(adapter, signature)
                if terminal is not None:
                    _finish_handoff(
                        adapter,
                        signature,
                        state="complete",
                        terminal_outcome=terminal,
                    )
                    continue
                if attempts < MAX_HANDOFF_ATTEMPTS:
                    _finish_handoff(adapter, signature, state="pending", error=error)
                    await asyncio.sleep(min(0.75, 0.15 * attempts))
                    continue
                _finish_handoff(
                    adapter,
                    signature,
                    state="failed_closed",
                    error=error or "canonical_strategy_handoff_failed",
                )
            except asyncio.CancelledError:
                _finish_handoff(adapter, signature, state="pending", error="worker_cancelled")
                raise
            except Exception as exc:
                if attempts < MAX_HANDOFF_ATTEMPTS:
                    _finish_handoff(
                        adapter,
                        signature,
                        state="pending",
                        error=f"{type(exc).__name__}: canonical strategy handoff retry",
                    )
                    await asyncio.sleep(min(0.75, 0.15 * attempts))
                else:
                    _finish_handoff(
                        adapter,
                        signature,
                        state="failed_closed",
                        error=f"{type(exc).__name__}: canonical strategy handoff exhausted",
                    )
    finally:
        tasks = getattr(adapter, "_roi_later_activity_handoff_tasks", None)
        if isinstance(tasks, set):
            current = asyncio.current_task()
            if current is not None:
                tasks.discard(current)


def _ensure_handoff_workers(adapter: Any) -> None:
    if _pending_count(adapter) <= 0:
        return
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    tasks = getattr(adapter, "_roi_later_activity_handoff_tasks", None)
    if not isinstance(tasks, set):
        tasks = set()
        setattr(adapter, "_roi_later_activity_handoff_tasks", tasks)
    tasks.difference_update({task for task in tasks if task.done()})
    desired = min(DURABLE_HANDOFF_WORKERS, max(1, _pending_count(adapter)))
    while len(tasks) < desired:
        task = asyncio.create_task(_handoff_worker(adapter), name="later-activity-strategy-handoff")
        tasks.add(task)


def _durable_schedule(self: Any, signature: str) -> None:
    if not signature:
        return
    _enqueue_handoff(self, str(signature))
    _ensure_handoff_workers(self)


def _reset_orphaned_processing(adapter: Any) -> None:
    _ensure_handoff_schema(adapter)
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "UPDATE later_activity_strategy_handoff SET state='pending',last_error='process_restart_requeued',updated_at=? "
            "WHERE release_commit=? AND state='processing'",
            (_utcnow(), adapter.release_commit),
        )


async def _run_with_durable_handoff(self: Any, stop: asyncio.Event) -> None:
    if _ORIGINAL_DISCOVERY_RUN is None:
        raise RuntimeError("later-activity discovery wrapper is not installed")
    from .profit_first_entity_final_research import _adapter

    adapter = _adapter(self)
    _reset_orphaned_processing(adapter)
    _ensure_handoff_workers(adapter)
    try:
        await _ORIGINAL_DISCOVERY_RUN(self, stop)
    finally:
        tasks = getattr(adapter, "_roi_later_activity_handoff_tasks", None)
        if isinstance(tasks, set):
            for task in tuple(tasks):
                task.cancel()
            if tasks:
                await asyncio.gather(*tuple(tasks), return_exceptions=True)
            tasks.clear()


def _status_with_later_activity(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("later-activity status wrapper is not installed")
    payload = _ORIGINAL_STATUS(self)
    try:
        _ensure_handoff_schema(self)
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT state,COUNT(*) AS n FROM later_activity_strategy_handoff "
                "WHERE release_commit=? GROUP BY state",
                (self.release_commit,),
            ).fetchall()
            oldest = self.store.db.execute(
                "SELECT MIN(enqueued_at) AS oldest FROM later_activity_strategy_handoff "
                "WHERE release_commit=? AND state='pending'",
                (self.release_commit,),
            ).fetchone()
            failed = self.store.db.execute(
                "SELECT signature,last_error FROM later_activity_strategy_handoff "
                "WHERE release_commit=? AND state='failed_closed' ORDER BY updated_at DESC LIMIT 1",
                (self.release_commit,),
            ).fetchone()
        counts = {str(row["state"]): int(row["n"]) for row in rows}
        payload["later_activity_execution_repair"] = {
            "repair_version": REPAIR_VERSION,
            "durable_strategy_handoff": True,
            "legacy_in_memory_task_drop_removed": True,
            "handoff_worker_limit": DURABLE_HANDOFF_WORKERS,
            "max_handoff_attempts": MAX_HANDOFF_ATTEMPTS,
            "handoff_states": counts,
            "pending": counts.get("pending", 0),
            "processing": counts.get("processing", 0),
            "complete": counts.get("complete", 0),
            "failed_closed": counts.get("failed_closed", 0),
            "oldest_pending_at": str(oldest["oldest"] or "") if oldest is not None else None,
            "last_failed_signature": str(failed["signature"] or "") if failed is not None else None,
            "last_failed_error": str(failed["last_error"] or "") if failed is not None else None,
            "lifecycle_age_basis": "first_prior_same_venue_forward_observation_only",
            "unproven_launch_or_pool_age_is_fabricated": False,
            "later_lifecycle_partitioned": True,
            "pumpfun_late_buckets": ["2-5m", "5-15m", "15-60m", "1-6h", "6h+"],
            "pumpswap_late_buckets": ["5-15m", "15-60m", "1-6h", "6h+"],
            "raydium_late_buckets": ["5-30m", "30m-2h", "2-6h", "6h+"],
            "strategy_thresholds_changed": False,
            "tracking_capacity_changed": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    except Exception as exc:
        payload["later_activity_execution_repair"] = {
            "repair_version": REPAIR_VERSION,
            "failed_closed": True,
            "error": f"{type(exc).__name__}: later-activity handoff status unavailable",
            "paper_only": True,
            "live_money_authority": False,
        }
    return payload


def install_later_activity_execution_repair() -> None:
    global _INSTALLED, _ORIGINAL_LIFECYCLE, _ORIGINAL_SCHEDULE, _ORIGINAL_OBSERVE
    global _ORIGINAL_STATUS, _ORIGINAL_DISCOVERY_RUN
    if _INSTALLED:
        return

    from . import risk_conditioned_alpha_v5 as v5
    from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
    from .wallet_discovery import ContinuousWalletDiscovery

    current_lifecycle = v5._lifecycle
    if not bool(getattr(current_lifecycle, "_roi_later_activity_lifecycle", False)):
        _ORIGINAL_LIFECYCLE = current_lifecycle
        setattr(_lifecycle_with_observed_age, "_roi_later_activity_lifecycle", True)
        v5._lifecycle = _lifecycle_with_observed_age

    current_schedule = FinalProfitFirstResearchAdapter.schedule
    if not bool(getattr(current_schedule, "_roi_later_activity_durable_handoff", False)):
        _ORIGINAL_SCHEDULE = current_schedule
        _ORIGINAL_OBSERVE = FinalProfitFirstResearchAdapter.observe
        setattr(_durable_schedule, "_roi_later_activity_durable_handoff", True)
        FinalProfitFirstResearchAdapter.schedule = _durable_schedule  # type: ignore[method-assign]

    current_status = FinalProfitFirstResearchAdapter.status
    if not bool(getattr(current_status, "_roi_later_activity_durable_handoff", False)):
        _ORIGINAL_STATUS = current_status
        setattr(_status_with_later_activity, "_roi_later_activity_durable_handoff", True)
        FinalProfitFirstResearchAdapter.status = _status_with_later_activity  # type: ignore[method-assign]

    current_run = ContinuousWalletDiscovery.run
    if not bool(getattr(current_run, "_roi_later_activity_durable_handoff", False)):
        _ORIGINAL_DISCOVERY_RUN = current_run
        setattr(_run_with_durable_handoff, "_roi_later_activity_durable_handoff", True)
        ContinuousWalletDiscovery.run = _run_with_durable_handoff  # type: ignore[method-assign]

    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "DURABLE_HANDOFF_WORKERS",
    "MAX_HANDOFF_ATTEMPTS",
    "_enqueue_handoff",
    "_lifecycle_with_observed_age",
    "_prior_same_venue_age_seconds",
    "install_later_activity_execution_repair",
]
