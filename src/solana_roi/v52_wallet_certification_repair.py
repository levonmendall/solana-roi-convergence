from __future__ import annotations

import asyncio
import os
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from .strategy_v52_authority import target_sizing_policy
from .v52_wallet_intelligence_alignment import status as wallet_alignment_status
from .wallet_realtime_tracking_repair import RealtimeWalletTracker, utcnow


REPAIR_VERSION = "v52-wallet-certification-runtime-v1"
STATUS_PATH = "/v1/strategy/v52/wallet-certification"
RECOVERY_DB_LOCK_RETRIES = max(
    1,
    int(os.getenv("SOLANA_ROI_WALLET_RECOVERY_DB_LOCK_RETRIES", "6")),
)
RECOVERY_DB_LOCK_RETRY_SECONDS = max(
    0.01,
    float(os.getenv("SOLANA_ROI_WALLET_RECOVERY_DB_LOCK_RETRY_SECONDS", "0.20")),
)
_INSTALLED = False
_RUNTIME: Any | None = None
_WALLET_ALPHA: Any | None = None
_PREVIOUS_STATUS: Callable[..., dict[str, Any]] | None = None


def _is_sqlite_lock_error(exc: BaseException) -> bool:
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    text = str(exc).lower()
    return (
        "database is locked" in text
        or "database table is locked" in text
        or "database is busy" in text
    )


def _ensure_recovery_metrics(tracker: Any) -> None:
    if not hasattr(tracker, "_roi_recovery_db_lock_retries"):
        tracker._roi_recovery_db_lock_retries = 0
    if not hasattr(tracker, "_roi_recovery_db_lock_failures"):
        tracker._roi_recovery_db_lock_failures = 0
    if not hasattr(tracker, "_roi_recovery_serial_lock"):
        tracker._roi_recovery_serial_lock = asyncio.Lock()


async def _with_sqlite_lock_retry(
    tracker: Any,
    operation: Callable[[], Awaitable[Any]],
) -> Any:
    _ensure_recovery_metrics(tracker)
    for attempt in range(RECOVERY_DB_LOCK_RETRIES + 1):
        try:
            return await operation()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not _is_sqlite_lock_error(exc):
                raise
            if attempt >= RECOVERY_DB_LOCK_RETRIES:
                tracker._roi_recovery_db_lock_failures += 1
                raise
            tracker._roi_recovery_db_lock_retries += 1
            await asyncio.sleep(
                min(1.0, RECOVERY_DB_LOCK_RETRY_SECONDS * (2**attempt))
            )
    raise RuntimeError("wallet sqlite retry loop exhausted unexpectedly")


async def _write_last_recovery_at(tracker: Any) -> None:
    def write() -> None:
        with tracker.store._lock, tracker.store.db:
            tracker.store.db.execute(
                "UPDATE wallet_realtime_runtime SET last_recovery_at=? WHERE id=1",
                (utcnow().isoformat(),),
            )

    async def operation() -> None:
        write()

    await _with_sqlite_lock_retry(tracker, operation)


async def _recover_wallet_with_retry(tracker: Any, wallet: str) -> bool:
    async def operation() -> bool:
        return bool(await tracker._recover_wallet(wallet))

    return bool(await _with_sqlite_lock_retry(tracker, operation))


async def _serialized_recover_all(tracker: Any) -> None:
    """Recover tracked wallets serially and tolerate only bounded SQLite contention.

    Live hydration remains prioritized by the existing scheduler. Recovery no longer
    opens two concurrent wallet catch-up writers against SQLite, and transient locks
    are retried with bounded exponential backoff. Exhausted contention remains a
    recovery failure: no success timestamp is published and no continuity gate is
    weakened.
    """

    _ensure_recovery_metrics(tracker)
    tracker._recovery_runs += 1
    tracker._recovery_task = asyncio.current_task()
    failed = False
    try:
        async with tracker._roi_recovery_serial_lock:
            for wallet in tuple(tracker._wallets):
                try:
                    recovered = await _recover_wallet_with_retry(tracker, wallet)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    failed = True
                    if _is_sqlite_lock_error(exc):
                        tracker._last_error = (
                            "OperationalError: wallet recovery sqlite lock retry exhausted"
                        )
                    else:
                        tracker._last_error = (
                            f"{type(exc).__name__}: wallet realtime recovery failed"
                        )
                    break
                if not recovered:
                    failed = True
            if not failed:
                try:
                    await _write_last_recovery_at(tracker)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    failed = True
                    if _is_sqlite_lock_error(exc):
                        tracker._last_error = (
                            "OperationalError: wallet recovery completion sqlite lock retry exhausted"
                        )
                    else:
                        tracker._last_error = (
                            f"{type(exc).__name__}: wallet recovery completion failed"
                        )
        if failed:
            tracker._recovery_failures += 1
    finally:
        tracker._recovery_task = None


def _status_with_recovery_repair(tracker: Any) -> dict[str, Any]:
    _ensure_recovery_metrics(tracker)
    predecessor = _PREVIOUS_STATUS
    payload = dict(predecessor(tracker)) if callable(predecessor) else {}
    payload["recovery_lock_repair"] = {
        "installed": True,
        "version": REPAIR_VERSION,
        "serialized_recovery": True,
        "sqlite_lock_retry_limit": RECOVERY_DB_LOCK_RETRIES,
        "sqlite_lock_retry_base_seconds": RECOVERY_DB_LOCK_RETRY_SECONDS,
        "sqlite_lock_retries_session": int(tracker._roi_recovery_db_lock_retries),
        "sqlite_lock_failures_session": int(tracker._roi_recovery_db_lock_failures),
        "failed_recovery_does_not_publish_success_timestamp": True,
        "continuity_gate_unchanged": True,
        "paper_only": True,
    }
    return payload


def _table_exists(store: Any, table: str) -> bool:
    with store._lock:
        row = store.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
    return row is not None


def _alpha_certification(alpha: Any) -> dict[str, Any]:
    base = dict(alpha.status())
    minimum = int(target_sizing_policy()["minimum_forward_samples"])
    store = alpha.store
    contexts: list[tuple[str, str]] = []
    if _table_exists(store, "v52_wallet_marginal_alpha"):
        with store._lock:
            rows = store.db.execute(
                "SELECT DISTINCT wallet, context_key FROM v52_wallet_marginal_alpha "
                "ORDER BY wallet, context_key"
            ).fetchall()
        contexts = [(str(row["wallet"]), str(row["context_key"])) for row in rows]

    scores = [alpha.score(wallet, context) for wallet, context in contexts]
    blocker_counts: Counter[str] = Counter()
    for score in scores:
        blocker_counts.update(str(value) for value in score.blockers)
    eligible = [score for score in scores if score.eligible_for_strategy_influence]
    sample_ready = [score for score in scores if score.paired_forward_episodes >= minimum]
    positive_alpha = [score for score in scores if score.decayed_marginal_alpha > 0.0]
    copyable = [score for score in scores if score.copyability_rate >= 0.80]

    return {
        **base,
        "context_count": len(scores),
        "minimum_forward_samples_per_wallet_context": minimum,
        "minimum_copyability_rate": 0.80,
        "positive_decayed_marginal_alpha_required": True,
        "contexts_meeting_forward_sample_minimum": len(sample_ready),
        "contexts_with_positive_decayed_marginal_alpha": len(positive_alpha),
        "contexts_meeting_copyability_minimum": len(copyable),
        "eligible_context_count": len(eligible),
        "max_paired_forward_episodes_per_context": max(
            (score.paired_forward_episodes for score in scores),
            default=0,
        ),
        "blocker_counts": dict(sorted(blocker_counts.items())),
        "any_context_incremental_alpha_proven": bool(eligible),
        "strategy_influence_gate_satisfied": bool(eligible),
        "gate_not_lowered": minimum >= 30,
    }


def _coverage_status(runtime: Any) -> dict[str, Any]:
    store = runtime.store
    queue: dict[str, int] = {}
    active_wallets = 0
    wallet_state_errors = 0
    epoch_resets = 0
    oldest_live_received_at: str | None = None
    newest_live_received_at: str | None = None
    runtime_row: Any | None = None
    broad_scan_row: Any | None = None

    if _table_exists(store, "wallet_realtime_receipts"):
        with store._lock:
            rows = store.db.execute(
                "SELECT status, COUNT(*) AS n FROM wallet_realtime_receipts GROUP BY status"
            ).fetchall()
        queue = {str(row["status"]): int(row["n"]) for row in rows}
    if _table_exists(store, "wallet_realtime_state"):
        with store._lock:
            row = store.db.execute(
                "SELECT COUNT(*) AS active_wallets, "
                "SUM(CASE WHEN active=1 AND last_error IS NOT NULL THEN 1 ELSE 0 END) AS state_errors, "
                "COALESCE(SUM(epoch_resets), 0) AS resets, "
                "MIN(CASE WHEN active=1 THEN last_live_received_at END) AS oldest_live, "
                "MAX(CASE WHEN active=1 THEN last_live_received_at END) AS newest_live "
                "FROM wallet_realtime_state WHERE active=1"
            ).fetchone()
        if row is not None:
            active_wallets = int(row["active_wallets"] or 0)
            wallet_state_errors = int(row["state_errors"] or 0)
            epoch_resets = int(row["resets"] or 0)
            oldest_live_received_at = str(row["oldest_live"] or "") or None
            newest_live_received_at = str(row["newest_live"] or "") or None
    if _table_exists(store, "wallet_realtime_runtime"):
        with store._lock:
            runtime_row = store.db.execute(
                "SELECT last_cycle_at, last_error, last_provider_change_at, last_recovery_at "
                "FROM wallet_realtime_runtime WHERE id=1"
            ).fetchone()
    if _table_exists(store, "wallet_discovery_state"):
        with store._lock:
            columns = {
                str(row["name"])
                for row in store.db.execute("PRAGMA table_info(wallet_discovery_state)").fetchall()
            }
            select = ["last_broad_scan_at"]
            if "last_normalized_swap_id" in columns:
                select.append("last_normalized_swap_id")
            broad_scan_row = store.db.execute(
                "SELECT " + ", ".join(select) + " FROM wallet_discovery_state WHERE id=1"
            ).fetchone()

    failed_receipts = int(queue.get("failed", 0))
    pending_receipts = int(queue.get("pending", 0))
    persisted_runtime_error = (
        str(runtime_row["last_error"] or "") if runtime_row is not None else ""
    )
    return {
        "repair_version": REPAIR_VERSION,
        "recovery_serialized": True,
        "bounded_sqlite_lock_retry": True,
        "sqlite_lock_retry_limit": RECOVERY_DB_LOCK_RETRIES,
        "active_wallet_count": active_wallets,
        "wallet_state_error_count": wallet_state_errors,
        "epoch_resets_total": epoch_resets,
        "durable_receipt_queue": queue,
        "pending_receipts": pending_receipts,
        "failed_receipts": failed_receipts,
        "oldest_active_wallet_live_received_at": oldest_live_received_at,
        "newest_active_wallet_live_received_at": newest_live_received_at,
        "last_cycle_at": (
            str(runtime_row["last_cycle_at"] or "") or None
            if runtime_row is not None
            else None
        ),
        "last_provider_change_at": (
            str(runtime_row["last_provider_change_at"] or "") or None
            if runtime_row is not None
            else None
        ),
        "last_successful_recovery_at": (
            str(runtime_row["last_recovery_at"] or "") or None
            if runtime_row is not None
            else None
        ),
        "persisted_runtime_error": persisted_runtime_error or None,
        "last_broad_scan_at": (
            str(broad_scan_row["last_broad_scan_at"] or "") or None
            if broad_scan_row is not None
            else None
        ),
        "last_normalized_swap_id": (
            int(broad_scan_row["last_normalized_swap_id"] or 0)
            if broad_scan_row is not None and "last_normalized_swap_id" in broad_scan_row.keys()
            else None
        ),
        "concrete_continuity_failure_present": bool(
            wallet_state_errors or failed_receipts or persisted_runtime_error
        ),
        "discovery_universe_comprehensiveness_proven": False,
        "coverage_forward_observation_required": True,
        "duplicate_fetch_reintroduced": False,
        "paper_only": True,
        "live_money_authority": False,
    }


def wallet_certification_status(
    runtime: Any | None = None,
    wallet_alpha: Any | None = None,
) -> dict[str, Any]:
    runtime = runtime if runtime is not None else _RUNTIME
    wallet_alpha = wallet_alpha if wallet_alpha is not None else _WALLET_ALPHA
    if runtime is None or wallet_alpha is None:
        return {
            "installed": _INSTALLED,
            "version": REPAIR_VERSION,
            "ready": False,
            "reason": "runtime_or_wallet_alpha_not_installed",
            "paper_only": True,
            "live_money_authority": False,
        }
    alpha = _alpha_certification(wallet_alpha)
    coverage = _coverage_status(runtime)
    alignment = wallet_alignment_status(runtime)
    return {
        "installed": _INSTALLED,
        "version": REPAIR_VERSION,
        "wallet_alpha": alpha,
        "wallet_discovery_alignment": alignment,
        "wallet_coverage": coverage,
        "wallet_alpha_proven": bool(alpha["any_context_incremental_alpha_proven"]),
        "wallet_discovery_comprehensiveness_proven": bool(
            coverage["discovery_universe_comprehensiveness_proven"]
        ),
        "certification_complete": bool(
            alpha["any_context_incremental_alpha_proven"]
            and coverage["discovery_universe_comprehensiveness_proven"]
            and not coverage["concrete_continuity_failure_present"]
        ),
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def install_v52_wallet_certification_repair(
    app: Any,
    runtime: Any,
    wallet_alpha: Any,
) -> None:
    global _INSTALLED, _RUNTIME, _WALLET_ALPHA, _PREVIOUS_STATUS
    _RUNTIME = runtime
    _WALLET_ALPHA = wallet_alpha

    if not bool(getattr(RealtimeWalletTracker._recover_all, "_roi_wallet_certification_repair", False)):
        setattr(_serialized_recover_all, "_roi_wallet_certification_repair", True)
        RealtimeWalletTracker._recover_all = _serialized_recover_all  # type: ignore[method-assign]
    if not bool(getattr(RealtimeWalletTracker.status, "_roi_wallet_certification_repair", False)):
        _PREVIOUS_STATUS = RealtimeWalletTracker.status
        setattr(_status_with_recovery_repair, "_roi_wallet_certification_repair", True)
        RealtimeWalletTracker.status = _status_with_recovery_repair  # type: ignore[method-assign]

    existing = {getattr(route, "path", None) for route in app.routes}
    if STATUS_PATH not in existing:
        @app.get(STATUS_PATH)
        def wallet_certification() -> dict[str, Any]:
            return wallet_certification_status(runtime, wallet_alpha)

    app.state.roi_v52_wallet_certification_status = (
        lambda: wallet_certification_status(runtime, wallet_alpha)
    )
    app.state.roi_v52_wallet_recovery_lock_repair = True
    _INSTALLED = True


__all__ = [
    "RECOVERY_DB_LOCK_RETRIES",
    "REPAIR_VERSION",
    "STATUS_PATH",
    "_is_sqlite_lock_error",
    "_serialized_recover_all",
    "install_v52_wallet_certification_repair",
    "wallet_certification_status",
]
