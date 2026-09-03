from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta
from typing import Any

from .direct_transaction import normalize_standard_transaction
from .ingestion import NormalizedSwap
from .wallet_realtime_tracking_repair import (
    REALTIME_HYDRATION_ATTEMPTS,
    REALTIME_MARK_DELAY_LIMIT_SECONDS,
    RealtimeWalletTracker,
    utcnow,
)


RECOVERY_PROVIDER = "bounded-realtime-recovery"
FRESH_LIVE_WINDOW_SECONDS = float(
    os.getenv("SOLANA_ROI_WALLET_FRESH_LIVE_WINDOW_SECONDS", str(REALTIME_MARK_DELAY_LIMIT_SECONDS))
)
LIVE_WORKERS = max(1, int(os.getenv("SOLANA_ROI_WALLET_LIVE_HYDRATION_WORKERS", "6")))
BACKLOG_WORKERS = max(1, int(os.getenv("SOLANA_ROI_WALLET_BACKLOG_HYDRATION_WORKERS", "1")))
RISK_WORKERS = max(1, int(os.getenv("SOLANA_ROI_WALLET_RISK_WORKERS", "2")))
RISK_RETRY_SECONDS = max(0.5, float(os.getenv("SOLANA_ROI_WALLET_RISK_RETRY_SECONDS", "3")))
RECOVERY_CONCURRENCY = max(1, int(os.getenv("SOLANA_ROI_WALLET_RECOVERY_CONCURRENCY", "2")))

_ORIGINAL_TRACKER_INIT = RealtimeWalletTracker.__init__
_ORIGINAL_TRACKER_STATUS = RealtimeWalletTracker.status


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, int((len(ordered) - 1) * q))
    return ordered[index]


def _ensure_priority_schema(self: Any) -> None:
    now = utcnow().isoformat()
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "CREATE TABLE IF NOT EXISTS wallet_realtime_risk_work ("
            "signature TEXT PRIMARY KEY, status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, "
            "enqueued_at TEXT NOT NULL, updated_at TEXT NOT NULL, next_attempt_at TEXT NOT NULL, last_error TEXT)"
        )
        self.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_wallet_realtime_risk_work_status "
            "ON wallet_realtime_risk_work(status, next_attempt_at, enqueued_at)"
        )
        self.store.db.execute(
            "UPDATE wallet_realtime_risk_work SET status='pending', updated_at=?, next_attempt_at=? "
            "WHERE status='processing'",
            (now, now),
        )
        self.store.db.execute(
            "INSERT OR IGNORE INTO wallet_realtime_risk_work("
            "signature, status, attempts, enqueued_at, updated_at, next_attempt_at) "
            "SELECT signature, 'pending', 0, received_at, ?, ? "
            "FROM wallet_discovery_forward_observations "
            "WHERE tracking_transport='logsSubscribe' AND side='buy' AND risk_complete=0",
            (now, now),
        )


def _priority_init(self: RealtimeWalletTracker, discovery: Any) -> None:
    _ORIGINAL_TRACKER_INIT(self, discovery)
    self._roi_fresh_live_delay_ms: list[float] = []
    self._roi_backlog_delay_ms: list[float] = []
    self._roi_fresh_live_normalized = 0
    self._roi_fresh_live_copyable = 0
    self._roi_backlog_normalized = 0
    self._roi_risk_completed = 0
    self._roi_risk_incomplete_retries = 0
    self._roi_risk_failures = 0
    _ensure_priority_schema(self)


def _claim_priority_receipt(self: Any, lane: str) -> dict[str, Any] | None:
    now = utcnow()
    cutoff = (now - timedelta(seconds=FRESH_LIVE_WINDOW_SECONDS)).isoformat()
    now_iso = now.isoformat()
    with self.store._lock, self.store.db:
        if lane == "fresh_live":
            row = self.store.db.execute(
                "SELECT id, signature, wallet, slot, received_at, provider, attempts "
                "FROM wallet_realtime_receipts WHERE status='pending' "
                "AND provider<>? AND received_at>=? "
                "ORDER BY received_at DESC, id DESC LIMIT 1",
                (RECOVERY_PROVIDER, cutoff),
            ).fetchone()
        elif lane == "backlog":
            row = self.store.db.execute(
                "SELECT id, signature, wallet, slot, received_at, provider, attempts "
                "FROM wallet_realtime_receipts WHERE status='pending' "
                "AND (provider=? OR received_at<?) "
                "ORDER BY CASE WHEN provider=? THEN 1 ELSE 0 END, received_at, id LIMIT 1",
                (RECOVERY_PROVIDER, cutoff, RECOVERY_PROVIDER),
            ).fetchone()
        else:
            raise ValueError(f"unknown wallet receipt lane: {lane}")
        if row is None:
            return None
        cursor = self.store.db.execute(
            "UPDATE wallet_realtime_receipts SET status='processing', attempts=attempts+1, updated_at=? "
            "WHERE id=? AND status='pending'",
            (now_iso, int(row["id"])),
        )
        if cursor.rowcount != 1:
            return None
    return dict(row)


def _record_lane_result(self: Any, *, lane: str, signature: str, received_at: datetime, inserted: bool) -> None:
    delay_ms = max(0.0, (utcnow() - received_at).total_seconds() * 1000.0)
    if lane == "fresh_live":
        window = self._roi_fresh_live_delay_ms
    else:
        window = self._roi_backlog_delay_ms
    window.append(delay_ms)
    if len(window) > 500:
        del window[:-500]
    if not inserted:
        return
    with self.store._lock:
        row = self.store.db.execute(
            "SELECT copyable FROM wallet_discovery_forward_observations WHERE signature=?",
            (signature,),
        ).fetchone()
    if lane == "fresh_live":
        self._roi_fresh_live_normalized += 1
        if row is not None and bool(row["copyable"]):
            self._roi_fresh_live_copyable += 1
    else:
        self._roi_backlog_normalized += 1


async def _process_priority_receipt(self: Any, row: dict[str, Any], *, lane: str) -> None:
    row_id = int(row["id"])
    signature = str(row["signature"])
    wallet = str(row["wallet"])
    received_at = datetime.fromisoformat(str(row["received_at"]))
    try:
        result = await self._hydrate_transaction(signature)
        self._hydrated += 1
        if result is None:
            raise RuntimeError("confirmed transaction unavailable")
        swap = normalize_standard_transaction(
            result,
            signature=signature,
            trigger_received_at=received_at,
            source_hint=None,
        )
        inserted = False
        if swap is not None and swap.wallet == wallet:
            swap = NormalizedSwap(
                signature=swap.signature,
                slot=swap.slot,
                observed_at=swap.observed_at,
                received_at=received_at,
                wallet=swap.wallet,
                token_mint=swap.token_mint,
                side=swap.side,
                token_amount=swap.token_amount,
                native_amount_sol=swap.native_amount_sol,
                reference_price_sol=swap.reference_price_sol,
                source="wallet-realtime:" + swap.source,
            )
            inserted = await self._record_quick_forward_swap(swap)
        _record_lane_result(self, lane=lane, signature=signature, received_at=received_at, inserted=inserted)
        self._finish_receipt(row_id, status="complete")
    except asyncio.CancelledError:
        self._finish_receipt(row_id, status="pending", error="cancelled")
        raise
    except Exception as exc:
        attempts = int(row.get("attempts") or 0) + 1
        if attempts >= REALTIME_HYDRATION_ATTEMPTS:
            self._finish_receipt(
                row_id,
                status="failed",
                error=f"{type(exc).__name__}: realtime hydration continuity lost",
            )
            await self._begin_epoch(
                wallet,
                reason="terminal_realtime_hydration_failure",
                reset_count=True,
            )
        else:
            self._finish_receipt(
                row_id,
                status="pending",
                error=f"{type(exc).__name__}: realtime hydration retry",
            )
            await asyncio.sleep(min(1.0, 0.05 * attempts))


async def _priority_receipt_worker(self: Any, stop: asyncio.Event, *, lane: str) -> None:
    idle_seconds = 0.02 if lane == "fresh_live" else 0.10
    while not stop.is_set():
        row = _claim_priority_receipt(self, lane)
        if row is None:
            try:
                await asyncio.wait_for(stop.wait(), timeout=idle_seconds)
            except asyncio.TimeoutError:
                continue
            return
        await _process_priority_receipt(self, row, lane=lane)


def _sync_risk_work(self: Any) -> None:
    now = utcnow().isoformat()
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "INSERT OR IGNORE INTO wallet_realtime_risk_work("
            "signature, status, attempts, enqueued_at, updated_at, next_attempt_at) "
            "SELECT signature, 'pending', 0, received_at, ?, ? "
            "FROM wallet_discovery_forward_observations "
            "WHERE tracking_transport='logsSubscribe' AND side='buy' AND risk_complete=0",
            (now, now),
        )
        self.store.db.execute(
            "UPDATE wallet_realtime_risk_work SET status='complete', updated_at=?, last_error=NULL "
            "WHERE status<>'complete' AND signature IN ("
            "SELECT signature FROM wallet_discovery_forward_observations "
            "WHERE tracking_transport='logsSubscribe' AND side='buy' AND risk_complete=1)",
            (now,),
        )


def _claim_risk_work(self: Any) -> dict[str, Any] | None:
    _sync_risk_work(self)
    now = utcnow().isoformat()
    with self.store._lock, self.store.db:
        row = self.store.db.execute(
            "SELECT w.signature, w.attempts, o.wallet, o.token_mint, o.side, o.token_amount, "
            "o.observed_at, o.received_at, o.wallet_price_sol, o.source "
            "FROM wallet_realtime_risk_work w "
            "JOIN wallet_discovery_forward_observations o ON o.signature=w.signature "
            "WHERE w.status='pending' AND w.next_attempt_at<=? AND o.risk_complete=0 "
            "ORDER BY w.next_attempt_at, w.enqueued_at LIMIT 1",
            (now,),
        ).fetchone()
        if row is None:
            return None
        cursor = self.store.db.execute(
            "UPDATE wallet_realtime_risk_work SET status='processing', attempts=attempts+1, updated_at=? "
            "WHERE signature=? AND status='pending'",
            (now, str(row["signature"])),
        )
        if cursor.rowcount != 1:
            return None
    return dict(row)


def _finish_risk_work(
    self: Any,
    signature: str,
    *,
    status: str,
    error: str | None = None,
    retry_after_seconds: float = 0.0,
) -> None:
    now = utcnow()
    next_attempt = now + timedelta(seconds=max(0.0, retry_after_seconds))
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "UPDATE wallet_realtime_risk_work SET status=?, updated_at=?, next_attempt_at=?, last_error=? "
            "WHERE signature=?",
            (status, now.isoformat(), next_attempt.isoformat(), error, signature),
        )


async def _priority_risk_worker(self: Any, stop: asyncio.Event) -> None:
    while not stop.is_set():
        row = _claim_risk_work(self)
        if row is None:
            try:
                await asyncio.wait_for(stop.wait(), timeout=0.15)
            except asyncio.TimeoutError:
                continue
            return
        signature = str(row["signature"])
        swap = NormalizedSwap(
            signature=signature,
            slot=0,
            observed_at=datetime.fromisoformat(str(row["observed_at"])),
            received_at=datetime.fromisoformat(str(row["received_at"])),
            wallet=str(row["wallet"]),
            token_mint=str(row["token_mint"]),
            side="buy",
            token_amount=float(row["token_amount"]),
            native_amount_sol=float(row["token_amount"]) * float(row["wallet_price_sol"]),
            reference_price_sol=float(row["wallet_price_sol"]),
            source=str(row["source"]),
        )
        try:
            complete, manipulation, side_wallet = await self.discovery._risk_flags(swap)
        except asyncio.CancelledError:
            _finish_risk_work(self, signature, status="pending", error="cancelled")
            raise
        except Exception as exc:
            self._roi_risk_failures += 1
            _finish_risk_work(
                self,
                signature,
                status="pending",
                error=f"{type(exc).__name__}: risk enrichment failed",
                retry_after_seconds=RISK_RETRY_SECONDS,
            )
            continue
        if not complete:
            self._roi_risk_incomplete_retries += 1
            _finish_risk_work(
                self,
                signature,
                status="pending",
                error="risk evidence incomplete",
                retry_after_seconds=RISK_RETRY_SECONDS,
            )
            continue
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE wallet_discovery_forward_observations SET risk_complete=1, "
                "manipulation_flag=?, side_wallet_flag=? WHERE signature=?",
                (1 if manipulation else 0, 1 if side_wallet else 0, signature),
            )
        _finish_risk_work(self, signature, status="complete")
        self._roi_risk_completed += 1
        self.discovery.refresh_wallet_snapshot(swap.wallet)
        try:
            self.discovery.maybe_propose_adaptive_cohort()
        except Exception:
            pass


async def _priority_recover_all(self: Any) -> None:
    self._recovery_runs += 1
    self._recovery_task = asyncio.current_task()
    failed = False
    try:
        wallets = tuple(self._wallets)
        for index in range(0, len(wallets), RECOVERY_CONCURRENCY):
            batch = wallets[index : index + RECOVERY_CONCURRENCY]
            results = await asyncio.gather(
                *(self._recover_wallet(wallet) for wallet in batch),
                return_exceptions=True,
            )
            if any(value is False or isinstance(value, Exception) for value in results):
                failed = True
            await asyncio.sleep(0.05)
        if failed:
            self._recovery_failures += 1
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE wallet_realtime_runtime SET last_recovery_at=? WHERE id=1",
                (utcnow().isoformat(),),
            )
    finally:
        self._recovery_task = None


async def _priority_run(self: Any, stop: asyncio.Event) -> None:
    live_workers = [
        asyncio.create_task(
            _priority_receipt_worker(self, stop, lane="fresh_live"),
            name=f"wallet-realtime-live-hydrator:{index}",
        )
        for index in range(LIVE_WORKERS)
    ]
    backlog_workers = [
        asyncio.create_task(
            _priority_receipt_worker(self, stop, lane="backlog"),
            name=f"wallet-realtime-backlog-hydrator:{index}",
        )
        for index in range(BACKLOG_WORKERS)
    ]
    risk_workers = [
        asyncio.create_task(_priority_risk_worker(self, stop), name=f"wallet-realtime-risk:{index}")
        for index in range(RISK_WORKERS)
    ]
    try:
        while not stop.is_set():
            desired = await self._sync_wallets()
            if desired != self._wallets:
                await self._replace_provider_tasks(desired, stop)
            with self.store._lock, self.store.db:
                self.store.db.execute(
                    "UPDATE wallet_realtime_runtime SET last_cycle_at=?, last_error=? WHERE id=1",
                    (utcnow().isoformat(), self._last_error),
                )
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.refresh_seconds)
            except asyncio.TimeoutError:
                continue
    finally:
        self._planned_reconfigure = True
        for task in self._provider_tasks:
            task.cancel()
        tasks = [*self._provider_tasks, *live_workers, *backlog_workers, *risk_workers]
        for task in [*live_workers, *backlog_workers, *risk_workers]:
            task.cancel()
        if self._recovery_task is not None:
            self._recovery_task.cancel()
            tasks.append(self._recovery_task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._provider_tasks = []


def _priority_status(self: Any) -> dict[str, Any]:
    payload = _ORIGINAL_TRACKER_STATUS(self)
    now = utcnow()
    cutoff = (now - timedelta(seconds=FRESH_LIVE_WINDOW_SECONDS)).isoformat()
    with self.store._lock:
        fresh_pending = int(
            self.store.db.execute(
                "SELECT COUNT(*) FROM wallet_realtime_receipts WHERE status='pending' "
                "AND provider<>? AND received_at>=?",
                (RECOVERY_PROVIDER, cutoff),
            ).fetchone()[0]
        )
        stale_live_pending = int(
            self.store.db.execute(
                "SELECT COUNT(*) FROM wallet_realtime_receipts WHERE status='pending' "
                "AND provider<>? AND received_at<?",
                (RECOVERY_PROVIDER, cutoff),
            ).fetchone()[0]
        )
        recovery_pending = int(
            self.store.db.execute(
                "SELECT COUNT(*) FROM wallet_realtime_receipts WHERE status='pending' AND provider=?",
                (RECOVERY_PROVIDER,),
            ).fetchone()[0]
        )
        risk_rows = self.store.db.execute(
            "SELECT status, COUNT(*) AS n FROM wallet_realtime_risk_work GROUP BY status"
        ).fetchall()
        oldest_risk = self.store.db.execute(
            "SELECT MIN(enqueued_at) FROM wallet_realtime_risk_work WHERE status IN ('pending','processing')"
        ).fetchone()[0]
    risk_queue = {str(row["status"]): int(row["n"]) for row in risk_rows}
    oldest_risk_age_ms: float | None = None
    if oldest_risk:
        try:
            oldest_risk_age_ms = max(
                0.0,
                (now - datetime.fromisoformat(str(oldest_risk))).total_seconds() * 1000.0,
            )
        except (TypeError, ValueError):
            oldest_risk_age_ms = None

    fresh_p50 = _percentile(self._roi_fresh_live_delay_ms, 0.50)
    fresh_p95 = _percentile(self._roi_fresh_live_delay_ms, 0.95)
    fresh_total = max(1, int(self._roi_fresh_live_normalized))
    payload["priority_scheduler"] = {
        "installed": True,
        "fresh_live_window_seconds": FRESH_LIVE_WINDOW_SECONDS,
        "live_workers": LIVE_WORKERS,
        "backlog_workers": BACKLOG_WORKERS,
        "recovery_concurrency": RECOVERY_CONCURRENCY,
        "fresh_live_pending": fresh_pending,
        "stale_live_pending": stale_live_pending,
        "recovery_pending": recovery_pending,
        "fresh_live_preempts_recovery": True,
        "stale_live_moves_to_backlog_after_sla": True,
        "recovery_cannot_consume_live_worker_slots": True,
    }
    payload["fresh_live_quality"] = {
        "normalized_forward_swaps": int(self._roi_fresh_live_normalized),
        "copyable_forward_swaps": int(self._roi_fresh_live_copyable),
        "copyable_fraction": (
            self._roi_fresh_live_copyable / fresh_total if self._roi_fresh_live_normalized else 0.0
        ),
        "mark_processing_delay_p50_ms": fresh_p50,
        "mark_processing_delay_p95_ms": fresh_p95,
        "sla_seconds": REALTIME_MARK_DELAY_LIMIT_SECONDS,
        "sla_healthy": bool(fresh_p95 is None or fresh_p95 <= REALTIME_MARK_DELAY_LIMIT_SECONDS * 1000.0),
    }
    payload["backlog_quality"] = {
        "normalized_forward_swaps": int(self._roi_backlog_normalized),
        "mark_processing_delay_p50_ms": _percentile(self._roi_backlog_delay_ms, 0.50),
        "mark_processing_delay_p95_ms": _percentile(self._roi_backlog_delay_ms, 0.95),
        "has_promotion_latency_authority": False,
    }
    payload["risk_enrichment"] = {
        "workers": RISK_WORKERS,
        "queue": risk_queue,
        "oldest_pending_age_ms": oldest_risk_age_ms,
        "completed_session": int(self._roi_risk_completed),
        "incomplete_retries_session": int(self._roi_risk_incomplete_retries),
        "failures_session": int(self._roi_risk_failures),
        "pending_evidence_fails_closed": True,
        "pending_default_risk_is_not_a_malicious_classification": True,
    }
    return payload


def install_wallet_live_priority_repair() -> None:
    """Reserve realtime capacity for fresh wallet observations and isolate catch-up."""

    if not bool(getattr(RealtimeWalletTracker.__init__, "_roi_live_priority_repair", False)):
        setattr(_priority_init, "_roi_live_priority_repair", True)
        RealtimeWalletTracker.__init__ = _priority_init  # type: ignore[method-assign]
    if not bool(getattr(RealtimeWalletTracker.run, "_roi_live_priority_repair", False)):
        setattr(_priority_run, "_roi_live_priority_repair", True)
        RealtimeWalletTracker.run = _priority_run  # type: ignore[method-assign]
    if not bool(getattr(RealtimeWalletTracker.status, "_roi_live_priority_repair", False)):
        setattr(_priority_status, "_roi_live_priority_repair", True)
        RealtimeWalletTracker.status = _priority_status  # type: ignore[method-assign]
    if not bool(getattr(RealtimeWalletTracker._recover_all, "_roi_live_priority_repair", False)):
        setattr(_priority_recover_all, "_roi_live_priority_repair", True)
        RealtimeWalletTracker._recover_all = _priority_recover_all  # type: ignore[method-assign]


__all__ = [
    "BACKLOG_WORKERS",
    "FRESH_LIVE_WINDOW_SECONDS",
    "LIVE_WORKERS",
    "RECOVERY_PROVIDER",
    "RISK_WORKERS",
    "_claim_priority_receipt",
    "_claim_risk_work",
    "_ensure_priority_schema",
    "_sync_risk_work",
    "install_wallet_live_priority_repair",
]
