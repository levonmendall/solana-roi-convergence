from __future__ import annotations

import asyncio
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from .ingestion import NormalizedSwap
from .wallet_discovery import (
    MANIPULATION_BLOCKERS,
    SIDE_WALLET_BLOCKERS,
    ContinuousWalletDiscovery,
)
from .wallet_realtime_tracking_repair import RealtimeWalletTracker
from . import wallet_live_priority_repair as priority

RISK_PREWARM_SECONDS = 20.0
RISK_OLD_OBSERVATION_STATUS = "pre_repair_unverifiable"
RISK_POINT_IN_TIME_MISS_STATUS = "point_in_time_miss"
RECOVERY_PROVIDER = "bounded-realtime-recovery"

_ORIGINAL_TRACKER_INIT = RealtimeWalletTracker.__init__
_ORIGINAL_TRACKER_STATUS = RealtimeWalletTracker.status
_ORIGINAL_RECORD_QUICK_FORWARD_SWAP = RealtimeWalletTracker._record_quick_forward_swap
_ORIGINAL_ENQUEUE_RECEIPT = RealtimeWalletTracker._enqueue_receipt
_ORIGINAL_RECOVER_WALLET = RealtimeWalletTracker._recover_wallet


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _copyability_reasons(
    row: dict[str, Any],
    *,
    max_chase_fraction: float,
    max_observation_lag_seconds: float,
    max_mark_delay_seconds: float,
) -> tuple[str, ...]:
    if bool(row.get("copyable")):
        return ("copyable",)
    reasons: list[str] = []
    try:
        price = float(row["copyable_price_sol"]) if row.get("copyable_price_sol") is not None else None
    except (TypeError, ValueError):
        price = None
    if price is None or price <= 0.0:
        reasons.append("mark_unavailable")
    try:
        chase = float(row["chase_fraction"]) if row.get("chase_fraction") is not None else None
    except (TypeError, ValueError):
        chase = None
    if chase is None:
        reasons.append("chase_unavailable")
    elif chase > float(max_chase_fraction):
        reasons.append("chase_above_ceiling")
    try:
        lag_ms = float(row.get("observation_lag_ms") or 0.0)
    except (TypeError, ValueError):
        lag_ms = float("inf")
    if lag_ms > float(max_observation_lag_seconds) * 1000.0:
        reasons.append("observation_lag_above_limit")
    try:
        delay_ms = float(row.get("processing_delay_ms") or 0.0)
    except (TypeError, ValueError):
        delay_ms = float("inf")
    if delay_ms > float(max_mark_delay_seconds) * 1000.0:
        reasons.append("mark_delay_above_limit")
    return tuple(dict.fromkeys(reasons or ["other_copyability_rejection"]))


def _tracker_init(self: RealtimeWalletTracker, discovery: ContinuousWalletDiscovery) -> None:
    _ORIGINAL_TRACKER_INIT(self, discovery)
    boundary = utcnow()
    self._roi_wallet_evidence_repair_started_at = boundary
    discovery._roi_wallet_evidence_repair_started_at = boundary
    self._roi_copyability_rejection_counts = Counter()
    self._roi_anchor_pending_count = 0
    self._roi_live_anchor_established_count = 0
    self._roi_recovery_waiting_for_anchor_count = 0
    discovery._roi_risk_prewarm_locks = {}
    discovery._roi_risk_prewarm_next_at = {}
    discovery._roi_risk_prewarm_attempts = 0
    discovery._roi_risk_prewarm_errors = 0
    discovery._roi_risk_point_in_time_hits = 0
    discovery._roi_risk_point_in_time_misses = 0


async def _safe_begin_epoch(
    self: RealtimeWalletTracker,
    wallet: str,
    *,
    reason: str,
    reset_count: bool,
) -> None:
    now = utcnow()
    signature: str | None = None
    slot = 0
    anchor_error: str | None = None
    try:
        signature, slot = await self._head_signature(wallet)
    except Exception as exc:
        anchor_error = f"{type(exc).__name__}: realtime epoch anchor pending"
    anchor_ready = bool(signature and int(slot) > 0)
    if not anchor_ready and anchor_error is None:
        anchor_error = "realtime epoch anchor pending"

    with self.store._lock, self.store.db:
        self.store.db.execute(
            "DELETE FROM wallet_discovery_forward_observations WHERE wallet=?", (wallet,)
        )
        self.store.db.execute("DELETE FROM wallet_realtime_receipts WHERE wallet=?", (wallet,))
        existing = self.store.db.execute(
            "SELECT epoch_resets FROM wallet_realtime_state WHERE wallet=?", (wallet,)
        ).fetchone()
        resets = int(existing["epoch_resets"]) if existing is not None else 0
        if reset_count:
            resets += 1
        self.store.db.execute(
            "UPDATE wallet_discovery_candidates SET forward_started_at=?, last_signature=?, "
            "last_polled_at=?, last_error=? WHERE wallet=?",
            (
                now.isoformat(),
                signature if anchor_ready else None,
                now.isoformat(),
                None if anchor_ready else anchor_error,
                wallet,
            ),
        )
        self.store.db.execute(
            "INSERT INTO wallet_realtime_state("
            "wallet, epoch_started_at, anchor_signature, last_live_signature, last_live_slot, "
            "last_live_received_at, active, epoch_resets, last_error) "
            "VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?) "
            "ON CONFLICT(wallet) DO UPDATE SET epoch_started_at=excluded.epoch_started_at, "
            "anchor_signature=excluded.anchor_signature, last_live_signature=excluded.last_live_signature, "
            "last_live_slot=excluded.last_live_slot, last_live_received_at=excluded.last_live_received_at, "
            "active=1, epoch_resets=excluded.epoch_resets, last_error=excluded.last_error",
            (
                wallet,
                now.isoformat(),
                signature if anchor_ready else None,
                signature if anchor_ready else None,
                int(slot) if anchor_ready else 0,
                now.isoformat(),
                resets,
                None if anchor_ready else anchor_error,
            ),
        )

    if reset_count:
        self._epoch_resets += 1
    if not anchor_ready:
        self._last_error = anchor_error
        self._roi_anchor_pending_count += 1
    self.store.append(
        "wallet_realtime_epoch_started",
        now.isoformat(),
        {
            "wallet": wallet,
            "reason": reason,
            "anchor_signature": signature if anchor_ready else None,
            "anchor_slot": int(slot) if anchor_ready else 0,
            "anchor_pending": not anchor_ready,
            "first_live_receipt_is_boundary_only_when_anchor_pending": True,
            "historical_or_old_forward_evidence_has_promotion_authority": False,
            "paper_only": True,
        },
    )


def _anchor_or_enqueue(
    self: RealtimeWalletTracker,
    *,
    wallet: str,
    signature: str,
    slot: int,
    received_at: datetime,
    provider: str,
) -> bool:
    with self.store._lock:
        state = self.store.db.execute(
            "SELECT active, anchor_signature FROM wallet_realtime_state WHERE wallet=?", (wallet,)
        ).fetchone()
    if state is not None and bool(state["active"]) and not str(state["anchor_signature"] or ""):
        if provider == RECOVERY_PROVIDER:
            return False
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE wallet_realtime_state SET anchor_signature=?, last_live_signature=?, "
                "last_live_slot=?, last_live_received_at=?, last_error=NULL WHERE wallet=?",
                (signature, int(slot), received_at.isoformat(), wallet),
            )
            self.store.db.execute(
                "UPDATE wallet_discovery_candidates SET last_signature=?, last_polled_at=?, "
                "last_error=NULL WHERE wallet=?",
                (signature, received_at.isoformat(), wallet),
            )
        self._notifications += 1
        self._last_error = None
        self._roi_live_anchor_established_count += 1
        self.store.append(
            "wallet_realtime_live_anchor_established",
            received_at.isoformat(),
            {
                "wallet": wallet,
                "signature": signature,
                "slot": int(slot),
                "provider": provider,
                "boundary_receipt_has_promotion_authority": False,
                "paper_only": True,
            },
        )
        return False
    return _ORIGINAL_ENQUEUE_RECEIPT(
        self,
        wallet=wallet,
        signature=signature,
        slot=slot,
        received_at=received_at,
        provider=provider,
    )


async def _recover_wallet_anchor_safe(self: RealtimeWalletTracker, wallet: str) -> bool:
    with self.store._lock:
        state = self.store.db.execute(
            "SELECT active, anchor_signature FROM wallet_realtime_state WHERE wallet=?", (wallet,)
        ).fetchone()
    if state is not None and bool(state["active"]) and not str(state["anchor_signature"] or ""):
        self._roi_recovery_waiting_for_anchor_count += 1
        return True
    return await _ORIGINAL_RECOVER_WALLET(self, wallet)


async def _prewarm_token_risk(discovery: ContinuousWalletDiscovery, swap: NormalizedSwap) -> None:
    locks = discovery._roi_risk_prewarm_locks
    lock = locks.setdefault(swap.token_mint, asyncio.Lock())
    async with lock:
        now_mono = time.monotonic()
        if float(discovery._roi_risk_prewarm_next_at.get(swap.token_mint, 0.0)) > now_mono:
            return
        discovery._roi_risk_prewarm_next_at[swap.token_mint] = now_mono + RISK_PREWARM_SECONDS
        discovery._roi_risk_prewarm_attempts += 1
        collectors = discovery.risk_collectors
        if collectors is None:
            return
        actual_at = utcnow()
        current_swap = swap if 0.0 <= (actual_at - swap.received_at).total_seconds() <= 60.0 else None
        try:
            candidate = getattr(collectors, "refresh_candidate", None)
            coverage = getattr(collectors, "refresh_coverage", None)
            if callable(candidate):
                await candidate(swap.token_mint, actual_at, current_swap=current_swap)
                if callable(coverage):
                    await coverage(swap.token_mint, actual_at, current_swap=current_swap)
            else:
                await collectors.refresh(swap.token_mint, actual_at, current_swap=current_swap)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            discovery._roi_risk_prewarm_errors += 1
            discovery.store.append(
                "wallet_risk_prewarm_error",
                actual_at.isoformat(),
                {"token_mint": swap.token_mint, "error_type": type(exc).__name__, "paper_only": True},
            )


async def _point_in_time_risk_flags(
    self: ContinuousWalletDiscovery,
    swap: NormalizedSwap,
) -> tuple[bool, bool, bool]:
    if swap.side != "buy":
        return True, False, False
    boundary = getattr(self, "_roi_wallet_evidence_repair_started_at", None)
    if isinstance(boundary, datetime) and swap.received_at < boundary:
        self._roi_risk_point_in_time_misses += 1
        return False, True, True
    try:
        entity_id = self.entity_resolver.entity_id_for(
            swap.wallet, fallback_entity_id=None, as_of=swap.received_at
        )
        snapshot = await self.risk.snapshot(
            swap.token_mint,
            swap.received_at,
            scout_wallet=swap.wallet,
            scout_entity_id=entity_id,
        )
        component = self.entity_resolver.component(swap.wallet, as_of=swap.received_at)
    except Exception:
        snapshot = None
        component = {swap.wallet}
    if snapshot is not None:
        blockers = set(snapshot.blockers)
        self._roi_risk_point_in_time_hits += 1
        return (
            True,
            bool(blockers & MANIPULATION_BLOCKERS),
            bool(blockers & SIDE_WALLET_BLOCKERS) or len(component) > 1,
        )
    self._roi_risk_point_in_time_misses += 1
    await _prewarm_token_risk(self, swap)
    return False, True, True


async def _risk_worker_no_lookahead(self: RealtimeWalletTracker, stop: asyncio.Event) -> None:
    while not stop.is_set():
        priority._sync_risk_work(self)
        row = priority._claim_risk_work(self)
        if row is None:
            try:
                await asyncio.wait_for(stop.wait(), timeout=0.10)
            except asyncio.TimeoutError:
                continue
            return
        signature = str(row["signature"])
        try:
            swap = priority._risk_swap(row)
            boundary = self._roi_wallet_evidence_repair_started_at
            if swap.received_at < boundary:
                priority._finish_risk_work(
                    self,
                    signature,
                    status=RISK_OLD_OBSERVATION_STATUS,
                    error="pre-repair delayed risk evidence is not point-in-time verifiable",
                )
                continue
            complete, manipulation, side_wallet = await self.discovery._risk_flags(swap)
            if not complete:
                priority._finish_risk_work(
                    self,
                    signature,
                    status=RISK_POINT_IN_TIME_MISS_STATUS,
                    error="complete risk bundle unavailable at observation time; future evidence prewarmed",
                )
                continue
            with self.store._lock, self.store.db:
                self.store.db.execute(
                    "UPDATE wallet_discovery_forward_observations SET risk_complete=1, "
                    "manipulation_flag=?, side_wallet_flag=? WHERE signature=?",
                    (1 if manipulation else 0, 1 if side_wallet else 0, signature),
                )
            priority._finish_risk_work(self, signature, status="complete")
            self._roi_risk_completed += 1
            self.discovery.refresh_wallet_snapshot(swap.wallet)
            try:
                self.discovery.maybe_propose_adaptive_cohort()
            except Exception:
                pass
        except asyncio.CancelledError:
            priority._finish_risk_work(self, signature, status="pending", error="cancelled")
            raise
        except Exception as exc:
            self._roi_risk_failures += 1
            priority._finish_risk_work(
                self,
                signature,
                status=RISK_POINT_IN_TIME_MISS_STATUS,
                error=f"{type(exc).__name__}: risk evaluation failed closed",
            )


async def _record_with_copyability_diagnostics(
    self: RealtimeWalletTracker,
    swap: NormalizedSwap,
) -> bool:
    inserted = await _ORIGINAL_RECORD_QUICK_FORWARD_SWAP(self, swap)
    if not inserted:
        return inserted
    with self.store._lock:
        row = self.store.db.execute(
            "SELECT copyable, copyable_price_sol, chase_fraction, observation_lag_ms, "
            "processing_delay_ms FROM wallet_discovery_forward_observations WHERE signature=?",
            (swap.signature,),
        ).fetchone()
    if row is not None:
        self._roi_copyability_rejection_counts.update(
            _copyability_reasons(
                dict(row),
                max_chase_fraction=self.discovery.policy.max_chase_fraction,
                max_observation_lag_seconds=self.discovery.policy.max_observation_lag_seconds,
                max_mark_delay_seconds=20.0,
            )
        )
    return inserted


def _status_with_evidence_repair(self: RealtimeWalletTracker) -> dict[str, Any]:
    payload = _ORIGINAL_TRACKER_STATUS(self)
    discovery = self.discovery
    payload["copyability_diagnostics"] = {
        "session_counts": dict(sorted(self._roi_copyability_rejection_counts.items())),
        "max_chase_fraction_unchanged": float(discovery.policy.max_chase_fraction),
        "max_observation_lag_seconds_unchanged": float(discovery.policy.max_observation_lag_seconds),
        "max_mark_delay_seconds_unchanged": 20.0,
        "classification_only_no_threshold_change": True,
    }
    payload["risk_evidence_pipeline"] = {
        "point_in_time_only": True,
        "retroactive_risk_completion_allowed": False,
        "prewarm_uses_actual_collection_time": True,
        "prewarm_min_interval_seconds_per_token": RISK_PREWARM_SECONDS,
        "prewarm_attempts": int(discovery._roi_risk_prewarm_attempts),
        "prewarm_errors": int(discovery._roi_risk_prewarm_errors),
        "point_in_time_hits": int(discovery._roi_risk_point_in_time_hits),
        "point_in_time_misses": int(discovery._roi_risk_point_in_time_misses),
        "full_six_dimension_refresh_per_signature_removed": True,
        "missing_bundle_prepares_future_observations_only": True,
    }
    payload["epoch_anchor_safety"] = {
        "null_anchor_observations_allowed": False,
        "first_live_receipt_can_establish_boundary": True,
        "boundary_receipt_has_promotion_authority": False,
        "anchor_pending_epochs": int(self._roi_anchor_pending_count),
        "live_anchors_established": int(self._roi_live_anchor_established_count),
        "recovery_waits_for_live_anchor": int(self._roi_recovery_waiting_for_anchor_count),
    }
    return payload


def install_wallet_evidence_rpc_repair() -> None:
    if bool(getattr(RealtimeWalletTracker.status, "_roi_wallet_evidence_rpc_repair", False)):
        return
    priority._priority_risk_worker = _risk_worker_no_lookahead
    ContinuousWalletDiscovery._risk_flags = _point_in_time_risk_flags  # type: ignore[method-assign]
    RealtimeWalletTracker.__init__ = _tracker_init  # type: ignore[method-assign]
    RealtimeWalletTracker._begin_epoch = _safe_begin_epoch  # type: ignore[method-assign]
    RealtimeWalletTracker._enqueue_receipt = _anchor_or_enqueue  # type: ignore[method-assign]
    RealtimeWalletTracker._recover_wallet = _recover_wallet_anchor_safe  # type: ignore[method-assign]
    RealtimeWalletTracker._record_quick_forward_swap = _record_with_copyability_diagnostics  # type: ignore[method-assign]
    setattr(_status_with_evidence_repair, "_roi_wallet_evidence_rpc_repair", True)
    RealtimeWalletTracker.status = _status_with_evidence_repair  # type: ignore[method-assign]


__all__ = [
    "_copyability_reasons",
    "_point_in_time_risk_flags",
    "install_wallet_evidence_rpc_repair",
]
