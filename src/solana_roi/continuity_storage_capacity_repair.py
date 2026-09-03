from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Any

from . import direct_solana as direct_solana_module
from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from . import poll_watermark_repair as watermark
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget
from .solana_rpc import RpcEndpoint, SolanaRpcPool


# Routine standby polling is still exactly one observation per target every four
# seconds. Split those ten reads evenly across the available public providers and
# phase them across the same four-second interval so startup/cycle alignment cannot
# create a synchronized ten-request burst on one preferred endpoint.
ROUTINE_POLL_PHASE_SPREAD_SECONDS = live_poll.POLL_INTERVAL_SECONDS

# These are operational retention bounds only. Canonical append-only events,
# normalized swaps, risk evidence, coverage evidence, cohort evidence and raw
# receipt acquisition are never pruned here.
TERMINAL_QUEUE_RETENTION_SECONDS = 3600.0
HYDRATION_METRIC_RETENTION_SECONDS = 6.0 * 3600.0
MAINTENANCE_IDLE_SECONDS = 60.0
MAINTENANCE_DRAIN_SECONDS = 0.10
MAINTENANCE_BATCH_ROWS = 5000
WAL_CHECKPOINT_INTERVAL_SECONDS = 300.0

_ORIGINAL_POLL_TARGET = live_poll._poll_target
_ORIGINAL_CLOSE_POLL_RPC = live_poll._close_poll_rpc
_ORIGINAL_DIRECT_RUN = DirectSolanaIngestionPlane.run
_ORIGINAL_DIRECT_STATUS = DirectSolanaIngestionPlane.status


def _target_key(target: WatchTarget) -> str:
    return live_poll._poll_target_key(target)


def _routine_endpoints(self: Any) -> tuple[RpcEndpoint, ...]:
    endpoints = tuple(getattr(getattr(self, "rpc", None), "endpoints", ()) or ())
    if endpoints:
        return endpoints
    return tuple(getattr(self, "endpoints", ()) or ())


def _target_index(self: Any, target: WatchTarget) -> int:
    key = _target_key(target)
    for index, candidate in enumerate(tuple(self.watch_targets)):
        if _target_key(candidate) == key:
            return index
    # Compatibility callers can supply an equivalent target that is not currently
    # in watch_targets. A stable byte sum keeps routing deterministic without using
    # Python's per-process randomized hash().
    return sum(key.encode("utf-8"))


def _assigned_endpoint(self: Any, target: WatchTarget) -> RpcEndpoint:
    endpoints = _routine_endpoints(self)
    if not endpoints:
        raise RuntimeError("routine live-poll provider sharding requires at least one RPC endpoint")
    return endpoints[_target_index(self, target) % len(endpoints)]


def _target_phase_seconds(self: Any, target: WatchTarget) -> float:
    targets = tuple(self.watch_targets)
    if not targets:
        return 0.0
    index = _target_index(self, target) % len(targets)
    return (float(index) / float(len(targets))) * ROUTINE_POLL_PHASE_SPREAD_SECONDS


def _routine_poll_pools(self: Any) -> dict[str, SolanaRpcPool]:
    pools = getattr(self, "_roi_routine_poll_pools", None)
    if isinstance(pools, dict):
        return pools
    pools = {}
    for endpoint in _routine_endpoints(self):
        pools[endpoint.name] = SolanaRpcPool(
            (endpoint,),
            timeout_seconds=2.5,
            hedge_delay_seconds=0.15,
        )
    setattr(self, "_roi_routine_poll_pools", pools)
    return pools


def _routine_poll_pool(self: Any, target: WatchTarget) -> SolanaRpcPool:
    endpoint = _assigned_endpoint(self, target)
    pool = _routine_poll_pools(self).get(endpoint.name)
    if not isinstance(pool, SolanaRpcPool):
        raise RuntimeError(f"routine poll pool missing assigned endpoint {endpoint.name}")
    return pool


async def _sharded_slot_poll_page(
    self: Any,
    target: WatchTarget,
    *,
    before: str | None = None,
    min_context_slot: int | None = None,
    limit: int | None = None,
) -> tuple[list[dict[str, Any]], str, float]:
    """Run routine confirmed polling on exactly one deterministic provider.

    This duplicates the established watermark page contract. Only provider
    selection changes: routine standby work no longer fails over from its assigned
    public provider into the other routine provider and therefore cannot synchronize
    their 429 cooldowns. Actual real-gap recovery remains on the existing dedicated,
    hedged recovery pool and keeps the unchanged 12-second/3x1000 fail-closed bound.
    """

    page_limit = live_poll.POLL_LIMIT if limit is None else int(limit)
    config: dict[str, Any] = {
        "commitment": "confirmed",
        "limit": max(1, min(1000, page_limit)),
    }
    if before:
        config["before"] = before
    if min_context_slot is not None and int(min_context_slot) > 0:
        config["minContextSlot"] = int(min_context_slot)
    result, provider, latency = await _routine_poll_pool(self, target).call_with_meta(
        "getSignaturesForAddress",
        [target.address, config],
        hedge=False,
    )
    rows = [row for row in result if isinstance(row, dict)] if isinstance(result, list) else []
    return rows, provider, latency


async def _phase_staggered_poll_target(self: Any, target: WatchTarget, stop: asyncio.Event) -> None:
    phase = _target_phase_seconds(self, target)
    if phase > 0.0:
        try:
            await asyncio.wait_for(stop.wait(), timeout=phase)
            return
        except asyncio.TimeoutError:
            pass
    await _ORIGINAL_POLL_TARGET(self, target, stop)


async def _close_poll_rpc_with_shards(self: Any) -> None:
    pools = getattr(self, "_roi_routine_poll_pools", None)
    clients: list[Any] = []
    if isinstance(pools, dict):
        for pool in pools.values():
            if isinstance(pool, SolanaRpcPool):
                clients.extend(list(getattr(pool, "_clients", {}).values()))
    if clients:
        await asyncio.gather(*(client.aclose() for client in clients), return_exceptions=True)
    await _ORIGINAL_CLOSE_POLL_RPC(self)


def _ensure_maintenance_state(self: Any) -> None:
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "CREATE TABLE IF NOT EXISTS direct_solana_storage_maintenance ("
            "id INTEGER PRIMARY KEY CHECK(id=1), "
            "queue_rows_pruned INTEGER NOT NULL DEFAULT 0, "
            "metric_rows_pruned INTEGER NOT NULL DEFAULT 0, "
            "last_maintenance_at TEXT, last_checkpoint_at TEXT, "
            "last_checkpoint_busy INTEGER, last_checkpoint_log INTEGER, "
            "last_checkpointed INTEGER, last_error TEXT)"
        )
        self.store.db.execute(
            "INSERT OR IGNORE INTO direct_solana_storage_maintenance(id) VALUES (1)"
        )


def _prune_operational_rows_once(self: Any) -> tuple[int, int]:
    now = direct_solana_module.utcnow()
    queue_cutoff = (now - timedelta(seconds=TERMINAL_QUEUE_RETENTION_SECONDS)).isoformat()
    metric_cutoff = (now - timedelta(seconds=HYDRATION_METRIC_RETENTION_SECONDS)).isoformat()
    _ensure_maintenance_state(self)
    with self.store._lock, self.store.db:
        queue_cur = self.store.db.execute(
            "DELETE FROM direct_solana_hydration_queue WHERE signature IN ("
            "SELECT signature FROM direct_solana_hydration_queue "
            "WHERE status IN ('complete','failed') AND updated_at<? "
            "ORDER BY updated_at, signature LIMIT ?) ",
            (queue_cutoff, MAINTENANCE_BATCH_ROWS),
        )
        metric_cur = self.store.db.execute(
            "DELETE FROM direct_solana_hydration_metrics WHERE signature IN ("
            "SELECT signature FROM direct_solana_hydration_metrics "
            "WHERE historical_recovery=0 AND hydrated_at<? "
            "ORDER BY hydrated_at, signature LIMIT ?) ",
            (metric_cutoff, MAINTENANCE_BATCH_ROWS),
        )
        queue_rows = int(queue_cur.rowcount or 0)
        metric_rows = int(metric_cur.rowcount or 0)
        self.store.db.execute(
            "UPDATE direct_solana_storage_maintenance SET "
            "queue_rows_pruned=queue_rows_pruned+?, metric_rows_pruned=metric_rows_pruned+?, "
            "last_maintenance_at=?, last_error=NULL WHERE id=1",
            (queue_rows, metric_rows, now.isoformat()),
        )
    return queue_rows, metric_rows


def _checkpoint_wal(self: Any) -> tuple[int, int, int] | None:
    _ensure_maintenance_state(self)
    now = direct_solana_module.utcnow()
    try:
        # Do not enter the sqlite connection context here: wal_checkpoint must run
        # outside an active transaction. The process RLock still serializes access.
        with self.store._lock:
            row = self.store.db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if row is None:
            result = (0, 0, 0)
        else:
            result = (int(row[0]), int(row[1]), int(row[2]))
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE direct_solana_storage_maintenance SET last_checkpoint_at=?, "
                "last_checkpoint_busy=?, last_checkpoint_log=?, last_checkpointed=?, last_error=NULL WHERE id=1",
                (now.isoformat(), result[0], result[1], result[2]),
            )
        return result
    except Exception as exc:
        try:
            with self.store._lock, self.store.db:
                self.store.db.execute(
                    "UPDATE direct_solana_storage_maintenance SET last_error=? WHERE id=1",
                    (f"{type(exc).__name__}: WAL checkpoint failed",),
                )
        except Exception:
            pass
        return None


def _maintenance_snapshot(self: Any) -> dict[str, Any]:
    _ensure_maintenance_state(self)
    path = Path(getattr(self.store, "path", ""))
    with self.store._lock:
        state = self.store.db.execute(
            "SELECT queue_rows_pruned, metric_rows_pruned, last_maintenance_at, "
            "last_checkpoint_at, last_checkpoint_busy, last_checkpoint_log, "
            "last_checkpointed, last_error FROM direct_solana_storage_maintenance WHERE id=1"
        ).fetchone()
        queue_rows = self.store.db.execute(
            "SELECT status, COUNT(*) AS n FROM direct_solana_hydration_queue GROUP BY status"
        ).fetchall()
        page_size_row = self.store.db.execute("PRAGMA page_size").fetchone()
        page_count_row = self.store.db.execute("PRAGMA page_count").fetchone()
        freelist_row = self.store.db.execute("PRAGMA freelist_count").fetchone()
    page_size = int(page_size_row[0]) if page_size_row is not None else 0
    page_count = int(page_count_row[0]) if page_count_row is not None else 0
    freelist = int(freelist_row[0]) if freelist_row is not None else 0
    queue = {str(row["status"]): int(row["n"]) for row in queue_rows}

    wal_bytes: int | None = None
    filesystem_free_bytes: int | None = None
    try:
        wal_path = Path(f"{path}-wal")
        wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0
    except OSError:
        wal_bytes = None
    try:
        stat = os.statvfs(str(path.parent or Path(".")))
        filesystem_free_bytes = int(stat.f_bavail) * int(stat.f_frsize)
    except OSError:
        filesystem_free_bytes = None

    state_dict = dict(state) if state is not None else {}
    return {
        "installed": True,
        "terminal_queue_retention_seconds": TERMINAL_QUEUE_RETENTION_SECONDS,
        "nonhistorical_hydration_metric_retention_seconds": HYDRATION_METRIC_RETENTION_SECONDS,
        "maintenance_batch_rows": MAINTENANCE_BATCH_ROWS,
        "terminal_queue_rows_pruned_total": int(state_dict.get("queue_rows_pruned") or 0),
        "nonhistorical_metric_rows_pruned_total": int(state_dict.get("metric_rows_pruned") or 0),
        "last_maintenance_at": state_dict.get("last_maintenance_at"),
        "last_checkpoint_at": state_dict.get("last_checkpoint_at"),
        "last_checkpoint_result": (
            {
                "busy": state_dict.get("last_checkpoint_busy"),
                "log_frames": state_dict.get("last_checkpoint_log"),
                "checkpointed_frames": state_dict.get("last_checkpointed"),
            }
            if state_dict.get("last_checkpoint_at")
            else None
        ),
        "last_error": state_dict.get("last_error"),
        "hydration_queue_current": queue,
        "sqlite_allocated_bytes": page_size * page_count,
        "sqlite_reusable_freelist_bytes": page_size * freelist,
        "sqlite_wal_bytes": wal_bytes,
        "filesystem_free_bytes": filesystem_free_bytes,
        "automatic_vacuum_enabled": False,
        "wal_checkpoint_truncate_enabled": True,
        "canonical_evidence_pruned": False,
        "historical_recovery_metrics_pruned": False,
        "raw_receipt_retention_or_scope_changed": False,
        "pending_or_processing_queue_rows_pruned": False,
        "certification_thresholds_unchanged": True,
        "paper_only_authority_unchanged": True,
    }


async def _storage_maintenance_worker(self: Any, stop: asyncio.Event) -> None:
    next_checkpoint = 0.0
    while not stop.is_set():
        queue_rows = 0
        metric_rows = 0
        try:
            queue_rows, metric_rows = await asyncio.to_thread(_prune_operational_rows_once, self)
            now_mono = time.monotonic()
            if now_mono >= next_checkpoint:
                await asyncio.to_thread(_checkpoint_wal, self)
                next_checkpoint = now_mono + WAL_CHECKPOINT_INTERVAL_SECONDS
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            try:
                await asyncio.to_thread(_ensure_maintenance_state, self)
                with self.store._lock, self.store.db:
                    self.store.db.execute(
                        "UPDATE direct_solana_storage_maintenance SET last_error=? WHERE id=1",
                        (f"{type(exc).__name__}: storage maintenance failed",),
                    )
            except Exception:
                pass

        delay = MAINTENANCE_DRAIN_SECONDS if (queue_rows or metric_rows) else MAINTENANCE_IDLE_SECONDS
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            continue


def _run_with_storage_maintenance(original: Callable[[Any, asyncio.Event], Any]) -> Callable[[Any, asyncio.Event], Any]:
    async def run(self: Any, stop: asyncio.Event) -> None:
        maintenance = asyncio.create_task(
            _storage_maintenance_worker(self, stop),
            name="direct-solana-storage-maintenance",
        )
        try:
            await original(self, stop)
        finally:
            maintenance.cancel()
            await asyncio.gather(maintenance, return_exceptions=True)

    try:
        run.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(run, "_roi_storage_capacity_maintenance", True)
    return run


def _status_with_capacity_repair(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        endpoints = _routine_endpoints(self)
        targets = tuple(self.watch_targets)
        assignments: dict[str, str] = {}
        counts: dict[str, int] = {endpoint.name: 0 for endpoint in endpoints}
        for target in targets:
            endpoint = _assigned_endpoint(self, target)
            key = _target_key(target)
            assignments[key] = endpoint.name
            counts[endpoint.name] = int(counts.get(endpoint.name, 0) or 0) + 1
        pools = getattr(self, "_roi_routine_poll_pools", None)
        pool_status = {
            name: pool.status()
            for name, pool in (pools.items() if isinstance(pools, dict) else [])
            if isinstance(pool, SolanaRpcPool)
        }
        poll = payload.setdefault("live_poll_redundancy", {})
        if isinstance(poll, dict):
            poll["routine_provider_sharding"] = {
                "installed": True,
                "assignment_policy": "watch-target-index-mod-provider-count",
                "provider_target_counts": counts,
                "target_provider_assignments": assignments,
                "startup_phase_spread_seconds": ROUTINE_POLL_PHASE_SPREAD_SECONDS,
                "routine_poll_hedging": False,
                "routine_cross_provider_fallback": False,
                "urgent_real_gap_recovery_pool_unchanged": True,
                "urgent_real_gap_recovery_hedging_unchanged": True,
                "poll_interval_seconds_unchanged": live_poll.POLL_INTERVAL_SECONDS,
                "recoverability_lease_seconds_unchanged": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
                "recovery_page_count_unchanged": live_poll.POLL_CURSOR_MAX_PAGES,
                "recovery_page_size_unchanged": live_poll.POLL_LIMIT,
                "routine_provider_pools": pool_status,
            }
        payload["storage_capacity_maintenance"] = _maintenance_snapshot(self)
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "routine_live_poll_provider_sharded": True,
                    "routine_live_poll_cross_provider_fallback": False,
                    "routine_live_poll_phase_staggered": True,
                    "urgent_real_gap_recovery_behavior_unchanged": True,
                    "live_poll_recoverability_lease_seconds": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
                    "live_poll_hard_delta_bound_unchanged": True,
                    "terminal_hydration_operational_retention_enabled": True,
                    "canonical_evidence_retention_unchanged": True,
                    "raw_receipt_scope_reduced": False,
                    "certification_thresholds_unchanged": True,
                    "paper_only_authority_unchanged": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_continuity_storage_capacity", True)
    return status


def install_continuity_storage_capacity_repair() -> None:
    """Protect public RPC capacity and bound operational SQLite growth."""

    if not bool(getattr(live_poll._poll_target, "_roi_routine_provider_sharding", False)):
        setattr(_phase_staggered_poll_target, "_roi_routine_provider_sharding", True)
        live_poll._poll_target = _phase_staggered_poll_target  # type: ignore[assignment]

    watermark._slot_poll_page = _sharded_slot_poll_page  # type: ignore[assignment]
    live_poll._poll_page = _sharded_slot_poll_page  # type: ignore[assignment]

    if not bool(getattr(live_poll._close_poll_rpc, "_roi_routine_provider_sharding", False)):
        setattr(_close_poll_rpc_with_shards, "_roi_routine_provider_sharding", True)
        live_poll._close_poll_rpc = _close_poll_rpc_with_shards  # type: ignore[assignment]

    current_run = DirectSolanaIngestionPlane.run
    if not bool(getattr(current_run, "_roi_storage_capacity_maintenance", False)):
        DirectSolanaIngestionPlane.run = _run_with_storage_maintenance(current_run)  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_continuity_storage_capacity", False)):
        DirectSolanaIngestionPlane.status = _status_with_capacity_repair(current_status)  # type: ignore[method-assign]


__all__ = [
    "HYDRATION_METRIC_RETENTION_SECONDS",
    "MAINTENANCE_BATCH_ROWS",
    "ROUTINE_POLL_PHASE_SPREAD_SECONDS",
    "TERMINAL_QUEUE_RETENTION_SECONDS",
    "_assigned_endpoint",
    "_prune_operational_rows_once",
    "_sharded_slot_poll_page",
    "_target_phase_seconds",
    "install_continuity_storage_capacity_repair",
]
