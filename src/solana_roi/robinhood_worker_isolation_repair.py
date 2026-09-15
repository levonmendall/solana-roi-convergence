from __future__ import annotations

import asyncio
import copy
import os
import sqlite3
import threading
import time
from contextlib import suppress
from pathlib import Path
from typing import Any, Callable

from .observation_store import ObservationEventStore
from .robinhood_chain_paper import RobinhoodChainPaperPlane


REPAIR_VERSION = "robinhood-dedicated-worker-isolation-v3-page-cache-bounded"
STATUS_PUBLISH_SECONDS = 1.0
STATUS_STALE_SECONDS = 5.0
PROOF_PUBLISH_SECONDS = 5.0
THREAD_JOIN_TIMEOUT_SECONDS = 3.0
THREAD_NAME = "robinhood-chain-paper-isolated"

_PROOF_GENERATION_TABLE = "robinhood_proof_input_generation"
_PROOF_INPUT_TABLES = (
    "v51_robinhood_candidate_ledger",
    "robinhood_paper_trials",
    "robinhood_paper_outcomes",
    "robinhood_v5_trial_context",
)

_STATUS_LOCK = threading.Lock()
_STATUS_SNAPSHOT: dict[str, Any] | None = None
_STATUS_PUBLISHED_MONOTONIC: float | None = None
_PROOF_LOCK = threading.Lock()
_PROOF_SNAPSHOT: dict[str, Any] | None = None
_PROOF_PUBLISHED_MONOTONIC: float | None = None
_PROOF_INPUT_GENERATION: int | None = None
_WORKER_THREAD: threading.Thread | None = None
_WORKER_STOP: threading.Event | None = None
_BASE_STATUS: Callable[[], dict[str, Any]] | None = None
# Kept for compatibility with the later v5.1 installer. It may wrap this reference,
# but the latency-critical publisher intentionally never calls it after installation.
_ORIGINAL_STATUS: Callable[[], dict[str, Any]] | None = None
_INSTALLED = False


def _runtime_install_module() -> Any:
    from . import robinhood_runtime_install as module
    return module


def _dedicated_store_path(canonical_store: Any) -> Path:
    canonical = Path(canonical_store.path).expanduser().resolve()
    configured = os.getenv("ROBINHOOD_CHAIN_STORE_PATH", "").strip()
    if configured:
        candidate = Path(configured).expanduser()
        if not candidate.is_absolute():
            candidate = canonical.parent / candidate
    else:
        suffix = canonical.suffix or ".sqlite3"
        candidate = canonical.with_name(f"{canonical.stem}-robinhood-chain{suffix}")
    candidate = candidate.resolve()
    if candidate == canonical:
        raise RuntimeError("Robinhood dedicated store must not be the canonical Solana SQLite file")
    return candidate


def _worker_isolation_metadata(*, store_path: str | None = None) -> dict[str, Any]:
    thread = _WORKER_THREAD
    with _PROOF_LOCK:
        proof_at = _PROOF_PUBLISHED_MONOTONIC
        proof_generation = _PROOF_INPUT_GENERATION
    proof_age = max(0.0, time.monotonic() - proof_at) if proof_at is not None else None
    return {
        "repair_version": REPAIR_VERSION,
        "worker_topology": "dedicated_os_thread_with_private_asyncio_loop",
        "worker_thread_name": THREAD_NAME,
        "worker_thread_alive": bool(thread is not None and thread.is_alive()),
        "dedicated_sqlite_store": True,
        "dedicated_store_path": store_path,
        "canonical_store_shared_for_robinhood_writes": False,
        "canonical_store_used_only_for_one_time_cursor_seed": True,
        "status_served_from_nonblocking_cache": True,
        "fast_status_uses_in_memory_runtime_state_only": True,
        "fast_status_history_scaled_sqlite_reads": False,
        "proof_refresh_uses_separate_sqlite_connection": True,
        "proof_refresh_runs_in_worker_threadpool": True,
        "proof_refresh_input_generation_gated": True,
        "raw_swap_writes_trigger_deep_proof": False,
        "proof_input_generation": proof_generation,
        "proof_publish_seconds": PROOF_PUBLISH_SECONDS,
        "proof_cache_age_seconds": proof_age,
        "proof_blocks_live_frontier": False,
        "source_history_deleted": False,
        "retention_changed": False,
        "uvicorn_event_loop_runs_robinhood_chain_worker": False,
        "paper_decision_gate_changed": False,
        "strategy_thresholds_changed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def _current_proof_snapshot() -> dict[str, Any] | None:
    with _PROOF_LOCK:
        return copy.deepcopy(_PROOF_SNAPSHOT)


def _publish_proof_snapshot(payload: dict[str, Any]) -> None:
    global _PROOF_SNAPSHOT, _PROOF_PUBLISHED_MONOTONIC, _PROOF_INPUT_GENERATION
    with _PROOF_LOCK:
        _PROOF_SNAPSHOT = copy.deepcopy(payload)
        _PROOF_PUBLISHED_MONOTONIC = time.monotonic()
        generation = payload.get("proof_input_generation")
        if isinstance(generation, int):
            _PROOF_INPUT_GENERATION = generation


def _publish_snapshot(payload: dict[str, Any], *, store_path: str | None) -> None:
    global _STATUS_SNAPSHOT, _STATUS_PUBLISHED_MONOTONIC
    published = copy.deepcopy(payload)
    proof = _current_proof_snapshot()
    if proof is not None:
        published["v51_proof"] = proof
    published["worker_isolation"] = _worker_isolation_metadata(store_path=store_path)
    with _STATUS_LOCK:
        _STATUS_SNAPSHOT = published
        _STATUS_PUBLISHED_MONOTONIC = time.monotonic()


def _failed_closed_payload(error: str, *, store_path: str | None = None) -> dict[str, Any]:
    module = _runtime_install_module()
    return {
        "enabled": True,
        "chain": "ROBINHOOD_CHAIN",
        "chain_id": 4663,
        "strategy_version": getattr(module, "ROBINHOOD_V5_VERSION", "robinhood-chain-paper"),
        "paper_only": True,
        "paper_trading_authority": False,
        "shadow_only": False,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
        "runtime_ready": False,
        "failed_closed": True,
        "caught_up_for_paper_decisions": False,
        "paper_decision_transport_ready": False,
        "error": error,
        "production_install": dict(getattr(module, "_STATE", {})),
        "worker_isolation": _worker_isolation_metadata(store_path=store_path),
    }


def _fast_live_status(plane: Any) -> dict[str, Any]:
    """Build the one-second status snapshot without touching SQLite.

    Historical counts, NAV analytics and wallet-return summaries are proof/analytics
    surfaces. They must not rescan a growing SQLite file merely to keep the live
    heartbeat fresh. Decision readiness here is derived only from the same in-memory
    cursor/frontier state maintained by the worker itself.
    """
    module = _runtime_install_module()
    cursor = getattr(plane, "_cursor", None)
    latest = getattr(plane, "_latest_block", None)
    live_cursor = getattr(plane, "_roi_live_epoch_cursor", None)
    if live_cursor is not None:
        decision_cursor = int(live_cursor)
        transport_error = getattr(plane, "_roi_live_epoch_last_error_type", None)
        transport_ready = (
            bool(getattr(plane, "_roi_live_epoch_ready", False))
            and not bool(getattr(plane, "_roi_live_epoch_suppress_entries", False))
            and not transport_error
        )
    else:
        decision_cursor = int(cursor) if cursor is not None else None
        transport_error = getattr(plane, "_last_error", None)
        transport_ready = bool(getattr(plane, "_caught_up", False)) and not transport_error
    lag = (
        max(0, int(latest) - int(decision_cursor))
        if latest is not None and decision_cursor is not None
        else None
    )
    historical_lag = (
        max(0, int(latest) - int(cursor))
        if latest is not None and cursor is not None
        else None
    )
    legacy_requests = int(getattr(plane, "_roi_market_log_legacy_equivalent_requests", 0) or 0)
    actual_requests = int(getattr(plane, "_roi_market_log_actual_requests", 0) or 0)
    saved_requests = max(0, legacy_requests - actual_requests)
    last_error = getattr(plane, "_last_error", None)
    return {
        "enabled": bool(getattr(plane, "enabled", True)),
        "chain": "ROBINHOOD_CHAIN",
        "chain_id": 4663,
        "strategy_version": getattr(module, "ROBINHOOD_V5_VERSION", "robinhood-chain-paper"),
        "paper_only": True,
        "paper_trading_authority": True,
        "shadow_only": False,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
        "worker_process_ready": True,
        "runtime_ready": transport_ready,
        "failed_closed": False,
        "cursor_block": cursor,
        "latest_block": latest,
        "block_lag": lag,
        "historical_block_lag": historical_lag,
        "caught_up_for_paper_decisions": transport_ready,
        "paper_decision_transport_ready": transport_ready,
        "last_poll_at": getattr(plane, "_last_poll_at", None),
        "last_success_at": getattr(plane, "_last_success_at", None),
        "last_error": last_error,
        "error": transport_error or None,
        "rpc_failures": int(getattr(plane, "_rpc_failures", 0) or 0),
        "tracked_v3_pools": len(getattr(plane, "v3_pools", {}) or {}),
        "tracked_pons_v2_curves": len(getattr(plane, "v2_curves", {}) or {}),
        "getlogs_efficiency": {
            "legacy_equivalent_market_log_requests": legacy_requests,
            "actual_market_log_requests": actual_requests,
            "market_log_requests_saved": saved_requests,
            "market_log_request_savings_pct": (
                round(saved_requests / legacy_requests * 100.0, 3)
                if legacy_requests > 0
                else 0.0
            ),
            "block_coverage_reduced": False,
            "market_coverage_reduced": False,
        },
        "live_frontier_verification": {
            "verified_live_epoch": live_cursor is not None,
            "live_epoch_ready": transport_ready if live_cursor is not None else False,
            "live_epoch_anchor_block": getattr(plane, "_roi_live_epoch_anchor_block", None),
            "live_epoch_cursor_block": live_cursor,
            "live_epoch_lag_blocks": lag if live_cursor is not None else None,
            "live_epoch_started_at": getattr(plane, "_roi_live_epoch_started_at", None),
            "live_epoch_last_success_at": getattr(plane, "_roi_live_epoch_last_success_at", None),
            "live_epoch_reason": getattr(plane, "_roi_live_epoch_reason", None),
            "historical_cursor_block": cursor,
            "historical_block_lag": historical_lag,
            "historical_backfill_preserved": True,
            "historical_backfill_can_authorize_entries": False,
        },
        "status_read_boundary": {
            "history_scaled_sqlite_reads": False,
            "swap_count_scanned_on_heartbeat": False,
            "outcome_history_scanned_on_heartbeat": False,
            "nav_history_scanned_on_heartbeat": False,
            "proof_analytics_published_separately": True,
        },
        "production_install": dict(getattr(module, "_STATE", {})),
    }


def _nonblocking_status() -> dict[str, Any]:
    """Return Robinhood telemetry without ever waiting on its live SQLite connection."""
    module = _runtime_install_module()
    with _STATUS_LOCK:
        snapshot = copy.deepcopy(_STATUS_SNAPSHOT)
        published_at = _STATUS_PUBLISHED_MONOTONIC

    if snapshot is None:
        return _failed_closed_payload(
            getattr(module, "_STARTUP_ERROR", None) or "isolated_robinhood_worker_not_ready"
        )

    proof = _current_proof_snapshot()
    if proof is not None:
        snapshot["v51_proof"] = proof
    age = max(0.0, time.monotonic() - published_at) if published_at is not None else None
    isolation = snapshot.setdefault("worker_isolation", {})
    if isinstance(isolation, dict):
        isolation.update(_worker_isolation_metadata(store_path=isolation.get("dedicated_store_path")))
        isolation["status_cache_age_seconds"] = age
        isolation["status_cache_stale_after_seconds"] = STATUS_STALE_SECONDS

    snapshot["production_install"] = dict(getattr(module, "_STATE", {}))
    thread_alive = bool(_WORKER_THREAD is not None and _WORKER_THREAD.is_alive())
    stale = age is None or age > STATUS_STALE_SECONDS
    if stale or not thread_alive:
        snapshot["runtime_ready"] = False
        snapshot["failed_closed"] = True
        snapshot["paper_trading_authority"] = False
        snapshot["caught_up_for_paper_decisions"] = False
        snapshot["paper_decision_transport_ready"] = False
        snapshot["error"] = (
            "robinhood_isolated_worker_thread_not_alive"
            if not thread_alive
            else "robinhood_isolated_status_snapshot_stale"
        )
        if isinstance(isolation, dict):
            isolation["status_cache_stale"] = stale
    elif isinstance(isolation, dict):
        isolation["status_cache_stale"] = False
    return snapshot


def _seed_cursor_from_canonical(plane: Any, canonical_store: Any) -> int | None:
    if getattr(plane, "_cursor", None) is not None:
        return int(plane._cursor)
    try:
        with canonical_store._lock:
            row = canonical_store.db.execute(
                "SELECT value FROM robinhood_chain_state WHERE key='cursor_block'"
            ).fetchone()
    except sqlite3.Error:
        return None
    if row is None:
        return None
    value = int(row["value"] if hasattr(row, "keys") else row[0])
    plane._set_cursor(value)
    return value


def _ensure_proof_generation_schema(store: Any) -> int:
    """Return a durable O(1) generation for proof-relevant evidence changes.

    Raw robinhood_swaps is intentionally excluded: swap ingestion is high-volume raw
    evidence and does not by itself justify rebuilding the deep economic proof every
    five seconds. Candidate/trial/outcome/context changes do.
    """
    with store._lock, store.db:
        store.db.execute(
            f"CREATE TABLE IF NOT EXISTS {_PROOF_GENERATION_TABLE} ("
            "singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
            "generation INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL)"
        )
        store.db.execute(
            f"INSERT OR IGNORE INTO {_PROOF_GENERATION_TABLE}(singleton,generation,updated_at) "
            "VALUES (1,0,strftime('%Y-%m-%dT%H:%M:%fZ','now'))"
        )
        for table in _PROOF_INPUT_TABLES:
            exists = store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table,),
            ).fetchone()
            if exists is None:
                continue
            for operation in ("INSERT", "UPDATE", "DELETE"):
                trigger = f"trg_robinhood_proof_generation_{table}_{operation.lower()}"
                store.db.execute(
                    f"CREATE TRIGGER IF NOT EXISTS {trigger} AFTER {operation} ON {table} BEGIN "
                    f"UPDATE {_PROOF_GENERATION_TABLE} SET generation=generation+1, "
                    "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE singleton=1; END"
                )
        row = store.db.execute(
            f"SELECT generation FROM {_PROOF_GENERATION_TABLE} WHERE singleton=1"
        ).fetchone()
    return int(row["generation"] if hasattr(row, "keys") else row[0]) if row is not None else 0


def _refresh_proof_on_separate_connection(
    store_path: str,
    *,
    store_factory: Callable[..., Any] = ObservationEventStore,
) -> dict[str, Any]:
    """Build proof only when durable proof-relevant inputs changed."""
    proof_store: Any | None = None
    try:
        proof_store = store_factory(store_path)
        generation_before = _ensure_proof_generation_schema(proof_store)
        with _PROOF_LOCK:
            cached_generation = _PROOF_INPUT_GENERATION
            cached_proof = copy.deepcopy(_PROOF_SNAPSHOT)
        if cached_proof is not None and cached_generation == generation_before:
            cached_proof["proof_input_generation"] = generation_before
            cached_proof["proof_refresh_skipped_unchanged_inputs"] = True
            cached_proof["deep_proof_rebuilt"] = False
            cached_proof["proof_refresh_topology"] = "generation_gated_separate_sqlite_connection_in_threadpool"
            return cached_proof

        from .v51_consolidated_strategy import install_v51_consolidated_strategy
        from .v51_robinhood_consolidation import refresh_robinhood_candidate_learning
        from .v51_robinhood_proof import cached_robinhood_proof

        release = os.getenv("RENDER_GIT_COMMIT") or os.getenv("GITHUB_SHA") or "local"
        install_v51_consolidated_strategy(store=proof_store, release_commit=release)
        # The v5.1 install may create proof-input tables on a fresh store. Re-run the
        # constant-size trigger installer before any proof writes so later changes are
        # observed without a historical scan.
        generation_before = _ensure_proof_generation_schema(proof_store)
        refresh_robinhood_candidate_learning(proof_store)
        proof = cached_robinhood_proof(proof_store, max_age_seconds=0.0)
        generation_after = _ensure_proof_generation_schema(proof_store)
        proof["available"] = True
        proof["proof_input_generation"] = generation_before
        proof["proof_inputs_changed_during_build"] = generation_after != generation_before
        proof["proof_refresh_skipped_unchanged_inputs"] = False
        proof["deep_proof_rebuilt"] = True
        proof["proof_refresh_topology"] = "generation_gated_separate_sqlite_connection_in_threadpool"
        proof["live_frontier_blocked_by_proof_refresh"] = False
        proof["raw_swap_writes_trigger_deep_proof"] = False
        proof["source_history_deleted"] = False
        proof["retention_changed"] = False
        return proof
    except Exception as exc:
        return {
            "available": False,
            "reason": "isolated_robinhood_proof_failed_closed",
            "error_type": type(exc).__name__,
            "proof_refresh_topology": "generation_gated_separate_sqlite_connection_in_threadpool",
            "live_frontier_blocked_by_proof_refresh": False,
            "paper_only": True,
            "live_money_authority": False,
        }
    finally:
        if proof_store is not None:
            with suppress(Exception):
                proof_store.close()


async def _status_publisher(local_stop: asyncio.Event, *, store_path: str) -> None:
    """Publish live status using in-memory worker state only."""
    while not local_stop.is_set():
        try:
            module = _runtime_install_module()
            plane = getattr(module, "_PLANE", None)
            if plane is not None:
                _publish_snapshot(_fast_live_status(plane), store_path=store_path)
        except Exception as exc:
            module = _runtime_install_module()
            module._STARTUP_ERROR = f"{type(exc).__name__}: Robinhood status snapshot failed"
        try:
            await asyncio.wait_for(local_stop.wait(), timeout=STATUS_PUBLISH_SECONDS)
        except TimeoutError:
            pass


async def _proof_publisher(local_stop: asyncio.Event, *, store_path: str) -> None:
    """Refresh economic/counterfactual proof without occupying the live event loop."""
    while not local_stop.is_set():
        proof = await asyncio.to_thread(_refresh_proof_on_separate_connection, store_path)
        _publish_proof_snapshot(proof)
        try:
            await asyncio.wait_for(local_stop.wait(), timeout=PROOF_PUBLISH_SECONDS)
        except TimeoutError:
            pass


async def _thread_stop_bridge(thread_stop: threading.Event, local_stop: asyncio.Event) -> None:
    while not thread_stop.is_set():
        await asyncio.sleep(0.10)
    local_stop.set()


async def _worker_async(
    canonical_store: Any,
    thread_stop: threading.Event,
    *,
    plane_factory: Callable[..., Any] = RobinhoodChainPaperPlane,
    store_factory: Callable[..., Any] = ObservationEventStore,
) -> None:
    module = _runtime_install_module()
    dedicated_store: Any | None = None
    plane: Any | None = None
    local_stop = asyncio.Event()
    tasks: list[asyncio.Task[Any]] = []
    store_path: Path | None = None
    try:
        store_path = _dedicated_store_path(canonical_store)
        dedicated_store = store_factory(store_path)
        plane = plane_factory(dedicated_store)
        module._PLANE = plane
        module._STARTUP_ERROR = None
        seeded_cursor = _seed_cursor_from_canonical(plane, canonical_store)
        module._STATE.update(
            {
                "state": "running" if plane.enabled else "disabled",
                "worker_isolation": "dedicated_os_thread_with_private_asyncio_loop",
                "dedicated_store": str(store_path),
                "cursor_seeded_from_canonical": seeded_cursor,
                "proof_refresh_topology": "generation_gated_separate_sqlite_connection_in_threadpool",
                "fast_status_history_scaled_sqlite_reads": False,
                "proof_refresh_input_generation_gated": True,
            }
        )
        _publish_snapshot(_fast_live_status(plane), store_path=str(store_path))
        if not plane.enabled:
            return

        tasks = [
            asyncio.create_task(
                _thread_stop_bridge(thread_stop, local_stop), name="robinhood-thread-stop-bridge"
            ),
            asyncio.create_task(
                _status_publisher(local_stop, store_path=str(store_path)),
                name="robinhood-thread-status-publisher",
            ),
            asyncio.create_task(
                _proof_publisher(local_stop, store_path=str(store_path)),
                name="robinhood-thread-proof-publisher",
            ),
        ]
        await plane.run(local_stop)
        if not local_stop.is_set() and not thread_stop.is_set():
            raise RuntimeError("Robinhood isolated worker returned unexpectedly")
    finally:
        local_stop.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError, Exception):
                await task
        if plane is not None:
            with suppress(Exception):
                await plane.close()
        if dedicated_store is not None:
            with suppress(Exception):
                dedicated_store.close()
        if thread_stop.is_set():
            module._STATE["state"] = "stopped"


def _worker_thread_main(
    canonical_store: Any,
    thread_stop: threading.Event,
    *,
    plane_factory: Callable[..., Any] = RobinhoodChainPaperPlane,
    store_factory: Callable[..., Any] = ObservationEventStore,
) -> None:
    module = _runtime_install_module()
    try:
        asyncio.run(
            _worker_async(
                canonical_store,
                thread_stop,
                plane_factory=plane_factory,
                store_factory=store_factory,
            )
        )
    except BaseException as exc:
        module._STARTUP_ERROR = f"{type(exc).__name__}: {exc}"
        module._STATE["state"] = "failed_closed"
        store_path: str | None = None
        with suppress(Exception):
            store_path = str(_dedicated_store_path(canonical_store))
        _publish_snapshot(
            _failed_closed_payload(module._STARTUP_ERROR, store_path=store_path),
            store_path=store_path,
        )
    finally:
        module._PLANE = None


def _start_worker_thread(
    canonical_store: Any,
    *,
    plane_factory: Callable[..., Any] = RobinhoodChainPaperPlane,
    store_factory: Callable[..., Any] = ObservationEventStore,
) -> tuple[threading.Thread, threading.Event]:
    global _WORKER_THREAD, _WORKER_STOP
    stop = threading.Event()
    thread = threading.Thread(
        target=_worker_thread_main,
        args=(canonical_store, stop),
        kwargs={"plane_factory": plane_factory, "store_factory": store_factory},
        name=THREAD_NAME,
        daemon=True,
    )
    _WORKER_THREAD = thread
    _WORKER_STOP = stop
    thread.start()
    return thread, stop


async def _isolated_runtime_workers(runtime: Any, stop: asyncio.Event) -> None:
    module = _runtime_install_module()
    original_workers = getattr(module, "_ORIGINAL_RUNTIME_WORKERS", None)
    if original_workers is None:
        raise RuntimeError("Robinhood production worker composition is not installed")

    canonical_store = getattr(runtime, "store", None)
    if canonical_store is None or not hasattr(canonical_store, "path"):
        module._STATE["worker_isolation_skipped_no_store"] = True
        await original_workers(runtime, stop)
        return

    module._STATE["worker_isolation_skipped_no_store"] = False
    module._STATE["attempts"] = int(module._STATE.get("attempts", 0)) + 1
    thread, thread_stop = _start_worker_thread(canonical_store)
    try:
        await original_workers(runtime, stop)
    finally:
        thread_stop.set()
        await asyncio.to_thread(thread.join, THREAD_JOIN_TIMEOUT_SECONDS)
        if thread.is_alive():
            module._STATE["state"] = "shutdown_timeout_daemon_thread"
        elif stop.is_set() and module._STATE.get("state") != "failed_closed":
            module._STATE["state"] = "stopped"


setattr(_isolated_runtime_workers, "_roi_robinhood_dedicated_worker_isolation", True)
setattr(_nonblocking_status, "_roi_robinhood_dedicated_worker_isolation", True)


def install_robinhood_worker_isolation_repair() -> None:
    global _BASE_STATUS, _ORIGINAL_STATUS, _INSTALLED
    if _INSTALLED:
        return
    module = _runtime_install_module()
    _BASE_STATUS = module._status
    _ORIGINAL_STATUS = module._status
    module._runtime_workers_with_robinhood = _isolated_runtime_workers
    module._status = _nonblocking_status
    module._STATE.update(
        {
            "worker_isolation_repair": REPAIR_VERSION,
            "worker_isolation": "dedicated_os_thread_with_private_asyncio_loop",
            "proof_refresh_offloaded": True,
            "fast_status_history_scaled_sqlite_reads": False,
            "proof_refresh_input_generation_gated": True,
        }
    )
    _INSTALLED = True


__all__ = [
    "PROOF_PUBLISH_SECONDS",
    "REPAIR_VERSION",
    "STATUS_STALE_SECONDS",
    "THREAD_JOIN_TIMEOUT_SECONDS",
    "_dedicated_store_path",
    "_ensure_proof_generation_schema",
    "_fast_live_status",
    "_nonblocking_status",
    "_refresh_proof_on_separate_connection",
    "_seed_cursor_from_canonical",
    "_start_worker_thread",
    "install_robinhood_worker_isolation_repair",
]
