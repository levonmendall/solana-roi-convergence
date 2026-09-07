from __future__ import annotations

import asyncio
import os
import queue
import sqlite3
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable

from . import direct_solana as direct_module
from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from . import poll_watermark_repair as watermark
from . import render_runtime_bootstrap_repair as render_bootstrap
from . import robinhood_live_frontier_verification_repair as frontier
from . import robinhood_production_ws_transport as prod_ws
from . import robinhood_worker_isolation_repair as robinhood_isolation
from . import strategy_relevant_continuity as strategy_continuity
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


REPAIR_VERSION = "batch9-continuity-frontier-proof-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_ORIGINAL_SLOT_PAGE: Callable[..., Any] | None = None
_ORIGINAL_SLOT_FETCH: Callable[..., Any] | None = None
_ORIGINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_PROOF_REFRESH: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_PROOF_METADATA: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_PLANE_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_RUNTIME_WORKERS: Callable[..., Any] | None = None
_INSTALLED = False

_PROOF_STATS_LOCK = threading.Lock()
_PROOF_STATS: dict[str, Any] = {
    "refreshes": 0,
    "failures": 0,
    "consecutive_failures": 0,
    "last_started_at": None,
    "last_completed_at": None,
    "last_duration_seconds": None,
    "last_error_type": None,
}


def _release_commit() -> str:
    return strategy_continuity._release_commit()


def _target_key(target: WatchTarget) -> str:
    return live_poll._poll_target_key(target)


def _checkpoint_store(self: Any) -> Any | None:
    store = getattr(self, "store", None)
    if store is None or not hasattr(store, "db") or not hasattr(store, "_lock"):
        return None
    return store


def _ensure_checkpoint_schema(self: Any) -> None:
    store = _checkpoint_store(self)
    if store is None:
        return
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS direct_solana_strategy_poll_checkpoint ("
            "release_commit TEXT NOT NULL, target_key TEXT NOT NULL, cursor_slot INTEGER NOT NULL, "
            "ws_gap_generation INTEGER NOT NULL, last_success_at TEXT NOT NULL, updated_at TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "PRIMARY KEY(release_commit,target_key))"
        )


def _checkpoint_row(self: Any, target: WatchTarget) -> dict[str, Any] | None:
    store = _checkpoint_store(self)
    if store is None or target.kind != "scout":
        return None
    try:
        _ensure_checkpoint_schema(self)
        with store._lock:
            row = store.db.execute(
                "SELECT release_commit,target_key,cursor_slot,ws_gap_generation,last_success_at,updated_at "
                "FROM direct_solana_strategy_poll_checkpoint WHERE release_commit=? AND target_key=?",
                (_release_commit(), _target_key(target)),
            ).fetchone()
    except Exception:
        return None
    return dict(row) if row is not None else None


def _save_checkpoint(
    self: Any,
    target: WatchTarget,
    *,
    cursor_slot: int,
    ws_gap_generation: int,
    last_success_at: str | None = None,
) -> None:
    store = _checkpoint_store(self)
    if store is None or target.kind != "scout" or int(cursor_slot) <= 0:
        return
    _ensure_checkpoint_schema(self)
    now = direct_module.utcnow().isoformat()
    success = str(last_success_at or now)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO direct_solana_strategy_poll_checkpoint("
            "release_commit,target_key,cursor_slot,ws_gap_generation,last_success_at,updated_at,"
            "paper_only,live_money_authority) VALUES (?,?,?,?,?,?,1,0) "
            "ON CONFLICT(release_commit,target_key) DO UPDATE SET "
            "cursor_slot=excluded.cursor_slot,ws_gap_generation=excluded.ws_gap_generation,"
            "last_success_at=excluded.last_success_at,updated_at=excluded.updated_at",
            (
                _release_commit(),
                _target_key(target),
                int(cursor_slot),
                int(ws_gap_generation),
                success,
                now,
            ),
        )


async def _scout_rpc_page(
    self: Any,
    target: WatchTarget,
    *,
    before: str | None = None,
    min_context_slot: int | None = None,
    limit: int | None = None,
) -> tuple[list[dict[str, Any]], str, float]:
    """Use the existing provider set but hedge strategy-scout continuity reads."""
    page_limit = live_poll.POLL_LIMIT if limit is None else int(limit)
    config: dict[str, Any] = {
        "commitment": "confirmed",
        "limit": max(1, min(1000, page_limit)),
    }
    if before:
        config["before"] = before
    if min_context_slot is not None and int(min_context_slot) > 0:
        config["minContextSlot"] = int(min_context_slot)
    result, provider, latency = await live_poll._poll_rpc(self).call_with_meta(
        "getSignaturesForAddress",
        [target.address, config],
        hedge=True,
    )
    rows = [row for row in result if isinstance(row, dict)] if isinstance(result, list) else []
    return rows, provider, latency


async def _fetch_scout_delta(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    pages: list[list[dict[str, Any]]] = []
    before: str | None = None
    provider: str | None = None
    latency: float | None = None
    complete = False
    for _page_index in range(live_poll.POLL_CURSOR_MAX_PAGES):
        page, provider, latency = await _scout_rpc_page(
            self,
            target,
            before=before,
            min_context_slot=cursor_slot if cursor_slot > 0 else None,
            limit=live_poll.POLL_LIMIT,
        )
        pages.append(page)
        if not page:
            complete = True
            break
        if cursor_slot > 0 and any(watermark._row_slot(row) <= cursor_slot for row in page):
            complete = True
            break
        if len(page) < live_poll.POLL_LIMIT:
            complete = True
            break
        before = str(page[-1].get("signature") or "")
        if not before:
            complete = True
            break
    if not complete:
        return [], False, provider, latency

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for page in reversed(pages):
        for row in reversed(page):
            signature = str(row.get("signature") or "")
            slot = watermark._row_slot(row)
            if not signature or signature in seen or slot <= cursor_slot:
                continue
            seen.add(signature)
            rows.append(row)
    return rows, True, provider, latency


def _record_reconciliation_audit_rows(self: Any, target: WatchTarget, rows: list[dict[str, Any]]) -> int:
    """Preserve restart reconciliation as audit evidence without retrospective entry authority."""
    inserted = 0
    journal = getattr(self, "journal", None)
    if journal is None or not hasattr(journal, "record_receipt"):
        return 0
    source_key = target.source_hint or f"SCOUT:{target.address}"
    for row in rows:
        signature = str(row.get("signature") or "")
        slot = watermark._row_slot(row)
        if not signature or slot <= 0:
            continue
        try:
            if journal.record_receipt(
                signature=signature,
                source_key=source_key,
                slot=slot,
                received_at=direct_module.utcnow(),
                launch_like=False,
            ):
                inserted += 1
        except Exception:
            continue
    return inserted


async def _slot_page_with_durable_scout_checkpoint(
    self: Any,
    target: WatchTarget,
    *,
    before: str | None = None,
    min_context_slot: int | None = None,
    limit: int | None = None,
) -> tuple[list[dict[str, Any]], str, float]:
    if target.kind != "scout":
        if _ORIGINAL_SLOT_PAGE is None:
            raise RuntimeError("Batch 9 scout continuity repair missing original slot page")
        return await _ORIGINAL_SLOT_PAGE(
            self,
            target,
            before=before,
            min_context_slot=min_context_slot,
            limit=limit,
        )

    is_initial_baseline = before is None and min_context_slot is None and int(limit or 1) == 1
    if not is_initial_baseline:
        return await _scout_rpc_page(
            self,
            target,
            before=before,
            min_context_slot=min_context_slot,
            limit=limit,
        )

    checkpoint = _checkpoint_row(self, target)
    if checkpoint is None:
        rows, provider, latency = await _scout_rpc_page(self, target, limit=1)
        slot = watermark._row_slot(rows[0]) if rows else 0
        if slot > 0:
            _save_checkpoint(
                self,
                target,
                cursor_slot=slot,
                ws_gap_generation=lease._current_ws_generation(self, target),
            )
        return rows, provider, latency

    cursor_slot = int(checkpoint.get("cursor_slot") or 0)
    rows, complete, provider, latency = await _fetch_scout_delta(self, target, cursor_slot)
    if not complete:
        raise RuntimeError("DurableScoutCheckpointReconciliationIncomplete")
    audit_rows = _record_reconciliation_audit_rows(self, target, rows)
    newest = max((watermark._row_slot(row) for row in rows), default=cursor_slot)
    _save_checkpoint(
        self,
        target,
        cursor_slot=max(cursor_slot, newest),
        ws_gap_generation=lease._current_ws_generation(self, target),
    )
    setattr(
        self,
        "_roi_batch9_scout_restart_reconciliation_rows",
        int(getattr(self, "_roi_batch9_scout_restart_reconciliation_rows", 0) or 0) + audit_rows,
    )
    # The lease loop consumes only the slot from this initialization row. The row is
    # deliberately synthetic and is never queued as a candidate.
    return (
        [{"signature": f"durable-scout-checkpoint:{_target_key(target)}", "slot": max(cursor_slot, newest)}],
        str(provider or "durable-scout-checkpoint"),
        float(latency or 0.0),
    )


async def _slot_fetch_with_durable_scout_checkpoint(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    if target.kind != "scout":
        if _ORIGINAL_SLOT_FETCH is None:
            raise RuntimeError("Batch 9 scout continuity repair missing original slot fetch")
        return await _ORIGINAL_SLOT_FETCH(self, target, cursor_slot)

    rows, complete, provider, latency = await _fetch_scout_delta(self, target, cursor_slot)
    if complete:
        newest = max((watermark._row_slot(row) for row in rows), default=int(cursor_slot))
        _save_checkpoint(
            self,
            target,
            cursor_slot=max(int(cursor_slot), newest),
            ws_gap_generation=lease._current_ws_generation(self, target),
        )
    return rows, complete, provider, latency


def _checkpoint_summary(self: Any) -> dict[str, Any]:
    store = _checkpoint_store(self)
    if store is None:
        return {"checkpoint_count": 0, "targets": []}
    try:
        _ensure_checkpoint_schema(self)
        with store._lock:
            rows = store.db.execute(
                "SELECT target_key,cursor_slot,ws_gap_generation,last_success_at,updated_at "
                "FROM direct_solana_strategy_poll_checkpoint WHERE release_commit=? ORDER BY target_key",
                (_release_commit(),),
            ).fetchall()
    except Exception:
        rows = []
    return {
        "checkpoint_count": len(rows),
        "targets": [dict(row) for row in rows],
    }


def _direct_status_with_batch9(self: Any) -> dict[str, Any]:
    if _ORIGINAL_DIRECT_STATUS is None:
        raise RuntimeError("Batch 9 direct status repair missing original")
    payload = _ORIGINAL_DIRECT_STATUS(self)
    payload["strategy_scout_durable_poll_recovery"] = {
        "repair_version": REPAIR_VERSION,
        "release_commit": _release_commit(),
        "checkpoint_scope": "strategy_scouts_only",
        "checkpoint_model": "durable_confirmed_slot_per_release_per_target",
        "same_release_restart_reconciles_before_poll_quorum": True,
        "restart_reconciliation_rows_are_audit_only": True,
        "restart_reconciliation_rows_have_retrospective_entry_authority": False,
        "scout_rpc_hedging_enabled": True,
        "provider_scope_changed": False,
        "recoverability_lease_seconds": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
        "recoverability_lease_changed": False,
        "recovery_page_limit": live_poll.POLL_CURSOR_MAX_PAGES,
        "recovery_page_size": live_poll.POLL_LIMIT,
        "recovery_bound_changed": False,
        "recorded_gap_can_be_cleared": False,
        "restart_reconciliation_audit_rows": int(
            getattr(self, "_roi_batch9_scout_restart_reconciliation_rows", 0) or 0
        ),
        **_checkpoint_summary(self),
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "strategy_scout_poll_checkpoint_durable": True,
                "strategy_scout_poll_restart_reconciliation_before_quorum": True,
                "strategy_scout_poll_rpc_hedged": True,
                "strategy_scout_recorded_gap_still_fail_closed": True,
                "continuity_lease_unchanged": True,
                "recovery_bound_unchanged": True,
            }
        )
    return payload


def _proof_snapshot_path(store_path: str) -> Path:
    parent = Path(store_path).expanduser().resolve().parent
    fd, name = tempfile.mkstemp(prefix=".robinhood-proof-snapshot-", suffix=".sqlite3", dir=parent)
    os.close(fd)
    return Path(name)


def _cleanup_sqlite_snapshot(path: Path) -> None:
    for candidate in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm")):
        try:
            candidate.unlink(missing_ok=True)
        except Exception:
            pass


def _snapshot_robinhood_proof_refresh(
    store_path: str,
    *,
    store_factory: Callable[..., Any] = robinhood_isolation.ObservationEventStore,
) -> dict[str, Any]:
    """Move proof DDL/refresh writes off the live Robinhood SQLite database."""
    if _ORIGINAL_PROOF_REFRESH is None:
        raise RuntimeError("Batch 9 proof snapshot repair missing original refresh")
    started = time.monotonic()
    started_at = direct_module.utcnow().isoformat()
    snapshot = _proof_snapshot_path(store_path)
    error_type: str | None = None
    try:
        source_uri = f"file:{Path(store_path).expanduser().resolve().as_posix()}?mode=ro"
        source = sqlite3.connect(source_uri, uri=True, timeout=5.0)
        destination = sqlite3.connect(snapshot, timeout=5.0)
        try:
            source.execute("PRAGMA query_only=ON")
            source.execute("PRAGMA busy_timeout=5000")
            source.backup(destination, pages=512, sleep=0.01)
            destination.commit()
        finally:
            destination.close()
            source.close()

        proof = _ORIGINAL_PROOF_REFRESH(str(snapshot), store_factory=store_factory)
        proof = dict(proof) if isinstance(proof, dict) else {"available": False}
        proof["proof_refresh_source"] = "consistent_sqlite_online_backup_snapshot"
        proof["live_dedicated_store_write_authority"] = False
        proof["proof_refresh_writes_live_store"] = False
        return proof
    except Exception as exc:
        error_type = type(exc).__name__
        return {
            "available": False,
            "reason": "isolated_robinhood_proof_snapshot_failed_closed",
            "error_type": error_type,
            "proof_refresh_source": "consistent_sqlite_online_backup_snapshot",
            "live_dedicated_store_write_authority": False,
            "proof_refresh_writes_live_store": False,
            "paper_only": True,
            "live_money_authority": False,
        }
    finally:
        duration = max(0.0, time.monotonic() - started)
        completed_at = direct_module.utcnow().isoformat()
        with _PROOF_STATS_LOCK:
            _PROOF_STATS["refreshes"] = int(_PROOF_STATS.get("refreshes", 0) or 0) + 1
            _PROOF_STATS["last_started_at"] = started_at
            _PROOF_STATS["last_completed_at"] = completed_at
            _PROOF_STATS["last_duration_seconds"] = duration
            _PROOF_STATS["last_error_type"] = error_type
            if error_type is None:
                _PROOF_STATS["consecutive_failures"] = 0
            else:
                _PROOF_STATS["failures"] = int(_PROOF_STATS.get("failures", 0) or 0) + 1
                _PROOF_STATS["consecutive_failures"] = int(
                    _PROOF_STATS.get("consecutive_failures", 0) or 0
                ) + 1
        _cleanup_sqlite_snapshot(snapshot)


def _proof_metadata_with_batch9(*, store_path: str | None = None) -> dict[str, Any]:
    if _ORIGINAL_PROOF_METADATA is None:
        return {}
    payload = _ORIGINAL_PROOF_METADATA(store_path=store_path)
    with _PROOF_STATS_LOCK:
        stats = dict(_PROOF_STATS)
    payload.update(
        {
            "proof_refresh_live_store_mode": "read_only_online_backup",
            "proof_refresh_work_store": "temporary_consistent_snapshot",
            "proof_refresh_writes_live_dedicated_store": False,
            "proof_refresh_lock_contention_repair": REPAIR_VERSION,
            "proof_refreshes_session": stats.get("refreshes", 0),
            "proof_refresh_failures_session": stats.get("failures", 0),
            "proof_refresh_consecutive_failures": stats.get("consecutive_failures", 0),
            "proof_refresh_last_started_at": stats.get("last_started_at"),
            "proof_refresh_last_completed_at": stats.get("last_completed_at"),
            "proof_refresh_last_duration_seconds": stats.get("last_duration_seconds"),
            "proof_refresh_last_error_type": stats.get("last_error_type"),
        }
    )
    return payload


def _strict_live_epoch_active(self: Any) -> bool:
    return bool(
        frontier._live_cursor(self) is not None
        and getattr(self, "_roi_live_epoch_anchor_block", None) is not None
        and bool(getattr(self, "_roi_live_epoch_started_at", None))
    )


def _epoch_integrity(self: Any) -> bool:
    state = prod_ws._state(self)
    return bool(
        _strict_live_epoch_active(self)
        and getattr(self, "_roi_prod_ws_epoch_generation", None) == state.get("generation")
    )


async def _generation_anchored_production_ws_run(self: Any, stop: asyncio.Event) -> None:
    """Advance readiness only after a real WSS generation is anchored and processed."""
    if not self.enabled:
        return
    reader_stop = threading.Event()
    reader = threading.Thread(
        target=prod_ws._reader_thread_main,
        args=(self, reader_stop),
        name="robinhood-production-ws-reader",
        daemon=True,
    )
    setattr(self, "_roi_prod_ws_reader_thread", reader)
    reader.start()
    pending: dict[int, list[dict[str, Any]]] = {}
    processing_generation: int | None = None
    last_settlement = 0.0
    try:
        while not stop.is_set():
            state = prod_ws._state(self)
            generation = int(state.get("generation", 0) or 0)
            if processing_generation != generation:
                pending.clear()
                processing_generation = generation
                setattr(self, "_roi_prod_ws_epoch_generation", None)
                self._caught_up = False
                setattr(self, "_roi_live_epoch_ready", False)
                setattr(
                    self,
                    "_roi_production_transport_block_reason",
                    "robinhood_production_websocket_reanchoring",
                )

            drained = 0
            q = prod_ws._event_queue(self)
            while drained < prod_ws.PROCESS_BATCH_MAX:
                try:
                    item = q.get_nowait()
                except queue.Empty:
                    break
                drained += 1
                if int(item.get("generation", -1)) != processing_generation:
                    continue
                try:
                    block = int(str(item["log"].get("blockNumber") or "0x0"), 16)
                except (TypeError, ValueError):
                    continue
                pending.setdefault(block, []).append(item)

            state = prod_ws._state(self)
            head = state.get("head_block")
            reader_ready = prod_ws._reader_ready(self)
            if isinstance(head, int) and reader_ready:
                self._latest_block = int(head)
                epoch_generation = getattr(self, "_roi_prod_ws_epoch_generation", None)
                anchor = getattr(self, "_roi_live_epoch_anchor_block", None)
                if epoch_generation != processing_generation or anchor is None or not _strict_live_epoch_active(self):
                    frontier._start_epoch(
                        self,
                        anchor_block=int(head),
                        reason="production_ws_generation_anchor",
                    )
                    setattr(self, "_roi_prod_ws_epoch_generation", processing_generation)
                    # Everything already received at or before the anchor predates
                    # the new prospective epoch and remains non-authoritative.
                    pending = {block: items for block, items in pending.items() if block > int(head)}
                    self._caught_up = False
                    setattr(self, "_roi_live_epoch_ready", False)
                    setattr(
                        self,
                        "_roi_production_transport_block_reason",
                        "robinhood_production_websocket_epoch_anchoring",
                    )
                elif int(head) > int(anchor):
                    flush_blocks = sorted(block for block in pending if block < int(head))
                    # A verified WSS generation may authorize the events being
                    # processed, but the durable frontier is advanced only after all
                    # prior closed blocks finish successfully.
                    self._caught_up = True
                    setattr(self, "_roi_live_epoch_ready", True)
                    try:
                        for block in flush_blocks:
                            items = pending.pop(block)
                            await prod_ws._process_block(
                                self,
                                items,
                                generation=int(processing_generation or 0),
                            )
                            setattr(self, "_roi_prod_ws_last_processed_block", block)
                            setattr(self, "_roi_prod_ws_last_processed_at", direct_module.utcnow().isoformat())
                    except Exception:
                        self._caught_up = False
                        setattr(self, "_roi_live_epoch_ready", False)
                        raise

                    setattr(self, "_roi_live_epoch_cursor", int(head))
                    setattr(self, "_roi_live_epoch_factory_verified_through", int(head))
                    setattr(self, "_roi_live_epoch_last_success_at", frontier._utcnow())
                    setattr(self, "_roi_live_epoch_last_error_type", None)
                    setattr(self, "_roi_live_epoch_ready", True)
                    self._caught_up = True
                    setattr(self, "_roi_production_transport_block_reason", None)
                else:
                    self._caught_up = False
                    setattr(self, "_roi_live_epoch_ready", False)

            elif not reader_ready:
                self._caught_up = False
                setattr(self, "_roi_live_epoch_ready", False)

            now = time.monotonic()
            if now - last_settlement >= prod_ws.SETTLEMENT_INTERVAL_SECONDS:
                await self._settle_open_positions()
                last_settlement = now

            try:
                await asyncio.wait_for(stop.wait(), timeout=prod_ws.PROCESS_SLEEP_SECONDS)
            except asyncio.TimeoutError:
                pass
    finally:
        reader_stop.set()
        await asyncio.to_thread(reader.join, 3.0)
        prod_ws._update_state(self, connected=False, synchronized=False)
        self._caught_up = False
        setattr(self, "_roi_live_epoch_ready", False)


def _plane_status_with_epoch_integrity(self: Any) -> dict[str, Any]:
    if _ORIGINAL_PLANE_STATUS is None:
        raise RuntimeError("Batch 9 Robinhood status repair missing original")
    payload = _ORIGINAL_PLANE_STATUS(self)
    configured = prod_ws.production_provider_configured()
    reader_ready = prod_ws._reader_ready(self)
    integrity = _epoch_integrity(self)
    decision_ready = bool(reader_ready and integrity and getattr(self, "_roi_live_epoch_ready", False))
    transport = payload.get("production_transport_authority")
    if isinstance(transport, dict):
        transport.update(
            {
                "batch9_repair_version": REPAIR_VERSION,
                "live_epoch_integrity_valid": integrity,
                "live_epoch_anchor_block": getattr(self, "_roi_live_epoch_anchor_block", None),
                "live_epoch_started_at": getattr(self, "_roi_live_epoch_started_at", None),
                "live_epoch_generation": getattr(self, "_roi_prod_ws_epoch_generation", None),
                "frontier_advanced_after_closed_block_processing": True,
                "decision_authoritative": bool(transport.get("decision_authoritative") and decision_ready),
            }
        )
    if configured and not decision_ready:
        payload["runtime_ready"] = False
        payload["failed_closed"] = True
        payload["paper_trading_authority"] = False
        payload["caught_up_for_paper_decisions"] = False
        payload["paper_decision_transport_ready"] = False
        payload["forward_frontier_ready"] = False
        payload["error"] = (
            "robinhood_production_ws_epoch_not_anchored"
            if reader_ready and not integrity
            else payload.get("error") or "robinhood_production_websocket_not_ready"
        )
    return payload


async def _proof_precompute_worker(app: Any, stop: asyncio.Event) -> None:
    app.state.roi_v51_system_proof_precompute_worker_enabled = True
    app.state.roi_v51_system_proof_precompute_state = "starting"
    try:
        try:
            await asyncio.wait_for(stop.wait(), timeout=2.0)
            return
        except asyncio.TimeoutError:
            pass
        while not stop.is_set():
            callback = getattr(app.state, "roi_v51_system_proof_precompute", None)
            interval = max(
                1.0,
                float(getattr(app.state, "roi_v51_system_proof_precompute_seconds", 15.0) or 15.0),
            )
            if callable(callback):
                started = time.monotonic()
                app.state.roi_v51_system_proof_precompute_state = "running"
                app.state.roi_v51_system_proof_precompute_last_started_at = direct_module.utcnow().isoformat()
                try:
                    await asyncio.to_thread(callback)
                    app.state.roi_v51_system_proof_precompute_last_completed_at = direct_module.utcnow().isoformat()
                    app.state.roi_v51_system_proof_precompute_last_duration_seconds = max(
                        0.0, time.monotonic() - started
                    )
                    app.state.roi_v51_system_proof_precompute_last_error_type = None
                    app.state.roi_v51_system_proof_precompute_state = "ready"
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    app.state.roi_v51_system_proof_precompute_last_error_type = type(exc).__name__
                    app.state.roi_v51_system_proof_precompute_state = "degraded"
            else:
                app.state.roi_v51_system_proof_precompute_state = "waiting_for_callback"

            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
    finally:
        app.state.roi_v51_system_proof_precompute_worker_enabled = False
        if stop.is_set():
            app.state.roi_v51_system_proof_precompute_state = "stopped"


async def _runtime_workers_with_proof_precompute(runtime: Any, stop: asyncio.Event) -> None:
    if _ORIGINAL_RUNTIME_WORKERS is None:
        raise RuntimeError("Batch 9 proof precompute repair missing original runtime workers")
    from . import api as api_module

    task = asyncio.create_task(
        _proof_precompute_worker(api_module.app, stop),
        name="v51-system-proof-precompute",
    )
    try:
        await _ORIGINAL_RUNTIME_WORKERS(runtime, stop)
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def install_batch9_continuity_frontier_proof_repair(app: Any) -> None:
    global _INSTALLED, _ORIGINAL_SLOT_PAGE, _ORIGINAL_SLOT_FETCH, _ORIGINAL_DIRECT_STATUS
    global _ORIGINAL_PROOF_REFRESH, _ORIGINAL_PROOF_METADATA, _ORIGINAL_PLANE_STATUS
    global _ORIGINAL_RUNTIME_WORKERS
    if _INSTALLED:
        return

    # Solana/FOMO: keep the existing 12-second/3x1000 fail-closed contract while
    # making strategy-scout polling durable across same-release process restarts and
    # hedging only the already-configured read-only providers.
    _ORIGINAL_SLOT_PAGE = watermark._slot_poll_page
    _ORIGINAL_SLOT_FETCH = watermark._slot_fetch_delta
    watermark._slot_poll_page = _slot_page_with_durable_scout_checkpoint  # type: ignore[assignment]
    watermark._slot_fetch_delta = _slot_fetch_with_durable_scout_checkpoint  # type: ignore[assignment]
    current_direct_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_direct_status, "_roi_batch9_scout_checkpoint", False)):
        _ORIGINAL_DIRECT_STATUS = current_direct_status
        try:
            _direct_status_with_batch9.__dict__.update(getattr(current_direct_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_direct_status_with_batch9, "_roi_batch9_scout_checkpoint", True)
        DirectSolanaIngestionPlane.status = _direct_status_with_batch9  # type: ignore[method-assign]

    # Robinhood: the proof builder may mutate its work database, but it must never be
    # a second writer on the live dedicated store. Build a consistent SQLite online
    # backup first, then run every proof refresh against that disposable snapshot.
    _ORIGINAL_PROOF_REFRESH = robinhood_isolation._refresh_proof_on_separate_connection
    robinhood_isolation._refresh_proof_on_separate_connection = _snapshot_robinhood_proof_refresh  # type: ignore[assignment]
    _ORIGINAL_PROOF_METADATA = robinhood_isolation._worker_isolation_metadata
    try:
        _proof_metadata_with_batch9.__dict__.update(getattr(_ORIGINAL_PROOF_METADATA, "__dict__", {}))
    except Exception:
        pass
    robinhood_isolation._worker_isolation_metadata = _proof_metadata_with_batch9  # type: ignore[assignment]

    # Robinhood production WSS: require a concrete generation anchor and do not move
    # the durable frontier past a closed block until all queued logs below the head
    # have completed. Cursor-only state can no longer masquerade as a verified epoch.
    prod_ws._production_ws_run = _generation_anchored_production_ws_run  # type: ignore[assignment]
    frontier._live_epoch_active = _strict_live_epoch_active  # type: ignore[assignment]
    from .robinhood_chain_paper import RobinhoodChainPaperPlane

    current_plane_status = RobinhoodChainPaperPlane.status
    if not bool(getattr(current_plane_status, "_roi_batch9_epoch_integrity", False)):
        _ORIGINAL_PLANE_STATUS = current_plane_status
        try:
            _plane_status_with_epoch_integrity.__dict__.update(getattr(current_plane_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_plane_status_with_epoch_integrity, "_roi_batch9_epoch_integrity", True)
        RobinhoodChainPaperPlane.status = _plane_status_with_epoch_integrity  # type: ignore[method-assign]

    # Phase 13 already publishes a precompute callback. Run it beside the canonical
    # workers in a to_thread task, preserving the Uvicorn liveness boundary.
    current_workers = render_bootstrap._run_runtime_workers
    if not bool(getattr(current_workers, "_roi_batch9_proof_precompute", False)):
        _ORIGINAL_RUNTIME_WORKERS = current_workers
        setattr(_runtime_workers_with_proof_precompute, "_roi_batch9_proof_precompute", True)
        render_bootstrap._run_runtime_workers = _runtime_workers_with_proof_precompute  # type: ignore[assignment]

    app.state.roi_batch9_continuity_frontier_proof_repair = True
    app.state.roi_batch9_continuity_frontier_proof_repair_version = REPAIR_VERSION
    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "_checkpoint_row",
    "_epoch_integrity",
    "_generation_anchored_production_ws_run",
    "_save_checkpoint",
    "_snapshot_robinhood_proof_refresh",
    "_strict_live_epoch_active",
    "install_batch9_continuity_frontier_proof_repair",
]
