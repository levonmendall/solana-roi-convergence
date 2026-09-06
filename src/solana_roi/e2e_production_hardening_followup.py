from __future__ import annotations

import asyncio
from typing import Any, Callable

from . import certification_hotpath_repair as certification_hotpath
from . import e2e_production_hardening_repair as base
from . import funding_provenance_repair as funding
from . import launch_chain_timing_repair as legacy_launch_timing
from . import launch_coverage_bridge as launch_bridge
from . import launch_ws_frontier_timing_repair as frontier
from . import live_poll_redundancy as live_poll
from . import robinhood_runtime_install as robinhood_runtime
from . import robinhood_worker_isolation_repair as robinhood_isolation
from . import rpc_workload_governor as governor
from .direct_solana import DirectSolanaIngestionPlane


REPAIR_VERSION = "e2e-production-hardening-followup-v1"
_INSTALLED = False
_ORIGINAL_TX: Callable[..., Any] | None = None
_ORIGINAL_SIGNATURE: Callable[..., Any] | None = None
_ORIGINAL_FINAL_FRONTIER_HYDRATE: Callable[..., Any] | None = None
_ORIGINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_START_WORKER: Callable[..., Any] | None = None
_ORIGINAL_LEGACY_TIMING_TASKS: Callable[..., dict[str, asyncio.Task[Any]]] | None = None


def _inc(obj: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_e2e_followup_{name}"
    setattr(obj, attr, int(getattr(obj, attr, 0) or 0) + int(amount))


def _inflight(self: Any, attribute: str) -> dict[str, asyncio.Task[Any]]:
    value = getattr(self, attribute, None)
    if isinstance(value, dict):
        return value
    value = {}
    setattr(self, attribute, value)
    return value


def _evict_when_done(
    owner: Any,
    mapping: dict[str, asyncio.Task[Any]],
    key: str,
    task: asyncio.Task[Any],
    counter: str,
) -> None:
    def done(completed: asyncio.Task[Any]) -> None:
        if mapping.get(key) is completed:
            mapping.pop(key, None)
            _inc(owner, counter)
        # Retrieve terminal exception so a cancelled/abandoned first waiter cannot
        # leave an unobserved-task warning while later callers still receive the
        # result through asyncio.shield.
        if not completed.cancelled():
            try:
                completed.exception()
            except BaseException:
                pass

    task.add_done_callback(done)


async def _transaction_singleflight_cleanup(self: Any, signature: str) -> dict[str, Any]:
    if _ORIGINAL_TX is None:
        raise RuntimeError("funding transaction cleanup repair not installed")
    mapping = _inflight(self, "_roi_e2e_funding_tx_inflight")
    task = mapping.get(signature)
    if isinstance(task, asyncio.Task):
        _inc(self, "funding_tx_singleflight_joins")
        return await asyncio.shield(task)

    task = asyncio.create_task(
        _ORIGINAL_TX(self, signature),
        name=f"funding-tx-singleflight-clean:{signature[:10]}",
    )
    mapping[signature] = task
    _inc(self, "funding_tx_singleflight_starts")
    _evict_when_done(self, mapping, signature, task, "funding_tx_singleflight_evictions")
    return await asyncio.shield(task)


async def _signature_singleflight_cleanup(
    self: Any,
    wallet: str,
    *,
    before: str | None,
) -> list[dict[str, Any]]:
    if _ORIGINAL_SIGNATURE is None:
        raise RuntimeError("funding signature cleanup repair not installed")
    key = f"{wallet}|{before or ''}"
    mapping = _inflight(self, "_roi_e2e_funding_signature_inflight")
    task = mapping.get(key)
    if isinstance(task, asyncio.Task):
        _inc(self, "funding_signature_singleflight_joins")
        return await asyncio.shield(task)

    task = asyncio.create_task(
        _ORIGINAL_SIGNATURE(self, wallet, before=before),
        name=f"funding-signature-singleflight-clean:{wallet[:8]}",
    )
    mapping[key] = task
    _inc(self, "funding_signature_singleflight_starts")
    _evict_when_done(self, mapping, key, task, "funding_signature_singleflight_evictions")
    return await asyncio.shield(task)


async def _final_frontier_hydrate_retry(
    self: Any,
    *,
    mint: str,
    source: str,
    launch_signature: str,
    created_at: Any,
) -> tuple[int, bool, int]:
    if _ORIGINAL_FINAL_FRONTIER_HYDRATE is None:
        raise RuntimeError("final frontier retry repair not installed")
    result = await _ORIGINAL_FINAL_FRONTIER_HYDRATE(
        self,
        mint=mint,
        source=source,
        launch_signature=launch_signature,
        created_at=created_at,
    )
    row = frontier._frontier_row(self.store, launch_signature)
    if not isinstance(row, dict):
        return result
    try:
        launch_slot = int(row.get("launch_slot") or 0)
        frontier_slot = int(row.get("frontier_slot") or 0)
    except (TypeError, ValueError):
        return result
    if launch_slot <= 0 or frontier_slot <= launch_slot or row.get("frontier_block_time") is not None:
        return result

    for attempt in range(base.FRONTIER_BLOCK_TIME_RETRIES):
        _inc(self, "final_frontier_block_time_retry_attempts")
        try:
            with governor.rpc_workload("certification"):
                value, _provider, _latency = await live_poll._poll_rpc(self).call_with_meta(
                    "getBlockTime",
                    [frontier_slot],
                    hedge=True,
                )
            if value is None:
                raise RuntimeError("preexisting WebSocket frontier blockTime unavailable")
            frontier._set_frontier_block_time(
                self.store,
                launch_signature,
                block_time=float(value),
            )
            _inc(self, "final_frontier_block_time_retry_recoveries")
            return result
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if attempt + 1 >= base.FRONTIER_BLOCK_TIME_RETRIES:
                frontier._set_frontier_block_time(
                    self.store,
                    launch_signature,
                    block_time=None,
                    error_type=type(exc).__name__,
                )
                _inc(self, "final_frontier_block_time_retry_exhausted")
            else:
                await asyncio.sleep(base.FRONTIER_BLOCK_TIME_RETRY_SECONDS)
    return result


def _legacy_timing_tasks_pruned(self: Any) -> dict[str, asyncio.Task[Any]]:
    if _ORIGINAL_LEGACY_TIMING_TASKS is None:
        return {}
    tasks = _ORIGINAL_LEGACY_TIMING_TASKS(self)
    removed = 0
    for signature, task in list(tasks.items()):
        if isinstance(task, asyncio.Task) and task.done():
            tasks.pop(signature, None)
            removed += 1
    if removed:
        _inc(self, "legacy_timing_task_evictions", removed)
    return tasks


def _start_worker_with_attempt_accounting(*args: Any, **kwargs: Any) -> Any:
    if _ORIGINAL_START_WORKER is None:
        raise RuntimeError("Robinhood supervised start accounting not installed")
    robinhood_runtime._STATE["attempts"] = int(robinhood_runtime._STATE.get("attempts", 0) or 0) + 1
    return _ORIGINAL_START_WORKER(*args, **kwargs)


def _status_with_authority_refinement(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _ORIGINAL_DIRECT_STATUS is None:
        raise RuntimeError("E2E followup status not installed")
    payload = _ORIGINAL_DIRECT_STATUS(self)
    dispatch = payload.get("raw_receipt_dispatch")
    if isinstance(dispatch, dict):
        commits = int(dispatch.get("full_scope_batch_commits") or 0)
        writer_progress = commits > 0
        worker_alive = bool(dispatch.get("worker_alive"))
        dispatch.update(
            {
                "authoritative_writer_progress_observed_session": writer_progress,
                "worker_alive_field_superseded_by_observed_full_scope_progress": (
                    writer_progress and not worker_alive
                ),
                "legacy_worker_health_is_readiness_authority": not writer_progress,
                "worker_health_interpretation": (
                    "durable full-scope batch progress is authoritative for this session"
                    if writer_progress
                    else "no full-scope batch progress yet; worker health remains actionable"
                ),
            }
        )
    hardening = payload.get("e2e_production_hardening")
    if isinstance(hardening, dict):
        collector = base._funding_collector(self)
        hardening.update(
            {
                "followup_version": REPAIR_VERSION,
                "funding_singleflight_done_callback_eviction": True,
                "funding_tx_singleflight_evictions": int(
                    getattr(collector, "_roi_e2e_followup_funding_tx_singleflight_evictions", 0) or 0
                ) if collector is not None else 0,
                "funding_signature_singleflight_evictions": int(
                    getattr(collector, "_roi_e2e_followup_funding_signature_singleflight_evictions", 0) or 0
                ) if collector is not None else 0,
                "final_frontier_retry_attached_after_all_launch_wrappers": True,
                "final_frontier_block_time_retry_recoveries": int(
                    getattr(self, "_roi_e2e_followup_final_frontier_block_time_retry_recoveries", 0) or 0
                ),
                "robinhood_supervisor_attempt_accounting": True,
            }
        )
    return payload


setattr(_status_with_authority_refinement, "_roi_e2e_production_hardening_followup", True)


def install_e2e_production_hardening_followup() -> None:
    global _INSTALLED, _ORIGINAL_TX, _ORIGINAL_SIGNATURE
    global _ORIGINAL_FINAL_FRONTIER_HYDRATE, _ORIGINAL_DIRECT_STATUS
    global _ORIGINAL_START_WORKER, _ORIGINAL_LEGACY_TIMING_TASKS
    if _INSTALLED:
        return

    # Use the pre-singleflight implementations captured by the base repair so the
    # followup replaces, rather than recursively wraps, the first implementation.
    _ORIGINAL_TX = base._ORIGINAL_FUNDING_TX_CACHED or certification_hotpath._transaction_cached
    _ORIGINAL_SIGNATURE = base._ORIGINAL_SIGNATURE_PAGE or funding._signature_page_with_retry
    certification_hotpath._transaction_cached = _transaction_singleflight_cleanup  # type: ignore[assignment]
    funding._signature_page_with_retry = _signature_singleflight_cleanup  # type: ignore[assignment]

    # Attach to the actual final bridge global after every earlier timing/attestation
    # wrapper has composed. A retry occurs only if the immutable frontier still lacks
    # the block-time proof; successful earlier work is never repeated.
    _ORIGINAL_FINAL_FRONTIER_HYDRATE = launch_bridge._hydrate_mint_launch_context
    launch_bridge._hydrate_mint_launch_context = _final_frontier_hydrate_retry  # type: ignore[assignment]

    _ORIGINAL_LEGACY_TIMING_TASKS = legacy_launch_timing._timing_tasks
    legacy_launch_timing._timing_tasks = _legacy_timing_tasks_pruned  # type: ignore[assignment]

    _ORIGINAL_START_WORKER = robinhood_isolation._start_worker_thread
    robinhood_isolation._start_worker_thread = _start_worker_with_attempt_accounting  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_e2e_production_hardening_followup", False)):
        _ORIGINAL_DIRECT_STATUS = current_status
        try:
            _status_with_authority_refinement.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        DirectSolanaIngestionPlane.status = _status_with_authority_refinement  # type: ignore[method-assign]

    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "funding_singleflight_done_callback_eviction": True,
        "final_frontier_retry_attached_after_all_launch_wrappers": True,
        "raw_dispatch_worker_health_only_superseded_by_observed_writer_progress": True,
        "robinhood_supervisor_attempt_accounting": True,
        "strategy_thresholds_changed": False,
        "certification_thresholds_changed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = ["REPAIR_VERSION", "install_e2e_production_hardening_followup", "status"]
