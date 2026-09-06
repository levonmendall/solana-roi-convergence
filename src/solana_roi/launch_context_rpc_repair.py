from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from . import direct_solana as direct_solana_module
from . import launch_coverage_bridge as bridge
from . import launch_reference_timing_repair as legacy_reference
from .direct_solana import DirectSolanaIngestionPlane


CONTEXT_TRANSACTION_RPC_ROUNDS = 3
CONTEXT_TRANSACTION_RETRY_DELAY_SECONDS = 0.05


def _increment(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_launch_context_rpc_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


def _adjust(self: Any, name: str, amount: int) -> None:
    attr = f"_roi_launch_context_rpc_{name}"
    current = int(getattr(self, attr, 0) or 0)
    setattr(self, attr, max(0, current + int(amount)))


def _requeue_cancelled_launch(self: Any, launch_signature: str) -> None:
    """Persist an explicit recoverable disposition before cancellation propagates."""

    journal = getattr(self, "journal", None)
    finish = getattr(journal, "finish", None)
    if not callable(finish):
        _increment(self, "cancellation_accounting_failures")
        return
    try:
        finish(
            launch_signature,
            error="CancelledError: launch context acquisition interrupted; retry required",
            retry=True,
        )
        _increment(self, "cancelled_launches_requeued")
    except Exception:
        _increment(self, "cancellation_accounting_failures")


async def _drain_context_tasks(
    self: Any,
    tasks: list[asyncio.Task[None]],
    aggregate: asyncio.Future[Any] | None,
) -> None:
    """Cancel and retrieve the complete child/aggregate task graph.

    The aggregate is explicitly retained by the caller so an outer cancellation
    cannot leave a `_GatheringFuture` whose terminal CancelledError is never
    retrieved. Child tasks are always awaited with return_exceptions=True before
    the aggregate itself is observed.
    """

    for task in tasks:
        if not task.done():
            task.cancel()
    if aggregate is not None and not aggregate.done():
        aggregate.cancel()
    try:
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if aggregate is not None:
            await asyncio.gather(aggregate, return_exceptions=True)
    except asyncio.CancelledError:
        # A second cancellation while cleanup is executing must still be visible
        # rather than converted to a successful context result.
        _increment(self, "cleanup_failures")
        raise
    except BaseException:
        _increment(self, "cleanup_failures")
        raise


async def _secondary_non_null_read(
    self: Any,
    signature: str,
    first_provider: str | None,
) -> tuple[dict[str, Any] | None, str | None, float | None]:
    rpc = self.rpc
    params = [
        signature,
        {
            "encoding": "jsonParsed",
            "commitment": "confirmed",
            "maxSupportedTransactionVersion": 0,
        },
    ]
    for endpoint in list(rpc._ordered("getTransaction")):  # type: ignore[attr-defined]
        if first_provider and endpoint.name == first_provider:
            continue
        try:
            result, provider, latency = await rpc._call_endpoint(  # type: ignore[attr-defined]
                endpoint,
                "getTransaction",
                params,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            _increment(self, "secondary_errors")
            continue
        if isinstance(result, dict):
            _increment(self, "secondary_non_null_recoveries")
            return result, provider, latency
    return None, None, None


async def _context_transaction_ready(
    self: Any,
    signature: str,
) -> tuple[dict[str, Any], str | None, float | None]:
    last_error: Exception | None = None
    for round_index in range(CONTEXT_TRANSACTION_RPC_ROUNDS):
        _increment(self, "read_rounds")
        try:
            result, provider, latency = await self.rpc.get_transaction(signature, hedge=True)
            if isinstance(result, dict):
                if round_index:
                    _increment(self, "recovered_after_retry")
                return result, provider, latency
            _increment(self, "null_results")
            secondary, secondary_provider, secondary_latency = await _secondary_non_null_read(
                self,
                signature,
                provider,
            )
            if isinstance(secondary, dict):
                if round_index:
                    _increment(self, "recovered_after_retry")
                return secondary, secondary_provider, secondary_latency
            last_error = RuntimeError("confirmed launch-window transaction unavailable across configured RPC endpoints")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_error = exc
            _increment(self, "round_errors")

        if round_index + 1 < CONTEXT_TRANSACTION_RPC_ROUNDS:
            await asyncio.sleep(CONTEXT_TRANSACTION_RETRY_DELAY_SECONDS)

    _increment(self, "exhausted")
    raise last_error or RuntimeError("confirmed launch-window transaction unavailable")


async def _hydrate_mint_launch_context_with_retry(
    self: Any,
    *,
    mint: str,
    source: str,
    launch_signature: str,
    created_at: datetime,
) -> tuple[int, bool, int]:
    """Hydrate the unchanged immutable launch window with bounded resilient reads.

    Candidate eligibility is still fixed by Solana blockTime inside the established
    eight-second event window. Only retrieval reliability changes: a transient RPC
    exception or successful-null result gets bounded retries and an explicit sibling
    endpoint read before the context is declared incomplete.

    Cancellation is an operational interruption, not a completed measurement. The
    durable launch hydration is therefore re-queued fail-closed, every owned child
    task is drained, and the original CancelledError is re-raised.
    """

    tasks: list[asyncio.Task[None]] = []
    aggregate: asyncio.Future[Any] | None = None
    active_accounted = False

    try:
        window_start = created_at - timedelta(seconds=1.0)
        window_end = created_at + timedelta(seconds=bridge.LAUNCH_WINDOW_SECONDS)
        now = direct_solana_module.utcnow()
        if now < window_end:
            await asyncio.sleep((window_end - now).total_seconds())

        bridge._increment(self, "signature_queries")
        rows, _provider, _latency = await self.rpc.get_signatures_for_address(
            mint,
            limit=bridge.LAUNCH_CONTEXT_SIGNATURE_LIMIT,
            hedge=True,
        )
        bridge._increment(self, "signature_queries_ok")
        bridge._increment(self, "signature_rows", len(rows))

        candidates: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            signature = str(row.get("signature") or "")
            if not signature or signature == launch_signature or row.get("err") is not None:
                continue
            try:
                block_time = int(row.get("blockTime") or 0)
            except (TypeError, ValueError):
                block_time = 0
            if block_time <= 0:
                continue
            observed_at = datetime.fromtimestamp(block_time, tz=timezone.utc)
            if window_start <= observed_at <= window_end + timedelta(seconds=1.0):
                candidates.append(row)
        bridge._increment(self, "window_candidate_rows", len(candidates))

        semaphore = asyncio.Semaphore(bridge.LAUNCH_CONTEXT_CONCURRENCY)
        persisted = 0
        rpc_failures = 0
        persisted_lock = asyncio.Lock()

        async def hydrate(row: dict[str, Any]) -> None:
            nonlocal persisted, rpc_failures
            async with semaphore:
                signature = str(row["signature"])
                block_time = int(row.get("blockTime") or 0)
                trigger = datetime.fromtimestamp(block_time, tz=timezone.utc)
                bridge._increment(self, "transaction_hydrations_attempted")
                try:
                    result, provider, latency = await _context_transaction_ready(self, signature)
                    bridge._increment(self, "transaction_hydrations_ok")
                    swap = direct_solana_module.normalize_standard_transaction(
                        result,
                        signature=signature,
                        trigger_received_at=trigger,
                        source_hint=source,
                    )
                    if swap is None or str(swap.token_mint) != mint:
                        bridge._increment(self, "normalization_misses")
                        return
                    self._persist_context_swap(swap)
                    self.journal.record_hydration(
                        signature=signature,
                        source=source,
                        trigger_received_at=trigger,
                        hydrated_at=direct_solana_module.utcnow(),
                        rpc_provider=provider,
                        rpc_latency_ms=latency,
                        normalized=True,
                        candidate_context_prefilled=True,
                        historical_recovery=False,
                    )
                    bridge._increment(self, "normalization_matches")
                    async with persisted_lock:
                        persisted += 1
                except asyncio.CancelledError:
                    raise
                except Exception:
                    async with persisted_lock:
                        rpc_failures += 1
                    bridge._increment(self, "context_rpc_failures")
                    _increment(self, "candidate_failures")

        tasks = [
            asyncio.create_task(hydrate(row), name=f"launch-context:{str(row.get('signature') or '')}")
            for row in candidates
        ]
        if not tasks:
            return 0, True, 0

        _increment(self, "child_tasks_created", len(tasks))
        _adjust(self, "active_child_tasks", len(tasks))
        active_accounted = True
        aggregate = asyncio.gather(*tasks)

        timed_out = False
        try:
            await asyncio.wait_for(
                aggregate,
                timeout=bridge.LAUNCH_CONTEXT_DEADLINE_SECONDS,
            )
        except asyncio.TimeoutError:
            timed_out = True
            bridge._increment(self, "context_timeouts")
            await _drain_context_tasks(self, tasks, aggregate)

        complete = not timed_out and rpc_failures == 0
        if not complete:
            bridge._increment(self, "context_incomplete")
        return persisted, complete, len(candidates)
    except asyncio.CancelledError:
        _increment(self, "parent_cancellations")
        bridge._increment(self, "context_incomplete")
        if tasks:
            await _drain_context_tasks(self, tasks, aggregate)
        _requeue_cancelled_launch(self, launch_signature)
        raise
    finally:
        if tasks:
            cancelled = sum(1 for task in tasks if task.cancelled())
            drained = sum(1 for task in tasks if task.done())
            orphaned = len(tasks) - drained
            if cancelled:
                _increment(self, "child_tasks_cancelled", cancelled)
            if drained:
                _increment(self, "child_tasks_drained", drained)
            if orphaned:
                _increment(self, "orphan_tasks_detected", orphaned)
            if active_accounted:
                _adjust(self, "active_child_tasks", -len(tasks))


def _status_with_context_rpc(original: Any) -> Any:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        launch_bridge = payload.get("launch_coverage_bridge")
        if isinstance(launch_bridge, dict):
            launch_bridge.update(
                {
                    "context_transaction_rpc_rounds": CONTEXT_TRANSACTION_RPC_ROUNDS,
                    "context_transaction_retry_delay_seconds": CONTEXT_TRANSACTION_RETRY_DELAY_SECONDS,
                    "context_transaction_null_result_secondary_fallback": True,
                    "context_event_window_unchanged": True,
                    "context_deadline_unchanged": True,
                    "context_parent_cancellation_fails_closed": True,
                    "context_cancelled_launch_requeued": True,
                    "context_transaction_read_rounds": int(
                        getattr(self, "_roi_launch_context_rpc_read_rounds", 0) or 0
                    ),
                    "context_transaction_null_results": int(
                        getattr(self, "_roi_launch_context_rpc_null_results", 0) or 0
                    ),
                    "context_transaction_secondary_non_null_recoveries": int(
                        getattr(self, "_roi_launch_context_rpc_secondary_non_null_recoveries", 0) or 0
                    ),
                    "context_transaction_recovered_after_retry": int(
                        getattr(self, "_roi_launch_context_rpc_recovered_after_retry", 0) or 0
                    ),
                    "context_transaction_candidate_failures": int(
                        getattr(self, "_roi_launch_context_rpc_candidate_failures", 0) or 0
                    ),
                    "context_transaction_exhausted": int(
                        getattr(self, "_roi_launch_context_rpc_exhausted", 0) or 0
                    ),
                    "context_parent_cancellations": int(
                        getattr(self, "_roi_launch_context_rpc_parent_cancellations", 0) or 0
                    ),
                    "context_child_tasks_created": int(
                        getattr(self, "_roi_launch_context_rpc_child_tasks_created", 0) or 0
                    ),
                    "context_child_tasks_cancelled": int(
                        getattr(self, "_roi_launch_context_rpc_child_tasks_cancelled", 0) or 0
                    ),
                    "context_child_tasks_drained": int(
                        getattr(self, "_roi_launch_context_rpc_child_tasks_drained", 0) or 0
                    ),
                    "context_active_child_tasks": int(
                        getattr(self, "_roi_launch_context_rpc_active_child_tasks", 0) or 0
                    ),
                    "context_orphan_tasks_detected": int(
                        getattr(self, "_roi_launch_context_rpc_orphan_tasks_detected", 0) or 0
                    ),
                    "context_cleanup_failures": int(
                        getattr(self, "_roi_launch_context_rpc_cleanup_failures", 0) or 0
                    ),
                    "context_cancelled_launches_requeued": int(
                        getattr(self, "_roi_launch_context_rpc_cancelled_launches_requeued", 0) or 0
                    ),
                    "context_cancellation_accounting_failures": int(
                        getattr(self, "_roi_launch_context_rpc_cancellation_accounting_failures", 0) or 0
                    ),
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_launch_context_rpc_repair", True)
    return status


def install_launch_context_rpc_repair() -> None:
    # v7's active wrapper resolves this module-level base hydrator dynamically.
    # Replace only that immutable-window acquisition function; v7 timing/frontier
    # semantics remain untouched.
    legacy_reference._PRE_CHAIN_HYDRATE_MINT_LAUNCH_CONTEXT = _hydrate_mint_launch_context_with_retry  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_launch_context_rpc_repair", False)):
        DirectSolanaIngestionPlane.status = _status_with_context_rpc(current_status)  # type: ignore[method-assign]
