from __future__ import annotations

import asyncio
import hashlib
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Any

from . import coverage_completeness_repair as coverage
from . import funding_provenance_repair as funding
from . import launch_coverage_bridge as bridge
from . import launch_ws_frontier_timing_repair as frontier
from . import production_capacity_repair as capacity
from . import continuity_recovery_isolation_repair as recovery_isolation
from .direct_solana import DirectSolanaIngestionPlane
from .launch_funding import FundingSource
from .solana_rpc import SolanaRpcPool


FUNDING_TRANSACTION_CHUNK_SIZE = 8
FUNDING_TRANSACTION_GLOBAL_CONCURRENCY = 4
FUNDING_TRANSACTION_CACHE_MAX = 4096

_PRE_HOTPATH_HYDRATE_MINT_LAUNCH_CONTEXT = bridge._hydrate_mint_launch_context
_PRE_HOTPATH_RECOVERY_RPC = recovery_isolation._recovery_rpc


def _increment(obj: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_production_hotpath_{name}"
    setattr(obj, attr, int(getattr(obj, attr, 0) or 0) + int(amount))


def _persist_background_batch_aggregated(self: Any, items: list[Any]) -> int:
    """Persist ordinary raw receipts with one minute-state read/write per bucket.

    Every unique raw receipt is still inserted individually under the canonical
    UNIQUE(signature, source_key) constraint. The optimization only removes the
    redundant SELECT+UPDATE pair that previously ran for every receipt in the same
    minute/source bucket. Rolling hashes are advanced in the exact dispatch order.
    """

    journal = self.journal
    inserted_count = 0
    provider_last: dict[str, datetime] = {}
    inserted_rows: list[tuple[str, str, int, datetime]] = []
    cleanup_at: datetime | None = None
    previous_insert_count = int(getattr(journal, "_receipt_inserts", 0) or 0)

    with self.store._lock, self.store.db:
        for item in items:
            _priority, _mono, _seq, received_at, provider, _targets, _message = capacity._parse_dispatch_item(item)
            fields = capacity._dispatch_fields(item)
            if fields is None:
                raise RuntimeError("invalid raw receipt dispatch item")
            target, slot, signature, _failed, source = fields
            source_key = source or f"SCOUT:{str(getattr(target, 'address', '') or '')}"
            expires_at = received_at + timedelta(seconds=float(journal.raw_retention_seconds))
            cur = self.store.db.execute(
                "INSERT OR IGNORE INTO direct_solana_recent_receipts("
                "signature, source_key, slot, received_at, launch_like, expires_at) VALUES (?, ?, ?, ?, 0, ?)",
                (signature, source_key, int(slot), received_at.isoformat(), expires_at.isoformat()),
            )
            previous_provider_time = provider_last.get(provider)
            if previous_provider_time is None or received_at > previous_provider_time:
                provider_last[provider] = received_at
            if cur.rowcount != 1:
                continue

            inserted_count += 1
            inserted_rows.append((signature, source_key, int(slot), received_at))
            journal._receipt_inserts = int(getattr(journal, "_receipt_inserts", 0) or 0) + 1
            if journal._receipt_inserts % 500 == 0:
                cleanup_at = received_at

        minute_states: dict[tuple[str, str], dict[str, Any]] = {}
        for signature, source_key, slot, received_at in inserted_rows:
            if source_key not in {"PUMP_FUN", "PUMP_AMM", "RAYDIUM"}:
                continue
            bucket = received_at.replace(second=0, microsecond=0).isoformat()
            key = (bucket, source_key)
            state = minute_states.get(key)
            if state is None:
                row = self.store.db.execute(
                    "SELECT receipt_count, rolling_sha256 "
                    "FROM direct_solana_minute_receipts WHERE bucket=? AND source=?",
                    key,
                ).fetchone()
                _increment(self, "minute_state_reads")
                state = {
                    "existed": row is not None,
                    "count": int(row["receipt_count"] or 0) if row is not None else 0,
                    "hash": str(row["rolling_sha256"]) if row is not None else "",
                    "first_received_at": received_at.isoformat(),
                    "last_received_at": received_at.isoformat(),
                    "last_slot": int(slot),
                }
                minute_states[key] = state

            digest = hashlib.sha256(
                f"{state['hash']}|{signature}|{int(slot)}|{received_at.isoformat()}".encode("utf-8")
            ).hexdigest()
            state["count"] = int(state["count"]) + 1
            state["hash"] = digest
            state["last_received_at"] = received_at.isoformat()
            state["last_slot"] = int(slot)

        for (bucket, source_key), state in minute_states.items():
            if bool(state["existed"]):
                self.store.db.execute(
                    "UPDATE direct_solana_minute_receipts SET receipt_count=?, "
                    "last_received_at=?, last_slot=?, rolling_sha256=? "
                    "WHERE bucket=? AND source=?",
                    (
                        int(state["count"]),
                        str(state["last_received_at"]),
                        int(state["last_slot"]),
                        str(state["hash"]),
                        bucket,
                        source_key,
                    ),
                )
            else:
                self.store.db.execute(
                    "INSERT INTO direct_solana_minute_receipts("
                    "bucket, source, receipt_count, first_received_at, last_received_at, last_slot, rolling_sha256) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        bucket,
                        source_key,
                        int(state["count"]),
                        str(state["first_received_at"]),
                        str(state["last_received_at"]),
                        int(state["last_slot"]),
                        str(state["hash"]),
                    ),
                )
            _increment(self, "minute_state_writes")

        if cleanup_at is not None:
            self.store.db.execute(
                "DELETE FROM direct_solana_recent_receipts WHERE expires_at<?",
                (cleanup_at.isoformat(),),
            )
            _increment(self, "retention_cleanups")

        for provider, received_at in provider_last.items():
            self.store.db.execute(
                "UPDATE direct_solana_provider_state SET last_message_at=? WHERE provider=?",
                (received_at.isoformat(), provider),
            )

    _increment(self, "background_batches")
    _increment(self, "background_receipts_inserted", inserted_count)
    if inserted_count and previous_insert_count // 500 != int(journal._receipt_inserts) // 500:
        _increment(self, "batches_crossing_retention_boundary")
    return inserted_count


def _funding_transaction_cache(self: Any) -> OrderedDict[str, dict[str, Any]]:
    value = getattr(self, "_roi_production_hotpath_funding_tx_cache", None)
    if isinstance(value, OrderedDict):
        return value
    value = OrderedDict()
    setattr(self, "_roi_production_hotpath_funding_tx_cache", value)
    return value


def _funding_transaction_gate(self: Any) -> asyncio.Semaphore:
    value = getattr(self, "_roi_production_hotpath_funding_tx_gate", None)
    if isinstance(value, asyncio.Semaphore):
        return value
    value = asyncio.Semaphore(FUNDING_TRANSACTION_GLOBAL_CONCURRENCY)
    setattr(self, "_roi_production_hotpath_funding_tx_gate", value)
    return value


async def _funding_transaction_cached(self: Any, signature: str) -> dict[str, Any]:
    cache = _funding_transaction_cache(self)
    cached = cache.get(signature)
    if isinstance(cached, dict):
        cache.move_to_end(signature)
        _increment(self, "funding_tx_cache_hits")
        return cached

    async with _funding_transaction_gate(self):
        cached = cache.get(signature)
        if isinstance(cached, dict):
            cache.move_to_end(signature)
            _increment(self, "funding_tx_cache_hits")
            return cached

        _increment(self, "funding_tx_rpc_reads")
        tx = await funding._transaction_with_retry(self, signature)
        if not isinstance(tx, dict):
            raise RuntimeError("confirmed funding transaction unavailable")
        cache[signature] = tx
        cache.move_to_end(signature)
        while len(cache) > FUNDING_TRANSACTION_CACHE_MAX:
            cache.popitem(last=False)
        return tx


def _funding_source_from_transaction(
    self: Any,
    *,
    wallet: str,
    before_at: datetime,
    row_block_time: int,
    tx: dict[str, Any],
    threshold_lamports: int,
) -> FundingSource | None:
    try:
        tx_block_time = int(tx.get("blockTime") or row_block_time or 0)
    except (TypeError, ValueError):
        tx_block_time = row_block_time
    transfer_at = datetime.fromtimestamp(tx_block_time, tz=timezone.utc) if tx_block_time > 0 else before_at
    candidates = [
        (source, lamports)
        for source, lamports in funding._native_inbound_transfers_extended(tx, wallet)
        if lamports >= threshold_lamports
    ]
    if not candidates:
        return None
    source, lamports = max(candidates, key=lambda item: item[1])
    return FundingSource(wallet, source, lamports / 1_000_000_000, transfer_at)


async def _funding_source_result_concurrent(
    self: Any,
    wallet: str,
    before_at: datetime,
    before_slot: int,
) -> tuple[FundingSource | None, bool, str]:
    """Trace the latest qualifying funder with bounded parallel transaction reads.

    Signature pages, slot chronology, the seven-day lookback, 5x1000 history cap,
    minimum transfer threshold, retries, and fail-closed behavior are unchanged.
    Only getTransaction calls within the next newest-first chunk are overlapped.
    Results are consumed in original chain-history order, so a later transaction
    can never outrank an earlier qualifying funding source.
    """

    start_at = before_at - timedelta(days=self.policy.funding_lookback_days)
    before_signature: str | None = None
    threshold_lamports = int(self.policy.min_funding_transfer_sol * 1_000_000_000)

    async def resolve_chunk(chunk: list[tuple[str, int]]) -> tuple[FundingSource | None, str | None]:
        if not chunk:
            return None, None
        _increment(self, "funding_tx_chunks")
        results = await asyncio.gather(
            *(_funding_transaction_cached(self, signature) for signature, _block_time in chunk),
            return_exceptions=True,
        )
        for (_signature, block_time), result in zip(chunk, results):
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, BaseException):
                return None, f"transaction_rpc:{type(result).__name__}"
            if not isinstance(result, dict):
                return None, "transaction_unavailable"
            source = _funding_source_from_transaction(
                self,
                wallet=wallet,
                before_at=before_at,
                row_block_time=block_time,
                tx=result,
                threshold_lamports=threshold_lamports,
            )
            if source is not None:
                return source, None
        return None, None

    for _page_index in range(self.policy.max_history_pages):
        try:
            rows = await funding._signature_page_with_retry(self, wallet, before=before_signature)
        except Exception as exc:
            return None, False, f"signature_history_rpc:{type(exc).__name__}"
        if not rows:
            return None, True, "history_exhausted"

        chunk: list[tuple[str, int]] = []
        boundary_reached = False

        async def flush() -> tuple[FundingSource | None, str | None]:
            nonlocal chunk
            current = chunk
            chunk = []
            return await resolve_chunk(current)

        for row in rows:
            try:
                row_slot = int(row.get("slot") or 0)
            except (TypeError, ValueError):
                row_slot = 0
            try:
                block_time = int(row.get("blockTime") or 0)
            except (TypeError, ValueError):
                block_time = 0
            observed = datetime.fromtimestamp(block_time, tz=timezone.utc) if block_time > 0 else None

            if before_slot > 0 and row_slot > 0:
                if row_slot >= before_slot:
                    continue
                if observed is not None and int(observed.timestamp()) == int(before_at.timestamp()):
                    funding._increment(self, "same_second_prebuy_rows")
            else:
                if observed is None:
                    source, error = await flush()
                    if source is not None:
                        return source, True, "latest_qualifying_source_found"
                    if error is not None:
                        return None, False, error
                    return None, False, "history_row_missing_slot_and_block_time"
                if observed >= before_at:
                    continue

            if observed is not None and observed < start_at:
                source, error = await flush()
                if source is not None:
                    return source, True, "latest_qualifying_source_found"
                if error is not None:
                    return None, False, error
                boundary_reached = True
                break

            if row.get("err") is not None:
                continue
            signature = str(row.get("signature") or "")
            if not signature:
                continue
            chunk.append((signature, block_time))
            if len(chunk) >= FUNDING_TRANSACTION_CHUNK_SIZE:
                source, error = await flush()
                if source is not None:
                    return source, True, "latest_qualifying_source_found"
                if error is not None:
                    return None, False, error

        source, error = await flush()
        if source is not None:
            return source, True, "latest_qualifying_source_found"
        if error is not None:
            return None, False, error

        if boundary_reached:
            return None, True, "lookback_boundary_reached"
        if len(rows) < 1000:
            return None, True, "provider_history_exhausted_short_page"
        before_signature = str(rows[-1].get("signature") or "")
        if not before_signature:
            return None, False, "history_pagination_cursor_missing"

    return None, False, "history_page_cap_exhausted"


def _recovery_rpc_with_urgent_hedging(self: Any) -> SolanaRpcPool:
    pool = _PRE_HOTPATH_RECOVERY_RPC(self)
    setattr(pool, "_roi_urgent_gap_recovery_pool", True)
    return pool


async def _capacity_call_with_urgent_recovery(
    self: SolanaRpcPool,
    method: str,
    params: list[Any],
    *,
    hedge: bool = False,
) -> tuple[Any, str, float]:
    if hedge and bool(getattr(self, "_roi_urgent_gap_recovery_pool", False)):
        return await capacity._ORIGINAL_RPC_CALL_WITH_META(self, method, params, hedge=True)
    return await capacity._capacity_call_with_meta(self, method, params, hedge=hedge)


async def _hydrate_mint_launch_context_final_attestation(
    self: Any,
    *,
    mint: str,
    source: str,
    launch_signature: str,
    created_at: datetime,
) -> tuple[int, bool, int]:
    """Guarantee the final v7 timing layer retains launch/funding completeness."""

    persisted, complete, candidate_count = await _PRE_HOTPATH_HYDRATE_MINT_LAUNCH_CONTEXT(
        self,
        mint=mint,
        source=source,
        launch_signature=launch_signature,
        created_at=created_at,
    )

    signature_window_bounded = candidate_count < max(1, bridge.LAUNCH_CONTEXT_SIGNATURE_LIMIT - 1)
    attested_complete = bool(complete and signature_window_bounded)
    raw = bridge._raw_collectors(self)
    launch = getattr(raw, "launch", None)
    funding_collector = getattr(raw, "funding", None)
    launch_context = coverage._launch_contexts(launch).get(mint) if launch is not None else None
    funding_context = coverage._funding_contexts(funding_collector).get(mint) if funding_collector is not None else None
    has_final_signature = bool(
        isinstance(launch_context, dict)
        and str(launch_context.get("launch_signature") or "") == launch_signature
    )
    needs_handoff = (
        not isinstance(launch_context, dict)
        or funding_context is None
        or not has_final_signature
        or bool(launch_context.get("complete")) != attested_complete
    )
    if needs_handoff:
        observed_at = coverage._queue_trigger_received_at(self, launch_signature)
        if observed_at is not None and coverage._seed_runtime_collectors(
            self,
            mint=mint,
            created_at=created_at,
            observed_at=observed_at,
            complete=attested_complete,
        ):
            launch_context = coverage._launch_contexts(launch).get(mint) if launch is not None else None
            if isinstance(launch_context, dict):
                row = frontier._frontier_row(self.store, launch_signature) or {}
                launch_context["launch_signature"] = launch_signature
                launch_context["launch_slot"] = int(row.get("launch_slot") or 0)
            _increment(self, "final_attestation_handoffs")
        else:
            _increment(self, "final_attestation_unavailable")

    return persisted, attested_complete, candidate_count


def _status_with_production_hotpath(original: Any) -> Any:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        dispatch = payload.get("raw_receipt_dispatch")
        if isinstance(dispatch, dict):
            dispatch.update(
                {
                    "minute_state_aggregation": True,
                    "minute_state_reads": int(getattr(self, "_roi_production_hotpath_minute_state_reads", 0) or 0),
                    "minute_state_writes": int(getattr(self, "_roi_production_hotpath_minute_state_writes", 0) or 0),
                    "background_batches_aggregated": int(getattr(self, "_roi_production_hotpath_background_batches", 0) or 0),
                    "background_receipts_inserted_aggregated": int(
                        getattr(self, "_roi_production_hotpath_background_receipts_inserted", 0) or 0
                    ),
                    "raw_scope_or_durability_reduced": False,
                }
            )
        launch_bridge = payload.get("launch_coverage_bridge")
        if isinstance(launch_bridge, dict):
            launch_bridge.update(
                {
                    "funding_transaction_chunk_size": FUNDING_TRANSACTION_CHUNK_SIZE,
                    "funding_transaction_global_concurrency": FUNDING_TRANSACTION_GLOBAL_CONCURRENCY,
                    "funding_transaction_cache_max": FUNDING_TRANSACTION_CACHE_MAX,
                    "final_v7_attestation_handoff": True,
                    "final_attestation_handoffs": int(
                        getattr(self, "_roi_production_hotpath_final_attestation_handoffs", 0) or 0
                    ),
                    "final_attestation_unavailable": int(
                        getattr(self, "_roi_production_hotpath_final_attestation_unavailable", 0) or 0
                    ),
                    "coverage_thresholds_unchanged": True,
                }
            )
            raw = bridge._raw_collectors(self)
            funding_collector = getattr(raw, "funding", None)
            if funding_collector is not None:
                launch_bridge.update(
                    {
                        "funding_tx_rpc_reads": int(
                            getattr(funding_collector, "_roi_production_hotpath_funding_tx_rpc_reads", 0) or 0
                        ),
                        "funding_tx_cache_hits": int(
                            getattr(funding_collector, "_roi_production_hotpath_funding_tx_cache_hits", 0) or 0
                        ),
                        "funding_tx_chunks": int(
                            getattr(funding_collector, "_roi_production_hotpath_funding_tx_chunks", 0) or 0
                        ),
                    }
                )
        poll = payload.get("live_poll_redundancy")
        if isinstance(poll, dict):
            poll["real_gap_recovery_urgent_hedging"] = True
            poll["real_gap_recovery_lease_unchanged"] = True
            poll["real_gap_recovery_hard_delta_bound_unchanged"] = True
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "raw_receipt_minute_updates_aggregated": True,
                    "funding_transaction_reads_bounded_parallel": True,
                    "urgent_real_gap_recovery_hedged": True,
                    "routine_official_public_proactive_hedge_disabled": True,
                    "certification_thresholds_unchanged": True,
                    "paper_only_authority_unchanged": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_production_hotpath_repair", True)
    return status


def install_production_hotpath_repair() -> None:
    """Repair the observed production capacity boundaries without weakening gates."""

    capacity._persist_background_batch = _persist_background_batch_aggregated  # type: ignore[assignment]
    funding._funding_source_result_slot_aware = _funding_source_result_concurrent  # type: ignore[assignment]
    recovery_isolation._recovery_rpc = _recovery_rpc_with_urgent_hedging  # type: ignore[assignment]
    SolanaRpcPool.call_with_meta = _capacity_call_with_urgent_recovery  # type: ignore[method-assign]
    bridge._hydrate_mint_launch_context = _hydrate_mint_launch_context_final_attestation  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_production_hotpath_repair", False)):
        DirectSolanaIngestionPlane.status = _status_with_production_hotpath(current_status)  # type: ignore[method-assign]


__all__ = [
    "FUNDING_TRANSACTION_CACHE_MAX",
    "FUNDING_TRANSACTION_CHUNK_SIZE",
    "FUNDING_TRANSACTION_GLOBAL_CONCURRENCY",
    "install_production_hotpath_repair",
    "_capacity_call_with_urgent_recovery",
    "_funding_source_result_concurrent",
    "_hydrate_mint_launch_context_final_attestation",
    "_persist_background_batch_aggregated",
]
