from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from . import direct_solana as direct_solana_module
from . import target_quorum
from . import target_stream_fanout as fanout
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget
from .solana_rpc import RpcEndpoint, SolanaRpcPool


POLL_PROVIDER_NAME = "rpc-live-poll"
POLL_INTERVAL_SECONDS = 4.0
POLL_LIMIT = 1000
POLL_CURSOR_MAX_PAGES = 3
POLL_FALLBACK_STALE_SECONDS = 30.0

# Synthetic endpoint used only to participate in the existing target-quorum state
# machine. Its URLs are never called; actual reads use a dedicated read-only RPC
# pool built from the same two hydration endpoints so polling health/latency does
# not distort candidate hydration provider ordering.
_POLL_ENDPOINT = RpcEndpoint(
    name=POLL_PROVIDER_NAME,
    http_url="https://rpc-live-poll.invalid",
    ws_url="wss://rpc-live-poll.invalid",
)


def _poll_state(self: Any) -> dict[str, Any]:
    state = getattr(self, "_roi_live_poll_state", None)
    if not isinstance(state, dict):
        state = {}
        setattr(self, "_roi_live_poll_state", state)
    return state


def _poll_target_key(target: WatchTarget) -> str:
    return f"{target.kind}:{target.address}"


def _poll_rpc(self: Any) -> SolanaRpcPool:
    pool = getattr(self, "_roi_live_poll_rpc_pool", None)
    if isinstance(pool, SolanaRpcPool):
        return pool
    endpoints = tuple(getattr(self.rpc, "endpoints", ()) or ())
    pool = SolanaRpcPool(
        endpoints,
        timeout_seconds=2.5,
        hedge_delay_seconds=0.15,
    )
    setattr(self, "_roi_live_poll_rpc_pool", pool)
    return pool


async def _close_poll_rpc(self: Any) -> None:
    pool = getattr(self, "_roi_live_poll_rpc_pool", None)
    if not isinstance(pool, SolanaRpcPool):
        return
    clients = list(getattr(pool, "_clients", {}).values())
    if clients:
        await asyncio.gather(*(client.aclose() for client in clients), return_exceptions=True)


def _ws_target_covered(self: Any, target: WatchTarget) -> bool:
    """Return whether a real WebSocket provider currently covers this target."""

    _lock, provider_targets, _events, _states = fanout._state_maps(self)
    key = fanout._target_key(target)
    return any(
        key in targets
        for provider, targets in provider_targets.items()
        if provider != POLL_PROVIDER_NAME
    )


async def _poll_page(
    self: Any,
    target: WatchTarget,
    *,
    before: str | None = None,
    until: str | None = None,
    limit: int = POLL_LIMIT,
) -> tuple[list[dict[str, Any]], str, float]:
    config: dict[str, Any] = {
        "commitment": "confirmed",
        "limit": max(1, min(1000, int(limit))),
    }
    if before:
        config["before"] = before
    if until:
        # Server-side cursor bounding is critical on high-throughput program
        # addresses. It prevents every poll from downloading the same latest
        # 100/1000 historical signatures just to search for the prior cursor.
        config["until"] = until
    result, provider, latency = await _poll_rpc(self).call_with_meta(
        "getSignaturesForAddress",
        [target.address, config],
        hedge=False,
    )
    rows = [row for row in result if isinstance(row, dict)] if isinstance(result, list) else []
    return rows, provider, latency


async def _fetch_delta(
    self: Any,
    target: WatchTarget,
    cursor: str,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    """Fetch only signatures newer than the prospective cursor, oldest first."""

    pages: list[list[dict[str, Any]]] = []
    before: str | None = None
    provider: str | None = None
    latency: float | None = None
    complete = False

    for _page_index in range(POLL_CURSOR_MAX_PAGES):
        page, provider, latency = await _poll_page(
            self,
            target,
            before=before,
            until=cursor or None,
            limit=POLL_LIMIT,
        )
        pages.append(page)
        if len(page) < POLL_LIMIT:
            complete = True
            break
        before = str(page[-1].get("signature") or "") if page else None
        if not before:
            complete = True
            break

    if not complete:
        return [], False, provider, latency

    rows: list[dict[str, Any]] = []
    # RPC pages are newest-first; reverse both page order and each page so the
    # durable journal observes the delta chronologically.
    for page in reversed(pages):
        for row in reversed(page):
            signature = str(row.get("signature") or "")
            if not signature or signature == cursor:
                continue
            rows.append(row)
    return rows, True, provider, latency


async def _record_poll_rows(self: Any, target: WatchTarget, rows: list[dict[str, Any]]) -> int:
    inserted_count = 0
    source_key = target.source_hint or f"SCOUT:{target.address}"
    source_hint = str(target.source_hint or "") or None
    for row in rows:
        signature = str(row.get("signature") or "")
        if not signature:
            continue
        try:
            slot = int(row.get("slot") or 0)
        except (TypeError, ValueError):
            continue
        if slot <= 0:
            continue
        received_at = direct_solana_module.utcnow()
        inserted = self.journal.record_receipt(
            signature=signature,
            source_key=source_key,
            slot=slot,
            received_at=received_at,
            launch_like=False,
        )
        if not inserted or row.get("err") is not None:
            continue
        inserted_count += 1
        if target.kind == "scout":
            self.journal.enqueue(
                signature=signature,
                slot=slot,
                trigger_received_at=received_at,
                source_hint=None,
                priority=0,
                reason="frozen_scout_live_poll_trigger",
            )
        else:
            # Only poll rows that actually bridge loss of both WebSocket copies are
            # queued. They are background work, so they cannot consume the three
            # candidate-reserved workers. Hydration inspects logs and keeps ordinary
            # non-launch swaps lightweight while preserving deep analysis for a real
            # launch.
            self.journal.enqueue(
                signature=signature,
                slot=slot,
                trigger_received_at=received_at,
                source_hint=source_hint,
                priority=10,
                reason="live_poll_fallback",
            )
    return inserted_count


def _expire_poll_fallback(self: Any, *, all_pending: bool = False) -> int:
    now = direct_solana_module.utcnow()
    sql = (
        "UPDATE direct_solana_hydration_queue SET status='failed', last_error=?, updated_at=? "
        "WHERE status='pending' AND reason='live_poll_fallback'"
    )
    args: list[Any] = [
        "stale live-poll fallback expired fail-closed; fresh prospective evidence required",
        now.isoformat(),
    ]
    if not all_pending:
        sql += " AND trigger_received_at<?"
        args.append((now - timedelta(seconds=POLL_FALLBACK_STALE_SECONDS)).isoformat())
    with self.store._lock, self.store.db:
        cur = self.store.db.execute(sql, tuple(args))
    expired = int(cur.rowcount or 0)
    total = int(getattr(self, "_roi_live_poll_expired_total", 0) or 0) + expired
    setattr(self, "_roi_live_poll_expired_total", total)
    return expired


async def _fallback_cleanup(self: Any, stop: asyncio.Event) -> None:
    # Any queued fallback surviving process start belongs to an interrupted prior
    # observation interval and cannot be valid low-latency prospective evidence.
    _expire_poll_fallback(self, all_pending=True)
    while not stop.is_set():
        _expire_poll_fallback(self)
        try:
            await asyncio.wait_for(stop.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            continue


async def _poll_target(self: Any, target: WatchTarget, stop: asyncio.Event) -> None:
    state = _poll_state(self)
    key = _poll_target_key(target)
    cursor = ""
    initialized = False
    failures = 0
    poll_only_total = 0
    suppressed_total = 0

    while not stop.is_set():
        started = time.monotonic()
        try:
            if not initialized:
                # Baseline with one signature only. Existing history is never
                # relabeled as live evidence on a new exact-release epoch.
                baseline, provider, latency = await _poll_page(self, target, limit=1)
                cursor = str(baseline[0].get("signature") or "") if baseline else ""
                initialized = True
                failures = 0
                state[key] = {
                    "connected": True,
                    "baseline_established": True,
                    "last_provider": provider,
                    "last_latency_ms": latency,
                    "last_success_at": direct_solana_module.utcnow().isoformat(),
                    "poll_only_receipts_total": poll_only_total,
                    "suppressed_while_websocket_covered_total": suppressed_total,
                    "cursor_overflow": False,
                    "failures": failures,
                }
                await target_quorum._quorum_set_target_state(self, _POLL_ENDPOINT, target, connected=True)
            else:
                new_rows, complete, provider, latency = await _fetch_delta(self, target, cursor)
                if not complete:
                    failures += 1
                    state[key] = {
                        "connected": False,
                        "baseline_established": True,
                        "last_provider": provider,
                        "last_latency_ms": latency,
                        "last_success_at": direct_solana_module.utcnow().isoformat(),
                        "poll_only_receipts_total": poll_only_total,
                        "suppressed_while_websocket_covered_total": suppressed_total,
                        "cursor_overflow": True,
                        "failures": failures,
                    }
                    await target_quorum._quorum_set_target_state(
                        self,
                        _POLL_ENDPOINT,
                        target,
                        connected=False,
                        error_type="LivePollCursorOverflow",
                        error_message=(
                            f"live polling cursor exceeded bounded {POLL_CURSOR_MAX_PAGES}x{POLL_LIMIT} delta window"
                        ),
                    )
                else:
                    newest = str(new_rows[-1].get("signature") or "") if new_rows else cursor
                    if new_rows:
                        if _ws_target_covered(self, target):
                            # WebSocket delivery is already authoritative for this
                            # target. Advance the independent poll cursor, but do not
                            # duplicate program-wide hydration/risk work.
                            suppressed_total += len(new_rows)
                        else:
                            inserted = await _record_poll_rows(self, target, new_rows)
                            poll_only_total += inserted
                    cursor = newest
                    failures = 0
                    state[key] = {
                        "connected": True,
                        "baseline_established": True,
                        "last_provider": provider,
                        "last_latency_ms": latency,
                        "last_success_at": direct_solana_module.utcnow().isoformat(),
                        "poll_only_receipts_total": poll_only_total,
                        "suppressed_while_websocket_covered_total": suppressed_total,
                        "cursor_overflow": False,
                        "failures": failures,
                    }
                    await target_quorum._quorum_set_target_state(self, _POLL_ENDPOINT, target, connected=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failures += 1
            previous = state.get(key)
            previous_success = previous.get("last_success_at") if isinstance(previous, dict) else None
            state[key] = {
                "connected": False,
                "baseline_established": initialized,
                "last_provider": None,
                "last_latency_ms": None,
                "last_success_at": previous_success,
                "poll_only_receipts_total": poll_only_total,
                "suppressed_while_websocket_covered_total": suppressed_total,
                "cursor_overflow": False,
                "failures": failures,
                "last_error_type": type(exc).__name__,
            }
            await target_quorum._quorum_set_target_state(
                self,
                _POLL_ENDPOINT,
                target,
                connected=False,
                error_type=type(exc).__name__,
            )

        elapsed = max(0.0, time.monotonic() - started)
        await asyncio.sleep(max(0.05, POLL_INTERVAL_SECONDS - elapsed))


def _transaction_logs(result: Any) -> list[str]:
    if not isinstance(result, dict):
        return []
    meta = result.get("meta")
    if not isinstance(meta, dict):
        return []
    logs = meta.get("logMessages")
    return [str(row) for row in logs] if isinstance(logs, list) else []


def _wrap_hydrate(original: Callable[[Any, dict[str, Any]], Any]) -> Callable[[Any, dict[str, Any]], Any]:
    async def hydrate(self: Any, row: dict[str, Any]) -> None:
        if str(row.get("reason") or "") != "live_poll_fallback":
            await original(self, row)
            return

        signature = str(row["signature"])
        trigger = direct_solana_module.datetime.fromisoformat(str(row["trigger_received_at"]))
        source_hint = str(row.get("source_hint") or "") or None
        try:
            result, provider, latency = await self._get_transaction_ready(
                signature,
                hedge=False,
                attempts=3,
            )
            if result is None:
                attempts = int(row.get("attempts") or 0) + 1
                self.journal.finish(
                    signature,
                    error="confirmed live-poll transaction not yet available",
                    retry=attempts < 2,
                )
                return

            launch_like = bool(self._launch_like(_transaction_logs(result)))
            swap = direct_solana_module.normalize_standard_transaction(
                result,
                signature=signature,
                trigger_received_at=trigger,
                source_hint=source_hint,
            )
            context_prefilled = False
            if swap is not None:
                profile = self.service.registry.get(swap.wallet)
                if launch_like or profile is not None:
                    needs_context = bool(launch_like or (profile is not None and swap.side == "buy"))
                    if needs_context:
                        context_prefilled = await self._prefill_launch_context(swap)
                    await self.service.ingest_swap(swap)
                else:
                    self._persist_context_swap(swap)

            source = swap.source.split(":")[1] if swap is not None and ":" in swap.source else source_hint
            self.journal.record_hydration(
                signature=signature,
                source=source,
                trigger_received_at=trigger,
                hydrated_at=direct_solana_module.utcnow(),
                rpc_provider=provider,
                rpc_latency_ms=latency,
                normalized=swap is not None,
                candidate_context_prefilled=context_prefilled,
                historical_recovery=False,
            )
            self.journal.finish(signature)
        except Exception as exc:
            attempts = int(row.get("attempts") or 0) + 1
            self.journal.finish(
                signature,
                error=f"{type(exc).__name__}: live-poll hydration failed closed",
                retry=attempts < 2,
            )

    try:
        hydrate.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(hydrate, "_roi_live_poll_hydrate", True)
    return hydrate


def _wrap_run(original: Callable[[Any, asyncio.Event], Any]) -> Callable[[Any, asyncio.Event], Any]:
    async def run(self: Any, stop: asyncio.Event) -> None:
        poll_tasks = [
            asyncio.create_task(
                _poll_target(self, target, stop),
                name=f"direct-solana-live-poll:{target.kind}:{target.address[:8]}",
            )
            for target in tuple(self.watch_targets)
        ]
        cleanup = asyncio.create_task(_fallback_cleanup(self, stop), name="direct-solana-live-poll-cleanup")
        try:
            await original(self, stop)
        finally:
            for task in poll_tasks:
                task.cancel()
            cleanup.cancel()
            await asyncio.gather(*poll_tasks, cleanup, return_exceptions=True)
            await _close_poll_rpc(self)

    try:
        run.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(run, "_roi_live_poll_redundancy", True)
    return run


def _status_with_live_poll(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        rows = _poll_state(self)
        connected = sum(1 for row in rows.values() if isinstance(row, dict) and bool(row.get("connected")))
        suppressed = sum(
            int(row.get("suppressed_while_websocket_covered_total") or 0)
            for row in rows.values()
            if isinstance(row, dict)
        )
        poll_pool = getattr(self, "_roi_live_poll_rpc_pool", None)
        payload["live_poll_redundancy"] = {
            "enabled": True,
            "transport": "getSignaturesForAddress-continuous-live-poll",
            "commitment": "confirmed",
            "target_count": len(tuple(self.watch_targets)),
            "connected_target_count": connected,
            "poll_interval_seconds": POLL_INTERVAL_SECONDS,
            "page_limit": POLL_LIMIT,
            "max_cursor_pages": POLL_CURSOR_MAX_PAGES,
            "max_delta_rows_per_target_cycle": POLL_LIMIT * POLL_CURSOR_MAX_PAGES,
            "theoretical_base_poll_requests_per_second": len(tuple(self.watch_targets)) / POLL_INTERVAL_SECONDS,
            "server_side_until_cursor": True,
            "poll_rpc_health_isolated_from_hydration_pool": True,
            "websocket_covered_rows_suppressed_total": suppressed,
            "fallback_stale_seconds": POLL_FALLBACK_STALE_SECONDS,
            "expired_fallback_rows_total": int(getattr(self, "_roi_live_poll_expired_total", 0) or 0),
            "historical_backfill": False,
            "poll_only_program_signatures_hydrated_for_launch_detection": True,
            "signing_available": False,
            "transaction_submission_available": False,
            "poll_rpc_pool": poll_pool.status() if isinstance(poll_pool, SolanaRpcPool) else None,
            "targets": dict(rows),
        }
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "live_poll_transport_participates_in_target_quorum": True,
                    "live_poll_historical_backfill_allowed": False,
                    "live_poll_cursor_overflow_fails_target_closed": True,
                    "live_poll_server_side_cursor_bounding": True,
                    "live_poll_skips_duplicate_hydration_when_websocket_covered": True,
                    "live_poll_background_rows_expire_fail_closed": True,
                    "live_poll_rpc_health_isolated_from_candidate_hydration": True,
                    "unusable_default_drpc_stream_retired": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_live_poll_redundancy", True)
    return status


def install_live_poll_redundancy() -> None:
    current_hydrate = DirectSolanaIngestionPlane._hydrate_one
    if not bool(getattr(current_hydrate, "_roi_live_poll_hydrate", False)):
        DirectSolanaIngestionPlane._hydrate_one = _wrap_hydrate(current_hydrate)  # type: ignore[method-assign]

    current_run = DirectSolanaIngestionPlane.run
    if not bool(getattr(current_run, "_roi_live_poll_redundancy", False)):
        DirectSolanaIngestionPlane.run = _wrap_run(current_run)  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_live_poll_redundancy", False)):
        DirectSolanaIngestionPlane.status = _status_with_live_poll(current_status)  # type: ignore[method-assign]


__all__ = [
    "POLL_INTERVAL_SECONDS",
    "POLL_LIMIT",
    "POLL_CURSOR_MAX_PAGES",
    "POLL_FALLBACK_STALE_SECONDS",
    "install_live_poll_redundancy",
]
