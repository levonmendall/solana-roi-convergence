from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from . import direct_solana as direct_solana_module
from . import target_quorum
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget
from .solana_rpc import RpcEndpoint


POLL_PROVIDER_NAME = "rpc-live-poll"
POLL_INTERVAL_SECONDS = 2.0
POLL_LIMIT = 100
POLL_CURSOR_MAX_PAGES = 3

# Synthetic endpoint used only to participate in the existing target-quorum state
# machine. Its URLs are never called; actual reads use the already-constructed
# read-only SolanaRpcPool so no signing/submission authority is introduced.
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


def _new_rows_until_cursor(
    pages: list[list[dict[str, Any]]],
    cursor: str,
) -> tuple[list[dict[str, Any]], bool]:
    """Return rows newer than cursor, oldest-first, and whether cursor was found."""

    new_rows: list[dict[str, Any]] = []
    for page in pages:
        for row in page:
            signature = str(row.get("signature") or "")
            if not signature:
                continue
            if signature == cursor:
                return list(reversed(new_rows)), True
            new_rows.append(row)
    return [], False


async def _poll_page(self: Any, target: WatchTarget, *, before: str | None = None) -> tuple[list[dict[str, Any]], str, float]:
    return await self.rpc.get_signatures_for_address(
        target.address,
        before=before,
        limit=POLL_LIMIT,
        hedge=False,
    )


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
            # A poll-only program signature is hydrated so its transaction logs can
            # determine whether it is a launch. Non-launch swaps stay lightweight;
            # launch/scout activity retains the full deep-analysis path.
            self.journal.enqueue(
                signature=signature,
                slot=slot,
                trigger_received_at=received_at,
                source_hint=source_hint,
                priority=5,
                reason="live_poll_fallback",
            )
    return inserted_count


async def _poll_target(self: Any, target: WatchTarget, stop: asyncio.Event) -> None:
    state = _poll_state(self)
    key = _poll_target_key(target)
    cursor = ""
    initialized = False
    failures = 0
    poll_only_total = 0
    while not stop.is_set():
        started = time.monotonic()
        try:
            first_page, provider, latency = await _poll_page(self, target)
            newest = str(first_page[0].get("signature") or "") if first_page else ""
            if not initialized:
                # Establish a prospective baseline only. Existing signatures are
                # historical and must never be relabeled as live observations. An
                # empty baseline is still valid; the first later signature is new.
                cursor = newest
                initialized = True
                failures = 0
                state[key] = {
                    "connected": True,
                    "baseline_established": True,
                    "last_provider": provider,
                    "last_latency_ms": latency,
                    "last_success_at": direct_solana_module.utcnow().isoformat(),
                    "poll_only_receipts_total": poll_only_total,
                    "cursor_overflow": False,
                    "failures": failures,
                }
                await target_quorum._quorum_set_target_state(self, _POLL_ENDPOINT, target, connected=True)
            elif newest and newest != cursor:
                if not cursor:
                    # The baseline was empty, so every signature in the first
                    # non-empty page was created after prospective polling began.
                    new_rows = list(reversed(first_page))
                    found = True
                else:
                    pages = [first_page]
                    found = any(str(row.get("signature") or "") == cursor for row in first_page)
                    before = str(first_page[-1].get("signature") or "") if first_page else None
                    for _ in range(1, POLL_CURSOR_MAX_PAGES):
                        if found or not before:
                            break
                        page, provider, latency = await _poll_page(self, target, before=before)
                        if not page:
                            break
                        pages.append(page)
                        found = any(str(row.get("signature") or "") == cursor for row in page)
                        before = str(page[-1].get("signature") or "")
                    new_rows, found = _new_rows_until_cursor(pages, cursor)
                if not found:
                    state[key] = {
                        "connected": False,
                        "baseline_established": True,
                        "last_provider": provider,
                        "last_latency_ms": latency,
                        "last_success_at": direct_solana_module.utcnow().isoformat(),
                        "poll_only_receipts_total": poll_only_total,
                        "cursor_overflow": True,
                        "failures": failures,
                    }
                    await target_quorum._quorum_set_target_state(
                        self,
                        _POLL_ENDPOINT,
                        target,
                        connected=False,
                        error_type="LivePollCursorOverflow",
                        error_message="live polling cursor exceeded bounded page window",
                    )
                    cursor = newest
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
                        "cursor_overflow": False,
                        "failures": failures,
                    }
                    await target_quorum._quorum_set_target_state(self, _POLL_ENDPOINT, target, connected=True)
            else:
                failures = 0
                state[key] = {
                    "connected": True,
                    "baseline_established": True,
                    "last_provider": provider,
                    "last_latency_ms": latency,
                    "last_success_at": direct_solana_module.utcnow().isoformat(),
                    "poll_only_receipts_total": poll_only_total,
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
                attempts=4,
            )
            if result is None:
                attempts = int(row.get("attempts") or 0) + 1
                self.journal.finish(
                    signature,
                    error="confirmed live-poll transaction not yet available",
                    retry=attempts < 3,
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
                retry=attempts < 3,
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
        try:
            await original(self, stop)
        finally:
            for task in poll_tasks:
                task.cancel()
            await asyncio.gather(*poll_tasks, return_exceptions=True)

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
        payload["live_poll_redundancy"] = {
            "enabled": True,
            "transport": "getSignaturesForAddress-continuous-live-poll",
            "commitment": "confirmed",
            "target_count": len(tuple(self.watch_targets)),
            "connected_target_count": connected,
            "poll_interval_seconds": POLL_INTERVAL_SECONDS,
            "page_limit": POLL_LIMIT,
            "max_cursor_pages": POLL_CURSOR_MAX_PAGES,
            "theoretical_poll_requests_per_second": len(tuple(self.watch_targets)) / POLL_INTERVAL_SECONDS,
            "historical_backfill": False,
            "poll_only_program_signatures_hydrated_for_launch_detection": True,
            "signing_available": False,
            "transaction_submission_available": False,
            "targets": dict(rows),
        }
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "live_poll_transport_participates_in_target_quorum": True,
                    "live_poll_historical_backfill_allowed": False,
                    "live_poll_cursor_overflow_fails_target_closed": True,
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
    "install_live_poll_redundancy",
]
