from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any

from . import certification_runtime_architecture_repair as runtime_arch
from . import continuity_gap_clock_repair as gap_clock
from . import continuity_high_volume_poll_affinity_repair as affinity
from . import continuity_immediate_recovery_repair as immediate
from . import continuity_recovery_isolation_repair as isolation
from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from . import raw_receipt_dispatch_repair as raw_dispatch
from .direct_solana import DirectSolanaIngestionPlane, DirectSolanaJournal, WatchTarget


# This repair does not increase either immutable recovery bound. It changes only
# the *lower proof boundary* for high-volume PUMP targets: when an exact WebSocket
# signature is already durably committed, recovery may stop at that exact signature
# instead of replaying the entire confirmed slot via slot-1.
EXACT_BOUNDARY_CONFIRMATION_ATTEMPTS = 2
EXACT_BOUNDARY_CONFIRMATION_RETRY_SECONDS = 0.10

_ORIGINAL_RECORD_RECEIPT: Callable[..., bool] | None = None
_ORIGINAL_KICK: Callable[..., Any] | None = None
_ORIGINAL_INTERVAL_FETCH: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None


def _increment(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_exact_durable_signature_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


def _journal_frontiers(journal: Any) -> dict[str, dict[str, Any]]:
    value = getattr(journal, "_roi_exact_durable_ws_frontiers", None)
    if not isinstance(value, dict):
        value = {}
        setattr(journal, "_roi_exact_durable_ws_frontiers", value)
    return value


def _boundaries(self: Any) -> dict[str, dict[str, Any]]:
    value = getattr(self, "_roi_exact_durable_signature_boundaries", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_exact_durable_signature_boundaries", value)
    return value


def _record_receipt_with_durable_frontier(
    self: DirectSolanaJournal,
    *,
    signature: str,
    source_key: str,
    slot: int,
    received_at: datetime,
    launch_like: bool,
) -> bool:
    """Remember an exact high-volume WebSocket signature only after SQLite commit.

    The raw-dispatch ContextVar is non-null only while the canonical queued
    WebSocket handler is executing. Live-poll/backfill calls to record_receipt do
    not inherit that context and therefore cannot become this recovery authority.
    The delegate returns only after its transaction context exits, so publishing the
    in-memory frontier after it returns proves the referenced raw receipt is already
    durable. The frontier is continuity metadata only; it creates no market sample.
    """

    if _ORIGINAL_RECORD_RECEIPT is None:
        raise RuntimeError("exact durable signature repair is not installed")
    inserted = bool(
        _ORIGINAL_RECORD_RECEIPT(
            self,
            signature=signature,
            source_key=source_key,
            slot=slot,
            received_at=received_at,
            launch_like=launch_like,
        )
    )
    if not inserted:
        return False

    if str(source_key) not in affinity.HIGH_VOLUME_ROUTINE_SOURCES:
        return True
    if raw_dispatch._RECEIPT_WALL_TIME.get() is None:
        # Poll/recovery/history can persist the same schema, but only a real
        # WebSocket dispatch may establish this exact prospective lower boundary.
        return True

    try:
        parsed_slot = int(slot)
    except (TypeError, ValueError):
        parsed_slot = 0
    signature = str(signature or "")
    if not signature or parsed_slot <= 0:
        return True

    frontiers = _journal_frontiers(self)
    row = {
        "signature": signature,
        "slot": parsed_slot,
        "source_key": str(source_key),
        "received_at": received_at.isoformat(),
        "committed_monotonic": time.monotonic(),
        "durable": True,
        "transport": "websocket",
    }
    current = frontiers.get(str(source_key))
    if not isinstance(current, dict) or (
        parsed_slot,
        float(row["committed_monotonic"]),
    ) >= (
        int(current.get("slot", 0) or 0),
        float(current.get("committed_monotonic", 0.0) or 0.0),
    ):
        frontiers[str(source_key)] = row
        setattr(
            self,
            "_roi_exact_durable_ws_frontier_updates",
            int(getattr(self, "_roi_exact_durable_ws_frontier_updates", 0) or 0) + 1,
        )
    return True


setattr(_record_receipt_with_durable_frontier, "_roi_exact_durable_signature", True)


def _source_key(target: WatchTarget) -> str | None:
    source = str(target.source_hint or "")
    return source if source in affinity.HIGH_VOLUME_ROUTINE_SOURCES else None


def _gap_started_monotonic(self: Any, target: WatchTarget) -> float | None:
    row = gap_clock._gap_clocks(self).get(live_poll._poll_target_key(target))
    if not isinstance(row, dict):
        return None
    try:
        return float(row.get("started_monotonic"))
    except (TypeError, ValueError):
        return None


def _snapshot_exact_durable_boundary(
    self: Any,
    target: WatchTarget,
    generation: int,
) -> dict[str, Any] | None:
    source = _source_key(target)
    if source is None:
        return None
    journal = getattr(self, "journal", None)
    if journal is None:
        return None
    frontier = _journal_frontiers(journal).get(source)
    if not isinstance(frontier, dict) or not bool(frontier.get("durable")):
        _increment(self, "snapshot_missing")
        return None

    key = live_poll._poll_target_key(target)
    previous_generation = int(generation) - 1
    runtime = lease._runtime(self).get(key, {})
    if not isinstance(runtime, dict):
        _increment(self, "snapshot_generation_rejected")
        return None
    try:
        cursor_generation = int(runtime.get("cursor_ws_generation", previous_generation) or 0)
    except (TypeError, ValueError):
        cursor_generation = -1
    if previous_generation < 0 or cursor_generation != previous_generation:
        # A stale/unrecovered generation can never be skipped by a newer durable
        # receipt. Canonical generation recovery remains authoritative first.
        _increment(self, "snapshot_generation_rejected")
        return None

    try:
        committed = float(frontier.get("committed_monotonic") or 0.0)
        slot = int(frontier.get("slot") or 0)
    except (TypeError, ValueError):
        _increment(self, "snapshot_invalid")
        return None
    signature = str(frontier.get("signature") or "")
    if not signature or slot <= 0 or committed <= 0.0:
        _increment(self, "snapshot_invalid")
        return None

    gap_started = _gap_started_monotonic(self, target)
    if gap_started is not None and committed > gap_started:
        # Conservatively reject a commit that happened after the real zero-coverage
        # transition. It may still describe a queued pre-gap receipt, but proving
        # that ordering is unnecessary because the slot-based fallback remains.
        _increment(self, "snapshot_after_gap_rejected")
        return None

    boundary = {
        "target": key,
        "source_key": source,
        "generation": int(generation),
        "previous_generation": previous_generation,
        "signature": signature,
        "slot": slot,
        "committed_monotonic": committed,
        "snapshot_monotonic": time.monotonic(),
        "confirmed": False,
        "confirmation_failed": False,
        "lower_boundary_model": "last-durably-committed-websocket-signature",
        "exclusive_after_signature": True,
        "recorded_gap_can_be_repaired": False,
    }
    _boundaries(self)[key] = boundary
    _increment(self, "boundary_snapshots")
    setattr(self, "_roi_exact_durable_signature_last_boundary", dict(boundary))
    return boundary


def _kick_with_exact_durable_boundary(self: Any, target: WatchTarget, generation: int) -> None:
    if _ORIGINAL_KICK is None:
        raise RuntimeError("exact durable signature repair is not installed")
    if _source_key(target) is not None:
        _snapshot_exact_durable_boundary(self, target, int(generation))
    _ORIGINAL_KICK(self, target, generation)


setattr(_kick_with_exact_durable_boundary, "_roi_exact_durable_signature", True)


def _confirmation_rows(result: Any) -> list[Any]:
    value = result.get("value") if isinstance(result, dict) else None
    return list(value) if isinstance(value, list) else []


async def _confirm_boundary(
    self: Any,
    target: WatchTarget,
    boundary: dict[str, Any],
) -> bool:
    if bool(boundary.get("confirmed")):
        return True
    if bool(boundary.get("confirmation_failed")):
        return False

    generation = int(boundary.get("generation", -1))
    signature = str(boundary.get("signature") or "")
    expected_slot = int(boundary.get("slot") or 0)
    if generation < 0 or not signature or expected_slot <= 0:
        boundary["confirmation_failed"] = True
        return False

    deadline = (
        (_gap_started_monotonic(self, target) or time.monotonic())
        + lease.POLL_RECOVERABILITY_LEASE_SECONDS
    )
    for attempt in range(EXACT_BOUNDARY_CONFIRMATION_ATTEMPTS):
        if immediate._generation(self, target) != generation:
            boundary["confirmation_failed"] = True
            boundary["confirmation_error_type"] = "GapGenerationSuperseded"
            return False
        if attempt > 0 and time.monotonic() >= deadline:
            break
        _increment(self, "confirmation_attempts")
        try:
            result, provider, latency = await isolation._recovery_rpc(self).call_with_meta(
                "getSignatureStatuses",
                [[signature], {"searchTransactionHistory": True}],
                hedge=True,
            )
            statuses = _confirmation_rows(result)
            status = statuses[0] if statuses else None
            if isinstance(status, dict):
                confirmation = str(status.get("confirmationStatus") or "").lower()
                try:
                    status_slot = int(status.get("slot") or 0)
                except (TypeError, ValueError):
                    status_slot = 0
                if confirmation in {"confirmed", "finalized"} and status_slot == expected_slot:
                    boundary["confirmed"] = True
                    boundary["confirmation_failed"] = False
                    boundary["confirmation_provider"] = provider
                    boundary["confirmation_latency_ms"] = latency
                    boundary["confirmed_monotonic"] = time.monotonic()
                    _increment(self, "confirmation_successes")
                    return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            boundary["confirmation_error_type"] = type(exc).__name__
            _increment(self, "confirmation_errors")

        if attempt + 1 < EXACT_BOUNDARY_CONFIRMATION_ATTEMPTS:
            remaining = deadline - time.monotonic()
            if remaining > 0.0:
                await asyncio.sleep(min(EXACT_BOUNDARY_CONFIRMATION_RETRY_SECONDS, remaining))

    boundary["confirmation_failed"] = True
    _increment(self, "confirmation_failures")
    return False


async def _exact_signature_interval_fetch(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None, dict[str, Any]]:
    """Recover exact missing signatures without replaying already-durable same-slot rows.

    The first successfully recorded post-gap WebSocket receipt remains the optional
    exclusive upper boundary from the existing generation-interval architecture.
    The lower boundary is the confirmed exact signature that was durably committed
    before zero WebSocket coverage. Pagination remains exactly 3 x 1000.
    """

    if _ORIGINAL_INTERVAL_FETCH is None:
        raise RuntimeError("exact durable signature interval repair is not installed")
    source = _source_key(target)
    if source is None:
        return await _ORIGINAL_INTERVAL_FETCH(self, target, cursor_slot)

    key = live_poll._poll_target_key(target)
    generation = immediate._generation(self, target)
    boundary = _boundaries(self).get(key)
    if not isinstance(boundary, dict) or int(boundary.get("generation", -1)) != generation:
        _increment(self, "fallback_no_boundary")
        return await _ORIGINAL_INTERVAL_FETCH(self, target, cursor_slot)
    if not await _confirm_boundary(self, target, boundary):
        _increment(self, "fallback_unconfirmed")
        return await _ORIGINAL_INTERVAL_FETCH(self, target, cursor_slot)

    lower_signature = str(boundary.get("signature") or "")
    lower_slot = int(boundary.get("slot") or 0)
    upper = runtime_arch._recovery_upper_boundaries(self).get(key)
    before: str | None = None
    upper_slot = 0
    if isinstance(upper, dict) and int(upper.get("generation", -1)) == generation:
        before = str(upper.get("signature") or "") or None
        try:
            upper_slot = int(upper.get("slot") or 0)
        except (TypeError, ValueError):
            upper_slot = 0

    pages: list[list[dict[str, Any]]] = []
    page_providers: list[str | None] = []
    page_latencies: list[float | None] = []
    provider: str | None = None
    latency: float | None = None
    lower_reached = False
    context_floor = max(0, int(cursor_slot), lower_slot)

    _increment(self, "exact_recovery_attempts")
    for _page_index in range(live_poll.POLL_CURSOR_MAX_PAGES):
        config: dict[str, Any] = {
            "commitment": "confirmed",
            "limit": live_poll.POLL_LIMIT,
        }
        if before:
            config["before"] = before
        if context_floor > 0:
            config["minContextSlot"] = context_floor
        result, provider, latency = await isolation._recovery_rpc(self).call_with_meta(
            "getSignaturesForAddress",
            [target.address, config],
            hedge=True,
        )
        page = [row for row in result if isinstance(row, dict)] if isinstance(result, list) else []
        page_providers.append(provider)
        page_latencies.append(float(latency) if latency is not None else None)

        lower_index: int | None = None
        for index, row in enumerate(page):
            if str(row.get("signature") or "") == lower_signature:
                lower_index = index
                break
        if lower_index is not None:
            # Keep the exact lower row only as a terminator. Any rows after it are
            # older address history, including older same-slot signatures that were
            # already outside the missing interval.
            page = page[: lower_index + 1]
            pages.append(page)
            lower_reached = True
            break

        pages.append(page)
        if not page or len(page) < live_poll.POLL_LIMIT:
            # A confirmed exact anchor must itself be observed before this path can
            # claim completeness. A short/inconsistent RPC page is not proof.
            break
        before = str(page[-1].get("signature") or "") or None
        if not before:
            break

    if not lower_reached:
        _increment(self, "exact_recovery_incomplete")
        all_slots = [isolation.watermark._row_slot(row) for page in pages for row in page]
        meta = {
            "page_count": len(pages),
            "page_sizes": [len(page) for page in pages],
            "page_providers": page_providers,
            "page_latencies_ms": page_latencies,
            "newest_slot_seen": max(all_slots, default=0),
            "oldest_slot_seen": min((slot for slot in all_slots if slot > 0), default=0),
            "cursor_slot": int(cursor_slot),
            "cursor_reached": False,
            "complete": False,
            "recovered_row_count": 0,
            "hard_page_limit": live_poll.POLL_CURSOR_MAX_PAGES,
            "hard_page_size": live_poll.POLL_LIMIT,
            "exact_durable_lower_boundary_applied": True,
            "exact_durable_lower_signature": lower_signature,
            "exact_durable_lower_slot": lower_slot,
            "exact_durable_lower_reached": False,
            "generation_upper_boundary_applied": bool(upper and upper_slot > 0),
            "generation_upper_boundary_slot": upper_slot or None,
            "generation_upper_boundary_source": (
                str(upper.get("source")) if isinstance(upper, dict) and upper_slot > 0 else None
            ),
        }
        return [], False, provider, latency, meta

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for page in reversed(pages):
        for row in reversed(page):
            signature = str(row.get("signature") or "")
            if not signature or signature == lower_signature or signature in seen:
                continue
            seen.add(signature)
            rows.append(row)

    all_slots = [isolation.watermark._row_slot(row) for page in pages for row in page]
    meta = {
        "page_count": len(pages),
        "page_sizes": [len(page) for page in pages],
        "page_providers": page_providers,
        "page_latencies_ms": page_latencies,
        "newest_slot_seen": max(all_slots, default=0),
        "oldest_slot_seen": min((slot for slot in all_slots if slot > 0), default=0),
        "cursor_slot": int(cursor_slot),
        "cursor_reached": True,
        "complete": True,
        "recovered_row_count": len(rows),
        "hard_page_limit": live_poll.POLL_CURSOR_MAX_PAGES,
        "hard_page_size": live_poll.POLL_LIMIT,
        "exact_durable_lower_boundary_applied": True,
        "exact_durable_lower_signature": lower_signature,
        "exact_durable_lower_slot": lower_slot,
        "exact_durable_lower_reached": True,
        "same_slot_already_durable_rows_can_be_excluded": True,
        "generation_upper_boundary_applied": bool(upper and upper_slot > 0),
        "generation_upper_boundary_slot": upper_slot or None,
        "generation_upper_boundary_source": (
            str(upper.get("source")) if isinstance(upper, dict) and upper_slot > 0 else None
        ),
    }
    _increment(self, "exact_recovery_completed")
    setattr(
        self,
        "_roi_exact_durable_signature_last_success",
        {
            "target": key,
            "generation": generation,
            "lower_signature": lower_signature,
            "lower_slot": lower_slot,
            "recovered_row_count": len(rows),
            "page_count": len(pages),
            "upper_boundary_applied": bool(upper and upper_slot > 0),
        },
    )
    return rows, True, provider, latency, meta


setattr(_exact_signature_interval_fetch, "_roi_exact_durable_signature", True)


def _status_with_exact_durable_signature(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("exact durable signature repair is not installed")
    payload = _ORIGINAL_STATUS(self)
    journal = getattr(self, "journal", None)
    journal_frontiers = _journal_frontiers(journal) if journal is not None else {}
    boundary_rows: dict[str, Any] = {}
    for key, row in _boundaries(self).items():
        if not isinstance(row, dict):
            continue
        boundary_rows[key] = {
            "generation": int(row.get("generation", 0) or 0),
            "signature": row.get("signature"),
            "slot": int(row.get("slot", 0) or 0),
            "confirmed": bool(row.get("confirmed")),
            "confirmation_failed": bool(row.get("confirmation_failed")),
            "lower_boundary_model": row.get("lower_boundary_model"),
        }

    payload["exact_durable_signature_continuity"] = {
        "installed": True,
        "scope": sorted(affinity.HIGH_VOLUME_ROUTINE_SOURCES),
        "lower_boundary_model": "confirmed-last-durably-committed-websocket-signature",
        "durable_ws_frontier_count": len(journal_frontiers),
        "durable_ws_frontier_updates": int(
            getattr(journal, "_roi_exact_durable_ws_frontier_updates", 0) or 0
        ) if journal is not None else 0,
        "boundary_snapshots": int(getattr(self, "_roi_exact_durable_signature_boundary_snapshots", 0) or 0),
        "snapshot_missing": int(getattr(self, "_roi_exact_durable_signature_snapshot_missing", 0) or 0),
        "snapshot_generation_rejected": int(getattr(self, "_roi_exact_durable_signature_snapshot_generation_rejected", 0) or 0),
        "confirmation_attempts": int(getattr(self, "_roi_exact_durable_signature_confirmation_attempts", 0) or 0),
        "confirmation_successes": int(getattr(self, "_roi_exact_durable_signature_confirmation_successes", 0) or 0),
        "confirmation_failures": int(getattr(self, "_roi_exact_durable_signature_confirmation_failures", 0) or 0),
        "exact_recovery_attempts": int(getattr(self, "_roi_exact_durable_signature_exact_recovery_attempts", 0) or 0),
        "exact_recovery_completed": int(getattr(self, "_roi_exact_durable_signature_exact_recovery_completed", 0) or 0),
        "exact_recovery_incomplete": int(getattr(self, "_roi_exact_durable_signature_exact_recovery_incomplete", 0) or 0),
        "fallback_no_boundary": int(getattr(self, "_roi_exact_durable_signature_fallback_no_boundary", 0) or 0),
        "fallback_unconfirmed": int(getattr(self, "_roi_exact_durable_signature_fallback_unconfirmed", 0) or 0),
        "boundaries": boundary_rows,
        "last_success": getattr(self, "_roi_exact_durable_signature_last_success", None),
        "slot_minus_one_fallback_preserved": True,
        "same_slot_replay_preserved_on_fallback": True,
        "recorded_gap_can_be_repaired": False,
        "historical_promotion_authority": False,
        "recoverability_lease_seconds_unchanged": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
        "hard_page_limit_unchanged": live_poll.POLL_CURSOR_MAX_PAGES,
        "hard_page_size_unchanged": live_poll.POLL_LIMIT,
        "paper_only_authority_unchanged": True,
        "signing_or_submission_available": False,
    }
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "real_gap_exact_durable_signature_lower_boundary": True,
                "exact_boundary_requires_durable_websocket_receipt": True,
                "exact_boundary_requires_confirmed_or_finalized_rpc_proof": True,
                "exact_boundary_never_uses_live_poll_or_history_receipt": True,
                "exact_boundary_cannot_restore_recorded_gap": True,
                "exact_boundary_slot_minus_one_fallback_preserved": True,
                "continuity_lease_unchanged": True,
                "recovery_bound_unchanged": True,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    return payload


setattr(_status_with_exact_durable_signature, "_roi_exact_durable_signature", True)


def install_exact_durable_signature_continuity_repair() -> None:
    """Install exact durable lower-boundary recovery after PR100 final composition."""

    global _ORIGINAL_RECORD_RECEIPT, _ORIGINAL_KICK, _ORIGINAL_INTERVAL_FETCH, _ORIGINAL_STATUS

    current_record = DirectSolanaJournal.record_receipt
    if not bool(getattr(current_record, "_roi_exact_durable_signature", False)):
        _ORIGINAL_RECORD_RECEIPT = current_record
        DirectSolanaJournal.record_receipt = _record_receipt_with_durable_frontier  # type: ignore[method-assign]

    current_kick = immediate._kick_immediate_recovery
    if not bool(getattr(current_kick, "_roi_exact_durable_signature", False)):
        _ORIGINAL_KICK = current_kick
        try:
            _kick_with_exact_durable_boundary.__dict__.update(getattr(current_kick, "__dict__", {}))
        except Exception:
            pass
        setattr(_kick_with_exact_durable_boundary, "_roi_exact_durable_signature", True)
        immediate._kick_immediate_recovery = _kick_with_exact_durable_boundary  # type: ignore[assignment]

    current_fetch = isolation._isolated_gap_fetch_delta
    if not bool(getattr(current_fetch, "_roi_exact_durable_signature", False)):
        _ORIGINAL_INTERVAL_FETCH = current_fetch
        try:
            _exact_signature_interval_fetch.__dict__.update(getattr(current_fetch, "__dict__", {}))
        except Exception:
            pass
        setattr(_exact_signature_interval_fetch, "_roi_exact_durable_signature", True)
        isolation._isolated_gap_fetch_delta = _exact_signature_interval_fetch  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_exact_durable_signature", False)):
        _ORIGINAL_STATUS = current_status
        try:
            _status_with_exact_durable_signature.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_status_with_exact_durable_signature, "_roi_exact_durable_signature", True)
        DirectSolanaIngestionPlane.status = _status_with_exact_durable_signature  # type: ignore[method-assign]


__all__ = [
    "EXACT_BOUNDARY_CONFIRMATION_ATTEMPTS",
    "EXACT_BOUNDARY_CONFIRMATION_RETRY_SECONDS",
    "install_exact_durable_signature_continuity_repair",
    "_exact_signature_interval_fetch",
    "_record_receipt_with_durable_frontier",
    "_snapshot_exact_durable_boundary",
]
