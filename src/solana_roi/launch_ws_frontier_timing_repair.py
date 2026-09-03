from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

from . import direct_solana as direct_solana_module
from . import launch_coverage_bridge as bridge
from . import launch_reference_timing_repair as legacy_reference
from . import live_poll_redundancy as live_poll
from .coverage_completeness_repair import _launch_contexts
from .direct_solana import DirectSolanaIngestionPlane
from .launch_funding import DexScreenerLaunchCollector, LaunchFundingPolicy
from .live_collectors import _fresh
from .risk import LaunchEvidence, RiskDimension


# The freshness guard intentionally equals the already-governed launch-lag policy.
# It is not a relaxed threshold: a stale WebSocket frontier cannot prove near-creation.
FRONTIER_MAX_AGE_SECONDS = LaunchFundingPolicy().max_pair_stream_lag_seconds

_FRONTIER_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS direct_solana_launch_ws_frontier ("
    "signature TEXT PRIMARY KEY, launch_slot INTEGER NOT NULL, frontier_slot INTEGER, "
    "frontier_provider TEXT, frontier_age_ms REAL, frontier_block_time REAL, "
    "captured_at TEXT NOT NULL, status TEXT NOT NULL, last_error_type TEXT)"
)


def _increment(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_launch_ws_frontier_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


def _frontier_state(self: Any) -> dict[str, dict[str, Any]]:
    value = getattr(self, "_roi_launch_ws_frontier_state", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_launch_ws_frontier_state", value)
    return value


def _observe_frontier(self: Any, provider: str, slot: int, observed_monotonic: float) -> None:
    if int(slot) <= 0:
        return
    state = _frontier_state(self)
    current = state.get(str(provider))
    previous_slot = int(current.get("slot") or 0) if isinstance(current, dict) else 0
    if int(slot) <= previous_slot:
        return
    state[str(provider)] = {
        "slot": int(slot),
        "received_monotonic": float(observed_monotonic),
        "received_at": direct_solana_module.utcnow().isoformat(),
    }
    _increment(self, "frontier_advances")


def _write_frontier_row(
    store: Any,
    *,
    signature: str,
    launch_slot: int,
    frontier_slot: int | None,
    frontier_provider: str | None,
    frontier_age_ms: float | None,
    captured_at: str,
    status: str,
) -> bool:
    with store._lock, store.db:
        store.db.execute(_FRONTIER_TABLE_SQL)
        cur = store.db.execute(
            "INSERT OR IGNORE INTO direct_solana_launch_ws_frontier("
            "signature, launch_slot, frontier_slot, frontier_provider, frontier_age_ms, "
            "frontier_block_time, captured_at, status, last_error_type) "
            "VALUES (?, ?, ?, ?, ?, NULL, ?, ?, NULL)",
            (
                signature,
                int(launch_slot),
                int(frontier_slot) if frontier_slot is not None else None,
                frontier_provider,
                max(0.0, float(frontier_age_ms)) if frontier_age_ms is not None else None,
                captured_at,
                status,
            ),
        )
        return bool(cur.rowcount == 1)


def _frontier_row(store: Any, signature: str) -> dict[str, Any] | None:
    try:
        with store._lock, store.db:
            store.db.execute(_FRONTIER_TABLE_SQL)
            row = store.db.execute(
                "SELECT signature, launch_slot, frontier_slot, frontier_provider, frontier_age_ms, "
                "frontier_block_time, captured_at, status, last_error_type "
                "FROM direct_solana_launch_ws_frontier WHERE signature=? LIMIT 1",
                (signature,),
            ).fetchone()
        return dict(row) if row is not None else None
    except Exception:
        return None


def _set_frontier_block_time(
    store: Any,
    signature: str,
    *,
    block_time: float | None,
    error_type: str | None = None,
) -> None:
    with store._lock, store.db:
        store.db.execute(_FRONTIER_TABLE_SQL)
        if block_time is None:
            store.db.execute(
                "UPDATE direct_solana_launch_ws_frontier SET status='block_time_failed', "
                "last_error_type=? WHERE signature=?",
                (error_type or "WebSocketFrontierBlockTimeUnavailable", signature),
            )
        else:
            store.db.execute(
                "UPDATE direct_solana_launch_ws_frontier SET frontier_block_time=?, "
                "status='complete', last_error_type=NULL WHERE signature=?",
                (float(block_time), signature),
            )


def _capture_preexisting_frontier(
    self: Any,
    signature: str,
    launch_slot: int,
    receipt_monotonic: float,
) -> bool:
    candidates: list[tuple[int, float, str]] = []
    for provider, row in _frontier_state(self).items():
        if not isinstance(row, dict):
            continue
        try:
            slot = int(row.get("slot") or 0)
            observed = float(row.get("received_monotonic"))
        except (TypeError, ValueError):
            continue
        if slot <= 0:
            continue
        age_seconds = max(0.0, float(receipt_monotonic) - observed)
        if age_seconds <= FRONTIER_MAX_AGE_SECONDS:
            candidates.append((slot, age_seconds, str(provider)))

    if candidates:
        # The highest recent preexisting WebSocket head is the conservative chain
        # frontier: if it is already beyond the launch slot, that chain-time delta
        # must remain inside the unchanged three-second gate.
        frontier_slot, age_seconds, provider = max(
            candidates,
            key=lambda item: (item[0], -item[1], item[2]),
        )
        status = "captured"
        age_ms: float | None = age_seconds * 1000.0
    else:
        frontier_slot = None
        provider = None
        age_ms = None
        status = "missing_recent_frontier"

    inserted = _write_frontier_row(
        self.store,
        signature=signature,
        launch_slot=launch_slot,
        frontier_slot=frontier_slot,
        frontier_provider=provider,
        frontier_age_ms=age_ms,
        captured_at=direct_solana_module.utcnow().isoformat(),
        status=status,
    )
    if inserted:
        if frontier_slot is None:
            _increment(self, "launches_without_frontier")
        else:
            _increment(self, "launches_with_frontier")
    else:
        _increment(self, "duplicate_launch_receipts")
    return bool(inserted and frontier_slot is not None)


async def _handle_notification_with_ws_frontier(
    self: Any,
    provider: str,
    subscription_targets: dict[int, Any],
    message: dict[str, Any],
) -> None:
    observed_monotonic = time.monotonic()
    try:
        params = message.get("params")
        result = params.get("result") if isinstance(params, dict) else None
        value = result.get("value") if isinstance(result, dict) else None
        signature = str(value.get("signature") or "") if isinstance(value, dict) else ""
        slot = int(result.get("context", {}).get("slot") or 0) if isinstance(result, dict) else 0
        logs = value.get("logs") if isinstance(value, dict) else None
        is_launch = bool(
            message.get("method") == "logsNotification"
            and signature
            and slot > 0
            and isinstance(value, dict)
            and value.get("err") is None
            and self._launch_like(logs)
        )
        if is_launch:
            # Snapshot first. The launch notification itself must never manufacture
            # the preexisting frontier used to judge its own timeliness.
            _capture_preexisting_frontier(self, signature, slot, observed_monotonic)
        if message.get("method") == "logsNotification" and slot > 0:
            _observe_frontier(self, provider, slot, observed_monotonic)
    except Exception:
        # Timing proof is additive. The canonical notification path still records
        # the receipt; absence of a proof later fails the near-creation gate closed.
        pass

    # Bypass v4/v5's on-demand and background getSlot timing probes. This is the
    # exact pre-v4 canonical notification handler captured by the reference module.
    await legacy_reference._PRE_CHAIN_HANDLE_NOTIFICATION(
        self,
        provider,
        subscription_targets,
        message,
    )


async def _hydrate_mint_launch_context_with_ws_frontier(
    self: Any,
    *,
    mint: str,
    source: str,
    launch_signature: str,
    created_at: datetime,
) -> tuple[int, bool, int]:
    # Preserve the established immutable launch-window acquisition/attestation path
    # while removing timing-probe RPC work from launch receipt processing.
    persisted, complete, candidate_count = await legacy_reference._PRE_CHAIN_HYDRATE_MINT_LAUNCH_CONTEXT(
        self,
        mint=mint,
        source=source,
        launch_signature=launch_signature,
        created_at=created_at,
    )

    row = _frontier_row(self.store, launch_signature)
    if isinstance(row, dict) and str(row.get("status") or "") == "captured":
        try:
            launch_slot = int(row.get("launch_slot") or 0)
            frontier_slot = int(row.get("frontier_slot") or 0)
        except (TypeError, ValueError):
            launch_slot = 0
            frontier_slot = 0
        if frontier_slot > launch_slot > 0:
            try:
                # The frontier slot was selected before first launch receipt. This
                # later lookup cannot move that reference and its RPC latency is not
                # charged as launch lateness.
                value, _provider, _latency = await live_poll._poll_rpc(self).call_with_meta(
                    "getBlockTime",
                    [frontier_slot],
                    hedge=True,
                )
                if value is None:
                    raise RuntimeError("preexisting WebSocket frontier blockTime unavailable")
                _set_frontier_block_time(
                    self.store,
                    launch_signature,
                    block_time=float(value),
                )
                _increment(self, "frontier_block_times_complete")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _set_frontier_block_time(
                    self.store,
                    launch_signature,
                    block_time=None,
                    error_type=type(exc).__name__,
                )
                _increment(self, "frontier_block_times_failed")
        elif frontier_slot > 0:
            # A recent preexisting frontier at/before the launch is sufficient for
            # zero chain-frontier lag; no blockTime RPC is required.
            _set_frontier_block_time(
                self.store,
                launch_signature,
                block_time=float(created_at.timestamp()),
            )

    raw = bridge._raw_collectors(self)
    launch = getattr(raw, "launch", None)
    if launch is not None:
        context = _launch_contexts(launch).get(mint)
        if isinstance(context, dict):
            context["launch_signature"] = launch_signature
            context["launch_slot"] = int(
                (_frontier_row(self.store, launch_signature) or {}).get("launch_slot") or 0
            )
    return persisted, complete, candidate_count


def _ws_frontier_lag_seconds(
    store: Any,
    *,
    signature: str,
    created_at: datetime,
    max_age_seconds: float,
) -> tuple[float | None, str]:
    row = _frontier_row(store, signature)
    if not isinstance(row, dict):
        return None, "missing_preexisting_websocket_frontier"
    status = str(row.get("status") or "")
    if status == "missing_recent_frontier":
        return None, "missing_recent_preexisting_websocket_frontier"
    if status not in {"captured", "complete"}:
        return None, "incomplete_preexisting_websocket_frontier"
    try:
        launch_slot = int(row.get("launch_slot") or 0)
        frontier_slot = int(row.get("frontier_slot") or 0)
        age_seconds = max(0.0, float(row.get("frontier_age_ms") or 0.0) / 1000.0)
    except (TypeError, ValueError):
        return None, "invalid_preexisting_websocket_frontier"
    if launch_slot <= 0 or frontier_slot <= 0:
        return None, "invalid_preexisting_websocket_frontier"
    if age_seconds > float(max_age_seconds):
        return None, "stale_preexisting_websocket_frontier"

    if frontier_slot <= launch_slot:
        return 0.0, "recent-preexisting-websocket-frontier-not-ahead"

    try:
        frontier_block_time = float(row.get("frontier_block_time"))
    except (TypeError, ValueError):
        return None, "missing_preexisting_websocket_frontier_block_time"
    launch_block_time = created_at.timestamp()
    if frontier_block_time < launch_block_time:
        return None, "non_monotonic_websocket_frontier_block_time"
    return (
        max(0.0, frontier_block_time - launch_block_time),
        "preexisting-websocket-chain-frontier-lag",
    )


async def _launch_collect_with_ws_frontier(
    self: DexScreenerLaunchCollector,
    mint: str,
    at: datetime,
) -> bool:
    if _fresh(self.risk, mint, RiskDimension.LAUNCH, at):
        return True
    created_at = await self._created_at(mint)
    if created_at is None:
        return False
    if at < created_at + timedelta(seconds=self.policy.launch_window_seconds):
        return False

    rows = self._early_rows(
        mint,
        start=created_at - timedelta(seconds=1),
        end=created_at + timedelta(seconds=self.policy.launch_window_seconds),
        decision_at=at,
    )
    buys = [row for row in rows if row["side"] == "buy"]
    buyers = {str(row["wallet"]) for row in buys}

    context = _launch_contexts(self).get(mint)
    if isinstance(context, dict):
        signature = str(context.get("launch_signature") or "")
        if signature:
            lag_seconds, timing_proof = _ws_frontier_lag_seconds(
                self.store,
                signature=signature,
                created_at=created_at,
                max_age_seconds=self.policy.max_pair_stream_lag_seconds,
            )
            source = "program-wide-swaps+preexisting-websocket-chain-frontier:launch-window-v7"
        else:
            # Preserve direct/offline compatibility when no production bridge
            # signature is present. Negative cross-clock skew still does not count
            # as positive lateness under the established v3 semantics.
            observed_at = context.get("observed_at")
            if isinstance(observed_at, datetime):
                lag_seconds = max(0.0, (observed_at - created_at).total_seconds())
            else:
                lag_seconds = None
            timing_proof = "legacy_direct_seed_live_receipt"
            source = "program-wide-swaps+confirmed-launch-context:launch-window-v3"
        near_creation = (
            lag_seconds is not None
            and lag_seconds <= self.policy.max_pair_stream_lag_seconds
        )
        early_complete = bool(context.get("complete"))
        setattr(self, "_roi_last_launch_timing_proof", timing_proof)
    else:
        earliest = min(
            (datetime.fromisoformat(str(row["observed_at"])) for row in rows),
            default=None,
        )
        lag_seconds = (
            abs((earliest - created_at).total_seconds())
            if earliest is not None
            else None
        )
        near_creation = (
            lag_seconds is not None
            and lag_seconds <= self.policy.max_pair_stream_lag_seconds
        )
        early_complete = (
            len(buys) >= self.policy.min_launch_buys
            and len(buyers) >= self.policy.min_launch_buyers
        )
        source = "program-wide-swaps+dexscreener:launch-window-v1"

    if hasattr(self.store, "record_program_coverage"):
        self.store.record_program_coverage(
            token_mint=mint,
            pair_created_at=created_at.isoformat(),
            assessed_at=at.isoformat(),
            launch_lag_ms=lag_seconds * 1000.0 if lag_seconds is not None else None,
            launch_near_creation=near_creation,
            early_buy_count=len(buys),
            early_buyer_count=len(buyers),
            early_buyers_complete=early_complete,
        )
    if not early_complete or not near_creation:
        return False

    slot_buyers: dict[int, set[str]] = {}
    buyer_sol: dict[str, float] = {}
    total_sol = 0.0
    for row in buys:
        slot_buyers.setdefault(int(row["slot"]), set()).add(str(row["wallet"]))
        amount = float(row["native_amount_sol"])
        total_sol += amount
        buyer_sol[str(row["wallet"])] = buyer_sol.get(str(row["wallet"]), 0.0) + amount
    bundled = max((len(value) for value in slot_buyers.values()), default=0) >= self.policy.bundled_same_slot_buyers
    top_two = sum(sorted(buyer_sol.values(), reverse=True)[:2])
    sniper_heavy = total_sol > 0 and top_two / total_sol >= self.policy.sniper_top_two_buy_share
    self.risk.record_launch(
        mint,
        LaunchEvidence(bundled_launch=bundled, sniper_heavy=sniper_heavy),
        observed_at=at,
        received_at=at,
        source=source,
    )
    return True


async def _disabled_legacy_reference_sampler(_self: Any, stop: asyncio.Event) -> None:
    # v5's run wrapper resolves this module global dynamically. Keeping the wrapper
    # but parking its sampler removes continuous getSlot pressure without changing
    # the surrounding runtime/task topology.
    await stop.wait()


def _status_with_ws_frontier(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        launch_bridge = payload.get("launch_coverage_bridge")
        if isinstance(launch_bridge, dict):
            launch_bridge.update(
                {
                    "coverage_semantics": "recent-preexisting-websocket-chain-frontier+immutable-window-acquisition-v7",
                    "near_creation_timing_model": "recent-preexisting-websocket-chain-frontier-lag",
                    "near_creation_uses_host_wall_clock": False,
                    "near_creation_legacy_reference_rpc_sampler_enabled": False,
                    "near_creation_launch_path_rpc_reads": False,
                    "near_creation_reference_transport": "existing-live-logsSubscribe-frontier",
                    "near_creation_reference_must_preexist_launch_receipt": True,
                    "near_creation_reference_max_age_seconds": FRONTIER_MAX_AGE_SECONDS,
                    "near_creation_reference_age_is_freshness_gate_not_lag_addend": True,
                    "near_creation_reference_block_time_only_when_frontier_ahead": True,
                    "near_creation_threshold_unchanged": True,
                    "near_creation_missing_timing_proof_fails_closed": True,
                    "ws_frontier_advances": int(getattr(self, "_roi_launch_ws_frontier_frontier_advances", 0) or 0),
                    "launches_with_ws_frontier": int(getattr(self, "_roi_launch_ws_frontier_launches_with_frontier", 0) or 0),
                    "launches_without_ws_frontier": int(getattr(self, "_roi_launch_ws_frontier_launches_without_frontier", 0) or 0),
                    "ws_frontier_duplicate_launch_receipts": int(getattr(self, "_roi_launch_ws_frontier_duplicate_launch_receipts", 0) or 0),
                    "ws_frontier_block_times_complete": int(getattr(self, "_roi_launch_ws_frontier_frontier_block_times_complete", 0) or 0),
                    "ws_frontier_block_times_failed": int(getattr(self, "_roi_launch_ws_frontier_frontier_block_times_failed", 0) or 0),
                    "candidate_activation_from_launch_bridge": False,
                    "latency_samples_from_launch_bridge": False,
                    "quote_samples_from_launch_bridge": False,
                    "paper_authority": False,
                }
            )
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "launch_near_creation_websocket_chain_frontier": True,
                    "launch_near_creation_continuous_getslot_sampler": False,
                    "launch_near_creation_reference_freshness_gate_seconds": FRONTIER_MAX_AGE_SECONDS,
                    "launch_near_creation_threshold_unchanged": True,
                    "launch_near_creation_missing_proof_fails_closed": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_launch_ws_frontier_timing_repair", True)
    return status


def install_launch_ws_frontier_timing_repair() -> None:
    DirectSolanaIngestionPlane._handle_notification = _handle_notification_with_ws_frontier  # type: ignore[method-assign]
    bridge._hydrate_mint_launch_context = _hydrate_mint_launch_context_with_ws_frontier  # type: ignore[assignment]
    DexScreenerLaunchCollector.collect = _launch_collect_with_ws_frontier  # type: ignore[method-assign]

    # Stop v5's continuously maintained getSlot sampler. v7 derives its reference
    # from the already-required live WebSocket traffic instead, reducing read-RPC
    # contention with continuity recovery and hydration.
    legacy_reference._reference_sampler = _disabled_legacy_reference_sampler  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_launch_ws_frontier_timing_repair", False)):
        DirectSolanaIngestionPlane.status = _status_with_ws_frontier(current_status)  # type: ignore[method-assign]


__all__ = [
    "FRONTIER_MAX_AGE_SECONDS",
    "install_launch_ws_frontier_timing_repair",
    "_capture_preexisting_frontier",
    "_frontier_row",
    "_handle_notification_with_ws_frontier",
    "_hydrate_mint_launch_context_with_ws_frontier",
    "_observe_frontier",
    "_ws_frontier_lag_seconds",
]
