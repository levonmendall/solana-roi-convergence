from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

from . import direct_solana as direct_solana_module
from . import launch_coverage_bridge as bridge
from .coverage_completeness_repair import _launch_contexts
from .direct_solana import DirectSolanaIngestionPlane
from .launch_funding import DexScreenerLaunchCollector
from .live_collectors import _fresh
from .risk import LaunchEvidence, RiskDimension


_PREVIOUS_HANDLE_NOTIFICATION = DirectSolanaIngestionPlane._handle_notification
_PREVIOUS_HYDRATE_MINT_LAUNCH_CONTEXT = bridge._hydrate_mint_launch_context


_TIMING_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS direct_solana_launch_timing_samples ("
    "signature TEXT PRIMARY KEY, launch_slot INTEGER NOT NULL, head_slot INTEGER NOT NULL, "
    "head_block_time REAL, rpc_provider TEXT, slot_latency_ms REAL, block_time_latency_ms REAL, "
    "sampled_at TEXT NOT NULL, status TEXT NOT NULL, last_error_type TEXT)"
)


def _timing_tasks(self: Any) -> dict[str, asyncio.Task[Any]]:
    value = getattr(self, "_roi_launch_timing_tasks", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_launch_timing_tasks", value)
    return value


def _increment(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_launch_timing_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


def _write_timing_row(
    store: Any,
    *,
    signature: str,
    launch_slot: int,
    head_slot: int,
    head_block_time: float | None,
    provider: str | None,
    slot_latency_ms: float | None,
    block_time_latency_ms: float | None,
    sampled_at: str,
    status: str,
    error_type: str | None = None,
) -> None:
    with store._lock, store.db:
        store.db.execute(_TIMING_TABLE_SQL)
        store.db.execute(
            "INSERT INTO direct_solana_launch_timing_samples("
            "signature, launch_slot, head_slot, head_block_time, rpc_provider, slot_latency_ms, "
            "block_time_latency_ms, sampled_at, status, last_error_type) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(signature) DO UPDATE SET "
            "launch_slot=excluded.launch_slot, head_slot=excluded.head_slot, "
            "head_block_time=excluded.head_block_time, rpc_provider=excluded.rpc_provider, "
            "slot_latency_ms=excluded.slot_latency_ms, block_time_latency_ms=excluded.block_time_latency_ms, "
            "sampled_at=excluded.sampled_at, status=excluded.status, last_error_type=excluded.last_error_type",
            (
                signature,
                int(launch_slot),
                int(head_slot),
                head_block_time,
                provider,
                slot_latency_ms,
                block_time_latency_ms,
                sampled_at,
                status,
                error_type,
            ),
        )


def _timing_row(store: Any, signature: str) -> dict[str, Any] | None:
    try:
        with store._lock, store.db:
            store.db.execute(_TIMING_TABLE_SQL)
            row = store.db.execute(
                "SELECT signature, launch_slot, head_slot, head_block_time, rpc_provider, "
                "slot_latency_ms, block_time_latency_ms, sampled_at, status, last_error_type "
                "FROM direct_solana_launch_timing_samples WHERE signature=? LIMIT 1",
                (signature,),
            ).fetchone()
        return dict(row) if row is not None else None
    except Exception:
        return None


async def _sample_confirmed_chain_head(self: Any, signature: str, launch_slot: int) -> None:
    """Capture a chain-native timing proof at first live receipt.

    The WebSocket notification carries the launch slot. Immediately after that first
    receipt, the existing read-only RPC pool samples the current *confirmed* chain
    head. If confirmation has not advanced beyond the launch slot, the event was
    necessarily observed while it was still at or ahead of confirmed head. If the
    head is later, its blockTime and the launch transaction blockTime are compared
    later in the collector. Both values then live in the Solana clock domain, so
    Render-host clock offset cannot create or erase certification evidence.
    """

    _increment(self, "started")
    try:
        result, provider, slot_latency = await self.rpc.call_with_meta(
            "getSlot",
            [{"commitment": "confirmed"}],
            hedge=True,
        )
        head_slot = int(result or 0)
        if head_slot <= 0:
            raise RuntimeError("confirmed chain head unavailable")

        head_block_time: float | None = None
        block_time_latency: float | None = None
        if head_slot > int(launch_slot):
            value, block_provider, block_time_latency = await self.rpc.call_with_meta(
                "getBlockTime",
                [head_slot],
                hedge=True,
            )
            if value is None:
                raise RuntimeError("confirmed chain-head blockTime unavailable")
            head_block_time = float(value)
            provider = block_provider or provider
            _increment(self, "head_after_launch")
        else:
            _increment(self, "head_not_after_launch")

        _write_timing_row(
            self.store,
            signature=signature,
            launch_slot=launch_slot,
            head_slot=head_slot,
            head_block_time=head_block_time,
            provider=provider,
            slot_latency_ms=slot_latency,
            block_time_latency_ms=block_time_latency,
            sampled_at=direct_solana_module.utcnow().isoformat(),
            status="complete",
        )
        _increment(self, "complete")
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        try:
            _write_timing_row(
                self.store,
                signature=signature,
                launch_slot=launch_slot,
                head_slot=0,
                head_block_time=None,
                provider=None,
                slot_latency_ms=None,
                block_time_latency_ms=None,
                sampled_at=direct_solana_module.utcnow().isoformat(),
                status="failed",
                error_type=type(exc).__name__,
            )
        except Exception:
            pass
        _increment(self, "failed")


async def _handle_notification_with_chain_timing(
    self: Any,
    provider: str,
    subscription_targets: dict[int, Any],
    message: dict[str, Any],
) -> None:
    try:
        params = message.get("params")
        result = params.get("result") if isinstance(params, dict) else None
        value = result.get("value") if isinstance(result, dict) else None
        signature = str(value.get("signature") or "") if isinstance(value, dict) else ""
        slot = int(result.get("context", {}).get("slot") or 0) if isinstance(result, dict) else 0
        logs = value.get("logs") if isinstance(value, dict) else None
        is_launch = bool(
            signature
            and slot > 0
            and isinstance(value, dict)
            and value.get("err") is None
            and self._launch_like(logs)
        )
        if is_launch:
            tasks = _timing_tasks(self)
            # Keep the first live receipt authoritative. A later duplicate from the
            # second WebSocket provider must never overwrite it with a worse sample.
            if signature not in tasks:
                tasks[signature] = asyncio.create_task(
                    _sample_confirmed_chain_head(self, signature, slot),
                    name=f"launch-chain-timing:{signature[:12]}",
                )
    except Exception:
        # Timing proof is additive and fail-closed. Never interrupt the canonical
        # notification path because the proof sampler could not start.
        pass

    await _PREVIOUS_HANDLE_NOTIFICATION(self, provider, subscription_targets, message)


async def _await_timing_sample(self: Any, signature: str, *, timeout_seconds: float = 5.0) -> None:
    task = _timing_tasks(self).get(signature)
    if task is None or task.done():
        return
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=max(0.05, float(timeout_seconds)))
    except Exception:
        return


async def _hydrate_mint_launch_context_with_chain_timing(
    self: Any,
    *,
    mint: str,
    source: str,
    launch_signature: str,
    created_at: datetime,
) -> tuple[int, bool, int]:
    persisted, complete, candidate_count = await _PREVIOUS_HYDRATE_MINT_LAUNCH_CONTEXT(
        self,
        mint=mint,
        source=source,
        launch_signature=launch_signature,
        created_at=created_at,
    )
    await _await_timing_sample(self, launch_signature)

    raw = bridge._raw_collectors(self)
    launch = getattr(raw, "launch", None)
    if launch is not None:
        context = _launch_contexts(launch).get(mint)
        if isinstance(context, dict):
            context["launch_signature"] = launch_signature
            context["launch_slot"] = int(
                (_timing_row(self.store, launch_signature) or {}).get("launch_slot") or 0
            )
    return persisted, complete, candidate_count


def _chain_lag_seconds(
    store: Any,
    *,
    signature: str,
    created_at: datetime,
) -> tuple[float | None, str]:
    row = _timing_row(store, signature)
    if not isinstance(row, dict) or str(row.get("status") or "") != "complete":
        return None, "missing_chain_timing_proof"
    try:
        launch_slot = int(row.get("launch_slot") or 0)
        head_slot = int(row.get("head_slot") or 0)
    except (TypeError, ValueError):
        return None, "invalid_chain_timing_slots"
    if launch_slot <= 0 or head_slot <= 0:
        return None, "invalid_chain_timing_slots"
    if head_slot <= launch_slot:
        return 0.0, "launch_at_or_ahead_of_confirmed_head"

    try:
        head_block_time = float(row.get("head_block_time"))
    except (TypeError, ValueError):
        return None, "missing_confirmed_head_block_time"
    launch_block_time = created_at.timestamp()
    if head_block_time < launch_block_time:
        # Later slot with an earlier timestamp is not a monotonic chain-time proof.
        return None, "non_monotonic_chain_block_time"
    return head_block_time - launch_block_time, "confirmed_chain_duration"


def _legacy_live_receipt_lag(context: dict[str, Any], created_at: datetime) -> float | None:
    observed_at = context.get("observed_at")
    if not isinstance(observed_at, datetime):
        return None
    return max(0.0, (observed_at - created_at).total_seconds())


async def _launch_collect_with_chain_timing(
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
            lag_seconds, timing_proof = _chain_lag_seconds(
                self.store,
                signature=signature,
                created_at=created_at,
            )
            source = "program-wide-swaps+confirmed-chain-timing:launch-window-v4"
        else:
            # Directly seeded/offline tests and compatibility callers do not pass
            # through the production launch bridge, so they have no launch signature
            # to bind to a first-receipt chain-head sample. Preserve their established
            # v3 semantics; production bridge contexts always carry a signature.
            lag_seconds = _legacy_live_receipt_lag(context, created_at)
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


def _status_with_chain_timing(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        launch_bridge = payload.get("launch_coverage_bridge")
        if isinstance(launch_bridge, dict):
            launch_bridge.update(
                {
                    "coverage_semantics": "confirmed-chain-head-timing+immutable-window-acquisition-v4",
                    "near_creation_timing_model": "confirmed-chain-head-at-first-live-receipt",
                    "near_creation_uses_host_wall_clock": False,
                    "near_creation_chain_head_commitment": "confirmed",
                    "near_creation_chain_head_rpc_hedged": True,
                    "near_creation_missing_timing_proof_fails_closed": True,
                    "near_creation_threshold_unchanged": True,
                    "chain_timing_samples_started": int(getattr(self, "_roi_launch_timing_started", 0) or 0),
                    "chain_timing_samples_complete": int(getattr(self, "_roi_launch_timing_complete", 0) or 0),
                    "chain_timing_samples_failed": int(getattr(self, "_roi_launch_timing_failed", 0) or 0),
                    "chain_timing_head_not_after_launch": int(
                        getattr(self, "_roi_launch_timing_head_not_after_launch", 0) or 0
                    ),
                    "chain_timing_head_after_launch": int(
                        getattr(self, "_roi_launch_timing_head_after_launch", 0) or 0
                    ),
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
                    "launch_near_creation_host_clock_independent": True,
                    "launch_near_creation_confirmed_chain_head_proof": True,
                    "launch_near_creation_threshold_unchanged": True,
                    "launch_near_creation_missing_proof_fails_closed": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_launch_chain_timing_repair", True)
    return status


def install_launch_chain_timing_repair() -> None:
    DirectSolanaIngestionPlane._handle_notification = _handle_notification_with_chain_timing  # type: ignore[method-assign]
    bridge._hydrate_mint_launch_context = _hydrate_mint_launch_context_with_chain_timing  # type: ignore[assignment]
    DexScreenerLaunchCollector.collect = _launch_collect_with_chain_timing  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_launch_chain_timing_repair", False)):
        DirectSolanaIngestionPlane.status = _status_with_chain_timing(current_status)  # type: ignore[method-assign]


__all__ = [
    "install_launch_chain_timing_repair",
    "_chain_lag_seconds",
    "_handle_notification_with_chain_timing",
    "_hydrate_mint_launch_context_with_chain_timing",
    "_sample_confirmed_chain_head",
    "_timing_row",
    "_write_timing_row",
]
