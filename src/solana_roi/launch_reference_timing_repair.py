from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

from . import direct_solana as direct_solana_module
from . import launch_chain_timing_repair as chain_timing
from . import launch_coverage_bridge as bridge
from . import live_poll_redundancy as live_poll
from .coverage_completeness_repair import _launch_contexts
from .direct_solana import DirectSolanaIngestionPlane
from .launch_funding import DexScreenerLaunchCollector
from .live_collectors import _fresh
from .risk import LaunchEvidence, RiskDimension


REFERENCE_SAMPLE_INTERVAL_SECONDS = 0.5
REFERENCE_MAX_INFLIGHT = 2

_REFERENCE_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS direct_solana_launch_reference_samples ("
    "signature TEXT PRIMARY KEY, launch_slot INTEGER NOT NULL, reference_head_slot INTEGER NOT NULL, "
    "reference_provider TEXT, reference_rpc_latency_ms REAL NOT NULL, reference_age_ms REAL NOT NULL, "
    "reference_head_block_time REAL, captured_at TEXT NOT NULL, status TEXT NOT NULL, last_error_type TEXT)"
)

_PRE_CHAIN_HANDLE_NOTIFICATION = chain_timing._PREVIOUS_HANDLE_NOTIFICATION
_PRE_CHAIN_HYDRATE_MINT_LAUNCH_CONTEXT = chain_timing._PREVIOUS_HYDRATE_MINT_LAUNCH_CONTEXT


def _reference_state(self: Any) -> dict[str, Any]:
    state = getattr(self, "_roi_launch_reference_state", None)
    if not isinstance(state, dict):
        state = {
            "head_slot": 0,
            "provider": None,
            "rpc_latency_ms": None,
            "completed_monotonic": None,
            "completed_at": None,
            "advance_count": 0,
        }
        setattr(self, "_roi_launch_reference_state", state)
    return state


def _increment(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_launch_reference_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


def _write_reference_row(
    store: Any,
    *,
    signature: str,
    launch_slot: int,
    reference_head_slot: int,
    provider: str | None,
    rpc_latency_ms: float,
    reference_age_ms: float,
    captured_at: str,
) -> bool:
    with store._lock, store.db:
        store.db.execute(_REFERENCE_TABLE_SQL)
        cur = store.db.execute(
            "INSERT OR IGNORE INTO direct_solana_launch_reference_samples("
            "signature, launch_slot, reference_head_slot, reference_provider, reference_rpc_latency_ms, "
            "reference_age_ms, reference_head_block_time, captured_at, status, last_error_type) "
            "VALUES (?, ?, ?, ?, ?, ?, NULL, ?, 'captured', NULL)",
            (
                signature,
                int(launch_slot),
                int(reference_head_slot),
                provider,
                max(0.0, float(rpc_latency_ms)),
                max(0.0, float(reference_age_ms)),
                captured_at,
            ),
        )
        return bool(cur.rowcount == 1)


def _reference_row(store: Any, signature: str) -> dict[str, Any] | None:
    try:
        with store._lock, store.db:
            store.db.execute(_REFERENCE_TABLE_SQL)
            row = store.db.execute(
                "SELECT signature, launch_slot, reference_head_slot, reference_provider, "
                "reference_rpc_latency_ms, reference_age_ms, reference_head_block_time, "
                "captured_at, status, last_error_type "
                "FROM direct_solana_launch_reference_samples WHERE signature=? LIMIT 1",
                (signature,),
            ).fetchone()
        return dict(row) if row is not None else None
    except Exception:
        return None


def _set_reference_block_time(
    store: Any,
    signature: str,
    *,
    block_time: float | None,
    error_type: str | None = None,
) -> None:
    with store._lock, store.db:
        store.db.execute(_REFERENCE_TABLE_SQL)
        if block_time is None:
            store.db.execute(
                "UPDATE direct_solana_launch_reference_samples SET status='block_time_failed', "
                "last_error_type=? WHERE signature=?",
                (error_type or "ReferenceBlockTimeUnavailable", signature),
            )
        else:
            store.db.execute(
                "UPDATE direct_solana_launch_reference_samples SET reference_head_block_time=?, "
                "status='complete', last_error_type=NULL WHERE signature=?",
                (float(block_time), signature),
            )


async def _sample_reference_once(self: Any) -> None:
    _increment(self, "samples_started")
    try:
        result, provider, latency = await live_poll._poll_rpc(self).call_with_meta(
            "getSlot",
            [{"commitment": "confirmed"}],
            hedge=True,
        )
        head_slot = int(result or 0)
        if head_slot <= 0:
            raise RuntimeError("confirmed chain reference slot unavailable")
        completed_mono = time.monotonic()
        state = _reference_state(self)
        previous_slot = int(state.get("head_slot") or 0)
        _increment(self, "samples_complete")
        # Only a genuinely advancing confirmed head may refresh the reference age.
        # A stale/repeated load-balanced backend therefore cannot manufacture fresh
        # prospective timing evidence.
        if head_slot <= previous_slot:
            _increment(self, "samples_nonadvancing")
            return
        state.update(
            head_slot=head_slot,
            provider=provider,
            rpc_latency_ms=max(0.0, float(latency or 0.0)),
            completed_monotonic=completed_mono,
            completed_at=direct_solana_module.utcnow().isoformat(),
            advance_count=int(state.get("advance_count") or 0) + 1,
        )
        _increment(self, "samples_advanced")
    except asyncio.CancelledError:
        raise
    except Exception:
        _increment(self, "samples_failed")


async def _reference_sampler(self: Any, stop: asyncio.Event) -> None:
    pending: set[asyncio.Task[Any]] = set()
    try:
        while not stop.is_set():
            pending = {task for task in pending if not task.done()}
            if len(pending) < REFERENCE_MAX_INFLIGHT:
                task = asyncio.create_task(
                    _sample_reference_once(self),
                    name="launch-confirmed-reference-sample",
                )
                pending.add(task)
            try:
                await asyncio.wait_for(stop.wait(), timeout=REFERENCE_SAMPLE_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                continue
    finally:
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


def _wrap_run(original: Callable[[Any, asyncio.Event], Any]) -> Callable[[Any, asyncio.Event], Any]:
    async def run(self: Any, stop: asyncio.Event) -> None:
        sampler = asyncio.create_task(
            _reference_sampler(self, stop),
            name="launch-confirmed-reference-sampler",
        )
        try:
            await original(self, stop)
        finally:
            sampler.cancel()
            await asyncio.gather(sampler, return_exceptions=True)

    try:
        run.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(run, "_roi_launch_reference_timing_run", True)
    return run


def _capture_preexisting_reference(self: Any, signature: str, launch_slot: int) -> bool:
    state = _reference_state(self)
    completed = state.get("completed_monotonic")
    latency = state.get("rpc_latency_ms")
    head_slot = int(state.get("head_slot") or 0)
    # Require a warmed, advancing reference. The launch path never waits for a new
    # RPC request; absence of a preexisting proof remains fail-closed.
    if (
        completed is None
        or latency is None
        or head_slot <= 0
        or int(state.get("advance_count") or 0) < 2
    ):
        _increment(self, "launches_without_reference")
        return False
    age_ms = max(0.0, (time.monotonic() - float(completed)) * 1000.0)
    inserted = _write_reference_row(
        self.store,
        signature=signature,
        launch_slot=launch_slot,
        reference_head_slot=head_slot,
        provider=str(state.get("provider") or "") or None,
        rpc_latency_ms=float(latency),
        reference_age_ms=age_ms,
        captured_at=direct_solana_module.utcnow().isoformat(),
    )
    if inserted:
        _increment(self, "launches_with_reference")
    else:
        _increment(self, "duplicate_launch_receipts")
    return inserted


async def _handle_notification_with_preexisting_reference(
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
            _capture_preexisting_reference(self, signature, slot)
    except Exception:
        # Timing proof is additive. Canonical receipt/hydration remains authoritative;
        # a missing proof is rejected later by the unchanged coverage gate.
        pass

    # Deliberately bypass the v4 on-demand getSlot sampler. This is the exact
    # pre-v4 canonical notification path captured by launch_chain_timing_repair.
    await _PRE_CHAIN_HANDLE_NOTIFICATION(self, provider, subscription_targets, message)


async def _hydrate_mint_launch_context_with_reference(
    self: Any,
    *,
    mint: str,
    source: str,
    launch_signature: str,
    created_at: datetime,
) -> tuple[int, bool, int]:
    # Deliberately bypass the v4 waiter/on-demand timing row. Immutable launch-window
    # acquisition and coverage attestation remain the already-established v2 path.
    persisted, complete, candidate_count = await _PRE_CHAIN_HYDRATE_MINT_LAUNCH_CONTEXT(
        self,
        mint=mint,
        source=source,
        launch_signature=launch_signature,
        created_at=created_at,
    )

    row = _reference_row(self.store, launch_signature)
    if isinstance(row, dict):
        try:
            launch_slot = int(row.get("launch_slot") or 0)
            head_slot = int(row.get("reference_head_slot") or 0)
        except (TypeError, ValueError):
            launch_slot = 0
            head_slot = 0
        if head_slot > launch_slot > 0 and row.get("reference_head_block_time") is None:
            try:
                # This lookup may be slow, but its slot was fixed *before* the launch
                # arrived, so lookup latency cannot move the timing reference.
                value, _provider, _latency = await live_poll._poll_rpc(self).call_with_meta(
                    "getBlockTime",
                    [head_slot],
                    hedge=True,
                )
                if value is None:
                    raise RuntimeError("reference head blockTime unavailable")
                _set_reference_block_time(
                    self.store,
                    launch_signature,
                    block_time=float(value),
                )
                _increment(self, "reference_block_times_complete")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _set_reference_block_time(
                    self.store,
                    launch_signature,
                    block_time=None,
                    error_type=type(exc).__name__,
                )
                _increment(self, "reference_block_times_failed")
        elif head_slot > 0:
            _set_reference_block_time(
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
                (_reference_row(self.store, launch_signature) or {}).get("launch_slot") or 0
            )
    return persisted, complete, candidate_count


def _reference_lag_seconds(
    store: Any,
    *,
    signature: str,
    created_at: datetime,
) -> tuple[float | None, str]:
    row = _reference_row(store, signature)
    if not isinstance(row, dict):
        return None, "missing_preexisting_chain_reference"
    if str(row.get("status") or "") not in {"captured", "complete"}:
        return None, "incomplete_preexisting_chain_reference"
    try:
        launch_slot = int(row.get("launch_slot") or 0)
        head_slot = int(row.get("reference_head_slot") or 0)
        rpc_seconds = max(0.0, float(row.get("reference_rpc_latency_ms") or 0.0) / 1000.0)
        age_seconds = max(0.0, float(row.get("reference_age_ms") or 0.0) / 1000.0)
    except (TypeError, ValueError):
        return None, "invalid_preexisting_chain_reference"
    if launch_slot <= 0 or head_slot <= 0:
        return None, "invalid_preexisting_chain_reference"

    chain_delta = 0.0
    if head_slot > launch_slot:
        try:
            head_block_time = float(row.get("reference_head_block_time"))
        except (TypeError, ValueError):
            return None, "missing_preexisting_reference_block_time"
        launch_block_time = created_at.timestamp()
        if head_block_time < launch_block_time:
            return None, "non_monotonic_reference_block_time"
        chain_delta = head_block_time - launch_block_time

    # Conservative upper bound: the selected chain head was sampled before the
    # launch receipt. Add its RPC round-trip and monotonic age; never subtract either
    # uncertainty from the unchanged three-second near-creation gate.
    return chain_delta + rpc_seconds + age_seconds, "preexisting-confirmed-head-upper-bound"


def _timing_lag_with_v4_compatibility(
    store: Any,
    *,
    signature: str,
    created_at: datetime,
) -> tuple[float | None, str, str]:
    reference_row = _reference_row(store, signature)
    if reference_row is not None:
        lag, proof = _reference_lag_seconds(
            store,
            signature=signature,
            created_at=created_at,
        )
        return lag, proof, "program-wide-swaps+preexisting-confirmed-chain-reference:launch-window-v5"

    # Preserve the previously published v4 helper/test contract. Production v5 no
    # longer creates these on-demand rows, so a new production launch with no v5
    # reference still fails closed. Only an already-existing explicit v4 row can use
    # the compatibility path.
    if chain_timing._timing_row(store, signature) is not None:
        lag, proof = chain_timing._chain_lag_seconds(
            store,
            signature=signature,
            created_at=created_at,
        )
        return lag, proof, "program-wide-swaps+confirmed-chain-timing:launch-window-v4"
    return None, "missing_preexisting_chain_reference", "program-wide-swaps+preexisting-confirmed-chain-reference:launch-window-v5"


async def _launch_collect_with_reference_timing(
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
            lag_seconds, timing_proof, source = _timing_lag_with_v4_compatibility(
                self.store,
                signature=signature,
                created_at=created_at,
            )
        else:
            lag_seconds = chain_timing._legacy_live_receipt_lag(context, created_at)
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


def _status_with_reference_timing(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        launch_bridge = payload.get("launch_coverage_bridge")
        if isinstance(launch_bridge, dict):
            launch_bridge.update(
                {
                    "coverage_semantics": "preexisting-confirmed-head-upper-bound+immutable-window-acquisition-v5",
                    "near_creation_timing_model": "preexisting-confirmed-head-upper-bound-at-first-live-receipt",
                    "near_creation_uses_host_wall_clock": False,
                    "near_creation_launch_path_rpc_reads": False,
                    "near_creation_reference_is_preexisting": True,
                    "near_creation_reference_uncertainty_includes_rpc_rtt": True,
                    "near_creation_reference_uncertainty_includes_sample_age": True,
                    "near_creation_threshold_unchanged": True,
                    "near_creation_missing_timing_proof_fails_closed": True,
                    "reference_sample_interval_seconds": REFERENCE_SAMPLE_INTERVAL_SECONDS,
                    "reference_max_inflight": REFERENCE_MAX_INFLIGHT,
                    "reference_samples_started": int(getattr(self, "_roi_launch_reference_samples_started", 0) or 0),
                    "reference_samples_complete": int(getattr(self, "_roi_launch_reference_samples_complete", 0) or 0),
                    "reference_samples_advanced": int(getattr(self, "_roi_launch_reference_samples_advanced", 0) or 0),
                    "reference_samples_nonadvancing": int(getattr(self, "_roi_launch_reference_samples_nonadvancing", 0) or 0),
                    "reference_samples_failed": int(getattr(self, "_roi_launch_reference_samples_failed", 0) or 0),
                    "launches_with_preexisting_reference": int(getattr(self, "_roi_launch_reference_launches_with_reference", 0) or 0),
                    "launches_without_preexisting_reference": int(getattr(self, "_roi_launch_reference_launches_without_reference", 0) or 0),
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
                    "launch_near_creation_preexisting_chain_reference": True,
                    "launch_near_creation_on_demand_chain_probe": False,
                    "launch_near_creation_host_clock_independent": True,
                    "launch_near_creation_threshold_unchanged": True,
                    "launch_near_creation_missing_proof_fails_closed": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_launch_reference_timing_repair", True)
    return status


def install_launch_reference_timing_repair() -> None:
    # Preserve v4's helper identity/tests; production v5 changes only the collector
    # selection and the two launch-path hooks that previously started/waited for an
    # on-demand timing probe.
    DirectSolanaIngestionPlane._handle_notification = _handle_notification_with_preexisting_reference  # type: ignore[method-assign]
    bridge._hydrate_mint_launch_context = _hydrate_mint_launch_context_with_reference  # type: ignore[assignment]
    DexScreenerLaunchCollector.collect = _launch_collect_with_reference_timing  # type: ignore[method-assign]

    current_run = DirectSolanaIngestionPlane.run
    if not bool(getattr(current_run, "_roi_launch_reference_timing_run", False)):
        DirectSolanaIngestionPlane.run = _wrap_run(current_run)  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_launch_reference_timing_repair", False)):
        DirectSolanaIngestionPlane.status = _status_with_reference_timing(current_status)  # type: ignore[method-assign]


__all__ = [
    "REFERENCE_MAX_INFLIGHT",
    "REFERENCE_SAMPLE_INTERVAL_SECONDS",
    "install_launch_reference_timing_repair",
    "_capture_preexisting_reference",
    "_launch_collect_with_reference_timing",
    "_reference_lag_seconds",
    "_reference_row",
    "_sample_reference_once",
]
