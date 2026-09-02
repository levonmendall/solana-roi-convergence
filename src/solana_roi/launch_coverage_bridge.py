from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from . import direct_solana as direct_solana_module
from .direct_solana import DirectSolanaIngestionPlane
from .observation import WSOL_MINT


LAUNCH_WINDOW_SECONDS = 8.0
LAUNCH_CONTEXT_SIGNATURE_LIMIT = 96
LAUNCH_CONTEXT_CONCURRENCY = 8
LAUNCH_CONTEXT_DEADLINE_SECONDS = 35.0
LAUNCH_PAIR_LOOKUP_ATTEMPTS = 5
LAUNCH_PAIR_LOOKUP_INITIAL_DELAY_SECONDS = 0.5
LAUNCH_BRIDGE_MAX_ATTEMPTS = 4


def _instruction_rows(transaction: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    tx = transaction.get("transaction")
    message = tx.get("message") if isinstance(tx, dict) else None
    top = message.get("instructions") if isinstance(message, dict) else None
    if isinstance(top, list):
        rows.extend(row for row in top if isinstance(row, dict))
    meta = transaction.get("meta")
    inner = meta.get("innerInstructions") if isinstance(meta, dict) else None
    if isinstance(inner, list):
        for group in inner:
            if not isinstance(group, dict):
                continue
            nested = group.get("instructions")
            if isinstance(nested, list):
                rows.extend(row for row in nested if isinstance(row, dict))
    return rows


def launch_mint_from_transaction(transaction: Any) -> str | None:
    """Resolve one newly created SPL mint without provider-specific decoding.

    Parsed initialize-mint instructions are authoritative when present. Otherwise
    the bridge accepts exactly one non-WSOL mint that appears in post-token balances
    but not pre-token balances. Ambiguous transactions fail closed.
    """

    if not isinstance(transaction, dict):
        return None

    explicit: set[str] = set()
    for row in _instruction_rows(transaction):
        parsed = row.get("parsed")
        if not isinstance(parsed, dict):
            continue
        instruction_type = str(parsed.get("type") or "").lower()
        if instruction_type not in {"initializemint", "initializemint2"}:
            continue
        info = parsed.get("info")
        mint = str(info.get("mint") or "") if isinstance(info, dict) else ""
        if mint and mint != WSOL_MINT:
            explicit.add(mint)
    if len(explicit) == 1:
        return next(iter(explicit))
    if len(explicit) > 1:
        return None

    meta = transaction.get("meta")
    if not isinstance(meta, dict):
        return None

    def mints(value: Any) -> set[str]:
        result: set[str] = set()
        if not isinstance(value, list):
            return result
        for row in value:
            if not isinstance(row, dict):
                continue
            mint = str(row.get("mint") or "")
            if mint and mint != WSOL_MINT:
                result.add(mint)
        return result

    before = mints(meta.get("preTokenBalances"))
    newly_visible = mints(meta.get("postTokenBalances")) - before
    return next(iter(newly_visible)) if len(newly_visible) == 1 else None


def launch_created_at_from_transaction(transaction: Any) -> datetime | None:
    """Return the confirmed chain timestamp for a proven launch transaction."""

    if not isinstance(transaction, dict):
        return None
    try:
        block_time = float(transaction.get("blockTime") or 0.0)
    except (TypeError, ValueError):
        return None
    if block_time <= 0:
        return None
    try:
        return datetime.fromtimestamp(block_time, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


async def _pair_created_at_ready(self: Any, mint: str) -> datetime | None:
    delay = LAUNCH_PAIR_LOOKUP_INITIAL_DELAY_SECONDS
    for attempt in range(LAUNCH_PAIR_LOOKUP_ATTEMPTS):
        try:
            created = await self._pair_created_at(mint)
        except Exception:
            created = None
        if created is not None:
            return created
        if attempt + 1 < LAUNCH_PAIR_LOOKUP_ATTEMPTS:
            await asyncio.sleep(delay)
            delay = min(4.0, delay * 2.0)
    return None


def _raw_collectors(self: Any) -> Any:
    timed = getattr(self.service, "collectors", None)
    return getattr(timed, "inner", timed)


def _seed_launch_created_at(self: Any, mint: str, created_at: datetime) -> bool:
    launch = getattr(_raw_collectors(self), "launch", None)
    seed = getattr(launch, "seed_created_at", None)
    if not callable(seed):
        return False
    seed(mint, created_at)
    return True


def _coverage_row_exists(self: Any, mint: str, *, since: datetime) -> bool:
    with self.store._lock:
        row = self.store.db.execute(
            "SELECT 1 FROM program_coverage_observations "
            "WHERE token_mint=? AND assessed_at>=? LIMIT 1",
            (mint, since.isoformat()),
        ).fetchone()
    return row is not None


def _increment(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_launch_bridge_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


async def _hydrate_mint_launch_context(
    self: Any,
    *,
    mint: str,
    source: str,
    launch_signature: str,
    created_at: datetime,
) -> tuple[int, bool, int]:
    """Hydrate immutable launch-window transactions for one newly created mint.

    Event eligibility remains permanently bounded to the existing eight-second
    chain-time window. Retrieval may finish later and is retried by the durable
    hydration queue; delayed RPC completion never widens the evidence window.
    """

    window_start = created_at - timedelta(seconds=1.0)
    window_end = created_at + timedelta(seconds=LAUNCH_WINDOW_SECONDS)
    now = direct_solana_module.utcnow()
    if now < window_end:
        await asyncio.sleep((window_end - now).total_seconds())

    _increment(self, "signature_queries")
    rows, _provider, _latency = await self.rpc.get_signatures_for_address(
        mint,
        limit=LAUNCH_CONTEXT_SIGNATURE_LIMIT,
        hedge=True,
    )
    _increment(self, "signature_queries_ok")
    _increment(self, "signature_rows", len(rows))

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
    _increment(self, "window_candidate_rows", len(candidates))

    semaphore = asyncio.Semaphore(LAUNCH_CONTEXT_CONCURRENCY)
    persisted = 0
    rpc_failures = 0
    persisted_lock = asyncio.Lock()

    async def hydrate(row: dict[str, Any]) -> None:
        nonlocal persisted, rpc_failures
        async with semaphore:
            signature = str(row["signature"])
            block_time = int(row.get("blockTime") or 0)
            trigger = datetime.fromtimestamp(block_time, tz=timezone.utc)
            _increment(self, "transaction_hydrations_attempted")
            try:
                result, provider, latency = await self.rpc.get_transaction(signature, hedge=True)
                _increment(self, "transaction_hydrations_ok")
                swap = direct_solana_module.normalize_standard_transaction(
                    result,
                    signature=signature,
                    trigger_received_at=trigger,
                    source_hint=source,
                )
                if swap is None or str(swap.token_mint) != mint:
                    _increment(self, "normalization_misses")
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
                _increment(self, "normalization_matches")
                async with persisted_lock:
                    persisted += 1
            except asyncio.CancelledError:
                raise
            except Exception:
                async with persisted_lock:
                    rpc_failures += 1
                _increment(self, "context_rpc_failures")

    tasks = [asyncio.create_task(hydrate(row)) for row in candidates]
    if not tasks:
        return 0, True, 0

    timed_out = False
    try:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=LAUNCH_CONTEXT_DEADLINE_SECONDS)
    except asyncio.TimeoutError:
        timed_out = True
        _increment(self, "context_timeouts")
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    complete = not timed_out and rpc_failures == 0
    if not complete:
        _increment(self, "context_incomplete")
    return persisted, complete, len(candidates)


async def _refresh_coverage_only(self: Any, mint: str, at: datetime) -> None:
    refresh = getattr(_raw_collectors(self), "refresh_coverage", None)
    if not callable(refresh):
        raise RuntimeError("coverage collector unavailable")
    await refresh(mint, at, current_swap=None)


async def _hydrate_prospective_launch(
    self: Any,
    row: dict[str, Any],
    original: Callable[[Any, dict[str, Any]], Any],
) -> None:
    signature = str(row["signature"])
    trigger = datetime.fromisoformat(str(row["trigger_received_at"]))
    source = str(row.get("source_hint") or "").upper()
    attempts = int(row.get("attempts") or 0) + 1
    _increment(self, "attempted")

    try:
        result, provider, latency = await self._get_transaction_ready(
            signature,
            hedge=True,
            attempts=4,
        )
        if result is None:
            self.journal.finish(
                signature,
                error="confirmed launch transaction not yet available",
                retry=attempts < LAUNCH_BRIDGE_MAX_ATTEMPTS,
            )
            _increment(self, "failed")
            return

        mint = launch_mint_from_transaction(result)
        if mint is None:
            # Preserve the pre-repair path for unusual launch-like transactions
            # that cannot be resolved to exactly one mint from standard metadata.
            _increment(self, "mint_unresolved")
            await original(self, row)
            return
        _increment(self, "mint_resolved")

        created_at = launch_created_at_from_transaction(result)
        if created_at is not None:
            _increment(self, "chain_created_at")
            if _seed_launch_created_at(self, mint, created_at):
                _increment(self, "collector_timestamp_seeded")
        else:
            _increment(self, "pair_time_fallback_attempted")
            created_at = await _pair_created_at_ready(self, mint)
            if created_at is None:
                self.journal.record_hydration(
                    signature=signature,
                    source=source or None,
                    trigger_received_at=trigger,
                    hydrated_at=direct_solana_module.utcnow(),
                    rpc_provider=provider,
                    rpc_latency_ms=latency,
                    normalized=False,
                    candidate_context_prefilled=False,
                    historical_recovery=False,
                )
                self.journal.finish(
                    signature,
                    error="launch creation timestamp not yet available",
                    retry=attempts < LAUNCH_BRIDGE_MAX_ATTEMPTS,
                )
                _increment(self, "failed")
                return
            _increment(self, "pair_time_fallback_resolved")
            if _seed_launch_created_at(self, mint, created_at):
                _increment(self, "collector_timestamp_seeded")

        context_count, context_complete, _candidate_count = await _hydrate_mint_launch_context(
            self,
            mint=mint,
            source=source,
            launch_signature=signature,
            created_at=created_at,
        )
        if context_count:
            _increment(self, "context_swaps", context_count)

        assessed_at = direct_solana_module.utcnow()
        _increment(self, "coverage_refresh_attempted")
        await _refresh_coverage_only(self, mint, assessed_at)
        coverage_recorded = _coverage_row_exists(self, mint, since=trigger)
        if coverage_recorded:
            _increment(self, "coverage_rows")
            _increment(self, "coverage_refresh_recorded")

        self.journal.record_hydration(
            signature=signature,
            source=source or None,
            trigger_received_at=trigger,
            hydrated_at=direct_solana_module.utcnow(),
            rpc_provider=provider,
            rpc_latency_ms=latency,
            normalized=False,
            candidate_context_prefilled=context_count > 0,
            historical_recovery=False,
        )
        if coverage_recorded and context_complete:
            self.journal.finish(signature)
            return

        if not context_complete:
            error = "launch context acquisition incomplete; immutable launch window retained"
        else:
            error = "launch coverage observation not yet recordable"
        self.journal.finish(
            signature,
            error=error,
            retry=attempts < LAUNCH_BRIDGE_MAX_ATTEMPTS,
        )
        _increment(self, "failed")
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        self.journal.finish(
            signature,
            error=f"{type(exc).__name__}: launch coverage bridge failed closed",
            retry=attempts < LAUNCH_BRIDGE_MAX_ATTEMPTS,
        )
        _increment(self, "failed")


def _launch_aware_hydrator(
    original: Callable[[Any, dict[str, Any]], Any],
) -> Callable[[Any, dict[str, Any]], Any]:
    async def hydrate(self: Any, row: dict[str, Any]) -> None:
        if str(row.get("reason") or "") != "prospective_launch" or not str(row.get("source_hint") or ""):
            await original(self, row)
            return
        await _hydrate_prospective_launch(self, row, original)

    try:
        hydrate.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(hydrate, "_roi_launch_coverage_bridge", True)
    return hydrate


def _status_with_launch_bridge(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        payload["launch_coverage_bridge"] = {
            "enabled": True,
            "source": "standard-solana-rpc-mint-address-context",
            "non_swap_launch_transactions_supported": True,
            "mint_resolution": "initializeMint-or-unique-new-postTokenBalance",
            "creation_time_source": "confirmed-chain-blockTime-with-dexscreener-fallback",
            "launch_window_seconds": LAUNCH_WINDOW_SECONDS,
            "event_time_window_immutable": True,
            "retrieval_after_window_allowed": True,
            "mint_signature_limit": LAUNCH_CONTEXT_SIGNATURE_LIMIT,
            "context_concurrency": LAUNCH_CONTEXT_CONCURRENCY,
            "context_deadline_seconds": LAUNCH_CONTEXT_DEADLINE_SECONDS,
            "rpc_hedging": True,
            "attempted": int(getattr(self, "_roi_launch_bridge_attempted", 0) or 0),
            "mint_resolved": int(getattr(self, "_roi_launch_bridge_mint_resolved", 0) or 0),
            "mint_unresolved": int(getattr(self, "_roi_launch_bridge_mint_unresolved", 0) or 0),
            "chain_created_at": int(getattr(self, "_roi_launch_bridge_chain_created_at", 0) or 0),
            "pair_time_fallback_attempted": int(getattr(self, "_roi_launch_bridge_pair_time_fallback_attempted", 0) or 0),
            "pair_time_fallback_resolved": int(getattr(self, "_roi_launch_bridge_pair_time_fallback_resolved", 0) or 0),
            "collector_timestamp_seeded": int(getattr(self, "_roi_launch_bridge_collector_timestamp_seeded", 0) or 0),
            "signature_queries": int(getattr(self, "_roi_launch_bridge_signature_queries", 0) or 0),
            "signature_queries_ok": int(getattr(self, "_roi_launch_bridge_signature_queries_ok", 0) or 0),
            "signature_rows": int(getattr(self, "_roi_launch_bridge_signature_rows", 0) or 0),
            "window_candidate_rows": int(getattr(self, "_roi_launch_bridge_window_candidate_rows", 0) or 0),
            "transaction_hydrations_attempted": int(getattr(self, "_roi_launch_bridge_transaction_hydrations_attempted", 0) or 0),
            "transaction_hydrations_ok": int(getattr(self, "_roi_launch_bridge_transaction_hydrations_ok", 0) or 0),
            "context_rpc_failures": int(getattr(self, "_roi_launch_bridge_context_rpc_failures", 0) or 0),
            "normalization_matches": int(getattr(self, "_roi_launch_bridge_normalization_matches", 0) or 0),
            "normalization_misses": int(getattr(self, "_roi_launch_bridge_normalization_misses", 0) or 0),
            "context_timeouts": int(getattr(self, "_roi_launch_bridge_context_timeouts", 0) or 0),
            "context_incomplete": int(getattr(self, "_roi_launch_bridge_context_incomplete", 0) or 0),
            "context_swaps": int(getattr(self, "_roi_launch_bridge_context_swaps", 0) or 0),
            "coverage_refresh_attempted": int(getattr(self, "_roi_launch_bridge_coverage_refresh_attempted", 0) or 0),
            "coverage_refresh_recorded": int(getattr(self, "_roi_launch_bridge_coverage_refresh_recorded", 0) or 0),
            "coverage_rows": int(getattr(self, "_roi_launch_bridge_coverage_rows", 0) or 0),
            "failed": int(getattr(self, "_roi_launch_bridge_failed", 0) or 0),
            "candidate_activation_from_launch_bridge": False,
            "latency_samples_from_launch_bridge": False,
            "quote_samples_from_launch_bridge": False,
            "paper_authority": False,
        }
        throughput = payload.setdefault("throughput_policy", {})
        if isinstance(throughput, dict):
            throughput.update(
                {
                    "launch_context_scope": "mint-address-only",
                    "program_firehose_reopened_for_launch_context": False,
                    "non_swap_launch_coverage_bridge": True,
                    "launch_context_retrieval_retries_preserve_event_window": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_launch_coverage_bridge", True)
    return status


def install_launch_coverage_bridge() -> None:
    current_hydrator = DirectSolanaIngestionPlane._hydrate_one
    if not bool(getattr(current_hydrator, "_roi_launch_coverage_bridge", False)):
        DirectSolanaIngestionPlane._hydrate_one = _launch_aware_hydrator(current_hydrator)  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_launch_coverage_bridge", False)):
        DirectSolanaIngestionPlane.status = _status_with_launch_bridge(current_status)  # type: ignore[method-assign]


__all__ = [
    "LAUNCH_CONTEXT_SIGNATURE_LIMIT",
    "LAUNCH_WINDOW_SECONDS",
    "install_launch_coverage_bridge",
    "launch_created_at_from_transaction",
    "launch_mint_from_transaction",
]
