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
LAUNCH_CONTEXT_DEADLINE_SECONDS = 5.0
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


def _coverage_row_exists(self: Any, mint: str) -> bool:
    with self.store._lock:
        row = self.store.db.execute(
            "SELECT 1 FROM program_coverage_observations WHERE token_mint=? LIMIT 1",
            (mint,),
        ).fetchone()
    return row is not None


async def _hydrate_mint_launch_context(
    self: Any,
    *,
    mint: str,
    source: str,
    launch_signature: str,
    created_at: datetime,
) -> int:
    """Hydrate only transactions that touched the newly created mint.

    This replaces the old need to sample a whole Pump/Raydium program firehose.
    The standard RPC query is bounded to one recent-address page and each returned
    transaction must still normalize to the exact launch mint.
    """

    window_start = created_at - timedelta(seconds=1.0)
    window_end = created_at + timedelta(seconds=LAUNCH_WINDOW_SECONDS)
    now = direct_solana_module.utcnow()
    if now < window_end:
        await asyncio.sleep((window_end - now).total_seconds())

    rows, _provider, _latency = await self.rpc.get_signatures_for_address(
        mint,
        limit=LAUNCH_CONTEXT_SIGNATURE_LIMIT,
        hedge=False,
    )
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

    semaphore = asyncio.Semaphore(LAUNCH_CONTEXT_CONCURRENCY)
    persisted = 0
    persisted_lock = asyncio.Lock()

    async def hydrate(row: dict[str, Any]) -> None:
        nonlocal persisted
        async with semaphore:
            signature = str(row["signature"])
            block_time = int(row.get("blockTime") or 0)
            trigger = datetime.fromtimestamp(block_time, tz=timezone.utc)
            try:
                result, provider, latency = await self.rpc.get_transaction(signature, hedge=False)
                swap = direct_solana_module.normalize_standard_transaction(
                    result,
                    signature=signature,
                    trigger_received_at=trigger,
                    source_hint=source,
                )
                if swap is None or str(swap.token_mint) != mint:
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
                async with persisted_lock:
                    persisted += 1
            except Exception:
                return

    tasks = [asyncio.create_task(hydrate(row)) for row in candidates]
    if not tasks:
        return 0
    try:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=LAUNCH_CONTEXT_DEADLINE_SECONDS)
    except asyncio.TimeoutError:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return persisted


async def _refresh_coverage_only(self: Any, mint: str, at: datetime) -> None:
    timed = getattr(self.service, "collectors", None)
    raw = getattr(timed, "inner", timed)
    refresh = getattr(raw, "refresh_coverage", None)
    if not callable(refresh):
        raise RuntimeError("coverage collector unavailable")
    await refresh(mint, at, current_swap=None)


def _increment(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_launch_bridge_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


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
            hedge=False,
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
                error="launch pair creation metadata not yet available",
                retry=attempts < LAUNCH_BRIDGE_MAX_ATTEMPTS,
            )
            _increment(self, "failed")
            return

        context_count = await _hydrate_mint_launch_context(
            self,
            mint=mint,
            source=source,
            launch_signature=signature,
            created_at=created_at,
        )
        if context_count:
            _increment(self, "context_swaps", context_count)

        assessed_at = direct_solana_module.utcnow()
        await _refresh_coverage_only(self, mint, assessed_at)
        coverage_recorded = _coverage_row_exists(self, mint)
        if coverage_recorded:
            _increment(self, "coverage_rows")

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
        if coverage_recorded:
            self.journal.finish(signature)
            return

        self.journal.finish(
            signature,
            error="launch coverage observation not yet recordable",
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
            "source":"standard-solana-rpc-mint-address-context",
            "non_swap_launch_transactions_supported": True,
            "mint_resolution":"initializeMint-or-unique-new-postTokenBalance",
            "launch_window_seconds": LAUNCH_WINDOW_SECONDS,
            "mint_signature_limit": LAUNCH_CONTEXT_SIGNATURE_LIMIT,
            "context_concurrency": LAUNCH_CONTEXT_CONCURRENCY,
            "context_deadline_seconds": LAUNCH_CONTEXT_DEADLINE_SECONDS,
            "attempted": int(getattr(self, "_roi_launch_bridge_attempted", 0) or 0),
            "mint_resolved": int(getattr(self, "_roi_launch_bridge_mint_resolved", 0) or 0),
            "mint_unresolved": int(getattr(self, "_roi_launch_bridge_mint_unresolved", 0) or 0),
            "context_swaps": int(getattr(self, "_roi_launch_bridge_context_swaps", 0) or 0),
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
                    "launch_context_scope":"mint-address-only",
                    "program_firehose_reopened_for_launch_context": False,
                    "non_swap_launch_coverage_bridge": True,
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
    "launch_mint_from_transaction",
]
