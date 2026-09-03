from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from . import coverage_completeness_repair as coverage
from . import direct_funding
from . import launch_coverage_bridge as bridge
from .direct_funding import SolanaRpcFundingCollector
from .direct_solana import DirectSolanaIngestionPlane
from .launch_funding import FundingSource
from .live_collectors import _fresh
from .risk import EntityLink, FundingEvidence, RiskDimension


FUNDING_RPC_ATTEMPTS = 2
FUNDING_SOURCE_CONCURRENCY = 3


def _increment(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_funding_provenance_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


def _failure_counts(self: Any) -> dict[str, int]:
    value = getattr(self, "_roi_funding_provenance_failure_counts", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_funding_provenance_failure_counts", value)
    return value


def _record_failure(self: Any, reason: str) -> None:
    _increment(self, "failed")
    counts = _failure_counts(self)
    counts[str(reason)] = int(counts.get(str(reason), 0) or 0) + 1
    setattr(self, "_roi_funding_provenance_last_failure", str(reason))


def _native_inbound_transfers_extended(transaction: dict[str, Any], wallet: str) -> list[tuple[str, int]]:
    """Recognize every parsed System Program instruction that can fund a wallet.

    The previous parser recognized only transfer/transferWithSeed. Fresh burner
    wallets are also commonly funded when the account itself is created. Those
    createAccount/createAccountWithSeed lamports are genuine native provenance and
    must not be dropped from a complete early-buyer funding trace.
    """

    result: list[tuple[str, int]] = []
    for instruction in direct_funding._walk_instructions(transaction):
        program = str(instruction.get("program") or "")
        program_id = str(instruction.get("programId") or "")
        if program != "system" and program_id != direct_funding.SYSTEM_PROGRAM_ID:
            continue
        parsed = instruction.get("parsed")
        if not isinstance(parsed, dict):
            continue
        instruction_type = str(parsed.get("type") or "")
        if instruction_type not in {
            "transfer",
            "transferWithSeed",
            "createAccount",
            "createAccountWithSeed",
        }:
            continue
        info = parsed.get("info")
        if not isinstance(info, dict):
            continue
        if instruction_type in {"createAccount", "createAccountWithSeed"}:
            destination = str(info.get("newAccount") or info.get("destination") or "")
        else:
            destination = str(info.get("destination") or "")
        if destination != wallet:
            continue
        source = str(info.get("source") or "")
        try:
            lamports = int(info.get("lamports") or 0)
        except (TypeError, ValueError):
            continue
        if source and source != wallet and lamports > 0:
            result.append((source, lamports))
    return result


def _early_buyer_points(
    self: SolanaRpcFundingCollector,
    mint: str,
    at: datetime,
) -> list[tuple[str, datetime, int]]:
    start: datetime | None = None
    end: datetime | None = None
    try:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT pair_created_at FROM program_coverage_observations "
                "WHERE token_mint=? ORDER BY assessed_at DESC LIMIT 1",
                (mint,),
            ).fetchone()
        if row is not None:
            raw = row["pair_created_at"] if hasattr(row, "keys") else row[0]
            created = datetime.fromisoformat(str(raw))
            start = created - timedelta(seconds=1)
            end = created + timedelta(seconds=self.policy.launch_window_seconds)
    except Exception:
        start = None
        end = None

    sql = (
        "SELECT wallet, observed_at, slot FROM normalized_swaps "
        "WHERE token_mint=? AND side='buy' AND received_at<=?"
    )
    args: list[Any] = [mint, at.isoformat()]
    if start is not None and end is not None:
        sql += " AND observed_at>=? AND observed_at<=?"
        args.extend((start.isoformat(), end.isoformat()))
    sql += " ORDER BY observed_at, slot, id LIMIT 200"
    with self.store._lock:
        rows = self.store.db.execute(sql, tuple(args)).fetchall()

    seen: set[str] = set()
    result: list[tuple[str, datetime, int]] = []
    for row in rows:
        wallet = str(row["wallet"])
        if wallet in seen:
            continue
        try:
            slot = int(row["slot"] or 0)
        except (TypeError, ValueError):
            slot = 0
        seen.add(wallet)
        result.append((wallet, datetime.fromisoformat(str(row["observed_at"])), slot))
        if len(result) >= self.policy.funding_early_buyer_count:
            break
    return result


async def _signature_page_with_retry(
    self: SolanaRpcFundingCollector,
    wallet: str,
    *,
    before: str | None,
) -> list[dict[str, Any]]:
    last_error: Exception | None = None
    for attempt in range(FUNDING_RPC_ATTEMPTS):
        try:
            rows, _provider, _latency = await self.rpc.get_signatures_for_address(
                wallet,
                before=before,
                limit=1000,
                hedge=True,
            )
            if attempt:
                _increment(self, "signature_rpc_recovered_after_retry")
            return rows
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_error = exc
            _increment(self, "signature_rpc_errors")
            if attempt + 1 < FUNDING_RPC_ATTEMPTS:
                await asyncio.sleep(0.05)
    assert last_error is not None
    raise last_error


async def _transaction_with_retry(
    self: SolanaRpcFundingCollector,
    signature: str,
) -> dict[str, Any] | None:
    last_error: Exception | None = None
    for attempt in range(FUNDING_RPC_ATTEMPTS):
        try:
            tx, _provider, _latency = await self.rpc.get_transaction(signature, hedge=True)
            if isinstance(tx, dict):
                if attempt:
                    _increment(self, "transaction_rpc_recovered_after_retry")
                return tx
            last_error = RuntimeError("confirmed funding transaction unavailable")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_error = exc
            _increment(self, "transaction_rpc_errors")
        if attempt + 1 < FUNDING_RPC_ATTEMPTS:
            await asyncio.sleep(0.05)
    if last_error is not None:
        raise last_error
    return None


async def _funding_source_result_slot_aware(
    self: SolanaRpcFundingCollector,
    wallet: str,
    before_at: datetime,
    before_slot: int,
) -> tuple[FundingSource | None, bool, str]:
    start_at = before_at - timedelta(days=self.policy.funding_lookback_days)
    before_signature: str | None = None
    threshold_lamports = int(self.policy.min_funding_transfer_sol * 1_000_000_000)

    for _page_index in range(self.policy.max_history_pages):
        try:
            rows = await _signature_page_with_retry(
                self,
                wallet,
                before=before_signature,
            )
        except Exception as exc:
            return None, False, f"signature_history_rpc:{type(exc).__name__}"
        if not rows:
            return None, True, "history_exhausted"

        boundary_reached = False
        for row in rows:
            try:
                row_slot = int(row.get("slot") or 0)
            except (TypeError, ValueError):
                row_slot = 0
            try:
                block_time = int(row.get("blockTime") or 0)
            except (TypeError, ValueError):
                block_time = 0
            observed = (
                datetime.fromtimestamp(block_time, tz=timezone.utc)
                if block_time > 0
                else None
            )

            # Slot ordering is authoritative when available. It permits a real
            # same-second funding transfer that occurred in an earlier slot while
            # still excluding any transaction at/after the buyer's purchase slot.
            if before_slot > 0 and row_slot > 0:
                if row_slot >= before_slot:
                    continue
                if observed is not None and int(observed.timestamp()) == int(before_at.timestamp()):
                    _increment(self, "same_second_prebuy_rows")
            else:
                if observed is None:
                    # Without either slot ordering or chain time this row cannot be
                    # placed prospectively; keep provenance fail-closed.
                    return None, False, "history_row_missing_slot_and_block_time"
                if observed >= before_at:
                    continue

            if observed is not None and observed < start_at:
                boundary_reached = True
                break
            if row.get("err") is not None:
                continue
            signature = str(row.get("signature") or "")
            if not signature:
                continue
            try:
                tx = await _transaction_with_retry(self, signature)
            except Exception as exc:
                return None, False, f"transaction_rpc:{type(exc).__name__}"
            if not isinstance(tx, dict):
                return None, False, "transaction_unavailable"
            try:
                tx_block_time = int(tx.get("blockTime") or block_time or 0)
            except (TypeError, ValueError):
                tx_block_time = block_time
            transfer_at = (
                datetime.fromtimestamp(tx_block_time, tz=timezone.utc)
                if tx_block_time > 0
                else before_at
            )
            candidates = [
                (source, lamports)
                for source, lamports in _native_inbound_transfers_extended(tx, wallet)
                if lamports >= threshold_lamports
            ]
            if candidates:
                source, lamports = max(candidates, key=lambda item: item[1])
                return (
                    FundingSource(
                        wallet,
                        source,
                        lamports / 1_000_000_000,
                        transfer_at,
                    ),
                    True,
                    "latest_qualifying_source_found",
                )

        if boundary_reached:
            return None, True, "lookback_boundary_reached"
        if len(rows) < 1000:
            return None, True, "provider_history_exhausted_short_page"
        before_signature = str(rows[-1].get("signature") or "")
        if not before_signature:
            return None, False, "history_pagination_cursor_missing"

    return None, False, "history_page_cap_exhausted"


async def _funding_collect_slot_aware(
    self: SolanaRpcFundingCollector,
    mint: str,
    at: datetime,
) -> bool:
    if _fresh(self.risk, mint, RiskDimension.FUNDING, at):
        return True
    _increment(self, "attempted")
    buyers = _early_buyer_points(self, mint, at)
    context_complete = coverage._funding_contexts(self).get(mint)
    if context_complete is not True and len(buyers) < 3:
        _record_failure(self, "launch_window_not_attested_and_fewer_than_three_buyers")
        return False

    semaphore = asyncio.Semaphore(FUNDING_SOURCE_CONCURRENCY)

    async def trace(point: tuple[str, datetime, int]) -> tuple[tuple[str, datetime, int], FundingSource | None, bool, str]:
        wallet, buy_at, buy_slot = point
        async with semaphore:
            source, complete, reason = await _funding_source_result_slot_aware(
                self,
                wallet,
                min(at, buy_at),
                buy_slot,
            )
        return point, source, complete, reason

    results = await asyncio.gather(*(trace(point) for point in buyers))
    incomplete = [reason for _point, _source, complete, reason in results if not complete]
    if incomplete:
        _record_failure(self, incomplete[0])
        return False

    sources = [source for _point, source, _complete, _reason in results if source is not None]
    by_funder: dict[str, list[FundingSource]] = {}
    for source in sources:
        by_funder.setdefault(source.funder, []).append(source)
    for funder, group in by_funder.items():
        if len(group) < 2:
            continue
        ordered = sorted(group, key=lambda item: item.transfer_at)
        for index, left in enumerate(ordered):
            for right in ordered[index + 1:]:
                gap = abs((right.transfer_at - left.transfer_at).total_seconds())
                denom = max(left.amount_sol, right.amount_sol)
                amount_gap = abs(left.amount_sol - right.amount_sol) / denom if denom else 1.0
                if (
                    gap <= self.policy.common_funder_max_gap_seconds
                    and amount_gap <= self.policy.common_funder_amount_tolerance
                ):
                    self.risk.entity_resolver.record_link(
                        EntityLink(
                            wallet_a=left.wallet,
                            wallet_b=right.wallet,
                            relationship=f"common_recent_native_funder:{funder}",
                            confidence=self.policy.common_funder_link_confidence,
                            observed_at=max(left.transfer_at, right.transfer_at),
                            received_at=at,
                            source="solana-standard-rpc:funding-provenance-v3-slot-aware",
                        )
                    )

    self.risk.record_funding(
        mint,
        FundingEvidence(tuple(wallet for wallet, _buy_at, _slot in buyers)),
        observed_at=at,
        received_at=at,
        source="solana-standard-rpc:complete-early-buyer-provenance-v3-slot-aware",
    )
    if hasattr(self.store, "mark_program_coverage_funding_complete"):
        self.store.mark_program_coverage_funding_complete(mint, assessed_at=at.isoformat())
    _increment(self, "complete")
    setattr(self, "_roi_funding_provenance_last_failure", None)
    return True


def _status_with_funding_provenance(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        launch_bridge = payload.get("launch_coverage_bridge")
        raw = bridge._raw_collectors(self)
        funding = getattr(raw, "funding", None)
        if isinstance(launch_bridge, dict):
            launch_bridge.update(
                {
                    "funding_slot_ordering_before_buy": True,
                    "funding_same_second_prebuy_supported": True,
                    "funding_system_create_account_supported": True,
                    "funding_rpc_attempts_per_read": FUNDING_RPC_ATTEMPTS,
                    "funding_source_concurrency": FUNDING_SOURCE_CONCURRENCY,
                    "funding_history_page_cap_unchanged": True,
                    "funding_min_transfer_threshold_unchanged": True,
                }
            )
            if funding is not None:
                launch_bridge.update(
                    {
                        "funding_provenance_attempted": int(getattr(funding, "_roi_funding_provenance_attempted", 0) or 0),
                        "funding_provenance_complete": int(getattr(funding, "_roi_funding_provenance_complete", 0) or 0),
                        "funding_provenance_failed": int(getattr(funding, "_roi_funding_provenance_failed", 0) or 0),
                        "funding_provenance_failure_counts": dict(getattr(funding, "_roi_funding_provenance_failure_counts", {}) or {}),
                        "funding_provenance_last_failure": getattr(funding, "_roi_funding_provenance_last_failure", None),
                        "funding_same_second_prebuy_rows": int(getattr(funding, "_roi_funding_provenance_same_second_prebuy_rows", 0) or 0),
                        "funding_signature_rpc_errors": int(getattr(funding, "_roi_funding_provenance_signature_rpc_errors", 0) or 0),
                        "funding_transaction_rpc_errors": int(getattr(funding, "_roi_funding_provenance_transaction_rpc_errors", 0) or 0),
                    }
                )
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "funding_provenance_slot_ordered": True,
                    "funding_provenance_same_second_prebuy_allowed_only_earlier_slot": True,
                    "funding_provenance_create_account_lamports_supported": True,
                    "funding_provenance_history_page_cap_unchanged": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_funding_provenance_repair", True)
    return status


def install_funding_provenance_repair() -> None:
    # Patch both module globals: coverage v2 imported the helper by value, while
    # direct_funding retains its own reference for compatibility/offline callers.
    direct_funding._native_inbound_transfers = _native_inbound_transfers_extended  # type: ignore[assignment]
    coverage._native_inbound_transfers = _native_inbound_transfers_extended  # type: ignore[assignment]
    SolanaRpcFundingCollector.collect = _funding_collect_slot_aware  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_funding_provenance_repair", False)):
        DirectSolanaIngestionPlane.status = _status_with_funding_provenance(current_status)  # type: ignore[method-assign]


__all__ = [
    "FUNDING_RPC_ATTEMPTS",
    "FUNDING_SOURCE_CONCURRENCY",
    "install_funding_provenance_repair",
    "_early_buyer_points",
    "_funding_collect_slot_aware",
    "_funding_source_result_slot_aware",
    "_native_inbound_transfers_extended",
]
