from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from .launch_funding import FundingSource, LaunchFundingPolicy
from .risk import EntityLink, FundingEvidence, RiskDimension, TokenRiskIntelligence
from .live_collectors import _fresh
from .solana_rpc import SolanaRpcPool


SYSTEM_PROGRAM_ID = "11111111111111111111111111111111"


def _walk_instructions(transaction: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    tx = transaction.get("transaction")
    message = tx.get("message") if isinstance(tx, dict) else None
    top = message.get("instructions") if isinstance(message, dict) else []
    meta = transaction.get("meta")
    inner = meta.get("innerInstructions") if isinstance(meta, dict) else []
    if isinstance(top, list):
        rows.extend(row for row in top if isinstance(row, dict))
    if isinstance(inner, list):
        for group in inner:
            if not isinstance(group, dict):
                continue
            instructions = group.get("instructions")
            if isinstance(instructions, list):
                rows.extend(row for row in instructions if isinstance(row, dict))
    return rows


def _native_inbound_transfers(transaction: dict[str, Any], wallet: str) -> list[tuple[str, int]]:
    result: list[tuple[str, int]] = []
    for instruction in _walk_instructions(transaction):
        program = str(instruction.get("program") or "")
        program_id = str(instruction.get("programId") or "")
        if program != "system" and program_id != SYSTEM_PROGRAM_ID:
            continue
        parsed = instruction.get("parsed")
        if not isinstance(parsed, dict) or str(parsed.get("type") or "") not in {"transfer", "transferWithSeed"}:
            continue
        info = parsed.get("info")
        if not isinstance(info, dict) or str(info.get("destination") or "") != wallet:
            continue
        source = str(info.get("source") or "")
        try:
            lamports = int(info.get("lamports") or 0)
        except (TypeError, ValueError):
            continue
        if source and source != wallet and lamports > 0:
            result.append((source, lamports))
    return result


class SolanaRpcFundingCollector:
    """Trace early-buyer SOL provenance using only standard Solana RPC.

    History pagination is bounded exactly like the previous collector and fails
    closed unless the configured lookback boundary is reached. Individual
    transactions are hydrated newest-first and stop as soon as the latest
    qualifying inbound SOL transfer is proved.
    """

    def __init__(
        self,
        risk: TokenRiskIntelligence,
        rpc: SolanaRpcPool,
        *,
        policy: LaunchFundingPolicy | None = None,
    ):
        self.risk, self.store = risk, risk.store
        self.rpc = rpc
        self.policy = policy or LaunchFundingPolicy()

    def _early_buyers(self, mint: str, at: datetime) -> list[tuple[str, datetime]]:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT wallet, observed_at, received_at FROM normalized_swaps WHERE token_mint=? AND side='buy' "
                "AND received_at<=? ORDER BY observed_at, id LIMIT 200",
                (mint, at.isoformat()),
            ).fetchall()
        seen: set[str] = set()
        result: list[tuple[str, datetime]] = []
        for row in rows:
            wallet = str(row["wallet"])
            if wallet in seen:
                continue
            seen.add(wallet)
            result.append((wallet, datetime.fromisoformat(str(row["observed_at"]))))
            if len(result) >= self.policy.funding_early_buyer_count:
                break
        return result

    async def _signatures(self, wallet: str, before_at: datetime) -> tuple[list[dict[str, Any]], bool]:
        start_at = before_at - timedelta(days=self.policy.funding_lookback_days)
        before_signature: str | None = None
        collected: list[dict[str, Any]] = []
        covered = False
        for _ in range(self.policy.max_history_pages):
            rows, _provider, _latency = await self.rpc.get_signatures_for_address(
                wallet,
                before=before_signature,
                limit=1000,
                hedge=False,
            )
            if not rows:
                covered = True
                break
            accepted: list[dict[str, Any]] = []
            for row in rows:
                try:
                    block_time = int(row.get("blockTime") or 0)
                except (TypeError, ValueError):
                    continue
                if block_time <= 0:
                    continue
                observed = datetime.fromtimestamp(block_time, tz=timezone.utc)
                if observed >= before_at:
                    continue
                if observed < start_at:
                    covered = True
                    break
                if row.get("err") is None and row.get("signature"):
                    accepted.append(row)
            collected.extend(accepted)
            if covered:
                break
            before_signature = str(rows[-1].get("signature") or "")
            if not before_signature:
                return [], False
        return collected, covered

    async def _source(self, wallet: str, before_at: datetime) -> FundingSource | None:
        rows, covered = await self._signatures(wallet, before_at)
        if not covered:
            return None
        threshold_lamports = int(self.policy.min_funding_transfer_sol * 1_000_000_000)
        for row in rows:
            signature = str(row.get("signature") or "")
            if not signature:
                continue
            try:
                tx, _provider, _latency = await self.rpc.get_transaction(signature, hedge=False)
            except Exception:
                continue
            if not isinstance(tx, dict):
                continue
            try:
                block_time = int(tx.get("blockTime") or row.get("blockTime") or 0)
            except (TypeError, ValueError):
                block_time = 0
            transfer_at = datetime.fromtimestamp(block_time, tz=timezone.utc) if block_time > 0 else before_at
            candidates = [
                (source, lamports)
                for source, lamports in _native_inbound_transfers(tx, wallet)
                if lamports >= threshold_lamports
            ]
            if candidates:
                source, lamports = max(candidates, key=lambda item: item[1])
                return FundingSource(wallet, source, lamports / 1_000_000_000, transfer_at)
        return None

    async def collect(self, mint: str, at: datetime) -> bool:
        if _fresh(self.risk, mint, RiskDimension.FUNDING, at):
            return True
        buyers = self._early_buyers(mint, at)
        if len(buyers) < 3:
            return False
        sources: list[FundingSource] = []
        for wallet, buy_at in buyers:
            source = await self._source(wallet, min(at, buy_at))
            if source is None:
                return False
            sources.append(source)
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
                    if gap <= self.policy.common_funder_max_gap_seconds and amount_gap <= self.policy.common_funder_amount_tolerance:
                        self.risk.entity_resolver.record_link(EntityLink(
                            wallet_a=left.wallet,
                            wallet_b=right.wallet,
                            relationship=f"common_recent_native_funder:{funder}",
                            confidence=self.policy.common_funder_link_confidence,
                            observed_at=max(left.transfer_at, right.transfer_at),
                            received_at=at,
                            source="solana-standard-rpc:funding-provenance-v1",
                        ))
        self.risk.record_funding(
            mint,
            FundingEvidence(tuple(wallet for wallet, _ in buyers)),
            observed_at=at,
            received_at=at,
            source="solana-standard-rpc:complete-early-buyer-provenance-v1",
        )
        if hasattr(self.store, "mark_program_coverage_funding_complete"):
            self.store.mark_program_coverage_funding_complete(mint, assessed_at=at.isoformat())
        return True
