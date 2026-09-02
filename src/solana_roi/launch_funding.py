from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from .live_collectors import LiveRiskCollectors, build_live_collectors
from .risk import EntityLink, FundingEvidence, LaunchEvidence, TokenRiskIntelligence


@dataclass(frozen=True, slots=True)
class LaunchFundingPolicy:
    launch_window_seconds: float = 8.0
    max_pair_stream_lag_seconds: float = 3.0
    min_launch_buys: int = 3
    min_launch_buyers: int = 3
    bundled_same_slot_buyers: int = 3
    sniper_top_two_buy_share: float = 0.65
    funding_lookback_days: int = 7
    funding_early_buyer_count: int = 5
    min_funding_transfer_sol: float = 0.05
    common_funder_max_gap_seconds: float = 1800.0
    common_funder_amount_tolerance: float = 0.02
    common_funder_link_confidence: float = 0.99
    max_history_pages: int = 5


class DexScreenerLaunchCollector:
    """Derive launch structure and publish prospective coverage evidence."""

    def __init__(self, risk: TokenRiskIntelligence, *, client: Any | None = None, policy: LaunchFundingPolicy | None = None):
        self.risk, self.store = risk, risk.store
        self.client = client or httpx.AsyncClient(timeout=1.5)
        self.policy = policy or LaunchFundingPolicy()

    def _early_rows(self, mint: str, *, start: datetime, end: datetime, decision_at: datetime) -> list[dict[str, Any]]:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT signature, slot, observed_at, received_at, wallet, side, native_amount_sol "
                "FROM normalized_swaps WHERE token_mint=? AND observed_at>=? AND observed_at<=? "
                "AND received_at<=? ORDER BY observed_at, id LIMIT 1000",
                (mint, start.isoformat(), end.isoformat(), decision_at.isoformat()),
            ).fetchall()
        return [dict(r) for r in rows]

    async def collect(self, mint: str, at: datetime) -> bool:
        response = await self.client.get(f"https://api.dexscreener.com/token-pairs/v1/solana/{mint}", headers={"Accept": "application/json"})
        response.raise_for_status()
        pairs = response.json()
        if not isinstance(pairs, list):
            return False
        candidates = []
        for row in pairs:
            if not isinstance(row, dict) or row.get("chainId") != "solana" or row.get("pairCreatedAt") is None:
                continue
            liquidity = row.get("liquidity")
            try:
                usd = float(liquidity.get("usd") or 0.0) if isinstance(liquidity, dict) else 0.0
                created = datetime.fromtimestamp(float(row["pairCreatedAt"]) / 1000.0, tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                continue
            if usd > 0:
                candidates.append((usd, created))
        if not candidates:
            return False
        _, created_at = max(candidates, key=lambda item: item[0])
        if at < created_at + timedelta(seconds=self.policy.launch_window_seconds):
            return False
        rows = self._early_rows(
            mint,
            start=created_at - timedelta(seconds=1),
            end=created_at + timedelta(seconds=self.policy.launch_window_seconds),
            decision_at=at,
        )
        buys = [r for r in rows if r["side"] == "buy"]
        buyers = {str(r["wallet"]) for r in buys}
        earliest = min((datetime.fromisoformat(str(r["observed_at"])) for r in rows), default=None)
        lag_seconds = abs((earliest - created_at).total_seconds()) if earliest is not None else None
        near_creation = lag_seconds is not None and lag_seconds <= self.policy.max_pair_stream_lag_seconds
        early_complete = len(buys) >= self.policy.min_launch_buys and len(buyers) >= self.policy.min_launch_buyers
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
        bundled = max((len(v) for v in slot_buyers.values()), default=0) >= self.policy.bundled_same_slot_buyers
        top_two = sum(sorted(buyer_sol.values(), reverse=True)[:2])
        sniper_heavy = total_sol > 0 and top_two / total_sol >= self.policy.sniper_top_two_buy_share
        self.risk.record_launch(
            mint,
            LaunchEvidence(bundled_launch=bundled, sniper_heavy=sniper_heavy),
            observed_at=at,
            received_at=at,
            source="program-wide-swaps+dexscreener:launch-window-v1",
        )
        return True


@dataclass(frozen=True, slots=True)
class FundingSource:
    wallet: str
    funder: str
    amount_sol: float
    transfer_at: datetime


class HeliusFundingCollector:
    """Complete funding evidence only when every selected early buyer has traced provenance."""

    def __init__(self, risk: TokenRiskIntelligence, api_key: str, *, client: Any | None = None, policy: LaunchFundingPolicy | None = None):
        self.risk, self.store = risk, risk.store
        self.api_key = api_key
        self.client = client or httpx.AsyncClient(timeout=2.0)
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

    async def _history(self, wallet: str, before_at: datetime) -> tuple[list[dict[str, Any]], bool]:
        start_at = before_at - timedelta(days=self.policy.funding_lookback_days)
        before_signature: str | None = None
        collected: list[dict[str, Any]] = []
        covered = False
        for _ in range(self.policy.max_history_pages):
            params: dict[str, Any] = {
                "api-key": self.api_key,
                "sort-order": "desc",
                "lt-time": int(before_at.timestamp()),
                "gt-time": int(start_at.timestamp()),
            }
            if before_signature:
                params["before-signature"] = before_signature
            response = await self.client.get(
                f"https://api-mainnet.helius-rpc.com/v0/addresses/{wallet}/transactions",
                params=params,
                headers={"Accept": "application/json"},
            )
            response.raise_for_status()
            page = response.json()
            if not isinstance(page, list):
                return [], False
            if not page:
                covered = True
                break
            collected.extend(r for r in page if isinstance(r, dict))
            oldest = min((int(r.get("timestamp") or 0) for r in page if isinstance(r, dict)), default=0)
            if oldest and oldest <= int(start_at.timestamp()):
                covered = True
                break
            signature = page[-1].get("signature") if isinstance(page[-1], dict) else None
            if not signature:
                return [], False
            before_signature = str(signature)
        return collected, covered

    async def _source(self, wallet: str, before_at: datetime) -> FundingSource | None:
        rows, covered = await self._history(wallet, before_at)
        if not covered:
            return None
        best: FundingSource | None = None
        for tx in rows:
            timestamp = int(tx.get("timestamp") or 0)
            if timestamp <= 0:
                continue
            transfer_at = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            for transfer in tx.get("nativeTransfers") or []:
                if not isinstance(transfer, dict) or str(transfer.get("toUserAccount") or "") != wallet:
                    continue
                funder = str(transfer.get("fromUserAccount") or "")
                try:
                    amount_sol = float(transfer.get("amount") or 0.0) / 1_000_000_000
                except (TypeError, ValueError):
                    continue
                if not funder or funder == wallet or amount_sol < self.policy.min_funding_transfer_sol:
                    continue
                candidate = FundingSource(wallet, funder, amount_sol, transfer_at)
                if best is None or candidate.transfer_at > best.transfer_at:
                    best = candidate
        return best

    async def collect(self, mint: str, at: datetime) -> bool:
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
            ordered = sorted(group, key=lambda x: x.transfer_at)
            for i, left in enumerate(ordered):
                for right in ordered[i + 1:]:
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
                            source="helius-enhanced-history:funding-provenance-v1",
                        ))
        self.risk.record_funding(
            mint,
            FundingEvidence(tuple(wallet for wallet, _ in buyers)),
            observed_at=at,
            received_at=at,
            source="helius-enhanced-history:complete-early-buyer-provenance-v1",
        )
        if hasattr(self.store, "mark_program_coverage_funding_complete"):
            self.store.mark_program_coverage_funding_complete(mint, assessed_at=at.isoformat())
        return True


class CompleteLiveRiskCollectors(LiveRiskCollectors):
    def __init__(self, *args: Any, launch: Any = None, funding: Any = None, coverage_asserted: bool = False, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.launch, self.funding, self.coverage_asserted = launch, funding, coverage_asserted

    async def refresh(self, mint: str, at: datetime, *, current_swap: Any = None) -> None:
        await super().refresh(mint, at, current_swap=current_swap)
        tasks = []
        if self.coverage_asserted and self.launch is not None:
            tasks.append(self._safe("launch", mint, at, self.launch.collect(mint, at)))
        if self.coverage_asserted and self.funding is not None:
            tasks.append(self._safe("funding", mint, at, self.funding.collect(mint, at)))
        if tasks:
            import asyncio
            await asyncio.gather(*tasks)

    def status(self) -> dict[str, object]:
        base = super().status()
        automated = list(base["automated_dimensions"])
        blocked = []
        if self.coverage_asserted and self.launch is not None:
            automated.append("launch")
        else:
            blocked.append("launch")
        if self.coverage_asserted and self.funding is not None:
            automated.append("funding")
        else:
            blocked.append("funding")
        return {
            "automated_dimensions": automated,
            "still_fail_closed": blocked,
            "program_wide_swap_collection_configured": self.coverage_asserted,
            "program_wide_swap_coverage_verified": False,
            "configuration_is_not_coverage_proof": True,
            "latency_certified_for_forward_cohort": False,
            "paper_signal_promotion_enabled": False,
        }


def build_complete_live_collectors(risk: TokenRiskIntelligence) -> CompleteLiveRiskCollectors:
    base = build_live_collectors(risk)
    coverage = os.getenv("SOLANA_ROI_PROGRAM_WIDE_SWAP_COVERAGE", "").strip().lower() in {"1", "true", "yes"}
    api_key = os.getenv("HELIUS_API_KEY", "").strip()
    return CompleteLiveRiskCollectors(
        risk,
        authority=base.authority,
        liquidity=base.liquidity,
        deployer=base.deployer,
        flow=base.flow,
        launch=DexScreenerLaunchCollector(risk) if coverage else None,
        funding=HeliusFundingCollector(risk, api_key) if coverage and api_key else None,
        coverage_asserted=coverage,
    )
