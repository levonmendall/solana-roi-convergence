from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol, TYPE_CHECKING

import httpx

from .risk import (
    AuthorityEvidence,
    DeployerEvidence,
    FlowEvidence,
    LiquidityEvidence,
    RiskDimension,
    TokenRiskIntelligence,
)

if TYPE_CHECKING:
    from .ingestion import NormalizedSwap


class JsonRpc(Protocol):
    async def call(self, method: str, params: list[Any]) -> Any: ...


class HeliusJsonRpcClient:
    def __init__(self, api_key: str, *, timeout_seconds: float = 1.5):
        if not api_key:
            raise ValueError("Helius API key is required")
        self.url = f"https://mainnet.helius-rpc.com/?api-key={api_key}"
        self.client = httpx.AsyncClient(timeout=timeout_seconds)

    async def call(self, method: str, params: list[Any]) -> Any:
        response = await self.client.post(
            self.url,
            json={"jsonrpc": "2.0", "id": "solana-roi", "method": method, "params": params},
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("error"):
            raise RuntimeError(f"helius rpc {method} failed: {payload['error']}")
        return payload.get("result")


def _fresh(risk: TokenRiskIntelligence, mint: str, dimension: RiskDimension, at: datetime) -> bool:
    row = risk.store.latest_risk_evidence(mint, dimension.value, as_of_received_at=at.isoformat())
    if row is None:
        return False
    observed = datetime.fromisoformat(str(row["observed_at"]))
    age = (at - observed).total_seconds()
    return 0 <= age <= risk.policy.max_age(dimension)


class HeliusAuthorityCollector:
    def __init__(self, risk: TokenRiskIntelligence, rpc: JsonRpc):
        self.risk, self.rpc = risk, rpc

    async def collect(self, mint: str, at: datetime) -> bool:
        if _fresh(self.risk, mint, RiskDimension.AUTHORITY, at):
            return True
        result = await self.rpc.call("getAccountInfo", [mint, {"encoding": "jsonParsed", "commitment": "confirmed"}])
        value = result.get("value") if isinstance(result, dict) else None
        data = value.get("data") if isinstance(value, dict) else None
        parsed = data.get("parsed") if isinstance(data, dict) else None
        info = parsed.get("info") if isinstance(parsed, dict) else None
        if not isinstance(info, dict):
            return False
        self.risk.record_authority(
            mint,
            AuthorityEvidence(bool(info.get("mintAuthority")), bool(info.get("freezeAuthority"))),
            observed_at=at,
            received_at=at,
            source="helius-rpc:getAccountInfo:jsonParsed",
        )
        return True


class DexScreenerLiquidityCollector:
    def __init__(self, risk: TokenRiskIntelligence, *, client: Any | None = None, timeout_seconds: float = 1.5):
        self.risk = risk
        self.client = client or httpx.AsyncClient(timeout=timeout_seconds)

    async def collect(self, mint: str, at: datetime) -> bool:
        if _fresh(self.risk, mint, RiskDimension.LIQUIDITY, at):
            return True
        response = await self.client.get(
            f"https://api.dexscreener.com/token-pairs/v1/solana/{mint}",
            headers={"Accept": "application/json"},
        )
        response.raise_for_status()
        rows = response.json()
        if not isinstance(rows, list):
            return False
        candidates: list[tuple[float, float | None]] = []
        for row in rows:
            if not isinstance(row, dict) or row.get("chainId") != "solana":
                continue
            liquidity = row.get("liquidity")
            if not isinstance(liquidity, dict):
                continue
            try:
                usd = float(liquidity.get("usd") or 0.0)
                market_cap = float(row["marketCap"]) if row.get("marketCap") is not None else None
            except (TypeError, ValueError):
                continue
            if usd > 0:
                candidates.append((usd, market_cap if market_cap and market_cap > 0 else None))
        if not candidates:
            return False
        usd, market_cap = max(candidates, key=lambda x: x[0])
        self.risk.record_liquidity(
            mint,
            LiquidityEvidence(usd, market_cap),
            observed_at=at,
            received_at=at,
            source="dexscreener:token-pairs:max-single-pool",
        )
        return True


class HeliusDeployerCollector:
    """Record creator only if bounded mint history reaches the mint's earliest transaction."""

    def __init__(self, risk: TokenRiskIntelligence, rpc: JsonRpc, *, page_limit: int = 1000, max_pages: int = 3):
        self.risk, self.rpc = risk, rpc
        self.page_limit, self.max_pages = page_limit, max_pages

    async def collect(self, mint: str, at: datetime) -> bool:
        if _fresh(self.risk, mint, RiskDimension.DEPLOYER, at):
            return True
        before: str | None = None
        oldest: str | None = None
        exhausted = False
        for _ in range(self.max_pages):
            config: dict[str, Any] = {"limit": self.page_limit, "commitment": "confirmed"}
            if before:
                config["before"] = before
            rows = await self.rpc.call("getSignaturesForAddress", [mint, config])
            if not isinstance(rows, list) or not rows:
                exhausted = True
                break
            valid = [r for r in rows if isinstance(r, dict) and r.get("signature")]
            if not valid:
                return False
            oldest = str(valid[-1]["signature"])
            if len(rows) < self.page_limit:
                exhausted = True
                break
            before = oldest
        if not exhausted or not oldest:
            return False
        tx = await self.rpc.call(
            "getTransaction",
            [oldest, {"encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 0}],
        )
        if not isinstance(tx, dict) or (isinstance(tx.get("meta"), dict) and tx["meta"].get("err")):
            return False
        transaction = tx.get("transaction")
        message = transaction.get("message") if isinstance(transaction, dict) else None
        keys = message.get("accountKeys") if isinstance(message, dict) else None
        if not isinstance(keys, list) or not keys:
            return False
        first = keys[0]
        creator = str(first.get("pubkey") or "") if isinstance(first, dict) else str(first or "")
        if not creator:
            return False
        self.risk.record_deployer(
            mint,
            DeployerEvidence(creator),
            observed_at=at,
            received_at=at,
            source="helius-rpc:bounded-mint-history:first-fee-payer",
        )
        return True


@dataclass(frozen=True, slots=True)
class FlowCollectorPolicy:
    lookback_seconds: float = 60.0
    min_observations: int = 3
    min_unique_buyers: int = 2
    abnormal_sell_share: float = 0.60
    min_sells_for_pressure: int = 3


class PersistedSwapFlowCollector:
    def __init__(self, risk: TokenRiskIntelligence, *, policy: FlowCollectorPolicy | None = None):
        self.risk, self.store = risk, risk.store
        self.policy = policy or FlowCollectorPolicy()

    def _rows(self, mint: str, at: datetime) -> list[dict[str, Any]]:
        start = (at - timedelta(seconds=self.policy.lookback_seconds)).isoformat()
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT signature, received_at, wallet, side, native_amount_sol FROM normalized_swaps "
                "WHERE token_mint=? AND received_at>=? AND received_at<=? ORDER BY received_at, id LIMIT 1000",
                (mint, start, at.isoformat()),
            ).fetchall()
        return [dict(r) for r in rows]

    async def collect(self, mint: str, at: datetime, *, current_swap: NormalizedSwap | None = None) -> bool:
        rows = self._rows(mint, at)
        if current_swap is not None and all(str(r["signature"]) != current_swap.signature for r in rows):
            rows.append({
                "signature": current_swap.signature,
                "received_at": current_swap.received_at.isoformat(),
                "wallet": current_swap.wallet,
                "side": current_swap.side,
                "native_amount_sol": current_swap.native_amount_sol,
            })
        if len(rows) < self.policy.min_observations:
            return False
        buyers = {str(r["wallet"]) for r in rows if r["side"] == "buy"}
        if len(buyers) < self.policy.min_unique_buyers:
            return False
        first_buy_at: dict[str, str] = {}
        early_exit = False
        buy_sol = sell_sol = 0.0
        sell_count = 0
        for row in rows:
            wallet, side = str(row["wallet"]), str(row["side"])
            amount, received = float(row["native_amount_sol"]), str(row["received_at"])
            if side == "buy":
                buy_sol += amount
                first_buy_at.setdefault(wallet, received)
            elif side == "sell":
                sell_sol += amount
                sell_count += 1
                early_exit = early_exit or (wallet in first_buy_at and received >= first_buy_at[wallet])
        total = buy_sol + sell_sol
        sell_share = sell_sol / total if total > 0 else 0.0
        abnormal = sell_count >= self.policy.min_sells_for_pressure and sell_share >= self.policy.abnormal_sell_share
        self.risk.record_flow(
            mint,
            FlowEvidence(early_exit, abnormal),
            observed_at=at,
            received_at=at,
            source="normalized-swaps:60s-point-in-time-flow-v1",
        )
        return True


class LiveRiskCollectors:
    def __init__(self, risk: TokenRiskIntelligence, *, authority: Any, liquidity: Any, deployer: Any, flow: PersistedSwapFlowCollector):
        self.risk, self.authority, self.liquidity, self.deployer, self.flow = risk, authority, liquidity, deployer, flow

    async def _safe(self, name: str, mint: str, at: datetime, awaitable: Any) -> None:
        try:
            await awaitable
        except Exception as exc:
            self.risk.store.append(
                "risk_collector_error",
                at.isoformat(),
                {"collector": name, "token_mint": mint, "error_type": type(exc).__name__, "error": str(exc)[:500]},
            )

    async def refresh(self, mint: str, at: datetime, *, current_swap: NormalizedSwap | None = None) -> None:
        tasks = [self._safe("flow", mint, at, self.flow.collect(mint, at, current_swap=current_swap))]
        for name, collector in (("authority", self.authority), ("liquidity", self.liquidity), ("deployer", self.deployer)):
            if collector is not None:
                tasks.append(self._safe(name, mint, at, collector.collect(mint, at)))
        await asyncio.gather(*tasks)

    def status(self) -> dict[str, object]:
        return {
            "automated_dimensions": [name for name, collector in (("authority", self.authority), ("liquidity", self.liquidity), ("flow", self.flow), ("deployer", self.deployer)) if collector is not None],
            "still_fail_closed": ["launch", "funding"],
            "paper_signal_promotion_enabled": False,
        }


def build_live_collectors(risk: TokenRiskIntelligence) -> LiveRiskCollectors:
    api_key = os.getenv("HELIUS_API_KEY", "").strip()
    rpc = HeliusJsonRpcClient(api_key) if api_key else None
    return LiveRiskCollectors(
        risk,
        authority=HeliusAuthorityCollector(risk, rpc) if rpc else None,
        liquidity=DexScreenerLiquidityCollector(risk),
        deployer=HeliusDeployerCollector(risk, rpc) if rpc else None,
        flow=PersistedSwapFlowCollector(risk),
    )
