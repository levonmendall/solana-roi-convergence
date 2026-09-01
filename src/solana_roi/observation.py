from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

import httpx

from .engine import PaperTradingEngine
from .observation_store import ObservationEventStore
from .risk import TokenRiskIntelligence


WSOL_MINT = "So11111111111111111111111111111111111111112"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


@dataclass(frozen=True, slots=True)
class LatencyCertificationPolicy:
    min_samples: int = 100
    min_complete_fresh_fraction: float = 0.95
    max_p95_end_to_end_ms: float = 5_000.0
    max_p99_end_to_end_ms: float = 10_000.0
    max_p95_ingestion_ms: float = 2_000.0


class TimedRiskCollectors:
    """Wrap risk collectors and publish actual wall-clock completion latency."""

    def __init__(
        self,
        inner: Any,
        *,
        risk: TokenRiskIntelligence,
        store: ObservationEventStore,
        now_fn: Callable[[], datetime] = utcnow,
        perf_fn: Callable[[], float] = time.perf_counter,
    ):
        self.inner = inner
        self.risk = risk
        self.store = store
        self.now_fn = now_fn
        self.perf_fn = perf_fn

    async def refresh(self, mint: str, at: datetime, *, current_swap: Any = None) -> None:
        started_at = self.now_fn()
        started_perf = self.perf_fn()
        await self.inner.refresh(mint, at, current_swap=current_swap)
        completed_at = self.now_fn()
        elapsed_ms = max(0.0, (self.perf_fn() - started_perf) * 1000.0)
        readiness = self.risk.readiness(mint, as_of=completed_at)
        trigger_observed_at = getattr(current_swap, "observed_at", at)
        trigger_received_at = getattr(current_swap, "received_at", at)
        ingestion_latency_ms = float(getattr(current_swap, "ingestion_latency_ms", 0.0) or 0.0)
        end_to_end_ms = max(0.0, (completed_at - trigger_observed_at).total_seconds() * 1000.0)
        self.store.record_risk_refresh(
            token_mint=mint,
            trigger_observed_at=trigger_observed_at.isoformat(),
            trigger_received_at=trigger_received_at.isoformat(),
            started_at=started_at.isoformat(),
            completed_at=completed_at.isoformat(),
            elapsed_ms=elapsed_ms,
            ingestion_latency_ms=ingestion_latency_ms,
            end_to_end_ms=end_to_end_ms,
            complete=bool(readiness["complete"]),
            fresh=bool(readiness["fresh"]),
            readiness=readiness,
        )

    def status(self) -> dict[str, object]:
        return self.inner.status()


class LatencyCertificationGate:
    def __init__(self, store: ObservationEventStore, *, policy: LatencyCertificationPolicy | None = None):
        self.store = store
        self.policy = policy or LatencyCertificationPolicy()

    def status(self, *, limit: int = 500) -> dict[str, object]:
        rows = self.store.recent_risk_refreshes(limit)
        complete_fresh = [row for row in rows if row["complete"] and row["fresh"]]
        e2e = [float(row["end_to_end_ms"]) for row in rows]
        ingestion = [float(row["ingestion_latency_ms"]) for row in rows]
        fraction = len(complete_fresh) / len(rows) if rows else 0.0
        p95_e2e = _percentile(e2e, 0.95)
        p99_e2e = _percentile(e2e, 0.99)
        p95_ingestion = _percentile(ingestion, 0.95)
        enough = len(rows) >= self.policy.min_samples
        passed = bool(
            enough
            and fraction >= self.policy.min_complete_fresh_fraction
            and p95_e2e is not None
            and p95_e2e <= self.policy.max_p95_end_to_end_ms
            and p99_e2e is not None
            and p99_e2e <= self.policy.max_p99_end_to_end_ms
            and p95_ingestion is not None
            and p95_ingestion <= self.policy.max_p95_ingestion_ms
        )
        return {
            "certified": passed,
            "automatic_activation": False,
            "sample_count": len(rows),
            "complete_fresh_count": len(complete_fresh),
            "complete_fresh_fraction": fraction,
            "p95_end_to_end_ms": p95_e2e,
            "p99_end_to_end_ms": p99_e2e,
            "p95_ingestion_ms": p95_ingestion,
            "requirements": {
                "min_samples": self.policy.min_samples,
                "min_complete_fresh_fraction": self.policy.min_complete_fresh_fraction,
                "max_p95_end_to_end_ms": self.policy.max_p95_end_to_end_ms,
                "max_p99_end_to_end_ms": self.policy.max_p99_end_to_end_ms,
                "max_p95_ingestion_ms": self.policy.max_p95_ingestion_ms,
            },
        }


class DexScreenerSolMarkProvider:
    """Poll the deepest WSOL-quoted Solana pool and return a SOL/token mark."""

    def __init__(self, *, client: Any | None = None, timeout_seconds: float = 1.0, now_fn: Callable[[], datetime] = utcnow):
        self.client = client or httpx.AsyncClient(timeout=timeout_seconds)
        self.now_fn = now_fn

    async def mark(self, mint: str) -> dict[str, object] | None:
        response = await self.client.get(
            f"https://api.dexscreener.com/token-pairs/v1/solana/{mint}",
            headers={"Accept": "application/json"},
        )
        response.raise_for_status()
        rows = response.json()
        if not isinstance(rows, list):
            return None
        candidates: list[tuple[float, float, str]] = []
        for row in rows:
            if not isinstance(row, dict) or row.get("chainId") != "solana":
                continue
            quote = row.get("quoteToken")
            if not isinstance(quote, dict) or str(quote.get("address") or "") != WSOL_MINT:
                continue
            liquidity = row.get("liquidity")
            try:
                liquidity_usd = float(liquidity.get("usd") or 0.0) if isinstance(liquidity, dict) else 0.0
                price_sol = float(row.get("priceNative") or 0.0)
            except (TypeError, ValueError):
                continue
            pair = str(row.get("pairAddress") or "")
            if liquidity_usd > 0 and price_sol > 0:
                candidates.append((liquidity_usd, price_sol, pair))
        if not candidates:
            return None
        _, price_sol, pair = max(candidates, key=lambda item: item[0])
        received_at = self.now_fn()
        return {
            "token_mint": mint,
            "observed_at": received_at,
            "received_at": received_at,
            "price_sol": price_sol,
            "source": "dexscreener:deepest-wsol-pair",
            "source_ref": pair or None,
        }


class ShadowPriceClock:
    """Persist periodic price paths and drive strategy clocks only when explicitly enabled."""

    def __init__(
        self,
        *,
        store: ObservationEventStore,
        engine: PaperTradingEngine,
        provider: DexScreenerSolMarkProvider | None = None,
        interval_seconds: float = 1.0,
        tracking_horizon_seconds: float = 300.0,
        drive_paper_engine: bool = False,
        now_fn: Callable[[], datetime] = utcnow,
    ):
        self.store = store
        self.engine = engine
        self.provider = provider or DexScreenerSolMarkProvider()
        self.interval_seconds = interval_seconds
        self.tracking_horizon_seconds = tracking_horizon_seconds
        self.drive_paper_engine = drive_paper_engine
        self.now_fn = now_fn

    def record_swap_mark(self, swap: Any) -> None:
        inserted = self.store.record_price_mark(
            token_mint=swap.token_mint,
            observed_at=swap.observed_at.isoformat(),
            received_at=swap.received_at.isoformat(),
            price_sol=float(swap.reference_price_sol),
            source="helius-normalized-swap",
            source_ref=str(swap.signature),
        )
        if inserted and self.drive_paper_engine:
            self.engine.on_price(swap.token_mint, swap.received_at, float(swap.reference_price_sol))

    async def tick(self) -> int:
        now = self.now_fn()
        mints = self.store.tracked_mints(as_of=now, horizon_seconds=self.tracking_horizon_seconds)
        recorded = 0
        for mint in mints:
            try:
                mark = await self.provider.mark(mint)
            except Exception as exc:
                self.store.append(
                    "price_mark_error",
                    self.now_fn().isoformat(),
                    {"token_mint": mint, "error_type": type(exc).__name__, "error": str(exc)[:500]},
                )
                continue
            if mark is None:
                continue
            inserted = self.store.record_price_mark(
                token_mint=mint,
                observed_at=mark["observed_at"].isoformat(),
                received_at=mark["received_at"].isoformat(),
                price_sol=float(mark["price_sol"]),
                source=str(mark["source"]),
                source_ref=str(mark["source_ref"]) if mark.get("source_ref") else None,
            )
            if not inserted:
                continue
            recorded += 1
            if self.drive_paper_engine:
                self.engine.on_price(mint, mark["received_at"], float(mark["price_sol"]))
        return recorded

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await self.tick()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.interval_seconds)
            except TimeoutError:
                pass
