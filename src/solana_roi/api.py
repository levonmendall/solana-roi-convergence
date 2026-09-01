from __future__ import annotations

import asyncio
import hmac
import os
from contextlib import asynccontextmanager, suppress
from dataclasses import asdict
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

from fastapi import FastAPI, Header, HTTPException

from .config import BASELINE
from .runtime import IngestionRuntime, build_runtime


@lru_cache(maxsize=1)
def ingestion_runtime() -> IngestionRuntime:
    return build_runtime()


@asynccontextmanager
async def lifespan(app: FastAPI):
    runtime = ingestion_runtime()
    stop_event: asyncio.Event | None = None
    task: asyncio.Task[None] | None = None
    enabled = os.getenv("SOLANA_ROI_SHADOW_CLOCK_ENABLED", "").strip().lower() in {"1", "true", "yes"}
    if enabled:
        stop_event = asyncio.Event()
        task = asyncio.create_task(runtime.price_clock.run(stop_event), name="shadow-price-clock")
    try:
        yield
    finally:
        if stop_event is not None:
            stop_event.set()
        if task is not None:
            with suppress(asyncio.CancelledError):
                await task


app = FastAPI(title="Solana ROI Convergence", version="0.6.0", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, object]:
    runtime = ingestion_runtime()
    latency = runtime.latency_gate.status()
    return {
        "status": "ok",
        "paper_only": True,
        "strategy_version": BASELINE.version,
        "paper_signal_promotion_enabled": runtime.paper_signal_promotion_enabled,
        "risk_entity_plane_connected": True,
        "live_risk_collectors_connected": True,
        "latency_certified": latency["certified"],
        "post_risk_execution_price_certified": False,
    }


@app.get("/v1/strategy/baseline")
def strategy_baseline() -> dict[str, object]:
    return asdict(BASELINE)


@app.get("/v1/ingestion/status")
def ingestion_status() -> dict[str, object]:
    runtime = ingestion_runtime()
    return {
        "paper_only": True,
        "strategy_version": BASELINE.version,
        "paper_nav_usd": runtime.engine.nav_usd,
        "paper_signal_promotion_enabled": runtime.paper_signal_promotion_enabled,
        "paper_signal_promotion_blocker": runtime.paper_signal_promotion_blocker,
        "risk_entity_plane_connected": True,
        "collectors": runtime.collectors.status(),
        "latency": runtime.latency_gate.status(),
        "shadow_price_clock_enabled": os.getenv("SOLANA_ROI_SHADOW_CLOCK_ENABLED", "").strip().lower() in {"1", "true", "yes"},
        "evidence_counts": runtime.store.evidence_counts(),
        "event_chain_valid": runtime.store.verify(),
    }


@app.get("/v1/latency/status")
def latency_status() -> dict[str, object]:
    runtime = ingestion_runtime()
    return {
        **runtime.latency_gate.status(),
        "paper_only": True,
        "paper_signal_promotion_enabled": False,
        "post_risk_execution_price_certified": False,
    }


@app.get("/v1/price/{token_mint}")
def price_status(token_mint: str) -> dict[str, object]:
    runtime = ingestion_runtime()
    mark = runtime.store.latest_price_mark(token_mint)
    return {
        "token_mint": token_mint,
        "available": mark is not None,
        "latest_mark": mark,
        "paper_only": True,
    }


@app.get("/v1/risk/{token_mint}")
async def risk_snapshot(token_mint: str, scout_wallet: str | None = None) -> dict[str, object]:
    runtime = ingestion_runtime()
    now = datetime.now(timezone.utc)
    profile = runtime.registry.get(scout_wallet) if scout_wallet else None
    snapshot = await runtime.risk.snapshot(
        token_mint,
        now,
        scout_wallet=scout_wallet,
        scout_entity_id=profile.entity_id if profile is not None else None,
    )
    return {
        "token_mint": token_mint,
        "available": snapshot is not None,
        "readiness": runtime.risk.readiness(token_mint, as_of=now),
        "snapshot": asdict(snapshot) if snapshot is not None else None,
        "paper_only": True,
    }


@app.get("/v1/entity/{wallet}")
def entity_summary(wallet: str) -> dict[str, object]:
    runtime = ingestion_runtime()
    return {
        **runtime.entity_resolver.component_summary(wallet, as_of=datetime.now(timezone.utc)),
        "paper_only": True,
    }


@app.post("/v1/ingestion/helius")
async def helius_webhook(payload: Any, authorization: str | None = Header(default=None)) -> dict[str, object]:
    expected = os.getenv("HELIUS_WEBHOOK_AUTH", "").strip()
    if not expected:
        raise HTTPException(status_code=503, detail="Helius webhook authentication is not configured")
    if not hmac.compare_digest(authorization or "", expected):
        raise HTTPException(status_code=401, detail="invalid webhook authorization")
    runtime = ingestion_runtime()
    decisions = await runtime.service.ingest_webhook(payload)
    return {
        "accepted": True,
        "normalized_swap_count": len(decisions),
        "decisions": [asdict(item) for item in decisions],
        "paper_signal_promotion_enabled": runtime.paper_signal_promotion_enabled,
    }
