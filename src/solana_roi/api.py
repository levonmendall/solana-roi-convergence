from __future__ import annotations

import hmac
import os
from dataclasses import asdict
from functools import lru_cache
from typing import Any

from fastapi import FastAPI, Header, HTTPException

from .config import BASELINE
from .runtime import IngestionRuntime, build_runtime


app = FastAPI(title="Solana ROI Convergence", version="0.2.0")


@lru_cache(maxsize=1)
def ingestion_runtime() -> IngestionRuntime:
    return build_runtime()


@app.get("/health")
def health() -> dict[str, object]:
    runtime = ingestion_runtime()
    return {
        "status": "ok",
        "paper_only": True,
        "strategy_version": BASELINE.version,
        "paper_signal_promotion_enabled": runtime.paper_signal_promotion_enabled,
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
        "evidence_counts": runtime.store.evidence_counts(),
        "event_chain_valid": runtime.store.verify(),
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
