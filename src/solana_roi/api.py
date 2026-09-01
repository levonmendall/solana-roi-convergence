from __future__ import annotations

import hmac
import os
from dataclasses import asdict
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

from fastapi import FastAPI, Header, HTTPException

from .config import BASELINE
from .runtime import IngestionRuntime, build_runtime


app = FastAPI(title="Solana ROI Convergence", version="0.3.0")


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
        "risk_entity_plane_connected": True,
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
        "evidence_counts": runtime.store.evidence_counts(),
        "event_chain_valid": runtime.store.verify(),
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
