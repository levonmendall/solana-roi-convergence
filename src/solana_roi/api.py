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
from pydantic import BaseModel

from .activation import ARM_CONFIRMATION
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


app = FastAPI(title="Solana ROI Convergence", version="0.8.0", lifespan=lifespan)


class ArmRequest(BaseModel):
    confirmation: str


def _require_cohort_admin(authorization: str | None) -> None:
    expected = os.getenv("SOLANA_ROI_COHORT_ARM_AUTH", "").strip()
    if not expected:
        raise HTTPException(status_code=503, detail="forward cohort administrative authorization is not configured")
    if not hmac.compare_digest(authorization or "", expected):
        raise HTTPException(status_code=401, detail="invalid forward cohort administrative authorization")


@app.get("/health")
def health() -> dict[str, object]:
    runtime = ingestion_runtime()
    cohort = runtime.cohort_controller.status()
    return {
        "status": "ok",
        "paper_only": True,
        "live_money_authority": False,
        "strategy_version": BASELINE.version,
        "paper_signal_promotion_enabled": runtime.paper_signal_promotion_enabled,
        "risk_entity_plane_connected": True,
        "live_risk_collectors_connected": True,
        "latency_certified": cohort["latency"]["certified"],
        "amount_specific_quote_certified": cohort["execution_quotes"]["certified"],
        "program_wide_coverage_verified": cohort["coverage"]["certified"],
        "forward_cohort_ready": cohort["forward_cohort_ready"],
        "forward_cohort_armed": cohort["armed"],
        "runtime_continuity_ok": cohort["runtime_continuity_ok"],
    }


@app.get("/v1/strategy/baseline")
def strategy_baseline() -> dict[str, object]:
    return asdict(BASELINE)


@app.get("/v1/ingestion/status")
def ingestion_status() -> dict[str, object]:
    runtime = ingestion_runtime()
    return {
        "paper_only": True,
        "live_money_authority": False,
        "strategy_version": BASELINE.version,
        "paper_nav_usd": runtime.engine.nav_usd,
        "paper_cash_usd": runtime.engine.portfolio.cash_usd,
        "paper_signal_promotion_enabled": runtime.paper_signal_promotion_enabled,
        "paper_signal_promotion_blocker": runtime.paper_signal_promotion_blocker,
        "risk_entity_plane_connected": True,
        "collectors": runtime.collectors.status(),
        "program_coverage": runtime.coverage_gate.status(),
        "latency": runtime.latency_gate.status(),
        "execution_quotes": runtime.quote_gate.status(),
        "forward_cohort": runtime.cohort_controller.status(),
        "shadow_price_clock_enabled": os.getenv("SOLANA_ROI_SHADOW_CLOCK_ENABLED", "").strip().lower() in {"1", "true", "yes"},
        "price_clock_drives_paper_engine": runtime.price_clock.drive_paper_engine,
        "evidence_counts": runtime.store.evidence_counts(),
        "event_chain_valid": runtime.store.verify(),
    }


@app.get("/v1/latency/status")
def latency_status() -> dict[str, object]:
    runtime = ingestion_runtime()
    return {
        **runtime.latency_gate.status(),
        "paper_only": True,
        "paper_signal_promotion_enabled": runtime.paper_signal_promotion_enabled,
    }


@app.get("/v1/execution-quote/status")
def execution_quote_status() -> dict[str, object]:
    runtime = ingestion_runtime()
    return {
        **runtime.quote_gate.status(),
        "quote_transport": "Jupiter Swap V2 /order quote-only",
        "jupiter_configured": runtime.quote_handoff.client is not None,
        "live_transaction_execution_available": False,
        "paper_only": True,
    }


@app.get("/v1/program-coverage/status")
def program_coverage_status() -> dict[str, object]:
    runtime = ingestion_runtime()
    return {**runtime.coverage_gate.status(), "paper_only": True}


@app.get("/v1/forward-cohort/status")
def forward_cohort_status() -> dict[str, object]:
    return ingestion_runtime().cohort_controller.status()


@app.post("/v1/forward-cohort/freeze")
def freeze_forward_cohort(authorization: str | None = Header(default=None)) -> dict[str, object]:
    _require_cohort_admin(authorization)
    runtime = ingestion_runtime()
    try:
        manifest = runtime.cohort_controller.freeze_manifest()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {
        "frozen": True,
        "manifest_sha256": manifest.get("manifest_sha256"),
        "release_commit": manifest.get("release_commit"),
        "forward_cohort": runtime.cohort_controller.status(),
    }


@app.post("/v1/forward-cohort/arm")
def arm_forward_cohort(request: ArmRequest, authorization: str | None = Header(default=None)) -> dict[str, object]:
    _require_cohort_admin(authorization)
    runtime = ingestion_runtime()
    try:
        arm = runtime.cohort_controller.arm(request.confirmation)
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    runtime.price_clock.drive_paper_engine = True
    return {
        "armed": True,
        "armed_at": arm.get("armed_at"),
        "manifest_sha256": arm.get("manifest_sha256"),
        "required_confirmation": ARM_CONFIRMATION,
        "paper_only": True,
        "live_money_authority": False,
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
