from __future__ import annotations

import asyncio
import hmac
import json
import os
from contextlib import asynccontextmanager, suppress
from dataclasses import asdict
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

from fastapi import Body, FastAPI, Header, HTTPException
from pydantic import BaseModel

from .activation import ARM_CONFIRMATION
from .config import BASELINE
from .direct_deployment import deployment_preflight
from .runtime import IngestionRuntime, build_runtime
from .wallet_intelligence import ContinuousWalletIntelligence


@lru_cache(maxsize=1)
def ingestion_runtime() -> IngestionRuntime:
    return build_runtime()


@lru_cache(maxsize=1)
def wallet_intelligence() -> ContinuousWalletIntelligence:
    return ContinuousWalletIntelligence(ingestion_runtime().store)


@asynccontextmanager
async def lifespan(app: FastAPI):
    runtime = ingestion_runtime()
    clock_stop: asyncio.Event | None = None
    clock_task: asyncio.Task[None] | None = None
    webhook_stop = asyncio.Event()
    direct_stop = asyncio.Event()
    webhook_task = asyncio.create_task(runtime.webhook_worker.run(webhook_stop), name="legacy-helius-webhook-worker")
    direct_task = asyncio.create_task(runtime.direct_ingestion.run(direct_stop), name="direct-solana-ingestion")
    enabled = os.getenv("SOLANA_ROI_SHADOW_CLOCK_ENABLED", "").strip().lower() in {"1", "true", "yes"}
    if enabled:
        clock_stop = asyncio.Event()
        clock_task = asyncio.create_task(runtime.price_clock.run(clock_stop), name="shadow-price-clock")
    try:
        yield
    finally:
        direct_stop.set()
        webhook_stop.set()
        if clock_stop is not None:
            clock_stop.set()
        if clock_task is not None:
            with suppress(asyncio.CancelledError):
                await clock_task
        with suppress(asyncio.CancelledError):
            await direct_task
        with suppress(asyncio.CancelledError):
            await webhook_task


app = FastAPI(title="Solana ROI Convergence", version="0.13.0", lifespan=lifespan)


class ArmRequest(BaseModel):
    confirmation: str


def _require_cohort_admin(authorization: str | None) -> None:
    expected = os.getenv("SOLANA_ROI_COHORT_ARM_AUTH", "").strip()
    if not expected:
        raise HTTPException(status_code=503, detail="forward cohort administrative authorization is not configured")
    if not hmac.compare_digest(authorization or "", expected):
        raise HTTPException(status_code=401, detail="invalid forward cohort administrative authorization")


def _require_helius(authorization: str | None) -> None:
    expected = os.getenv("HELIUS_WEBHOOK_AUTH", "").strip()
    if not expected:
        raise HTTPException(status_code=503, detail="legacy Helius webhook authentication is not configured")
    if not hmac.compare_digest(authorization or "", expected):
        raise HTTPException(status_code=401, detail="invalid webhook authorization")


def _enqueue_helius(payload: Any, authorization: str | None, *, feed: str) -> dict[str, object]:
    _require_helius(authorization)
    runtime = ingestion_runtime()
    received_at = datetime.now(timezone.utc)
    try:
        inbox_id, inserted = runtime.webhook_queue.enqueue(payload, received_at=received_at)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="invalid JSON webhook payload") from exc
    return {
        "accepted": True,
        "durably_queued": True,
        "duplicate_delivery": not inserted,
        "inbox_id": inbox_id,
        "received_at": received_at.isoformat(),
        "feed": feed,
        "legacy_compatibility_path": True,
        "paper_signal_promotion_enabled": runtime.paper_signal_promotion_enabled,
    }


def _latest_helius_bootstrap(runtime: IngestionRuntime) -> dict[str, object]:
    with runtime.store._lock:
        row = runtime.store.db.execute(
            "SELECT event_type, observed_at, payload_json FROM events "
            "WHERE event_type IN ('helius_split_webhook_bootstrap', "
            "'helius_split_webhook_bootstrap_failed', 'helius_split_webhook_bootstrap_skipped') "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
    if row is None:
        return {"available": False, "state": "not_observed", "legacy_only": True}
    payload = json.loads(str(row["payload_json"]))
    return {
        "available": True,
        "state": str(row["event_type"]),
        "observed_at": str(row["observed_at"]),
        "result": payload if isinstance(payload, dict) else {},
        "legacy_only": True,
        "production_data_plane": False,
    }


@app.get("/health")
def health() -> dict[str, object]:
    # Render liveness must never contend on the SQLite evidence plane or execute
    # certification/readiness scans. Deep health remains under the explicit
    # status endpoints; this route proves only that the HTTP process can answer.
    return {
        "status": "ok",
        "liveness_only": True,
        "paper_only": True,
        "live_money_authority": False,
        "strategy_version": BASELINE.version,
    }


@app.get("/v1/deployment/preflight")
def deployment_preflight_status() -> dict[str, object]:
    return deployment_preflight()


@app.get("/v1/deployment/helius-bootstrap")
def helius_bootstrap_status() -> dict[str, object]:
    return _latest_helius_bootstrap(ingestion_runtime())


@app.get("/v1/direct-solana/status")
def direct_solana_status() -> dict[str, object]:
    return {**ingestion_runtime().direct_ingestion.status(), "paper_only": True}


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
        "data_plane": "direct-solana",
        "direct_solana": runtime.direct_ingestion.status(),
        "webhook_queue": runtime.webhook_queue.status(),
        "helius_webhook_bootstrap": _latest_helius_bootstrap(runtime),
        "collectors": runtime.collectors.status(),
        "program_coverage": runtime.coverage_gate.status(),
        "latency": runtime.latency_gate.status(),
        "execution_quotes": runtime.quote_gate.status(),
        "forward_cohort": runtime.cohort_controller.status(),
        "wallet_intelligence": wallet_intelligence().status(),
        "deployment_preflight": deployment_preflight(),
        "certification_epoch": runtime.certification_epoch.isoformat(),
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
        "quote_transport": "Jupiter Swap V2 /order plus unsigned taker simulation",
        "solana_metadata_transport": "redundant-standard-json-rpc",
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


@app.get("/v1/wallet-intelligence/status")
def wallet_intelligence_status() -> dict[str, object]:
    status = wallet_intelligence().status()
    return {
        **status,
        "ecosystem_wide_discovery_complete": False,
        "promotion_authority": "future_immutable_cohort_only",
        "active_v3_1_mutation_allowed": False,
    }


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
def helius_webhook(payload: Any = Body(...), authorization: str | None = Header(default=None)) -> dict[str, object]:
    return _enqueue_helius(payload, authorization, feed="enhanced")


@app.post("/v1/ingestion/helius/pump-raw")
def helius_pump_raw_webhook(payload: Any = Body(...), authorization: str | None = Header(default=None)) -> dict[str, object]:
    return _enqueue_helius(payload, authorization, feed="pump_fun_raw")
