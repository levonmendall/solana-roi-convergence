from __future__ import annotations

from dataclasses import asdict

from fastapi import FastAPI

from .config import BASELINE

app = FastAPI(title="Solana ROI Convergence", version="0.1.0")


@app.get("/health")
def health() -> dict[str, object]:
    return {"status": "ok", "paper_only": True, "strategy_version": BASELINE.version}


@app.get("/v1/strategy/baseline")
def strategy_baseline() -> dict[str, object]:
    return asdict(BASELINE)
