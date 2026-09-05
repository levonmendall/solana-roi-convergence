from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

AUTHORITY_ID = "roi-convergence-v5.1-consolidated-proof-1"
STRATEGY_VERSION = "roi-convergence-v5.1-context-exactness-1"
ECONOMIC_FREEZE_EPOCH = "v51-consolidated-proof-20260905"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
PIPELINE_STAGES = (
    "ingestion",
    "candidate",
    "context",
    "execution_evidence",
    "decision",
    "position",
    "settlement",
    "learning",
)

_AUTHORITY_PATH = Path(__file__).resolve().parents[2] / "strategy_v51_authority.json"


def authority() -> dict[str, Any]:
    payload = json.loads(_AUTHORITY_PATH.read_text(encoding="utf-8"))
    if payload.get("authority_id") != AUTHORITY_ID:
        raise RuntimeError("canonical v5.1 authority id mismatch")
    if payload.get("strategy_version") != STRATEGY_VERSION:
        raise RuntimeError("canonical v5.1 strategy version mismatch")
    if payload.get("economic_freeze_epoch") != ECONOMIC_FREEZE_EPOCH:
        raise RuntimeError("canonical v5.1 economic epoch mismatch")
    if payload.get("pipeline_stages") != list(PIPELINE_STAGES):
        raise RuntimeError("canonical v5.1 pipeline stage mismatch")
    if not bool(payload.get("paper_only")) or bool(payload.get("live_money_authority")):
        raise RuntimeError("canonical v5.1 authority crossed the paper-only boundary")
    if bool(payload.get("signing_available")) or bool(payload.get("transaction_submission_available")):
        raise RuntimeError("canonical v5.1 authority exposed execution authority")
    return payload


def authority_fingerprint() -> str:
    canonical = json.dumps(authority(), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def hazard_bin(severity: float | None, risk_signature: str | None = None) -> str:
    if str(risk_signature or "clean") == "clean" and float(severity or 0.0) <= 0.0:
        return "clean"
    value = max(0.0, min(1.0, float(severity or 0.0)))
    if value < 0.20:
        return "low"
    if value < 0.45:
        return "moderate"
    if value < 0.70:
        return "high"
    return "extreme"


def hazard_requirements(severity: float | None, risk_signature: str | None = None) -> dict[str, Any]:
    label = hazard_bin(severity, risk_signature)
    return dict(authority()["hazard_evidence_burden"][label], hazard_bin=label)


__all__ = [
    "AUTHORITY_ID",
    "STRATEGY_VERSION",
    "ECONOMIC_FREEZE_EPOCH",
    "PIPELINE_STAGES",
    "PAPER_ONLY",
    "LIVE_MONEY_AUTHORITY",
    "SIGNING_AVAILABLE",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "authority",
    "authority_fingerprint",
    "hazard_bin",
    "hazard_requirements",
]
