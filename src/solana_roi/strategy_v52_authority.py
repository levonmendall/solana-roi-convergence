from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .v52_lane_contract import CANONICAL_LANES, LANE_DESCRIPTORS

AUTHORITY_ID = "roi-convergence-v5.2-authoritative-1"
STRATEGY_VERSION = "roi-convergence-v5.2-continuation-capture-1"
ECONOMIC_FREEZE_EPOCH = "v52-authoritative-cutover-20260908"
CONTROL_STRATEGY_VERSION = "roi-convergence-v5.1-context-exactness-1"
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

_AUTHORITY_PATH = Path(__file__).resolve().parents[2] / "strategy_v52_authority.json"


def authority() -> dict[str, Any]:
    payload = json.loads(_AUTHORITY_PATH.read_text(encoding="utf-8"))
    if payload.get("authority_id") != AUTHORITY_ID:
        raise RuntimeError("canonical v5.2 authority id mismatch")
    if payload.get("strategy_version") != STRATEGY_VERSION:
        raise RuntimeError("canonical v5.2 strategy version mismatch")
    if payload.get("economic_freeze_epoch") != ECONOMIC_FREEZE_EPOCH:
        raise RuntimeError("canonical v5.2 economic epoch mismatch")
    if payload.get("control_strategy_version") != CONTROL_STRATEGY_VERSION:
        raise RuntimeError("canonical v5.2 control strategy mismatch")
    if payload.get("pipeline_stages") != list(PIPELINE_STAGES):
        raise RuntimeError("canonical v5.2 pipeline stage mismatch")
    if tuple(payload.get("canonical_lanes") or ()) != CANONICAL_LANES:
        raise RuntimeError("canonical v5.2 lane contract mismatch")
    lane_payload = dict(payload.get("lane_contract") or {})
    for lane, expected in LANE_DESCRIPTORS.items():
        actual = dict(lane_payload.get(lane) or {})
        expected_payload = expected.as_dict()
        expected_payload.pop("lane")
        if actual != expected_payload:
            raise RuntimeError(f"canonical v5.2 lane descriptor mismatch:{lane}")
    if not bool(payload.get("paper_only")) or bool(payload.get("live_money_authority")):
        raise RuntimeError("canonical v5.2 authority crossed the paper-only boundary")
    if bool(payload.get("signing_available")) or bool(payload.get("transaction_submission_available")):
        raise RuntimeError("canonical v5.2 authority exposed execution authority")
    execution = dict(payload.get("execution") or {})
    if float(execution.get("latency_hard_max_seconds") or 0.0) != 20.0:
        raise RuntimeError("canonical v5.2 latency hard max changed")
    if float(execution.get("chase_observe_only_above_fraction") or 0.0) != 0.40:
        raise RuntimeError("canonical v5.2 chase boundary changed")
    if not bool(execution.get("amount_specific_entry_and_exit_quotes_required")):
        raise RuntimeError("canonical v5.2 exact quote requirement disabled")
    if bool(execution.get("first_slot_pump_fun_sniping_allowed")):
        raise RuntimeError("canonical v5.2 first-slot sniping enabled")
    position = dict(payload.get("position_management") or {})
    if bool(position.get("averaging_down_allowed")):
        raise RuntimeError("canonical v5.2 averaging down enabled")
    if not bool(position.get("scale_requires_new_forward_evidence")):
        raise RuntimeError("canonical v5.2 scale evidence requirement disabled")
    if not bool(position.get("scale_requires_price_not_below_last_add")):
        raise RuntimeError("canonical v5.2 no-average-down guard disabled")
    governance = dict(payload.get("governance") or {})
    if bool(governance.get("historical_promotion_authority")):
        raise RuntimeError("canonical v5.2 historical promotion authority enabled")
    if bool(governance.get("automatic_parameter_mutation_authority")):
        raise RuntimeError("canonical v5.2 parameter mutation authority enabled")
    if bool(governance.get("v51_control_has_final_decision_authority")):
        raise RuntimeError("canonical v5.2 authority left v5.1 final decision authority")
    return payload


def authority_fingerprint() -> str:
    canonical = json.dumps(authority(), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def position_policy() -> dict[str, Any]:
    return dict(authority()["position_management"])


def execution_policy() -> dict[str, Any]:
    return dict(authority()["execution"])


def safety_manifest() -> dict[str, Any]:
    payload = authority()
    return {
        "authority_id": AUTHORITY_ID,
        "strategy_version": STRATEGY_VERSION,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "authority_fingerprint": authority_fingerprint(),
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        "canonical_lanes": list(CANONICAL_LANES),
        "mechanical_hard_stops": list(payload["mechanical_hard_stops"]),
        "latency_hard_max_seconds": float(payload["execution"]["latency_hard_max_seconds"]),
        "chase_observe_only_above_fraction": float(payload["execution"]["chase_observe_only_above_fraction"]),
        "amount_specific_entry_and_exit_quotes_required": bool(payload["execution"]["amount_specific_entry_and_exit_quotes_required"]),
        "first_slot_pump_fun_sniping_allowed": bool(payload["execution"]["first_slot_pump_fun_sniping_allowed"]),
        "averaging_down_allowed": bool(payload["position_management"]["averaging_down_allowed"]),
        "economic_superiority_claim": bool(payload["economic_superiority_claim"]),
        "control_strategy_version": CONTROL_STRATEGY_VERSION,
        "v51_control_has_final_decision_authority": bool(payload["governance"]["v51_control_has_final_decision_authority"]),
    }


__all__ = [
    "AUTHORITY_ID",
    "CONTROL_STRATEGY_VERSION",
    "ECONOMIC_FREEZE_EPOCH",
    "LIVE_MONEY_AUTHORITY",
    "PAPER_ONLY",
    "PIPELINE_STAGES",
    "SIGNING_AVAILABLE",
    "STRATEGY_VERSION",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "authority",
    "authority_fingerprint",
    "execution_policy",
    "position_policy",
    "safety_manifest",
]
