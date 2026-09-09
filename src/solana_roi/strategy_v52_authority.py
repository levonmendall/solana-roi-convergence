from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from .v52_continuous_evolution import ContinuousStrategyEvolution, StrategyEpoch, TournamentDecision
from .v52_lane_contract import CANONICAL_LANES, LANE_DESCRIPTORS
from .v52_strategy_promotion import promote_tournament_winner

AUTHORITY_ID = "roi-convergence-v5.2-authoritative-1"
STRATEGY_VERSION = "roi-convergence-v5.2-continuation-capture-1"
# Compatibility identifier retained for persisted v5.2 release/outcome lineage. It
# is the baseline strategy epoch, not a permanent strategy freeze.
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
_CANONICAL_EVOLUTION: ContinuousStrategyEvolution | None = None


def _fraction(value: Any, name: str) -> float:
    result = float(value)
    if result < 0.0 or result > 1.0:
        raise RuntimeError(f"canonical v5.2 {name} must be between zero and one")
    return result


def authority() -> dict[str, Any]:
    """Load and validate the immutable authority boundary plus baseline strategy.

    Strategy values in this file are startup defaults. They are deliberately not
    asserted against one permanent numeric tuple: governed forward evidence may
    create later strategy epochs. Paper/live-money authority remains immutable.
    """
    payload = json.loads(_AUTHORITY_PATH.read_text(encoding="utf-8"))
    if payload.get("authority_id") != AUTHORITY_ID:
        raise RuntimeError("canonical v5.2 authority id mismatch")
    if payload.get("strategy_version") != STRATEGY_VERSION:
        raise RuntimeError("canonical v5.2 strategy version mismatch")
    if payload.get("economic_freeze_epoch") != ECONOMIC_FREEZE_EPOCH:
        raise RuntimeError("canonical v5.2 baseline economic epoch mismatch")
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
    if float(execution.get("latency_hard_max_seconds") or 0.0) <= 0.0:
        raise RuntimeError("canonical v5.2 latency ceiling must be positive")
    _fraction(execution.get("chase_observe_only_above_fraction"), "chase boundary")
    if not isinstance(execution.get("amount_specific_entry_and_exit_quotes_required"), bool):
        raise RuntimeError("canonical v5.2 exact quote policy must be boolean")
    if not isinstance(execution.get("first_slot_pump_fun_sniping_allowed"), bool):
        raise RuntimeError("canonical v5.2 first-slot policy must be boolean")
    if int(execution.get("maximum_sizing_requotes") or 0) < 0:
        raise RuntimeError("canonical v5.2 sizing requotes cannot be negative")

    sizing = dict(payload.get("target_sizing") or {})
    if int(sizing.get("minimum_forward_samples") or 0) <= 0:
        raise RuntimeError("canonical v5.2 minimum forward samples must be positive")
    if not bool(sizing.get("fresh_v52_forward_evidence_required_for_promotion")):
        raise RuntimeError("canonical v5.2 fresh-forward promotion requirement disabled")
    if bool(sizing.get("v51_outcomes_may_grant_v52_promotion")):
        raise RuntimeError("canonical v5.2 allowed v5.1 promotion evidence")
    if bool(sizing.get("v51_outcomes_may_seed_target_selection_prior")):
        raise RuntimeError("canonical v5.2 allowed v5.1 target-selection prior")
    for key in (
        "bootstrap_fraction_clean",
        "bootstrap_fraction_hazard_or_high_severity",
        "fomo_max_target_fraction",
        "robinhood_max_target_fraction",
        "robinhood_max_open_exposure_fraction",
    ):
        _fraction(sizing.get(key), key)

    position = dict(payload.get("position_management") or {})
    for key in (
        "starter_fraction_of_target",
        "max_scale_fraction_of_target_per_add",
        "first_derisk_fraction_of_position",
        "second_derisk_fraction_of_position",
        "runner_fraction_of_target",
    ):
        _fraction(position.get(key), key)
    if float(position.get("minimum_exit_depth_coverage_ratio") or 0.0) <= 0.0:
        raise RuntimeError("canonical v5.2 exit-depth coverage ratio must be positive")
    for key in (
        "scale_requires_new_forward_evidence",
        "scale_requires_price_not_below_last_add",
        "averaging_down_allowed",
        "staged_derisk_enabled",
        "runner_enabled",
        "second_leg_reentry_enabled",
    ):
        if not isinstance(position.get(key), bool):
            raise RuntimeError(f"canonical v5.2 {key} must be boolean")

    detection = dict(payload.get("detection_intelligence") or {})
    _fraction(detection.get("minimum_wallet_quality"), "minimum wallet quality")
    _fraction(detection.get("anomaly_percentile_threshold"), "anomaly percentile threshold")
    _fraction(detection.get("discovered_wallet_initial_signal_weight"), "initial wallet signal weight")
    for key in (
        "minimum_skilled_independent_clusters",
        "minimum_broad_independent_clusters",
        "minimum_comparable_peer_count",
    ):
        if int(detection.get(key) or 0) <= 0:
            raise RuntimeError(f"canonical v5.2 {key} must be positive")
    if not bool(detection.get("wallet_signal_requires_prospective_validation")):
        raise RuntimeError("canonical v5.2 prospective wallet validation disabled")
    if not bool(detection.get("creator_funder_propagation_requires_incremental_forward_alpha")):
        raise RuntimeError("canonical v5.2 creator/funder incremental-alpha requirement disabled")

    governance = dict(payload.get("governance") or {})
    if bool(governance.get("historical_promotion_authority")):
        raise RuntimeError("canonical v5.2 historical promotion authority enabled")
    if bool(governance.get("v51_control_has_final_decision_authority")):
        raise RuntimeError("canonical v5.2 authority left v5.1 final decision authority")
    if not bool(governance.get("v51_control_is_read_only")):
        raise RuntimeError("canonical v5.2 authority did not preserve v5.1 as read-only control")
    if not bool(governance.get("continuous_strategy_evolution_enabled")):
        raise RuntimeError("canonical v5.2 continuous strategy evolution disabled")
    if not bool(governance.get("protected_strategy_change_requires_forward_validation")):
        raise RuntimeError("canonical v5.2 protected strategy validation disabled")
    if not bool(governance.get("prospective_tournament_promotion_authority")):
        raise RuntimeError("canonical v5.2 prospective tournament promotion disabled")
    immutable = set(governance.get("immutable_authority_boundary") or ())
    expected_immutable = {
        "paper_only",
        "signing_available",
        "transaction_submission_available",
        "live_money_authority",
    }
    if immutable != expected_immutable:
        raise RuntimeError("canonical v5.2 immutable authority boundary mismatch")
    return payload


def authority_fingerprint() -> str:
    """Fingerprint the immutable authority manifest and baseline strategy file."""
    canonical = json.dumps(authority(), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _initial_evolution_config(payload: Mapping[str, Any]) -> dict[str, Any]:
    execution = dict(payload["execution"])
    position = dict(payload["position_management"])
    config: dict[str, Any] = {
        "absolute_latency_ceiling_seconds": float(execution["latency_hard_max_seconds"]),
        "chase_observe_only_fraction": float(execution["chase_observe_only_above_fraction"]),
        "require_exact_two_sided_quote": bool(execution["amount_specific_entry_and_exit_quotes_required"]),
        "allow_first_slot_sniping": bool(execution["first_slot_pump_fun_sniping_allowed"]),
        "allow_averaging_down": bool(position["averaging_down_allowed"]),
        "require_exact_sell_route": True,
        "require_structural_exitability": True,
    }
    for section_name in ("execution", "target_sizing", "position_management", "detection_intelligence"):
        for key, value in dict(payload[section_name]).items():
            config[f"{section_name}.{key}"] = value
    return config


def new_strategy_evolution() -> ContinuousStrategyEvolution:
    """Create an isolated governed evolution runtime from the canonical baseline."""
    return ContinuousStrategyEvolution(_initial_evolution_config(authority()), version=STRATEGY_VERSION)


def canonical_strategy_evolution() -> ContinuousStrategyEvolution:
    """Return the process-canonical strategy epoch owner used by live paper policy reads."""
    global _CANONICAL_EVOLUTION
    if _CANONICAL_EVOLUTION is None:
        _CANONICAL_EVOLUTION = new_strategy_evolution()
    return _CANONICAL_EVOLUTION


def promote_prospective_tournament_winner(
    decision: TournamentDecision,
    policy_configs: Mapping[str, Mapping[str, Any]],
) -> tuple[StrategyEpoch, ...]:
    """Promote a forward tournament winner into the canonical paper strategy.

    The promotion helper separates ordinary from protected strategy changes and
    rejects immutable authority changes before it writes any new strategy epoch.
    """
    return promote_tournament_winner(canonical_strategy_evolution(), decision, policy_configs)


def strategy_evolution_snapshot(
    evolution: ContinuousStrategyEvolution | None = None,
) -> dict[str, Any]:
    current = (evolution or canonical_strategy_evolution()).current
    return {
        "sequence": current.sequence,
        "strategy_version": current.strategy_version,
        "parent_version": current.parent_version,
        "created_at": current.created_at,
        "fingerprint": current.fingerprint,
        "rationale": current.rationale,
        "evidence_refs": list(current.evidence_refs),
        "protected_change": current.protected_change,
        "continuous_evolution_enabled": True,
        "paper_only": bool(current.config["paper_only"]),
        "signing_enabled": bool(current.config["signing_enabled"]),
        "transaction_submission_enabled": bool(current.config["transaction_submission_enabled"]),
        "live_money_authority": bool(current.config["live_money_authority"]),
    }


def _effective_section(
    section_name: str,
    evolution: ContinuousStrategyEvolution | None,
) -> dict[str, Any]:
    result = dict(authority()[section_name])
    config = (evolution or canonical_strategy_evolution()).current.config
    for key in tuple(result):
        namespaced = f"{section_name}.{key}"
        if namespaced in config:
            result[key] = config[namespaced]
    return result


def position_policy(evolution: ContinuousStrategyEvolution | None = None) -> dict[str, Any]:
    result = _effective_section("position_management", evolution)
    config = (evolution or canonical_strategy_evolution()).current.config
    result["averaging_down_allowed"] = bool(config["allow_averaging_down"])
    return result


def execution_policy(evolution: ContinuousStrategyEvolution | None = None) -> dict[str, Any]:
    result = _effective_section("execution", evolution)
    config = (evolution or canonical_strategy_evolution()).current.config
    result["latency_hard_max_seconds"] = float(config["absolute_latency_ceiling_seconds"])
    result["chase_observe_only_above_fraction"] = float(config["chase_observe_only_fraction"])
    result["amount_specific_entry_and_exit_quotes_required"] = bool(config["require_exact_two_sided_quote"])
    result["first_slot_pump_fun_sniping_allowed"] = bool(config["allow_first_slot_sniping"])
    result["exact_sell_route_required"] = bool(config["require_exact_sell_route"])
    result["structural_exitability_required"] = bool(config["require_structural_exitability"])
    return result


def target_sizing_policy(evolution: ContinuousStrategyEvolution | None = None) -> dict[str, Any]:
    return _effective_section("target_sizing", evolution)


def detection_policy(evolution: ContinuousStrategyEvolution | None = None) -> dict[str, Any]:
    return _effective_section("detection_intelligence", evolution)


def safety_manifest(evolution: ContinuousStrategyEvolution | None = None) -> dict[str, Any]:
    payload = authority()
    execution = execution_policy(evolution)
    position = position_policy(evolution)
    strategy_epoch = strategy_evolution_snapshot(evolution)
    return {
        "authority_id": AUTHORITY_ID,
        "strategy_version": STRATEGY_VERSION,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "baseline_strategy_epoch": ECONOMIC_FREEZE_EPOCH,
        "authority_fingerprint": authority_fingerprint(),
        "active_strategy_epoch": strategy_epoch,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        "canonical_lanes": list(CANONICAL_LANES),
        "mechanical_hard_stops": list(payload["mechanical_hard_stops"]),
        "latency_hard_max_seconds": float(execution["latency_hard_max_seconds"]),
        "chase_observe_only_above_fraction": float(execution["chase_observe_only_above_fraction"]),
        "amount_specific_entry_and_exit_quotes_required": bool(execution["amount_specific_entry_and_exit_quotes_required"]),
        "first_slot_pump_fun_sniping_allowed": bool(execution["first_slot_pump_fun_sniping_allowed"]),
        "averaging_down_allowed": bool(position["averaging_down_allowed"]),
        "economic_superiority_claim": bool(payload["economic_superiority_claim"]),
        "control_strategy_version": CONTROL_STRATEGY_VERSION,
        "v51_control_has_final_decision_authority": bool(payload["governance"]["v51_control_has_final_decision_authority"]),
        "v51_control_is_read_only": bool(payload["governance"]["v51_control_is_read_only"]),
        "historical_promotion_authority": bool(payload["governance"]["historical_promotion_authority"]),
        "fresh_v52_forward_evidence_required_for_promotion": bool(payload["target_sizing"]["fresh_v52_forward_evidence_required_for_promotion"]),
        "continuous_strategy_evolution_enabled": bool(payload["governance"]["continuous_strategy_evolution_enabled"]),
        "protected_strategy_change_requires_forward_validation": bool(payload["governance"]["protected_strategy_change_requires_forward_validation"]),
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
    "canonical_strategy_evolution",
    "detection_policy",
    "execution_policy",
    "new_strategy_evolution",
    "position_policy",
    "promote_prospective_tournament_winner",
    "safety_manifest",
    "strategy_evolution_snapshot",
    "target_sizing_policy",
]
