from __future__ import annotations

from typing import Any, Callable

from . import v51_evidence_analytics as analytics
from . import v51_strategy_api as strategy_api
from .v51_candidate_ledger import ensure_schema as ensure_candidate_stage_schema

REPAIR_VERSION = "v51-empty-epoch-forward-slo-v1"
_ORIGINAL: Callable[[Any], dict[str, Any]] | None = None
_INSTALLED = False


def _measurable_empty_epoch(store: Any) -> dict[str, Any]:
    if _ORIGINAL is None:
        raise RuntimeError("empty-epoch SLO repair not installed")
    # Schema existence is a property of the measurement plane, not market activity.
    # Create the append-only/current-state tables even when no candidate has arrived;
    # the existing SLO then correctly reports confirmed with zero recent events.
    ensure_candidate_stage_schema(store)
    return _ORIGINAL(store)


setattr(_measurable_empty_epoch, "_roi_v51_empty_epoch_slo", True)


def install_empty_epoch_slo_repair() -> None:
    global _ORIGINAL, _INSTALLED
    if _INSTALLED:
        return
    current = analytics.build_forward_proof_slo
    if bool(getattr(current, "_roi_v51_empty_epoch_slo", False)):
        _INSTALLED = True
        return
    _ORIGINAL = current
    analytics.build_forward_proof_slo = _measurable_empty_epoch
    # v51_strategy_api imported the function by value; keep its public diagnostic
    # endpoint on the same semantics. build_evidence_validity_bundle resolves the
    # analytics module global dynamically and therefore needs no additional wrapper.
    strategy_api.build_forward_proof_slo = _measurable_empty_epoch

    # This installer is invoked only after the canonical Solana app and Robinhood
    # runtime have composed and after post-183 proof wiring is attached. Use that
    # exact late boundary to install repairs 116-123 without reopening v5.1
    # economics, the 20-second hard maximum, certification gates, or paper-only
    # authority.
    from .e2e_production_hardening_repair import install_e2e_production_hardening
    from .e2e_production_hardening_followup import (
        install_e2e_production_hardening_followup,
    )

    install_e2e_production_hardening()
    install_e2e_production_hardening_followup()
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "empty_epoch_is_measurement_unavailable": False,
        "zero_recent_events_satisfy_candidate_flow": False,
        "strategy_thresholds_changed": False,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = ["REPAIR_VERSION", "install_empty_epoch_slo_repair", "status"]
