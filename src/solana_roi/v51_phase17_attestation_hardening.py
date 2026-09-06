from __future__ import annotations

from typing import Any

from . import v51_phase17_context_certification as phase17


ATTESTATION_HARDENING_VERSION = "v51-phase17-surface-attestation-fail-closed-v1"
ALIASES = {
    "SOLANA": ("solana", "SOLANA"),
    "FOMO": ("fomo", "FOMO"),
    "ROBINHOOD_CHAIN": ("robinhood", "ROBINHOOD_CHAIN", "robinhood_chain"),
}


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def strict_surface_attestations(forward_certification: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Require an explicit current-release attestation for every proof surface.

    Phase 17 originally retained an aggregate-attestation compatibility fallback for
    historical fixtures. That is unsafe for current production proof because an
    aggregate pass cannot prove that the exact Solana, FOMO, or Robinhood surface
    used by an economic context was present and attested. Missing surface evidence
    therefore remains missing and blocks only contexts that require that surface.
    """

    checks = _dict(_dict(forward_certification).get("checks"))
    release = _dict(checks.get("41_current_release_attestation"))
    raw_surfaces = _dict(release.get("surfaces"))
    result: dict[str, dict[str, Any]] = {}
    for surface, names in ALIASES.items():
        payload: dict[str, Any] = {}
        for name in names:
            candidate = raw_surfaces.get(name)
            if isinstance(candidate, dict):
                payload = _dict(candidate)
                break
        if payload:
            present = bool(payload.get("present", True))
            attested = bool(present and payload.get("attested"))
            reasons = list(payload.get("reasons") or [])
            if not present and "surface_attestation_missing" not in reasons:
                reasons.append("surface_attestation_missing")
            if present and not attested and not reasons:
                reasons.append("surface_attestation_not_attested")
            source = "surface_scoped_release_attestation"
        else:
            present = False
            attested = False
            reasons = ["surface_attestation_unavailable"]
            source = "missing_surface_attestation_fail_closed"
        result[surface] = {
            "present": present,
            "attested": attested,
            "reasons": reasons,
            "source": source,
        }
    return result


def install_phase17_surface_attestation_hardening() -> None:
    current = phase17._surface_attestations
    if bool(getattr(current, "_roi_surface_attestation_fail_closed", False)):
        return
    setattr(strict_surface_attestations, "_roi_surface_attestation_fail_closed", True)
    phase17._surface_attestations = strict_surface_attestations


def status() -> dict[str, Any]:
    return {
        "version": ATTESTATION_HARDENING_VERSION,
        "surface_scoped_attestation_required": True,
        "aggregate_attestation_fallback_allowed": False,
        "paper_only": True,
        "live_money_authority": False,
        "changes_strategy_authority": False,
        "changes_economic_thresholds": False,
    }


__all__ = [
    "ATTESTATION_HARDENING_VERSION",
    "install_phase17_surface_attestation_hardening",
    "status",
    "strict_surface_attestations",
]
