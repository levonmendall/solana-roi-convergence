from __future__ import annotations

"""Fail-closed serialization for the one-time legacy-to-active storage transition.

The compact active-store shadow must not race the isolated certifier while both are
reading the multi-gigabyte legacy SQLite database.  During the explicit transition
window an operator can quiesce the certifier service.  The authoritative service then
requires only the shadow migration to settle before normal paper-runtime workers are
released.  Certification remains unavailable/fail-closed until active storage is
made authoritative and this transition flag is removed.

This module changes startup ordering only.  It grants no storage-activation,
certification, strategy, signing, submission, or live-money authority.
"""

import asyncio
import os
from functools import wraps
from typing import Any

VERSION = "storage-transition-certifier-quiesce-v1"
QUIESCE_ENV = "SOLANA_ROI_STORAGE_TRANSITION_CERTIFIER_QUIESCED"
SHADOW_ENV = "SOLANA_ROI_ACTIVE_STORAGE_SHADOW"
ACTIVE_ENV = "SOLANA_ROI_ACTIVE_STORAGE_ENABLED"

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_CONFIGURED = False
_ORIGINALS: dict[str, Any] = {}


def _env_true(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def transition_certifier_quiesce_requested() -> bool:
    """Return true only for legacy-authoritative shadow preparation."""
    return (
        _env_true(QUIESCE_ENV)
        and _env_true(SHADOW_ENV)
        and not _env_true(ACTIVE_ENV)
    )


async def _wait_for_shadow_only(stop: Any, render_bootstrap: Any, timeout_seconds: float) -> str:
    """Wait for the non-authoritative shadow while certification is quiesced.

    This is intentionally an availability gate, not an activation gate.  A timeout
    or failed shadow releases the paper runtime but never authorizes active storage.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0.01, float(timeout_seconds))
    print(
        "ROI_BOOTSTRAP_WORKER_GATE "
        f"event=waiting timeout_seconds={float(timeout_seconds):.3f} "
        "requires_bootstrap_complete=false requires_certifier_quiesced=true "
        "requires_shadow_settled=true storage_activation_authorized=false "
        "paper_only=true live_money_authority=false",
        flush=True,
    )
    while True:
        if bool(stop.is_set()):
            print("ROI_BOOTSTRAP_WORKER_GATE event=stopped", flush=True)
            return "stopped"
        shadow_state = str(render_bootstrap._shadow_state(render_bootstrap)) if hasattr(render_bootstrap, "_shadow_state") else ""
        # render_runtime_bootstrap_repair owns _SHADOW_STATE directly; use it when
        # the lease helper has not exported a state accessor.
        state = getattr(render_bootstrap, "_SHADOW_STATE", None)
        if isinstance(state, dict):
            shadow_state = str(state.get("state") or "idle")
        if shadow_state in {"not_requested", "completed", "failed_closed", "stopped_before_start"}:
            print(
                "ROI_BOOTSTRAP_WORKER_GATE "
                f"event=release reason=certifier_quiesced_shadow_settled shadow_state={shadow_state} "
                "storage_activation_authorized=false paper_only=true live_money_authority=false",
                flush=True,
            )
            return "released"
        remaining = deadline - loop.time()
        if remaining <= 0:
            print(
                "ROI_BOOTSTRAP_WORKER_GATE "
                f"event=timeout certifier_quiesced=true shadow_state={shadow_state} "
                "storage_activation_authorized=false paper_only=true live_money_authority=false",
                flush=True,
            )
            return "timeout"
        try:
            await asyncio.wait_for(stop.wait(), timeout=min(0.25, remaining))
        except asyncio.TimeoutError:
            continue


def configure_storage_transition_quiesce() -> None:
    """Install transition-only startup serialization before production composition."""
    global _CONFIGURED
    if _CONFIGURED or not transition_certifier_quiesce_requested():
        return

    from . import certification_bootstrap_autocheckpoint_lease as lease
    from . import render_runtime_bootstrap_repair as render_bootstrap

    _ORIGINALS["shadow_prerequisite"] = render_bootstrap._certification_bootstrap_complete_for_shadow
    _ORIGINALS["worker_gate"] = lease._wait_for_transition_worker_gate
    _ORIGINALS["lease_status"] = lease.status
    _ORIGINALS["public_status"] = render_bootstrap._public_status

    original_shadow_prerequisite = render_bootstrap._certification_bootstrap_complete_for_shadow

    @wraps(original_shadow_prerequisite)
    def shadow_prerequisite() -> bool:
        if transition_certifier_quiesce_requested():
            return True
        return bool(original_shadow_prerequisite())

    render_bootstrap._certification_bootstrap_complete_for_shadow = shadow_prerequisite

    original_worker_gate = lease._wait_for_transition_worker_gate

    @wraps(original_worker_gate)
    async def worker_gate(store: Any, stop: Any, bootstrap_module: Any) -> str:
        if not transition_certifier_quiesce_requested():
            return await original_worker_gate(store, stop, bootstrap_module)
        return await _wait_for_shadow_only(stop, bootstrap_module, lease._worker_gate_seconds())

    lease._wait_for_transition_worker_gate = worker_gate

    original_lease_status = lease.status

    @wraps(original_lease_status)
    def lease_status() -> dict[str, Any]:
        payload = dict(original_lease_status())
        requested = transition_certifier_quiesce_requested()
        payload.update(
            {
                "storage_transition_quiesce_version": VERSION,
                "transition_certifier_quiesced": requested,
                "transition_worker_gate_requires_bootstrap_complete": not requested,
                "transition_worker_gate_requires_certifier_quiesced": requested,
                "transition_worker_gate_requires_shadow_settled": True,
                "transition_worker_gate_timeout_authorizes_storage": False,
            }
        )
        return payload

    lease.status = lease_status

    original_public_status = render_bootstrap._public_status

    @wraps(original_public_status)
    def public_status() -> dict[str, Any]:
        payload = dict(original_public_status())
        payload.update(
            {
                "storage_transition_quiesce_version": VERSION,
                "storage_transition_certifier_quiesced": transition_certifier_quiesce_requested(),
                "storage_transition_certification_available": not transition_certifier_quiesce_requested(),
                "storage_transition_activation_authorized_by_quiesce": False,
            }
        )
        return payload

    render_bootstrap._public_status = public_status
    _CONFIGURED = True


def _reset_for_tests() -> None:
    global _CONFIGURED
    if not _CONFIGURED:
        return
    from . import certification_bootstrap_autocheckpoint_lease as lease
    from . import render_runtime_bootstrap_repair as render_bootstrap

    render_bootstrap._certification_bootstrap_complete_for_shadow = _ORIGINALS["shadow_prerequisite"]
    lease._wait_for_transition_worker_gate = _ORIGINALS["worker_gate"]
    lease.status = _ORIGINALS["lease_status"]
    render_bootstrap._public_status = _ORIGINALS["public_status"]
    _ORIGINALS.clear()
    _CONFIGURED = False


__all__ = [
    "ACTIVE_ENV",
    "LIVE_MONEY_AUTHORITY",
    "PAPER_ONLY",
    "QUIESCE_ENV",
    "SHADOW_ENV",
    "SIGNING_AVAILABLE",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "VERSION",
    "configure_storage_transition_quiesce",
    "transition_certifier_quiesce_requested",
]
