from __future__ import annotations

from typing import Any

from . import canonical_worker_isolation_repair as canonical
from . import continuation_market_recalibration as continuation


REPAIR_VERSION = "fomo-canonical-worker-binding-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_INSTALLED = False
_STATE: dict[str, Any] = {
    "installed": False,
    "bound": False,
    "state": "not_started",
    "predecessor_name": None,
}


def binding_status() -> dict[str, Any]:
    payload = dict(_STATE)
    payload.update(
        {
            "repair_version": REPAIR_VERSION,
            "canonical_isolation_installed": bool(getattr(canonical, "_INSTALLED", False)),
            "continuation_installed": bool(getattr(continuation, "_INSTALLED", False)),
            "canonical_worker_points_to_fomo_wrapper": (
                canonical._ORIGINAL_RUNTIME_WORKERS
                is continuation._canonical_workers_with_independent_fomo
            ),
            "strategy_thresholds_changed": False,
            "provider_scope_changed": False,
            "paper_only": PAPER_ONLY,
            "live_money_authority": LIVE_MONEY_AUTHORITY,
            "signing_available": SIGNING_AVAILABLE,
            "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        }
    )
    return payload


def install_fomo_canonical_worker_binding() -> None:
    """Bind independent FOMO after canonical isolation captures its predecessor.

    Continuation recalibration can be installed before the final production worker
    graph is isolated. Its original installer therefore cannot assume
    ``canonical._ORIGINAL_RUNTIME_WORKERS`` is populated. Production telemetry proved
    that exact ordering: FOMO liveness was installed, but its worker had zero starts.

    This final boundary first makes canonical isolation idempotently concrete, then
    inserts the already-existing continuation/FOMO wrapper *inside* that isolated
    graph. It does not create another scanner or alter FOMO/Solana thresholds,
    providers, certification, risk, sizing, signing, submission, or live authority.
    """
    global _INSTALLED

    if _INSTALLED:
        return
    if not bool(getattr(continuation, "_INSTALLED", False)):
        raise RuntimeError("FOMO canonical binding requires continuation recalibration")

    canonical.install_canonical_worker_isolation()
    current = canonical._ORIGINAL_RUNTIME_WORKERS
    if current is None:
        raise RuntimeError("canonical worker predecessor unavailable for FOMO binding")

    if current is continuation._canonical_workers_with_independent_fomo:
        if continuation._ORIGINAL_CANONICAL_WORKERS is None:
            raise RuntimeError("FOMO wrapper is bound without a canonical predecessor")
        predecessor = continuation._ORIGINAL_CANONICAL_WORKERS
        _STATE.update(
            {
                "installed": True,
                "bound": True,
                "state": "already_bound",
                "predecessor_name": getattr(predecessor, "__name__", type(predecessor).__name__),
            }
        )
        _INSTALLED = True
        return

    continuation._ORIGINAL_CANONICAL_WORKERS = current
    setattr(
        continuation._canonical_workers_with_independent_fomo,
        "_roi_fomo_canonical_worker_binding",
        True,
    )
    canonical._ORIGINAL_RUNTIME_WORKERS = continuation._canonical_workers_with_independent_fomo

    _STATE.update(
        {
            "installed": True,
            "bound": True,
            "state": "bound",
            "predecessor_name": getattr(current, "__name__", type(current).__name__),
        }
    )
    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "binding_status",
    "install_fomo_canonical_worker_binding",
]
