from __future__ import annotations

from datetime import timedelta
from typing import Any, Callable


RECOVERY_VERSION = "v51-exit-due-recovery-v1"
STALE_EXIT_DUE_SECONDS = 5.0
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_INSTALLED = False
_ORIGINAL_RETRY_DUE: Callable[..., Any] | None = None
_RECOVERY_TICKS = 0
_RECOVERED_EXIT_DUE = 0
_RETRIED_FAILED = 0


async def _retry_due_with_exit_due(adapter: Any) -> None:
    """Recover durable first-attempt exits as well as scheduled failed retries.

    The exact-exit observer normally attempts a newly persisted ``exit_due`` row
    immediately. A process loss between those two operations previously stranded
    the durable row forever because the background retry clock selected only
    ``paper_exit_execution_failed`` rows. This owner adds restart recovery while
    preserving the existing exact liquidation implementation.

    A short stale-age guard avoids racing the normal immediate attempt. All rows
    still execute through ``exact._attempt_liquidation``; after the paper lifecycle
    runtime is installed that symbol is the canonical paper-inventory wrapper, so
    recovery cannot bypass paper accounting, executable-exit evidence, or settlement.
    """

    global _RECOVERY_TICKS, _RECOVERED_EXIT_DUE, _RETRIED_FAILED
    from . import v51_exact_exit_execution as exact

    exact._ensure_schema(adapter)
    now = exact._utcnow()
    stale_before = now - timedelta(seconds=STALE_EXIT_DUE_SECONDS)
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT * FROM profit_first_final_exit_liquidations WHERE epoch_id=? AND ("
            "(status='exit_due' AND first_exit_due_at<=?) OR "
            "(status='paper_exit_execution_failed' AND next_retry_at IS NOT NULL AND next_retry_at<=?)"
            ") ORDER BY CASE WHEN status='exit_due' THEN first_exit_due_at ELSE next_retry_at END LIMIT 32",
            (adapter.epoch_id, stale_before.isoformat(), now.isoformat()),
        ).fetchall()

    recovered_exit_due = 0
    retried_failed = 0
    for row in rows:
        payload = dict(row)
        if str(payload.get("status") or "") == "exit_due":
            recovered_exit_due += 1
        else:
            retried_failed += 1
        await exact._attempt_liquidation(adapter, payload)

    _RECOVERY_TICKS += 1
    _RECOVERED_EXIT_DUE += recovered_exit_due
    _RETRIED_FAILED += retried_failed


def install_exit_due_recovery() -> None:
    """Install restart-safe exit ownership after the canonical lifecycle wrapper."""

    global _INSTALLED, _ORIGINAL_RETRY_DUE
    if _INSTALLED:
        return
    from . import v51_exact_exit_execution as exact

    if not bool(getattr(exact, "_INSTALLED", False)):
        raise RuntimeError("canonical_exact_exit_engine_must_be_installed_first")

    _ORIGINAL_RETRY_DUE = exact._retry_due
    exact._retry_due = _retry_due_with_exit_due  # type: ignore[assignment]
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": RECOVERY_VERSION,
        "installed": _INSTALLED,
        "stale_exit_due_seconds": STALE_EXIT_DUE_SECONDS,
        "recovery_ticks": _RECOVERY_TICKS,
        "recovered_exit_due_count": _RECOVERED_EXIT_DUE,
        "retried_failed_count": _RETRIED_FAILED,
        "owns_initial_exit_due_after_restart": True,
        "failed_retry_semantics_preserved": True,
        "uses_canonical_attempt_liquidation": True,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "LIVE_MONEY_AUTHORITY",
    "PAPER_ONLY",
    "RECOVERY_VERSION",
    "SIGNING_AVAILABLE",
    "STALE_EXIT_DUE_SECONDS",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "install_exit_due_recovery",
    "status",
]
