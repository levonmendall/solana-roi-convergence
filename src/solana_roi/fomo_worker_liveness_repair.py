from __future__ import annotations

import asyncio
import json
from typing import Any, Callable

from . import candidate_fomo_runtime_repair as candidate
from . import continuation_market_recalibration as continuation


REPAIR_VERSION = "fomo-worker-liveness-v1"
RESTART_BACKOFF_SECONDS = 1.0
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_ORIGINAL_FOMO_WORKER: Callable[..., Any] | None = None
_ORIGINAL_READ_RUNTIME: Callable[[Any], dict[str, Any]] | None = None
_INSTALLED = False
_STATE: dict[str, Any] = {
    "installed": False,
    "state": "not_started",
    "starts": 0,
    "restarts": 0,
    "unexpected_exits": 0,
    "last_error": None,
}


def _parse_runtime_value(value: str) -> Any:
    if value in {"true", "false"}:
        return value == "true"
    try:
        return json.loads(value)
    except Exception:
        try:
            return int(value)
        except (TypeError, ValueError):
            try:
                return float(value)
            except (TypeError, ValueError):
                return value


async def _supervised_independent_fomo_worker(runtime: Any, stop: asyncio.Event) -> None:
    """Restart the existing independent-FOMO worker only if it exits unexpectedly.

    The original worker remains the sole scanner/open/settlement authority and already
    catches ordinary per-cycle failures. This wrapper exists for initialization-time
    or otherwise terminal failures that occur outside that inner cycle handler.
    """
    if _ORIGINAL_FOMO_WORKER is None:
        raise RuntimeError("FOMO worker liveness repair is not installed")

    first = True
    while first or not stop.is_set():
        first = False
        _STATE["starts"] = int(_STATE.get("starts", 0) or 0) + 1
        _STATE["state"] = "running"
        _STATE["last_error"] = None
        try:
            await _ORIGINAL_FOMO_WORKER(runtime, stop)
            if stop.is_set():
                _STATE["state"] = "stopped"
                return
            raise RuntimeError("independent FOMO worker returned unexpectedly")
        except asyncio.CancelledError:
            _STATE["state"] = "cancelled"
            raise
        except Exception as exc:
            _STATE["unexpected_exits"] = int(_STATE.get("unexpected_exits", 0) or 0) + 1
            _STATE["state"] = "restart_pending"
            _STATE["last_error"] = f"{type(exc).__name__}: {exc}"
            try:
                await asyncio.wait_for(stop.wait(), timeout=RESTART_BACKOFF_SECONDS)
            except asyncio.TimeoutError:
                _STATE["restarts"] = int(_STATE.get("restarts", 0) or 0) + 1
                continue
            _STATE["state"] = "stopped"
            return


setattr(_supervised_independent_fomo_worker, "_roi_fomo_worker_liveness", True)


def _read_runtime_with_liveness(adapter: Any) -> dict[str, Any]:
    """Expose the existing scanner-cycle heartbeat alongside PR168 diagnostics."""
    if _ORIGINAL_READ_RUNTIME is None:
        raise RuntimeError("FOMO runtime reader liveness repair is not installed")

    output = dict(_ORIGINAL_READ_RUNTIME(adapter))
    continuation._continuation_schema(adapter)
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT key,value,updated_at FROM independent_fomo_runtime "
            "WHERE key IN ('last_scan_at','last_error','candidate_count','opened_count')"
        ).fetchall()

    latest_updated_at = str(output.get("updated_at") or "")
    for raw in rows:
        row = dict(raw)
        key = str(row.get("key") or "")
        value = str(row.get("value") or "")
        mapped = {
            "last_scan_at": "scanner_cycle_last_scan_at",
            "last_error": "scanner_cycle_last_error",
            "candidate_count": "scanner_cycle_candidate_count",
            "opened_count": "scanner_cycle_opened_count",
        }.get(key)
        if mapped:
            output[mapped] = _parse_runtime_value(value)
        updated_at = str(row.get("updated_at") or "")
        if updated_at > latest_updated_at:
            latest_updated_at = updated_at
    if latest_updated_at:
        output["updated_at"] = latest_updated_at

    output["worker_liveness_version"] = REPAIR_VERSION
    output["worker_state"] = str(_STATE.get("state") or "unknown")
    output["worker_starts"] = int(_STATE.get("starts", 0) or 0)
    output["worker_restarts"] = int(_STATE.get("restarts", 0) or 0)
    output["worker_unexpected_exits"] = int(_STATE.get("unexpected_exits", 0) or 0)
    output["worker_last_terminal_error"] = _STATE.get("last_error")
    output["worker_restart_backoff_seconds"] = RESTART_BACKOFF_SECONDS
    output["strategy_thresholds_changed"] = False
    output["paper_only"] = PAPER_ONLY
    output["live_money_authority"] = LIVE_MONEY_AUTHORITY
    output["signing_available"] = SIGNING_AVAILABLE
    output["transaction_submission_available"] = TRANSACTION_SUBMISSION_AVAILABLE
    return output


setattr(_read_runtime_with_liveness, "_roi_fomo_worker_liveness", True)


def install_fomo_worker_liveness_repair() -> None:
    """Make independent-FOMO activity and terminal failures observable and restartable."""
    global _ORIGINAL_FOMO_WORKER, _ORIGINAL_READ_RUNTIME, _INSTALLED
    if _INSTALLED:
        return
    if not bool(getattr(candidate, "_INSTALLED", False)):
        raise RuntimeError("FOMO worker liveness requires candidate/FOMO runtime repair")
    if not bool(getattr(continuation, "_INSTALLED", False)):
        raise RuntimeError("FOMO worker liveness requires continuation recalibration")

    current_worker = continuation._independent_fomo_worker
    if not bool(getattr(current_worker, "_roi_fomo_worker_liveness", False)):
        _ORIGINAL_FOMO_WORKER = current_worker
        continuation._independent_fomo_worker = _supervised_independent_fomo_worker  # type: ignore[assignment]

    current_reader = candidate._read_fomo_runtime
    if not bool(getattr(current_reader, "_roi_fomo_worker_liveness", False)):
        _ORIGINAL_READ_RUNTIME = current_reader
        candidate._read_fomo_runtime = _read_runtime_with_liveness  # type: ignore[assignment]

    _STATE.update(
        {
            "installed": True,
            "state": "installed_waiting_for_runtime",
            "starts": 0,
            "restarts": 0,
            "unexpected_exits": 0,
            "last_error": None,
        }
    )
    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "RESTART_BACKOFF_SECONDS",
    "_read_runtime_with_liveness",
    "_supervised_independent_fomo_worker",
    "install_fomo_worker_liveness_repair",
]
