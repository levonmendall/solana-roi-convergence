from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from .robinhood_chain_ingest import RobinhoodIngestMixin
from .robinhood_chain_runtime import RobinhoodRuntimeMixin


REPAIR_VERSION = "robinhood-catchup-event-loop-fairness-v1"


def _ensure_fairness_metrics(self: Any) -> None:
    if not hasattr(self, "_roi_catchup_cooperative_yields_total"):
        setattr(self, "_roi_catchup_cooperative_yields_total", 0)
    if not hasattr(self, "_roi_catchup_last_yield_at_monotonic"):
        setattr(self, "_roi_catchup_last_yield_at_monotonic", time.monotonic())
    if not hasattr(self, "_roi_catchup_max_checkpoint_interval_seconds"):
        setattr(self, "_roi_catchup_max_checkpoint_interval_seconds", 0.0)
    if not hasattr(self, "_roi_catchup_last_checkpoint_phase"):
        setattr(self, "_roi_catchup_last_checkpoint_phase", None)


async def _cooperative_catchup_checkpoint(self: Any, *, phase: str) -> None:
    """Yield after each successfully processed historical log during catch-up.

    Robinhood shares one asyncio event loop with the FastAPI server on Render.
    The accelerated catch-up path can process hundreds of historical swap rows whose
    handlers contain synchronous SQLite transactions. Awaiting those coroutines does
    not yield when ``live=False`` because they return immediately after the SQLite
    write. Explicitly scheduling other ready tasks after every processed row keeps
    health/API requests serviceable without reducing block coverage or advancing the
    cursor before durable processing finishes.
    """

    if not bool(getattr(self, "_roi_catchup_mode", False)):
        return
    if bool(getattr(self, "_caught_up", False)):
        return

    _ensure_fairness_metrics(self)
    now = time.monotonic()
    previous = float(getattr(self, "_roi_catchup_last_yield_at_monotonic", now))
    interval = max(0.0, now - previous)
    current_max = float(getattr(self, "_roi_catchup_max_checkpoint_interval_seconds", 0.0))
    setattr(self, "_roi_catchup_max_checkpoint_interval_seconds", max(current_max, interval))
    setattr(
        self,
        "_roi_catchup_cooperative_yields_total",
        int(getattr(self, "_roi_catchup_cooperative_yields_total", 0)) + 1,
    )
    setattr(self, "_roi_catchup_last_checkpoint_phase", phase)

    # sleep(0) is intentionally used instead of adding a fixed throttle. It gives
    # Uvicorn/health checks and the other canonical workers a scheduling point while
    # retaining the catch-up throughput gained by the 800-block acquisition path.
    await asyncio.sleep(0)
    setattr(self, "_roi_catchup_last_yield_at_monotonic", time.monotonic())


def _wrap_ingest_method(
    original: Callable[..., Any],
    *,
    phase: str,
) -> Callable[..., Any]:
    async def wrapped(self: Any, *args: Any, **kwargs: Any) -> Any:
        result = await original(self, *args, **kwargs)
        await _cooperative_catchup_checkpoint(self, phase=phase)
        return result

    try:
        wrapped.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(wrapped, "_roi_event_loop_fairness", True)
    setattr(wrapped, "_roi_event_loop_fairness_phase", phase)
    return wrapped


def _status_with_event_loop_fairness(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        _ensure_fairness_metrics(self)
        payload["event_loop_fairness"] = {
            "repair_version": REPAIR_VERSION,
            "cooperative_yield_after_each_processed_catchup_log": True,
            "catchup_cooperative_yields_total": int(
                getattr(self, "_roi_catchup_cooperative_yields_total", 0)
            ),
            "max_interval_between_cooperative_checkpoints_seconds": float(
                getattr(self, "_roi_catchup_max_checkpoint_interval_seconds", 0.0)
            ),
            "last_checkpoint_phase": getattr(self, "_roi_catchup_last_checkpoint_phase", None),
            "catchup_batch_limit_changed": False,
            "catchup_poll_cadence_changed": False,
            "block_ranges_skipped": False,
            "cursor_advance_semantics_changed": False,
            "paper_decision_gate_changed": False,
            "strategy_thresholds_changed": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_event_loop_fairness", True)
    return status


def install_robinhood_event_loop_fairness_repair() -> None:
    for name, phase in (
        ("_process_factory_log", "factory_log"),
        ("_process_v3_swap", "v3_swap"),
        ("_process_v2_curve_log", "v2_curve_log"),
    ):
        current = getattr(RobinhoodIngestMixin, name)
        if not bool(getattr(current, "_roi_event_loop_fairness", False)):
            setattr(RobinhoodIngestMixin, name, _wrap_ingest_method(current, phase=phase))

    current_status = RobinhoodRuntimeMixin.status
    if not bool(getattr(current_status, "_roi_event_loop_fairness", False)):
        RobinhoodRuntimeMixin.status = _status_with_event_loop_fairness(current_status)  # type: ignore[method-assign]


__all__ = [
    "REPAIR_VERSION",
    "_cooperative_catchup_checkpoint",
    "_status_with_event_loop_fairness",
    "_wrap_ingest_method",
    "install_robinhood_event_loop_fairness_repair",
]
