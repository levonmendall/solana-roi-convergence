from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Callable
from typing import Any

from .robinhood_chain_ingest import RobinhoodIngestMixin
from .robinhood_chain_runtime import RobinhoodRuntimeMixin


REPAIR_VERSION = "robinhood-catchup-cpu-headroom-v2"
DEFAULT_CATCHUP_CPU_DUTY_CYCLE = 0.20
MIN_CATCHUP_CPU_DUTY_CYCLE = 0.10
MAX_CATCHUP_CPU_DUTY_CYCLE = 0.50
DEFAULT_CATCHUP_CPU_BURST_SECONDS = 0.020
MIN_CATCHUP_CPU_BURST_SECONDS = 0.005
MAX_CATCHUP_CPU_BURST_SECONDS = 0.100


def _catchup_cpu_duty_cycle() -> float:
    try:
        value = float(
            os.getenv(
                "ROBINHOOD_CATCHUP_CPU_DUTY_CYCLE",
                str(DEFAULT_CATCHUP_CPU_DUTY_CYCLE),
            )
        )
    except (TypeError, ValueError):
        value = DEFAULT_CATCHUP_CPU_DUTY_CYCLE
    return max(MIN_CATCHUP_CPU_DUTY_CYCLE, min(MAX_CATCHUP_CPU_DUTY_CYCLE, value))


def _catchup_cpu_burst_seconds() -> float:
    try:
        value = float(
            os.getenv(
                "ROBINHOOD_CATCHUP_CPU_BURST_SECONDS",
                str(DEFAULT_CATCHUP_CPU_BURST_SECONDS),
            )
        )
    except (TypeError, ValueError):
        value = DEFAULT_CATCHUP_CPU_BURST_SECONDS
    return max(MIN_CATCHUP_CPU_BURST_SECONDS, min(MAX_CATCHUP_CPU_BURST_SECONDS, value))


def _ensure_fairness_metrics(self: Any) -> None:
    if not hasattr(self, "_roi_catchup_cooperative_yields_total"):
        setattr(self, "_roi_catchup_cooperative_yields_total", 0)
    if not hasattr(self, "_roi_catchup_last_yield_at_monotonic"):
        setattr(self, "_roi_catchup_last_yield_at_monotonic", time.monotonic())
    if not hasattr(self, "_roi_catchup_max_checkpoint_interval_seconds"):
        setattr(self, "_roi_catchup_max_checkpoint_interval_seconds", 0.0)
    if not hasattr(self, "_roi_catchup_last_checkpoint_phase"):
        setattr(self, "_roi_catchup_last_checkpoint_phase", None)
    if not hasattr(self, "_roi_catchup_last_thread_cpu_seconds"):
        setattr(self, "_roi_catchup_last_thread_cpu_seconds", time.thread_time())
    if not hasattr(self, "_roi_catchup_cpu_since_governor_seconds"):
        setattr(self, "_roi_catchup_cpu_since_governor_seconds", 0.0)
    if not hasattr(self, "_roi_catchup_cpu_governor_sleeps_total"):
        setattr(self, "_roi_catchup_cpu_governor_sleeps_total", 0)
    if not hasattr(self, "_roi_catchup_cpu_governor_sleep_seconds_total"):
        setattr(self, "_roi_catchup_cpu_governor_sleep_seconds_total", 0.0)
    if not hasattr(self, "_roi_catchup_cpu_governor_last_sleep_seconds"):
        setattr(self, "_roi_catchup_cpu_governor_last_sleep_seconds", 0.0)


async def _cooperative_catchup_checkpoint(self: Any, *, phase: str) -> None:
    """Reserve CPU headroom after durable historical Robinhood work.

    PR #132 moved Robinhood onto a private OS thread, asyncio loop, and SQLite file,
    which removed direct Uvicorn-loop and canonical-store contention. Production then
    proved the remaining shared resource was the service's single CPU: Render metrics
    reached the exact 1.0 CPU limit during the same windows in which the five-second
    health probe disappeared.

    This checkpoint therefore budgets *thread CPU time*, not wall-clock or block
    count. Network waits and SQLite waits do not consume the budget. Dense historical
    Python/decoding/strategy work is allowed a short CPU burst, then sleeps long
    enough to maintain a bounded catch-up CPU duty cycle. This preserves every block,
    row, durable cursor boundary, 800-block acquisition range and <=2-block paper
    decision gate while reserving CPU for Uvicorn and strategy-critical Solana/FOMO
    work. Once Robinhood is live, the governor is completely inert.
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

    current_cpu = time.thread_time()
    previous_cpu = float(getattr(self, "_roi_catchup_last_thread_cpu_seconds", current_cpu))
    cpu_delta = max(0.0, current_cpu - previous_cpu)
    cpu_since_governor = float(
        getattr(self, "_roi_catchup_cpu_since_governor_seconds", 0.0)
    ) + cpu_delta
    setattr(self, "_roi_catchup_last_thread_cpu_seconds", current_cpu)
    setattr(self, "_roi_catchup_cpu_since_governor_seconds", cpu_since_governor)

    sleep_seconds = 0.0
    if cpu_since_governor >= _catchup_cpu_burst_seconds():
        duty = _catchup_cpu_duty_cycle()
        sleep_seconds = cpu_since_governor * ((1.0 - duty) / duty)
        setattr(
            self,
            "_roi_catchup_cpu_governor_sleeps_total",
            int(getattr(self, "_roi_catchup_cpu_governor_sleeps_total", 0)) + 1,
        )
        setattr(
            self,
            "_roi_catchup_cpu_governor_sleep_seconds_total",
            float(getattr(self, "_roi_catchup_cpu_governor_sleep_seconds_total", 0.0))
            + sleep_seconds,
        )
        setattr(self, "_roi_catchup_cpu_governor_last_sleep_seconds", sleep_seconds)
        await asyncio.sleep(sleep_seconds)
        setattr(self, "_roi_catchup_cpu_since_governor_seconds", 0.0)
        setattr(self, "_roi_catchup_last_thread_cpu_seconds", time.thread_time())
    else:
        # Retain the original zero-delay scheduler handoff between CPU budget sleeps.
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
            "thread_cpu_governor_enabled_during_catchup": True,
            "catchup_cpu_duty_cycle": _catchup_cpu_duty_cycle(),
            "catchup_cpu_burst_seconds": _catchup_cpu_burst_seconds(),
            "catchup_cpu_governor_sleeps_total": int(
                getattr(self, "_roi_catchup_cpu_governor_sleeps_total", 0)
            ),
            "catchup_cpu_governor_sleep_seconds_total": float(
                getattr(self, "_roi_catchup_cpu_governor_sleep_seconds_total", 0.0)
            ),
            "catchup_cpu_governor_last_sleep_seconds": float(
                getattr(self, "_roi_catchup_cpu_governor_last_sleep_seconds", 0.0)
            ),
            "catchup_cooperative_yields_total": int(
                getattr(self, "_roi_catchup_cooperative_yields_total", 0)
            ),
            "max_interval_between_cooperative_checkpoints_seconds": float(
                getattr(self, "_roi_catchup_max_checkpoint_interval_seconds", 0.0)
            ),
            "last_checkpoint_phase": getattr(self, "_roi_catchup_last_checkpoint_phase", None),
            "catchup_batch_limit_changed": False,
            "catchup_poll_cadence_changed": False,
            "historical_work_skipped": False,
            "block_ranges_skipped": False,
            "cursor_advance_semantics_changed": False,
            "paper_decision_gate_changed": False,
            "strategy_thresholds_changed": False,
            "governor_inert_once_caught_up": True,
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
    "DEFAULT_CATCHUP_CPU_BURST_SECONDS",
    "DEFAULT_CATCHUP_CPU_DUTY_CYCLE",
    "REPAIR_VERSION",
    "_catchup_cpu_burst_seconds",
    "_catchup_cpu_duty_cycle",
    "_cooperative_catchup_checkpoint",
    "_status_with_event_loop_fairness",
    "_wrap_ingest_method",
    "install_robinhood_event_loop_fairness_repair",
]
