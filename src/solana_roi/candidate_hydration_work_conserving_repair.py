from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from . import runtime_guards as guards
from . import target_stream_fanout as fanout
from .direct_solana import DirectSolanaIngestionPlane


# Keep three of the nine non-candidate workers exclusively available for launch
# and other certification background work. The remaining background workers may
# lend capacity to priority<=2 scout/gap work only while such work is pending.
# Total hydration workers and the established three candidate-reserved workers are
# unchanged.
BACKGROUND_RESERVED_WORKERS = 3


def _increment(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_candidate_hydration_flex_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


def _worker_counts(self: Any) -> tuple[int, int, int]:
    total = max(2, int(getattr(self, "worker_count", 12) or 12))
    fast = min(guards.DIRECT_FAST_WORKER_SLOTS, max(1, total - 1))
    background = max(1, total - fast)
    reserved_background = min(BACKGROUND_RESERVED_WORKERS, background)
    flex = max(0, background - reserved_background)
    return fast, reserved_background, flex


def _background_worker_index() -> int | None:
    try:
        task = asyncio.current_task()
    except RuntimeError:
        return None
    if task is None:
        return None
    name = task.get_name()
    prefix = "direct-solana-background:"
    if not name.startswith(prefix):
        return None
    try:
        return int(name[len(prefix) :])
    except (TypeError, ValueError):
        return None


def _background_worker_can_flex(self: Any) -> bool:
    index = _background_worker_index()
    if index is None:
        return False
    _fast, reserved_background, _flex = _worker_counts(self)
    return index >= reserved_background


def _claim_work(self: Any, *, fast_only: bool) -> tuple[dict[str, Any] | None, str]:
    if fast_only:
        row = guards._claim_priority(self.journal, fast_only=True)
        return row, "candidate_reserved" if row is not None else "none"

    if _background_worker_can_flex(self):
        row = guards._claim_priority(self.journal, fast_only=True)
        if row is not None:
            return row, "candidate_flex"

    row = guards._claim_priority(self.journal, fast_only=False)
    return row, "background" if row is not None else "none"


async def _work_conserving_reserved_worker(
    self: Any,
    stop: asyncio.Event,
    *,
    fast_only: bool,
) -> None:
    """Let only non-reserved background workers assist an expiring scout queue.

    Claims remain atomic through the existing SQLite queue reservation. Three
    candidate workers are still permanently reserved. Three background workers are
    still permanently reserved for launches/background certification. The six
    remaining workers become work-conserving: they check priority<=2 first and fall
    back immediately to their original priority>2 work when no urgent row exists.
    """

    next_cleanup = 0.0
    while not stop.is_set():
        if not fast_only and time.monotonic() >= next_cleanup:
            guards._expire_stale_background(self)
            next_cleanup = time.monotonic() + 5.0

        row, lane = _claim_work(self, fast_only=fast_only)
        if row is None:
            await asyncio.sleep(0.01 if fast_only else 0.025)
            continue

        if lane == "candidate_reserved":
            _increment(self, "reserved_candidate_claims")
        elif lane == "candidate_flex":
            _increment(self, "flex_candidate_claims")
        elif lane == "background":
            _increment(self, "background_claims")

        await self._hydrate_one(row)


try:
    _work_conserving_reserved_worker.__dict__.update(
        getattr(guards._reserved_worker, "__dict__", {})
    )
except Exception:
    pass
setattr(_work_conserving_reserved_worker, "_roi_candidate_work_conserving", True)


def _status_with_candidate_flex(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        fast, reserved_background, flex = _worker_counts(self)
        throughput = payload.setdefault("throughput_policy", {})
        if isinstance(throughput, dict):
            throughput.update(
                {
                    "candidate_reserved_workers": fast,
                    "background_reserved_workers": reserved_background,
                    "candidate_flex_workers": flex,
                    "candidate_flex_work_conserving": True,
                    "candidate_flex_claim_priority": "priority<=2-before-priority>2",
                    "candidate_flex_never_reduces_reserved_background_workers": True,
                    "total_workers_unchanged": int(self.worker_count),
                    "candidate_reserved_claims_session": int(
                        getattr(self, "_roi_candidate_hydration_flex_reserved_candidate_claims", 0) or 0
                    ),
                    "candidate_flex_claims_session": int(
                        getattr(self, "_roi_candidate_hydration_flex_flex_candidate_claims", 0) or 0
                    ),
                    "background_claims_session": int(
                        getattr(self, "_roi_candidate_hydration_flex_background_claims", 0) or 0
                    ),
                }
            )
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "candidate_hydration_pool_work_conserving": True,
                    "candidate_reserved_workers_unchanged": fast,
                    "background_certification_capacity_reserved": reserved_background,
                    "hydration_worker_count_unchanged": int(self.worker_count),
                    "candidate_entry_window_unchanged": True,
                    "certification_thresholds_unchanged": True,
                    "strategy_thresholds_unchanged": True,
                    "paper_only_authority_unchanged": True,
                    "signing_or_submission_available": False,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_candidate_work_conserving", True)
    return status


def install_candidate_hydration_work_conserving_repair() -> None:
    """Install final scheduler composition without changing worker count."""

    # target_stream_fanout imported the worker symbol by value, so patch both the
    # canonical guard module and the active fanout module before runtime tasks are
    # created.
    guards._reserved_worker = _work_conserving_reserved_worker  # type: ignore[assignment]
    fanout._reserved_worker = _work_conserving_reserved_worker  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_candidate_work_conserving", False)):
        DirectSolanaIngestionPlane.status = _status_with_candidate_flex(current_status)  # type: ignore[method-assign]


__all__ = [
    "BACKGROUND_RESERVED_WORKERS",
    "_background_worker_can_flex",
    "_claim_work",
    "_work_conserving_reserved_worker",
    "_worker_counts",
    "install_candidate_hydration_work_conserving_repair",
]
