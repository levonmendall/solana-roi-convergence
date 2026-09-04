from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

from .observation import TimedRiskCollectors


CANDIDATE_PROCESSING_TARGET_SECONDS = 5.0
CANDIDATE_ENTRY_WINDOW_SECONDS = 20.0
CANDIDATE_RECORDING_RESERVE_SECONDS = 0.10
CANDIDATE_RETRY_YIELD_SECONDS = 0.20

_ORIGINAL_REFRESH: Callable[..., Any] | None = None


def _inc(obj: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_candidate_risk_window_{name}"
    setattr(obj, attr, int(getattr(obj, attr, 0) or 0) + int(amount))


def _eligible(self: TimedRiskCollectors, current_swap: Any) -> bool:
    return bool(
        current_swap is not None
        and callable(getattr(self.inner, "refresh_coverage", None))
        and callable(getattr(self.inner, "refresh_candidate", None))
        and self._eligible_candidate(current_swap)
    )


async def _refresh_until_entry_ceiling(
    self: TimedRiskCollectors,
    mint: str,
    at: datetime,
    *,
    current_swap: Any = None,
) -> None:
    """Keep the 5s latency target but allow evidence collection through the 20s entry ceiling.

    The previous candidate hot path used the five-second latency-certification target
    as an asyncio cancellation deadline. That made a candidate arriving after five
    seconds incapable of ever completing launch/funding/dynamic risk even though the
    strategy explicitly permits a first executable entry through twenty seconds.

    This wrapper does not relax the five-second certification target. It records the
    real end-to-end measurement exactly as before; samples completing after five
    seconds still fail latency certification. It only prevents that diagnostic target
    from cancelling point-in-time research that is still inside the unchanged twenty-
    second strategy window.
    """
    if _ORIGINAL_REFRESH is None:
        raise RuntimeError("candidate risk-window repair is not installed")
    if not _eligible(self, current_swap):
        await _ORIGINAL_REFRESH(self, mint, at, current_swap=current_swap)
        return

    trigger_observed_at = getattr(current_swap, "observed_at", at)
    trigger_received_at = getattr(current_swap, "received_at", at)
    started_at = self.now_fn()
    started_perf = self.perf_fn()
    ingestion_latency_ms = float(getattr(current_swap, "ingestion_latency_ms", 0.0) or 0.0)
    entry_deadline = trigger_observed_at + timedelta(
        seconds=CANDIDATE_ENTRY_WINDOW_SECONDS - CANDIDATE_RECORDING_RESERVE_SECONDS
    )
    target_deadline = trigger_observed_at + timedelta(seconds=CANDIDATE_PROCESSING_TARGET_SECONDS)
    target_exceeded = started_at > target_deadline
    if target_exceeded:
        _inc(self, "processing_target_exceeded")

    coverage_refresh = getattr(self.inner, "refresh_coverage")
    candidate_refresh = getattr(self.inner, "refresh_candidate")
    hard_timeout = False
    unexpected_error: str | None = None
    rounds = 0
    readiness: dict[str, Any] = {"complete": False, "fresh": False}

    while True:
        attempt_at = self.now_fn()
        remaining = (entry_deadline - attempt_at).total_seconds()
        if remaining <= 0.0:
            hard_timeout = True
            break
        rounds += 1
        try:
            # Use the actual prospective decision time, not the original swap receipt
            # time. This is required for an eight-second launch window to become
            # observable without lookahead and keeps 5-second liquidity/flow evidence
            # fresh at the time a delayed (but <=20s) paper entry would be evaluated.
            await asyncio.wait_for(
                asyncio.gather(
                    coverage_refresh(mint, attempt_at, current_swap=current_swap),
                    candidate_refresh(mint, attempt_at, current_swap=current_swap),
                ),
                timeout=remaining,
            )
        except asyncio.TimeoutError:
            hard_timeout = True
            break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            unexpected_error = f"{type(exc).__name__}:{str(exc)[:300]}"
            break

        completed_round_at = self.now_fn()
        raw = self.risk.readiness(mint, as_of=completed_round_at)
        readiness = dict(raw) if isinstance(raw, dict) else {"complete": False, "fresh": False}
        if bool(readiness.get("complete")) and bool(readiness.get("fresh")):
            break
        remaining = (entry_deadline - completed_round_at).total_seconds()
        if remaining <= 0.0:
            hard_timeout = True
            break
        await asyncio.sleep(min(CANDIDATE_RETRY_YIELD_SECONDS, max(0.0, remaining)))

    completed_at = self.now_fn()
    if not readiness.get("complete") or not readiness.get("fresh"):
        raw = self.risk.readiness(mint, as_of=completed_at)
        readiness = dict(raw) if isinstance(raw, dict) else {"complete": False, "fresh": False}

    end_to_end_ms = max(0.0, (completed_at - trigger_observed_at).total_seconds() * 1000.0)
    target_exceeded = target_exceeded or end_to_end_ms > CANDIDATE_PROCESSING_TARGET_SECONDS * 1000.0
    if target_exceeded:
        readiness["candidate_processing_target_exceeded"] = True
        readiness["candidate_processing_target_seconds"] = CANDIDATE_PROCESSING_TARGET_SECONDS
    readiness["candidate_processing_target_is_not_entry_authority"] = True
    readiness["candidate_entry_window_seconds"] = CANDIDATE_ENTRY_WINDOW_SECONDS
    readiness["candidate_risk_retry_rounds"] = rounds

    if hard_timeout:
        readiness["candidate_entry_window_exhausted"] = True
        _inc(self, "entry_window_exhausted")
    if unexpected_error is not None:
        readiness["candidate_risk_window_error"] = unexpected_error
        _inc(self, "unexpected_errors")
    if rounds > 1:
        _inc(self, "retry_rounds", rounds - 1)

    complete = bool(readiness.get("complete")) and not hard_timeout and unexpected_error is None
    fresh = bool(readiness.get("fresh")) and not hard_timeout and unexpected_error is None
    if complete and fresh and target_exceeded:
        _inc(self, "late_but_complete")

    self.store.record_risk_refresh(
        token_mint=mint,
        trigger_observed_at=trigger_observed_at.isoformat(),
        trigger_received_at=trigger_received_at.isoformat(),
        started_at=started_at.isoformat(),
        completed_at=completed_at.isoformat(),
        elapsed_ms=max(0.0, (self.perf_fn() - started_perf) * 1000.0),
        ingestion_latency_ms=ingestion_latency_ms,
        end_to_end_ms=end_to_end_ms,
        complete=complete,
        fresh=fresh,
        readiness=readiness,
    )
    _inc(self, "measurements_recorded")


setattr(_refresh_until_entry_ceiling, "_roi_candidate_risk_window", True)


def install_candidate_risk_window_repair() -> None:
    global _ORIGINAL_REFRESH
    current = TimedRiskCollectors.refresh
    if bool(getattr(current, "_roi_candidate_risk_window", False)):
        return
    _ORIGINAL_REFRESH = current
    try:
        _refresh_until_entry_ceiling.__dict__.update(getattr(current, "__dict__", {}))
    except Exception:
        pass
    setattr(_refresh_until_entry_ceiling, "_roi_candidate_risk_window", True)
    TimedRiskCollectors.refresh = _refresh_until_entry_ceiling  # type: ignore[method-assign]


__all__ = [
    "CANDIDATE_ENTRY_WINDOW_SECONDS",
    "CANDIDATE_PROCESSING_TARGET_SECONDS",
    "CANDIDATE_RECORDING_RESERVE_SECONDS",
    "CANDIDATE_RETRY_YIELD_SECONDS",
    "_refresh_until_entry_ceiling",
    "install_candidate_risk_window_repair",
]
