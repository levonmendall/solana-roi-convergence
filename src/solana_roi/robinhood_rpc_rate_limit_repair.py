from __future__ import annotations

import asyncio
import time
from email.utils import parsedate_to_datetime
from typing import Any, Callable

import httpx

from . import robinhood_chain_core as core

REPAIR_VERSION = "robinhood-public-rpc-rate-limit-v1"
MAX_ATTEMPTS = 4
DEFAULT_429_DELAY_SECONDS = 1.0
MAX_429_DELAY_SECONDS = 30.0
RECOVERY_MODE_SECONDS = 60.0
RECOVERY_MIN_REQUEST_INTERVAL_SECONDS = 0.25
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_ORIGINAL_RPC: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[[Any], dict[str, Any]] | None = None


def _ensure_state(self: Any) -> None:
    if not hasattr(self, "_roi_rate_limit_cooldown_until"):
        self._roi_rate_limit_cooldown_until = 0.0
    if not hasattr(self, "_roi_rate_limit_recovery_until"):
        self._roi_rate_limit_recovery_until = 0.0
    if not hasattr(self, "_roi_rate_limit_last_request_at"):
        self._roi_rate_limit_last_request_at = 0.0
    if not hasattr(self, "_roi_rate_limit_events"):
        self._roi_rate_limit_events = 0
    if not hasattr(self, "_roi_rate_limit_retry_attempts"):
        self._roi_rate_limit_retry_attempts = 0
    if not hasattr(self, "_roi_rate_limit_cooldown_waits"):
        self._roi_rate_limit_cooldown_waits = 0
    if not hasattr(self, "_roi_rate_limit_cooldown_seconds"):
        self._roi_rate_limit_cooldown_seconds = 0.0
    if not hasattr(self, "_roi_rate_limit_throttled_requests"):
        self._roi_rate_limit_throttled_requests = 0
    if not hasattr(self, "_roi_rate_limit_last_retry_after_seconds"):
        self._roi_rate_limit_last_retry_after_seconds = None
    if not hasattr(self, "_roi_rate_limit_gate"):
        self._roi_rate_limit_gate = asyncio.Lock()


def _retry_after_seconds(response: httpx.Response, attempt: int) -> float:
    raw = str(response.headers.get("Retry-After") or "").strip()
    if raw:
        try:
            value = float(raw)
        except ValueError:
            try:
                deadline = parsedate_to_datetime(raw)
                value = deadline.timestamp() - time.time()
            except Exception:
                value = 0.0
        if value > 0:
            return max(0.05, min(MAX_429_DELAY_SECONDS, value))
    return min(MAX_429_DELAY_SECONDS, DEFAULT_429_DELAY_SECONDS * (2**attempt))


async def _wait_for_rate_limit_window(self: Any) -> None:
    _ensure_state(self)
    now = time.monotonic()
    cooldown = max(0.0, float(self._roi_rate_limit_cooldown_until) - now)
    if cooldown > 0:
        self._roi_rate_limit_cooldown_waits += 1
        self._roi_rate_limit_cooldown_seconds += cooldown
        await asyncio.sleep(cooldown)

    if time.monotonic() >= float(self._roi_rate_limit_recovery_until):
        return
    async with self._roi_rate_limit_gate:
        now = time.monotonic()
        wait = max(
            0.0,
            float(self._roi_rate_limit_last_request_at)
            + RECOVERY_MIN_REQUEST_INTERVAL_SECONDS
            - now,
        )
        if wait > 0:
            self._roi_rate_limit_throttled_requests += 1
            self._roi_rate_limit_cooldown_seconds += wait
            await asyncio.sleep(wait)
        self._roi_rate_limit_last_request_at = time.monotonic()


async def _rpc_with_adaptive_rate_limit(self: Any, method: str, params: list[Any]) -> Any:
    if _ORIGINAL_RPC is None:
        raise RuntimeError("Robinhood RPC rate-limit repair is not installed")
    _ensure_state(self)
    last_error: BaseException | None = None
    for attempt in range(MAX_ATTEMPTS):
        await _wait_for_rate_limit_window(self)
        try:
            result = await _ORIGINAL_RPC(self, method, params)
            return result
        except asyncio.CancelledError:
            raise
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 429:
                raise
            last_error = exc
            self._roi_rate_limit_events += 1
            if attempt >= MAX_ATTEMPTS - 1:
                break
            delay = _retry_after_seconds(exc.response, attempt)
            now = time.monotonic()
            self._roi_rate_limit_last_retry_after_seconds = delay
            self._roi_rate_limit_cooldown_until = max(
                float(self._roi_rate_limit_cooldown_until), now + delay
            )
            self._roi_rate_limit_recovery_until = max(
                float(self._roi_rate_limit_recovery_until), now + RECOVERY_MODE_SECONDS
            )
            self._roi_rate_limit_retry_attempts += 1
            continue
    assert last_error is not None
    raise last_error


setattr(_rpc_with_adaptive_rate_limit, "_roi_robinhood_rate_limit_repair", True)


def _status_with_rate_limit(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        rpc = getattr(self, "rpc", None)
        if rpc is None:
            metrics = {
                "repair_version": REPAIR_VERSION,
                "active": False,
            }
        else:
            _ensure_state(rpc)
            now = time.monotonic()
            metrics = {
                "repair_version": REPAIR_VERSION,
                "active": True,
                "rate_limit_events_session": int(getattr(rpc, "_roi_rate_limit_events", 0) or 0),
                "retry_attempts_session": int(getattr(rpc, "_roi_rate_limit_retry_attempts", 0) or 0),
                "cooldown_waits_session": int(getattr(rpc, "_roi_rate_limit_cooldown_waits", 0) or 0),
                "cooldown_seconds_total_session": float(getattr(rpc, "_roi_rate_limit_cooldown_seconds", 0.0) or 0.0),
                "throttled_requests_session": int(getattr(rpc, "_roi_rate_limit_throttled_requests", 0) or 0),
                "cooldown_remaining_seconds": max(0.0, float(getattr(rpc, "_roi_rate_limit_cooldown_until", 0.0) or 0.0) - now),
                "recovery_mode": now < float(getattr(rpc, "_roi_rate_limit_recovery_until", 0.0) or 0.0),
                "recovery_min_request_interval_seconds": RECOVERY_MIN_REQUEST_INTERVAL_SECONDS,
                "last_retry_after_seconds": getattr(rpc, "_roi_rate_limit_last_retry_after_seconds", None),
                "max_attempts_per_exact_request": MAX_ATTEMPTS,
                "failed_ranges_skipped": False,
                "catchup_batch_limit_changed": False,
                "catchup_query_concurrency_changed": False,
                "paper_entries_allowed_during_catchup": False,
                "strategy_thresholds_changed": False,
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
            }
        payload["rpc_rate_limit_recovery"] = metrics
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_robinhood_rate_limit_repair", True)
    return status


def install_robinhood_rpc_rate_limit_repair(plane_cls: type[Any]) -> None:
    global _ORIGINAL_RPC, _ORIGINAL_STATUS
    current_rpc = core.RobinhoodRpc.rpc
    if not bool(getattr(current_rpc, "_roi_robinhood_rate_limit_repair", False)):
        _ORIGINAL_RPC = current_rpc
        core.RobinhoodRpc.rpc = _rpc_with_adaptive_rate_limit  # type: ignore[method-assign]

    current_status = plane_cls.status
    if not bool(getattr(current_status, "_roi_robinhood_rate_limit_repair", False)):
        _ORIGINAL_STATUS = current_status
        plane_cls.status = _status_with_rate_limit(current_status)  # type: ignore[method-assign]


__all__ = [
    "REPAIR_VERSION",
    "install_robinhood_rpc_rate_limit_repair",
]
