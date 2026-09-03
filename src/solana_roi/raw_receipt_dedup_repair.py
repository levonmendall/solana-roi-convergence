from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable
from typing import Any

from . import launch_ws_frontier_timing_repair as frontier
from .direct_solana import DirectSolanaIngestionPlane


DEDUP_WINDOW_SECONDS = 60.0
DEDUP_MAX_KEYS = 16_384


def _increment(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_raw_receipt_dedup_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


def _cache(self: Any) -> OrderedDict[tuple[str, str], float]:
    value = getattr(self, "_roi_raw_receipt_dedup_cache", None)
    if isinstance(value, OrderedDict):
        return value
    value = OrderedDict()
    setattr(self, "_roi_raw_receipt_dedup_cache", value)
    return value


def _source_key(target: Any) -> str:
    source_hint = str(getattr(target, "source_hint", "") or "")
    if source_hint:
        return source_hint
    return f"SCOUT:{str(getattr(target, 'address', '') or '')}"


def _prune(cache: OrderedDict[tuple[str, str], float], now: float) -> None:
    cutoff = float(now) - DEDUP_WINDOW_SECONDS
    while cache:
        _key, observed = next(iter(cache.items()))
        if len(cache) <= DEDUP_MAX_KEYS and float(observed) >= cutoff:
            break
        cache.popitem(last=False)


def _first_durable_copy(self: Any, key: tuple[str, str], now: float) -> bool:
    cache = _cache(self)
    _prune(cache, now)
    previous = cache.get(key)
    if previous is not None and float(now) - float(previous) <= DEDUP_WINDOW_SECONDS:
        cache.move_to_end(key)
        cache[key] = float(now)
        return False
    cache[key] = float(now)
    cache.move_to_end(key)
    _prune(cache, now)
    return True


def _parse(
    self: Any,
    subscription_targets: dict[int, Any],
    message: dict[str, Any],
) -> tuple[Any, int, str, bool] | None:
    if message.get("method") != "logsNotification":
        return None
    params = message.get("params")
    if not isinstance(params, dict):
        return None
    try:
        subscription = int(params["subscription"])
        result = params["result"]
        slot = int(result["context"]["slot"])
        value = result["value"]
        signature = str(value["signature"])
    except (KeyError, TypeError, ValueError):
        return None
    target = subscription_targets.get(subscription)
    if target is None or slot <= 0 or not signature or not isinstance(value, dict):
        return None
    launch_like = bool(value.get("err") is None and self._launch_like(value.get("logs") or []))
    return target, slot, signature, launch_like


def _deduplicating_handler(
    original: Callable[[Any, str, dict[int, Any], dict[str, Any]], Any],
) -> Callable[[Any, str, dict[int, Any], dict[str, Any]], Any]:
    async def handle(
        self: Any,
        provider: str,
        subscription_targets: dict[int, Any],
        message: dict[str, Any],
    ) -> None:
        parsed = _parse(self, subscription_targets, message)
        if parsed is None:
            await original(self, provider, subscription_targets, message)
            return

        target, slot, signature, launch_like = parsed
        observed_monotonic = time.monotonic()
        _increment(self, "raw_seen")
        key = (_source_key(target), signature)
        if not _first_durable_copy(self, key, observed_monotonic):
            # The durable journal already enforces UNIQUE(signature, source_key).
            # Suppress only the redundant provider copy before it consumes the
            # bounded durable-dispatch queue. Keep every provider's live chain
            # frontier advancing so timing evidence and provider diversity remain
            # truthful; a launch's first copy has already snapshotted its frontier
            # before this later duplicate can advance it.
            try:
                frontier._observe_frontier(self, str(provider), int(slot), observed_monotonic)
            except Exception:
                _increment(self, "duplicate_frontier_errors")
            _increment(self, "duplicates_suppressed")
            if launch_like:
                _increment(self, "launch_duplicates_suppressed")
            return

        _increment(self, "unique_admitted")
        await original(self, provider, subscription_targets, message)

    try:
        handle.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(handle, "_roi_raw_receipt_provider_dedup", True)
    return handle


def _status_with_dedup(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        seen = int(getattr(self, "_roi_raw_receipt_dedup_raw_seen", 0) or 0)
        suppressed = int(getattr(self, "_roi_raw_receipt_dedup_duplicates_suppressed", 0) or 0)
        admitted = int(getattr(self, "_roi_raw_receipt_dedup_unique_admitted", 0) or 0)
        cache = _cache(self)
        dispatch = payload.get("raw_receipt_dispatch")
        if isinstance(dispatch, dict):
            dispatch.update(
                {
                    "provider_duplicate_suppression": True,
                    "provider_duplicate_key": "durable-source-key+signature",
                    "provider_duplicate_window_seconds": DEDUP_WINDOW_SECONDS,
                    "provider_duplicate_cache_max_keys": DEDUP_MAX_KEYS,
                    "provider_duplicate_cache_size": len(cache),
                    "raw_network_receipts_seen": seen,
                    "durable_unique_receipts_admitted": admitted,
                    "provider_duplicate_receipts_suppressed": suppressed,
                    "provider_duplicate_suppression_fraction": (suppressed / seen) if seen else 0.0,
                    "launch_duplicate_receipts_suppressed": int(
                        getattr(self, "_roi_raw_receipt_dedup_launch_duplicates_suppressed", 0) or 0
                    ),
                    "duplicate_frontier_errors": int(
                        getattr(self, "_roi_raw_receipt_dedup_duplicate_frontier_errors", 0) or 0
                    ),
                    "unique_receipts_still_durable": True,
                    "duplicate_suppression_matches_existing_unique_constraint": True,
                }
            )
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "redundant_provider_receipts_suppressed_before_sqlite_dispatch": True,
                    "redundant_provider_suppression_preserves_frontier_observation": True,
                    "raw_unique_market_scope_preserved": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_raw_receipt_provider_dedup", True)
    return status


def install_raw_receipt_dedup_repair() -> None:
    current_handler = DirectSolanaIngestionPlane._handle_notification
    if not bool(getattr(current_handler, "_roi_raw_receipt_provider_dedup", False)):
        DirectSolanaIngestionPlane._handle_notification = _deduplicating_handler(current_handler)  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_raw_receipt_provider_dedup", False)):
        DirectSolanaIngestionPlane.status = _status_with_dedup(current_status)  # type: ignore[method-assign]


__all__ = [
    "DEDUP_WINDOW_SECONDS",
    "DEDUP_MAX_KEYS",
    "install_raw_receipt_dedup_repair",
    "_first_durable_copy",
    "_source_key",
]
