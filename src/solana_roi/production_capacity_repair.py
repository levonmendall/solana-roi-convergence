from __future__ import annotations

import asyncio
import hashlib
import time
import weakref
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

from . import public_data_economics as public_data
from . import raw_receipt_dispatch_repair as raw_dispatch
from .direct_solana import DirectSolanaIngestionPlane
from .solana_rpc import RpcEndpoint, SolanaRpcPool
from .wallet_discovery import ContinuousWalletDiscovery


OFFICIAL_SOLANA_HTTP_HOST = "api.mainnet.solana.com"
RATE_LIMIT_INITIAL_COOLDOWN_SECONDS = 60.0
RATE_LIMIT_MAX_COOLDOWN_SECONDS = 300.0
TRANSIENT_SERVER_COOLDOWN_SECONDS = 5.0
RAW_RECEIPT_BATCH_MAX = 128
RESEARCH_QUEUE_PRESSURE_FRACTION = 0.25
RESEARCH_RPC_FAILURE_MIN_SAMPLES = 10
RESEARCH_RPC_FAILURE_FRACTION = 0.50

_ORIGINAL_RPC_CALL_ENDPOINT = SolanaRpcPool._call_endpoint
_ORIGINAL_RPC_ORDERED = SolanaRpcPool._ordered
_ORIGINAL_RPC_CALL_WITH_META = SolanaRpcPool.call_with_meta
_ORIGINAL_RPC_STATUS = SolanaRpcPool.status
_ORIGINAL_DIRECT_RUN = DirectSolanaIngestionPlane.run
_ORIGINAL_DISCOVERY_RUN_ONCE = ContinuousWalletDiscovery.run_once
_ORIGINAL_DISCOVERY_STATUS = ContinuousWalletDiscovery.status

_ACTIVE_DIRECT_PLANE: weakref.ReferenceType[Any] | None = None


class RpcEndpointCoolingDown(RuntimeError):
    """Internal signal that a read-only endpoint is temporarily unavailable."""


def _endpoint_host(endpoint: RpcEndpoint) -> str:
    try:
        return endpoint.http_url.split("/", 3)[2].lower()
    except Exception:
        return ""


def _is_official_public(endpoint: RpcEndpoint) -> bool:
    return _endpoint_host(endpoint) == OFFICIAL_SOLANA_HTTP_HOST


def _capacity_maps(pool: Any) -> tuple[dict[str, float], dict[str, int], dict[str, int], dict[str, int]]:
    cooldowns = getattr(pool, "_roi_capacity_cooldown_until", None)
    if not isinstance(cooldowns, dict):
        cooldowns = {}
        setattr(pool, "_roi_capacity_cooldown_until", cooldowns)
    rate_limits = getattr(pool, "_roi_capacity_rate_limit_events", None)
    if not isinstance(rate_limits, dict):
        rate_limits = {}
        setattr(pool, "_roi_capacity_rate_limit_events", rate_limits)
    cooldown_skips = getattr(pool, "_roi_capacity_cooldown_skips", None)
    if not isinstance(cooldown_skips, dict):
        cooldown_skips = {}
        setattr(pool, "_roi_capacity_cooldown_skips", cooldown_skips)
    server_errors = getattr(pool, "_roi_capacity_server_errors", None)
    if not isinstance(server_errors, dict):
        server_errors = {}
        setattr(pool, "_roi_capacity_server_errors", server_errors)
    return cooldowns, rate_limits, cooldown_skips, server_errors


def _http_status(exc: BaseException) -> int | None:
    response = getattr(exc, "response", None)
    raw = getattr(response, "status_code", None)
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _cooldown_remaining(pool: Any, endpoint: RpcEndpoint, *, now: float | None = None) -> float:
    cooldowns, _rate_limits, _skips, _server_errors = _capacity_maps(pool)
    current = time.monotonic() if now is None else float(now)
    return max(0.0, float(cooldowns.get(endpoint.name, 0.0) or 0.0) - current)


def _set_rate_limit_cooldown(pool: Any, endpoint: RpcEndpoint) -> None:
    cooldowns, rate_limits, _skips, _server_errors = _capacity_maps(pool)
    count = int(rate_limits.get(endpoint.name, 0) or 0) + 1
    rate_limits[endpoint.name] = count
    # Escalate repeated 429s, but keep a bounded automatic probe opportunity.
    seconds = min(
        RATE_LIMIT_MAX_COOLDOWN_SECONDS,
        RATE_LIMIT_INITIAL_COOLDOWN_SECONDS * (2.0 ** min(3, max(0, count - 1))),
    )
    cooldowns[endpoint.name] = max(
        float(cooldowns.get(endpoint.name, 0.0) or 0.0),
        time.monotonic() + seconds,
    )


def _set_server_cooldown(pool: Any, endpoint: RpcEndpoint) -> None:
    cooldowns, _rate_limits, _skips, server_errors = _capacity_maps(pool)
    server_errors[endpoint.name] = int(server_errors.get(endpoint.name, 0) or 0) + 1
    cooldowns[endpoint.name] = max(
        float(cooldowns.get(endpoint.name, 0.0) or 0.0),
        time.monotonic() + TRANSIENT_SERVER_COOLDOWN_SECONDS,
    )


async def _capacity_call_endpoint(
    self: SolanaRpcPool,
    endpoint: RpcEndpoint,
    method: str,
    params: list[Any],
) -> tuple[Any, str, float]:
    remaining = _cooldown_remaining(self, endpoint)
    if remaining > 0.0:
        _cooldowns, _limits, skips, _servers = _capacity_maps(self)
        skips[endpoint.name] = int(skips.get(endpoint.name, 0) or 0) + 1
        raise RpcEndpointCoolingDown(f"RPC endpoint cooling down for {remaining:.3f}s")
    try:
        return await _ORIGINAL_RPC_CALL_ENDPOINT(self, endpoint, method, params)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        status = _http_status(exc)
        if status == 429:
            _set_rate_limit_cooldown(self, endpoint)
        elif status is not None and status >= 500:
            _set_server_cooldown(self, endpoint)
        raise


def _capacity_ordered(self: SolanaRpcPool, method: str) -> list[RpcEndpoint]:
    ordered = list(_ORIGINAL_RPC_ORDERED(self, method))
    now = time.monotonic()
    # Preserve the canonical method-specific ordering inside each partition, but
    # never choose a cooling endpoint ahead of a usable one. The official public
    # endpoint is a burst-limited emergency fallback, not a routine hedge target.
    return sorted(
        ordered,
        key=lambda endpoint: (
            1 if _cooldown_remaining(self, endpoint, now=now) > 0.0 else 0,
            1 if _is_official_public(endpoint) else 0,
            ordered.index(endpoint),
        ),
    )


def _official_pair_requires_sequential_fallback(pool: SolanaRpcPool) -> bool:
    endpoints = tuple(getattr(pool, "endpoints", ()) or ())
    return len(endpoints) == 2 and any(_is_official_public(endpoint) for endpoint in endpoints)


async def _capacity_call_with_meta(
    self: SolanaRpcPool,
    method: str,
    params: list[Any],
    *,
    hedge: bool = False,
) -> tuple[Any, str, float]:
    if not hedge or not _official_pair_requires_sequential_fallback(self):
        return await _ORIGINAL_RPC_CALL_WITH_META(self, method, params, hedge=hedge)

    # PublicNode routinely takes longer than the 150ms hedge delay while still
    # returning valid results. Starting api.mainnet.solana.com on every such read
    # converted normal latency into a sustained 429 load. For this exact public
    # pair, use the official endpoint only after the preferred endpoint actually
    # fails. Generic endpoint pairs and explicitly opted-in managed capacity keep
    # the established hedge behavior.
    errors: list[Exception] = []
    for endpoint in self._ordered(method):
        try:
            return await self._call_endpoint(endpoint, method, params)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            errors.append(exc)
    raise RuntimeError(f"all Solana RPC endpoints failed for {method}") from (errors[-1] if errors else None)


def _capacity_status(self: SolanaRpcPool) -> dict[str, Any]:
    payload = _ORIGINAL_RPC_STATUS(self)
    cooldowns, rate_limits, cooldown_skips, server_errors = _capacity_maps(self)
    now = time.monotonic()
    endpoint_capacity: list[dict[str, Any]] = []
    for endpoint in self.endpoints:
        remaining = max(0.0, float(cooldowns.get(endpoint.name, 0.0) or 0.0) - now)
        endpoint_capacity.append(
            {
                "name": endpoint.name,
                "http_host": _endpoint_host(endpoint),
                "official_public_fallback": _is_official_public(endpoint),
                "cooling_down": remaining > 0.0,
                "cooldown_remaining_seconds": remaining,
                "rate_limit_events": int(rate_limits.get(endpoint.name, 0) or 0),
                "cooldown_skips": int(cooldown_skips.get(endpoint.name, 0) or 0),
                "transient_server_errors": int(server_errors.get(endpoint.name, 0) or 0),
            }
        )
    payload["capacity_control"] = {
        "installed": True,
        "rate_limit_status": 429,
        "rate_limit_initial_cooldown_seconds": RATE_LIMIT_INITIAL_COOLDOWN_SECONDS,
        "rate_limit_max_cooldown_seconds": RATE_LIMIT_MAX_COOLDOWN_SECONDS,
        "official_public_proactive_hedge_disabled": _official_pair_requires_sequential_fallback(self),
        "managed_or_generic_hedging_unchanged": True,
        "usable_endpoint_count": sum(1 for row in endpoint_capacity if not row["cooling_down"]),
        "endpoint_count": len(endpoint_capacity),
        "endpoints": endpoint_capacity,
        "read_only": True,
    }
    return payload


def _parse_dispatch_item(item: Any) -> tuple[int, float, int, datetime, str, dict[int, Any], dict[str, Any]]:
    priority, received_monotonic, sequence, received_at, provider, targets, message = item
    return (
        int(priority),
        float(received_monotonic),
        int(sequence),
        received_at,
        str(provider),
        targets,
        message,
    )


def _dispatch_fields(item: Any) -> tuple[Any, int, str, bool, str | None] | None:
    _priority, _mono, _seq, _received_at, _provider, targets, message = _parse_dispatch_item(item)
    params = message.get("params") if isinstance(message, dict) else None
    result = params.get("result") if isinstance(params, dict) else None
    context = result.get("context") if isinstance(result, dict) else None
    value = result.get("value") if isinstance(result, dict) else None
    if not isinstance(value, dict):
        return None
    try:
        subscription = int(params.get("subscription"))
        slot = int(context.get("slot"))
    except (TypeError, ValueError, AttributeError):
        return None
    signature = str(value.get("signature") or "")
    target = targets.get(subscription) if isinstance(targets, dict) else None
    if target is None or slot <= 0 or not signature:
        return None
    failed = value.get("err") is not None
    source = str(getattr(target, "source_hint", "") or "") or None
    return target, slot, signature, failed, source


def _can_batch_background(self: Any, item: Any) -> bool:
    fields = _dispatch_fields(item)
    if fields is None:
        return False
    target, _slot, _signature, failed, source = fields
    if str(getattr(target, "kind", "")) != "program" or not source:
        return False
    priority = int(item[0])
    if priority < 10:
        return False
    if failed:
        return True
    # Successful ordinary program receipts can bypass the canonical per-receipt
    # transaction only after that source has already satisfied the unchanged
    # normalized-swap bootstrap minimum. If readiness is uncertain, use the
    # canonical handler and preserve its enqueue behavior.
    return not public_data._source_needs_bootstrap(self, source)


def _observe_dispatch_delay(self: Any, item: Any) -> None:
    _priority, received_monotonic, _seq, _at, _provider, _targets, _message = _parse_dispatch_item(item)
    delay_ms = max(0.0, (time.monotonic() - received_monotonic) * 1000.0)
    raw_dispatch._delay_window(self).append(delay_ms)
    setattr(self, "_roi_raw_receipt_dispatch_last_delay_ms", delay_ms)
    setattr(
        self,
        "_roi_raw_receipt_dispatch_max_delay_ms",
        max(float(getattr(self, "_roi_raw_receipt_dispatch_max_delay_ms", 0.0) or 0.0), delay_ms),
    )


def _persist_background_batch(self: Any, items: list[Any]) -> int:
    """Persist ordinary unique receipts with one SQLite commit per micro-batch.

    This is intentionally only the no-hydration branch of the canonical public-data
    handler. Launches, scouts, and source-bootstrap receipts still execute the
    original handler. The minute counters and rolling digest are updated in the
    same item order as DirectSolanaJournal.record_receipt, and every unique receipt
    remains durable.
    """

    journal = self.journal
    inserted_count = 0
    provider_last: dict[str, datetime] = {}
    with self.store._lock, self.store.db:
        for item in items:
            _priority, _mono, _seq, received_at, provider, _targets, _message = _parse_dispatch_item(item)
            fields = _dispatch_fields(item)
            if fields is None:
                raise RuntimeError("invalid raw receipt dispatch item")
            target, slot, signature, _failed, source = fields
            source_key = source or f"SCOUT:{str(getattr(target, 'address', '') or '')}"
            expires_at = received_at + timedelta(seconds=float(journal.raw_retention_seconds))
            cur = self.store.db.execute(
                "INSERT OR IGNORE INTO direct_solana_recent_receipts("
                "signature, source_key, slot, received_at, launch_like, expires_at) VALUES (?, ?, ?, ?, 0, ?)",
                (signature, source_key, int(slot), received_at.isoformat(), expires_at.isoformat()),
            )
            previous_provider_time = provider_last.get(provider)
            if previous_provider_time is None or received_at > previous_provider_time:
                provider_last[provider] = received_at
            if cur.rowcount != 1:
                continue

            inserted_count += 1
            if source_key in {"PUMP_FUN", "PUMP_AMM", "RAYDIUM"}:
                bucket = received_at.replace(second=0, microsecond=0).isoformat()
                row = self.store.db.execute(
                    "SELECT receipt_count, rolling_sha256 FROM direct_solana_minute_receipts WHERE bucket=? AND source=?",
                    (bucket, source_key),
                ).fetchone()
                previous_hash = str(row["rolling_sha256"]) if row is not None else ""
                digest = hashlib.sha256(
                    f"{previous_hash}|{signature}|{int(slot)}|{received_at.isoformat()}".encode("utf-8")
                ).hexdigest()
                if row is None:
                    self.store.db.execute(
                        "INSERT INTO direct_solana_minute_receipts("
                        "bucket, source, receipt_count, first_received_at, last_received_at, last_slot, rolling_sha256) "
                        "VALUES (?, ?, 1, ?, ?, ?, ?)",
                        (bucket, source_key, received_at.isoformat(), received_at.isoformat(), int(slot), digest),
                    )
                else:
                    self.store.db.execute(
                        "UPDATE direct_solana_minute_receipts SET receipt_count=receipt_count+1, "
                        "last_received_at=?, last_slot=?, rolling_sha256=? WHERE bucket=? AND source=?",
                        (received_at.isoformat(), int(slot), digest, bucket, source_key),
                    )

            journal._receipt_inserts = int(getattr(journal, "_receipt_inserts", 0) or 0) + 1
            if journal._receipt_inserts % 500 == 0:
                self.store.db.execute(
                    "DELETE FROM direct_solana_recent_receipts WHERE expires_at<?",
                    (received_at.isoformat(),),
                )

        for provider, received_at in provider_last.items():
            self.store.db.execute(
                "UPDATE direct_solana_provider_state SET last_message_at=? WHERE provider=?",
                (received_at.isoformat(), provider),
            )
    return inserted_count


async def _capacity_dispatch_worker(
    self: Any,
    stop: asyncio.Event,
    handler: Callable[[Any, str, dict[int, Any], dict[str, Any]], Any],
) -> None:
    queue = raw_dispatch._dispatch_queue(self)
    if queue is None:
        return
    while not stop.is_set():
        try:
            first = await asyncio.wait_for(queue.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue

        items = [first]
        while len(items) < RAW_RECEIPT_BATCH_MAX:
            try:
                items.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        background: list[Any] = []
        try:
            # Queue order already preserves launch/scout priority. Process critical
            # items canonically; collect only ordinary no-hydration program receipts
            # into one durable transaction.
            for item in items:
                _observe_dispatch_delay(self, item)
                if _can_batch_background(self, item):
                    background.append(item)
                    continue
                _priority, _mono, _seq, received_at, provider, targets, message = _parse_dispatch_item(item)
                token = raw_dispatch._RECEIPT_WALL_TIME.set(received_at)
                try:
                    await handler(self, provider, targets, message)
                    raw_dispatch._increment(self, "completed")
                    setattr(
                        self,
                        "_roi_capacity_dispatch_canonical_critical",
                        int(getattr(self, "_roi_capacity_dispatch_canonical_critical", 0) or 0) + 1,
                    )
                finally:
                    raw_dispatch._RECEIPT_WALL_TIME.reset(token)

            if background:
                _persist_background_batch(self, background)
                raw_dispatch._increment(self, "completed", len(background))
                setattr(
                    self,
                    "_roi_capacity_dispatch_batch_commits",
                    int(getattr(self, "_roi_capacity_dispatch_batch_commits", 0) or 0) + 1,
                )
                setattr(
                    self,
                    "_roi_capacity_dispatch_batched_receipts",
                    int(getattr(self, "_roi_capacity_dispatch_batched_receipts", 0) or 0) + len(background),
                )
                setattr(
                    self,
                    "_roi_capacity_dispatch_max_batch_size",
                    max(int(getattr(self, "_roi_capacity_dispatch_max_batch_size", 0) or 0), len(background)),
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raw_dispatch._increment(self, "failed")
            setattr(self, "_roi_raw_receipt_dispatch_last_error_type", type(exc).__name__)
            setattr(self, "_roi_raw_receipt_dispatch_fatal", exc)
            return
        finally:
            for _item in items:
                queue.task_done()


async def _capacity_direct_run(self: Any, stop: asyncio.Event) -> None:
    global _ACTIVE_DIRECT_PLANE
    _ACTIVE_DIRECT_PLANE = weakref.ref(self)
    try:
        await _ORIGINAL_DIRECT_RUN(self, stop)
    finally:
        current = _ACTIVE_DIRECT_PLANE() if _ACTIVE_DIRECT_PLANE is not None else None
        if current is self:
            _ACTIVE_DIRECT_PLANE = None


def _raw_queue_pressure() -> tuple[float, int, int]:
    plane = _ACTIVE_DIRECT_PLANE() if _ACTIVE_DIRECT_PLANE is not None else None
    if plane is None:
        return 0.0, 0, int(raw_dispatch.RAW_RECEIPT_QUEUE_MAX)
    queue = raw_dispatch._dispatch_queue(plane)
    if queue is None:
        return 0.0, 0, int(raw_dispatch.RAW_RECEIPT_QUEUE_MAX)
    maximum = max(1, int(getattr(queue, "maxsize", 0) or raw_dispatch.RAW_RECEIPT_QUEUE_MAX))
    depth = int(queue.qsize())
    return min(1.0, max(0.0, depth / maximum)), depth, maximum


def _rpc_redundancy_degraded(pool: Any) -> bool:
    endpoints = tuple(getattr(pool, "endpoints", ()) or ())
    if len(endpoints) < 2:
        return True
    if any(_cooldown_remaining(pool, endpoint) > 0.0 for endpoint in endpoints):
        return True
    health = getattr(pool, "_health", None)
    if not isinstance(health, dict):
        return False
    degraded = 0
    for endpoint in endpoints:
        row = health.get(endpoint.name)
        if not isinstance(row, dict):
            continue
        successes = int(row.get("successes", 0) or 0)
        failures = int(row.get("failures", 0) or 0)
        total = successes + failures
        if total >= RESEARCH_RPC_FAILURE_MIN_SAMPLES and failures / total >= RESEARCH_RPC_FAILURE_FRACTION:
            degraded += 1
    return degraded > 0


def _research_pressure_reason(self: Any) -> str | None:
    pressure, _depth, _maximum = _raw_queue_pressure()
    if pressure >= RESEARCH_QUEUE_PRESSURE_FRACTION:
        return "raw_receipt_dispatch_pressure"
    if _rpc_redundancy_degraded(self.rpc):
        return "critical_rpc_redundancy_degraded"
    return None


async def _capacity_discovery_run_once(self: ContinuousWalletDiscovery) -> dict[str, Any]:
    reason = _research_pressure_reason(self)
    if reason is None:
        setattr(self, "_roi_capacity_pause_reason", None)
        return await _ORIGINAL_DISCOVERY_RUN_ONCE(self)

    setattr(self, "_roi_capacity_pause_reason", reason)
    setattr(
        self,
        "_roi_capacity_paused_cycles",
        int(getattr(self, "_roi_capacity_paused_cycles", 0) or 0) + 1,
    )
    payload = self.status()
    payload["cycle"] = {
        "paused_for_critical_capacity": True,
        "pause_reason": reason,
        "broad_samples_added": 0,
        "tracked_wallets_polled": 0,
        "forward_observations_added": 0,
        "adaptive_proposal_evaluated": False,
        "adaptive_proposal": None,
    }
    return payload


def _capacity_discovery_status(self: ContinuousWalletDiscovery) -> dict[str, Any]:
    payload = _ORIGINAL_DISCOVERY_STATUS(self)
    pressure, depth, maximum = _raw_queue_pressure()
    reason = _research_pressure_reason(self)
    payload["critical_capacity_backpressure"] = {
        "installed": True,
        "active": reason is not None,
        "reason": reason,
        "paused_cycles": int(getattr(self, "_roi_capacity_paused_cycles", 0) or 0),
        "raw_queue_pressure_fraction": pressure,
        "raw_queue_depth": depth,
        "raw_queue_max": maximum,
        "pause_threshold_fraction": RESEARCH_QUEUE_PRESSURE_FRACTION,
        "rpc_redundancy_degraded": _rpc_redundancy_degraded(self.rpc),
        "research_only": True,
        "strategy_thresholds_unchanged": True,
    }
    return payload


def _status_with_dispatch_capacity(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        dispatch = payload.get("raw_receipt_dispatch")
        if isinstance(dispatch, dict):
            depth = int(dispatch.get("queue_depth", 0) or 0)
            maximum = max(1, int(dispatch.get("queue_max", raw_dispatch.RAW_RECEIPT_QUEUE_MAX) or 1))
            dispatch.update(
                {
                    "microbatch_durable_commit_enabled": True,
                    "microbatch_max_receipts": RAW_RECEIPT_BATCH_MAX,
                    "microbatch_only_ordinary_no_hydration_program_receipts": True,
                    "launch_and_scout_canonical_path_preserved": True,
                    "unique_receipts_still_durable": True,
                    "batch_commits": int(getattr(self, "_roi_capacity_dispatch_batch_commits", 0) or 0),
                    "batched_receipts": int(getattr(self, "_roi_capacity_dispatch_batched_receipts", 0) or 0),
                    "max_batch_size": int(getattr(self, "_roi_capacity_dispatch_max_batch_size", 0) or 0),
                    "canonical_critical_receipts": int(
                        getattr(self, "_roi_capacity_dispatch_canonical_critical", 0) or 0
                    ),
                    "queue_pressure_fraction": min(1.0, max(0.0, depth / maximum)),
                }
            )
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "official_public_http_is_fallback_not_proactive_hedge": True,
                    "rpc_429_cooldown_enabled": True,
                    "raw_receipt_microbatch_commit": True,
                    "research_yields_to_critical_capacity": True,
                    "certification_thresholds_unchanged": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_production_capacity_repair", True)
    return status


def install_production_capacity_repair() -> None:
    """Protect certification lanes from public-RPC throttling and SQLite backlog."""

    if not bool(getattr(SolanaRpcPool._call_endpoint, "_roi_production_capacity_repair", False)):
        setattr(_capacity_call_endpoint, "_roi_production_capacity_repair", True)
        setattr(_capacity_ordered, "_roi_production_capacity_repair", True)
        setattr(_capacity_call_with_meta, "_roi_production_capacity_repair", True)
        setattr(_capacity_status, "_roi_production_capacity_repair", True)
        SolanaRpcPool._call_endpoint = _capacity_call_endpoint  # type: ignore[method-assign]
        SolanaRpcPool._ordered = _capacity_ordered  # type: ignore[method-assign]
        SolanaRpcPool.call_with_meta = _capacity_call_with_meta  # type: ignore[method-assign]
        SolanaRpcPool.status = _capacity_status  # type: ignore[method-assign]

    # raw_receipt_dispatch_repair's run wrapper resolves this module-global worker
    # when the production task starts, so replacing it here upgrades the worker
    # without stacking a second in-memory queue or changing socket-read timestamps.
    raw_dispatch._dispatch_worker = _capacity_dispatch_worker  # type: ignore[assignment]

    current_run = DirectSolanaIngestionPlane.run
    if not bool(getattr(current_run, "_roi_production_capacity_repair", False)):
        try:
            _capacity_direct_run.__dict__.update(getattr(current_run, "__dict__", {}))
        except Exception:
            pass
        setattr(_capacity_direct_run, "_roi_production_capacity_repair", True)
        DirectSolanaIngestionPlane.run = _capacity_direct_run  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_production_capacity_repair", False)):
        DirectSolanaIngestionPlane.status = _status_with_dispatch_capacity(current_status)  # type: ignore[method-assign]

    if not bool(getattr(ContinuousWalletDiscovery.run_once, "_roi_production_capacity_repair", False)):
        setattr(_capacity_discovery_run_once, "_roi_production_capacity_repair", True)
        setattr(_capacity_discovery_status, "_roi_production_capacity_repair", True)
        ContinuousWalletDiscovery.run_once = _capacity_discovery_run_once  # type: ignore[method-assign]
        ContinuousWalletDiscovery.status = _capacity_discovery_status  # type: ignore[method-assign]


__all__ = [
    "OFFICIAL_SOLANA_HTTP_HOST",
    "RATE_LIMIT_INITIAL_COOLDOWN_SECONDS",
    "RATE_LIMIT_MAX_COOLDOWN_SECONDS",
    "RAW_RECEIPT_BATCH_MAX",
    "RESEARCH_QUEUE_PRESSURE_FRACTION",
    "RpcEndpointCoolingDown",
    "install_production_capacity_repair",
    "_persist_background_batch",
    "_research_pressure_reason",
]
