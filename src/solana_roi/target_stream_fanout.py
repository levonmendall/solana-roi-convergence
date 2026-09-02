from __future__ import annotations

import asyncio
import json
import os
import time
from contextlib import suppress
from typing import Any, Callable

from . import direct_solana as direct_solana_module
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget
from .runtime_guards import (
    DIRECT_FAST_WORKER_SLOTS,
    _expire_stale_background,
    _reserved_worker,
)
from .solana_rpc import RpcEndpoint
from .stream_resilience import (
    STREAM_RECONNECT_INITIAL_SECONDS,
    STREAM_RECONNECT_MAX_SECONDS,
    _error_parts,
    _subscription_key,
)


TARGET_WS_MAX_QUEUE = 8
TARGET_WS_MAX_SIZE_BYTES = 1024 * 1024
TARGET_START_STAGGER_SECONDS = 0.10
TARGET_ACK_TIMEOUT_SECONDS = 20.0


def _target_key(target: WatchTarget) -> str:
    return f"{target.kind}:{target.address}"


def _release_id() -> tuple[str | None, str | None]:
    for key in ("RENDER_GIT_COMMIT", "SOLANA_ROI_RELEASE_COMMIT", "GIT_COMMIT"):
        value = str(os.getenv(key) or "").strip()
        if value:
            return value, key
    return None, None


def _begin_exact_release_continuity_epoch(self: Any) -> None:
    """Start a fresh prospective continuity epoch only when the exact release changes.

    A historical outage cannot be reconstructed into live arrival-time evidence. On a
    new exact release we therefore record the old gap as unrecovered evidence and
    begin a new prospective epoch instead of pretending that bounded RPC backfill
    repaired it. A process restart on the same release does *not* clear a gap.
    """

    release_id, source = _release_id()
    if not release_id:
        setattr(
            self,
            "_roi_continuity_epoch",
            {
                "release_id": None,
                "release_id_source": None,
                "reset_performed": False,
                "reset_allowed": False,
                "reason": "exact release id unavailable; existing gap state preserved fail-closed",
            },
        )
        return

    now = direct_solana_module.utcnow().isoformat()
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "CREATE TABLE IF NOT EXISTS direct_solana_continuity_epoch ("
            "id INTEGER PRIMARY KEY CHECK(id=1), release_id TEXT NOT NULL, release_id_source TEXT, "
            "started_at TEXT NOT NULL, prior_gap_unrecovered INTEGER NOT NULL, "
            "prior_outage_started_at TEXT, prior_gap_error TEXT)"
        )
        existing = self.store.db.execute(
            "SELECT release_id, release_id_source, started_at, prior_gap_unrecovered, "
            "prior_outage_started_at, prior_gap_error FROM direct_solana_continuity_epoch WHERE id=1"
        ).fetchone()
        if existing is not None and str(existing["release_id"]) == release_id:
            setattr(self, "_roi_continuity_epoch", dict(existing))
            return

        global_row = self.store.db.execute(
            "SELECT outage_started_at, unresolved_gap, last_backfill_error "
            "FROM direct_solana_global_state WHERE id=1"
        ).fetchone()
        prior_outage = str(global_row["outage_started_at"] or "") if global_row is not None else ""
        prior_error = str(global_row["last_backfill_error"] or "") if global_row is not None else ""
        prior_unresolved = bool(global_row["unresolved_gap"]) if global_row is not None else False
        prior_gap = bool(prior_unresolved or prior_outage or prior_error)

        self.store.db.execute(
            "INSERT INTO direct_solana_continuity_epoch("
            "id, release_id, release_id_source, started_at, prior_gap_unrecovered, "
            "prior_outage_started_at, prior_gap_error) VALUES (1, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET release_id=excluded.release_id, "
            "release_id_source=excluded.release_id_source, started_at=excluded.started_at, "
            "prior_gap_unrecovered=excluded.prior_gap_unrecovered, "
            "prior_outage_started_at=excluded.prior_outage_started_at, "
            "prior_gap_error=excluded.prior_gap_error",
            (release_id, source, now, 1 if prior_gap else 0, prior_outage or None, prior_error or None),
        )
        # This is not a backfill success. It is an explicit new-release prospective
        # epoch. The previous gap remains recorded above and cannot count as live
        # evidence in the new release.
        self.store.db.execute(
            "UPDATE direct_solana_global_state SET outage_started_at=NULL, unresolved_gap=0, "
            "last_backfill_complete_at=NULL, last_backfill_error=NULL WHERE id=1"
        )
        self.store.db.execute(
            "UPDATE direct_solana_hydration_queue SET status='failed', last_error=?, updated_at=? "
            "WHERE status IN ('pending','processing') AND reason='gap_backfill'",
            ("superseded by new exact-release prospective continuity epoch", now),
        )
        row = self.store.db.execute(
            "SELECT release_id, release_id_source, started_at, prior_gap_unrecovered, "
            "prior_outage_started_at, prior_gap_error FROM direct_solana_continuity_epoch WHERE id=1"
        ).fetchone()
    payload = dict(row) if row is not None else {}
    payload["reset_performed"] = True
    payload["reset_allowed"] = True
    payload["policy"] = "prior gaps are recorded, never converted into prospective live evidence"
    setattr(self, "_roi_continuity_epoch", payload)


def _state_maps(self: Any) -> tuple[asyncio.Lock, dict[str, set[str]], dict[str, asyncio.Event], dict[str, dict[str, Any]]]:
    lock = getattr(self, "_roi_target_state_lock", None)
    if lock is None:
        lock = asyncio.Lock()
        setattr(self, "_roi_target_state_lock", lock)
    connected = getattr(self, "_roi_target_connected", None)
    if not isinstance(connected, dict):
        connected = {}
        setattr(self, "_roi_target_connected", connected)
    ready_events = getattr(self, "_roi_provider_ready_events", None)
    if not isinstance(ready_events, dict):
        ready_events = {}
        setattr(self, "_roi_provider_ready_events", ready_events)
    target_states = getattr(self, "_roi_target_stream_states", None)
    if not isinstance(target_states, dict):
        target_states = {}
        setattr(self, "_roi_target_stream_states", target_states)
    return lock, connected, ready_events, target_states


def _provider_event(self: Any, provider: str) -> asyncio.Event:
    _lock, _connected, ready_events, _states = _state_maps(self)
    event = ready_events.get(provider)
    if event is None:
        event = asyncio.Event()
        ready_events[provider] = event
    return event


async def _set_target_state(
    self: Any,
    endpoint: RpcEndpoint,
    target: WatchTarget,
    *,
    connected: bool,
    error_type: str | None = None,
    error_code: int | None = None,
    error_message: str | None = None,
) -> None:
    lock, provider_targets, ready_events, states = _state_maps(self)
    provider = endpoint.name
    key = _target_key(target)
    all_keys = {_target_key(row) for row in self.watch_targets}
    event = ready_events.setdefault(provider, asyncio.Event())

    transition_to_full = False
    transition_from_full = False
    async with lock:
        live = provider_targets.setdefault(provider, set())
        before_full = live == all_keys
        if connected:
            live.add(key)
        else:
            live.discard(key)
        after_full = live == all_keys
        if after_full:
            event.set()
        else:
            event.clear()
        transition_to_full = after_full and not before_full
        transition_from_full = before_full and not after_full
        provider_state = states.setdefault(provider, {})
        previous = provider_state.get(key) if isinstance(provider_state.get(key), dict) else {}
        reconnects = int(previous.get("reconnect_count") or 0)
        if connected and not bool(previous.get("connected")):
            reconnects += 1
        provider_state[key] = {
            "connected": bool(connected),
            "kind": target.kind,
            "address": target.address,
            "source_hint": target.source_hint,
            "reconnect_count": reconnects,
            "last_change_at": direct_solana_module.utcnow().isoformat(),
            "last_error_type": error_type,
            "last_error_code": error_code,
            "last_error_message": error_message,
        }

        setup = getattr(self, "_roi_subscription_setup", None)
        if not isinstance(setup, dict):
            setup = {}
            setattr(self, "_roi_subscription_setup", setup)
        setup[provider] = {
            "ready": after_full,
            "phase": "live" if after_full else ("partial" if live else "connecting"),
            "target_count": len(all_keys),
            "acknowledged_count": len(live),
            "current_target": None,
            "current_target_kind": None,
            "attempt": None,
            "error_code": error_code,
            "error_message": error_message,
            "error_type": error_type,
            "topology": "one-logsSubscribe-per-websocket",
        }

    if transition_to_full:
        await self._connection_state(provider, True)
    elif transition_from_full:
        await self._connection_state(provider, False, error_type)
    elif not connected and not bool(getattr(self, "_initial_connection_observed", False)):
        # Keep durable provider telemetry truthful before any provider has ever
        # achieved complete ten-target readiness.
        self.journal.set_provider(provider, connected=False, error_type=error_type)


async def _single_target_stream(
    self: Any,
    endpoint: RpcEndpoint,
    target: WatchTarget,
    stop: asyncio.Event,
) -> None:
    """Run one logsSubscribe on one WebSocket with bounded independent backpressure."""

    backoff = STREAM_RECONNECT_INITIAL_SECONDS
    key = _target_key(target)
    while not stop.is_set():
        declared = False
        try:
            async with direct_solana_module.websockets.connect(
                endpoint.ws_url,
                ping_interval=15,
                ping_timeout=15,
                close_timeout=2,
                max_queue=TARGET_WS_MAX_QUEUE,
                max_size=TARGET_WS_MAX_SIZE_BYTES,
                compression=None,
            ) as ws:
                await ws.send(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "logsSubscribe",
                            "params": [{"mentions": [target.address]}, {"commitment": "processed"}],
                        }
                    )
                )
                deadline = time.monotonic() + TARGET_ACK_TIMEOUT_SECONDS
                external_subscription: str | None = None
                while not stop.is_set() and time.monotonic() < deadline:
                    remaining = max(0.05, deadline - time.monotonic())
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                    message = json.loads(raw)
                    if not isinstance(message, dict):
                        continue
                    if message.get("id") not in (1, "1"):
                        # No evidence collected before this target is acknowledged.
                        continue
                    if message.get("error") is not None:
                        code, provider_message = _error_parts(message.get("error"))
                        raise RuntimeError(f"logsSubscribe rejected code={code}: {provider_message}")
                    external_subscription = _subscription_key(message.get("result"))
                    break
                if not external_subscription:
                    raise TimeoutError("single-target Solana logsSubscribe acknowledgement timed out")

                await _set_target_state(self, endpoint, target, connected=True)
                declared = True
                backoff = STREAM_RECONNECT_INITIAL_SECONDS
                provider_ready = _provider_event(self, endpoint.name)
                subscription_targets = {1: target}

                while not stop.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                    except asyncio.TimeoutError:
                        pong = await ws.ping()
                        await asyncio.wait_for(pong, timeout=5.0)
                        continue
                    message = json.loads(raw)
                    if not isinstance(message, dict) or message.get("method") != "logsNotification":
                        continue
                    params = message.get("params")
                    if not isinstance(params, dict):
                        continue
                    try:
                        if _subscription_key(params.get("subscription")) != external_subscription:
                            continue
                    except Exception:
                        continue
                    # A partial provider is never allowed to contribute prospective
                    # evidence. If another target on this provider is down, consume
                    # and discard until all ten are live again. Another complete
                    # provider may continue independently.
                    if not provider_ready.is_set():
                        continue
                    mapped = dict(message)
                    mapped_params = dict(params)
                    mapped_params["subscription"] = 1
                    mapped["params"] = mapped_params
                    await self._handle_notification(endpoint.name, subscription_targets, mapped)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if declared:
                await _set_target_state(self, endpoint, target, connected=False, error_type=type(exc).__name__)
            else:
                await _set_target_state(self, endpoint, target, connected=False, error_type=type(exc).__name__)
            if not stop.is_set():
                await asyncio.sleep(backoff)
                backoff = min(STREAM_RECONNECT_MAX_SECONDS, backoff * 2.0)
        else:
            if declared:
                await _set_target_state(self, endpoint, target, connected=False)


async def _provider_fanout(self: Any, endpoint: RpcEndpoint, stop: asyncio.Event) -> None:
    tasks: list[asyncio.Task[Any]] = []
    try:
        for target in tuple(self.watch_targets):
            if stop.is_set():
                break
            tasks.append(
                asyncio.create_task(
                    _single_target_stream(self, endpoint, target, stop),
                    name=f"direct-solana-target:{endpoint.name}:{target.kind}:{target.address[:8]}",
                )
            )
            await asyncio.sleep(TARGET_START_STAGGER_SECONDS)
        await stop.wait()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _fanout_run(self: Any, stop: asyncio.Event) -> None:
    """Run isolated target streams while preserving the twelve-worker split."""

    if not self.enabled:
        await stop.wait()
        return

    _begin_exact_release_continuity_epoch(self)
    _expire_stale_background(self)
    fast_workers = min(DIRECT_FAST_WORKER_SLOTS, max(1, self.worker_count - 1))
    background_workers = max(1, self.worker_count - fast_workers)
    tasks = [
        asyncio.create_task(_provider_fanout(self, endpoint, stop), name=f"direct-solana-provider:{endpoint.name}")
        for endpoint in self.endpoints
    ]
    tasks.extend(
        asyncio.create_task(_reserved_worker(self, stop, fast_only=True), name=f"direct-solana-fast:{index}")
        for index in range(fast_workers)
    )
    tasks.extend(
        asyncio.create_task(_reserved_worker(self, stop, fast_only=False), name=f"direct-solana-background:{index}")
        for index in range(background_workers)
    )
    try:
        await stop.wait()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for endpoint in self.endpoints:
            with suppress(Exception):
                self.journal.set_provider(endpoint.name, connected=False)


setattr(_fanout_run, "_roi_worker_partitioned", True)
setattr(_fanout_run, "_roi_target_fanout", True)


def _status_with_target_fanout(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        targets = tuple(self.watch_targets)
        states = getattr(self, "_roi_target_stream_states", {})
        provider_rows: dict[str, Any] = {}
        for endpoint in self.endpoints:
            rows = states.get(endpoint.name, {}) if isinstance(states, dict) else {}
            if not isinstance(rows, dict):
                rows = {}
            connected = sum(1 for row in rows.values() if isinstance(row, dict) and bool(row.get("connected")))
            provider_rows[endpoint.name] = {
                "ready": connected == len(targets),
                "connected_target_count": connected,
                "target_count": len(targets),
                "targets": rows,
            }

        boundary = payload.setdefault("production_memory_boundary", {})
        if isinstance(boundary, dict):
            per_provider = len(targets) * TARGET_WS_MAX_QUEUE * TARGET_WS_MAX_SIZE_BYTES
            boundary.update(
                {
                    "websocket_topology": "one-target-per-websocket",
                    "websocket_max_queue": TARGET_WS_MAX_QUEUE,
                    "websocket_max_queue_per_target": TARGET_WS_MAX_QUEUE,
                    "websocket_max_size_bytes": TARGET_WS_MAX_SIZE_BYTES,
                    "target_streams_per_provider": len(targets),
                    "receive_payload_ceiling_bytes_per_provider": per_provider,
                    "receive_payload_ceiling_bytes_all_providers": per_provider * len(self.endpoints),
                    "compression_enabled": False,
                    "strategy_scope_reduced": False,
                }
            )

        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.pop("max_inflight_notification_handlers", None)
            policy.update(
                {
                    "subscription_topology": "one-logsSubscribe-per-websocket",
                    "provider_ready_requires_all_targets": True,
                    "partial_provider_evidence_recorded": False,
                    "notification_dispatch_path": "serial-isolated-target-stream",
                    "max_inflight_notification_handlers_per_stream": 1,
                    "target_start_stagger_ms": TARGET_START_STAGGER_SECONDS * 1000.0,
                    "full_target_count_unchanged": len(targets),
                }
            )

        payload["target_stream_fanout"] = {
            "enabled": True,
            "provider_count": len(self.endpoints),
            "target_count_per_provider": len(targets),
            "total_websocket_target_streams": len(self.endpoints) * len(targets),
            "providers": provider_rows,
        }
        epoch = getattr(self, "_roi_continuity_epoch", None)
        if isinstance(epoch, dict):
            payload["continuity_epoch"] = dict(epoch)
        else:
            release_id, source = _release_id()
            payload["continuity_epoch"] = {
                "release_id": release_id,
                "release_id_source": source,
                "reset_performed": False,
            }
        return payload

    setattr(status, "_roi_memory_bounded", True)
    setattr(status, "_roi_subscription_telemetry", True)
    setattr(status, "_roi_transport_hardened", True)
    setattr(status, "_roi_target_fanout", True)
    return status


def install_target_stream_fanout() -> None:
    current_run = DirectSolanaIngestionPlane.run
    if not bool(getattr(current_run, "_roi_target_fanout", False)):
        DirectSolanaIngestionPlane.run = _fanout_run  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_target_fanout", False)):
        DirectSolanaIngestionPlane.status = _status_with_target_fanout(current_status)  # type: ignore[method-assign]


__all__ = [
    "TARGET_WS_MAX_QUEUE",
    "TARGET_WS_MAX_SIZE_BYTES",
    "TARGET_START_STAGGER_SECONDS",
    "install_target_stream_fanout",
]
