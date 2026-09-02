from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

from . import direct_solana as direct_solana_module
from . import solana_rpc as solana_rpc_module
from .direct_solana import DirectSolanaIngestionPlane
from .solana_rpc import RpcEndpoint


NotificationHandler = Callable[[Any, str, dict[int, Any], dict[str, Any]], Awaitable[None]]
ContextPrefill = Callable[[Any, Any], Awaitable[bool]]
EndpointFactory = Callable[..., tuple[RpcEndpoint, ...]]

# Hard production resource ceilings. These constrain buffering/fanout only; they
# do not reduce the seven-program strategy scope, scout cohort, evidence depth,
# or certification thresholds.
DIRECT_WS_MAX_QUEUE = 64
DIRECT_WS_MAX_SIZE_BYTES = 256 * 1024
DIRECT_CANDIDATE_CONTEXT_SLOTS = 3
DIRECT_BACKGROUND_CONTEXT_SLOTS = 1
DIRECT_FAST_WORKER_SLOTS = 3
DIRECT_STALE_BACKGROUND_SECONDS = 120.0
DIRECT_RECONNECT_INITIAL_SECONDS = 0.5
DIRECT_RECONNECT_MAX_SECONDS = 30.0

_ONFINALITY_PUBLIC_HTTP = "https://solana.api.onfinality.io/public"
_ONFINALITY_PUBLIC_WS = "wss://solana.api.onfinality.io/public-ws"
_DRPC_PUBLIC = RpcEndpoint(
    name="drpc",
    http_url="https://solana.drpc.org/",
    ws_url="wss://solana.drpc.org",
)

# Only instruction logs emitted while the frozen program itself is at the top of
# the invocation stack may assert launch-like traffic. This removes false launch
# positives from nested SPL Token/ATA "Initialize" instructions while retaining
# the actual Pump/PumpSwap/Raydium pool/token creation instructions.
_LAUNCH_INSTRUCTIONS_BY_SOURCE: dict[str, frozenset[str]] = {
    "PUMP_FUN": frozenset({"create", "createv2"}),
    "PUMP_AMM": frozenset({"createpool", "createpoolv2"}),
    "RAYDIUM": frozenset(
        {
            "initialize",
            "initialize2",
            "initializev2",
            "preinitialize",
            "createpool",
            "createpoolv2",
            "initializepool",
        }
    ),
}
_PROGRAM_INVOKE = re.compile(r"^Program ([1-9A-HJ-NP-Za-km-z]+) invoke")
_PROGRAM_EXIT = re.compile(r"^Program ([1-9A-HJ-NP-Za-km-z]+) (?:success|failed:)")
_INSTRUCTION_LOG = re.compile(r"^Program log: Instruction: (.+)$", re.IGNORECASE)


class SubscriptionAcknowledgementError(RuntimeError):
    pass


class SubscriptionIdentifierError(RuntimeError):
    pass


def _cooperative_handler(original: NotificationHandler) -> NotificationHandler:
    """Force a scheduler handoff after every raw Solana notification."""

    async def handle(
        self: Any,
        provider: str,
        subscription_targets: dict[int, Any],
        message: dict[str, Any],
    ) -> None:
        await original(self, provider, subscription_targets, message)
        await asyncio.sleep(0)

    setattr(handle, "_roi_cooperative_yield", True)
    return handle


def _bounded_ws_connect(original: Callable[..., Any]) -> Callable[..., Any]:
    """Clamp receive buffering while preserving TCP/WebSocket backpressure."""

    def connect(*args: Any, **kwargs: Any) -> Any:
        requested_queue = kwargs.get("max_queue")
        requested_size = kwargs.get("max_size")
        kwargs["max_queue"] = DIRECT_WS_MAX_QUEUE if requested_queue is None else min(
            int(requested_queue), DIRECT_WS_MAX_QUEUE
        )
        kwargs["max_size"] = DIRECT_WS_MAX_SIZE_BYTES if requested_size is None else min(
            int(requested_size), DIRECT_WS_MAX_SIZE_BYTES
        )
        return original(*args, **kwargs)

    setattr(connect, "_roi_memory_bounded", True)
    return connect


def _bounded_context_prefill(original: ContextPrefill) -> ContextPrefill:
    """Bound overlapping 600-signature expansions and reserve candidate capacity."""

    async def prefill(self: Any, candidate: Any) -> bool:
        critical = False
        try:
            profile = self.service.registry.get(candidate.wallet)
            critical = bool(profile is not None and str(candidate.side).lower() == "buy")
        except Exception:
            critical = False

        attribute = "_roi_candidate_context_gate" if critical else "_roi_background_context_gate"
        slots = DIRECT_CANDIDATE_CONTEXT_SLOTS if critical else DIRECT_BACKGROUND_CONTEXT_SLOTS
        gate = getattr(self, attribute, None)
        if gate is None:
            gate = asyncio.Semaphore(slots)
            setattr(self, attribute, gate)
        async with gate:
            return await original(self, candidate)

    setattr(prefill, "_roi_memory_bounded", True)
    return prefill


def _replace_unusable_public_onfinality(original: EndpointFactory) -> EndpointFactory:
    """Replace only the known shared OnFinality public endpoint with dRPC public."""

    def endpoints(*args: Any, **kwargs: Any) -> tuple[RpcEndpoint, ...]:
        configured = original(*args, **kwargs)
        rows: list[RpcEndpoint] = []
        for endpoint in configured:
            if (
                endpoint.http_url.rstrip("/") == _ONFINALITY_PUBLIC_HTTP.rstrip("/")
                and endpoint.ws_url.rstrip("/") == _ONFINALITY_PUBLIC_WS.rstrip("/")
            ):
                rows.append(_DRPC_PUBLIC)
            else:
                rows.append(endpoint)

        deduped: list[RpcEndpoint] = []
        seen: set[tuple[str, str]] = set()
        for endpoint in rows:
            key = (endpoint.http_url.rstrip("/"), endpoint.ws_url.rstrip("/"))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(endpoint)
        return tuple(deduped)

    setattr(endpoints, "_roi_provider_repair", True)
    return endpoints


def _subscription_key(value: Any) -> str:
    """Accept provider subscription identifiers without assuming integer IDs."""

    if isinstance(value, bool) or value is None:
        raise SubscriptionIdentifierError("Solana logsSubscribe acknowledgement has no usable subscription id")
    if isinstance(value, (int, str)):
        key = str(value).strip()
        if key:
            return key
    raise SubscriptionIdentifierError("Solana logsSubscribe acknowledgement has an unsupported subscription id")


def _precise_launch_like(logs: Any) -> bool:
    """Identify launches only from the active frozen program's own instruction log."""

    if not isinstance(logs, list):
        return False
    stack: list[str] = []
    for raw in logs:
        line = str(raw)
        invoke = _PROGRAM_INVOKE.match(line)
        if invoke is not None:
            stack.append(invoke.group(1))
            continue

        instruction = _INSTRUCTION_LOG.match(line)
        if instruction is not None and stack:
            program_id = stack[-1]
            source = direct_solana_module.PROGRAM_SOURCE_BY_ID.get(program_id)
            allowed = _LAUNCH_INSTRUCTIONS_BY_SOURCE.get(str(source or ""))
            if allowed:
                normalized = "".join(ch for ch in instruction.group(1).lower() if ch.isalnum())
                if normalized in allowed:
                    return True
            continue

        exited = _PROGRAM_EXIT.match(line)
        if exited is not None and stack:
            program_id = exited.group(1)
            for index in range(len(stack) - 1, -1, -1):
                if stack[index] == program_id:
                    del stack[index:]
                    break
    return False


setattr(_precise_launch_like, "_roi_program_scoped_launch_detection", True)


async def _guarded_stream_endpoint(self: Any, endpoint: RpcEndpoint, stop: asyncio.Event) -> None:
    """Stream one provider with bounded memory and truthful connection state.

    Providers may return numeric or string subscription identifiers. Internally we
    map either form to deterministic request IDs so the existing ingestion logic
    remains provider-agnostic.
    """

    backoff = DIRECT_RECONNECT_INITIAL_SECONDS
    while not stop.is_set():
        declared_connected = False
        try:
            async with direct_solana_module.websockets.connect(
                endpoint.ws_url,
                ping_interval=15,
                ping_timeout=15,
                close_timeout=2,
                max_queue=DIRECT_WS_MAX_QUEUE,
                max_size=DIRECT_WS_MAX_SIZE_BYTES,
            ) as ws:
                request_targets: dict[int, Any] = {}
                subscription_targets: dict[int, Any] = {}
                external_to_internal: dict[str, int] = {}

                async def dispatch(message: dict[str, Any]) -> None:
                    if message.get("method") == "logsNotification":
                        params = message.get("params")
                        if not isinstance(params, dict):
                            return
                        try:
                            external_key = _subscription_key(params.get("subscription"))
                        except SubscriptionIdentifierError:
                            return
                        internal = external_to_internal.get(external_key)
                        if internal is None:
                            return
                        mapped = dict(message)
                        mapped_params = dict(params)
                        mapped_params["subscription"] = internal
                        mapped["params"] = mapped_params
                        await self._handle_notification(endpoint.name, subscription_targets, mapped)
                        return
                    await self._handle_notification(endpoint.name, subscription_targets, message)

                for request_id, target in enumerate(self.watch_targets, start=1):
                    request_targets[request_id] = target
                    await ws.send(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": request_id,
                                "method": "logsSubscribe",
                                "params": [{"mentions": [target.address]}, {"commitment": "processed"}],
                            }
                        )
                    )

                pending_acks = set(request_targets)
                while pending_acks and not stop.is_set():
                    message = json.loads(await asyncio.wait_for(ws.recv(), timeout=20.0))
                    if isinstance(message, dict) and message.get("id") in pending_acks:
                        request_id = int(message["id"])
                        if message.get("error") is not None:
                            raise SubscriptionAcknowledgementError(
                                "Solana logsSubscribe acknowledgement returned an error"
                            )
                        external_key = _subscription_key(message.get("result"))
                        external_to_internal[external_key] = request_id
                        subscription_targets[request_id] = request_targets[request_id]
                        pending_acks.discard(request_id)
                    elif isinstance(message, dict):
                        await dispatch(message)

                if pending_acks or stop.is_set():
                    continue

                await self._connection_state(endpoint.name, True)
                declared_connected = True
                backoff = DIRECT_RECONNECT_INITIAL_SECONDS

                while not stop.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                    except asyncio.TimeoutError:
                        pong = await ws.ping()
                        await asyncio.wait_for(pong, timeout=5.0)
                        continue
                    await dispatch(json.loads(raw))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if declared_connected:
                await self._connection_state(endpoint.name, False, type(exc).__name__)
            else:
                self.journal.set_provider(endpoint.name, connected=False, error_type=type(exc).__name__)
            if not stop.is_set():
                await asyncio.sleep(backoff)
                backoff = min(DIRECT_RECONNECT_MAX_SECONDS, backoff * 2.0)
        else:
            if declared_connected:
                await self._connection_state(endpoint.name, False, None)


setattr(_guarded_stream_endpoint, "_roi_stream_guarded", True)


async def _priority_routed_hydrate(self: Any, row: dict[str, Any]) -> None:
    """Keep random market samples lightweight; reserve deep collection for launches/scouts."""

    signature = str(row["signature"])
    trigger = direct_solana_module.datetime.fromisoformat(str(row["trigger_received_at"]))
    priority = int(row["priority"])
    reason = str(row["reason"])
    source_hint = str(row["source_hint"] or "") or None
    historical_recovery = reason == "gap_backfill"
    try:
        result, provider, latency = await self._get_transaction_ready(
            signature,
            hedge=priority <= 2 and not historical_recovery,
            attempts=8 if priority <= 2 else 4,
        )
        if result is None:
            attempts = int(row["attempts"]) + 1
            self.journal.finish(
                signature,
                error="confirmed transaction not yet available",
                retry=priority <= 2 and attempts < 5,
            )
            return

        swap = direct_solana_module.normalize_standard_transaction(
            result,
            signature=signature,
            trigger_received_at=trigger,
            source_hint=source_hint,
        )
        context_prefilled = False
        if swap is not None:
            profile = self.service.registry.get(swap.wallet)
            lightweight_market_sample = reason == "deterministic_market_sample" and profile is None
            if historical_recovery or lightweight_market_sample:
                # This still contributes the authoritative normalized transaction
                # needed for source-delivery proof and chronology, but it does not
                # launch six-dimensional/deployer/funding analysis for an unrelated
                # random swap.
                self._persist_context_swap(swap)
            else:
                needs_context = bool(
                    (profile is not None and swap.side == "buy")
                    or reason == "prospective_launch"
                )
                if needs_context:
                    context_prefilled = await self._prefill_launch_context(swap)
                await self.service.ingest_swap(swap)

        source = swap.source.split(":")[1] if swap is not None and ":" in swap.source else source_hint
        self.journal.record_hydration(
            signature=signature,
            source=source,
            trigger_received_at=trigger,
            hydrated_at=direct_solana_module.utcnow(),
            rpc_provider=provider,
            rpc_latency_ms=latency,
            normalized=swap is not None,
            candidate_context_prefilled=context_prefilled,
            historical_recovery=historical_recovery,
        )
        self.journal.finish(signature)
    except Exception as exc:
        attempts = int(row["attempts"]) + 1
        self.journal.finish(
            signature,
            error=f"{type(exc).__name__}: direct hydration failed closed",
            retry=priority <= 2 and attempts < 5,
        )


setattr(_priority_routed_hydrate, "_roi_priority_routed", True)


def _claim_priority(journal: Any, *, fast_only: bool) -> dict[str, Any] | None:
    """Atomically reserve either the candidate/gap lane or background lane."""

    now = direct_solana_module.utcnow().isoformat()
    comparator = "priority<=2" if fast_only else "priority>2"
    with journal.store._lock, journal.store.db:
        row = journal.store.db.execute(
            "SELECT signature, slot, trigger_received_at, source_hint, priority, reason, attempts "
            "FROM direct_solana_hydration_queue WHERE status='pending' AND "
            + comparator
            + " ORDER BY priority, updated_at, signature LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        signature = str(row["signature"])
        updated = journal.store.db.execute(
            "UPDATE direct_solana_hydration_queue SET status='processing', attempts=attempts+1, updated_at=? "
            "WHERE signature=? AND status='pending'",
            (now, signature),
        )
        if updated.rowcount != 1:
            return None
        return dict(row)


def _expire_stale_background(self: Any) -> int:
    """Fail closed background work that can no longer be valid low-latency evidence."""

    now = direct_solana_module.utcnow()
    cutoff = (now - timedelta(seconds=DIRECT_STALE_BACKGROUND_SECONDS)).isoformat()
    with self.store._lock, self.store.db:
        cur = self.store.db.execute(
            "UPDATE direct_solana_hydration_queue SET status='failed', last_error=?, updated_at=? "
            "WHERE status='pending' AND priority>2 "
            "AND reason IN ('deterministic_market_sample','prospective_launch') "
            "AND trigger_received_at<?",
            (
                "stale background hydration expired fail-closed; fresh prospective evidence required",
                now.isoformat(),
                cutoff,
            ),
        )
    return int(cur.rowcount or 0)


async def _reserved_worker(self: Any, stop: asyncio.Event, *, fast_only: bool) -> None:
    next_cleanup = 0.0
    while not stop.is_set():
        if not fast_only and time.monotonic() >= next_cleanup:
            _expire_stale_background(self)
            next_cleanup = time.monotonic() + 5.0
        row = _claim_priority(self.journal, fast_only=fast_only)
        if row is None:
            await asyncio.sleep(0.01 if fast_only else 0.025)
            continue
        await self._hydrate_one(row)


async def _reserved_run(self: Any, stop: asyncio.Event) -> None:
    """Reserve three of the existing twelve hydrators for candidate/gap work."""

    if not self.enabled:
        await stop.wait()
        return

    _expire_stale_background(self)
    fast_workers = min(DIRECT_FAST_WORKER_SLOTS, max(1, self.worker_count - 1))
    background_workers = max(1, self.worker_count - fast_workers)
    tasks = [
        asyncio.create_task(self._stream_endpoint(endpoint, stop), name=f"direct-solana-ws:{endpoint.name}")
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
            self.journal.set_provider(endpoint.name, connected=False)


setattr(_reserved_run, "_roi_worker_partitioned", True)


def _bounded_status(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    """Expose active protections and filter retired provider rows from live state."""

    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        active_names = {endpoint.name for endpoint in self.endpoints}
        states = payload.get("provider_states")
        retired_count = 0
        if isinstance(states, list):
            active_states = [row for row in states if isinstance(row, dict) and str(row.get("provider")) in active_names]
            retired_count = len(states) - len(active_states)
            payload["provider_states"] = active_states
            connected = sum(1 for row in active_states if bool(row.get("connected")))
            payload["connected_provider_count"] = connected
            payload["continuity_ok"] = bool(connected >= 1 and not payload.get("unresolved_gap", True))

        fast_workers = min(DIRECT_FAST_WORKER_SLOTS, max(1, int(self.worker_count) - 1))
        payload["production_memory_boundary"] = {
            "installed_intrinsically": True,
            "websocket_max_queue": DIRECT_WS_MAX_QUEUE,
            "websocket_max_size_bytes": DIRECT_WS_MAX_SIZE_BYTES,
            "candidate_context_slots": DIRECT_CANDIDATE_CONTEXT_SLOTS,
            "background_context_slots": DIRECT_BACKGROUND_CONTEXT_SLOTS,
            "strategy_scope_reduced": False,
            "context_signature_limit_unchanged": int(self.candidate_context_max_signatures),
            "hydration_worker_count_unchanged": int(self.worker_count),
        }
        payload["provider_runtime_policy"] = {
            "subscription_ack_required_before_connected": True,
            "provider_subscription_id_type_agnostic": True,
            "reconnect_initial_seconds": DIRECT_RECONNECT_INITIAL_SECONDS,
            "reconnect_max_seconds": DIRECT_RECONNECT_MAX_SECONDS,
            "known_unusable_public_onfinality_replaced_with_drpc": True,
            "retired_provider_state_rows_hidden": retired_count,
        }
        payload["throughput_policy"] = {
            "candidate_reserved_workers": fast_workers,
            "background_workers": max(1, int(self.worker_count) - fast_workers),
            "total_workers_unchanged": int(self.worker_count),
            "stale_background_seconds": DIRECT_STALE_BACKGROUND_SECONDS,
            "random_market_samples_deep_risk": False,
            "launches_and_scouts_preserve_deep_analysis": True,
            "launch_detection": "frozen-program-stack-scoped-instruction",
            "full_raw_market_scope_preserved": True,
        }
        return payload

    setattr(status, "_roi_memory_bounded", True)
    return status


def install_runtime_guards() -> None:
    """Install all production protections independently of the Render entrypoint."""

    current_handler = DirectSolanaIngestionPlane._handle_notification
    if not bool(getattr(current_handler, "_roi_cooperative_yield", False)):
        DirectSolanaIngestionPlane._handle_notification = _cooperative_handler(current_handler)  # type: ignore[method-assign]

    current_connect = direct_solana_module.websockets.connect
    if not bool(getattr(current_connect, "_roi_memory_bounded", False)):
        direct_solana_module.websockets.connect = _bounded_ws_connect(current_connect)  # type: ignore[assignment]

    current_prefill = DirectSolanaIngestionPlane._prefill_launch_context
    if not bool(getattr(current_prefill, "_roi_memory_bounded", False)):
        DirectSolanaIngestionPlane._prefill_launch_context = _bounded_context_prefill(current_prefill)  # type: ignore[method-assign]

    # Patch the authoritative solana_rpc module before runtime.py imports its
    # factory, then bind direct_solana to the exact same repaired factory. This
    # prevents the stream and hydration pool from silently using different peers.
    current_endpoint_factory = solana_rpc_module.rpc_endpoints_from_env
    if not bool(getattr(current_endpoint_factory, "_roi_provider_repair", False)):
        solana_rpc_module.rpc_endpoints_from_env = _replace_unusable_public_onfinality(current_endpoint_factory)  # type: ignore[assignment]
    direct_solana_module.rpc_endpoints_from_env = solana_rpc_module.rpc_endpoints_from_env

    current_launch = DirectSolanaIngestionPlane._launch_like
    if not bool(getattr(current_launch, "_roi_program_scoped_launch_detection", False)):
        DirectSolanaIngestionPlane._launch_like = staticmethod(_precise_launch_like)  # type: ignore[method-assign]

    current_stream = DirectSolanaIngestionPlane._stream_endpoint
    if not bool(getattr(current_stream, "_roi_stream_guarded", False)):
        DirectSolanaIngestionPlane._stream_endpoint = _guarded_stream_endpoint  # type: ignore[method-assign]

    current_hydrate = DirectSolanaIngestionPlane._hydrate_one
    if not bool(getattr(current_hydrate, "_roi_priority_routed", False)):
        DirectSolanaIngestionPlane._hydrate_one = _priority_routed_hydrate  # type: ignore[method-assign]

    current_run = DirectSolanaIngestionPlane.run
    if not bool(getattr(current_run, "_roi_worker_partitioned", False)):
        DirectSolanaIngestionPlane.run = _reserved_run  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_memory_bounded", False)):
        DirectSolanaIngestionPlane.status = _bounded_status(current_status)  # type: ignore[method-assign]


__all__ = [
    "DIRECT_WS_MAX_QUEUE",
    "DIRECT_WS_MAX_SIZE_BYTES",
    "DIRECT_CANDIDATE_CONTEXT_SLOTS",
    "DIRECT_BACKGROUND_CONTEXT_SLOTS",
    "DIRECT_FAST_WORKER_SLOTS",
    "DIRECT_STALE_BACKGROUND_SECONDS",
    "install_runtime_guards",
]
