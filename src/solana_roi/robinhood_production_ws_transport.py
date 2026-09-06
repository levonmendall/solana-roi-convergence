from __future__ import annotations

import asyncio
import json
import os
import queue
import threading
import time
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse

import websockets

from . import robinhood_chain_runtime as runtime
from . import robinhood_live_frontier_verification_repair as frontier
from .strategy_v51_authority import authority


TRANSPORT_VERSION = "robinhood-production-ws-transport-v1"
PUBLIC_SEQUENCER_FEED = "wss://feed.mainnet.chain.robinhood.com"
READER_STALE_SECONDS = 5.0
RECONNECT_SECONDS = 0.25
QUEUE_MAX_ITEMS = 100_000
PROCESS_BATCH_MAX = 10_000
PROCESS_SLEEP_SECONDS = 0.01
SETTLEMENT_INTERVAL_SECONDS = 1.0

_EVENT_TOPICS = (
    runtime.V3_POOL_CREATED_TOPIC,
    runtime.V3_SWAP_TOPIC,
    runtime.PONS_V1_TOKEN_LAUNCHED_TOPIC,
    runtime.PONS_V2_TOKEN_LAUNCHED_TOPIC,
    runtime.PONS_V2_CURVE_BUY_TOPIC,
    runtime.PONS_V2_CURVE_SELL_TOPIC,
)
_FACTORY_ADDRESSES = {
    runtime.UNISWAP_V3_FACTORY,
    runtime.PONS_V1_ACTIVE_FACTORY,
    runtime.PONS_V1_LEGACY_FACTORY,
    runtime.PONS_V2_FACTORY,
}

_CANONICAL_LATENCY_SECONDS = float(authority()["execution"]["latency_hard_max_seconds"])
_INSTALLED = False
_ORIGINAL_RUN: Callable[..., Awaitable[Any]] | None = None
_ORIGINAL_STATUS: Callable[[Any], dict[str, Any]] | None = None


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_latency_hard_max_seconds() -> float:
    return _CANONICAL_LATENCY_SECONDS


def _rpc_url() -> str:
    return (os.getenv("ROBINHOOD_RPC_URL") or runtime.ROBINHOOD_PUBLIC_RPC).strip()


def _ws_url() -> str:
    return (os.getenv("ROBINHOOD_WS_URL") or "").strip()


def _normalized_endpoint(value: str) -> str:
    return value.rstrip("/").lower()


def production_provider_configured() -> bool:
    rpc_url = _rpc_url()
    ws_url = _ws_url()
    if not rpc_url or not ws_url:
        return False
    if _normalized_endpoint(rpc_url) == _normalized_endpoint(runtime.ROBINHOOD_PUBLIC_RPC):
        return False
    if _normalized_endpoint(ws_url) == _normalized_endpoint(PUBLIC_SEQUENCER_FEED):
        return False
    try:
        rpc = urlparse(rpc_url)
        ws = urlparse(ws_url)
    except Exception:
        return False
    return rpc.scheme == "https" and bool(rpc.netloc) and ws.scheme == "wss" and bool(ws.netloc)


def endpoint_kind() -> str:
    if production_provider_configured():
        return "configured_production_rpc_and_websocket"
    if _normalized_endpoint(_rpc_url()) == _normalized_endpoint(runtime.ROBINHOOD_PUBLIC_RPC):
        return "official_public_rate_limited_research_only"
    return "production_provider_configuration_incomplete"


def _state_lock(self: Any) -> threading.Lock:
    value = getattr(self, "_roi_prod_ws_state_lock", None)
    if not isinstance(value, type(threading.Lock())):
        value = threading.Lock()
        setattr(self, "_roi_prod_ws_state_lock", value)
    return value


def _event_queue(self: Any) -> queue.Queue[dict[str, Any]]:
    value = getattr(self, "_roi_prod_ws_queue", None)
    if not isinstance(value, queue.Queue):
        value = queue.Queue(maxsize=QUEUE_MAX_ITEMS)
        setattr(self, "_roi_prod_ws_queue", value)
    return value


def _state(self: Any) -> dict[str, Any]:
    lock = _state_lock(self)
    with lock:
        value = getattr(self, "_roi_prod_ws_state", None)
        if not isinstance(value, dict):
            value = {
                "generation": 0,
                "connected": False,
                "synchronized": False,
                "head_block": None,
                "last_head_monotonic": None,
                "last_message_at": None,
                "last_error_type": None,
                "connections": 0,
                "disconnects": 0,
                "logs_received": 0,
                "queue_overflows": 0,
            }
            setattr(self, "_roi_prod_ws_state", value)
        return dict(value)


def _update_state(self: Any, **updates: Any) -> dict[str, Any]:
    lock = _state_lock(self)
    with lock:
        value = getattr(self, "_roi_prod_ws_state", None)
        if not isinstance(value, dict):
            value = {
                "generation": 0,
                "connected": False,
                "synchronized": False,
                "head_block": None,
                "last_head_monotonic": None,
                "last_message_at": None,
                "last_error_type": None,
                "connections": 0,
                "disconnects": 0,
                "logs_received": 0,
                "queue_overflows": 0,
            }
            setattr(self, "_roi_prod_ws_state", value)
        value.update(updates)
        return dict(value)


def _bump_state(self: Any, name: str, amount: int = 1) -> int:
    lock = _state_lock(self)
    with lock:
        value = getattr(self, "_roi_prod_ws_state", None)
        if not isinstance(value, dict):
            value = {}
            setattr(self, "_roi_prod_ws_state", value)
        current = int(value.get(name, 0) or 0) + int(amount)
        value[name] = current
        return current


def _reader_ready(self: Any) -> bool:
    if not production_provider_configured():
        return False
    state = _state(self)
    last = state.get("last_head_monotonic")
    return bool(
        state.get("connected")
        and state.get("synchronized")
        and isinstance(last, (int, float))
        and time.monotonic() - float(last) <= READER_STALE_SECONDS
    )


def _block_reason(self: Any) -> str | None:
    if not production_provider_configured():
        return "robinhood_production_rpc_websocket_required"
    if not _reader_ready(self):
        return "robinhood_production_websocket_not_ready"
    event_age = getattr(self, "_roi_prod_ws_active_event_age_seconds", None)
    event_generation = getattr(self, "_roi_prod_ws_active_event_generation", None)
    state = _state(self)
    if event_age is None or event_generation != state.get("generation"):
        return "robinhood_no_current_production_ws_event_context"
    if float(event_age) > canonical_latency_hard_max_seconds():
        return "robinhood_event_exceeded_v51_latency_hard_max"
    return None


async def _production_fresh_ready(self: Any) -> bool:
    """Final paper-entry guard: production WS continuity + canonical v5.1 time latency.

    The legacy two-block heuristic was a polling transport approximation. It is not
    present in frozen v5.1 economic authority and has no production authority here.
    """
    reason = _block_reason(self)
    setattr(self, "_roi_production_transport_block_reason", reason)
    ready = reason is None
    setattr(self, "_roi_live_epoch_ready", ready)
    if not ready:
        self._caught_up = False
    return ready


def _reader_generation_start(self: Any) -> int:
    state = _state(self)
    generation = int(state.get("generation", 0) or 0) + 1
    _update_state(
        self,
        generation=generation,
        connected=False,
        synchronized=False,
        head_block=None,
        last_head_monotonic=None,
        last_error_type=None,
    )
    return generation


async def _recv_response(ws: Any, request_id: int) -> Any:
    while True:
        raw = await ws.recv()
        payload = json.loads(raw)
        if payload.get("id") != request_id:
            continue
        if payload.get("error") is not None:
            raise RuntimeError(f"websocket rpc request {request_id}: {payload['error']}")
        return payload.get("result")


async def _reader_async(self: Any, stop: threading.Event) -> None:
    while not stop.is_set():
        generation = _reader_generation_start(self)
        try:
            async with websockets.connect(
                _ws_url(),
                open_timeout=10,
                close_timeout=3,
                ping_interval=20,
                max_size=8 * 1024 * 1024,
            ) as ws:
                await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}))
                chain_id = int(str(await _recv_response(ws, 1)), 16)
                if chain_id != runtime.ROBINHOOD_CHAIN_ID:
                    raise RuntimeError(f"wrong Robinhood websocket chain id: {chain_id}")

                await ws.send(json.dumps({"jsonrpc": "2.0", "id": 2, "method": "eth_subscribe", "params": ["newHeads"]}))
                head_subscription = str(await _recv_response(ws, 2))
                await ws.send(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": 3,
                            "method": "eth_subscribe",
                            "params": ["logs", {"topics": [list(_EVENT_TOPICS)]}],
                        }
                    )
                )
                log_subscription = str(await _recv_response(ws, 3))
                _bump_state(self, "connections")
                _update_state(self, connected=True, synchronized=True, last_error_type=None)

                while not stop.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    except asyncio.TimeoutError:
                        continue
                    payload = json.loads(raw)
                    if payload.get("method") != "eth_subscription":
                        continue
                    params = payload.get("params") or {}
                    subscription = str(params.get("subscription") or "")
                    result = params.get("result")
                    now_mono = time.monotonic()
                    now_iso = _utcnow()
                    if subscription == head_subscription and isinstance(result, dict):
                        try:
                            head = int(str(result.get("number") or "0x0"), 16)
                        except (TypeError, ValueError):
                            continue
                        _update_state(
                            self,
                            connected=True,
                            synchronized=True,
                            head_block=head,
                            last_head_monotonic=now_mono,
                            last_message_at=now_iso,
                            last_error_type=None,
                        )
                        continue
                    if subscription != log_subscription or not isinstance(result, dict):
                        continue
                    item = {
                        "generation": generation,
                        "received_monotonic": now_mono,
                        "received_at": now_iso,
                        "log": result,
                    }
                    try:
                        _event_queue(self).put_nowait(item)
                    except queue.Full as exc:
                        _bump_state(self, "queue_overflows")
                        _update_state(self, connected=False, synchronized=False, last_error_type="ProductionWsQueueOverflow")
                        raise RuntimeError("Robinhood production websocket queue overflow") from exc
                    _bump_state(self, "logs_received")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _bump_state(self, "disconnects")
            _update_state(
                self,
                connected=False,
                synchronized=False,
                last_error_type=type(exc).__name__,
            )
        if not stop.is_set():
            await asyncio.sleep(RECONNECT_SECONDS)


def _reader_thread_main(self: Any, stop: threading.Event) -> None:
    try:
        asyncio.run(_reader_async(self, stop))
    except BaseException as exc:
        _update_state(
            self,
            connected=False,
            synchronized=False,
            last_error_type=type(exc).__name__,
        )


def _sort_key(item: dict[str, Any]) -> tuple[int, int, int]:
    log = item["log"]
    return (
        int(str(log.get("blockNumber") or "0x0"), 16),
        int(str(log.get("transactionIndex") or "0x0"), 16),
        int(str(log.get("logIndex") or "0x0"), 16),
    )


async def _process_block(self: Any, items: list[dict[str, Any]], *, generation: int) -> None:
    items.sort(key=_sort_key)
    # Process market-definition logs first so a pool/curve launched and traded in the
    # same block can be recognized without retrospective acquisition.
    for item in items:
        log = item["log"]
        address = runtime._clean_address(log.get("address"))
        if address in _FACTORY_ADDRESSES:
            await self._process_factory_log(log)

    for item in items:
        log = item["log"]
        topics = list(log.get("topics") or ())
        topic0 = str(topics[0]).lower() if topics else ""
        address = runtime._clean_address(log.get("address"))
        age = max(0.0, time.monotonic() - float(item["received_monotonic"]))
        setattr(self, "_roi_prod_ws_active_event_age_seconds", age)
        setattr(self, "_roi_prod_ws_active_event_generation", generation)
        setattr(self, "_roi_prod_ws_active_event_received_at", item["received_at"])
        live = bool(_reader_ready(self) and age <= canonical_latency_hard_max_seconds())
        if topic0 == runtime.V3_SWAP_TOPIC.lower():
            market = getattr(self, "v3_pools", {}).get(address)
            if market is not None:
                await self._process_v3_swap(market, log, live=live, observed_at=str(item["received_at"]))
        elif topic0 in {
            runtime.PONS_V2_CURVE_BUY_TOPIC.lower(),
            runtime.PONS_V2_CURVE_SELL_TOPIC.lower(),
        }:
            market = getattr(self, "v2_curves", {}).get(address)
            if market is not None:
                await self._process_v2_curve_log(market, log, live=live, observed_at=str(item["received_at"]))
        if not live:
            setattr(
                self,
                "_roi_production_transport_block_reason",
                "robinhood_event_exceeded_v51_latency_hard_max"
                if age > canonical_latency_hard_max_seconds()
                else "robinhood_production_websocket_not_ready",
            )


async def _production_ws_run(self: Any, stop: asyncio.Event) -> None:
    if not self.enabled:
        return
    reader_stop = threading.Event()
    reader = threading.Thread(
        target=_reader_thread_main,
        args=(self, reader_stop),
        name="robinhood-production-ws-reader",
        daemon=True,
    )
    setattr(self, "_roi_prod_ws_reader_thread", reader)
    reader.start()
    pending: dict[int, list[dict[str, Any]]] = {}
    processing_generation: int | None = None
    last_settlement = 0.0
    try:
        while not stop.is_set():
            state = _state(self)
            generation = int(state.get("generation", 0) or 0)
            if processing_generation != generation:
                pending.clear()
                processing_generation = generation
                self._caught_up = False
                setattr(self, "_roi_live_epoch_ready", False)
                setattr(self, "_roi_production_transport_block_reason", "robinhood_production_websocket_reanchoring")

            drained = 0
            q = _event_queue(self)
            while drained < PROCESS_BATCH_MAX:
                try:
                    item = q.get_nowait()
                except queue.Empty:
                    break
                drained += 1
                if int(item.get("generation", -1)) != processing_generation:
                    continue
                try:
                    block = int(str(item["log"].get("blockNumber") or "0x0"), 16)
                except (TypeError, ValueError):
                    continue
                pending.setdefault(block, []).append(item)

            state = _state(self)
            head = state.get("head_block")
            if isinstance(head, int):
                self._latest_block = head
                setattr(self, "_roi_live_epoch_cursor", head)
                setattr(self, "_roi_live_epoch_factory_verified_through", head)
                ready = _reader_ready(self)
                self._caught_up = ready  # compatibility shim only; no block-lag authority
                setattr(self, "_roi_live_epoch_ready", ready)
                if ready:
                    setattr(self, "_roi_production_transport_block_reason", None)
                flush_blocks = [block for block in pending if block < head]
                for block in sorted(flush_blocks):
                    items = pending.pop(block)
                    await _process_block(self, items, generation=processing_generation or 0)
                    setattr(self, "_roi_prod_ws_last_processed_block", block)
                    setattr(self, "_roi_prod_ws_last_processed_at", _utcnow())

            now = time.monotonic()
            if now - last_settlement >= SETTLEMENT_INTERVAL_SECONDS:
                await self._settle_open_positions()
                last_settlement = now

            try:
                await asyncio.wait_for(stop.wait(), timeout=PROCESS_SLEEP_SECONDS)
            except asyncio.TimeoutError:
                pass
    finally:
        reader_stop.set()
        await asyncio.to_thread(reader.join, 3.0)
        _update_state(self, connected=False, synchronized=False)
        self._caught_up = False
        setattr(self, "_roi_live_epoch_ready", False)


def _run_wrapper(original: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(self: Any, stop: asyncio.Event) -> None:
        if production_provider_configured():
            await _production_ws_run(self, stop)
            return
        # Public sequencer/RPC remains useful research observation, but the final
        # entry guard below prevents it from becoming decision-authoritative.
        await original(self, stop)

    setattr(wrapped, "_roi_robinhood_production_ws_transport", True)
    setattr(wrapped, "_roi_robinhood_forward_only_run", True)
    return wrapped


def _status_wrapper(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    @wraps(original)
    def wrapped(self: Any) -> dict[str, Any]:
        payload = original(self)
        state = _state(self)
        reader_thread = getattr(self, "_roi_prod_ws_reader_thread", None)
        configured = production_provider_configured()
        reader_ready = _reader_ready(self)
        blocker = None
        if not configured:
            blocker = "robinhood_production_rpc_websocket_required"
        elif not reader_ready:
            blocker = "robinhood_production_websocket_not_ready"
        queue_depth = _event_queue(self).qsize()
        head_age = None
        last_head = state.get("last_head_monotonic")
        if isinstance(last_head, (int, float)):
            head_age = max(0.0, time.monotonic() - float(last_head))

        payload["rpc_endpoint_kind"] = endpoint_kind()
        payload["production_transport_authority"] = {
            "transport_version": TRANSPORT_VERSION,
            "provider_configured": configured,
            "decision_authoritative": bool(configured and reader_ready and not blocker),
            "blocker": blocker,
            "public_rpc_is_research_observation_only": not configured,
            "public_sequencer_is_research_observation_only": not configured,
            "websocket_reader_thread_alive": bool(reader_thread is not None and reader_thread.is_alive()),
            "websocket_connected": bool(state.get("connected")),
            "websocket_synchronized": bool(state.get("synchronized")),
            "websocket_head_block": state.get("head_block"),
            "websocket_head_age_seconds": head_age,
            "websocket_generation": state.get("generation"),
            "websocket_connections": state.get("connections"),
            "websocket_disconnects": state.get("disconnects"),
            "websocket_logs_received": state.get("logs_received"),
            "websocket_queue_depth": queue_depth,
            "websocket_queue_overflows": state.get("queue_overflows"),
            "websocket_last_error_type": state.get("last_error_type"),
            "last_processed_block": getattr(self, "_roi_prod_ws_last_processed_block", None),
            "last_processed_at": getattr(self, "_roi_prod_ws_last_processed_at", None),
            "active_event_age_seconds": getattr(self, "_roi_prod_ws_active_event_age_seconds", None),
            "canonical_latency_hard_max_seconds": canonical_latency_hard_max_seconds(),
            "canonical_latency_source": "strategy_v51_authority.json:execution.latency_hard_max_seconds",
            "legacy_two_block_gate_has_production_authority": False,
            "legacy_two_block_gate_role": "retained_compatibility_and_audit_only",
            "retrospective_entry_authority": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }

        if blocker is not None:
            payload["runtime_ready"] = False
            payload["failed_closed"] = True
            payload["paper_trading_authority"] = False
            payload["caught_up_for_paper_decisions"] = False
            payload["paper_decision_transport_ready"] = False
            payload["forward_frontier_ready"] = False
            payload["error"] = blocker
        else:
            # Preserve unrelated fail-closed failures from lower layers. Only transport
            # fields are asserted here when the production reader itself is healthy.
            lower_failed = bool(payload.get("failed_closed", False)) and str(payload.get("error") or "") not in {
                "robinhood_production_rpc_websocket_required",
                "robinhood_production_websocket_not_ready",
            }
            ready = bool(reader_ready and not lower_failed)
            payload["runtime_ready"] = ready
            payload["paper_trading_authority"] = ready
            payload["caught_up_for_paper_decisions"] = ready
            payload["paper_decision_transport_ready"] = ready
            payload["forward_frontier_ready"] = ready
            if ready:
                payload["failed_closed"] = False
                payload["error"] = None
        return payload

    setattr(wrapped, "_roi_robinhood_production_ws_transport", True)
    return wrapped


def install_robinhood_production_ws_transport(plane_cls: type[Any]) -> None:
    global _INSTALLED, _ORIGINAL_RUN, _ORIGINAL_STATUS
    if _INSTALLED:
        return
    _ORIGINAL_RUN = plane_cls.run
    _ORIGINAL_STATUS = plane_cls.status
    plane_cls.run = _run_wrapper(plane_cls.run)  # type: ignore[method-assign]
    plane_cls.status = _status_wrapper(plane_cls.status)  # type: ignore[method-assign]
    # Existing entry guards resolve this module global dynamically; the final guard
    # now uses production-WS continuity and v5.1's 20-second authority, not block lag.
    frontier._fresh_head_ready = _production_fresh_ready  # type: ignore[assignment]
    setattr(plane_cls, "_roi_robinhood_production_ws_transport_version", TRANSPORT_VERSION)
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "transport_version": TRANSPORT_VERSION,
        "installed": _INSTALLED,
        "endpoint_kind": endpoint_kind(),
        "provider_configured": production_provider_configured(),
        "canonical_latency_hard_max_seconds": canonical_latency_hard_max_seconds(),
        "legacy_two_block_gate_has_production_authority": False,
        "public_transport_can_authorize_paper_entries": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "PUBLIC_SEQUENCER_FEED",
    "TRANSPORT_VERSION",
    "canonical_latency_hard_max_seconds",
    "endpoint_kind",
    "install_robinhood_production_ws_transport",
    "production_provider_configured",
    "status",
]
