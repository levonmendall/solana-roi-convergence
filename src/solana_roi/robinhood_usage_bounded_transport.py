from __future__ import annotations

import asyncio
import json
import queue
import threading
import time
from functools import wraps
from typing import Any, Callable

import websockets

from . import robinhood_chain_runtime as runtime
from . import robinhood_production_ws_transport as transport


USAGE_BOUNDED_VERSION = "robinhood-production-ws-transport-v2-usage-bounded"
SUBSCRIPTION_MODE = "factory_discovery_plus_bounded_market_addresses"
TARGET_REFRESH_SECONDS = 0.25
REORDER_HOLD_SECONDS = 0.25
MAX_TARGET_GAP_BACKFILL_BLOCKS = 64

DISCOVERY_ADDRESSES = frozenset(
    {
        runtime.UNISWAP_V3_FACTORY,
        runtime.PONS_V1_ACTIVE_FACTORY,
        runtime.PONS_V1_LEGACY_FACTORY,
        runtime.PONS_V2_FACTORY,
    }
)
DISCOVERY_TOPICS = frozenset(
    {
        runtime.V3_POOL_CREATED_TOPIC.lower(),
        runtime.PONS_V1_TOKEN_LAUNCHED_TOPIC.lower(),
        runtime.PONS_V2_TOKEN_LAUNCHED_TOPIC.lower(),
    }
)
MARKET_TOPICS = frozenset(
    {
        runtime.V3_SWAP_TOPIC.lower(),
        runtime.PONS_V2_CURVE_BUY_TOPIC.lower(),
        runtime.PONS_V2_CURVE_SELL_TOPIC.lower(),
    }
)

_INSTALLED = False
_BASE_STATUS_WRAPPER: Callable[[Callable[[Any], dict[str, Any]]], Callable[[Any], dict[str, Any]]] | None = None
_BASE_MODULE_STATUS: Callable[[], dict[str, Any]] | None = None


class _TargetSetChanged(RuntimeError):
    """Internal reconnect signal used when the bounded market address set changes."""


def _market_targets(self: Any) -> dict[str, int]:
    """Return a race-tolerant snapshot of only the markets actively observed.

    Durable launch discovery is intentionally separate from this bounded set. The
    existing runtime persists every qualifying factory launch before trimming active
    observation maps, so provider cost cannot narrow factory discovery.
    """
    for _ in range(3):
        try:
            v3 = list(getattr(self, "v3_pools", {}).items())
            v2 = list(getattr(self, "v2_curves", {}).items())
            break
        except RuntimeError:
            continue
    else:
        return {}

    result: dict[str, int] = {}
    for address, subject in v3 + v2:
        clean = runtime._clean_address(address)
        if clean:
            result[clean] = int(getattr(subject, "launch_block", 0) or 0)
    return result


def _subscription_filter(self: Any) -> tuple[dict[str, Any], dict[str, int]]:
    targets = _market_targets(self)
    addresses = sorted(DISCOVERY_ADDRESSES | set(targets))
    topics = sorted(DISCOVERY_TOPICS | MARKET_TOPICS)
    return {"address": addresses, "topics": [topics]}, targets


def _reader_ready(self: Any) -> bool:
    """Production readiness without paying for a global new-head stream.

    WebSocket transport continuity is maintained by the websocket library's ping/pong
    health checks. Entry authority still requires the exact event to be current under
    frozen v5.1's 20-second event-age gate, so quiet chain periods do not require paid
    block-head traffic merely to prove liveness.
    """
    if not transport.production_provider_configured():
        return False
    state = transport._state(self)
    reader = getattr(self, "_roi_prod_ws_reader_thread", None)
    return bool(
        state.get("connected")
        and state.get("synchronized")
        and reader is not None
        and reader.is_alive()
    )


async def _rpc_request(ws: Any, request_id: int, method: str, params: list[Any]) -> Any:
    await ws.send(json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}))
    return await transport._recv_response(ws, request_id)


def _enqueue(self: Any, *, generation: int, log: dict[str, Any], live_authority: bool, source: str) -> None:
    now_mono = time.monotonic()
    item = {
        "generation": generation,
        "received_monotonic": now_mono,
        "received_at": transport._utcnow(),
        "live_authority": bool(live_authority),
        "source": source,
        "log": log,
    }
    try:
        transport._event_queue(self).put_nowait(item)
    except queue.Full as exc:
        transport._bump_state(self, "queue_overflows")
        transport._update_state(
            self,
            connected=False,
            synchronized=False,
            last_error_type="ProductionWsQueueOverflow",
        )
        raise RuntimeError("Robinhood production websocket queue overflow") from exc
    transport._bump_state(self, "logs_received")
    try:
        block = int(str(log.get("blockNumber") or "0x0"), 16)
    except (TypeError, ValueError):
        block = 0
    if block > 0:
        setattr(self, "_roi_usage_last_stream_block", block)


async def _research_only_new_target_backfill(
    self: Any,
    ws: Any,
    *,
    generation: int,
    previous_targets: dict[str, int] | None,
    current_targets: dict[str, int],
    request_id: int,
) -> int:
    """Close only the tiny subscription-change gap without retrospective authority.

    This executes only after the initial connection and only for newly active market
    addresses. Results are persisted as research observations but can never authorize
    a paper entry. The range is hard bounded to avoid turning recovery into another
    provider-usage problem.
    """
    if previous_targets is None:
        return request_id
    new_addresses = sorted(set(current_targets) - set(previous_targets))
    if not new_addresses:
        return request_id

    request_id += 1
    latest_raw = await _rpc_request(ws, request_id, "eth_blockNumber", [])
    latest = int(str(latest_raw or "0x0"), 16)
    launch_blocks = [int(current_targets.get(address, latest) or latest) for address in new_addresses]
    earliest_launch = min(launch_blocks) if launch_blocks else latest
    from_block = max(0, earliest_launch, latest - MAX_TARGET_GAP_BACKFILL_BLOCKS)
    if latest < from_block:
        return request_id

    request_id += 1
    result = await _rpc_request(
        ws,
        request_id,
        "eth_getLogs",
        [
            {
                "fromBlock": hex(from_block),
                "toBlock": hex(latest),
                "address": new_addresses,
                "topics": [sorted(MARKET_TOPICS)],
            }
        ],
    )
    for log in list(result or []):
        if isinstance(log, dict):
            _enqueue(
                self,
                generation=generation,
                log=log,
                live_authority=False,
                source="new_target_gap_backfill_research_only",
            )
    transport._update_state(
        self,
        target_gap_backfill_addresses=len(new_addresses),
        target_gap_backfill_from_block=from_block,
        target_gap_backfill_to_block=latest,
    )
    return request_id


async def _reader_async(self: Any, stop: threading.Event) -> None:
    previous_targets: dict[str, int] | None = None
    while not stop.is_set():
        generation = transport._reader_generation_start(self)
        planned_refresh = False
        try:
            event_filter, targets = _subscription_filter(self)
            async with websockets.connect(
                transport._ws_url(),
                open_timeout=10,
                close_timeout=3,
                ping_interval=10,
                ping_timeout=10,
                max_size=8 * 1024 * 1024,
            ) as ws:
                request_id = 1
                chain_raw = await _rpc_request(ws, request_id, "eth_chainId", [])
                chain_id = int(str(chain_raw), 16)
                if chain_id != runtime.ROBINHOOD_CHAIN_ID:
                    raise RuntimeError(f"wrong Robinhood websocket chain id: {chain_id}")

                request_id = await _research_only_new_target_backfill(
                    self,
                    ws,
                    generation=generation,
                    previous_targets=previous_targets,
                    current_targets=targets,
                    request_id=request_id,
                )
                request_id += 1
                subscription = str(
                    await _rpc_request(ws, request_id, "eth_subscribe", ["logs", event_filter])
                )
                previous_targets = dict(targets)
                setattr(self, "_roi_usage_subscribed_market_addresses", dict(targets))
                transport._bump_state(self, "connections")
                transport._update_state(
                    self,
                    connected=True,
                    synchronized=True,
                    head_block=None,
                    last_head_monotonic=None,
                    last_error_type=None,
                    subscription_mode=SUBSCRIPTION_MODE,
                    global_newheads_subscription=False,
                    chain_wide_log_subscription=False,
                    factory_discovery_address_count=len(DISCOVERY_ADDRESSES),
                    active_market_subscription_address_count=len(targets),
                    total_subscription_address_count=len(event_filter["address"]),
                )

                while not stop.is_set():
                    _, current_targets = _subscription_filter(self)
                    if set(current_targets) != set(targets):
                        planned_refresh = True
                        raise _TargetSetChanged("Robinhood bounded market target set changed")
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=TARGET_REFRESH_SECONDS)
                    except asyncio.TimeoutError:
                        continue
                    payload = json.loads(raw)
                    if payload.get("method") != "eth_subscription":
                        continue
                    params = payload.get("params") or {}
                    if str(params.get("subscription") or "") != subscription:
                        continue
                    log = params.get("result")
                    if not isinstance(log, dict):
                        continue
                    _enqueue(
                        self,
                        generation=generation,
                        log=log,
                        live_authority=True,
                        source="bounded_production_websocket",
                    )
        except asyncio.CancelledError:
            raise
        except _TargetSetChanged:
            transport._update_state(
                self,
                connected=False,
                synchronized=False,
                last_error_type=None,
            )
        except Exception as exc:
            transport._bump_state(self, "disconnects")
            transport._update_state(
                self,
                connected=False,
                synchronized=False,
                last_error_type=type(exc).__name__,
            )
        if not stop.is_set() and not planned_refresh:
            await asyncio.sleep(transport.RECONNECT_SECONDS)


async def _process_block(self: Any, items: list[dict[str, Any]], *, generation: int) -> None:
    items.sort(key=transport._sort_key)
    for item in items:
        log = item["log"]
        address = runtime._clean_address(log.get("address"))
        if address in DISCOVERY_ADDRESSES:
            await self._process_factory_log(log)

    for item in items:
        log = item["log"]
        topics = list(log.get("topics") or ())
        topic0 = str(topics[0]).lower() if topics else ""
        address = runtime._clean_address(log.get("address"))
        if address in DISCOVERY_ADDRESSES:
            continue
        age = max(0.0, time.monotonic() - float(item["received_monotonic"]))
        setattr(self, "_roi_prod_ws_active_event_age_seconds", age)
        setattr(self, "_roi_prod_ws_active_event_generation", int(item.get("generation", -1)))
        setattr(self, "_roi_prod_ws_active_event_received_at", item["received_at"])
        same_generation = int(item.get("generation", -1)) == int(generation)
        live = bool(
            item.get("live_authority", False)
            and same_generation
            and _reader_ready(self)
            and age <= transport.canonical_latency_hard_max_seconds()
        )
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
            if not item.get("live_authority", False):
                reason = "robinhood_research_only_target_gap_backfill"
            elif age > transport.canonical_latency_hard_max_seconds():
                reason = "robinhood_event_exceeded_v51_latency_hard_max"
            else:
                reason = "robinhood_production_websocket_not_ready"
            setattr(self, "_roi_production_transport_block_reason", reason)


async def _production_ws_run(self: Any, stop: asyncio.Event) -> None:
    if not self.enabled:
        return
    reader_stop = threading.Event()
    reader = threading.Thread(
        target=transport._reader_thread_main,
        args=(self, reader_stop),
        name="robinhood-production-ws-reader",
        daemon=True,
    )
    setattr(self, "_roi_prod_ws_reader_thread", reader)
    reader.start()
    pending: list[dict[str, Any]] = []
    last_settlement = 0.0
    try:
        while not stop.is_set():
            current_generation = int(transport._state(self).get("generation", 0) or 0)
            drained = 0
            q = transport._event_queue(self)
            while drained < transport.PROCESS_BATCH_MAX:
                try:
                    pending.append(q.get_nowait())
                except queue.Empty:
                    break
                drained += 1

            now = time.monotonic()
            ready_items: list[dict[str, Any]] = []
            waiting: list[dict[str, Any]] = []
            for item in pending:
                received = float(item.get("received_monotonic", now))
                if now - received >= REORDER_HOLD_SECONDS:
                    ready_items.append(item)
                else:
                    waiting.append(item)
            pending = waiting

            if ready_items:
                by_block: dict[int, list[dict[str, Any]]] = {}
                for item in ready_items:
                    try:
                        block = int(str(item["log"].get("blockNumber") or "0x0"), 16)
                    except (TypeError, ValueError):
                        continue
                    by_block.setdefault(block, []).append(item)
                for block in sorted(by_block):
                    await _process_block(self, by_block[block], generation=current_generation)
                    self._latest_block = block
                    setattr(self, "_roi_live_epoch_cursor", block)
                    setattr(self, "_roi_live_epoch_factory_verified_through", block)
                    setattr(self, "_roi_prod_ws_last_processed_block", block)
                    setattr(self, "_roi_prod_ws_last_processed_at", transport._utcnow())

            ready = _reader_ready(self)
            self._caught_up = ready
            setattr(self, "_roi_live_epoch_ready", ready)
            if ready:
                current_reason = getattr(self, "_roi_production_transport_block_reason", None)
                if current_reason in {
                    "robinhood_production_websocket_reanchoring",
                    "robinhood_production_websocket_not_ready",
                }:
                    setattr(self, "_roi_production_transport_block_reason", None)

            now = time.monotonic()
            if now - last_settlement >= transport.SETTLEMENT_INTERVAL_SECONDS:
                await self._settle_open_positions()
                last_settlement = now

            try:
                await asyncio.wait_for(stop.wait(), timeout=transport.PROCESS_SLEEP_SECONDS)
            except asyncio.TimeoutError:
                pass
    finally:
        reader_stop.set()
        await asyncio.to_thread(reader.join, 3.0)
        transport._update_state(self, connected=False, synchronized=False)
        self._caught_up = False
        setattr(self, "_roi_live_epoch_ready", False)


def _augment_status_wrapper(
    original_factory: Callable[[Callable[[Any], dict[str, Any]]], Callable[[Any], dict[str, Any]]]
) -> Callable[[Callable[[Any], dict[str, Any]]], Callable[[Any], dict[str, Any]]]:
    def factory(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
        wrapped = original_factory(original)

        @wraps(wrapped)
        def bounded_status(self: Any) -> dict[str, Any]:
            payload = wrapped(self)
            authority = payload.setdefault("production_transport_authority", {})
            state = transport._state(self)
            authority.update(
                {
                    "transport_version": USAGE_BOUNDED_VERSION,
                    "subscription_mode": SUBSCRIPTION_MODE,
                    "global_newheads_subscription": False,
                    "chain_wide_log_subscription": False,
                    "factory_discovery_address_count": len(DISCOVERY_ADDRESSES),
                    "active_market_subscription_address_count": int(
                        state.get("active_market_subscription_address_count", 0) or 0
                    ),
                    "total_subscription_address_count": int(
                        state.get("total_subscription_address_count", 0) or 0
                    ),
                    "active_market_subscription_cap": runtime.MAX_TRACKED_V3_POOLS
                    + runtime.MAX_TRACKED_V2_CURVES,
                    "candidate_discovery_constrained_by_active_subscription_cap": False,
                    "candidate_discovery_scope": "all_known_robinhood_factory_addresses",
                    "target_gap_backfill_authority": "research_only_no_paper_entry",
                    "reorder_hold_seconds": REORDER_HOLD_SECONDS,
                    "provider_usage_scales_with": "factory_launches_plus_bounded_tracked_market_activity",
                }
            )
            return payload

        setattr(bounded_status, "_roi_robinhood_usage_bounded_transport", True)
        return bounded_status

    return factory


def _bounded_module_status() -> dict[str, Any]:
    base = dict(_BASE_MODULE_STATUS() if _BASE_MODULE_STATUS is not None else {})
    base.update(
        {
            "transport_version": USAGE_BOUNDED_VERSION,
            "usage_bounded_transport_installed": _INSTALLED,
            "subscription_mode": SUBSCRIPTION_MODE,
            "global_newheads_subscription": False,
            "chain_wide_log_subscription": False,
            "candidate_discovery_constrained_by_active_subscription_cap": False,
            "target_gap_backfill_authority": "research_only_no_paper_entry",
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    )
    return base


def install_robinhood_usage_bounded_transport() -> None:
    """Replace only provider transport mechanics; frozen strategy authority is unchanged."""
    global _INSTALLED, _BASE_STATUS_WRAPPER, _BASE_MODULE_STATUS
    if _INSTALLED:
        return
    if bool(getattr(transport, "_INSTALLED", False)):
        raise RuntimeError("usage-bounded Robinhood transport must install before production WS transport")

    _BASE_STATUS_WRAPPER = transport._status_wrapper
    _BASE_MODULE_STATUS = transport.status
    transport.TRANSPORT_VERSION = USAGE_BOUNDED_VERSION
    transport._reader_ready = _reader_ready
    transport._reader_async = _reader_async
    transport._process_block = _process_block
    transport._production_ws_run = _production_ws_run
    transport._status_wrapper = _augment_status_wrapper(_BASE_STATUS_WRAPPER)
    transport.status = _bounded_module_status
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": USAGE_BOUNDED_VERSION,
        "installed": _INSTALLED,
        "subscription_mode": SUBSCRIPTION_MODE,
        "factory_discovery_addresses": sorted(DISCOVERY_ADDRESSES),
        "global_newheads_subscription": False,
        "chain_wide_log_subscription": False,
        "candidate_discovery_constrained_by_active_subscription_cap": False,
        "target_gap_backfill_authority": "research_only_no_paper_entry",
        "canonical_latency_hard_max_seconds": transport.canonical_latency_hard_max_seconds(),
        "legacy_two_block_gate_has_production_authority": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "DISCOVERY_ADDRESSES",
    "DISCOVERY_TOPICS",
    "MARKET_TOPICS",
    "SUBSCRIPTION_MODE",
    "USAGE_BOUNDED_VERSION",
    "install_robinhood_usage_bounded_transport",
    "status",
]
