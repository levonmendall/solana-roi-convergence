from __future__ import annotations

import asyncio
import math
import os
import threading
import time
from collections import deque
from functools import wraps
from typing import Any, Callable

from . import robinhood_chain_runtime as runtime
from . import robinhood_production_ws_transport as transport
from . import robinhood_usage_bounded_transport as bounded


BUDGET_VERSION = "robinhood-production-ws-transport-v3-prospective-budget"
SUBSCRIPTION_MODE = "factory_discovery_plus_research_promoted_live_shortlist"
DEFAULT_LIVE_MARKET_CAP = 8
MAX_LIVE_MARKET_CAP = 16
RESEARCH_POLL_SECONDS = 5.0
RESEARCH_STALE_SECONDS = 20.0
RESEARCH_EVENT_WINDOW_SECONDS = 90.0
RESEARCH_BATCH_SIZE = 64
RESEARCH_MAX_BLOCKS_PER_PASS = 200
FREE_PLAN_TARGET_CU_PER_MINUTE = 600

_INSTALLED = False
_BASE_AUGMENT_STATUS_WRAPPER: Callable[..., Any] | None = None
_BASE_BOUNDED_STATUS: Callable[[], dict[str, Any]] | None = None


def _live_market_cap() -> int:
    raw = os.getenv("ROBINHOOD_ALCHEMY_LIVE_MARKET_CAP", str(DEFAULT_LIVE_MARKET_CAP))
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = DEFAULT_LIVE_MARKET_CAP
    return max(1, min(MAX_LIVE_MARKET_CAP, value))


def _research_lock(self: Any) -> threading.Lock:
    lock = getattr(self, "_roi_research_screen_lock", None)
    if lock is None:
        lock = threading.Lock()
        setattr(self, "_roi_research_screen_lock", lock)
    return lock


def _research_events(self: Any) -> dict[str, deque[dict[str, Any]]]:
    lock = _research_lock(self)
    with lock:
        value = getattr(self, "_roi_research_screen_events", None)
        if not isinstance(value, dict):
            value = {}
            setattr(self, "_roi_research_screen_events", value)
        return value


def _research_state(self: Any) -> dict[str, Any]:
    lock = _research_lock(self)
    with lock:
        value = getattr(self, "_roi_research_screen_state", None)
        if not isinstance(value, dict):
            value = {
                "ready": False,
                "last_success_monotonic": None,
                "last_success_at": None,
                "last_error_type": None,
                "cursor_block": None,
                "passes": 0,
                "rpc_failures": 0,
                "logs_seen": 0,
                "universe_size": 0,
            }
            setattr(self, "_roi_research_screen_state", value)
        return dict(value)


def _update_research_state(self: Any, **updates: Any) -> None:
    lock = _research_lock(self)
    with lock:
        value = getattr(self, "_roi_research_screen_state", None)
        if not isinstance(value, dict):
            value = {}
            setattr(self, "_roi_research_screen_state", value)
        value.update(updates)


def _research_ready(self: Any) -> bool:
    state = _research_state(self)
    last = state.get("last_success_monotonic")
    return bool(
        state.get("ready")
        and isinstance(last, (int, float))
        and time.monotonic() - float(last) <= RESEARCH_STALE_SECONDS
    )


def _candidate_universe(self: Any) -> dict[str, dict[str, Any]]:
    """All persisted, paper-eligible markets in the current release cohort.

    The durable launch table is the candidate universe; the in-memory 64/64 tracking
    maps are only caches. Provider budgeting therefore cannot erase a discovered
    candidate from broad research screening.
    """
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT protocol,venue,lifecycle,token,pool,curve,deployer,pair_token,fee,launch_block,"
            "restrictions_end_block,graduation_threshold FROM robinhood_launches "
            "WHERE release_commit=? AND paper_eligible=1 ORDER BY id",
            (self.release_commit,),
        ).fetchall()
    result: dict[str, dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        pool = runtime._clean_address(row.get("pool"))
        curve = runtime._clean_address(row.get("curve"))
        address = pool or curve
        if not address:
            continue
        result[address] = {
            "address": address,
            "kind": "v3" if pool else "v2",
            "protocol": str(row.get("protocol") or ""),
            "venue": str(row.get("venue") or ""),
            "lifecycle": str(row.get("lifecycle") or ""),
            "token": runtime._clean_address(row.get("token")),
            "deployer": runtime._clean_address(row.get("deployer")),
            "pair_token": runtime._clean_address(row.get("pair_token")),
            "fee": int(row.get("fee") or 10_000),
            "launch_block": int(row.get("launch_block") or 0),
            "restrictions_end_block": int(row.get("restrictions_end_block") or 0),
            "graduation_threshold": int(row.get("graduation_threshold") or 0),
        }
    return result


def _open_market_addresses(self: Any) -> set[str]:
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT DISTINCT t.market FROM robinhood_paper_trials t "
            "LEFT JOIN robinhood_paper_outcomes o ON o.trial_id=t.id "
            "WHERE t.release_commit=? AND o.id IS NULL",
            (self.release_commit,),
        ).fetchall()
    return {
        address
        for raw in rows
        if (address := runtime._clean_address(raw["market"]))
    }


def _ensure_runtime_market(self: Any, descriptor: dict[str, Any]) -> None:
    address = descriptor["address"]
    token = descriptor["token"]
    if not token:
        return
    if descriptor["kind"] == "v3":
        if address in getattr(self, "v3_pools", {}):
            return
        token0, token1 = sorted([runtime.WETH, token], key=lambda item: int(item, 16))
        self.v3_pools[address] = runtime.V3Pool(
            token=token,
            pool=address,
            token0=token0,
            token1=token1,
            fee=int(descriptor["fee"]),
            token_decimals=18,
            venue=str(descriptor["venue"] or "UNISWAP_V3_DIRECT"),
            lifecycle=str(descriptor["lifecycle"] or "new_weth_pool"),
            deployer=str(descriptor["deployer"] or ""),
            launch_block=int(descriptor["launch_block"]),
            restrictions_end_block=int(descriptor["restrictions_end_block"]),
        )
    else:
        if address in getattr(self, "v2_curves", {}):
            return
        self.v2_curves[address] = runtime.V2Curve(
            token=token,
            curve=address,
            deployer=str(descriptor["deployer"] or ""),
            pair_token=str(descriptor["pair_token"] or ""),
            launch_config_id=0,
            graduation_threshold=int(descriptor["graduation_threshold"]),
            launch_block=int(descriptor["launch_block"]),
        )


def _record_research_event(
    self: Any,
    *,
    address: str,
    side: str,
    quote_amount: int,
    actor: str,
) -> None:
    now = time.monotonic()
    lock = _research_lock(self)
    with lock:
        events = getattr(self, "_roi_research_screen_events", None)
        if not isinstance(events, dict):
            events = {}
            setattr(self, "_roi_research_screen_events", events)
        q = events.get(address)
        if not isinstance(q, deque):
            q = deque(maxlen=256)
            events[address] = q
        q.append(
            {
                "at": now,
                "side": side,
                "quote_amount": max(0, int(quote_amount)),
                "actor": runtime._clean_address(actor),
            }
        )


def _research_log_signal(descriptor: dict[str, Any], log: dict[str, Any]) -> tuple[str, int, str] | None:
    topics = [str(t).lower() for t in (log.get("topics") or [])]
    if not topics:
        return None
    words = runtime._words(str(log.get("data") or ""))
    if descriptor["kind"] == "v3":
        if topics[0] != runtime.V3_SWAP_TOPIC.lower() or len(topics) < 3 or len(words) < 2:
            return None
        amount0 = runtime._signed(words[0])
        amount1 = runtime._signed(words[1])
        weth_is_token0 = int(runtime.WETH, 16) < int(descriptor["token"], 16)
        quote = amount0 if weth_is_token0 else amount1
        token_amount = amount1 if weth_is_token0 else amount0
        if quote > 0 and token_amount < 0:
            side = "buy"
        elif quote < 0 and token_amount > 0:
            side = "sell"
        else:
            return None
        return side, abs(int(quote)), runtime._topic_address(topics[2])

    if len(topics) < 2 or len(words) < 2:
        return None
    if topics[0] == runtime.PONS_V2_CURVE_BUY_TOPIC.lower():
        return "buy", runtime._uint(words[0]), runtime._topic_address(topics[1])
    if topics[0] == runtime.PONS_V2_CURVE_SELL_TOPIC.lower():
        return "sell", runtime._uint(words[1]), runtime._topic_address(topics[1])
    return None


def _research_rankings(self: Any, universe: dict[str, dict[str, Any]]) -> list[tuple[str, float, str]]:
    now = time.monotonic()
    lock = _research_lock(self)
    ranked: list[tuple[str, float, str]] = []
    with lock:
        events_by_market = getattr(self, "_roi_research_screen_events", {})
        for address, descriptor in universe.items():
            events = list(events_by_market.get(address) or ())
            recent = [e for e in events if now - float(e["at"]) <= RESEARCH_EVENT_WINDOW_SECONDS]
            if not recent:
                continue
            buys = [e for e in recent if e["side"] == "buy"]
            if not buys:
                continue
            sells = [e for e in recent if e["side"] == "sell"]
            actors = {e["actor"] for e in buys if e.get("actor")}
            buy_quote = sum(int(e["quote_amount"]) for e in buys)
            sell_quote = sum(int(e["quote_amount"]) for e in sells)
            creator_buy = any(e.get("actor") == descriptor.get("deployer") for e in buys if descriptor.get("deployer"))
            last_age = max(0.0, now - max(float(e["at"]) for e in recent))
            flow_ratio = buy_quote / max(1, sell_quote)
            # This score allocates provider capacity only. It has no economic or
            # paper-entry authority; the existing v5.1 lane/risk/quote gates run later
            # using fresh Alchemy events only.
            score = (
                (1000.0 if creator_buy else 0.0)
                + len(actors) * 100.0
                + len(buys) * 25.0
                + min(100.0, math.log10(max(1, buy_quote)) * 4.0)
                + min(100.0, flow_ratio * 10.0)
                + max(0.0, 90.0 - last_age)
            )
            reason = "creator_buy" if creator_buy else "prospective_buy_flow"
            ranked.append((address, score, reason))
    ranked.sort(key=lambda item: (item[1], item[0]), reverse=True)
    return ranked


def _selected_market_targets(self: Any) -> tuple[dict[str, int], dict[str, str]]:
    universe = _candidate_universe(self)
    open_markets = _open_market_addresses(self)
    selected: dict[str, int] = {}
    reasons: dict[str, str] = {}

    for address in sorted(open_markets):
        descriptor = universe.get(address)
        if descriptor is None:
            continue
        _ensure_runtime_market(self, descriptor)
        selected[address] = int(descriptor["launch_block"])
        reasons[address] = "open_position_forced_live"

    available = max(0, _live_market_cap() - len(selected))
    if available:
        for address, _score, reason in _research_rankings(self, universe):
            if address in selected:
                continue
            descriptor = universe.get(address)
            if descriptor is None:
                continue
            _ensure_runtime_market(self, descriptor)
            selected[address] = int(descriptor["launch_block"])
            reasons[address] = reason
            if len([a for a in selected if a not in open_markets]) >= available:
                break

    _update_research_state(self, universe_size=len(universe))
    setattr(self, "_roi_budget_live_target_reasons", reasons)
    setattr(self, "_roi_budget_live_targets", dict(selected))
    return selected, reasons


def _subscription_filter(self: Any) -> tuple[dict[str, Any], dict[str, int]]:
    targets, _ = _selected_market_targets(self)
    addresses = sorted(bounded.DISCOVERY_ADDRESSES | set(targets))
    topics = sorted(bounded.DISCOVERY_TOPICS | bounded.MARKET_TOPICS)
    return {"address": addresses, "topics": [topics]}, targets


def _reader_ready(self: Any) -> bool:
    if not transport.production_provider_configured() or not _research_ready(self):
        return False
    state = transport._state(self)
    reader = getattr(self, "_roi_prod_ws_reader_thread", None)
    return bool(
        state.get("connected")
        and state.get("synchronized")
        and reader is not None
        and reader.is_alive()
    )


async def _no_alchemy_gap_backfill(
    self: Any,
    ws: Any,
    *,
    generation: int,
    previous_targets: dict[str, int] | None,
    current_targets: dict[str, int],
    request_id: int,
) -> int:
    # The public research plane already covers the pre-subscription interval. Those
    # observations never receive entry authority. Prospective paper authority begins
    # only with subsequent messages from the live Alchemy subscription.
    if previous_targets is not None:
        new_addresses = sorted(set(current_targets) - set(previous_targets))
        if new_addresses:
            transport._update_state(
                self,
                target_gap_backfill_addresses=0,
                target_gap_backfill_authority="public_research_plane_only",
                prospective_live_targets_added=len(new_addresses),
            )
    return request_id


def _chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


async def _research_pass(self: Any, rpc: runtime.RobinhoodRpc) -> None:
    universe = _candidate_universe(self)
    latest = await rpc.block_number()
    state = _research_state(self)
    cursor = state.get("cursor_block")
    if not isinstance(cursor, int):
        # Prospective-only startup: establish a head and begin screening from here.
        _update_research_state(
            self,
            ready=True,
            cursor_block=latest,
            last_success_monotonic=time.monotonic(),
            last_success_at=transport._utcnow(),
            last_error_type=None,
            universe_size=len(universe),
            passes=int(state.get("passes", 0) or 0) + 1,
        )
        return
    if latest <= cursor:
        _update_research_state(
            self,
            ready=True,
            last_success_monotonic=time.monotonic(),
            last_success_at=transport._utcnow(),
            last_error_type=None,
            universe_size=len(universe),
            passes=int(state.get("passes", 0) or 0) + 1,
        )
        return

    to_block = min(latest, cursor + RESEARCH_MAX_BLOCKS_PER_PASS)
    v3 = [a for a, d in universe.items() if d["kind"] == "v3"]
    v2 = [a for a, d in universe.items() if d["kind"] == "v2"]
    logs: list[dict[str, Any]] = []
    for batch in _chunks(v3, RESEARCH_BATCH_SIZE):
        logs.extend(
            await rpc.get_logs(
                from_block=cursor + 1,
                to_block=to_block,
                addresses=batch,
                topics=[runtime.V3_SWAP_TOPIC],
            )
        )
    for batch in _chunks(v2, RESEARCH_BATCH_SIZE):
        logs.extend(
            await rpc.get_logs(
                from_block=cursor + 1,
                to_block=to_block,
                addresses=batch,
                topics=[[runtime.PONS_V2_CURVE_BUY_TOPIC, runtime.PONS_V2_CURVE_SELL_TOPIC]],
            )
        )

    for log in logs:
        address = runtime._clean_address(log.get("address"))
        descriptor = universe.get(address)
        if descriptor is None:
            continue
        signal = _research_log_signal(descriptor, log)
        if signal is None:
            continue
        side, quote, actor = signal
        _record_research_event(self, address=address, side=side, quote_amount=quote, actor=actor)

    _update_research_state(
        self,
        ready=True,
        cursor_block=to_block,
        last_success_monotonic=time.monotonic(),
        last_success_at=transport._utcnow(),
        last_error_type=None,
        universe_size=len(universe),
        logs_seen=int(state.get("logs_seen", 0) or 0) + len(logs),
        passes=int(state.get("passes", 0) or 0) + 1,
    )


async def _research_async(self: Any, stop: threading.Event) -> None:
    rpc = runtime.RobinhoodRpc(rpc_url=runtime.ROBINHOOD_PUBLIC_RPC, timeout_seconds=3.0)
    try:
        while not stop.is_set():
            try:
                await _research_pass(self, rpc)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                state = _research_state(self)
                _update_research_state(
                    self,
                    ready=False,
                    last_error_type=type(exc).__name__,
                    rpc_failures=int(state.get("rpc_failures", 0) or 0) + 1,
                )
            if not stop.is_set():
                await asyncio.sleep(RESEARCH_POLL_SECONDS)
    finally:
        await rpc.close()


def _research_thread_main(self: Any, stop: threading.Event) -> None:
    try:
        asyncio.run(_research_async(self, stop))
    except BaseException as exc:
        _update_research_state(self, ready=False, last_error_type=type(exc).__name__)


def _production_ws_run(original: Callable[[Any, asyncio.Event], Any]) -> Callable[[Any, asyncio.Event], Any]:
    @wraps(original)
    async def wrapped(self: Any, stop: asyncio.Event) -> None:
        if not self.enabled:
            return
        research_stop = threading.Event()
        research = threading.Thread(
            target=_research_thread_main,
            args=(self, research_stop),
            name="robinhood-public-research-screener",
            daemon=True,
        )
        setattr(self, "_roi_research_screen_thread", research)
        research.start()
        try:
            await original(self, stop)
        finally:
            research_stop.set()
            await asyncio.to_thread(research.join, 3.0)
            _update_research_state(self, ready=False)

    setattr(wrapped, "_roi_robinhood_provider_budget_transport", True)
    return wrapped


def _augment_status_wrapper(original_factory: Callable[..., Any]) -> Callable[..., Any]:
    def factory(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
        wrapped = original_factory(original)

        @wraps(wrapped)
        def budget_status(self: Any) -> dict[str, Any]:
            payload = wrapped(self)
            authority = payload.setdefault("production_transport_authority", {})
            research = _research_state(self)
            selected = dict(getattr(self, "_roi_budget_live_targets", {}) or {})
            reasons = dict(getattr(self, "_roi_budget_live_target_reasons", {}) or {})
            open_markets = _open_market_addresses(self)
            research_thread = getattr(self, "_roi_research_screen_thread", None)
            authority.update(
                {
                    "transport_version": BUDGET_VERSION,
                    "subscription_mode": SUBSCRIPTION_MODE,
                    "global_newheads_subscription": False,
                    "chain_wide_log_subscription": False,
                    "alchemy_live_market_cap": _live_market_cap(),
                    "alchemy_live_market_count": len(selected),
                    "open_positions_forced_live_count": len(open_markets & set(selected)),
                    "live_market_reasons": reasons,
                    "candidate_discovery_constrained_by_live_cap": False,
                    "research_screening_scope": "all_persisted_paper_eligible_markets_current_release",
                    "research_screening_ready": _research_ready(self),
                    "research_screening_thread_alive": bool(research_thread is not None and research_thread.is_alive()),
                    "research_screening_universe_size": int(research.get("universe_size", 0) or 0),
                    "research_screening_last_success_at": research.get("last_success_at"),
                    "research_screening_last_error_type": research.get("last_error_type"),
                    "research_screening_rpc_failures": int(research.get("rpc_failures", 0) or 0),
                    "research_screening_logs_seen": int(research.get("logs_seen", 0) or 0),
                    "research_transport_authority": "promotion_only_no_paper_entry",
                    "promotion_to_alchemy_authority": "prospective_next_live_event_only",
                    "alchemy_gap_backfill_enabled": False,
                    "provider_budget_target_cu_per_minute": FREE_PLAN_TARGET_CU_PER_MINUTE,
                    "provider_budget_changes_strategy_economics": False,
                }
            )
            if transport.production_provider_configured() and not _research_ready(self):
                authority["decision_authoritative"] = False
                authority["blocker"] = "robinhood_broad_research_screening_not_ready"
            return payload

        setattr(budget_status, "_roi_robinhood_provider_budget_status", True)
        return budget_status

    return factory


def _budget_module_status() -> dict[str, Any]:
    base = dict(_BASE_BOUNDED_STATUS() if _BASE_BOUNDED_STATUS is not None else {})
    base.update(
        {
            "transport_version": BUDGET_VERSION,
            "provider_budget_transport_installed": _INSTALLED,
            "subscription_mode": SUBSCRIPTION_MODE,
            "global_newheads_subscription": False,
            "chain_wide_log_subscription": False,
            "alchemy_live_market_cap": _live_market_cap(),
            "candidate_discovery_constrained_by_live_cap": False,
            "research_transport_authority": "promotion_only_no_paper_entry",
            "promotion_to_alchemy_authority": "prospective_next_live_event_only",
            "alchemy_gap_backfill_enabled": False,
            "provider_budget_target_cu_per_minute": FREE_PLAN_TARGET_CU_PER_MINUTE,
            "canonical_latency_hard_max_seconds": transport.canonical_latency_hard_max_seconds(),
            "legacy_two_block_gate_has_production_authority": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    )
    return base


def install_robinhood_provider_budget_transport() -> None:
    """Patch provider acquisition before the v2 bounded transport installs on the plane."""
    global _INSTALLED, _BASE_AUGMENT_STATUS_WRAPPER, _BASE_BOUNDED_STATUS
    if _INSTALLED:
        return
    if bool(getattr(bounded, "_INSTALLED", False)):
        raise RuntimeError("provider-budget transport must install before bounded transport")

    _BASE_AUGMENT_STATUS_WRAPPER = bounded._augment_status_wrapper
    _BASE_BOUNDED_STATUS = bounded.status
    bounded.USAGE_BOUNDED_VERSION = BUDGET_VERSION
    bounded.SUBSCRIPTION_MODE = SUBSCRIPTION_MODE
    bounded._market_targets = lambda self: _selected_market_targets(self)[0]
    bounded._subscription_filter = _subscription_filter
    bounded._reader_ready = _reader_ready
    bounded._research_only_new_target_backfill = _no_alchemy_gap_backfill
    bounded._production_ws_run = _production_ws_run(bounded._production_ws_run)
    bounded._augment_status_wrapper = _augment_status_wrapper(_BASE_AUGMENT_STATUS_WRAPPER)
    bounded.status = _budget_module_status
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": BUDGET_VERSION,
        "installed": _INSTALLED,
        "subscription_mode": SUBSCRIPTION_MODE,
        "alchemy_live_market_cap": _live_market_cap(),
        "candidate_discovery_constrained_by_live_cap": False,
        "research_transport_authority": "promotion_only_no_paper_entry",
        "promotion_to_alchemy_authority": "prospective_next_live_event_only",
        "alchemy_gap_backfill_enabled": False,
        "provider_budget_target_cu_per_minute": FREE_PLAN_TARGET_CU_PER_MINUTE,
        "canonical_latency_hard_max_seconds": transport.canonical_latency_hard_max_seconds(),
        "legacy_two_block_gate_has_production_authority": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "BUDGET_VERSION",
    "SUBSCRIPTION_MODE",
    "install_robinhood_provider_budget_transport",
    "status",
]
