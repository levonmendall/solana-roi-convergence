from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any

from . import robinhood_catchup_capacity_repair as catchup
from . import robinhood_chain_runtime as runtime
from . import robinhood_live_frontier_verification_repair as frontier


OBSERVATION_VERSION = "robinhood-uniswap-v2-v4-observation-v1"
UNISWAP_V2_FACTORY = "0x8bceaa40b9acdfaedf85adf4ff01f5ad6517937f"
UNISWAP_V2_PAIR_CREATED_TOPIC = (
    "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
)
UNISWAP_V2_SWAP_TOPIC = (
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
)
UNISWAP_V4_INITIALIZE_TOPIC = (
    "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438"
)
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

DEFAULT_MAX_RECOVERY_BLOCKS = 64
DEFAULT_MAX_V2_PAIRS = 255
DEFAULT_MAX_V4_POOLS = 1024
DEFAULT_V2_ADDRESS_BATCH = 64
MAX_V2_ADDRESS_BATCH = 64

_STATE_CURSOR = "robinhood_v2_v4_observation_cursor"
_STATE_V2 = "robinhood_v2_observation_pairs"
_STATE_V4 = "robinhood_v4_observation_pools"

_ORIGINAL_FETCH: Callable[..., Awaitable[list[tuple[str, Any, dict[str, Any]]]]] | None = None
_ORIGINAL_STATUS: Callable[[Any], dict[str, Any]] | None = None


def _enabled() -> bool:
    return os.getenv("ROBINHOOD_V2_V4_OBSERVATION_ENABLED", "true").strip().lower() not in {
        "0",
        "false",
        "no",
    }


def _bounded_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def _max_recovery_blocks() -> int:
    return _bounded_int(
        "ROBINHOOD_V2_V4_MAX_RECOVERY_BLOCKS",
        DEFAULT_MAX_RECOVERY_BLOCKS,
        minimum=1,
        maximum=256,
    )


def _max_v2_pairs() -> int:
    return _bounded_int(
        "ROBINHOOD_V2_OBSERVATION_MAX_PAIRS",
        DEFAULT_MAX_V2_PAIRS,
        minimum=1,
        maximum=1024,
    )


def _max_v4_pools() -> int:
    return _bounded_int(
        "ROBINHOOD_V4_OBSERVATION_MAX_POOLS",
        DEFAULT_MAX_V4_POOLS,
        minimum=1,
        maximum=4096,
    )


def _v2_address_batch() -> int:
    return _bounded_int(
        "ROBINHOOD_V2_OBSERVATION_ADDRESS_BATCH",
        DEFAULT_V2_ADDRESS_BATCH,
        minimum=1,
        maximum=MAX_V2_ADDRESS_BATCH,
    )


def _state_json(self: Any, key: str, default: Any) -> Any:
    try:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT value FROM robinhood_chain_state WHERE key=?",
                (key,),
            ).fetchone()
        if row is None:
            return default
        return json.loads(str(row["value"]))
    except Exception:
        return default


def _set_state_json(self: Any, key: str, value: Any) -> None:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    now = runtime._utcnow()
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "INSERT INTO robinhood_chain_state(key,value,updated_at) VALUES (?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
            (key, raw, now),
        )


def _ensure_state(self: Any) -> None:
    if bool(getattr(self, "_roi_v2v4_observation_initialized", False)):
        return
    raw_v2 = _state_json(self, _STATE_V2, {})
    raw_v4 = _state_json(self, _STATE_V4, {})
    cursor = _state_json(self, _STATE_CURSOR, None)
    self._roi_v2_observation_pairs = raw_v2 if isinstance(raw_v2, dict) else {}
    self._roi_v4_observation_pools = raw_v4 if isinstance(raw_v4, dict) else {}
    try:
        self._roi_v2v4_observation_cursor = int(cursor) if cursor is not None else None
    except (TypeError, ValueError):
        self._roi_v2v4_observation_cursor = None
    self._roi_v2v4_observation_metrics = {
        "ranges_completed": 0,
        "ranges_failed": 0,
        "ranges_reanchored": 0,
        "blocks_intentionally_not_backfilled": 0,
        "discovery_requests": 0,
        "market_requests": 0,
        "v2_pairs_discovered": 0,
        "v4_pools_discovered": 0,
        "v2_swaps_persisted": 0,
        "v4_swaps_persisted": 0,
        "duplicates_ignored": 0,
        "parse_failures": 0,
        "untracked_v4_swaps_ignored": 0,
    }
    self._roi_v2v4_observation_last_error = None
    self._roi_v2v4_observation_last_success_at = None
    self._roi_v2v4_observation_last_range = None
    self._roi_v2v4_observation_initialized = True
    _trim_registries(self, persist=False)


def _metric(self: Any, name: str, amount: int = 1) -> None:
    _ensure_state(self)
    metrics = self._roi_v2v4_observation_metrics
    metrics[name] = int(metrics.get(name, 0) or 0) + int(amount)


def _ordered_trim(registry: dict[str, dict[str, Any]], limit: int) -> dict[str, dict[str, Any]]:
    if len(registry) <= limit:
        return registry
    keep = sorted(
        registry.items(),
        key=lambda item: (
            int((item[1] or {}).get("launch_block") or 0),
            str(item[0]),
        ),
        reverse=True,
    )[:limit]
    return dict(keep)


def _trim_registries(self: Any, *, persist: bool) -> None:
    self._roi_v2_observation_pairs = _ordered_trim(
        dict(self._roi_v2_observation_pairs),
        _max_v2_pairs(),
    )
    self._roi_v4_observation_pools = _ordered_trim(
        dict(self._roi_v4_observation_pools),
        _max_v4_pools(),
    )
    if persist:
        _set_state_json(self, _STATE_V2, self._roi_v2_observation_pairs)
        _set_state_json(self, _STATE_V4, self._roi_v4_observation_pools)


def _block(log: dict[str, Any]) -> int:
    return int(str(log.get("blockNumber") or "0x0"), 16)


def _log_index(log: dict[str, Any]) -> int:
    return int(str(log.get("logIndex") or "0x0"), 16)


def _tx_index(log: dict[str, Any]) -> int:
    return int(str(log.get("transactionIndex") or "0x0"), 16)


def _event_key(log: dict[str, Any]) -> tuple[int, int, int]:
    return (_block(log), _tx_index(log), _log_index(log))


def _pool_id(topic: Any) -> str:
    raw = str(topic or "").lower()
    return raw if raw.startswith("0x") and len(raw) == 66 else ""


def _decode_v2_pair(log: dict[str, Any]) -> dict[str, Any] | None:
    topics = [str(topic).lower() for topic in (log.get("topics") or [])]
    words = runtime._words(str(log.get("data") or ""))
    if len(topics) < 3 or topics[0] != UNISWAP_V2_PAIR_CREATED_TOPIC or len(words) < 1:
        return None
    token0 = runtime._topic_address(topics[1])
    token1 = runtime._topic_address(topics[2])
    pair = runtime._word_address(words[0])
    if not token0 or not token1 or not pair:
        return None
    if runtime.WETH not in {token0, token1}:
        return None
    token = token1 if token0 == runtime.WETH else token0
    return {
        "chain": "ROBINHOOD_CHAIN",
        "protocol": "uniswap_v2",
        "venue": "UNISWAP_V2",
        "lifecycle": "observation_only_weth_pair",
        "token": token,
        "token0": token0,
        "token1": token1,
        "quote_asset": runtime.WETH,
        "market": pair,
        "pair": pair,
        "launch_block": _block(log),
        "tx_hash": str(log.get("transactionHash") or ""),
        "log_index": _log_index(log),
        "paper_eligible": False,
        "analyzable": True,
        "execution_authorized": False,
    }


def _decode_v4_pool(log: dict[str, Any]) -> dict[str, Any] | None:
    topics = [str(topic).lower() for topic in (log.get("topics") or [])]
    words = runtime._words(str(log.get("data") or ""))
    if len(topics) < 4 or topics[0] != UNISWAP_V4_INITIALIZE_TOPIC or len(words) < 5:
        return None
    pool = _pool_id(topics[1])
    currency0 = runtime._topic_address(topics[2])
    currency1 = runtime._topic_address(topics[3])
    if not pool or not currency0 or not currency1:
        return None
    quote_assets = {ZERO_ADDRESS, runtime.WETH}
    if currency0 not in quote_assets and currency1 not in quote_assets:
        return None
    if currency0 in quote_assets and currency1 in quote_assets:
        return None
    quote_asset = currency0 if currency0 in quote_assets else currency1
    token = currency1 if quote_asset == currency0 else currency0
    return {
        "chain": "ROBINHOOD_CHAIN",
        "protocol": "uniswap_v4",
        "venue": "UNISWAP_V4",
        "lifecycle": "observation_only_quote_pool",
        "token": token,
        "currency0": currency0,
        "currency1": currency1,
        "quote_asset": quote_asset,
        "market": pool,
        "pool_id": pool,
        "fee": runtime._uint(words[0]),
        "tick_spacing": runtime._signed(words[1], 24),
        "hooks": runtime._word_address(words[2]),
        "sqrt_price_x96": runtime._uint(words[3]),
        "tick": runtime._signed(words[4], 24),
        "launch_block": _block(log),
        "tx_hash": str(log.get("transactionHash") or ""),
        "log_index": _log_index(log),
        "paper_eligible": False,
        "analyzable": True,
        "execution_authorized": False,
    }


def _persist_market(self: Any, market: dict[str, Any], *, observed_at: str) -> None:
    self.store.append(
        "robinhood_market_observation",
        observed_at,
        {
            **market,
            "observation_version": OBSERVATION_VERSION,
            "source": "eth_getLogs",
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        },
    )


def _register_discovery(self: Any, log: dict[str, Any], *, observed_at: str) -> bool:
    address = runtime._clean_address(log.get("address"))
    topics = [str(topic).lower() for topic in (log.get("topics") or [])]
    if not topics:
        return False
    if address == UNISWAP_V2_FACTORY and topics[0] == UNISWAP_V2_PAIR_CREATED_TOPIC:
        market = _decode_v2_pair(log)
        if market is None:
            return False
        key = str(market["pair"])
        if key in self._roi_v2_observation_pairs:
            return False
        self._roi_v2_observation_pairs[key] = market
        _metric(self, "v2_pairs_discovered")
        _persist_market(self, market, observed_at=observed_at)
        return True
    if address == runtime.UNISWAP_V4_POOL_MANAGER and topics[0] == UNISWAP_V4_INITIALIZE_TOPIC:
        market = _decode_v4_pool(log)
        if market is None:
            return False
        key = str(market["pool_id"])
        if key in self._roi_v4_observation_pools:
            return False
        self._roi_v4_observation_pools[key] = market
        _metric(self, "v4_pools_discovered")
        _persist_market(self, market, observed_at=observed_at)
        return True
    return False


def _v2_swap_payload(meta: dict[str, Any], log: dict[str, Any]) -> dict[str, Any] | None:
    topics = [str(topic).lower() for topic in (log.get("topics") or [])]
    words = runtime._words(str(log.get("data") or ""))
    if len(topics) < 3 or topics[0] != UNISWAP_V2_SWAP_TOPIC or len(words) < 4:
        return None
    amount0_in, amount1_in, amount0_out, amount1_out = map(runtime._uint, words[:4])
    delta0 = amount0_in - amount0_out
    delta1 = amount1_in - amount1_out
    if delta0 == 0 or delta1 == 0:
        return None
    quote_is_0 = str(meta.get("token0")) == runtime.WETH
    quote_delta = delta0 if quote_is_0 else delta1
    token_delta = delta1 if quote_is_0 else delta0
    if quote_delta > 0 and token_delta < 0:
        side = "buy"
    elif quote_delta < 0 and token_delta > 0:
        side = "sell"
    else:
        return None
    return {
        "venue": "UNISWAP_V2",
        "lifecycle": "observation_only_weth_pair",
        "token": str(meta["token"]),
        "market": str(meta["pair"]),
        "tx_hash": str(log.get("transactionHash") or ""),
        "log_index": _log_index(log),
        "block_number": _block(log),
        "actor": runtime._topic_address(topics[2]),
        "actor_source": "v2_swap_recipient",
        "side": side,
        "quote_amount_wei": abs(quote_delta),
        "token_amount_raw": abs(token_delta),
        "price_eth": None,
        "raw": {
            "amount0_in": amount0_in,
            "amount1_in": amount1_in,
            "amount0_out": amount0_out,
            "amount1_out": amount1_out,
        },
    }


def _v4_swap_payload(meta: dict[str, Any], log: dict[str, Any]) -> dict[str, Any] | None:
    topics = [str(topic).lower() for topic in (log.get("topics") or [])]
    words = runtime._words(str(log.get("data") or ""))
    if len(topics) < 3 or topics[0] != runtime.V4_SWAP_TOPIC or len(words) < 6:
        return None
    amount0 = runtime._signed(words[0], 128)
    amount1 = runtime._signed(words[1], 128)
    if amount0 == 0 or amount1 == 0:
        return None
    quote_is_0 = str(meta.get("quote_asset")) == str(meta.get("currency0"))
    quote_delta = amount0 if quote_is_0 else amount1
    token_delta = amount1 if quote_is_0 else amount0
    if quote_delta > 0 and token_delta < 0:
        side = "buy"
    elif quote_delta < 0 and token_delta > 0:
        side = "sell"
    else:
        return None
    return {
        "venue": "UNISWAP_V4",
        "lifecycle": "observation_only_quote_pool",
        "token": str(meta["token"]),
        "market": str(meta["pool_id"]),
        "tx_hash": str(log.get("transactionHash") or ""),
        "log_index": _log_index(log),
        "block_number": _block(log),
        "actor": runtime._topic_address(topics[2]),
        "actor_source": "v4_swap_sender",
        "side": side,
        "quote_amount_wei": abs(quote_delta),
        "token_amount_raw": abs(token_delta),
        "price_eth": None,
        "raw": {
            "amount0": amount0,
            "amount1": amount1,
            "sqrt_price_x96": runtime._uint(words[2]),
            "liquidity": runtime._uint(words[3]),
            "tick": runtime._signed(words[4], 24),
            "fee": runtime._uint(words[5]),
        },
    }


def _persist_swap(self: Any, payload: dict[str, Any], *, observed_at: str) -> bool:
    inserted = self._record_swap(
        venue=str(payload["venue"]),
        lifecycle=str(payload["lifecycle"]),
        token=str(payload["token"]),
        market=str(payload["market"]),
        tx_hash=str(payload["tx_hash"]),
        log_index=int(payload["log_index"]),
        block_number=int(payload["block_number"]),
        actor=str(payload["actor"]),
        actor_source=str(payload["actor_source"]),
        side=str(payload["side"]),
        quote_amount_wei=int(payload["quote_amount_wei"]),
        token_amount_raw=int(payload["token_amount_raw"]),
        price_eth=None,
        observed_at=observed_at,
    )
    if not inserted:
        _metric(self, "duplicates_ignored")
        return False
    self.store.append(
        "robinhood_swap_observation",
        observed_at,
        {
            **payload,
            "observation_version": OBSERVATION_VERSION,
            "source": "eth_getLogs",
            "paper_eligible": False,
            "analyzable": True,
            "execution_authorized": False,
            "paper_only": True,
            "live_money_authority": False,
        },
    )
    _metric(self, "v2_swaps_persisted" if payload["venue"] == "UNISWAP_V2" else "v4_swaps_persisted")
    return True


async def _observe_range(self: Any, *, from_block: int, to_block: int) -> None:
    _ensure_state(self)
    if not _enabled() or from_block > to_block:
        return

    cursor = self._roi_v2v4_observation_cursor
    start = int(from_block) if cursor is None else max(int(from_block), int(cursor) + 1)
    if start > to_block:
        return
    max_blocks = _max_recovery_blocks()
    span = to_block - start + 1
    if span > max_blocks:
        skipped = span - max_blocks
        start = to_block - max_blocks + 1
        _metric(self, "ranges_reanchored")
        _metric(self, "blocks_intentionally_not_backfilled", skipped)

    observed_at = runtime._utcnow()
    discovery = await catchup._logs_with_resilient_range(
        self,
        from_block=start,
        to_block=to_block,
        addresses=[UNISWAP_V2_FACTORY, runtime.UNISWAP_V4_POOL_MANAGER],
        topics=[[UNISWAP_V2_PAIR_CREATED_TOPIC, UNISWAP_V4_INITIALIZE_TOPIC]],
    )
    _metric(self, "discovery_requests")
    changed = False
    for log in sorted(discovery, key=_event_key):
        try:
            changed = _register_discovery(self, log, observed_at=observed_at) or changed
        except Exception:
            _metric(self, "parse_failures")
    if changed:
        _trim_registries(self, persist=True)

    rows: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    pairs = list(self._roi_v2_observation_pairs.values())
    batch_size = _v2_address_batch()
    jobs: list[Awaitable[tuple[str, list[dict[str, Any]], list[dict[str, Any]]]]] = []

    async def fetch_v2(batch: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
        result = await catchup._logs_with_resilient_range(
            self,
            from_block=start,
            to_block=to_block,
            addresses=[str(item["pair"]) for item in batch],
            topics=[UNISWAP_V2_SWAP_TOPIC],
        )
        return "v2", batch, result

    for index in range(0, len(pairs), batch_size):
        batch = pairs[index : index + batch_size]
        if batch:
            jobs.append(fetch_v2(batch))

    async def fetch_v4() -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
        result = await catchup._logs_with_resilient_range(
            self,
            from_block=start,
            to_block=to_block,
            addresses=[runtime.UNISWAP_V4_POOL_MANAGER],
            topics=[runtime.V4_SWAP_TOPIC],
        )
        return "v4", [], result

    jobs.append(fetch_v4())
    gate = asyncio.Semaphore(2)

    async def gated(job: Awaitable[tuple[str, list[dict[str, Any]], list[dict[str, Any]]]]) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
        async with gate:
            return await job

    results = await asyncio.gather(*(gated(job) for job in jobs))
    _metric(self, "market_requests", len(results))
    for kind, batch, logs in results:
        if kind == "v2":
            by_pair = {str(item["pair"]): item for item in batch}
            for log in logs:
                meta = by_pair.get(runtime._clean_address(log.get("address")))
                if meta is not None:
                    rows.append(("v2", meta, log))
        else:
            for log in logs:
                topics = [str(topic).lower() for topic in (log.get("topics") or [])]
                pool = _pool_id(topics[1]) if len(topics) > 1 else ""
                meta = self._roi_v4_observation_pools.get(pool)
                if meta is None:
                    _metric(self, "untracked_v4_swaps_ignored")
                    continue
                rows.append(("v4", meta, log))

    rows.sort(key=lambda item: _event_key(item[2]))
    for kind, meta, log in rows:
        try:
            payload = _v2_swap_payload(meta, log) if kind == "v2" else _v4_swap_payload(meta, log)
            if payload is not None:
                _persist_swap(self, payload, observed_at=observed_at)
        except Exception:
            _metric(self, "parse_failures")

    self._roi_v2v4_observation_cursor = int(to_block)
    _set_state_json(self, _STATE_CURSOR, int(to_block))
    self._roi_v2v4_observation_last_success_at = runtime._utcnow()
    self._roi_v2v4_observation_last_error = None
    self._roi_v2v4_observation_last_range = {
        "from_block": int(start),
        "to_block": int(to_block),
        "discovery_logs": len(discovery),
        "activity_logs": len(rows),
        "bounded_recovery": True,
    }
    _metric(self, "ranges_completed")


def _fetch_with_observation(
    original: Callable[..., Awaitable[list[tuple[str, Any, dict[str, Any]]]]],
) -> Callable[..., Awaitable[list[tuple[str, Any, dict[str, Any]]]]]:
    @wraps(original)
    async def wrapped(self: Any, *, from_block: int, to_block: int) -> list[tuple[str, Any, dict[str, Any]]]:
        canonical = await original(self, from_block=from_block, to_block=to_block)
        try:
            await _observe_range(self, from_block=from_block, to_block=to_block)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _ensure_state(self)
            _metric(self, "ranges_failed")
            self._roi_v2v4_observation_last_error = f"{type(exc).__name__}: {exc}"
        # The expansion is deliberately absent from this return value. The verified
        # live-frontier decision loop therefore sees exactly the pre-existing V3/Pons
        # markets and cannot acquire V2/V4 entry authority from observation alone.
        return canonical

    setattr(wrapped, "_roi_v2_v4_observation_fetch", True)
    return wrapped


def _status_with_observation(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    @wraps(original)
    def wrapped(self: Any) -> dict[str, Any]:
        payload = original(self)
        _ensure_state(self)
        latest = getattr(self, "_latest_block", None)
        cursor = self._roi_v2v4_observation_cursor
        lag = max(0, int(latest) - int(cursor)) if latest is not None and cursor is not None else None
        payload["v2_v4_observation"] = {
            "version": OBSERVATION_VERSION,
            "enabled": _enabled(),
            "production_default_enabled": True,
            "chain": "ROBINHOOD_CHAIN",
            "protocols": ["UNISWAP_V2", "UNISWAP_V4"],
            "v2_factory": UNISWAP_V2_FACTORY,
            "v4_pool_manager": runtime.UNISWAP_V4_POOL_MANAGER,
            "cursor_block": cursor,
            "latest_block": latest,
            "lag_blocks": lag,
            "last_success_at": self._roi_v2v4_observation_last_success_at,
            "last_error": self._roi_v2v4_observation_last_error,
            "last_range": self._roi_v2v4_observation_last_range,
            "tracked_v2_pairs": len(self._roi_v2_observation_pairs),
            "tracked_v4_quote_pools": len(self._roi_v4_observation_pools),
            "max_v2_pairs": _max_v2_pairs(),
            "max_v4_pools": _max_v4_pools(),
            "max_recovery_blocks": _max_recovery_blocks(),
            "v2_address_batch_size": _v2_address_batch(),
            "metrics": dict(self._roi_v2v4_observation_metrics),
            "historical_backfill_enabled": False,
            "bounded_resumable_frontier": True,
            "shared_existing_rpc_transport": True,
            "separate_database_created": False,
            "schema_migration_required": False,
            "observation_only": True,
            "analyzable": True,
            "canonical_trade_eligible": False,
            "new_entry_authority": False,
            "execution_authorized": False,
            "strategy_thresholds_changed": False,
            "wallet_policy_changed": False,
            "sizing_policy_changed": False,
            "exit_policy_changed": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
        venues = payload.get("venues")
        if isinstance(venues, dict):
            venues["uniswap_v2_observation"] = {
                "paper_authority": False,
                "new_entry_authority": False,
                "discovery": "bounded_forward_pair_created",
            }
            venues["uniswap_v4_observation"] = {
                "paper_authority": False,
                "new_entry_authority": False,
                "discovery": "bounded_forward_poolmanager_initialize",
            }
        return payload

    setattr(wrapped, "_roi_v2_v4_observation_status", True)
    return wrapped


def install_robinhood_v2_v4_observation(plane_cls: type[Any]) -> None:
    global _ORIGINAL_FETCH, _ORIGINAL_STATUS

    current_fetch = frontier._fetch_market_logs
    if not bool(getattr(current_fetch, "_roi_v2_v4_observation_fetch", False)):
        _ORIGINAL_FETCH = current_fetch
        frontier._fetch_market_logs = _fetch_with_observation(current_fetch)  # type: ignore[assignment]

    current_status = getattr(plane_cls, "status", None)
    if current_status is not None and not bool(
        getattr(current_status, "_roi_v2_v4_observation_status", False)
    ):
        _ORIGINAL_STATUS = current_status
        plane_cls.status = _status_with_observation(current_status)  # type: ignore[method-assign]

    setattr(plane_cls, "_roi_v2_v4_observation_installed", True)
    setattr(plane_cls, "_roi_v2_v4_observation_version", OBSERVATION_VERSION)


__all__ = [
    "OBSERVATION_VERSION",
    "UNISWAP_V2_FACTORY",
    "UNISWAP_V2_PAIR_CREATED_TOPIC",
    "UNISWAP_V2_SWAP_TOPIC",
    "UNISWAP_V4_INITIALIZE_TOPIC",
    "_decode_v2_pair",
    "_decode_v4_pool",
    "_observe_range",
    "install_robinhood_v2_v4_observation",
]
