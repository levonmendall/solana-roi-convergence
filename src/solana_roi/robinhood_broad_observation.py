from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from functools import wraps
from typing import Any

from . import robinhood_chain_core as core
from . import robinhood_live_frontier_verification_repair as frontier
from .robinhood_catchup_capacity_repair import _logs_with_resilient_range

OBSERVATION_VERSION = "robinhood-broad-observation-v1"
UNISWAP_V2_FACTORY = "0x8bceaa40b9acdfaedf85adf4ff01f5ad6517937f"
V2_PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
V2_SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
V4_INITIALIZE_TOPIC = "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
DEFAULT_MAX_TRACKED_MARKETS = 512
MAX_TRACKED_MARKETS = 2048
QUERY_BATCH_SIZE = 32

_ORIGINAL_SYNC: Callable[..., Awaitable[Any]] | None = None


@dataclass(slots=True)
class ObservedV2:
    token: str
    pool: str
    token0: str
    token1: str
    launch_block: int
    decimals: int | None = None


@dataclass(slots=True)
class ObservedV4:
    token: str
    pool_id: str
    currency0: str
    currency1: str
    fee: int
    tick_spacing: int
    launch_block: int
    decimals: int | None = None

    @property
    def quote(self) -> str:
        return self.currency0 if self.currency0 in {ZERO_ADDRESS, core.WETH} else self.currency1


def _enabled() -> bool:
    return os.getenv("ROBINHOOD_BROAD_OBSERVATION_ENABLED", "false").strip().lower() in {
        "1", "true", "yes", "on"
    }


def _limit() -> int:
    try:
        value = int(os.getenv("ROBINHOOD_BROAD_OBSERVATION_MAX_MARKETS", str(DEFAULT_MAX_TRACKED_MARKETS)))
    except (TypeError, ValueError):
        value = DEFAULT_MAX_TRACKED_MARKETS
    return max(32, min(MAX_TRACKED_MARKETS, value))


def _block(log: dict[str, Any]) -> int:
    return int(str(log.get("blockNumber") or "0x0"), 16)


def _index(log: dict[str, Any], key: str) -> int:
    return int(str(log.get(key) or "0x0"), 16)


def _sort(log: dict[str, Any]) -> tuple[int, int, int]:
    return _block(log), _index(log, "transactionIndex"), _index(log, "logIndex")


def _pool_id(value: str | None) -> str:
    value = str(value or "").lower()
    return value if value.startswith("0x") and len(value) == 66 else ""


def _v4_pair(currency0: str, currency1: str) -> tuple[str, str] | None:
    quotes = {ZERO_ADDRESS, core.WETH}
    q0, q1 = currency0 in quotes, currency1 in quotes
    if q0 == q1:
        return None
    token, quote = (currency1, currency0) if q0 else (currency0, currency1)
    return (token, quote) if token and token not in quotes else None


def _ensure_metrics(self: Any) -> None:
    defaults = {
        "_roi_broad_ranges": 0,
        "_roi_broad_failures": 0,
        "_roi_broad_v2_swaps": 0,
        "_roi_broad_v4_swaps": 0,
        "_roi_broad_last_range": None,
        "_roi_broad_last_success_at": None,
        "_roi_broad_last_error": None,
    }
    for name, value in defaults.items():
        if not hasattr(self, name):
            setattr(self, name, value)


def _trim(items: dict[str, Any]) -> None:
    for key in list(items)[: max(0, len(items) - _limit())]:
        items.pop(key, None)


def _ensure_state(self: Any) -> tuple[dict[str, ObservedV2], dict[str, ObservedV4]]:
    _ensure_metrics(self)
    v2 = getattr(self, "_roi_observed_uniswap_v2", None)
    v4 = getattr(self, "_roi_observed_uniswap_v4", None)
    if isinstance(v2, dict) and isinstance(v4, dict):
        return v2, v4
    v2, v4 = {}, {}
    store = getattr(self, "store", None)
    if store is not None and hasattr(store, "_lock") and hasattr(store, "db"):
        try:
            with store._lock:
                rows = store.db.execute(
                    "SELECT protocol,token,pool,pair_token,fee,tick_spacing,launch_block "
                    "FROM robinhood_launches WHERE paper_eligible=0 "
                    "AND protocol IN ('uniswap_v2','uniswap_v4') ORDER BY id DESC LIMIT ?",
                    (_limit() * 2,),
                ).fetchall()
            for row in reversed(rows):
                token = core._clean_address(str(row["token"] or ""))
                pool = str(row["pool"] or "").lower()
                pair = core._clean_address(str(row["pair_token"] or ""))
                block = int(row["launch_block"] or 0)
                if str(row["protocol"]) == "uniswap_v2":
                    pool = core._clean_address(pool)
                    if token and pool and pair == core.WETH:
                        t0, t1 = sorted((core.WETH, token), key=lambda x: int(x, 16))
                        v2[pool] = ObservedV2(token, pool, t0, t1, block)
                elif token and _pool_id(pool) and pair in {ZERO_ADDRESS, core.WETH}:
                    c0, c1 = sorted((pair, token), key=lambda x: int(x, 16))
                    v4[pool] = ObservedV4(
                        token, pool, c0, c1, int(row["fee"] or 0),
                        int(row["tick_spacing"] or 0), block
                    )
        except Exception:
            v2, v4 = {}, {}
    _trim(v2)
    _trim(v4)
    self._roi_observed_uniswap_v2 = v2
    self._roi_observed_uniswap_v4 = v4
    return v2, v4


async def _decimals(self: Any, market: ObservedV2 | ObservedV4) -> int:
    if market.decimals is None:
        market.decimals = await self.rpc.token_decimals(market.token)
    return int(market.decimals)


def _price(quote: int, token: int, decimals: int) -> float | None:
    units = token / float(10**decimals) if token > 0 else 0.0
    return (quote / 1e18) / units if quote > 0 and units > 0 else None


async def _register_v2(self: Any, log: dict[str, Any]) -> None:
    topics = [str(x).lower() for x in (log.get("topics") or [])]
    words = core._words(str(log.get("data") or ""))
    if len(topics) < 3 or topics[0] != V2_PAIR_CREATED_TOPIC or not words:
        return
    t0, t1 = core._topic_address(topics[1]), core._topic_address(topics[2])
    if core.WETH not in {t0, t1}:
        return
    token, pool = (t1 if t0 == core.WETH else t0), core._word_address(words[0])
    v2, _ = _ensure_state(self)
    if not token or token == core.WETH or not pool or pool in v2:
        return
    v2[pool] = ObservedV2(token, pool, t0, t1, _block(log))
    _trim(v2)
    self._persist_launch(
        protocol="uniswap_v2", venue="UNISWAP_V2_OBSERVED", lifecycle="observed_weth_pool",
        token=token, pool=pool, pair_token=core.WETH, launch_block=_block(log),
        paper_eligible=False, source_tx=str(log.get("transactionHash") or "")
    )


async def _register_v4(self: Any, log: dict[str, Any]) -> None:
    topics = [str(x).lower() for x in (log.get("topics") or [])]
    words = core._words(str(log.get("data") or ""))
    if len(topics) < 4 or topics[0] != V4_INITIALIZE_TOPIC or len(words) < 2:
        return
    pool_id = _pool_id(topics[1])
    c0, c1 = core._topic_address(topics[2]), core._topic_address(topics[3])
    pair = _v4_pair(c0, c1)
    _, v4 = _ensure_state(self)
    if not pool_id or pair is None or pool_id in v4:
        return
    token, quote = pair
    fee, tick = core._uint(words[0]) & ((1 << 24) - 1), core._signed(words[1], 24)
    v4[pool_id] = ObservedV4(token, pool_id, c0, c1, fee, tick, _block(log))
    _trim(v4)
    self._persist_launch(
        protocol="uniswap_v4", venue="UNISWAP_V4_OBSERVED",
        lifecycle="observed_eth_pool" if quote == ZERO_ADDRESS else "observed_weth_pool",
        token=token, pool=pool_id, pair_token=quote, fee=fee, tick_spacing=tick,
        launch_block=_block(log), paper_eligible=False,
        source_tx=str(log.get("transactionHash") or "")
    )


async def _v2_swap(self: Any, market: ObservedV2, log: dict[str, Any], observed_at: str) -> None:
    topics = [str(x).lower() for x in (log.get("topics") or [])]
    words = core._words(str(log.get("data") or ""))
    if len(topics) < 3 or topics[0] != V2_SWAP_TOPIC or len(words) < 4:
        return
    a0i, a1i, a0o, a1o = map(core._uint, words[:4])
    qi, qo, ti, to = (a0i, a0o, a1i, a1o) if market.token0 == core.WETH else (a1i, a1o, a0i, a0o)
    if qi > 0 and to > 0:
        side, quote, token = "buy", qi, to
    elif ti > 0 and qo > 0:
        side, quote, token = "sell", qo, ti
    else:
        return
    inserted = self._record_swap(
        venue="UNISWAP_V2_OBSERVED", lifecycle="observed_weth_pool", token=market.token,
        market=market.pool, tx_hash=str(log.get("transactionHash") or ""),
        log_index=_index(log, "logIndex"), block_number=_block(log),
        actor=core._topic_address(topics[2]), actor_source="swap_recipient_observation_only",
        side=side, quote_amount_wei=quote, token_amount_raw=token,
        price_eth=_price(quote, token, await _decimals(self, market)), observed_at=observed_at
    )
    if inserted:
        self._roi_broad_v2_swaps += 1


async def _v4_swap(self: Any, market: ObservedV4, log: dict[str, Any], observed_at: str) -> None:
    topics = [str(x).lower() for x in (log.get("topics") or [])]
    words = core._words(str(log.get("data") or ""))
    if len(topics) < 3 or topics[0] != core.V4_SWAP_TOPIC or len(words) < 2:
        return
    a0, a1 = core._signed(words[0], 128), core._signed(words[1], 128)
    quote, token = (a0, a1) if market.quote == market.currency0 else (a1, a0)
    if quote > 0 and token < 0:
        side = "buy"
    elif quote < 0 and token > 0:
        side = "sell"
    else:
        return
    quote, token = abs(quote), abs(token)
    inserted = self._record_swap(
        venue="UNISWAP_V4_OBSERVED",
        lifecycle="observed_eth_pool" if market.quote == ZERO_ADDRESS else "observed_weth_pool",
        token=market.token, market=market.pool_id,
        tx_hash=str(log.get("transactionHash") or ""), log_index=_index(log, "logIndex"),
        block_number=_block(log), actor=core._topic_address(topics[2]),
        actor_source="pool_manager_sender_observation_only", side=side,
        quote_amount_wei=quote, token_amount_raw=token,
        price_eth=_price(quote, token, await _decimals(self, market)), observed_at=observed_at
    )
    if inserted:
        self._roi_broad_v4_swaps += 1


def _prospective_live_range(self: Any, *, from_block: int, to_block: int) -> bool:
    if not frontier._live_epoch_active(self):
        return False
    cursor, latest = frontier._live_cursor(self), getattr(self, "_latest_block", None)
    if cursor is None or latest is None:
        return False
    gap = int(latest) - int(cursor)
    return (
        0 < gap <= frontier.MAX_LIVE_FRONTIER_GAP_BLOCKS
        and int(from_block) == int(cursor) + 1
        and int(to_block) == int(latest)
    )


async def _observe(self: Any, *, from_block: int, to_block: int, include_swaps: bool) -> None:
    _ensure_state(self)
    v2_created = await _logs_with_resilient_range(
        self, from_block=from_block, to_block=to_block,
        addresses=[UNISWAP_V2_FACTORY], topics=[V2_PAIR_CREATED_TOPIC]
    )
    v4_created = await _logs_with_resilient_range(
        self, from_block=from_block, to_block=to_block,
        addresses=[core.UNISWAP_V4_POOL_MANAGER], topics=[V4_INITIALIZE_TOPIC]
    )
    for log in sorted(v2_created, key=_sort):
        await _register_v2(self, log)
    for log in sorted(v4_created, key=_sort):
        await _register_v4(self, log)
    if not include_swaps:
        self._roi_broad_last_range = {
            "from_block": from_block, "to_block": to_block, "metadata_recovery_only": True,
            "v2_factory_logs": len(v2_created), "v4_initialize_logs": len(v4_created),
            "v2_swap_logs": 0, "v4_swap_logs": 0
        }
        return

    v2, v4 = _ensure_state(self)
    v2_logs: list[dict[str, Any]] = []
    v2_values = list(v2.values())
    for i in range(0, len(v2_values), QUERY_BATCH_SIZE):
        pools = v2_values[i:i + QUERY_BATCH_SIZE]
        v2_logs += await _logs_with_resilient_range(
            self, from_block=from_block, to_block=to_block,
            addresses=[x.pool for x in pools], topics=[V2_SWAP_TOPIC]
        )
    observed_at = core._utcnow()
    for log in sorted(v2_logs, key=_sort):
        market = v2.get(core._clean_address(log.get("address")))
        if market is not None:
            await _v2_swap(self, market, log, observed_at)

    v4_logs: list[dict[str, Any]] = []
    pool_ids = list(v4)
    for i in range(0, len(pool_ids), QUERY_BATCH_SIZE):
        ids = pool_ids[i:i + QUERY_BATCH_SIZE]
        v4_logs += await _logs_with_resilient_range(
            self, from_block=from_block, to_block=to_block,
            addresses=[core.UNISWAP_V4_POOL_MANAGER], topics=[core.V4_SWAP_TOPIC, ids]
        )
    for log in sorted(v4_logs, key=_sort):
        topics = [str(x).lower() for x in (log.get("topics") or [])]
        market = v4.get(_pool_id(topics[1]) if len(topics) > 1 else "")
        if market is not None:
            await _v4_swap(self, market, log, observed_at)

    self._roi_broad_ranges += 1
    self._roi_broad_last_success_at = core._utcnow()
    self._roi_broad_last_error = None
    self._roi_broad_last_range = {
        "from_block": from_block, "to_block": to_block, "metadata_recovery_only": False,
        "v2_factory_logs": len(v2_created), "v4_initialize_logs": len(v4_created),
        "v2_swap_logs": len(v2_logs), "v4_swap_logs": len(v4_logs)
    }


async def _sync(self: Any, *, from_block: int, to_block: int) -> Any:
    if _ORIGINAL_SYNC is None:
        raise RuntimeError("broad observation installer missing canonical factory sync")
    result = await _ORIGINAL_SYNC(self, from_block=from_block, to_block=to_block)
    _ensure_metrics(self)
    if not _enabled():
        return result
    try:
        await _observe(
            self, from_block=from_block, to_block=to_block,
            include_swaps=_prospective_live_range(self, from_block=from_block, to_block=to_block)
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        self._roi_broad_failures += 1
        self._roi_broad_last_error = f"{type(exc).__name__}: {exc}"
    return result


def _status(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    @wraps(original)
    def wrapped(self: Any) -> dict[str, Any]:
        payload = original(self)
        v2, v4 = _ensure_state(self)
        enabled = _enabled()
        payload["broad_market_observation"] = {
            "version": OBSERVATION_VERSION, "installed": True, "enabled": enabled,
            "activation_hold": None if enabled else "storage_migration_certification",
            "observation_only": True, "strategy_authority": False,
            "paper_entry_authority": False, "paper_exit_authority": False,
            "paper_eligible": False, "schema_changes": False,
            "storage_migration_touched": False, "canonical_storage_authority_changed": False,
            "uniswap_v2": {"factory": UNISWAP_V2_FACTORY, "tracked_weth_pools": len(v2),
                           "swap_observations": int(getattr(self, "_roi_broad_v2_swaps", 0) or 0)},
            "uniswap_v4": {"pool_manager": core.UNISWAP_V4_POOL_MANAGER,
                           "tracked_eth_or_weth_pools": len(v4),
                           "swap_observations": int(getattr(self, "_roi_broad_v4_swaps", 0) or 0)},
            "ranges_completed": int(getattr(self, "_roi_broad_ranges", 0) or 0),
            "failures": int(getattr(self, "_roi_broad_failures", 0) or 0),
            "last_success_at": getattr(self, "_roi_broad_last_success_at", None),
            "last_error": getattr(self, "_roi_broad_last_error", None),
            "last_range": getattr(self, "_roi_broad_last_range", None),
            "paper_only": True, "live_money_authority": False,
            "signing_available": False, "transaction_submission_available": False,
        }
        return payload
    wrapped._roi_robinhood_broad_observation_status = True  # type: ignore[attr-defined]
    return wrapped


def install_robinhood_broad_observation(cls: type[Any]) -> None:
    global _ORIGINAL_SYNC
    if bool(getattr(cls, "_roi_robinhood_broad_observation_installed", False)):
        return
    _ORIGINAL_SYNC = frontier._sync_factory_state
    frontier._sync_factory_state = _sync
    cls.status = _status(cls.status)
    cls._roi_robinhood_broad_observation_installed = True


__all__ = [
    "OBSERVATION_VERSION", "UNISWAP_V2_FACTORY", "V2_PAIR_CREATED_TOPIC",
    "V2_SWAP_TOPIC", "V4_INITIALIZE_TOPIC", "install_robinhood_broad_observation"
]
