from __future__ import annotations

import asyncio
import json
import math
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from statistics import mean, median
from typing import Any

import httpx

from .observation_store import ObservationEventStore


ROBINHOOD_CHAIN_ID = 4663
ROBINHOOD_PUBLIC_RPC = "https://rpc.mainnet.chain.robinhood.com"
ROBINHOOD_CHAIN_PAPER_VERSION = "robinhood-chain-paper-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
PAPER_TRADING_AUTHORITY = True

WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
PONS_V1_ACTIVE_FACTORY = "0xa5aab3f0c6eeadf30ef1d3eb997108e976351feb"
PONS_V1_LEGACY_FACTORY = "0x0c37a24f5d23a486fa692d1500881d698b1f77a4"
PONS_V1_START_BLOCK = 8_991_118
PONS_V1_TOKEN_LAUNCHED_TOPIC = "0xdb51ea9ad51ab453a65a4cb7e60c3cb378c9501bb002609f8f97778fb6c4235a"
PONS_V1_LEGACY_START_BLOCK = 8_600_612
UNISWAP_V3_FACTORY = "0x1f7d7550b1b028f7571e69a784071f0205fd2efa"
UNISWAP_V3_QUOTER_V2 = "0x33e885ed0ec9bf04ecfb19341582aadcb4c8a9e7"
PONS_V1_SWAP_ROUTER = "0xcaf681a66d020601342297493863e78c959e5cb2"

PONS_V2_FACTORY = "0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e"
PONS_V2_MEME_HOOK = "0xe5e702641ea86f4ae6cc3cdaed2b886f976be044"
UNISWAP_V4_POOL_MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"
UNISWAP_V4_QUOTER = "0x8dc178efb8111bb0973dd9d722ebeff267c98f94"
BLOCKSCOUT_API = "https://robinhoodchain.blockscout.com/api/v2"
ROBINHOOD_STOCK_ASSETS_API = "https://api.robinhood.com/rhj/assets"
V3_QUOTE_EXACT_INPUT_SINGLE_SELECTOR = "0xc6a5026a"
V4_QUOTE_EXACT_INPUT_SINGLE_SELECTOR = "0xaa9d21cb"

V3_POOL_CREATED_TOPIC = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
V3_SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
V4_SWAP_TOPIC = "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"
PONS_V2_TOKEN_LAUNCHED_TOPIC = "0x8d4aad4953d0ca700d468f3753aa14432d1b35b43ec6409f051fb6aa43a89607"
PONS_V2_CURVE_BUY_TOPIC = "0xec36bf571f136799e8dc0b0b8bea4b04d8bd3d43de838aab0d5fc21d4cbfc455"
PONS_V2_CURVE_SELL_TOPIC = "0x8113d738abdcb6b38357e9d53a54a7157861a09031b453651f0fe7fe151f59df"

BOOTSTRAP_PAPER_FRACTION = 0.01
POSITION_FRACTION_GRID = (0.005, 0.01, 0.02, 0.05)
MAX_POSITION_FRACTION = 0.05
MAX_OPEN_EXPOSURE_FRACTION = 0.20
MIN_CONTEXT_SAMPLES = 30
MAX_CHASE_FRACTION = 0.15
MAX_IMMEDIATE_ROUND_TRIP_COST = 0.15
STOP_LOSS_FRACTION = -0.12
HARVEST_FRACTION = 0.30
MAX_HOLD_SECONDS = 20 * 60
POLL_SECONDS = 5.0
MAX_BLOCKS_PER_POLL = 200
MAX_TRACKED_V3_POOLS = 64
MAX_TRACKED_V2_CURVES = 64
LIVE_LAG_BLOCKS = 2

KNOWN_NON_ACTORS = {
    UNISWAP_V3_QUOTER_V2,
    PONS_V1_SWAP_ROUTER,
    UNISWAP_V3_FACTORY,
    PONS_V1_ACTIVE_FACTORY,
    PONS_V1_LEGACY_FACTORY,
    PONS_V2_FACTORY,
    UNISWAP_V4_POOL_MANAGER,
    UNISWAP_V4_QUOTER,
}


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_address(value: str | None) -> str:
    raw = (value or "").lower()
    if raw.startswith("0x") and len(raw) == 42:
        return raw
    return ""


def _topic_address(topic: str) -> str:
    raw = str(topic).lower().removeprefix("0x")
    if len(raw) != 64:
        return ""
    return "0x" + raw[-40:]


def _word_address(word: str) -> str:
    raw = str(word).lower().removeprefix("0x")
    if len(raw) != 64:
        return ""
    return "0x" + raw[-40:]


def _words(data: str) -> list[str]:
    raw = str(data or "").removeprefix("0x")
    if len(raw) % 64:
        return []
    return ["0x" + raw[i : i + 64] for i in range(0, len(raw), 64)]


def _uint(word: str) -> int:
    return int(str(word), 16)


def _signed(word: str, bits: int = 256) -> int:
    value = int(str(word), 16)
    limit = 1 << bits
    sign = 1 << (bits - 1)
    value &= limit - 1
    return value - limit if value & sign else value


def _hex_quantity(value: int) -> str:
    return hex(max(0, int(value)))


def _word_uint(value: int) -> str:
    if value < 0:
        value = (1 << 256) + value
    return int(value).to_bytes(32, "big", signed=False).hex()


def _word_bool(value: bool) -> str:
    return _word_uint(1 if value else 0)


def _word_addr(value: str) -> str:
    raw = _clean_address(value).removeprefix("0x")
    if len(raw) != 40:
        raise ValueError("invalid EVM address")
    return ("0" * 24) + raw


def _finite(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _trimmed_ex_best(values: list[float], n: int = 1) -> float | None:
    if len(values) <= n:
        return None
    remaining = sorted(values, reverse=True)[n:]
    return mean(remaining) if remaining else None


def _expected_log_growth(values: list[float], fraction: float) -> float | None:
    if not values or fraction <= 0:
        return None
    terms: list[float] = []
    for value in values:
        terminal = 1.0 + fraction * value
        if terminal <= 0:
            return float("-inf")
        terms.append(math.log(terminal))
    return mean(terms) if terms else None


def classify_context_returns(values: list[float]) -> dict[str, Any]:
    clean = [float(v) for v in values if math.isfinite(float(v))]
    count = len(clean)
    trimmed = _trimmed_ex_best(clean, 1)
    med = median(clean) if clean else None
    positive_rate = sum(v > 0 for v in clean) / count if clean else None
    if count < MIN_CONTEXT_SAMPLES:
        state = "bootstrap_paper_evidence"
    elif trimmed is not None and med is not None and trimmed > 0 and med > 0 and positive_rate is not None and positive_rate >= 0.50:
        state = "promoted_paper_context"
    elif trimmed is not None and med is not None and (trimmed <= 0 or med <= 0):
        state = "demoted_paper_context"
    else:
        state = "observe_mixed_context"

    growth = {f: _expected_log_growth(clean, f) for f in POSITION_FRACTION_GRID}
    viable = [(f, g) for f, g in growth.items() if g is not None and math.isfinite(g) and g > 0]
    best_fraction = max(viable, key=lambda item: item[1])[0] if viable else 0.0
    return {
        "sample_count": count,
        "mean_roi_pct": mean(clean) * 100.0 if clean else None,
        "median_roi_pct": med * 100.0 if med is not None else None,
        "trimmed_mean_roi_ex_best_1_pct": trimmed * 100.0 if trimmed is not None else None,
        "positive_rate_pct": positive_rate * 100.0 if positive_rate is not None else None,
        "state": state,
        "best_paper_position_fraction": min(MAX_POSITION_FRACTION, best_fraction),
        "historical_promotion_authority": False,
    }


@dataclass(slots=True)
class V3Pool:
    token: str
    pool: str
    token0: str
    token1: str
    fee: int
    token_decimals: int = 18
    venue: str = "UNISWAP_V3_DIRECT"
    lifecycle: str = "new_weth_pool"
    deployer: str = ""
    launch_block: int = 0
    restrictions_end_block: int = 0
    first_price_eth: float | None = None
    first_live_observed_at: str | None = None
    recent_swaps: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=500))

    @property
    def weth_is_token0(self) -> bool:
        return self.token0 == WETH


@dataclass(slots=True)
class V2Curve:
    token: str
    curve: str
    deployer: str
    pair_token: str
    launch_config_id: int
    graduation_threshold: int
    launch_block: int
    recent_swaps: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=500))


class RobinhoodRpc:
    def __init__(self, rpc_url: str | None = None, *, timeout_seconds: float = 4.0) -> None:
        self.rpc_url = (rpc_url or os.getenv("ROBINHOOD_RPC_URL") or ROBINHOOD_PUBLIC_RPC).strip()
        self.client = httpx.AsyncClient(timeout=timeout_seconds)
        self._request_id = 0
        self._selectors: dict[str, str] = {}

    async def close(self) -> None:
        await self.client.aclose()

    async def rpc(self, method: str, params: list[Any]) -> Any:
        self._request_id += 1
        response = await self.client.post(
            self.rpc_url,
            json={"jsonrpc": "2.0", "id": self._request_id, "method": method, "params": params},
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("error") is not None:
            raise RuntimeError(f"{method}: {payload['error']}")
        return payload.get("result")

    async def chain_id(self) -> int:
        return int(str(await self.rpc("eth_chainId", [])), 16)

    async def block_number(self) -> int:
        return int(str(await self.rpc("eth_blockNumber", [])), 16)

    async def gas_price(self) -> int:
        return int(str(await self.rpc("eth_gasPrice", [])), 16)

    async def get_logs(
        self,
        *,
        from_block: int,
        to_block: int,
        addresses: list[str] | tuple[str, ...] | None = None,
        topics: list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {
            "fromBlock": _hex_quantity(from_block),
            "toBlock": _hex_quantity(to_block),
        }
        if addresses:
            query["address"] = list(addresses) if len(addresses) > 1 else addresses[0]
        if topics is not None:
            query["topics"] = topics
        result = await self.rpc("eth_getLogs", [query])
        return list(result or [])

    async def eth_call(self, to: str, data: str) -> str:
        result = await self.rpc("eth_call", [{"to": to, "data": data}, "latest"])
        return str(result or "0x")

    async def selector(self, signature: str) -> str:
        cached = self._selectors.get(signature)
        if cached:
            return cached
        result = str(await self.rpc("web3_sha3", ["0x" + signature.encode().hex()]))
        if not result.startswith("0x") or len(result) < 10:
            raise RuntimeError(f"selector unavailable for {signature}")
        selector = result[:10]
        self._selectors[signature] = selector
        return selector

    async def tx_from(self, tx_hash: str) -> str:
        try:
            tx = await self.rpc("eth_getTransactionByHash", [tx_hash])
            return _clean_address((tx or {}).get("from"))
        except Exception:
            return ""

    async def token_decimals(self, token: str) -> int:
        try:
            raw = await self.eth_call(token, "0x313ce567")
            words = _words(raw)
            value = _uint(words[0]) if words else 18
            return value if 0 <= value <= 36 else 18
        except Exception:
            return 18

    async def v3_quote_exact_input(
        self,
        *,
        token_in: str,
        token_out: str,
        fee: int,
        amount_in: int,
    ) -> tuple[int, int]:
        selector = V3_QUOTE_EXACT_INPUT_SINGLE_SELECTOR
        data = "0x" + selector.removeprefix("0x") + "".join(
            [
                _word_addr(token_in),
                _word_addr(token_out),
                _word_uint(amount_in),
                _word_uint(fee),
                _word_uint(0),
            ]
        )
        raw = await self.eth_call(UNISWAP_V3_QUOTER_V2, data)
        words = _words(raw)
        if len(words) < 4:
            raise RuntimeError("V3 quoter returned incomplete result")
        return _uint(words[0]), _uint(words[3])

    async def call_uint(self, contract: str, signature: str, args: list[str] | None = None) -> int:
        selector = await self.selector(signature)
        data = "0x" + selector.removeprefix("0x") + "".join(args or [])
        raw = await self.eth_call(contract, data)
        words = _words(raw)
        if not words:
            raise RuntimeError(f"{signature} returned no data")
        return _uint(words[0])

    async def pons_v2_launch_state(self, token: str) -> dict[str, Any]:
        selector = await self.selector("getLaunchedToken(address)")
        raw = await self.eth_call(PONS_V2_FACTORY, "0x" + selector.removeprefix("0x") + _word_addr(token))
        words = _words(raw)
        if len(words) < 13:
            raise RuntimeError("pons v2 launch state incomplete")
        return {
            "token": _word_address(words[0]),
            "curve": _word_address(words[1]),
            "deployer": _word_address(words[2]),
            "creator_fee_recipient": _word_address(words[3]),
            "pair_token": _word_address(words[4]),
            "graduation_threshold": _uint(words[5]),
            "pool_fee": _uint(words[6]),
            "tick_spacing": _signed(words[7], 24),
            "creator_tax_bps": _uint(words[8]),
            "buyback_enabled": bool(_uint(words[9])),
            "phase": _uint(words[10]),
            "swept_quote": _uint(words[11]),
            "swept_tokens": _uint(words[12]),
        }

    async def pons_v2_curve_quote(
        self,
        *,
        curve: str,
        quote_in: int,
        recipient: str,
    ) -> dict[str, int]:
        reserves_selector = await self.selector("getReserves()")
        raw = await self.eth_call(curve, reserves_selector)
        words = _words(raw)
        if len(words) < 2:
            raise RuntimeError("curve reserves unavailable")
        quote_reserve, token_reserve = _uint(words[0]), _uint(words[1])
        sellable = await self.call_uint(curve, "sellableTokens()")
        fee_bps = await self.call_uint(curve, "feeBps()")
        creator_tax_bps = await self.call_uint(curve, "creatorTaxBps()")
        snipe_bps = await self.call_uint(
            curve,
            "currentSnipeTaxBps(address)",
            [_word_addr(recipient)],
        )
        max_snipe = max(0, 10_000 - fee_bps - creator_tax_bps - 100)
        snipe_bps = min(snipe_bps, max_snipe)
        spent = int(quote_in)
        fee = spent * fee_bps // 10_000
        tax = spent * creator_tax_bps // 10_000
        snipe_tax = spent * snipe_bps // 10_000
        net = spent - fee - tax - snipe_tax
        if net <= 0 or quote_reserve <= 0 or token_reserve <= 0:
            raise RuntimeError("curve buy quote has no executable net input")
        tokens_out = net * token_reserve // (quote_reserve + net)
        if tokens_out > sellable:
            tokens_out = sellable
            if sellable >= token_reserve:
                raise RuntimeError("invalid sellable reserve")
            required_net = (sellable * quote_reserve) // (token_reserve - sellable) + 1
            denom = 10_000 - fee_bps - creator_tax_bps - snipe_bps
            if denom <= 0:
                raise RuntimeError("curve fee denominator unavailable")
            grossed = (required_net * 10_000 + denom - 1) // denom
            spent = min(spent, grossed)
        return {
            "tokens_out": int(tokens_out),
            "spent": int(spent),
            "refund": int(quote_in - spent),
            "fee_bps": int(fee_bps),
            "creator_tax_bps": int(creator_tax_bps),
            "snipe_tax_bps": int(snipe_bps),
            "quote_reserve": int(quote_reserve),
            "token_reserve": int(token_reserve),
            "sellable_tokens": int(sellable),
        }

    async def pons_v2_curve_sell_quote(self, *, curve: str, tokens_in: int) -> int:
        reserves_selector = await self.selector("getReserves()")
        raw = await self.eth_call(curve, reserves_selector)
        words = _words(raw)
        if len(words) < 2:
            raise RuntimeError("curve reserves unavailable")
        quote_reserve, token_reserve = _uint(words[0]), _uint(words[1])
        ready = bool(await self.call_uint(curve, "readyToGraduate()"))
        if ready:
            raise RuntimeError("curve sell side closed for graduation")
        fee_bps = await self.call_uint(curve, "feeBps()")
        creator_tax_bps = await self.call_uint(curve, "creatorTaxBps()")
        gross = tokens_in * quote_reserve // (token_reserve + tokens_in)
        return gross - (gross * fee_bps // 10_000) - (gross * creator_tax_bps // 10_000)

    async def v4_quote_exact_input(
        self,
        *,
        currency0: str,
        currency1: str,
        fee: int,
        tick_spacing: int,
        hooks: str,
        zero_for_one: bool,
        amount_in: int,
    ) -> tuple[int, int]:
        selector = V4_QUOTE_EXACT_INPUT_SINGLE_SELECTOR
        # One dynamic tuple argument. The tuple head is five PoolKey words + bool +
        # exactAmount + hookData offset (8 words); hookData is empty.
        tuple_head = "".join(
            [
                _word_addr(currency0),
                _word_addr(currency1),
                _word_uint(fee),
                _word_uint(tick_spacing),
                _word_addr(hooks),
                _word_bool(zero_for_one),
                _word_uint(amount_in),
                _word_uint(8 * 32),
            ]
        )
        payload = _word_uint(32) + tuple_head + _word_uint(0)
        raw = await self.eth_call(UNISWAP_V4_QUOTER, "0x" + selector.removeprefix("0x") + payload)
        words = _words(raw)
        if len(words) < 2:
            raise RuntimeError("V4 quoter returned incomplete result")
        return _uint(words[0]), _uint(words[1])

__all__ = ['asyncio', 'json', 'math', 'os', 'time', 'defaultdict', 'deque', 'datetime', 'timezone', 'mean', 'median', 'Any', 'httpx', 'ObservationEventStore', 'ROBINHOOD_CHAIN_ID', 'ROBINHOOD_PUBLIC_RPC', 'ROBINHOOD_CHAIN_PAPER_VERSION', 'PAPER_ONLY', 'LIVE_MONEY_AUTHORITY', 'SIGNING_AVAILABLE', 'TRANSACTION_SUBMISSION_AVAILABLE', 'PAPER_TRADING_AUTHORITY', 'WETH', 'PONS_V1_ACTIVE_FACTORY', 'PONS_V1_LEGACY_FACTORY', 'PONS_V1_START_BLOCK', 'PONS_V1_TOKEN_LAUNCHED_TOPIC', 'PONS_V1_LEGACY_START_BLOCK', 'UNISWAP_V3_FACTORY', 'UNISWAP_V3_QUOTER_V2', 'PONS_V1_SWAP_ROUTER', 'PONS_V2_FACTORY', 'PONS_V2_MEME_HOOK', 'UNISWAP_V4_POOL_MANAGER', 'UNISWAP_V4_QUOTER', 'BLOCKSCOUT_API', 'ROBINHOOD_STOCK_ASSETS_API', 'V3_QUOTE_EXACT_INPUT_SINGLE_SELECTOR', 'V4_QUOTE_EXACT_INPUT_SINGLE_SELECTOR', 'V3_POOL_CREATED_TOPIC', 'V3_SWAP_TOPIC', 'V4_SWAP_TOPIC', 'PONS_V2_TOKEN_LAUNCHED_TOPIC', 'PONS_V2_CURVE_BUY_TOPIC', 'PONS_V2_CURVE_SELL_TOPIC', 'BOOTSTRAP_PAPER_FRACTION', 'POSITION_FRACTION_GRID', 'MAX_POSITION_FRACTION', 'MAX_OPEN_EXPOSURE_FRACTION', 'MIN_CONTEXT_SAMPLES', 'MAX_CHASE_FRACTION', 'MAX_IMMEDIATE_ROUND_TRIP_COST', 'STOP_LOSS_FRACTION', 'HARVEST_FRACTION', 'MAX_HOLD_SECONDS', 'POLL_SECONDS', 'MAX_BLOCKS_PER_POLL', 'MAX_TRACKED_V3_POOLS', 'MAX_TRACKED_V2_CURVES', 'LIVE_LAG_BLOCKS', 'KNOWN_NON_ACTORS', '_utcnow', '_clean_address', '_topic_address', '_word_address', '_words', '_uint', '_signed', '_hex_quantity', '_word_uint', '_word_bool', '_word_addr', '_finite', '_trimmed_ex_best', '_expected_log_growth', 'classify_context_returns', 'V3Pool', 'V2Curve', 'RobinhoodRpc']