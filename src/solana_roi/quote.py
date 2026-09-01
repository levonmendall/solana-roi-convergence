from __future__ import annotations

import math
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Callable

import httpx

from .live_collectors import HeliusJsonRpcClient
from .observation import WSOL_MINT
from .observation_store import ObservationEventStore

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
LAMPORTS_PER_SOL = 1_000_000_000
USDC_DECIMALS = 6


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * q
    lo, hi = math.floor(pos), math.ceil(pos)
    if lo == hi:
        return ordered[lo]
    weight = pos - lo
    return ordered[lo] * (1 - weight) + ordered[hi] * weight


@dataclass(frozen=True, slots=True)
class ExecutableQuote:
    token_mint: str
    stage: str
    requested_notional_usd: float
    input_sol: float
    sol_usd: float
    output_token_units: float
    effective_price_sol: float
    scout_reference_price_sol: float
    drift_fraction: float
    router: str
    fee_bps: int
    token_decimals: int
    quoted_at: datetime
    received_at: datetime
    quote_latency_ms: float
    chain_to_quote_ms: float
    usable: bool
    reason: str


class ExecutableQuoteLedger:
    def __init__(self, store: ObservationEventStore):
        self.store = store
        with store._lock, store.db:
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS execution_quote_observations ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, token_mint TEXT NOT NULL, stage TEXT NOT NULL, "
                "requested_notional_usd REAL NOT NULL, input_sol REAL NOT NULL, sol_usd REAL NOT NULL, "
                "output_token_units REAL NOT NULL, effective_price_sol REAL NOT NULL, "
                "scout_reference_price_sol REAL NOT NULL, drift_fraction REAL NOT NULL, router TEXT NOT NULL, "
                "fee_bps INTEGER NOT NULL, token_decimals INTEGER NOT NULL, quoted_at TEXT NOT NULL, "
                "received_at TEXT NOT NULL, quote_latency_ms REAL NOT NULL, chain_to_quote_ms REAL NOT NULL, "
                "usable INTEGER NOT NULL, reason TEXT NOT NULL)"
            )
            store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_execution_quote_received ON execution_quote_observations(received_at)"
            )
            store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_execution_quote_token ON execution_quote_observations(token_mint, received_at)"
            )

    def record(self, quote: ExecutableQuote) -> None:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT INTO execution_quote_observations("
                "token_mint, stage, requested_notional_usd, input_sol, sol_usd, output_token_units, "
                "effective_price_sol, scout_reference_price_sol, drift_fraction, router, fee_bps, token_decimals, "
                "quoted_at, received_at, quote_latency_ms, chain_to_quote_ms, usable, reason) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    quote.token_mint, quote.stage, quote.requested_notional_usd, quote.input_sol, quote.sol_usd,
                    quote.output_token_units, quote.effective_price_sol, quote.scout_reference_price_sol,
                    quote.drift_fraction, quote.router, quote.fee_bps, quote.token_decimals,
                    quote.quoted_at.isoformat(), quote.received_at.isoformat(), quote.quote_latency_ms,
                    quote.chain_to_quote_ms, 1 if quote.usable else 0, quote.reason,
                ),
            )
        self.store.append("execution_quote_observation", quote.received_at.isoformat(), asdict(quote))

    def recent(self, limit: int = 500) -> list[dict[str, Any]]:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT token_mint, stage, requested_notional_usd, input_sol, sol_usd, output_token_units, "
                "effective_price_sol, scout_reference_price_sol, drift_fraction, router, fee_bps, token_decimals, "
                "quoted_at, received_at, quote_latency_ms, chain_to_quote_ms, usable, reason "
                "FROM execution_quote_observations ORDER BY id DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["usable"] = bool(item["usable"])
            result.append(item)
        return result


class JupiterQuoteOnlyClient:
    """Amount-specific quote-only client. Never requests execution and never signs anything."""

    def __init__(
        self,
        *,
        jupiter_api_key: str,
        helius_api_key: str,
        client: Any | None = None,
        rpc: Any | None = None,
        now_fn: Callable[[], datetime] = utcnow,
        perf_fn: Callable[[], float] = time.perf_counter,
    ):
        if not jupiter_api_key:
            raise ValueError("JUPITER_API_KEY is required for quote-only execution observations")
        if not helius_api_key:
            raise ValueError("HELIUS_API_KEY is required to resolve token decimals")
        self.jupiter_api_key = jupiter_api_key
        self.client = client or httpx.AsyncClient(timeout=2.0)
        self.rpc = rpc or HeliusJsonRpcClient(helius_api_key, timeout_seconds=1.5)
        self.now_fn = now_fn
        self.perf_fn = perf_fn
        self._sol_usd_cache: tuple[datetime, float] | None = None

    async def _order(self, input_mint: str, output_mint: str, amount: int) -> dict[str, Any]:
        response = await self.client.get(
            "https://api.jup.ag/swap/v2/order",
            params={"inputMint": input_mint, "outputMint": output_mint, "amount": str(int(amount))},
            headers={"x-api-key": self.jupiter_api_key, "Accept": "application/json"},
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or not payload.get("outAmount"):
            raise RuntimeError(f"Jupiter quote unavailable: {payload}")
        return payload

    async def _sol_usd(self) -> float:
        now = self.now_fn()
        if self._sol_usd_cache is not None and (now - self._sol_usd_cache[0]).total_seconds() <= 2.0:
            return self._sol_usd_cache[1]
        input_lamports = 100_000_000
        quote = await self._order(WSOL_MINT, USDC_MINT, input_lamports)
        out_usdc = int(quote["outAmount"]) / (10 ** USDC_DECIMALS)
        price = out_usdc / (input_lamports / LAMPORTS_PER_SOL)
        if price <= 0:
            raise RuntimeError("invalid SOL/USD quote")
        self._sol_usd_cache = (now, price)
        return price

    async def _token_decimals(self, mint: str) -> int:
        result = await self.rpc.call("getTokenSupply", [mint, {"commitment": "confirmed"}])
        value = result.get("value") if isinstance(result, dict) else None
        if not isinstance(value, dict) or value.get("decimals") is None:
            raise RuntimeError("token decimals unavailable")
        decimals = int(value["decimals"])
        if decimals < 0 or decimals > 18:
            raise RuntimeError("invalid token decimals")
        return decimals

    async def quote_buy(
        self,
        *,
        token_mint: str,
        stage: str,
        notional_usd: float,
        scout_reference_price_sol: float,
        trigger_observed_at: datetime,
        max_chase_fraction: float,
    ) -> ExecutableQuote:
        started = self.perf_fn()
        quoted_at = self.now_fn()
        sol_usd = await self._sol_usd()
        input_sol = max(1e-9, notional_usd / sol_usd)
        lamports = max(1, int(input_sol * LAMPORTS_PER_SOL))
        decimals = await self._token_decimals(token_mint)
        quote = await self._order(WSOL_MINT, token_mint, lamports)
        received_at = self.now_fn()
        quote_latency_ms = max(0.0, (self.perf_fn() - started) * 1000.0)
        output_units = int(quote["outAmount"]) / (10 ** decimals)
        if output_units <= 0:
            raise RuntimeError("Jupiter returned zero token output")
        actual_input_sol = lamports / LAMPORTS_PER_SOL
        effective_price = actual_input_sol / output_units
        drift = effective_price / scout_reference_price_sol - 1.0
        usable = drift <= max_chase_fraction
        return ExecutableQuote(
            token_mint=token_mint,
            stage=stage,
            requested_notional_usd=notional_usd,
            input_sol=actual_input_sol,
            sol_usd=sol_usd,
            output_token_units=output_units,
            effective_price_sol=effective_price,
            scout_reference_price_sol=scout_reference_price_sol,
            drift_fraction=drift,
            router=str(quote.get("router") or "unknown"),
            fee_bps=int(quote.get("feeBps") or 0),
            token_decimals=decimals,
            quoted_at=quoted_at,
            received_at=received_at,
            quote_latency_ms=quote_latency_ms,
            chain_to_quote_ms=max(0.0, (received_at - trigger_observed_at).total_seconds() * 1000.0),
            usable=usable,
            reason="within chase ceiling" if usable else "post-risk quote exceeds 15% chase ceiling",
        )


class ShadowExecutableQuoteHandoff:
    def __init__(self, *, store: ObservationEventStore, client: JupiterQuoteOnlyClient | None, full_position_notional_fn: Callable[[], float], max_chase_fraction: float):
        self.store = store
        self.client = client
        self.ledger = ExecutableQuoteLedger(store)
        self.full_position_notional_fn = full_position_notional_fn
        self.max_chase_fraction = max_chase_fraction

    async def observe(
        self,
        *,
        token_mint: str,
        stage: str,
        fraction_of_full_position: float,
        scout_reference_price_sol: float,
        trigger_observed_at: datetime,
    ) -> ExecutableQuote | None:
        if self.client is None:
            self.store.append(
                "execution_quote_unavailable",
                utcnow().isoformat(),
                {"token_mint": token_mint, "stage": stage, "reason": "Jupiter/Helius quote credentials unavailable"},
            )
            return None
        notional = max(0.0, self.full_position_notional_fn() * fraction_of_full_position)
        if notional <= 0:
            return None
        try:
            quote = await self.client.quote_buy(
                token_mint=token_mint,
                stage=stage,
                notional_usd=notional,
                scout_reference_price_sol=scout_reference_price_sol,
                trigger_observed_at=trigger_observed_at,
                max_chase_fraction=self.max_chase_fraction,
            )
        except Exception as exc:
            self.store.append(
                "execution_quote_error",
                utcnow().isoformat(),
                {"token_mint": token_mint, "stage": stage, "error_type": type(exc).__name__, "error": str(exc)[:500]},
            )
            return None
        self.ledger.record(quote)
        return quote


@dataclass(frozen=True, slots=True)
class QuoteCertificationPolicy:
    min_samples: int = 100
    min_quote_success_fraction: float = 0.95
    max_p95_quote_latency_ms: float = 2_000.0
    max_p95_chain_to_quote_ms: float = 5_000.0


class QuoteCertificationGate:
    def __init__(self, ledger: ExecutableQuoteLedger, *, policy: QuoteCertificationPolicy | None = None):
        self.ledger = ledger
        self.policy = policy or QuoteCertificationPolicy()

    def status(self, limit: int = 500) -> dict[str, object]:
        rows = self.ledger.recent(limit)
        usable = [row for row in rows if row["usable"]]
        success_fraction = len(usable) / len(rows) if rows else 0.0
        p95_quote = _percentile([float(row["quote_latency_ms"]) for row in rows], 0.95)
        p95_chain = _percentile([float(row["chain_to_quote_ms"]) for row in rows], 0.95)
        certified = bool(
            len(rows) >= self.policy.min_samples
            and success_fraction >= self.policy.min_quote_success_fraction
            and p95_quote is not None and p95_quote <= self.policy.max_p95_quote_latency_ms
            and p95_chain is not None and p95_chain <= self.policy.max_p95_chain_to_quote_ms
        )
        return {
            "certified": certified,
            "automatic_activation": False,
            "sample_count": len(rows),
            "usable_count": len(usable),
            "usable_fraction": success_fraction,
            "p95_quote_latency_ms": p95_quote,
            "p95_chain_to_quote_ms": p95_chain,
            "requirements": asdict(self.policy),
        }
