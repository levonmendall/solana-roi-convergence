from __future__ import annotations

import base64
import hashlib
import json
import math
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Callable

from .observation import WSOL_MINT
from .observation_store import ObservationEventStore
from .quote import (
    LAMPORTS_PER_SOL,
    ExecutableQuote,
    ExecutableQuoteLedger,
    JupiterQuoteOnlyClient,
    QuoteCertificationGate,
    QuoteCertificationPolicy,
)

_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


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


def _base58_decode(value: str) -> bytes:
    number = 0
    for char in value:
        index = _BASE58_ALPHABET.find(char)
        if index < 0:
            raise ValueError("invalid base58 character")
        number = number * 58 + index
    payload = number.to_bytes((number.bit_length() + 7) // 8, "big") if number else b""
    leading_zeroes = len(value) - len(value.lstrip("1"))
    return b"\x00" * leading_zeroes + payload


def validate_solana_public_key(value: str) -> str:
    key = value.strip()
    if not key:
        raise ValueError("SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY is required")
    if len(_base58_decode(key)) != 32:
        raise ValueError("shadow wallet must decode to a 32-byte Solana public key")
    return key


@dataclass(frozen=True, slots=True)
class ShadowExecutionObservation:
    token_mint: str
    stage: str
    shadow_wallet: str
    observed_at: datetime
    completed_at: datetime
    input_lamports: int
    transaction_built: bool
    transaction_sha256: str | None
    transaction_size_bytes: int | None
    last_valid_block_height: int | None
    router: str
    order_out_token_units: float | None
    order_effective_price_sol: float | None
    order_drift_fraction: float | None
    signature_fee_lamports: int | None
    prioritization_fee_lamports: int | None
    rent_fee_lamports: int | None
    simulation_ok: bool
    units_consumed: int | None
    simulation_slot: int | None
    logs_count: int
    total_latency_ms: float
    error: str | None


class ShadowExecutionLedger:
    def __init__(self, store: ObservationEventStore):
        self.store = store
        with store._lock, store.db:
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS shadow_execution_observations ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, token_mint TEXT NOT NULL, stage TEXT NOT NULL, "
                "shadow_wallet TEXT NOT NULL, observed_at TEXT NOT NULL, completed_at TEXT NOT NULL, "
                "input_lamports INTEGER NOT NULL, transaction_built INTEGER NOT NULL, "
                "transaction_sha256 TEXT, transaction_size_bytes INTEGER, last_valid_block_height INTEGER, "
                "router TEXT NOT NULL, order_out_token_units REAL, order_effective_price_sol REAL, "
                "order_drift_fraction REAL, signature_fee_lamports INTEGER, prioritization_fee_lamports INTEGER, "
                "rent_fee_lamports INTEGER, simulation_ok INTEGER NOT NULL, units_consumed INTEGER, "
                "simulation_slot INTEGER, logs_count INTEGER NOT NULL, total_latency_ms REAL NOT NULL, error TEXT)"
            )
            store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_shadow_execution_completed ON shadow_execution_observations(completed_at)"
            )

    def record(self, observation: ShadowExecutionObservation) -> None:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT INTO shadow_execution_observations("
                "token_mint, stage, shadow_wallet, observed_at, completed_at, input_lamports, transaction_built, "
                "transaction_sha256, transaction_size_bytes, last_valid_block_height, router, order_out_token_units, "
                "order_effective_price_sol, order_drift_fraction, signature_fee_lamports, prioritization_fee_lamports, "
                "rent_fee_lamports, simulation_ok, units_consumed, simulation_slot, logs_count, total_latency_ms, error) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    observation.token_mint,
                    observation.stage,
                    observation.shadow_wallet,
                    observation.observed_at.isoformat(),
                    observation.completed_at.isoformat(),
                    observation.input_lamports,
                    1 if observation.transaction_built else 0,
                    observation.transaction_sha256,
                    observation.transaction_size_bytes,
                    observation.last_valid_block_height,
                    observation.router,
                    observation.order_out_token_units,
                    observation.order_effective_price_sol,
                    observation.order_drift_fraction,
                    observation.signature_fee_lamports,
                    observation.prioritization_fee_lamports,
                    observation.rent_fee_lamports,
                    1 if observation.simulation_ok else 0,
                    observation.units_consumed,
                    observation.simulation_slot,
                    observation.logs_count,
                    observation.total_latency_ms,
                    observation.error,
                ),
            )
        self.store.append("shadow_execution_observation", observation.completed_at.isoformat(), asdict(observation))

    def recent(self, limit: int = 500) -> list[dict[str, Any]]:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT token_mint, stage, shadow_wallet, observed_at, completed_at, input_lamports, "
                "transaction_built, transaction_sha256, transaction_size_bytes, last_valid_block_height, router, "
                "order_out_token_units, order_effective_price_sol, order_drift_fraction, signature_fee_lamports, "
                "prioritization_fee_lamports, rent_fee_lamports, simulation_ok, units_consumed, simulation_slot, "
                "logs_count, total_latency_ms, error FROM shadow_execution_observations ORDER BY id DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["transaction_built"] = bool(item["transaction_built"])
            item["simulation_ok"] = bool(item["simulation_ok"])
            result.append(item)
        return result


class JupiterShadowTransactionSimulator:
    """Build and simulate an exact unsigned Jupiter transaction for one public key.

    This class contains no signing or submission method and accepts no private
    key material. Solana simulateTransaction is called with sigVerify=false.
    """

    def __init__(
        self,
        *,
        jupiter_api_key: str,
        shadow_wallet_public_key: str,
        http_client: Any,
        rpc: Any,
        now_fn: Callable[[], datetime] = utcnow,
        perf_fn: Callable[[], float] = time.perf_counter,
    ):
        if not jupiter_api_key:
            raise ValueError("JUPITER_API_KEY is required")
        self.jupiter_api_key = jupiter_api_key
        self.shadow_wallet = validate_solana_public_key(shadow_wallet_public_key)
        self.http_client = http_client
        self.rpc = rpc
        self.now_fn = now_fn
        self.perf_fn = perf_fn

    async def observe(self, quote: ExecutableQuote) -> ShadowExecutionObservation:
        started = self.perf_fn()
        observed_at = self.now_fn()
        input_lamports = max(1, int(round(quote.input_sol * LAMPORTS_PER_SOL)))
        transaction_built = False
        transaction_sha256: str | None = None
        transaction_size_bytes: int | None = None
        last_valid_block_height: int | None = None
        router = "unknown"
        order_out_units: float | None = None
        order_effective_price: float | None = None
        order_drift: float | None = None
        signature_fee: int | None = None
        priority_fee: int | None = None
        rent_fee: int | None = None
        simulation_ok = False
        units_consumed: int | None = None
        simulation_slot: int | None = None
        logs_count = 0
        error: str | None = None

        try:
            response = await self.http_client.get(
                "https://api.jup.ag/swap/v2/order",
                params={
                    "inputMint": WSOL_MINT,
                    "outputMint": quote.token_mint,
                    "amount": str(input_lamports),
                    "taker": self.shadow_wallet,
                },
                headers={"x-api-key": self.jupiter_api_key, "Accept": "application/json"},
            )
            response.raise_for_status()
            order = response.json()
            if not isinstance(order, dict):
                raise RuntimeError("Jupiter order response is not an object")
            router = str(order.get("router") or "unknown")
            signature_fee = int(order["signatureFeeLamports"]) if order.get("signatureFeeLamports") is not None else None
            priority_fee = int(order["prioritizationFeeLamports"]) if order.get("prioritizationFeeLamports") is not None else None
            rent_fee = int(order["rentFeeLamports"]) if order.get("rentFeeLamports") is not None else None
            last_valid_block_height = int(order["lastValidBlockHeight"]) if order.get("lastValidBlockHeight") is not None else None
            if order.get("outAmount") is not None:
                order_out_units = int(order["outAmount"]) / (10 ** quote.token_decimals)
                if order_out_units > 0:
                    order_effective_price = (input_lamports / LAMPORTS_PER_SOL) / order_out_units
                    order_drift = order_effective_price / quote.scout_reference_price_sol - 1.0
            transaction = order.get("transaction")
            if not isinstance(transaction, str) or not transaction:
                error = str(order.get("errorMessage") or order.get("error") or "Jupiter did not return an assembled transaction")[:1000]
            else:
                raw_tx = base64.b64decode(transaction, validate=True)
                transaction_built = True
                transaction_size_bytes = len(raw_tx)
                transaction_sha256 = hashlib.sha256(raw_tx).hexdigest()
                simulation = await self.rpc.call(
                    "simulateTransaction",
                    [
                        transaction,
                        {
                            "encoding": "base64",
                            "sigVerify": False,
                            "replaceRecentBlockhash": True,
                            "commitment": "processed",
                        },
                    ],
                )
                if not isinstance(simulation, dict):
                    raise RuntimeError("simulateTransaction result unavailable")
                context = simulation.get("context")
                if isinstance(context, dict) and context.get("slot") is not None:
                    simulation_slot = int(context["slot"])
                value = simulation.get("value")
                if not isinstance(value, dict):
                    raise RuntimeError("simulateTransaction value unavailable")
                err = value.get("err")
                logs = value.get("logs")
                logs_count = len(logs) if isinstance(logs, list) else 0
                if value.get("unitsConsumed") is not None:
                    units_consumed = int(value["unitsConsumed"])
                simulation_ok = err is None
                if err is not None:
                    error = json.dumps(err, sort_keys=True, separators=(",", ":"), default=str)[:1000]
        except Exception as exc:
            error = f"{type(exc).__name__}:{str(exc)}"[:1000]

        completed_at = self.now_fn()
        return ShadowExecutionObservation(
            token_mint=quote.token_mint,
            stage=quote.stage,
            shadow_wallet=self.shadow_wallet,
            observed_at=observed_at,
            completed_at=completed_at,
            input_lamports=input_lamports,
            transaction_built=transaction_built,
            transaction_sha256=transaction_sha256,
            transaction_size_bytes=transaction_size_bytes,
            last_valid_block_height=last_valid_block_height,
            router=router,
            order_out_token_units=order_out_units,
            order_effective_price_sol=order_effective_price,
            order_drift_fraction=order_drift,
            signature_fee_lamports=signature_fee,
            prioritization_fee_lamports=priority_fee,
            rent_fee_lamports=rent_fee,
            simulation_ok=simulation_ok,
            units_consumed=units_consumed,
            simulation_slot=simulation_slot,
            logs_count=logs_count,
            total_latency_ms=max(0.0, (self.perf_fn() - started) * 1000.0),
            error=error,
        )


@dataclass(frozen=True, slots=True)
class ShadowExecutionCertificationPolicy:
    min_samples: int = 100
    min_simulation_success_fraction: float = 0.95
    max_p95_shadow_latency_ms: float = 3_000.0
    max_transaction_size_bytes: int = 1232


class ShadowExecutionCertificationGate:
    def __init__(
        self,
        ledger: ShadowExecutionLedger,
        *,
        shadow_wallet_public_key: str | None,
        policy: ShadowExecutionCertificationPolicy | None = None,
    ):
        self.ledger = ledger
        self.shadow_wallet_public_key = shadow_wallet_public_key.strip() if shadow_wallet_public_key else ""
        self.policy = policy or ShadowExecutionCertificationPolicy()

    def status(self, limit: int = 500) -> dict[str, Any]:
        rows = self.ledger.recent(limit)
        configured = False
        if self.shadow_wallet_public_key:
            try:
                validate_solana_public_key(self.shadow_wallet_public_key)
                configured = True
            except ValueError:
                configured = False
        successful = [
            row
            for row in rows
            if row["transaction_built"]
            and row["simulation_ok"]
            and row.get("transaction_size_bytes") is not None
            and int(row["transaction_size_bytes"]) <= self.policy.max_transaction_size_bytes
        ]
        success_fraction = len(successful) / len(rows) if rows else 0.0
        p95_latency = _percentile([float(row["total_latency_ms"]) for row in rows], 0.95)
        certified = bool(
            configured
            and len(rows) >= self.policy.min_samples
            and success_fraction >= self.policy.min_simulation_success_fraction
            and p95_latency is not None
            and p95_latency <= self.policy.max_p95_shadow_latency_ms
        )
        return {
            "certified": certified,
            "configured": configured,
            "private_key_access": False,
            "signing_available": False,
            "submission_available": False,
            "sample_count": len(rows),
            "simulation_success_count": len(successful),
            "simulation_success_fraction": success_fraction,
            "p95_shadow_execution_latency_ms": p95_latency,
            "requirements": asdict(self.policy),
        }


@dataclass(frozen=True, slots=True)
class ShadowAwareQuoteCertificationPolicy:
    quote: QuoteCertificationPolicy = field(default_factory=QuoteCertificationPolicy)
    shadow: ShadowExecutionCertificationPolicy = field(default_factory=ShadowExecutionCertificationPolicy)


class ShadowAwareQuoteCertificationGate:
    def __init__(
        self,
        quote_gate: QuoteCertificationGate,
        shadow_gate: ShadowExecutionCertificationGate,
    ):
        self.quote_gate = quote_gate
        self.shadow_gate = shadow_gate
        self.policy = ShadowAwareQuoteCertificationPolicy(quote=quote_gate.policy, shadow=shadow_gate.policy)

    def status(self, limit: int = 500) -> dict[str, Any]:
        quote = self.quote_gate.status(limit)
        shadow = self.shadow_gate.status(limit)
        return {
            "certified": bool(quote["certified"] and shadow["certified"]),
            "automatic_activation": False,
            "measurement_path": "chain event -> complete/fresh risk decision -> amount-specific Jupiter quote -> unsigned taker transaction -> mainnet simulateTransaction",
            "quote": quote,
            "shadow_execution": shadow,
            "requirements": asdict(self.policy),
        }


class ShadowWalletExecutableQuoteHandoff:
    """Quote, build and simulate. No private key, signing, execute, or send path exists."""

    def __init__(
        self,
        *,
        store: ObservationEventStore,
        client: JupiterQuoteOnlyClient | None,
        simulator: JupiterShadowTransactionSimulator | None,
        full_position_notional_fn: Callable[[], float],
        max_chase_fraction: float,
    ):
        self.store = store
        self.client = client
        self.simulator = simulator
        self.ledger = ExecutableQuoteLedger(store)
        self.shadow_ledger = ShadowExecutionLedger(store)
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
        if self.client is None or self.simulator is None:
            self.store.append(
                "execution_quote_unavailable",
                utcnow().isoformat(),
                {
                    "token_mint": token_mint,
                    "stage": stage,
                    "reason": "Jupiter/Helius credentials or dedicated shadow wallet public key unavailable",
                },
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
            shadow = await self.simulator.observe(quote)
            self.shadow_ledger.record(shadow)
            shadow_price_ok = bool(
                shadow.order_drift_fraction is not None
                and shadow.order_drift_fraction <= self.max_chase_fraction
            )
            shadow_ok = bool(shadow.transaction_built and shadow.simulation_ok and shadow_price_ok)
            final_quote = replace(
                quote,
                received_at=shadow.completed_at,
                quote_latency_ms=quote.quote_latency_ms + shadow.total_latency_ms,
                chain_to_quote_ms=quote.chain_to_quote_ms + shadow.total_latency_ms,
                usable=bool(quote.usable and shadow_ok),
                reason=(
                    quote.reason
                    if quote.usable and shadow_ok
                    else (
                        "shadow transaction failed mainnet execution plausibility: "
                        + (shadow.error or "assembled transaction unavailable, simulation failed, or order exceeds chase ceiling")
                    )
                ),
            )
        except Exception as exc:
            self.store.append(
                "execution_quote_error",
                utcnow().isoformat(),
                {"token_mint": token_mint, "stage": stage, "error_type": type(exc).__name__, "error": str(exc)[:500]},
            )
            return None
        self.ledger.record(final_quote)
        return final_quote
