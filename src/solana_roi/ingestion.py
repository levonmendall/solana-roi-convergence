from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from .engine import PaperTradingEngine
from .models import Confirmation, RiskSnapshot, WalletTier, WalletTouch
from .storage import AppendOnlyEventStore


LAMPORTS_PER_SOL = 1_000_000_000


@dataclass(frozen=True, slots=True)
class WalletProfile:
    wallet: str
    entity_id: str
    tier: WalletTier
    first_touch_sample_size: int
    historically_eligible: bool
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class NormalizedSwap:
    signature: str
    slot: int
    observed_at: datetime
    received_at: datetime
    wallet: str
    token_mint: str
    side: str
    token_amount: float
    native_amount_sol: float
    reference_price_sol: float
    source: str = "helius-enhanced-webhook"

    @property
    def ingestion_latency_ms(self) -> float:
        return max(0.0, (self.received_at - self.observed_at).total_seconds() * 1000.0)


@dataclass(frozen=True, slots=True)
class IngestionDecision:
    signature: str
    token_mint: str
    wallet: str
    decision: str
    reason: str
    observed_at: datetime
    ingestion_latency_ms: float


class RiskEvidenceProvider(Protocol):
    async def snapshot(
        self,
        token_mint: str,
        observed_at: datetime,
        *,
        scout_wallet: str | None = None,
        scout_entity_id: str | None = None,
    ) -> RiskSnapshot | None: ...


class UnavailableRiskEvidenceProvider:
    async def snapshot(
        self,
        token_mint: str,
        observed_at: datetime,
        *,
        scout_wallet: str | None = None,
        scout_entity_id: str | None = None,
    ) -> RiskSnapshot | None:
        return None


class StaticRiskEvidenceProvider:
    """Deterministic provider used by tests and offline replay only."""

    def __init__(self, snapshot: RiskSnapshot):
        self.value = snapshot

    async def snapshot(
        self,
        token_mint: str,
        observed_at: datetime,
        *,
        scout_wallet: str | None = None,
        scout_entity_id: str | None = None,
    ) -> RiskSnapshot | None:
        return self.value


class WalletProfileRegistry:
    def __init__(self, store: AppendOnlyEventStore):
        self.store = store

    def register(self, profile: WalletProfile) -> None:
        self.store.upsert_wallet_profile(
            wallet=profile.wallet,
            entity_id=profile.entity_id,
            tier=profile.tier.value,
            first_touch_sample_size=profile.first_touch_sample_size,
            historically_eligible=profile.historically_eligible,
            updated_at=profile.updated_at.isoformat(),
        )

    def get(self, wallet: str) -> WalletProfile | None:
        row = self.store.wallet_profile(wallet)
        if row is None:
            return None
        return WalletProfile(
            wallet=str(row["wallet"]),
            entity_id=str(row["entity_id"]),
            tier=WalletTier(str(row["tier"])),
            first_touch_sample_size=int(row["first_touch_sample_size"]),
            historically_eligible=bool(row["historically_eligible"]),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
        )


class HeliusEnhancedWebhookParser:
    """Normalize unambiguous SOL<->SPL trades from supported enhanced webhook types."""

    SWAP_TYPES = {"SWAP", "SWAP_EXACT_OUT", "SWAP_WITH_PRICE_IMPACT"}
    DIRECT_TRADE_TYPES = {"BUY", "SELL"}

    @staticmethod
    def _token_amount(leg: dict[str, Any]) -> float | None:
        raw = leg.get("rawTokenAmount")
        if isinstance(raw, dict):
            try:
                amount = float(raw["tokenAmount"])
                decimals = int(raw["decimals"])
            except (KeyError, TypeError, ValueError):
                return None
            scaled = amount / (10 ** decimals)
            return scaled if scaled > 0 else None
        try:
            amount = float(leg.get("tokenAmount") or 0.0)
        except (TypeError, ValueError):
            return None
        return amount if amount > 0 else None

    @staticmethod
    def _native_sol(leg: dict[str, Any] | None) -> float | None:
        if not isinstance(leg, dict):
            return None
        try:
            lamports = float(leg["amount"])
        except (KeyError, TypeError, ValueError):
            return None
        sol = lamports / LAMPORTS_PER_SOL
        return sol if sol > 0 else None

    @staticmethod
    def _wallet_for_legs(fee_payer: str, legs: list[dict[str, Any]]) -> str | None:
        users = {str(row.get("userAccount") or "") for row in legs}
        users.discard("")
        if fee_payer and fee_payer in users:
            return fee_payer
        if len(users) == 1:
            return next(iter(users))
        return None

    @staticmethod
    def _source_label(tx: dict[str, Any], tx_type: str) -> str:
        source = str(tx.get("source") or "UNKNOWN").upper()
        return f"helius-enhanced-webhook:{source}:{tx_type.lower()}"

    def _from_swap_event(
        self,
        tx: dict[str, Any],
        *,
        tx_type: str,
        signature: str,
        slot: int,
        observed_at: datetime,
        received_at: datetime,
        fee_payer: str,
    ) -> NormalizedSwap | None:
        events = tx.get("events")
        swap = events.get("swap") if isinstance(events, dict) else None
        if not isinstance(swap, dict):
            return None
        native_in = self._native_sol(swap.get("nativeInput"))
        native_out = self._native_sol(swap.get("nativeOutput"))
        token_inputs = [row for row in (swap.get("tokenInputs") or []) if isinstance(row, dict)]
        token_outputs = [row for row in (swap.get("tokenOutputs") or []) if isinstance(row, dict)]
        if native_in is not None and token_outputs:
            side, legs, native_amount = "buy", token_outputs, native_in
        elif native_out is not None and token_inputs:
            side, legs, native_amount = "sell", token_inputs, native_out
        else:
            return None
        wallet = self._wallet_for_legs(fee_payer, legs)
        if wallet is None:
            return None
        priced_legs: list[tuple[str, float]] = []
        for leg in legs:
            amount = self._token_amount(leg)
            mint = str(leg.get("mint") or "")
            if amount is not None and mint:
                priced_legs.append((mint, amount))
        if len(priced_legs) != 1:
            return None
        token_mint, token_amount = priced_legs[0]
        return NormalizedSwap(
            signature=signature,
            slot=slot,
            observed_at=observed_at,
            received_at=received_at,
            wallet=wallet,
            token_mint=token_mint,
            side=side,
            token_amount=token_amount,
            native_amount_sol=native_amount,
            reference_price_sol=native_amount / token_amount,
            source=self._source_label(tx, tx_type),
        )

    def _from_direct_transfers(
        self,
        tx: dict[str, Any],
        *,
        tx_type: str,
        signature: str,
        slot: int,
        observed_at: datetime,
        received_at: datetime,
        fee_payer: str,
    ) -> NormalizedSwap | None:
        """Fail-closed fallback for sources such as PUMP_AMM whose type is BUY/SELL.

        Helius can classify Pump AMM transactions as BUY/SELL rather than SWAP. When
        no structured swap event is present, derive only a simple one-mint SOL trade
        from user-level transfers. Ambiguous/multi-mint flow is discarded.
        """
        if tx_type not in self.DIRECT_TRADE_TYPES or not fee_payer:
            return None
        token_transfers = [row for row in (tx.get("tokenTransfers") or []) if isinstance(row, dict)]
        native_transfers = [row for row in (tx.get("nativeTransfers") or []) if isinstance(row, dict)]
        amounts: dict[str, float] = {}
        if tx_type == "BUY":
            for row in token_transfers:
                if str(row.get("toUserAccount") or "") != fee_payer:
                    continue
                mint = str(row.get("mint") or "")
                amount = self._token_amount(row)
                if mint and amount is not None:
                    amounts[mint] = amounts.get(mint, 0.0) + amount
            lamports = sum(
                float(row.get("amount") or 0.0)
                for row in native_transfers
                if str(row.get("fromUserAccount") or "") == fee_payer
            )
            side = "buy"
        else:
            for row in token_transfers:
                if str(row.get("fromUserAccount") or "") != fee_payer:
                    continue
                mint = str(row.get("mint") or "")
                amount = self._token_amount(row)
                if mint and amount is not None:
                    amounts[mint] = amounts.get(mint, 0.0) + amount
            lamports = sum(
                float(row.get("amount") or 0.0)
                for row in native_transfers
                if str(row.get("toUserAccount") or "") == fee_payer
            )
            side = "sell"
        nonzero = [(mint, amount) for mint, amount in amounts.items() if amount > 0]
        native_amount = lamports / LAMPORTS_PER_SOL
        if len(nonzero) != 1 or native_amount <= 0:
            return None
        token_mint, token_amount = nonzero[0]
        return NormalizedSwap(
            signature=signature,
            slot=slot,
            observed_at=observed_at,
            received_at=received_at,
            wallet=fee_payer,
            token_mint=token_mint,
            side=side,
            token_amount=token_amount,
            native_amount_sol=native_amount,
            reference_price_sol=native_amount / token_amount,
            source=self._source_label(tx, tx_type),
        )

    def parse(self, payload: Any, *, received_at: datetime | None = None) -> list[NormalizedSwap]:
        received_at = received_at or datetime.now(timezone.utc)
        transactions = payload if isinstance(payload, list) else [payload]
        rows: list[NormalizedSwap] = []
        for tx in transactions:
            if not isinstance(tx, dict) or tx.get("transactionError"):
                continue
            tx_type = str(tx.get("type") or "").upper()
            if tx_type not in self.SWAP_TYPES | self.DIRECT_TRADE_TYPES:
                continue
            signature = str(tx.get("signature") or "")
            try:
                slot = int(tx["slot"])
                timestamp = int(tx["timestamp"])
            except (KeyError, TypeError, ValueError):
                continue
            if not signature or timestamp <= 0:
                continue
            observed_at = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            fee_payer = str(tx.get("feePayer") or "")
            normalized = self._from_swap_event(
                tx,
                tx_type=tx_type,
                signature=signature,
                slot=slot,
                observed_at=observed_at,
                received_at=received_at,
                fee_payer=fee_payer,
            )
            if normalized is None:
                normalized = self._from_direct_transfers(
                    tx,
                    tx_type=tx_type,
                    signature=signature,
                    slot=slot,
                    observed_at=observed_at,
                    received_at=received_at,
                    fee_payer=fee_payer,
                )
            if normalized is not None:
                rows.append(normalized)
        return rows


class LiveEvidenceIngestionService:
    """Normalize, risk-check, entity-resolve and optionally promote live Solana evidence."""

    def __init__(
        self,
        *,
        engine: PaperTradingEngine,
        store: AppendOnlyEventStore,
        registry: WalletProfileRegistry | None = None,
        risk_provider: RiskEvidenceProvider | None = None,
        entity_resolver: Any | None = None,
        promote_paper_signals: bool = True,
    ):
        self.engine = engine
        self.store = store
        self.registry = registry or WalletProfileRegistry(store)
        self.risk_provider = risk_provider or UnavailableRiskEvidenceProvider()
        self.entity_resolver = entity_resolver
        self.promote_paper_signals = promote_paper_signals

    def _decision(self, swap: NormalizedSwap, decision: str, reason: str) -> IngestionDecision:
        row = IngestionDecision(
            signature=swap.signature,
            token_mint=swap.token_mint,
            wallet=swap.wallet,
            decision=decision,
            reason=reason,
            observed_at=swap.observed_at,
            ingestion_latency_ms=swap.ingestion_latency_ms,
        )
        self.store.append("ingestion_decision", swap.received_at.isoformat(), asdict(row))
        return row

    def _effective_entity_id(self, profile: WalletProfile, *, as_of: datetime) -> str:
        if self.entity_resolver is None:
            return profile.entity_id
        return str(self.entity_resolver.entity_id_for(
            profile.wallet,
            fallback_entity_id=profile.entity_id,
            as_of=as_of,
        ))

    def _same_entity(self, *, first_wallet: str, first_entity_id: str, current_wallet: str, current_entity_id: str, as_of: datetime) -> bool:
        if self.entity_resolver is None:
            return first_entity_id == current_entity_id
        return bool(self.entity_resolver.same_entity(
            first_wallet,
            current_wallet,
            as_of=as_of,
            fallback_entity_a=first_entity_id,
            fallback_entity_b=current_entity_id,
        ))

    async def ingest_swap(self, swap: NormalizedSwap) -> IngestionDecision:
        inserted = self.store.record_swap(
            signature=swap.signature,
            slot=swap.slot,
            observed_at=swap.observed_at.isoformat(),
            received_at=swap.received_at.isoformat(),
            wallet=swap.wallet,
            token_mint=swap.token_mint,
            side=swap.side,
            token_amount=swap.token_amount,
            native_amount_sol=swap.native_amount_sol,
            reference_price_sol=swap.reference_price_sol,
            ingestion_latency_ms=swap.ingestion_latency_ms,
            source=swap.source,
        )
        if not inserted:
            return self._decision(swap, "duplicate", "normalized swap already persisted")
        self.store.append("normalized_swap", swap.received_at.isoformat(), asdict(swap))

        profile = self.registry.get(swap.wallet)
        if profile is None:
            return self._decision(swap, "record_only", "wallet profile unavailable")
        if not profile.historically_eligible or profile.tier not in {WalletTier.S, WalletTier.A}:
            return self._decision(swap, "record_only", "wallet not eligible for frozen scout cohort")
        if swap.side != "buy":
            return self._decision(swap, "record_only", "sell evidence retained but cannot initiate/confirm entry")

        effective_entity_id = self._effective_entity_id(profile, as_of=swap.received_at)
        first = self.store.first_touch(swap.token_mint)
        scout_wallet = str(first["wallet"]) if first is not None else swap.wallet
        scout_entity_id = str(first["entity_id"]) if first is not None else effective_entity_id
        risk = await self.risk_provider.snapshot(
            swap.token_mint,
            swap.received_at,
            scout_wallet=scout_wallet,
            scout_entity_id=scout_entity_id,
        )
        if risk is None:
            return self._decision(swap, "record_only", "complete fresh point-in-time token risk evidence unavailable")
        self.store.append("risk_snapshot", swap.received_at.isoformat(), {
            "token_mint": swap.token_mint,
            "scout_wallet": scout_wallet,
            "decision_at": swap.received_at.isoformat(),
            "risk": asdict(risk),
        })

        if first is None:
            claimed = self.store.claim_first_touch(
                token_mint=swap.token_mint,
                signature=swap.signature,
                wallet=swap.wallet,
                entity_id=effective_entity_id,
                tier=profile.tier.value,
                observed_at=swap.observed_at.isoformat(),
                reference_price_sol=swap.reference_price_sol,
            )
            if not claimed:
                first = self.store.first_touch(swap.token_mint)
            else:
                touch = WalletTouch(
                    token_mint=swap.token_mint,
                    wallet=swap.wallet,
                    entity_id=effective_entity_id,
                    observed_at=swap.observed_at,
                    reference_price=swap.reference_price_sol,
                    market_cap_usd=None,
                    tier=profile.tier,
                    historically_eligible=profile.historically_eligible,
                )
                if self.promote_paper_signals:
                    self.engine.on_first_touch(touch, risk)
                    return self._decision(swap, "first_touch", "eligible wallet claimed first tracked buy")
                reason = "shadow first touch; paper cohort disabled"
                if not risk.clean:
                    reason += "; risk_veto:" + ",".join(risk.blockers)
                return self._decision(swap, "shadow_first_touch", reason)

        first = first or self.store.first_touch(swap.token_mint)
        if first is None:
            return self._decision(swap, "record_only", "first-touch claim unresolved")
        if self._same_entity(
            first_wallet=str(first["wallet"]),
            first_entity_id=str(first["entity_id"]),
            current_wallet=swap.wallet,
            current_entity_id=effective_entity_id,
            as_of=swap.received_at,
        ):
            return self._decision(swap, "record_only", "same economic entity cannot confirm its own first touch")

        confirmation = Confirmation(
            token_mint=swap.token_mint,
            wallet=swap.wallet,
            entity_id=effective_entity_id,
            observed_at=swap.observed_at,
            reference_price=swap.reference_price_sol,
            historically_eligible=profile.historically_eligible,
        )
        if self.promote_paper_signals:
            self.engine.on_confirmation(confirmation, risk)
            return self._decision(swap, "confirmation", "independent eligible wallet buy observed")
        reason = "shadow independent confirmation; paper cohort disabled"
        if not risk.clean:
            reason += "; risk_veto:" + ",".join(risk.blockers)
        return self._decision(swap, "shadow_confirmation", reason)

    async def ingest_webhook(self, payload: Any, *, received_at: datetime | None = None) -> list[IngestionDecision]:
        parser = HeliusEnhancedWebhookParser()
        decisions: list[IngestionDecision] = []
        for swap in parser.parse(payload, received_at=received_at):
            decisions.append(await self.ingest_swap(swap))
        return decisions
