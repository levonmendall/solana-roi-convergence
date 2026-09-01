from __future__ import annotations

from dataclasses import asdict
from typing import Any

from .ingestion import IngestionDecision, LiveEvidenceIngestionService, NormalizedSwap
from .models import Confirmation, WalletTier, WalletTouch
from .live_collectors import LiveRiskCollectors


class CollectingLiveEvidenceIngestionService(LiveEvidenceIngestionService):
    """Production shadow service: preserve original first touch, refresh risk, then fail closed."""

    def __init__(self, *args: Any, collectors: LiveRiskCollectors, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.collectors = collectors

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

        await self.collectors.refresh(swap.token_mint, swap.received_at, current_swap=swap)

        profile = self.registry.get(swap.wallet)
        if profile is None:
            return self._decision(swap, "record_only", "wallet profile unavailable")
        if not profile.historically_eligible or profile.tier not in {WalletTier.S, WalletTier.A}:
            return self._decision(swap, "record_only", "wallet not eligible for frozen scout cohort")
        if swap.side != "buy":
            return self._decision(swap, "record_only", "sell evidence retained but cannot initiate/confirm entry")

        effective_entity_id = self._effective_entity_id(profile, as_of=swap.received_at)
        first = self.store.first_touch(swap.token_mint)
        first_claimed_now = False
        if first is None:
            first_claimed_now = self.store.claim_first_touch(
                token_mint=swap.token_mint,
                signature=swap.signature,
                wallet=swap.wallet,
                entity_id=effective_entity_id,
                tier=profile.tier.value,
                observed_at=swap.observed_at.isoformat(),
                reference_price_sol=swap.reference_price_sol,
            )
            first = self.store.first_touch(swap.token_mint)
        if first is None:
            return self._decision(swap, "record_only", "first-touch claim unresolved")

        if not first_claimed_now and self._same_entity(
            first_wallet=str(first["wallet"]),
            first_entity_id=str(first["entity_id"]),
            current_wallet=swap.wallet,
            current_entity_id=effective_entity_id,
            as_of=swap.received_at,
        ):
            return self._decision(swap, "record_only", "same economic entity cannot confirm its own first touch")

        scout_wallet = str(first["wallet"])
        scout_entity_id = str(first["entity_id"])
        risk = await self.risk_provider.snapshot(
            swap.token_mint,
            swap.received_at,
            scout_wallet=scout_wallet,
            scout_entity_id=scout_entity_id,
        )
        if risk is None:
            label = "first_touch_pending_risk" if first_claimed_now else "confirmation_pending_risk"
            return self._decision(
                swap,
                "record_only",
                f"{label}; original first touch preserved; complete fresh token risk evidence unavailable",
            )
        self.store.append(
            "risk_snapshot",
            swap.received_at.isoformat(),
            {
                "token_mint": swap.token_mint,
                "scout_wallet": scout_wallet,
                "decision_at": swap.received_at.isoformat(),
                "risk": asdict(risk),
            },
        )

        if first_claimed_now:
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
                return self._decision(swap, "first_touch", "eligible wallet claimed original tracked first buy")
            reason = "shadow first touch; paper cohort disabled"
            if not risk.clean:
                reason += "; risk_veto:" + ",".join(risk.blockers)
            return self._decision(swap, "shadow_first_touch", reason)

        confirmation = Confirmation(
            token_mint=swap.token_mint,
            wallet=swap.wallet,
            entity_id=effective_entity_id,
            observed_at=swap.observed_at,
            reference_price=swap.reference_price_sol,
            historically_eligible=profile.historically_eligible,
        )
        if self.promote_paper_signals:
            if swap.token_mint not in self.engine.strategy.candidates:
                return self._decision(swap, "record_only", "original first touch failed closed; no paper candidate exists")
            self.engine.on_confirmation(confirmation, risk)
            return self._decision(swap, "confirmation", "independent eligible wallet buy observed")
        reason = "shadow independent confirmation; paper cohort disabled"
        if not risk.clean:
            reason += "; risk_veto:" + ",".join(risk.blockers)
        return self._decision(swap, "shadow_confirmation", reason)
