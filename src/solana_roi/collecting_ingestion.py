from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Callable

from .ingestion import IngestionDecision, LiveEvidenceIngestionService, NormalizedSwap
from .models import Confirmation, WalletTier, WalletTouch


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class CollectingLiveEvidenceIngestionService(LiveEvidenceIngestionService):
    """Production shadow service: preserve chronology, collect risk/quotes, then fail closed."""

    def __init__(
        self,
        *args: Any,
        collectors: Any,
        mark_recorder: Any | None = None,
        quote_handoff: Any | None = None,
        decision_clock: Callable[[], datetime] = _utcnow,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self.collectors = collectors
        self.mark_recorder = mark_recorder
        self.quote_handoff = quote_handoff
        self.decision_clock = decision_clock

    async def _quote(
        self,
        *,
        token_mint: str,
        stage: str,
        fraction: float,
        scout_reference_price_sol: float,
        trigger_observed_at: datetime,
    ) -> Any | None:
        if self.quote_handoff is None:
            return None
        return await self.quote_handoff.observe(
            token_mint=token_mint,
            stage=stage,
            fraction_of_full_position=fraction,
            scout_reference_price_sol=scout_reference_price_sol,
            trigger_observed_at=trigger_observed_at,
        )

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
        if self.mark_recorder is not None:
            self.mark_recorder.record_swap_mark(swap)

        await self.collectors.refresh(swap.token_mint, swap.received_at, current_swap=swap)
        decision_at = max(swap.received_at, self.decision_clock())

        profile = self.registry.get(swap.wallet)
        if profile is None:
            return self._decision(swap, "record_only", "wallet profile unavailable")
        if not profile.historically_eligible or profile.tier not in {WalletTier.S, WalletTier.A}:
            return self._decision(swap, "record_only", "wallet not eligible for frozen scout cohort")
        if swap.side != "buy":
            return self._decision(swap, "record_only", "sell evidence retained but cannot initiate/confirm entry")

        effective_entity_id = self._effective_entity_id(profile, as_of=decision_at)
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
            as_of=decision_at,
        ):
            return self._decision(swap, "record_only", "same economic entity cannot confirm its own first touch")

        scout_wallet = str(first["wallet"])
        scout_entity_id = str(first["entity_id"])
        scout_reference_price = float(first["reference_price_sol"])
        risk = await self.risk_provider.snapshot(
            swap.token_mint,
            decision_at,
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
            decision_at.isoformat(),
            {
                "token_mint": swap.token_mint,
                "scout_wallet": scout_wallet,
                "trigger_received_at": swap.received_at.isoformat(),
                "decision_at": decision_at.isoformat(),
                "decision_latency_ms": max(0.0, (decision_at - swap.observed_at).total_seconds() * 1000.0),
                "risk": asdict(risk),
            },
        )

        # `promote_paper_signals` is intentionally not sufficient authority. Until the
        # candidate-level activation gate exists and explicitly authorizes the frozen
        # forward cohort, an accidentally enabled promotion flag must still fail closed.
        if self.promote_paper_signals:
            return self._decision(
                swap,
                "record_only",
                "activation blocked: post-risk reference-price evidence alone cannot authorize paper execution; final forward-cohort activation gate has not authorized this candidate",
            )

        if first_claimed_now:
            quote = None
            if risk.clean and profile.tier is WalletTier.S:
                quote = await self._quote(
                    token_mint=swap.token_mint,
                    stage="starter",
                    fraction=self.engine.config.starter_fraction_of_full_position,
                    scout_reference_price_sol=scout_reference_price,
                    trigger_observed_at=swap.observed_at,
                )
            reason = "shadow first touch; paper cohort disabled"
            if not risk.clean:
                reason += "; risk_veto:" + ",".join(risk.blockers)
            elif profile.tier is WalletTier.S:
                if quote is None:
                    reason += "; executable quote unavailable"
                elif not quote.usable:
                    reason += "; " + quote.reason
                else:
                    reason += f"; starter quote captured at {quote.effective_price_sol:.12g} SOL/token"
            return self._decision(swap, "shadow_first_touch", reason)

        first_tier = WalletTier(str(first["tier"]))
        quote = None
        if risk.clean:
            if first_tier is WalletTier.S:
                stage = "confirmation_add"
                fraction = 1.0 - self.engine.config.starter_fraction_of_full_position
            else:
                stage = "confirmed_full"
                fraction = 1.0
            quote = await self._quote(
                token_mint=swap.token_mint,
                stage=stage,
                fraction=fraction,
                scout_reference_price_sol=scout_reference_price,
                trigger_observed_at=swap.observed_at,
            )
        reason = "shadow independent confirmation; paper cohort disabled"
        if not risk.clean:
            reason += "; risk_veto:" + ",".join(risk.blockers)
        elif quote is None:
            reason += "; executable quote unavailable"
        elif not quote.usable:
            reason += "; " + quote.reason
        else:
            reason += f"; executable quote captured at {quote.effective_price_sol:.12g} SOL/token"
        return self._decision(swap, "shadow_confirmation", reason)
