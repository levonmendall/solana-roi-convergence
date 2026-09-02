from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Callable

from .ingestion import IngestionDecision, LiveEvidenceIngestionService, NormalizedSwap
from .models import CandidateStatus, Confirmation, WalletTier, WalletTouch
from .risk import RiskDimension


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class CollectingLiveEvidenceIngestionService(LiveEvidenceIngestionService):
    """Production service: preserve chronology, collect evidence, and fail closed unless the final gate authorizes paper entry."""

    def __init__(
        self,
        *args: Any,
        collectors: Any,
        mark_recorder: Any | None = None,
        quote_handoff: Any | None = None,
        activation_gate: Any | None = None,
        decision_clock: Callable[[], datetime] = _utcnow,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self.collectors = collectors
        self.mark_recorder = mark_recorder
        self.quote_handoff = quote_handoff
        self.activation_gate = activation_gate
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

    def _risk_readiness(self, token_mint: str, at: datetime) -> dict[str, Any]:
        readiness = getattr(self.risk_provider, "readiness", None)
        if not callable(readiness):
            return {"complete": False, "fresh": False, "fresh_dimensions": {}}
        result = readiness(token_mint, as_of=at)
        return result if isinstance(result, dict) else {"complete": False, "fresh": False, "fresh_dimensions": {}}

    @staticmethod
    def _six_dimensions_fresh(readiness: dict[str, Any]) -> bool:
        fresh = readiness.get("fresh_dimensions")
        return bool(
            readiness.get("complete")
            and readiness.get("fresh")
            and isinstance(fresh, dict)
            and all(bool(fresh.get(dimension.value)) for dimension in RiskDimension)
        )

    def _cohort_armed(self) -> bool:
        return bool(self.activation_gate is not None and self.activation_gate.controller.is_armed())

    def _chronology_conflict(self, token_mint: str) -> bool:
        checker = getattr(self.store, "token_first_touch_has_earlier_eligible_swap", None)
        return bool(callable(checker) and checker(token_mint))

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
            return self._decision(swap, "duplicate", "normalized swap already persisted; chronology unchanged")
        self.store.append("normalized_swap", swap.received_at.isoformat(), asdict(swap))
        if self.mark_recorder is not None:
            self.mark_recorder.record_swap_mark(swap)

        await self.collectors.refresh(swap.token_mint, swap.received_at, current_swap=swap)
        risk_completed_at = max(swap.received_at, self.decision_clock())

        profile = self.registry.get(swap.wallet)
        if profile is None:
            return self._decision(swap, "record_only", "wallet profile unavailable")
        if not profile.historically_eligible or profile.tier not in {WalletTier.S, WalletTier.A}:
            return self._decision(swap, "record_only", "wallet not eligible for frozen scout cohort")
        if swap.side != "buy":
            return self._decision(swap, "record_only", "sell evidence retained but cannot initiate/confirm entry")

        effective_entity_id = self._effective_entity_id(profile, as_of=risk_completed_at)
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

        if self._chronology_conflict(swap.token_mint):
            return self._decision(swap, "record_only", "first-touch chronology conflict detected from out-of-order eligible evidence")

        if not first_claimed_now and self._same_entity(
            first_wallet=str(first["wallet"]),
            first_entity_id=str(first["entity_id"]),
            current_wallet=swap.wallet,
            current_entity_id=effective_entity_id,
            as_of=risk_completed_at,
        ):
            return self._decision(swap, "record_only", "same economic entity cannot confirm its own first touch")

        scout_wallet = str(first["wallet"])
        scout_entity_id = str(first["entity_id"])
        scout_profile = self.registry.get(scout_wallet)
        scout_reference_price = float(first["reference_price_sol"])
        risk = await self.risk_provider.snapshot(
            swap.token_mint,
            risk_completed_at,
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
            risk_completed_at.isoformat(),
            {
                "token_mint": swap.token_mint,
                "scout_wallet": scout_wallet,
                "trigger_received_at": swap.received_at.isoformat(),
                "decision_at": risk_completed_at.isoformat(),
                "decision_latency_ms": max(0.0, (risk_completed_at - swap.observed_at).total_seconds() * 1000.0),
                "risk": asdict(risk),
            },
        )

        armed = self._cohort_armed()
        if self.promote_paper_signals and not armed:
            return self._decision(
                swap,
                "record_only",
                "activation blocked: post-risk reference-price evidence alone cannot authorize paper execution; final forward-cohort activation gate has not authorized this candidate",
            )

        if not armed:
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

        if first_claimed_now and profile.tier is WalletTier.A:
            readiness = self._risk_readiness(swap.token_mint, risk_completed_at)
            if not risk.clean or not self._six_dimensions_fresh(readiness):
                return self._decision(swap, "record_only", "A-tier candidate blocked: exact six-dimension risk bundle not clean and fresh")
            touch = WalletTouch(
                token_mint=swap.token_mint,
                wallet=scout_wallet,
                entity_id=scout_entity_id,
                observed_at=datetime.fromisoformat(str(first["observed_at"])),
                reference_price=scout_reference_price,
                market_cap_usd=None,
                tier=WalletTier.A,
                historically_eligible=True,
            )
            self.engine.on_first_touch(touch, risk)
            return self._decision(swap, "candidate_waiting_confirmation", "clean A-tier first touch registered; no starter permitted")

        if first_claimed_now:
            stage = "starter"
            fraction = self.engine.config.starter_fraction_of_full_position
        else:
            candidate = self.engine.strategy.candidates.get(swap.token_mint)
            if candidate is None or candidate.status is not CandidateStatus.WAITING_CONFIRMATION:
                return self._decision(swap, "record_only", "paper candidate state is not waiting for confirmation")
            first_tier = WalletTier(str(first["tier"]))
            if first_tier is WalletTier.S:
                stage = "confirmation_add"
                fraction = 1.0 - self.engine.config.starter_fraction_of_full_position
            else:
                stage = "confirmed_full"
                fraction = 1.0

        quote = None
        if risk.clean:
            quote = await self._quote(
                token_mint=swap.token_mint,
                stage=stage,
                fraction=fraction,
                scout_reference_price_sol=scout_reference_price,
                trigger_observed_at=swap.observed_at,
            )
        activation_at = max(risk_completed_at, quote.received_at if quote is not None else risk_completed_at, self.decision_clock())
        final_risk = await self.risk_provider.snapshot(
            swap.token_mint,
            activation_at,
            scout_wallet=scout_wallet,
            scout_entity_id=scout_entity_id,
        )
        readiness = self._risk_readiness(swap.token_mint, activation_at)
        gate = self.activation_gate.evaluate(
            token_mint=swap.token_mint,
            stage=stage,
            fraction_of_full_position=fraction,
            scout_profile=scout_profile,
            first_touch=first,
            risk=final_risk,
            risk_readiness=readiness,
            quote=quote,
            risk_completed_at=risk_completed_at,
            decision_at=activation_at,
            confirmation_observed_at=None if first_claimed_now else swap.observed_at,
            confirmation_entity_id=None if first_claimed_now else effective_entity_id,
        )
        if not gate.authorized or quote is None or final_risk is None:
            reason = "candidate activation blocked"
            if gate.blockers:
                reason += ":" + ",".join(gate.blockers)
            return self._decision(swap, gate.code, reason)

        if first_claimed_now:
            touch = WalletTouch(
                token_mint=swap.token_mint,
                wallet=scout_wallet,
                entity_id=scout_entity_id,
                observed_at=datetime.fromisoformat(str(first["observed_at"])),
                reference_price=scout_reference_price,
                market_cap_usd=None,
                tier=WalletTier.S,
                historically_eligible=True,
            )
            self.engine.on_first_touch(touch, final_risk, execution_price=quote.effective_price_sol)
            return self._decision(swap, gate.code, "authorized S-tier 30% starter using fresh amount-specific executable quote")

        confirmation = Confirmation(
            token_mint=swap.token_mint,
            wallet=swap.wallet,
            entity_id=effective_entity_id,
            observed_at=swap.observed_at,
            reference_price=quote.effective_price_sol,
            historically_eligible=profile.historically_eligible,
        )
        self.engine.on_confirmation(confirmation, final_risk, execution_price=quote.effective_price_sol)
        return self._decision(swap, gate.code, f"authorized {stage} using independent confirmation and fresh executable quote")
