from __future__ import annotations

from datetime import datetime

from .config import BASELINE, StrategyConfig
from .models import Candidate, CandidateStatus, Confirmation, IntentKind, RiskSnapshot, TradeIntent, WalletTier, WalletTouch


class RoiConvergenceStrategy:
    """Deterministic state machine for the frozen v3.1 forward baseline."""

    def __init__(self, config: StrategyConfig = BASELINE):
        self.config = config
        self.candidates: dict[str, Candidate] = {}

    def first_touch(self, touch: WalletTouch, risk: RiskSnapshot) -> list[TradeIntent]:
        if touch.token_mint in self.candidates:
            return []
        if not touch.historically_eligible or touch.tier not in {WalletTier.S, WalletTier.A}:
            return []
        candidate = Candidate(touch.token_mint, touch.wallet, touch.entity_id, touch.tier, touch.observed_at, touch.reference_price, risk)
        self.candidates[touch.token_mint] = candidate
        if not risk.clean:
            candidate.status = CandidateStatus.REJECTED
            candidate.closed_reason = "risk_veto:" + ",".join(risk.blockers)
            return []
        if touch.tier is WalletTier.S:
            return [TradeIntent(IntentKind.OPEN_STARTER, touch.token_mint, touch.observed_at, self.config.starter_fraction_of_full_position, "clean S-tier first touch")]
        return []

    def confirm(self, confirmation: Confirmation, risk: RiskSnapshot) -> list[TradeIntent]:
        candidate = self.candidates.get(confirmation.token_mint)
        if candidate is None or candidate.status is not CandidateStatus.WAITING_CONFIRMATION:
            return []
        age = (confirmation.observed_at - candidate.first_touch_at).total_seconds()
        if age < 0 or age > self.config.confirmation_window_seconds or not confirmation.historically_eligible:
            return []
        if confirmation.entity_id == candidate.scout_entity_id or not risk.clean:
            return []
        drift = confirmation.reference_price / candidate.scout_reference_price - 1.0
        if drift > self.config.max_chase_fraction:
            return []
        candidate.status = CandidateStatus.CONFIRMED
        candidate.confirmed_at = confirmation.observed_at
        candidate.confirmation_wallet = confirmation.wallet
        candidate.confirmation_entity_id = confirmation.entity_id
        candidate.full_entry_reference_price = confirmation.reference_price
        if candidate.scout_tier is WalletTier.S:
            fraction, kind, reason = 1.0 - self.config.starter_fraction_of_full_position, IntentKind.ADD_CONFIRMATION, "independent confirmation for S-tier starter"
        else:
            fraction, kind, reason = 1.0, IntentKind.OPEN_FULL, "independent confirmation for A-tier signal"
        return [TradeIntent(kind, confirmation.token_mint, confirmation.observed_at, fraction, reason)]

    def on_clock(self, token_mint: str, observed_at: datetime, reference_price: float | None = None) -> list[TradeIntent]:
        candidate = self.candidates.get(token_mint)
        if candidate is None or candidate.status in {CandidateStatus.REJECTED, CandidateStatus.EXPIRED, CandidateStatus.CLOSED}:
            return []
        age = (observed_at - candidate.first_touch_at).total_seconds()
        if candidate.status is CandidateStatus.WAITING_CONFIRMATION and age > self.config.confirmation_window_seconds:
            candidate.status = CandidateStatus.EXPIRED
            candidate.closed_reason = "confirmation_timeout"
            if candidate.scout_tier is WalletTier.S:
                return [TradeIntent(IntentKind.EXIT_STARTER, token_mint, observed_at, reason="no independent confirmation within 20 seconds")]
            return []
        if candidate.status is not CandidateStatus.CONFIRMED or reference_price is None:
            return []
        entry = candidate.full_entry_reference_price or candidate.scout_reference_price
        trade_age = (observed_at - (candidate.confirmed_at or candidate.first_touch_at)).total_seconds()
        gross_return = reference_price / entry - 1.0
        if not candidate.harvest_triggered and gross_return <= -self.config.catastrophic_stop_fraction:
            candidate.status = CandidateStatus.CLOSED
            candidate.closed_reason = "catastrophic_stop"
            return [TradeIntent(IntentKind.EXIT_STOP, token_mint, observed_at, reason="-30% catastrophic stop")]
        if not candidate.harvest_triggered and gross_return >= self.config.harvest_gain_fraction:
            candidate.harvest_triggered = True
            candidate.runner_high_water_price = reference_price
            return [TradeIntent(IntentKind.HARVEST, token_mint, observed_at, self.config.harvest_fraction, "+50% harvest; retain 30% runner")]
        if candidate.harvest_triggered:
            candidate.runner_high_water_price = max(candidate.runner_high_water_price or reference_price, reference_price)
            trail = (candidate.runner_high_water_price or reference_price) * (1.0 - self.config.runner_trailing_drawdown_fraction)
            if reference_price <= trail:
                candidate.status = CandidateStatus.CLOSED
                candidate.closed_reason = "runner_trailing_stop"
                return [TradeIntent(IntentKind.EXIT_RUNNER, token_mint, observed_at, reason="40% runner trailing drawdown")]
            return []
        if trade_age >= self.config.stagnation_check_seconds and gross_return <= 0:
            candidate.status = CandidateStatus.CLOSED
            candidate.closed_reason = "stagnation_exit"
            return [TradeIntent(IntentKind.EXIT_THESIS, token_mint, observed_at, reason="non-positive after 90 seconds")]
        if trade_age >= self.config.thesis_timeout_seconds:
            candidate.status = CandidateStatus.CLOSED
            candidate.closed_reason = "thesis_timeout"
            return [TradeIntent(IntentKind.EXIT_THESIS, token_mint, observed_at, reason="+50% thesis not reached within 180 seconds")]
        return []
