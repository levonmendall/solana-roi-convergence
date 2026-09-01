from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class WalletTier(str, Enum):
    S = "S"
    A = "A"
    B = "B"
    REJECT = "REJECT"


class CandidateStatus(str, Enum):
    WAITING_CONFIRMATION = "waiting_confirmation"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CLOSED = "closed"


class IntentKind(str, Enum):
    OPEN_STARTER = "open_starter"
    OPEN_FULL = "open_full"
    ADD_CONFIRMATION = "add_confirmation"
    EXIT_STARTER = "exit_starter"
    EXIT_THESIS = "exit_thesis"
    EXIT_STOP = "exit_stop"
    HARVEST = "harvest"
    EXIT_RUNNER = "exit_runner"


@dataclass(frozen=True, slots=True)
class RiskSnapshot:
    observed_at: datetime
    early_buyers_exiting: bool = False
    bundled_launch: bool = False
    sniper_heavy: bool = False
    abnormal_sell_pressure: bool = False
    common_funded_early_wallet_cluster: bool = False
    scout_deployer_connection: bool = False
    dangerous_authority: bool = False
    unacceptable_liquidity: bool = False
    stale: bool = False

    @property
    def blockers(self) -> tuple[str, ...]:
        names = ("early_buyers_exiting", "bundled_launch", "sniper_heavy", "abnormal_sell_pressure", "common_funded_early_wallet_cluster", "scout_deployer_connection", "dangerous_authority", "unacceptable_liquidity", "stale")
        return tuple(name for name in names if getattr(self, name))

    @property
    def clean(self) -> bool:
        return not self.blockers


@dataclass(frozen=True, slots=True)
class WalletTouch:
    token_mint: str
    wallet: str
    entity_id: str
    observed_at: datetime
    reference_price: float
    market_cap_usd: float | None
    tier: WalletTier
    historically_eligible: bool = True


@dataclass(frozen=True, slots=True)
class Confirmation:
    token_mint: str
    wallet: str
    entity_id: str
    observed_at: datetime
    reference_price: float
    historically_eligible: bool = True


@dataclass(frozen=True, slots=True)
class TradeIntent:
    kind: IntentKind
    token_mint: str
    observed_at: datetime
    fraction_of_full_position: float = 0.0
    reason: str = ""


@dataclass(slots=True)
class Candidate:
    token_mint: str
    scout_wallet: str
    scout_entity_id: str
    scout_tier: WalletTier
    first_touch_at: datetime
    scout_reference_price: float
    risk_at_entry: RiskSnapshot
    status: CandidateStatus = CandidateStatus.WAITING_CONFIRMATION
    confirmed_at: datetime | None = None
    confirmation_wallet: str | None = None
    confirmation_entity_id: str | None = None
    full_entry_reference_price: float | None = None
    harvest_triggered: bool = False
    runner_high_water_price: float | None = None
    closed_reason: str | None = None


@dataclass(frozen=True, slots=True)
class SimulatedFill:
    token_mint: str
    side: str
    observed_at: datetime
    reference_price: float
    fill_price: float
    notional_usd: float
    units: float
    execution_drag_usd: float
    intent: IntentKind


@dataclass(slots=True)
class PaperPosition:
    token_mint: str
    scout_wallet: str
    opened_at: datetime
    units: float = 0.0
    cost_basis_usd: float = 0.0
    entry_capital_usd: float = 0.0
    realized_pnl_usd: float = 0.0
    harvest_hit: bool = False
    runner_units: float = 0.0
    high_water_price: float | None = None
    closed_at: datetime | None = None
    closed_reason: str | None = None
    fills: list[SimulatedFill] = field(default_factory=list)

    @property
    def average_entry_price(self) -> float | None:
        return self.cost_basis_usd / self.units if self.units > 0 else None

    @property
    def is_open(self) -> bool:
        return self.units > 0 and self.closed_at is None


@dataclass(frozen=True, slots=True)
class TradeOutcome:
    token_mint: str
    scout_wallet: str
    opened_at: datetime
    closed_at: datetime
    starting_nav_usd: float
    ending_nav_usd: float
    net_pnl_usd: float
    return_on_starting_nav: float
    harvest_hit: bool
    closed_reason: str
