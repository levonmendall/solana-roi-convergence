from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from statistics import mean, median
from typing import Any


FOMO_RESEARCH_VERSION = "fomo-continuation-shadow-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
ACTIVE_STRATEGY_MUTATION_ALLOWED = False
HISTORICAL_PROMOTION_AUTHORITY = False
FOMO_LANE = "fomo_continuation_shadow"
SIGNAL_DECAY_DELAYS_SECONDS = (1, 2, 5, 10, 20, 30, 60)


@dataclass(frozen=True)
class FomoFeatures:
    token_mint: str
    observed_at: str
    venue: str
    lifecycle: str
    regime: str
    independent_buyers_short: int
    independent_buyers_long: int
    buys_short: int
    buys_long: int
    sells_short: int
    sells_long: int
    buy_volume_short: float
    buy_volume_long: float
    sell_volume_short: float
    sell_volume_long: float
    new_buyer_acceleration: float
    transaction_frequency_acceleration: float
    net_buy_flow_acceleration: float
    buy_sell_imbalance: float
    independent_demand_persistence: float
    momentum_wallet_participation: int
    creator_accumulating: bool
    creator_distributing: bool
    early_holder_exit_fraction: float
    chase_fraction: float | None
    signal_to_entry_seconds: float | None
    quote_deterioration_fraction: float | None
    depth_growth_fraction: float | None
    exit_slippage_deterioration_fraction: float | None
    risk_complete: bool


@dataclass(frozen=True)
class FomoState:
    state: str
    score: float
    structurally_accessible: bool
    blockers: tuple[str, ...]
    feature_version: str = FOMO_RESEARCH_VERSION


@dataclass(frozen=True)
class FomoOutcome:
    source_signature: str
    observed_at: str
    venue: str
    lifecycle: str
    regime: str
    fomo_state: str
    signal_to_entry_seconds: float
    net_return: float
    release_commit: str


def _safe_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def _ratio(short_value: float, long_value: float, *, short_seconds: float = 5.0, long_seconds: float = 20.0) -> float:
    short_rate = max(0.0, short_value) / max(short_seconds, 1e-9)
    long_rate = max(0.0, long_value) / max(long_seconds, 1e-9)
    if long_rate <= 0:
        return 1.0 if short_rate > 0 else 0.0
    return short_rate / long_rate


def build_fomo_features(
    *,
    token_mint: str,
    observed_at: str,
    venue: str,
    lifecycle: str,
    regime: str,
    independent_buyers_short: int,
    independent_buyers_long: int,
    buys_short: int,
    buys_long: int,
    sells_short: int,
    sells_long: int,
    buy_volume_short: float,
    buy_volume_long: float,
    sell_volume_short: float,
    sell_volume_long: float,
    momentum_wallet_participation: int,
    creator_accumulating: bool,
    creator_distributing: bool,
    early_holder_exit_fraction: float,
    chase_fraction: float | None,
    signal_to_entry_seconds: float | None,
    quote_deterioration_fraction: float | None = None,
    depth_growth_fraction: float | None = None,
    exit_slippage_deterioration_fraction: float | None = None,
    risk_complete: bool = True,
) -> FomoFeatures:
    buyer_acceleration = _ratio(float(independent_buyers_short), float(independent_buyers_long))
    tx_acceleration = _ratio(float(buys_short), float(buys_long))
    short_net = max(0.0, buy_volume_short) - max(0.0, sell_volume_short)
    long_net = max(0.0, buy_volume_long) - max(0.0, sell_volume_long)
    flow_acceleration = _ratio(short_net, max(0.0, long_net))
    gross = max(0.0, buy_volume_short) + max(0.0, sell_volume_short)
    imbalance = ((max(0.0, buy_volume_short) - max(0.0, sell_volume_short)) / gross) if gross > 0 else 0.0
    persistence = min(1.0, max(0.0, independent_buyers_short) / max(1.0, independent_buyers_long))
    return FomoFeatures(
        token_mint=token_mint,
        observed_at=observed_at,
        venue=venue,
        lifecycle=lifecycle,
        regime=regime,
        independent_buyers_short=max(0, int(independent_buyers_short)),
        independent_buyers_long=max(0, int(independent_buyers_long)),
        buys_short=max(0, int(buys_short)),
        buys_long=max(0, int(buys_long)),
        sells_short=max(0, int(sells_short)),
        sells_long=max(0, int(sells_long)),
        buy_volume_short=float(buy_volume_short),
        buy_volume_long=float(buy_volume_long),
        sell_volume_short=float(sell_volume_short),
        sell_volume_long=float(sell_volume_long),
        new_buyer_acceleration=buyer_acceleration,
        transaction_frequency_acceleration=tx_acceleration,
        net_buy_flow_acceleration=flow_acceleration,
        buy_sell_imbalance=imbalance,
        independent_demand_persistence=persistence,
        momentum_wallet_participation=max(0, int(momentum_wallet_participation)),
        creator_accumulating=bool(creator_accumulating),
        creator_distributing=bool(creator_distributing),
        early_holder_exit_fraction=max(0.0, float(early_holder_exit_fraction)),
        chase_fraction=_safe_float(chase_fraction),
        signal_to_entry_seconds=_safe_float(signal_to_entry_seconds),
        quote_deterioration_fraction=_safe_float(quote_deterioration_fraction),
        depth_growth_fraction=_safe_float(depth_growth_fraction),
        exit_slippage_deterioration_fraction=_safe_float(exit_slippage_deterioration_fraction),
        risk_complete=bool(risk_complete),
    )


def classify_fomo_state(features: FomoFeatures, *, max_chase_fraction: float = 0.15, max_latency_seconds: float = 20.0) -> FomoState:
    blockers: list[str] = []
    if features.chase_fraction is None:
        blockers.append("chase_unknown")
    elif features.chase_fraction > max_chase_fraction:
        blockers.append("chase_above_limit")
    if features.signal_to_entry_seconds is None:
        blockers.append("signal_to_entry_unknown")
    elif features.signal_to_entry_seconds > max_latency_seconds:
        blockers.append("signal_to_entry_above_limit")
    if not features.risk_complete:
        blockers.append("risk_incomplete")
    if features.creator_distributing:
        blockers.append("creator_distributing")
    if features.early_holder_exit_fraction >= 0.20:
        blockers.append("early_holder_distribution")
    if features.quote_deterioration_fraction is not None and features.quote_deterioration_fraction >= 0.05:
        blockers.append("quote_deteriorating")
    if features.exit_slippage_deterioration_fraction is not None and features.exit_slippage_deterioration_fraction >= 0.05:
        blockers.append("exit_slippage_deteriorating")

    score = 0.0
    score += min(2.0, max(0.0, features.new_buyer_acceleration - 1.0))
    score += min(2.0, max(0.0, features.transaction_frequency_acceleration - 1.0))
    score += min(2.0, max(0.0, features.net_buy_flow_acceleration - 1.0))
    score += max(0.0, features.buy_sell_imbalance)
    score += max(0.0, min(1.0, features.independent_demand_persistence))
    score += min(1.0, features.momentum_wallet_participation / 2.0)
    if features.creator_accumulating:
        score += 0.5
    if features.depth_growth_fraction is not None and features.depth_growth_fraction > 0:
        score += min(1.0, features.depth_growth_fraction * 5.0)

    if blockers:
        return FomoState("late_or_inaccessible_fomo", score, False, tuple(dict.fromkeys(blockers)))
    if score >= 5.0 and features.new_buyer_acceleration > 1.0 and features.net_buy_flow_acceleration > 1.0:
        return FomoState("active_fomo", score, True, ())
    if score >= 2.5 and features.new_buyer_acceleration >= 1.0:
        return FomoState("pre_fomo", score, True, ())
    return FomoState("no_fomo", score, True, ())


class FomoContinuationShadow:
    """Production-shadow FOMO research surface with zero strategy authority."""

    def __init__(self, store: Any, *, release_commit: str):
        self.store = store
        self.release_commit = release_commit
        self._schema()

    def _schema(self) -> None:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS fomo_shadow_observations ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, "
                "token_mint TEXT NOT NULL, observed_at TEXT NOT NULL, venue TEXT NOT NULL, lifecycle TEXT NOT NULL, "
                "regime TEXT NOT NULL, feature_json TEXT NOT NULL, state_json TEXT NOT NULL, created_at TEXT NOT NULL, "
                "UNIQUE(release_commit,source_signature))"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS fomo_shadow_outcomes ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, "
                "observed_at TEXT NOT NULL, venue TEXT NOT NULL, lifecycle TEXT NOT NULL, regime TEXT NOT NULL, "
                "fomo_state TEXT NOT NULL, signal_to_entry_seconds REAL NOT NULL, net_return REAL NOT NULL, created_at TEXT NOT NULL, "
                "UNIQUE(release_commit,source_signature))"
            )

    def record_observation(self, *, source_signature: str, features: FomoFeatures, state: FomoState) -> None:
        import json
        now = datetime.now(timezone.utc).isoformat()
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT OR IGNORE INTO fomo_shadow_observations("
                "release_commit,source_signature,token_mint,observed_at,venue,lifecycle,regime,feature_json,state_json,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    self.release_commit,
                    source_signature,
                    features.token_mint,
                    features.observed_at,
                    features.venue,
                    features.lifecycle,
                    features.regime,
                    json.dumps(asdict(features), sort_keys=True, separators=(",", ":")),
                    json.dumps(asdict(state), sort_keys=True, separators=(",", ":")),
                    now,
                ),
            )

    def record_outcome(self, outcome: FomoOutcome) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT OR IGNORE INTO fomo_shadow_outcomes("
                "release_commit,source_signature,observed_at,venue,lifecycle,regime,fomo_state,signal_to_entry_seconds,net_return,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    outcome.release_commit,
                    outcome.source_signature,
                    outcome.observed_at,
                    outcome.venue,
                    outcome.lifecycle,
                    outcome.regime,
                    outcome.fomo_state,
                    outcome.signal_to_entry_seconds,
                    outcome.net_return,
                    now,
                ),
            )

    @staticmethod
    def _trimmed(values: list[float], n: int) -> float | None:
        if len(values) <= n:
            return None
        ordered = sorted(values, reverse=True)[n:]
        return mean(ordered) if ordered else None

    @staticmethod
    def _delay_bucket(delay: float) -> int:
        for value in SIGNAL_DECAY_DELAYS_SECONDS:
            if delay <= value:
                return value
        return SIGNAL_DECAY_DELAYS_SECONDS[-1]

    def status(self) -> dict[str, Any]:
        with self.store._lock:
            observations = self.store.db.execute(
                "SELECT COUNT(*) n, SUM(CASE WHEN json_extract(state_json,'$.state')='pre_fomo' THEN 1 ELSE 0 END) pre, "
                "SUM(CASE WHEN json_extract(state_json,'$.state')='active_fomo' THEN 1 ELSE 0 END) active, "
                "SUM(CASE WHEN json_extract(state_json,'$.state')='late_or_inaccessible_fomo' THEN 1 ELSE 0 END) late "
                "FROM fomo_shadow_observations WHERE release_commit=?",
                (self.release_commit,),
            ).fetchone()
            rows = self.store.db.execute(
                "SELECT fomo_state,signal_to_entry_seconds,net_return,venue,lifecycle,regime FROM fomo_shadow_outcomes "
                "WHERE release_commit=? ORDER BY id",
                (self.release_commit,),
            ).fetchall()

        values = [float(row["net_return"]) for row in rows]
        latency: dict[str, dict[str, Any]] = {}
        buckets: dict[int, list[float]] = {value: [] for value in SIGNAL_DECAY_DELAYS_SECONDS}
        by_state: dict[str, list[float]] = {}
        for row in rows:
            state = str(row["fomo_state"])
            value = float(row["net_return"])
            by_state.setdefault(state, []).append(value)
            buckets[self._delay_bucket(float(row["signal_to_entry_seconds"]))].append(value)
        for delay, bucket_values in buckets.items():
            latency[str(delay)] = {
                "sample_count": len(bucket_values),
                "mean_residual_roi_pct": mean(bucket_values) * 100.0 if bucket_values else None,
                "median_residual_roi_pct": median(bucket_values) * 100.0 if bucket_values else None,
            }

        return {
            "research_version": FOMO_RESEARCH_VERSION,
            "lane": FOMO_LANE,
            "release_commit": self.release_commit,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
            "active_strategy_mutation_allowed": False,
            "historical_promotion_authority": False,
            "observation_count": int(observations["n"] or 0) if observations else 0,
            "pre_fomo_count": int(observations["pre"] or 0) if observations else 0,
            "active_fomo_count": int(observations["active"] or 0) if observations else 0,
            "late_or_inaccessible_fomo_count": int(observations["late"] or 0) if observations else 0,
            "outcome_count": len(values),
            "mean_residual_roi_pct": mean(values) * 100.0 if values else None,
            "median_residual_roi_pct": median(values) * 100.0 if values else None,
            "trimmed_mean_residual_roi_ex_best_1_pct": self._trimmed(values, 1) * 100.0 if self._trimmed(values, 1) is not None else None,
            "trimmed_mean_residual_roi_ex_best_3_pct": self._trimmed(values, 3) * 100.0 if self._trimmed(values, 3) is not None else None,
            "trimmed_mean_residual_roi_ex_best_5_pct": self._trimmed(values, 5) * 100.0 if self._trimmed(values, 5) is not None else None,
            "positive_rate_pct": (sum(value > 0 for value in values) / len(values) * 100.0) if values else None,
            "signal_decay": latency,
            "by_fomo_state": {
                state: {
                    "sample_count": len(state_values),
                    "mean_residual_roi_pct": mean(state_values) * 100.0,
                    "median_residual_roi_pct": median(state_values) * 100.0,
                }
                for state, state_values in sorted(by_state.items())
            },
            "promotion_gate": "forward_evidence_only; zero automatic authority",
        }


__all__ = [
    "ACTIVE_STRATEGY_MUTATION_ALLOWED",
    "FOMO_LANE",
    "FOMO_RESEARCH_VERSION",
    "FomoContinuationShadow",
    "FomoFeatures",
    "FomoOutcome",
    "FomoState",
    "HISTORICAL_PROMOTION_AUTHORITY",
    "LIVE_MONEY_AUTHORITY",
    "PAPER_ONLY",
    "SIGNING_AVAILABLE",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "build_fomo_features",
    "classify_fomo_state",
]
