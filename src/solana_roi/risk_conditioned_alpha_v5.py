from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from statistics import mean, median
from typing import Any, Iterable, Sequence


STRATEGY_VERSION = "roi-convergence-v5.0-risk-conditioned-alpha-1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
HISTORICAL_PROMOTION_AUTHORITY = False
MIN_FORWARD_SAMPLES = 30

MECHANICAL_HARD_STOPS = frozenset(
    {
        "sell_route_unavailable",
        "transfer_restricted",
        "liquidity_unexitable",
        "entry_quote_unavailable",
        "exit_quote_unavailable",
        "authority_can_block_transfer_or_exit",
        "linked_entity_can_remove_required_liquidity",
    }
)

HAZARD_WEIGHTS: dict[str, float] = {
    "bundled_launch": 0.22,
    "sniper_heavy": 0.18,
    "abnormal_sell_pressure": 0.28,
    "common_funded_early_wallet_cluster": 0.30,
    "scout_deployer_connection": 0.30,
    "mint_authority_active": 0.18,
    "early_buyers_exiting": 0.28,
    "creator_distributing": 0.30,
    "creator_linked_trigger": 0.12,
    "early_holder_distribution": 0.25,
    "quote_deteriorating": 0.15,
    "exit_slippage_deteriorating": 0.20,
    "late_lifecycle": 0.12,
    "high_snipe_tax": 0.16,
}

SOLANA_POSITION_GRID = (0.005, 0.01, 0.02, 0.05, 0.10, 0.15, 0.20)
FOMO_ACTIVE_GRID = (0.005, 0.01, 0.02, 0.05)
FOMO_CHALLENGER_GRID = (0.075, 0.10)

_ORIGINAL_FINAL_BUY: Any = None
_ORIGINAL_FINAL_SELL: Any = None
_ORIGINAL_FINAL_STATUS: Any = None
_INSTALLED = False


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dump(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _safe_json(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if not raw:
        return {}
    try:
        value = json.loads(str(raw))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _trimmed_ex_best(values: Sequence[float], n: int = 1) -> float | None:
    if len(values) <= n:
        return None
    remaining = sorted((float(v) for v in values), reverse=True)[n:]
    return mean(remaining) if remaining else None


def _expected_log_growth(values: Sequence[float], fraction: float) -> float | None:
    if not values or fraction <= 0.0:
        return None
    terms: list[float] = []
    for value in values:
        terminal = 1.0 + fraction * float(value)
        if terminal <= 0.0:
            return float("-inf")
        terms.append(math.log(terminal))
    return mean(terms) if terms else None


def _expected_shortfall(values: Sequence[float], tail_fraction: float = 0.20) -> float | None:
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    count = max(1, math.ceil(len(ordered) * tail_fraction))
    return mean(ordered[:count])


def _winner_concentration(values: Sequence[float]) -> float | None:
    positives = sorted((float(v) for v in values if float(v) > 0.0), reverse=True)
    if not positives:
        return None
    total = sum(positives)
    return positives[0] / total if total > 0.0 else None


def _scaled_max_drawdown(values: Sequence[float], fraction: float) -> float | None:
    if not values or fraction <= 0.0:
        return None
    equity = 1.0
    peak = 1.0
    drawdown = 0.0
    for result in values:
        equity *= max(1e-9, 1.0 + fraction * float(result))
        peak = max(peak, equity)
        if peak > 0.0:
            drawdown = max(drawdown, (peak - equity) / peak)
    return min(1.0, max(0.0, drawdown))


@dataclass(frozen=True)
class RobustReturnProfile:
    sample_count: int
    state: str
    mean_return: float | None
    median_return: float | None
    trimmed_mean_ex_best: float | None
    hit_rate: float | None
    expected_shortfall_20: float | None
    winner_concentration: float | None
    best_fraction: float
    best_expected_log_growth: float | None
    max_drawdown_at_best_fraction: float | None
    growth_by_fraction: dict[float, float | None]


def robust_return_profile(
    values: Iterable[float],
    *,
    grid: Sequence[float] = SOLANA_POSITION_GRID,
    max_fraction: float = 0.20,
    min_samples: int = MIN_FORWARD_SAMPLES,
) -> RobustReturnProfile:
    clean = [float(v) for v in values if _finite(v) is not None]
    count = len(clean)
    med = median(clean) if clean else None
    trimmed = _trimmed_ex_best(clean, 1)
    hit_rate = (sum(v > 0.0 for v in clean) / count) if clean else None
    shortfall = _expected_shortfall(clean)
    concentration = _winner_concentration(clean)
    growth = {
        float(f): _expected_log_growth(clean, float(f))
        for f in grid
        if float(f) > 0.0 and float(f) <= max_fraction
    }
    viable = [
        (fraction, value)
        for fraction, value in growth.items()
        if value is not None and math.isfinite(value)
    ]
    best_fraction = 0.0
    best_growth: float | None = None
    if viable:
        best_fraction, best_growth = max(viable, key=lambda item: item[1])
        if best_growth <= 0.0:
            best_fraction = 0.0
    drawdown = _scaled_max_drawdown(clean, best_fraction) if best_fraction > 0.0 else None

    # Win rate and positive median are deliberately diagnostic only. Positively
    # skewed continuation strategies are promoted on robust compounded growth,
    # leave-the-best-trade-out evidence and bounded tail/drawdown behavior.
    if count < min_samples:
        state = "bootstrap_forward_evidence"
    else:
        robust_positive = (
            trimmed is not None
            and trimmed > 0.0
            and best_growth is not None
            and math.isfinite(best_growth)
            and best_growth > 0.0
            and (drawdown is None or drawdown <= 0.35)
            and (shortfall is None or shortfall > -0.85)
            and (concentration is None or concentration <= 0.80 or count >= 60)
        )
        clearly_negative = (
            best_growth is None
            or not math.isfinite(best_growth)
            or best_growth <= 0.0
            or (trimmed is not None and trimmed <= 0.0)
        )
        if robust_positive:
            state = "promoted_positive_log_growth"
        elif clearly_negative:
            state = "demoted_nonpositive_log_growth"
        else:
            state = "observe_mixed_forward_evidence"

    return RobustReturnProfile(
        sample_count=count,
        state=state,
        mean_return=mean(clean) if clean else None,
        median_return=med,
        trimmed_mean_ex_best=trimmed,
        hit_rate=hit_rate,
        expected_shortfall_20=shortfall,
        winner_concentration=concentration,
        best_fraction=float(best_fraction),
        best_expected_log_growth=best_growth,
        max_drawdown_at_best_fraction=drawdown,
        growth_by_fraction=growth,
    )


def chase_band(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value <= 0.15:
        return "baseline_le_15pct"
    if value <= 0.25:
        return "challenger_15_25pct"
    if value <= 0.40:
        return "challenger_25_40pct"
    return "challenger_gt_40pct"


def latency_band(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value <= 5.0:
        return "le_5s"
    if value <= 10.0:
        return "5_10s"
    if value <= 20.0:
        return "10_20s"
    return "gt_20s"


def risk_descriptor(
    *,
    soft_flags: Iterable[str],
    hard_flags: Iterable[str],
    creator_flow_state: str = "neutral",
    creator_linked_trigger: bool = False,
    early_exit_fraction: float = 0.0,
    extra_hazards: Iterable[str] = (),
) -> dict[str, Any]:
    hard = sorted({str(flag) for flag in hard_flags if str(flag)})
    mechanical = sorted(set(hard) & set(MECHANICAL_HARD_STOPS))
    hazards = {str(flag) for flag in soft_flags if str(flag)}
    hazards.update(str(flag) for flag in extra_hazards if str(flag))
    if creator_flow_state == "distributing":
        hazards.add("creator_distributing")
    if creator_linked_trigger:
        hazards.add("creator_linked_trigger")
    if early_exit_fraction >= 0.20:
        hazards.add("early_holder_distribution")
    severity = min(1.0, sum(HAZARD_WEIGHTS.get(flag, 0.12) for flag in hazards))
    signature = "clean" if not hazards else "+".join(sorted(hazards))
    if severity < 0.20:
        severity_bin = "low"
    elif severity < 0.45:
        severity_bin = "moderate"
    elif severity < 0.70:
        severity_bin = "high"
    else:
        severity_bin = "extreme"
    return {
        "mechanical_hard_stops": mechanical,
        "other_hard_flags": sorted(set(hard) - set(mechanical)),
        "hazards": sorted(hazards),
        "risk_signature": signature,
        "risk_severity": severity,
        "risk_severity_bin": severity_bin,
        "structurally_tradeable": not mechanical,
    }


def _source_venue(source: str | None) -> str:
    raw = str(source or "").upper().replace("/", ":")
    parts = {part for part in raw.split(":") if part}
    for venue in ("PUMP_FUN", "PUMP_AMM", "RAYDIUM"):
        if venue in parts:
            return venue
    return "UNKNOWN"


def _prior_pump_evidence(adapter: Any, token: str, received_at: str) -> bool:
    try:
        with adapter.store._lock:
            row = adapter.store.db.execute(
                "SELECT 1 FROM wallet_discovery_forward_observations "
                "WHERE token_mint=? AND received_at<? AND (UPPER(source) LIKE '%PUMP_FUN%' OR UPPER(source) LIKE '%PUMP_AMM%') "
                "LIMIT 1",
                (token, received_at),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _cross_venue_persistence(adapter: Any, token: str, wallet: str, received_at: str) -> bool:
    try:
        with adapter.store._lock:
            row = adapter.store.db.execute(
                "SELECT 1 FROM wallet_discovery_forward_observations "
                "WHERE token_mint=? AND wallet=? AND side='buy' AND received_at<? "
                "AND (UPPER(source) LIKE '%PUMP_FUN%' OR UPPER(source) LIKE '%PUMP_AMM%') LIMIT 1",
                (token, wallet, received_at),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _lifecycle(adapter: Any, row: dict[str, Any], venue: str) -> str:
    token = str(row.get("token_mint") or "")
    received_at = str(row.get("received_at") or "")
    if venue == "PUMP_FUN":
        return "pump_bonding_curve"
    if venue == "PUMP_AMM":
        try:
            with adapter.store._lock:
                first = adapter.store.db.execute(
                    "SELECT MIN(received_at) AS first_at FROM wallet_discovery_forward_observations "
                    "WHERE token_mint=? AND UPPER(source) LIKE '%PUMP_AMM%'",
                    (token,),
                ).fetchone()
            first_at = str(first["first_at"] or "") if first else ""
            if first_at and received_at:
                start = datetime.fromisoformat(first_at)
                current = datetime.fromisoformat(received_at)
                age = max(0.0, (current - start).total_seconds())
                if age <= 30:
                    return "pump_amm_immediate_graduation_0_30s"
                if age <= 120:
                    return "pump_amm_early_post_graduation_30_120s"
                if age <= 300:
                    return "pump_amm_established_continuation_2_5m"
                return "pump_amm_mature_intraday_momentum"
        except Exception:
            pass
        return "pump_amm_post_bonding_curve"
    if venue == "RAYDIUM":
        return "raydium_post_pump_migration_evidence" if _prior_pump_evidence(adapter, token, received_at) else "raydium_native_or_migration_unproven"
    return "unknown_or_unsupported_venue"


def _v5_schema(adapter: Any) -> None:
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS risk_conditioned_alpha_v5_trials ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, strategy_version TEXT NOT NULL, "
            "source_signature TEXT NOT NULL, token_mint TEXT NOT NULL, trigger_wallet TEXT NOT NULL, "
            "lane TEXT NOT NULL, selected INTEGER NOT NULL, decision TEXT NOT NULL, decision_reason TEXT NOT NULL, "
            "venue TEXT NOT NULL, lifecycle TEXT NOT NULL, regime TEXT NOT NULL, trigger_role TEXT NOT NULL, "
            "flow_state TEXT NOT NULL, risk_signature TEXT NOT NULL, risk_severity REAL NOT NULL, risk_json TEXT NOT NULL, "
            "context_key TEXT NOT NULL, chase_band TEXT NOT NULL, latency_band TEXT NOT NULL, threshold_challenger INTEGER NOT NULL, "
            "position_fraction REAL NOT NULL, quote_input_lamports INTEGER, entry_fee_lamports INTEGER, entry_token_raw INTEGER, "
            "entry_cost_sol REAL, immediate_exit_net_sol REAL, round_trip_cost_fraction REAL, entry_executable INTEGER NOT NULL, "
            "exit_executable INTEGER NOT NULL, observed_at TEXT NOT NULL, created_at TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "UNIQUE(release_commit, source_signature, lane))"
        )
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS risk_conditioned_alpha_v5_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, strategy_version TEXT NOT NULL, "
            "source_signature TEXT NOT NULL, exit_signature TEXT NOT NULL, token_mint TEXT NOT NULL, lane TEXT NOT NULL, "
            "venue TEXT NOT NULL, lifecycle TEXT NOT NULL, regime TEXT NOT NULL, risk_signature TEXT NOT NULL, "
            "context_key TEXT NOT NULL, position_fraction REAL NOT NULL, net_return REAL NOT NULL, "
            "exit_reason TEXT NOT NULL, settled_at TEXT NOT NULL, paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "UNIQUE(release_commit, source_signature, lane))"
        )
        adapter.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_risk_conditioned_v5_context ON "
            "risk_conditioned_alpha_v5_outcomes(release_commit,lane,venue,lifecycle,regime,id)"
        )


def _context_returns(adapter: Any, *, lane: str, venue: str, lifecycle: str, regime: str, context_key: str) -> tuple[list[float], str]:
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT net_return FROM risk_conditioned_alpha_v5_outcomes WHERE release_commit=? AND context_key=? ORDER BY id",
            (adapter.release_commit, context_key),
        ).fetchall()
        if len(rows) >= 20:
            return [float(row["net_return"]) for row in rows], "exact_context"
        rows = adapter.store.db.execute(
            "SELECT net_return FROM risk_conditioned_alpha_v5_outcomes WHERE release_commit=? AND lane=? AND venue=? AND lifecycle=? AND regime=? ORDER BY id",
            (adapter.release_commit, lane, venue, lifecycle, regime),
        ).fetchall()
        if len(rows) >= MIN_FORWARD_SAMPLES:
            return [float(row["net_return"]) for row in rows], "lane_venue_lifecycle_regime"
        rows = adapter.store.db.execute(
            "SELECT net_return FROM risk_conditioned_alpha_v5_outcomes WHERE release_commit=? AND lane=? AND venue=? AND lifecycle=? ORDER BY id",
            (adapter.release_commit, lane, venue, lifecycle),
        ).fetchall()
    return [float(row["net_return"]) for row in rows], "lane_venue_lifecycle" if rows else "none"


def _candidate_lanes(
    *,
    venue: str,
    lifecycle: str,
    creator_linked: bool,
    independent_count: int,
    hazards: Sequence[str],
    cross_venue_persistence: bool,
) -> list[str]:
    lanes: list[str] = []
    if creator_linked:
        lanes.append("creator_insider_continuation")
    else:
        lanes.append("elite_wallet_continuation")
    if independent_count >= 1:
        lanes.append("entity_flow_momentum")
    if venue == "PUMP_AMM" and lifecycle.startswith("pump_amm_"):
        lanes.append("graduation_continuation")
    if venue == "RAYDIUM" and cross_venue_persistence:
        lanes.append("raydium_cross_venue_persistence")
    if hazards:
        lanes.append("hazard_continuation")
    return list(dict.fromkeys(lanes))


def _lane_cap(lane: str, risk_severity: float) -> float:
    if lane == "hazard_continuation":
        return 0.02
    if lane == "creator_insider_continuation":
        return 0.05
    if lane in {"graduation_continuation", "raydium_cross_venue_persistence", "entity_flow_momentum"}:
        return 0.10
    return 0.20 if risk_severity < 0.20 else 0.05


def _regime_multiplier(regime: str) -> float:
    if regime == "weak_or_deteriorating":
        return 0.50
    if regime == "high_speculation":
        return 1.10
    if regime == "broad_mania":
        # Individual entry may remain permissive, but correlated exposure should
        # not expand simply because the whole market is manic.
        return 0.85
    return 1.0


def _bootstrap_fraction(lane: str, severity: float) -> float:
    if lane == "hazard_continuation" or severity >= 0.45:
        return 0.005
    if lane == "creator_insider_continuation":
        return 0.005
    return 0.01


def _v5_pre_context(adapter: Any, row: dict[str, Any], *, hard: Sequence[str], soft: Sequence[str], early_exit: float) -> dict[str, Any]:
    at = datetime.fromisoformat(str(row["received_at"]))
    token = str(row["token_mint"])
    wallet = str(row["wallet"])
    creator = adapter.execution._deployer(token, at)
    trigger_entity, creator_entity, independent_count = adapter._confirmation_context(token, wallet, creator, at)
    creator_linked = creator_entity is not None and creator_entity == trigger_entity
    creator_flow = adapter._creator_flow_state(token, creator, at)
    regime = adapter._market_regime(at).value
    venue = _source_venue(str(row.get("source") or ""))
    lifecycle = _lifecycle(adapter, row, venue)
    descriptor = risk_descriptor(
        soft_flags=soft,
        hard_flags=hard,
        creator_flow_state=creator_flow,
        creator_linked_trigger=creator_linked,
        early_exit_fraction=early_exit,
    )
    persistence = venue == "RAYDIUM" and _cross_venue_persistence(adapter, token, wallet, str(row["received_at"]))
    role = "creator" if creator_linked else "independent_wallet"
    flow_state = creator_flow
    lanes = _candidate_lanes(
        venue=venue,
        lifecycle=lifecycle,
        creator_linked=creator_linked,
        independent_count=independent_count,
        hazards=descriptor["hazards"],
        cross_venue_persistence=persistence,
    )
    return {
        "at": at,
        "token": token,
        "wallet": wallet,
        "creator": creator,
        "trigger_entity": trigger_entity,
        "creator_entity": creator_entity,
        "independent_count": independent_count,
        "creator_linked": creator_linked,
        "creator_flow": creator_flow,
        "regime": regime,
        "venue": venue,
        "lifecycle": lifecycle,
        "risk": descriptor,
        "cross_venue_persistence": persistence,
        "role": role,
        "flow_state": flow_state,
        "lanes": lanes,
        "early_exit": early_exit,
    }


def _context_key(pre: dict[str, Any], lane: str, *, chase: float | None, latency: float | None) -> str:
    return "|".join(
        (
            lane,
            str(pre["venue"]),
            str(pre["lifecycle"]),
            str(pre["regime"]),
            str(pre["role"]),
            str(pre["risk"]["risk_signature"]),
            str(pre["flow_state"]),
            chase_band(chase),
            latency_band(latency),
        )
    )


def _choose_lane_and_fraction(adapter: Any, pre: dict[str, Any], *, chase: float | None = None, latency: float | None = None) -> tuple[str | None, float, dict[str, Any]]:
    profiles: dict[str, Any] = {}
    candidates: list[tuple[str, RobustReturnProfile, str]] = []
    for lane in pre["lanes"]:
        key = _context_key(pre, lane, chase=chase, latency=latency)
        values, source = _context_returns(
            adapter,
            lane=lane,
            venue=pre["venue"],
            lifecycle=pre["lifecycle"],
            regime=pre["regime"],
            context_key=key,
        )
        cap = _lane_cap(lane, float(pre["risk"]["risk_severity"]))
        profile = robust_return_profile(values, max_fraction=cap)
        profiles[lane] = {**asdict(profile), "evidence_source": source, "context_key": key}
        candidates.append((lane, profile, key))

    promoted = [item for item in candidates if item[1].state == "promoted_positive_log_growth" and item[1].best_fraction > 0.0]
    if promoted:
        lane, profile, _ = max(
            promoted,
            key=lambda item: item[1].best_expected_log_growth if item[1].best_expected_log_growth is not None else float("-inf"),
        )
        requested = profile.best_fraction
    else:
        nondemoted = [item for item in candidates if item[1].state != "demoted_nonpositive_log_growth"]
        if not nondemoted:
            return None, 0.0, profiles
        priorities = {
            "graduation_continuation": 6,
            "raydium_cross_venue_persistence": 5,
            "creator_insider_continuation": 4,
            "entity_flow_momentum": 3,
            "elite_wallet_continuation": 2,
            "hazard_continuation": 1,
        }
        lane, profile, _ = max(nondemoted, key=lambda item: priorities.get(item[0], 0))
        requested = _bootstrap_fraction(lane, float(pre["risk"]["risk_severity"]))

    requested *= _regime_multiplier(str(pre["regime"]))
    requested *= max(0.25, 1.0 - 0.60 * float(pre["risk"]["risk_severity"]))
    requested = min(_lane_cap(lane, float(pre["risk"]["risk_severity"])), requested)
    return lane, max(0.0025, requested) if requested > 0.0 else 0.0, profiles


async def _buy_with_v5(self: Any, row: dict[str, Any]) -> None:
    """Production v5 buy path: one exact quote snapshot, legacy compatibility plus v5 authority."""
    if not bool(row["copyable"]):
        return
    _v5_schema(self)
    at = datetime.fromisoformat(str(row["received_at"]))
    hard, soft, early_exit = await self.execution._risk(row, at)
    pre = _v5_pre_context(self, row, hard=hard, soft=soft, early_exit=float(early_exit))

    selected_lane, requested_fraction, pre_profiles = _choose_lane_and_fraction(self, pre)
    if pre["risk"]["mechanical_hard_stops"]:
        requested_fraction = 0.0
    # Keep >40% chase as a measured challenger surface, but do not assign paper
    # authority until an exact observation establishes that state below.
    fraction_for_quote = requested_fraction if requested_fraction > 0.0 else 0.005
    execution = await self._execution(row, fraction_for_quote) if not pre["risk"]["mechanical_hard_stops"] else None
    chase = float(execution["chase_fraction"]) if execution else _finite(row.get("chase_fraction"))
    latency = float(execution["signal_to_entry_seconds"]) if execution else float(row.get("observation_lag_ms") or 0.0) / 1000.0

    # Re-select with the actual executable chase/latency bucket. This can only
    # choose among already-computed strategy families; no second quote is issued.
    selected_lane, desired_fraction, profiles = _choose_lane_and_fraction(self, pre, chase=chase, latency=latency)
    threshold_challenger = bool(chase is not None and chase > 0.15)
    latency_inaccessible = latency is None or latency > 20.0
    extreme_chase = chase is not None and chase > 0.40
    entry_executable = execution is not None
    exit_executable = bool(execution and execution.get("exit_net_sol") is not None)
    structural_ok = bool(pre["risk"]["structurally_tradeable"] and entry_executable and exit_executable and not latency_inaccessible)
    paper_enter = structural_ok and selected_lane is not None and desired_fraction > 0.0 and not extreme_chase

    # Compatibility mirror: preserve the existing final-strategy evidence surface
    # and FOMO's exact-entry dependency, but use this one v5 quote snapshot rather
    # than adding duplicate Jupiter/RPC work.
    from .profit_first_entity_final import FinalLane, FinalOpportunity, UNIFIED_LANE
    from .profit_first_entity_final_research import FINAL_STRATEGY_VERSION

    signal_to_entry = float(latency or 0.0)
    opportunity = FinalOpportunity(
        token=pre["token"],
        source_signature=str(row["signature"]),
        observed_at=str(row["observed_at"]),
        trigger_entity=pre["trigger_entity"],
        creator_entity=pre["creator_entity"],
        independent_confirmation_count=int(pre["independent_count"]),
        creator_linked_trigger=bool(pre["creator_linked"]),
        creator_flow_state=str(pre["creator_flow"]),
        chase_fraction=float(chase or 0.0),
        signal_to_entry_seconds=signal_to_entry,
        round_trip_cost_fraction=float(execution["round_trip_cost_fraction"]) if execution else 0.0,
        entry_executable=entry_executable,
        exit_executable=exit_executable,
        regime=self._market_regime(at),
        independent_demand_strength=min(1.0, int(pre["independent_count"]) / 3.0),
        early_buyer_exit_fraction=float(early_exit),
        soft_risk_flags=frozenset(soft),
        hard_risk_flags=frozenset(hard),
    )
    constraints = self._constraints(pre["creator_entity"], exit_executable)
    legacy_decisions = self.strategy.evaluate_all(opportunity, constraints)
    now = _utcnow()
    group = str(row["signature"])
    with self.store._lock, self.store.db:
        for lane in FinalLane:
            decision = legacy_decisions[lane.value]
            context = self.strategy.context(opportunity, lane)
            self.store.db.execute(
                "INSERT OR IGNORE INTO profit_first_final_trials("
                "epoch_id,release_commit,strategy_version,source_signature,observation_group,token_mint,trigger_wallet,lane,"
                "observed_at,received_at,regime,opportunity_json,context_json,decision_json,assigned_position_fraction,"
                "quote_input_lamports,entry_fee_lamports,entry_token_raw,token_decimals,entry_all_in_price_sol,immediate_exit_net_sol,"
                "round_trip_cost_fraction,signal_to_entry_seconds,quote_latency_ms,entry_executable,exit_executable,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    self.epoch_id,self.release_commit,FINAL_STRATEGY_VERSION,str(row["signature"]),group,pre["token"],pre["wallet"],lane.value,
                    str(row["observed_at"]),str(row["received_at"]),pre["regime"],_dump(asdict(opportunity)),_dump(asdict(context)),
                    _dump(asdict(decision)),fraction_for_quote,execution["input_lamports"] if execution else None,
                    execution["entry_fee_lamports"] if execution else None,execution["token_raw"] if execution else None,
                    execution["decimals"] if execution else None,execution["entry_price_sol"] if execution else None,
                    execution["exit_net_sol"] if execution else None,execution["round_trip_cost_fraction"] if execution else 0.0,
                    signal_to_entry,execution["quote_latency_ms"] if execution else None,1 if entry_executable else 0,
                    1 if exit_executable else 0,now,
                ),
            )
        unified = legacy_decisions[UNIFIED_LANE]
        self.store.db.execute(
            "INSERT OR IGNORE INTO profit_first_final_trials("
            "epoch_id,release_commit,strategy_version,source_signature,observation_group,token_mint,trigger_wallet,lane,"
            "observed_at,received_at,regime,opportunity_json,context_json,decision_json,assigned_position_fraction,"
            "quote_input_lamports,entry_fee_lamports,entry_token_raw,token_decimals,entry_all_in_price_sol,immediate_exit_net_sol,"
            "round_trip_cost_fraction,signal_to_entry_seconds,quote_latency_ms,entry_executable,exit_executable,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                self.epoch_id,self.release_commit,FINAL_STRATEGY_VERSION,str(row["signature"]),group,pre["token"],pre["wallet"],UNIFIED_LANE,
                str(row["observed_at"]),str(row["received_at"]),pre["regime"],_dump(asdict(opportunity)),None,
                _dump(asdict(unified)),fraction_for_quote,execution["input_lamports"] if execution else None,
                execution["entry_fee_lamports"] if execution else None,execution["token_raw"] if execution else None,
                execution["decimals"] if execution else None,execution["entry_price_sol"] if execution else None,
                execution["exit_net_sol"] if execution else None,execution["round_trip_cost_fraction"] if execution else 0.0,
                signal_to_entry,execution["quote_latency_ms"] if execution else None,1 if entry_executable else 0,
                1 if exit_executable else 0,now,
            ),
        )

        for lane in pre["lanes"]:
            key = _context_key(pre, lane, chase=chase, latency=latency)
            is_selected = lane == selected_lane
            decision = "paper_enter" if is_selected and paper_enter else "paper_observe"
            if pre["risk"]["mechanical_hard_stops"]:
                decision = "reject_mechanical_hard_stop"
            elif latency_inaccessible:
                decision = "reject_latency_inaccessible"
            elif extreme_chase:
                decision = "paper_challenger_observe_gt_40pct_chase"
            elif not entry_executable or not exit_executable:
                decision = "reject_execution_incomplete"
            elif is_selected and threshold_challenger:
                decision = "paper_enter_threshold_challenger"
            self.store.db.execute(
                "INSERT OR IGNORE INTO risk_conditioned_alpha_v5_trials("
                "release_commit,strategy_version,source_signature,token_mint,trigger_wallet,lane,selected,decision,decision_reason,"
                "venue,lifecycle,regime,trigger_role,flow_state,risk_signature,risk_severity,risk_json,context_key,chase_band,latency_band,"
                "threshold_challenger,position_fraction,quote_input_lamports,entry_fee_lamports,entry_token_raw,entry_cost_sol,"
                "immediate_exit_net_sol,round_trip_cost_fraction,entry_executable,exit_executable,observed_at,created_at,paper_only,live_money_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    self.release_commit,STRATEGY_VERSION,str(row["signature"]),pre["token"],pre["wallet"],lane,1 if is_selected else 0,
                    decision,"risk_conditioned_forward_only_expected_log_growth",pre["venue"],pre["lifecycle"],pre["regime"],pre["role"],
                    pre["flow_state"],pre["risk"]["risk_signature"],float(pre["risk"]["risk_severity"]),_dump(pre["risk"]),key,
                    chase_band(chase),latency_band(latency),1 if threshold_challenger else 0,float(fraction_for_quote),
                    execution["input_lamports"] if execution else None,execution["entry_fee_lamports"] if execution else None,
                    execution["token_raw"] if execution else None,
                    ((execution["input_lamports"] + execution["entry_fee_lamports"]) / 1_000_000_000.0) if execution else None,
                    execution["exit_net_sol"] if execution else None,execution["round_trip_cost_fraction"] if execution else None,
                    1 if entry_executable else 0,1 if exit_executable else 0,str(row["observed_at"]),now,
                ),
            )
    try:
        self.store.append(
            "risk_conditioned_alpha_v5_decision",
            now,
            {
                "source_signature": str(row["signature"]),
                "venue": pre["venue"],
                "lifecycle": pre["lifecycle"],
                "selected_lane": selected_lane,
                "risk_signature": pre["risk"]["risk_signature"],
                "risk_severity": pre["risk"]["risk_severity"],
                "paper_enter": paper_enter,
                "threshold_challenger": threshold_challenger,
                "paper_only": True,
                "live_money_authority": False,
            },
        )
    except Exception:
        pass


async def _sell_with_v5(self: Any, row: dict[str, Any]) -> None:
    if _ORIGINAL_FINAL_SELL is None:
        raise RuntimeError("risk-conditioned alpha v5 sell wrapper not installed")
    await _ORIGINAL_FINAL_SELL(self, row)
    _v5_schema(self)
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT v.source_signature,v.token_mint,v.lane,v.venue,v.lifecycle,v.regime,v.risk_signature,v.context_key,v.position_fraction,"
            "o.exit_signature,o.net_return,o.exit_reason FROM risk_conditioned_alpha_v5_trials v "
            "JOIN profit_first_final_outcomes o ON o.epoch_id=? AND o.source_signature=v.source_signature AND o.lane='unified_profit_maximizer' "
            "LEFT JOIN risk_conditioned_alpha_v5_outcomes x ON x.release_commit=v.release_commit AND x.source_signature=v.source_signature AND x.lane=v.lane "
            "WHERE v.release_commit=? AND v.token_mint=? AND v.decision LIKE 'paper_enter%' AND x.id IS NULL",
            (self.epoch_id, self.release_commit, str(row.get("token_mint") or "")),
        ).fetchall()
    now = _utcnow()
    with self.store._lock, self.store.db:
        for item in rows:
            self.store.db.execute(
                "INSERT OR IGNORE INTO risk_conditioned_alpha_v5_outcomes("
                "release_commit,strategy_version,source_signature,exit_signature,token_mint,lane,venue,lifecycle,regime,risk_signature,"
                "context_key,position_fraction,net_return,exit_reason,settled_at,paper_only,live_money_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    self.release_commit,STRATEGY_VERSION,str(item["source_signature"]),str(item["exit_signature"]),str(item["token_mint"]),
                    str(item["lane"]),str(item["venue"]),str(item["lifecycle"]),str(item["regime"]),str(item["risk_signature"]),
                    str(item["context_key"]),float(item["position_fraction"]),float(item["net_return"]),str(item["exit_reason"]),now,
                ),
            )


def _fomo_classify_v5(features: Any, *, max_chase_fraction: float = 0.15, max_latency_seconds: float = 20.0) -> Any:
    from .fomo_continuation_shadow import FomoState

    blockers: list[str] = []
    hazards: list[str] = []
    if features.chase_fraction is None:
        blockers.append("chase_unknown")
    elif features.chase_fraction > 0.40:
        blockers.append("chase_above_research_ceiling")
    elif features.chase_fraction > max_chase_fraction:
        hazards.append(chase_band(features.chase_fraction))
    if features.signal_to_entry_seconds is None:
        blockers.append("signal_to_entry_unknown")
    elif features.signal_to_entry_seconds > max_latency_seconds:
        blockers.append("signal_to_entry_above_limit")
    if not features.risk_complete:
        blockers.append("risk_incomplete")
    if features.creator_distributing:
        hazards.append("creator_distributing")
    if features.early_holder_exit_fraction >= 0.20:
        hazards.append("early_holder_distribution")
    if features.quote_deterioration_fraction is not None and features.quote_deterioration_fraction >= 0.05:
        hazards.append("quote_deteriorating")
    if features.exit_slippage_deterioration_fraction is not None and features.exit_slippage_deterioration_fraction >= 0.05:
        hazards.append("exit_slippage_deteriorating")

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

    # Hazard evidence reduces confidence but does not itself veto a trade. Actual
    # sell-pressure exhaustion still exits when flow is decelerating/reversing.
    score -= min(1.25, 0.20 * len(hazards))
    exhaustion = (
        features.transaction_frequency_acceleration < 0.8
        and features.net_buy_flow_acceleration < 0.8
        and features.buy_sell_imbalance < 0.0
    )
    if exhaustion:
        state = "fomo_exhaustion"
    elif blockers:
        state = "late_or_inaccessible_fomo"
    elif score >= 5.0 and features.new_buyer_acceleration > 1.0 and features.net_buy_flow_acceleration > 1.0:
        state = "active_fomo"
    elif score >= 2.5 and features.new_buyer_acceleration >= 1.0:
        state = "pre_fomo"
    else:
        state = "no_fomo"

    variants: list[str] = []
    if features.trigger_is_proven_wallet:
        variants.append("wallet_signal_only")
        if features.independent_buyers_long >= 1:
            variants.append("wallet_plus_entity_confirmation")
        if state in {"pre_fomo", "active_fomo"}:
            variants.append("wallet_plus_fomo_acceleration")
    elif state in {"pre_fomo", "active_fomo"} and features.independent_buyers_long >= 1:
        variants.append("pure_entity_flow_fomo")
    variants.append("hazard_fomo" if hazards else "clean_fomo")
    variants.extend(hazards)
    return FomoState(
        state=state,
        score=score,
        structurally_accessible=not blockers,
        blockers=tuple(dict.fromkeys(blockers)),
        experiment_variants=tuple(dict.fromkeys(variants)),
        feature_version="fomo-risk-conditioned-v2",
    )


def _fomo_profile_v5(values: list[float]) -> dict[str, Any]:
    profile = robust_return_profile(values, grid=FOMO_ACTIVE_GRID, max_fraction=0.05)
    challenger = {
        fraction: _expected_log_growth(values, fraction)
        for fraction in FOMO_CHALLENGER_GRID
    }
    if profile.sample_count < MIN_FORWARD_SAMPLES:
        state = "bootstrap_forward_evidence"
    elif profile.state == "promoted_positive_log_growth":
        state = "promoted_fomo_wallet"
    elif profile.state == "demoted_nonpositive_log_growth":
        state = "demoted_fomo_wallet"
    else:
        state = "observe_mixed_fomo_wallet"
    return {
        "sample_count": profile.sample_count,
        "mean_residual_roi_pct": profile.mean_return * 100.0 if profile.mean_return is not None else None,
        "median_residual_roi_pct": profile.median_return * 100.0 if profile.median_return is not None else None,
        "trimmed_mean_residual_roi_ex_best_1_pct": profile.trimmed_mean_ex_best * 100.0 if profile.trimmed_mean_ex_best is not None else None,
        "positive_rate_pct": profile.hit_rate * 100.0 if profile.hit_rate is not None else None,
        "mature": profile.sample_count >= MIN_FORWARD_SAMPLES,
        "state": state,
        "best_paper_position_fraction": min(0.05, profile.best_fraction),
        "best_expected_log_growth": profile.best_expected_log_growth,
        "expected_shortfall_20_pct": profile.expected_shortfall_20 * 100.0 if profile.expected_shortfall_20 is not None else None,
        "winner_concentration_pct": profile.winner_concentration * 100.0 if profile.winner_concentration is not None else None,
        "max_drawdown_at_best_fraction_pct": profile.max_drawdown_at_best_fraction * 100.0 if profile.max_drawdown_at_best_fraction is not None else None,
        "challenger_expected_log_growth": challenger,
        "hit_rate_is_promotion_veto": False,
        "historical_evidence_used_for_promotion": False,
    }


def _fomo_risk_class(state_payload: dict[str, Any]) -> str:
    variants = {str(value) for value in (state_payload.get("experiment_variants") or ())}
    return "hazard_fomo" if "hazard_fomo" in variants else "clean_fomo"


def _fomo_context_returns_v5(adapter: Any, *, wallet: str, venue: str, lifecycle: str, regime: str, risk_class: str) -> list[float]:
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT s.state_json,o.net_return,t.trigger_wallet FROM fomo_shadow_observations s "
            "JOIN profit_first_final_trials t ON t.epoch_id=? AND t.source_signature=s.source_signature AND t.lane='unified_profit_maximizer' "
            "JOIN fomo_shadow_outcomes o ON o.release_commit=s.release_commit AND o.source_signature=s.source_signature "
            "WHERE s.release_commit=? AND s.venue=? AND s.lifecycle=? AND s.regime=? ORDER BY s.id",
            (adapter.epoch_id, adapter.release_commit, venue, lifecycle, regime),
        ).fetchall()
    values: list[float] = []
    for row in rows:
        if str(row["trigger_wallet"] or "") != wallet:
            continue
        payload = _safe_json(row["state_json"])
        if str(payload.get("state") or "") not in {"pre_fomo", "active_fomo"}:
            continue
        if _fomo_risk_class(payload) != risk_class:
            continue
        value = _finite(row["net_return"])
        if value is not None:
            values.append(value)
    return values


def _fomo_paper_decision_v5(adapter: Any, *, observation: dict[str, Any], trial: dict[str, Any]) -> dict[str, Any]:
    state_payload = _safe_json(observation.get("state_json"))
    fomo_state = str(state_payload.get("state") or "unknown")
    accessible = bool(state_payload.get("structurally_accessible"))
    wallet = str(trial.get("trigger_wallet") or "")
    venue = str(observation.get("venue") or "UNKNOWN")
    lifecycle = str(observation.get("lifecycle") or "unknown")
    regime = str(observation.get("regime") or trial.get("regime") or "unknown")
    risk_class = _fomo_risk_class(state_payload)
    values = _fomo_context_returns_v5(
        adapter,
        wallet=wallet,
        venue=venue,
        lifecycle=lifecycle,
        regime=regime,
        risk_class=risk_class,
    )
    profile = _fomo_profile_v5(values)
    profile.update({"wallet": wallet, "venue": venue, "lifecycle": lifecycle, "regime": regime, "risk_class": risk_class})

    if fomo_state not in {"pre_fomo", "active_fomo"}:
        return {"decision": "no_entry_nonactionable_fomo_state", "reason": fomo_state, "position_fraction": 0.0, "profile": profile}
    if not accessible:
        return {
            "decision": "no_entry_structurally_inaccessible",
            "reason": ",".join(str(x) for x in (state_payload.get("blockers") or ())) or "fomo_accessibility_failed",
            "position_fraction": 0.0,
            "profile": profile,
        }
    if not bool(trial.get("entry_executable")) or not bool(trial.get("exit_executable")):
        return {"decision": "no_entry_execution_incomplete", "reason": "entry_and_exit_executable_evidence_required", "position_fraction": 0.0, "profile": profile}
    import solana_roi.fomo_paper_strategy as paper
    if paper._token_already_open(adapter, str(trial.get("token_mint") or "")):
        return {"decision": "no_entry_token_already_open", "reason": "one_open_fomo_paper_position_per_token", "position_fraction": 0.0, "profile": profile}
    if profile["state"] == "demoted_fomo_wallet":
        return {"decision": "no_entry_demoted_fomo_wallet", "reason": "nonpositive_robust_expected_log_growth", "position_fraction": 0.0, "profile": profile}

    if profile["state"] == "promoted_fomo_wallet":
        requested = float(profile["best_paper_position_fraction"] or 0.0)
        decision = "paper_enter_promoted_hazard_fomo" if risk_class == "hazard_fomo" else "paper_enter_promoted_clean_fomo"
    else:
        requested = 0.005 if risk_class == "hazard_fomo" else 0.01
        decision = "paper_enter_hazard_fomo_probe" if risk_class == "hazard_fomo" else "paper_enter_clean_fomo_probe"
    if risk_class == "hazard_fomo":
        requested = min(0.02, requested)
    requested *= _regime_multiplier(regime)
    available = max(0.0, 1.0 - paper._open_position_fraction(adapter))
    fraction = min(0.05, requested, available)
    if fraction <= 0.0:
        return {"decision": "no_entry_paper_capacity_exhausted", "reason": "open_fomo_paper_fraction_at_capacity", "position_fraction": 0.0, "profile": profile}
    return {
        "decision": decision,
        "reason": f"risk_conditioned_{risk_class}_forward_log_growth",
        "position_fraction": fraction,
        "profile": profile,
    }


def _status_with_v5(self: Any) -> dict[str, Any]:
    if _ORIGINAL_FINAL_STATUS is None:
        raise RuntimeError("risk-conditioned alpha v5 status wrapper not installed")
    payload = _ORIGINAL_FINAL_STATUS(self)
    try:
        _v5_schema(self)
        with self.store._lock:
            trials = int(self.store.db.execute(
                "SELECT COUNT(*) FROM risk_conditioned_alpha_v5_trials WHERE release_commit=?",
                (self.release_commit,),
            ).fetchone()[0])
            selected = int(self.store.db.execute(
                "SELECT COUNT(*) FROM risk_conditioned_alpha_v5_trials WHERE release_commit=? AND selected=1 AND decision LIKE 'paper_enter%'",
                (self.release_commit,),
            ).fetchone()[0])
            hazards = int(self.store.db.execute(
                "SELECT COUNT(*) FROM risk_conditioned_alpha_v5_trials WHERE release_commit=? AND lane='hazard_continuation'",
                (self.release_commit,),
            ).fetchone()[0])
            outcomes = int(self.store.db.execute(
                "SELECT COUNT(*) FROM risk_conditioned_alpha_v5_outcomes WHERE release_commit=?",
                (self.release_commit,),
            ).fetchone()[0])
        payload["risk_conditioned_alpha_v5"] = {
            "strategy_version": STRATEGY_VERSION,
            "paper_strategy_authority": True,
            "forward_only_promotion": True,
            "historical_promotion_authority": False,
            "venue_lifecycle_isolated": True,
            "risk_signature_replaces_soft_risk_count_for_v5": True,
            "danger_is_probabilistic_hazard_not_automatic_veto": True,
            "mechanical_hard_stops": sorted(MECHANICAL_HARD_STOPS),
            "hazard_challenger_enabled": True,
            "pumpfun_first_slot_sniping_enabled": False,
            "threshold_challenger_bands": ["15-25%", "25-40%", ">40% observe-only"],
            "trial_rows": trials,
            "selected_paper_entries": selected,
            "hazard_trial_rows": hazards,
            "forward_outcome_rows": outcomes,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    except Exception as exc:
        payload["risk_conditioned_alpha_v5"] = {
            "strategy_version": STRATEGY_VERSION,
            "failed_closed": True,
            "error": f"{type(exc).__name__}: v5 status unavailable",
            "paper_only": True,
            "live_money_authority": False,
        }
    return payload


def install_risk_conditioned_alpha_v5() -> None:
    """Install the v5 paper-only strategy layer exactly once.

    The installer is intentionally composition-only: it adds no signer, private-key
    surface or transaction submission method. Existing exact quote/exit plumbing is
    reused and all promotion remains release-bound forward evidence.
    """
    global _INSTALLED, _ORIGINAL_FINAL_BUY, _ORIGINAL_FINAL_SELL, _ORIGINAL_FINAL_STATUS
    if _INSTALLED:
        return

    from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
    from . import fomo_continuation_shadow as fomo_shadow
    from . import fomo_paper_strategy as fomo_paper

    current_buy = FinalProfitFirstResearchAdapter._buy
    if not bool(getattr(current_buy, "_roi_risk_conditioned_v5", False)):
        _ORIGINAL_FINAL_BUY = current_buy
        setattr(_buy_with_v5, "_roi_risk_conditioned_v5", True)
        FinalProfitFirstResearchAdapter._buy = _buy_with_v5  # type: ignore[method-assign]

    current_sell = FinalProfitFirstResearchAdapter._sell
    if not bool(getattr(current_sell, "_roi_risk_conditioned_v5", False)):
        _ORIGINAL_FINAL_SELL = current_sell
        setattr(_sell_with_v5, "_roi_risk_conditioned_v5", True)
        FinalProfitFirstResearchAdapter._sell = _sell_with_v5  # type: ignore[method-assign]

    current_status = FinalProfitFirstResearchAdapter.status
    if not bool(getattr(current_status, "_roi_risk_conditioned_v5", False)):
        _ORIGINAL_FINAL_STATUS = current_status
        setattr(_status_with_v5, "_roi_risk_conditioned_v5", True)
        FinalProfitFirstResearchAdapter.status = _status_with_v5  # type: ignore[method-assign]

    fomo_shadow.classify_fomo_state = _fomo_classify_v5
    fomo_paper.classify_fomo_wallet_returns = _fomo_profile_v5
    fomo_paper._paper_decision = _fomo_paper_decision_v5
    _INSTALLED = True


__all__ = [
    "STRATEGY_VERSION",
    "MECHANICAL_HARD_STOPS",
    "RobustReturnProfile",
    "robust_return_profile",
    "risk_descriptor",
    "chase_band",
    "latency_band",
    "install_risk_conditioned_alpha_v5",
]
