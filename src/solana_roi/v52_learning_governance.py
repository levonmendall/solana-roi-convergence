from __future__ import annotations

import json
import math
import statistics
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from . import v52_profit_confidence_completion as completion
from .strategy_v52_authority import canonical_strategy_evolution
from .v52_continuous_evolution import (
    PROTECTED_STRATEGY_KEYS,
    PolicyOutcome,
    ProspectivePolicyTournament,
    TournamentDecision,
)

VERSION = "v52-learning-governance-v1"
INCUMBENT_ID = "canonical_v52"
FIXED_CHALLENGERS: dict[str, dict[str, Any]] = {
    "aggressive_sizing": {
        "target_sizing.bootstrap_fraction_clean": 0.015,
        "target_sizing.bootstrap_fraction_hazard_or_high_severity": 0.0075,
        "position_management.starter_fraction_of_target": 0.50,
    },
    "aggressive_continuation": {
        "position_management.max_scale_fraction_of_target_per_add": 0.50,
        "position_management.runner_fraction_of_target": 0.15,
    },
    "exit_capture": {
        "position_management.first_derisk_fraction_of_position": 0.20,
        "position_management.second_derisk_fraction_of_position": 0.40,
        "position_management.runner_fraction_of_target": 0.20,
    },
    "wallet_acceleration": {
        "detection_intelligence.minimum_broad_independent_clusters": 4,
        "detection_intelligence.minimum_wallet_quality": 0.70,
    },
    "chase_optimization": {
        "chase_observe_only_fraction": 0.45,
    },
    "concentration": {
        "target_sizing.bootstrap_fraction_clean": 0.02,
        "position_management.starter_fraction_of_target": 0.75,
    },
}

LANE_DECAY_PRIORS_HOURS = {
    "elite_wallet_continuation": 24.0,
    "graduation_continuation": 36.0,
    "raydium_cross_venue_persistence": 72.0,
    "fomo": 12.0,
    "fomo_continuation": 12.0,
    "robinhood": 96.0,
    "robinhood_entity_continuation": 96.0,
    "default": 72.0,
}

MIN_FORWARD_EPISODES = 30
MIN_IMPROVEMENT_RATIO = 1.05
POSTERIOR_PROMOTION_PROBABILITY = 0.95
MAX_PROMOTION_DRAWDOWN = 0.35
DEMOTION_RATIO = 0.95
MIN_DEMOTION_EPISODES = 20

_STORE: Any | None = None
_INSTALLED = False
_BASE_WALLET_PROFILE: Any = None
_BASE_EXIT_FEATURES: Any = None
_BASE_FINALIZE: Any = None
_BASE_RESOLVE_COUNTERFACTUALS: Any = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None = None) -> str:
    return (value or _utcnow()).isoformat()


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _table_exists(store: Any, name: str) -> bool:
    try:
        with store._lock:
            row = store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_lane_decay_profiles ("
            "lane TEXT PRIMARY KEY, half_life_hours REAL NOT NULL, sample_count INTEGER NOT NULL, "
            "median_alpha_life_seconds REAL, updated_at TEXT NOT NULL, paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_governed_challengers ("
            "challenger_id TEXT PRIMARY KEY, family TEXT NOT NULL, trigger_reason TEXT NOT NULL, config_json TEXT NOT NULL, "
            "created_at TEXT NOT NULL, status TEXT NOT NULL, analytical_only INTEGER NOT NULL, paper_only INTEGER NOT NULL, "
            "live_money_authority INTEGER NOT NULL, evidence_json TEXT NOT NULL)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_tournament_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, challenger_id TEXT NOT NULL, stream_id TEXT NOT NULL, lane TEXT NOT NULL, "
            "observed_at TEXT NOT NULL, net_return REAL NOT NULL, drawdown REAL NOT NULL, execution_complete INTEGER NOT NULL, "
            "same_stream INTEGER NOT NULL, prospective INTEGER NOT NULL, UNIQUE(challenger_id,stream_id))"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v52_tournament_outcomes_policy ON "
            "v52_tournament_outcomes(challenger_id,observed_at)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_tournament_decisions ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, evaluated_at TEXT NOT NULL, incumbent_id TEXT NOT NULL, winner_id TEXT, "
            "eligible INTEGER NOT NULL, improvement_ratio REAL, posterior_probability REAL, posterior_lower_advantage REAL, "
            "blockers_json TEXT NOT NULL, scores_json TEXT NOT NULL, action TEXT NOT NULL, paper_only INTEGER NOT NULL, "
            "live_money_authority INTEGER NOT NULL)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_strategy_governance_history ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT NOT NULL, challenger_id TEXT NOT NULL, observed_at TEXT NOT NULL, "
            "from_fingerprint TEXT NOT NULL, to_fingerprint TEXT NOT NULL, changes_json TEXT NOT NULL, rollback_json TEXT NOT NULL, "
            "evidence_json TEXT NOT NULL, paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
        )


def _append(store: Any, kind: str, payload: Mapping[str, Any]) -> None:
    try:
        store.append(kind, _iso(), {**dict(payload), "paper_only": True, "live_money_authority": False})
    except Exception:
        pass


def lane_decay_profile(store: Any, lane: str) -> dict[str, Any]:
    """Learn a lane-specific evidence half-life from forward alpha-life observations."""
    _schema(store)
    lane = str(lane or "default")
    prior = float(LANE_DECAY_PRIORS_HOURS.get(lane, LANE_DECAY_PRIORS_HOURS["default"]))
    values: list[float] = []
    if _table_exists(store, "v52_wallet_lead_outcomes"):
        with store._lock:
            rows = store.db.execute(
                "SELECT alpha_life_seconds FROM v52_wallet_lead_outcomes "
                "WHERE lane=? AND alpha_life_seconds IS NOT NULL AND alpha_life_seconds>0 AND net_return>0 "
                "ORDER BY id DESC LIMIT 250",
                (lane,),
            ).fetchall()
        values = [float(row["alpha_life_seconds"]) for row in rows]
    samples = len(values)
    if samples >= 8:
        median_seconds = statistics.median(values)
        learned = min(168.0, max(2.0, median_seconds / 3600.0))
        evidence = min(1.0, samples / 40.0)
        half_life = prior * (1.0 - evidence) + learned * evidence
    else:
        median_seconds = statistics.median(values) if values else None
        half_life = prior
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO v52_lane_decay_profiles(lane,half_life_hours,sample_count,median_alpha_life_seconds,updated_at,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,1,0) ON CONFLICT(lane) DO UPDATE SET half_life_hours=excluded.half_life_hours,"
            "sample_count=excluded.sample_count,median_alpha_life_seconds=excluded.median_alpha_life_seconds,updated_at=excluded.updated_at",
            (lane, half_life, samples, median_seconds, _iso()),
        )
    return {
        "lane": lane,
        "half_life_hours": half_life,
        "sample_count": samples,
        "median_alpha_life_seconds": median_seconds,
        "learned": samples >= 8,
    }


def _weighted_wallet_rows(store: Any, wallet: str, context_key: str, lane: str) -> tuple[list[dict[str, Any]], float]:
    if not wallet or not _table_exists(store, "v52_wallet_lead_outcomes"):
        return [], float(LANE_DECAY_PRIORS_HOURS.get(lane, 72.0))
    decay = lane_decay_profile(store, lane)
    half_life = float(decay["half_life_hours"])
    cutoff = (_utcnow() - timedelta(hours=half_life * 6.0)).isoformat()
    with store._lock:
        rows = [
            dict(row)
            for row in store.db.execute(
                "SELECT net_return,executable_mfe,executable_mae,capture_ratio,lead_seconds,created_at "
                "FROM v52_wallet_lead_outcomes WHERE wallet=? AND (context_key=? OR lane=?) AND created_at>=? "
                "ORDER BY id DESC LIMIT 300",
                (wallet, context_key, lane, cutoff),
            ).fetchall()
        ]
    return rows, half_life


def bayesian_wallet_posterior(store: Any, wallet: str, context_key: str, lane: str) -> dict[str, Any]:
    """Conservative Beta-Bernoulli + Normal-prior posterior from forward executable outcomes."""
    rows, half_life = _weighted_wallet_rows(store, wallet, context_key, lane)
    now = _utcnow()
    weighted: list[tuple[dict[str, Any], float]] = []
    for row in rows:
        created = _parse_time(row.get("created_at")) or now
        age_h = max(0.0, (now - created).total_seconds() / 3600.0)
        weighted.append((row, 0.5 ** (age_h / max(1e-9, half_life))))
    effective_n = sum(weight for _, weight in weighted)
    successes = sum(weight for row, weight in weighted if float(row["net_return"]) > 0.0)
    failures = max(0.0, effective_n - successes)

    alpha0 = 1.5
    beta0 = 1.5
    alpha = alpha0 + successes
    beta = beta0 + failures
    win_mean = alpha / (alpha + beta)
    win_var = (alpha * beta) / (((alpha + beta) ** 2) * (alpha + beta + 1.0))
    win_lower = max(0.0, win_mean - 1.645 * math.sqrt(max(0.0, win_var)))

    if effective_n > 0.0:
        weighted_mean = sum(float(row["net_return"]) * weight for row, weight in weighted) / effective_n
        weighted_var = sum(weight * (float(row["net_return"]) - weighted_mean) ** 2 for row, weight in weighted) / effective_n
    else:
        weighted_mean = 0.0
        weighted_var = 0.25
    prior_strength = 6.0
    prior_mean = 0.0
    prior_var = 0.25
    posterior_mean = (prior_strength * prior_mean + effective_n * weighted_mean) / (prior_strength + effective_n)
    posterior_var = (prior_var + max(0.0, weighted_var)) / max(1.0, prior_strength + effective_n)
    posterior_se = math.sqrt(max(1e-12, posterior_var))
    z = posterior_mean / posterior_se
    probability_positive = 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))
    return_lower = posterior_mean - 1.645 * posterior_se
    evidence_strength = min(1.0, effective_n / float(MIN_FORWARD_EPISODES))
    confidence = max(0.0, min(1.0, probability_positive * evidence_strength * min(1.0, win_lower / 0.50 if win_lower > 0 else 0.0)))
    return {
        "samples": len(rows),
        "effective_samples": effective_n,
        "lane_half_life_hours": half_life,
        "beta_alpha": alpha,
        "beta_beta": beta,
        "posterior_win_probability_mean": win_mean,
        "posterior_win_probability_lower_90": win_lower,
        "posterior_return_mean": posterior_mean,
        "posterior_return_lower_90": return_lower,
        "posterior_probability_positive": probability_positive,
        "posterior_confidence": confidence,
        "credible_positive_edge": bool(probability_positive >= 0.90 and return_lower > 0.0),
        "paper_only": True,
        "live_money_authority": False,
    }


def _bayesian_wallet_profile(store: Any, wallet: str, context_key: str, lane: str) -> dict[str, Any]:
    base = dict(_BASE_WALLET_PROFILE(store, wallet, context_key, lane)) if _BASE_WALLET_PROFILE else {}
    posterior = bayesian_wallet_posterior(store, wallet, context_key, lane)
    base_quality = max(0.0, min(1.0, float(base.get("quality") or 0.0)))
    confidence = float(posterior["posterior_confidence"])
    quality = base_quality * confidence
    if not posterior["credible_positive_edge"]:
        quality = min(quality, 0.20)
    base["quality"] = quality
    max_mult = 1.25
    base["multiplier"] = 1.0 + (max_mult - 1.0) * quality
    base["bayesian_posterior"] = posterior
    base["lane_specific_decay"] = True
    return base


def wallet_distribution_reversal(store: Any, token: str, trigger_wallet: str, at: datetime) -> dict[str, Any]:
    if not token or not _table_exists(store, "wallet_discovery_forward_observations"):
        return {"triggered": False, "reason": "no_forward_wallet_stream"}
    start = (at - timedelta(seconds=90)).isoformat()
    with store._lock:
        rows = [
            dict(row)
            for row in store.db.execute(
                "SELECT wallet,side,received_at FROM wallet_discovery_forward_observations "
                "WHERE token_mint=? AND received_at>=? AND received_at<=? ORDER BY received_at",
                (token, start, at.isoformat()),
            ).fetchall()
        ]
    buyers = {str(row.get("wallet") or "") for row in rows if str(row.get("side") or "").lower() == "buy" and row.get("wallet")}
    sellers = {str(row.get("wallet") or "") for row in rows if str(row.get("side") or "").lower() == "sell" and row.get("wallet")}
    active = buyers | sellers
    sell_ratio = len(sellers) / max(1, len(active))
    trigger_departed = bool(trigger_wallet and trigger_wallet in sellers)
    coordinated = len(sellers) >= 2 and sell_ratio >= 0.60
    return {
        "triggered": bool(trigger_departed or coordinated),
        "trigger_wallet_departed": trigger_departed,
        "coordinated_distribution": coordinated,
        "distinct_sellers_90s": len(sellers),
        "distinct_buyers_90s": len(buyers),
        "sell_participation_ratio": sell_ratio,
        "reason": "lead_wallet_departure" if trigger_departed else ("coordinated_wallet_distribution" if coordinated else "no_distribution_reversal"),
    }


def _distribution_exit_features(self: Any, item: Mapping[str, Any], row: Mapping[str, Any]) -> Any:
    features = _BASE_EXIT_FEATURES(self, item, row)
    token = str(item.get("token_mint") or row.get("token_mint") or "")
    trigger = str(item.get("trigger_wallet") or "")
    at = _parse_time(row.get("received_at") or row.get("observed_at")) or _utcnow()
    signal = wallet_distribution_reversal(self.store, token, trigger, at)
    if not signal["triggered"]:
        return features
    return replace(
        features,
        successful_scout_exit=bool(features.successful_scout_exit or signal["trigger_wallet_departed"]),
        linked_entity_distribution=bool(features.linked_entity_distribution or signal["coordinated_distribution"]),
        independent_flow_decelerating=True,
        buy_sell_flow_reversal=True,
    )


def _challenger_family(reason: str) -> str | None:
    value = reason.lower()
    if "chase" in value:
        return "chase_optimization"
    if "priority" in value or "portfolio" in value or "capital" in value:
        return "concentration"
    if "wallet" in value or "evidence" in value or "cluster" in value:
        return "wallet_acceleration"
    if "scale" in value or "target" in value or "position" in value:
        return "aggressive_sizing"
    if "continuation" in value or "expiry" in value or "expired" in value:
        return "aggressive_continuation"
    if "exit" in value or "runner" in value or "capture" in value:
        return "exit_capture"
    return None


def ensure_named_challengers(store: Any) -> None:
    _schema(store)
    now = _iso()
    with store._lock, store.db:
        for challenger_id, config in FIXED_CHALLENGERS.items():
            store.db.execute(
                "INSERT OR IGNORE INTO v52_governed_challengers("
                "challenger_id,family,trigger_reason,config_json,created_at,status,analytical_only,paper_only,live_money_authority,evidence_json"
                ") VALUES (?,?,?,?,?,'active',1,1,0,'{}')",
                (challenger_id, challenger_id, "canonical_named_forward_tournament", json.dumps(config, sort_keys=True), now),
            )


def generate_challengers_from_missed_opportunities(store: Any) -> list[str]:
    """Create bounded forward challengers from repeated executable missed-profit reasons."""
    ensure_named_challengers(store)
    if not _table_exists(store, "v52_counterfactual_decisions"):
        return []
    cutoff = (_utcnow() - timedelta(days=7)).isoformat()
    with store._lock:
        rows = store.db.execute(
            "SELECT lane,reason,COUNT(*) n,COALESCE(SUM(opportunity_cost_usd),0) missed,COALESCE(SUM(avoided_loss_usd),0) avoided "
            "FROM v52_counterfactual_decisions WHERE resolved_at IS NOT NULL AND resolved_at>=? "
            "GROUP BY lane,reason HAVING COUNT(*)>=8",
            (cutoff,),
        ).fetchall()
    created: list[str] = []
    for row in rows:
        missed = float(row["missed"] or 0.0)
        avoided = float(row["avoided"] or 0.0)
        if missed <= max(1.0, avoided * 1.25):
            continue
        family = _challenger_family(str(row["reason"] or ""))
        if not family:
            continue
        base = dict(FIXED_CHALLENGERS[family])
        lane = str(row["lane"] or "default")
        challenger_id = f"auto_{family}_{lane}_{abs(hash(str(row['reason']))) % 100000:05d}"
        evidence = {
            "lane": lane,
            "reason": str(row["reason"]),
            "episodes": int(row["n"]),
            "missed_opportunity_usd": missed,
            "avoided_loss_usd": avoided,
            "generated_from_forward_counterfactuals": True,
        }
        with store._lock, store.db:
            cursor = store.db.execute(
                "INSERT OR IGNORE INTO v52_governed_challengers("
                "challenger_id,family,trigger_reason,config_json,created_at,status,analytical_only,paper_only,live_money_authority,evidence_json"
                ") VALUES (?,?,?,?,?,'active',1,1,0,?)",
                (challenger_id, family, str(row["reason"]), json.dumps(base, sort_keys=True), _iso(), json.dumps(evidence, sort_keys=True)),
            )
        if cursor.rowcount == 1:
            created.append(challenger_id)
            _append(store, "v52_governed_challenger_created", {"challenger_id": challenger_id, **evidence, "config": base})
    return created


def _active_challengers(store: Any) -> list[dict[str, Any]]:
    ensure_named_challengers(store)
    with store._lock:
        return [
            dict(row)
            for row in store.db.execute(
                "SELECT * FROM v52_governed_challengers WHERE status='active' ORDER BY created_at,challenger_id"
            ).fetchall()
        ]


def _stream_rows(store: Any, created_after: str) -> list[dict[str, Any]]:
    streams: dict[str, dict[str, Any]] = {}
    if _table_exists(store, "v52_profit_signal_events"):
        with store._lock:
            rows = store.db.execute(
                "SELECT source_signature,lane,observed_at,position_fraction,realized_net_return,realized_mae,chase_fraction,decision,reason "
                "FROM v52_profit_signal_events WHERE observed_at>=? AND realized_net_return IS NOT NULL ORDER BY id",
                (created_after,),
            ).fetchall()
        for row in rows:
            key = f"{row['lane']}:{row['source_signature']}"
            streams[key] = {
                "stream_id": key,
                "lane": str(row["lane"]),
                "observed_at": str(row["observed_at"]),
                "net_return": float(row["realized_net_return"]),
                "drawdown": max(0.0, float(row["realized_mae"] or 0.0)),
                "position_fraction": max(0.0, float(row["position_fraction"] or 0.0)),
                "chase_fraction": float(row["chase_fraction"] or 0.0),
                "entered": str(row["decision"] or "").startswith("paper_enter"),
                "reason": str(row["reason"] or ""),
            }
    if _table_exists(store, "v52_counterfactual_decisions"):
        with store._lock:
            rows = store.db.execute(
                "SELECT source_signature,lane,observed_at,hypothetical_fraction,net_return,executable_mae,chase_fraction,reason "
                "FROM v52_counterfactual_decisions WHERE observed_at>=? AND resolved_at IS NOT NULL AND net_return IS NOT NULL ORDER BY id",
                (created_after,),
            ).fetchall()
        for row in rows:
            key = f"{row['lane']}:{row['source_signature']}"
            if key in streams:
                continue
            streams[key] = {
                "stream_id": key,
                "lane": str(row["lane"]),
                "observed_at": str(row["observed_at"]),
                "net_return": float(row["net_return"]),
                "drawdown": max(0.0, float(row["executable_mae"] or 0.0)),
                "position_fraction": max(0.0, float(row["hypothetical_fraction"] or 0.0)),
                "chase_fraction": float(row["chase_fraction"] or 0.0),
                "entered": False,
                "reason": str(row["reason"] or ""),
            }
    return list(streams.values())


def _policy_stream_return(policy_id: str, family: str, stream: Mapping[str, Any]) -> tuple[float, bool]:
    trade_return = float(stream["net_return"])
    fraction = max(0.0, float(stream.get("position_fraction") or 0.0))
    entered = bool(stream.get("entered"))
    reason_family = _challenger_family(str(stream.get("reason") or ""))
    if policy_id == INCUMBENT_ID:
        return (trade_return * fraction if entered else 0.0), entered
    if not entered and reason_family != family:
        return 0.0, True
    mult = 1.0
    if family == "aggressive_sizing":
        mult = 1.50
    elif family == "aggressive_continuation":
        mult = 1.25 if trade_return > 0 else 1.0
    elif family == "exit_capture":
        mult = 1.20 if trade_return > 0 else 0.90
    elif family == "wallet_acceleration":
        mult = 1.15 if trade_return > 0 else 1.0
    elif family == "chase_optimization":
        if not entered and float(stream.get("chase_fraction") or 0.0) <= 0.45:
            mult = 1.0
        elif not entered:
            return 0.0, True
    elif family == "concentration":
        mult = 1.75 if trade_return > 0 else 1.25
    exposure = fraction if fraction > 0 else 0.01
    portfolio_return = trade_return * exposure * mult
    return max(-0.999, portfolio_return), True


def record_same_stream_tournament_outcomes(store: Any) -> int:
    challengers = _active_challengers(store)
    if not challengers:
        return 0
    common_start = max(str(item["created_at"]) for item in challengers)
    streams = _stream_rows(store, common_start)
    inserted = 0
    policies = [{"challenger_id": INCUMBENT_ID, "family": INCUMBENT_ID}, *challengers]
    with store._lock, store.db:
        for stream in streams:
            for policy in policies:
                policy_id = str(policy["challenger_id"])
                family = str(policy.get("family") or policy_id)
                net_return, complete = _policy_stream_return(policy_id, family, stream)
                cursor = store.db.execute(
                    "INSERT OR IGNORE INTO v52_tournament_outcomes("
                    "challenger_id,stream_id,lane,observed_at,net_return,drawdown,execution_complete,same_stream,prospective"
                    ") VALUES (?,?,?,?,?,?,?,?,1)",
                    (
                        policy_id, str(stream["stream_id"]), str(stream["lane"]), str(stream["observed_at"]),
                        net_return, float(stream["drawdown"]), 1 if complete else 0, 1,
                    ),
                )
                inserted += int(cursor.rowcount == 1)
    return inserted


def _posterior_advantage(store: Any, winner: str, incumbent: str = INCUMBENT_ID) -> dict[str, float]:
    with store._lock:
        rows = store.db.execute(
            "SELECT w.net_return winner_return,i.net_return incumbent_return FROM v52_tournament_outcomes w "
            "JOIN v52_tournament_outcomes i ON i.stream_id=w.stream_id AND i.challenger_id=? "
            "WHERE w.challenger_id=? ORDER BY w.id",
            (incumbent, winner),
        ).fetchall()
    diffs = [float(row["winner_return"]) - float(row["incumbent_return"]) for row in rows]
    n = len(diffs)
    if not diffs:
        return {"episodes": 0, "mean": 0.0, "lower_90": -1.0, "probability_positive": 0.0}
    mean = statistics.fmean(diffs)
    variance = statistics.pvariance(diffs) if len(diffs) > 1 else 0.01
    prior_strength = 8.0
    prior_var = 0.01
    posterior_mean = n * mean / (n + prior_strength)
    posterior_var = (variance + prior_var) / max(1.0, n + prior_strength)
    se = math.sqrt(max(1e-12, posterior_var))
    probability = 0.5 * (1.0 + math.erf((posterior_mean / se) / math.sqrt(2.0)))
    return {
        "episodes": n,
        "mean": posterior_mean,
        "lower_90": posterior_mean - 1.645 * se,
        "probability_positive": probability,
    }


def evaluate_tournament(store: Any) -> tuple[TournamentDecision, dict[str, Any]]:
    active = _active_challengers(store)
    tournament = ProspectivePolicyTournament(
        min_paired_episodes=MIN_FORWARD_EPISODES,
        min_improvement_ratio=MIN_IMPROVEMENT_RATIO,
    )
    with store._lock:
        rows = store.db.execute(
            "SELECT challenger_id,stream_id,net_return,drawdown,execution_complete FROM v52_tournament_outcomes ORDER BY id"
        ).fetchall()
    for row in rows:
        tournament.record(
            PolicyOutcome(
                policy_id=str(row["challenger_id"]),
                stream_id=str(row["stream_id"]),
                net_return=float(row["net_return"]),
                drawdown=float(row["drawdown"]),
                execution_complete=bool(row["execution_complete"]),
            )
        )
    decision = tournament.compare(INCUMBENT_ID, [str(item["challenger_id"]) for item in active])
    posterior = _posterior_advantage(store, decision.winner) if decision.winner else {
        "episodes": 0, "mean": 0.0, "lower_90": -1.0, "probability_positive": 0.0
    }
    blockers = list(decision.blockers)
    if decision.winner:
        score = next((item for item in decision.scores if item.policy_id == decision.winner), None)
        if score is None or score.max_drawdown > MAX_PROMOTION_DRAWDOWN:
            blockers.append("winner_drawdown_above_governed_maximum")
        if score is None or score.execution_completion_rate < 0.95:
            blockers.append("winner_execution_completion_below_minimum")
        if posterior["probability_positive"] < POSTERIOR_PROMOTION_PROBABILITY:
            blockers.append("posterior_probability_below_promotion_threshold")
        if posterior["lower_90"] <= 0.0:
            blockers.append("posterior_lower_advantage_not_positive")
    if blockers and decision.eligible:
        decision = TournamentDecision(
            winner=decision.winner,
            incumbent=decision.incumbent,
            eligible=False,
            improvement_ratio=decision.improvement_ratio,
            blockers=tuple(dict.fromkeys(blockers)),
            scores=decision.scores,
        )
    return decision, posterior


def _challenger_config(store: Any, challenger_id: str) -> dict[str, Any]:
    with store._lock:
        row = store.db.execute(
            "SELECT config_json FROM v52_governed_challengers WHERE challenger_id=?", (challenger_id,)
        ).fetchone()
    return json.loads(str(row["config_json"])) if row is not None else {}


def _apply_changes(changes: Mapping[str, Any], *, rationale: str, evidence_refs: tuple[str, ...]) -> tuple[Any, ...]:
    evolution = canonical_strategy_evolution()
    ordinary = {key: value for key, value in changes.items() if key not in PROTECTED_STRATEGY_KEYS}
    protected = {key: value for key, value in changes.items() if key in PROTECTED_STRATEGY_KEYS}
    epochs = []
    if ordinary:
        epochs.append(evolution.evolve(ordinary, rationale=rationale, evidence_refs=evidence_refs))
    if protected:
        epochs.append(
            evolution.evolve_protected_strategy_constraint(
                protected,
                rationale=rationale,
                validation_refs=evidence_refs,
            )
        )
    return tuple(epochs)


def _replay_governance_history(store: Any) -> None:
    _schema(store)
    evolution = canonical_strategy_evolution()
    if evolution.current.sequence > 1:
        return
    with store._lock:
        rows = [dict(row) for row in store.db.execute("SELECT * FROM v52_strategy_governance_history ORDER BY id").fetchall()]
    for row in rows:
        changes = json.loads(str(row["changes_json"] or "{}"))
        if not changes:
            continue
        _apply_changes(
            changes,
            rationale=f"replay:{row['action']}:{row['challenger_id']}",
            evidence_refs=(f"governance_history:{row['id']}",),
        )


def maybe_promote(store: Any) -> dict[str, Any]:
    decision, posterior = evaluate_tournament(store)
    action = "hold"
    if decision.eligible and decision.winner:
        changes = _challenger_config(store, decision.winner)
        evolution = canonical_strategy_evolution()
        before = dict(evolution.current.config)
        rollback = {key: before.get(key) for key in changes}
        from_fp = evolution.current.fingerprint
        evidence_refs = (
            f"paired_forward_tournament:{decision.winner}",
            f"posterior_probability:{posterior['probability_positive']:.6f}",
            f"posterior_lower_advantage:{posterior['lower_90']:.8f}",
        )
        epochs = _apply_changes(
            changes,
            rationale=f"automatic_forward_promotion:{decision.winner}:ratio={decision.improvement_ratio}",
            evidence_refs=evidence_refs,
        )
        if epochs:
            action = "promote"
            to_fp = canonical_strategy_evolution().current.fingerprint
            with store._lock, store.db:
                store.db.execute(
                    "INSERT INTO v52_strategy_governance_history("
                    "action,challenger_id,observed_at,from_fingerprint,to_fingerprint,changes_json,rollback_json,evidence_json,paper_only,live_money_authority"
                    ") VALUES (?,?,?,?,?,?,?,?,1,0)",
                    (
                        action, decision.winner, _iso(), from_fp, to_fp, json.dumps(changes, sort_keys=True),
                        json.dumps(rollback, sort_keys=True), json.dumps({"decision": asdict(decision), "posterior": posterior}, sort_keys=True),
                    ),
                )
                store.db.execute(
                    "UPDATE v52_governed_challengers SET status='promoted' WHERE challenger_id=?", (decision.winner,)
                )
            _append(store, "v52_strategy_automatic_promotion", {"challenger_id": decision.winner, "posterior": posterior, "changes": changes})
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO v52_tournament_decisions("
            "evaluated_at,incumbent_id,winner_id,eligible,improvement_ratio,posterior_probability,posterior_lower_advantage,"
            "blockers_json,scores_json,action,paper_only,live_money_authority) VALUES (?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                _iso(), INCUMBENT_ID, decision.winner, 1 if decision.eligible else 0, decision.improvement_ratio,
                posterior["probability_positive"], posterior["lower_90"], json.dumps(list(decision.blockers)),
                json.dumps([asdict(score) for score in decision.scores], sort_keys=True), action,
            ),
        )
    return {"action": action, "decision": asdict(decision), "posterior": posterior}


def maybe_demote(store: Any) -> dict[str, Any]:
    _schema(store)
    with store._lock:
        row = store.db.execute(
            "SELECT * FROM v52_strategy_governance_history WHERE action='promote' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    if row is None:
        return {"action": "hold", "reason": "no_active_promotion"}
    promoted = dict(row)
    challenger_id = str(promoted["challenger_id"])
    promoted_at = str(promoted["observed_at"])
    with store._lock:
        scores = store.db.execute(
            "SELECT challenger_id,stream_id,net_return,drawdown,execution_complete FROM v52_tournament_outcomes "
            "WHERE observed_at>=? AND challenger_id IN (?,?) ORDER BY id",
            (promoted_at, INCUMBENT_ID, challenger_id),
        ).fetchall()
    by_id: dict[str, dict[str, float]] = {INCUMBENT_ID: {}, challenger_id: {}}
    for item in scores:
        by_id[str(item["challenger_id"])][str(item["stream_id"])] = float(item["net_return"])
    shared = set(by_id[INCUMBENT_ID]) & set(by_id[challenger_id])
    if len(shared) < MIN_DEMOTION_EPISODES:
        return {"action": "hold", "reason": "insufficient_post_promotion_forward_episodes", "episodes": len(shared)}
    incumbent_growth = math.exp(statistics.fmean(math.log1p(max(-0.999, by_id[INCUMBENT_ID][key])) for key in shared)) - 1.0
    promoted_growth = math.exp(statistics.fmean(math.log1p(max(-0.999, by_id[challenger_id][key])) for key in shared)) - 1.0
    ratio = promoted_growth / incumbent_growth if incumbent_growth > 0 else (1.0 if promoted_growth >= 0 else 0.0)
    posterior = _posterior_advantage(store, challenger_id)
    if ratio >= DEMOTION_RATIO and posterior["probability_positive"] >= 0.50:
        return {"action": "hold", "reason": "promoted_policy_not_deteriorated", "ratio": ratio, "posterior": posterior}
    rollback = json.loads(str(promoted["rollback_json"] or "{}"))
    evolution = canonical_strategy_evolution()
    from_fp = evolution.current.fingerprint
    epochs = _apply_changes(
        rollback,
        rationale=f"automatic_forward_demotion:{challenger_id}:ratio={ratio}",
        evidence_refs=(f"post_promotion_forward_episodes:{len(shared)}",),
    )
    if not epochs:
        return {"action": "hold", "reason": "rollback_empty"}
    to_fp = canonical_strategy_evolution().current.fingerprint
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO v52_strategy_governance_history("
            "action,challenger_id,observed_at,from_fingerprint,to_fingerprint,changes_json,rollback_json,evidence_json,paper_only,live_money_authority"
            ") VALUES ('demote',?,?,?,?,?,?,?,1,0)",
            (
                challenger_id, _iso(), from_fp, to_fp, json.dumps(rollback, sort_keys=True), "{}",
                json.dumps({"ratio": ratio, "episodes": len(shared), "posterior": posterior}, sort_keys=True),
            ),
        )
        store.db.execute("UPDATE v52_governed_challengers SET status='demoted' WHERE challenger_id=?", (challenger_id,))
    _append(store, "v52_strategy_automatic_demotion", {"challenger_id": challenger_id, "ratio": ratio, "rollback": rollback})
    return {"action": "demote", "challenger_id": challenger_id, "ratio": ratio, "posterior": posterior}


def governance_tick(store: Any) -> dict[str, Any]:
    _schema(store)
    generated = generate_challengers_from_missed_opportunities(store)
    recorded = record_same_stream_tournament_outcomes(store)
    demotion = maybe_demote(store)
    promotion = {"action": "hold", "reason": "demotion_precedence"}
    if demotion.get("action") != "demote":
        promotion = maybe_promote(store)
    return {
        "generated_challengers": generated,
        "new_tournament_outcomes": recorded,
        "promotion": promotion,
        "demotion": demotion,
    }


def _finalize_and_govern(*args: Any, **kwargs: Any) -> Any:
    result = _BASE_FINALIZE(*args, **kwargs)
    owner = args[0] if args else None
    if owner is not None and getattr(owner, "store", None) is not None:
        try:
            governance_tick(owner.store)
        except Exception:
            pass
    return result


async def _resolve_and_govern(*args: Any, **kwargs: Any) -> Any:
    result = await _BASE_RESOLVE_COUNTERFACTUALS(*args, **kwargs)
    owner = args[0] if args else None
    if owner is not None and getattr(owner, "store", None) is not None:
        try:
            governance_tick(owner.store)
        except Exception:
            pass
    return result


def install_v52_learning_governance(runtime: Any) -> None:
    global _STORE, _INSTALLED, _BASE_WALLET_PROFILE, _BASE_EXIT_FEATURES, _BASE_FINALIZE, _BASE_RESOLVE_COUNTERFACTUALS
    if _INSTALLED:
        return
    store = runtime.store
    _schema(store)
    _STORE = store
    _BASE_WALLET_PROFILE = completion._wallet_lead_profile
    _BASE_EXIT_FEATURES = completion._exit_features
    _BASE_FINALIZE = completion._finalize_profit_outcomes
    _BASE_RESOLVE_COUNTERFACTUALS = completion._resolve_counterfactuals
    completion._wallet_lead_profile = _bayesian_wallet_profile
    completion._exit_features = _distribution_exit_features
    completion._finalize_profit_outcomes = _finalize_and_govern
    completion._resolve_counterfactuals = _resolve_and_govern
    ensure_named_challengers(store)
    _replay_governance_history(store)
    for lane in LANE_DECAY_PRIORS_HOURS:
        if lane != "default":
            lane_decay_profile(store, lane)
    _INSTALLED = True


def status() -> dict[str, Any]:
    store = _STORE
    payload: dict[str, Any] = {
        "version": VERSION,
        "installed": _INSTALLED,
        "bayesian_posterior_confidence": True,
        "wallet_distribution_reversal_primary_exit_signal": True,
        "lane_specific_learned_decay": True,
        "automatic_challenger_generation": True,
        "concurrent_named_same_stream_tournament": True,
        "automatic_forward_promotion": True,
        "automatic_forward_demotion": True,
        "minimum_forward_paired_episodes": MIN_FORWARD_EPISODES,
        "minimum_improvement_ratio": MIN_IMPROVEMENT_RATIO,
        "posterior_promotion_probability": POSTERIOR_PROMOTION_PROBABILITY,
        "maximum_promotion_drawdown": MAX_PROMOTION_DRAWDOWN,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    if store is None:
        return payload
    _schema(store)
    with store._lock:
        payload["active_challengers"] = int(store.db.execute("SELECT COUNT(*) FROM v52_governed_challengers WHERE status='active'").fetchone()[0])
        payload["promoted_challengers"] = int(store.db.execute("SELECT COUNT(*) FROM v52_governed_challengers WHERE status='promoted'").fetchone()[0])
        payload["tournament_outcomes"] = int(store.db.execute("SELECT COUNT(*) FROM v52_tournament_outcomes").fetchone()[0])
        payload["governance_history_rows"] = int(store.db.execute("SELECT COUNT(*) FROM v52_strategy_governance_history").fetchone()[0])
        payload["lane_decay_profiles"] = [dict(row) for row in store.db.execute("SELECT * FROM v52_lane_decay_profiles ORDER BY lane").fetchall()]
    return payload


__all__ = [
    "FIXED_CHALLENGERS",
    "VERSION",
    "bayesian_wallet_posterior",
    "ensure_named_challengers",
    "evaluate_tournament",
    "generate_challengers_from_missed_opportunities",
    "governance_tick",
    "install_v52_learning_governance",
    "lane_decay_profile",
    "maybe_demote",
    "maybe_promote",
    "record_same_stream_tournament_outcomes",
    "status",
    "wallet_distribution_reversal",
]
