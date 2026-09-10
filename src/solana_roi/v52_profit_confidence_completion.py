from __future__ import annotations

import asyncio
import json
import math
import statistics
import threading
import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping

from . import fomo_continuation_shadow as fomo_shadow
from . import fomo_paper_strategy as fomo_paper
from . import risk_conditioned_alpha_v5 as solana_strategy
from . import v52_adaptive_continuation_refinement as adaptive
from .observation import WSOL_MINT
from .profit_first_entity_final import ExitFeatures, ExitSignal, FinalForwardOutcome, UNIFIED_LANE
from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from .quote import LAMPORTS_PER_SOL
from .strategy_v52_authority import (
    LIVE_MONEY_AUTHORITY,
    PAPER_ONLY,
    SIGNING_AVAILABLE,
    STRATEGY_VERSION,
    TRANSACTION_SUBMISSION_AVAILABLE,
    detection_policy,
    execution_policy,
    position_policy,
    target_sizing_policy,
)

COMPLETION_VERSION = "v52-max-profit-confidence-completion-v1"
_POLICY_PATH = Path(__file__).resolve().parents[2] / "strategy_v52_profit_confidence_completion.json"

_INSTALLED = False
_STORE: Any | None = None
_BASE_SOLANA_CHOOSE: Callable[..., Any] | None = None
_BASE_FOMO_DECISION: Callable[..., Any] | None = None
_BASE_FOMO_CLASSIFY: Callable[..., Any] | None = None
_BASE_ROBINHOOD_CHOOSE: Callable[..., Any] | None = None
_BASE_SOLANA_BUY: Callable[..., Awaitable[Any]] | None = None
_BASE_SOLANA_SELL: Callable[..., Awaitable[Any]] | None = None
_BASE_EXECUTION: Callable[..., Awaitable[Any]] | None = None
_BASE_ROBINHOOD_RPC: Callable[..., Awaitable[Any]] | None = None

_PROVIDER_LOCK = threading.RLock()
_PROVIDER_STATS: dict[str, dict[str, float]] = {}
_PROVIDER_LAST_SWITCH = 0.0


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def completion_policy() -> dict[str, Any]:
    payload = json.loads(_POLICY_PATH.read_text(encoding="utf-8"))
    if payload.get("version") != COMPLETION_VERSION:
        raise RuntimeError("v52 completion policy version mismatch")
    if not bool(payload.get("canonical_direct_enabled")):
        raise RuntimeError("v52 completion policy not canonical")
    if not bool(payload.get("paper_only")) or bool(payload.get("live_money_authority")):
        raise RuntimeError("v52 completion policy violated paper-only authority")
    if bool(payload.get("signing_available")) or bool(payload.get("transaction_submission_available")):
        raise RuntimeError("v52 completion policy exposed live execution authority")
    if float(payload.get("absolute_chase_max_fraction", 1.0)) > 0.80:
        raise RuntimeError("v52 completion policy weakened absolute chase ceiling")
    if float(payload.get("minimum_exit_depth_coverage_ratio", 0.0)) < 2.0:
        raise RuntimeError("v52 completion policy weakened exact exit-depth coverage")
    return payload


def _policy_float(key: str, default: float) -> float:
    try:
        return float(completion_policy().get(key, default))
    except (TypeError, ValueError):
        return float(default)


def _policy_int(key: str, default: int) -> int:
    try:
        return int(completion_policy().get(key, default))
    except (TypeError, ValueError):
        return int(default)


def _table_exists(store: Any, name: str) -> bool:
    try:
        with store._lock:
            row = store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (name,),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_profit_signal_events ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, "
            "token_mint TEXT NOT NULL, wallet TEXT NOT NULL, lane TEXT NOT NULL, venue TEXT NOT NULL, lifecycle TEXT NOT NULL, "
            "regime TEXT NOT NULL, context_key TEXT NOT NULL, observed_at TEXT NOT NULL, first_executable_at TEXT, "
            "chase_fraction REAL, latency_seconds REAL, quote_cost_fraction REAL, independent_count INTEGER NOT NULL DEFAULT 0, "
            "distinct_entities_20s INTEGER NOT NULL DEFAULT 0, distinct_entities_60s INTEGER NOT NULL DEFAULT 0, "
            "repeat_buy_count INTEGER NOT NULL DEFAULT 0, acceleration_ratio REAL NOT NULL DEFAULT 0, "
            "priority_score REAL NOT NULL DEFAULT 0, target_fraction REAL NOT NULL DEFAULT 0, position_fraction REAL NOT NULL DEFAULT 0, "
            "decision TEXT NOT NULL, reason TEXT NOT NULL, closed_at TEXT, realized_net_return REAL, realized_mfe REAL, "
            "realized_mae REAL, capture_ratio REAL, paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "UNIQUE(release_commit,source_signature,lane))"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v52_profit_signal_open ON "
            "v52_profit_signal_events(release_commit,closed_at,position_fraction,priority_score)"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v52_profit_signal_wallet ON "
            "v52_profit_signal_events(wallet,context_key,observed_at)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_wallet_lead_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, "
            "token_mint TEXT NOT NULL, wallet TEXT NOT NULL, lane TEXT NOT NULL, context_key TEXT NOT NULL, "
            "entry_observed_at TEXT NOT NULL, exit_observed_at TEXT NOT NULL, net_return REAL NOT NULL, "
            "executable_mfe REAL NOT NULL, executable_mae REAL NOT NULL, capture_ratio REAL, lead_seconds REAL, "
            "alpha_life_seconds REAL, created_at TEXT NOT NULL, paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "UNIQUE(release_commit,source_signature,lane))"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v52_wallet_lead_context ON "
            "v52_wallet_lead_outcomes(wallet,context_key,created_at)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_counterfactual_decisions ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, "
            "token_mint TEXT NOT NULL, wallet TEXT NOT NULL, lane TEXT NOT NULL, context_key TEXT NOT NULL, "
            "observed_at TEXT NOT NULL, first_executable_at TEXT, reason TEXT NOT NULL, chase_fraction REAL, latency_seconds REAL, "
            "hypothetical_fraction REAL NOT NULL DEFAULT 0, entry_token_raw INTEGER, entry_cost_sol REAL, entry_price_sol REAL, "
            "resolved_at TEXT, exact_exit_net_sol REAL, net_return REAL, executable_mfe REAL, executable_mae REAL, "
            "classification TEXT, opportunity_cost_usd REAL NOT NULL DEFAULT 0, avoided_loss_usd REAL NOT NULL DEFAULT 0, "
            "analytical_only INTEGER NOT NULL, paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "UNIQUE(release_commit,source_signature,lane))"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_staged_position_lifecycle ("
            "release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, token_mint TEXT NOT NULL, wallet TEXT NOT NULL, "
            "lane TEXT NOT NULL, context_key TEXT NOT NULL, entry_observed_at TEXT NOT NULL, entry_cost_sol REAL NOT NULL, "
            "total_token_raw INTEGER NOT NULL, remaining_token_raw INTEGER NOT NULL, position_fraction REAL NOT NULL, "
            "derisk_stage INTEGER NOT NULL DEFAULT 0, runner_fraction REAL NOT NULL DEFAULT 0.10, realized_exit_sol REAL NOT NULL DEFAULT 0, "
            "opened_at TEXT NOT NULL, closed_at TEXT, exit_reason TEXT, paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "PRIMARY KEY(release_commit,source_signature,lane))"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_staged_exit_fills ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, "
            "token_mint TEXT NOT NULL, lane TEXT NOT NULL, exit_signature TEXT NOT NULL, stage INTEGER NOT NULL, "
            "token_raw INTEGER NOT NULL, exit_net_sol REAL NOT NULL, reason TEXT NOT NULL, observed_at TEXT NOT NULL, created_at TEXT NOT NULL, "
            "UNIQUE(release_commit,source_signature,exit_signature,stage))"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_portfolio_rotation_requests ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, incoming_token TEXT NOT NULL, outgoing_token TEXT NOT NULL, "
            "incoming_priority REAL NOT NULL, outgoing_priority REAL NOT NULL, requested_at TEXT NOT NULL, resolved_at TEXT, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_provider_economics ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, provider_name TEXT NOT NULL, method TEXT NOT NULL, priority TEXT NOT NULL, "
            "started_at TEXT NOT NULL, latency_ms REAL NOT NULL, success INTEGER NOT NULL, created_at TEXT NOT NULL)"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v52_provider_economics ON "
            "v52_provider_economics(provider_name,created_at)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_reliability_economics ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, occurred_at TEXT NOT NULL, lane TEXT NOT NULL, kind TEXT NOT NULL, "
            "duration_seconds REAL NOT NULL DEFAULT 0, opportunity_cost_usd REAL NOT NULL DEFAULT 0, details_json TEXT NOT NULL)"
        )


def _record_reliability(kind: str, *, lane: str = "system", duration_seconds: float = 0.0, details: Mapping[str, Any] | None = None) -> None:
    store = _STORE
    if store is None:
        return
    try:
        _schema(store)
        with store._lock, store.db:
            store.db.execute(
                "INSERT INTO v52_reliability_economics(occurred_at,lane,kind,duration_seconds,opportunity_cost_usd,details_json) "
                "VALUES (?,?,?,?,0,?)",
                (_utcnow(), lane, kind, max(0.0, float(duration_seconds)), json.dumps(dict(details or {}), sort_keys=True, default=str)),
            )
    except Exception:
        return


def _source_signature(pre: Mapping[str, Any]) -> str:
    for key in ("source_signature", "signature", "candidate_id"):
        value = pre.get(key)
        if value:
            return str(value)
    return ""


def _signal_metrics(adapter: Any, pre: Mapping[str, Any]) -> dict[str, Any]:
    token = str(pre.get("token") or pre.get("token_mint") or "")
    wallet = str(pre.get("wallet") or pre.get("trigger_wallet") or "")
    at = pre.get("at")
    if not isinstance(at, datetime):
        at = _parse_time(pre.get("received_at") or pre.get("observed_at")) or datetime.now(timezone.utc)
    fast_seconds = _policy_float("wallet_acceleration_fast_seconds", 20.0)
    slow_seconds = max(fast_seconds, _policy_float("wallet_acceleration_slow_seconds", 60.0))
    slow_start = (at - timedelta(seconds=slow_seconds)).isoformat()
    wallets_fast: list[str] = []
    wallets_slow: list[str] = []
    repeat_buy_count = 0
    if token and _table_exists(adapter.store, "wallet_discovery_forward_observations"):
        try:
            with adapter.store._lock:
                rows = adapter.store.db.execute(
                    "SELECT wallet,received_at FROM wallet_discovery_forward_observations "
                    "WHERE token_mint=? AND side='buy' AND received_at>=? AND received_at<=? ORDER BY received_at",
                    (token, slow_start, at.isoformat()),
                ).fetchall()
            for item in rows:
                candidate = str(item["wallet"] or "")
                received = _parse_time(item["received_at"])
                if not candidate or received is None:
                    continue
                wallets_slow.append(candidate)
                if received >= at - timedelta(seconds=fast_seconds):
                    wallets_fast.append(candidate)
                if candidate == wallet:
                    repeat_buy_count += 1
        except Exception:
            pass

    distinct_fast = len(set(wallets_fast))
    distinct_slow = len(set(wallets_slow))
    entity_fast = distinct_fast
    entity_slow = distinct_slow
    try:
        all_wallets = tuple(dict.fromkeys(wallets_slow))
        if all_wallets:
            graph = adapter.execution._entity_graph(all_wallets, at)
            fast_entities = set(graph.distinct_entities(tuple(dict.fromkeys(wallets_fast))))
            slow_entities = set(graph.distinct_entities(all_wallets))
            entity_fast = len(fast_entities)
            entity_slow = len(slow_entities)
    except Exception:
        pass

    fast_rate = entity_fast / max(1.0, fast_seconds)
    slow_rate = entity_slow / max(1.0, slow_seconds)
    acceleration = fast_rate / slow_rate if slow_rate > 0 else (1.0 if fast_rate > 0 else 0.0)
    acceleration = min(5.0, max(0.0, acceleration))
    canonical_independent = int(pre.get("independent_count") or pre.get("independent_confirmation_count") or 0)
    graph_independent = max(0, entity_slow - 1)
    effective_independent = min(canonical_independent, graph_independent) if canonical_independent > 0 and entity_slow > 0 else canonical_independent
    return {
        "distinct_entities_20s": entity_fast,
        "distinct_entities_60s": entity_slow,
        "repeat_buy_count": repeat_buy_count,
        "acceleration_ratio": acceleration,
        "effective_independent_count": effective_independent,
        "entity_graph_deduplicated": True,
    }


def _wallet_lead_profile(store: Any, wallet: str, context_key: str, lane: str) -> dict[str, Any]:
    if not wallet or not _table_exists(store, "v52_wallet_lead_outcomes"):
        return {"samples": 0, "quality": 0.0, "multiplier": 1.0}
    half_life = max(1.0, _policy_float("wallet_lead_half_life_hours", 72.0))
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=half_life * 4.0)).isoformat()
    with store._lock:
        rows = store.db.execute(
            "SELECT net_return,executable_mfe,executable_mae,capture_ratio,lead_seconds,created_at "
            "FROM v52_wallet_lead_outcomes WHERE wallet=? AND (context_key=? OR lane=?) AND created_at>=? ORDER BY id DESC LIMIT 200",
            (wallet, context_key, lane, cutoff),
        ).fetchall()
    if not rows:
        return {"samples": 0, "quality": 0.0, "multiplier": 1.0}
    now = datetime.now(timezone.utc)
    weighted = []
    for row in rows:
        created = _parse_time(row["created_at"]) or now
        age_h = max(0.0, (now - created).total_seconds() / 3600.0)
        weight = 0.5 ** (age_h / half_life)
        weighted.append((row, weight))
    weight_sum = sum(w for _, w in weighted) or 1.0
    mean_return = sum(float(r["net_return"]) * w for r, w in weighted) / weight_sum
    mean_mfe = sum(float(r["executable_mfe"]) * w for r, w in weighted) / weight_sum
    mean_mae = sum(float(r["executable_mae"]) * w for r, w in weighted) / weight_sum
    captures = [(float(r["capture_ratio"]), w) for r, w in weighted if r["capture_ratio"] is not None]
    capture = (sum(v * w for v, w in captures) / sum(w for _, w in captures)) if captures else 0.0
    leads = [float(r["lead_seconds"]) for r, _ in weighted if r["lead_seconds"] is not None]
    positive_rate = sum(w for r, w in weighted if float(r["net_return"]) > 0) / weight_sum
    min_samples = _policy_int("wallet_lead_min_samples", 8)
    evidence = min(1.0, len(rows) / max(1, min_samples))
    edge_quality = max(0.0, min(1.0, mean_return / max(0.05, mean_mfe if mean_mfe > 0 else 0.25)))
    downside_quality = max(0.0, 1.0 - min(1.0, mean_mae / 0.35))
    quality = evidence * max(0.0, min(1.0, 0.35 * positive_rate + 0.30 * capture + 0.20 * edge_quality + 0.15 * downside_quality))
    if mean_return <= 0:
        quality = 0.0
    max_mult = _policy_float("wallet_lead_multiplier_max", 1.25)
    return {
        "samples": len(rows),
        "quality": quality,
        "multiplier": 1.0 + max(0.0, max_mult - 1.0) * quality,
        "mean_net_return": mean_return,
        "mean_mfe": mean_mfe,
        "mean_mae": mean_mae,
        "capture_ratio": capture,
        "positive_rate": positive_rate,
        "median_lead_seconds": statistics.median(leads) if leads else None,
    }


def _latency_bucket(seconds: float | None) -> str:
    if seconds is None:
        return "unknown"
    value = max(0.0, float(seconds))
    if value <= 2:
        return "<=2s"
    if value <= 5:
        return "2-5s"
    if value <= 10:
        return "5-10s"
    if value <= 20:
        return "10-20s"
    return ">20s"


def _latency_economic_multiplier(store: Any, lane: str, seconds: float | None) -> float:
    if seconds is None or not _table_exists(store, "v52_profit_signal_events"):
        return 1.0
    bucket = _latency_bucket(seconds)
    with store._lock:
        rows = store.db.execute(
            "SELECT latency_seconds,realized_net_return FROM v52_profit_signal_events "
            "WHERE lane=? AND realized_net_return IS NOT NULL ORDER BY id DESC LIMIT 300",
            (lane,),
        ).fetchall()
    bucket_values = [float(r["realized_net_return"]) for r in rows if _latency_bucket(r["latency_seconds"]) == bucket]
    all_values = [float(r["realized_net_return"]) for r in rows]
    if len(bucket_values) < _policy_int("latency_economic_min_samples", 8) or not all_values:
        return 1.0
    bmean = statistics.fmean(bucket_values)
    overall = statistics.fmean(all_values)
    if bmean <= 0:
        return 0.65
    if overall <= 0:
        return 1.0
    ratio = bmean / max(1e-9, overall)
    return min(1.15, max(0.70, ratio))


def _learned_chase_limit(store: Any, lane: str) -> float:
    baseline = float(execution_policy()["chase_observe_only_above_fraction"])
    absolute = min(0.80, _policy_float("absolute_chase_max_fraction", 0.80))
    samples: list[tuple[float, float]] = []
    if _table_exists(store, "v52_profit_signal_events"):
        with store._lock:
            rows = store.db.execute(
                "SELECT chase_fraction,realized_net_return FROM v52_profit_signal_events "
                "WHERE lane=? AND chase_fraction>? AND chase_fraction<=? AND realized_net_return IS NOT NULL "
                "ORDER BY id DESC LIMIT 250",
                (lane, baseline, absolute),
            ).fetchall()
        samples.extend((float(r["chase_fraction"]), float(r["realized_net_return"])) for r in rows)
    if _table_exists(store, "v52_counterfactual_decisions"):
        with store._lock:
            rows = store.db.execute(
                "SELECT chase_fraction,net_return FROM v52_counterfactual_decisions "
                "WHERE lane=? AND chase_fraction>? AND chase_fraction<=? AND net_return IS NOT NULL "
                "ORDER BY id DESC LIMIT 250",
                (lane, baseline, absolute),
            ).fetchall()
        samples.extend((float(r["chase_fraction"]), float(r["net_return"])) for r in rows)
    minimum = _policy_int("learned_chase_min_samples", 20)
    if len(samples) < minimum:
        return baseline
    returns = [ret for _, ret in samples]
    positive_rate = sum(ret > 0 for ret in returns) / len(returns)
    if statistics.fmean(returns) <= 0 or positive_rate < _policy_float("learned_chase_min_positive_rate", 0.55):
        return baseline
    positive_chases = sorted(chase for chase, ret in samples if ret > 0)
    if not positive_chases:
        return baseline
    index = min(len(positive_chases) - 1, max(0, int(round(0.75 * (len(positive_chases) - 1)))))
    return min(absolute, max(baseline, positive_chases[index]))


def _learned_ttl_seconds(store: Any, lane: str) -> float | None:
    if not _table_exists(store, "v52_wallet_lead_outcomes"):
        return None
    with store._lock:
        rows = store.db.execute(
            "SELECT alpha_life_seconds FROM v52_wallet_lead_outcomes "
            "WHERE lane=? AND alpha_life_seconds IS NOT NULL AND net_return>0 ORDER BY id DESC LIMIT 200",
            (lane,),
        ).fetchall()
    values = [float(r["alpha_life_seconds"]) for r in rows if float(r["alpha_life_seconds"] or 0.0) > 0]
    if len(values) < _policy_int("learned_ttl_min_samples", 8):
        return None
    ttl = statistics.median(values)
    return min(_policy_float("ttl_max_seconds", 900.0), max(_policy_float("ttl_min_seconds", 20.0), ttl))


def _candidate_age_seconds(adapter: Any, token: str, at: datetime) -> float:
    if not token or not _table_exists(adapter.store, "wallet_discovery_forward_observations"):
        return 0.0
    try:
        with adapter.store._lock:
            row = adapter.store.db.execute(
                "SELECT MIN(received_at) first_at FROM wallet_discovery_forward_observations WHERE token_mint=?",
                (token,),
            ).fetchone()
        first = _parse_time(row["first_at"]) if row is not None else None
        return max(0.0, (at - first).total_seconds()) if first else 0.0
    except Exception:
        return 0.0


def _priority_from_profile(profile: Mapping[str, Any]) -> float:
    auth = profile.get("v52_authority")
    if isinstance(auth, Mapping) and auth.get("portfolio_priority_score") is not None:
        try:
            return max(0.0, float(auth["portfolio_priority_score"]))
        except (TypeError, ValueError):
            pass
    growth = float(profile.get("best_expected_log_growth") or 0.0)
    samples = int(profile.get("sample_count") or 0)
    return adaptive.opportunity_priority_score(expected_log_growth=growth, sample_count=samples, risk_severity=0.0)


def _profile_growth(profile: Mapping[str, Any]) -> float:
    try:
        return float(profile.get("best_expected_log_growth") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _portfolio_compete(
    store: Any,
    *,
    release_commit: str,
    token: str,
    fraction: float,
    target: float,
    priority: float,
    nav_usd: float,
) -> tuple[float, dict[str, Any]]:
    fraction = max(0.0, float(fraction))
    target = max(0.0, float(target))
    if fraction <= 0.0 or target <= 0.0:
        return 0.0, {"reason": "no_fraction"}
    _schema(store)
    with store._lock:
        rows = store.db.execute(
            "SELECT token_mint,SUM(position_fraction) open_fraction,MAX(priority_score) priority "
            "FROM v52_profit_signal_events WHERE release_commit=? AND closed_at IS NULL "
            "AND position_fraction>0 AND decision LIKE 'paper_enter%' GROUP BY token_mint",
            (release_commit,),
        ).fetchall()
    existing = [dict(row) for row in rows]
    same_open = sum(float(row["open_fraction"] or 0.0) for row in existing if str(row["token_mint"]) == token)
    other = [row for row in existing if str(row["token_mint"]) != token]
    total_open = sum(float(row["open_fraction"] or 0.0) for row in existing)
    small = nav_usd <= _policy_float("small_nav_threshold_usd", 1000.0)
    risk_budget = _policy_float(
        "small_nav_total_risk_budget_fraction" if small else "standard_total_risk_budget_fraction",
        0.50 if small else 0.35,
    )
    available = max(0.0, risk_budget - total_open)
    target_remaining = max(0.0, target - same_open)
    if target_remaining <= 0.0:
        return 0.0, {"reason": "target_already_filled", "total_open_fraction": total_open}

    max_tokens = _policy_int("small_nav_max_concurrent_tokens", 3)
    rotation = None
    if small and token and all(str(row["token_mint"]) != token for row in existing) and len(other) >= max_tokens:
        weakest = min(other, key=lambda row: float(row["priority"] or 0.0))
        weakest_priority = float(weakest["priority"] or 0.0)
        if priority <= weakest_priority:
            return 0.0, {
                "reason": "deferred_lower_priority_than_open_portfolio",
                "total_open_fraction": total_open,
                "weakest_open_priority": weakest_priority,
            }
        rotation = str(weakest["token_mint"])
        try:
            with store._lock, store.db:
                store.db.execute(
                    "INSERT INTO v52_portfolio_rotation_requests("
                    "release_commit,incoming_token,outgoing_token,incoming_priority,outgoing_priority,requested_at,resolved_at,paper_only,live_money_authority"
                    ") VALUES (?,?,?,?,?,?,NULL,1,0)",
                    (release_commit, token, rotation, priority, weakest_priority, _utcnow()),
                )
        except Exception:
            rotation = None

    if available <= 0.0:
        return 0.0, {"reason": "global_paper_risk_budget_full", "total_open_fraction": total_open}

    priorities = sorted(float(row["priority"] or 0.0) for row in other)
    winner_multiplier = 1.0
    if priorities and priority > 0:
        rank = sum(value <= priority for value in priorities) / len(priorities)
        if rank >= 0.75:
            winner_multiplier = _policy_float("winner_concentration_multiplier_max", 1.35)
    requested = min(target_remaining, fraction * winner_multiplier)

    minimum_usd = _policy_float("minimum_economic_position_usd", 2.50)
    if requested * nav_usd < minimum_usd:
        minimum_fraction = minimum_usd / max(1e-9, nav_usd)
        if target_remaining + 1e-12 < minimum_fraction or available + 1e-12 < minimum_fraction:
            return 0.0, {
                "reason": "below_nav_economic_minimum",
                "minimum_economic_position_usd": minimum_usd,
                "total_open_fraction": total_open,
            }
        requested = minimum_fraction
    final = min(requested, available, target_remaining)
    return max(0.0, final), {
        "reason": "allocated_by_global_priority",
        "total_open_fraction": total_open,
        "risk_budget_fraction": risk_budget,
        "available_fraction_before": available,
        "winner_concentration_multiplier": winner_multiplier,
        "rotation_requested_from_token": rotation,
        "minimum_economic_position_usd": minimum_usd,
    }


def _upsert_signal_event(
    store: Any,
    *,
    release_commit: str,
    source_signature: str,
    token: str,
    wallet: str,
    lane: str,
    venue: str,
    lifecycle: str,
    regime: str,
    context_key: str,
    observed_at: str,
    chase: float | None,
    latency: float | None,
    quote_cost: float | None,
    metrics: Mapping[str, Any],
    priority: float,
    target: float,
    fraction: float,
    decision: str,
    reason: str,
) -> None:
    if not source_signature:
        return
    _schema(store)
    first_executable = None
    observed = _parse_time(observed_at)
    if observed is not None and latency is not None:
        first_executable = (observed + timedelta(seconds=max(0.0, float(latency)))).isoformat()
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO v52_profit_signal_events("
            "release_commit,source_signature,token_mint,wallet,lane,venue,lifecycle,regime,context_key,observed_at,first_executable_at,"
            "chase_fraction,latency_seconds,quote_cost_fraction,independent_count,distinct_entities_20s,distinct_entities_60s,"
            "repeat_buy_count,acceleration_ratio,priority_score,target_fraction,position_fraction,decision,reason,paper_only,live_money_authority"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0) "
            "ON CONFLICT(release_commit,source_signature,lane) DO UPDATE SET "
            "first_executable_at=COALESCE(excluded.first_executable_at,v52_profit_signal_events.first_executable_at),"
            "chase_fraction=excluded.chase_fraction,latency_seconds=excluded.latency_seconds,quote_cost_fraction=excluded.quote_cost_fraction,"
            "independent_count=excluded.independent_count,distinct_entities_20s=excluded.distinct_entities_20s,"
            "distinct_entities_60s=excluded.distinct_entities_60s,repeat_buy_count=excluded.repeat_buy_count,"
            "acceleration_ratio=excluded.acceleration_ratio,priority_score=excluded.priority_score,target_fraction=excluded.target_fraction,"
            "position_fraction=excluded.position_fraction,decision=excluded.decision,reason=excluded.reason",
            (
                release_commit, source_signature, token, wallet, lane, venue, lifecycle, regime, context_key, observed_at,
                first_executable, chase, latency, quote_cost, int(metrics.get("effective_independent_count") or 0),
                int(metrics.get("distinct_entities_20s") or 0), int(metrics.get("distinct_entities_60s") or 0),
                int(metrics.get("repeat_buy_count") or 0), float(metrics.get("acceleration_ratio") or 0.0),
                priority, target, fraction, decision, reason,
            ),
        )


def _reentry_allowed(
    store: Any,
    *,
    release_commit: str,
    token: str,
    acceleration: float,
    cross_venue: bool,
    profile_growth: float,
    lead_profile: Mapping[str, Any],
) -> bool:
    if not token or not _table_exists(store, "v52_profit_signal_events"):
        return True
    with store._lock:
        row = store.db.execute(
            "SELECT closed_at,realized_net_return FROM v52_profit_signal_events "
            "WHERE release_commit=? AND token_mint=? AND closed_at IS NOT NULL ORDER BY id DESC LIMIT 1",
            (release_commit, token),
        ).fetchone()
    if row is None:
        return True
    if acceleration >= _policy_float("reentry_min_acceleration_ratio", 1.10) or cross_venue:
        return bool(profile_growth > 0.0 or float(lead_profile.get("mean_net_return") or 0.0) > 0.0)
    return False


def _completed_solana_choose(
    adapter: Any,
    pre: dict[str, Any],
    *,
    chase: float | None = None,
    latency: float | None = None,
) -> tuple[str | None, float, dict[str, Any]]:
    if _BASE_SOLANA_CHOOSE is None:
        raise RuntimeError("v52 completion Solana chooser unavailable")
    _schema(adapter.store)
    enriched = dict(pre)
    metrics = _signal_metrics(adapter, enriched)
    if metrics.get("effective_independent_count") is not None:
        enriched["independent_count"] = int(metrics["effective_independent_count"])
        enriched["independent_confirmation_count"] = int(metrics["effective_independent_count"])
    enriched["wallet_acceleration_ratio"] = float(metrics.get("acceleration_ratio") or 0.0)
    lane, fraction, profiles = _BASE_SOLANA_CHOOSE(adapter, enriched, chase=chase, latency=latency)
    copied = {key: dict(value) if isinstance(value, dict) else value for key, value in dict(profiles or {}).items()}
    if not lane or float(fraction or 0.0) <= 0.0 or lane not in copied:
        return lane, float(fraction or 0.0), copied

    profile = dict(copied[lane])
    auth = dict(profile.get("v52_authority") or {})
    target = max(float(fraction), float(auth.get("target_fraction") or fraction))
    context_key = str(profile.get("context_key") or "")
    wallet = str(enriched.get("wallet") or enriched.get("trigger_wallet") or "")
    token = str(enriched.get("token") or enriched.get("token_mint") or "")
    lead = _wallet_lead_profile(adapter.store, wallet, context_key, lane)
    accel = float(metrics.get("acceleration_ratio") or 0.0)
    accel_mult = 1.0
    if (
        accel >= _policy_float("wallet_acceleration_strong_ratio", 1.25)
        and int(metrics.get("effective_independent_count") or 0) >= int(detection_policy()["minimum_skilled_independent_clusters"])
    ):
        max_mult = _policy_float("wallet_acceleration_multiplier_max", 1.25)
        accel_mult = min(max_mult, 1.0 + 0.10 * max(0.0, accel - 1.0))
    latency_mult = _latency_economic_multiplier(adapter.store, lane, latency)
    cap = float(auth.get("lane_cap_preserved") or solana_strategy._lane_cap(lane, float((enriched.get("risk") or {}).get("risk_severity") or 0.0)))
    target = min(cap, target * float(lead.get("multiplier") or 1.0) * accel_mult * latency_mult)
    final = min(target, float(fraction) * float(lead.get("multiplier") or 1.0) * accel_mult * latency_mult)

    learned_chase = _learned_chase_limit(adapter.store, lane)
    reason = "v52_complete_profit_confidence"
    if chase is not None and float(chase) > learned_chase + 1e-12:
        final = 0.0
        reason = "deferred_above_forward_learned_lane_chase_limit"
    ttl = _learned_ttl_seconds(adapter.store, lane)
    at = enriched.get("at") if isinstance(enriched.get("at"), datetime) else (_parse_time(enriched.get("received_at")) or datetime.now(timezone.utc))
    age = _candidate_age_seconds(adapter, token, at)
    if ttl is not None and age > ttl and accel < _policy_float("wallet_acceleration_strong_ratio", 1.25) and not bool(enriched.get("cross_venue_persistence")):
        final = 0.0
        reason = "deferred_forward_learned_opportunity_expired"
    if not _reentry_allowed(
        adapter.store,
        release_commit=str(getattr(adapter, "release_commit", "")),
        token=token,
        acceleration=accel,
        cross_venue=bool(enriched.get("cross_venue_persistence")),
        profile_growth=_profile_growth(profile),
        lead_profile=lead,
    ):
        final = 0.0
        reason = "deferred_reentry_without_new_forward_continuation"

    priority = _priority_from_profile(profile) * float(lead.get("multiplier") or 1.0) * accel_mult
    nav = _policy_float("paper_nav_usd", 500.0)
    if final > 0.0:
        final, allocation = _portfolio_compete(
            adapter.store,
            release_commit=str(getattr(adapter, "release_commit", "")),
            token=token,
            fraction=final,
            target=target,
            priority=priority,
            nav_usd=nav,
        )
        if final <= 0.0:
            reason = str(allocation.get("reason") or "portfolio_competition_deferred")
    else:
        allocation = {"reason": reason}

    auth.update(
        {
            "v52_max_profit_confidence_completion": True,
            "wallet_lead_profile": lead,
            "wallet_acceleration_ratio": accel,
            "entity_graph_distinct_entities_20s": int(metrics.get("distinct_entities_20s") or 0),
            "entity_graph_distinct_entities_60s": int(metrics.get("distinct_entities_60s") or 0),
            "effective_independent_count": int(metrics.get("effective_independent_count") or 0),
            "latency_economic_multiplier": latency_mult,
            "forward_learned_lane_chase_limit": learned_chase,
            "forward_learned_opportunity_ttl_seconds": ttl,
            "candidate_age_seconds": age,
            "paper_nav_usd": nav,
            "global_portfolio_allocation": allocation,
            "target_fraction": target,
            "final_fraction": final,
            "portfolio_priority_score": priority,
            "reason": reason,
        }
    )
    profile["v52_authority"] = auth
    copied[lane] = profile

    signature = _source_signature(enriched)
    if signature and (chase is not None or latency is not None):
        _upsert_signal_event(
            adapter.store,
            release_commit=str(getattr(adapter, "release_commit", "")),
            source_signature=signature,
            token=token,
            wallet=wallet,
            lane=lane,
            venue=str(enriched.get("venue") or ""),
            lifecycle=str(enriched.get("lifecycle") or ""),
            regime=str(enriched.get("regime") or ""),
            context_key=context_key,
            observed_at=str(enriched.get("received_at") or enriched.get("observed_at") or _utcnow()),
            chase=chase,
            latency=latency,
            quote_cost=None,
            metrics=metrics,
            priority=priority,
            target=target,
            fraction=final,
            decision="paper_enter_v52_profit_confidence" if final > 0.0 else "paper_observe_v52_profit_confidence",
            reason=reason,
        )
    return (lane if final > 0.0 else None), max(0.0, final), copied


def _completed_fomo_decision(adapter: Any, *, observation: dict[str, Any], trial: dict[str, Any]) -> dict[str, Any]:
    if _BASE_FOMO_DECISION is None:
        raise RuntimeError("v52 completion FOMO decision unavailable")
    result = dict(_BASE_FOMO_DECISION(adapter, observation=observation, trial=trial))
    fraction = float(result.get("position_fraction") or 0.0)
    if fraction <= 0.0 or not str(result.get("decision") or "").startswith("paper_enter"):
        return result
    _schema(adapter.store)
    profile = dict(result.get("profile") or {})
    auth = dict(result.get("v52_authority") or profile.get("v52_authority") or {})
    token = str(trial.get("token_mint") or "")
    wallet = str(trial.get("trigger_wallet") or "")
    context_key = "|".join(("fomo", wallet, str(observation.get("venue") or ""), str(observation.get("lifecycle") or ""), str(observation.get("regime") or "")))
    latency = float(trial.get("signal_to_entry_seconds") or 0.0)
    chase = None
    try:
        opportunity = json.loads(str(trial.get("opportunity_json") or "{}"))
        chase = float(opportunity.get("chase_fraction")) if opportunity.get("chase_fraction") is not None else None
    except Exception:
        chase = None
    priority = adaptive.opportunity_priority_score(
        expected_log_growth=float(profile.get("best_expected_log_growth") or 0.0),
        sample_count=int(profile.get("sample_count") or 0),
        risk_severity=float((fomo_paper._safe_json(observation.get("state_json")).get("risk_severity") or 0.0)),
    )
    target = min(float(target_sizing_policy()["fomo_max_target_fraction"]), max(fraction, float(auth.get("target_fraction") or fraction)))
    latency_mult = _latency_economic_multiplier(adapter.store, "fomo", latency)
    fraction = min(target, fraction * latency_mult)
    learned_chase = _learned_chase_limit(adapter.store, "fomo")
    reason = "v52_complete_fomo_profit_confidence"
    if chase is not None and chase > learned_chase + 1e-12:
        fraction = 0.0
        reason = "deferred_above_forward_learned_fomo_chase_limit"
    nav = _policy_float("paper_nav_usd", 500.0)
    if fraction > 0:
        fraction, allocation = _portfolio_compete(
            adapter.store,
            release_commit=str(getattr(adapter, "release_commit", "")),
            token=token,
            fraction=fraction,
            target=target,
            priority=priority,
            nav_usd=nav,
        )
        if fraction <= 0.0:
            reason = str(allocation.get("reason") or reason)
    else:
        allocation = {"reason": reason}
    auth.update(
        {
            "v52_max_profit_confidence_completion": True,
            "latency_economic_multiplier": latency_mult,
            "forward_learned_lane_chase_limit": learned_chase,
            "global_portfolio_allocation": allocation,
            "portfolio_priority_score": priority,
            "paper_nav_usd": nav,
            "final_fraction": fraction,
            "reason": reason,
        }
    )
    result["position_fraction"] = fraction
    result["v52_authority"] = auth
    profile["v52_authority"] = auth
    result["profile"] = profile
    if fraction <= 0.0:
        result["decision"] = "no_entry_v52_global_profit_confidence"
        result["reason"] = reason
    signature = str(trial.get("source_signature") or trial.get("signature") or "")
    if signature:
        _upsert_signal_event(
            adapter.store,
            release_commit=str(getattr(adapter, "release_commit", "")),
            source_signature=signature,
            token=token,
            wallet=wallet,
            lane="fomo",
            venue=str(observation.get("venue") or ""),
            lifecycle=str(observation.get("lifecycle") or ""),
            regime=str(observation.get("regime") or trial.get("regime") or ""),
            context_key=context_key,
            observed_at=str(trial.get("observed_at") or observation.get("observed_at") or _utcnow()),
            chase=chase,
            latency=latency,
            quote_cost=float(trial.get("round_trip_cost_fraction") or 0.0),
            metrics={"effective_independent_count": 0, "distinct_entities_20s": 0, "distinct_entities_60s": 0, "repeat_buy_count": 0, "acceleration_ratio": 0.0},
            priority=priority,
            target=target,
            fraction=fraction,
            decision=str(result.get("decision") or ""),
            reason=str(result.get("reason") or reason),
        )
    return result


def _completed_fomo_classify(features: Any, *, max_chase_fraction: float = 0.15, max_latency_seconds: float = 20.0) -> Any:
    if _BASE_FOMO_CLASSIFY is None:
        raise RuntimeError("v52 completion FOMO classifier unavailable")
    result = _BASE_FOMO_CLASSIFY(features, max_chase_fraction=max_chase_fraction, max_latency_seconds=max_latency_seconds)
    store = _STORE
    if store is None:
        return result
    chase = getattr(features, "chase_fraction", None)
    if chase is None or float(chase) <= 0.40:
        return result
    limit = _learned_chase_limit(store, "fomo")
    if float(chase) > limit or float(chase) > 0.80:
        return result
    blockers = [str(value) for value in getattr(result, "blockers", ()) if str(value) != "chase_above_research_ceiling"]
    if blockers or not bool(getattr(features, "risk_complete", False)):
        return result
    acceleration = min(
        float(getattr(features, "new_buyer_acceleration", 0.0) or 0.0),
        float(getattr(features, "net_buy_flow_acceleration", 0.0) or 0.0),
    )
    if acceleration < _policy_float("wallet_acceleration_strong_ratio", 1.25):
        return result
    state = "active_fomo" if float(getattr(result, "score", 0.0) or 0.0) >= 5.0 else "pre_fomo"
    variants = tuple(dict.fromkeys((*getattr(result, "experiment_variants", ()), "forward_learned_lane_chase")))
    return type(result)(
        state=state,
        score=float(getattr(result, "score", 0.0) or 0.0),
        structurally_accessible=True,
        blockers=(),
        experiment_variants=variants,
        feature_version=COMPLETION_VERSION,
    )


def _completed_robinhood_choose(self: Any, **kwargs: Any) -> tuple[str | None, float, dict[str, Any]]:
    if _BASE_ROBINHOOD_CHOOSE is None:
        raise RuntimeError("v52 completion Robinhood chooser unavailable")
    lane, fraction, profiles = _BASE_ROBINHOOD_CHOOSE(self, **kwargs)
    copied = {key: dict(value) if isinstance(value, dict) else value for key, value in dict(profiles or {}).items()}
    if not lane or float(fraction or 0.0) <= 0.0 or lane not in copied:
        return lane, float(fraction or 0.0), copied
    store = getattr(self, "store", None)
    if store is None:
        return lane, float(fraction), copied
    _schema(store)
    profile = dict(copied[lane])
    auth = dict(profile.get("v52_authority") or {})
    priority = _priority_from_profile(profile)
    token = str(kwargs.get("token") or kwargs.get("token_address") or kwargs.get("pool") or kwargs.get("candidate_id") or "robinhood")
    latency = kwargs.get("signal_to_entry_seconds")
    latency_value = float(latency) if latency is not None else None
    latency_mult = _latency_economic_multiplier(store, "robinhood", latency_value)
    target = min(
        float(target_sizing_policy()["robinhood_max_target_fraction"]),
        max(float(fraction), float(auth.get("target_fraction") or fraction)),
    )
    final = min(target, float(fraction) * latency_mult)
    nav = _policy_float("paper_nav_usd", 500.0)
    final, allocation = _portfolio_compete(
        store,
        release_commit=str(getattr(self, "release_commit", "")),
        token=token,
        fraction=final,
        target=target,
        priority=priority,
        nav_usd=nav,
    )
    auth.update(
        {
            "v52_max_profit_confidence_completion": True,
            "latency_economic_multiplier": latency_mult,
            "global_portfolio_allocation": allocation,
            "paper_nav_usd": nav,
            "final_fraction": final,
            "reason": str(allocation.get("reason") or "v52_complete_robinhood_profit_confidence"),
        }
    )
    profile["v52_authority"] = auth
    copied[lane] = profile
    signature = str(kwargs.get("source_signature") or kwargs.get("candidate_id") or kwargs.get("trial_id") or "")
    if signature:
        _upsert_signal_event(
            store,
            release_commit=str(getattr(self, "release_commit", "")),
            source_signature=signature,
            token=token,
            wallet=str(kwargs.get("wallet") or kwargs.get("entity") or ""),
            lane="robinhood",
            venue=str(kwargs.get("venue") or "UNISWAP_V3"),
            lifecycle=str(kwargs.get("lifecycle") or "new_weth_pool"),
            regime=str(kwargs.get("regime") or ""),
            context_key=str(profile.get("context_key") or "robinhood"),
            observed_at=str(kwargs.get("observed_at") or kwargs.get("received_at") or _utcnow()),
            chase=float(kwargs["chase_fraction"]) if kwargs.get("chase_fraction") is not None else None,
            latency=latency_value,
            quote_cost=float(kwargs["round_trip_cost_fraction"]) if kwargs.get("round_trip_cost_fraction") is not None else None,
            metrics={"effective_independent_count": int(kwargs.get("independent_confirmation_count") or 0), "distinct_entities_20s": 0, "distinct_entities_60s": 0, "repeat_buy_count": 0, "acceleration_ratio": 0.0},
            priority=priority,
            target=target,
            fraction=final,
            decision="paper_enter_robinhood_v52_profit_confidence" if final > 0.0 else "paper_observe_robinhood_v52_profit_confidence",
            reason=str(auth.get("reason") or "v52_complete_robinhood_profit_confidence"),
        )
    return (lane if final > 0.0 else None), final, copied


async def _parallel_v52_execution(self: Any, row: dict[str, Any], fraction: float) -> dict[str, Any] | None:
    observed = _parse_time(row.get("observed_at")) or datetime.now(timezone.utc)
    started = time.perf_counter()
    token = str(row.get("token_mint") or "")
    try:
        sol_usd, decimals = await asyncio.gather(
            self._sol_usd(),
            self.execution._token_decimals(token),
        )
    except Exception as exc:
        _record_reliability("parallel_quote_prerequisite_failure", lane="solana", details={"error_type": type(exc).__name__})
        return None
    if sol_usd is None or decimals is None:
        _record_reliability("parallel_quote_prerequisite_unavailable", lane="solana")
        return None
    nav = _policy_float("paper_nav_usd", 500.0)
    input_usd = nav * max(0.0, float(fraction))
    input_sol = input_usd / float(sol_usd)
    input_lamports = max(1, int(round(input_sol * LAMPORTS_PER_SOL)))
    buy = await self.execution._route(WSOL_MINT, token, input_lamports)
    if buy is None or int(buy.get("out_amount") or 0) <= 0:
        _record_reliability("entry_quote_unavailable", lane="solana")
        return None
    token_raw = int(buy["out_amount"])
    token_units = token_raw / (10 ** int(decimals))
    if token_units <= 0:
        return None
    entry_cost_sol = (input_lamports + int(buy.get("fee_lamports") or 0)) / LAMPORTS_PER_SOL
    entry_price_sol = entry_cost_sol / token_units
    coverage = max(
        2.0,
        float(position_policy()["minimum_exit_depth_coverage_ratio"]),
        _policy_float("minimum_exit_depth_coverage_ratio", 2.0),
    )
    required_raw = max(token_raw, int(math.ceil(token_raw * coverage)))
    exact_task = asyncio.create_task(self.execution._route(token, WSOL_MINT, token_raw))
    depth_task = asyncio.create_task(self.execution._route(token, WSOL_MINT, required_raw))
    try:
        exit_route, depth_route = await asyncio.gather(exact_task, depth_task)
    except Exception as exc:
        _record_reliability("parallel_exit_quote_failure", lane="solana", details={"error_type": type(exc).__name__})
        return None
    if exit_route is None or depth_route is None:
        return None
    if int(exit_route.get("out_amount") or 0) <= 0 or int(depth_route.get("out_amount") or 0) <= 0:
        return None
    exit_net_sol = (int(exit_route["out_amount"]) - int(exit_route.get("fee_lamports") or 0)) / LAMPORTS_PER_SOL
    if exit_net_sol <= 0:
        return None
    completed_at = datetime.now(timezone.utc)
    quote_latency_ms = (time.perf_counter() - started) * 1000.0
    wallet_price = float(row.get("wallet_price_sol") or 0.0)
    chase = max(0.0, entry_price_sol / wallet_price - 1.0) if wallet_price > 0 else 1.0
    return {
        "paper_nav_usd": nav,
        "position_fraction": float(fraction),
        "input_usd": input_usd,
        "sol_usd": float(sol_usd),
        "input_lamports": input_lamports,
        "entry_fee_lamports": int(buy.get("fee_lamports") or 0),
        "entry_cost_sol": entry_cost_sol,
        "token_raw": token_raw,
        "decimals": int(decimals),
        "entry_price_sol": entry_price_sol,
        "exit_net_sol": exit_net_sol,
        "round_trip_cost_fraction": max(0.0, 1.0 - exit_net_sol / entry_cost_sol),
        "chase_fraction": chase,
        "signal_to_entry_seconds": max(0.0, (completed_at - observed).total_seconds()),
        "quote_latency_ms": quote_latency_ms,
        "v52_exit_depth_coverage_ratio": coverage,
        "v52_exit_depth_quote_available": True,
        "v52_exit_depth_quote_input_raw": required_raw,
        "v52_exit_depth_quote_output_lamports": int(depth_route.get("out_amount") or 0),
        "v52_parallel_exact_quote_acquisition": True,
    }


def _build_pre_from_trials(self: Any, row: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]] | None:
    signature = str(row.get("signature") or "")
    with self.store._lock:
        selected = self.store.db.execute(
            "SELECT * FROM risk_conditioned_alpha_v5_trials WHERE release_commit=? AND source_signature=? AND selected=1 ORDER BY id DESC LIMIT 1",
            (self.release_commit, signature),
        ).fetchone()
        lanes = self.store.db.execute(
            "SELECT * FROM risk_conditioned_alpha_v5_trials WHERE release_commit=? AND source_signature=? ORDER BY id",
            (self.release_commit, signature),
        ).fetchall()
        unified = self.store.db.execute(
            "SELECT * FROM profit_first_final_trials WHERE epoch_id=? AND source_signature=? AND lane=? ORDER BY id DESC LIMIT 1",
            (self.epoch_id, signature, UNIFIED_LANE),
        ).fetchone()
    if selected is None or unified is None:
        return None
    selected_row = dict(selected)
    unified_row = dict(unified)
    opportunity: dict[str, Any] = {}
    try:
        opportunity = json.loads(str(unified_row.get("opportunity_json") or "{}"))
    except Exception:
        pass
    try:
        risk = json.loads(str(selected_row.get("risk_json") or "{}"))
    except Exception:
        risk = {}
    pre = {
        "at": _parse_time(row.get("received_at")) or datetime.now(timezone.utc),
        "source_signature": signature,
        "signature": signature,
        "token": str(selected_row.get("token_mint") or row.get("token_mint") or ""),
        "token_mint": str(selected_row.get("token_mint") or row.get("token_mint") or ""),
        "wallet": str(selected_row.get("trigger_wallet") or row.get("wallet") or ""),
        "trigger_wallet": str(selected_row.get("trigger_wallet") or row.get("wallet") or ""),
        "venue": str(selected_row.get("venue") or ""),
        "lifecycle": str(selected_row.get("lifecycle") or ""),
        "regime": str(selected_row.get("regime") or ""),
        "role": str(selected_row.get("trigger_role") or ""),
        "flow_state": str(selected_row.get("flow_state") or ""),
        "risk": risk,
        "cross_venue_persistence": str(selected_row.get("lane") or "") == "raydium_cross_venue_persistence",
        "lanes": [str(item["lane"]) for item in lanes],
        "independent_count": int(opportunity.get("independent_confirmation_count") or 0),
        "independent_confirmation_count": int(opportunity.get("independent_confirmation_count") or 0),
        "received_at": str(row.get("received_at") or unified_row.get("received_at") or ""),
        "observed_at": str(row.get("observed_at") or unified_row.get("observed_at") or ""),
    }
    return pre, unified_row, [dict(item) for item in lanes]


def _counterfactual_from_trial(
    self: Any,
    *,
    pre: Mapping[str, Any],
    unified: Mapping[str, Any],
    lane: str,
    context_key: str,
    reason: str,
    hypothetical_fraction: float,
) -> None:
    if not bool(unified.get("entry_executable")) or not bool(unified.get("exit_executable")):
        return
    _schema(self.store)
    entry_cost = None
    try:
        entry_cost = (int(unified.get("quote_input_lamports") or 0) + int(unified.get("entry_fee_lamports") or 0)) / LAMPORTS_PER_SOL
    except Exception:
        entry_cost = None
    if not entry_cost or entry_cost <= 0:
        return
    observed = str(unified.get("observed_at") or pre.get("observed_at") or _utcnow())
    latency = float(unified.get("signal_to_entry_seconds") or 0.0)
    parsed_observed = _parse_time(observed)
    first_exec = (parsed_observed + timedelta(seconds=latency)).isoformat() if parsed_observed else None
    opportunity = {}
    try:
        opportunity = json.loads(str(unified.get("opportunity_json") or "{}"))
    except Exception:
        pass
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "INSERT INTO v52_counterfactual_decisions("
            "release_commit,source_signature,token_mint,wallet,lane,context_key,observed_at,first_executable_at,reason,chase_fraction,"
            "latency_seconds,hypothetical_fraction,entry_token_raw,entry_cost_sol,entry_price_sol,analytical_only,paper_only,live_money_authority"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,1,0) "
            "ON CONFLICT(release_commit,source_signature,lane) DO UPDATE SET reason=excluded.reason,hypothetical_fraction=excluded.hypothetical_fraction",
            (
                self.release_commit, str(pre.get("source_signature") or ""), str(pre.get("token") or ""), str(pre.get("wallet") or ""),
                lane, context_key, observed, first_exec, reason,
                opportunity.get("chase_fraction"), latency, max(0.0, float(hypothetical_fraction)),
                int(unified.get("entry_token_raw") or 0), entry_cost, unified.get("entry_all_in_price_sol"),
            ),
        )


async def _completed_solana_buy(self: Any, row: dict[str, Any]) -> None:
    if _BASE_SOLANA_BUY is None:
        raise RuntimeError("v52 completion buy predecessor unavailable")
    await _BASE_SOLANA_BUY(self, row)
    _schema(self.store)
    built = _build_pre_from_trials(self, row)
    if built is None:
        return
    pre, unified, lane_rows = built
    initial_chase = None
    try:
        opportunity = json.loads(str(unified.get("opportunity_json") or "{}"))
        initial_chase = float(opportunity.get("chase_fraction")) if opportunity.get("chase_fraction") is not None else None
    except Exception:
        initial_chase = None
    latency = float(unified.get("signal_to_entry_seconds") or 0.0)
    lane, desired, profiles = solana_strategy._choose_lane_and_fraction(self, pre, chase=initial_chase, latency=latency)
    selected = next((item for item in lane_rows if int(item.get("selected") or 0) == 1), None)
    quoted_fraction = float(selected.get("position_fraction") or 0.0) if selected else 0.0
    max_requotes = int(execution_policy().get("max_sizing_requotes", 2))
    execution = None
    requotes = 0
    actual_chase = initial_chase
    actual_latency = latency
    final_lane = lane
    final_fraction = max(0.0, float(desired or 0.0))
    final_profiles = profiles

    while final_lane and final_fraction > 0.0 and requotes < max_requotes and (
        execution is None or abs(final_fraction - quoted_fraction) > 1e-9
    ):
        execution = await self._execution(row, final_fraction)
        requotes += 1
        if execution is None:
            final_lane = None
            final_fraction = 0.0
            break
        actual_chase = float(execution.get("chase_fraction") or 0.0)
        actual_latency = float(execution.get("signal_to_entry_seconds") or 0.0)
        lane2, fraction2, profiles2 = solana_strategy._choose_lane_and_fraction(
            self, pre, chase=actual_chase, latency=actual_latency
        )
        final_lane = lane2
        final_profiles = profiles2
        next_fraction = max(0.0, float(fraction2 or 0.0))
        if not final_lane or next_fraction <= 0.0:
            final_fraction = 0.0
            break
        if abs(next_fraction - final_fraction) <= 1e-9:
            final_fraction = next_fraction
            break
        quoted_fraction = final_fraction
        final_fraction = next_fraction

    if execution is None and final_lane and final_fraction > 0.0 and abs(final_fraction - quoted_fraction) <= 1e-9:
        execution = {
            "input_lamports": unified.get("quote_input_lamports"),
            "entry_fee_lamports": unified.get("entry_fee_lamports"),
            "token_raw": unified.get("entry_token_raw"),
            "decimals": unified.get("token_decimals"),
            "entry_price_sol": unified.get("entry_all_in_price_sol"),
            "exit_net_sol": unified.get("immediate_exit_net_sol"),
            "round_trip_cost_fraction": unified.get("round_trip_cost_fraction"),
            "signal_to_entry_seconds": unified.get("signal_to_entry_seconds"),
            "quote_latency_ms": unified.get("quote_latency_ms"),
            "chase_fraction": initial_chase,
            "v52_exit_depth_quote_available": True,
            "v52_exit_depth_coverage_ratio": float(position_policy()["minimum_exit_depth_coverage_ratio"]),
        }

    hard_ok = bool(
        execution
        and execution.get("v52_exit_depth_quote_available")
        and float(execution.get("v52_exit_depth_coverage_ratio") or 0.0) >= float(position_policy()["minimum_exit_depth_coverage_ratio"])
        and actual_latency <= float(execution_policy()["latency_hard_max_seconds"])
        and (actual_chase is None or actual_chase <= _policy_float("absolute_chase_max_fraction", 0.80))
    )
    if not final_lane or final_fraction <= 0.0 or not hard_ok:
        reason = "v52_profit_confidence_final_reconciliation_deferred"
        lane_for_cf = str((selected or {}).get("lane") or lane or "unknown")
        context_key = str((selected or {}).get("context_key") or "")
        _counterfactual_from_trial(
            self,
            pre=pre,
            unified=unified,
            lane=lane_for_cf,
            context_key=context_key,
            reason=reason,
            hypothetical_fraction=max(quoted_fraction, float((selected or {}).get("position_fraction") or 0.0)),
        )
        if selected is not None:
            with self.store._lock, self.store.db:
                self.store.db.execute(
                    "UPDATE risk_conditioned_alpha_v5_trials SET decision='paper_observe_v52_profit_confidence',"
                    "decision_reason=?,position_fraction=0 WHERE release_commit=? AND source_signature=? AND selected=1",
                    (reason, self.release_commit, str(row["signature"])),
                )
        return

    profile = dict(final_profiles.get(final_lane) or {})
    context_key = str(profile.get("context_key") or solana_strategy._context_key(pre, final_lane, chase=actual_chase, latency=actual_latency))
    decision = "paper_enter_v52_profit_confidence"
    if actual_chase is not None and actual_chase > 0.40:
        auth = dict(profile.get("v52_authority") or {})
        if str(auth.get("chase_classification") or "") != "exceptional_continuation":
            _counterfactual_from_trial(
                self, pre=pre, unified=unified, lane=final_lane, context_key=context_key,
                reason="high_chase_without_exceptional_continuation", hypothetical_fraction=final_fraction
            )
            return
        decision = "paper_enter_v52_exceptional_continuation"

    updated_opportunity = {}
    try:
        updated_opportunity = json.loads(str(unified.get("opportunity_json") or "{}"))
    except Exception:
        pass
    updated_opportunity.update(
        {
            "chase_fraction": actual_chase,
            "signal_to_entry_seconds": actual_latency,
            "round_trip_cost_fraction": float(execution.get("round_trip_cost_fraction") or 0.0),
            "entry_executable": True,
            "exit_executable": True,
        }
    )
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "UPDATE risk_conditioned_alpha_v5_trials SET selected=CASE WHEN lane=? THEN 1 ELSE 0 END,"
            "decision=CASE WHEN lane=? THEN ? ELSE 'paper_observe' END,"
            "decision_reason=CASE WHEN lane=? THEN 'v52_final_exact_fraction_requoted' ELSE decision_reason END,"
            "context_key=CASE WHEN lane=? THEN ? ELSE context_key END,"
            "chase_band=?,latency_band=?,position_fraction=?,quote_input_lamports=?,entry_fee_lamports=?,entry_token_raw=?,"
            "entry_cost_sol=?,immediate_exit_net_sol=?,round_trip_cost_fraction=?,entry_executable=1,exit_executable=1 "
            "WHERE release_commit=? AND source_signature=?",
            (
                final_lane, final_lane, decision, final_lane, final_lane, context_key,
                solana_strategy.chase_band(actual_chase), solana_strategy.latency_band(actual_latency), final_fraction,
                int(execution.get("input_lamports") or 0), int(execution.get("entry_fee_lamports") or 0),
                int(execution.get("token_raw") or 0), float(execution.get("entry_cost_sol") or 0.0),
                float(execution.get("exit_net_sol") or 0.0), float(execution.get("round_trip_cost_fraction") or 0.0),
                self.release_commit, str(row["signature"]),
            ),
        )
        self.store.db.execute(
            "UPDATE profit_first_final_trials SET assigned_position_fraction=?,quote_input_lamports=?,entry_fee_lamports=?,"
            "entry_token_raw=?,token_decimals=?,entry_all_in_price_sol=?,immediate_exit_net_sol=?,round_trip_cost_fraction=?,"
            "signal_to_entry_seconds=?,quote_latency_ms=?,entry_executable=1,exit_executable=1,opportunity_json=? "
            "WHERE epoch_id=? AND source_signature=?",
            (
                final_fraction, int(execution.get("input_lamports") or 0), int(execution.get("entry_fee_lamports") or 0),
                int(execution.get("token_raw") or 0), int(execution.get("decimals") or 0),
                float(execution.get("entry_price_sol") or 0.0), float(execution.get("exit_net_sol") or 0.0),
                float(execution.get("round_trip_cost_fraction") or 0.0), actual_latency,
                float(execution.get("quote_latency_ms") or 0.0), json.dumps(updated_opportunity, sort_keys=True, default=str),
                self.epoch_id, str(row["signature"]),
            ),
        )
    metrics = _signal_metrics(self, pre)
    auth = dict(profile.get("v52_authority") or {})
    _upsert_signal_event(
        self.store,
        release_commit=self.release_commit,
        source_signature=str(row["signature"]),
        token=str(pre.get("token") or ""),
        wallet=str(pre.get("wallet") or ""),
        lane=final_lane,
        venue=str(pre.get("venue") or ""),
        lifecycle=str(pre.get("lifecycle") or ""),
        regime=str(pre.get("regime") or ""),
        context_key=context_key,
        observed_at=str(row.get("received_at") or row.get("observed_at") or _utcnow()),
        chase=actual_chase,
        latency=actual_latency,
        quote_cost=float(execution.get("round_trip_cost_fraction") or 0.0),
        metrics=metrics,
        priority=float(auth.get("portfolio_priority_score") or _priority_from_profile(profile)),
        target=float(auth.get("target_fraction") or final_fraction),
        fraction=final_fraction,
        decision=decision,
        reason="v52_final_exact_fraction_requoted" if requotes else "v52_final_exact_fraction_confirmed",
    )


def _price_path_metrics(store: Any, token: str, entry_price: float, start: str, end: str) -> tuple[float, float, float | None, float | None]:
    if entry_price <= 0 or not _table_exists(store, "wallet_discovery_forward_observations"):
        return 0.0, 0.0, None, None
    with store._lock:
        rows = store.db.execute(
            "SELECT received_at,wallet_price_sol,copyable_price_sol FROM wallet_discovery_forward_observations "
            "WHERE token_mint=? AND received_at>=? AND received_at<=? ORDER BY received_at",
            (token, start, end),
        ).fetchall()
    mfe = 0.0
    mae = 0.0
    lead = None
    alpha_life = None
    expansion_seen = False
    start_dt = _parse_time(start)
    for row in rows:
        price = float(row["copyable_price_sol"] or row["wallet_price_sol"] or 0.0)
        if price <= 0:
            continue
        ret = price / entry_price - 1.0
        mfe = max(mfe, ret)
        mae = max(mae, -ret)
        at = _parse_time(row["received_at"])
        if at is None or start_dt is None:
            continue
        elapsed = max(0.0, (at - start_dt).total_seconds())
        if lead is None and ret >= _policy_float("wallet_lead_expansion_threshold", 0.05):
            lead = elapsed
            expansion_seen = True
        if expansion_seen and alpha_life is None and ret <= 0.0:
            alpha_life = elapsed
    return mfe, mae, lead, alpha_life


def _exit_policy(store: Any, lane: str) -> dict[str, float]:
    profiles = dict(completion_policy().get("lane_exit_profiles") or {})
    raw = dict(profiles.get(lane) or profiles.get("default") or {})
    first = float(raw.get("first_derisk_fraction", 0.25))
    second = float(raw.get("second_derisk_fraction", 0.50))
    runner = float(raw.get("runner_fraction", 0.10))
    if _table_exists(store, "v52_wallet_lead_outcomes"):
        with store._lock:
            rows = store.db.execute(
                "SELECT net_return,executable_mfe,executable_mae,capture_ratio FROM v52_wallet_lead_outcomes "
                "WHERE lane=? ORDER BY id DESC LIMIT 100",
                (lane,),
            ).fetchall()
        if len(rows) >= _policy_int("lane_exit_learning_min_samples", 8):
            mean_net = statistics.fmean(float(r["net_return"]) for r in rows)
            mean_mae = statistics.fmean(float(r["executable_mae"]) for r in rows)
            captures = [float(r["capture_ratio"]) for r in rows if r["capture_ratio"] is not None]
            capture = statistics.fmean(captures) if captures else 0.0
            if mean_net > 0 and capture < 0.35 and mean_mae < 0.25:
                first = max(0.10, first - 0.05)
                runner = min(0.35, runner + 0.05)
            elif mean_net <= 0 or mean_mae > 0.35:
                first = min(0.50, first + 0.10)
                runner = max(0.05, runner - 0.05)
    return {
        "first_derisk_fraction": min(0.60, max(0.05, first)),
        "second_derisk_fraction": min(0.75, max(0.10, second)),
        "runner_fraction": min(0.35, max(0.05, runner)),
    }


def _open_selected_positions(self: Any, token: str) -> list[dict[str, Any]]:
    if not _table_exists(self.store, "risk_conditioned_alpha_v5_trials"):
        return []
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT v.*,f.entry_token_raw,f.entry_all_in_price_sol,f.quote_input_lamports,f.entry_fee_lamports,"
            "f.observed_at entry_observed_at,f.opportunity_json,f.context_json "
            "FROM risk_conditioned_alpha_v5_trials v "
            "JOIN profit_first_final_trials f ON f.epoch_id=? AND f.source_signature=v.source_signature AND f.lane=? "
            "LEFT JOIN risk_conditioned_alpha_v5_outcomes o ON o.release_commit=v.release_commit AND o.source_signature=v.source_signature AND o.lane=v.lane "
            "WHERE v.release_commit=? AND v.token_mint=? AND v.selected=1 AND v.decision LIKE 'paper_enter%' AND o.id IS NULL ORDER BY v.id",
            (self.epoch_id, UNIFIED_LANE, self.release_commit, token),
        ).fetchall()
    return [dict(row) for row in rows]


def _effective_exit_lane(self: Any, item: Mapping[str, Any]) -> str:
    source = str(item.get("source_signature") or "")
    if _table_exists(self.store, "fomo_paper_trials"):
        try:
            with self.store._lock:
                row = self.store.db.execute(
                    "SELECT 1 FROM fomo_paper_trials WHERE release_commit=? AND source_signature=? "
                    "AND decision LIKE 'paper_enter_%' LIMIT 1",
                    (self.release_commit, source),
                ).fetchone()
            if row is not None:
                return "fomo"
        except Exception:
            pass
    return str(item.get("lane") or "default")


def _exit_features(self: Any, item: Mapping[str, Any], row: Mapping[str, Any]) -> ExitFeatures:
    at = _parse_time(row.get("received_at")) or datetime.now(timezone.utc)
    token = str(item.get("token_mint") or row.get("token_mint") or "")
    seller = str(row.get("wallet") or "")
    opportunity = {}
    try:
        opportunity = json.loads(str(item.get("opportunity_json") or "{}"))
    except Exception:
        pass
    creator_wallet = self.execution._deployer(token, at)
    seller_entity, current_creator_entity = self._seller_entity(seller, creator_wallet, at)
    creator_entity = opportunity.get("creator_entity") or current_creator_entity
    reversed_flow = bool(self._flow_reversed(token, at))
    return ExitFeatures(
        creator_distribution=bool(creator_entity and seller_entity == creator_entity),
        linked_entity_distribution=bool(creator_entity and seller_entity == creator_entity and seller != creator_wallet),
        early_holder_exit_fraction=float(opportunity.get("early_buyer_exit_fraction") or 0.0),
        successful_scout_exit=seller == str(item.get("trigger_wallet") or ""),
        independent_flow_decelerating=reversed_flow,
        buy_sell_flow_reversal=reversed_flow,
    )


def _ensure_lifecycle(self: Any, item: Mapping[str, Any], lane: str) -> dict[str, Any]:
    _schema(self.store)
    source = str(item["source_signature"])
    with self.store._lock:
        current = self.store.db.execute(
            "SELECT * FROM v52_staged_position_lifecycle WHERE release_commit=? AND source_signature=? AND lane=?",
            (self.release_commit, source, lane),
        ).fetchone()
    if current is not None:
        return dict(current)
    total_raw = int(item.get("entry_token_raw") or 0)
    entry_cost = (int(item.get("quote_input_lamports") or 0) + int(item.get("entry_fee_lamports") or 0)) / LAMPORTS_PER_SOL
    if total_raw <= 0 or entry_cost <= 0:
        return {}
    policy = _exit_policy(self.store, lane)
    now = _utcnow()
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "INSERT OR IGNORE INTO v52_staged_position_lifecycle("
            "release_commit,source_signature,token_mint,wallet,lane,context_key,entry_observed_at,entry_cost_sol,total_token_raw,"
            "remaining_token_raw,position_fraction,derisk_stage,runner_fraction,realized_exit_sol,opened_at,closed_at,exit_reason,paper_only,live_money_authority"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,NULL,NULL,1,0)",
            (
                self.release_commit, source, str(item.get("token_mint") or ""), str(item.get("trigger_wallet") or ""), lane,
                str(item.get("context_key") or ""), str(item.get("entry_observed_at") or item.get("observed_at") or now),
                entry_cost, total_raw, total_raw, float(item.get("position_fraction") or 0.0), 0,
                float(policy["runner_fraction"]), now,
            ),
        )
        current = self.store.db.execute(
            "SELECT * FROM v52_staged_position_lifecycle WHERE release_commit=? AND source_signature=? AND lane=?",
            (self.release_commit, source, lane),
        ).fetchone()
    return dict(current) if current is not None else {}


def _record_exit_signal(self: Any, item: Mapping[str, Any], row: Mapping[str, Any], features: ExitFeatures, signal: ExitSignal) -> None:
    try:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT OR IGNORE INTO profit_first_final_exit_signals("
                "epoch_id,token_mint,source_signature,seller_wallet,observed_at,features_json,signal_json,created_at"
                ") VALUES (?,?,?,?,?,?,?,?)",
                (
                    self.epoch_id, str(item.get("token_mint") or ""), str(row.get("signature") or ""),
                    str(row.get("wallet") or ""), str(row.get("observed_at") or ""),
                    json.dumps(asdict(features), sort_keys=True), json.dumps(asdict(signal), sort_keys=True), _utcnow(),
                ),
            )
    except Exception:
        pass


async def _exact_stage_fill(self: Any, lifecycle: Mapping[str, Any], row: Mapping[str, Any], token_raw: int, stage: int, reason: str) -> float | None:
    if token_raw <= 0:
        return None
    token = str(lifecycle.get("token_mint") or row.get("token_mint") or "")
    route = await self.execution._route(token, WSOL_MINT, int(token_raw))
    if route is None or int(route.get("out_amount") or 0) <= 0:
        _record_reliability("staged_exit_quote_unavailable", lane=str(lifecycle.get("lane") or "solana"))
        return None
    net_lamports = int(route.get("out_amount") or 0) - int(route.get("fee_lamports") or 0)
    if net_lamports <= 0:
        return None
    net_sol = net_lamports / LAMPORTS_PER_SOL
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "INSERT OR IGNORE INTO v52_staged_exit_fills("
            "release_commit,source_signature,token_mint,lane,exit_signature,stage,token_raw,exit_net_sol,reason,observed_at,created_at"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                self.release_commit, str(lifecycle["source_signature"]), token, str(lifecycle["lane"]),
                str(row.get("signature") or ""), stage, int(token_raw), net_sol, reason,
                str(row.get("observed_at") or row.get("received_at") or _utcnow()), _utcnow(),
            ),
        )
        self.store.db.execute(
            "UPDATE v52_staged_position_lifecycle SET remaining_token_raw=MAX(0,remaining_token_raw-?),"
            "realized_exit_sol=realized_exit_sol+?,derisk_stage=MAX(derisk_stage,?) WHERE release_commit=? AND source_signature=? AND lane=?",
            (
                int(token_raw), net_sol, stage, self.release_commit,
                str(lifecycle["source_signature"]), str(lifecycle["lane"]),
            ),
        )
    return net_sol


def _record_wallet_lead_outcome(self: Any, item: Mapping[str, Any], lifecycle: Mapping[str, Any], row: Mapping[str, Any], net_return: float) -> tuple[float, float, float | None, float | None]:
    entry_price = float(item.get("entry_all_in_price_sol") or 0.0)
    start = str(lifecycle.get("entry_observed_at") or item.get("entry_observed_at") or item.get("observed_at") or "")
    end = str(row.get("observed_at") or row.get("received_at") or _utcnow())
    mfe, mae, lead, alpha_life = _price_path_metrics(self.store, str(item.get("token_mint") or ""), entry_price, start, end)
    capture = net_return / mfe if mfe > 0 else None
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "INSERT OR IGNORE INTO v52_wallet_lead_outcomes("
            "release_commit,source_signature,token_mint,wallet,lane,context_key,entry_observed_at,exit_observed_at,net_return,"
            "executable_mfe,executable_mae,capture_ratio,lead_seconds,alpha_life_seconds,created_at,paper_only,live_money_authority"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                self.release_commit, str(item["source_signature"]), str(item.get("token_mint") or ""),
                str(item.get("trigger_wallet") or ""), str(lifecycle.get("lane") or item.get("lane") or ""),
                str(item.get("context_key") or ""), start, end, net_return, mfe, mae, capture, lead, alpha_life, _utcnow(),
            ),
        )
    return mfe, mae, lead, alpha_life


def _finalize_profit_outcomes(self: Any, item: Mapping[str, Any], lifecycle: Mapping[str, Any], row: Mapping[str, Any], features: ExitFeatures, reason: str) -> float:
    source = str(item["source_signature"])
    with self.store._lock:
        total = self.store.db.execute(
            "SELECT COALESCE(SUM(exit_net_sol),0) total FROM v52_staged_exit_fills "
            "WHERE release_commit=? AND source_signature=? AND lane=?",
            (self.release_commit, source, str(lifecycle["lane"])),
        ).fetchone()
        trials = self.store.db.execute(
            "SELECT * FROM profit_first_final_trials WHERE epoch_id=? AND source_signature=? ORDER BY id",
            (self.epoch_id, source),
        ).fetchall()
    exit_net = float(total["total"] or 0.0) if total is not None else 0.0
    entry_cost = float(lifecycle.get("entry_cost_sol") or 0.0)
    net_return = exit_net / entry_cost - 1.0 if exit_net > 0 and entry_cost > 0 else -1.0
    now = _utcnow()
    inserted: list[FinalForwardOutcome] = []
    from . import profit_first_entity_final_research as research
    with self.store._lock, self.store.db:
        for trial_raw in trials:
            trial = dict(trial_raw)
            cursor = self.store.db.execute(
                "INSERT OR IGNORE INTO profit_first_final_outcomes("
                "epoch_id,release_commit,strategy_version,source_signature,exit_signature,token_mint,trigger_wallet,lane,context_json,"
                "entry_observed_at,exit_observed_at,signal_to_entry_seconds,position_fraction,entry_cost_sol,exit_net_sol,net_return,"
                "evidence_phase,exit_reason,exit_features_json,created_at"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    self.epoch_id, self.release_commit, str(trial.get("strategy_version") or STRATEGY_VERSION), source,
                    str(row.get("signature") or ""), str(item.get("token_mint") or ""), str(trial.get("trigger_wallet") or ""),
                    str(trial.get("lane") or ""), trial.get("context_json"), str(trial.get("observed_at") or ""),
                    str(row.get("observed_at") or ""), float(trial.get("signal_to_entry_seconds") or 0.0),
                    float(trial.get("assigned_position_fraction") or 0.0), entry_cost, exit_net, net_return, "forward",
                    reason, json.dumps(asdict(features), sort_keys=True), now,
                ),
            )
            if cursor.rowcount == 1 and trial.get("context_json") is not None and str(trial.get("lane")) != UNIFIED_LANE:
                try:
                    inserted.append(
                        FinalForwardOutcome(
                            context=research._context(str(trial["context_json"])),
                            net_return=net_return,
                            source_signature=source,
                            release_commit=self.release_commit,
                            observed_at=str(trial.get("observed_at") or ""),
                            signal_to_entry_seconds=float(trial.get("signal_to_entry_seconds") or 0.0),
                            position_fraction=float(trial.get("assigned_position_fraction") or 0.0),
                            evidence_phase="forward",
                            exit_reason=reason,
                        )
                    )
                except Exception:
                    pass
        self.store.db.execute(
            "UPDATE v52_staged_position_lifecycle SET remaining_token_raw=0,closed_at=?,exit_reason=? "
            "WHERE release_commit=? AND source_signature=? AND lane=?",
            (now, reason, self.release_commit, source, str(lifecycle["lane"])),
        )
    for outcome in inserted:
        try:
            self.ledger.add(outcome)
        except Exception:
            pass
    mfe, mae, _, _ = _record_wallet_lead_outcome(self, item, lifecycle, row, net_return)
    capture = net_return / mfe if mfe > 0 else None
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "UPDATE v52_profit_signal_events SET closed_at=?,realized_net_return=?,realized_mfe=?,realized_mae=?,capture_ratio=? "
            "WHERE release_commit=? AND source_signature=?",
            (now, net_return, mfe, mae, capture, self.release_commit, source),
        )
        self.store.db.execute(
            "UPDATE v52_portfolio_rotation_requests SET resolved_at=? WHERE release_commit=? AND outgoing_token=? AND resolved_at IS NULL",
            (now, self.release_commit, str(item.get("token_mint") or "")),
        )
        self.store.db.execute(
            "INSERT OR IGNORE INTO risk_conditioned_alpha_v5_outcomes("
            "release_commit,strategy_version,source_signature,exit_signature,token_mint,lane,venue,lifecycle,regime,risk_signature,"
            "context_key,position_fraction,net_return,exit_reason,settled_at,paper_only,live_money_authority"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                self.release_commit, STRATEGY_VERSION, source, str(row.get("signature") or ""),
                str(item.get("token_mint") or ""), str(item.get("lane") or ""), str(item.get("venue") or ""),
                str(item.get("lifecycle") or ""), str(item.get("regime") or ""), str(item.get("risk_signature") or ""),
                str(item.get("context_key") or ""), float(item.get("position_fraction") or 0.0), net_return, reason, now,
            ),
        )
    return net_return


async def _resolve_counterfactuals(self: Any, row: Mapping[str, Any]) -> None:
    token = str(row.get("token_mint") or "")
    if not token or not _table_exists(self.store, "v52_counterfactual_decisions"):
        return
    limit = _policy_int("counterfactual_max_per_exit", 5)
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT * FROM v52_counterfactual_decisions WHERE release_commit=? AND token_mint=? AND resolved_at IS NULL "
            "AND entry_token_raw>0 AND entry_cost_sol>0 ORDER BY id LIMIT ?",
            (self.release_commit, token, limit),
        ).fetchall()
    nav = _policy_float("paper_nav_usd", 500.0)
    for raw in rows:
        item = dict(raw)
        route = await self.execution._route(token, WSOL_MINT, int(item["entry_token_raw"]))
        if route is None or int(route.get("out_amount") or 0) <= 0:
            continue
        exit_net = (int(route["out_amount"]) - int(route.get("fee_lamports") or 0)) / LAMPORTS_PER_SOL
        if exit_net <= 0:
            continue
        net_return = exit_net / float(item["entry_cost_sol"]) - 1.0
        end = str(row.get("observed_at") or row.get("received_at") or _utcnow())
        mfe, mae, _, _ = _price_path_metrics(
            self.store, token, float(item.get("entry_price_sol") or 0.0),
            str(item.get("first_executable_at") or item.get("observed_at") or ""), end,
        )
        fraction = float(item.get("hypothetical_fraction") or 0.0)
        opportunity_cost = max(0.0, net_return) * fraction * nav
        avoided = max(0.0, -net_return) * fraction * nav
        classification = "missed_profitable_executable_opportunity" if net_return > 0 else "avoided_loss"
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE v52_counterfactual_decisions SET resolved_at=?,exact_exit_net_sol=?,net_return=?,executable_mfe=?,"
                "executable_mae=?,classification=?,opportunity_cost_usd=?,avoided_loss_usd=? WHERE id=?",
                (end, exit_net, net_return, mfe, mae, classification, opportunity_cost, avoided, int(item["id"])),
            )


async def _completed_solana_sell(self: Any, row: dict[str, Any]) -> None:
    if _BASE_SOLANA_SELL is None:
        raise RuntimeError("v52 completion sell predecessor unavailable")
    _schema(self.store)
    await _resolve_counterfactuals(self, row)
    token = str(row.get("token_mint") or "")
    positions = _open_selected_positions(self, token)
    if not positions:
        await _BASE_SOLANA_SELL(self, row)
        return
    for item in positions:
        lane = _effective_exit_lane(self, item)
        lifecycle = _ensure_lifecycle(self, item, lane)
        if not lifecycle or int(lifecycle.get("remaining_token_raw") or 0) <= 0:
            continue
        features = _exit_features(self, item, row)
        base_signal = self.strategy.exit_model.evaluate(features)
        _record_exit_signal(self, item, row, features, base_signal)
        ttl = _learned_ttl_seconds(self.store, lane)
        entry_at = _parse_time(lifecycle.get("entry_observed_at"))
        now_at = _parse_time(row.get("observed_at") or row.get("received_at")) or datetime.now(timezone.utc)
        expired = bool(ttl is not None and entry_at is not None and (now_at - entry_at).total_seconds() >= ttl)
        high_urgency = bool(
            features.successful_scout_exit
            or features.creator_distribution
            or features.linked_entity_distribution
            or features.buy_sell_flow_reversal
            or float(base_signal.urgency_score) >= 4.0
            or expired
        )
        if not high_urgency and not bool(base_signal.should_exit):
            continue
        policy = _exit_policy(self.store, lane)
        remaining = int(lifecycle["remaining_token_raw"])
        total_raw = int(lifecycle["total_token_raw"])
        stage = int(lifecycle.get("derisk_stage") or 0)
        runner_raw = int(round(total_raw * float(policy["runner_fraction"])))
        reason_parts = list(base_signal.reasons)
        if expired:
            reason_parts.append("forward_learned_opportunity_expired")
        reason = "v52_lane_exit:" + ",".join(dict.fromkeys(reason_parts or ["risk_exit"]))
        if high_urgency:
            slice_raw = remaining
            next_stage = 3
        elif stage <= 0:
            slice_raw = min(remaining, max(1, int(round(total_raw * float(policy["first_derisk_fraction"])))))
            next_stage = 1
        elif stage == 1:
            desired = max(
                int(round(total_raw * float(policy["second_derisk_fraction"]))),
                max(0, remaining - runner_raw),
            )
            slice_raw = min(remaining, max(1, desired))
            next_stage = 2
        else:
            continue
        if not high_urgency:
            max_sell = max(0, remaining - runner_raw)
            slice_raw = min(slice_raw, max_sell)
            if slice_raw <= 0:
                continue
        filled = await _exact_stage_fill(self, lifecycle, row, slice_raw, next_stage, reason)
        if filled is None:
            continue
        with self.store._lock:
            refreshed = self.store.db.execute(
                "SELECT * FROM v52_staged_position_lifecycle WHERE release_commit=? AND source_signature=? AND lane=?",
                (self.release_commit, str(lifecycle["source_signature"]), lane),
            ).fetchone()
        lifecycle = dict(refreshed) if refreshed is not None else lifecycle
        remaining_after = int(lifecycle.get("remaining_token_raw") or 0)
        if high_urgency or remaining_after <= 0:
            _finalize_profit_outcomes(self, item, lifecycle, row, features, reason)
    return


def _provider_kind(url: str) -> str:
    try:
        from . import robinhood_provider_capacity_budget as capacity
        return str(capacity._provider_kind_from_url(url))
    except Exception:
        return "unknown"


def _provider_priority() -> str:
    try:
        from . import robinhood_provider_capacity_budget as capacity
        return str(capacity._priority_kind())
    except Exception:
        return "qualification"


def _provider_headroom(kind: str) -> float:
    if kind not in {"chainstack", "alchemy"}:
        return 1.0
    try:
        from . import robinhood_provider_capacity_budget as capacity
        with capacity._LOCK:
            remaining, _, limit, _ = capacity._monthly_remaining_locked(kind)
        return max(0.0, min(1.0, remaining / max(1, limit)))
    except Exception:
        return 1.0


def _record_provider(provider: str, method: str, priority: str, latency_ms: float, success: bool) -> None:
    with _PROVIDER_LOCK:
        stats = _PROVIDER_STATS.setdefault(provider, {"samples": 0.0, "latency_ewma_ms": latency_ms, "failures": 0.0})
        stats["samples"] += 1.0
        alpha = 0.20
        stats["latency_ewma_ms"] = latency_ms if stats["samples"] <= 1 else (1.0 - alpha) * stats["latency_ewma_ms"] + alpha * latency_ms
        if not success:
            stats["failures"] += 1.0
    store = _STORE
    if store is not None:
        try:
            _schema(store)
            with store._lock, store.db:
                store.db.execute(
                    "INSERT INTO v52_provider_economics(provider_name,method,priority,started_at,latency_ms,success,created_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (provider, method, priority, _utcnow(), latency_ms, 1 if success else 0, _utcnow()),
                )
        except Exception:
            pass


def _hydrate_provider_stats() -> None:
    store = _STORE
    if store is None or not _table_exists(store, "v52_provider_economics"):
        return
    try:
        with store._lock:
            rows = store.db.execute(
                "SELECT provider_name,latency_ms,success FROM v52_provider_economics ORDER BY id DESC LIMIT 500"
            ).fetchall()
        grouped: dict[str, list[Any]] = {}
        for row in rows:
            grouped.setdefault(str(row["provider_name"] or "unknown"), []).append(row)
        with _PROVIDER_LOCK:
            for name, values in grouped.items():
                latencies = [float(r["latency_ms"] or 0.0) for r in values if float(r["latency_ms"] or 0.0) > 0]
                if not latencies:
                    continue
                _PROVIDER_STATS[name] = {
                    "samples": float(len(values)),
                    "latency_ewma_ms": statistics.fmean(latencies[: min(20, len(latencies))]),
                    "failures": float(sum(1 for r in values if not int(r["success"] or 0))),
                }
    except Exception:
        return


def _provider_score(name: str, kind: str, priority: str) -> float:
    with _PROVIDER_LOCK:
        stats = dict(_PROVIDER_STATS.get(name) or {})
    samples = float(stats.get("samples") or 0.0)
    latency = float(stats.get("latency_ewma_ms") or 10_000.0)
    failures = float(stats.get("failures") or 0.0)
    failure_rate = failures / max(1.0, samples)
    headroom = _provider_headroom(kind)
    if priority == "background":
        return (1.0 / max(0.05, headroom)) * (1.0 + failure_rate)
    return latency * (1.0 + 2.0 * failure_rate) / max(0.25, headroom)


def _maybe_select_fast_provider(rpc_self: Any) -> str:
    global _PROVIDER_LAST_SWITCH
    try:
        from . import robinhood_provider_failover as failover
        items = list(failover.providers())
        current = failover.active_provider()
    except Exception:
        return _provider_kind(str(getattr(rpc_self, "rpc_url", "") or ""))
    if not items or current is None:
        return _provider_kind(str(getattr(rpc_self, "rpc_url", "") or ""))
    priority = _provider_priority()
    now = time.monotonic()
    if now - _PROVIDER_LAST_SWITCH < _policy_float("provider_switch_cooldown_seconds", 60.0):
        return _provider_kind(current.http)
    with _PROVIDER_LOCK:
        current_samples = float((_PROVIDER_STATS.get(current.name) or {}).get("samples") or 0.0)
    if current_samples < _policy_int("provider_min_samples", 3):
        return _provider_kind(current.http)
    current_score = _provider_score(current.name, _provider_kind(current.http), priority)
    candidate = current
    candidate_score = current_score
    for item in items:
        if item.name == current.name:
            continue
        state = getattr(failover, "_PROVIDER_STATE", {}).get(item.name, {})
        if not bool(state.get("chain_verified")):
            continue
        with _PROVIDER_LOCK:
            samples = float((_PROVIDER_STATS.get(item.name) or {}).get("samples") or 0.0)
        if samples < _policy_int("provider_min_samples", 3):
            continue
        score = _provider_score(item.name, _provider_kind(item.http), priority)
        if score < candidate_score:
            candidate, candidate_score = item, score
    improvement = 1.0 - candidate_score / max(1e-9, current_score)
    if candidate.name != current.name and improvement >= _policy_float("provider_switch_improvement_fraction", 0.20):
        try:
            with failover._LOCK:
                failover._ACTIVE_NAME = candidate.name
                failover._GENERATION += 1
            _PROVIDER_LAST_SWITCH = now
            rpc_self.rpc_url = candidate.http
            return _provider_kind(candidate.http)
        except Exception:
            pass
    return _provider_kind(current.http)


def _profit_routed_robinhood_rpc(original: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(rpc_self: Any, method: str, params: list[Any]) -> Any:
        _maybe_select_fast_provider(rpc_self)
        provider = "unknown"
        try:
            from . import robinhood_provider_failover as failover
            provider = str(failover.active_name() or _provider_kind(str(getattr(rpc_self, "rpc_url", "") or "")))
        except Exception:
            provider = _provider_kind(str(getattr(rpc_self, "rpc_url", "") or ""))
        priority = _provider_priority()
        started = time.perf_counter()
        try:
            result = await original(rpc_self, method, params)
        except Exception:
            latency_ms = (time.perf_counter() - started) * 1000.0
            _record_provider(provider, method, priority, latency_ms, False)
            _record_reliability("robinhood_provider_request_failure", lane="robinhood", duration_seconds=latency_ms / 1000.0, details={"provider": provider, "method": method})
            raise
        latency_ms = (time.perf_counter() - started) * 1000.0
        _record_provider(provider, method, priority, latency_ms, True)
        return result

    setattr(wrapped, "_roi_v52_profit_routed_provider", True)
    return wrapped


def _policy_feedback(store: Any, hours: int = 168) -> dict[str, Any]:
    _schema(store)
    since = (datetime.now(timezone.utc) - timedelta(hours=max(1, int(hours)))).isoformat()
    with store._lock:
        rows = store.db.execute(
            "SELECT reason,classification,opportunity_cost_usd,avoided_loss_usd,net_return FROM v52_counterfactual_decisions "
            "WHERE resolved_at>=? ORDER BY id",
            (since,),
        ).fetchall()
    by_reason: dict[str, dict[str, float]] = {}
    for row in rows:
        reason = str(row["reason"] or "unknown")
        bucket = by_reason.setdefault(reason, {"samples": 0.0, "missed_profit_usd": 0.0, "avoided_loss_usd": 0.0, "net_counterfactual_return": 0.0})
        bucket["samples"] += 1.0
        bucket["missed_profit_usd"] += float(row["opportunity_cost_usd"] or 0.0)
        bucket["avoided_loss_usd"] += float(row["avoided_loss_usd"] or 0.0)
        bucket["net_counterfactual_return"] += float(row["net_return"] or 0.0)
    ranked = sorted(
        (
            {"reason": reason, **values, "net_opportunity_tradeoff_usd": values["missed_profit_usd"] - values["avoided_loss_usd"]}
            for reason, values in by_reason.items()
        ),
        key=lambda item: float(item["net_opportunity_tradeoff_usd"]),
        reverse=True,
    )
    return {
        "window_hours": max(1, int(hours)),
        "rules_ranked_by_missed_profit_minus_avoided_loss": ranked,
        "protected_structural_hard_stops_are_never_auto_loosened": True,
        "learned_chase_and_ttl_use_forward_resolved_evidence": True,
    }


def report(hours: int = 24) -> dict[str, Any]:
    store = _STORE
    if store is None:
        return {"available": False, "reason": "v52_completion_runtime_not_installed", "hours": hours}
    _schema(store)
    hours = max(1, min(24 * 31, int(hours)))
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with store._lock:
        events = [dict(r) for r in store.db.execute(
            "SELECT * FROM v52_profit_signal_events WHERE observed_at>=? ORDER BY id", (since,)
        ).fetchall()]
        counter = [dict(r) for r in store.db.execute(
            "SELECT * FROM v52_counterfactual_decisions WHERE observed_at>=? ORDER BY id", (since,)
        ).fetchall()]
        providers = [dict(r) for r in store.db.execute(
            "SELECT * FROM v52_provider_economics WHERE created_at>=? ORDER BY id", (since,)
        ).fetchall()]
        reliability = [dict(r) for r in store.db.execute(
            "SELECT * FROM v52_reliability_economics WHERE occurred_at>=? ORDER BY id", (since,)
        ).fetchall()]
    nav = _policy_float("paper_nav_usd", 500.0)
    lane: dict[str, dict[str, Any]] = {}
    wallet: dict[str, dict[str, Any]] = {}
    for event in events:
        lane_name = str(event.get("lane") or "unknown")
        bucket = lane.setdefault(lane_name, {"candidates": 0, "entries": 0, "realized": 0, "wins": 0, "paper_pnl_usd": 0.0, "mfe_sum": 0.0, "mae_sum": 0.0})
        bucket["candidates"] += 1
        if str(event.get("decision") or "").startswith("paper_enter"):
            bucket["entries"] += 1
        if event.get("realized_net_return") is not None:
            ret = float(event["realized_net_return"])
            pnl = float(event.get("position_fraction") or 0.0) * nav * ret
            bucket["realized"] += 1
            bucket["wins"] += int(ret > 0)
            bucket["paper_pnl_usd"] += pnl
            bucket["mfe_sum"] += float(event.get("realized_mfe") or 0.0)
            bucket["mae_sum"] += float(event.get("realized_mae") or 0.0)
            w = str(event.get("wallet") or "unknown")
            wb = wallet.setdefault(w, {"realized": 0, "paper_pnl_usd": 0.0, "wins": 0})
            wb["realized"] += 1
            wb["paper_pnl_usd"] += pnl
            wb["wins"] += int(ret > 0)
    for bucket in lane.values():
        realized = max(1, int(bucket["realized"]))
        bucket["win_rate"] = bucket["wins"] / realized if bucket["realized"] else None
        bucket["mean_executable_mfe"] = bucket.pop("mfe_sum") / realized if bucket["realized"] else None
        bucket["mean_executable_mae"] = bucket.pop("mae_sum") / realized if bucket["realized"] else None
        bucket["entry_rate_from_observed_candidates"] = bucket["entries"] / bucket["candidates"] if bucket["candidates"] else None
    missed = sum(float(item.get("opportunity_cost_usd") or 0.0) for item in counter)
    avoided = sum(float(item.get("avoided_loss_usd") or 0.0) for item in counter)
    reliability_missed = sum(
        float(item.get("opportunity_cost_usd") or 0.0)
        for item in counter
        if any(marker in str(item.get("reason") or "").lower() for marker in ("latency", "quote", "provider", "execution"))
    )
    provider_summary: dict[str, dict[str, Any]] = {}
    for item in providers:
        name = str(item.get("provider_name") or "unknown")
        p = provider_summary.setdefault(name, {"requests": 0, "successes": 0, "latency_ms_total": 0.0})
        p["requests"] += 1
        p["successes"] += int(item.get("success") or 0)
        p["latency_ms_total"] += float(item.get("latency_ms") or 0.0)
    for p in provider_summary.values():
        p["success_rate"] = p["successes"] / p["requests"] if p["requests"] else None
        p["mean_latency_ms"] = p.pop("latency_ms_total") / p["requests"] if p["requests"] else None
    return {
        "available": True,
        "version": COMPLETION_VERSION,
        "window_hours": hours,
        "paper_nav_usd": nav,
        "candidate_events": len(events),
        "paper_entries": sum(1 for e in events if str(e.get("decision") or "").startswith("paper_enter")),
        "realized_entries": sum(1 for e in events if e.get("realized_net_return") is not None),
        "paper_pnl_usd": sum(float(e.get("position_fraction") or 0.0) * nav * float(e.get("realized_net_return") or 0.0) for e in events if e.get("realized_net_return") is not None),
        "missed_executable_opportunity_usd": missed,
        "avoided_loss_usd": avoided,
        "net_missed_minus_avoided_usd": missed - avoided,
        "lane_attribution": lane,
        "wallet_attribution": wallet,
        "provider_economics": provider_summary,
        "reliability_event_count": len(reliability),
        "reliability_observed_duration_seconds": sum(float(item.get("duration_seconds") or 0.0) for item in reliability),
        "reliability_attributed_missed_opportunity_usd": reliability_missed,
        "policy_feedback": _policy_feedback(store, hours=max(hours, 168)),
        "detection_coverage_scope": "observed_canonical_candidates_not_unobservable_total_market",
        "paper_only": True,
        "live_money_authority": False,
    }


def status() -> dict[str, Any]:
    policy = completion_policy()
    return {
        "version": COMPLETION_VERSION,
        "installed": _INSTALLED,
        "canonical_direct_enabled": True,
        "canonical_strategy_version": STRATEGY_VERSION,
        "conviction_weighted_sizing": True,
        "adaptive_fractional_kelly_targeting": True,
        "graduated_wallet_confidence": True,
        "bayesian_uncertainty_shrinkage": True,
        "wallet_lead_time_alpha": True,
        "contextual_wallet_quality": True,
        "entity_graph_cluster_deduplication": True,
        "wallet_arrival_acceleration": True,
        "global_portfolio_capital_competition": True,
        "small_nav_mode": True,
        "paper_nav_usd": float(policy["paper_nav_usd"]),
        "winner_concentration": True,
        "faster_confirmation_scaling": True,
        "acceleration_aware_scaling": True,
        "lane_specific_learned_exits": True,
        "capture_ratio_exit_learning": True,
        "dynamic_runner": True,
        "wallet_distribution_priority_exit": True,
        "fresh_evidence_reentry": True,
        "forward_learned_lane_chase_limits": True,
        "first_realistic_executable_timestamp_learning": True,
        "missed_opportunity_optimizer": True,
        "avoided_loss_accounting": True,
        "counterfactual_rejection_accounting_analytical_only": True,
        "separate_challenger_strategy_authority": False,
        "compounded_growth_objective": True,
        "rolling_recency_weighting": True,
        "forward_learned_opportunity_expiration": True,
        "latency_economic_sizing": True,
        "profit_aware_provider_routing": True,
        "parallel_exact_quote_acquisition": True,
        "minimum_exit_depth_coverage_ratio": float(policy["minimum_exit_depth_coverage_ratio"]),
        "structural_hard_stops_preserved": True,
        "averaging_down_allowed": False,
        "first_slot_pumpfun_sniping_enabled": False,
        "counterfactual_reports_24h_and_7d": True,
        "lane_attribution": True,
        "wallet_attribution": True,
        "uncertainty_penalties": True,
        "forward_only_point_in_time_learning": True,
        "reliability_opportunity_cost_accounting": True,
        "detection_coverage_reporting": True,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


def _preserve_lineage(wrapper: Any, predecessor: Any) -> None:
    setattr(wrapper, "__wrapped__", predecessor)
    for name, value in vars(predecessor).items():
        if name.startswith("_roi_") and not hasattr(wrapper, name):
            setattr(wrapper, name, value)


def install_v52_profit_confidence_completion(runtime: Any) -> None:
    global _INSTALLED, _STORE, _BASE_SOLANA_CHOOSE, _BASE_FOMO_DECISION, _BASE_FOMO_CLASSIFY
    global _BASE_ROBINHOOD_CHOOSE, _BASE_SOLANA_BUY, _BASE_SOLANA_SELL, _BASE_EXECUTION, _BASE_ROBINHOOD_RPC
    _STORE = runtime.store
    _schema(_STORE)
    if _INSTALLED:
        return
    completion_policy()
    from .robinhood_chain_paper import RobinhoodChainPaperPlane
    from . import robinhood_chain_runtime as robinhood_runtime

    _BASE_SOLANA_CHOOSE = solana_strategy._choose_lane_and_fraction
    _BASE_FOMO_DECISION = fomo_paper._paper_decision
    _BASE_FOMO_CLASSIFY = fomo_shadow.classify_fomo_state
    _BASE_ROBINHOOD_CHOOSE = RobinhoodChainPaperPlane._v5_choose_lane_fraction
    _BASE_SOLANA_BUY = FinalProfitFirstResearchAdapter._buy
    _BASE_SOLANA_SELL = FinalProfitFirstResearchAdapter._sell
    _BASE_EXECUTION = FinalProfitFirstResearchAdapter._execution
    _BASE_ROBINHOOD_RPC = robinhood_runtime.RobinhoodRpc.rpc

    _preserve_lineage(_completed_solana_choose, _BASE_SOLANA_CHOOSE)
    _preserve_lineage(_completed_fomo_decision, _BASE_FOMO_DECISION)
    _preserve_lineage(_completed_robinhood_choose, _BASE_ROBINHOOD_CHOOSE)
    _preserve_lineage(_completed_solana_buy, _BASE_SOLANA_BUY)
    _preserve_lineage(_completed_solana_sell, _BASE_SOLANA_SELL)
    _preserve_lineage(_parallel_v52_execution, _BASE_EXECUTION)

    solana_strategy._choose_lane_and_fraction = _completed_solana_choose
    fomo_paper._paper_decision = _completed_fomo_decision
    fomo_shadow.classify_fomo_state = _completed_fomo_classify
    RobinhoodChainPaperPlane._v5_choose_lane_fraction = _completed_robinhood_choose  # type: ignore[method-assign]
    FinalProfitFirstResearchAdapter._execution = _parallel_v52_execution  # type: ignore[method-assign]
    FinalProfitFirstResearchAdapter._buy = _completed_solana_buy  # type: ignore[method-assign]
    FinalProfitFirstResearchAdapter._sell = _completed_solana_sell  # type: ignore[method-assign]
    _hydrate_provider_stats()
    if not bool(getattr(robinhood_runtime.RobinhoodRpc.rpc, "_roi_v52_profit_routed_provider", False)):
        robinhood_runtime.RobinhoodRpc.rpc = _profit_routed_robinhood_rpc(robinhood_runtime.RobinhoodRpc.rpc)  # type: ignore[method-assign]

    for wrapper in (
        solana_strategy._choose_lane_and_fraction,
        fomo_paper._paper_decision,
        RobinhoodChainPaperPlane._v5_choose_lane_fraction,
        FinalProfitFirstResearchAdapter._buy,
        FinalProfitFirstResearchAdapter._sell,
    ):
        setattr(wrapper, "_roi_v52_final_authority", True)
        setattr(wrapper, "_roi_v52_profit_confidence_completion", True)
    setattr(FinalProfitFirstResearchAdapter._execution, "_roi_v52_exact_depth_authority", True)
    setattr(FinalProfitFirstResearchAdapter._execution, "_roi_v52_parallel_exact_quote", True)

    _INSTALLED = True


__all__ = [
    "COMPLETION_VERSION",
    "completion_policy",
    "install_v52_profit_confidence_completion",
    "report",
    "status",
]
