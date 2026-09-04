from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from statistics import mean, median
from typing import Any, Callable

from .fomo_continuation_shadow import FOMO_LANE as FOMO_RESEARCH_LANE
from .profit_first_entity_final import STARTING_PAPER_NAV_USD, UNIFIED_LANE
from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from .wallet_entity_universe_v4 import MIN_MATURE_FORWARD_SAMPLES


FOMO_PAPER_STRATEGY_VERSION = "fomo-continuation-paper-v1"
FOMO_PAPER_LANE = "fomo_continuation_paper"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
ACTIVE_FOMO_PAPER_STRATEGY_AUTHORITY = True
HISTORICAL_PROMOTION_AUTHORITY = False
MIN_FOMO_WALLET_FORWARD_SAMPLES = MIN_MATURE_FORWARD_SAMPLES
BOOTSTRAP_PAPER_FRACTION = 0.01
FOMO_POSITION_FRACTION_GRID = (0.005, 0.01, 0.02, 0.05)
MAX_FOMO_POSITION_FRACTION = 0.05
ACTIONABLE_FOMO_STATES = frozenset({"pre_fomo", "active_fomo"})

_ORIGINAL_OBSERVE: Callable[..., Any] | None = None
_ORIGINAL_SELL: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_MANIFEST: Callable[..., dict[str, Any]] | None = None


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


def _trimmed_ex_best(values: list[float], n: int = 1) -> float | None:
    if len(values) <= n:
        return None
    ordered = sorted(values, reverse=True)[n:]
    return mean(ordered) if ordered else None


def _expected_log_growth(values: list[float], fraction: float) -> float | None:
    if not values or fraction <= 0.0:
        return None
    terms: list[float] = []
    for value in values:
        terminal = 1.0 + fraction * value
        if terminal <= 0.0:
            return float("-inf")
        terms.append(math.log(terminal))
    return mean(terms) if terms else None


def best_fomo_position_fraction(values: list[float]) -> tuple[float, float | None]:
    """Choose the paper fraction that maximizes forward expected log growth."""
    growth = {
        fraction: _expected_log_growth(values, fraction)
        for fraction in FOMO_POSITION_FRACTION_GRID
    }
    finite = [
        (fraction, value)
        for fraction, value in growth.items()
        if value is not None and math.isfinite(value)
    ]
    if not finite:
        return 0.0, None
    fraction, value = max(finite, key=lambda item: item[1])
    if value <= 0.0:
        return 0.0, value
    return min(MAX_FOMO_POSITION_FRACTION, float(fraction)), float(value)


def classify_fomo_wallet_returns(values: list[float]) -> dict[str, Any]:
    """Forward-only promotion state for one exact FOMO wallet context."""
    clean = [float(value) for value in values if math.isfinite(float(value))]
    sample_count = len(clean)
    trimmed = _trimmed_ex_best(clean, 1)
    result = {
        "sample_count": sample_count,
        "mean_residual_roi_pct": mean(clean) * 100.0 if clean else None,
        "median_residual_roi_pct": median(clean) * 100.0 if clean else None,
        "trimmed_mean_residual_roi_ex_best_1_pct": trimmed * 100.0 if trimmed is not None else None,
        "positive_rate_pct": (
            sum(value > 0.0 for value in clean) / sample_count * 100.0
            if clean
            else None
        ),
        "mature": sample_count >= MIN_FOMO_WALLET_FORWARD_SAMPLES,
    }
    if not result["mature"]:
        state = "bootstrap_forward_evidence"
    else:
        median_roi = median(clean)
        positive_rate = sum(value > 0.0 for value in clean) / sample_count
        if trimmed is not None and trimmed > 0.0 and median_roi > 0.0 and positive_rate >= 0.50:
            state = "promoted_fomo_wallet"
        elif trimmed is not None and (trimmed <= 0.0 or median_roi <= 0.0):
            state = "demoted_fomo_wallet"
        else:
            state = "observe_mixed_fomo_wallet"
    fraction, growth = best_fomo_position_fraction(clean)
    result["state"] = state
    result["best_paper_position_fraction"] = fraction
    result["best_expected_log_growth"] = growth
    result["historical_evidence_used_for_promotion"] = False
    return result


def _schema(adapter: FinalProfitFirstResearchAdapter) -> None:
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS fomo_paper_trials ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, strategy_version TEXT NOT NULL, "
            "source_signature TEXT NOT NULL, token_mint TEXT NOT NULL, trigger_wallet TEXT NOT NULL, "
            "venue TEXT NOT NULL, lifecycle TEXT NOT NULL, regime TEXT NOT NULL, fomo_state TEXT NOT NULL, "
            "wallet_context_state TEXT NOT NULL, decision TEXT NOT NULL, decision_reason TEXT NOT NULL, "
            "position_fraction REAL NOT NULL, entry_observed_at TEXT NOT NULL, signal_to_entry_seconds REAL NOT NULL, "
            "entry_cost_sol REAL, entry_token_raw INTEGER, token_decimals INTEGER, entry_all_in_price_sol REAL, "
            "entry_executable INTEGER NOT NULL, exit_executable INTEGER NOT NULL, created_at TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "UNIQUE(release_commit, source_signature))"
        )
        adapter.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_fomo_paper_trials_open "
            "ON fomo_paper_trials(release_commit, token_mint, decision, id)"
        )
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS fomo_paper_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, strategy_version TEXT NOT NULL, "
            "source_signature TEXT NOT NULL, exit_signature TEXT NOT NULL, token_mint TEXT NOT NULL, "
            "trigger_wallet TEXT NOT NULL, venue TEXT NOT NULL, lifecycle TEXT NOT NULL, regime TEXT NOT NULL, "
            "fomo_state TEXT NOT NULL, position_fraction REAL NOT NULL, net_return REAL NOT NULL, "
            "paper_return_contribution REAL NOT NULL, exit_reason TEXT NOT NULL, settled_at TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "UNIQUE(release_commit, source_signature))"
        )
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS fomo_wallet_cohort ("
            "release_commit TEXT NOT NULL, wallet TEXT NOT NULL, venue TEXT NOT NULL, lifecycle TEXT NOT NULL, "
            "regime TEXT NOT NULL, state TEXT NOT NULL, sample_count INTEGER NOT NULL, "
            "mean_residual_roi_pct REAL, median_residual_roi_pct REAL, "
            "trimmed_mean_residual_roi_ex_best_1_pct REAL, positive_rate_pct REAL, "
            "best_paper_position_fraction REAL NOT NULL, best_expected_log_growth REAL, updated_at TEXT NOT NULL, "
            "historical_promotion_authority INTEGER NOT NULL, PRIMARY KEY(release_commit,wallet,venue,lifecycle,regime))"
        )


def _fomo_observation(adapter: FinalProfitFirstResearchAdapter, signature: str) -> dict[str, Any] | None:
    with adapter.store._lock:
        row = adapter.store.db.execute(
            "SELECT source_signature,token_mint,observed_at,venue,lifecycle,regime,state_json "
            "FROM fomo_shadow_observations WHERE release_commit=? AND source_signature=? LIMIT 1",
            (adapter.release_commit, signature),
        ).fetchone()
    return dict(row) if row is not None else None


def _unified_trial(adapter: FinalProfitFirstResearchAdapter, signature: str) -> dict[str, Any] | None:
    with adapter.store._lock:
        row = adapter.store.db.execute(
            "SELECT source_signature,token_mint,trigger_wallet,observed_at,received_at,regime,opportunity_json,"
            "decision_json,assigned_position_fraction,quote_input_lamports,entry_fee_lamports,entry_token_raw,"
            "token_decimals,entry_all_in_price_sol,immediate_exit_net_sol,round_trip_cost_fraction,"
            "signal_to_entry_seconds,entry_executable,exit_executable "
            "FROM profit_first_final_trials WHERE epoch_id=? AND source_signature=? AND lane=? "
            "ORDER BY id DESC LIMIT 1",
            (adapter.epoch_id, signature, UNIFIED_LANE),
        ).fetchone()
    return dict(row) if row is not None else None


def _forward_fomo_rows(adapter: FinalProfitFirstResearchAdapter) -> list[dict[str, Any]]:
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT s.source_signature,s.venue,s.lifecycle,s.regime,s.state_json,t.trigger_wallet,o.net_return "
            "FROM fomo_shadow_observations s "
            "JOIN profit_first_final_trials t ON t.epoch_id=? AND t.source_signature=s.source_signature AND t.lane=? "
            "JOIN fomo_shadow_outcomes o ON o.release_commit=s.release_commit AND o.source_signature=s.source_signature "
            "WHERE s.release_commit=? ORDER BY s.id",
            (adapter.epoch_id, UNIFIED_LANE, adapter.release_commit),
        ).fetchall()
    return [dict(row) for row in rows]


def _context_returns(
    adapter: FinalProfitFirstResearchAdapter,
    *,
    wallet: str,
    venue: str,
    lifecycle: str,
    regime: str,
) -> list[float]:
    values: list[float] = []
    for row in _forward_fomo_rows(adapter):
        state = str(_safe_json(row.get("state_json")).get("state") or "")
        if state not in ACTIONABLE_FOMO_STATES:
            continue
        if str(row.get("trigger_wallet") or "") != wallet:
            continue
        if str(row.get("venue") or "") != venue:
            continue
        if str(row.get("lifecycle") or "") != lifecycle:
            continue
        if str(row.get("regime") or "") != regime:
            continue
        value = _finite(row.get("net_return"))
        if value is not None:
            values.append(value)
    return values


def _profile(
    adapter: FinalProfitFirstResearchAdapter,
    *,
    wallet: str,
    venue: str,
    lifecycle: str,
    regime: str,
) -> dict[str, Any]:
    values = _context_returns(
        adapter,
        wallet=wallet,
        venue=venue,
        lifecycle=lifecycle,
        regime=regime,
    )
    result = classify_fomo_wallet_returns(values)
    result.update(
        {
            "wallet": wallet,
            "venue": venue,
            "lifecycle": lifecycle,
            "regime": regime,
        }
    )
    return result


def _refresh_cohort(adapter: FinalProfitFirstResearchAdapter) -> list[dict[str, Any]]:
    _schema(adapter)
    grouped: dict[tuple[str, str, str, str], list[float]] = defaultdict(list)
    for row in _forward_fomo_rows(adapter):
        state = str(_safe_json(row.get("state_json")).get("state") or "")
        if state not in ACTIONABLE_FOMO_STATES:
            continue
        value = _finite(row.get("net_return"))
        wallet = str(row.get("trigger_wallet") or "")
        if value is None or not wallet:
            continue
        key = (
            wallet,
            str(row.get("venue") or "UNKNOWN"),
            str(row.get("lifecycle") or "unknown"),
            str(row.get("regime") or "unknown"),
        )
        grouped[key].append(value)

    now = datetime.now(timezone.utc).isoformat()
    results: list[dict[str, Any]] = []
    with adapter.store._lock, adapter.store.db:
        for (wallet, venue, lifecycle, regime), values in grouped.items():
            profile = classify_fomo_wallet_returns(values)
            adapter.store.db.execute(
                "INSERT INTO fomo_wallet_cohort("
                "release_commit,wallet,venue,lifecycle,regime,state,sample_count,mean_residual_roi_pct,"
                "median_residual_roi_pct,trimmed_mean_residual_roi_ex_best_1_pct,positive_rate_pct,"
                "best_paper_position_fraction,best_expected_log_growth,updated_at,historical_promotion_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,0) "
                "ON CONFLICT(release_commit,wallet,venue,lifecycle,regime) DO UPDATE SET "
                "state=excluded.state,sample_count=excluded.sample_count,mean_residual_roi_pct=excluded.mean_residual_roi_pct,"
                "median_residual_roi_pct=excluded.median_residual_roi_pct,"
                "trimmed_mean_residual_roi_ex_best_1_pct=excluded.trimmed_mean_residual_roi_ex_best_1_pct,"
                "positive_rate_pct=excluded.positive_rate_pct,"
                "best_paper_position_fraction=excluded.best_paper_position_fraction,"
                "best_expected_log_growth=excluded.best_expected_log_growth,updated_at=excluded.updated_at",
                (
                    adapter.release_commit,
                    wallet,
                    venue,
                    lifecycle,
                    regime,
                    str(profile["state"]),
                    int(profile["sample_count"]),
                    profile["mean_residual_roi_pct"],
                    profile["median_residual_roi_pct"],
                    profile["trimmed_mean_residual_roi_ex_best_1_pct"],
                    profile["positive_rate_pct"],
                    float(profile["best_paper_position_fraction"]),
                    profile["best_expected_log_growth"],
                    now,
                ),
            )
            results.append(
                {
                    "wallet": wallet,
                    "venue": venue,
                    "lifecycle": lifecycle,
                    "regime": regime,
                    **profile,
                }
            )
    results.sort(
        key=lambda row: (
            1 if row["state"] == "promoted_fomo_wallet" else 0,
            row["trimmed_mean_residual_roi_ex_best_1_pct"]
            if row["trimmed_mean_residual_roi_ex_best_1_pct"] is not None
            else float("-inf"),
            row["sample_count"],
        ),
        reverse=True,
    )
    return results


def _open_position_fraction(adapter: FinalProfitFirstResearchAdapter) -> float:
    with adapter.store._lock:
        row = adapter.store.db.execute(
            "SELECT COALESCE(SUM(t.position_fraction),0) AS total FROM fomo_paper_trials t "
            "LEFT JOIN fomo_paper_outcomes o ON o.release_commit=t.release_commit AND o.source_signature=t.source_signature "
            "WHERE t.release_commit=? AND t.decision LIKE 'paper_enter_%' AND o.id IS NULL",
            (adapter.release_commit,),
        ).fetchone()
    return min(1.0, max(0.0, float(row["total"] or 0.0))) if row is not None else 0.0


def _token_already_open(adapter: FinalProfitFirstResearchAdapter, token_mint: str) -> bool:
    with adapter.store._lock:
        row = adapter.store.db.execute(
            "SELECT 1 FROM fomo_paper_trials t LEFT JOIN fomo_paper_outcomes o "
            "ON o.release_commit=t.release_commit AND o.source_signature=t.source_signature "
            "WHERE t.release_commit=? AND t.token_mint=? AND t.decision LIKE 'paper_enter_%' "
            "AND o.id IS NULL LIMIT 1",
            (adapter.release_commit, token_mint),
        ).fetchone()
    return row is not None


def _paper_decision(
    adapter: FinalProfitFirstResearchAdapter,
    *,
    observation: dict[str, Any],
    trial: dict[str, Any],
) -> dict[str, Any]:
    state_payload = _safe_json(observation.get("state_json"))
    fomo_state = str(state_payload.get("state") or "unknown")
    accessible = bool(state_payload.get("structurally_accessible"))
    wallet = str(trial.get("trigger_wallet") or "")
    venue = str(observation.get("venue") or "UNKNOWN")
    lifecycle = str(observation.get("lifecycle") or "unknown")
    regime = str(observation.get("regime") or trial.get("regime") or "unknown")
    profile = _profile(
        adapter,
        wallet=wallet,
        venue=venue,
        lifecycle=lifecycle,
        regime=regime,
    )

    if fomo_state not in ACTIONABLE_FOMO_STATES:
        return {
            "decision": "no_entry_nonactionable_fomo_state",
            "reason": fomo_state,
            "position_fraction": 0.0,
            "profile": profile,
        }
    if not accessible:
        return {
            "decision": "no_entry_structurally_inaccessible",
            "reason": ",".join(str(x) for x in (state_payload.get("blockers") or ())) or "fomo_accessibility_failed",
            "position_fraction": 0.0,
            "profile": profile,
        }
    if not bool(trial.get("entry_executable")) or not bool(trial.get("exit_executable")):
        return {
            "decision": "no_entry_execution_incomplete",
            "reason": "entry_and_exit_executable_evidence_required",
            "position_fraction": 0.0,
            "profile": profile,
        }
    if _token_already_open(adapter, str(trial.get("token_mint") or "")):
        return {
            "decision": "no_entry_token_already_open",
            "reason": "one_open_fomo_paper_position_per_token",
            "position_fraction": 0.0,
            "profile": profile,
        }
    wallet_state = str(profile["state"])
    if wallet_state == "demoted_fomo_wallet":
        return {
            "decision": "no_entry_demoted_fomo_wallet",
            "reason": "mature_forward_fomo_context_nonpositive_after_robustness_check",
            "position_fraction": 0.0,
            "profile": profile,
        }
    if wallet_state == "promoted_fomo_wallet":
        requested = float(profile["best_paper_position_fraction"] or 0.0)
        if requested <= 0.0:
            return {
                "decision": "no_entry_nonpositive_fomo_growth",
                "reason": "forward_expected_log_growth_not_positive",
                "position_fraction": 0.0,
                "profile": profile,
            }
        decision = "paper_enter_promoted_fomo_wallet"
    else:
        requested = BOOTSTRAP_PAPER_FRACTION
        decision = "paper_enter_bootstrap_probe"

    available = max(0.0, 1.0 - _open_position_fraction(adapter))
    fraction = min(MAX_FOMO_POSITION_FRACTION, requested, available)
    if fraction <= 0.0:
        return {
            "decision": "no_entry_paper_capacity_exhausted",
            "reason": "open_fomo_paper_fraction_at_capacity",
            "position_fraction": 0.0,
            "profile": profile,
        }
    return {
        "decision": decision,
        "reason": "forward_only_fomo_wallet_policy",
        "position_fraction": fraction,
        "profile": profile,
    }


def _record_paper_trial(adapter: FinalProfitFirstResearchAdapter, signature: str) -> bool:
    _schema(adapter)
    observation = _fomo_observation(adapter, signature)
    trial = _unified_trial(adapter, signature)
    if observation is None or trial is None:
        return False
    decision = _paper_decision(adapter, observation=observation, trial=trial)
    profile = dict(decision["profile"])
    fomo_state = str(_safe_json(observation.get("state_json")).get("state") or "unknown")
    quote_input = int(trial.get("quote_input_lamports") or 0)
    entry_fee = int(trial.get("entry_fee_lamports") or 0)
    entry_cost_sol = (quote_input + entry_fee) / 1_000_000_000.0 if quote_input > 0 else None
    now = datetime.now(timezone.utc).isoformat()
    with adapter.store._lock, adapter.store.db:
        cursor = adapter.store.db.execute(
            "INSERT OR IGNORE INTO fomo_paper_trials("
            "release_commit,strategy_version,source_signature,token_mint,trigger_wallet,venue,lifecycle,regime,"
            "fomo_state,wallet_context_state,decision,decision_reason,position_fraction,entry_observed_at,"
            "signal_to_entry_seconds,entry_cost_sol,entry_token_raw,token_decimals,entry_all_in_price_sol,"
            "entry_executable,exit_executable,created_at,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                adapter.release_commit,
                FOMO_PAPER_STRATEGY_VERSION,
                signature,
                str(trial.get("token_mint") or observation.get("token_mint") or ""),
                str(trial.get("trigger_wallet") or ""),
                str(observation.get("venue") or "UNKNOWN"),
                str(observation.get("lifecycle") or "unknown"),
                str(observation.get("regime") or trial.get("regime") or "unknown"),
                fomo_state,
                str(profile.get("state") or "bootstrap_forward_evidence"),
                str(decision["decision"]),
                str(decision["reason"]),
                float(decision["position_fraction"]),
                str(trial.get("observed_at") or observation.get("observed_at") or now),
                float(trial.get("signal_to_entry_seconds") or 0.0),
                entry_cost_sol,
                int(trial.get("entry_token_raw") or 0) or None,
                int(trial.get("token_decimals") or 0) if trial.get("token_decimals") is not None else None,
                _finite(trial.get("entry_all_in_price_sol")),
                1 if bool(trial.get("entry_executable")) else 0,
                1 if bool(trial.get("exit_executable")) else 0,
                now,
            ),
        )
    if cursor.rowcount == 1:
        adapter.store.append(
            "fomo_paper_strategy_decision",
            now,
            {
                "source_signature": signature,
                "token_mint": str(trial.get("token_mint") or ""),
                "trigger_wallet": str(trial.get("trigger_wallet") or ""),
                "fomo_state": fomo_state,
                "wallet_context_state": str(profile.get("state") or ""),
                "decision": str(decision["decision"]),
                "position_fraction": float(decision["position_fraction"]),
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
                "historical_promotion_authority": False,
            },
        )
        return True
    return False


def _sync_paper_outcomes(adapter: FinalProfitFirstResearchAdapter) -> int:
    _schema(adapter)
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT t.source_signature,t.token_mint,t.trigger_wallet,t.venue,t.lifecycle,t.regime,t.fomo_state,"
            "t.position_fraction,o.exit_signature,o.net_return,o.exit_reason "
            "FROM fomo_paper_trials t "
            "JOIN profit_first_final_outcomes o ON o.epoch_id=? AND o.source_signature=t.source_signature AND o.lane=? "
            "LEFT JOIN fomo_paper_outcomes p ON p.release_commit=t.release_commit AND p.source_signature=t.source_signature "
            "WHERE t.release_commit=? AND t.decision LIKE 'paper_enter_%' AND p.id IS NULL ORDER BY t.id LIMIT 500",
            (adapter.epoch_id, UNIFIED_LANE, adapter.release_commit),
        ).fetchall()
    inserted = 0
    now = datetime.now(timezone.utc).isoformat()
    for row in rows:
        net_return = float(row["net_return"])
        fraction = float(row["position_fraction"])
        with adapter.store._lock, adapter.store.db:
            cursor = adapter.store.db.execute(
                "INSERT OR IGNORE INTO fomo_paper_outcomes("
                "release_commit,strategy_version,source_signature,exit_signature,token_mint,trigger_wallet,"
                "venue,lifecycle,regime,fomo_state,position_fraction,net_return,paper_return_contribution,"
                "exit_reason,settled_at,paper_only,live_money_authority) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    adapter.release_commit,
                    FOMO_PAPER_STRATEGY_VERSION,
                    str(row["source_signature"]),
                    str(row["exit_signature"]),
                    str(row["token_mint"]),
                    str(row["trigger_wallet"]),
                    str(row["venue"]),
                    str(row["lifecycle"]),
                    str(row["regime"]),
                    str(row["fomo_state"]),
                    fraction,
                    net_return,
                    fraction * net_return,
                    str(row["exit_reason"]),
                    now,
                ),
            )
        if cursor.rowcount == 1:
            inserted += 1
    if inserted:
        _refresh_cohort(adapter)
    return inserted


def _paper_status(adapter: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    _schema(adapter)
    _sync_paper_outcomes(adapter)
    rankings = _refresh_cohort(adapter)
    with adapter.store._lock:
        counts = adapter.store.db.execute(
            "SELECT COUNT(*) AS considered,"
            "SUM(CASE WHEN decision LIKE 'paper_enter_%' THEN 1 ELSE 0 END) AS entered "
            "FROM fomo_paper_trials WHERE release_commit=?",
            (adapter.release_commit,),
        ).fetchone()
        outcomes = adapter.store.db.execute(
            "SELECT net_return,paper_return_contribution FROM fomo_paper_outcomes WHERE release_commit=? ORDER BY id",
            (adapter.release_commit,),
        ).fetchall()
    net_returns = [float(row["net_return"]) for row in outcomes]
    contributions = [float(row["paper_return_contribution"]) for row in outcomes]
    promoted = [row for row in rankings if row["state"] == "promoted_fomo_wallet"]
    return {
        "strategy_version": FOMO_PAPER_STRATEGY_VERSION,
        "lane": FOMO_PAPER_LANE,
        "research_evidence_lane": FOMO_RESEARCH_LANE,
        "paper_strategy_authority": True,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
        "historical_promotion_authority": False,
        "starting_paper_nav_usd": STARTING_PAPER_NAV_USD,
        "wallet_promotion_evidence": "same_release_forward_fomo_outcomes_only",
        "wallet_context_key": "wallet_x_venue_x_lifecycle_x_regime",
        "actionable_fomo_states": sorted(ACTIONABLE_FOMO_STATES),
        "bootstrap_paper_fraction": BOOTSTRAP_PAPER_FRACTION,
        "max_position_fraction": MAX_FOMO_POSITION_FRACTION,
        "minimum_forward_samples_for_wallet_promotion": MIN_FOMO_WALLET_FORWARD_SAMPLES,
        "one_open_position_per_token": True,
        "considered_count": int(counts["considered"] or 0) if counts is not None else 0,
        "paper_entry_count": int(counts["entered"] or 0) if counts is not None else 0,
        "settled_paper_outcome_count": len(net_returns),
        "mean_settled_residual_roi_pct": mean(net_returns) * 100.0 if net_returns else None,
        "median_settled_residual_roi_pct": median(net_returns) * 100.0 if net_returns else None,
        "paper_return_contribution_sum_pct_of_nav": sum(contributions) * 100.0 if contributions else 0.0,
        "open_position_fraction": _open_position_fraction(adapter),
        "promoted_fomo_wallet_context_count": len(promoted),
        "promoted_fomo_wallets": sorted({str(row["wallet"]) for row in promoted}),
        "wallet_rankings": rankings[:25],
        "tracking_scope": "fomo_specific_logical_cohort_over_existing_prospective_wallet_transport",
        "existing_scout_wallet_authority_modified": False,
    }


async def _observe_with_fomo_paper(self: FinalProfitFirstResearchAdapter, signature: str) -> None:
    if _ORIGINAL_OBSERVE is None:
        raise RuntimeError("FOMO paper strategy missing original observe")
    await _ORIGINAL_OBSERVE(self, signature)
    try:
        _record_paper_trial(self, signature)
    except Exception as exc:
        setattr(self, "_roi_fomo_paper_last_error", f"{type(exc).__name__}: {exc}")


async def _sell_with_fomo_paper(self: FinalProfitFirstResearchAdapter, row: dict[str, Any]) -> None:
    if _ORIGINAL_SELL is None:
        raise RuntimeError("FOMO paper strategy missing original sell")
    await _ORIGINAL_SELL(self, row)
    try:
        _sync_paper_outcomes(self)
    except Exception as exc:
        setattr(self, "_roi_fomo_paper_last_error", f"{type(exc).__name__}: outcome sync failed: {exc}")


def _status_with_fomo_paper(self: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("FOMO paper strategy missing original status")
    payload = _ORIGINAL_STATUS(self)
    try:
        status = _paper_status(self)
        status["last_error"] = getattr(self, "_roi_fomo_paper_last_error", None)
        payload["fomo_paper_strategy"] = status
    except Exception as exc:
        payload["fomo_paper_strategy"] = {
            "strategy_version": FOMO_PAPER_STRATEGY_VERSION,
            "paper_strategy_authority": True,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
            "failed_closed": True,
            "last_error": f"{type(exc).__name__}: {exc}",
        }
    return payload


def _manifest_with_fomo_paper(self: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    if _ORIGINAL_MANIFEST is None:
        raise RuntimeError("FOMO paper strategy missing original manifest")
    payload = _ORIGINAL_MANIFEST(self)
    payload.update(
        {
            "fomo_paper_strategy_version": FOMO_PAPER_STRATEGY_VERSION,
            "fomo_paper_lane": FOMO_PAPER_LANE,
            "fomo_strategy_authority": "paper_only",
            "fomo_research_collector_remains_shadow_for_audit": True,
            "fomo_wallet_ranking_basis": "percentage_forward_residual_roi_with_best_trade_trim",
            "fomo_wallet_promotion_evidence": "same_release_forward_fomo_outcomes_only",
            "fomo_wallet_context_key": "wallet_x_venue_x_lifecycle_x_regime",
            "fomo_bootstrap_paper_probe_fraction": BOOTSTRAP_PAPER_FRACTION,
            "fomo_max_paper_position_fraction": MAX_FOMO_POSITION_FRACTION,
            "fomo_active_states_for_paper_entry": sorted(ACTIONABLE_FOMO_STATES),
            "fomo_historical_promotion_authority": False,
            "fomo_paper_only": True,
            "fomo_live_money_authority": False,
            "fomo_signing_available": False,
            "fomo_transaction_submission_available": False,
            "existing_scout_strategy_authority_modified_by_fomo": False,
        }
    )
    return payload


def install_fomo_paper_strategy() -> None:
    """Activate a separate FOMO paper lane above the shadow evidence collector."""
    global _ORIGINAL_OBSERVE, _ORIGINAL_SELL, _ORIGINAL_STATUS, _ORIGINAL_MANIFEST

    if _ORIGINAL_OBSERVE is None:
        _ORIGINAL_OBSERVE = FinalProfitFirstResearchAdapter.observe
        _observe_with_fomo_paper.__dict__.update(getattr(_ORIGINAL_OBSERVE, "__dict__", {}))
        setattr(_observe_with_fomo_paper, "_roi_fomo_paper_strategy", True)
        FinalProfitFirstResearchAdapter.observe = _observe_with_fomo_paper  # type: ignore[method-assign]

    if _ORIGINAL_SELL is None:
        _ORIGINAL_SELL = FinalProfitFirstResearchAdapter._sell
        _sell_with_fomo_paper.__dict__.update(getattr(_ORIGINAL_SELL, "__dict__", {}))
        setattr(_sell_with_fomo_paper, "_roi_fomo_paper_strategy", True)
        FinalProfitFirstResearchAdapter._sell = _sell_with_fomo_paper  # type: ignore[method-assign]

    if _ORIGINAL_STATUS is None:
        _ORIGINAL_STATUS = FinalProfitFirstResearchAdapter.status
        _status_with_fomo_paper.__dict__.update(getattr(_ORIGINAL_STATUS, "__dict__", {}))
        setattr(_status_with_fomo_paper, "_roi_fomo_paper_strategy", True)
        FinalProfitFirstResearchAdapter.status = _status_with_fomo_paper  # type: ignore[method-assign]

    if _ORIGINAL_MANIFEST is None:
        _ORIGINAL_MANIFEST = FinalProfitFirstResearchAdapter._manifest
        _manifest_with_fomo_paper.__dict__.update(getattr(_ORIGINAL_MANIFEST, "__dict__", {}))
        setattr(_manifest_with_fomo_paper, "_roi_fomo_paper_strategy", True)
        FinalProfitFirstResearchAdapter._manifest = _manifest_with_fomo_paper  # type: ignore[method-assign]


__all__ = [
    "ACTIVE_FOMO_PAPER_STRATEGY_AUTHORITY",
    "ACTIONABLE_FOMO_STATES",
    "BOOTSTRAP_PAPER_FRACTION",
    "FOMO_PAPER_LANE",
    "FOMO_PAPER_STRATEGY_VERSION",
    "HISTORICAL_PROMOTION_AUTHORITY",
    "LIVE_MONEY_AUTHORITY",
    "MAX_FOMO_POSITION_FRACTION",
    "MIN_FOMO_WALLET_FORWARD_SAMPLES",
    "PAPER_ONLY",
    "SIGNING_AVAILABLE",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "best_fomo_position_fraction",
    "classify_fomo_wallet_returns",
    "install_fomo_paper_strategy",
]
