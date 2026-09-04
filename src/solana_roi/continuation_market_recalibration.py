from __future__ import annotations

import asyncio
import json
import math
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from . import canonical_worker_isolation_repair as canonical_isolation
from . import risk_conditioned_alpha_v5 as v5
from . import risk_conditioned_alpha_v51 as v51
from . import strategy_candidate_admission_repair as admission
from .fomo_paper_strategy import MAX_FOMO_POSITION_FRACTION
from .observation import WSOL_MINT
from .profit_first_entity_final import STARTING_PAPER_NAV_USD
from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from .quote import LAMPORTS_PER_SOL
from .robinhood_chain_core import _clean_address, _finite, _utcnow
from .robinhood_chain_profit_maximizer import RobinhoodProfitMaximizerMixin


RECALIBRATION_VERSION = "continuation-market-recalibration-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

FOMO_SCAN_SECONDS = 2.0
FOMO_LONG_WINDOW_SECONDS = 60.0
FOMO_SHORT_WINDOW_SECONDS = 10.0
FOMO_MAX_ROWS_PER_SCAN = 2000
FOMO_MAX_CANDIDATES_PER_SCAN = 12
FOMO_MAX_OPEN_EXPOSURE = 0.20
FOMO_STOP_LOSS = -0.12
FOMO_HARVEST = 0.30
FOMO_MAX_HOLD_SECONDS = 20 * 60

_ORIGINAL_SOLANA_BUY: Callable[..., Any] | None = None
_ORIGINAL_SOLANA_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_CANONICAL_WORKERS: Callable[..., Any] | None = None
_ORIGINAL_RH_V3: Callable[..., Any] | None = None
_ORIGINAL_RH_V2: Callable[..., Any] | None = None
_ORIGINAL_RH_FLOW: Callable[..., Any] | None = None
_INSTALLED = False


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def continuation_chase_band(value: float | None) -> str:
    if value is None or not math.isfinite(float(value)):
        return "unknown"
    numeric = max(0.0, float(value))
    if numeric <= 0.15:
        return "0_15pct"
    if numeric <= 0.25:
        return "15_25pct"
    if numeric <= 0.40:
        return "25_40pct"
    if numeric <= 0.75:
        return "40_75pct"
    if numeric <= 1.25:
        return "75_125pct"
    return "gt_125pct"


def continuation_latency_band(value: float | None) -> str:
    if value is None or not math.isfinite(float(value)):
        return "unknown"
    numeric = max(0.0, float(value))
    if numeric <= 5.0:
        return "le_5s"
    if numeric <= 10.0:
        return "5_10s"
    if numeric <= 20.0:
        return "10_20s"
    if numeric <= 30.0:
        return "20_30s"
    if numeric <= 60.0:
        return "30_60s"
    if numeric <= 120.0:
        return "1_2m"
    if numeric <= 300.0:
        return "2_5m"
    return "gt_5m"


def continuation_strategy_evaluation_eligible(row: dict[str, Any]) -> bool:
    """Admit valid forward buys without turning elapsed time into a sniper-era veto."""
    if str(row.get("side") or "").lower() != "buy":
        return False
    if not str(row.get("signature") or "") or not str(row.get("token_mint") or "") or not str(row.get("wallet") or ""):
        return False
    wallet_price = _safe_float(row.get("wallet_price_sol"))
    lag_ms = _safe_float(row.get("observation_lag_ms"))
    return bool(wallet_price is not None and wallet_price > 0.0 and lag_ms is not None and lag_ms >= 0.0)


def _recent_token_flow(store: Any, token_mint: str, *, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    long_start = (now - timedelta(seconds=60)).isoformat()
    short_start = (now - timedelta(seconds=15)).isoformat()
    end = now.isoformat()
    rows: list[dict[str, Any]] = []
    try:
        with store._lock:
            raw = store.db.execute(
                "SELECT wallet,side,native_amount_sol,received_at FROM normalized_swaps "
                "WHERE token_mint=? AND received_at>=? AND received_at<=? ORDER BY received_at,id",
                (token_mint, long_start, end),
            ).fetchall()
        rows = [dict(row) for row in raw]
    except Exception:
        rows = []
    if not rows:
        try:
            with store._lock:
                raw = store.db.execute(
                    "SELECT wallet,side,(token_amount*wallet_price_sol) native_amount_sol,received_at "
                    "FROM wallet_discovery_forward_observations WHERE token_mint=? AND received_at>=? AND received_at<=? "
                    "ORDER BY received_at,id",
                    (token_mint, long_start, end),
                ).fetchall()
            rows = [dict(row) for row in raw]
        except Exception:
            rows = []

    def summarize(items: list[dict[str, Any]]) -> dict[str, Any]:
        buys = [row for row in items if str(row.get("side") or "").lower() == "buy"]
        sells = [row for row in items if str(row.get("side") or "").lower() == "sell"]
        buy_sol = sum(max(0.0, float(row.get("native_amount_sol") or 0.0)) for row in buys)
        sell_sol = sum(max(0.0, float(row.get("native_amount_sol") or 0.0)) for row in sells)
        return {
            "buys": len(buys),
            "sells": len(sells),
            "buyers": len({str(row.get("wallet") or "") for row in buys if str(row.get("wallet") or "")}),
            "buy_sol": buy_sol,
            "sell_sol": sell_sol,
        }

    short = summarize([row for row in rows if str(row.get("received_at") or "") >= short_start])
    long = summarize(rows)
    return {"short": short, "long": long, "row_count": len(rows)}


def _residual_state(
    store: Any,
    token_mint: str,
    *,
    chase: float | None,
    latency: float | None,
    round_trip_cost: float | None,
) -> dict[str, Any]:
    flow = _recent_token_flow(store, token_mint)
    short = flow["short"]
    long = flow["long"]
    chase_n = max(0.0, float(chase or 0.0))
    latency_n = max(0.0, float(latency or 0.0))
    cost_n = max(0.0, float(round_trip_cost or 0.0))
    clear_reversal = bool(short["sells"] >= 2 and short["sell_sol"] > short["buy_sol"] and short["sells"] > short["buys"])
    persistent = bool(long["buys"] >= 2 and long["buy_sol"] >= long["sell_sol"] and not clear_reversal)
    strong = bool(long["buyers"] >= 2 and short["buys"] >= 1 and long["buy_sol"] > long["sell_sol"] and not clear_reversal)
    very_strong = bool(long["buyers"] >= 3 and short["buys"] >= 2 and long["buy_sol"] > 1.15 * max(long["sell_sol"], 1e-12) and not clear_reversal)

    if clear_reversal:
        state = "flow_reversed"
        actionable = False
    elif chase_n > 1.25:
        state = "extended_continuation" if very_strong else "extended_unconfirmed"
        actionable = very_strong
    elif chase_n > 0.75:
        state = "high_chase_continuation" if strong else "high_chase_unconfirmed"
        actionable = strong
    elif chase_n > 0.40 or latency_n > 20.0:
        state = "continuation_confirmed" if persistent else "continuation_unconfirmed"
        actionable = persistent
    else:
        state = "normal_window"
        actionable = True

    return {
        "state": state,
        "actionable": actionable,
        "flow": flow,
        "chase_band": continuation_chase_band(chase_n),
        "latency_band": continuation_latency_band(latency_n),
        "round_trip_cost_fraction": cost_n,
        "cost_is_context_not_static_veto": True,
    }


def _continuation_fraction_cap(chase: float | None, latency: float | None, cost: float | None) -> float:
    cap = 0.20
    chase_n = max(0.0, float(chase or 0.0))
    latency_n = max(0.0, float(latency or 0.0))
    cost_n = max(0.0, float(cost or 0.0))
    if chase_n > 1.25:
        cap = min(cap, 0.0025)
    elif chase_n > 0.75:
        cap = min(cap, 0.005)
    elif chase_n > 0.40:
        cap = min(cap, 0.01)
    if latency_n > 300.0:
        cap = min(cap, 0.0025)
    elif latency_n > 60.0:
        cap = min(cap, 0.005)
    elif latency_n > 20.0:
        cap = min(cap, 0.01)
    if cost_n > 0.30:
        cap = min(cap, 0.0025)
    elif cost_n > 0.15:
        cap = min(cap, 0.005)
    return cap


def _continuation_schema(adapter: Any) -> None:
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS continuation_recalibration_audit ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, "
            "token_mint TEXT NOT NULL, lane TEXT, decision TEXT NOT NULL, reason TEXT NOT NULL, position_fraction REAL NOT NULL, "
            "chase_fraction REAL, latency_seconds REAL, round_trip_cost_fraction REAL, chase_band TEXT NOT NULL, latency_band TEXT NOT NULL, "
            "residual_state TEXT NOT NULL, residual_json TEXT NOT NULL, created_at TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, UNIQUE(release_commit,source_signature))"
        )
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS independent_fomo_runtime ("
            "key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)"
        )


def _write_continuation_audit(adapter: Any, *, row: dict[str, Any], lane: str | None, decision: str, reason: str,
                              fraction: float, chase: float | None, latency: float | None, cost: float | None,
                              residual: dict[str, Any]) -> None:
    _continuation_schema(adapter)
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "INSERT OR REPLACE INTO continuation_recalibration_audit("
            "release_commit,source_signature,token_mint,lane,decision,reason,position_fraction,chase_fraction,latency_seconds,"
            "round_trip_cost_fraction,chase_band,latency_band,residual_state,residual_json,created_at,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                adapter.release_commit,str(row.get("signature") or ""),str(row.get("token_mint") or ""),lane,decision,reason,float(fraction),
                chase,latency,cost,continuation_chase_band(chase),continuation_latency_band(latency),str(residual.get("state") or "unknown"),
                json.dumps(residual,sort_keys=True,separators=(",", ":")),datetime.now(timezone.utc).isoformat(),
            ),
        )


async def _buy_with_continuation_recalibration(self: Any, row: dict[str, Any]) -> None:
    if _ORIGINAL_SOLANA_BUY is None:
        raise RuntimeError("continuation recalibration is not installed")
    await _ORIGINAL_SOLANA_BUY(self, row)
    signature = str(row.get("signature") or "")
    if not signature:
        return
    _continuation_schema(self)
    with self.store._lock:
        raw_rows = self.store.db.execute(
            "SELECT * FROM risk_conditioned_alpha_v5_trials WHERE release_commit=? AND source_signature=? ORDER BY id",
            (self.release_commit, signature),
        ).fetchall()
        unified_raw = self.store.db.execute(
            "SELECT * FROM profit_first_final_trials WHERE epoch_id=? AND source_signature=? AND lane='unified_profit_maximizer' "
            "ORDER BY id DESC LIMIT 1",
            (self.epoch_id, signature),
        ).fetchone()
    if not raw_rows or unified_raw is None:
        return
    lane_rows = [dict(item) for item in raw_rows]
    if any(int(item.get("selected") or 0) == 1 and str(item.get("decision") or "").startswith("paper_enter") for item in lane_rows):
        return
    unified = dict(unified_raw)
    selected = lane_rows[0]
    pre = v51._reconstruct_pre(self, selected, unified, lane_rows)
    risk = pre.get("risk") or {}
    if not bool(risk.get("structurally_tradeable", False)):
        return

    wallet_price = _safe_float(row.get("wallet_price_sol"))
    entry_price = _safe_float(unified.get("entry_all_in_price_sol"))
    chase = _safe_float(row.get("chase_fraction"))
    if wallet_price and entry_price and wallet_price > 0.0:
        chase = max(0.0, entry_price / wallet_price - 1.0)
    latency = _safe_float(unified.get("signal_to_entry_seconds"))
    cost = _safe_float(unified.get("round_trip_cost_fraction"))
    lane, requested, _profiles = v5._choose_lane_and_fraction(self, pre, chase=chase, latency=latency)
    requested = v51._constraints_cap(self, pre.get("creator_entity"), bool(unified.get("exit_executable")), requested)
    if lane is None or requested <= 0.0:
        return

    residual = _residual_state(self.store, str(row.get("token_mint") or ""), chase=chase, latency=latency, round_trip_cost=cost)
    if not bool(residual["actionable"]):
        _write_continuation_audit(
            self,row=row,lane=lane,decision="paper_observe_residual_state",reason=str(residual["state"]),fraction=0.0,
            chase=chase,latency=latency,cost=cost,residual=residual,
        )
        return

    cap = _continuation_fraction_cap(chase, latency, cost)
    desired = min(float(requested), cap)
    ladder = []
    for candidate in (desired, min(desired, 0.01), min(desired, 0.005), min(desired, 0.0025)):
        if candidate > 0.0 and all(abs(candidate - old) > 1e-9 for old in ladder):
            ladder.append(candidate)
    ladder.sort(reverse=True)

    execution: dict[str, Any] | None = None
    chosen = 0.0
    final_residual = residual
    for fraction in ladder:
        candidate = await self._execution(row, fraction)
        if candidate is None or candidate.get("exit_net_sol") is None:
            continue
        candidate_chase = _safe_float(candidate.get("chase_fraction"))
        candidate_latency = _safe_float(candidate.get("signal_to_entry_seconds"))
        candidate_cost = _safe_float(candidate.get("round_trip_cost_fraction"))
        current_residual = _residual_state(
            self.store,str(row.get("token_mint") or ""),chase=candidate_chase,latency=candidate_latency,round_trip_cost=candidate_cost,
        )
        if not bool(current_residual["actionable"]):
            final_residual = current_residual
            continue
        hard_cap = _continuation_fraction_cap(candidate_chase, candidate_latency, candidate_cost)
        if fraction > hard_cap + 1e-9:
            continue
        execution = candidate
        chosen = fraction
        chase, latency, cost = candidate_chase, candidate_latency, candidate_cost
        final_residual = current_residual
        break

    if execution is None or chosen <= 0.0:
        _write_continuation_audit(
            self,row=row,lane=lane,decision="paper_observe_no_safe_executable_probe",reason="descending_liquidity_ladder_exhausted",
            fraction=0.0,chase=chase,latency=latency,cost=cost,residual=final_residual,
        )
        return

    decision = "paper_enter_continuation_probe"
    if chase is not None and chase > 0.40:
        decision = "paper_enter_residual_chase_continuation"
    elif latency is not None and latency > 20.0:
        decision = "paper_enter_state_fresh_continuation"
    reason = "current_flow_persistent_plus_exact_executable_round_trip"
    now = datetime.now(timezone.utc).isoformat()
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "UPDATE risk_conditioned_alpha_v5_trials SET selected=CASE WHEN lane=? THEN 1 ELSE 0 END, "
            "decision=CASE WHEN lane=? THEN ? ELSE 'paper_observe_not_selected_continuation' END, decision_reason=?, "
            "position_fraction=?,quote_input_lamports=?,entry_fee_lamports=?,entry_token_raw=?,entry_cost_sol=?,immediate_exit_net_sol=?,"
            "round_trip_cost_fraction=?,entry_executable=1,exit_executable=1,chase_band=?,latency_band=? "
            "WHERE release_commit=? AND source_signature=?",
            (
                lane,lane,decision,reason,chosen,int(execution["input_lamports"]),int(execution["entry_fee_lamports"]),int(execution["token_raw"]),
                float(execution["entry_cost_sol"]),float(execution["exit_net_sol"]),float(execution["round_trip_cost_fraction"]),
                continuation_chase_band(chase),continuation_latency_band(latency),self.release_commit,signature,
            ),
        )
        decision_json = json.dumps(
            {"action":"paper_enter","selected_lane":lane,"reason":reason,"continuation_recalibration":RECALIBRATION_VERSION},
            sort_keys=True,separators=(",", ":"),
        )
        self.store.db.execute(
            "UPDATE profit_first_final_trials SET assigned_position_fraction=?,quote_input_lamports=?,entry_fee_lamports=?,entry_token_raw=?,"
            "token_decimals=?,entry_all_in_price_sol=?,immediate_exit_net_sol=?,round_trip_cost_fraction=?,signal_to_entry_seconds=?,"
            "quote_latency_ms=?,entry_executable=1,exit_executable=1,decision_json=CASE WHEN lane='unified_profit_maximizer' THEN ? ELSE decision_json END "
            "WHERE epoch_id=? AND source_signature=?",
            (
                chosen,int(execution["input_lamports"]),int(execution["entry_fee_lamports"]),int(execution["token_raw"]),int(execution["decimals"]),
                float(execution["entry_price_sol"]),float(execution["exit_net_sol"]),float(execution["round_trip_cost_fraction"]),
                float(execution["signal_to_entry_seconds"]),float(execution["quote_latency_ms"]),decision_json,self.epoch_id,signature,
            ),
        )
    setattr(self,"_roi_continuation_entries",int(getattr(self,"_roi_continuation_entries",0) or 0)+1)
    _write_continuation_audit(
        self,row=row,lane=lane,decision=decision,reason=reason,fraction=chosen,chase=chase,latency=latency,cost=cost,residual=final_residual,
    )


def _status_with_continuation_recalibration(self: Any) -> dict[str, Any]:
    if _ORIGINAL_SOLANA_STATUS is None:
        raise RuntimeError("continuation recalibration status not installed")
    payload = _ORIGINAL_SOLANA_STATUS(self)
    _continuation_schema(self)
    with self.store._lock:
        audits = int(self.store.db.execute(
            "SELECT COUNT(*) FROM continuation_recalibration_audit WHERE release_commit=?",(self.release_commit,),
        ).fetchone()[0])
        independent = int(self.store.db.execute(
            "SELECT COUNT(*) FROM fomo_paper_trials WHERE release_commit=? AND decision_reason LIKE 'independent_market_flow%'",(self.release_commit,),
        ).fetchone()[0]) if self.store.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='fomo_paper_trials'").fetchone() else 0
    payload["continuation_market_recalibration"] = {
        "version": RECALIBRATION_VERSION,
        "sniper_timing_veto_active": False,
        "fixed_40pct_chase_veto_active": False,
        "state_based_continuation_authority": True,
        "chase_is_context_not_kill_switch": True,
        "latency_is_context_not_kill_switch": True,
        "expanded_chase_bands": ["0_15pct","15_25pct","25_40pct","40_75pct","75_125pct","gt_125pct"],
        "expanded_latency_bands": ["le_5s","5_10s","10_20s","20_30s","30_60s","1_2m","2_5m","gt_5m"],
        "sizing_policy": "descending_exact_executable_liquidity_ladder_fail_small",
        "static_execution_cost_is_entry_veto": False,
        "mechanical_entry_and_full_exit_required": True,
        "independent_fomo_candidate_source": "bounded_recent_normalized_swaps",
        "independent_fomo_requires_tracked_wallet": False,
        "market_cap_is_sizing_authority": False,
        "liquidity_and_price_impact_are_sizing_authority": True,
        "audit_rows": audits,
        "continuation_entries_session": int(getattr(self,"_roi_continuation_entries",0) or 0),
        "independent_fomo_trials": independent,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    return payload


def _ensure_fomo_schema(adapter: Any) -> None:
    from . import fomo_paper_strategy as paper
    paper._schema(adapter)
    _continuation_schema(adapter)


def _fomo_open_fraction(adapter: Any) -> float:
    _ensure_fomo_schema(adapter)
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT position_fraction FROM fomo_paper_trials t LEFT JOIN fomo_paper_outcomes o "
            "ON o.release_commit=t.release_commit AND o.source_signature=t.source_signature "
            "WHERE t.release_commit=? AND t.decision LIKE 'paper_enter%' AND o.id IS NULL",
            (adapter.release_commit,),
        ).fetchall()
    return min(1.0, sum(float(row["position_fraction"] or 0.0) for row in rows))


def _fomo_flow_candidates(adapter: Any) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    start = (now - timedelta(seconds=FOMO_LONG_WINDOW_SECONDS)).isoformat()
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT signature,wallet,token_mint,side,token_amount,native_amount_sol,reference_price_sol,observed_at,received_at,source "
            "FROM normalized_swaps WHERE received_at>=? ORDER BY received_at DESC,id DESC LIMIT ?",
            (start,FOMO_MAX_ROWS_PER_SCAN),
        ).fetchall()
    grouped: dict[str,list[dict[str,Any]]] = {}
    for raw in reversed(rows):
        row = dict(raw)
        token = str(row.get("token_mint") or "")
        if token:
            grouped.setdefault(token,[]).append(row)
    candidates: list[dict[str,Any]] = []
    short_cutoff = (now - timedelta(seconds=FOMO_SHORT_WINDOW_SECONDS)).isoformat()
    for token, items in grouped.items():
        buys = [r for r in items if str(r.get("side") or "").lower()=="buy"]
        sells = [r for r in items if str(r.get("side") or "").lower()=="sell"]
        short_buys = [r for r in buys if str(r.get("received_at") or "")>=short_cutoff]
        if not buys or not short_buys:
            continue
        buy_sol = sum(max(0.0,float(r.get("native_amount_sol") or 0.0)) for r in buys)
        sell_sol = sum(max(0.0,float(r.get("native_amount_sol") or 0.0)) for r in sells)
        buyers = len({str(r.get("wallet") or "") for r in buys if str(r.get("wallet") or "")})
        acceleration = (len(short_buys)/max(FOMO_SHORT_WINDOW_SECONDS,1.0))/(len(buys)/FOMO_LONG_WINDOW_SECONDS)
        if len(buys)<3 or buyers<2 or buy_sol<=sell_sol or acceleration<0.9:
            continue
        latest = buys[-1]
        score = acceleration + buyers/2.0 + buy_sol/max(sell_sol,0.01)
        state = "active_fomo" if len(short_buys)>=2 and buyers>=3 and acceleration>=1.25 else "pre_fomo"
        candidates.append({"token":token,"rows":items,"latest":latest,"buyers":buyers,"buy_sol":buy_sol,"sell_sol":sell_sol,
                           "acceleration":acceleration,"score":score,"state":state})
    candidates.sort(key=lambda x: float(x["score"]),reverse=True)
    return candidates[:FOMO_MAX_CANDIDATES_PER_SCAN]


async def _open_independent_fomo(adapter: Any, candidate: dict[str, Any]) -> bool:
    _ensure_fomo_schema(adapter)
    token = str(candidate["token"])
    latest = dict(candidate["latest"])
    source_signature = f"market-flow:{latest.get('signature')}"
    with adapter.store._lock:
        exists = adapter.store.db.execute(
            "SELECT 1 FROM fomo_paper_trials WHERE release_commit=? AND (source_signature=? OR token_mint=? AND decision LIKE 'paper_enter%') LIMIT 1",
            (adapter.release_commit,source_signature,token),
        ).fetchone()
    if exists is not None:
        return False
    price = _safe_float(latest.get("reference_price_sol"))
    if price is None or price<=0.0:
        amount = _safe_float(latest.get("token_amount"))
        native = _safe_float(latest.get("native_amount_sol"))
        price = native/amount if amount and native and amount>0 else None
    if price is None or price<=0.0:
        return False
    at = datetime.fromisoformat(str(latest.get("received_at") or datetime.now(timezone.utc).isoformat()))
    risk_row = {
        "token_mint":token,"wallet":str(latest.get("wallet") or "market_flow"),"signature":source_signature,
        "observed_at":str(latest.get("observed_at") or at.isoformat()),"received_at":str(latest.get("received_at") or at.isoformat()),
        "wallet_price_sol":price,"token_amount":float(latest.get("token_amount") or 0.0),"side":"buy","source":str(latest.get("source") or "MARKET_FLOW"),
    }
    hard, soft, early_exit = await adapter.execution._risk(risk_row, at)
    descriptor = v5.risk_descriptor(soft_flags=soft,hard_flags=hard,early_exit_fraction=float(early_exit))
    if not bool(descriptor.get("structurally_tradeable")):
        return False
    regime = adapter._market_regime(at).value
    severity = float(descriptor.get("risk_severity") or 0.0)
    fraction = 0.005 if candidate["state"]=="active_fomo" else 0.0025
    fraction *= v5._regime_multiplier(regime)
    fraction *= max(0.25,1.0-0.60*severity)
    fraction = min(MAX_FOMO_POSITION_FRACTION,max(0.0025,fraction),max(0.0,FOMO_MAX_OPEN_EXPOSURE-_fomo_open_fraction(adapter)))
    if fraction<=0.0:
        return False
    execution = await adapter._execution(risk_row,fraction)
    if execution is None or execution.get("exit_net_sol") is None:
        return False
    if float(execution["round_trip_cost_fraction"])>0.15:
        fraction=min(fraction,0.0025)
        execution=await adapter._execution(risk_row,fraction)
        if execution is None or execution.get("exit_net_sol") is None:
            return False
    chase=float(execution["chase_fraction"])
    latency=float(execution["signal_to_entry_seconds"])
    residual=_residual_state(adapter.store,token,chase=chase,latency=latency,round_trip_cost=float(execution["round_trip_cost_fraction"]))
    if not bool(residual["actionable"]):
        return False
    now=datetime.now(timezone.utc).isoformat()
    venue=v5._source_venue(str(latest.get("source") or ""))
    lifecycle=v5._lifecycle(adapter,risk_row,venue)
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "INSERT OR IGNORE INTO fomo_paper_trials("
            "release_commit,strategy_version,source_signature,token_mint,trigger_wallet,venue,lifecycle,regime,fomo_state,wallet_context_state,"
            "decision,decision_reason,position_fraction,entry_observed_at,signal_to_entry_seconds,entry_cost_sol,entry_token_raw,token_decimals,"
            "entry_all_in_price_sol,entry_executable,exit_executable,created_at,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                adapter.release_commit,RECALIBRATION_VERSION,source_signature,token,str(latest.get("wallet") or "market_flow"),venue,lifecycle,regime,
                str(candidate["state"]),"independent_market_flow","paper_enter_independent_fomo_probe",
                "independent_market_flow_plus_exact_executable_round_trip",fraction,str(latest.get("observed_at") or now),latency,
                float(execution["entry_cost_sol"]),int(execution["token_raw"]),int(execution["decimals"]),float(execution["entry_price_sol"]),1,1,now,
            ),
        )
    return True


async def _settle_independent_fomo(adapter: Any) -> None:
    _ensure_fomo_schema(adapter)
    with adapter.store._lock:
        rows=adapter.store.db.execute(
            "SELECT t.* FROM fomo_paper_trials t LEFT JOIN fomo_paper_outcomes o ON o.release_commit=t.release_commit "
            "AND o.source_signature=t.source_signature WHERE t.release_commit=? AND t.decision_reason LIKE 'independent_market_flow%' "
            "AND o.id IS NULL ORDER BY t.id",
            (adapter.release_commit,),
        ).fetchall()
    now=datetime.now(timezone.utc)
    for raw in rows:
        trial=dict(raw)
        token=str(trial["token_mint"])
        token_raw=int(trial.get("entry_token_raw") or 0)
        entry_cost=float(trial.get("entry_cost_sol") or 0.0)
        if token_raw<=0 or entry_cost<=0:
            continue
        route=await adapter.execution._route(token,WSOL_MINT,token_raw)
        if route is None:
            continue
        exit_net=(int(route["out_amount"])-int(route["fee_lamports"]))/LAMPORTS_PER_SOL
        if exit_net<=0:
            continue
        net_return=exit_net/entry_cost-1.0
        opened=datetime.fromisoformat(str(trial["created_at"]))
        age=max(0.0,(now-opened).total_seconds())
        flow=_recent_token_flow(adapter.store,token,now=now)
        short=flow["short"]
        reversed_flow=bool(short["sells"]>=2 and short["sell_sol"]>short["buy_sol"] and short["sells"]>short["buys"])
        reason=None
        if net_return<=FOMO_STOP_LOSS:
            reason="stop_loss"
        elif net_return>=FOMO_HARVEST:
            reason="harvest"
        elif reversed_flow:
            reason="flow_reversal"
        elif age>=FOMO_MAX_HOLD_SECONDS:
            reason="max_hold"
        if reason is None:
            continue
        settled=now.isoformat()
        with adapter.store._lock, adapter.store.db:
            adapter.store.db.execute(
                "INSERT OR IGNORE INTO fomo_paper_outcomes("
                "release_commit,strategy_version,source_signature,exit_signature,token_mint,trigger_wallet,venue,lifecycle,regime,fomo_state,"
                "position_fraction,net_return,paper_return_contribution,exit_reason,settled_at,paper_only,live_money_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    adapter.release_commit,RECALIBRATION_VERSION,str(trial["source_signature"]),f"paper-mark:{int(now.timestamp())}:{token}",token,
                    str(trial["trigger_wallet"]),str(trial["venue"]),str(trial["lifecycle"]),str(trial["regime"]),str(trial["fomo_state"]),
                    float(trial["position_fraction"]),net_return,float(trial["position_fraction"])*net_return,
                    f"independent_market_flow:{reason}",settled,
                ),
            )


async def _independent_fomo_worker(runtime: Any, stop: asyncio.Event) -> None:
    from . import profit_first_entity_final_research as final_research
    adapter=final_research._adapter(runtime.wallet_discovery)
    _ensure_fomo_schema(adapter)
    while not stop.is_set():
        error: str | None=None
        opened=0
        candidates=0
        try:
            items=_fomo_flow_candidates(adapter)
            candidates=len(items)
            for item in items:
                if await _open_independent_fomo(adapter,item):
                    opened+=1
            await _settle_independent_fomo(adapter)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            error=f"{type(exc).__name__}: {exc}"
        now=datetime.now(timezone.utc).isoformat()
        with adapter.store._lock, adapter.store.db:
            for key,value in {
                "last_scan_at":now,"last_error":error or "","candidate_count":str(candidates),"opened_count":str(opened),
            }.items():
                adapter.store.db.execute(
                    "INSERT OR REPLACE INTO independent_fomo_runtime(key,value,updated_at) VALUES (?,?,?)",(key,value,now),
                )
        try:
            await asyncio.wait_for(stop.wait(),timeout=FOMO_SCAN_SECONDS)
        except asyncio.TimeoutError:
            pass


async def _canonical_workers_with_independent_fomo(runtime: Any, stop: asyncio.Event) -> None:
    if _ORIGINAL_CANONICAL_WORKERS is None:
        raise RuntimeError("independent FOMO worker composition is not installed")
    fomo_stop=asyncio.Event()
    fomo_task=asyncio.create_task(_independent_fomo_worker(runtime,fomo_stop),name="independent-market-flow-fomo")
    try:
        await _ORIGINAL_CANONICAL_WORKERS(runtime,stop)
    finally:
        fomo_stop.set()
        if not fomo_task.done():
            fomo_task.cancel()
        with suppress(asyncio.CancelledError):
            await fomo_task


def _robinhood_risk_with_cost(risk: dict[str, Any], cost: float) -> dict[str, Any]:
    result=dict(risk)
    if cost>0.15:
        hazards=list(result.get("hazards") or [])
        if "high_execution_cost" not in hazards:
            hazards.append("high_execution_cost")
        result["hazards"]=hazards
        result["risk_signature"]="+".join(sorted(hazards)) if hazards else "clean"
        result["risk_severity"]=min(1.0,float(result.get("risk_severity") or 0.0)+min(0.20,cost*0.25))
    return result


async def _rh_flow_without_sniper_cap(self: Any, swaps: Any, *, deployer: str="") -> dict[str, Any]:
    if _ORIGINAL_RH_FLOW is None:
        raise RuntimeError("Robinhood continuation flow wrapper not installed")
    metrics=dict(await _ORIGINAL_RH_FLOW(self,swaps,deployer=deployer))
    if not bool(metrics.get("entity_resolution_complete")):
        return metrics
    buys=int(metrics.get("buy_count_60s") or 0)
    independent=int(metrics.get("independent_entities_60s") or 0)
    ratio=float(metrics.get("buy_sell_quote_ratio") or 0.0)
    accel=float(metrics.get("buy_count_acceleration") or 0.0)
    change=float(metrics.get("price_change_60s") or 0.0)
    if buys>=4 and independent>=3 and ratio>=1.5 and accel>=1.25 and change>=0.01:
        metrics["state"]="active_fomo"
    elif buys>=3 and independent>=2 and ratio>=1.15:
        metrics["state"]="pre_fomo"
    elif str(metrics.get("state"))=="neutral" and buys>=1 and ratio>=1.0 and str(metrics.get("trigger_entity") or ""):
        metrics["state"]="bootstrap_continuation"
    return metrics


async def _rh_v3_continuation(self: Any, pool: Any, *, current_block: int) -> None:
    if not self._caught_up or self._token_open(pool.token):
        return
    if pool.restrictions_end_block and current_block<=pool.restrictions_end_block:
        return
    if pool.venue=="UNISWAP_V3_DIRECT" and not await self._direct_v3_token_allowed(pool.token):
        return
    metrics=await self._v5_flow_metrics(pool.recent_swaps,deployer=pool.deployer)
    if not bool(metrics.get("entity_resolution_complete")):
        return
    actor=_clean_address(str(metrics.get("trigger_actor") or "")); entity=_clean_address(str(metrics.get("trigger_entity") or ""))
    from .robinhood_chain_core import KNOWN_NON_ACTORS
    if not actor or not entity or actor in KNOWN_NON_ACTORS:
        return
    role="creator_deployer" if bool(metrics.get("trigger_is_creator")) else "independent_entity"
    lifecycle="post_protection_v3" if pool.venue=="PONS_V1_UNISWAP_V3" else "new_weth_pool"
    chase=None
    if pool.first_price_eth and pool.recent_swaps:
        latest=_finite(pool.recent_swaps[-1].get("price_eth"))
        if latest is not None and pool.first_price_eth>0:
            chase=latest/pool.first_price_eth-1.0
    extra=[]
    if chase is None:
        extra.append("chase_unknown")
    elif chase>0.15:
        extra.append("late_lifecycle")
    if float(metrics.get("creator_sell_pressure") or 0.0)>=0.25:
        extra.append("creator_distributing")
    risk=v5.risk_descriptor(soft_flags=(),hard_flags=(),creator_flow_state="distributing" if "creator_distributing" in extra else "neutral",
                            creator_linked_trigger=role=="creator_deployer",extra_hazards=extra)
    regime=self._v5_regime(metrics)
    lanes=self._v5_candidate_lanes(metrics=metrics,hazards=list(risk["hazards"]),lifecycle_progress=None)
    lane,fraction,_=self._v5_choose_lane_fraction(entity=entity,role=role,venue=pool.venue,lifecycle=lifecycle,regime=regime,
        risk_signature=str(risk["risk_signature"]),risk_severity=float(risk["risk_severity"]),flow_state=str(metrics["state"]),lanes=lanes)
    if not lane or fraction<=0:
        return
    quote=await self._quote_v3_round_trip(pool,fraction)
    if quote is None:
        return
    cost=float(quote["round_trip_cost_fraction"])
    if cost>0.15:
        fraction=min(fraction,0.0025)
        quote=await self._quote_v3_round_trip(pool,fraction)
        if quote is None:
            return
        cost=float(quote["round_trip_cost_fraction"])
        risk=_robinhood_risk_with_cost(risk,cost)
    self._v5_insert_trial(token=pool.token,market=pool.pool,venue=pool.venue,lifecycle=lifecycle,trigger_actor=actor,trigger_entity=entity,
        flow_state=str(metrics["state"]),fraction=fraction,quote=quote,lane=lane,role=role,regime=regime,risk=risk,lifecycle_progress=None,
        threshold_challenger=bool(chase is not None and chase>0.15),candidate_lanes=lanes)


async def _rh_v2_continuation(self: Any, curve: Any) -> None:
    if not self._caught_up or self._token_open(curve.token):
        return
    metrics=await self._v5_flow_metrics(curve.recent_swaps,deployer=curve.deployer)
    if not bool(metrics.get("entity_resolution_complete")):
        return
    actor=_clean_address(str(metrics.get("trigger_actor") or "")); entity=_clean_address(str(metrics.get("trigger_entity") or ""))
    from .robinhood_chain_core import KNOWN_NON_ACTORS
    if not actor or not entity or actor in KNOWN_NON_ACTORS:
        return
    role="creator_deployer" if bool(metrics.get("trigger_is_creator")) else "independent_entity"
    eth_usd=await self._eth_usd()
    if eth_usd is None or eth_usd<=0:
        return
    try:
        state=await self.rpc.pons_v2_launch_state(curve.token)
        if int(state["phase"])!=0:
            return
        real_quote=await self.rpc.call_uint(curve.curve,"realQuoteReserve()")
        threshold=max(1,int(state["graduation_threshold"] or curve.graduation_threshold)); progress=real_quote/threshold
    except Exception:
        return
    extra=[]
    if progress>=0.85:
        extra.append("late_lifecycle")
    if float(metrics.get("creator_sell_pressure") or 0.0)>=0.25:
        extra.append("creator_distributing")
    risk=v5.risk_descriptor(soft_flags=(),hard_flags=(),creator_flow_state="distributing" if "creator_distributing" in extra else "neutral",
        creator_linked_trigger=role=="creator_deployer",extra_hazards=extra)
    regime=self._v5_regime(metrics); lanes=self._v5_candidate_lanes(metrics=metrics,hazards=list(risk["hazards"]),lifecycle_progress=progress)
    lane,fraction,_=self._v5_choose_lane_fraction(entity=entity,role=role,venue="PONS_V2_CURVE",lifecycle="bonding_curve",regime=regime,
        risk_signature=str(risk["risk_signature"]),risk_severity=float(risk["risk_severity"]),flow_state=str(metrics["state"]),lanes=lanes)
    if not lane or fraction<=0:
        return
    amount_in=int((self._paper_nav_usd()*fraction/eth_usd)*1e18)
    if amount_in<=0:
        return
    try:
        buy=await self.rpc.pons_v2_curve_quote(curve=curve.curve,quote_in=amount_in,recipient=self.paper_recipient)
        if int(buy["tokens_out"])<=0:
            return
        if int(buy.get("snipe_tax_bps") or 0)>500:
            risk=v5.risk_descriptor(soft_flags=(),hard_flags=(),creator_flow_state="distributing" if "creator_distributing" in extra else "neutral",
                creator_linked_trigger=role=="creator_deployer",extra_hazards=(*extra,"high_snipe_tax"))
        exit_out=await self.rpc.pons_v2_curve_sell_quote(curve=curve.curve,tokens_in=buy["tokens_out"])
        gas_price=await self.rpc.gas_price(); entry_gas_wei=220_000*gas_price; exit_gas_wei=220_000*gas_price
        total_cost=buy["spent"]+entry_gas_wei; immediate_net=max(0,exit_out-exit_gas_wei)
        if immediate_net<=0:
            return
        round_trip=1.0-immediate_net/max(1,total_cost)
        quote={"amount_in_wei":buy["spent"],"token_out":buy["tokens_out"],"entry_gas_wei":entry_gas_wei,"exit_gas_wei":exit_gas_wei,
            "entry_total_cost_wei":total_cost,"immediate_exit_wei":immediate_net,"round_trip_cost_fraction":round_trip,
            "entry_price_eth":(buy["spent"]/1e18)/(buy["tokens_out"]/1e18)}
    except Exception:
        return
    if float(quote["round_trip_cost_fraction"])>0.15 and fraction>0.0025:
        fraction=0.0025
        amount_in=int((self._paper_nav_usd()*fraction/eth_usd)*1e18)
        try:
            buy=await self.rpc.pons_v2_curve_quote(curve=curve.curve,quote_in=amount_in,recipient=self.paper_recipient)
            exit_out=await self.rpc.pons_v2_curve_sell_quote(curve=curve.curve,tokens_in=buy["tokens_out"])
            gas_price=await self.rpc.gas_price(); entry_gas_wei=220_000*gas_price; exit_gas_wei=220_000*gas_price
            total_cost=buy["spent"]+entry_gas_wei; immediate_net=max(0,exit_out-exit_gas_wei)
            if immediate_net<=0:
                return
            quote={"amount_in_wei":buy["spent"],"token_out":buy["tokens_out"],"entry_gas_wei":entry_gas_wei,"exit_gas_wei":exit_gas_wei,
                "entry_total_cost_wei":total_cost,"immediate_exit_wei":immediate_net,"round_trip_cost_fraction":1.0-immediate_net/max(1,total_cost),
                "entry_price_eth":(buy["spent"]/1e18)/(buy["tokens_out"]/1e18)}
        except Exception:
            return
        risk=_robinhood_risk_with_cost(risk,float(quote["round_trip_cost_fraction"]))
    self._v5_insert_trial(token=curve.token,market=curve.curve,venue="PONS_V2_CURVE",lifecycle="bonding_curve",trigger_actor=actor,trigger_entity=entity,
        flow_state=str(metrics["state"]),fraction=fraction,quote=quote,lane=lane,role=role,regime=regime,risk=risk,lifecycle_progress=progress,
        threshold_challenger=progress>=0.85,candidate_lanes=lanes)


def install_continuation_market_recalibration() -> None:
    """Install the final paper-only continuation policy after v5.1/wallet authority."""
    global _INSTALLED,_ORIGINAL_SOLANA_BUY,_ORIGINAL_SOLANA_STATUS,_ORIGINAL_CANONICAL_WORKERS,_ORIGINAL_RH_V3,_ORIGINAL_RH_V2,_ORIGINAL_RH_FLOW
    if _INSTALLED:
        return
    current=FinalProfitFirstResearchAdapter._buy
    if not bool(getattr(current,"_roi_strategy_candidate_admission",False)):
        raise RuntimeError("continuation recalibration requires final strategy admission composition")
    _ORIGINAL_SOLANA_BUY=current
    FinalProfitFirstResearchAdapter._buy=_buy_with_continuation_recalibration  # type: ignore[method-assign]
    _ORIGINAL_SOLANA_STATUS=FinalProfitFirstResearchAdapter.status
    FinalProfitFirstResearchAdapter.status=_status_with_continuation_recalibration  # type: ignore[method-assign]

    admission.strategy_evaluation_eligible=continuation_strategy_evaluation_eligible
    admission.ENTRY_WINDOW_SECONDS=float("inf")
    v5.chase_band=continuation_chase_band
    v5.latency_band=continuation_latency_band

    _ORIGINAL_CANONICAL_WORKERS=canonical_isolation._ORIGINAL_RUNTIME_WORKERS
    if _ORIGINAL_CANONICAL_WORKERS is not None:
        canonical_isolation._ORIGINAL_RUNTIME_WORKERS=_canonical_workers_with_independent_fomo

    _ORIGINAL_RH_FLOW=RobinhoodProfitMaximizerMixin._v5_flow_metrics
    _ORIGINAL_RH_V3=RobinhoodProfitMaximizerMixin._maybe_open_v3
    _ORIGINAL_RH_V2=RobinhoodProfitMaximizerMixin._maybe_open_v2
    RobinhoodProfitMaximizerMixin._v5_flow_metrics=_rh_flow_without_sniper_cap  # type: ignore[method-assign]
    RobinhoodProfitMaximizerMixin._maybe_open_v3=_rh_v3_continuation  # type: ignore[method-assign]
    RobinhoodProfitMaximizerMixin._maybe_open_v2=_rh_v2_continuation  # type: ignore[method-assign]
    _INSTALLED=True


__all__=[
    "RECALIBRATION_VERSION","continuation_chase_band","continuation_latency_band","continuation_strategy_evaluation_eligible",
    "install_continuation_market_recalibration",
]
