from __future__ import annotations

import asyncio
import json
import math
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from .config import BASELINE
from .strategy_v52_authority import target_sizing_policy
from .v52_wallet_forward_alpha import (
    HORIZON_SECONDS,
    ReplayComparisonObservation,
    STATUS_INCOMPLETE,
    VALIDATION_WINDOWS,
    WalletForwardAlphaEngine,
    WalletForwardOutcome,
    WalletForwardValidationReport,
    WalletIntegritySnapshot,
    WalletPointInTimeObservation,
)
from .v52_wallet_forward_retention import prune_replay_history, should_persist_validation

RUNTIME_VERSION = "v52-wallet-forward-alpha-runtime-v1"
REFERENCE_PORTFOLIO_USD = 500.0
FIXED_HORIZONS = {key: seconds for key, seconds in HORIZON_SECONDS.items() if seconds is not None}
WALLET_DERIVED_LANES = frozenset({"elite_wallet_continuation"})
_CAPTURE_INTERVAL_SECONDS = 1.0
_VALIDATION_INTERVAL_SECONDS = 60.0
_MARK_TOLERANCE_SECONDS = 12.0

_RUNTIME: "WalletForwardAlphaRuntime | None" = None
_INSTALLED = False
_BASE_TRACKER_RECORD: Any = None
_BASE_TRACKER_RUN: Any = None


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _table_exists(store: Any, name: str) -> bool:
    try:
        with store._lock:
            row = store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _source_signature(pre: Mapping[str, Any]) -> str:
    for key in ("source_signature", "signature", "candidate_id"):
        value = pre.get(key)
        if value:
            return str(value)
    return ""


def _decision_time(pre: Mapping[str, Any]) -> datetime:
    for key in ("at", "received_at", "observed_at"):
        value = pre.get(key)
        if value:
            try:
                return _utc(value)
            except Exception:
                pass
    return _now()


def _capacity_from_liquidity(liquidity_usd: float) -> float:
    """Constant-product capacity at the existing per-side drag ceiling.

    Liquidity is treated as two-sided. The quote-side reserve is conservatively
    approximated as half total USD liquidity. The maximum size is the constant-
    product input that reaches, but does not exceed, the already-governed v3.1
    execution-drag-per-side ceiling. No new optimization threshold is introduced.
    """

    liquidity = max(0.0, float(liquidity_usd))
    drag = max(0.0, min(0.50, float(BASELINE.execution_drag_per_side_fraction)))
    if liquidity <= 0.0 or drag <= 0.0 or drag >= 1.0:
        return 0.0
    quote_reserve = liquidity * 0.5
    return quote_reserve * drag / (1.0 - drag)


def _portfolio_metrics(rows: list[dict[str, Any]], fraction_key: str) -> dict[str, Any]:
    capital = REFERENCE_PORTFOLIO_USD
    peak = capital
    max_drawdown = 0.0
    wins = 0
    losses = 0
    fees_slippage = 0.0
    largest_win = 0.0
    largest_loss = 0.0
    turnover = 0.0
    used = 0.0
    for row in sorted(rows, key=lambda item: (str(item["resolved_at"]), int(item["id"]))):
        fraction = max(0.0, min(1.0, float(row[fraction_key] or 0.0)))
        net_return = float(row["net_return"] or 0.0)
        gross_return = row.get("gross_return")
        deployed = capital * fraction
        pnl = deployed * net_return
        capital += pnl
        turnover += deployed
        used += fraction
        if pnl > 0:
            wins += 1
            largest_win = max(largest_win, pnl)
        elif pnl < 0:
            losses += 1
            largest_loss = min(largest_loss, pnl)
        if gross_return is not None:
            fees_slippage += max(0.0, deployed * (float(gross_return) - net_return))
        peak = max(peak, capital)
        if peak > 0:
            max_drawdown = max(max_drawdown, (peak - capital) / peak)
    return {
        "starting_capital_usd": REFERENCE_PORTFOLIO_USD,
        "ending_capital_usd": capital,
        "net_pnl_usd": capital - REFERENCE_PORTFOLIO_USD,
        "return_fraction": capital / REFERENCE_PORTFOLIO_USD - 1.0,
        "max_drawdown_fraction": max_drawdown,
        "trades": len(rows),
        "wins": wins,
        "losses": losses,
        "average_position_fraction": used / len(rows) if rows else 0.0,
        "average_position_usd_at_initial_nav": (used / len(rows) * REFERENCE_PORTFOLIO_USD) if rows else 0.0,
        "turnover_usd": turnover,
        "largest_win_usd": largest_win,
        "largest_loss_usd": largest_loss,
        "estimated_fees_slippage_usd": fees_slippage,
    }


class WalletForwardAlphaRuntime:
    def __init__(self, store: Any, runtime: Any, *, started_at: datetime | None = None) -> None:
        self.store = store
        self.runtime = runtime
        self.started_at = _utc(started_at or _now())
        self.engine = WalletForwardAlphaEngine(store)
        self.last_error: str | None = None
        self.last_capture_at: datetime | None = None
        self.last_validation_at: datetime | None = None
        self._schema()

    def _schema(self) -> None:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_wallet_forward_runtime_state ("
                "id INTEGER PRIMARY KEY CHECK(id=1), runtime_version TEXT NOT NULL, started_at TEXT NOT NULL, "
                "last_capture_at TEXT, last_validation_at TEXT, last_error TEXT, paper_only INTEGER NOT NULL, "
                "live_money_authority INTEGER NOT NULL)"
            )
            self.store.db.execute(
                "INSERT OR IGNORE INTO v52_wallet_forward_runtime_state("
                "id,runtime_version,started_at,paper_only,live_money_authority) VALUES (1,?,?,1,0)",
                (RUNTIME_VERSION, self.started_at.isoformat()),
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_wallet_forward_integrity_seen ("
                "signature TEXT PRIMARY KEY, recorded_at TEXT NOT NULL)"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_wallet_forward_shadow_decisions ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, candidate_id TEXT NOT NULL, source_signature TEXT NOT NULL, "
                "token_mint TEXT NOT NULL, wallet TEXT NOT NULL, lane TEXT NOT NULL, no_wallet_lane TEXT, "
                "context_key TEXT NOT NULL, observed_at TEXT NOT NULL, release_commit TEXT, "
                "current_target_fraction REAL NOT NULL, no_wallet_target_fraction REAL NOT NULL, "
                "wallet_forward_target_fraction REAL NOT NULL, wallet_forward_multiplier REAL NOT NULL, "
                "wallet_forward_score_json TEXT NOT NULL, current_eligible INTEGER NOT NULL, "
                "no_wallet_eligible INTEGER NOT NULL, wallet_forward_eligible INTEGER NOT NULL, "
                "same_stream INTEGER NOT NULL, lookahead_free INTEGER NOT NULL, paper_only INTEGER NOT NULL, "
                "live_money_authority INTEGER NOT NULL, UNIQUE(candidate_id,lane))"
            )
            self.store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_v52_wallet_forward_shadow_decision_time "
                "ON v52_wallet_forward_shadow_decisions(observed_at,id)"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_wallet_forward_shadow_outcomes ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, decision_id INTEGER NOT NULL UNIQUE, candidate_id TEXT NOT NULL, "
                "lane TEXT NOT NULL, resolved_at TEXT NOT NULL, net_return REAL NOT NULL, gross_return REAL, "
                "current_fraction REAL NOT NULL, no_wallet_fraction REAL NOT NULL, wallet_forward_fraction REAL NOT NULL, "
                "execution_realistic INTEGER NOT NULL, same_stream INTEGER NOT NULL, lookahead_free INTEGER NOT NULL, "
                "outcome_source TEXT NOT NULL, paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
            )
            self.store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_v52_wallet_forward_shadow_outcome_time "
                "ON v52_wallet_forward_shadow_outcomes(resolved_at,id)"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_wallet_forward_replay_runs ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, evaluated_at TEXT NOT NULL, runtime_started_at TEXT NOT NULL, "
                "status TEXT NOT NULL, strategy_influence_enabled INTEGER NOT NULL, report_json TEXT NOT NULL, "
                "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
            )

    def _liquidity_as_of(self, token: str, as_of: datetime) -> tuple[float, dict[str, Any]]:
        try:
            raw = self.store.latest_risk_evidence(
                token, "liquidity", as_of_received_at=_utc(as_of).isoformat()
            )
        except Exception:
            raw = None
        if raw is None:
            return 0.0, {"available": False, "reason": "point_in_time_liquidity_missing"}
        payload = dict(raw.get("payload") or {})
        try:
            liquidity = max(0.0, float(payload.get("liquidity_usd") or 0.0))
        except (TypeError, ValueError):
            liquidity = 0.0
        return liquidity, {
            "available": liquidity > 0.0,
            "observed_at": str(raw.get("observed_at") or ""),
            "received_at": str(raw.get("received_at") or ""),
            "source": str(raw.get("source") or ""),
            "capacity_model": "constant_product_half_liquidity_at_existing_execution_drag_ceiling",
        }

    def _entity_at(self, wallet: str, as_of: datetime) -> tuple[str | None, dict[str, Any]]:
        discovery = getattr(self.runtime, "wallet_discovery", None)
        inner = getattr(discovery, "_inner", None) if discovery is not None else None
        owner = inner or discovery
        resolver = getattr(owner, "entity_resolver", None)
        if resolver is None:
            return None, {"available": False}
        try:
            entity = resolver.entity_id_for(wallet, fallback_entity_id=None, as_of=as_of)
            component = sorted(resolver.component(wallet, as_of=as_of))
            return str(entity), {"available": True, "component": component}
        except Exception:
            return None, {"available": False}

    def capture_initial_observation(self, tracker: Any, swap: Any) -> bool:
        if str(getattr(swap, "side", "")).lower() != "buy":
            return False
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT * FROM wallet_discovery_forward_observations WHERE signature=? LIMIT 1",
                (str(swap.signature),),
            ).fetchone()
        if row is None:
            return False
        item = dict(row)
        detected = _utc(item["received_at"])
        chain = _utc(item["observed_at"])
        liquidity, liquidity_meta = self._liquidity_as_of(str(item["token_mint"]), detected)
        entity, relationship = self._entity_at(str(item["wallet"]), detected)
        executable = float(item.get("copyable_price_sol") or 0.0)
        if executable <= 0.0:
            executable = float(item.get("wallet_price_sol") or 0.0)
        observation = WalletPointInTimeObservation(
            wallet=str(item["wallet"]),
            context_key="*",
            candidate_id=str(item["signature"]),
            token_mint=str(item["token_mint"]),
            transaction_signature=str(item["signature"]),
            chain_timestamp=chain,
            first_observable_at=detected,
            detected_at=detected,
            lifecycle="unknown_at_transport_boundary",
            graduation_state="unknown_at_transport_boundary",
            observed_price=float(item["wallet_price_sol"]),
            earliest_executable_price=executable,
            liquidity_usd=liquidity,
            slippage_fraction=float(BASELINE.execution_drag_per_side_fraction),
            fee_fraction=0.0,
            market_impact_fraction=0.0,
            max_executable_usd=_capacity_from_liquidity(liquidity),
            entity_id=entity,
            relationships={
                **relationship,
                "liquidity": liquidity_meta,
                "cost_provenance": "frozen_execution_drag_per_side_inclusive",
            },
            integrity_known={
                "risk_complete": bool(item.get("risk_complete")),
                "manipulation_flag": bool(item.get("manipulation_flag")),
                "side_wallet_flag": bool(item.get("side_wallet_flag")),
            },
            wallet_statistics_known={
                "observation_lag_ms": float(item.get("observation_lag_ms") or 0.0),
                "processing_delay_ms": float(item.get("processing_delay_ms") or 0.0),
                "copyable": bool(item.get("copyable")),
                "future_outcomes_included": False,
            },
            v52_candidate_state="transport_observed",
            v52_decision_state="not_yet_evaluated",
            could_enter=False,
            entry_blocker="v52_decision_not_yet_observed",
        )
        return self.engine.record_observation(observation)

    def _copy_context_observation(
        self,
        *,
        wallet: str,
        context_key: str,
        candidate_id: str,
        decision_at: datetime,
        lane: str,
        could_enter: bool,
    ) -> None:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT * FROM v52_wallet_point_in_time_observations "
                "WHERE wallet=? AND context_key='*' AND candidate_id=? LIMIT 1",
                (wallet, candidate_id),
            ).fetchone()
        if row is None:
            return
        source = dict(row)
        liquidity, liquidity_meta = self._liquidity_as_of(str(source["token_mint"]), decision_at)
        entity, relationship = self._entity_at(wallet, decision_at)
        observation = WalletPointInTimeObservation(
            wallet=wallet,
            context_key=context_key,
            candidate_id=candidate_id,
            token_mint=str(source["token_mint"]),
            transaction_signature=str(source["transaction_signature"]),
            chain_timestamp=_utc(source["chain_timestamp"]),
            first_observable_at=_utc(source["first_observable_at"]),
            detected_at=decision_at,
            lifecycle=str(context_key.split("|")[2] if len(context_key.split("|")) > 2 else "unknown"),
            graduation_state="decision_context",
            observed_price=float(source["observed_price"]),
            earliest_executable_price=float(source["earliest_executable_price"]),
            liquidity_usd=liquidity,
            slippage_fraction=float(BASELINE.execution_drag_per_side_fraction),
            fee_fraction=0.0,
            market_impact_fraction=0.0,
            max_executable_usd=_capacity_from_liquidity(liquidity),
            entity_id=entity,
            relationships={
                **relationship,
                "liquidity": liquidity_meta,
                "context_first_known_at": decision_at.isoformat(),
                "cost_provenance": "frozen_execution_drag_per_side_inclusive",
            },
            integrity_known={"decision_context_point_in_time": True},
            wallet_statistics_known={"future_outcomes_included": False},
            v52_candidate_state="eligible" if could_enter else "not_targeted",
            v52_decision_state="target_positive" if could_enter else "no_target",
            could_enter=could_enter,
            entry_blocker=None if could_enter else "v52_baseline_no_positive_target",
        )
        self.engine.record_observation(observation)

    def research_profile(
        self,
        *,
        wallet: str,
        context_key: str,
        as_of: datetime,
        lane: str,
    ) -> dict[str, Any]:
        if not wallet:
            return {"available": False, "sizing_multiplier": 1.0, "reason": "wallet_missing"}
        score = self.engine.score(wallet, context_key, horizon="60s", as_of=as_of, reference_capital_usd=REFERENCE_PORTFOLIO_USD)
        selected_context = context_key
        if score.observations == 0:
            fallback = self.engine.score(wallet, "*", horizon="60s", as_of=as_of, reference_capital_usd=REFERENCE_PORTFOLIO_USD)
            if fallback.observations > 0:
                score = fallback
                selected_context = "*"
        multiplier = 1.0
        if score.eligible_for_strategy_influence:
            multiplier = 1.0 + max(-0.20, min(0.10, score.shrunk_expected_marginal_alpha * score.confidence))
        return {
            "available": score.observations > 0,
            "selected_context": selected_context,
            "score": asdict(score),
            "sizing_multiplier": max(0.80, min(1.10, multiplier)),
            "research_only": True,
            "global_production_validation_gate_bypassed": False,
            "may_create_eligibility": False,
            "paper_only": True,
        }

    def record_shadow_decision(
        self,
        *,
        adapter: Any,
        pre: Mapping[str, Any],
        lane: str | None,
        current_target: float,
        no_wallet_lane: str | None,
        no_wallet_target: float,
        context_key: str,
    ) -> dict[str, Any]:
        candidate_id = _source_signature(pre)
        wallet = str(pre.get("wallet") or "")
        token = str(pre.get("token") or pre.get("token_mint") or "")
        if not candidate_id or not token:
            return {"recorded": False, "reason": "candidate_identity_missing"}
        at = _decision_time(pre)
        self._copy_context_observation(
            wallet=wallet,
            context_key=context_key,
            candidate_id=candidate_id,
            decision_at=at,
            lane=str(lane or "none"),
            could_enter=bool(lane and current_target > 0.0),
        )
        research = self.research_profile(wallet=wallet, context_key=context_key, as_of=at, lane=str(lane or "none"))
        multiplier = float(research.get("sizing_multiplier") or 1.0)
        wallet_forward_target = max(0.0, float(current_target)) * multiplier
        release = str(getattr(adapter, "release_commit", "") or "")
        payload = json.dumps(research, sort_keys=True, default=str)
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT INTO v52_wallet_forward_shadow_decisions("
                "candidate_id,source_signature,token_mint,wallet,lane,no_wallet_lane,context_key,observed_at,release_commit,"
                "current_target_fraction,no_wallet_target_fraction,wallet_forward_target_fraction,wallet_forward_multiplier,"
                "wallet_forward_score_json,current_eligible,no_wallet_eligible,wallet_forward_eligible,same_stream,lookahead_free,"
                "paper_only,live_money_authority) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0) "
                "ON CONFLICT(candidate_id,lane) DO NOTHING",
                (
                    candidate_id,
                    candidate_id,
                    token,
                    wallet,
                    str(lane or "none"),
                    str(no_wallet_lane) if no_wallet_lane else None,
                    context_key,
                    at.isoformat(),
                    release,
                    max(0.0, float(current_target)),
                    max(0.0, float(no_wallet_target)),
                    wallet_forward_target,
                    multiplier,
                    payload,
                    1 if lane and current_target > 0.0 else 0,
                    1 if no_wallet_lane and no_wallet_target > 0.0 else 0,
                    1 if lane and wallet_forward_target > 0.0 else 0,
                    1,
                    1,
                ),
            )
        return {"recorded": True, "research": research, "wallet_forward_target": wallet_forward_target}

    def _sync_integrity(self, limit: int = 25) -> int:
        if not _table_exists(self.store, "wallet_discovery_forward_observations"):
            return 0
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT o.signature,o.wallet,o.received_at,o.manipulation_flag,o.side_wallet_flag "
                "FROM wallet_discovery_forward_observations o "
                "LEFT JOIN v52_wallet_forward_integrity_seen s ON s.signature=o.signature "
                "WHERE o.side='buy' AND o.risk_complete=1 AND o.received_at>=? AND s.signature IS NULL "
                "ORDER BY o.received_at LIMIT ?",
                (self.started_at.isoformat(), max(1, int(limit))),
            ).fetchall()
        inserted = 0
        for raw in rows:
            item = dict(raw)
            at = _now()
            suspicious = bool(item["manipulation_flag"]) or bool(item["side_wallet_flag"])
            reasons: list[str] = []
            if bool(item["manipulation_flag"]):
                reasons.append("point_in_time_manipulation_flag")
            if bool(item["side_wallet_flag"]):
                reasons.append("point_in_time_linked_wallet_flag")
            entity, relation = self._entity_at(str(item["wallet"]), at)
            if relation.get("available") and len(relation.get("component") or ()) > 1:
                reasons.append("multi_wallet_entity_component")
            self.engine.record_integrity(
                WalletIntegritySnapshot(
                    wallet=str(item["wallet"]),
                    observed_at=at,
                    integrity_score=0.0 if suspicious else 1.0,
                    suspicious=suspicious,
                    creator_associated=False,
                    common_funder_cluster=entity if suspicious and entity else None,
                    reasons=tuple(reasons),
                )
            )
            with self.store._lock, self.store.db:
                self.store.db.execute(
                    "INSERT OR IGNORE INTO v52_wallet_forward_integrity_seen(signature,recorded_at) VALUES (?,?)",
                    (str(item["signature"]), at.isoformat()),
                )
            inserted += 1
        return inserted

    def _matched_decision(self, candidate_id: str, context_key: str, as_of: datetime) -> dict[str, Any] | None:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT * FROM v52_wallet_forward_shadow_decisions WHERE candidate_id=? AND context_key=? "
                "AND observed_at<=? ORDER BY id DESC LIMIT 1",
                (candidate_id, context_key, as_of.isoformat()),
            ).fetchone()
        return dict(row) if row is not None else None

    def _mature_fixed_horizons(self, limit: int = 25) -> int:
        if not _table_exists(self.store, "price_marks"):
            return 0
        now = _now()
        matured = 0
        with self.store._lock:
            observations = self.store.db.execute(
                "SELECT * FROM v52_wallet_point_in_time_observations WHERE detected_at>=? "
                "ORDER BY detected_at,id LIMIT 250",
                (self.started_at.isoformat(),),
            ).fetchall()
        drag = float(BASELINE.execution_drag_per_side_fraction)
        for raw in observations:
            if matured >= limit:
                break
            obs = dict(raw)
            detected = _utc(obs["detected_at"])
            for horizon, seconds in FIXED_HORIZONS.items():
                due = detected + timedelta(seconds=int(seconds))
                if now < due:
                    continue
                with self.store._lock:
                    exists = self.store.db.execute(
                        "SELECT 1 FROM v52_wallet_forward_outcomes WHERE wallet=? AND context_key=? "
                        "AND candidate_id=? AND horizon=? LIMIT 1",
                        (obs["wallet"], obs["context_key"], obs["candidate_id"], horizon),
                    ).fetchone()
                    mark = self.store.db.execute(
                        "SELECT observed_at,received_at,price_sol FROM price_marks WHERE token_mint=? "
                        "AND received_at>=? AND received_at<=? ORDER BY received_at,id LIMIT 1",
                        (
                            obs["token_mint"],
                            due.isoformat(),
                            (due + timedelta(seconds=_MARK_TOLERANCE_SECONDS)).isoformat(),
                        ),
                    ).fetchone()
                if exists is not None or mark is None:
                    continue
                available = _utc(mark["received_at"])
                decision = self._matched_decision(str(obs["candidate_id"]), str(obs["context_key"]), available)
                if decision is None:
                    continue
                entry = float(obs["earliest_executable_price"])
                exit_price = float(mark["price_sol"])
                if entry <= 0.0 or exit_price <= 0.0:
                    continue
                gross = exit_price / entry - 1.0
                net = (exit_price * (1.0 - drag)) / (entry * (1.0 + drag)) - 1.0
                with self.store._lock:
                    path = self.store.db.execute(
                        "SELECT price_sol FROM price_marks WHERE token_mint=? AND received_at>=? AND received_at<=? ORDER BY received_at,id",
                        (obs["token_mint"], detected.isoformat(), available.isoformat()),
                    ).fetchall()
                returns = [float(row["price_sol"]) / entry - 1.0 for row in path if float(row["price_sol"] or 0.0) > 0.0]
                mfe = max([0.0, *returns])
                mae = max([0.0, *(-value for value in returns if value < 0.0)])
                control = net if float(decision["no_wallet_target_fraction"] or 0.0) > 0.0 else 0.0
                self.engine.record_forward_outcome(
                    WalletForwardOutcome(
                        wallet=str(obs["wallet"]),
                        context_key=str(obs["context_key"]),
                        candidate_id=str(obs["candidate_id"]),
                        horizon=horizon,
                        available_at=available,
                        gross_return=gross,
                        net_executable_return=net,
                        matched_control_net_return=control,
                        exit_price=exit_price,
                        exit_liquidity_usd=float(obs["liquidity_usd"] or 0.0),
                        exit_capacity_usd=float(obs["max_executable_usd"] or 0.0),
                        max_favorable_excursion=mfe,
                        max_adverse_excursion=mae,
                        copyable=bool(float(obs["max_executable_usd"] or 0.0) > 0.0),
                    )
                )
                matured += 1
        return matured

    def _reconcile_shadow_outcomes(self, limit: int = 50) -> int:
        with self.store._lock:
            decisions = self.store.db.execute(
                "SELECT d.* FROM v52_wallet_forward_shadow_decisions d "
                "LEFT JOIN v52_wallet_forward_shadow_outcomes o ON o.decision_id=d.id "
                "WHERE o.id IS NULL ORDER BY d.id LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
        inserted = 0
        for raw in decisions:
            d = dict(raw)
            source = str(d["source_signature"])
            lane = str(d["lane"])
            event = None
            if _table_exists(self.store, "v52_profit_signal_events"):
                with self.store._lock:
                    event = self.store.db.execute(
                        "SELECT closed_at,realized_net_return,realized_mfe,realized_mae,position_fraction "
                        "FROM v52_profit_signal_events WHERE source_signature=? AND closed_at IS NOT NULL "
                        "AND realized_net_return IS NOT NULL ORDER BY id DESC LIMIT 1",
                        (source,),
                    ).fetchone()
            execution_realistic = event is not None
            outcome_source = "v52_realized_paper_outcome"
            position_fraction = None
            if event is not None:
                net_return = float(event["realized_net_return"])
                resolved_at = _utc(event["closed_at"])
                mfe = float(event["realized_mfe"] or 0.0)
                mae = float(event["realized_mae"] or 0.0)
                position_fraction = float(event["position_fraction"] or 0.0)
            else:
                cf = None
                if _table_exists(self.store, "v52_counterfactual_decisions"):
                    with self.store._lock:
                        cf = self.store.db.execute(
                            "SELECT resolved_at,net_return,executable_mfe,executable_mae,hypothetical_fraction,analytical_only "
                            "FROM v52_counterfactual_decisions WHERE source_signature=? AND resolved_at IS NOT NULL "
                            "AND net_return IS NOT NULL ORDER BY id DESC LIMIT 1",
                            (source,),
                        ).fetchone()
                if cf is None:
                    continue
                net_return = float(cf["net_return"])
                resolved_at = _utc(cf["resolved_at"])
                mfe = float(cf["executable_mfe"] or 0.0)
                mae = float(cf["executable_mae"] or 0.0)
                position_fraction = float(cf["hypothetical_fraction"] or 0.0)
                execution_realistic = not bool(cf["analytical_only"])
                outcome_source = "v52_resolved_counterfactual"
            current_target = float(d["current_target_fraction"] or 0.0)
            scale = (position_fraction / current_target) if current_target > 0.0 and position_fraction is not None else 1.0
            current_fraction = max(0.0, float(position_fraction or 0.0))
            no_wallet_fraction = max(0.0, min(1.0, float(d["no_wallet_target_fraction"] or 0.0) * scale))
            wfa_fraction = max(0.0, min(1.0, float(d["wallet_forward_target_fraction"] or 0.0) * scale))
            with self.store._lock, self.store.db:
                cursor = self.store.db.execute(
                    "INSERT OR IGNORE INTO v52_wallet_forward_shadow_outcomes("
                    "decision_id,candidate_id,lane,resolved_at,net_return,gross_return,current_fraction,no_wallet_fraction,"
                    "wallet_forward_fraction,execution_realistic,same_stream,lookahead_free,outcome_source,paper_only,live_money_authority"
                    ") VALUES (?,?,?,?,?,NULL,?,?,?,?,?,?,?,1,0)",
                    (
                        int(d["id"]),
                        str(d["candidate_id"]),
                        lane,
                        resolved_at.isoformat(),
                        net_return,
                        current_fraction,
                        no_wallet_fraction,
                        wfa_fraction,
                        1 if execution_realistic else 0,
                        1,
                        1,
                        outcome_source,
                    ),
                )
            if cursor.rowcount != 1:
                continue
            inserted += 1
            with self.store._lock:
                obs = self.store.db.execute(
                    "SELECT * FROM v52_wallet_point_in_time_observations WHERE wallet=? AND context_key=? "
                    "AND candidate_id=? LIMIT 1",
                    (d["wallet"], d["context_key"], d["candidate_id"]),
                ).fetchone()
            if obs is not None:
                o = dict(obs)
                control = net_return if no_wallet_fraction > 0.0 else 0.0
                try:
                    self.engine.record_forward_outcome(
                        WalletForwardOutcome(
                            wallet=str(d["wallet"]),
                            context_key=str(d["context_key"]),
                            candidate_id=str(d["candidate_id"]),
                            horizon="v52_exit",
                            available_at=resolved_at,
                            gross_return=net_return,
                            net_executable_return=net_return,
                            matched_control_net_return=control,
                            exit_price=max(1e-12, float(o["earliest_executable_price"]) * (1.0 + net_return)),
                            exit_liquidity_usd=float(o["liquidity_usd"] or 0.0),
                            exit_capacity_usd=float(o["max_executable_usd"] or 0.0),
                            max_favorable_excursion=mfe,
                            max_adverse_excursion=mae,
                            copyable=execution_realistic,
                        )
                    )
                except Exception:
                    pass
        return inserted

    def _window_rows(self, window: str, now: datetime) -> list[dict[str, Any]]:
        hours = {"24h": 24, "7d": 24 * 7, "30d": 24 * 30}[window]
        cutoff = now - timedelta(hours=hours)
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT o.*,d.observed_at,d.wallet_forward_multiplier FROM v52_wallet_forward_shadow_outcomes o "
                "JOIN v52_wallet_forward_shadow_decisions d ON d.id=o.decision_id "
                "WHERE d.observed_at>=? AND o.resolved_at<=? ORDER BY o.resolved_at,o.id",
                (cutoff.isoformat(), now.isoformat()),
            ).fetchall()
        return [dict(row) for row in rows]

    def run_real_validation(self, *, as_of: datetime | None = None, persist_if_complete: bool = True) -> dict[str, Any]:
        now = _utc(as_of or _now())
        replay_rows: list[ReplayComparisonObservation] = []
        portfolio: dict[str, Any] = {}
        coverage_blockers: list[str] = []
        minimum = int(target_sizing_policy()["minimum_forward_samples"])
        for window in VALIDATION_WINDOWS:
            hours = {"24h": 24, "7d": 168, "30d": 720}[window]
            rows = self._window_rows(window, now)
            baseline_metrics = _portfolio_metrics(rows, "no_wallet_fraction")
            current_metrics = _portfolio_metrics(rows, "current_fraction")
            forward_metrics = _portfolio_metrics(rows, "wallet_forward_fraction")
            portfolio[window] = {
                "paired_same_stream_rows": len(rows),
                "execution_realistic_rows": sum(bool(row["execution_realistic"]) for row in rows),
                "baseline_v52_no_wallet": baseline_metrics,
                "current_v52_wallet": current_metrics,
                "wallet_forward_alpha": forward_metrics,
                "wallet_intelligence_attributable_pnl_usd": current_metrics["net_pnl_usd"] - baseline_metrics["net_pnl_usd"],
                "wallet_forward_alpha_attributable_pnl_usd": forward_metrics["net_pnl_usd"] - current_metrics["net_pnl_usd"],
                "false_positive_wallet_signals": sum(
                    1 for row in rows
                    if float(row["wallet_forward_fraction"] or 0.0) > float(row["current_fraction"] or 0.0)
                    and float(row["net_return"] or 0.0) < 0.0
                ),
                "negative_alpha_losses_avoided": sum(
                    1 for row in rows
                    if float(row["wallet_forward_fraction"] or 0.0) < float(row["current_fraction"] or 0.0)
                    and float(row["net_return"] or 0.0) < 0.0
                ),
            }
            if now - self.started_at < timedelta(hours=hours):
                coverage_blockers.append(f"{window}_prospective_runtime_window_not_complete")
            if len(rows) < minimum:
                coverage_blockers.append(f"{window}_insufficient_paired_same_stream_rows")
            bdd = float(baseline_metrics["max_drawdown_fraction"])
            cdd = float(current_metrics["max_drawdown_fraction"])
            fdd = float(forward_metrics["max_drawdown_fraction"])
            for row in rows:
                replay_rows.append(
                    ReplayComparisonObservation(
                        window=window,
                        candidate_id=str(row["candidate_id"]),
                        baseline_v52_return=float(row["no_wallet_fraction"] or 0.0) * float(row["net_return"] or 0.0),
                        current_wallet_return=float(row["current_fraction"] or 0.0) * float(row["net_return"] or 0.0),
                        wallet_forward_alpha_return=float(row["wallet_forward_fraction"] or 0.0) * float(row["net_return"] or 0.0),
                        baseline_drawdown=bdd,
                        current_wallet_drawdown=cdd,
                        wallet_forward_alpha_drawdown=fdd,
                        lookahead_free=bool(row["lookahead_free"]),
                        execution_realistic=bool(row["execution_realistic"]),
                    )
                )
        base_report = WalletForwardAlphaEngine.evaluate_replay(replay_rows)
        reasons = tuple(dict.fromkeys([*coverage_blockers, *base_report.reasons]))
        full_coverage = not coverage_blockers
        report = WalletForwardValidationReport(
            status=base_report.status if full_coverage else STATUS_INCOMPLETE,
            windows=base_report.windows,
            strategy_influence_enabled=bool(full_coverage and base_report.strategy_influence_enabled),
            influence_scope=base_report.influence_scope if full_coverage else (),
            reasons=reasons,
        )
        payload = {
            "runtime_version": RUNTIME_VERSION,
            "evaluated_at": now.isoformat(),
            "runtime_started_at": self.started_at.isoformat(),
            "runtime_age_hours": max(0.0, (now - self.started_at).total_seconds() / 3600.0),
            "reference_portfolio_usd": REFERENCE_PORTFOLIO_USD,
            "validation": asdict(report),
            "portfolio": portfolio,
            "comparison_definition": {
                "baseline_v52_no_wallet": "same opportunity with elite_wallet_continuation removed before v5.2 target selection",
                "current_v52_wallet": "authoritative current v5.2 target and realized/counterfactual outcome",
                "wallet_forward_alpha": "current v5.2 target multiplied only by contemporaneous prior Wallet Forward Alpha score, bounded 0.80-1.10",
                "same_opportunity_stream": True,
                "point_in_time": True,
                "future_outcomes_available_at_or_before_decision_only": True,
            },
            "acceptance_decision": report.status,
            "strategy_influence_enabled": report.strategy_influence_enabled,
            "paper_only": True,
            "live_money_authority": False,
        }
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT INTO v52_wallet_forward_replay_runs("
                "evaluated_at,runtime_started_at,status,strategy_influence_enabled,report_json,paper_only,live_money_authority"
                ") VALUES (?,?,?,?,?,1,0)",
                (now.isoformat(), self.started_at.isoformat(), report.status, 1 if report.strategy_influence_enabled else 0, json.dumps(payload, sort_keys=True, default=str)),
            )
        prune_replay_history(self.store)
        if persist_if_complete and full_coverage and should_persist_validation(self.store, report, now):
            self.engine.persist_validation(report, evaluated_at=now)
        self.last_validation_at = now
        return payload

    async def run(self, tracker: Any, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                self._sync_integrity()
                self._mature_fixed_horizons()
                self._reconcile_shadow_outcomes()
                now = _now()
                if self.last_validation_at is None or (now - self.last_validation_at).total_seconds() >= _VALIDATION_INTERVAL_SECONDS:
                    self.run_real_validation(as_of=now)
                self.last_capture_at = now
                self.last_error = None
                with self.store._lock, self.store.db:
                    self.store.db.execute(
                        "UPDATE v52_wallet_forward_runtime_state SET last_capture_at=?,last_validation_at=?,last_error=NULL WHERE id=1",
                        (
                            now.isoformat(),
                            self.last_validation_at.isoformat() if self.last_validation_at else None,
                        ),
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = f"{type(exc).__name__}:{exc}"
                with self.store._lock, self.store.db:
                    self.store.db.execute(
                        "UPDATE v52_wallet_forward_runtime_state SET last_error=? WHERE id=1", (self.last_error,)
                    )
            try:
                await asyncio.wait_for(stop.wait(), timeout=_CAPTURE_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                continue

    def status(self) -> dict[str, Any]:
        engine = self.engine.status()
        now = _now()
        with self.store._lock:
            decisions = int(self.store.db.execute("SELECT COUNT(*) FROM v52_wallet_forward_shadow_decisions").fetchone()[0])
            outcomes = int(self.store.db.execute("SELECT COUNT(*) FROM v52_wallet_forward_shadow_outcomes").fetchone()[0])
            latest = self.store.db.execute(
                "SELECT report_json FROM v52_wallet_forward_replay_runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
        report = json.loads(str(latest["report_json"])) if latest is not None else self.run_real_validation(as_of=now, persist_if_complete=False)
        return {
            "installed": True,
            "runtime_version": RUNTIME_VERSION,
            "runtime_started_at": self.started_at.isoformat(),
            "runtime_age_hours": max(0.0, (now - self.started_at).total_seconds() / 3600.0),
            "shadow_decisions": decisions,
            "shadow_outcomes": outcomes,
            "engine": engine,
            "real_three_way_replay": report,
            "automatic_point_in_time_capture": True,
            "wallet_neutral_control_prospective_only": True,
            "historical_hindsight_backfill_allowed": False,
            "strategy_influence_enabled": bool(report.get("strategy_influence_enabled")),
            "last_capture_at": self.last_capture_at.isoformat() if self.last_capture_at else None,
            "last_validation_at": self.last_validation_at.isoformat() if self.last_validation_at else None,
            "last_error": self.last_error,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }


def runtime() -> WalletForwardAlphaRuntime:
    if _RUNTIME is None:
        raise RuntimeError("Wallet Forward Alpha runtime not installed")
    return _RUNTIME


def research_profile(*, store: Any, wallet: str, context_key: str, as_of: datetime, lane: str) -> dict[str, Any]:
    current = _RUNTIME
    if current is None or current.store is not store:
        return {"available": False, "sizing_multiplier": 1.0, "reason": "runtime_not_installed"}
    return current.research_profile(wallet=wallet, context_key=context_key, as_of=as_of, lane=lane)


def record_shadow_decision(**kwargs: Any) -> dict[str, Any]:
    current = _RUNTIME
    adapter = kwargs.get("adapter")
    if current is None or adapter is None or current.store is not getattr(adapter, "store", None):
        return {"recorded": False, "reason": "runtime_not_installed_for_store"}
    return current.record_shadow_decision(**kwargs)


async def _record_with_wallet_forward_alpha(self: Any, swap: Any) -> bool:
    if _BASE_TRACKER_RECORD is None:
        raise RuntimeError("wallet forward alpha realtime predecessor unavailable")
    inserted = await _BASE_TRACKER_RECORD(self, swap)
    current = _RUNTIME
    if inserted and current is not None and current.store is getattr(self, "store", None):
        try:
            current.capture_initial_observation(self, swap)
        except Exception as exc:
            current.last_error = f"capture_initial:{type(exc).__name__}:{exc}"
    return inserted


async def _run_with_wallet_forward_alpha(self: Any, stop: asyncio.Event) -> None:
    if _BASE_TRACKER_RUN is None:
        raise RuntimeError("wallet forward alpha realtime run predecessor unavailable")
    current = _RUNTIME
    task: asyncio.Task[None] | None = None
    if current is not None and current.store is getattr(self, "store", None):
        task = asyncio.create_task(current.run(self, stop), name="v52-wallet-forward-alpha-runtime")
    try:
        await _BASE_TRACKER_RUN(self, stop)
    finally:
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


setattr(_record_with_wallet_forward_alpha, "_roi_v52_wallet_forward_alpha_runtime", True)
setattr(_run_with_wallet_forward_alpha, "_roi_v52_wallet_forward_alpha_runtime", True)


def install_v52_wallet_forward_alpha_runtime(runtime_provider: Any) -> WalletForwardAlphaRuntime:
    global _RUNTIME, _INSTALLED, _BASE_TRACKER_RECORD, _BASE_TRACKER_RUN
    owner = runtime_provider() if callable(runtime_provider) else runtime_provider
    store = getattr(owner, "store", None)
    if store is None:
        raise RuntimeError("canonical store unavailable for Wallet Forward Alpha runtime")
    if _RUNTIME is None or _RUNTIME.store is not store:
        _RUNTIME = WalletForwardAlphaRuntime(store, owner)
    if _INSTALLED:
        return _RUNTIME
    from .wallet_realtime_tracking_repair import RealtimeWalletTracker

    current_record = RealtimeWalletTracker._record_quick_forward_swap
    current_run = RealtimeWalletTracker.run
    if not bool(getattr(current_record, "_roi_v52_wallet_forward_alpha_runtime", False)):
        _BASE_TRACKER_RECORD = current_record
        _record_with_wallet_forward_alpha.__dict__.update(getattr(current_record, "__dict__", {}))
        setattr(_record_with_wallet_forward_alpha, "_roi_v52_wallet_forward_alpha_runtime", True)
        RealtimeWalletTracker._record_quick_forward_swap = _record_with_wallet_forward_alpha  # type: ignore[method-assign]
    if not bool(getattr(current_run, "_roi_v52_wallet_forward_alpha_runtime", False)):
        _BASE_TRACKER_RUN = current_run
        _run_with_wallet_forward_alpha.__dict__.update(getattr(current_run, "__dict__", {}))
        setattr(_run_with_wallet_forward_alpha, "_roi_v52_wallet_forward_alpha_runtime", True)
        RealtimeWalletTracker.run = _run_with_wallet_forward_alpha  # type: ignore[method-assign]
    _INSTALLED = True
    return _RUNTIME


def status() -> dict[str, Any]:
    if _RUNTIME is None:
        return {
            "installed": False,
            "runtime_version": RUNTIME_VERSION,
            "strategy_influence_enabled": False,
            "acceptance_decision": STATUS_INCOMPLETE,
            "paper_only": True,
            "live_money_authority": False,
        }
    return _RUNTIME.status()


def report() -> dict[str, Any]:
    if _RUNTIME is None:
        return status()
    return _RUNTIME.run_real_validation(as_of=_now(), persist_if_complete=False)


__all__ = [
    "REFERENCE_PORTFOLIO_USD",
    "RUNTIME_VERSION",
    "WALLET_DERIVED_LANES",
    "WalletForwardAlphaRuntime",
    "install_v52_wallet_forward_alpha_runtime",
    "record_shadow_decision",
    "report",
    "research_profile",
    "runtime",
    "status",
]
