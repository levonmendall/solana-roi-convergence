from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from .observation import WSOL_MINT
from .profit_first_entity_final import (
    FINAL_STRATEGY_VERSION,
    PARENT_RESEARCH_VERSION,
    SIGNAL_DECAY_DELAYS_SECONDS,
    STARTING_PAPER_NAV_USD,
    UNIFIED_LANE,
    ExitFeatures,
    FinalForwardOutcome,
    FinalLane,
    FinalLaneContext,
    FinalOpportunity,
    FinalPolicy,
    FinalProfitFirstStrategy,
    MarketRegime,
    SignalDecayCurve,
    SizingConstraints,
    WalkForwardLedger,
    build_robustness_report,
)
from .profit_first_entity_research import ProfitFirstResearchAdapter
from .quote import LAMPORTS_PER_SOL
from .wallet_discovery import ContinuousWalletDiscovery


USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
_ORIGINAL_RECORD: Callable[..., Any] | None = None
_ORIGINAL_REALTIME_RECORD: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_ADAPTER_ATTR = "_roi_profit_first_entity_final_research"
_MAX_TASKS = 16


def _release_commit() -> str:
    for key in ("RENDER_GIT_COMMIT", "SOLANA_ROI_RELEASE_COMMIT", "GITHUB_SHA"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return "unbound-local-release"


def _dump(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _context(raw: str) -> FinalLaneContext:
    row = json.loads(raw)
    return FinalLaneContext(
        lane=FinalLane(row["lane"]),
        regime=MarketRegime(row["regime"]),
        creator_flow_state=str(row["creator_flow_state"]),
        confirmation_bin=str(row["confirmation_bin"]),
        chase_bin=str(row["chase_bin"]),
        latency_bin=str(row["latency_bin"]),
        early_exit_bin=str(row["early_exit_bin"]),
        soft_risk_bin=str(row["soft_risk_bin"]),
        creator_linked_trigger=bool(row["creator_linked_trigger"]),
    )


class FinalProfitFirstResearchAdapter:
    """Definitive paper-only prospective evidence collector for the final v4 strategy."""

    def __init__(self, discovery: ContinuousWalletDiscovery):
        self.discovery = discovery
        self.store = discovery.store
        self.release_commit = _release_commit()
        self.epoch_id = hashlib.sha256(
            f"{FINAL_STRATEGY_VERSION}|{self.release_commit}".encode()
        ).hexdigest()[:20]
        # Reuse the already-governed Jupiter unsigned quote/simulation plumbing without
        # scheduling the parent research strategy itself.
        self.execution = ProfitFirstResearchAdapter(discovery)
        self.ledger = WalkForwardLedger()
        self.strategy = FinalProfitFirstStrategy(ledger=self.ledger)
        self._tasks: set[asyncio.Task[None]] = set()
        self.backpressure_drops = 0
        self.last_observed_at: str | None = None
        self.last_error: str | None = None
        self._sol_usd_cache: tuple[float, float] | None = None
        self._schema()
        self._ensure_epoch()
        self._load_outcomes()

    def _manifest(self) -> dict[str, Any]:
        policy = self.strategy.policy
        return {
            "strategy_version": FINAL_STRATEGY_VERSION,
            "parent_research_version": PARENT_RESEARCH_VERSION,
            "source_release_commit": self.release_commit,
            "evidence_epoch_id": self.epoch_id,
            "objective": "maximize_out_of_sample_compounded_net_return_for_500_usd_paper_portfolio",
            "starting_paper_nav_usd": STARTING_PAPER_NAV_USD,
            "wallet_entity_universe": "continuous_wallet_discovery_point_in_time_entities",
            "signal_definitions": [lane.value for lane in FinalLane],
            "unified_lane": UNIFIED_LANE,
            "entry_logic": "first_system_observable_amount_specific_jupiter_quote_plus_unsigned_simulation",
            "exit_logic": "separate_shadow_exit_alpha_plus_trigger_wallet_baseline",
            "position_sizing_policy": {
                "grid": list(policy.position_fraction_grid),
                "objective": "maximize_E_log_1_plus_fR",
                "constraints": ["liquidity", "entity_concentration", "correlation", "sample_confidence"],
                "experiment_assignment": "deterministic_signature_rotation",
            },
            "entity_resolution": "append_only_point_in_time_existing_entity_graph",
            "creator_association_automatic_veto": False,
            "risk_features": [
                "creator_flow_state",
                "linked_entity_distribution",
                "early_buyer_distribution",
                "bundled_launch",
                "sniper_heavy",
                "liquidity_exit_capacity",
            ],
            "signal_decay_delays_seconds": list(SIGNAL_DECAY_DELAYS_SECONDS),
            "max_chase_fraction": policy.max_chase_fraction,
            "latency_certification_threshold_seconds": policy.max_certified_observation_latency_seconds,
            "continuity_lease_seconds": 12.0,
            "real_gap_recovery_bound": "3x1000",
            "rpc_workload": "research",
            "helius_required": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
            "historical_evidence_promotion_authority": False,
            "unified_forward_gate": policy.min_forward_outcomes_for_selection,
        }

    def _schema(self) -> None:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS profit_first_final_epochs ("
                "epoch_id TEXT PRIMARY KEY, strategy_version TEXT NOT NULL, release_commit TEXT NOT NULL, "
                "started_at TEXT NOT NULL, manifest_json TEXT NOT NULL, paper_only INTEGER NOT NULL, "
                "live_money_authority INTEGER NOT NULL, historical_promotion_authority INTEGER NOT NULL)"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS profit_first_final_trials ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_id TEXT NOT NULL, release_commit TEXT NOT NULL, "
                "strategy_version TEXT NOT NULL, source_signature TEXT NOT NULL, observation_group TEXT NOT NULL, "
                "token_mint TEXT NOT NULL, trigger_wallet TEXT NOT NULL, lane TEXT NOT NULL, observed_at TEXT NOT NULL, "
                "received_at TEXT NOT NULL, regime TEXT NOT NULL, opportunity_json TEXT NOT NULL, context_json TEXT, "
                "decision_json TEXT NOT NULL, assigned_position_fraction REAL NOT NULL, quote_input_lamports INTEGER, "
                "entry_fee_lamports INTEGER, entry_token_raw INTEGER, token_decimals INTEGER, entry_all_in_price_sol REAL, "
                "immediate_exit_net_sol REAL, round_trip_cost_fraction REAL, signal_to_entry_seconds REAL NOT NULL, "
                "quote_latency_ms REAL, entry_executable INTEGER NOT NULL, exit_executable INTEGER NOT NULL, created_at TEXT NOT NULL, "
                "UNIQUE(epoch_id, source_signature, lane))"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS profit_first_final_outcomes ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_id TEXT NOT NULL, release_commit TEXT NOT NULL, "
                "strategy_version TEXT NOT NULL, source_signature TEXT NOT NULL, exit_signature TEXT NOT NULL, "
                "token_mint TEXT NOT NULL, trigger_wallet TEXT NOT NULL, lane TEXT NOT NULL, context_json TEXT, "
                "entry_observed_at TEXT NOT NULL, exit_observed_at TEXT NOT NULL, signal_to_entry_seconds REAL NOT NULL, "
                "position_fraction REAL NOT NULL, entry_cost_sol REAL NOT NULL, exit_net_sol REAL NOT NULL, net_return REAL NOT NULL, "
                "evidence_phase TEXT NOT NULL, exit_reason TEXT NOT NULL, exit_features_json TEXT NOT NULL, created_at TEXT NOT NULL, "
                "UNIQUE(epoch_id, source_signature, lane))"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS profit_first_final_exit_signals ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_id TEXT NOT NULL, token_mint TEXT NOT NULL, "
                "source_signature TEXT NOT NULL, seller_wallet TEXT NOT NULL, observed_at TEXT NOT NULL, "
                "features_json TEXT NOT NULL, signal_json TEXT NOT NULL, created_at TEXT NOT NULL, "
                "UNIQUE(epoch_id, source_signature, seller_wallet))"
            )
            self.store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_profit_first_final_outcomes_lane ON "
                "profit_first_final_outcomes(epoch_id,lane,id)"
            )
            self.store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_profit_first_final_trials_token ON "
                "profit_first_final_trials(epoch_id,token_mint,id)"
            )

    def _ensure_epoch(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT OR IGNORE INTO profit_first_final_epochs("
                "epoch_id,strategy_version,release_commit,started_at,manifest_json,paper_only,live_money_authority,historical_promotion_authority) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (self.epoch_id, FINAL_STRATEGY_VERSION, self.release_commit, now, _dump(self._manifest()), 1, 0, 0),
            )

    def _load_outcomes(self) -> None:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT context_json,net_return,source_signature,release_commit,entry_observed_at,signal_to_entry_seconds,"
                "position_fraction,evidence_phase,exit_reason FROM profit_first_final_outcomes "
                "WHERE epoch_id=? AND lane<>? AND context_json IS NOT NULL ORDER BY id",
                (self.epoch_id, UNIFIED_LANE),
            ).fetchall()
        for row in rows:
            try:
                self.ledger.add(
                    FinalForwardOutcome(
                        context=_context(str(row["context_json"])),
                        net_return=float(row["net_return"]),
                        source_signature=str(row["source_signature"]),
                        release_commit=str(row["release_commit"]),
                        observed_at=str(row["entry_observed_at"]),
                        signal_to_entry_seconds=float(row["signal_to_entry_seconds"]),
                        position_fraction=float(row["position_fraction"]),
                        evidence_phase=str(row["evidence_phase"]),
                        exit_reason=str(row["exit_reason"]),
                    )
                )
            except Exception:
                continue

    def schedule(self, signature: str) -> None:
        if not signature:
            return
        if len(self._tasks) >= _MAX_TASKS:
            self.backpressure_drops += 1
            self.last_error = "final strategy research task bound reached; sample skipped fail-closed"
            return
        task = asyncio.create_task(self.observe(signature), name="profit-first-entity-final-research")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _assigned_fraction(self, signature: str) -> float:
        grid = self.strategy.policy.position_fraction_grid
        index = int(hashlib.sha256(signature.encode()).hexdigest()[:8], 16) % len(grid)
        return float(grid[index])

    async def _sol_usd(self) -> float | None:
        now = time.monotonic()
        if self._sol_usd_cache is not None and now - self._sol_usd_cache[0] <= 60.0:
            return self._sol_usd_cache[1]
        route = await self.execution._route(WSOL_MINT, USDC_MINT, LAMPORTS_PER_SOL)
        if route is None or int(route.get("out_amount") or 0) <= 0:
            return None
        value = int(route["out_amount"]) / 1_000_000.0
        if value <= 0:
            return None
        self._sol_usd_cache = (now, value)
        return value

    async def _execution(self, row: dict[str, Any], fraction: float) -> dict[str, Any] | None:
        observed = datetime.fromisoformat(str(row["observed_at"]))
        started = time.perf_counter()
        sol_usd = await self._sol_usd()
        decimals = await self.execution._token_decimals(str(row["token_mint"]))
        if sol_usd is None or decimals is None:
            return None
        input_usd = STARTING_PAPER_NAV_USD * fraction
        input_sol = input_usd / sol_usd
        input_lamports = max(1, int(round(input_sol * LAMPORTS_PER_SOL)))
        buy = await self.execution._route(WSOL_MINT, str(row["token_mint"]), input_lamports)
        if buy is None or int(buy.get("out_amount") or 0) <= 0:
            return None
        token_raw = int(buy["out_amount"])
        token_units = token_raw / (10**decimals)
        if token_units <= 0:
            return None
        entry_cost_sol = (input_lamports + int(buy["fee_lamports"])) / LAMPORTS_PER_SOL
        entry_price_sol = entry_cost_sol / token_units
        exit_route = await self.execution._route(str(row["token_mint"]), WSOL_MINT, token_raw)
        exit_net_sol = None
        if exit_route is not None:
            candidate = (int(exit_route["out_amount"]) - int(exit_route["fee_lamports"])) / LAMPORTS_PER_SOL
            if candidate > 0:
                exit_net_sol = candidate
        completed_at = datetime.now(timezone.utc)
        quote_latency_ms = (time.perf_counter() - started) * 1000.0
        wallet_price = float(row["wallet_price_sol"])
        chase = max(0.0, entry_price_sol / wallet_price - 1.0) if wallet_price > 0 else 1.0
        return {
            "paper_nav_usd": STARTING_PAPER_NAV_USD,
            "position_fraction": fraction,
            "input_usd": input_usd,
            "sol_usd": sol_usd,
            "input_lamports": input_lamports,
            "entry_fee_lamports": int(buy["fee_lamports"]),
            "entry_cost_sol": entry_cost_sol,
            "token_raw": token_raw,
            "decimals": decimals,
            "entry_price_sol": entry_price_sol,
            "exit_net_sol": exit_net_sol,
            "round_trip_cost_fraction": max(0.0, 1.0 - exit_net_sol / entry_cost_sol) if exit_net_sol else 0.0,
            "chase_fraction": chase,
            "signal_to_entry_seconds": max(0.0, (completed_at - observed).total_seconds()),
            "quote_latency_ms": quote_latency_ms,
        }

    def _market_regime(self, at: datetime) -> MarketRegime:
        start = (at - timedelta(minutes=5)).isoformat()
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT SUM(CASE WHEN side='buy' THEN 1 ELSE 0 END) buys, "
                "SUM(CASE WHEN side='sell' THEN 1 ELSE 0 END) sells, "
                "SUM(CASE WHEN side='buy' AND copyable=1 THEN 1 ELSE 0 END) copyable_buys, "
                "COUNT(DISTINCT wallet) entities FROM wallet_discovery_forward_observations "
                "WHERE received_at>=? AND received_at<=?",
                (start, at.isoformat()),
            ).fetchone()
        buys = int(row["buys"] or 0) if row else 0
        sells = int(row["sells"] or 0) if row else 0
        copyable = int(row["copyable_buys"] or 0) if row else 0
        entities = int(row["entities"] or 0) if row else 0
        if sells > buys and sells >= 3:
            return MarketRegime.WEAK
        if buys >= max(4, 2 * max(1, sells)) and entities >= 4:
            return MarketRegime.BROAD_MANIA
        if buys > sells and copyable >= 2:
            return MarketRegime.HIGH_SPECULATION
        return MarketRegime.NEUTRAL

    def _creator_flow_state(self, token_mint: str, creator: str | None, at: datetime) -> str:
        if not creator:
            return "neutral"
        try:
            members = sorted(self.discovery.entity_resolver.component(creator, as_of=at))
        except Exception:
            members = [creator]
        if not members:
            return "neutral"
        placeholders = ",".join("?" for _ in members)
        start = (at - timedelta(minutes=10)).isoformat()
        sql = (
            "SELECT side,SUM(token_amount) amount FROM wallet_discovery_forward_observations "
            f"WHERE token_mint=? AND wallet IN ({placeholders}) AND received_at>=? AND received_at<=? GROUP BY side"
        )
        params: tuple[Any, ...] = (token_mint, *members, start, at.isoformat())
        with self.store._lock:
            rows = self.store.db.execute(sql, params).fetchall()
        flow = {str(row["side"]): float(row["amount"] or 0.0) for row in rows}
        buys, sells = flow.get("buy", 0.0), flow.get("sell", 0.0)
        if buys > sells * 1.10 and buys > 0:
            return "accumulating"
        if sells > buys * 1.10 and sells > 0:
            return "distributing"
        return "neutral"

    def _confirmation_context(self, token_mint: str, wallet: str, creator: str | None, at: datetime) -> tuple[str, str | None, int]:
        confirmations = self.execution._confirmations(token_mint, at, wallet)
        graph = self.execution._entity_graph((wallet, creator or "", *confirmations), at)
        trigger_entity = graph.entity_id(wallet) or f"entity:{wallet}"
        creator_entity = graph.entity_id(creator)
        excluded = {trigger_entity}
        if creator_entity:
            excluded.add(creator_entity)
        independent = [entity for entity in graph.distinct_entities(confirmations) if entity not in excluded]
        return trigger_entity, creator_entity, len(independent)

    def _open_cluster_exposure(self, creator_entity: str | None) -> float:
        if not creator_entity:
            return 0.0
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT opportunity_json,assigned_position_fraction FROM profit_first_final_trials t "
                "LEFT JOIN profit_first_final_outcomes o ON o.epoch_id=t.epoch_id AND o.source_signature=t.source_signature AND o.lane=t.lane "
                "WHERE t.epoch_id=? AND t.lane=? AND o.id IS NULL",
                (self.epoch_id, FinalLane.ELITE_WALLET_CONTINUATION.value),
            ).fetchall()
        total = 0.0
        for row in rows:
            try:
                opportunity = json.loads(str(row["opportunity_json"]))
                if opportunity.get("creator_entity") == creator_entity:
                    total += float(row["assigned_position_fraction"])
            except Exception:
                continue
        return min(1.0, total)

    def _constraints(self, creator_entity: str | None, exit_executable: bool) -> SizingConstraints:
        cluster_exposure = self._open_cluster_exposure(creator_entity)
        return SizingConstraints(
            liquidity_headroom_fraction=0.20 if exit_executable else 0.0,
            entity_concentration_headroom_fraction=max(0.0, 1.0 - cluster_exposure),
            correlation_headroom_fraction=max(0.0, 1.0 - cluster_exposure),
            confidence_multiplier=1.0,
        )

    async def _buy(self, row: dict[str, Any]) -> None:
        if not bool(row["copyable"]):
            return
        at = datetime.fromisoformat(str(row["received_at"]))
        fraction = self._assigned_fraction(str(row["signature"]))
        execution = await self._execution(row, fraction)
        hard, soft, early_exit = await self.execution._risk(row, at)
        token_mint, wallet = str(row["token_mint"]), str(row["wallet"])
        creator = self.execution._deployer(token_mint, at)
        trigger_entity, creator_entity, independent_count = self._confirmation_context(token_mint, wallet, creator, at)
        creator_linked = creator_entity is not None and creator_entity == trigger_entity
        creator_flow = self._creator_flow_state(token_mint, creator, at)
        signal_to_entry = float(execution["signal_to_entry_seconds"]) if execution else float(row["observation_lag_ms"]) / 1000.0
        opportunity = FinalOpportunity(
            token=token_mint,
            source_signature=str(row["signature"]),
            observed_at=str(row["observed_at"]),
            trigger_entity=trigger_entity,
            creator_entity=creator_entity,
            independent_confirmation_count=independent_count,
            creator_linked_trigger=creator_linked,
            creator_flow_state=creator_flow,
            chase_fraction=float(execution["chase_fraction"]) if execution else float(row.get("chase_fraction") or 0.0),
            signal_to_entry_seconds=signal_to_entry,
            round_trip_cost_fraction=float(execution["round_trip_cost_fraction"]) if execution else 0.0,
            entry_executable=execution is not None,
            exit_executable=bool(execution and execution["exit_net_sol"] is not None),
            regime=self._market_regime(at),
            independent_demand_strength=min(1.0, independent_count / 3.0),
            early_buyer_exit_fraction=early_exit,
            soft_risk_flags=frozenset(soft),
            hard_risk_flags=frozenset(hard),
        )
        constraints = self._constraints(creator_entity, opportunity.exit_executable)
        decisions = self.strategy.evaluate_all(opportunity, constraints)
        now = datetime.now(timezone.utc).isoformat()
        group = hashlib.sha256(f"{self.epoch_id}|{row['signature']}|{row['received_at']}".encode()).hexdigest()[:20]
        with self.store._lock, self.store.db:
            for lane in FinalLane:
                decision = decisions[lane.value]
                context = self.strategy.context(opportunity, lane)
                self.store.db.execute(
                    "INSERT OR IGNORE INTO profit_first_final_trials("
                    "epoch_id,release_commit,strategy_version,source_signature,observation_group,token_mint,trigger_wallet,lane,"
                    "observed_at,received_at,regime,opportunity_json,context_json,decision_json,assigned_position_fraction,"
                    "quote_input_lamports,entry_fee_lamports,entry_token_raw,token_decimals,entry_all_in_price_sol,immediate_exit_net_sol,"
                    "round_trip_cost_fraction,signal_to_entry_seconds,quote_latency_ms,entry_executable,exit_executable,created_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        self.epoch_id,self.release_commit,FINAL_STRATEGY_VERSION,str(row["signature"]),group,token_mint,wallet,lane.value,
                        str(row["observed_at"]),str(row["received_at"]),opportunity.regime.value,_dump(asdict(opportunity)),_dump(asdict(context)),
                        _dump(asdict(decision)),fraction,execution["input_lamports"] if execution else None,
                        execution["entry_fee_lamports"] if execution else None,execution["token_raw"] if execution else None,
                        execution["decimals"] if execution else None,execution["entry_price_sol"] if execution else None,
                        execution["exit_net_sol"] if execution else None,execution["round_trip_cost_fraction"] if execution else 0.0,
                        signal_to_entry,execution["quote_latency_ms"] if execution else None,1 if execution else 0,
                        1 if execution and execution["exit_net_sol"] is not None else 0,now,
                    ),
                )
            unified = decisions[UNIFIED_LANE]
            self.store.db.execute(
                "INSERT OR IGNORE INTO profit_first_final_trials("
                "epoch_id,release_commit,strategy_version,source_signature,observation_group,token_mint,trigger_wallet,lane,"
                "observed_at,received_at,regime,opportunity_json,context_json,decision_json,assigned_position_fraction,"
                "quote_input_lamports,entry_fee_lamports,entry_token_raw,token_decimals,entry_all_in_price_sol,immediate_exit_net_sol,"
                "round_trip_cost_fraction,signal_to_entry_seconds,quote_latency_ms,entry_executable,exit_executable,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    self.epoch_id,self.release_commit,FINAL_STRATEGY_VERSION,str(row["signature"]),group,token_mint,wallet,UNIFIED_LANE,
                    str(row["observed_at"]),str(row["received_at"]),opportunity.regime.value,_dump(asdict(opportunity)),None,
                    _dump(asdict(unified)),fraction,execution["input_lamports"] if execution else None,
                    execution["entry_fee_lamports"] if execution else None,execution["token_raw"] if execution else None,
                    execution["decimals"] if execution else None,execution["entry_price_sol"] if execution else None,
                    execution["exit_net_sol"] if execution else None,execution["round_trip_cost_fraction"] if execution else 0.0,
                    signal_to_entry,execution["quote_latency_ms"] if execution else None,1 if execution else 0,
                    1 if execution and execution["exit_net_sol"] is not None else 0,now,
                ),
            )

    def _flow_reversed(self, token_mint: str, at: datetime) -> bool:
        start = (at - timedelta(minutes=2)).isoformat()
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT SUM(CASE WHEN side='buy' THEN 1 ELSE 0 END) buys, SUM(CASE WHEN side='sell' THEN 1 ELSE 0 END) sells "
                "FROM wallet_discovery_forward_observations WHERE token_mint=? AND received_at>=? AND received_at<=?",
                (token_mint, start, at.isoformat()),
            ).fetchone()
        buys = int(row["buys"] or 0) if row else 0
        sells = int(row["sells"] or 0) if row else 0
        return sells > buys and sells >= 2

    def _seller_entity(self, seller: str, creator_wallet: str | None, at: datetime) -> tuple[str, str | None]:
        graph = self.execution._entity_graph((seller, creator_wallet or ""), at)
        return graph.entity_id(seller) or f"entity:{seller}", graph.entity_id(creator_wallet)

    async def _sell(self, row: dict[str, Any]) -> None:
        at = datetime.fromisoformat(str(row["received_at"]))
        token_mint, seller = str(row["token_mint"]), str(row["wallet"])
        with self.store._lock:
            candidates = self.store.db.execute(
                "SELECT t.* FROM profit_first_final_trials t LEFT JOIN profit_first_final_outcomes o ON "
                "o.epoch_id=t.epoch_id AND o.source_signature=t.source_signature AND o.lane=t.lane "
                "WHERE t.epoch_id=? AND t.token_mint=? AND t.entry_executable=1 AND t.exit_executable=1 "
                "AND t.observed_at<? AND o.id IS NULL ORDER BY t.id",
                (self.epoch_id, token_mint, str(row["observed_at"])),
            ).fetchall()
        groups: dict[str, list[dict[str, Any]]] = {}
        for candidate in candidates:
            groups.setdefault(str(candidate["source_signature"]), []).append(dict(candidate))
        for entry_signature, trials in groups.items():
            first = trials[0]
            opportunity = json.loads(str(first["opportunity_json"]))
            creator_wallet = self.execution._deployer(token_mint, at)
            seller_entity, current_creator_entity = self._seller_entity(seller, creator_wallet, at)
            creator_entity = opportunity.get("creator_entity") or current_creator_entity
            features = ExitFeatures(
                creator_distribution=bool(creator_entity and seller_entity == creator_entity),
                linked_entity_distribution=bool(creator_entity and seller_entity == creator_entity and seller != creator_wallet),
                early_holder_exit_fraction=float(opportunity.get("early_buyer_exit_fraction") or 0.0),
                successful_scout_exit=seller == str(first["trigger_wallet"]),
                buy_sell_flow_reversal=self._flow_reversed(token_mint, at),
            )
            signal = self.strategy.exit_model.evaluate(features)
            now = datetime.now(timezone.utc).isoformat()
            with self.store._lock, self.store.db:
                self.store.db.execute(
                    "INSERT OR IGNORE INTO profit_first_final_exit_signals("
                    "epoch_id,token_mint,source_signature,seller_wallet,observed_at,features_json,signal_json,created_at) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (self.epoch_id,token_mint,str(row["signature"]),seller,str(row["observed_at"]),_dump(asdict(features)),_dump(asdict(signal)),now),
                )
            if not signal.should_exit and seller != str(first["trigger_wallet"]):
                continue
            token_raw = int(first["entry_token_raw"] or 0)
            exit_route = await self.execution._route(token_mint, WSOL_MINT, token_raw)
            if exit_route is None:
                continue
            exit_net_sol = (int(exit_route["out_amount"]) - int(exit_route["fee_lamports"])) / LAMPORTS_PER_SOL
            entry_cost_sol = (int(first["quote_input_lamports"]) + int(first["entry_fee_lamports"])) / LAMPORTS_PER_SOL
            if exit_net_sol <= 0 or entry_cost_sol <= 0:
                continue
            net_return = exit_net_sol / entry_cost_sol - 1.0
            exit_reason = "dynamic_exit_alpha:" + ",".join(signal.reasons) if signal.should_exit else "trigger_wallet_exit_baseline"
            inserted: list[FinalForwardOutcome] = []
            with self.store._lock, self.store.db:
                for trial in trials:
                    cursor = self.store.db.execute(
                        "INSERT OR IGNORE INTO profit_first_final_outcomes("
                        "epoch_id,release_commit,strategy_version,source_signature,exit_signature,token_mint,trigger_wallet,lane,context_json,"
                        "entry_observed_at,exit_observed_at,signal_to_entry_seconds,position_fraction,entry_cost_sol,exit_net_sol,net_return,"
                        "evidence_phase,exit_reason,exit_features_json,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            self.epoch_id,self.release_commit,FINAL_STRATEGY_VERSION,entry_signature,str(row["signature"]),token_mint,
                            str(trial["trigger_wallet"]),str(trial["lane"]),trial["context_json"],str(trial["observed_at"]),str(row["observed_at"]),
                            float(trial["signal_to_entry_seconds"]),float(trial["assigned_position_fraction"]),entry_cost_sol,exit_net_sol,net_return,
                            "forward",exit_reason,_dump(asdict(features)),now,
                        ),
                    )
                    if cursor.rowcount == 1 and trial["context_json"] is not None and str(trial["lane"]) != UNIFIED_LANE:
                        inserted.append(
                            FinalForwardOutcome(
                                context=_context(str(trial["context_json"])),net_return=net_return,source_signature=entry_signature,
                                release_commit=self.release_commit,observed_at=str(trial["observed_at"]),
                                signal_to_entry_seconds=float(trial["signal_to_entry_seconds"]),
                                position_fraction=float(trial["assigned_position_fraction"]),evidence_phase="forward",exit_reason=exit_reason,
                            )
                        )
            for outcome in inserted:
                self.ledger.add(outcome)

    async def observe(self, signature: str) -> None:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT signature,wallet,token_mint,side,token_amount,observed_at,received_at,wallet_price_sol,"
                "copyable_price_sol,chase_fraction,copyable,observation_lag_ms,risk_complete,manipulation_flag,side_wallet_flag,source "
                "FROM wallet_discovery_forward_observations WHERE signature=? LIMIT 1",
                (signature,),
            ).fetchone()
        if row is None:
            return
        data = dict(row)
        try:
            if str(data["side"]).lower() == "buy":
                await self._buy(data)
            elif str(data["side"]).lower() == "sell":
                await self._sell(data)
            self.last_observed_at = str(data["received_at"])
            self.last_error = None
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: final profit-first observation failed"

    def status(self) -> dict[str, Any]:
        with self.store._lock:
            trials = int(self.store.db.execute("SELECT COUNT(*) FROM profit_first_final_trials WHERE epoch_id=?", (self.epoch_id,)).fetchone()[0])
            groups = int(self.store.db.execute("SELECT COUNT(DISTINCT observation_group) FROM profit_first_final_trials WHERE epoch_id=?", (self.epoch_id,)).fetchone()[0])
            complete_groups = int(self.store.db.execute(
                "SELECT COUNT(*) FROM (SELECT observation_group FROM profit_first_final_trials WHERE epoch_id=? GROUP BY observation_group HAVING COUNT(DISTINCT lane)=5)",
                (self.epoch_id,),
            ).fetchone()[0])
            outcomes = int(self.store.db.execute("SELECT COUNT(*) FROM profit_first_final_outcomes WHERE epoch_id=?", (self.epoch_id,)).fetchone()[0])
            exit_signals = int(self.store.db.execute("SELECT COUNT(*) FROM profit_first_final_exit_signals WHERE epoch_id=?", (self.epoch_id,)).fetchone()[0])
        curve = [asdict(row) for row in SignalDecayCurve.from_outcomes(self.ledger.outcomes)]
        lane_reports: dict[str, Any] = {}
        for lane in FinalLane:
            rows = [item for item in self.ledger.outcomes if item.context.lane == lane]
            lane_reports[lane.value] = asdict(build_robustness_report(rows))
        base = self.strategy.status()
        return {
            **base,
            "integration_installed": True,
            "evidence_epoch_id": self.epoch_id,
            "release_commit": self.release_commit,
            "release_bound": self.release_commit != "unbound-local-release",
            "manifest": self._manifest(),
            "clean_final_version_epoch": True,
            "parent_research_evidence_rewritten": False,
            "rpc_workload_class": "research",
            "uses_isolated_wallet_research_rpc_pool": bool(getattr(self.discovery.rpc, "_roi_wallet_research_pool", False)),
            "jupiter_amount_specific_quote_and_unsigned_simulation_only": True,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
            "full_raw_receipt_scope_modified": False,
            "strategy_scope_reduced": False,
            "continuity_lease_seconds_unchanged": 12.0,
            "recovery_bound_unchanged": "3x1000",
            "certification_thresholds_unchanged": True,
            "five_lane_observation_groups": groups,
            "five_lane_complete_groups": complete_groups,
            "all_lanes_receive_identical_chronological_source_observation": groups == complete_groups,
            "shadow_trial_rows": trials,
            "forward_outcome_rows": outcomes,
            "exit_signal_rows": exit_signals,
            "signal_decay_curve": curve,
            "lane_robustness_reports": lane_reports,
            "unified_profit_maximizer_has_authority": False if max((r["sample_count"] for r in lane_reports.values()), default=0) < self.strategy.policy.min_forward_outcomes_for_selection else "paper_only_if_positive_forward_edge",
            "pending_research_tasks": len(self._tasks),
            "research_task_bound": _MAX_TASKS,
            "research_backpressure_drops": self.backpressure_drops,
            "last_observed_at": self.last_observed_at,
            "last_error": self.last_error,
        }


def _adapter(discovery: ContinuousWalletDiscovery) -> FinalProfitFirstResearchAdapter:
    current = getattr(discovery, _ADAPTER_ATTR, None)
    if isinstance(current, FinalProfitFirstResearchAdapter):
        return current
    current = FinalProfitFirstResearchAdapter(discovery)
    setattr(discovery, _ADAPTER_ATTR, current)
    return current


async def _record_with_final(self: ContinuousWalletDiscovery, *args: Any, **kwargs: Any) -> bool:
    if _ORIGINAL_RECORD is None:
        raise RuntimeError("final profit-first adapter not installed")
    inserted = await _ORIGINAL_RECORD(self, *args, **kwargs)
    swap = args[0] if args else kwargs.get("swap")
    if inserted and getattr(swap, "signature", None):
        try:
            _adapter(self).schedule(str(swap.signature))
        except Exception:
            pass
    return bool(inserted)


async def _realtime_record_with_final(self: Any, *args: Any, **kwargs: Any) -> bool:
    if _ORIGINAL_REALTIME_RECORD is None:
        raise RuntimeError("final realtime profit-first adapter not installed")
    inserted = await _ORIGINAL_REALTIME_RECORD(self, *args, **kwargs)
    swap = args[0] if args else kwargs.get("swap")
    if inserted and getattr(swap, "signature", None):
        try:
            _adapter(self.discovery).schedule(str(swap.signature))
        except Exception:
            pass
    return bool(inserted)


def _status_with_final(self: ContinuousWalletDiscovery) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("final profit-first adapter not installed")
    payload = _ORIGINAL_STATUS(self)
    legacy = payload.get("profit_first_entity_strategy")
    if legacy is not None:
        payload["profit_first_entity_research_baseline"] = legacy
    try:
        payload["profit_first_entity_strategy"] = _adapter(self).status()
    except Exception as exc:
        payload["profit_first_entity_strategy"] = {
            "strategy_version": FINAL_STRATEGY_VERSION,
            "integration_installed": True,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
            "failed_closed": True,
            "last_error": f"{type(exc).__name__}: final adapter unavailable",
        }
    return payload


def _legacy_schedule_disabled(self: Any, signature: str) -> None:
    # The final strategy replaces new parent-research sampling. Historical rows remain
    # untouched and visible, but new RPC work is not duplicated across both versions.
    return None


def install_final_profit_first_entity_research() -> None:
    global _ORIGINAL_RECORD, _ORIGINAL_REALTIME_RECORD, _ORIGINAL_STATUS
    from . import profit_first_entity_research as legacy

    legacy.ProfitFirstResearchAdapter.schedule = _legacy_schedule_disabled  # type: ignore[method-assign]

    current = ContinuousWalletDiscovery._record_forward_swap
    if not getattr(current, "_roi_profit_first_entity_final", False):
        _ORIGINAL_RECORD = current
        _record_with_final.__dict__.update(getattr(current, "__dict__", {}))
        setattr(_record_with_final, "_roi_profit_first_entity_final", True)
        ContinuousWalletDiscovery._record_forward_swap = _record_with_final  # type: ignore[method-assign]

    try:
        from . import wallet_realtime_tracking_repair as realtime
        current_realtime = realtime.RealtimeWalletTracker._record_quick_forward_swap
        if not getattr(current_realtime, "_roi_profit_first_entity_final", False):
            _ORIGINAL_REALTIME_RECORD = current_realtime
            _realtime_record_with_final.__dict__.update(getattr(current_realtime, "__dict__", {}))
            setattr(_realtime_record_with_final, "_roi_profit_first_entity_final", True)
            realtime.RealtimeWalletTracker._record_quick_forward_swap = _realtime_record_with_final  # type: ignore[method-assign]
    except Exception:
        pass

    current_status = ContinuousWalletDiscovery.status
    if not getattr(current_status, "_roi_profit_first_entity_final", False):
        _ORIGINAL_STATUS = current_status
        _status_with_final.__dict__.update(getattr(current_status, "__dict__", {}))
        setattr(_status_with_final, "_roi_profit_first_entity_final", True)
        ContinuousWalletDiscovery.status = _status_with_final  # type: ignore[method-assign]


__all__ = ["FinalProfitFirstResearchAdapter", "install_final_profit_first_entity_research"]
