from __future__ import annotations

import asyncio
import base64
import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Callable

import httpx

from .ingestion import NormalizedSwap
from .observation import WSOL_MINT
from .profit_first_entity_strategy import (
    EntityGraph,
    EntityLink,
    ForwardOutcome,
    Lane,
    OpportunityContext,
    OpportunitySnapshot,
    OutcomeLedger,
    ProfitFirstEntityStrategy,
    STRATEGY_VERSION,
)
from .quote import LAMPORTS_PER_SOL
from .wallet_discovery import ContinuousWalletDiscovery


_ORIGINAL_RECORD: Callable[..., Any] | None = None
_ORIGINAL_REALTIME_RECORD: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_ADAPTER_ATTR = "_roi_profit_first_entity_research"


def _release_commit() -> str:
    for key in ("RENDER_GIT_COMMIT", "SOLANA_ROI_RELEASE_COMMIT", "GITHUB_SHA"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return "unbound-local-release"


def _dump(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _context(raw: str) -> OpportunityContext:
    row = json.loads(raw)
    return OpportunityContext(
        lane=Lane(row["lane"]),
        trigger_entity=row["trigger_entity"],
        creator_entity=row.get("creator_entity"),
        confirmation_entities=tuple(row.get("confirmation_entities", ())),
        independent_confirmation_count=int(row.get("independent_confirmation_count", 0)),
        creator_linked_trigger=bool(row.get("creator_linked_trigger", False)),
        creator_flow_state=row.get("creator_flow_state", "neutral"),
        confirmation_bin=row.get("confirmation_bin", "0"),
        chase_bin=row.get("chase_bin", "<=5%"),
        early_exit_bin=row.get("early_exit_bin", "<=5%"),
        soft_risk_bin=row.get("soft_risk_bin", "0"),
    )


class ProfitFirstResearchAdapter:
    """Paper-only v4 adapter hosted by the isolated wallet-research process."""

    def __init__(self, discovery: ContinuousWalletDiscovery):
        self.discovery = discovery
        self.store = discovery.store
        self.release_commit = _release_commit()
        self.ledger = OutcomeLedger()
        self.strategy = ProfitFirstEntityStrategy(ledger=self.ledger)
        self._http: httpx.AsyncClient | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self.backpressure_drops = 0
        self.last_observed_at: str | None = None
        self.last_error: str | None = None
        self._schema()
        self._load_outcomes()

    def _schema(self) -> None:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS profit_first_entity_shadow_trials ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, strategy_version TEXT NOT NULL, "
                "entry_signature TEXT NOT NULL, token_mint TEXT NOT NULL, trigger_wallet TEXT NOT NULL, lane TEXT NOT NULL, "
                "observed_at TEXT NOT NULL, received_at TEXT NOT NULL, context_json TEXT NOT NULL, decision_json TEXT NOT NULL, "
                "entry_input_lamports INTEGER, entry_fee_lamports INTEGER, entry_token_raw INTEGER, token_decimals INTEGER, "
                "entry_all_in_price_sol REAL, immediate_exit_net_sol REAL, round_trip_cost_fraction REAL, "
                "entry_executable INTEGER NOT NULL, exit_executable INTEGER NOT NULL, created_at TEXT NOT NULL, "
                "UNIQUE(release_commit, strategy_version, entry_signature, lane))"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS profit_first_entity_forward_outcomes ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, strategy_version TEXT NOT NULL, "
                "entry_signature TEXT NOT NULL, exit_signature TEXT NOT NULL, token_mint TEXT NOT NULL, trigger_wallet TEXT NOT NULL, "
                "lane TEXT NOT NULL, context_json TEXT NOT NULL, entry_observed_at TEXT NOT NULL, exit_observed_at TEXT NOT NULL, "
                "entry_cost_sol REAL NOT NULL, exit_net_sol REAL NOT NULL, net_return REAL NOT NULL, "
                "maximum_adverse_excursion REAL NOT NULL DEFAULT 0, maximum_favorable_excursion REAL NOT NULL DEFAULT 0, "
                "exit_reason TEXT NOT NULL, created_at TEXT NOT NULL, "
                "UNIQUE(release_commit, strategy_version, entry_signature, lane))"
            )
            self.store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_profit_first_outcomes ON "
                "profit_first_entity_forward_outcomes(strategy_version, lane, id)"
            )

    def _load_outcomes(self) -> None:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT context_json, net_return, maximum_adverse_excursion, maximum_favorable_excursion, exit_reason "
                "FROM profit_first_entity_forward_outcomes WHERE strategy_version=? ORDER BY id",
                (STRATEGY_VERSION,),
            ).fetchall()
        for row in rows:
            try:
                self.ledger.add(
                    ForwardOutcome(
                        context=_context(str(row["context_json"])),
                        net_return=float(row["net_return"]),
                        maximum_adverse_excursion=float(row["maximum_adverse_excursion"]),
                        maximum_favorable_excursion=float(row["maximum_favorable_excursion"]),
                        exit_reason=str(row["exit_reason"]),
                    )
                )
            except Exception:
                continue

    def schedule(self, signature: str) -> None:
        if not signature:
            return
        if len(self._tasks) >= 32:
            self.backpressure_drops += 1
            self.last_error = "profit-first research task bound reached; sample skipped fail-closed"
            return
        task = asyncio.create_task(self.observe(signature), name="profit-first-entity-research")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _client(self) -> httpx.AsyncClient | None:
        if not os.getenv("JUPITER_API_KEY", "").strip():
            return None
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=2.0)
        return self._http

    async def _route(self, input_mint: str, output_mint: str, amount: int) -> dict[str, int] | None:
        client = self._client()
        taker = os.getenv("SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY", "").strip()
        api_key = os.getenv("JUPITER_API_KEY", "").strip()
        if client is None or not taker or amount <= 0:
            return None
        try:
            response = await client.get(
                "https://api.jup.ag/swap/v2/order",
                params={"inputMint": input_mint, "outputMint": output_mint, "amount": str(amount), "taker": taker},
                headers={"x-api-key": api_key, "Accept": "application/json"},
            )
            response.raise_for_status()
            order = response.json()
            transaction = order.get("transaction") if isinstance(order, dict) else None
            if not isinstance(transaction, str) or not transaction or order.get("outAmount") is None:
                return None
            if not base64.b64decode(transaction, validate=True):
                return None
            simulated = await self.discovery.rpc.call(
                "simulateTransaction",
                [transaction, {"encoding": "base64", "sigVerify": False, "replaceRecentBlockhash": True, "commitment": "processed"}],
            )
            value = simulated.get("value") if isinstance(simulated, dict) else None
            if not isinstance(value, dict) or value.get("err") is not None:
                return None
            fees = [int(order[key]) for key in ("signatureFeeLamports", "prioritizationFeeLamports", "rentFeeLamports")]
            if any(value < 0 for value in fees):
                return None
            return {"out_amount": int(order["outAmount"]), "fee_lamports": sum(fees)}
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: research executable route unavailable"
            return None

    async def _token_decimals(self, token_mint: str) -> int | None:
        try:
            result = await self.discovery.rpc.call("getTokenSupply", [token_mint, {"commitment": "confirmed"}])
            value = result.get("value") if isinstance(result, dict) else None
            decimals = int(value["decimals"]) if isinstance(value, dict) and value.get("decimals") is not None else -1
            return decimals if 0 <= decimals <= 18 else None
        except Exception:
            return None

    def _deployer(self, token_mint: str, at: datetime) -> str | None:
        try:
            row = self.store.latest_risk_evidence(token_mint, "deployer", as_of_received_at=at.isoformat())
            payload = dict(row.get("payload") or {}) if row else {}
            return str(payload.get("deployer_wallet") or "") or None
        except Exception:
            return None

    def _entity_graph(self, addresses: tuple[str, ...], at: datetime) -> EntityGraph:
        links: list[EntityLink] = []
        for address in addresses:
            if not address:
                continue
            try:
                members = sorted(self.discovery.entity_resolver.component(address, as_of=at))
            except Exception:
                members = [address]
            if len(members) > 1:
                links.extend(EntityLink(members[0], member, "point_in_time_entity_component") for member in members[1:])
        return EntityGraph(links)

    def _confirmations(self, token_mint: str, at: datetime, trigger: str) -> tuple[str, ...]:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT wallet FROM wallet_discovery_forward_observations "
                "WHERE token_mint=? AND side='buy' AND copyable=1 AND received_at<=? ORDER BY received_at, id",
                (token_mint, at.isoformat()),
            ).fetchall()
        return tuple(dict.fromkeys(str(row["wallet"]) for row in rows if str(row["wallet"]) != trigger))

    async def _risk(self, row: dict[str, Any], at: datetime) -> tuple[set[str], set[str], float]:
        hard: set[str] = set()
        soft: set[str] = set()
        token_mint = str(row["token_mint"])
        try:
            authority = self.store.latest_risk_evidence(token_mint, "authority", as_of_received_at=at.isoformat())
            payload = dict(authority.get("payload") or {}) if authority else {}
            if payload.get("freeze_authority_active"):
                hard.add("authority_can_block_transfer_or_exit")
            if payload.get("mint_authority_active"):
                soft.add("mint_authority_active")
        except Exception:
            pass
        try:
            snapshot = await self.discovery.risk.snapshot(
                token_mint,
                at,
                scout_wallet=str(row["wallet"]),
                scout_entity_id=self.discovery.entity_resolver.entity_id_for(
                    str(row["wallet"]), fallback_entity_id=None, as_of=at
                ),
            )
        except Exception:
            snapshot = None
        if snapshot is None or bool(getattr(snapshot, "unacceptable_liquidity", False)):
            hard.add("liquidity_unexitable")
        if snapshot is None:
            return hard, soft, 0.0
        for name in (
            "bundled_launch",
            "sniper_heavy",
            "abnormal_sell_pressure",
            "common_funded_early_wallet_cluster",
            "scout_deployer_connection",
        ):
            if bool(getattr(snapshot, name, False)):
                soft.add(name)
        early_exit = 1.0 if bool(getattr(snapshot, "early_buyers_exiting", False)) else 0.0
        if early_exit:
            soft.add("early_buyers_exiting")
        return hard, soft, early_exit

    async def _entry(self, row: dict[str, Any]) -> dict[str, Any] | None:
        token_mint = str(row["token_mint"])
        wallet_price = float(row["wallet_price_sol"])
        input_sol = float(row["token_amount"]) * wallet_price
        decimals = await self._token_decimals(token_mint)
        if input_sol <= 0.0 or decimals is None:
            return None
        input_lamports = max(1, int(round(input_sol * LAMPORTS_PER_SOL)))
        buy = await self._route(WSOL_MINT, token_mint, input_lamports)
        if buy is None or buy["out_amount"] <= 0:
            return None
        entry_cost_sol = (input_lamports + buy["fee_lamports"]) / LAMPORTS_PER_SOL
        token_raw = buy["out_amount"]
        token_units = token_raw / (10**decimals)
        if token_units <= 0:
            return None
        entry_price = entry_cost_sol / token_units
        immediate_exit = await self._route(token_mint, WSOL_MINT, token_raw)
        exit_net_sol = None
        if immediate_exit is not None:
            exit_net_sol = (immediate_exit["out_amount"] - immediate_exit["fee_lamports"]) / LAMPORTS_PER_SOL
            if exit_net_sol <= 0:
                exit_net_sol = None
        return {
            "input_lamports": input_lamports,
            "entry_fee_lamports": buy["fee_lamports"],
            "entry_cost_sol": entry_cost_sol,
            "token_raw": token_raw,
            "decimals": decimals,
            "entry_price": entry_price,
            "chase": max(0.0, entry_price / wallet_price - 1.0),
            "exit_net_sol": exit_net_sol,
            "round_trip": max(0.0, 1.0 - exit_net_sol / entry_cost_sol) if exit_net_sol else 0.0,
        }

    async def _buy(self, row: dict[str, Any]) -> None:
        if not bool(row["copyable"]):
            return
        at = datetime.fromisoformat(str(row["received_at"]))
        try:
            swap = NormalizedSwap(
                signature=str(row["signature"]), slot=0,
                observed_at=datetime.fromisoformat(str(row["observed_at"])), received_at=at,
                wallet=str(row["wallet"]), token_mint=str(row["token_mint"]), side="buy",
                token_amount=float(row["token_amount"]),
                native_amount_sol=float(row["token_amount"]) * float(row["wallet_price_sol"]),
                reference_price_sol=float(row["wallet_price_sol"]), source=str(row["source"]),
            )
            await self.discovery._risk_flags(swap)
        except Exception:
            pass
        execution = await self._entry(row)
        hard, soft, early_exit = await self._risk(row, at)
        token_mint, wallet = str(row["token_mint"]), str(row["wallet"])
        creator = self._deployer(token_mint, at)
        confirmations = self._confirmations(token_mint, at, wallet)
        graph = self._entity_graph((wallet, creator or "", *confirmations), at)
        strategy = ProfitFirstEntityStrategy(entity_graph=graph, ledger=self.ledger, policy=self.strategy.policy)
        snapshot = OpportunitySnapshot(
            token=token_mint,
            trigger_wallet=wallet,
            creator_wallet=creator,
            confirming_wallets=confirmations,
            chase_fraction=float(execution["chase"]) if execution else float(row.get("chase_fraction") or 0.0),
            observation_lag_seconds=float(row["observation_lag_ms"]) / 1000.0,
            round_trip_cost_fraction=float(execution["round_trip"]) if execution else 0.0,
            entry_executable=execution is not None,
            exit_executable=bool(execution and execution["exit_net_sol"] is not None),
            early_buyer_exit_fraction=early_exit,
            hard_risk_flags=frozenset(hard),
            soft_risk_flags=frozenset(soft),
        )
        decisions = strategy.evaluate_all(snapshot)
        now = datetime.now(timezone.utc).isoformat()
        with self.store._lock, self.store.db:
            for name, decision in decisions.items():
                if name == "unified_profit_maximizer" or decision.selected_lane is None:
                    continue
                context = strategy.context_for_lane(snapshot, decision.selected_lane)
                self.store.db.execute(
                    "INSERT OR IGNORE INTO profit_first_entity_shadow_trials("
                    "release_commit,strategy_version,entry_signature,token_mint,trigger_wallet,lane,observed_at,received_at,"
                    "context_json,decision_json,entry_input_lamports,entry_fee_lamports,entry_token_raw,token_decimals,"
                    "entry_all_in_price_sol,immediate_exit_net_sol,round_trip_cost_fraction,entry_executable,exit_executable,created_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        self.release_commit, STRATEGY_VERSION, str(row["signature"]), token_mint, wallet, name,
                        str(row["observed_at"]), str(row["received_at"]), _dump(asdict(context)), _dump(asdict(decision)),
                        execution["input_lamports"] if execution else None,
                        execution["entry_fee_lamports"] if execution else None,
                        execution["token_raw"] if execution else None,
                        execution["decimals"] if execution else None,
                        execution["entry_price"] if execution else None,
                        execution["exit_net_sol"] if execution else None,
                        execution["round_trip"] if execution else 0.0,
                        1 if execution else 0,
                        1 if execution and execution["exit_net_sol"] is not None else 0,
                        now,
                    ),
                )

    async def _sell(self, row: dict[str, Any]) -> None:
        with self.store._lock:
            trials = self.store.db.execute(
                "SELECT t.* FROM profit_first_entity_shadow_trials t LEFT JOIN profit_first_entity_forward_outcomes o ON "
                "o.release_commit=t.release_commit AND o.strategy_version=t.strategy_version "
                "AND o.entry_signature=t.entry_signature AND o.lane=t.lane "
                "WHERE t.release_commit=? AND t.strategy_version=? AND t.trigger_wallet=? AND t.token_mint=? "
                "AND t.entry_executable=1 AND t.exit_executable=1 AND t.observed_at<? AND o.id IS NULL ORDER BY t.id",
                (self.release_commit, STRATEGY_VERSION, str(row["wallet"]), str(row["token_mint"]), str(row["observed_at"])),
            ).fetchall()
        groups: dict[str, list[dict[str, Any]]] = {}
        for trial in trials:
            groups.setdefault(str(trial["entry_signature"]), []).append(dict(trial))
        for entry_signature, items in groups.items():
            first = items[0]
            exit_route = await self._route(str(row["token_mint"]), WSOL_MINT, int(first["entry_token_raw"] or 0))
            if exit_route is None:
                continue
            exit_net_sol = (exit_route["out_amount"] - exit_route["fee_lamports"]) / LAMPORTS_PER_SOL
            entry_cost_sol = (int(first["entry_input_lamports"]) + int(first["entry_fee_lamports"])) / LAMPORTS_PER_SOL
            if exit_net_sol <= 0 or entry_cost_sol <= 0:
                continue
            net_return = exit_net_sol / entry_cost_sol - 1.0
            now = datetime.now(timezone.utc).isoformat()
            inserted: list[ForwardOutcome] = []
            with self.store._lock, self.store.db:
                for trial in items:
                    cursor = self.store.db.execute(
                        "INSERT OR IGNORE INTO profit_first_entity_forward_outcomes("
                        "release_commit,strategy_version,entry_signature,exit_signature,token_mint,trigger_wallet,lane,context_json,"
                        "entry_observed_at,exit_observed_at,entry_cost_sol,exit_net_sol,net_return,exit_reason,created_at) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            self.release_commit, STRATEGY_VERSION, entry_signature, str(row["signature"]), str(row["token_mint"]),
                            str(row["wallet"]), str(trial["lane"]), str(trial["context_json"]), str(trial["observed_at"]),
                            str(row["observed_at"]), entry_cost_sol, exit_net_sol, net_return,
                            "first_observed_trigger_wallet_sell_exact_shadow_exit", now,
                        ),
                    )
                    if cursor.rowcount == 1:
                        inserted.append(ForwardOutcome(context=_context(str(trial["context_json"])), net_return=net_return,
                                                       exit_reason="first_observed_trigger_wallet_sell_exact_shadow_exit"))
            for outcome in inserted:
                self.ledger.add(outcome)

    async def observe(self, signature: str) -> None:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT signature,wallet,token_mint,side,token_amount,observed_at,received_at,wallet_price_sol,"
                "copyable_price_sol,chase_fraction,copyable,observation_lag_ms,risk_complete,manipulation_flag,"
                "side_wallet_flag,source FROM wallet_discovery_forward_observations WHERE signature=? LIMIT 1",
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
            self.last_error = f"{type(exc).__name__}: profit-first research observation failed"

    def status(self) -> dict[str, Any]:
        try:
            with self.store._lock:
                trials = self.store.db.execute(
                    "SELECT COUNT(*) FROM profit_first_entity_shadow_trials WHERE strategy_version=?", (STRATEGY_VERSION,)
                ).fetchone()[0]
                executable = self.store.db.execute(
                    "SELECT COUNT(*) FROM profit_first_entity_shadow_trials WHERE strategy_version=? "
                    "AND entry_executable=1 AND exit_executable=1", (STRATEGY_VERSION,)
                ).fetchone()[0]
                outcomes = self.store.db.execute(
                    "SELECT COUNT(*) FROM profit_first_entity_forward_outcomes WHERE strategy_version=?", (STRATEGY_VERSION,)
                ).fetchone()[0]
                release_outcomes = self.store.db.execute(
                    "SELECT COUNT(*) FROM profit_first_entity_forward_outcomes WHERE strategy_version=? AND release_commit=?",
                    (STRATEGY_VERSION, self.release_commit),
                ).fetchone()[0]
        except Exception as exc:
            trials = executable = outcomes = release_outcomes = 0
            self.last_error = f"{type(exc).__name__}: profit-first research status failed"
        return {
            **self.strategy.status(),
            "integration_installed": True,
            "production_owner": "continuous_wallet_discovery_research_process",
            "release_commit": self.release_commit,
            "release_bound": self.release_commit != "unbound-local-release",
            "rpc_workload_class": "research",
            "uses_discovery_research_rpc_pool": bool(getattr(self.discovery.rpc, "_roi_wallet_research_pool", False)),
            "jupiter_quote_and_unsigned_simulation_only": True,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
            "active_v3_1_cohort_mutation_allowed": False,
            "shadow_trials": int(trials),
            "fully_executable_shadow_trials": int(executable),
            "forward_outcomes": int(outcomes),
            "current_release_forward_outcomes": int(release_outcomes),
            "forward_outcomes_append_only": True,
            "creator_entity_links_point_in_time_only": True,
            "critical_continuity_path_modified": False,
            "continuity_lease_seconds_unchanged": 12.0,
            "recovery_bound_unchanged": "3x1000",
            "direct_solana_scope_unchanged": True,
            "certification_thresholds_unchanged": True,
            "pending_research_tasks": len(self._tasks),
            "research_task_bound": 32,
            "research_backpressure_drops": self.backpressure_drops,
            "last_observed_at": self.last_observed_at,
            "last_error": self.last_error,
        }


def _adapter(discovery: ContinuousWalletDiscovery) -> ProfitFirstResearchAdapter:
    current = getattr(discovery, _ADAPTER_ATTR, None)
    if isinstance(current, ProfitFirstResearchAdapter):
        return current
    current = ProfitFirstResearchAdapter(discovery)
    setattr(discovery, _ADAPTER_ATTR, current)
    return current


async def _record_with_v4(self: ContinuousWalletDiscovery, *args: Any, **kwargs: Any) -> bool:
    if _ORIGINAL_RECORD is None:
        raise RuntimeError("profit-first adapter not installed")
    inserted = await _ORIGINAL_RECORD(self, *args, **kwargs)
    swap = args[0] if args else kwargs.get("swap")
    if inserted and getattr(swap, "signature", None):
        try:
            _adapter(self).schedule(str(swap.signature))
        except Exception:
            pass
    return bool(inserted)


async def _realtime_record_with_v4(self: Any, *args: Any, **kwargs: Any) -> bool:
    if _ORIGINAL_REALTIME_RECORD is None:
        raise RuntimeError("profit-first realtime adapter not installed")
    inserted = await _ORIGINAL_REALTIME_RECORD(self, *args, **kwargs)
    swap = args[0] if args else kwargs.get("swap")
    if inserted and getattr(swap, "signature", None):
        try:
            _adapter(self.discovery).schedule(str(swap.signature))
        except Exception:
            pass
    return bool(inserted)


def _status_with_v4(self: ContinuousWalletDiscovery) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("profit-first adapter not installed")
    payload = _ORIGINAL_STATUS(self)
    try:
        payload["profit_first_entity_strategy"] = _adapter(self).status()
    except Exception as exc:
        payload["profit_first_entity_strategy"] = {
            "strategy_version": STRATEGY_VERSION,
            "integration_installed": True,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
            "active_v3_1_cohort_mutation_allowed": False,
            "failed_closed": True,
            "last_error": f"{type(exc).__name__}: adapter unavailable",
        }
    return payload


def install_profit_first_entity_research() -> None:
    global _ORIGINAL_RECORD, _ORIGINAL_REALTIME_RECORD, _ORIGINAL_STATUS
    current = ContinuousWalletDiscovery._record_forward_swap
    if not getattr(current, "_roi_profit_first_entity_research", False):
        _ORIGINAL_RECORD = current
        _record_with_v4.__dict__.update(getattr(current, "__dict__", {}))
        setattr(_record_with_v4, "_roi_profit_first_entity_research", True)
        ContinuousWalletDiscovery._record_forward_swap = _record_with_v4  # type: ignore[method-assign]

    try:
        from . import wallet_realtime_tracking_repair as realtime
        current_realtime = realtime.RealtimeWalletTracker._record_quick_forward_swap
        if not getattr(current_realtime, "_roi_profit_first_entity_research", False):
            _ORIGINAL_REALTIME_RECORD = current_realtime
            _realtime_record_with_v4.__dict__.update(getattr(current_realtime, "__dict__", {}))
            setattr(_realtime_record_with_v4, "_roi_profit_first_entity_research", True)
            realtime.RealtimeWalletTracker._record_quick_forward_swap = _realtime_record_with_v4  # type: ignore[method-assign]
    except Exception:
        pass

    current_status = ContinuousWalletDiscovery.status
    if not getattr(current_status, "_roi_profit_first_entity_research", False):
        _ORIGINAL_STATUS = current_status
        _status_with_v4.__dict__.update(getattr(current_status, "__dict__", {}))
        setattr(_status_with_v4, "_roi_profit_first_entity_research", True)
        ContinuousWalletDiscovery.status = _status_with_v4  # type: ignore[method-assign]


__all__ = ["ProfitFirstResearchAdapter", "install_profit_first_entity_research"]
