from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from .config import BASELINE, StrategyConfig
from .engine import PaperTradingEngine
from .models import (
    Candidate,
    CandidateStatus,
    Confirmation,
    IntentKind,
    PaperPosition,
    RiskSnapshot,
    SimulatedFill,
    TradeOutcome,
    WalletTier,
    WalletTouch,
)
from .observation_store import ObservationEventStore

_ENGINE_EVENT_TYPES = ("first_touch", "confirmation", "price", "trade_intent", "trade_outcome")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _risk_to_dict(risk: RiskSnapshot) -> dict[str, Any]:
    payload = asdict(risk)
    payload["observed_at"] = risk.observed_at.isoformat()
    return payload


def _risk_from_dict(payload: dict[str, Any]) -> RiskSnapshot:
    values = dict(payload)
    values["observed_at"] = datetime.fromisoformat(str(values["observed_at"]))
    return RiskSnapshot(**values)


class DurablePaperTradingEngine(PaperTradingEngine):
    """Paper engine with a fail-closed durable checkpoint.

    The checkpoint is not an authority source. It only restores the exact
    in-memory paper state after a process restart. If engine events exist after
    the latest checkpoint, startup fails closed rather than guessing or
    silently resetting the experiment to $500.
    """

    def __init__(
        self,
        *,
        store: ObservationEventStore,
        config: StrategyConfig = BASELINE,
    ):
        super().__init__(config=config, store=store)
        self.store = store
        with store._lock, store.db:
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS paper_engine_checkpoint ("
                "id INTEGER PRIMARY KEY CHECK(id=1), saved_at TEXT NOT NULL, "
                "last_engine_event_id INTEGER NOT NULL, state_json TEXT NOT NULL, "
                "state_sha256 TEXT NOT NULL)"
            )
        self._restore_or_fail_closed()

    def _latest_engine_event_id(self) -> int:
        placeholders = ",".join("?" for _ in _ENGINE_EVENT_TYPES)
        with self.store._lock:
            row = self.store.db.execute(
                f"SELECT COALESCE(MAX(id), 0) FROM events WHERE event_type IN ({placeholders})",
                _ENGINE_EVENT_TYPES,
            ).fetchone()
        return int(row[0]) if row else 0

    def _state_dict(self) -> dict[str, Any]:
        candidates: dict[str, Any] = {}
        for mint, candidate in self.strategy.candidates.items():
            candidates[mint] = {
                "token_mint": candidate.token_mint,
                "scout_wallet": candidate.scout_wallet,
                "scout_entity_id": candidate.scout_entity_id,
                "scout_tier": candidate.scout_tier.value,
                "first_touch_at": candidate.first_touch_at.isoformat(),
                "scout_reference_price": candidate.scout_reference_price,
                "risk_at_entry": _risk_to_dict(candidate.risk_at_entry),
                "status": candidate.status.value,
                "confirmed_at": candidate.confirmed_at.isoformat() if candidate.confirmed_at else None,
                "confirmation_wallet": candidate.confirmation_wallet,
                "confirmation_entity_id": candidate.confirmation_entity_id,
                "full_entry_reference_price": candidate.full_entry_reference_price,
                "harvest_triggered": candidate.harvest_triggered,
                "runner_high_water_price": candidate.runner_high_water_price,
                "closed_reason": candidate.closed_reason,
            }

        positions: dict[str, Any] = {}
        for mint, position in self.portfolio.positions.items():
            positions[mint] = {
                "token_mint": position.token_mint,
                "scout_wallet": position.scout_wallet,
                "opened_at": position.opened_at.isoformat(),
                "units": position.units,
                "cost_basis_usd": position.cost_basis_usd,
                "entry_capital_usd": position.entry_capital_usd,
                "realized_pnl_usd": position.realized_pnl_usd,
                "harvest_hit": position.harvest_hit,
                "runner_units": position.runner_units,
                "high_water_price": position.high_water_price,
                "closed_at": position.closed_at.isoformat() if position.closed_at else None,
                "closed_reason": position.closed_reason,
                "fills": [
                    {
                        "token_mint": fill.token_mint,
                        "side": fill.side,
                        "observed_at": fill.observed_at.isoformat(),
                        "reference_price": fill.reference_price,
                        "fill_price": fill.fill_price,
                        "notional_usd": fill.notional_usd,
                        "units": fill.units,
                        "execution_drag_usd": fill.execution_drag_usd,
                        "intent": fill.intent.value,
                    }
                    for fill in position.fills
                ],
            }

        closed = [
            {
                "token_mint": outcome.token_mint,
                "scout_wallet": outcome.scout_wallet,
                "opened_at": outcome.opened_at.isoformat(),
                "closed_at": outcome.closed_at.isoformat(),
                "starting_nav_usd": outcome.starting_nav_usd,
                "ending_nav_usd": outcome.ending_nav_usd,
                "net_pnl_usd": outcome.net_pnl_usd,
                "return_on_starting_nav": outcome.return_on_starting_nav,
                "harvest_hit": outcome.harvest_hit,
                "closed_reason": outcome.closed_reason,
            }
            for outcome in self.portfolio.closed
        ]
        return {
            "schema": "roi-convergence-paper-engine-checkpoint.v1",
            "strategy_version": self.config.version,
            "initial_capital_usd": self.portfolio.initial_capital_usd,
            "cash_usd": self.portfolio.cash_usd,
            "marks": dict(self.marks),
            "trade_start_nav": dict(self.portfolio._trade_start_nav),
            "candidates": candidates,
            "positions": positions,
            "closed": closed,
        }

    def _save_checkpoint(self) -> None:
        state = self._state_dict()
        raw = json.dumps(state, sort_keys=True, separators=(",", ":"), allow_nan=False)
        digest = hashlib.sha256(raw.encode()).hexdigest()
        last_engine_event_id = self._latest_engine_event_id()
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT INTO paper_engine_checkpoint(id, saved_at, last_engine_event_id, state_json, state_sha256) "
                "VALUES (1, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET "
                "saved_at=excluded.saved_at, last_engine_event_id=excluded.last_engine_event_id, "
                "state_json=excluded.state_json, state_sha256=excluded.state_sha256",
                (_now_iso(), last_engine_event_id, raw, digest),
            )

    def _restore_or_fail_closed(self) -> None:
        if not self.store.verify():
            raise RuntimeError("paper engine restore blocked: event hash chain invalid")
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT last_engine_event_id, state_json, state_sha256 FROM paper_engine_checkpoint WHERE id=1"
            ).fetchone()
        latest_engine_event_id = self._latest_engine_event_id()
        if row is None:
            if latest_engine_event_id != 0:
                raise RuntimeError("paper engine restore blocked: engine history exists without a durable checkpoint")
            return
        raw = str(row["state_json"])
        digest = hashlib.sha256(raw.encode()).hexdigest()
        if digest != str(row["state_sha256"]):
            raise RuntimeError("paper engine restore blocked: checkpoint digest mismatch")
        if int(row["last_engine_event_id"]) != latest_engine_event_id:
            raise RuntimeError("paper engine restore blocked: checkpoint does not cover latest engine event")
        state = json.loads(raw)
        if state.get("schema") != "roi-convergence-paper-engine-checkpoint.v1":
            raise RuntimeError("paper engine restore blocked: unsupported checkpoint schema")
        if state.get("strategy_version") != self.config.version:
            raise RuntimeError("paper engine restore blocked: strategy version mismatch")
        if abs(float(state.get("initial_capital_usd")) - self.config.initial_capital_usd) > 1e-9:
            raise RuntimeError("paper engine restore blocked: genesis capital mismatch")

        self.marks = {str(k): float(v) for k, v in dict(state.get("marks") or {}).items()}
        self.portfolio.cash_usd = float(state["cash_usd"])
        self.portfolio.positions = {}
        self.portfolio.closed = []
        self.portfolio._trade_start_nav = {
            str(k): float(v) for k, v in dict(state.get("trade_start_nav") or {}).items()
        }
        self.strategy.candidates = {}

        for mint, payload in dict(state.get("candidates") or {}).items():
            candidate = Candidate(
                token_mint=str(payload["token_mint"]),
                scout_wallet=str(payload["scout_wallet"]),
                scout_entity_id=str(payload["scout_entity_id"]),
                scout_tier=WalletTier(str(payload["scout_tier"])),
                first_touch_at=datetime.fromisoformat(str(payload["first_touch_at"])),
                scout_reference_price=float(payload["scout_reference_price"]),
                risk_at_entry=_risk_from_dict(dict(payload["risk_at_entry"])),
                status=CandidateStatus(str(payload["status"])),
                confirmed_at=datetime.fromisoformat(str(payload["confirmed_at"])) if payload.get("confirmed_at") else None,
                confirmation_wallet=payload.get("confirmation_wallet"),
                confirmation_entity_id=payload.get("confirmation_entity_id"),
                full_entry_reference_price=float(payload["full_entry_reference_price"]) if payload.get("full_entry_reference_price") is not None else None,
                harvest_triggered=bool(payload.get("harvest_triggered")),
                runner_high_water_price=float(payload["runner_high_water_price"]) if payload.get("runner_high_water_price") is not None else None,
                closed_reason=payload.get("closed_reason"),
            )
            self.strategy.candidates[str(mint)] = candidate

        for mint, payload in dict(state.get("positions") or {}).items():
            position = PaperPosition(
                token_mint=str(payload["token_mint"]),
                scout_wallet=str(payload["scout_wallet"]),
                opened_at=datetime.fromisoformat(str(payload["opened_at"])),
                units=float(payload["units"]),
                cost_basis_usd=float(payload["cost_basis_usd"]),
                entry_capital_usd=float(payload["entry_capital_usd"]),
                realized_pnl_usd=float(payload["realized_pnl_usd"]),
                harvest_hit=bool(payload["harvest_hit"]),
                runner_units=float(payload["runner_units"]),
                high_water_price=float(payload["high_water_price"]) if payload.get("high_water_price") is not None else None,
                closed_at=datetime.fromisoformat(str(payload["closed_at"])) if payload.get("closed_at") else None,
                closed_reason=payload.get("closed_reason"),
            )
            position.fills = [
                SimulatedFill(
                    token_mint=str(fill["token_mint"]),
                    side=str(fill["side"]),
                    observed_at=datetime.fromisoformat(str(fill["observed_at"])),
                    reference_price=float(fill["reference_price"]),
                    fill_price=float(fill["fill_price"]),
                    notional_usd=float(fill["notional_usd"]),
                    units=float(fill["units"]),
                    execution_drag_usd=float(fill["execution_drag_usd"]),
                    intent=IntentKind(str(fill["intent"])),
                )
                for fill in payload.get("fills") or []
            ]
            self.portfolio.positions[str(mint)] = position

        self.portfolio.closed = [
            TradeOutcome(
                token_mint=str(payload["token_mint"]),
                scout_wallet=str(payload["scout_wallet"]),
                opened_at=datetime.fromisoformat(str(payload["opened_at"])),
                closed_at=datetime.fromisoformat(str(payload["closed_at"])),
                starting_nav_usd=float(payload["starting_nav_usd"]),
                ending_nav_usd=float(payload["ending_nav_usd"]),
                net_pnl_usd=float(payload["net_pnl_usd"]),
                return_on_starting_nav=float(payload["return_on_starting_nav"]),
                harvest_hit=bool(payload["harvest_hit"]),
                closed_reason=str(payload["closed_reason"]),
            )
            for payload in state.get("closed") or []
        ]

    def on_first_touch(self, touch: WalletTouch, risk: RiskSnapshot, *, execution_price: float | None = None) -> None:
        super().on_first_touch(touch, risk, execution_price=execution_price)
        self._save_checkpoint()

    def on_confirmation(self, confirmation: Confirmation, risk: RiskSnapshot, *, execution_price: float | None = None) -> None:
        super().on_confirmation(confirmation, risk, execution_price=execution_price)
        self._save_checkpoint()

    def on_price(self, token_mint: str, observed_at: datetime, reference_price: float) -> None:
        super().on_price(token_mint, observed_at, reference_price)
        self._save_checkpoint()
