from __future__ import annotations

import hashlib
import json
import math
import time
from datetime import datetime, timezone
from typing import Any, Callable

from . import robinhood_chain_profit_maximizer as robinhood_strategy
from .robinhood_chain_core import WETH, _clean_address
from .robinhood_chain_profit_maximizer import RobinhoodProfitMaximizerMixin
from .strategy_v52_authority import (
    AUTHORITY_ID,
    ECONOMIC_FREEZE_EPOCH,
    LIVE_MONEY_AUTHORITY,
    PAPER_ONLY,
    STRATEGY_VERSION,
    position_policy,
)

LIFECYCLE_VERSION = "v52-robinhood-position-lifecycle-accounting-1"
STRESSED_DEPTH_FRACTION = 0.35
STRESSED_DEPTH_UTILIZATION_LIMIT = 0.25
STRESSED_EXIT_COVERAGE_RATIO = 1.0 / (
    STRESSED_DEPTH_FRACTION * STRESSED_DEPTH_UTILIZATION_LIMIT
)
MIN_REENTRY_CONSOLIDATION_SECONDS = 30.0
FLOW_RANK = {
    "exhaustion": 0,
    "neutral": 1,
    "entity_accumulation": 2,
    "pre_fomo": 3,
    "active_fomo": 4,
}

_INSTALLED = False
_BASE_CHOOSE: Callable[..., Any] | None = None
_BASE_MAYBE_V3: Callable[..., Any] | None = None
_BASE_MAYBE_V2: Callable[..., Any] | None = None
_BASE_TOKEN_OPEN: Callable[..., Any] | None = None
_BASE_SETTLE_ONE: Callable[..., Any] | None = None
_BASE_PAPER_NAV: Callable[..., Any] | None = None
_BASE_OPEN_EXPOSURE: Callable[..., Any] | None = None


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _ensure_schema(owner: Any) -> None:
    if bool(getattr(owner, "_roi_v52_robinhood_lifecycle_schema_ready", False)):
        return
    with owner.store._lock, owner.store.db:
        owner.store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_robinhood_positions ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, position_key TEXT NOT NULL UNIQUE, "
            "token TEXT NOT NULL, market TEXT NOT NULL, venue TEXT NOT NULL, lifecycle TEXT NOT NULL, "
            "generation INTEGER NOT NULL, status TEXT NOT NULL, target_fraction REAL NOT NULL, "
            "opened_fraction REAL NOT NULL, remaining_fraction REAL NOT NULL, "
            "remaining_token_raw TEXT NOT NULL, weighted_entry_price_eth REAL NOT NULL, "
            "last_add_price_eth REAL NOT NULL, last_evidence_fingerprint TEXT NOT NULL, "
            "last_evidence_json TEXT NOT NULL, impulse_id TEXT NOT NULL, "
            "last_trigger_entity TEXT NOT NULL, last_lane TEXT NOT NULL, "
            "last_risk_severity REAL NOT NULL, last_flow_state TEXT NOT NULL, "
            "derisk_stage INTEGER NOT NULL, runner_active INTEGER NOT NULL, "
            "opened_at TEXT NOT NULL, updated_at TEXT NOT NULL, closed_at TEXT, close_reason TEXT, "
            "authority_id TEXT NOT NULL, strategy_version TEXT NOT NULL, economic_freeze_epoch TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
        )
        owner.store.db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_v52_robinhood_open_token "
            "ON v52_robinhood_positions(token) "
            "WHERE status IN ('entered','scaling','de_risking','runner')"
        )
        owner.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v52_robinhood_position_history "
            "ON v52_robinhood_positions(token,generation,id)"
        )
        owner.store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_robinhood_position_lots ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, position_id INTEGER NOT NULL, trial_id INTEGER NOT NULL UNIQUE, "
            "lot_stage TEXT NOT NULL, entry_fraction REAL NOT NULL, remaining_fraction REAL NOT NULL, "
            "entry_token_raw TEXT NOT NULL, remaining_token_raw TEXT NOT NULL, "
            "entry_total_cost_wei TEXT NOT NULL, remaining_entry_cost_wei TEXT NOT NULL, "
            "realized_exit_net_wei TEXT NOT NULL, realized_exit_gas_wei TEXT NOT NULL, "
            "entry_price_eth REAL NOT NULL, evidence_fingerprint TEXT NOT NULL, "
            "capital_reservation_id TEXT, opened_at TEXT NOT NULL, closed_at TEXT, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "UNIQUE(position_id,evidence_fingerprint))"
        )
        owner.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v52_robinhood_lots_position "
            "ON v52_robinhood_position_lots(position_id,id)"
        )
        owner.store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_robinhood_position_events ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, position_id INTEGER NOT NULL, event_key TEXT NOT NULL UNIQUE, "
            "event_type TEXT NOT NULL, state_before TEXT NOT NULL, state_after TEXT NOT NULL, "
            "token_raw TEXT NOT NULL, position_fraction REAL NOT NULL, "
            "exit_quote_out_wei TEXT, exit_gas_wei TEXT, allocated_entry_cost_wei TEXT, "
            "net_return REAL, paper_nav_multiplier REAL, reason TEXT NOT NULL, "
            "quote_json TEXT NOT NULL, created_at TEXT NOT NULL, "
            "authority_id TEXT NOT NULL, strategy_version TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
        )
        owner.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v52_robinhood_events_position "
            "ON v52_robinhood_position_events(position_id,id)"
        )
    setattr(owner, "_roi_v52_robinhood_lifecycle_schema_ready", True)
    _bootstrap_existing_open_trials(owner)


def _table_exists(owner: Any, table: str) -> bool:
    try:
        with owner.store._lock:
            row = owner.store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table,),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _evidence_payload(kwargs: dict[str, Any], lane: str) -> dict[str, Any]:
    return {
        "entity": str(kwargs.get("entity") or ""),
        "role": str(kwargs.get("role") or "unknown"),
        "lane": str(lane or ""),
        "venue": str(kwargs.get("venue") or "UNKNOWN"),
        "lifecycle": str(kwargs.get("lifecycle") or "unknown"),
        "regime": str(kwargs.get("regime") or "unknown"),
        "risk_signature": str(kwargs.get("risk_signature") or "clean"),
        "risk_severity": float(kwargs.get("risk_severity") or 0.0),
        "flow_state": str(kwargs.get("flow_state") or "neutral"),
        "lanes": sorted(str(item) for item in (kwargs.get("lanes") or ())),
    }


def _fingerprint(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _open_position(owner: Any, token: str) -> dict[str, Any] | None:
    _ensure_schema(owner)
    token = _clean_address(token)
    if not token:
        return None
    with owner.store._lock:
        row = owner.store.db.execute(
            "SELECT * FROM v52_robinhood_positions WHERE token=? "
            "AND status IN ('entered','scaling','de_risking','runner') ORDER BY id DESC LIMIT 1",
            (token,),
        ).fetchone()
    return dict(row) if row is not None else None


def _last_closed_position(owner: Any, token: str) -> dict[str, Any] | None:
    _ensure_schema(owner)
    token = _clean_address(token)
    if not token:
        return None
    with owner.store._lock:
        row = owner.store.db.execute(
            "SELECT * FROM v52_robinhood_positions WHERE token=? AND status='closed' "
            "ORDER BY generation DESC,id DESC LIMIT 1",
            (token,),
        ).fetchone()
    return dict(row) if row is not None else None


def _legacy_untracked_open(owner: Any, token: str) -> bool:
    token = _clean_address(token)
    if not token:
        return True
    with owner.store._lock:
        row = owner.store.db.execute(
            "SELECT 1 FROM robinhood_paper_trials t "
            "LEFT JOIN robinhood_paper_outcomes o ON o.trial_id=t.id "
            "LEFT JOIN v52_robinhood_position_lots l ON l.trial_id=t.id "
            "WHERE t.paper_only=1 AND t.token=? AND o.id IS NULL AND l.id IS NULL LIMIT 1",
            (token,),
        ).fetchone()
    return row is not None


def _bootstrap_existing_open_trials(owner: Any) -> None:
    if bool(getattr(owner, "_roi_v52_robinhood_lifecycle_bootstrap_done", False)):
        return
    if not _table_exists(owner, "v52_economic_freeze_releases"):
        setattr(owner, "_roi_v52_robinhood_lifecycle_bootstrap_done", True)
        return
    with owner.store._lock:
        rows = owner.store.db.execute(
            "SELECT t.*,c.lane,c.trigger_role,c.regime,c.flow_state,c.risk_signature,c.risk_severity "
            "FROM robinhood_paper_trials t "
            "JOIN robinhood_v5_trial_context c ON c.trial_id=t.id "
            "JOIN v52_economic_freeze_releases e ON e.release_commit=t.release_commit "
            "LEFT JOIN robinhood_paper_outcomes o ON o.trial_id=t.id "
            "LEFT JOIN v52_robinhood_position_lots l ON l.trial_id=t.id "
            "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND e.strategy_version=? "
            "AND t.paper_only=1 AND o.id IS NULL AND l.id IS NULL ORDER BY t.id",
            (ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID, STRATEGY_VERSION),
        ).fetchall()
    for raw in rows:
        row = dict(raw)
        token = _clean_address(row.get("token"))
        if not token:
            continue
        payload = {
            "entity": str(row.get("trigger_entity") or ""),
            "role": str(row.get("trigger_role") or "unknown"),
            "lane": str(row.get("lane") or "unknown"),
            "venue": str(row.get("venue") or "UNKNOWN"),
            "lifecycle": str(row.get("lifecycle") or "unknown"),
            "regime": str(row.get("regime") or "unknown"),
            "risk_signature": str(row.get("risk_signature") or "clean"),
            "risk_severity": float(row.get("risk_severity") or 0.0),
            "flow_state": str(row.get("flow_state") or "neutral"),
            "lanes": [str(row.get("lane") or "unknown")],
        }
        evidence = _fingerprint(payload)
        with owner.store._lock, owner.store.db:
            pos = owner.store.db.execute(
                "SELECT * FROM v52_robinhood_positions WHERE token=? "
                "AND status IN ('entered','scaling','de_risking','runner') ORDER BY id DESC LIMIT 1",
                (token,),
            ).fetchone()
            lot_stage = "starter" if pos is None else "add"
            if pos is None:
                gen_row = owner.store.db.execute(
                    "SELECT COALESCE(MAX(generation),0) AS generation FROM v52_robinhood_positions WHERE token=?",
                    (token,),
                ).fetchone()
                generation = int(gen_row["generation"] or 0) + 1
                fraction = max(0.0, float(row.get("position_fraction") or 0.0))
                raw_amount = max(0, int(row.get("entry_token_raw") or 0))
                now = str(row.get("opened_at") or _utcnow())
                cursor = owner.store.db.execute(
                    "INSERT INTO v52_robinhood_positions("
                    "position_key,token,market,venue,lifecycle,generation,status,target_fraction,opened_fraction,"
                    "remaining_fraction,remaining_token_raw,weighted_entry_price_eth,last_add_price_eth,"
                    "last_evidence_fingerprint,last_evidence_json,impulse_id,last_trigger_entity,last_lane,"
                    "last_risk_severity,last_flow_state,derisk_stage,runner_active,opened_at,updated_at,"
                    "authority_id,strategy_version,economic_freeze_epoch,paper_only,live_money_authority"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)",
                    (
                        f"v52-rh:{token}:{generation}",
                        token,
                        _clean_address(row.get("market")),
                        str(row.get("venue") or "UNKNOWN"),
                        str(row.get("lifecycle") or "unknown"),
                        generation,
                        "entered",
                        max(fraction, fraction / max(1e-12, float(position_policy()["starter_fraction_of_target"]))),
                        fraction,
                        fraction,
                        str(raw_amount),
                        float(row.get("entry_price_eth") or 0.0),
                        float(row.get("entry_price_eth") or 0.0),
                        evidence,
                        json.dumps(payload, sort_keys=True, separators=(",", ":")),
                        evidence,
                        str(row.get("trigger_entity") or ""),
                        str(row.get("lane") or ""),
                        float(row.get("risk_severity") or 0.0),
                        str(row.get("flow_state") or "neutral"),
                        0,
                        0,
                        now,
                        _utcnow(),
                        AUTHORITY_ID,
                        STRATEGY_VERSION,
                        ECONOMIC_FREEZE_EPOCH,
                        1,
                    ),
                )
                position_id = int(cursor.lastrowid)
            else:
                posd = dict(pos)
                position_id = int(posd["id"])
                old_raw = max(0, int(posd["remaining_token_raw"] or 0))
                add_raw = max(0, int(row.get("entry_token_raw") or 0))
                old_fraction = max(0.0, float(posd["remaining_fraction"] or 0.0))
                add_fraction = max(0.0, float(row.get("position_fraction") or 0.0))
                aggregate_raw = old_raw + add_raw
                weighted = (
                    (
                        old_raw * float(posd["weighted_entry_price_eth"] or 0.0)
                        + add_raw * float(row.get("entry_price_eth") or 0.0)
                    )
                    / aggregate_raw
                    if aggregate_raw > 0
                    else 0.0
                )
                owner.store.db.execute(
                    "UPDATE v52_robinhood_positions SET opened_fraction=opened_fraction+?,remaining_fraction=?,"
                    "remaining_token_raw=?,weighted_entry_price_eth=?,last_add_price_eth=?,"
                    "last_evidence_fingerprint=?,last_evidence_json=?,last_trigger_entity=?,last_lane=?,"
                    "last_risk_severity=?,last_flow_state=?,status='scaling',updated_at=? WHERE id=?",
                    (
                        add_fraction,
                        old_fraction + add_fraction,
                        str(aggregate_raw),
                        weighted,
                        float(row.get("entry_price_eth") or 0.0),
                        evidence,
                        json.dumps(payload, sort_keys=True, separators=(",", ":")),
                        str(row.get("trigger_entity") or ""),
                        str(row.get("lane") or ""),
                        float(row.get("risk_severity") or 0.0),
                        str(row.get("flow_state") or "neutral"),
                        _utcnow(),
                        position_id,
                    ),
                )
            owner.store.db.execute(
                "INSERT OR IGNORE INTO v52_robinhood_position_lots("
                "position_id,trial_id,lot_stage,entry_fraction,remaining_fraction,entry_token_raw,remaining_token_raw,"
                "entry_total_cost_wei,remaining_entry_cost_wei,realized_exit_net_wei,realized_exit_gas_wei,"
                "entry_price_eth,evidence_fingerprint,capital_reservation_id,opened_at,paper_only,live_money_authority"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    position_id,
                    int(row["id"]),
                    lot_stage,
                    float(row.get("position_fraction") or 0.0),
                    float(row.get("position_fraction") or 0.0),
                    str(int(row.get("entry_token_raw") or 0)),
                    str(int(row.get("entry_token_raw") or 0)),
                    str(int(row.get("entry_total_cost_wei") or 0)),
                    str(int(row.get("entry_total_cost_wei") or 0)),
                    "0",
                    "0",
                    float(row.get("entry_price_eth") or 0.0),
                    evidence,
                    row.get("capital_reservation_id"),
                    str(row.get("opened_at") or _utcnow()),
                ),
            )
    setattr(owner, "_roi_v52_robinhood_lifecycle_bootstrap_done", True)


def _strengthened(position: dict[str, Any], payload: dict[str, Any]) -> bool:
    if str(payload["lane"]) != str(position.get("last_lane") or ""):
        return True
    if str(payload["lifecycle"]) != str(position.get("lifecycle") or ""):
        return True
    if str(payload["entity"]) and str(payload["entity"]) != str(position.get("last_trigger_entity") or ""):
        return True
    prior_flow = FLOW_RANK.get(str(position.get("last_flow_state") or "neutral"), 1)
    current_flow = FLOW_RANK.get(str(payload.get("flow_state") or "neutral"), 1)
    if current_flow > prior_flow:
        return True
    if float(payload.get("risk_severity") or 0.0) + 1e-12 < float(position.get("last_risk_severity") or 0.0):
        return True
    return False


def _pending_map(owner: Any) -> dict[str, dict[str, Any]]:
    pending = getattr(owner, "_roi_v52_robinhood_pending_lifecycle", None)
    if not isinstance(pending, dict):
        pending = {}
        setattr(owner, "_roi_v52_robinhood_pending_lifecycle", pending)
    return pending


def _choose_with_lifecycle(self: Any, **kwargs: Any) -> tuple[str | None, float, dict[str, Any]]:
    if _BASE_CHOOSE is None:
        raise RuntimeError("v52_robinhood_lifecycle_choose_base_missing")
    _ensure_schema(self)
    lane, base_fraction, profiles = _BASE_CHOOSE(self, **kwargs)
    copied = {
        key: dict(value) if isinstance(value, dict) else value
        for key, value in dict(profiles or {}).items()
    }
    token = _clean_address(getattr(self, "_roi_v52_candidate_token", ""))
    market = _clean_address(getattr(self, "_roi_v52_candidate_market", ""))
    if not token or not lane or float(base_fraction or 0.0) <= 0.0:
        return lane, 0.0 if not token else float(base_fraction or 0.0), copied

    profile = dict(copied.get(lane) or {})
    authority = dict(profile.get("v52_authority") or {})
    starter_policy = float(position_policy()["starter_fraction_of_target"])
    target = float(authority.get("target_fraction") or 0.0)
    if target <= 0.0:
        target = min(
            float(robinhood_strategy.ROBINHOOD_V5_MAX_POSITION),
            float(base_fraction) / max(starter_policy, 1e-12),
        )
    payload = _evidence_payload(kwargs, lane)
    evidence = _fingerprint(payload)
    position = _open_position(self, token)
    stage = "starter"
    reason = "v52_fractional_starter"
    final = float(base_fraction)

    if position is not None:
        stage = "scale"
        if str(position.get("status")) not in {"entered", "scaling"}:
            final = 0.0
            reason = "scale_blocked_position_in_derisk_or_runner"
        elif market and market != _clean_address(position.get("market")):
            final = 0.0
            reason = "scale_blocked_cross_market_aggregate_exitability_unproven"
        elif evidence == str(position.get("last_evidence_fingerprint") or ""):
            final = 0.0
            reason = "scale_blocked_no_new_forward_evidence"
        elif not _strengthened(position, payload):
            final = 0.0
            reason = "scale_blocked_no_new_strength_evidence"
        else:
            open_fraction = max(0.0, float(position.get("remaining_fraction") or 0.0))
            add_cap = target * float(position_policy()["max_scale_fraction_of_target_per_add"])
            final = max(0.0, min(add_cap, target - open_fraction))
            reason = "v52_scale_new_forward_strength"
    else:
        closed = _last_closed_position(self, token)
        if closed is not None:
            stage = "reentry"
            closed_at = _parse_time(closed.get("closed_at"))
            elapsed = (
                (datetime.now(timezone.utc) - closed_at).total_seconds()
                if closed_at is not None
                else 0.0
            )
            new_entity = (
                str(payload.get("entity") or "")
                and str(payload.get("entity") or "") != str(closed.get("last_trigger_entity") or "")
            )
            if evidence == str(closed.get("last_evidence_fingerprint") or ""):
                final = 0.0
                reason = "reentry_blocked_same_impulse"
            elif elapsed < MIN_REENTRY_CONSOLIDATION_SECONDS:
                final = 0.0
                reason = "reentry_blocked_consolidation_not_confirmed"
            elif str(payload.get("flow_state") or "") != "active_fomo":
                final = 0.0
                reason = "reentry_blocked_renewed_acceleration_missing"
            elif not new_entity:
                final = 0.0
                reason = "reentry_blocked_new_independent_buyer_missing"
            else:
                final = min(target, target * starter_policy)
                reason = "v52_reentry_new_impulse"

    open_before = float(position.get("remaining_fraction") or 0.0) if position else 0.0
    authority.update(
        {
            "target_fraction": target,
            "open_fraction_before": open_before,
            "final_fraction": final,
            "capture_stage": stage,
            "reason": reason,
            "scale_requires_new_forward_evidence": True,
            "averaging_down_allowed": False,
            "aggregate_position_exitability_required_before_add": True,
            "stressed_exit_capacity_required_before_entry_or_add": True,
        }
    )
    profile["v52_authority"] = authority
    copied[lane] = profile
    _pending_map(self)[token] = {
        "token": token,
        "market": market,
        "lane": lane,
        "stage": stage,
        "target_fraction": target,
        "evidence_fingerprint": evidence,
        "evidence": payload,
        "reason": reason,
    }
    return (lane if final > 0.0 else None), final, copied


def _token_open_with_v52_recheck(self: Any, token: str) -> bool:
    if _BASE_TOKEN_OPEN is None:
        raise RuntimeError("v52_robinhood_lifecycle_token_open_base_missing")
    token = _clean_address(token)
    if bool(getattr(self, "_roi_v52_allow_existing_position_evaluation", False)):
        position = _open_position(self, token)
        if position is not None and not _legacy_untracked_open(self, token):
            return False
    return bool(_BASE_TOKEN_OPEN(self, token))


def _capture_insert(owner: Any, token: str) -> tuple[list[dict[str, Any]], bool, Any]:
    captured: list[dict[str, Any]] = []
    had = "_v5_insert_trial" in owner.__dict__
    previous = owner.__dict__.get("_v5_insert_trial")

    def capture(**kwargs: Any) -> None:
        captured.append(dict(kwargs))

    owner.__dict__["_v5_insert_trial"] = capture
    return captured, had, previous


def _restore_insert(owner: Any, had: bool, previous: Any) -> None:
    if had:
        owner.__dict__["_v5_insert_trial"] = previous
    else:
        owner.__dict__.pop("_v5_insert_trial", None)


async def _maybe_open_v3_with_lifecycle(self: Any, pool: Any, *, current_block: int) -> None:
    if _BASE_MAYBE_V3 is None:
        raise RuntimeError("v52_robinhood_lifecycle_v3_base_missing")
    _ensure_schema(self)
    token = _clean_address(pool.token)
    position = _open_position(self, token)
    setattr(self, "_roi_v52_candidate_token", token)
    setattr(self, "_roi_v52_candidate_market", _clean_address(pool.pool))
    setattr(self, "_roi_v52_allow_existing_position_evaluation", position is not None)
    captured, had, previous = _capture_insert(self, token)
    try:
        await _BASE_MAYBE_V3(self, pool, current_block=current_block)
    finally:
        _restore_insert(self, had, previous)
        setattr(self, "_roi_v52_allow_existing_position_evaluation", False)
    for payload in captured[:1]:
        await _validate_and_commit(self, payload, venue_object=pool)


async def _maybe_open_v2_with_lifecycle(self: Any, curve: Any) -> None:
    if _BASE_MAYBE_V2 is None:
        raise RuntimeError("v52_robinhood_lifecycle_v2_base_missing")
    _ensure_schema(self)
    token = _clean_address(curve.token)
    position = _open_position(self, token)
    setattr(self, "_roi_v52_candidate_token", token)
    setattr(self, "_roi_v52_candidate_market", _clean_address(curve.curve))
    setattr(self, "_roi_v52_allow_existing_position_evaluation", position is not None)
    captured, had, previous = _capture_insert(self, token)
    try:
        await _BASE_MAYBE_V2(self, curve)
    finally:
        _restore_insert(self, had, previous)
        setattr(self, "_roi_v52_allow_existing_position_evaluation", False)
    for payload in captured[:1]:
        await _validate_and_commit(self, payload, venue_object=curve)


async def _sell_quote_for_raw(
    owner: Any,
    *,
    token: str,
    market: str,
    venue: str,
    raw_amount: int,
) -> dict[str, int] | None:
    raw_amount = int(raw_amount)
    if raw_amount <= 0:
        return None
    gas_price = await owner.rpc.gas_price()
    if venue in {"PONS_V1_UNISWAP_V3", "UNISWAP_V3_DIRECT"}:
        pool = owner.v3_pools.get(_clean_address(market))
        if pool is None:
            return None
        gross, gas_estimate = await owner.rpc.v3_quote_exact_input(
            token_in=token,
            token_out=WETH,
            fee=pool.fee,
            amount_in=raw_amount,
        )
        exit_gas = (int(gas_estimate) + 80_000) * int(gas_price)
        return {
            "gross_out_wei": max(0, int(gross)),
            "exit_gas_wei": max(0, int(exit_gas)),
            "net_out_wei": max(0, int(gross) - int(exit_gas)),
        }
    if venue == "PONS_V2_CURVE":
        state = await owner.rpc.pons_v2_launch_state(token)
        phase = int(state["phase"])
        if phase == 0:
            gross = await owner.rpc.pons_v2_curve_sell_quote(
                curve=market,
                tokens_in=raw_amount,
            )
            exit_gas = 220_000 * int(gas_price)
            return {
                "gross_out_wei": max(0, int(gross)),
                "exit_gas_wei": max(0, int(exit_gas)),
                "net_out_wei": max(0, int(gross) - int(exit_gas)),
            }
        if phase == 2:
            pair = _clean_address(state["pair_token"])
            currency_a = (
                "0x0000000000000000000000000000000000000000"
                if pair in {"", "0x0000000000000000000000000000000000000000"}
                else pair
            )
            currency0, currency1 = sorted([currency_a, token], key=lambda item: int(item, 16))
            gross, gas_estimate = await owner.rpc.v4_quote_exact_input(
                currency0=currency0,
                currency1=currency1,
                fee=int(state["pool_fee"]),
                tick_spacing=int(state["tick_spacing"]),
                hooks=robinhood_strategy.PONS_V2_MEME_HOOK,
                zero_for_one=token == currency0,
                amount_in=raw_amount,
            )
            exit_gas = (int(gas_estimate) + 120_000) * int(gas_price)
            return {
                "gross_out_wei": max(0, int(gross)),
                "exit_gas_wei": max(0, int(exit_gas)),
                "net_out_wei": max(0, int(gross) - int(exit_gas)),
            }
        return None
    return None


async def _validate_and_commit(owner: Any, payload: dict[str, Any], *, venue_object: Any) -> bool:
    started = time.monotonic()
    token = _clean_address(payload.get("token"))
    market = _clean_address(payload.get("market"))
    if not token or not market:
        return False
    pending = dict(_pending_map(owner).get(token) or {})
    if not pending or str(pending.get("reason") or "").endswith("_blocked"):
        return False
    quote = dict(payload.get("quote") or {})
    add_raw = max(0, int(quote.get("token_out") or 0))
    add_fraction = max(0.0, float(payload.get("fraction") or 0.0))
    entry_price = float(quote.get("entry_price_eth") or 0.0)
    if add_raw <= 0 or add_fraction <= 0.0 or entry_price <= 0.0:
        return False

    position = _open_position(owner, token)
    stage = str(pending.get("stage") or "starter")
    if stage == "scale":
        if position is None or str(position.get("status")) not in {"entered", "scaling"}:
            return False
        if market != _clean_address(position.get("market")):
            return False
        if entry_price + 1e-12 < float(position.get("last_add_price_eth") or 0.0):
            return False
        if str(pending.get("evidence_fingerprint")) == str(position.get("last_evidence_fingerprint") or ""):
            return False
        aggregate_raw = max(0, int(position.get("remaining_token_raw") or 0)) + add_raw
        aggregate_fraction = max(0.0, float(position.get("remaining_fraction") or 0.0)) + add_fraction
    else:
        if position is not None or _legacy_untracked_open(owner, token):
            return False
        aggregate_raw = add_raw
        aggregate_fraction = add_fraction

    target = max(0.0, float(pending.get("target_fraction") or 0.0))
    if target <= 0.0 or aggregate_fraction > target + 1e-12:
        return False

    try:
        aggregate_quote = await _sell_quote_for_raw(
            owner,
            token=token,
            market=market,
            venue=str(payload.get("venue") or ""),
            raw_amount=aggregate_raw,
        )
        stress_raw = max(
            aggregate_raw,
            int(math.ceil(aggregate_raw * STRESSED_EXIT_COVERAGE_RATIO)),
        )
        stress_quote = await _sell_quote_for_raw(
            owner,
            token=token,
            market=market,
            venue=str(payload.get("venue") or ""),
            raw_amount=stress_raw,
        )
    except Exception:
        return False
    validation_latency = max(0.0, time.monotonic() - started)
    if validation_latency > 20.0:
        return False
    if (
        aggregate_quote is None
        or stress_quote is None
        or int(aggregate_quote.get("gross_out_wei") or 0) <= 0
        or int(stress_quote.get("gross_out_wei") or 0) <= 0
    ):
        return False

    if stage == "reentry":
        closed = _last_closed_position(owner, token)
        if closed is None:
            return False
        closed_at = _parse_time(closed.get("closed_at"))
        if closed_at is None or (
            datetime.now(timezone.utc) - closed_at
        ).total_seconds() < MIN_REENTRY_CONSOLIDATION_SECONDS:
            return False
        evidence = dict(pending.get("evidence") or {})
        if str(evidence.get("flow_state") or "") != "active_fomo":
            return False
        if str(evidence.get("entity") or "") == str(closed.get("last_trigger_entity") or ""):
            return False

    quote["v52_aggregate_exit_raw"] = aggregate_raw
    quote["v52_aggregate_exit_quote_out_wei"] = int(aggregate_quote["gross_out_wei"])
    quote["v52_stress_exit_raw"] = stress_raw
    quote["v52_stress_exit_quote_out_wei"] = int(stress_quote["gross_out_wei"])
    quote["v52_stressed_exit_coverage_ratio"] = STRESSED_EXIT_COVERAGE_RATIO
    quote["v52_validation_latency_seconds"] = validation_latency
    payload = dict(payload)
    payload["quote"] = quote
    return _persist_lot(owner, payload, pending=pending)


def _persist_lot(owner: Any, payload: dict[str, Any], *, pending: dict[str, Any]) -> bool:
    token = _clean_address(payload["token"])
    market = _clean_address(payload["market"])
    quote = dict(payload["quote"])
    fraction = float(payload["fraction"])
    now = _utcnow()
    evidence = str(pending["evidence_fingerprint"])
    evidence_json = json.dumps(
        dict(pending.get("evidence") or {}),
        sort_keys=True,
        separators=(",", ":"),
    )
    with owner.store._lock:
        owner.store.db.execute("BEGIN IMMEDIATE")
        try:
            position = owner.store.db.execute(
                "SELECT * FROM v52_robinhood_positions WHERE token=? "
                "AND status IN ('entered','scaling','de_risking','runner') ORDER BY id DESC LIMIT 1",
                (token,),
            ).fetchone()
            stage = str(pending.get("stage") or "starter")
            if stage == "scale" and position is None:
                owner.store.db.rollback()
                return False
            if stage != "scale" and position is not None:
                owner.store.db.rollback()
                return False
            if position is not None and evidence == str(position["last_evidence_fingerprint"] or ""):
                owner.store.db.rollback()
                return False

            context_key = owner._v5_context_key(
                entity=str(payload["trigger_entity"]),
                role=str(payload["role"]),
                lane=str(payload["lane"]),
                venue=str(payload["venue"]),
                lifecycle=str(payload["lifecycle"]),
                regime=str(payload["regime"]),
                risk_signature=str(payload["risk"]["risk_signature"]),
                flow_state=str(payload["flow_state"]),
            )
            cursor = owner.store.db.execute(
                "INSERT INTO robinhood_paper_trials("
                "release_commit,strategy_version,token,market,venue,lifecycle,trigger_actor,trigger_entity,"
                "fomo_state,context_state,position_fraction,entry_quote_in_wei,entry_token_raw,entry_gas_wei,"
                "entry_total_cost_wei,entry_price_eth,entry_round_trip_cost_fraction,opened_at,decision_reason,"
                "capital_reservation_id,paper_only,live_money_authority"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    owner.release_commit,
                    robinhood_strategy.ROBINHOOD_V5_VERSION,
                    token,
                    market,
                    str(payload["venue"]),
                    str(payload["lifecycle"]),
                    str(payload["trigger_actor"]),
                    str(payload["trigger_entity"]),
                    str(payload["flow_state"]),
                    f"v52:{payload['lane']}:{stage}",
                    fraction,
                    str(int(quote["amount_in_wei"])),
                    str(int(quote["token_out"])),
                    str(int(quote["entry_gas_wei"])),
                    str(int(quote["entry_total_cost_wei"])),
                    float(quote["entry_price_eth"]),
                    float(quote["round_trip_cost_fraction"]),
                    now,
                    "v52_exact_entry_plus_aggregate_and_stressed_exitability",
                    None,
                ),
            )
            trial_id = int(cursor.lastrowid)
            owner.store.db.execute(
                "INSERT INTO robinhood_v5_trial_context("
                "trial_id,release_commit,strategy_version,lane,trigger_role,regime,flow_state,risk_signature,"
                "risk_severity,risk_json,context_key,latency_band,lifecycle_progress,threshold_challenger,"
                "candidate_lanes_json,created_at,paper_only,live_money_authority"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    trial_id,
                    owner.release_commit,
                    robinhood_strategy.ROBINHOOD_V5_VERSION,
                    str(payload["lane"]),
                    str(payload["role"]),
                    str(payload["regime"]),
                    str(payload["flow_state"]),
                    str(payload["risk"]["risk_signature"]),
                    float(payload["risk"]["risk_severity"]),
                    json.dumps(payload["risk"], sort_keys=True),
                    context_key,
                    "chain_poll",
                    payload.get("lifecycle_progress"),
                    1 if payload.get("threshold_challenger") else 0,
                    json.dumps(payload.get("candidate_lanes") or []),
                    now,
                ),
            )

            add_raw = int(quote["token_out"])
            entry_price = float(quote["entry_price_eth"])
            if position is None:
                gen_row = owner.store.db.execute(
                    "SELECT COALESCE(MAX(generation),0) AS generation FROM v52_robinhood_positions WHERE token=?",
                    (token,),
                ).fetchone()
                generation = int(gen_row["generation"] or 0) + 1
                position_cursor = owner.store.db.execute(
                    "INSERT INTO v52_robinhood_positions("
                    "position_key,token,market,venue,lifecycle,generation,status,target_fraction,opened_fraction,"
                    "remaining_fraction,remaining_token_raw,weighted_entry_price_eth,last_add_price_eth,"
                    "last_evidence_fingerprint,last_evidence_json,impulse_id,last_trigger_entity,last_lane,"
                    "last_risk_severity,last_flow_state,derisk_stage,runner_active,opened_at,updated_at,"
                    "authority_id,strategy_version,economic_freeze_epoch,paper_only,live_money_authority"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)",
                    (
                        f"v52-rh:{token}:{generation}",
                        token,
                        market,
                        str(payload["venue"]),
                        str(payload["lifecycle"]),
                        generation,
                        "entered",
                        float(pending["target_fraction"]),
                        fraction,
                        fraction,
                        str(add_raw),
                        entry_price,
                        entry_price,
                        evidence,
                        evidence_json,
                        evidence,
                        str(payload["trigger_entity"]),
                        str(payload["lane"]),
                        float(payload["risk"]["risk_severity"]),
                        str(payload["flow_state"]),
                        0,
                        0,
                        now,
                        now,
                        AUTHORITY_ID,
                        STRATEGY_VERSION,
                        ECONOMIC_FREEZE_EPOCH,
                        1,
                    ),
                )
                position_id = int(position_cursor.lastrowid)
                state_before = "pre_actionable" if stage != "reentry" else "reentry_watch"
                state_after = "entered"
            else:
                pos = dict(position)
                position_id = int(pos["id"])
                old_raw = int(pos["remaining_token_raw"] or 0)
                aggregate_raw = old_raw + add_raw
                weighted = (
                    (
                        old_raw * float(pos["weighted_entry_price_eth"] or 0.0)
                        + add_raw * entry_price
                    )
                    / aggregate_raw
                    if aggregate_raw > 0
                    else entry_price
                )
                owner.store.db.execute(
                    "UPDATE v52_robinhood_positions SET status='scaling',target_fraction=?,"
                    "opened_fraction=opened_fraction+?,remaining_fraction=remaining_fraction+?,"
                    "remaining_token_raw=?,weighted_entry_price_eth=?,last_add_price_eth=?,"
                    "last_evidence_fingerprint=?,last_evidence_json=?,last_trigger_entity=?,last_lane=?,"
                    "last_risk_severity=?,last_flow_state=?,updated_at=? WHERE id=?",
                    (
                        float(pending["target_fraction"]),
                        fraction,
                        fraction,
                        str(aggregate_raw),
                        weighted,
                        entry_price,
                        evidence,
                        evidence_json,
                        str(payload["trigger_entity"]),
                        str(payload["lane"]),
                        float(payload["risk"]["risk_severity"]),
                        str(payload["flow_state"]),
                        now,
                        position_id,
                    ),
                )
                state_before = str(pos["status"])
                state_after = "scaling"

            owner.store.db.execute(
                "INSERT INTO v52_robinhood_position_lots("
                "position_id,trial_id,lot_stage,entry_fraction,remaining_fraction,entry_token_raw,remaining_token_raw,"
                "entry_total_cost_wei,remaining_entry_cost_wei,realized_exit_net_wei,realized_exit_gas_wei,"
                "entry_price_eth,evidence_fingerprint,capital_reservation_id,opened_at,paper_only,live_money_authority"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    position_id,
                    trial_id,
                    stage,
                    fraction,
                    fraction,
                    str(add_raw),
                    str(add_raw),
                    str(int(quote["entry_total_cost_wei"])),
                    str(int(quote["entry_total_cost_wei"])),
                    "0",
                    "0",
                    entry_price,
                    evidence,
                    None,
                    now,
                ),
            )
            event_key = f"entry:{position_id}:{trial_id}"
            owner.store.db.execute(
                "INSERT INTO v52_robinhood_position_events("
                "position_id,event_key,event_type,state_before,state_after,token_raw,position_fraction,"
                "reason,quote_json,created_at,authority_id,strategy_version,paper_only,live_money_authority"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    position_id,
                    event_key,
                    stage,
                    state_before,
                    state_after,
                    str(add_raw),
                    fraction,
                    str(pending.get("reason") or stage),
                    json.dumps(quote, sort_keys=True, separators=(",", ":")),
                    now,
                    AUTHORITY_ID,
                    STRATEGY_VERSION,
                ),
            )
            owner.store.db.commit()
        except Exception:
            if owner.store.db.in_transaction:
                owner.store.db.rollback()
            raise
    _pending_map(owner).pop(token, None)
    return True


def _paper_nav_with_lifecycle(self: Any) -> float:
    if _BASE_PAPER_NAV is None:
        raise RuntimeError("v52_robinhood_lifecycle_nav_base_missing")
    _ensure_schema(self)
    with self.store._lock:
        legacy = self.store.db.execute(
            "SELECT o.paper_nav_multiplier FROM robinhood_paper_outcomes o "
            "LEFT JOIN v52_robinhood_position_lots l ON l.trial_id=o.trial_id "
            "WHERE o.paper_only=1 AND l.id IS NULL ORDER BY o.id"
        ).fetchall()
        events = self.store.db.execute(
            "SELECT paper_nav_multiplier FROM v52_robinhood_position_events "
            "WHERE paper_nav_multiplier IS NOT NULL ORDER BY id"
        ).fetchall()
    multiplier = 1.0
    for row in legacy:
        multiplier *= max(0.0, float(row["paper_nav_multiplier"] or 1.0))
    for row in events:
        multiplier *= max(0.0, float(row["paper_nav_multiplier"] or 1.0))
    return self.starting_nav_usd * multiplier


def _open_exposure_with_lifecycle(self: Any) -> float:
    if _BASE_OPEN_EXPOSURE is None:
        raise RuntimeError("v52_robinhood_lifecycle_exposure_base_missing")
    _ensure_schema(self)
    with self.store._lock:
        managed = self.store.db.execute(
            "SELECT COALESCE(SUM(remaining_fraction),0) AS total FROM v52_robinhood_positions "
            "WHERE status IN ('entered','scaling','de_risking','runner')"
        ).fetchone()
        legacy = self.store.db.execute(
            "SELECT COALESCE(SUM(t.position_fraction),0) AS total "
            "FROM robinhood_paper_trials t LEFT JOIN robinhood_paper_outcomes o ON o.trial_id=t.id "
            "LEFT JOIN v52_robinhood_position_lots l ON l.trial_id=t.id "
            "WHERE t.paper_only=1 AND o.id IS NULL AND l.id IS NULL"
        ).fetchone()
    total = float(managed["total"] or 0.0) + float(legacy["total"] or 0.0)
    return min(1.0, max(0.0, total))


async def _position_flow_state(self: Any, position: dict[str, Any]) -> str:
    venue = str(position["venue"])
    market = _clean_address(position["market"])
    try:
        if venue in {"PONS_V1_UNISWAP_V3", "UNISWAP_V3_DIRECT"}:
            pool = self.v3_pools.get(market)
            if pool is None:
                return "neutral"
            metrics = await self._v5_flow_metrics(pool.recent_swaps, deployer=pool.deployer)
            return str(metrics.get("state") or "neutral")
        if venue == "PONS_V2_CURVE":
            curve = self.v2_curves.get(market)
            if curve is None:
                return "neutral"
            metrics = await self._v5_flow_metrics(curve.recent_swaps, deployer=curve.deployer)
            return str(metrics.get("state") or "neutral")
    except Exception:
        return "neutral"
    return "neutral"


def _leader_trial_id(owner: Any, position_id: int) -> int | None:
    with owner.store._lock:
        row = owner.store.db.execute(
            "SELECT trial_id FROM v52_robinhood_position_lots "
            "WHERE position_id=? AND remaining_fraction>0 ORDER BY id LIMIT 1",
            (position_id,),
        ).fetchone()
    return int(row["trial_id"]) if row is not None else None


async def _settle_one_with_lifecycle(self: Any, trial: dict[str, Any]) -> None:
    if _BASE_SETTLE_ONE is None:
        raise RuntimeError("v52_robinhood_lifecycle_settle_base_missing")
    _ensure_schema(self)
    trial_id = int(trial["id"])
    with self.store._lock:
        lot = self.store.db.execute(
            "SELECT l.*,p.token,p.market,p.venue,p.lifecycle,p.status,p.target_fraction,"
            "p.remaining_fraction,p.remaining_token_raw,p.derisk_stage,p.runner_active,p.opened_at "
            "FROM v52_robinhood_position_lots l JOIN v52_robinhood_positions p ON p.id=l.position_id "
            "WHERE l.trial_id=? LIMIT 1",
            (trial_id,),
        ).fetchone()
    if lot is None:
        await _BASE_SETTLE_ONE(self, trial)
        return
    item = dict(lot)
    position_id = int(item["position_id"])
    leader = _leader_trial_id(self, position_id)
    if leader is None or leader != trial_id:
        return
    with self.store._lock:
        raw_position = self.store.db.execute(
            "SELECT * FROM v52_robinhood_positions WHERE id=? LIMIT 1",
            (position_id,),
        ).fetchone()
        cost_rows = self.store.db.execute(
            "SELECT remaining_entry_cost_wei FROM v52_robinhood_position_lots "
            "WHERE position_id=? AND remaining_fraction>0 ORDER BY id",
            (position_id,),
        ).fetchall()
    if raw_position is None:
        return
    position = dict(raw_position)
    remaining_raw = max(0, int(position["remaining_token_raw"] or 0))
    remaining_fraction = max(0.0, float(position["remaining_fraction"] or 0.0))
    remaining_cost = sum(max(0, int(row["remaining_entry_cost_wei"] or 0)) for row in cost_rows)
    if remaining_raw <= 0 or remaining_fraction <= 0.0 or remaining_cost <= 0:
        return
    try:
        aggregate_quote = await _sell_quote_for_raw(
            self,
            token=_clean_address(position["token"]),
            market=_clean_address(position["market"]),
            venue=str(position["venue"]),
            raw_amount=remaining_raw,
        )
    except Exception:
        return
    if aggregate_quote is None or int(aggregate_quote["gross_out_wei"]) <= 0:
        return
    net_return = int(aggregate_quote["net_out_wei"]) / max(1, remaining_cost) - 1.0
    flow_state = await _position_flow_state(self, position)
    policy = self._v5_learned_exit_policy(trial)
    opened = _parse_time(position.get("opened_at"))
    elapsed = (
        max(0.0, (datetime.now(timezone.utc) - opened).total_seconds())
        if opened is not None
        else 0.0
    )
    stop = float(policy["stop"])
    harvest = float(policy["harvest"])
    max_hold = float(policy["max_hold"])
    status = str(position["status"])
    derisk_stage = int(position["derisk_stage"] or 0)
    healthy = flow_state in {"entity_accumulation", "pre_fomo", "active_fomo"} and net_return > stop

    sell_raw = 0
    event_type = "hold"
    next_status = status
    next_stage = derisk_stage
    reason = "no_exit_trigger"

    if status == "runner":
        if healthy:
            return
        sell_raw = remaining_raw
        event_type = "exit_runner"
        next_status = "closed"
        reason = "runner_conditions_failed"
    elif net_return <= stop:
        sell_raw = remaining_raw
        event_type = "full_exit"
        next_status = "closed"
        reason = f"{policy['source']}:stop_loss"
    elif elapsed >= max_hold:
        sell_raw = remaining_raw
        event_type = "full_exit"
        next_status = "closed"
        reason = f"{policy['source']}:max_hold"
    else:
        deterioration = flow_state == "exhaustion" or net_return >= harvest
        if deterioration and derisk_stage == 0:
            sell_raw = max(1, int(round(remaining_raw * float(position_policy()["first_derisk_fraction_of_position"]))))
            event_type = "stage_derisk_1"
            next_status = "de_risking"
            next_stage = 1
            reason = "flow_deterioration" if flow_state == "exhaustion" else f"{policy['source']}:harvest_derisk_1"
        elif deterioration and derisk_stage == 1:
            sell_raw = max(1, int(round(remaining_raw * float(position_policy()["second_derisk_fraction_of_position"]))))
            event_type = "stage_derisk_2"
            next_status = "de_risking"
            next_stage = 2
            reason = "persistent_flow_deterioration" if flow_state == "exhaustion" else f"{policy['source']}:harvest_derisk_2"
        elif derisk_stage >= 2:
            if healthy:
                runner_fraction = min(
                    remaining_fraction,
                    float(position["target_fraction"]) * float(position_policy()["runner_fraction_of_target"]),
                )
                keep_ratio = runner_fraction / max(remaining_fraction, 1e-12)
                keep_raw = max(0, int(round(remaining_raw * keep_ratio)))
                sell_raw = max(0, remaining_raw - keep_raw)
                if sell_raw <= 0:
                    with self.store._lock, self.store.db:
                        self.store.db.execute(
                            "UPDATE v52_robinhood_positions SET status='runner',runner_active=1,updated_at=? WHERE id=?",
                            (_utcnow(), position_id),
                        )
                    return
                event_type = "enter_runner"
                next_status = "runner"
                next_stage = 2
                reason = "runner_conditions_healthy"
            else:
                sell_raw = remaining_raw
                event_type = "full_exit"
                next_status = "closed"
                next_stage = 2
                reason = "deterioration_persisted_after_staged_derisk"
        else:
            return

    sell_raw = min(remaining_raw, max(0, int(sell_raw)))
    if sell_raw <= 0:
        return
    try:
        exact = (
            aggregate_quote
            if sell_raw == remaining_raw
            else await _sell_quote_for_raw(
                self,
                token=_clean_address(position["token"]),
                market=_clean_address(position["market"]),
                venue=str(position["venue"]),
                raw_amount=sell_raw,
            )
        )
    except Exception:
        return
    if exact is None or int(exact["gross_out_wei"]) <= 0:
        return
    _apply_exit(
        self,
        position=position,
        sell_raw=sell_raw,
        exact=exact,
        event_type=event_type,
        next_status=next_status,
        next_stage=next_stage,
        reason=reason,
        flow_state=flow_state,
    )


def _apply_exit(
    owner: Any,
    *,
    position: dict[str, Any],
    sell_raw: int,
    exact: dict[str, int],
    event_type: str,
    next_status: str,
    next_stage: int,
    reason: str,
    flow_state: str,
) -> None:
    position_id = int(position["id"])
    with owner.store._lock:
        raw_lots = owner.store.db.execute(
            "SELECT l.*,t.token,t.market,t.venue,t.lifecycle,t.trigger_actor,t.trigger_entity,t.fomo_state "
            "FROM v52_robinhood_position_lots l JOIN robinhood_paper_trials t ON t.id=l.trial_id "
            "WHERE l.position_id=? AND l.remaining_fraction>0 ORDER BY l.id",
            (position_id,),
        ).fetchall()
    lots = [dict(row) for row in raw_lots]
    total_raw = sum(max(0, int(row["remaining_token_raw"] or 0)) for row in lots)
    if total_raw <= 0 or sell_raw > total_raw:
        return
    net_out = max(0, int(exact["net_out_wei"]))
    gas = max(0, int(exact["exit_gas_wei"]))
    raw_remaining_to_allocate = sell_raw
    net_remaining = net_out
    gas_remaining = gas
    allocations: list[dict[str, Any]] = []
    for index, lot in enumerate(lots):
        lot_raw = max(0, int(lot["remaining_token_raw"] or 0))
        if lot_raw <= 0:
            continue
        if index == len(lots) - 1:
            alloc_raw = min(lot_raw, raw_remaining_to_allocate)
        else:
            alloc_raw = min(
                lot_raw,
                int(math.floor(sell_raw * lot_raw / max(1, total_raw))),
            )
        if alloc_raw <= 0:
            continue
        raw_remaining_to_allocate -= alloc_raw
        if raw_remaining_to_allocate < 0:
            alloc_raw += raw_remaining_to_allocate
            raw_remaining_to_allocate = 0
        ratio = alloc_raw / max(1, sell_raw)
        alloc_net = net_remaining if raw_remaining_to_allocate == 0 else int(round(net_out * ratio))
        alloc_gas = gas_remaining if raw_remaining_to_allocate == 0 else int(round(gas * ratio))
        alloc_net = min(net_remaining, max(0, alloc_net))
        alloc_gas = min(gas_remaining, max(0, alloc_gas))
        net_remaining -= alloc_net
        gas_remaining -= alloc_gas
        lot_ratio = alloc_raw / max(1, lot_raw)
        alloc_fraction = float(lot["remaining_fraction"] or 0.0) * lot_ratio
        alloc_cost = int(round(int(lot["remaining_entry_cost_wei"] or 0) * lot_ratio))
        allocations.append(
            {
                "lot": lot,
                "raw": alloc_raw,
                "fraction": alloc_fraction,
                "cost": alloc_cost,
                "net": alloc_net,
                "gas": alloc_gas,
            }
        )
        if raw_remaining_to_allocate == 0:
            break
    if raw_remaining_to_allocate > 0:
        return

    realized_fraction = sum(item["fraction"] for item in allocations)
    allocated_cost = sum(item["cost"] for item in allocations)
    event_return = net_out / max(1, allocated_cost) - 1.0
    nav_multiplier = max(0.0, 1.0 + realized_fraction * event_return)
    now = _utcnow()
    state_before = str(position["status"])
    remaining_raw_after = max(0, int(position["remaining_token_raw"] or 0) - sell_raw)
    remaining_fraction_after = max(0.0, float(position["remaining_fraction"] or 0.0) - realized_fraction)
    if remaining_raw_after == 0 or remaining_fraction_after <= 1e-12:
        next_status = "closed"
        remaining_raw_after = 0
        remaining_fraction_after = 0.0
    event_key = f"exit:{position_id}:{int(position['derisk_stage'] or 0)}:{event_type}:{sell_raw}:{position.get('updated_at')}"

    closed_lots: list[dict[str, Any]] = []
    with owner.store._lock:
        owner.store.db.execute("BEGIN IMMEDIATE")
        try:
            for item in allocations:
                lot = item["lot"]
                new_raw = max(0, int(lot["remaining_token_raw"] or 0) - int(item["raw"]))
                new_fraction = max(0.0, float(lot["remaining_fraction"] or 0.0) - float(item["fraction"]))
                new_cost = max(0, int(lot["remaining_entry_cost_wei"] or 0) - int(item["cost"]))
                realized_net = int(lot["realized_exit_net_wei"] or 0) + int(item["net"])
                realized_gas = int(lot["realized_exit_gas_wei"] or 0) + int(item["gas"])
                owner.store.db.execute(
                    "UPDATE v52_robinhood_position_lots SET remaining_fraction=?,remaining_token_raw=?,"
                    "remaining_entry_cost_wei=?,realized_exit_net_wei=?,realized_exit_gas_wei=?,"
                    "closed_at=CASE WHEN ?=0 THEN ? ELSE closed_at END WHERE id=?",
                    (
                        new_fraction,
                        str(new_raw),
                        str(new_cost),
                        str(realized_net),
                        str(realized_gas),
                        new_raw,
                        now,
                        int(lot["id"]),
                    ),
                )
                if new_raw == 0:
                    closed_lots.append(
                        {
                            **lot,
                            "realized_exit_net_wei": realized_net,
                            "realized_exit_gas_wei": realized_gas,
                        }
                    )

            owner.store.db.execute(
                "UPDATE v52_robinhood_positions SET status=?,remaining_fraction=?,remaining_token_raw=?,"
                "derisk_stage=?,runner_active=?,last_flow_state=?,updated_at=?,"
                "closed_at=CASE WHEN ?='closed' THEN ? ELSE closed_at END,"
                "close_reason=CASE WHEN ?='closed' THEN ? ELSE close_reason END WHERE id=?",
                (
                    next_status,
                    remaining_fraction_after,
                    str(remaining_raw_after),
                    int(next_stage),
                    1 if next_status == "runner" else 0,
                    flow_state,
                    now,
                    next_status,
                    now,
                    next_status,
                    reason,
                    position_id,
                ),
            )
            owner.store.db.execute(
                "INSERT OR IGNORE INTO v52_robinhood_position_events("
                "position_id,event_key,event_type,state_before,state_after,token_raw,position_fraction,"
                "exit_quote_out_wei,exit_gas_wei,allocated_entry_cost_wei,net_return,paper_nav_multiplier,"
                "reason,quote_json,created_at,authority_id,strategy_version,paper_only,live_money_authority"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    position_id,
                    event_key,
                    event_type,
                    state_before,
                    next_status,
                    str(sell_raw),
                    realized_fraction,
                    str(int(exact["gross_out_wei"])),
                    str(int(exact["exit_gas_wei"])),
                    str(allocated_cost),
                    float(event_return),
                    float(nav_multiplier),
                    reason,
                    json.dumps(exact, sort_keys=True, separators=(",", ":")),
                    now,
                    AUTHORITY_ID,
                    STRATEGY_VERSION,
                ),
            )

            for lot in closed_lots:
                trial_id = int(lot["trial_id"])
                trial = owner.store.db.execute(
                    "SELECT * FROM robinhood_paper_trials WHERE id=? LIMIT 1",
                    (trial_id,),
                ).fetchone()
                if trial is None:
                    continue
                triald = dict(trial)
                total_cost = max(1, int(lot["entry_total_cost_wei"] or 0))
                lot_return = int(lot["realized_exit_net_wei"]) / total_cost - 1.0
                multiplier = max(0.0, 1.0 + float(lot["entry_fraction"]) * lot_return)
                owner.store.db.execute(
                    "INSERT OR IGNORE INTO robinhood_paper_outcomes("
                    "release_commit,trial_id,token,market,venue,lifecycle,trigger_actor,trigger_entity,fomo_state,"
                    "position_fraction,net_return,paper_nav_multiplier,exit_quote_out_wei,exit_gas_wei,"
                    "exit_reason,settled_at,paper_only,live_money_authority"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                    (
                        str(triald["release_commit"]),
                        trial_id,
                        str(triald["token"]),
                        str(triald["market"]),
                        str(triald["venue"]),
                        str(triald["lifecycle"]),
                        str(triald["trigger_actor"]),
                        str(triald["trigger_entity"]),
                        str(triald["fomo_state"]),
                        float(triald["position_fraction"]),
                        float(lot_return),
                        float(multiplier),
                        str(int(lot["realized_exit_net_wei"])),
                        str(int(lot["realized_exit_gas_wei"])),
                        reason,
                        now,
                    ),
                )
            owner.store.db.commit()
        except Exception:
            if owner.store.db.in_transaction:
                owner.store.db.rollback()
            raise


def lifecycle_status(owner: Any | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "version": LIFECYCLE_VERSION,
        "installed": _INSTALLED,
        "authority_id": AUTHORITY_ID,
        "strategy_version": STRATEGY_VERSION,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "durable_lot_inventory": True,
        "aggregate_exact_exitability_before_add": True,
        "stressed_exit_capacity_before_entry_or_add": True,
        "stressed_exit_coverage_ratio": STRESSED_EXIT_COVERAGE_RATIO,
        "scale_requires_new_forward_strength": True,
        "averaging_down_allowed": False,
        "staged_derisk_runner_authority": True,
        "second_leg_reentry_requires_new_impulse": True,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    if owner is not None:
        try:
            _ensure_schema(owner)
            with owner.store._lock:
                open_count = owner.store.db.execute(
                    "SELECT COUNT(*) AS n FROM v52_robinhood_positions "
                    "WHERE status IN ('entered','scaling','de_risking','runner')"
                ).fetchone()
                lot_count = owner.store.db.execute(
                    "SELECT COUNT(*) AS n FROM v52_robinhood_position_lots"
                ).fetchone()
                event_count = owner.store.db.execute(
                    "SELECT COUNT(*) AS n FROM v52_robinhood_position_events"
                ).fetchone()
            payload.update(
                {
                    "open_positions": int(open_count["n"] or 0),
                    "lot_count": int(lot_count["n"] or 0),
                    "event_count": int(event_count["n"] or 0),
                }
            )
        except Exception as exc:
            payload["accounting_status_error"] = f"{type(exc).__name__}: lifecycle accounting unavailable"
    return payload


def install_v52_robinhood_position_lifecycle() -> None:
    global _INSTALLED, _BASE_CHOOSE, _BASE_MAYBE_V3, _BASE_MAYBE_V2
    global _BASE_TOKEN_OPEN, _BASE_SETTLE_ONE, _BASE_PAPER_NAV, _BASE_OPEN_EXPOSURE
    if _INSTALLED:
        return
    from .robinhood_chain_paper import RobinhoodChainPaperPlane

    _BASE_CHOOSE = RobinhoodProfitMaximizerMixin._v5_choose_lane_fraction
    _BASE_MAYBE_V3 = RobinhoodChainPaperPlane._maybe_open_v3
    _BASE_MAYBE_V2 = RobinhoodChainPaperPlane._maybe_open_v2
    _BASE_TOKEN_OPEN = RobinhoodChainPaperPlane._token_open
    _BASE_SETTLE_ONE = RobinhoodChainPaperPlane._settle_one
    _BASE_PAPER_NAV = RobinhoodChainPaperPlane._paper_nav_usd
    _BASE_OPEN_EXPOSURE = RobinhoodChainPaperPlane._open_exposure

    RobinhoodProfitMaximizerMixin._v5_choose_lane_fraction = _choose_with_lifecycle  # type: ignore[method-assign]
    RobinhoodChainPaperPlane._maybe_open_v3 = _maybe_open_v3_with_lifecycle  # type: ignore[method-assign]
    RobinhoodChainPaperPlane._maybe_open_v2 = _maybe_open_v2_with_lifecycle  # type: ignore[method-assign]
    RobinhoodChainPaperPlane._token_open = _token_open_with_v52_recheck  # type: ignore[method-assign]
    RobinhoodChainPaperPlane._settle_one = _settle_one_with_lifecycle  # type: ignore[method-assign]
    RobinhoodChainPaperPlane._paper_nav_usd = _paper_nav_with_lifecycle  # type: ignore[method-assign]
    RobinhoodChainPaperPlane._open_exposure = _open_exposure_with_lifecycle  # type: ignore[method-assign]

    setattr(RobinhoodProfitMaximizerMixin._v5_choose_lane_fraction, "_roi_v52_final_authority", True)
    setattr(RobinhoodProfitMaximizerMixin._v5_choose_lane_fraction, "_roi_v52_position_lifecycle", True)
    setattr(RobinhoodChainPaperPlane._settle_one, "_roi_v52_position_lifecycle", True)
    setattr(RobinhoodChainPaperPlane._maybe_open_v3, "_roi_v52_position_lifecycle", True)
    setattr(RobinhoodChainPaperPlane._maybe_open_v2, "_roi_v52_position_lifecycle", True)
    _INSTALLED = True


__all__ = [
    "LIFECYCLE_VERSION",
    "MIN_REENTRY_CONSOLIDATION_SECONDS",
    "STRESSED_EXIT_COVERAGE_RATIO",
    "install_v52_robinhood_position_lifecycle",
    "lifecycle_status",
]
