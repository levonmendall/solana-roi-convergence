from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any


CONTEXT_VERSION = "continuation-market-context-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def market_cap_band(value_usd: float | None) -> str:
    """Descriptive only; market cap never directly authorizes or sizes an entry."""
    if value_usd is None or not math.isfinite(float(value_usd)) or float(value_usd) <= 0.0:
        return "unknown"
    value = float(value_usd)
    if value < 100_000.0:
        return "ultra_micro_lt_100k"
    if value < 500_000.0:
        return "micro_100k_500k"
    if value < 2_000_000.0:
        return "small_500k_2m"
    if value < 10_000_000.0:
        return "developing_2m_10m"
    return "developed_ge_10m"


def price_sensitivity_band(price_change_per_sol: float | None) -> str:
    """Descriptive response-to-flow band, not an entry veto."""
    if price_change_per_sol is None or not math.isfinite(float(price_change_per_sol)):
        return "unknown"
    value = abs(float(price_change_per_sol))
    if value < 0.01:
        return "low_lt_1pct_per_sol"
    if value < 0.03:
        return "moderate_1_3pct_per_sol"
    if value < 0.10:
        return "high_3_10pct_per_sol"
    return "extreme_ge_10pct_per_sol"


def _flow_context(store: Any, token_mint: str, *, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    start = (now - timedelta(seconds=60)).isoformat()
    end = now.isoformat()
    try:
        with store._lock:
            rows = store.db.execute(
                "SELECT side,native_amount_sol,reference_price_sol,received_at FROM normalized_swaps "
                "WHERE token_mint=? AND received_at>=? AND received_at<=? ORDER BY received_at,id",
                (token_mint, start, end),
            ).fetchall()
    except Exception:
        rows = []
    prices = [
        float(row["reference_price_sol"])
        for row in rows
        if _finite(row["reference_price_sol"]) is not None and float(row["reference_price_sol"]) > 0.0
    ]
    gross_sol = sum(max(0.0, float(row["native_amount_sol"] or 0.0)) for row in rows)
    buy_sol = sum(
        max(0.0, float(row["native_amount_sol"] or 0.0))
        for row in rows
        if str(row["side"] or "").lower() == "buy"
    )
    sell_sol = sum(
        max(0.0, float(row["native_amount_sol"] or 0.0))
        for row in rows
        if str(row["side"] or "").lower() == "sell"
    )
    price_change = None
    if len(prices) >= 2 and prices[0] > 0.0:
        price_change = prices[-1] / prices[0] - 1.0
    per_sol = (price_change / gross_sol) if price_change is not None and gross_sol > 0.0 else None
    return {
        "window_seconds": 60,
        "swap_count": len(rows),
        "gross_flow_sol": gross_sol,
        "buy_flow_sol": buy_sol,
        "sell_flow_sol": sell_sol,
        "reference_price_change_fraction": price_change,
        "absolute_price_change_per_gross_sol": abs(per_sol) if per_sol is not None else None,
        "price_sensitivity_band": price_sensitivity_band(per_sol),
    }


async def _supply_units(adapter: Any, token_mint: str) -> float | None:
    cache = getattr(adapter, "_roi_market_supply_cache", None)
    if not isinstance(cache, dict):
        cache = {}
        setattr(adapter, "_roi_market_supply_cache", cache)
    if token_mint in cache:
        return _finite(cache[token_mint])
    try:
        result = await adapter.discovery.rpc.call("getTokenSupply", [token_mint, {"commitment": "confirmed"}])
        value = result.get("value") if isinstance(result, dict) else None
        if not isinstance(value, dict):
            return None
        raw = int(value.get("amount") or 0)
        decimals = int(value.get("decimals") or 0)
        units = raw / float(10**decimals) if raw > 0 and 0 <= decimals <= 18 else None
    except Exception:
        units = None
    if units is not None and math.isfinite(units) and units > 0.0:
        cache[token_mint] = units
        return units
    return None


def _schema(adapter: Any) -> None:
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS continuation_market_context ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, token_mint TEXT NOT NULL, "
            "context_version TEXT NOT NULL, market_cap_usd REAL, market_cap_band TEXT NOT NULL, token_supply_units REAL, "
            "entry_price_sol REAL, sol_usd REAL, position_fraction REAL, round_trip_cost_fraction REAL, "
            "flow_json TEXT NOT NULL, observed_at TEXT NOT NULL, paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "UNIQUE(release_commit,source_signature))"
        )


async def record_candidate_market_context(adapter: Any, row: dict[str, Any]) -> None:
    """Persist market-cap/price-sensitivity context after the strategy has evaluated a candidate.

    This record is joinable to v5 outcomes by source_signature, allowing forward ROI
    analysis by asset scale and response-to-flow without making market cap a direct
    entry or sizing authority.
    """
    signature = str(row.get("signature") or "")
    token_mint = str(row.get("token_mint") or "")
    if not signature or not token_mint:
        return
    _schema(adapter)
    try:
        with adapter.store._lock:
            trial = adapter.store.db.execute(
                "SELECT assigned_position_fraction,entry_all_in_price_sol,round_trip_cost_fraction FROM profit_first_final_trials "
                "WHERE epoch_id=? AND source_signature=? AND lane='unified_profit_maximizer' ORDER BY id DESC LIMIT 1",
                (adapter.epoch_id, signature),
            ).fetchone()
        if trial is None:
            return
        entry_price_sol = _finite(trial["entry_all_in_price_sol"])
        if entry_price_sol is None or entry_price_sol <= 0.0:
            return
        supply = await _supply_units(adapter, token_mint)
        try:
            sol_usd = await adapter._sol_usd()
        except Exception:
            sol_usd = None
        sol_usd_n = _finite(sol_usd)
        market_cap = (
            supply * entry_price_sol * sol_usd_n
            if supply is not None and sol_usd_n is not None and sol_usd_n > 0.0
            else None
        )
        flow = _flow_context(adapter.store, token_mint)
        now = datetime.now(timezone.utc).isoformat()
        with adapter.store._lock, adapter.store.db:
            adapter.store.db.execute(
                "INSERT OR REPLACE INTO continuation_market_context("
                "release_commit,source_signature,token_mint,context_version,market_cap_usd,market_cap_band,token_supply_units,"
                "entry_price_sol,sol_usd,position_fraction,round_trip_cost_fraction,flow_json,observed_at,paper_only,live_money_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    adapter.release_commit, signature, token_mint, CONTEXT_VERSION, market_cap, market_cap_band(market_cap), supply,
                    entry_price_sol, sol_usd_n, float(trial["assigned_position_fraction"] or 0.0),
                    _finite(trial["round_trip_cost_fraction"]), json.dumps(flow, sort_keys=True, separators=(",", ":")), now,
                ),
            )
    except Exception as exc:
        setattr(adapter, "_roi_market_context_last_error", f"{type(exc).__name__}: {exc}")
        return
    setattr(adapter, "_roi_market_context_last_error", None)


def status(adapter: Any) -> dict[str, Any]:
    _schema(adapter)
    with adapter.store._lock:
        count = int(adapter.store.db.execute(
            "SELECT COUNT(*) FROM continuation_market_context WHERE release_commit=?", (adapter.release_commit,),
        ).fetchone()[0])
    return {
        "version": CONTEXT_VERSION,
        "observations": count,
        "dimensions": ["market_cap_usd", "market_cap_band", "price_sensitivity", "gross_flow_sol", "round_trip_cost_fraction"],
        "market_cap_is_direct_entry_veto": False,
        "market_cap_is_direct_size_authority": False,
        "liquidity_price_impact_and_exitability_remain_size_authority": True,
        "forward_outcome_join_key": "release_commit_x_source_signature",
        "last_error": getattr(adapter, "_roi_market_context_last_error", None),
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "CONTEXT_VERSION",
    "market_cap_band",
    "price_sensitivity_band",
    "record_candidate_market_context",
    "status",
]
