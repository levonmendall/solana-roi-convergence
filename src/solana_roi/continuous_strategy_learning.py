from __future__ import annotations

import asyncio
import json
import math
from datetime import datetime, timedelta, timezone
from statistics import mean, median
from typing import Any, Callable, Iterable

from .profit_first_entity_final import UNIFIED_LANE
from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from .wallet_entity_universe_v4 import TokenScopedEntityResolver
from .wallet_venue_lifecycle_research import lifecycle_stage, venue_from_source


LEARNING_VERSION = "continuous-strategy-learning-v1"
FOMO_ENTRY_WINDOWS_SECONDS = (1, 3, 5, 10, 20, 30)
PATH_HORIZONS_SECONDS = (1, 2, 5, 10, 20, 30, 60, 120, 300)
FOMO_POST_ENTRY_FLOW_HORIZONS_SECONDS = (5, 10, 20, 30, 60)
FINAL_PATH_HORIZON_SECONDS = 300
FINAL_PATH_GRACE_SECONDS = 10
MAX_SYNC_SUBJECTS_PER_PASS = 12

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
ACTIVE_STRATEGY_MUTATION_ALLOWED = False
HISTORICAL_PROMOTION_AUTHORITY = False
ADDITIONAL_RPC_FANOUT = False
STRATEGY_RULES_CHANGED = False

_ORIGINAL_OBSERVE: Callable[..., Any] | None = None
_ORIGINAL_SELL: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_MANIFEST: Callable[..., dict[str, Any]] | None = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        result = value
    elif value:
        try:
            result = datetime.fromisoformat(str(value))
        except (TypeError, ValueError):
            return None
    else:
        return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


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
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _ratio(
    short_value: float,
    long_value: float,
    *,
    short_seconds: float = 5.0,
    long_seconds: float = 20.0,
) -> float:
    short_rate = max(0.0, short_value) / max(1e-9, float(short_seconds))
    long_rate = max(0.0, long_value) / max(1e-9, float(long_seconds))
    if long_rate <= 0.0:
        return 1.0 if short_rate > 0.0 else 0.0
    return short_rate / long_rate


def _trimmed_ex_best(values: list[float], n: int = 1) -> float | None:
    if len(values) <= n:
        return None
    remaining = sorted(values, reverse=True)[n:]
    return mean(remaining) if remaining else None


def _return_stats(values: Iterable[float]) -> dict[str, Any]:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    trimmed = _trimmed_ex_best(clean, 1)
    return {
        "sample_count": len(clean),
        "mean_residual_roi_pct": mean(clean) * 100.0 if clean else None,
        "median_residual_roi_pct": median(clean) * 100.0 if clean else None,
        "trimmed_mean_residual_roi_ex_best_1_pct": trimmed * 100.0 if trimmed is not None else None,
        "positive_rate_pct": (
            sum(value > 0.0 for value in clean) / len(clean) * 100.0
            if clean
            else None
        ),
    }


def _window_stats(
    rows: list[dict[str, Any]],
    *,
    entity_mapping: dict[str, str],
    excluded_entities: set[str],
    qualified_momentum_wallets: set[str],
    creator_entity: str | None = None,
) -> dict[str, Any]:
    buys = sells = 0
    buy_volume = sell_volume = 0.0
    independent_buyers: set[str] = set()
    momentum_wallets: set[str] = set()
    creator_buy_volume = creator_sell_volume = 0.0

    for row in rows:
        wallet = str(row.get("wallet") or "")
        side = str(row.get("side") or "").lower()
        amount = max(0.0, float(row.get("token_amount") or 0.0))
        price = _finite(row.get("copyable_price_sol"))
        if price is None:
            price = _finite(row.get("wallet_price_sol"))
        notional = amount * max(0.0, float(price or 0.0))
        entity = entity_mapping.get(wallet, f"address:{wallet}")

        if side == "buy":
            buys += 1
            buy_volume += notional
            if wallet and entity not in excluded_entities:
                independent_buyers.add(entity)
            if wallet in qualified_momentum_wallets:
                momentum_wallets.add(wallet)
            if creator_entity is not None and entity == creator_entity:
                creator_buy_volume += notional
        elif side == "sell":
            sells += 1
            sell_volume += notional
            if creator_entity is not None and entity == creator_entity:
                creator_sell_volume += notional

    return {
        "independent_buyers": len(independent_buyers),
        "buys": buys,
        "sells": sells,
        "buy_volume": buy_volume,
        "sell_volume": sell_volume,
        "net_buy_volume": buy_volume - sell_volume,
        "momentum_wallet_participation": len(momentum_wallets),
        "creator_buy_volume": creator_buy_volume,
        "creator_sell_volume": creator_sell_volume,
    }


def _path_metrics(
    *,
    reference_price_sol: float | None,
    reference_at: datetime,
    marks: list[dict[str, Any]],
    realized_exit_return: float | None = None,
) -> dict[str, Any]:
    reference = _finite(reference_price_sol)
    samples: list[tuple[float, float]] = []
    if reference is not None and reference > 0.0:
        for row in marks:
            price = _finite(row.get("price_sol"))
            at = _dt(row.get("received_at"))
            if price is None or price <= 0.0 or at is None or at < reference_at:
                continue
            samples.append(((at - reference_at).total_seconds(), price / reference - 1.0))

    if not samples:
        return {
            "mark_count": 0,
            "mfe_mark_return_pct": None,
            "mae_mark_return_pct": None,
            "time_to_mfe_seconds": None,
            "time_to_mae_seconds": None,
            "mark_mfe_minus_realized_exit_return_pct": None,
        }

    mfe_time, mfe = max(samples, key=lambda item: item[1])
    mae_time, mae = min(samples, key=lambda item: item[1])
    giveback = None
    if realized_exit_return is not None and math.isfinite(float(realized_exit_return)):
        giveback = max(0.0, mfe - float(realized_exit_return))
    return {
        "mark_count": len(samples),
        "mfe_mark_return_pct": mfe * 100.0,
        "mae_mark_return_pct": mae * 100.0,
        "time_to_mfe_seconds": max(0.0, mfe_time),
        "time_to_mae_seconds": max(0.0, mae_time),
        "mark_mfe_minus_realized_exit_return_pct": giveback * 100.0 if giveback is not None else None,
    }


def _variant_performance_from_rows(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[float]] = {}
    for row in rows:
        payload = _safe_json(row.get("state_json"))
        value = _finite(row.get("net_return"))
        if value is None:
            continue
        for variant in payload.get("experiment_variants") or ():
            key = str(variant)
            if key:
                grouped.setdefault(key, []).append(value)
    return {key: _return_stats(values) for key, values in sorted(grouped.items())}


def _inherit_markers(wrapper: Callable[..., Any], wrapped: Callable[..., Any]) -> None:
    try:
        wrapper.__dict__.update(getattr(wrapped, "__dict__", {}))
    except Exception:
        pass


def _ensure_schema(adapter: FinalProfitFirstResearchAdapter) -> None:
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS strategy_learning_subjects ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_id TEXT NOT NULL, release_commit TEXT NOT NULL, "
            "source_signature TEXT NOT NULL, token_mint TEXT NOT NULL, trigger_wallet TEXT NOT NULL, "
            "signal_received_at TEXT NOT NULL, reference_at TEXT NOT NULL, reference_price_sol REAL, "
            "reference_price_kind TEXT NOT NULL, entry_executable INTEGER NOT NULL, exit_executable INTEGER NOT NULL, "
            "venue TEXT NOT NULL, lifecycle TEXT NOT NULL, regime TEXT NOT NULL, fomo_state TEXT NOT NULL, "
            "creator_wallet TEXT, qualified_momentum_wallets_json TEXT NOT NULL, opportunity_json TEXT NOT NULL, "
            "decision_json TEXT NOT NULL, created_at TEXT NOT NULL, paper_only INTEGER NOT NULL, "
            "live_money_authority INTEGER NOT NULL, UNIQUE(epoch_id,source_signature))"
        )
        adapter.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_strategy_learning_subjects_due "
            "ON strategy_learning_subjects(epoch_id,reference_at,id)"
        )
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS strategy_learning_horizon_marks ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_id TEXT NOT NULL, release_commit TEXT NOT NULL, "
            "source_signature TEXT NOT NULL, token_mint TEXT NOT NULL, horizon_seconds INTEGER NOT NULL, "
            "target_at TEXT NOT NULL, mark_observed_at TEXT NOT NULL, mark_received_at TEXT NOT NULL, "
            "mark_delay_seconds REAL NOT NULL, price_sol REAL NOT NULL, mark_return_to_reference REAL, "
            "reference_price_kind TEXT NOT NULL, source TEXT NOT NULL, source_ref TEXT, created_at TEXT NOT NULL, "
            "UNIQUE(epoch_id,source_signature,horizon_seconds))"
        )
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS strategy_learning_exit_paths ("
            "epoch_id TEXT NOT NULL, release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, token_mint TEXT NOT NULL, "
            "reference_at TEXT NOT NULL, exit_observed_at TEXT NOT NULL, realized_exit_return REAL NOT NULL, "
            "mark_count INTEGER NOT NULL, mfe_mark_return REAL, mae_mark_return REAL, time_to_mfe_seconds REAL, "
            "time_to_mae_seconds REAL, mark_mfe_minus_realized_exit_return REAL, created_at TEXT NOT NULL, "
            "PRIMARY KEY(epoch_id,source_signature))"
        )
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS strategy_learning_final_paths ("
            "epoch_id TEXT NOT NULL, release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, token_mint TEXT NOT NULL, "
            "reference_at TEXT NOT NULL, horizon_seconds INTEGER NOT NULL, mark_count INTEGER NOT NULL, "
            "captured_horizon_count INTEGER NOT NULL, mfe_mark_return REAL, mae_mark_return REAL, "
            "time_to_mfe_seconds REAL, time_to_mae_seconds REAL, finalized_at TEXT NOT NULL, "
            "PRIMARY KEY(epoch_id,source_signature))"
        )
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS fomo_learning_entry_windows ("
            "release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, token_mint TEXT NOT NULL, "
            "window_seconds INTEGER NOT NULL, signal_at TEXT NOT NULL, independent_buyers INTEGER NOT NULL, "
            "buys INTEGER NOT NULL, sells INTEGER NOT NULL, buy_volume REAL NOT NULL, sell_volume REAL NOT NULL, "
            "net_buy_volume REAL NOT NULL, momentum_wallet_participation INTEGER NOT NULL, created_at TEXT NOT NULL, "
            "PRIMARY KEY(release_commit,source_signature,window_seconds))"
        )
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS fomo_learning_post_entry_flow ("
            "release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, token_mint TEXT NOT NULL, "
            "horizon_seconds INTEGER NOT NULL, target_at TEXT NOT NULL, independent_buyers_short INTEGER NOT NULL, "
            "independent_buyers_long INTEGER NOT NULL, buys_short INTEGER NOT NULL, buys_long INTEGER NOT NULL, "
            "sells_short INTEGER NOT NULL, sells_long INTEGER NOT NULL, buy_volume_short REAL NOT NULL, "
            "buy_volume_long REAL NOT NULL, sell_volume_short REAL NOT NULL, sell_volume_long REAL NOT NULL, "
            "new_buyer_acceleration REAL NOT NULL, transaction_frequency_acceleration REAL NOT NULL, "
            "net_buy_flow_acceleration REAL NOT NULL, buy_sell_imbalance REAL NOT NULL, "
            "independent_demand_persistence REAL NOT NULL, momentum_wallet_participation INTEGER NOT NULL, "
            "creator_buy_volume_short REAL NOT NULL, creator_sell_volume_short REAL NOT NULL, "
            "creator_distributing INTEGER NOT NULL, created_at TEXT NOT NULL, "
            "PRIMARY KEY(release_commit,source_signature,horizon_seconds))"
        )


def _unified_trial(adapter: FinalProfitFirstResearchAdapter, signature: str) -> dict[str, Any] | None:
    with adapter.store._lock:
        row = adapter.store.db.execute(
            "SELECT source_signature,token_mint,trigger_wallet,observed_at,received_at,regime,opportunity_json,"
            "decision_json,entry_all_in_price_sol,signal_to_entry_seconds,entry_executable,exit_executable "
            "FROM profit_first_final_trials WHERE epoch_id=? AND source_signature=? AND lane=? "
            "ORDER BY id DESC LIMIT 1",
            (adapter.epoch_id, signature, UNIFIED_LANE),
        ).fetchone()
    return dict(row) if row is not None else None


def _wallet_observation(adapter: FinalProfitFirstResearchAdapter, signature: str) -> dict[str, Any] | None:
    with adapter.store._lock:
        row = adapter.store.db.execute(
            "SELECT signature,token_mint,wallet,copyable_price_sol,wallet_price_sol,source,received_at "
            "FROM wallet_discovery_forward_observations WHERE signature=? LIMIT 1",
            (signature,),
        ).fetchone()
    return dict(row) if row is not None else None


def _fomo_observation(adapter: FinalProfitFirstResearchAdapter, signature: str) -> dict[str, Any] | None:
    try:
        with adapter.store._lock:
            row = adapter.store.db.execute(
                "SELECT source_signature,venue,lifecycle,regime,state_json FROM fomo_shadow_observations "
                "WHERE release_commit=? AND source_signature=? LIMIT 1",
                (adapter.release_commit, signature),
            ).fetchone()
    except Exception:
        return None
    return dict(row) if row is not None else None


def _fallback_venue_lifecycle(
    adapter: FinalProfitFirstResearchAdapter,
    wallet_row: dict[str, Any] | None,
    *,
    token_mint: str,
    at: datetime,
) -> tuple[str, str]:
    venue = venue_from_source(str((wallet_row or {}).get("source") or "")) or "UNKNOWN"
    prior_pump = False
    if venue == "RAYDIUM":
        try:
            with adapter.store._lock:
                rows = adapter.store.db.execute(
                    "SELECT source FROM wallet_discovery_forward_observations "
                    "WHERE token_mint=? AND received_at<=? ORDER BY received_at DESC LIMIT 100",
                    (token_mint, at.isoformat()),
                ).fetchall()
            prior_pump = any(
                venue_from_source(str(row["source"] or "")) in {"PUMP_FUN", "PUMP_AMM"}
                for row in rows
            )
        except Exception:
            prior_pump = False
    return venue, lifecycle_stage(venue, prior_pump_evidence=prior_pump)


def _qualified_momentum_wallets(adapter: FinalProfitFirstResearchAdapter) -> set[str]:
    try:
        from .fomo_runtime_install import _qualified_momentum_wallets as current_policy

        return set(current_policy(adapter))
    except Exception:
        return set()


def _entity_context(
    adapter: FinalProfitFirstResearchAdapter,
    *,
    token_mint: str,
    at: datetime,
    rows: list[dict[str, Any]],
    trigger_wallet: str,
    creator_wallet: str | None,
) -> tuple[dict[str, str], set[str], str | None]:
    addresses = {
        str(row.get("wallet") or "")
        for row in rows
        if str(row.get("wallet") or "")
    }
    addresses.add(trigger_wallet)
    if creator_wallet:
        addresses.add(creator_wallet)
    resolver = getattr(adapter, "_roi_continuous_learning_entity_resolver", None)
    if not isinstance(resolver, TokenScopedEntityResolver):
        resolver = TokenScopedEntityResolver(adapter.discovery)
        setattr(adapter, "_roi_continuous_learning_entity_resolver", resolver)
    try:
        mapping, _ = resolver.components(
            token_mint,
            addresses,
            as_of=at,
            creator_wallet=creator_wallet,
        )
    except Exception:
        mapping = {wallet: f"address:{wallet}" for wallet in addresses if wallet}
    trigger_entity = mapping.get(trigger_wallet, f"address:{trigger_wallet}" if trigger_wallet else "")
    creator_entity = (
        mapping.get(creator_wallet, f"address:{creator_wallet}")
        if creator_wallet
        else None
    )
    excluded = {value for value in (trigger_entity, creator_entity) if value}
    return mapping, excluded, creator_entity


def _rows_between(
    adapter: FinalProfitFirstResearchAdapter,
    token_mint: str,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT wallet,side,token_amount,wallet_price_sol,copyable_price_sol,observed_at,received_at "
            "FROM wallet_discovery_forward_observations WHERE token_mint=? AND received_at>=? AND received_at<=? "
            "ORDER BY received_at,id",
            (token_mint, start.isoformat(), end.isoformat()),
        ).fetchall()
    return [dict(row) for row in rows]


def _record_entry_windows(
    adapter: FinalProfitFirstResearchAdapter,
    subject: dict[str, Any],
) -> None:
    fomo_state = str(subject.get("fomo_state") or "unknown")
    if fomo_state == "unknown":
        return
    signal_at = _dt(subject.get("signal_received_at"))
    if signal_at is None:
        return
    token_mint = str(subject["token_mint"])
    rows = _rows_between(
        adapter,
        token_mint,
        signal_at - timedelta(seconds=max(FOMO_ENTRY_WINDOWS_SECONDS)),
        signal_at,
    )
    qualified = set(_safe_json(subject.get("qualified_momentum_wallets_json")).get("wallets") or ())
    mapping, excluded, _ = _entity_context(
        adapter,
        token_mint=token_mint,
        at=signal_at,
        rows=rows,
        trigger_wallet=str(subject.get("trigger_wallet") or ""),
        creator_wallet=str(subject.get("creator_wallet") or "") or None,
    )
    now = _utcnow().isoformat()
    for window in FOMO_ENTRY_WINDOWS_SECONDS:
        cutoff = signal_at - timedelta(seconds=window)
        bucket = [row for row in rows if (_dt(row.get("received_at")) or signal_at) >= cutoff]
        stats = _window_stats(
            bucket,
            entity_mapping=mapping,
            excluded_entities=excluded,
            qualified_momentum_wallets=qualified,
        )
        with adapter.store._lock, adapter.store.db:
            adapter.store.db.execute(
                "INSERT OR IGNORE INTO fomo_learning_entry_windows("
                "release_commit,source_signature,token_mint,window_seconds,signal_at,independent_buyers,buys,sells,"
                "buy_volume,sell_volume,net_buy_volume,momentum_wallet_participation,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    adapter.release_commit,
                    str(subject["source_signature"]),
                    token_mint,
                    int(window),
                    signal_at.isoformat(),
                    int(stats["independent_buyers"]),
                    int(stats["buys"]),
                    int(stats["sells"]),
                    float(stats["buy_volume"]),
                    float(stats["sell_volume"]),
                    float(stats["net_buy_volume"]),
                    int(stats["momentum_wallet_participation"]),
                    now,
                ),
            )


def _record_subject(adapter: FinalProfitFirstResearchAdapter, signature: str) -> bool:
    _ensure_schema(adapter)
    trial = _unified_trial(adapter, signature)
    if trial is None:
        return False
    wallet_row = _wallet_observation(adapter, signature)
    fomo_row = _fomo_observation(adapter, signature)

    observed_at = _dt(trial.get("observed_at")) or _utcnow()
    signal_at = _dt(trial.get("received_at")) or observed_at
    signal_to_entry = max(0.0, float(trial.get("signal_to_entry_seconds") or 0.0))
    reference_at = observed_at + timedelta(seconds=signal_to_entry)
    executable_price = _finite(trial.get("entry_all_in_price_sol"))
    proxy_price = _finite((wallet_row or {}).get("copyable_price_sol"))
    if proxy_price is None:
        proxy_price = _finite((wallet_row or {}).get("wallet_price_sol"))
    if executable_price is not None and executable_price > 0.0 and bool(trial.get("entry_executable")):
        reference_price = executable_price
        reference_kind = "executable_all_in_entry"
    else:
        reference_price = proxy_price if proxy_price is not None and proxy_price > 0.0 else None
        reference_kind = "counterfactual_observed_price_proxy"

    token_mint = str(trial.get("token_mint") or "")
    trigger_wallet = str(trial.get("trigger_wallet") or "")
    fomo_payload = _safe_json((fomo_row or {}).get("state_json"))
    fomo_state = str(fomo_payload.get("state") or "unknown")
    venue = str((fomo_row or {}).get("venue") or "")
    lifecycle = str((fomo_row or {}).get("lifecycle") or "")
    if not venue or not lifecycle:
        venue, lifecycle = _fallback_venue_lifecycle(
            adapter,
            wallet_row,
            token_mint=token_mint,
            at=signal_at,
        )
    regime = str((fomo_row or {}).get("regime") or trial.get("regime") or "unknown")
    try:
        creator_wallet = adapter.execution._deployer(token_mint, signal_at)
    except Exception:
        creator_wallet = None
    qualified = sorted(_qualified_momentum_wallets(adapter))
    now = _utcnow().isoformat()

    with adapter.store._lock, adapter.store.db:
        cursor = adapter.store.db.execute(
            "INSERT OR IGNORE INTO strategy_learning_subjects("
            "epoch_id,release_commit,source_signature,token_mint,trigger_wallet,signal_received_at,reference_at,"
            "reference_price_sol,reference_price_kind,entry_executable,exit_executable,venue,lifecycle,regime,fomo_state,"
            "creator_wallet,qualified_momentum_wallets_json,opportunity_json,decision_json,created_at,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                adapter.epoch_id,
                adapter.release_commit,
                signature,
                token_mint,
                trigger_wallet,
                signal_at.isoformat(),
                reference_at.isoformat(),
                reference_price,
                reference_kind,
                1 if bool(trial.get("entry_executable")) else 0,
                1 if bool(trial.get("exit_executable")) else 0,
                venue,
                lifecycle,
                regime,
                fomo_state,
                str(creator_wallet) if creator_wallet else None,
                json.dumps({"wallets": qualified}, sort_keys=True, separators=(",", ":")),
                str(trial.get("opportunity_json") or "{}"),
                str(trial.get("decision_json") or "{}"),
                now,
            ),
        )
        row = adapter.store.db.execute(
            "SELECT * FROM strategy_learning_subjects WHERE epoch_id=? AND source_signature=? LIMIT 1",
            (adapter.epoch_id, signature),
        ).fetchone()
    if row is not None:
        _record_entry_windows(adapter, dict(row))
    return bool(cursor.rowcount == 1)


def _horizon_tolerance_seconds(horizon: int) -> float:
    return max(2.0, min(15.0, float(horizon) * 0.25))


def _capture_horizon_marks(
    adapter: FinalProfitFirstResearchAdapter,
    subject: dict[str, Any],
    *,
    now: datetime,
) -> None:
    reference_at = _dt(subject.get("reference_at"))
    if reference_at is None:
        return
    reference_price = _finite(subject.get("reference_price_sol"))
    for horizon in PATH_HORIZONS_SECONDS:
        target = reference_at + timedelta(seconds=horizon)
        if target > now:
            continue
        with adapter.store._lock:
            exists = adapter.store.db.execute(
                "SELECT 1 FROM strategy_learning_horizon_marks WHERE epoch_id=? AND source_signature=? AND horizon_seconds=? LIMIT 1",
                (adapter.epoch_id, str(subject["source_signature"]), int(horizon)),
            ).fetchone()
        if exists is not None:
            continue
        tolerance = _horizon_tolerance_seconds(horizon)
        with adapter.store._lock:
            mark = adapter.store.db.execute(
                "SELECT observed_at,received_at,price_sol,source,source_ref FROM price_marks "
                "WHERE token_mint=? AND received_at>=? AND received_at<=? ORDER BY received_at,id LIMIT 1",
                (
                    str(subject["token_mint"]),
                    target.isoformat(),
                    (target + timedelta(seconds=tolerance)).isoformat(),
                ),
            ).fetchone()
        if mark is None:
            continue
        mark_row = dict(mark)
        mark_received = _dt(mark_row.get("received_at")) or target
        price = float(mark_row["price_sol"])
        return_to_reference = (
            price / reference_price - 1.0
            if reference_price is not None and reference_price > 0.0
            else None
        )
        with adapter.store._lock, adapter.store.db:
            adapter.store.db.execute(
                "INSERT OR IGNORE INTO strategy_learning_horizon_marks("
                "epoch_id,release_commit,source_signature,token_mint,horizon_seconds,target_at,mark_observed_at,mark_received_at,"
                "mark_delay_seconds,price_sol,mark_return_to_reference,reference_price_kind,source,source_ref,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    adapter.epoch_id,
                    adapter.release_commit,
                    str(subject["source_signature"]),
                    str(subject["token_mint"]),
                    int(horizon),
                    target.isoformat(),
                    str(mark_row["observed_at"]),
                    str(mark_row["received_at"]),
                    max(0.0, (mark_received - target).total_seconds()),
                    price,
                    return_to_reference,
                    str(subject["reference_price_kind"]),
                    str(mark_row["source"]),
                    str(mark_row["source_ref"]) if mark_row.get("source_ref") else None,
                    now.isoformat(),
                ),
            )


def _capture_post_entry_flow(
    adapter: FinalProfitFirstResearchAdapter,
    subject: dict[str, Any],
    *,
    now: datetime,
) -> None:
    if str(subject.get("fomo_state") or "unknown") == "unknown":
        return
    reference_at = _dt(subject.get("reference_at"))
    if reference_at is None:
        return
    qualified = set(_safe_json(subject.get("qualified_momentum_wallets_json")).get("wallets") or ())
    for horizon in FOMO_POST_ENTRY_FLOW_HORIZONS_SECONDS:
        target = reference_at + timedelta(seconds=horizon)
        if target > now:
            continue
        with adapter.store._lock:
            exists = adapter.store.db.execute(
                "SELECT 1 FROM fomo_learning_post_entry_flow WHERE release_commit=? AND source_signature=? AND horizon_seconds=? LIMIT 1",
                (adapter.release_commit, str(subject["source_signature"]), int(horizon)),
            ).fetchone()
        if exists is not None:
            continue
        rows = _rows_between(
            adapter,
            str(subject["token_mint"]),
            target - timedelta(seconds=20),
            target,
        )
        mapping, excluded, creator_entity = _entity_context(
            adapter,
            token_mint=str(subject["token_mint"]),
            at=target,
            rows=rows,
            trigger_wallet=str(subject.get("trigger_wallet") or ""),
            creator_wallet=str(subject.get("creator_wallet") or "") or None,
        )
        short_cutoff = target - timedelta(seconds=5)
        short_rows = [row for row in rows if (_dt(row.get("received_at")) or target) >= short_cutoff]
        short = _window_stats(
            short_rows,
            entity_mapping=mapping,
            excluded_entities=excluded,
            qualified_momentum_wallets=qualified,
            creator_entity=creator_entity,
        )
        long = _window_stats(
            rows,
            entity_mapping=mapping,
            excluded_entities=excluded,
            qualified_momentum_wallets=qualified,
            creator_entity=creator_entity,
        )
        short_net = max(0.0, float(short["net_buy_volume"]))
        long_net = max(0.0, float(long["net_buy_volume"]))
        gross = max(0.0, float(short["buy_volume"])) + max(0.0, float(short["sell_volume"]))
        imbalance = (
            (float(short["buy_volume"]) - float(short["sell_volume"])) / gross
            if gross > 0.0
            else 0.0
        )
        persistence = min(
            1.0,
            max(0.0, float(short["independent_buyers"])) / max(1.0, float(long["independent_buyers"])),
        )
        creator_distributing = bool(
            float(short["creator_sell_volume"]) > 0.0
            and float(short["creator_sell_volume"]) > float(short["creator_buy_volume"]) * 1.10
        )
        with adapter.store._lock, adapter.store.db:
            adapter.store.db.execute(
                "INSERT OR IGNORE INTO fomo_learning_post_entry_flow("
                "release_commit,source_signature,token_mint,horizon_seconds,target_at,independent_buyers_short,"
                "independent_buyers_long,buys_short,buys_long,sells_short,sells_long,buy_volume_short,buy_volume_long,"
                "sell_volume_short,sell_volume_long,new_buyer_acceleration,transaction_frequency_acceleration,"
                "net_buy_flow_acceleration,buy_sell_imbalance,independent_demand_persistence,momentum_wallet_participation,"
                "creator_buy_volume_short,creator_sell_volume_short,creator_distributing,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    adapter.release_commit,
                    str(subject["source_signature"]),
                    str(subject["token_mint"]),
                    int(horizon),
                    target.isoformat(),
                    int(short["independent_buyers"]),
                    int(long["independent_buyers"]),
                    int(short["buys"]),
                    int(long["buys"]),
                    int(short["sells"]),
                    int(long["sells"]),
                    float(short["buy_volume"]),
                    float(long["buy_volume"]),
                    float(short["sell_volume"]),
                    float(long["sell_volume"]),
                    _ratio(float(short["independent_buyers"]), float(long["independent_buyers"])),
                    _ratio(float(short["buys"]), float(long["buys"])),
                    _ratio(short_net, long_net),
                    imbalance,
                    persistence,
                    int(short["momentum_wallet_participation"]),
                    float(short["creator_buy_volume"]),
                    float(short["creator_sell_volume"]),
                    1 if creator_distributing else 0,
                    now.isoformat(),
                ),
            )


def _marks_between(
    adapter: FinalProfitFirstResearchAdapter,
    token_mint: str,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT observed_at,received_at,price_sol,source,source_ref FROM price_marks "
            "WHERE token_mint=? AND received_at>=? AND received_at<=? ORDER BY received_at,id LIMIT 5000",
            (token_mint, start.isoformat(), end.isoformat()),
        ).fetchall()
    return [dict(row) for row in rows]


def _capture_exit_path(
    adapter: FinalProfitFirstResearchAdapter,
    subject: dict[str, Any],
    *,
    now: datetime,
) -> None:
    with adapter.store._lock:
        exists = adapter.store.db.execute(
            "SELECT 1 FROM strategy_learning_exit_paths WHERE epoch_id=? AND source_signature=? LIMIT 1",
            (adapter.epoch_id, str(subject["source_signature"])),
        ).fetchone()
    if exists is not None:
        return
    with adapter.store._lock:
        outcome = adapter.store.db.execute(
            "SELECT exit_observed_at,net_return FROM profit_first_final_outcomes "
            "WHERE epoch_id=? AND source_signature=? AND lane=? ORDER BY id DESC LIMIT 1",
            (adapter.epoch_id, str(subject["source_signature"]), UNIFIED_LANE),
        ).fetchone()
    if outcome is None:
        return
    reference_at = _dt(subject.get("reference_at"))
    exit_at = _dt(outcome["exit_observed_at"])
    if reference_at is None or exit_at is None or exit_at < reference_at:
        return
    realized = float(outcome["net_return"])
    marks = _marks_between(adapter, str(subject["token_mint"]), reference_at, exit_at)
    metrics = _path_metrics(
        reference_price_sol=_finite(subject.get("reference_price_sol")),
        reference_at=reference_at,
        marks=marks,
        realized_exit_return=realized,
    )
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "INSERT OR IGNORE INTO strategy_learning_exit_paths("
            "epoch_id,release_commit,source_signature,token_mint,reference_at,exit_observed_at,realized_exit_return,"
            "mark_count,mfe_mark_return,mae_mark_return,time_to_mfe_seconds,time_to_mae_seconds,"
            "mark_mfe_minus_realized_exit_return,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                adapter.epoch_id,
                adapter.release_commit,
                str(subject["source_signature"]),
                str(subject["token_mint"]),
                reference_at.isoformat(),
                exit_at.isoformat(),
                realized,
                int(metrics["mark_count"]),
                _finite(metrics["mfe_mark_return_pct"]) / 100.0 if metrics["mfe_mark_return_pct"] is not None else None,
                _finite(metrics["mae_mark_return_pct"]) / 100.0 if metrics["mae_mark_return_pct"] is not None else None,
                metrics["time_to_mfe_seconds"],
                metrics["time_to_mae_seconds"],
                _finite(metrics["mark_mfe_minus_realized_exit_return_pct"]) / 100.0
                if metrics["mark_mfe_minus_realized_exit_return_pct"] is not None
                else None,
                now.isoformat(),
            ),
        )


def _capture_final_path(
    adapter: FinalProfitFirstResearchAdapter,
    subject: dict[str, Any],
    *,
    now: datetime,
) -> None:
    reference_at = _dt(subject.get("reference_at"))
    if reference_at is None:
        return
    finalize_after = reference_at + timedelta(
        seconds=FINAL_PATH_HORIZON_SECONDS + FINAL_PATH_GRACE_SECONDS
    )
    if now < finalize_after:
        return
    with adapter.store._lock:
        exists = adapter.store.db.execute(
            "SELECT 1 FROM strategy_learning_final_paths WHERE epoch_id=? AND source_signature=? LIMIT 1",
            (adapter.epoch_id, str(subject["source_signature"])),
        ).fetchone()
    if exists is not None:
        return
    end = reference_at + timedelta(seconds=FINAL_PATH_HORIZON_SECONDS)
    marks = _marks_between(adapter, str(subject["token_mint"]), reference_at, end)
    metrics = _path_metrics(
        reference_price_sol=_finite(subject.get("reference_price_sol")),
        reference_at=reference_at,
        marks=marks,
    )
    with adapter.store._lock:
        captured_row = adapter.store.db.execute(
            "SELECT COUNT(*) AS n FROM strategy_learning_horizon_marks WHERE epoch_id=? AND source_signature=?",
            (adapter.epoch_id, str(subject["source_signature"])),
        ).fetchone()
    captured = int(captured_row["n"] or 0) if captured_row is not None else 0
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "INSERT OR IGNORE INTO strategy_learning_final_paths("
            "epoch_id,release_commit,source_signature,token_mint,reference_at,horizon_seconds,mark_count,"
            "captured_horizon_count,mfe_mark_return,mae_mark_return,time_to_mfe_seconds,time_to_mae_seconds,finalized_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                adapter.epoch_id,
                adapter.release_commit,
                str(subject["source_signature"]),
                str(subject["token_mint"]),
                reference_at.isoformat(),
                FINAL_PATH_HORIZON_SECONDS,
                int(metrics["mark_count"]),
                captured,
                _finite(metrics["mfe_mark_return_pct"]) / 100.0 if metrics["mfe_mark_return_pct"] is not None else None,
                _finite(metrics["mae_mark_return_pct"]) / 100.0 if metrics["mae_mark_return_pct"] is not None else None,
                metrics["time_to_mfe_seconds"],
                metrics["time_to_mae_seconds"],
                now.isoformat(),
            ),
        )


def _sync_subject(
    adapter: FinalProfitFirstResearchAdapter,
    subject: dict[str, Any],
    *,
    now: datetime,
) -> None:
    _capture_horizon_marks(adapter, subject, now=now)
    _capture_post_entry_flow(adapter, subject, now=now)
    _capture_exit_path(adapter, subject, now=now)
    _capture_final_path(adapter, subject, now=now)


def _sync_learning(
    adapter: FinalProfitFirstResearchAdapter,
    *,
    focus_signature: str | None = None,
) -> None:
    _ensure_schema(adapter)
    now = _utcnow()
    subjects: list[dict[str, Any]] = []
    seen: set[str] = set()
    if focus_signature:
        with adapter.store._lock:
            row = adapter.store.db.execute(
                "SELECT * FROM strategy_learning_subjects WHERE epoch_id=? AND source_signature=? LIMIT 1",
                (adapter.epoch_id, focus_signature),
            ).fetchone()
        if row is not None:
            subjects.append(dict(row))
            seen.add(str(row["source_signature"]))

    cursor = int(getattr(adapter, "_roi_continuous_learning_cursor_id", 0) or 0)
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT * FROM strategy_learning_subjects WHERE epoch_id=? AND id>? ORDER BY id LIMIT ?",
            (adapter.epoch_id, cursor, MAX_SYNC_SUBJECTS_PER_PASS),
        ).fetchall()
        if not rows and cursor > 0:
            rows = adapter.store.db.execute(
                "SELECT * FROM strategy_learning_subjects WHERE epoch_id=? ORDER BY id LIMIT ?",
                (adapter.epoch_id, MAX_SYNC_SUBJECTS_PER_PASS),
            ).fetchall()
    for row in rows:
        item = dict(row)
        signature = str(item["source_signature"])
        if signature not in seen:
            subjects.append(item)
            seen.add(signature)
    if rows:
        setattr(adapter, "_roi_continuous_learning_cursor_id", int(rows[-1]["id"]))
    elif cursor > 0:
        setattr(adapter, "_roi_continuous_learning_cursor_id", 0)

    for subject in subjects[: MAX_SYNC_SUBJECTS_PER_PASS + 1]:
        _sync_subject(adapter, subject, now=now)


def _record_and_sync(adapter: FinalProfitFirstResearchAdapter, signature: str) -> None:
    _record_subject(adapter, signature)
    _sync_learning(adapter, focus_signature=signature)


def _variant_performance(adapter: FinalProfitFirstResearchAdapter) -> dict[str, dict[str, Any]]:
    try:
        with adapter.store._lock:
            rows = adapter.store.db.execute(
                "SELECT s.state_json,o.net_return FROM fomo_shadow_observations s "
                "JOIN fomo_shadow_outcomes o ON o.release_commit=s.release_commit AND o.source_signature=s.source_signature "
                "WHERE s.release_commit=? ORDER BY s.id DESC LIMIT 2000",
                (adapter.release_commit,),
            ).fetchall()
    except Exception:
        return {}
    return _variant_performance_from_rows(dict(row) for row in rows)


def _learning_status(adapter: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    _ensure_schema(adapter)
    now = _utcnow()
    with adapter.store._lock:
        counts = adapter.store.db.execute(
            "SELECT COUNT(*) AS total,SUM(entry_executable) AS executable,"
            "SUM(CASE WHEN entry_executable=0 THEN 1 ELSE 0 END) AS counterfactual,"
            "SUM(CASE WHEN fomo_state<>'unknown' THEN 1 ELSE 0 END) AS fomo_subjects "
            "FROM strategy_learning_subjects WHERE epoch_id=?",
            (adapter.epoch_id,),
        ).fetchone()
        entry_windows = adapter.store.db.execute(
            "SELECT COUNT(*) AS n FROM fomo_learning_entry_windows WHERE release_commit=?",
            (adapter.release_commit,),
        ).fetchone()
        post_flow = adapter.store.db.execute(
            "SELECT COUNT(*) AS n FROM fomo_learning_post_entry_flow WHERE release_commit=?",
            (adapter.release_commit,),
        ).fetchone()
        exit_paths = adapter.store.db.execute(
            "SELECT COUNT(*) AS n FROM strategy_learning_exit_paths WHERE epoch_id=?",
            (adapter.epoch_id,),
        ).fetchone()
        final_paths = adapter.store.db.execute(
            "SELECT COUNT(*) AS n FROM strategy_learning_final_paths WHERE epoch_id=?",
            (adapter.epoch_id,),
        ).fetchone()

    horizon_coverage: dict[str, Any] = {}
    for horizon in PATH_HORIZONS_SECONDS:
        cutoff = (now - timedelta(seconds=horizon)).isoformat()
        with adapter.store._lock:
            eligible_row = adapter.store.db.execute(
                "SELECT COUNT(*) AS n FROM strategy_learning_subjects WHERE epoch_id=? AND reference_at<=?",
                (adapter.epoch_id, cutoff),
            ).fetchone()
            captured_row = adapter.store.db.execute(
                "SELECT COUNT(*) AS n FROM strategy_learning_horizon_marks WHERE epoch_id=? AND horizon_seconds=?",
                (adapter.epoch_id, int(horizon)),
            ).fetchone()
        eligible = int(eligible_row["n"] or 0) if eligible_row is not None else 0
        captured = int(captured_row["n"] or 0) if captured_row is not None else 0
        horizon_coverage[str(horizon)] = {
            "eligible_subjects": eligible,
            "captured_marks": captured,
            "coverage_pct": (min(captured, eligible) / eligible * 100.0) if eligible else None,
        }

    total = int(counts["total"] or 0) if counts is not None else 0
    executable = int(counts["executable"] or 0) if counts is not None else 0
    counterfactual = int(counts["counterfactual"] or 0) if counts is not None else 0
    fomo_subjects = int(counts["fomo_subjects"] or 0) if counts is not None else 0
    return {
        "learning_version": LEARNING_VERSION,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
        "active_strategy_mutation_allowed": False,
        "historical_promotion_authority": False,
        "strategy_rules_changed": False,
        "additional_rpc_fanout": False,
        "subject_count": total,
        "executable_subject_count": executable,
        "counterfactual_rejected_subject_count": counterfactual,
        "fomo_subject_count": fomo_subjects,
        "fomo_entry_window_snapshot_count": int(entry_windows["n"] or 0) if entry_windows is not None else 0,
        "fomo_post_entry_flow_snapshot_count": int(post_flow["n"] or 0) if post_flow is not None else 0,
        "exit_path_count": int(exit_paths["n"] or 0) if exit_paths is not None else 0,
        "final_300s_path_count": int(final_paths["n"] or 0) if final_paths is not None else 0,
        "entry_window_seconds": list(FOMO_ENTRY_WINDOWS_SECONDS),
        "price_path_horizons_seconds": list(PATH_HORIZONS_SECONDS),
        "post_entry_flow_horizons_seconds": list(FOMO_POST_ENTRY_FLOW_HORIZONS_SECONDS),
        "price_horizon_coverage": horizon_coverage,
        "fomo_experiment_variant_performance": _variant_performance(adapter),
        "continuous_improvement_surfaces": {
            "fomo_entry_window_selection": "multi_window_point_in_time_entity_flow",
            "fomo_feature_weight_and_threshold_research": "immutable_entry_features_plus_forward_outcomes",
            "fomo_incremental_alpha": "variant_specific_forward_outcomes",
            "fomo_exit_research": "post_entry_flow_rollover_plus_price_path_mfe_mae",
            "all_lane_exit_research": "generic_price_path_horizons_plus_exit_path_mfe_mae",
            "wallet_context_selection": "trigger_wallet_x_venue_x_lifecycle_x_regime_plus_forward_paths",
            "screening_false_negative_research": "rejected_counterfactual_subject_price_paths",
            "position_sizing_research": "existing_fraction_rotation_plus_forward_return_and_path_drawdown",
            "risk_threshold_research": "immutable_opportunity_and_decision_json_joined_to_forward_paths",
            "execution_latency_research": "existing_quote_risk_latency_evidence_joined_by_source_signature",
        },
    }


async def _observe_with_learning(self: FinalProfitFirstResearchAdapter, signature: str) -> None:
    if _ORIGINAL_OBSERVE is None:
        raise RuntimeError("continuous strategy learning is not installed")
    await _ORIGINAL_OBSERVE(self, signature)
    try:
        await asyncio.to_thread(_record_and_sync, self, signature)
        setattr(self, "_roi_continuous_learning_last_error", None)
    except Exception as exc:
        setattr(self, "_roi_continuous_learning_last_error", f"{type(exc).__name__}: {exc}")


async def _sell_with_learning(self: FinalProfitFirstResearchAdapter, row: dict[str, Any]) -> None:
    if _ORIGINAL_SELL is None:
        raise RuntimeError("continuous strategy learning is not installed")
    await _ORIGINAL_SELL(self, row)
    try:
        await asyncio.to_thread(_sync_learning, self)
        setattr(self, "_roi_continuous_learning_last_error", None)
    except Exception as exc:
        setattr(self, "_roi_continuous_learning_last_error", f"{type(exc).__name__}: {exc}")


def _status_with_learning(self: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("continuous strategy learning is not installed")
    payload = _ORIGINAL_STATUS(self)
    try:
        status = _learning_status(self)
        status["last_error"] = getattr(self, "_roi_continuous_learning_last_error", None)
        payload["continuous_strategy_learning"] = status
    except Exception as exc:
        payload["continuous_strategy_learning"] = {
            "learning_version": LEARNING_VERSION,
            "paper_only": True,
            "live_money_authority": False,
            "active_strategy_mutation_allowed": False,
            "failed_closed": True,
            "last_error": f"{type(exc).__name__}: {exc}",
        }
    return payload


def _manifest_with_learning(self: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    if _ORIGINAL_MANIFEST is None:
        raise RuntimeError("continuous strategy learning manifest is not installed")
    payload = _ORIGINAL_MANIFEST(self)
    payload.update(
        {
            "continuous_strategy_learning_version": LEARNING_VERSION,
            "continuous_strategy_learning_scope": "all_unified_forward_candidates_plus_fomo_specific_flow",
            "continuous_learning_fomo_entry_windows_seconds": list(FOMO_ENTRY_WINDOWS_SECONDS),
            "continuous_learning_price_path_horizons_seconds": list(PATH_HORIZONS_SECONDS),
            "continuous_learning_fomo_post_entry_flow_horizons_seconds": list(FOMO_POST_ENTRY_FLOW_HORIZONS_SECONDS),
            "continuous_learning_records_rejected_counterfactuals": True,
            "continuous_learning_records_mfe_mae": True,
            "continuous_learning_records_exit_giveback": True,
            "continuous_learning_variant_attribution": True,
            "continuous_learning_uses_existing_canonical_price_marks": True,
            "continuous_learning_additional_rpc_fanout": False,
            "continuous_learning_strategy_rules_changed": False,
            "continuous_learning_active_strategy_mutation_allowed": False,
            "continuous_learning_historical_promotion_authority": False,
            "continuous_learning_paper_only": True,
            "continuous_learning_live_money_authority": False,
            "continuous_learning_signing_available": False,
            "continuous_learning_transaction_submission_available": False,
        }
    )
    return payload


def install_continuous_strategy_learning() -> None:
    """Install a read/append-only learning plane above the final FOMO paper wrapper.

    The plane never changes a strategy decision. It stores alternative FOMO entry
    windows, generic forward price paths, post-entry FOMO flow rollovers, exit MFE/
    MAE/giveback and rejected-candidate counterfactuals using evidence the runtime
    already collects. All SQLite work runs off the event loop and no RPC/quote work
    is added to the candidate hot path.
    """

    global _ORIGINAL_OBSERVE, _ORIGINAL_SELL, _ORIGINAL_STATUS, _ORIGINAL_MANIFEST

    current_observe = FinalProfitFirstResearchAdapter.observe
    if _ORIGINAL_OBSERVE is None:
        _ORIGINAL_OBSERVE = current_observe
        _inherit_markers(_observe_with_learning, current_observe)
        setattr(_observe_with_learning, "_roi_continuous_strategy_learning", True)
        FinalProfitFirstResearchAdapter.observe = _observe_with_learning  # type: ignore[method-assign]

    current_sell = FinalProfitFirstResearchAdapter._sell
    if _ORIGINAL_SELL is None:
        _ORIGINAL_SELL = current_sell
        _inherit_markers(_sell_with_learning, current_sell)
        setattr(_sell_with_learning, "_roi_continuous_strategy_learning", True)
        FinalProfitFirstResearchAdapter._sell = _sell_with_learning  # type: ignore[method-assign]

    current_status = FinalProfitFirstResearchAdapter.status
    if _ORIGINAL_STATUS is None:
        _ORIGINAL_STATUS = current_status
        _inherit_markers(_status_with_learning, current_status)
        setattr(_status_with_learning, "_roi_continuous_strategy_learning", True)
        FinalProfitFirstResearchAdapter.status = _status_with_learning  # type: ignore[method-assign]

    current_manifest = FinalProfitFirstResearchAdapter._manifest
    if _ORIGINAL_MANIFEST is None:
        _ORIGINAL_MANIFEST = current_manifest
        _inherit_markers(_manifest_with_learning, current_manifest)
        setattr(_manifest_with_learning, "_roi_continuous_strategy_learning", True)
        FinalProfitFirstResearchAdapter._manifest = _manifest_with_learning  # type: ignore[method-assign]


__all__ = [
    "ACTIVE_STRATEGY_MUTATION_ALLOWED",
    "ADDITIONAL_RPC_FANOUT",
    "FINAL_PATH_HORIZON_SECONDS",
    "FOMO_ENTRY_WINDOWS_SECONDS",
    "FOMO_POST_ENTRY_FLOW_HORIZONS_SECONDS",
    "HISTORICAL_PROMOTION_AUTHORITY",
    "LEARNING_VERSION",
    "LIVE_MONEY_AUTHORITY",
    "PAPER_ONLY",
    "PATH_HORIZONS_SECONDS",
    "SIGNING_AVAILABLE",
    "STRATEGY_RULES_CHANGED",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "_path_metrics",
    "_variant_performance_from_rows",
    "_window_stats",
    "install_continuous_strategy_learning",
]
