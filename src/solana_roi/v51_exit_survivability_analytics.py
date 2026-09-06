from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from statistics import mean, median
from typing import Any, Iterable


SURVIVABILITY_ANALYTICS_VERSION = "v51-exit-survivability-analytics-114-v1"
SURVIVABILITY_HORIZONS_SECONDS = (10, 30, 60, 120, 300)
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
STRATEGY_AUTHORITY = False
PROMOTION_AUTHORITY = False
V52_RESEARCH_INPUT_ONLY = True


def _table_exists(adapter: Any, table: str) -> bool:
    with adapter.store._lock:
        row = adapter.store.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table,),
        ).fetchone()
    return row is not None


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


def _percentile(values: Iterable[float], p: float) -> float | None:
    ordered = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not ordered:
        return None
    if len(ordered) == 1:
        return ordered[0]
    rank = max(0.0, min(1.0, float(p))) * (len(ordered) - 1)
    low = int(math.floor(rank))
    high = int(math.ceil(rank))
    if low == high:
        return ordered[low]
    weight = rank - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


def _summary(values: Iterable[float]) -> dict[str, Any]:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    return {
        "count": len(clean),
        "mean": mean(clean) if clean else None,
        "median": median(clean) if clean else None,
        "p90": _percentile(clean, 0.90),
    }


def ensure_schema(adapter: Any) -> None:
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_exit_survivability_observations ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_id TEXT NOT NULL, release_commit TEXT NOT NULL, "
            "execution_model_epoch TEXT NOT NULL, position_scope TEXT NOT NULL, source_signature TEXT NOT NULL, "
            "token_mint TEXT NOT NULL, lane TEXT NOT NULL, context_key TEXT NOT NULL, venue TEXT NOT NULL, "
            "lifecycle TEXT NOT NULL, regime TEXT NOT NULL, risk_signature TEXT NOT NULL, flow_state TEXT NOT NULL, "
            "chase_band TEXT NOT NULL, latency_band TEXT NOT NULL, attempt_number INTEGER NOT NULL, "
            "scheduled_horizon_seconds INTEGER NOT NULL, first_exit_due_at TEXT NOT NULL, attempted_at TEXT NOT NULL, "
            "elapsed_seconds REAL NOT NULL, actual_position_raw INTEGER NOT NULL, entry_cost_sol REAL NOT NULL, "
            "position_fraction REAL NOT NULL, route_valid INTEGER NOT NULL, simulation_ok INTEGER NOT NULL, "
            "executable INTEGER NOT NULL, terminal INTEGER NOT NULL, status TEXT NOT NULL, price_impact_pct REAL, "
            "expected_output_lamports INTEGER, minimum_output_lamports INTEGER, total_fee_lamports INTEGER NOT NULL, "
            "realizable_return REAL, creator_distribution INTEGER NOT NULL, linked_entity_distribution INTEGER NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, created_at TEXT NOT NULL, "
            "UNIQUE(epoch_id,position_scope,source_signature,lane,attempt_number))"
        )
        adapter.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_exit_survivability_context ON "
            "v51_exit_survivability_observations(epoch_id,execution_model_epoch,context_key,scheduled_horizon_seconds,id)"
        )
        adapter.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_exit_survivability_hazard ON "
            "v51_exit_survivability_observations(epoch_id,execution_model_epoch,risk_signature,id)"
        )


def _v5_context_rows(adapter: Any, source_signature: str) -> list[dict[str, Any]]:
    if not _table_exists(adapter, "risk_conditioned_alpha_v5_trials"):
        return []
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT lane,venue,lifecycle,regime,risk_signature,flow_state,chase_band,latency_band,context_key "
            "FROM risk_conditioned_alpha_v5_trials WHERE release_commit=? AND source_signature=? "
            "AND selected=1 AND decision LIKE 'paper_enter%' ORDER BY id",
            (adapter.release_commit, source_signature),
        ).fetchall()
    return [dict(row) for row in rows]


def _fomo_context(adapter: Any, source_signature: str, risk_hint: dict[str, Any] | None) -> dict[str, Any] | None:
    if not _table_exists(adapter, "fomo_paper_trials"):
        return None
    with adapter.store._lock:
        row = adapter.store.db.execute(
            "SELECT venue,lifecycle,regime,fomo_state FROM fomo_paper_trials "
            "WHERE release_commit=? AND source_signature=? AND decision LIKE 'paper_enter_%' LIMIT 1",
            (adapter.release_commit, source_signature),
        ).fetchone()
    if row is None:
        return None
    item = dict(row)
    hint = dict(risk_hint or {})
    venue = str(item.get("venue") or "UNKNOWN")
    lifecycle = str(item.get("lifecycle") or "unknown")
    regime = str(item.get("regime") or "unknown")
    risk_signature = str(hint.get("risk_signature") or "unknown")
    flow_state = str(hint.get("flow_state") or item.get("fomo_state") or "unknown")
    chase = str(hint.get("chase_band") or "unknown")
    latency = str(hint.get("latency_band") or "unknown")
    return {
        "lane": "fomo_continuation_paper",
        "venue": venue,
        "lifecycle": lifecycle,
        "regime": regime,
        "risk_signature": risk_signature,
        "flow_state": flow_state,
        "chase_band": chase,
        "latency_band": latency,
        "context_key": (
            f"FOMO|{venue}|{lifecycle}|{regime}|{risk_signature}|{flow_state}|{chase}|{latency}"
        ),
    }


def _fallback_final_context(adapter: Any, source_signature: str) -> dict[str, Any]:
    if _table_exists(adapter, "profit_first_final_trials"):
        with adapter.store._lock:
            row = adapter.store.db.execute(
                "SELECT lane,regime,context_json,opportunity_json FROM profit_first_final_trials "
                "WHERE epoch_id=? AND source_signature=? ORDER BY id LIMIT 1",
                (adapter.epoch_id, source_signature),
            ).fetchone()
        if row is not None:
            item = dict(row)
            context = _safe_json(item.get("context_json"))
            opportunity = _safe_json(item.get("opportunity_json"))
            lane = str(item.get("lane") or "unified_profit_maximizer")
            regime = str(item.get("regime") or context.get("regime") or "unknown")
            venue = str(opportunity.get("venue") or opportunity.get("source_venue") or "UNKNOWN")
            lifecycle = str(opportunity.get("lifecycle") or "unknown")
            risk_signature = str(opportunity.get("risk_signature") or context.get("soft_risk_bin") or "unknown")
            flow_state = str(context.get("creator_flow_state") or "unknown")
            chase = str(context.get("chase_bin") or "unknown")
            latency = str(context.get("latency_bin") or "unknown")
            return {
                "lane": lane,
                "venue": venue,
                "lifecycle": lifecycle,
                "regime": regime,
                "risk_signature": risk_signature,
                "flow_state": flow_state,
                "chase_band": chase,
                "latency_band": latency,
                "context_key": f"FALLBACK|{lane}|{venue}|{lifecycle}|{regime}|{risk_signature}|{flow_state}|{chase}|{latency}",
            }
    return {
        "lane": "unified_profit_maximizer",
        "venue": "UNKNOWN",
        "lifecycle": "unknown",
        "regime": "unknown",
        "risk_signature": "unknown",
        "flow_state": "unknown",
        "chase_band": "unknown",
        "latency_band": "unknown",
        "context_key": "FALLBACK|unknown",
    }


def _context_rows(adapter: Any, liquidation: dict[str, Any]) -> list[dict[str, Any]]:
    signature = str(liquidation["source_signature"])
    v5 = _v5_context_rows(adapter, signature)
    if str(liquidation.get("position_scope") or "final") == "fomo":
        fomo = _fomo_context(adapter, signature, v5[0] if v5 else None)
        return [fomo] if fomo is not None else [_fallback_final_context(adapter, signature)]
    return v5 or [_fallback_final_context(adapter, signature)]


def record_attempt(
    adapter: Any,
    liquidation: dict[str, Any],
    evidence: dict[str, Any],
    *,
    attempt_number: int,
    status: str,
    execution_model_epoch: str,
) -> None:
    """Persist one research-only survivability observation per exact v5 context.

    This function has no strategy, sizing, promotion, signing or submission authority.
    It records only facts already produced by the canonical exact-exit engine.
    """

    ensure_schema(adapter)
    from .v51_exact_exit_execution import EXIT_RETRY_ELAPSED_SECONDS

    index = max(0, int(attempt_number) - 1)
    scheduled_horizon = int(EXIT_RETRY_ELAPSED_SECONDS[index]) if index < len(EXIT_RETRY_ELAPSED_SECONDS) else int(EXIT_RETRY_ELAPSED_SECONDS[-1])
    first_due = _dt(liquidation.get("first_exit_due_at"))
    attempted = _dt(evidence.get("attempted_at")) or datetime.now(timezone.utc)
    elapsed = max(0.0, (attempted - first_due).total_seconds()) if first_due is not None else float(scheduled_horizon)
    executable = bool(
        evidence.get("amount_match")
        and evidence.get("transaction_built")
        and evidence.get("route_valid")
        and evidence.get("simulation_ok")
        and _finite(evidence.get("exit_net_sol")) is not None
        and float(evidence.get("exit_net_sol") or 0.0) > 0.0
    )
    entry_cost = max(0.0, float(liquidation.get("entry_cost_sol") or 0.0))
    exit_net = _finite(evidence.get("exit_net_sol"))
    realizable = (exit_net / entry_cost - 1.0) if executable and exit_net is not None and entry_cost > 0.0 else None
    features = _safe_json(liquidation.get("exit_features_json"))
    creator_distribution = bool(features.get("creator_distribution"))
    linked_distribution = bool(features.get("linked_entity_distribution"))
    now = datetime.now(timezone.utc).isoformat()

    for context in _context_rows(adapter, liquidation):
        with adapter.store._lock, adapter.store.db:
            adapter.store.db.execute(
                "INSERT OR REPLACE INTO v51_exit_survivability_observations("
                "epoch_id,release_commit,execution_model_epoch,position_scope,source_signature,token_mint,lane,context_key,"
                "venue,lifecycle,regime,risk_signature,flow_state,chase_band,latency_band,attempt_number,"
                "scheduled_horizon_seconds,first_exit_due_at,attempted_at,elapsed_seconds,actual_position_raw,entry_cost_sol,"
                "position_fraction,route_valid,simulation_ok,executable,terminal,status,price_impact_pct,expected_output_lamports,"
                "minimum_output_lamports,total_fee_lamports,realizable_return,creator_distribution,linked_entity_distribution,"
                "paper_only,live_money_authority,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0,?)",
                (
                    adapter.epoch_id,
                    adapter.release_commit,
                    execution_model_epoch,
                    str(liquidation.get("position_scope") or "final"),
                    str(liquidation["source_signature"]),
                    str(liquidation["token_mint"]),
                    str(context["lane"]),
                    str(context["context_key"]),
                    str(context["venue"]),
                    str(context["lifecycle"]),
                    str(context["regime"]),
                    str(context["risk_signature"]),
                    str(context["flow_state"]),
                    str(context["chase_band"]),
                    str(context["latency_band"]),
                    int(attempt_number),
                    scheduled_horizon,
                    str(liquidation.get("first_exit_due_at") or attempted.isoformat()),
                    attempted.isoformat(),
                    elapsed,
                    int(liquidation.get("actual_position_raw") or 0),
                    entry_cost,
                    float(liquidation.get("position_fraction") or 0.0),
                    1 if evidence.get("route_valid") else 0,
                    1 if evidence.get("simulation_ok") else 0,
                    1 if executable else 0,
                    1 if status == "paper_exit_terminal_unexitable" else 0,
                    status,
                    _finite(evidence.get("price_impact_pct")),
                    int(evidence["expected_output_lamports"]) if evidence.get("expected_output_lamports") is not None else None,
                    int(evidence["minimum_output_lamports"]) if evidence.get("minimum_output_lamports") is not None else None,
                    int(evidence.get("total_fee_lamports") or 0),
                    realizable,
                    1 if creator_distribution else 0,
                    1 if linked_distribution else 0,
                    now,
                ),
            )


def _final_path_mfe(adapter: Any) -> dict[str, float]:
    if not _table_exists(adapter, "strategy_learning_final_paths"):
        return {}
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT source_signature,mfe_mark_return FROM strategy_learning_final_paths "
            "WHERE epoch_id=? AND mfe_mark_return IS NOT NULL",
            (adapter.epoch_id,),
        ).fetchall()
    result: dict[str, float] = {}
    for row in rows:
        value = _finite(row["mfe_mark_return"])
        if value is not None:
            result[str(row["source_signature"])] = value
    return result


def _context_identity(row: dict[str, Any]) -> dict[str, str]:
    return {
        "position_scope": str(row["position_scope"]),
        "lane": str(row["lane"]),
        "context_key": str(row["context_key"]),
        "venue": str(row["venue"]),
        "lifecycle": str(row["lifecycle"]),
        "regime": str(row["regime"]),
        "risk_signature": str(row["risk_signature"]),
        "flow_state": str(row["flow_state"]),
        "chase_band": str(row["chase_band"]),
        "latency_band": str(row["latency_band"]),
    }


def _group_key(row: dict[str, Any]) -> tuple[str, ...]:
    identity = _context_identity(row)
    return tuple(identity[key] for key in (
        "position_scope", "lane", "context_key", "venue", "lifecycle", "regime",
        "risk_signature", "flow_state", "chase_band", "latency_band"
    ))


def _decay_rates(rows: list[dict[str, Any]], field: str) -> list[float]:
    by_source: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if _finite(row.get(field)) is not None:
            by_source.setdefault(str(row["source_signature"]), []).append(row)
    rates: list[float] = []
    for source_rows in by_source.values():
        ordered = sorted(source_rows, key=lambda item: float(item["elapsed_seconds"]))
        if len(ordered) < 2:
            continue
        first, last = ordered[0], ordered[-1]
        elapsed_minutes = (float(last["elapsed_seconds"]) - float(first["elapsed_seconds"])) / 60.0
        if elapsed_minutes <= 0.0:
            continue
        start = _finite(first.get(field))
        end = _finite(last.get(field))
        if start is not None and end is not None:
            rates.append((end - start) / elapsed_minutes)
    return rates


def _recovery_times(rows: list[dict[str, Any]]) -> list[float]:
    by_source: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_source.setdefault(str(row["source_signature"]), []).append(row)
    values: list[float] = []
    for source_rows in by_source.values():
        ordered = sorted(source_rows, key=lambda item: int(item["attempt_number"]))
        if not ordered or bool(ordered[0]["executable"]):
            continue
        recovered = next((item for item in ordered[1:] if bool(item["executable"])), None)
        if recovered is not None:
            values.append(float(recovered["elapsed_seconds"]))
    return values


def _context_report(rows: list[dict[str, Any]], mfe_by_source: dict[str, float]) -> dict[str, Any]:
    positions = sorted({str(row["source_signature"]) for row in rows})
    horizons: dict[str, Any] = {}
    for horizon in SURVIVABILITY_HORIZONS_SECONDS:
        horizon_rows = [row for row in rows if int(row["scheduled_horizon_seconds"]) == horizon]
        at_risk = sorted({str(row["source_signature"]) for row in horizon_rows})
        executable_positions = {
            str(row["source_signature"]) for row in horizon_rows if bool(row["executable"])
        }
        horizons[str(horizon)] = {
            "at_risk_position_count": len(at_risk),
            "executable_route_position_count": len(executable_positions),
            "conditional_executable_route_probability": (
                len(executable_positions) / len(at_risk) if at_risk else None
            ),
            "denominator_definition": "positions_that_remained_open_and_reached_this_scheduled_retry",
        }

    price_impacts = [value for row in rows if (value := _finite(row.get("price_impact_pct"))) is not None]
    recovery = _recovery_times(rows)
    impact_decay = _decay_rates(rows, "price_impact_pct")
    return_decay = _decay_rates(rows, "realizable_return")
    terminal_sources = {str(row["source_signature"]) for row in rows if bool(row["terminal"])}

    executable_mfe: dict[str, float] = {}
    for row in rows:
        if not bool(row["executable"]):
            continue
        value = _finite(row.get("realizable_return"))
        if value is None:
            continue
        source = str(row["source_signature"])
        executable_mfe[source] = max(executable_mfe.get(source, float("-inf")), value)
    paired = [
        (mfe_by_source[source], executable_mfe[source])
        for source in positions
        if source in mfe_by_source and source in executable_mfe
    ]
    reference_mfe = [item[0] for item in paired]
    realizable_mfe = [item[1] for item in paired]
    gaps = [reference - realizable for reference, realizable in paired]

    first = rows[0]
    return {
        "context": _context_identity(first),
        "position_count": len(positions),
        "attempt_count": len(rows),
        "route_failure_rate": (
            sum(not bool(row["executable"]) for row in rows) / len(rows) if rows else None
        ),
        "terminal_unexitable_position_rate": (
            len(terminal_sources) / len(positions) if positions else None
        ),
        "route_survivability_by_retry_horizon": horizons,
        "actual_held_size_price_impact_pct": _summary(price_impacts),
        "time_to_recovered_exitability_seconds": _summary(recovery),
        "liquidity_decay": {
            "price_impact_pct_points_per_minute": _summary(impact_decay),
            "realizable_return_fraction_change_per_minute": _summary(return_decay),
            "positive_price_impact_rate_means_deteriorating_liquidity": True,
            "negative_realizable_return_rate_means_deteriorating_liquidity": True,
        },
        "mfe_realizability": {
            "paired_position_count": len(paired),
            "reference_price_mfe_fraction": _summary(reference_mfe),
            "executable_realizable_mfe_fraction": _summary(realizable_mfe),
            "reference_minus_realizable_mfe_gap_fraction": _summary(gaps),
        },
    }


def _dedup_global(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, int]] = set()
    result: list[dict[str, Any]] = []
    for row in rows:
        key = (str(row["position_scope"]), str(row["source_signature"]), int(row["attempt_number"]))
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def _group_failure(rows: list[dict[str, Any]], field: str) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = str(row.get(field) if row.get(field) is not None else "unknown")
        grouped.setdefault(key, []).append(row)
    result: dict[str, Any] = {}
    for key, items in sorted(grouped.items()):
        positions = {str(row["source_signature"]) for row in items}
        terminal = {str(row["source_signature"]) for row in items if bool(row["terminal"])}
        impacts = [value for row in items if (value := _finite(row.get("price_impact_pct"))) is not None]
        result[key] = {
            "position_count": len(positions),
            "attempt_count": len(items),
            "failed_attempt_rate": sum(not bool(row["executable"]) for row in items) / len(items),
            "terminal_unexitable_position_rate": len(terminal) / len(positions) if positions else None,
            "actual_size_price_impact_pct": _summary(impacts),
            "price_impact_decay_pct_points_per_minute": _summary(_decay_rates(items, "price_impact_pct")),
        }
    return result


def build_report(adapter: Any, *, execution_model_epoch: str | None = None) -> dict[str, Any]:
    ensure_schema(adapter)
    if execution_model_epoch is None:
        from .v51_exit_execution_terminal_fomo_followup import ACTIVE_EXECUTION_MODEL_EPOCH

        execution_model_epoch = ACTIVE_EXECUTION_MODEL_EPOCH
    with adapter.store._lock:
        raw = adapter.store.db.execute(
            "SELECT * FROM v51_exit_survivability_observations WHERE epoch_id=? AND execution_model_epoch=? ORDER BY id",
            (adapter.epoch_id, execution_model_epoch),
        ).fetchall()
    rows = [dict(row) for row in raw]
    mfe_by_source = _final_path_mfe(adapter)
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(_group_key(row), []).append(row)
    contexts = [_context_report(items, mfe_by_source) for _, items in sorted(grouped.items())]
    global_rows = _dedup_global(rows)
    return {
        "version": SURVIVABILITY_ANALYTICS_VERSION,
        "execution_model_epoch": execution_model_epoch,
        "research_scope": "v5.2_exit_survivability_input_only",
        "strategy_authority": False,
        "promotion_authority": False,
        "changes_v51_economics": False,
        "horizons_seconds": list(SURVIVABILITY_HORIZONS_SECONDS),
        "observation_count": len(global_rows),
        "context_observation_count": len(rows),
        "context_count": len(contexts),
        "contexts": contexts,
        "creator_distribution_route_deterioration": _group_failure(
            [dict(row, creator_distribution="true" if bool(row["creator_distribution"]) else "false") for row in global_rows],
            "creator_distribution",
        ),
        "hazard_signature_failed_exit": _group_failure(rows, "risk_signature"),
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def status(adapter: Any | None = None) -> dict[str, Any]:
    base = {
        "version": SURVIVABILITY_ANALYTICS_VERSION,
        "horizons_seconds": list(SURVIVABILITY_HORIZONS_SECONDS),
        "exact_context_metrics": True,
        "actual_held_size_price_impact": True,
        "reference_vs_executable_mfe": True,
        "exit_route_failure_rate": True,
        "time_to_recovered_exitability": True,
        "liquidity_decay_rate": True,
        "creator_distribution_route_deterioration": True,
        "hazard_signature_failed_exit": True,
        "v52_research_input_only": True,
        "strategy_authority": False,
        "promotion_authority": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    if adapter is None:
        return base
    ensure_schema(adapter)
    from .v51_exit_execution_terminal_fomo_followup import ACTIVE_EXECUTION_MODEL_EPOCH

    with adapter.store._lock:
        row = adapter.store.db.execute(
            "SELECT COUNT(*) AS n,COUNT(DISTINCT context_key) AS contexts,COUNT(DISTINCT source_signature) AS positions "
            "FROM v51_exit_survivability_observations WHERE epoch_id=? AND execution_model_epoch=?",
            (adapter.epoch_id, ACTIVE_EXECUTION_MODEL_EPOCH),
        ).fetchone()
    base.update(
        {
            "execution_model_epoch": ACTIVE_EXECUTION_MODEL_EPOCH,
            "observation_rows": int(row["n"] or 0) if row else 0,
            "context_count": int(row["contexts"] or 0) if row else 0,
            "position_count": int(row["positions"] or 0) if row else 0,
        }
    )
    return base


__all__ = [
    "LIVE_MONEY_AUTHORITY",
    "PAPER_ONLY",
    "PROMOTION_AUTHORITY",
    "SIGNING_AVAILABLE",
    "STRATEGY_AUTHORITY",
    "SURVIVABILITY_ANALYTICS_VERSION",
    "SURVIVABILITY_HORIZONS_SECONDS",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "V52_RESEARCH_INPUT_ONLY",
    "build_report",
    "ensure_schema",
    "record_attempt",
    "status",
]
