from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from statistics import mean, median
from typing import Any, Iterable

from .strategy_v51_authority import (
    AUTHORITY_ID,
    ECONOMIC_FREEZE_EPOCH,
    LIVE_MONEY_AUTHORITY,
    PAPER_ONLY,
    SIGNING_AVAILABLE,
    STRATEGY_VERSION,
    TRANSACTION_SUBMISSION_AVAILABLE,
)


RESEARCH_VERSION = "v51-roadmap-59-64-research-v1"
FOMO_LATENCY_BANDS = (
    ("fomo_0_2s", 0.0, 2.0),
    ("fomo_2_5s", 2.0, 5.0),
    ("fomo_5_10s", 5.0, 10.0),
    ("fomo_10_20s", 10.0, 20.0),
    ("fomo_gt_20s", 20.0, None),
)
LATENCY_DECAY_BANDS = (
    ("0_2s", 0.0, 2.0),
    ("2_5s", 2.0, 5.0),
    ("5_10s", 5.0, 10.0),
    ("10_20s", 10.0, 20.0),
    ("20_40s", 20.0, 40.0),
    ("40_90s", 40.0, 90.0),
    ("gt_90s", 90.0, None),
)
CHASE_DECAY_BANDS = (
    ("le_15pct", None, 0.15),
    ("15_25pct", 0.15, 0.25),
    ("25_40pct", 0.25, 0.40),
    ("gt_40pct", 0.40, None),
)
COST_DECAY_BANDS = (
    ("le_2pct", None, 0.02),
    ("2_5pct", 0.02, 0.05),
    ("5_10pct", 0.05, 0.10),
    ("gt_10pct", 0.10, None),
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _safe_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _table_exists(store: Any, table: str) -> bool:
    try:
        with store._lock:
            return (
                store.db.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (table,),
                ).fetchone()
                is not None
            )
    except Exception:
        return False


def _columns(store: Any, table: str) -> set[str]:
    if not _table_exists(store, table):
        return set()
    try:
        with store._lock:
            return {str(row[1]) for row in store.db.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


def _pick(mapping: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in mapping and mapping[name] not in (None, ""):
            return mapping[name]
    return None


def _normalize_venue(value: Any) -> str:
    raw = str(value or "").upper().replace("-", "_").replace("/", "_")
    if "PUMP_AMM" in raw or "PUMPSWAP" in raw or "PUMP_SWAP" in raw:
        return "PUMP_AMM"
    if "PUMP_FUN" in raw or "PUMPFUN" in raw:
        return "PUMP_FUN"
    if "RAYDIUM" in raw:
        return "RAYDIUM"
    if "FOMO" in raw:
        return "FOMO"
    if "ROBINHOOD" in raw:
        return "ROBINHOOD_CHAIN"
    return str(value or "UNKNOWN").upper() or "UNKNOWN"


def _risk_class(feature: dict[str, Any], context: dict[str, Any]) -> str:
    explicit = str(_pick(feature, "risk_class", "hazard_bin") or _pick(context, "risk_class", "hazard_bin") or "").lower()
    if explicit in {"clean", "hazard"}:
        return explicit
    signature = str(_pick(feature, "risk_signature") or _pick(context, "risk_signature") or "")
    severity = _safe_float(_pick(feature, "risk_severity") or _pick(context, "risk_severity"))
    hazardous = (
        bool(feature.get("creator_distributing"))
        or (_safe_float(feature.get("early_holder_exit_fraction")) or 0.0) >= 0.20
        or (_safe_float(feature.get("quote_deterioration_fraction")) or 0.0) >= 0.05
        or (_safe_float(feature.get("exit_slippage_deterioration_fraction")) or 0.0) >= 0.05
        or feature.get("risk_complete") is False
        or (signature not in {"", "clean"})
        or (severity is not None and severity > 0.0)
    )
    return "hazard" if hazardous else "clean"


def _normal_ci(values: list[float]) -> list[float | None]:
    if not values:
        return [None, None]
    if len(values) == 1:
        return [values[0], values[0]]
    mu = mean(values)
    variance = sum((value - mu) ** 2 for value in values) / (len(values) - 1)
    se = math.sqrt(max(0.0, variance)) / math.sqrt(len(values))
    return [mu - 1.96 * se, mu + 1.96 * se]


def _expected_log_growth(values: list[float], fraction: float = 0.01) -> float | None:
    if not values:
        return None
    terms: list[float] = []
    for value in values:
        terminal = 1.0 + float(fraction) * value
        if terminal <= 0.0:
            return float("-inf")
        terms.append(math.log(terminal))
    return mean(terms)


def _profile(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    material = list(rows)
    values = [value for row in material if (value := _safe_float(row.get("net_return"))) is not None]
    latencies = [value for row in material if (value := _safe_float(row.get("latency_seconds"))) is not None]
    chases = [value for row in material if (value := _safe_float(row.get("chase_fraction"))) is not None]
    costs = [value for row in material if (value := _safe_float(row.get("round_trip_cost_fraction"))) is not None]
    independent = {
        str(row.get("independent_event_id") or row.get("source_signature") or "")
        for row in material
        if str(row.get("independent_event_id") or row.get("source_signature") or "")
    }
    trimmed = None
    if len(values) > 1:
        trimmed = mean(sorted(values, reverse=True)[1:])
    ci = _normal_ci(values)
    return {
        "sample_count": len(values),
        "independent_event_count": len(independent),
        "mean_net_return": mean(values) if values else None,
        "median_net_return": median(values) if values else None,
        "hit_rate": (sum(value > 0.0 for value in values) / len(values)) if values else None,
        "expected_log_growth_at_1pct": _expected_log_growth(values),
        "leave_best_trade_out_mean": trimmed,
        "mean_return_95pct_ci": ci,
        "mean_latency_seconds": mean(latencies) if latencies else None,
        "mean_chase_fraction": mean(chases) if chases else None,
        "mean_round_trip_cost_fraction": mean(costs) if costs else None,
        "execution_cost_observed_count": len(costs),
    }


def fomo_latency_band(seconds: float | None) -> str:
    value = _safe_float(seconds)
    if value is None or value < 0:
        return "unknown"
    if value <= 2.0:
        return "fomo_0_2s"
    if value <= 5.0:
        return "fomo_2_5s"
    if value <= 10.0:
        return "fomo_5_10s"
    if value <= 20.0:
        return "fomo_10_20s"
    return "fomo_gt_20s"


def _bounded_band(value: float | None, bands: tuple[tuple[str, float | None, float | None], ...]) -> str:
    numeric = _safe_float(value)
    if numeric is None or numeric < 0:
        return "unknown"
    for name, lower, upper in bands:
        lower_ok = lower is None or numeric > lower
        upper_ok = upper is None or numeric <= upper
        if lower_ok and upper_ok:
            return name
    return "unknown"


def _fomo_rows(store: Any) -> list[dict[str, Any]]:
    if not _table_exists(store, "fomo_shadow_outcomes"):
        return []
    obs_available = _table_exists(store, "fomo_shadow_observations")
    query = (
        "SELECT o.source_signature,o.venue,o.lifecycle,o.regime,o.fomo_state,"
        "o.signal_to_entry_seconds,o.net_return,"
        + ("x.feature_json " if obs_available else "NULL AS feature_json ")
        + "FROM fomo_shadow_outcomes o "
        + (
            "LEFT JOIN fomo_shadow_observations x "
            "ON x.release_commit=o.release_commit AND x.source_signature=o.source_signature "
            if obs_available
            else ""
        )
        + "ORDER BY o.id"
    )
    try:
        with store._lock:
            raw = [dict(row) for row in store.db.execute(query).fetchall()]
    except Exception:
        return []
    rows: list[dict[str, Any]] = []
    for row in raw:
        feature = _safe_json(row.get("feature_json"))
        venue = _normalize_venue(row.get("venue"))
        rows.append(
            {
                "source_signature": str(row.get("source_signature") or ""),
                "independent_event_id": str(row.get("source_signature") or ""),
                "surface": "FOMO",
                "venue": venue,
                "lifecycle": str(row.get("lifecycle") or "unknown"),
                "regime": str(row.get("regime") or "unknown"),
                "risk_class": _risk_class(feature, {}),
                "latency_seconds": _safe_float(row.get("signal_to_entry_seconds")),
                "chase_fraction": _safe_float(feature.get("chase_fraction")),
                "round_trip_cost_fraction": None,
                "net_return": _safe_float(row.get("net_return")),
                "evidence_source": "fomo_shadow_outcome",
            }
        )
    return rows


def _profit_first_rows(store: Any) -> list[dict[str, Any]]:
    if not (_table_exists(store, "profit_first_final_outcomes") and _table_exists(store, "profit_first_final_trials")):
        return []
    ocols = _columns(store, "profit_first_final_outcomes")
    tcols = _columns(store, "profit_first_final_trials")
    needed_o = {"source_signature", "net_return", "signal_to_entry_seconds"}
    needed_t = {"source_signature", "opportunity_json"}
    if not needed_o.issubset(ocols) or not needed_t.issubset(tcols):
        return []
    join_parts = ["t.source_signature=o.source_signature"]
    if "epoch_id" in ocols and "epoch_id" in tcols:
        join_parts.append("t.epoch_id=o.epoch_id")
    if "lane" in ocols and "lane" in tcols:
        join_parts.append("t.lane=o.lane")
    select = [
        "o.source_signature AS source_signature",
        "o.net_return AS net_return",
        "o.signal_to_entry_seconds AS latency_seconds",
        "t.opportunity_json AS opportunity_json",
    ]
    select.append("t.context_json AS trial_context_json" if "context_json" in tcols else "NULL AS trial_context_json")
    select.append("o.context_json AS outcome_context_json" if "context_json" in ocols else "NULL AS outcome_context_json")
    select.append("t.round_trip_cost_fraction AS round_trip_cost_fraction" if "round_trip_cost_fraction" in tcols else "NULL AS round_trip_cost_fraction")
    select.append("o.token_mint AS token_mint" if "token_mint" in ocols else "NULL AS token_mint")
    wallet_join = ""
    wallet_cols = _columns(store, "wallet_discovery_forward_observations")
    if {"signature", "source"}.issubset(wallet_cols):
        wallet_join = " LEFT JOIN wallet_discovery_forward_observations w ON w.signature=o.source_signature "
        select.append("w.source AS observation_source")
        select.append("w.chase_fraction AS observation_chase_fraction" if "chase_fraction" in wallet_cols else "NULL AS observation_chase_fraction")
    else:
        select.extend(["NULL AS observation_source", "NULL AS observation_chase_fraction"])
    query = (
        "SELECT " + ",".join(select) + " FROM profit_first_final_outcomes o "
        "JOIN profit_first_final_trials t ON " + " AND ".join(join_parts) + wallet_join + " ORDER BY o.id"
    )
    try:
        with store._lock:
            raw = [dict(row) for row in store.db.execute(query).fetchall()]
    except Exception:
        return []
    rows: list[dict[str, Any]] = []
    for row in raw:
        opportunity = _safe_json(row.get("opportunity_json"))
        context = _safe_json(row.get("trial_context_json"))
        context.update({key: value for key, value in _safe_json(row.get("outcome_context_json")).items() if key not in context})
        venue = _normalize_venue(
            _pick(opportunity, "venue", "source_venue")
            or _pick(context, "venue", "source_venue")
            or row.get("observation_source")
        )
        lifecycle = str(_pick(opportunity, "lifecycle", "lifecycle_stage") or _pick(context, "lifecycle", "lifecycle_stage") or "unknown")
        if lifecycle == "unknown":
            lifecycle = {
                "PUMP_FUN": "pump_bonding_curve",
                "PUMP_AMM": "pump_amm_post_bonding_curve",
                "RAYDIUM": "raydium_independent_context",
                "FOMO": "fomo_continuation",
            }.get(venue, "unknown")
        regime = str(_pick(opportunity, "regime") or _pick(context, "regime") or "unknown")
        chase = _safe_float(
            _pick(opportunity, "chase_fraction", "raw_chase_fraction")
            or _pick(context, "chase_fraction", "raw_chase_fraction")
            or row.get("observation_chase_fraction")
        )
        rows.append(
            {
                "source_signature": str(row.get("source_signature") or ""),
                "independent_event_id": str(row.get("source_signature") or ""),
                "surface": "FOMO" if venue == "FOMO" else "SOLANA",
                "venue": venue,
                "lifecycle": lifecycle,
                "regime": regime,
                "risk_class": _risk_class(opportunity, context),
                "latency_seconds": _safe_float(row.get("latency_seconds")),
                "chase_fraction": chase,
                "round_trip_cost_fraction": _safe_float(row.get("round_trip_cost_fraction")),
                "net_return": _safe_float(row.get("net_return")),
                "token_mint": str(row.get("token_mint") or ""),
                "evidence_source": "profit_first_final_outcome",
            }
        )
    return rows


def load_execution_research_rows(store: Any) -> list[dict[str, Any]]:
    rows = _profit_first_rows(store) + _fomo_rows(store)
    seen: set[tuple[str, str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for row in rows:
        key = (
            str(row.get("evidence_source") or ""),
            str(row.get("source_signature") or ""),
            str(row.get("venue") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def build_fomo_signal_half_life_from_rows(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    fomo = [dict(row) for row in rows if _normalize_venue(row.get("venue")) == "FOMO" or str(row.get("surface") or "").upper() == "FOMO"]
    cohorts: dict[str, Any] = {}
    for risk_class in ("clean", "hazard"):
        selected = [row for row in fomo if str(row.get("risk_class") or "clean") == risk_class]
        band_profiles: dict[str, Any] = {}
        for name, _lower, _upper in FOMO_LATENCY_BANDS:
            bucket = [row for row in selected if fomo_latency_band(row.get("latency_seconds")) == name]
            band_profiles[name] = _profile(bucket)
        baseline = band_profiles["fomo_0_2s"]["mean_net_return"]
        half_life: dict[str, Any] = {
            "state": "insufficient_evidence",
            "baseline_band": "fomo_0_2s",
            "baseline_mean_net_return": baseline,
            "first_half_edge_band": None,
            "half_life_lower_bound_seconds": None,
        }
        if baseline is not None and baseline > 0.0:
            for name in ("fomo_2_5s", "fomo_5_10s", "fomo_10_20s", "fomo_gt_20s"):
                value = band_profiles[name]["mean_net_return"]
                if value is not None and value <= 0.5 * baseline:
                    lower = {
                        "fomo_2_5s": 2.0,
                        "fomo_5_10s": 5.0,
                        "fomo_10_20s": 10.0,
                        "fomo_gt_20s": 20.0,
                    }[name]
                    half_life = {
                        "state": "empirical_bucket_crossing_observed",
                        "baseline_band": "fomo_0_2s",
                        "baseline_mean_net_return": baseline,
                        "first_half_edge_band": name,
                        "half_life_lower_bound_seconds": lower,
                    }
                    break
            else:
                observed_later = any(band_profiles[name]["sample_count"] > 0 for name in ("fomo_2_5s", "fomo_5_10s", "fomo_10_20s", "fomo_gt_20s"))
                half_life["state"] = "half_edge_not_observed" if observed_later else "insufficient_evidence"
        cohorts[risk_class] = {
            "bands": band_profiles,
            "half_life": half_life,
        }
    return {
        "research_version": RESEARCH_VERSION,
        "definition": "forward FOMO net-return decay by exact observable signal-to-entry latency; clean and hazard cohorts never pool",
        "latency_bands_seconds": ["0-2", "2-5", "5-10", "10-20", ">20"],
        "cohorts": cohorts,
        "sample_count": len(fomo),
        "authority_effect": "measurement_only",
        "current_authority_changed": False,
        "above_20s_paper_entry_authority": False,
        "historical_promotion_authority": False,
        "paper_only": True,
        "live_money_authority": False,
    }


def build_fomo_signal_half_life(store: Any) -> dict[str, Any]:
    return build_fomo_signal_half_life_from_rows(load_execution_research_rows(store))


def build_venue_execution_decay_from_rows(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    material = [dict(row) for row in rows]
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in material:
        venue = _normalize_venue(row.get("venue"))
        lifecycle = str(row.get("lifecycle") or "unknown")
        risk_slice = str(row.get("risk_class") or "clean") if venue == "FOMO" else "all"
        groups[(venue, lifecycle, risk_slice)].append(row)

    segments: list[dict[str, Any]] = []
    for (venue, lifecycle, risk_slice), segment_rows in sorted(groups.items()):
        latency = {
            name: _profile([row for row in segment_rows if _bounded_band(row.get("latency_seconds"), LATENCY_DECAY_BANDS) == name])
            for name, _lower, _upper in LATENCY_DECAY_BANDS
        }
        chase = {
            name: _profile([row for row in segment_rows if _bounded_band(row.get("chase_fraction"), CHASE_DECAY_BANDS) == name])
            for name, _lower, _upper in CHASE_DECAY_BANDS
        }
        chase["unknown"] = _profile([row for row in segment_rows if _bounded_band(row.get("chase_fraction"), CHASE_DECAY_BANDS) == "unknown"])
        cost = {
            name: _profile([row for row in segment_rows if _bounded_band(row.get("round_trip_cost_fraction"), COST_DECAY_BANDS) == name])
            for name, _lower, _upper in COST_DECAY_BANDS
        }
        cost["unknown"] = _profile([row for row in segment_rows if _bounded_band(row.get("round_trip_cost_fraction"), COST_DECAY_BANDS) == "unknown"])
        segments.append(
            {
                "venue": venue,
                "lifecycle": lifecycle,
                "risk_slice": risk_slice,
                "overall": _profile(segment_rows),
                "residual_edge_by_latency": latency,
                "residual_edge_by_chase": chase,
                "residual_edge_by_round_trip_cost": cost,
            }
        )
    return {
        "research_version": RESEARCH_VERSION,
        "model": "venue_x_lifecycle_x_latency_x_chase_x_execution_cost_research_surface",
        "segments": segments,
        "segment_count": len(segments),
        "pump_amm_and_raydium_pooling_allowed": False,
        "fomo_clean_and_hazard_pooling_allowed": False,
        "selection_authority": False,
        "sizing_authority": False,
        "exit_authority": False,
        "promotion_authority": False,
        "current_authority_changed": False,
        "paper_only": True,
        "live_money_authority": False,
    }


def build_venue_execution_decay(store: Any) -> dict[str, Any]:
    return build_venue_execution_decay_from_rows(load_execution_research_rows(store))


def _observation_venue(source: Any) -> str | None:
    venue = _normalize_venue(source)
    return venue if venue in {"PUMP_FUN", "PUMP_AMM", "RAYDIUM"} else None


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _ensure_lifecycle_schema(store: Any) -> bool:
    try:
        with store._lock, store.db:
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS v51_token_lifecycle_research_events ("
                "lifecycle_event_id TEXT PRIMARY KEY,economic_freeze_epoch TEXT NOT NULL,token_mint TEXT NOT NULL,"
                "last_pump_fun_at TEXT,first_pump_amm_at TEXT,explicit_graduation_at TEXT,"
                "graduation_timestamp_source TEXT NOT NULL,first_raydium_at TEXT,"
                "pump_amm_0_30s_observations INTEGER NOT NULL,pump_amm_30_120s_observations INTEGER NOT NULL,"
                "pump_amm_120_300s_observations INTEGER NOT NULL,pump_amm_gt_300s_observations INTEGER NOT NULL,"
                "updated_at TEXT NOT NULL,paper_only INTEGER NOT NULL,live_money_authority INTEGER NOT NULL)"
            )
        return True
    except Exception:
        return False


def refresh_token_lifecycle_research(store: Any, execution_rows: Iterable[dict[str, Any]] | None = None) -> dict[str, Any]:
    if not _table_exists(store, "wallet_discovery_forward_observations"):
        return {
            "available": False,
            "reason": "wallet_discovery_forward_observations_unavailable",
            "graduation_events": [],
            "event_count": 0,
        }
    cols = _columns(store, "wallet_discovery_forward_observations")
    if not {"token_mint", "received_at", "source"}.issubset(cols):
        return {
            "available": False,
            "reason": "wallet_discovery_forward_observations_missing_required_columns",
            "graduation_events": [],
            "event_count": 0,
        }
    try:
        with store._lock:
            raw = [
                dict(row)
                for row in store.db.execute(
                    "SELECT token_mint,received_at,source FROM wallet_discovery_forward_observations "
                    "ORDER BY token_mint,received_at"
                ).fetchall()
            ]
    except Exception:
        return {"available": False, "reason": "observation_query_failed_closed", "graduation_events": [], "event_count": 0}

    by_token: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in raw:
        venue = _observation_venue(row.get("source"))
        if venue is None:
            continue
        timestamp = _parse_time(row.get("received_at"))
        if timestamp is None:
            continue
        by_token[str(row.get("token_mint") or "")].append(
            {
                "venue": venue,
                "at": timestamp,
                "at_text": timestamp.isoformat(),
                "source": str(row.get("source") or ""),
            }
        )

    events: list[dict[str, Any]] = []
    schema_ready = _ensure_lifecycle_schema(store)
    material_execution_rows = list(execution_rows) if execution_rows is not None else []
    for token, observations in sorted(by_token.items()):
        if not token:
            continue
        observations.sort(key=lambda row: row["at"])
        pump_fun = [row for row in observations if row["venue"] == "PUMP_FUN"]
        pump_amm = [row for row in observations if row["venue"] == "PUMP_AMM"]
        raydium = [row for row in observations if row["venue"] == "RAYDIUM"]
        if not pump_fun:
            continue

        first_amm = next((row for row in pump_amm if row["at"] >= pump_fun[0]["at"]), None)
        last_fun = None
        if first_amm is not None:
            prior = [row for row in pump_fun if row["at"] <= first_amm["at"]]
            last_fun = prior[-1] if prior else None
        first_raydium = next((row for row in raydium if row["at"] >= pump_fun[0]["at"]), None)
        explicit_candidates = [
            row
            for row in observations
            if "GRADUAT" in row["source"].upper()
            and (last_fun is None or row["at"] >= last_fun["at"])
            and (first_amm is None or row["at"] <= first_amm["at"])
        ]
        explicit = explicit_candidates[0] if explicit_candidates else None

        if first_amm is None:
            continue

        event_id = hashlib.sha256(
            f"{ECONOMIC_FREEZE_EPOCH}|{token}|{first_amm['at_text']}".encode("utf-8")
        ).hexdigest()[:24]
        windows = {"0_30s": 0, "30_120s": 0, "120_300s": 0, "gt_300s": 0}
        for row in pump_amm:
            if row["at"] < first_amm["at"]:
                continue
            age = (row["at"] - first_amm["at"]).total_seconds()
            if age <= 30.0:
                windows["0_30s"] += 1
            elif age <= 120.0:
                windows["30_120s"] += 1
            elif age <= 300.0:
                windows["120_300s"] += 1
            else:
                windows["gt_300s"] += 1

        token_execution = [
            row
            for row in material_execution_rows
            if str(row.get("token_mint") or "") == token and _normalize_venue(row.get("venue")) == "PUMP_AMM"
        ]
        execution_profile = _profile(token_execution)

        event = {
            "lifecycle_event_id": event_id,
            "token_mint": token,
            "last_pump_fun_observed_at": last_fun["at_text"] if last_fun else None,
            "first_pump_amm_observed_at": first_amm["at_text"],
            "graduation_timestamp": explicit["at_text"] if explicit else None,
            "graduation_timestamp_source": "explicit_observation" if explicit else "inferred_transition_window_only",
            "transition_window": {
                "start": last_fun["at_text"] if last_fun else None,
                "end": first_amm["at_text"],
            },
            "first_raydium_observed_at": first_raydium["at_text"] if first_raydium else None,
            "pump_amm_post_transition_observation_counts": windows,
            "post_transition_execution_profile": execution_profile,
            "forward_price_horizon_measurement_available": execution_profile["sample_count"] > 0,
            "forward_price_horizon_measurement_debt": (
                None
                if execution_profile["sample_count"] > 0
                else (
                    "lifecycle observations establish transition timing; exact forward executable price/quote horizons "
                    "must come from execution/outcome evidence and are not fabricated here"
                )
            ),
            "raydium_evidence_pooling_allowed": False,
        }
        events.append(event)

        if schema_ready:
            try:
                with store._lock, store.db:
                    store.db.execute(
                        "INSERT INTO v51_token_lifecycle_research_events("
                        "lifecycle_event_id,economic_freeze_epoch,token_mint,last_pump_fun_at,first_pump_amm_at,"
                        "explicit_graduation_at,graduation_timestamp_source,first_raydium_at,"
                        "pump_amm_0_30s_observations,pump_amm_30_120s_observations,pump_amm_120_300s_observations,"
                        "pump_amm_gt_300s_observations,updated_at,paper_only,live_money_authority"
                        ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                        "ON CONFLICT(lifecycle_event_id) DO UPDATE SET "
                        "last_pump_fun_at=excluded.last_pump_fun_at,first_pump_amm_at=excluded.first_pump_amm_at,"
                        "explicit_graduation_at=excluded.explicit_graduation_at,"
                        "graduation_timestamp_source=excluded.graduation_timestamp_source,"
                        "first_raydium_at=excluded.first_raydium_at,"
                        "pump_amm_0_30s_observations=excluded.pump_amm_0_30s_observations,"
                        "pump_amm_30_120s_observations=excluded.pump_amm_30_120s_observations,"
                        "pump_amm_120_300s_observations=excluded.pump_amm_120_300s_observations,"
                        "pump_amm_gt_300s_observations=excluded.pump_amm_gt_300s_observations,"
                        "updated_at=excluded.updated_at",
                        (
                            event_id,
                            ECONOMIC_FREEZE_EPOCH,
                            token,
                            event["last_pump_fun_observed_at"],
                            event["first_pump_amm_observed_at"],
                            event["graduation_timestamp"],
                            event["graduation_timestamp_source"],
                            event["first_raydium_observed_at"],
                            windows["0_30s"],
                            windows["30_120s"],
                            windows["120_300s"],
                            windows["gt_300s"],
                            _now(),
                            1,
                            0,
                        ),
                    )
            except Exception:
                schema_ready = False

    return {
        "available": True,
        "persistent_event_ledger_ready": schema_ready,
        "event_count": len(events),
        "graduation_events": events,
        "lifecycle_definition": [
            "pump_fun_bonding_curve",
            "pump_fun_near_graduation_observed",
            "pump_amm_transition_observed",
            "pump_amm_post_graduation_continuation",
            "mature_continuation",
        ],
        "exact_graduation_timestamp_policy": "publish_only_when_explicitly_observed; otherwise publish bounded transition window",
        "raydium_is_independent_venue_context": True,
        "raydium_evidence_pooling_allowed": False,
        "paper_only": True,
        "live_money_authority": False,
    }


def build_roadmap_59_64_research(store: Any) -> dict[str, Any]:
    rows = load_execution_research_rows(store)
    lifecycle = refresh_token_lifecycle_research(store, rows)
    return {
        "research_version": RESEARCH_VERSION,
        "authority_id": AUTHORITY_ID,
        "strategy_version": STRATEGY_VERSION,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "items": {
            "59_fomo_signal_half_life": build_fomo_signal_half_life_from_rows(rows),
            "60_pump_fun_first_slot_policy": {
                "policy": "research_only_residual_continuation_not_first_slot_sniping",
                "first_slot_promotion_authority": False,
                "current_strategy_mutation": False,
            },
            "61_cross_venue_token_lifecycle": lifecycle,
            "62_graduation_event_cluster": {
                "persistent_event_ledger": "v51_token_lifecycle_research_events",
                "event_count": lifecycle.get("event_count", 0),
                "exact_timestamp_fabrication_allowed": False,
                "transition_window_used_when_exact_event_missing": True,
            },
            "63_raydium_pumpswap_isolation": {
                "pump_amm_and_raydium_pooling_allowed": False,
                "raydium_remains_independent_venue_lifecycle_context": True,
            },
            "64_venue_specific_execution_decay": build_venue_execution_decay_from_rows(rows),
        },
        "execution_research_row_count": len(rows),
        "current_authority_changed": False,
        "latency_hard_max_seconds_changed": False,
        "above_20s_paper_entry_authority": False,
        "selection_authority": False,
        "sizing_authority": False,
        "exit_authority": False,
        "promotion_authority": False,
        "historical_promotion_authority": False,
        "paper_only": bool(PAPER_ONLY),
        "live_money_authority": bool(LIVE_MONEY_AUTHORITY),
        "signing_available": bool(SIGNING_AVAILABLE),
        "transaction_submission_available": bool(TRANSACTION_SUBMISSION_AVAILABLE),
    }


__all__ = [
    "RESEARCH_VERSION",
    "FOMO_LATENCY_BANDS",
    "fomo_latency_band",
    "load_execution_research_rows",
    "build_fomo_signal_half_life",
    "build_fomo_signal_half_life_from_rows",
    "build_venue_execution_decay",
    "build_venue_execution_decay_from_rows",
    "refresh_token_lifecycle_research",
    "build_roadmap_59_64_research",
]
