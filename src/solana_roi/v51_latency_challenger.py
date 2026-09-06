from __future__ import annotations

import json
import math
from collections import defaultdict
from statistics import mean, median
from typing import Any

from .strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH, authority
from .v51_economic_core import robust_profile
from .v51_evidence_analytics import ensure_counterfactual_schema, refresh_rejected_counterfactuals


LATENCY_CHALLENGER_VERSION = "v51-latency-challenger-research-v1"

# These are research cohorts only. The frozen v5.1 authority remains exactly as
# specified in strategy_v51_authority.json and continues to reject paper entries
# above execution.latency_hard_max_seconds.
RESEARCH_BANDS: tuple[tuple[str, float | None, float | None], ...] = (
    ("authorized_le_20s", 0.0, 20.0),
    ("challenger_20_40s", 20.0, 40.0),
    ("challenger_40_90s", 40.0, 90.0),
    ("later_lifecycle_gt_90s", 90.0, None),
)


def _table_exists(store: Any, table: str) -> bool:
    try:
        with store._lock:
            return store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table,),
            ).fetchone() is not None
    except Exception:
        return False


def _columns(store: Any, table: str) -> set[str]:
    if not _table_exists(store, table):
        return set()
    with store._lock:
        return {str(row["name"]) for row in store.db.execute(f"PRAGMA table_info({table})").fetchall()}


def _safe(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _json(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if not raw:
        return {}
    try:
        value = json.loads(str(raw))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def latency_research_band(seconds: float | None) -> str:
    if seconds is None or not math.isfinite(float(seconds)) or float(seconds) < 0.0:
        return "unknown"
    value = float(seconds)
    if value <= 20.0:
        return "authorized_le_20s"
    if value <= 40.0:
        return "challenger_20_40s"
    if value <= 90.0:
        return "challenger_40_90s"
    return "later_lifecycle_gt_90s"


def _trial_metadata(store: Any) -> dict[tuple[str, str], dict[str, Any]]:
    """Return economic latency/chase metadata for canonical Solana/FOMO candidates.

    signal_to_entry_seconds comes from the final amount-specific quote path and is
    intentionally distinct from transport/ingestion latency. We prefer the unified
    profit-maximizer row when more than one lane exists for the source signature.
    """
    table = "profit_first_final_trials"
    cols = _columns(store, table)
    required = {"source_signature", "signal_to_entry_seconds"}
    if not required.issubset(cols):
        return {}
    wanted = [
        column
        for column in (
            "release_commit",
            "source_signature",
            "lane",
            "signal_to_entry_seconds",
            "round_trip_cost_fraction",
            "opportunity_json",
            "context_json",
            "decision_json",
        )
        if column in cols
    ]
    with store._lock:
        rows = [
            dict(row)
            for row in store.db.execute(
                f"SELECT {','.join(wanted)} FROM {table} ORDER BY rowid"
            ).fetchall()
        ]
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        signature = str(row.get("source_signature") or "")
        if not signature:
            continue
        key = (str(row.get("release_commit") or ""), signature)
        previous = result.get(key)
        is_unified = str(row.get("lane") or "") == "unified_profit_maximizer"
        if previous is not None and not is_unified:
            continue
        opportunity = _json(row.get("opportunity_json"))
        context = _json(row.get("context_json"))
        decision = _json(row.get("decision_json"))
        result[key] = {
            "signal_to_entry_seconds": _safe(row.get("signal_to_entry_seconds")),
            "round_trip_cost_fraction": _safe(row.get("round_trip_cost_fraction")),
            "venue": str(
                context.get("venue")
                or opportunity.get("venue")
                or decision.get("venue")
                or "UNKNOWN"
            ),
            "lifecycle": str(
                context.get("lifecycle")
                or opportunity.get("lifecycle")
                or decision.get("lifecycle")
                or "unknown"
            ),
            "chase_fraction": _safe(
                context.get("chase_fraction")
                if context.get("chase_fraction") is not None
                else opportunity.get("chase_fraction")
            ),
        }
    return result


def _candidate_metadata(store: Any) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    if _table_exists(store, "v51_candidates"):
        cols = _columns(store, "v51_candidates")
        wanted = [
            column
            for column in (
                "surface",
                "candidate_id",
                "release_commit",
                "venue",
                "lifecycle",
                "raw_chase_fraction",
            )
            if column in cols
        ]
        if {"surface", "candidate_id"}.issubset(wanted):
            with store._lock:
                rows = [
                    dict(row)
                    for row in store.db.execute(
                        f"SELECT {','.join(wanted)} FROM v51_candidates"
                    ).fetchall()
                ]
            for row in rows:
                result[(str(row.get("surface") or ""), str(row.get("candidate_id") or ""))] = row
    if _table_exists(store, "v51_robinhood_candidate_ledger"):
        cols = _columns(store, "v51_robinhood_candidate_ledger")
        wanted = [
            column
            for column in (
                "candidate_id",
                "release_commit",
                "venue",
                "lifecycle",
            )
            if column in cols
        ]
        if "candidate_id" in wanted:
            with store._lock:
                rows = [
                    dict(row)
                    for row in store.db.execute(
                        f"SELECT {','.join(wanted)} FROM v51_robinhood_candidate_ledger"
                    ).fetchall()
                ]
            for row in rows:
                result[("ROBINHOOD_CHAIN", str(row.get("candidate_id") or ""))] = row
    return result


def _return_summary(values: list[float]) -> dict[str, Any]:
    profile = robust_profile(values)
    return {
        "resolved_count": len(values),
        "mean_return": mean(values) if values else None,
        "median_return": median(values) if values else None,
        "positive_rate": (sum(value > 0.0 for value in values) / len(values)) if values else None,
        "robust_profile": profile,
    }


def build_latency_challenger_research(store: Any) -> dict[str, Any]:
    """Measure whether the frozen 20-second cliff is leaving forward edge behind.

    This function is deliberately non-authoritative. It reads existing candidate,
    quote and rejected-counterfactual evidence; it never creates a paper trial,
    changes sizing, selects a lane, submits a transaction, or retroactively grants
    entry authority.
    """
    refresh_rejected_counterfactuals(store)
    ensure_counterfactual_schema(store)
    hard_max = float(authority()["execution"]["latency_hard_max_seconds"])
    trial_meta = _trial_metadata(store)
    candidate_meta = _candidate_metadata(store)
    counterfactual_cols = _columns(store, "v51_rejected_counterfactuals")
    optional_gross = ",forward_gross_return" if "forward_gross_return" in counterfactual_cols else ""

    with store._lock:
        rejected_rows = [
            dict(row)
            for row in store.db.execute(
                "SELECT surface,candidate_id,release_commit,decision_reason,forward_net_return,counterfactual_state"
                + optional_gross
                + " FROM v51_rejected_counterfactuals ORDER BY updated_at"
            ).fetchall()
        ]

    grouped: dict[tuple[str, str, str, str], list[float]] = defaultdict(list)
    counts: dict[tuple[str, str, str, str], dict[str, int]] = defaultdict(
        lambda: {"candidate_count": 0, "resolved_count": 0, "positive_count": 0}
    )
    missing_by_surface: dict[str, int] = defaultdict(int)
    above_authority_count = 0
    resolved_above_authority = 0

    for row in rejected_rows:
        surface = str(row.get("surface") or "UNKNOWN")
        candidate = str(row.get("candidate_id") or "")
        release = str(row.get("release_commit") or "")
        meta = trial_meta.get((release, candidate)) or trial_meta.get(("", candidate)) or {}
        candidate_row = candidate_meta.get((surface, candidate), {})
        latency = _safe(meta.get("signal_to_entry_seconds"))
        if latency is None:
            missing_by_surface[surface] += 1
        band = latency_research_band(latency)
        venue = str(meta.get("venue") or candidate_row.get("venue") or "UNKNOWN")
        lifecycle = str(meta.get("lifecycle") or candidate_row.get("lifecycle") or "unknown")
        key = (surface, venue, lifecycle, band)
        counts[key]["candidate_count"] += 1
        value = _safe(row.get("forward_net_return"))
        if value is None and surface == "ROBINHOOD_CHAIN":
            value = _safe(row.get("forward_gross_return"))
        if value is not None:
            counts[key]["resolved_count"] += 1
            counts[key]["positive_count"] += int(value > 0.0)
            grouped[key].append(value)
        if latency is not None and latency > hard_max:
            above_authority_count += 1
            resolved_above_authority += int(value is not None)

    cohorts: dict[str, Any] = {}
    for key in sorted(counts):
        surface, venue, lifecycle, band = key
        label = "|".join(key)
        values = grouped.get(key, [])
        cohorts[label] = {
            "surface": surface,
            "venue": venue,
            "lifecycle": lifecycle,
            "latency_band": band,
            **counts[key],
            "return_semantics": (
                "gross_observed_market_return_research_only_not_executable_pnl"
                if surface == "ROBINHOOD_CHAIN"
                else "resolved_shadow_net_return_after_recorded_execution_model"
            ),
            "positive_rate": (
                counts[key]["positive_count"] / counts[key]["resolved_count"]
                if counts[key]["resolved_count"]
                else None
            ),
            "return_profile": _return_summary(values),
        }

    by_band_values: dict[str, list[float]] = defaultdict(list)
    by_band_counts: dict[str, int] = defaultdict(int)
    for key, counter in counts.items():
        band = key[3]
        by_band_counts[band] += int(counter["candidate_count"])
        by_band_values[band].extend(grouped.get(key, []))
    bands = {
        band: {
            "candidate_count": by_band_counts.get(band, 0),
            **_return_summary(by_band_values.get(band, [])),
        }
        for band, _low, _high in RESEARCH_BANDS
    }
    missing_numeric_latency = sum(missing_by_surface.values())
    if missing_numeric_latency:
        bands["unknown"] = {
            "candidate_count": missing_numeric_latency,
            **_return_summary(by_band_values.get("unknown", [])),
        }

    return {
        "latency_challenger_version": LATENCY_CHALLENGER_VERSION,
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "current_authorized_hard_max_seconds": hard_max,
        "current_authority_changed": False,
        "research_bands": [
            {
                "name": name,
                "lower_exclusive_seconds": low if low and low > 0.0 else None,
                "upper_inclusive_seconds": high,
            }
            for name, low, high in RESEARCH_BANDS
        ],
        "bands": bands,
        "cohorts": cohorts,
        "rejected_candidate_count": len(rejected_rows),
        "above_current_authority_candidate_count": above_authority_count,
        "resolved_above_current_authority_count": resolved_above_authority,
        "missing_numeric_signal_to_entry_count": missing_numeric_latency,
        "missing_numeric_signal_to_entry_by_surface": dict(sorted(missing_by_surface.items())),
        "measurement_debt": (
            "Robinhood currently resolves rejected forward market returns but does not persist a numeric amount-specific "
            "signal_to_entry_seconds for rejected candidates; those rows remain in the explicit unknown latency cohort"
            if missing_by_surface.get("ROBINHOOD_CHAIN", 0)
            else None
        ),
        "selection_policy": (
            "research-only counterfactual evidence; no challenger cohort can create a paper position or satisfy current promotion authority"
        ),
        "future_epoch_decision_rule": (
            "consider venue/lifecycle-specific latency authority only after sufficient independent forward outcomes show positive "
            "after-cost robust log growth, positive leave-best-trade-out evidence and execution-stress resilience"
        ),
        "latency_semantics": "final amount-specific signal_to_entry_seconds; not RPC or ingestion transport delay",
        "retrospective_entry_authority": False,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "LATENCY_CHALLENGER_VERSION",
    "RESEARCH_BANDS",
    "build_latency_challenger_research",
    "latency_research_band",
]
