from __future__ import annotations

import asyncio
import math
from typing import Any, Callable

from . import robinhood_pumpfun_shadow_boundary as shadow
from .robinhood_chain_core import _finite


NATIVE_LEARNING_VERSION = "robinhood-native-shadow-learning-v1"
LEARNING_COMPATIBILITY_VERSION = shadow.SHADOW_BOUNDARY_VERSION
MIN_FORWARD_OUTCOMES_FOR_PAPER_ENTRY = shadow.MIN_FORWARD_OUTCOMES_FOR_PAPER_ENTRY

# These are learning bands, not trade vetoes. Lane is already an independent
# dimension in the context key, so timing is learned as lane x latency-band.
CHASE_BANDS = (
    (0.05, "0_5pct"),
    (0.15, "5_15pct"),
    (0.25, "15_25pct"),
    (0.40, "25_40pct"),
    (float("inf"), "gt_40pct"),
)
LATENCY_BANDS = (
    (2.0, "le_2s"),
    (5.0, "2_5s"),
    (10.0, "5_10s"),
    (20.0, "10_20s"),
    (60.0, "20_60s"),
    (180.0, "60_180s"),
    (float("inf"), "gt_180s"),
)
COST_BANDS = (
    (0.03, "le_3pct"),
    (0.07, "3_7pct"),
    (0.15, "7_15pct"),
    (0.30, "15_30pct"),
    (float("inf"), "gt_30pct"),
)

# Broader evidence needs more observations and is conservatively shrunk before
# it can substitute for an immature exact child context.
HIERARCHY = (
    ("same_entity_lane_venue_lifecycle_regime_risk", 30, 0.80),
    ("pooled_lane_venue_lifecycle_regime_risk_bands", 45, 0.65),
    ("pooled_lane_venue_lifecycle_regime_risk", 60, 0.55),
    ("pooled_lane_venue_lifecycle_regime", 75, 0.50),
    ("pooled_lane_venue_lifecycle", 90, 0.45),
    ("pooled_lane_venue", 120, 0.40),
    ("pooled_lane", 150, 0.35),
    ("global_robinhood", 240, 0.30),
)

_ORIGINAL_INSERT: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., Any] | None = None


def _band(value: float | None, bands: tuple[tuple[float, str], ...], unknown: str = "unknown") -> str:
    if value is None or not math.isfinite(float(value)):
        return unknown
    numeric = max(0.0, float(value))
    for upper, label in bands:
        if numeric <= upper:
            return label
    return bands[-1][1]


def robinhood_chase_band(value: float | None) -> str:
    return _band(value, CHASE_BANDS)


def robinhood_latency_band(lane: str, value: float | None) -> str:
    # The lane is intentionally preserved in the returned label for diagnostics;
    # economically, the chooser already conditions on lane as a separate dimension.
    return f"{lane}:{_band(value, LATENCY_BANDS)}"


def robinhood_cost_band(value: float | None) -> str:
    return _band(value, COST_BANDS)


def native_shadow_context_key(
    *,
    entity: str,
    lane: str,
    venue: str,
    lifecycle: str,
    regime: str,
    role: str,
    risk_signature: str,
    flow_state: str,
    chase_fraction: float | None,
    latency_seconds: float | None,
    round_trip_cost_fraction: float | None,
) -> str:
    return "|".join(
        (
            str(entity),
            str(lane),
            str(venue),
            str(lifecycle),
            str(regime),
            str(role),
            str(risk_signature),
            str(flow_state),
            robinhood_chase_band(chase_fraction),
            robinhood_latency_band(str(lane), latency_seconds),
            robinhood_cost_band(round_trip_cost_fraction),
        )
    )


def native_copyability_assessment(
    quote: dict[str, Any] | None,
    *,
    signal_price_eth: float | None,
    signal_observed_ts: float | None,
    now_ts: float | None = None,
) -> dict[str, Any]:
    """Separate mechanical feasibility from expensive/late economic context.

    Chase, latency and measurable execution cost are never universal vetoes here.
    They remain context dimensions and must prove positive after-cost geometric edge
    in zero-allocation shadow evidence before any paper allocation is possible.
    """

    blockers: list[str] = []
    if quote is None:
        return {
            "copyable": False,
            "mechanically_executable": False,
            "blockers": ["entry_or_exit_quote_unavailable"],
            "executable_chase_fraction": None,
            "observation_latency_seconds": None,
            "round_trip_cost_fraction": None,
            "chase_band": "unknown",
            "latency_band": "unknown",
            "cost_band": "unknown",
        }

    amount_in = int(quote.get("amount_in_wei") or 0)
    token_out = int(quote.get("token_out") or 0)
    total_cost = int(quote.get("entry_total_cost_wei") or 0)
    immediate_exit = int(quote.get("immediate_exit_wei") or 0)
    entry_price = _finite(quote.get("entry_price_eth"))
    round_trip = _finite(quote.get("round_trip_cost_fraction"))

    if amount_in <= 0 or token_out <= 0 or total_cost <= 0 or entry_price is None or entry_price <= 0.0:
        blockers.append("entry_quote_unavailable")
    if immediate_exit <= 0:
        blockers.append("exit_quote_unavailable")
    if round_trip is None:
        blockers.append("round_trip_cost_unmeasurable")

    chase: float | None = None
    if signal_price_eth is None or signal_price_eth <= 0.0 or entry_price is None:
        blockers.append("signal_price_unavailable")
    else:
        chase = max(0.0, float(entry_price) / float(signal_price_eth) - 1.0)

    latency: float | None = None
    if signal_observed_ts is None or signal_observed_ts <= 0.0:
        blockers.append("signal_observation_time_unavailable")
    else:
        import time

        latency = max(0.0, float(now_ts if now_ts is not None else time.time()) - float(signal_observed_ts))

    mechanically_executable = not blockers
    return {
        "copyable": mechanically_executable,
        "mechanically_executable": mechanically_executable,
        "blockers": list(dict.fromkeys(blockers)),
        "executable_chase_fraction": chase,
        "observation_latency_seconds": latency,
        "round_trip_cost_fraction": round_trip,
        "chase_band": robinhood_chase_band(chase),
        "latency_band": _band(latency, LATENCY_BANDS),
        "cost_band": robinhood_cost_band(round_trip),
        "chase_is_context_not_veto": True,
        "latency_is_lane_context_not_veto": True,
        "measurable_cost_is_context_not_veto": True,
    }


def _query_rows(
    self: Any,
    where_sql: str,
    params: tuple[Any, ...],
    *,
    limit: int = 2000,
) -> list[dict[str, Any]]:
    shadow._ensure_schema(self)
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT o.id outcome_id,o.net_return,t.trigger_entity,t.role,t.lane,t.venue,t.lifecycle,t.regime,"
            "t.risk_signature,t.flow_state,t.executable_chase_fraction,t.observation_latency_seconds,"
            "t.round_trip_cost_fraction,t.release_commit,t.strategy_version "
            "FROM robinhood_v5_shadow_outcomes o "
            "JOIN robinhood_v5_shadow_trials t ON t.id=o.shadow_trial_id "
            "WHERE t.strategy_version=? AND " + where_sql + " ORDER BY o.id DESC LIMIT ?",
            (LEARNING_COMPATIBILITY_VERSION, *params, int(limit)),
        ).fetchall()
    result = [dict(row) for row in rows]
    result.reverse()
    return result


def _same_feature_bands(
    row: dict[str, Any],
    *,
    lane: str,
    chase_fraction: float | None,
    latency_seconds: float | None,
    round_trip_cost_fraction: float | None,
) -> bool:
    return (
        robinhood_chase_band(_finite(row.get("executable_chase_fraction"))) == robinhood_chase_band(chase_fraction)
        and robinhood_latency_band(lane, _finite(row.get("observation_latency_seconds")))
        == robinhood_latency_band(lane, latency_seconds)
        and robinhood_cost_band(_finite(row.get("round_trip_cost_fraction")))
        == robinhood_cost_band(round_trip_cost_fraction)
    )


def _values(rows: list[dict[str, Any]]) -> list[float]:
    return [float(row["net_return"]) for row in rows]


def _hierarchy_rows(
    self: Any,
    *,
    level: str,
    entity: str,
    role: str,
    lane: str,
    venue: str,
    lifecycle: str,
    regime: str,
    risk_signature: str,
    flow_state: str,
    chase_fraction: float | None,
    latency_seconds: float | None,
    round_trip_cost_fraction: float | None,
) -> list[dict[str, Any]]:
    if level == "same_entity_lane_venue_lifecycle_regime_risk":
        return _query_rows(
            self,
            "t.trigger_entity=? AND t.role=? AND t.lane=? AND t.venue=? AND t.lifecycle=? AND t.regime=? AND t.risk_signature=?",
            (entity, role, lane, venue, lifecycle, regime, risk_signature),
        )
    if level == "pooled_lane_venue_lifecycle_regime_risk_bands":
        rows = _query_rows(
            self,
            "t.lane=? AND t.venue=? AND t.lifecycle=? AND t.regime=? AND t.risk_signature=?",
            (lane, venue, lifecycle, regime, risk_signature),
        )
        return [
            row
            for row in rows
            if _same_feature_bands(
                row,
                lane=lane,
                chase_fraction=chase_fraction,
                latency_seconds=latency_seconds,
                round_trip_cost_fraction=round_trip_cost_fraction,
            )
        ]
    if level == "pooled_lane_venue_lifecycle_regime_risk":
        return _query_rows(
            self,
            "t.lane=? AND t.venue=? AND t.lifecycle=? AND t.regime=? AND t.risk_signature=?",
            (lane, venue, lifecycle, regime, risk_signature),
        )
    if level == "pooled_lane_venue_lifecycle_regime":
        return _query_rows(
            self,
            "t.lane=? AND t.venue=? AND t.lifecycle=? AND t.regime=?",
            (lane, venue, lifecycle, regime),
        )
    if level == "pooled_lane_venue_lifecycle":
        return _query_rows(
            self,
            "t.lane=? AND t.venue=? AND t.lifecycle=?",
            (lane, venue, lifecycle),
        )
    if level == "pooled_lane_venue":
        return _query_rows(self, "t.lane=? AND t.venue=?", (lane, venue))
    if level == "pooled_lane":
        return _query_rows(self, "t.lane=?", (lane,))
    if level == "global_robinhood":
        return _query_rows(self, "1=1", ())
    return []


def native_context_returns_shadow(
    self: Any,
    *,
    entity: str,
    role: str,
    lane: str,
    venue: str,
    lifecycle: str,
    regime: str,
    risk_signature: str,
    flow_state: str,
    chase_fraction: float | None,
    latency_seconds: float | None,
    round_trip_cost_fraction: float | None,
) -> tuple[list[float], str]:
    """Use cross-release compatible hierarchical forward evidence.

    `release_commit` is retained on every row for audit lineage, but it is not a
    statistical partition. Specific evidence wins when mature. Otherwise a broader
    parent can contribute only enough real observations to reach the 30-outcome gate,
    with positive parent returns shrunk toward zero while losses remain unshrunk.
    """

    exact = _query_rows(
        self,
        "t.trigger_entity=? AND t.role=? AND t.lane=? AND t.venue=? AND t.lifecycle=? "
        "AND t.regime=? AND t.risk_signature=? AND t.flow_state=?",
        (entity, role, lane, venue, lifecycle, regime, risk_signature, flow_state),
    )
    exact = [
        row
        for row in exact
        if _same_feature_bands(
            row,
            lane=lane,
            chase_fraction=chase_fraction,
            latency_seconds=latency_seconds,
            round_trip_cost_fraction=round_trip_cost_fraction,
        )
    ]
    if len(exact) >= MIN_FORWARD_OUTCOMES_FOR_PAPER_ENTRY:
        return _values(exact), "native_exact_context_cross_release"

    direct_ids = {int(row["outcome_id"]) for row in exact}
    direct_values = _values(exact)
    need = max(0, MIN_FORWARD_OUTCOMES_FOR_PAPER_ENTRY - len(direct_values))

    for level, minimum_parent_samples, base_shrink in HIERARCHY:
        parent = _hierarchy_rows(
            self,
            level=level,
            entity=entity,
            role=role,
            lane=lane,
            venue=venue,
            lifecycle=lifecycle,
            regime=regime,
            risk_signature=risk_signature,
            flow_state=flow_state,
            chase_fraction=chase_fraction,
            latency_seconds=latency_seconds,
            round_trip_cost_fraction=round_trip_cost_fraction,
        )
        if len(parent) < minimum_parent_samples:
            continue
        parent = [row for row in parent if int(row["outcome_id"]) not in direct_ids]
        if len(parent) < need:
            continue
        chosen = parent[-need:] if need > 0 else []
        coverage = min(1.0, len(direct_values) / float(MIN_FORWARD_OUTCOMES_FOR_PAPER_ENTRY))
        shrink = float(base_shrink + (1.0 - base_shrink) * coverage)
        adjusted_parent = [
            float(row["net_return"]) * shrink if float(row["net_return"]) > 0.0 else float(row["net_return"])
            for row in chosen
        ]
        return (
            [*direct_values, *adjusted_parent],
            f"native_hierarchical:{level}:positive_parent_shrink={shrink:.3f}:cross_release",
        )

    return direct_values, "native_exact_context_bootstrap_cross_release" if direct_values else "none"


def _insert_cross_release_deduplicated(self: Any, **kwargs: Any) -> None:
    if _ORIGINAL_INSERT is None:
        raise RuntimeError("Robinhood native shadow insert wrapper is not installed")
    source_key = str(kwargs.get("source_key") or "")
    lanes = [str(value) for value in list(kwargs.get("lanes") or []) if str(value)]
    if not source_key or not lanes:
        return
    shadow._ensure_schema(self)
    allowed: list[str] = []
    with self.store._lock:
        for lane in lanes:
            row = self.store.db.execute(
                "SELECT 1 FROM robinhood_v5_shadow_trials WHERE strategy_version=? AND source_key=? AND lane=? LIMIT 1",
                (LEARNING_COMPATIBILITY_VERSION, source_key, lane),
            ).fetchone()
            if row is None:
                allowed.append(lane)
    if not allowed:
        return
    kwargs = dict(kwargs)
    kwargs["lanes"] = allowed
    _ORIGINAL_INSERT(self, **kwargs)


async def _settle_cross_release(self: Any) -> None:
    shadow._ensure_schema(self)
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT * FROM robinhood_v5_shadow_trials WHERE strategy_version=? AND settled_at IS NULL ORDER BY id LIMIT 120",
            (LEARNING_COMPATIBILITY_VERSION,),
        ).fetchall()
    for row in rows:
        try:
            await shadow._settle_shadow_one(self, dict(row))
        except asyncio.CancelledError:
            raise
        except Exception:
            continue


def _status_native(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("Robinhood native shadow status wrapper is not installed")
    payload = dict(_ORIGINAL_STATUS(self))
    try:
        shadow._ensure_schema(self)
        with self.store._lock:
            totals = self.store.db.execute(
                "SELECT COUNT(*) total,SUM(CASE WHEN settled_at IS NULL THEN 1 ELSE 0 END) open_count,"
                "SUM(CASE WHEN settled_at IS NOT NULL THEN 1 ELSE 0 END) settled_count,COUNT(DISTINCT release_commit) releases "
                "FROM robinhood_v5_shadow_trials WHERE strategy_version=?",
                (LEARNING_COMPATIBILITY_VERSION,),
            ).fetchone()
            outcomes = self.store.db.execute(
                "SELECT COUNT(*) count,AVG(net_return) mean_return,COUNT(DISTINCT release_commit) releases "
                "FROM robinhood_v5_shadow_outcomes WHERE strategy_version=?",
                (LEARNING_COMPATIBILITY_VERSION,),
            ).fetchone()
        strategy = dict(payload.get("strategy_pipeline") or {})
        strategy.update(
            {
                "native_learning_version": NATIVE_LEARNING_VERSION,
                "learning_compatibility_version": LEARNING_COMPATIBILITY_VERSION,
                "release_commit_role": "audit_lineage_only_not_statistical_partition",
                "release_scoped_learning": False,
                "chase_policy": "context_not_universal_veto",
                "chase_bands": [label for _upper, label in CHASE_BANDS],
                "latency_policy": "lane_x_latency_context_not_universal_veto",
                "latency_bands": [label for _upper, label in LATENCY_BANDS],
                "execution_cost_policy": "measurable_context_not_fixed_profitability_veto",
                "hierarchical_forward_learning": True,
                "hierarchy": [name for name, _minimum, _shrink in HIERARCHY],
                "hierarchy_rule": "specific_mature_evidence_overrides_conservatively_shrunk_parent_evidence",
                "mechanical_rejection_only": [
                    "entry_quote_unavailable",
                    "exit_quote_unavailable",
                    "round_trip_cost_unmeasurable",
                    "signal_price_unavailable",
                    "signal_observation_time_unavailable",
                    "structural_restriction_or_liquidity_failure_upstream",
                ],
                "minimum_closed_forward_outcomes_for_paper_entry": MIN_FORWARD_OUTCOMES_FOR_PAPER_ENTRY,
                "bootstrap_paper_allocation_allowed": False,
                "shadow_trials_across_compatible_releases": int(totals["total"] or 0) if totals else 0,
                "open_shadow_trials_across_compatible_releases": int(totals["open_count"] or 0) if totals else 0,
                "settled_shadow_trials_across_compatible_releases": int(totals["settled_count"] or 0) if totals else 0,
                "shadow_trial_release_count": int(totals["releases"] or 0) if totals else 0,
                "shadow_outcomes_across_compatible_releases": int(outcomes["count"] or 0) if outcomes else 0,
                "shadow_outcome_release_count": int(outcomes["releases"] or 0) if outcomes else 0,
                "mean_shadow_return_across_compatible_releases": (
                    float(outcomes["mean_return"])
                    if outcomes and outcomes["mean_return"] is not None
                    else None
                ),
                "new_wallet_specific_provider_requests_added": 0,
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
            }
        )
        # Remove the now-obsolete universal-veto wording from PR #162 status.
        strategy.pop("max_copyable_chase_fraction", None)
        strategy.pop("max_copyable_observation_latency_seconds", None)
        payload["strategy_pipeline"] = strategy
    except Exception as exc:
        payload["native_shadow_learning"] = {
            "version": NATIVE_LEARNING_VERSION,
            "failed_closed": True,
            "error": f"{type(exc).__name__}: native shadow learning status unavailable",
            "paper_only": True,
            "live_money_authority": False,
        }
    return payload


def install_robinhood_native_shadow_learning(plane_cls: type[Any]) -> None:
    global _ORIGINAL_INSERT, _ORIGINAL_STATUS
    if bool(getattr(plane_cls, "_roi_robinhood_native_shadow_learning_installed", False)):
        return
    if not bool(getattr(plane_cls, "_roi_robinhood_pumpfun_shadow_boundary_installed", False)):
        raise RuntimeError("Pump.fun shadow boundary must be installed before native Robinhood learning")

    _ORIGINAL_INSERT = shadow._insert_shadow_trials
    _ORIGINAL_STATUS = plane_cls.status

    # The already-installed shadow wrappers resolve these module globals at call time.
    # Replacing them here changes economics without introducing a second entry path.
    shadow.copyability_assessment = native_copyability_assessment
    shadow._shadow_context_key = native_shadow_context_key
    shadow._context_returns_shadow = native_context_returns_shadow
    shadow._insert_shadow_trials = _insert_cross_release_deduplicated
    shadow._settle_shadow_trials = _settle_cross_release

    plane_cls.status = _status_native  # type: ignore[method-assign]
    setattr(plane_cls.status, "_roi_robinhood_native_shadow_learning", True)
    setattr(plane_cls._v5_choose_lane_fraction, "_roi_robinhood_native_shadow_learning", True)
    setattr(plane_cls, "_roi_robinhood_native_shadow_learning_installed", True)
    setattr(plane_cls, "_roi_robinhood_native_shadow_learning_version", NATIVE_LEARNING_VERSION)


__all__ = [
    "NATIVE_LEARNING_VERSION",
    "LEARNING_COMPATIBILITY_VERSION",
    "CHASE_BANDS",
    "LATENCY_BANDS",
    "HIERARCHY",
    "robinhood_chase_band",
    "robinhood_latency_band",
    "native_copyability_assessment",
    "native_context_returns_shadow",
    "install_robinhood_native_shadow_learning",
]
