from __future__ import annotations

from typing import Any, Callable, Iterable, Sequence

from . import robinhood_pumpfun_wallet_intelligence as intelligence


ALIGNMENT_VERSION = "robinhood-wallet-intelligence-v51-alignment-v1"
IMMEDIATE_COPY_CHASE_REFERENCE_FRACTION = float(intelligence.MAX_CHASE_FRACTION)
IMMEDIATE_COPY_OBSERVATION_REFERENCE_SECONDS = float(intelligence.MAX_OBSERVATION_LAG_SECONDS)

_ORIGINAL_ENSURE_SCHEMA: Callable[..., Any] | None = None
_ORIGINAL_ENRICH: Callable[..., int] | None = None
_ORIGINAL_CANDIDATE_PROFILE: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_BUILD_UNIVERSE: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_INTELLIGENCE_STATUS: Callable[..., dict[str, Any]] | None = None
_INSTALLED = False


def _number(value: Any) -> float | None:
    return intelligence._safe_float(value)


def strategy_observable(
    *,
    copyable_price_eth: float | None,
    copyable_quote_wei: float | None,
    chase_fraction: float | None,
    observation_lag_ms: float | None,
) -> bool:
    """Return whether a forward mark is mechanically measurable for v5.1 learning.

    Chase and observation lag are context dimensions, not universal vetoes. A row is
    usable for teacher evaluation when the post-signal market mark and its timing are
    measurable. Profitability after those realized costs still has to clear the
    existing forward/geometric/risk gates before the wallet can become a teacher.
    """

    price = _number(copyable_price_eth)
    quote = _number(copyable_quote_wei)
    chase = _number(chase_fraction)
    lag = _number(observation_lag_ms)
    return bool(
        price is not None
        and price > 0.0
        and quote is not None
        and quote > 0.0
        and chase is not None
        and chase >= 0.0
        and lag is not None
        and lag >= 0.0
    )


def immediate_copyable(
    *,
    copyable_price_eth: float | None,
    copyable_quote_wei: float | None,
    chase_fraction: float | None,
    observation_lag_ms: float | None,
) -> bool:
    """Legacy immediate-copy diagnostic only; never strategy promotion authority."""

    chase = _number(chase_fraction)
    lag = _number(observation_lag_ms)
    return bool(
        strategy_observable(
            copyable_price_eth=copyable_price_eth,
            copyable_quote_wei=copyable_quote_wei,
            chase_fraction=chase_fraction,
            observation_lag_ms=observation_lag_ms,
        )
        and chase is not None
        and chase <= IMMEDIATE_COPY_CHASE_REFERENCE_FRACTION
        and lag is not None
        and lag <= IMMEDIATE_COPY_OBSERVATION_REFERENCE_SECONDS * 1000.0
    )


def _normalize_forward_rows_with_connection(db: Any, swap_ids: Sequence[int] | None = None) -> None:
    where = ""
    params: list[Any] = [
        IMMEDIATE_COPY_CHASE_REFERENCE_FRACTION,
        IMMEDIATE_COPY_OBSERVATION_REFERENCE_SECONDS * 1000.0,
    ]
    if swap_ids is not None:
        normalized = sorted({int(value) for value in swap_ids})
        if not normalized:
            return
        where = " WHERE swap_id IN (" + ",".join("?" for _ in normalized) + ")"
        params.extend(normalized)
    db.execute(
        "UPDATE robinhood_wallet_intelligence_forward SET "
        "immediate_copyable=CASE WHEN "
        "copyable_price_eth IS NOT NULL AND copyable_price_eth>0 "
        "AND copyable_quote_wei IS NOT NULL AND copyable_quote_wei>0 "
        "AND chase_fraction IS NOT NULL AND chase_fraction>=0 "
        "AND observation_lag_ms IS NOT NULL AND observation_lag_ms>=0 "
        "AND chase_fraction<=? AND observation_lag_ms<=? THEN 1 ELSE 0 END, "
        "copyable=CASE WHEN "
        "copyable_price_eth IS NOT NULL AND copyable_price_eth>0 "
        "AND copyable_quote_wei IS NOT NULL AND copyable_quote_wei>0 "
        "AND chase_fraction IS NOT NULL AND chase_fraction>=0 "
        "AND observation_lag_ms IS NOT NULL AND observation_lag_ms>=0 "
        "THEN 1 ELSE 0 END" + where,
        tuple(params),
    )


def _ensure_schema_v51(self: Any) -> None:
    if _ORIGINAL_ENSURE_SCHEMA is None:
        raise RuntimeError("Robinhood wallet intelligence v5.1 alignment is not installed")
    _ORIGINAL_ENSURE_SCHEMA(self)
    if bool(getattr(self, "_roi_robinhood_wallet_intelligence_v51_schema_ready", False)):
        return

    # Column creation and legacy-row rewrite share one transaction. If the process
    # exits mid-migration, SQLite rolls the migration back instead of leaving the new
    # column present with old hard-veto semantics. Once migrated, ordinary profile
    # reads are read-only; only freshly attempted rows are normalized after each poll.
    with self.store._lock, self.store.db:
        columns = {
            str(row[1])
            for row in self.store.db.execute(
                "PRAGMA table_info(robinhood_wallet_intelligence_forward)"
            ).fetchall()
        }
        if "immediate_copyable" not in columns:
            self.store.db.execute(
                "ALTER TABLE robinhood_wallet_intelligence_forward "
                "ADD COLUMN immediate_copyable INTEGER NOT NULL DEFAULT 0"
            )
            _normalize_forward_rows_with_connection(self.store.db)
    setattr(self, "_roi_robinhood_wallet_intelligence_v51_schema_ready", True)


def _pending_forward_swap_ids(self: Any, limit: int) -> list[int]:
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT f.swap_id FROM robinhood_wallet_selection_forward f "
            "LEFT JOIN robinhood_wallet_intelligence_forward i ON i.swap_id=f.swap_id "
            "WHERE i.swap_id IS NULL ORDER BY f.swap_id LIMIT ?",
            (max(1, int(limit)),),
        ).fetchall()
    return [int(row["swap_id"]) for row in rows]


def _enrich_forward_observations_v51(self: Any, limit: int = 250) -> int:
    if _ORIGINAL_ENRICH is None:
        raise RuntimeError("Robinhood wallet intelligence v5.1 alignment is not installed")
    _ensure_schema_v51(self)
    pending = _pending_forward_swap_ids(self, limit)
    inserted = int(_ORIGINAL_ENRICH(self, limit=limit))
    if pending:
        with self.store._lock, self.store.db:
            _normalize_forward_rows_with_connection(self.store.db, pending)
    return inserted


def _candidate_profile_v51(self: Any, actor: str) -> dict[str, Any]:
    if _ORIGINAL_CANDIDATE_PROFILE is None:
        raise RuntimeError("Robinhood wallet intelligence v5.1 alignment is not installed")
    _ensure_schema_v51(self)
    profile = dict(_ORIGINAL_CANDIDATE_PROFILE(self, actor))
    with self.store._lock:
        row = self.store.db.execute(
            "SELECT COUNT(*) AS total,"
            "SUM(CASE WHEN immediate_copyable=1 THEN 1 ELSE 0 END) AS immediate_count "
            "FROM robinhood_wallet_intelligence_forward WHERE actor=?",
            (actor,),
        ).fetchone()
    total = int(row["total"] if row is not None else 0)
    immediate_count = int(row["immediate_count"] or 0) if row is not None else 0

    # `copyable` is retained as a schema/API compatibility alias, but after this
    # repair it means mechanically observable strategy evidence, not <=15%/<=20s.
    profile.update(
        {
            "strategy_observable_observations": int(profile.get("copyable_observations") or 0),
            "strategy_observable_rate": float(profile.get("copyability_rate") or 0.0),
            "immediate_copyable_observations": immediate_count,
            "immediate_copyability_rate": immediate_count / total if total else 0.0,
            "teacher_promotion_uses_immediate_copy_gate": False,
            "chase_policy": "context_not_universal_veto",
            "latency_policy": "lane_x_latency_context_not_universal_veto",
            "immediate_copy_reference_chase_fraction": IMMEDIATE_COPY_CHASE_REFERENCE_FRACTION,
            "immediate_copy_reference_observation_seconds": IMMEDIATE_COPY_OBSERVATION_REFERENCE_SECONDS,
            "wallet_intelligence_alignment_version": ALIGNMENT_VERSION,
        }
    )
    return profile


def _decorate_universe_payload(payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    parity = dict(result.get("pumpfun_forward_intelligence_parity") or {})
    parity.pop("max_chase_fraction", None)
    parity.pop("max_observation_lag_seconds", None)
    parity.update(
        {
            "wallet_intelligence_alignment_version": ALIGNMENT_VERSION,
            "strategy_observable_definition": "measurable_post_signal_mark_with_measurable_latency_and_cost_proxy",
            "chase_policy": "context_not_universal_veto",
            "latency_policy": "lane_x_latency_context_not_universal_veto",
            "teacher_promotion_uses_immediate_copy_gate": False,
            "legacy_immediate_copy_diagnostic": {
                "max_chase_fraction": IMMEDIATE_COPY_CHASE_REFERENCE_FRACTION,
                "max_observation_lag_seconds": IMMEDIATE_COPY_OBSERVATION_REFERENCE_SECONDS,
                "promotion_authority": False,
                "purpose": "diagnostic comparison to legacy immediate-copy behavior only",
            },
        }
    )
    result["pumpfun_forward_intelligence_parity"] = parity
    result["robinhood_v51_forward_intelligence"] = {
        "alignment_version": ALIGNMENT_VERSION,
        "chase_policy": "context_not_universal_veto",
        "latency_policy": "lane_x_latency_context_not_universal_veto",
        "profitability_gate": "positive_after_cost_forward_return_and_geometric_growth",
        "risk_gates_preserved": True,
        "paper_only": True,
        "live_money_authority": False,
    }
    return result


def _build_intelligence_entity_universe_v51(
    evidence_rows: Iterable[dict[str, Any]],
    research_rows: Iterable[dict[str, Any]] = (),
    *,
    capacity: int = intelligence.universe.TRACKING_CAPACITY_LIMIT,
) -> dict[str, Any]:
    if _ORIGINAL_BUILD_UNIVERSE is None:
        raise RuntimeError("Robinhood wallet intelligence v5.1 alignment is not installed")
    return _decorate_universe_payload(
        _ORIGINAL_BUILD_UNIVERSE(evidence_rows, research_rows, capacity=capacity)
    )


def _intelligence_status_v51(self: Any) -> dict[str, Any]:
    if _ORIGINAL_INTELLIGENCE_STATUS is None:
        raise RuntimeError("Robinhood wallet intelligence v5.1 alignment is not installed")
    _ensure_schema_v51(self)
    payload = dict(_ORIGINAL_INTELLIGENCE_STATUS(self))
    with self.store._lock:
        row = self.store.db.execute(
            "SELECT COUNT(*) AS total,"
            "SUM(CASE WHEN copyable=1 THEN 1 ELSE 0 END) AS strategy_count,"
            "SUM(CASE WHEN immediate_copyable=1 THEN 1 ELSE 0 END) AS immediate_count "
            "FROM robinhood_wallet_intelligence_forward"
        ).fetchone()
    total = int(row["total"] if row is not None else 0)
    strategy_count = int(row["strategy_count"] or 0) if row is not None else 0
    immediate_count = int(row["immediate_count"] or 0) if row is not None else 0
    payload.update(
        {
            "selection_model": "robinhood_v51_contextual_forward_wallet_intelligence",
            "wallet_intelligence_alignment_version": ALIGNMENT_VERSION,
            "strategy_observable_forward_observations": strategy_count,
            "strategy_observable_forward_fraction": strategy_count / total if total else 0.0,
            "immediate_copy_forward_observations": immediate_count,
            "immediate_copy_forward_fraction": immediate_count / total if total else 0.0,
            "copyable_forward_fields_are_strategy_observable_compatibility_aliases": True,
            "teacher_promotion_uses_immediate_copy_gate": False,
            "chase_policy": "context_not_universal_veto",
            "latency_policy": "lane_x_latency_context_not_universal_veto",
            "legacy_immediate_copy_diagnostic": {
                "max_chase_fraction": IMMEDIATE_COPY_CHASE_REFERENCE_FRACTION,
                "max_observation_lag_seconds": IMMEDIATE_COPY_OBSERVATION_REFERENCE_SECONDS,
                "promotion_authority": False,
            },
        }
    )
    return payload


def install_robinhood_wallet_intelligence_v51_alignment(plane_cls: type[Any]) -> None:
    global _ORIGINAL_ENSURE_SCHEMA, _ORIGINAL_ENRICH, _ORIGINAL_CANDIDATE_PROFILE
    global _ORIGINAL_BUILD_UNIVERSE, _ORIGINAL_INTELLIGENCE_STATUS, _INSTALLED
    if _INSTALLED:
        return

    _ORIGINAL_ENSURE_SCHEMA = intelligence._ensure_schema
    _ORIGINAL_ENRICH = intelligence._enrich_forward_observations
    _ORIGINAL_CANDIDATE_PROFILE = intelligence._candidate_profile
    _ORIGINAL_BUILD_UNIVERSE = intelligence.build_intelligence_entity_universe
    _ORIGINAL_INTELLIGENCE_STATUS = intelligence._intelligence_status

    intelligence._ensure_schema = _ensure_schema_v51  # type: ignore[assignment]
    intelligence._enrich_forward_observations = _enrich_forward_observations_v51  # type: ignore[assignment]
    intelligence._candidate_profile = _candidate_profile_v51  # type: ignore[assignment]
    intelligence.build_intelligence_entity_universe = _build_intelligence_entity_universe_v51  # type: ignore[assignment]
    intelligence._intelligence_status = _intelligence_status_v51  # type: ignore[assignment]

    setattr(intelligence, "_roi_robinhood_wallet_intelligence_v51_alignment_installed", True)
    setattr(intelligence, "_roi_robinhood_wallet_intelligence_v51_alignment_version", ALIGNMENT_VERSION)
    setattr(plane_cls, "_roi_robinhood_wallet_intelligence_v51_alignment_installed", True)
    setattr(plane_cls, "_roi_robinhood_wallet_intelligence_v51_alignment_version", ALIGNMENT_VERSION)
    _INSTALLED = True


__all__ = [
    "ALIGNMENT_VERSION",
    "IMMEDIATE_COPY_CHASE_REFERENCE_FRACTION",
    "IMMEDIATE_COPY_OBSERVATION_REFERENCE_SECONDS",
    "strategy_observable",
    "immediate_copyable",
    "install_robinhood_wallet_intelligence_v51_alignment",
]
