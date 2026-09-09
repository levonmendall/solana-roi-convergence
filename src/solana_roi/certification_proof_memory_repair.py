from __future__ import annotations

import json
import math
import os
import random
import threading
from statistics import mean
from typing import Any, Callable, Iterator, Sequence

from . import production_proof_read_boundary_repair as proof
from . import v51_economic_certification as economic
from . import v51_economic_core as economic_core
from . import v51_phase14_profitability_certification as phase14
from . import v51_phase17_context_certification as phase17
from . import v51_cross_surface_proof as cross_surface
from . import risk_conditioned_alpha_v51 as v51
from .strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH


REPAIR_VERSION = "certification-proof-memory-v1-bounded-shadow-shared-promotion"
BOOTSTRAP_MEMORY_REPAIR_VERSION = "certification-bootstrap-memory-v1-weighted-cluster-resampling"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
CERTIFICATION_THRESHOLDS_CHANGED = False
ECONOMIC_THRESHOLDS_CHANGED = False
CANONICAL_EVIDENCE_RESET = False

_LOCK = threading.Lock()
_LOCAL = threading.local()
_INSTALLED = False
_ORIGINAL_ECONOMIC_RECORDS: Callable[[Any], list[dict[str, Any]]] = economic._records
_ORIGINAL_COMBINED_PROMOTION_RECORDS: Callable[
    [Any, dict[str, Any] | None], list[dict[str, Any]]
] = cross_surface.combined_promotion_records
_ORIGINAL_BOOTSTRAP_DISTRIBUTIONS = economic_core._bootstrap_distributions
_ORIGINAL_PROOF_BUILDER: Callable[[], dict[str, Any]] | None = None
_STATE: dict[str, Any] = {
    "bounded_record_builds": 0,
    "fomo_outcome_rows_read": 0,
    "promotion_cache_hits": 0,
    "promotion_cache_misses": 0,
    "proof_generations": 0,
    "bounded_bootstrap_calls": 0,
    "bounded_bootstrap_samples": 0,
    "bounded_bootstrap_max_source_values": 0,
}


def _inc(name: str, amount: int = 1) -> None:
    with _LOCK:
        _STATE[name] = int(_STATE.get(name, 0) or 0) + int(amount)


def _max_state(name: str, value: int) -> None:
    with _LOCK:
        _STATE[name] = max(int(_STATE.get(name, 0) or 0), int(value))


def _bounded_records(store: Any) -> list[dict[str, Any]]:
    """Preserve the canonical economic population without scanning all FOMO shadow history.

    The canonical shadow table is uniquely keyed by (release_commit, source_signature).
    Join only the shadow row belonging to each already freeze-epoch-bounded FOMO
    outcome. All non-FOMO queries and record shaping intentionally mirror the
    incumbent v5.1 economic certification reader.
    """

    if not economic._table_exists(store, "v51_economic_freeze_releases"):
        return []
    _inc("bounded_record_builds")
    records: list[dict[str, Any]] = []

    if economic._table_exists(store, "risk_conditioned_alpha_v5_outcomes") and economic._table_exists(
        store, "risk_conditioned_alpha_v5_trials"
    ):
        with store._lock:
            rows = store.db.execute(
                "SELECT o.id,o.release_commit,o.source_signature,o.token_mint,o.lane,o.venue,o.lifecycle,o.regime,"
                "o.risk_signature,o.context_key,o.position_fraction,o.net_return,o.settled_at,"
                "t.trigger_wallet,t.flow_state,t.risk_severity,t.chase_band,t.latency_band,t.round_trip_cost_fraction "
                "FROM risk_conditioned_alpha_v5_outcomes o "
                "JOIN v51_economic_freeze_releases e ON e.release_commit=o.release_commit "
                "LEFT JOIN risk_conditioned_alpha_v5_trials t ON t.release_commit=o.release_commit "
                "AND t.source_signature=o.source_signature AND t.lane=o.lane "
                "WHERE e.economic_freeze_epoch=? AND e.authority_id=? ORDER BY o.id",
                (ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID),
            ).fetchall()
        for row in rows:
            d = dict(row)
            d.update({"surface": "SOLANA_ALPHA", "entity": str(d.get("trigger_wallet") or "unknown")})
            d["family"] = economic._family(
                "SOLANA_ALPHA",
                str(d.get("venue") or "UNKNOWN"),
                str(d.get("risk_signature") or "clean"),
            )
            records.append(d)

    if economic._table_exists(store, "fomo_paper_outcomes") and economic._table_exists(
        store, "fomo_paper_trials"
    ):
        shadow_exists = economic._table_exists(store, "fomo_shadow_observations")
        shadow_select = "s.state_json AS shadow_state_json," if shadow_exists else "NULL AS shadow_state_json,"
        shadow_join = (
            "LEFT JOIN fomo_shadow_observations s ON s.release_commit=o.release_commit "
            "AND s.source_signature=o.source_signature "
            if shadow_exists
            else ""
        )
        with store._lock:
            rows = store.db.execute(
                "SELECT o.id,o.release_commit,o.source_signature,o.token_mint,o.trigger_wallet,o.venue,o.lifecycle,o.regime,"
                "o.position_fraction,o.net_return,o.settled_at,t.fomo_state,t.signal_to_entry_seconds,t.entry_cost_sol,"
                + shadow_select
                + "o.release_commit AS _bounded_release_commit "
                "FROM fomo_paper_outcomes o JOIN v51_economic_freeze_releases e ON e.release_commit=o.release_commit "
                "LEFT JOIN fomo_paper_trials t ON t.release_commit=o.release_commit AND t.source_signature=o.source_signature "
                + shadow_join
                + "WHERE e.economic_freeze_epoch=? AND e.authority_id=? ORDER BY o.id",
                (ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID),
            ).fetchall()
        _inc("fomo_outcome_rows_read", len(rows))
        for row in rows:
            d = dict(row)
            raw = str(d.pop("shadow_state_json", None) or "{}")
            d.pop("_bounded_release_commit", None)
            try:
                state = json.loads(raw)
            except Exception:
                state = {}
            normalized_state = state if isinstance(state, dict) else {}
            signature = v51.fomo_hazard_signature(normalized_state)
            d.update(
                {
                    "surface": "FOMO",
                    "entity": str(d.get("trigger_wallet") or "unknown"),
                    "lane": "fomo_continuation",
                    "risk_signature": signature,
                    "risk_severity": v51.fomo_hazard_severity(normalized_state),
                    "flow_state": str(d.get("fomo_state") or "unknown"),
                    "latency_band": economic._latency_band(
                        economic._safe(d.get("signal_to_entry_seconds"), -1.0)
                    ),
                    "round_trip_cost_fraction": None,
                    "chase_band": "unknown",
                    "context_key": "",
                }
            )
            d["family"] = economic._family(
                "FOMO", str(d.get("venue") or "UNKNOWN"), signature
            )
            records.append(d)

    if (
        economic._table_exists(store, "robinhood_paper_outcomes")
        and economic._table_exists(store, "robinhood_v5_trial_context")
        and economic._table_exists(store, "robinhood_paper_trials")
    ):
        with store._lock:
            rows = store.db.execute(
                "SELECT o.id,o.release_commit,o.trial_id,o.net_return,o.settled_at,t.token AS token_mint,t.trigger_entity AS entity,"
                "t.venue,t.lifecycle,t.position_fraction,t.entry_round_trip_cost_fraction AS round_trip_cost_fraction,"
                "c.lane,c.regime,c.flow_state,c.risk_signature,c.risk_severity,c.context_key,c.latency_band "
                "FROM robinhood_paper_outcomes o JOIN v51_economic_freeze_releases e ON e.release_commit=o.release_commit "
                "JOIN robinhood_paper_trials t ON t.id=o.trial_id JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
                "WHERE e.economic_freeze_epoch=? AND e.authority_id=? ORDER BY o.id",
                (ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID),
            ).fetchall()
        for row in rows:
            d = dict(row)
            d.update(
                {
                    "surface": "ROBINHOOD_CHAIN",
                    "source_signature": f"robinhood_trial:{d.get('trial_id')}",
                    "chase_band": "unknown",
                }
            )
            d["family"] = "ROBINHOOD_CHAIN"
            records.append(d)
    return records


def _iter_weighted_groups(
    groups: Sequence[Sequence[float]], multiplicities: Sequence[int]
) -> Iterator[float]:
    for index, group in enumerate(groups):
        count = int(multiplicities[index])
        for _ in range(count):
            for value in group:
                yield float(value)


def _weighted_median(
    ordered_items: Sequence[tuple[float, int]], multiplicities: Sequence[int], total_count: int
) -> float:
    if total_count <= 0:
        raise ValueError("weighted median requires observations")
    left_rank = (total_count - 1) // 2
    right_rank = total_count // 2
    left: float | None = None
    right: float | None = None
    seen = 0
    for value, group_index in ordered_items:
        weight = int(multiplicities[group_index])
        if weight <= 0:
            continue
        next_seen = seen + weight
        if left is None and left_rank < next_seen:
            left = float(value)
        if right_rank < next_seen:
            right = float(value)
            break
        seen = next_seen
    if left is None or right is None:
        raise RuntimeError("weighted median rank accounting failed")
    return (left + right) / 2.0 if left_rank != right_rank else right


def _weighted_expected_shortfall(
    ordered_items: Sequence[tuple[float, int]], multiplicities: Sequence[int], total_count: int
) -> float:
    tail_n = max(1, int(math.ceil(total_count * 0.20)))

    def tail_values() -> Iterator[float]:
        remaining = tail_n
        for value, group_index in ordered_items:
            if remaining <= 0:
                break
            weight = min(remaining, int(multiplicities[group_index]))
            for _ in range(max(0, weight)):
                yield float(value)
            remaining -= max(0, weight)

    return mean(tail_values())


def _bounded_bootstrap_distributions(
    values: Sequence[float],
    *,
    fraction: float,
    cluster_ids: Sequence[str] | None = None,
    samples: int = economic_core.BOOTSTRAP_SAMPLES,
) -> dict[str, list[float]]:
    """Canonical cluster bootstrap without materializing one full resampled draw per iteration.

    The RNG seed, number of cluster selections, sample count, and all reported statistics
    remain identical to the incumbent algorithm. A multiplicity vector represents the
    selected clusters; scalar statistics iterate those weights and median/shortfall use
    weighted order statistics over the same resampled multiset.
    """
    groups = economic_core._cluster_groups(values, cluster_ids)
    if not groups:
        return {"mean": [], "median": [], "log_growth": [], "expected_shortfall_20": []}
    normalized_groups = [tuple(float(value) for value in group) for group in groups]
    ordered_items = sorted(
        (float(value), group_index)
        for group_index, group in enumerate(normalized_groups)
        for value in group
    )
    rng = random.Random(economic_core.BOOTSTRAP_SEED + len(values) * 131 + len(groups) * 17)
    distributions: dict[str, list[float]] = {
        "mean": [],
        "median": [],
        "log_growth": [],
        "expected_shortfall_20": [],
    }
    count = max(1, int(samples))
    _inc("bounded_bootstrap_calls")
    _inc("bounded_bootstrap_samples", count)
    _max_state("bounded_bootstrap_max_source_values", len(values))
    group_count = len(normalized_groups)
    for _ in range(count):
        multiplicities = [0] * group_count
        for _index in range(group_count):
            multiplicities[rng.randrange(group_count)] += 1
        total_count = sum(
            multiplicities[index] * len(group)
            for index, group in enumerate(normalized_groups)
        )
        if total_count <= 0:
            continue
        distributions["mean"].append(mean(_iter_weighted_groups(normalized_groups, multiplicities)))
        distributions["median"].append(_weighted_median(ordered_items, multiplicities, total_count))

        def log_values() -> Iterator[float]:
            for value in _iter_weighted_groups(normalized_groups, multiplicities):
                terminal = 1.0 + fraction * value
                if terminal <= 0.0:
                    yield float("-inf")
                else:
                    yield math.log(terminal)

        distributions["log_growth"].append(mean(log_values()))
        distributions["expected_shortfall_20"].append(
            _weighted_expected_shortfall(ordered_items, multiplicities, total_count)
        )
    return distributions


def _shared_combined_promotion_records(
    store: Any,
    robinhood_proof: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Share one cross-surface promotion population without dropping Robinhood evidence.

    The canonical cross-surface reader accepts both the store and the isolated Robinhood
    proof. Cache only calls that use the same store and the same proof object during one
    production-proof generation so nested Phase 14/17 reads reuse the population while
    distinct Robinhood proof snapshots can never alias each other.
    """
    cache = getattr(_LOCAL, "promotion_cache", None)
    if not isinstance(cache, dict):
        return _ORIGINAL_COMBINED_PROMOTION_RECORDS(store, robinhood_proof)
    key = (id(store), id(robinhood_proof))
    if key in cache:
        _inc("promotion_cache_hits")
        return cache[key]
    _inc("promotion_cache_misses")
    rows = _ORIGINAL_COMBINED_PROMOTION_RECORDS(store, robinhood_proof)
    cache[key] = rows
    return rows


def _proof_with_shared_promotion_population() -> dict[str, Any]:
    if _ORIGINAL_PROOF_BUILDER is None:
        raise RuntimeError("certification proof memory repair missing canonical proof builder")
    previous = getattr(_LOCAL, "promotion_cache", None)
    _LOCAL.promotion_cache = {}
    _inc("proof_generations")
    try:
        return _ORIGINAL_PROOF_BUILDER()
    finally:
        if previous is None:
            try:
                delattr(_LOCAL, "promotion_cache")
            except AttributeError:
                pass
        else:
            _LOCAL.promotion_cache = previous


setattr(_proof_with_shared_promotion_population, "_roi_certification_proof_memory_bounded", True)


def _process_thread_count() -> int | None:
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("Threads:"):
                    return int(line.split(":", 1)[1].strip())
    except Exception:
        return None
    return None


def status() -> dict[str, Any]:
    with _LOCK:
        state = dict(_STATE)
    return {
        "repair_version": REPAIR_VERSION,
        "bootstrap_memory_repair_version": BOOTSTRAP_MEMORY_REPAIR_VERSION,
        "installed": _INSTALLED,
        **state,
        "fomo_shadow_lookup": "freeze_epoch_outcome_keyed_left_join",
        "unbounded_fomo_shadow_scan": False,
        "promotion_population_shared_within_production_proof": True,
        "bootstrap_materialized_resample_draw_lists": False,
        "bootstrap_seed_changed": False,
        "bootstrap_sample_count_changed": False,
        "bootstrap_cluster_resampling_changed": False,
        "process_thread_count": _process_thread_count(),
        "python_thread_count": threading.active_count(),
        "resource_guard_relaxed": False,
        "stale_gate_relaxed": False,
        "continuity_gate_relaxed": False,
        "certification_thresholds_changed": CERTIFICATION_THRESHOLDS_CHANGED,
        "economic_thresholds_changed": ECONOMIC_THRESHOLDS_CHANGED,
        "canonical_evidence_reset": CANONICAL_EVIDENCE_RESET,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


def install_certification_proof_memory_repair(app: Any) -> None:
    global _INSTALLED, _ORIGINAL_PROOF_BUILDER
    if _INSTALLED or bool(getattr(app.state, "roi_certification_proof_memory_repair", False)):
        return

    economic._records = _bounded_records  # type: ignore[assignment]
    economic_core._bootstrap_distributions = _bounded_bootstrap_distributions  # type: ignore[assignment]
    phase14.combined_promotion_records = _shared_combined_promotion_records  # type: ignore[assignment]
    phase17.combined_promotion_records = _shared_combined_promotion_records  # type: ignore[assignment]

    current = proof._ORIGINAL_PRODUCTION_PROOF
    if current is None:
        raise RuntimeError("production proof read boundary must be installed before proof memory repair")
    if not bool(getattr(current, "_roi_certification_proof_memory_bounded", False)):
        _ORIGINAL_PROOF_BUILDER = current
        proof._ORIGINAL_PRODUCTION_PROOF = _proof_with_shared_promotion_population

    path = "/v1/operations/certification-proof-memory-repair"
    if path not in {getattr(route, "path", None) for route in app.routes}:
        app.add_api_route(path, status, methods=["GET"], name="certification_proof_memory_repair")

    app.state.roi_certification_proof_memory_repair = True
    app.state.roi_certification_proof_memory_repair_version = REPAIR_VERSION
    app.state.roi_certification_bootstrap_memory_repair_version = BOOTSTRAP_MEMORY_REPAIR_VERSION
    _INSTALLED = True


__all__ = [
    "BOOTSTRAP_MEMORY_REPAIR_VERSION",
    "REPAIR_VERSION",
    "_bounded_bootstrap_distributions",
    "_bounded_records",
    "_process_thread_count",
    "_proof_with_shared_promotion_population",
    "_shared_combined_promotion_records",
    "install_certification_proof_memory_repair",
    "status",
]
