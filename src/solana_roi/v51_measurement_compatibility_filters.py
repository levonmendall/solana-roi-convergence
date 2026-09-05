from __future__ import annotations

from typing import Any

from .strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH
from .v51_economic_core import hierarchical_profile
from .v51_measurement_integrity import MEASUREMENT_EPOCH, ensure_release_compatibility
from .v51_promotion_proof import cluster_rows, surface_attested

FILTER_VERSION = "v51-measurement-compatible-promotion-filter-v3-attested-clustered-oos"
_INSTALLED = False


def _compatible_join(alias: str = "m") -> str:
    # Surface attestation is enforced in Python before these queries. The SQL join
    # keeps release/measurement lineage exact without relying on the old global
    # promotion_eligible bit, which cannot safely represent three isolated surfaces.
    return (
        f"JOIN v51_release_compatibility {alias} ON {alias}.release_commit=o.release_commit "
        f"AND {alias}.measurement_epoch=? "
    )


def _solana_evidence_compatible(
    adapter: Any,
    *,
    lane: str,
    pre: dict[str, Any],
    context_key: str,
) -> tuple[list[float], list[float]]:
    from . import risk_conditioned_alpha_v51 as v51
    from . import v51_consolidated_strategy as consolidated

    release = getattr(adapter, "release_commit", None)
    consolidated._ensure_epoch(adapter.store, release)
    ensure_release_compatibility(adapter.store, release)
    if not surface_attested(adapter.store, "SOLANA", release_commit=release):
        return [], []
    parsed = v51._parse_context_key(context_key)
    entity = str(parsed.get("entity") or pre.get("trigger_entity") or "")
    risk_signature = str((pre.get("risk") or {}).get("risk_signature") or "clean")
    if not consolidated._table_exists(adapter.store, "risk_conditioned_alpha_v5_outcomes"):
        return [], []
    with adapter.store._lock:
        exact_rows = adapter.store.db.execute(
            "SELECT o.source_signature,o.net_return,o.token_mint,o.lifecycle,o.venue,o.settled_at FROM risk_conditioned_alpha_v5_outcomes o "
            "JOIN v51_economic_freeze_releases e ON e.release_commit=o.release_commit "
            + _compatible_join("m")
            + "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND o.context_key=? ORDER BY o.id",
            (MEASUREMENT_EPOCH, ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID, context_key),
        ).fetchall()
        parent_rows = adapter.store.db.execute(
            "SELECT o.source_signature,o.net_return,o.token_mint,o.lifecycle,o.venue,o.settled_at FROM risk_conditioned_alpha_v5_outcomes o "
            "JOIN v51_economic_freeze_releases e ON e.release_commit=o.release_commit "
            + _compatible_join("m")
            + "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND o.lane=? AND o.venue=? AND o.lifecycle=? "
            "AND o.risk_signature=? AND o.context_key LIKE ? AND o.context_key<>? ORDER BY o.id",
            (
                MEASUREMENT_EPOCH,
                ECONOMIC_FREEZE_EPOCH,
                AUTHORITY_ID,
                lane,
                str(pre.get("venue") or "UNKNOWN"),
                str(pre.get("lifecycle") or "unknown"),
                risk_signature,
                entity + "|%",
                context_key,
            ),
        ).fetchall()
    exact_raw = [dict(row) for row in consolidated._dedup(list(exact_rows), "source_signature")]
    exact = cluster_rows(exact_raw, family=f"SOLANA:{lane}", promotion_only=True)
    exact_clusters = {str(row["event_cluster_id"]) for row in exact}
    parent_raw = [dict(row) for row in consolidated._dedup(list(parent_rows), "source_signature")]
    parent = cluster_rows(
        parent_raw,
        family=f"SOLANA:{lane}",
        excluded_cluster_ids=exact_clusters,
        promotion_only=True,
    )
    return [float(row["net_return"]) for row in exact], [float(row["net_return"]) for row in parent]


def _fomo_epoch_returns_compatible(
    adapter: Any,
    *,
    wallet: str,
    venue: str,
    lifecycle: str,
    regime: str,
    hazard_signature: str,
) -> list[float]:
    from . import risk_conditioned_alpha_v5 as v5
    from . import risk_conditioned_alpha_v51 as v51
    from . import v51_consolidated_strategy as consolidated

    release = getattr(adapter, "release_commit", None)
    consolidated._ensure_epoch(adapter.store, release)
    ensure_release_compatibility(adapter.store, release)
    if not surface_attested(adapter.store, "FOMO", release_commit=release):
        return []
    if not (
        consolidated._table_exists(adapter.store, "fomo_shadow_observations")
        and consolidated._table_exists(adapter.store, "fomo_shadow_outcomes")
    ):
        return []
    try:
        with adapter.store._lock:
            rows = adapter.store.db.execute(
                "SELECT s.source_signature,s.token_mint,s.lifecycle,s.venue,s.observed_at AS settled_at,"
                "s.state_json,o.net_return,t.trigger_wallet FROM fomo_shadow_observations s "
                "JOIN v51_economic_freeze_releases e ON e.release_commit=s.release_commit "
                "JOIN v51_release_compatibility m ON m.release_commit=s.release_commit AND m.measurement_epoch=? "
                "JOIN profit_first_final_trials t ON t.release_commit=s.release_commit AND t.source_signature=s.source_signature "
                "AND t.lane='unified_profit_maximizer' "
                "JOIN fomo_shadow_outcomes o ON o.release_commit=s.release_commit AND o.source_signature=s.source_signature "
                "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND s.venue=? AND s.lifecycle=? AND s.regime=? ORDER BY s.id",
                (MEASUREMENT_EPOCH, ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID, venue, lifecycle, regime),
            ).fetchall()
    except Exception:
        return []
    selected: list[dict[str, Any]] = []
    for row in consolidated._dedup(list(rows), "source_signature"):
        if str(row["trigger_wallet"] or "") != wallet:
            continue
        state = v5._safe_json(row["state_json"])
        if str(state.get("state") or "") not in {"pre_fomo", "active_fomo"}:
            continue
        if v51.fomo_hazard_signature(state) != hazard_signature:
            continue
        value = v5._finite(row["net_return"])
        if value is None:
            continue
        item = dict(row)
        item["net_return"] = float(value)
        selected.append(item)
    clusters = cluster_rows(selected, family="FOMO", promotion_only=True)
    return [float(row["net_return"]) for row in clusters]


def _rh_epoch_profile_compatible(self: Any, **context: Any) -> dict[str, Any]:
    from . import v51_consolidated_strategy as consolidated

    release = getattr(self, "release_commit", None)
    consolidated._ensure_epoch(self.store, release)
    ensure_release_compatibility(self.store, release)
    entity = str(context.get("entity") or "")
    role = str(context.get("role") or "unknown")
    lane = str(context.get("lane") or "unknown")
    venue = str(context.get("venue") or "UNKNOWN")
    lifecycle = str(context.get("lifecycle") or "unknown")
    regime = str(context.get("regime") or "unknown")
    risk_signature = str(context.get("risk_signature") or "clean")
    flow_state = str(context.get("flow_state") or "neutral")
    key = self._v5_context_key(
        entity=entity,
        role=role,
        lane=lane,
        venue=venue,
        lifecycle=lifecycle,
        regime=regime,
        risk_signature=risk_signature,
        flow_state=flow_state,
    )
    if not surface_attested(self.store, "ROBINHOOD_CHAIN", release_commit=release):
        hp = hierarchical_profile((), (), (), risk_severity=0.0 if risk_signature == "clean" else 0.45,
                                  risk_signature=risk_signature, max_fraction=0.10)
        return {
            "sample_count": 0,
            "state": "bootstrap_forward_evidence",
            "best_fraction": hp["best_fraction"],
            "best_expected_log_growth": hp["best_expected_log_growth"],
            "mean_return": hp["mean_return"],
            "median_return": hp["median_return"],
            "hit_rate": hp["hit_rate"],
            "trimmed_mean_ex_best": hp["leave_best_trade_out_mean"],
            "expected_shortfall_20": hp["expected_shortfall_20"],
            "winner_concentration": hp["winner_concentration"],
            "max_drawdown": hp["max_drawdown_at_best_fraction"],
            "evidence_source": "v51_release_not_live_attested_for_robinhood_promotion",
            "measurement_epoch": MEASUREMENT_EPOCH,
            "hierarchical_profile": hp,
            "hit_rate_is_promotion_veto": False,
        }
    exact_rows: list[Any] = []
    parent_rows: list[Any] = []
    if consolidated._table_exists(self.store, "robinhood_paper_outcomes") and consolidated._table_exists(
        self.store, "robinhood_v5_trial_context"
    ):
        with self.store._lock:
            exact_rows = list(
                self.store.db.execute(
                    "SELECT o.trial_id,o.net_return,o.settled_at,t.token AS token_mint,t.lifecycle,t.venue "
                    "FROM robinhood_paper_outcomes o "
                    "JOIN v51_economic_freeze_releases e ON e.release_commit=o.release_commit "
                    "JOIN v51_release_compatibility m ON m.release_commit=o.release_commit AND m.measurement_epoch=? "
                    "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
                    "JOIN robinhood_paper_trials t ON t.id=o.trial_id "
                    "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND c.context_key=? ORDER BY o.id",
                    (MEASUREMENT_EPOCH, ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID, key),
                ).fetchall()
            )
            parent_rows = list(
                self.store.db.execute(
                    "SELECT o.trial_id,o.net_return,o.settled_at,t.token AS token_mint,t.lifecycle,t.venue "
                    "FROM robinhood_paper_outcomes o "
                    "JOIN v51_economic_freeze_releases e ON e.release_commit=o.release_commit "
                    "JOIN v51_release_compatibility m ON m.release_commit=o.release_commit AND m.measurement_epoch=? "
                    "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
                    "JOIN robinhood_paper_trials t ON t.id=o.trial_id "
                    "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND t.trigger_entity=? AND c.trigger_role=? "
                    "AND c.lane=? AND t.venue=? AND t.lifecycle=? AND c.risk_signature=? AND c.context_key<>? ORDER BY o.id",
                    (
                        MEASUREMENT_EPOCH,
                        ECONOMIC_FREEZE_EPOCH,
                        AUTHORITY_ID,
                        entity,
                        role,
                        lane,
                        venue,
                        lifecycle,
                        risk_signature,
                        key,
                    ),
                ).fetchall()
            )
    exact_raw = [dict(row) for row in consolidated._dedup(exact_rows, "trial_id")]
    exact = cluster_rows(exact_raw, family=f"ROBINHOOD:{lane}", promotion_only=True)
    exact_clusters = {str(row["event_cluster_id"]) for row in exact}
    parent_raw = [dict(row) for row in consolidated._dedup(parent_rows, "trial_id")]
    parent = cluster_rows(
        parent_raw,
        family=f"ROBINHOOD:{lane}",
        excluded_cluster_ids=exact_clusters,
        promotion_only=True,
    )
    severity = 0.0 if risk_signature == "clean" else 0.45
    hp = hierarchical_profile(
        [float(row["net_return"]) for row in exact],
        [float(row["net_return"]) for row in parent],
        (),
        risk_severity=severity,
        risk_signature=risk_signature,
        max_fraction=0.10,
    )
    if bool(hp.get("promoted")):
        legacy_state = "promoted_positive_log_growth"
    elif bool(hp.get("killed")):
        legacy_state = "demoted_nonpositive_log_growth"
    else:
        legacy_state = "bootstrap_forward_evidence"
    return {
        "sample_count": hp["exact_sample_count"],
        "state": legacy_state,
        "best_fraction": hp["best_fraction"],
        "best_expected_log_growth": hp["best_expected_log_growth"],
        "mean_return": hp["mean_return"],
        "median_return": hp["median_return"],
        "hit_rate": hp["hit_rate"],
        "trimmed_mean_ex_best": hp["leave_best_trade_out_mean"],
        "expected_shortfall_20": hp["expected_shortfall_20"],
        "winner_concentration": hp["winner_concentration"],
        "max_drawdown": hp["max_drawdown_at_best_fraction"],
        "evidence_source": "v51_attested_measurement_compatible_event_clusters_validation_plus_holdout",
        "measurement_epoch": MEASUREMENT_EPOCH,
        "hierarchical_profile": hp,
        "hit_rate_is_promotion_veto": False,
    }


setattr(_solana_evidence_compatible, "_roi_v51_measurement_compatibility", True)
setattr(_fomo_epoch_returns_compatible, "_roi_v51_measurement_compatibility", True)
setattr(_rh_epoch_profile_compatible, "_roi_v51_measurement_compatibility", True)


def install_measurement_compatible_promotion_filters() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    from . import v51_consolidated_strategy as consolidated

    consolidated._solana_evidence = _solana_evidence_compatible  # type: ignore[assignment]
    consolidated._fomo_epoch_returns = _fomo_epoch_returns_compatible  # type: ignore[assignment]
    consolidated._rh_epoch_profile = _rh_epoch_profile_compatible  # type: ignore[assignment]
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": FILTER_VERSION,
        "installed": _INSTALLED,
        "measurement_epoch": MEASUREMENT_EPOCH,
        "solana_promotion_requires_measurement_compatible_release": True,
        "fomo_promotion_requires_measurement_compatible_release": True,
        "robinhood_promotion_requires_measurement_compatible_release": True,
        "promotion_requires_live_surface_attestation": True,
        "promotion_independence_unit": "token_lifecycle_event_cluster",
        "promotion_evidence_partition": "validation_plus_locked_holdout_only; discovery_excluded",
        "economic_certification_scope": "economic_epoch_audit_including_nonpromotable_rows",
        "economic_certification_grants_promotion_authority": False,
        "defective_release_promotion_authority": False,
        "historical_rows_deleted": False,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = ["FILTER_VERSION", "install_measurement_compatible_promotion_filters", "status"]
