from __future__ import annotations

import hashlib
import json
from typing import Any, Callable


FOLLOWUP_VERSION = "v51-exact-exit-terminal-fomo-followup-v2-single-engine"
ACTIVE_EXECUTION_MODEL_EPOCH = "v51-execution-model-exact-exit-v3-terminal-fomo"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_INSTALLED = False
_ORIGINAL_PROMOTION_RECORDS: Callable[..., Any] | None = None


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def execution_model_fingerprint() -> str:
    payload = {
        "execution_model_epoch": ACTIVE_EXECUTION_MODEL_EPOCH,
        "settlement_engine": "single_active_nonblocking_durable_exact_exit_liquidation",
        "previous_v2_runtime_engine_active": False,
        "retry_elapsed_seconds": [0, 10, 30, 60, 120, 300],
        "terminal_unsellable_measurement": "net_return_minus_1_after_300s",
        "failed_exit_disappears_from_forward_evidence": False,
        "fomo_exit_amount": "fomo_own_scaled_raw_held_amount",
        "fomo_promotion_return_source": "fomo_paper_outcomes_exact_size",
        "unsigned_simulation_only": True,
        "signing": False,
        "live_submission": False,
    }
    return hashlib.sha256(_json(payload).encode()).hexdigest()


def _table_exists(store: Any, table: str) -> bool:
    try:
        with store._lock:
            return store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table,),
            ).fetchone() is not None
    except Exception:
        return False


def _preserve_wrapped_markers(settlement: Any) -> None:
    """Preserve composition metadata while keeping one behavioral exit engine.

    The exact-exit implementation replaces observe/sell/status/manifest deliberately.
    Earlier research wrappers attach idempotence/architecture markers to those callables.
    Copying those marker dictionaries onto the replacement preserves composition truth
    without adding another behavioral wrapper or changing any settlement decision.
    """

    pairs = (
        ("_observe_with_exact_exit", "_ORIGINAL_OBSERVE"),
        ("_sell_exact", "_ORIGINAL_SELL"),
        ("_status_with_exact_exit", "_ORIGINAL_STATUS"),
        ("_manifest_with_exact_exit", "_ORIGINAL_MANIFEST"),
    )
    for wrapper_name, original_name in pairs:
        wrapper = getattr(settlement, wrapper_name, None)
        original = getattr(settlement, original_name, None)
        if not callable(wrapper) or not callable(original):
            continue
        try:
            wrapper.__dict__.update(getattr(original, "__dict__", {}))
        except Exception:
            pass
        setattr(wrapper, "_roi_exact_exit_execution_v2", True)
        setattr(wrapper, "_roi_exact_exit_execution_v3_terminal_fomo", True)


def _active_promotion_records(store: Any) -> list[dict[str, Any]]:
    if _ORIGINAL_PROMOTION_RECORDS is None:
        raise RuntimeError("active exact-exit analytics promotion source unavailable")
    rows = list(_ORIGINAL_PROMOTION_RECORDS(store))
    epochs: dict[str, str] = {}
    if _table_exists(store, "v51_release_compatibility"):
        with store._lock:
            compatibility = store.db.execute(
                "SELECT release_commit,execution_model_epoch FROM v51_release_compatibility"
            ).fetchall()
        epochs = {
            str(row["release_commit"] or ""): str(row["execution_model_epoch"] or "")
            for row in compatibility
        }
    selected: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        if str(row.get("surface") or "") in {"SOLANA", "SOLANA_ALPHA", "FOMO"}:
            epoch = epochs.get(str(row.get("release_commit") or ""), "")
            if epoch != ACTIVE_EXECUTION_MODEL_EPOCH:
                continue
            row["execution_model_epoch"] = epoch
        selected.append(row)
    return selected


def _solana_evidence_active_epoch(
    adapter: Any,
    *,
    lane: str,
    pre: dict[str, Any],
    context_key: str,
) -> tuple[list[float], list[float]]:
    from . import risk_conditioned_alpha_v51 as v51
    from . import v51_consolidated_strategy as consolidated
    from . import v51_measurement_integrity as measurement
    from .v51_promotion_proof import cluster_rows, surface_attested

    release = getattr(adapter, "release_commit", None)
    consolidated._ensure_epoch(adapter.store, release)
    measurement.ensure_release_compatibility(adapter.store, release)
    if not surface_attested(adapter.store, "SOLANA", release_commit=release):
        return [], []
    parsed = v51._parse_context_key(context_key)
    entity = str(parsed.get("entity") or pre.get("trigger_entity") or "")
    risk_signature = str((pre.get("risk") or {}).get("risk_signature") or "clean")
    if not consolidated._table_exists(adapter.store, "risk_conditioned_alpha_v5_outcomes"):
        return [], []
    with adapter.store._lock:
        exact_rows = adapter.store.db.execute(
            "SELECT o.source_signature,o.net_return,o.token_mint,o.lifecycle,o.venue,o.settled_at "
            "FROM risk_conditioned_alpha_v5_outcomes o "
            "JOIN v51_economic_freeze_releases e ON e.release_commit=o.release_commit "
            "JOIN v51_release_compatibility m ON m.release_commit=o.release_commit "
            "AND m.measurement_epoch=? AND m.execution_model_epoch=? "
            "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND o.context_key=? ORDER BY o.id",
            (
                measurement.MEASUREMENT_EPOCH,
                ACTIVE_EXECUTION_MODEL_EPOCH,
                consolidated.ECONOMIC_FREEZE_EPOCH,
                consolidated.AUTHORITY_ID,
                context_key,
            ),
        ).fetchall()
        parent_rows = adapter.store.db.execute(
            "SELECT o.source_signature,o.net_return,o.token_mint,o.lifecycle,o.venue,o.settled_at "
            "FROM risk_conditioned_alpha_v5_outcomes o "
            "JOIN v51_economic_freeze_releases e ON e.release_commit=o.release_commit "
            "JOIN v51_release_compatibility m ON m.release_commit=o.release_commit "
            "AND m.measurement_epoch=? AND m.execution_model_epoch=? "
            "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND o.lane=? AND o.venue=? AND o.lifecycle=? "
            "AND o.risk_signature=? AND o.context_key LIKE ? AND o.context_key<>? ORDER BY o.id",
            (
                measurement.MEASUREMENT_EPOCH,
                ACTIVE_EXECUTION_MODEL_EPOCH,
                consolidated.ECONOMIC_FREEZE_EPOCH,
                consolidated.AUTHORITY_ID,
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


def _fomo_epoch_returns_active(
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
    from . import v51_measurement_integrity as measurement
    from .v51_promotion_proof import cluster_rows, surface_attested

    release = getattr(adapter, "release_commit", None)
    consolidated._ensure_epoch(adapter.store, release)
    measurement.ensure_release_compatibility(adapter.store, release)
    if not surface_attested(adapter.store, "FOMO", release_commit=release):
        return []
    required = (
        "fomo_shadow_observations",
        "fomo_paper_outcomes",
        "fomo_paper_outcome_execution_models",
    )
    if not all(consolidated._table_exists(adapter.store, table) for table in required):
        return []
    try:
        with adapter.store._lock:
            rows = adapter.store.db.execute(
                "SELECT s.source_signature,s.token_mint,s.lifecycle,s.venue,p.settled_at,s.state_json,"
                "p.net_return,p.trigger_wallet FROM fomo_shadow_observations s "
                "JOIN v51_economic_freeze_releases e ON e.release_commit=s.release_commit "
                "JOIN v51_release_compatibility m ON m.release_commit=s.release_commit "
                "AND m.measurement_epoch=? AND m.execution_model_epoch=? "
                "JOIN fomo_paper_outcomes p ON p.release_commit=s.release_commit "
                "AND p.source_signature=s.source_signature "
                "JOIN fomo_paper_outcome_execution_models x ON x.release_commit=p.release_commit "
                "AND x.source_signature=p.source_signature AND x.execution_model_epoch=? "
                "WHERE e.economic_freeze_epoch=? AND e.authority_id=? "
                "AND s.venue=? AND s.lifecycle=? AND s.regime=? ORDER BY s.id",
                (
                    measurement.MEASUREMENT_EPOCH,
                    ACTIVE_EXECUTION_MODEL_EPOCH,
                    ACTIVE_EXECUTION_MODEL_EPOCH,
                    consolidated.ECONOMIC_FREEZE_EPOCH,
                    consolidated.AUTHORITY_ID,
                    venue,
                    lifecycle,
                    regime,
                ),
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
        if value is not None:
            item = dict(row)
            item["net_return"] = float(value)
            selected.append(item)
    clusters = cluster_rows(selected, family="FOMO", promotion_only=True)
    return [float(row["net_return"]) for row in clusters]


def _forward_fomo_rows_active(adapter: Any) -> list[dict[str, Any]]:
    required = (
        "fomo_shadow_observations",
        "fomo_paper_outcomes",
        "fomo_paper_outcome_execution_models",
    )
    if not all(_table_exists(adapter.store, table) for table in required):
        return []
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT s.source_signature,s.venue,s.lifecycle,s.regime,s.state_json,"
            "p.trigger_wallet,p.net_return FROM fomo_shadow_observations s "
            "JOIN fomo_paper_outcomes p ON p.release_commit=s.release_commit "
            "AND p.source_signature=s.source_signature "
            "JOIN fomo_paper_outcome_execution_models x ON x.release_commit=p.release_commit "
            "AND x.source_signature=p.source_signature AND x.execution_model_epoch=? "
            "WHERE s.release_commit=? ORDER BY s.id",
            (ACTIVE_EXECUTION_MODEL_EPOCH, adapter.release_commit),
        ).fetchall()
    return [dict(row) for row in rows]


def status(store: Any | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "version": FOLLOWUP_VERSION,
        "installed": _INSTALLED,
        "execution_model_epoch": ACTIVE_EXECUTION_MODEL_EPOCH,
        "execution_model_fingerprint": execution_model_fingerprint(),
        "single_active_exit_engine": True,
        "internal_settlement_monkeypatches": False,
        "retry_elapsed_seconds": [0, 10, 30, 60, 120, 300],
        "terminal_liquidation_assumption": "total_loss_after_300s_without_executable_exact_exit",
        "terminal_unsellable_net_return": -1.0,
        "failed_exit_can_disappear_from_forward_evidence": False,
        "fomo_uses_own_exact_held_size": True,
        "fomo_promotion_uses_size_specific_paper_outcomes": True,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    if store is not None and _table_exists(store, "profit_first_final_exit_liquidations"):
        with store._lock:
            pending = store.db.execute(
                "SELECT COUNT(*) FROM profit_first_final_exit_liquidations "
                "WHERE execution_model_epoch=? AND status='paper_exit_execution_failed'",
                (ACTIVE_EXECUTION_MODEL_EPOCH,),
            ).fetchone()[0]
            terminal = store.db.execute(
                "SELECT COUNT(*) FROM profit_first_final_exit_liquidations "
                "WHERE execution_model_epoch=? AND status='paper_exit_terminal_unexitable'",
                (ACTIVE_EXECUTION_MODEL_EPOCH,),
            ).fetchone()[0]
            fomo = store.db.execute(
                "SELECT COUNT(*) FROM profit_first_final_exit_liquidations "
                "WHERE execution_model_epoch=? AND position_scope='fomo'",
                (ACTIVE_EXECUTION_MODEL_EPOCH,),
            ).fetchone()[0]
        result.update(
            {
                "pending_liquidation_count": int(pending or 0),
                "terminal_unexitable_count": int(terminal or 0),
                "fomo_liquidation_count": int(fomo or 0),
            }
        )
    return result


def install_terminal_fomo_followup() -> None:
    global _INSTALLED, _ORIGINAL_PROMOTION_RECORDS
    if _INSTALLED:
        return

    from . import fomo_paper_strategy as fomo_paper
    from . import v51_cross_surface_proof as cross_surface
    from . import v51_evidence_analytics as analytics
    from . import v51_exact_exit_execution as settlement
    from . import v51_measurement_compatibility_filters as filters
    from . import v51_measurement_integrity as measurement

    # The v3 durable liquidation implementation is the sole active Phase 16 exit
    # engine. PR #215's v2 module remains in the repository for audit/regression
    # lineage but is not installed in production.
    settlement.EXACT_EXIT_EXECUTION_MODEL_EPOCH = ACTIVE_EXECUTION_MODEL_EPOCH
    settlement.install_exact_exit_execution_model()
    _preserve_wrapped_markers(settlement)

    measurement.EXECUTION_MODEL_EPOCH = ACTIVE_EXECUTION_MODEL_EPOCH
    measurement.execution_model_fingerprint = execution_model_fingerprint  # type: ignore[assignment]

    if _ORIGINAL_PROMOTION_RECORDS is None:
        _ORIGINAL_PROMOTION_RECORDS = analytics.promotion_records
    analytics.promotion_records = _active_promotion_records  # type: ignore[assignment]
    cross_surface.promotion_records = _active_promotion_records  # type: ignore[assignment]
    filters._solana_evidence_compatible = _solana_evidence_active_epoch  # type: ignore[assignment]
    filters._fomo_epoch_returns_compatible = _fomo_epoch_returns_active  # type: ignore[assignment]
    fomo_paper._forward_fomo_rows = _forward_fomo_rows_active  # type: ignore[assignment]

    _INSTALLED = True


__all__ = [
    "ACTIVE_EXECUTION_MODEL_EPOCH",
    "FOLLOWUP_VERSION",
    "LIVE_MONEY_AUTHORITY",
    "PAPER_ONLY",
    "SIGNING_AVAILABLE",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "execution_model_fingerprint",
    "install_terminal_fomo_followup",
    "status",
]
