from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable

from . import risk_conditioned_alpha_v5 as risk_v5
from .profit_first_entity_final import MarketRegime
from .robinhood_chain_profit_maximizer import RobinhoodProfitMaximizerMixin


STATUS_CONTRACT_VERSION = "all-strategy-e2e-status-v1"
REGIME_PROBE_VERSION = "regime-paper-round-trip-probe-v1"
REGIMES = tuple(regime.value for regime in MarketRegime)
REGIME_PROBE_LANE = "regime_e2e_round_trip_probe"
REGIME_PROBE_PROMOTION_AUTHORITY = False
REGIME_PROBE_PORTFOLIO_AUTHORITY = False
REGIME_PROBE_LIVE_MONEY_AUTHORITY = False

_ORIGINAL_V5_BUY: Callable[..., Any] | None = None


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _table_exists(store: Any, table: str) -> bool:
    try:
        with store._lock:
            row = store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table,),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _probe_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS regime_paper_e2e_probes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, "
            "probe_version TEXT NOT NULL, source_signature TEXT NOT NULL, regime TEXT NOT NULL, "
            "venue TEXT NOT NULL, lifecycle TEXT NOT NULL, source_lane TEXT NOT NULL, "
            "execution_notional_fraction REAL NOT NULL, entry_cost_sol REAL NOT NULL, "
            "immediate_exit_net_sol REAL NOT NULL, net_return REAL NOT NULL, "
            "observed_at TEXT NOT NULL, completed_at TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "promotion_authority INTEGER NOT NULL, portfolio_allocation_authority INTEGER NOT NULL, "
            "UNIQUE(release_commit, regime))"
        )


def regime_execution_contract() -> dict[str, dict[str, Any]]:
    """Return the immutable software contract proving no market regime is disabled.

    This is a capability contract, not a profitability claim. A regime can still
    reject a specific candidate for mechanical execution failure, inaccessible
    latency, extreme chase, or exhausted paper capacity. The contract only proves
    that the regime label itself never blocks paper execution.
    """
    payload: dict[str, dict[str, Any]] = {}
    for regime in REGIMES:
        solana_multiplier = float(risk_v5._regime_multiplier(regime))
        robinhood_multiplier = float(RobinhoodProfitMaximizerMixin._v5_regime_multiplier(regime))
        clean_fomo_probe = 0.01 * solana_multiplier
        hazard_fomo_probe = 0.005 * solana_multiplier
        solana_bootstrap = max(0.0025, 0.005 * solana_multiplier)
        robinhood_bootstrap = max(0.0025, 0.005 * robinhood_multiplier)
        payload[regime] = {
            "solana": {
                "paper_execution_enabled": solana_multiplier > 0.0 and solana_bootstrap > 0.0,
                "regime_multiplier": solana_multiplier,
                "minimum_bootstrap_fraction": solana_bootstrap,
                "regime_label_is_entry_veto": False,
            },
            "fomo": {
                "paper_execution_enabled": clean_fomo_probe > 0.0 and hazard_fomo_probe > 0.0,
                "regime_multiplier": solana_multiplier,
                "clean_bootstrap_fraction_before_other_caps": clean_fomo_probe,
                "hazard_bootstrap_fraction_before_other_caps": hazard_fomo_probe,
                "regime_label_is_entry_veto": False,
            },
            "robinhood": {
                "paper_execution_enabled": robinhood_multiplier > 0.0 and robinhood_bootstrap > 0.0,
                "regime_multiplier": robinhood_multiplier,
                "minimum_bootstrap_fraction": robinhood_bootstrap,
                "regime_label_is_entry_veto": False,
            },
        }
    return payload


def _record_regime_probe(adapter: Any, source_signature: str) -> bool:
    """Persist one real executable immediate round-trip paper proof per regime.

    The probe reuses the exact v5 entry and immediate-exit snapshot already paid for
    by the candidate path. It sends no additional RPC or quote request, never mutates
    the paper portfolio, and is excluded from alpha/promotion evidence. Its only job
    is to prove that an eligible observation in each market regime can traverse an
    executable paper buy and paper exit end to end.
    """
    _probe_schema(adapter.store)
    if not _table_exists(adapter.store, "risk_conditioned_alpha_v5_trials"):
        return False
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT source_signature,regime,venue,lifecycle,lane,position_fraction,entry_cost_sol,"
            "immediate_exit_net_sol,risk_json,chase_band,latency_band,observed_at,decision,selected "
            "FROM risk_conditioned_alpha_v5_trials "
            "WHERE release_commit=? AND source_signature=? AND entry_executable=1 AND exit_executable=1 "
            "ORDER BY selected DESC,id",
            (adapter.release_commit, source_signature),
        ).fetchall()
    for raw in rows:
        row = dict(raw)
        regime = str(row.get("regime") or "")
        if regime not in REGIMES:
            continue
        if str(row.get("chase_band") or "") == "challenger_gt_40pct":
            continue
        if str(row.get("latency_band") or "") == "gt_20s":
            continue
        if str(row.get("decision") or "").startswith("reject_"):
            continue
        try:
            risk = json.loads(str(row.get("risk_json") or "{}"))
        except Exception:
            risk = {}
        if not bool(risk.get("structurally_tradeable", False)):
            continue
        try:
            entry_cost = float(row.get("entry_cost_sol") or 0.0)
            exit_net = float(row.get("immediate_exit_net_sol") or 0.0)
            notional_fraction = float(row.get("position_fraction") or 0.0)
        except (TypeError, ValueError):
            continue
        if entry_cost <= 0.0 or exit_net <= 0.0 or notional_fraction <= 0.0:
            continue
        net_return = exit_net / entry_cost - 1.0
        now = _utcnow()
        with adapter.store._lock, adapter.store.db:
            cursor = adapter.store.db.execute(
                "INSERT OR IGNORE INTO regime_paper_e2e_probes("
                "release_commit,probe_version,source_signature,regime,venue,lifecycle,source_lane,"
                "execution_notional_fraction,entry_cost_sol,immediate_exit_net_sol,net_return,"
                "observed_at,completed_at,paper_only,live_money_authority,promotion_authority,portfolio_allocation_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1,0,0,0)",
                (
                    adapter.release_commit,
                    REGIME_PROBE_VERSION,
                    str(row.get("source_signature") or source_signature),
                    regime,
                    str(row.get("venue") or "UNKNOWN"),
                    str(row.get("lifecycle") or "unknown"),
                    str(row.get("lane") or "unknown"),
                    notional_fraction,
                    entry_cost,
                    exit_net,
                    net_return,
                    str(row.get("observed_at") or now),
                    now,
                ),
            )
        if cursor.rowcount == 1:
            try:
                adapter.store.append(
                    "regime_paper_e2e_probe_completed",
                    now,
                    {
                        "source_signature": source_signature,
                        "regime": regime,
                        "venue": str(row.get("venue") or "UNKNOWN"),
                        "lifecycle": str(row.get("lifecycle") or "unknown"),
                        "net_return": net_return,
                        "paper_only": True,
                        "live_money_authority": False,
                        "promotion_authority": False,
                        "portfolio_allocation_authority": False,
                    },
                )
            except Exception:
                pass
            return True
        return False
    return False


async def _buy_with_regime_probe(self: Any, row: dict[str, Any]) -> None:
    if _ORIGINAL_V5_BUY is None:
        raise RuntimeError("regime paper E2E probe missing wrapped v5 buy")
    await _ORIGINAL_V5_BUY(self, row)
    try:
        _record_regime_probe(self, str(row.get("signature") or ""))
        setattr(self, "_roi_regime_e2e_probe_last_error", None)
    except Exception as exc:
        setattr(self, "_roi_regime_e2e_probe_last_error", f"{type(exc).__name__}: {exc}")


def install_regime_paper_e2e_probe() -> None:
    global _ORIGINAL_V5_BUY
    from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter

    current = FinalProfitFirstResearchAdapter._buy
    if bool(getattr(current, "_roi_regime_paper_e2e_probe", False)):
        return
    _ORIGINAL_V5_BUY = current
    wrapped = wraps(current)(_buy_with_regime_probe)
    try:
        wrapped.__dict__.update(getattr(current, "__dict__", {}))
    except Exception:
        pass
    setattr(wrapped, "_roi_regime_paper_e2e_probe", True)
    FinalProfitFirstResearchAdapter._buy = wrapped  # type: ignore[method-assign]


def _empty_regime_counts() -> dict[str, dict[str, int]]:
    return {
        regime: {
            "considered": 0,
            "executable": 0,
            "paper_entries": 0,
            "settled_outcomes": 0,
        }
        for regime in REGIMES
    }


def _solana_regime_counts(store: Any, release_commit: str) -> dict[str, dict[str, int]]:
    result = _empty_regime_counts()
    if _table_exists(store, "risk_conditioned_alpha_v5_trials"):
        with store._lock:
            rows = store.db.execute(
                "SELECT regime,COUNT(*) considered,"
                "SUM(CASE WHEN entry_executable=1 AND exit_executable=1 THEN 1 ELSE 0 END) executable,"
                "SUM(CASE WHEN selected=1 AND decision LIKE 'paper_enter%' THEN 1 ELSE 0 END) paper_entries "
                "FROM risk_conditioned_alpha_v5_trials WHERE release_commit=? GROUP BY regime",
                (release_commit,),
            ).fetchall()
        for row in rows:
            regime = str(row["regime"])
            if regime in result:
                result[regime].update(
                    considered=int(row["considered"] or 0),
                    executable=int(row["executable"] or 0),
                    paper_entries=int(row["paper_entries"] or 0),
                )
    if _table_exists(store, "risk_conditioned_alpha_v5_outcomes"):
        with store._lock:
            rows = store.db.execute(
                "SELECT regime,COUNT(*) outcomes FROM risk_conditioned_alpha_v5_outcomes "
                "WHERE release_commit=? GROUP BY regime",
                (release_commit,),
            ).fetchall()
        for row in rows:
            regime = str(row["regime"])
            if regime in result:
                result[regime]["settled_outcomes"] = int(row["outcomes"] or 0)
    return result


def _fomo_regime_counts(store: Any, release_commit: str) -> dict[str, dict[str, int]]:
    result = _empty_regime_counts()
    if _table_exists(store, "fomo_paper_trials"):
        with store._lock:
            rows = store.db.execute(
                "SELECT regime,COUNT(*) considered,"
                "SUM(CASE WHEN entry_executable=1 AND exit_executable=1 THEN 1 ELSE 0 END) executable,"
                "SUM(CASE WHEN decision LIKE 'paper_enter_%' THEN 1 ELSE 0 END) paper_entries "
                "FROM fomo_paper_trials WHERE release_commit=? GROUP BY regime",
                (release_commit,),
            ).fetchall()
        for row in rows:
            regime = str(row["regime"])
            if regime in result:
                result[regime].update(
                    considered=int(row["considered"] or 0),
                    executable=int(row["executable"] or 0),
                    paper_entries=int(row["paper_entries"] or 0),
                )
    if _table_exists(store, "fomo_paper_outcomes"):
        with store._lock:
            rows = store.db.execute(
                "SELECT regime,COUNT(*) outcomes FROM fomo_paper_outcomes WHERE release_commit=? GROUP BY regime",
                (release_commit,),
            ).fetchall()
        for row in rows:
            regime = str(row["regime"])
            if regime in result:
                result[regime]["settled_outcomes"] = int(row["outcomes"] or 0)
    return result


def _robinhood_regime_counts(store: Any, release_commit: str) -> dict[str, dict[str, int]]:
    result = _empty_regime_counts()
    if not _table_exists(store, "robinhood_v5_trial_context"):
        return result
    with store._lock:
        rows = store.db.execute(
            "SELECT c.regime,COUNT(DISTINCT c.trial_id) considered,"
            "COUNT(DISTINCT t.id) paper_entries,COUNT(DISTINCT o.id) outcomes "
            "FROM robinhood_v5_trial_context c "
            "LEFT JOIN robinhood_paper_trials t ON t.id=c.trial_id AND t.release_commit=c.release_commit "
            "LEFT JOIN robinhood_paper_outcomes o ON o.trial_id=c.trial_id AND o.release_commit=c.release_commit "
            "WHERE c.release_commit=? GROUP BY c.regime",
            (release_commit,),
        ).fetchall()
    for row in rows:
        regime = str(row["regime"])
        if regime in result:
            paper_entries = int(row["paper_entries"] or 0)
            result[regime].update(
                considered=int(row["considered"] or 0),
                executable=paper_entries,
                paper_entries=paper_entries,
                settled_outcomes=int(row["outcomes"] or 0),
            )
    return result


def _probe_status(store: Any, release_commit: str) -> dict[str, dict[str, Any]]:
    _probe_schema(store)
    result = {
        regime: {
            "completed": False,
            "source_signature": None,
            "venue": None,
            "lifecycle": None,
            "execution_notional_fraction": None,
            "net_return_pct": None,
            "completed_at": None,
        }
        for regime in REGIMES
    }
    with store._lock:
        rows = store.db.execute(
            "SELECT source_signature,regime,venue,lifecycle,execution_notional_fraction,net_return,completed_at "
            "FROM regime_paper_e2e_probes WHERE release_commit=? ORDER BY id",
            (release_commit,),
        ).fetchall()
    for row in rows:
        regime = str(row["regime"])
        if regime not in result:
            continue
        result[regime] = {
            "completed": True,
            "source_signature": str(row["source_signature"]),
            "venue": str(row["venue"]),
            "lifecycle": str(row["lifecycle"]),
            "execution_notional_fraction": float(row["execution_notional_fraction"]),
            "net_return_pct": float(row["net_return"]) * 100.0,
            "completed_at": str(row["completed_at"]),
        }
    return result


def _regime_progress(
    counts: dict[str, int],
    *,
    contract_capable: bool,
    transport_ready: bool,
    probe: dict[str, Any] | None = None,
) -> tuple[str, list[str]]:
    blockers: list[str] = []
    if not contract_capable:
        blockers.append("regime_paper_contract_disabled")
    if not transport_ready:
        blockers.append("execution_transport_not_ready")
    if probe is not None and bool(probe.get("completed")):
        return "round_trip_paper_e2e_proven", blockers
    if int(counts.get("settled_outcomes", 0)) > 0:
        return "settled_paper_outcome_observed", blockers
    if int(counts.get("paper_entries", 0)) > 0:
        blockers.append("paper_entries_waiting_for_settlement")
        return "paper_entry_observed_waiting_settlement", blockers
    if int(counts.get("executable", 0)) > 0:
        blockers.append("executable_candidate_not_yet_paper_entered")
        return "executable_candidate_observed", blockers
    blockers.append("awaiting_executable_candidate_in_regime")
    return "awaiting_executable_candidate", blockers


def _release_commit(strategy_status: dict[str, Any]) -> str:
    value = str(strategy_status.get("release_commit") or "").strip()
    if value:
        return value
    manifest = strategy_status.get("manifest")
    if isinstance(manifest, dict):
        value = str(manifest.get("source_release_commit") or "").strip()
        if value:
            return value
    for key in ("RENDER_GIT_COMMIT", "GITHUB_SHA", "SOLANA_ROI_RELEASE_COMMIT"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return "unbound-local-release"


def build_unified_strategy_status(
    base_status: dict[str, Any],
    runtime: Any,
    robinhood_status: dict[str, Any],
) -> dict[str, Any]:
    enriched = dict(base_status)
    wallet_status = base_status.get("wallet_discovery")
    if not isinstance(wallet_status, dict):
        wallet_status = runtime.wallet_discovery.status()
    strategy = wallet_status.get("profit_first_entity_strategy") if isinstance(wallet_status, dict) else {}
    if not isinstance(strategy, dict):
        strategy = {}
    v5 = strategy.get("risk_conditioned_alpha_v5")
    if not isinstance(v5, dict):
        v5 = {}
    fomo = strategy.get("fomo_paper_strategy")
    if not isinstance(fomo, dict):
        fomo = {}
    fomo_shadow = strategy.get("fomo_continuation_shadow")
    if not isinstance(fomo_shadow, dict):
        fomo_shadow = {}

    release_commit = _release_commit(strategy)
    contracts = regime_execution_contract()
    solana_counts = _solana_regime_counts(runtime.store, release_commit)
    fomo_counts = _fomo_regime_counts(runtime.store, release_commit)
    robinhood_counts = _robinhood_regime_counts(runtime.store, release_commit)
    probes = _probe_status(runtime.store, release_commit)

    direct = base_status.get("direct_solana") if isinstance(base_status.get("direct_solana"), dict) else {}
    quote_client_ready = getattr(runtime.quote_handoff, "client", None) is not None
    simulator_ready = getattr(runtime.quote_handoff, "simulator", None) is not None
    solana_transport_ready = bool(
        direct.get("enabled")
        and direct.get("continuity_ok")
        and quote_client_ready
        and simulator_ready
    )
    fomo_transport_ready = bool(solana_transport_ready and fomo.get("paper_strategy_authority"))
    robinhood_transport_ready = bool(
        robinhood_status.get("runtime_ready")
        and robinhood_status.get("paper_trading_authority", True)
        and not robinhood_status.get("failed_closed", False)
    )

    solana_regimes: dict[str, Any] = {}
    fomo_regimes: dict[str, Any] = {}
    robinhood_regimes: dict[str, Any] = {}
    for regime in REGIMES:
        sol_contract = bool(contracts[regime]["solana"]["paper_execution_enabled"])
        fomo_contract = bool(contracts[regime]["fomo"]["paper_execution_enabled"])
        rh_contract = bool(contracts[regime]["robinhood"]["paper_execution_enabled"])

        sol_progress, sol_blockers = _regime_progress(
            solana_counts[regime],
            contract_capable=sol_contract,
            transport_ready=solana_transport_ready,
            probe=probes[regime],
        )
        fomo_progress, fomo_blockers = _regime_progress(
            fomo_counts[regime],
            contract_capable=fomo_contract,
            transport_ready=fomo_transport_ready,
        )
        rh_progress, rh_blockers = _regime_progress(
            robinhood_counts[regime],
            contract_capable=rh_contract,
            transport_ready=robinhood_transport_ready,
        )
        solana_regimes[regime] = {
            **solana_counts[regime],
            "contract": contracts[regime]["solana"],
            "round_trip_probe": probes[regime],
            "e2e_achievable": sol_contract and solana_transport_ready,
            "e2e_proven": bool(probes[regime]["completed"]),
            "progress": sol_progress,
            "blockers": sol_blockers,
        }
        fomo_regimes[regime] = {
            **fomo_counts[regime],
            "contract": contracts[regime]["fomo"],
            "e2e_achievable": fomo_contract and fomo_transport_ready,
            "e2e_proven": int(fomo_counts[regime]["settled_outcomes"]) > 0,
            "progress": fomo_progress,
            "blockers": fomo_blockers,
        }
        robinhood_regimes[regime] = {
            **robinhood_counts[regime],
            "contract": contracts[regime]["robinhood"],
            "e2e_achievable": rh_contract and robinhood_transport_ready,
            "e2e_proven": int(robinhood_counts[regime]["settled_outcomes"]) > 0,
            "progress": rh_progress,
            "blockers": rh_blockers,
        }

    solana_blockers: list[str] = []
    if not direct.get("enabled"):
        solana_blockers.append("direct_solana_disabled")
    if direct.get("enabled") and not direct.get("continuity_ok"):
        solana_blockers.append("direct_solana_continuity_not_ok")
    if not quote_client_ready:
        solana_blockers.append("jupiter_quote_client_not_configured")
    if not simulator_ready:
        solana_blockers.append("unsigned_shadow_simulator_not_configured")
    if not v5.get("paper_strategy_authority"):
        solana_blockers.append("risk_conditioned_v5_paper_authority_not_visible")

    fomo_blockers = list(solana_blockers)
    if not fomo.get("paper_strategy_authority"):
        fomo_blockers.append("fomo_paper_strategy_not_ready")
    if fomo.get("failed_closed"):
        fomo_blockers.append("fomo_paper_strategy_failed_closed")
    if fomo_shadow.get("last_error"):
        fomo_blockers.append("fomo_evidence_collector_error")

    robinhood_blockers: list[str] = []
    if not robinhood_status.get("runtime_ready"):
        robinhood_blockers.append("robinhood_runtime_not_ready")
    if robinhood_status.get("failed_closed"):
        robinhood_blockers.append("robinhood_failed_closed")
    if robinhood_status.get("error"):
        robinhood_blockers.append(str(robinhood_status.get("error")))

    solana_all_achievable = all(row["e2e_achievable"] for row in solana_regimes.values())
    fomo_all_achievable = all(row["e2e_achievable"] for row in fomo_regimes.values())
    robinhood_all_achievable = all(row["e2e_achievable"] for row in robinhood_regimes.values())
    solana_all_proven = all(row["e2e_proven"] for row in solana_regimes.values())
    fomo_all_proven = all(row["e2e_proven"] for row in fomo_regimes.values())
    robinhood_all_proven = all(row["e2e_proven"] for row in robinhood_regimes.values())
    all_contracts = all(
        contracts[regime][plane]["paper_execution_enabled"]
        for regime in REGIMES
        for plane in ("solana", "fomo", "robinhood")
    )

    enriched.update(
        {
            "status_contract_version": STATUS_CONTRACT_VERSION,
            "release_commit": release_commit,
            "solana": {
                "data_plane": base_status.get("data_plane", "direct-solana"),
                "runtime_ready": solana_transport_ready,
                "paper_strategy": v5,
                "regimes": solana_regimes,
                "all_regimes_paper_capable": all(
                    row["contract"]["paper_execution_enabled"] for row in solana_regimes.values()
                ),
                "all_regimes_e2e_achievable": solana_all_achievable,
                "all_regimes_e2e_proven": solana_all_proven,
                "blockers": list(dict.fromkeys(solana_blockers)),
            },
            "fomo": {
                "runtime_ready": fomo_transport_ready,
                "paper_strategy": fomo,
                "evidence_collector": fomo_shadow,
                "regimes": fomo_regimes,
                "all_regimes_paper_capable": all(
                    row["contract"]["paper_execution_enabled"] for row in fomo_regimes.values()
                ),
                "all_regimes_e2e_achievable": fomo_all_achievable,
                "all_regimes_e2e_proven": fomo_all_proven,
                "blockers": list(dict.fromkeys(fomo_blockers)),
            },
            "robinhood": {
                **robinhood_status,
                "regimes": robinhood_regimes,
                "all_regimes_paper_capable": all(
                    row["contract"]["paper_execution_enabled"] for row in robinhood_regimes.values()
                ),
                "all_regimes_e2e_achievable": robinhood_all_achievable,
                "all_regimes_e2e_proven": robinhood_all_proven,
                "blockers": list(dict.fromkeys(robinhood_blockers)),
            },
            "overall": {
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
                "all_regime_software_contracts_paper_capable": all_contracts,
                "all_paper_planes_e2e_achievable": bool(
                    solana_all_achievable and fomo_all_achievable and robinhood_all_achievable
                ),
                "all_regime_e2e_paths_empirically_proven": bool(
                    solana_all_proven and fomo_all_proven and robinhood_all_proven
                ),
                "regime_probe_semantics": {
                    "lane": REGIME_PROBE_LANE,
                    "version": REGIME_PROBE_VERSION,
                    "uses_existing_entry_and_immediate_exit_snapshot": True,
                    "additional_rpc_or_quote_work": False,
                    "promotion_authority": False,
                    "portfolio_allocation_authority": False,
                },
                "blocking_components": list(
                    dict.fromkeys(solana_blockers + fomo_blockers + robinhood_blockers)
                ),
            },
        }
    )
    return enriched


def install_unified_ingestion_status(
    app: Any,
    *,
    runtime_provider: Callable[[], Any],
    robinhood_status_provider: Callable[[], dict[str, Any]],
) -> None:
    """Upgrade the existing ingestion endpoint into the all-strategy command center."""
    if bool(getattr(app.state, "roi_unified_strategy_status", False)):
        return

    ingestion_route = None
    for route in app.routes:
        if getattr(route, "path", None) == "/v1/ingestion/status":
            ingestion_route = route
            break
    if ingestion_route is None:
        raise RuntimeError("canonical ingestion status route not found")

    original = ingestion_route.endpoint

    @wraps(original)
    def unified_ingestion_status() -> dict[str, Any]:
        base = original()
        runtime = runtime_provider()
        robinhood = robinhood_status_provider()
        return build_unified_strategy_status(base, runtime, robinhood)

    ingestion_route.endpoint = unified_ingestion_status
    dependant = getattr(ingestion_route, "dependant", None)
    if dependant is not None:
        dependant.call = unified_ingestion_status

    existing_paths = {getattr(route, "path", None) for route in app.routes}
    if "/v1/strategy/e2e-status" not in existing_paths:
        def dedicated_status() -> dict[str, Any]:
            payload = unified_ingestion_status()
            return {
                "status_contract_version": payload["status_contract_version"],
                "release_commit": payload["release_commit"],
                "solana": payload["solana"],
                "fomo": payload["fomo"],
                "robinhood": payload["robinhood"],
                "overall": payload["overall"],
            }

        app.add_api_route(
            "/v1/strategy/e2e-status",
            dedicated_status,
            methods=["GET"],
            name="strategy_e2e_status",
        )

    app.state.roi_unified_strategy_status = True


__all__ = [
    "REGIMES",
    "REGIME_PROBE_VERSION",
    "STATUS_CONTRACT_VERSION",
    "build_unified_strategy_status",
    "install_regime_paper_e2e_probe",
    "install_unified_ingestion_status",
    "regime_execution_contract",
]
