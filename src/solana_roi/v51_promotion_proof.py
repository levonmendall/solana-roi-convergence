from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from statistics import mean, stdev
from typing import Any, Callable, Iterable

from .strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH


PROMOTION_PROOF_VERSION = "v51-promotion-proof-v1"
DISCOVERY_PERCENT = 60
VALIDATION_PERCENT = 25
HOLDOUT_PERCENT = 15
FDR_Q = 0.10

_ORIGINAL_ENSURE_RELEASE_COMPATIBILITY: Callable[..., Any] | None = None
_INSTALLED = False


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _measurement() -> Any:
    from . import v51_measurement_integrity as measurement

    return measurement


def _release(release_commit: str | None = None) -> str | None:
    value = str(release_commit or _measurement().current_release_commit() or "").strip().lower()
    return value or None


def ensure_attestation_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_release_attestation ("
            "release_commit TEXT NOT NULL, measurement_epoch TEXT NOT NULL, surface TEXT NOT NULL, "
            "candidate_coverage_valid INTEGER NOT NULL, latency_measurement_valid INTEGER NOT NULL, "
            "execution_measurement_valid INTEGER NOT NULL, wallet_attribution_valid INTEGER NOT NULL, "
            "fomo_measurement_valid INTEGER NOT NULL, robinhood_measurement_valid INTEGER NOT NULL, "
            "attested INTEGER NOT NULL, evidence_json TEXT NOT NULL, checked_at TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "PRIMARY KEY(release_commit,measurement_epoch,surface))"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_release_attestation_state "
            "ON v51_release_attestation(measurement_epoch,surface,attested,checked_at)"
        )


def _count(store: Any, sql: str, params: tuple[Any, ...] = ()) -> int:
    try:
        with store._lock:
            row = store.db.execute(sql, params).fetchone()
        return int(row[0] or 0) if row is not None else 0
    except Exception:
        return 0


def _solana_attestation(store: Any, release: str) -> dict[str, Any]:
    measurement = _measurement()
    candidate_count = _count(
        store,
        "SELECT COUNT(*) FROM v51_candidates WHERE surface='SOLANA' AND release_commit=? AND measurement_epoch=?",
        (release, measurement.MEASUREMENT_EPOCH),
    ) if _table_exists(store, "v51_candidates") else 0
    coverage_debt = _count(
        store,
        "SELECT COUNT(*) FROM v51_candidate_current_state WHERE surface='SOLANA' AND release_commit=? "
        "AND measurement_epoch=? AND status='coverage_debt'",
        (release, measurement.MEASUREMENT_EPOCH),
    ) if _table_exists(store, "v51_candidate_current_state") else candidate_count
    latency_count = _count(
        store,
        "SELECT COUNT(*) FROM v51_candidates WHERE surface='SOLANA' AND release_commit=? AND measurement_epoch=? "
        "AND raw_latency_ms IS NOT NULL",
        (release, measurement.MEASUREMENT_EPOCH),
    ) if _table_exists(store, "v51_candidates") else 0
    execution_count = _count(
        store,
        "SELECT COUNT(*) FROM v51_candidate_current_state WHERE surface='SOLANA' AND release_commit=? "
        "AND measurement_epoch=? AND stage='execution_evidence' AND status='complete'",
        (release, measurement.MEASUREMENT_EPOCH),
    ) if _table_exists(store, "v51_candidate_current_state") else 0
    wallet_targets = _count(
        store,
        "SELECT COUNT(*) FROM v51_candidates WHERE surface='SOLANA' AND release_commit=? AND measurement_epoch=? "
        "AND trigger_wallet IS NOT NULL AND trigger_wallet<>''",
        (release, measurement.MEASUREMENT_EPOCH),
    ) if _table_exists(store, "v51_candidates") else 0
    wallet_lineage = _count(
        store,
        "SELECT COUNT(DISTINCT source_candidate_id) FROM v51_wallet_discovery_forward_lineage "
        "WHERE release_commit=? AND measurement_epoch=? AND source_candidate_id IS NOT NULL",
        (release, measurement.MEASUREMENT_EPOCH),
    ) if _table_exists(store, "v51_wallet_discovery_forward_lineage") else 0
    candidate_ok = candidate_count > 0 and coverage_debt == 0
    latency_ok = candidate_count > 0 and latency_count == candidate_count
    execution_ok = execution_count > 0
    wallet_ok = wallet_targets == 0 or wallet_lineage >= wallet_targets
    return {
        "surface": "SOLANA",
        "candidate_count": candidate_count,
        "coverage_debt_count": coverage_debt,
        "latency_measurement_count": latency_count,
        "execution_evidence_complete_count": execution_count,
        "wallet_candidate_count": wallet_targets,
        "wallet_lineage_count": wallet_lineage,
        "candidate_coverage_valid": candidate_ok,
        "latency_measurement_valid": latency_ok,
        "execution_measurement_valid": execution_ok,
        "wallet_attribution_valid": wallet_ok,
        "fomo_measurement_valid": False,
        "robinhood_measurement_valid": False,
        "attested": candidate_ok and latency_ok and execution_ok and wallet_ok,
    }


def _fomo_attestation(store: Any, release: str) -> dict[str, Any]:
    measurement = _measurement()
    candidate_count = _count(
        store,
        "SELECT COUNT(*) FROM v51_candidate_current_state WHERE surface='FOMO' AND release_commit=? "
        "AND measurement_epoch=? AND stage='candidate' AND status='complete'",
        (release, measurement.MEASUREMENT_EPOCH),
    ) if _table_exists(store, "v51_candidate_current_state") else 0
    coverage_debt = _count(
        store,
        "SELECT COUNT(*) FROM v51_candidate_current_state WHERE surface='FOMO' AND release_commit=? "
        "AND measurement_epoch=? AND status='coverage_debt'",
        (release, measurement.MEASUREMENT_EPOCH),
    ) if _table_exists(store, "v51_candidate_current_state") else candidate_count
    execution_count = _count(
        store,
        "SELECT COUNT(*) FROM v51_candidate_current_state WHERE surface='FOMO' AND release_commit=? "
        "AND measurement_epoch=? AND stage='execution_evidence' AND status='complete'",
        (release, measurement.MEASUREMENT_EPOCH),
    ) if _table_exists(store, "v51_candidate_current_state") else 0
    ok = candidate_count > 0 and coverage_debt == 0 and execution_count > 0
    return {
        "surface": "FOMO",
        "candidate_count": candidate_count,
        "coverage_debt_count": coverage_debt,
        "execution_evidence_complete_count": execution_count,
        "candidate_coverage_valid": candidate_count > 0 and coverage_debt == 0,
        "latency_measurement_valid": True,
        "execution_measurement_valid": execution_count > 0,
        "wallet_attribution_valid": True,
        "fomo_measurement_valid": ok,
        "robinhood_measurement_valid": False,
        "attested": ok,
    }


def _robinhood_attestation(store: Any, release: str) -> dict[str, Any]:
    ledger = "v51_robinhood_candidate_ledger"
    candidates = _count(
        store,
        "SELECT COUNT(*) FROM v51_robinhood_candidate_ledger WHERE release_commit=?",
        (release,),
    ) if _table_exists(store, ledger) and "release_commit" in _columns(store, ledger) else 0
    terminal = _count(
        store,
        "SELECT COUNT(*) FROM v51_robinhood_candidate_ledger WHERE release_commit=? "
        "AND decision IN ('paper_enter','paper_reject')",
        (release,),
    ) if candidates else 0
    execution_count = _count(
        store,
        "SELECT COUNT(*) FROM v51_candidate_pipeline_audit WHERE surface='ROBINHOOD_CHAIN' AND release_commit=? "
        "AND stage='execution_evidence' AND status='complete'",
        (release,),
    ) if _table_exists(store, "v51_candidate_pipeline_audit") else 0
    ok = candidates > 0 and terminal == candidates
    return {
        "surface": "ROBINHOOD_CHAIN",
        "candidate_count": candidates,
        "terminal_decision_count": terminal,
        "decision_coverage_debt_count": max(0, candidates - terminal),
        "execution_evidence_complete_count": execution_count,
        "candidate_coverage_valid": ok,
        "latency_measurement_valid": True,
        "execution_measurement_valid": execution_count > 0 or terminal > 0,
        "wallet_attribution_valid": True,
        "fomo_measurement_valid": False,
        "robinhood_measurement_valid": ok,
        "attested": ok,
    }


def _surface_payload(store: Any, surface: str, release: str) -> dict[str, Any]:
    if surface == "SOLANA":
        return _solana_attestation(store, release)
    if surface == "FOMO":
        return _fomo_attestation(store, release)
    if surface == "ROBINHOOD_CHAIN":
        return _robinhood_attestation(store, release)
    raise ValueError(f"unsupported promotion attestation surface: {surface}")


def _detected_surfaces(store: Any) -> list[str]:
    surfaces: list[str] = []
    if _table_exists(store, "v51_candidates") or _table_exists(store, "risk_conditioned_alpha_v5_trials"):
        surfaces.append("SOLANA")
    if _table_exists(store, "fomo_shadow_observations") or _table_exists(store, "fomo_paper_trials"):
        surfaces.append("FOMO")
    if _table_exists(store, "v51_robinhood_candidate_ledger") or _table_exists(store, "robinhood_paper_trials"):
        surfaces.append("ROBINHOOD_CHAIN")
    return surfaces


def refresh_release_attestation(
    store: Any,
    *,
    release_commit: str | None = None,
    surfaces: Iterable[str] | None = None,
) -> dict[str, Any]:
    ensure_attestation_schema(store)
    measurement = _measurement()
    release = _release(release_commit)
    if not release:
        return {
            "release_commit": None,
            "measurement_epoch": measurement.MEASUREMENT_EPOCH,
            "attested": False,
            "reason": "release_commit_unbound",
            "surfaces": {},
        }
    target = list(surfaces) if surfaces is not None else _detected_surfaces(store)
    payloads: dict[str, dict[str, Any]] = {}
    for surface in target:
        payload = _surface_payload(store, surface, release)
        payloads[surface] = payload
        with store._lock, store.db:
            store.db.execute(
                "INSERT INTO v51_release_attestation("
                "release_commit,measurement_epoch,surface,candidate_coverage_valid,latency_measurement_valid,"
                "execution_measurement_valid,wallet_attribution_valid,fomo_measurement_valid,robinhood_measurement_valid,"
                "attested,evidence_json,checked_at,paper_only,live_money_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,0) "
                "ON CONFLICT(release_commit,measurement_epoch,surface) DO UPDATE SET "
                "candidate_coverage_valid=excluded.candidate_coverage_valid,"
                "latency_measurement_valid=excluded.latency_measurement_valid,"
                "execution_measurement_valid=excluded.execution_measurement_valid,"
                "wallet_attribution_valid=excluded.wallet_attribution_valid,"
                "fomo_measurement_valid=excluded.fomo_measurement_valid,"
                "robinhood_measurement_valid=excluded.robinhood_measurement_valid,attested=excluded.attested,"
                "evidence_json=excluded.evidence_json,checked_at=excluded.checked_at,paper_only=1,live_money_authority=0",
                (
                    release,
                    measurement.MEASUREMENT_EPOCH,
                    surface,
                    1 if payload["candidate_coverage_valid"] else 0,
                    1 if payload["latency_measurement_valid"] else 0,
                    1 if payload["execution_measurement_valid"] else 0,
                    1 if payload["wallet_attribution_valid"] else 0,
                    1 if payload["fomo_measurement_valid"] else 0,
                    1 if payload["robinhood_measurement_valid"] else 0,
                    1 if payload["attested"] else 0,
                    json.dumps(payload, sort_keys=True, default=str),
                    _utcnow(),
                ),
            )
    # Keep the legacy release-level boolean conservative. Actual strategy promotion
    # gates are surface-specific and use `surface_attested` below.
    all_local = bool(payloads) and all(bool(value.get("attested")) for value in payloads.values())
    if _table_exists(store, "v51_release_compatibility"):
        solana = payloads.get("SOLANA") or {}
        fomo = payloads.get("FOMO") or {}
        robinhood = payloads.get("ROBINHOOD_CHAIN") or {}
        with store._lock, store.db:
            store.db.execute(
                "UPDATE v51_release_compatibility SET candidate_coverage_valid=?,latency_measurement_valid=?,"
                "execution_measurement_valid=?,wallet_attribution_valid=?,fomo_measurement_valid=?,"
                "robinhood_measurement_valid=?,promotion_eligible=?,reason=? WHERE release_commit=?",
                (
                    1 if bool(solana.get("candidate_coverage_valid") or robinhood.get("candidate_coverage_valid")) else 0,
                    1 if bool(solana.get("latency_measurement_valid", True)) else 0,
                    1 if bool(solana.get("execution_measurement_valid") or fomo.get("execution_measurement_valid") or robinhood.get("execution_measurement_valid")) else 0,
                    1 if bool(solana.get("wallet_attribution_valid", True)) else 0,
                    1 if bool(fomo.get("fomo_measurement_valid")) else 0,
                    1 if bool(robinhood.get("robinhood_measurement_valid")) else 0,
                    1 if all_local else 0,
                    "live_release_attestation_confirmed" if all_local else "live_release_attestation_pending",
                    release,
                ),
            )
    return {
        "promotion_proof_version": PROMOTION_PROOF_VERSION,
        "release_commit": release,
        "measurement_epoch": measurement.MEASUREMENT_EPOCH,
        "attested": all_local,
        "surfaces": payloads,
        "paper_only": True,
        "live_money_authority": False,
    }


def surface_attested(store: Any, surface: str, *, release_commit: str | None = None) -> bool:
    release = _release(release_commit)
    if not release:
        return False
    result = refresh_release_attestation(store, release_commit=release, surfaces=(surface,))
    return bool((result.get("surfaces") or {}).get(surface, {}).get("attested"))


def _ensure_release_with_attestation(store: Any, release_commit: str | None = None) -> dict[str, Any] | None:
    if _ORIGINAL_ENSURE_RELEASE_COMPATIBILITY is None:
        raise RuntimeError("promotion attestation gate is not installed")
    row = _ORIGINAL_ENSURE_RELEASE_COMPATIBILITY(store, release_commit)
    release = _release(release_commit)
    current = _release(None)
    if release and current and release == current:
        ensure_attestation_schema(store)
        with store._lock:
            existing = store.db.execute(
                "SELECT 1 FROM v51_release_attestation WHERE release_commit=? AND measurement_epoch=? LIMIT 1",
                (release, _measurement().MEASUREMENT_EPOCH),
            ).fetchone()
        if existing is None and _table_exists(store, "v51_release_compatibility"):
            with store._lock, store.db:
                store.db.execute(
                    "UPDATE v51_release_compatibility SET candidate_coverage_valid=0,latency_measurement_valid=0,"
                    "execution_measurement_valid=0,wallet_attribution_valid=0,fomo_measurement_valid=0,"
                    "robinhood_measurement_valid=0,promotion_eligible=0,reason=? WHERE release_commit=?",
                    ("current_release_pending_live_attestation", release),
                )
            with store._lock:
                current_row = store.db.execute(
                    "SELECT * FROM v51_release_compatibility WHERE release_commit=?", (release,)
                ).fetchone()
            return dict(current_row) if current_row is not None else row
    return row


def install_release_attestation_gate() -> None:
    global _ORIGINAL_ENSURE_RELEASE_COMPATIBILITY, _INSTALLED
    if _INSTALLED:
        return
    measurement = _measurement()
    current = measurement.ensure_release_compatibility
    if bool(getattr(current, "_roi_v51_release_attestation_gate", False)):
        _INSTALLED = True
        return
    _ORIGINAL_ENSURE_RELEASE_COMPATIBILITY = current
    setattr(_ensure_release_with_attestation, "_roi_v51_release_attestation_gate", True)
    measurement.ensure_release_compatibility = _ensure_release_with_attestation  # type: ignore[assignment]
    try:
        from . import v51_measurement_compatibility_filters as filters

        filters.ensure_release_compatibility = _ensure_release_with_attestation  # type: ignore[assignment]
    except Exception:
        pass
    _INSTALLED = True


def event_cluster_id(row: dict[str, Any], *, family: str | None = None) -> str:
    family_name = str(family or row.get("family") or row.get("venue") or row.get("surface") or "UNKNOWN")
    token = str(row.get("token_mint") or row.get("token") or "").strip()
    lifecycle = str(row.get("lifecycle") or "unknown").strip()
    if token:
        raw = f"{family_name}|{token}|{lifecycle}"
    else:
        identity = str(row.get("source_signature") or row.get("trial_id") or row.get("id") or "unknown")
        raw = f"{family_name}|source:{identity}|{lifecycle}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def evidence_partition(cluster_id: str) -> str:
    bucket = int(hashlib.sha256(cluster_id.encode()).hexdigest()[:8], 16) % 100
    if bucket < DISCOVERY_PERCENT:
        return "discovery"
    if bucket < DISCOVERY_PERCENT + VALIDATION_PERCENT:
        return "validation"
    return "holdout"


def cluster_rows(
    rows: Iterable[dict[str, Any]],
    *,
    family: str | None = None,
    excluded_cluster_ids: set[str] | None = None,
    promotion_only: bool = False,
) -> list[dict[str, Any]]:
    excluded = excluded_cluster_ids or set()
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in rows:
        row = dict(raw)
        cluster_id = event_cluster_id(row, family=family)
        if cluster_id in excluded:
            continue
        grouped[cluster_id].append(row)
    result: list[dict[str, Any]] = []
    for cluster_id, group in grouped.items():
        partition = evidence_partition(cluster_id)
        if promotion_only and partition == "discovery":
            continue
        values: list[float] = []
        for row in group:
            try:
                value = float(row.get("net_return"))
            except (TypeError, ValueError):
                continue
            if math.isfinite(value) and value > -1.0:
                values.append(value)
        if not values:
            continue
        settled_values = [str(row.get("settled_at") or "") for row in group if row.get("settled_at")]
        representative = dict(group[-1])
        representative.update(
            {
                "event_cluster_id": cluster_id,
                "evidence_partition": partition,
                "cluster_observation_count": len(group),
                "net_return": mean(values),
                "settled_at": max(settled_values) if settled_values else representative.get("settled_at"),
            }
        )
        result.append(representative)
    result.sort(key=lambda row: (str(row.get("settled_at") or ""), str(row.get("event_cluster_id") or "")))
    return result


def positive_edge_p_value(values: Iterable[float]) -> float:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    if len(clean) < 2:
        return 1.0
    sigma = stdev(clean)
    if sigma <= 0.0:
        return 0.0 if mean(clean) > 0.0 else 1.0
    z = mean(clean) / (sigma / math.sqrt(len(clean)))
    return max(0.0, min(1.0, 0.5 * math.erfc(z / math.sqrt(2.0))))


def benjamini_hochberg(p_values: dict[str, float], *, q: float = FDR_Q) -> dict[str, bool]:
    ordered = sorted((max(0.0, min(1.0, float(p))), key) for key, p in p_values.items())
    cutoff_rank = 0
    total = len(ordered)
    for rank, (p_value, _key) in enumerate(ordered, start=1):
        if p_value <= q * rank / max(1, total):
            cutoff_rank = rank
    accepted = {key: False for key in p_values}
    if cutoff_rank:
        threshold = ordered[cutoff_rank - 1][0]
        for p_value, key in ordered:
            accepted[key] = p_value <= threshold
    return accepted


def status() -> dict[str, Any]:
    return {
        "version": PROMOTION_PROOF_VERSION,
        "installed": _INSTALLED,
        "current_release_starts_unattested": True,
        "promotion_attestation_is_surface_specific": True,
        "event_independence_unit": "family_token_lifecycle_cluster",
        "partition_policy": {
            "discovery_percent": DISCOVERY_PERCENT,
            "validation_percent": VALIDATION_PERCENT,
            "holdout_percent": HOLDOUT_PERCENT,
            "partition_assignment": "stable_sha256_event_cluster",
            "discovery_rows_have_promotion_authority": False,
        },
        "false_discovery_q": FDR_Q,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "PROMOTION_PROOF_VERSION",
    "benjamini_hochberg",
    "cluster_rows",
    "ensure_attestation_schema",
    "event_cluster_id",
    "evidence_partition",
    "install_release_attestation_gate",
    "positive_edge_p_value",
    "refresh_release_attestation",
    "status",
    "surface_attested",
]
