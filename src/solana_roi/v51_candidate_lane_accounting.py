from __future__ import annotations

from typing import Any


ACCOUNTING_VERSION = "v51-five-lane-candidate-conservation-v1"
FIVE_LANES = ("pump_fun", "pump_amm", "raydium", "fomo", "robinhood")
LOCAL_LANES = ("pump_fun", "pump_amm", "raydium", "fomo")


def _lane_from_venue(venue: Any) -> str | None:
    normalized = str(venue or "").strip().upper()
    if normalized == "PUMP_FUN":
        return "pump_fun"
    if normalized in {"PUMP_AMM", "PUMPSWAP", "PUMP_SWAP"}:
        return "pump_amm"
    if normalized == "RAYDIUM":
        return "raydium"
    return None


def _empty_lane(*, verified: bool, status: str) -> dict[str, Any]:
    return {
        "verified": bool(verified),
        "status": status,
        "observed_candidate_count": 0 if verified else None,
        "terminal_candidate_count": 0 if verified else None,
        "terminal_rejected_count": 0 if verified else None,
        "terminal_settled_count": 0 if verified else None,
        "valid_pending_candidate_count": 0 if verified else None,
        "coverage_debt_candidate_count": 0 if verified else None,
        "unexplained_candidate_count": 0 if verified else None,
        "conservation_delta": 0 if verified else None,
        "conserved": bool(verified),
        "reconciled": bool(verified),
        "stage_summary": {},
    }


def _current_rows(
    store: Any,
    *,
    economic_epoch: str,
    measurement_epoch: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    with store._lock:
        candidate_rows = store.db.execute(
            "SELECT surface,candidate_id,venue FROM v51_candidates "
            "WHERE surface='SOLANA' AND economic_epoch=? AND measurement_epoch=?",
            (economic_epoch, measurement_epoch),
        ).fetchall()
        stage_rows = store.db.execute(
            "SELECT surface,candidate_id,stage,status,reason FROM v51_candidate_current_state "
            "WHERE surface IN ('SOLANA','FOMO') AND economic_epoch=? AND measurement_epoch=?",
            (economic_epoch, measurement_epoch),
        ).fetchall()
    return [dict(row) for row in candidate_rows], [dict(row) for row in stage_rows]


def _classify_candidate(states: list[dict[str, Any]]) -> str:
    statuses = {str(row.get("status") or "") for row in states}
    by_stage = {str(row.get("stage") or ""): str(row.get("status") or "") for row in states}
    if "coverage_debt" in statuses:
        return "coverage_debt"
    if by_stage.get("settlement") == "complete" or by_stage.get("learning") == "complete":
        return "settled"
    if by_stage.get("position") == "not_opened":
        return "rejected"
    if "pending" in statuses or by_stage.get("position") == "paper_position_authorized":
        return "valid_pending"
    return "unexplained"


def _local_lane_accounting(
    store: Any,
    *,
    economic_epoch: str,
    measurement_epoch: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    lanes = {lane: _empty_lane(verified=True, status="verified") for lane in LOCAL_LANES}
    anomalies: dict[str, Any] = {
        "local_candidate_store_readable": True,
        "unclassified_solana_candidate_count": 0,
        "unclassified_solana_candidate_ids": [],
        "orphan_stage_state_count": 0,
        "orphan_stage_candidate_ids": [],
    }
    try:
        candidate_rows, stage_rows = _current_rows(
            store,
            economic_epoch=economic_epoch,
            measurement_epoch=measurement_epoch,
        )
    except Exception as exc:
        for lane in LOCAL_LANES:
            lanes[lane] = _empty_lane(verified=False, status="unable_to_verify")
        anomalies["local_candidate_store_readable"] = False
        anomalies["local_candidate_store_error"] = type(exc).__name__
        return lanes, anomalies

    candidate_lane: dict[tuple[str, str], str] = {}
    for row in candidate_rows:
        candidate_id = str(row.get("candidate_id") or "")
        lane = _lane_from_venue(row.get("venue"))
        if lane is None:
            anomalies["unclassified_solana_candidate_count"] += 1
            if len(anomalies["unclassified_solana_candidate_ids"]) < 20:
                anomalies["unclassified_solana_candidate_ids"].append(candidate_id)
            continue
        candidate_lane[("SOLANA", candidate_id)] = lane

    # FOMO candidates are born in the shadow-observation pipeline rather than
    # v51_candidates. Its canonical identity is therefore the candidate-stage row.
    for row in stage_rows:
        if str(row.get("surface") or "") != "FOMO" or str(row.get("stage") or "") != "candidate":
            continue
        candidate_lane[("FOMO", str(row.get("candidate_id") or ""))] = "fomo"

    states_by_candidate: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in stage_rows:
        key = (str(row.get("surface") or ""), str(row.get("candidate_id") or ""))
        lane = candidate_lane.get(key)
        if lane is None:
            anomalies["orphan_stage_state_count"] += 1
            if len(anomalies["orphan_stage_candidate_ids"]) < 20:
                anomalies["orphan_stage_candidate_ids"].append(f"{key[0]}:{key[1]}")
            continue
        states_by_candidate.setdefault(key, []).append(row)
        stage = str(row.get("stage") or "unknown")
        status = str(row.get("status") or "unknown")
        lane_summary = lanes[lane]["stage_summary"]
        lane_summary.setdefault(stage, {})[status] = int(lane_summary.setdefault(stage, {}).get(status, 0)) + 1

    for key, lane in candidate_lane.items():
        lane_state = lanes[lane]
        lane_state["observed_candidate_count"] += 1
        classification = _classify_candidate(states_by_candidate.get(key, []))
        if classification == "settled":
            lane_state["terminal_settled_count"] += 1
            lane_state["terminal_candidate_count"] += 1
        elif classification == "rejected":
            lane_state["terminal_rejected_count"] += 1
            lane_state["terminal_candidate_count"] += 1
        elif classification == "valid_pending":
            lane_state["valid_pending_candidate_count"] += 1
        elif classification == "coverage_debt":
            lane_state["coverage_debt_candidate_count"] += 1
        else:
            lane_state["unexplained_candidate_count"] += 1

    for lane_state in lanes.values():
        accounted = (
            int(lane_state["terminal_candidate_count"])
            + int(lane_state["valid_pending_candidate_count"])
            + int(lane_state["coverage_debt_candidate_count"])
            + int(lane_state["unexplained_candidate_count"])
        )
        lane_state["conservation_delta"] = int(lane_state["observed_candidate_count"]) - accounted
        lane_state["conserved"] = lane_state["conservation_delta"] == 0
        lane_state["reconciled"] = bool(
            lane_state["conserved"]
            and int(lane_state["coverage_debt_candidate_count"]) == 0
            and int(lane_state["unexplained_candidate_count"]) == 0
        )
        lane_state["status"] = "reconciled" if lane_state["reconciled"] else "accounted_with_debt"

    return lanes, anomalies


def _robinhood_lane_accounting(
    merged_coverage: dict[str, Any],
    *,
    economic_epoch: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    proof = merged_coverage.get("robinhood")
    proof_state = str(merged_coverage.get("robinhood_proof_state") or "unavailable")
    available = bool(merged_coverage.get("robinhood_proof_available")) and isinstance(proof, dict)
    anomalies = {
        "robinhood_proof_available": available,
        "robinhood_proof_state": proof_state,
        "robinhood_count_inconsistency": False,
    }
    if not available:
        return _empty_lane(verified=False, status="unable_to_verify"), anomalies

    proof = dict(proof)
    authority_matches = str(proof.get("authority_id") or "") == str(merged_coverage.get("authority_id") or "")
    epoch_matches = str(proof.get("economic_freeze_epoch") or "") == economic_epoch
    verified = proof_state == "confirmed" and authority_matches and epoch_matches

    observed = int(proof.get("canonical_candidate_count") or 0)
    rejected = int(proof.get("explicit_rejection_count") or 0)
    settled = int(proof.get("settled_entry_count") or 0)
    pending = int(proof.get("pending_settlement_count") or 0)
    candidate_debt = int(proof.get("decision_coverage_debt_count") or 0)
    terminal = rejected + settled
    assigned_without_unexplained = terminal + pending + candidate_debt
    unexplained = max(0, observed - assigned_without_unexplained)
    delta = observed - (assigned_without_unexplained + unexplained)
    proof_debt = int(proof.get("coverage_debt_count") or 0)
    coverage_complete = bool(proof.get("coverage_complete"))
    if assigned_without_unexplained > observed:
        anomalies["robinhood_count_inconsistency"] = True

    lane = {
        "verified": verified,
        "status": "reconciled" if verified and delta == 0 and unexplained == 0 and proof_debt == 0 and coverage_complete else (
            "accounted_with_debt" if verified else "unable_to_verify"
        ),
        "observed_candidate_count": observed,
        "terminal_candidate_count": terminal,
        "terminal_rejected_count": rejected,
        "terminal_settled_count": settled,
        "valid_pending_candidate_count": pending,
        "coverage_debt_candidate_count": candidate_debt,
        "unexplained_candidate_count": unexplained,
        "conservation_delta": delta,
        "conserved": delta == 0 and not anomalies["robinhood_count_inconsistency"],
        "reconciled": bool(
            verified
            and delta == 0
            and not anomalies["robinhood_count_inconsistency"]
            and unexplained == 0
            and candidate_debt == 0
            and proof_debt == 0
            and coverage_complete
        ),
        "proof_coverage_debt_count": proof_debt,
        "coverage_complete": coverage_complete,
        "stage_summary": dict(proof.get("stage_summary") or {}),
        "proof_state": proof_state,
        "authority_matches": authority_matches,
        "economic_epoch_matches": epoch_matches,
    }
    return lane, anomalies


def build_five_lane_candidate_accounting(
    store: Any,
    *,
    merged_coverage: dict[str, Any] | None,
) -> dict[str, Any]:
    coverage = dict(merged_coverage or {})
    economic_epoch = str(coverage.get("economic_freeze_epoch") or "")
    measurement_epoch = str(coverage.get("measurement_epoch") or "")
    local_lanes, anomalies = _local_lane_accounting(
        store,
        economic_epoch=economic_epoch,
        measurement_epoch=measurement_epoch,
    )
    robinhood, robinhood_anomalies = _robinhood_lane_accounting(
        coverage,
        economic_epoch=economic_epoch,
    )
    lanes = {**local_lanes, "robinhood": robinhood}
    anomalies.update(robinhood_anomalies)

    unverified_lanes = [lane for lane in FIVE_LANES if not bool(lanes[lane].get("verified"))]
    all_verified = not unverified_lanes
    local_totals = {
        "observed_candidate_count": sum(int(lanes[lane].get("observed_candidate_count") or 0) for lane in LOCAL_LANES),
        "terminal_candidate_count": sum(int(lanes[lane].get("terminal_candidate_count") or 0) for lane in LOCAL_LANES),
        "valid_pending_candidate_count": sum(int(lanes[lane].get("valid_pending_candidate_count") or 0) for lane in LOCAL_LANES),
        "coverage_debt_candidate_count": sum(int(lanes[lane].get("coverage_debt_candidate_count") or 0) for lane in LOCAL_LANES),
        "unexplained_candidate_count": sum(int(lanes[lane].get("unexplained_candidate_count") or 0) for lane in LOCAL_LANES),
    }

    if all_verified:
        observed = sum(int(lanes[lane]["observed_candidate_count"]) for lane in FIVE_LANES)
        terminal = sum(int(lanes[lane]["terminal_candidate_count"]) for lane in FIVE_LANES)
        pending = sum(int(lanes[lane]["valid_pending_candidate_count"]) for lane in FIVE_LANES)
        debt = sum(int(lanes[lane]["coverage_debt_candidate_count"]) for lane in FIVE_LANES)
        unexplained = sum(int(lanes[lane]["unexplained_candidate_count"]) for lane in FIVE_LANES)
        delta = observed - terminal - pending - debt - unexplained
    else:
        observed = terminal = pending = debt = unexplained = delta = None

    local_readable = bool(anomalies.get("local_candidate_store_readable"))
    anomaly_free = (
        int(anomalies.get("unclassified_solana_candidate_count") or 0) == 0
        and int(anomalies.get("orphan_stage_state_count") or 0) == 0
        and not bool(anomalies.get("robinhood_count_inconsistency"))
    )
    conserved = bool(all_verified and delta == 0 and all(bool(lanes[lane].get("conserved")) for lane in FIVE_LANES))
    reconciled = bool(
        conserved
        and local_readable
        and anomaly_free
        and all(bool(lanes[lane].get("reconciled")) for lane in FIVE_LANES)
        and int(debt or 0) == 0
        and int(unexplained or 0) == 0
    )

    return {
        "accounting_version": ACCOUNTING_VERSION,
        "scope": "canonical_forward_paper_candidate_population_only_not_live_ingress_attestation",
        "equation": "observed = terminal + valid_pending + coverage_debt + unexplained",
        "economic_freeze_epoch": economic_epoch or None,
        "measurement_epoch": measurement_epoch or None,
        "lanes": lanes,
        "candidate_conservation": {
            "candidate_population_verifiable": all_verified,
            "unverified_lanes": unverified_lanes,
            "observed_candidate_count": observed,
            "terminal_candidate_count": terminal,
            "valid_pending_candidate_count": pending,
            "coverage_debt_candidate_count": debt,
            "unexplained_candidate_count": unexplained,
            "conservation_delta": delta,
            "conserved": conserved,
            "reconciled": reconciled,
        },
        "local_accounted_subtotal": local_totals,
        "classification_anomalies": anomalies,
        "paper_only": True,
        "live_money_authority": False,
        "changes_strategy_authority": False,
        "changes_economic_thresholds": False,
    }


__all__ = [
    "ACCOUNTING_VERSION",
    "FIVE_LANES",
    "build_five_lane_candidate_accounting",
]
