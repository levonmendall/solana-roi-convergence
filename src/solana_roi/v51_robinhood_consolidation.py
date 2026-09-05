from __future__ import annotations

import contextvars
import json
import math
from datetime import datetime, timezone
from statistics import median
from typing import Any, Callable

from .robinhood_chain_core import HARVEST_FRACTION, MAX_HOLD_SECONDS, STOP_LOSS_FRACTION
from .robinhood_chain_profit_maximizer import (
    ROBINHOOD_V5_MAX_OPEN_EXPOSURE,
    ROBINHOOD_V5_MAX_POSITION,
    ROBINHOOD_V5_MIN_SAMPLES,
    RobinhoodProfitMaximizerMixin,
)
from .strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH, hazard_requirements
from .v51_candidate_pipeline import _record as _record_stage

_INSTALLED = False
_ORIGINAL_CHOOSE: Callable[..., Any] | None = None
_ORIGINAL_EXIT: Callable[..., Any] | None = None
_ORIGINAL_OPEN_V2: Callable[..., Any] | None = None
_ORIGINAL_OPEN_V3: Callable[..., Any] | None = None
_TRACE: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar("v51_robinhood_candidate_trace", default=None)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_ledger(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_robinhood_candidate_ledger ("
            "candidate_id TEXT PRIMARY KEY, release_commit TEXT NOT NULL, token TEXT NOT NULL, market TEXT NOT NULL, "
            "venue TEXT NOT NULL, lifecycle TEXT NOT NULL, selected_lane TEXT, position_fraction REAL NOT NULL DEFAULT 0, "
            "decision TEXT NOT NULL, decision_reason TEXT NOT NULL, trial_id INTEGER, observed_at TEXT NOT NULL, "
            "authority_id TEXT NOT NULL, economic_freeze_epoch TEXT NOT NULL, paper_only INTEGER NOT NULL, "
            "live_money_authority INTEGER NOT NULL)"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_robinhood_candidate_release "
            "ON v51_robinhood_candidate_ledger(release_commit,decision,observed_at)"
        )


def _candidate_id(token: str, market: str, recent_swaps: Any) -> str:
    latest = recent_swaps[-1] if recent_swaps else {}
    tx_hash = str(latest.get("tx_hash") or latest.get("signature") or "")
    log_index = str(latest.get("log_index") if latest.get("log_index") is not None else "")
    if tx_hash:
        return f"rh:{token}:{tx_hash}:{log_index}"
    block = str(latest.get("block_number") or "")
    return f"rh:{token}:{market}:{block}:{len(recent_swaps or ())}"


def _upsert_ledger(self: Any, trace: dict[str, Any], *, decision: str, reason: str, trial_id: int | None = None) -> None:
    _ensure_ledger(self.store)
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "INSERT INTO v51_robinhood_candidate_ledger("
            "candidate_id,release_commit,token,market,venue,lifecycle,selected_lane,position_fraction,decision,decision_reason,"
            "trial_id,observed_at,authority_id,economic_freeze_epoch,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0) ON CONFLICT(candidate_id) DO UPDATE SET "
            "selected_lane=excluded.selected_lane,position_fraction=excluded.position_fraction,decision=excluded.decision,"
            "decision_reason=excluded.decision_reason,trial_id=COALESCE(excluded.trial_id,v51_robinhood_candidate_ledger.trial_id),"
            "observed_at=excluded.observed_at",
            (
                trace["candidate_id"], self.release_commit, trace["token"], trace["market"], trace["venue"], trace["lifecycle"],
                trace.get("selected_lane"), float(trace.get("position_fraction") or 0.0), decision, reason, trial_id, _utcnow(),
                AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH,
            ),
        )


def _profile_state_for_exact_hazard(profile: dict[str, Any], risk_severity: float, risk_signature: str) -> str:
    hp = dict(profile.get("hierarchical_profile") or {})
    req = hazard_requirements(risk_severity, risk_signature)
    exact_n = int(hp.get("exact_sample_count") or profile.get("sample_count") or 0)
    independent_n = int(hp.get("independent_evidence_count") or exact_n)
    growth = hp.get("best_expected_log_growth", profile.get("best_expected_log_growth"))
    leave_best = hp.get("leave_best_trade_out_mean", profile.get("trimmed_mean_ex_best"))
    ci_upper = hp.get("mean_return_ci95_upper")
    mature = exact_n >= int(req["minimum_exact_outcomes"]) and independent_n >= int(req["minimum_independent_outcomes"])
    promoted = bool(
        mature and growth is not None and float(growth) > float(req["minimum_expected_log_growth"])
        and leave_best is not None and float(leave_best) > 0.0
    )
    kill_min = max(60, int(req["minimum_independent_outcomes"]))
    killed = bool(
        exact_n >= int(req["minimum_exact_outcomes"]) and independent_n >= kill_min
        and growth is not None and float(growth) <= 0.0
        and leave_best is not None and float(leave_best) <= 0.0
        and ci_upper is not None and float(ci_upper) <= 0.0
    )
    if killed:
        return "killed_negative_robust_edge"
    if promoted:
        return "promoted_positive_hierarchical_edge"
    return "mature_unproven" if mature else "bootstrap_hierarchical_evidence"


def _choose(self: Any, **kwargs: Any) -> tuple[str | None, float, dict[str, Any]]:
    entity = str(kwargs.get("entity") or "")
    role = str(kwargs.get("role") or "unknown")
    venue = str(kwargs.get("venue") or "UNKNOWN")
    lifecycle = str(kwargs.get("lifecycle") or "unknown")
    regime = str(kwargs.get("regime") or "unknown")
    risk_signature = str(kwargs.get("risk_signature") or "clean")
    risk_severity = float(kwargs.get("risk_severity") or 0.0)
    flow_state = str(kwargs.get("flow_state") or "neutral")
    lanes = list(kwargs.get("lanes") or ())
    profiles: dict[str, Any] = {}
    promoted: list[tuple[str, dict[str, Any]]] = []
    viable: list[tuple[str, dict[str, Any]]] = []
    for lane in lanes:
        profile = self._v5_profile(
            entity=entity, role=role, lane=lane, venue=venue, lifecycle=lifecycle, regime=regime,
            risk_signature=risk_signature, flow_state=flow_state,
        )
        state = _profile_state_for_exact_hazard(profile, risk_severity, risk_signature)
        profile = dict(profile)
        profile["consolidated_state"] = state
        profile["exact_hazard_requirements"] = hazard_requirements(risk_severity, risk_signature)
        profiles[lane] = profile
        if state == "promoted_positive_hierarchical_edge" and float(profile.get("best_fraction") or 0.0) > 0.0:
            promoted.append((lane, profile))
        if state != "killed_negative_robust_edge":
            viable.append((lane, profile))
    if promoted:
        lane, profile = max(promoted, key=lambda item: float(item[1].get("best_expected_log_growth") or float("-inf")))
        fraction = float(profile.get("best_fraction") or 0.0)
        basis = "promoted_positive_hierarchical_edge"
    elif viable:
        priority = {
            "lifecycle_transition_continuation": 6,
            "creator_deployer_continuation": 5,
            "fomo_continuation": 4,
            "entity_flow_accumulation": 3,
            "elite_entity_continuation": 2,
            "hazard_continuation": 1,
        }
        lane, _ = max(viable, key=lambda item: priority.get(item[0], 0))
        req = hazard_requirements(risk_severity, risk_signature)
        base = 0.005 if lane in {"creator_deployer_continuation", "hazard_continuation"} or risk_severity >= 0.45 else 0.01
        fraction = base * float(req["bootstrap_size_multiplier"])
        basis = "hazard_conditioned_bootstrap"
    else:
        lane, fraction, basis = None, 0.0, "all_candidate_lanes_killed"
    if lane:
        fraction *= self._v5_regime_multiplier(regime)
        fraction *= max(0.30, 1.0 - 0.60 * risk_severity)
        if lane == "hazard_continuation":
            fraction = min(0.02, fraction)
        fraction = min(ROBINHOOD_V5_MAX_POSITION, max(0.0, fraction))
        available = max(0.0, ROBINHOOD_V5_MAX_OPEN_EXPOSURE - self._open_exposure())
        fraction = min(fraction, available)
        if fraction <= 0.0:
            lane, basis = None, "paper_exposure_cap_exhausted"
    trace = _TRACE.get()
    if trace is not None:
        trace["selected_lane"] = lane
        trace["position_fraction"] = fraction
        trace["selection_basis"] = basis
        trace["risk_signature"] = risk_signature
        trace["risk_severity"] = risk_severity
        trace["candidate_lanes"] = lanes
        _record_stage(
            self.store, surface="ROBINHOOD_CHAIN", candidate_id=trace["candidate_id"], release_commit=self.release_commit,
            stage="ingestion", status="complete", reason="forward_chain_candidate_reached_canonical_selection",
            payload={"token": trace["token"], "market": trace["market"]},
        )
        _record_stage(
            self.store, surface="ROBINHOOD_CHAIN", candidate_id=trace["candidate_id"], release_commit=self.release_commit,
            stage="candidate", status="complete", reason="entity_lifecycle_risk_preselection_passed",
            payload={"entity": entity, "role": role, "venue": venue, "lifecycle": lifecycle},
        )
        _record_stage(
            self.store, surface="ROBINHOOD_CHAIN", candidate_id=trace["candidate_id"], release_commit=self.release_commit,
            stage="context", status="complete", reason="canonical_v51_robinhood_context_evaluated",
            payload={"regime": regime, "flow_state": flow_state, "risk_signature": risk_signature, "risk_severity": risk_severity, "candidate_lanes": lanes},
        )
        if lane is None:
            _record_stage(
                self.store, surface="ROBINHOOD_CHAIN", candidate_id=trace["candidate_id"], release_commit=self.release_commit,
                stage="execution_evidence", status="not_requested", reason=basis,
            )
            _record_stage(
                self.store, surface="ROBINHOOD_CHAIN", candidate_id=trace["candidate_id"], release_commit=self.release_commit,
                stage="decision", status="complete", reason=basis, payload={"decision": "paper_reject"},
            )
            _record_stage(
                self.store, surface="ROBINHOOD_CHAIN", candidate_id=trace["candidate_id"], release_commit=self.release_commit,
                stage="position", status="not_opened", reason=basis,
            )
            _upsert_ledger(self, trace, decision="paper_reject", reason=basis)
    return lane, fraction, profiles


def _trial_ids(self: Any, token: str, market: str) -> list[int]:
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT id FROM robinhood_paper_trials WHERE release_commit=? AND token=? AND market=? ORDER BY id",
            (self.release_commit, token, market),
        ).fetchall()
    return [int(row["id"]) for row in rows]


async def _open_with_trace(self: Any, *, kind: str, subject: Any, current_block: int | None = None) -> None:
    if kind == "v3":
        token, market, venue = str(subject.token), str(subject.pool), str(subject.venue)
        lifecycle = "post_protection_v3" if venue == "PONS_V1_UNISWAP_V3" else "new_weth_pool"
        recent = subject.recent_swaps
        original = _ORIGINAL_OPEN_V3
    else:
        token, market, venue = str(subject.token), str(subject.curve), "PONS_V2_CURVE"
        lifecycle = "bonding_curve"
        recent = subject.recent_swaps
        original = _ORIGINAL_OPEN_V2
    if original is None:
        return
    candidate = _candidate_id(token, market, recent)
    trace = {
        "candidate_id": candidate, "token": token, "market": market, "venue": venue, "lifecycle": lifecycle,
        "selected_lane": None, "position_fraction": 0.0,
    }
    before = set(_trial_ids(self, token, market))
    token_ctx = _TRACE.set(trace)
    try:
        if kind == "v3":
            await original(self, subject, current_block=int(current_block or 0))
        else:
            await original(self, subject)
    finally:
        _TRACE.reset(token_ctx)
    after = set(_trial_ids(self, token, market))
    new_ids = sorted(after - before)
    if new_ids:
        trial_id = new_ids[-1]
        with self.store._lock:
            trial = self.store.db.execute("SELECT * FROM robinhood_paper_trials WHERE id=?", (trial_id,)).fetchone()
        td = dict(trial) if trial is not None else {}
        _record_stage(
            self.store, surface="ROBINHOOD_CHAIN", candidate_id=candidate, release_commit=self.release_commit,
            stage="execution_evidence", status="complete", reason="exact_entry_and_immediate_exit_round_trip_evidence_passed",
            payload={"round_trip_cost_fraction": td.get("entry_round_trip_cost_fraction")},
        )
        _record_stage(
            self.store, surface="ROBINHOOD_CHAIN", candidate_id=candidate, release_commit=self.release_commit,
            stage="decision", status="complete", reason=str(td.get("decision_reason") or "canonical_v51_paper_entry"),
            payload={"decision": "paper_enter", "lane": trace.get("selected_lane"), "position_fraction": td.get("position_fraction")},
        )
        _record_stage(
            self.store, surface="ROBINHOOD_CHAIN", candidate_id=candidate, release_commit=self.release_commit,
            stage="position", status="paper_position_authorized", reason="durable_robinhood_paper_trial_created", payload={"trial_id": trial_id},
        )
        _upsert_ledger(self, trace, decision="paper_enter", reason=str(td.get("decision_reason") or "canonical_v51_paper_entry"), trial_id=trial_id)
    elif trace.get("selected_lane"):
        reason = "exact_quote_unavailable_or_round_trip_cost_exceeded_context_ceiling"
        _record_stage(
            self.store, surface="ROBINHOOD_CHAIN", candidate_id=candidate, release_commit=self.release_commit,
            stage="execution_evidence", status="failed_closed", reason=reason,
        )
        _record_stage(
            self.store, surface="ROBINHOOD_CHAIN", candidate_id=candidate, release_commit=self.release_commit,
            stage="decision", status="complete", reason=reason, payload={"decision": "paper_reject", "selected_lane": trace.get("selected_lane")},
        )
        _record_stage(
            self.store, surface="ROBINHOOD_CHAIN", candidate_id=candidate, release_commit=self.release_commit,
            stage="position", status="not_opened", reason=reason,
        )
        _upsert_ledger(self, trace, decision="paper_reject", reason=reason)


async def _open_v3(self: Any, pool: Any, *, current_block: int) -> None:
    await _open_with_trace(self, kind="v3", subject=pool, current_block=current_block)


async def _open_v2(self: Any, curve: Any) -> None:
    await _open_with_trace(self, kind="v2", subject=curve)


def _learned_exit(self: Any, trial: dict[str, Any]) -> dict[str, Any]:
    trial_id = int(trial["id"])
    with self.store._lock:
        context = self.store.db.execute("SELECT lane FROM robinhood_v5_trial_context WHERE trial_id=? LIMIT 1", (trial_id,)).fetchone()
    lane = str(context["lane"]) if context else "legacy"
    with self.store._lock:
        closed = self.store.db.execute(
            "SELECT o.trial_id FROM robinhood_paper_outcomes o "
            "JOIN v51_economic_freeze_releases e ON e.release_commit=o.release_commit "
            "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id JOIN robinhood_paper_trials t ON t.id=o.trial_id "
            "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND c.lane=? AND t.venue=? AND t.lifecycle=? ORDER BY o.id",
            (ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID, lane, str(trial["venue"]), str(trial["lifecycle"])),
        ).fetchall()
    ids = [int(row["trial_id"]) for row in closed]
    if len(ids) < ROBINHOOD_V5_MIN_SAMPLES:
        return {"source": "bootstrap", "stop": STOP_LOSS_FRACTION, "harvest": HARVEST_FRACTION, "max_hold": MAX_HOLD_SECONDS}
    placeholders = ",".join("?" for _ in ids)
    with self.store._lock:
        marks = self.store.db.execute(
            f"SELECT trial_id,elapsed_seconds,net_return FROM robinhood_v5_marks WHERE trial_id IN ({placeholders}) ORDER BY id",
            tuple(ids),
        ).fetchall()
    grouped: dict[int, list[tuple[float, float]]] = {}
    for row in marks:
        grouped.setdefault(int(row["trial_id"]), []).append((float(row["elapsed_seconds"]), float(row["net_return"])))
    mfes: list[float] = []
    maes: list[float] = []
    time_to_mfe: list[float] = []
    for points in grouped.values():
        if not points:
            continue
        best = max(points, key=lambda item: item[1])
        mfes.append(best[1])
        maes.append(min(value for _, value in points))
        time_to_mfe.append(best[0])
    if len(mfes) < ROBINHOOD_V5_MIN_SAMPLES:
        return {"source": "bootstrap", "stop": STOP_LOSS_FRACTION, "harvest": HARVEST_FRACTION, "max_hold": MAX_HOLD_SECONDS}
    median_mfe = median(mfes)
    median_mae = median(maes)
    harvest = min(0.75, max(0.15, median_mfe * 0.70)) if median_mfe > 0 else HARVEST_FRACTION
    stop = min(-0.08, max(-0.30, median_mae * 1.20)) if median_mae < 0 else STOP_LOSS_FRACTION
    max_hold = min(20 * 60, max(120.0, median(time_to_mfe) * 1.50))
    return {"source": "frozen_epoch_forward_mfe_mae", "stop": stop, "harvest": harvest, "max_hold": max_hold}


def refresh_robinhood_candidate_learning(store: Any) -> dict[str, Any]:
    _ensure_ledger(store)
    with store._lock:
        rows = store.db.execute(
            "SELECT * FROM v51_robinhood_candidate_ledger WHERE economic_freeze_epoch=? AND trial_id IS NOT NULL ORDER BY observed_at",
            (ECONOMIC_FREEZE_EPOCH,),
        ).fetchall()
    settled = 0
    for row in rows:
        d = dict(row)
        trial_id = int(d["trial_id"])
        with store._lock:
            outcome = store.db.execute("SELECT * FROM robinhood_paper_outcomes WHERE trial_id=? LIMIT 1", (trial_id,)).fetchone()
        if outcome is None:
            _record_stage(store, surface="ROBINHOOD_CHAIN", candidate_id=d["candidate_id"], release_commit=d["release_commit"], stage="settlement", status="pending", reason="paper_position_not_yet_settled", payload={"trial_id": trial_id})
            continue
        od = dict(outcome)
        settled += 1
        _record_stage(store, surface="ROBINHOOD_CHAIN", candidate_id=d["candidate_id"], release_commit=d["release_commit"], stage="settlement", status="complete", reason=str(od.get("exit_reason") or "paper_settled"), payload={"trial_id": trial_id, "net_return": od.get("net_return")})
        _record_stage(store, surface="ROBINHOOD_CHAIN", candidate_id=d["candidate_id"], release_commit=d["release_commit"], stage="learning", status="complete", reason="outcome_available_to_frozen_epoch_learning", payload={"trial_id": trial_id, "net_return": od.get("net_return")})
    return {"candidate_count": len(rows), "settled_count": settled}


def install_v51_robinhood_consolidation() -> None:
    global _INSTALLED, _ORIGINAL_CHOOSE, _ORIGINAL_EXIT, _ORIGINAL_OPEN_V2, _ORIGINAL_OPEN_V3
    if _INSTALLED:
        return
    _ORIGINAL_CHOOSE = RobinhoodProfitMaximizerMixin._v5_choose_lane_fraction
    _ORIGINAL_EXIT = RobinhoodProfitMaximizerMixin._v5_learned_exit_policy
    _ORIGINAL_OPEN_V2 = RobinhoodProfitMaximizerMixin._maybe_open_v2
    _ORIGINAL_OPEN_V3 = RobinhoodProfitMaximizerMixin._maybe_open_v3
    RobinhoodProfitMaximizerMixin._v5_choose_lane_fraction = _choose
    RobinhoodProfitMaximizerMixin._v5_learned_exit_policy = _learned_exit
    RobinhoodProfitMaximizerMixin._maybe_open_v2 = _open_v2
    RobinhoodProfitMaximizerMixin._maybe_open_v3 = _open_v3
    _INSTALLED = True


__all__ = ["install_v51_robinhood_consolidation", "refresh_robinhood_candidate_learning"]
