from __future__ import annotations

import hashlib
import json
import math
import statistics
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any

from . import v52_learning_governance as governance
from .v52_continuous_evolution import PolicyOutcome, ProspectivePolicyTournament, TournamentDecision

VERSION = "v52-learning-governance-hardening-v2-exact-executable-evidence"
_INSTALLED = False
_BASE_PROMOTE: Any = None
_BASE_DEMOTE: Any = None


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stable_id(family: str, lane: str, reason: str) -> str:
    digest = hashlib.sha256(f"{family}\0{lane}\0{reason}".encode("utf-8")).hexdigest()[:12]
    return f"auto_{family}_{lane}_{digest}"


def _exact_schema(store: Any) -> None:
    governance._schema(store)
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_tournament_exact_evidence ("
            "challenger_id TEXT NOT NULL, stream_id TEXT NOT NULL, lane TEXT NOT NULL, observed_at TEXT NOT NULL, "
            "net_return REAL NOT NULL, drawdown REAL NOT NULL, evidence_ref TEXT NOT NULL, recorded_at TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "PRIMARY KEY(challenger_id,stream_id))"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v52_tournament_exact_evidence_time ON "
            "v52_tournament_exact_evidence(challenger_id,observed_at)"
        )


def generate_stable_challengers(store: Any) -> list[str]:
    governance.ensure_named_challengers(store)
    if not governance._table_exists(store, "v52_counterfactual_decisions"):
        return []
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    with store._lock:
        rows = store.db.execute(
            "SELECT lane,reason,COUNT(*) n,COALESCE(SUM(opportunity_cost_usd),0) missed,COALESCE(SUM(avoided_loss_usd),0) avoided "
            "FROM v52_counterfactual_decisions WHERE resolved_at IS NOT NULL AND resolved_at>=? "
            "GROUP BY lane,reason HAVING COUNT(*)>=8",
            (cutoff,),
        ).fetchall()
    created: list[str] = []
    for row in rows:
        reason = str(row["reason"] or "")
        family = governance._challenger_family(reason)
        if not family:
            continue
        missed = float(row["missed"] or 0.0)
        avoided = float(row["avoided"] or 0.0)
        if missed <= max(1.0, avoided * 1.25):
            continue
        lane = str(row["lane"] or "default")
        challenger_id = _stable_id(family, lane, reason)
        config = dict(governance.FIXED_CHALLENGERS[family])
        evidence = {
            "lane": lane,
            "reason": reason,
            "episodes": int(row["n"]),
            "missed_opportunity_usd": missed,
            "avoided_loss_usd": avoided,
            "generated_from_forward_counterfactuals": True,
            "stable_across_restarts": True,
        }
        with store._lock, store.db:
            cursor = store.db.execute(
                "INSERT OR IGNORE INTO v52_governed_challengers("
                "challenger_id,family,trigger_reason,config_json,created_at,status,analytical_only,paper_only,live_money_authority,evidence_json"
                ") VALUES (?,?,?,?,?,'active',1,1,0,?)",
                (
                    challenger_id,
                    family,
                    reason,
                    json.dumps(config, sort_keys=True),
                    _utcnow(),
                    json.dumps(evidence, sort_keys=True),
                ),
            )
        if cursor.rowcount == 1:
            created.append(challenger_id)
            governance._append(
                store,
                "v52_governed_challenger_created",
                {"challenger_id": challenger_id, **evidence, "config": config},
            )
    return created


def _strict_policy_stream_return(policy_id: str, family: str, stream: dict[str, Any]) -> tuple[float, bool]:
    """Keep heuristic challenger estimates analytical-only.

    The incumbent decision is an exact completed decision even when it correctly
    stays in cash. Challenger transforms are useful for prioritizing research, but
    are not executable policy simulations and therefore cannot satisfy promotion's
    execution-completion gate. Exact challenger outcomes must be written through
    ``record_exact_challenger_outcome``.
    """
    trade_return = float(stream["net_return"])
    fraction = max(0.0, float(stream.get("position_fraction") or 0.0))
    entered = bool(stream.get("entered"))
    if policy_id == governance.INCUMBENT_ID:
        return (trade_return * fraction if entered else 0.0), True

    reason_family = governance._challenger_family(str(stream.get("reason") or ""))
    if not entered and reason_family != family:
        return 0.0, False
    multiplier = 1.0
    if family == "aggressive_sizing":
        multiplier = 1.50
    elif family == "aggressive_continuation":
        multiplier = 1.25 if trade_return > 0 else 1.0
    elif family == "exit_capture":
        multiplier = 1.20 if trade_return > 0 else 0.90
    elif family == "wallet_acceleration":
        multiplier = 1.15 if trade_return > 0 else 1.0
    elif family == "chase_optimization":
        if not entered and float(stream.get("chase_fraction") or 0.0) > 0.45:
            return 0.0, False
    elif family == "concentration":
        multiplier = 1.75 if trade_return > 0 else 1.25
    exposure = fraction if fraction > 0 else 0.01
    estimate = max(-0.999, trade_return * exposure * multiplier)
    return estimate, False


def record_exact_challenger_outcome(
    store: Any,
    *,
    challenger_id: str,
    stream_id: str,
    lane: str,
    observed_at: str,
    net_return: float,
    drawdown: float,
    evidence_ref: str,
) -> bool:
    """Persist an exact paper-executable same-stream challenger outcome.

    This is the only path that upgrades a challenger tournament row to
    ``execution_complete=1``. It requires a paired incumbent stream row and an
    explicit immutable evidence reference so heuristic transforms can never be
    mistaken for promotion-quality proof.
    """
    _exact_schema(store)
    if not challenger_id or challenger_id == governance.INCUMBENT_ID:
        raise ValueError("challenger_id must identify a non-incumbent policy")
    if not stream_id or not lane or not evidence_ref:
        raise ValueError("stream_id, lane, and evidence_ref are required")
    if not math.isfinite(float(net_return)) or float(net_return) <= -1.0:
        raise ValueError("net_return must be finite and greater than -1")
    if not math.isfinite(float(drawdown)) or float(drawdown) < 0.0:
        raise ValueError("drawdown must be finite and non-negative")
    with store._lock:
        challenger = store.db.execute(
            "SELECT 1 FROM v52_governed_challengers WHERE challenger_id=? AND status IN ('active','promoted') LIMIT 1",
            (challenger_id,),
        ).fetchone()
        incumbent = store.db.execute(
            "SELECT lane,observed_at FROM v52_tournament_outcomes WHERE challenger_id=? AND stream_id=? LIMIT 1",
            (governance.INCUMBENT_ID, stream_id),
        ).fetchone()
    if challenger is None:
        raise ValueError("challenger is not active or promoted")
    if incumbent is None:
        raise ValueError("same-stream incumbent outcome is required before challenger proof")
    if str(incumbent["lane"]) != str(lane):
        raise ValueError("challenger lane does not match incumbent stream lane")
    with store._lock, store.db:
        prior = store.db.execute(
            "SELECT evidence_ref,net_return,drawdown FROM v52_tournament_exact_evidence WHERE challenger_id=? AND stream_id=?",
            (challenger_id, stream_id),
        ).fetchone()
        if prior is not None:
            same = (
                str(prior["evidence_ref"]) == evidence_ref
                and abs(float(prior["net_return"]) - float(net_return)) <= 1e-12
                and abs(float(prior["drawdown"]) - float(drawdown)) <= 1e-12
            )
            if not same:
                raise ValueError("conflicting exact challenger evidence for immutable stream")
            return False
        store.db.execute(
            "INSERT INTO v52_tournament_exact_evidence("
            "challenger_id,stream_id,lane,observed_at,net_return,drawdown,evidence_ref,recorded_at,paper_only,live_money_authority"
            ") VALUES (?,?,?,?,?,?,?,?,1,0)",
            (challenger_id, stream_id, lane, observed_at, float(net_return), float(drawdown), evidence_ref, _utcnow()),
        )
        store.db.execute(
            "INSERT INTO v52_tournament_outcomes("
            "challenger_id,stream_id,lane,observed_at,net_return,drawdown,execution_complete,same_stream,prospective"
            ") VALUES (?,?,?,?,?,?,1,1,1) "
            "ON CONFLICT(challenger_id,stream_id) DO UPDATE SET "
            "lane=excluded.lane,observed_at=excluded.observed_at,net_return=excluded.net_return,drawdown=excluded.drawdown,"
            "execution_complete=1,same_stream=1,prospective=1",
            (challenger_id, stream_id, lane, observed_at, float(net_return), float(drawdown)),
        )
    governance._append(
        store,
        "v52_tournament_exact_challenger_outcome",
        {
            "challenger_id": challenger_id,
            "stream_id": stream_id,
            "lane": lane,
            "observed_at": observed_at,
            "net_return": float(net_return),
            "drawdown": float(drawdown),
            "evidence_ref": evidence_ref,
            "same_stream": True,
            "exact_paper_executable": True,
        },
    )
    return True


def _posterior_since(store: Any, winner: str, common_start: str) -> dict[str, float]:
    with store._lock:
        rows = store.db.execute(
            "SELECT w.net_return winner_return,i.net_return incumbent_return FROM v52_tournament_outcomes w "
            "JOIN v52_tournament_outcomes i ON i.stream_id=w.stream_id AND i.challenger_id=? "
            "WHERE w.challenger_id=? AND w.observed_at>=? AND i.observed_at>=? "
            "AND w.execution_complete=1 AND i.execution_complete=1 AND w.same_stream=1 AND i.same_stream=1 "
            "AND w.prospective=1 AND i.prospective=1 ORDER BY w.id",
            (governance.INCUMBENT_ID, winner, common_start, common_start),
        ).fetchall()
    diffs = [float(row["winner_return"]) - float(row["incumbent_return"]) for row in rows]
    n = len(diffs)
    if not diffs:
        return {"episodes": 0, "mean": 0.0, "lower_90": -1.0, "probability_positive": 0.0}
    mean = statistics.fmean(diffs)
    variance = statistics.pvariance(diffs) if len(diffs) > 1 else 0.01
    prior_strength = 8.0
    prior_var = 0.01
    posterior_mean = n * mean / (n + prior_strength)
    posterior_var = (variance + prior_var) / max(1.0, n + prior_strength)
    se = math.sqrt(max(1e-12, posterior_var))
    probability = 0.5 * (1.0 + math.erf((posterior_mean / se) / math.sqrt(2.0)))
    return {
        "episodes": n,
        "mean": posterior_mean,
        "lower_90": posterior_mean - 1.645 * se,
        "probability_positive": probability,
    }


def evaluate_fresh_tournament(store: Any) -> tuple[TournamentDecision, dict[str, Any]]:
    active = governance._active_challengers(store)
    if not active:
        decision = TournamentDecision(
            winner=None,
            incumbent=governance.INCUMBENT_ID,
            eligible=False,
            improvement_ratio=None,
            blockers=("no_active_challengers",),
            scores=(),
        )
        return decision, {"episodes": 0, "mean": 0.0, "lower_90": -1.0, "probability_positive": 0.0}
    common_start = max(str(item["created_at"]) for item in active)
    tournament = ProspectivePolicyTournament(
        min_paired_episodes=governance.MIN_FORWARD_EPISODES,
        min_improvement_ratio=governance.MIN_IMPROVEMENT_RATIO,
    )
    with store._lock:
        rows = store.db.execute(
            "SELECT challenger_id,stream_id,net_return,drawdown,execution_complete FROM v52_tournament_outcomes "
            "WHERE observed_at>=? AND same_stream=1 AND prospective=1 ORDER BY id",
            (common_start,),
        ).fetchall()
    allowed = {governance.INCUMBENT_ID, *(str(item["challenger_id"]) for item in active)}
    for row in rows:
        if str(row["challenger_id"]) not in allowed:
            continue
        tournament.record(
            PolicyOutcome(
                policy_id=str(row["challenger_id"]),
                stream_id=str(row["stream_id"]),
                net_return=float(row["net_return"]),
                drawdown=float(row["drawdown"]),
                execution_complete=bool(row["execution_complete"]),
            )
        )
    decision = tournament.compare(
        governance.INCUMBENT_ID,
        [str(item["challenger_id"]) for item in active],
    )
    posterior = _posterior_since(store, decision.winner, common_start) if decision.winner else {
        "episodes": 0,
        "mean": 0.0,
        "lower_90": -1.0,
        "probability_positive": 0.0,
    }
    blockers = list(decision.blockers)
    if decision.winner:
        score = next((item for item in decision.scores if item.policy_id == decision.winner), None)
        if score is None or score.max_drawdown > governance.MAX_PROMOTION_DRAWDOWN:
            blockers.append("winner_drawdown_above_governed_maximum")
        if score is None or score.execution_completion_rate < 0.95:
            blockers.append("winner_exact_execution_completion_below_minimum")
        if posterior["episodes"] < governance.MIN_FORWARD_EPISODES:
            blockers.append("insufficient_exact_paired_forward_episodes")
        if posterior["probability_positive"] < governance.POSTERIOR_PROMOTION_PROBABILITY:
            blockers.append("posterior_probability_below_promotion_threshold")
        if posterior["lower_90"] <= 0.0:
            blockers.append("posterior_lower_advantage_not_positive")
    if blockers and decision.eligible:
        decision = TournamentDecision(
            winner=decision.winner,
            incumbent=decision.incumbent,
            eligible=False,
            improvement_ratio=decision.improvement_ratio,
            blockers=tuple(dict.fromkeys(blockers)),
            scores=decision.scores,
        )
    return decision, posterior


def promote_with_fresh_epoch(store: Any) -> dict[str, Any]:
    result = _BASE_PROMOTE(store)
    if result.get("action") != "promote":
        return result
    promoted = str((result.get("decision") or {}).get("winner") or "")
    now = _utcnow()
    with store._lock, store.db:
        store.db.execute("UPDATE v52_governed_challengers SET created_at=? WHERE status='active'", (now,))
    governance._append(
        store,
        "v52_tournament_forward_epoch_reset",
        {
            "promoted_challenger": promoted,
            "new_forward_epoch_started_at": now,
            "prior_evidence_reuse_for_next_promotion": False,
        },
    )
    return result


def demote_once_with_fresh_evidence(store: Any) -> dict[str, Any]:
    """Demote at most once and only from exact post-promotion paired evidence."""
    _exact_schema(store)
    with store._lock:
        latest = store.db.execute(
            "SELECT * FROM v52_strategy_governance_history ORDER BY id DESC LIMIT 1"
        ).fetchone()
    if latest is None or str(latest["action"]) != "promote":
        return {"action": "hold", "reason": "no_active_promotion"}
    promoted = dict(latest)
    challenger_id = str(promoted["challenger_id"])
    promoted_at = str(promoted["observed_at"])
    with store._lock:
        rows = store.db.execute(
            "SELECT c.stream_id,c.net_return challenger_return,i.net_return incumbent_return "
            "FROM v52_tournament_outcomes c JOIN v52_tournament_outcomes i "
            "ON i.stream_id=c.stream_id AND i.challenger_id=? "
            "WHERE c.challenger_id=? AND c.observed_at>=? AND i.observed_at>=? "
            "AND c.execution_complete=1 AND i.execution_complete=1 AND c.same_stream=1 AND i.same_stream=1 "
            "AND c.prospective=1 AND i.prospective=1 ORDER BY c.id",
            (governance.INCUMBENT_ID, challenger_id, promoted_at, promoted_at),
        ).fetchall()
    if len(rows) < governance.MIN_DEMOTION_EPISODES:
        return {
            "action": "hold",
            "reason": "insufficient_exact_post_promotion_forward_episodes",
            "episodes": len(rows),
        }
    incumbent_returns = [float(row["incumbent_return"]) for row in rows]
    challenger_returns = [float(row["challenger_return"]) for row in rows]
    incumbent_growth = math.exp(statistics.fmean(math.log1p(max(-0.999, value)) for value in incumbent_returns)) - 1.0
    promoted_growth = math.exp(statistics.fmean(math.log1p(max(-0.999, value)) for value in challenger_returns)) - 1.0
    if incumbent_growth > 0.0:
        ratio = promoted_growth / incumbent_growth
    else:
        ratio = 1.0 if promoted_growth >= incumbent_growth else 0.0
    posterior = _posterior_since(store, challenger_id, promoted_at)
    if ratio >= governance.DEMOTION_RATIO and posterior["probability_positive"] >= 0.50:
        return {
            "action": "hold",
            "reason": "promoted_policy_not_deteriorated",
            "ratio": ratio,
            "posterior": posterior,
        }
    rollback = json.loads(str(promoted["rollback_json"] or "{}"))
    if not rollback:
        return {"action": "hold", "reason": "rollback_empty"}
    evolution = governance.canonical_strategy_evolution()
    from_fp = evolution.current.fingerprint
    epochs = governance._apply_changes(
        rollback,
        rationale=f"automatic_forward_demotion:{challenger_id}:ratio={ratio}",
        evidence_refs=(
            f"exact_post_promotion_forward_episodes:{len(rows)}",
            f"posterior_probability:{posterior['probability_positive']:.6f}",
        ),
    )
    if not epochs:
        return {"action": "hold", "reason": "rollback_empty"}
    to_fp = governance.canonical_strategy_evolution().current.fingerprint
    now = _utcnow()
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO v52_strategy_governance_history("
            "action,challenger_id,observed_at,from_fingerprint,to_fingerprint,changes_json,rollback_json,evidence_json,paper_only,live_money_authority"
            ") VALUES ('demote',?,?,?,?,?,?,?,1,0)",
            (
                challenger_id,
                now,
                from_fp,
                to_fp,
                json.dumps(rollback, sort_keys=True),
                "{}",
                json.dumps({"ratio": ratio, "episodes": len(rows), "posterior": posterior}, sort_keys=True),
            ),
        )
        store.db.execute(
            "UPDATE v52_governed_challengers SET status='demoted' WHERE challenger_id=?",
            (challenger_id,),
        )
        store.db.execute("UPDATE v52_governed_challengers SET created_at=? WHERE status='active'", (now,))
    governance._append(
        store,
        "v52_strategy_automatic_demotion",
        {
            "challenger_id": challenger_id,
            "ratio": ratio,
            "posterior": posterior,
            "rollback": rollback,
            "exact_post_promotion_evidence": True,
        },
    )
    return {"action": "demote", "challenger_id": challenger_id, "ratio": ratio, "posterior": posterior}


def install_v52_learning_governance_hardening() -> None:
    global _INSTALLED, _BASE_PROMOTE, _BASE_DEMOTE
    if _INSTALLED:
        return
    _BASE_PROMOTE = governance.maybe_promote
    _BASE_DEMOTE = governance.maybe_demote
    governance.generate_challengers_from_missed_opportunities = generate_stable_challengers
    governance._policy_stream_return = _strict_policy_stream_return
    governance.evaluate_tournament = evaluate_fresh_tournament
    governance.maybe_promote = promote_with_fresh_epoch
    governance.maybe_demote = demote_once_with_fresh_evidence
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": VERSION,
        "installed": _INSTALLED,
        "stable_auto_challenger_ids": True,
        "fresh_same_stream_epoch_after_promotion": True,
        "old_forward_evidence_reuse_for_next_promotion": False,
        "heuristic_challenger_estimates_may_promote": False,
        "exact_executable_challenger_evidence_required_for_promotion": True,
        "exact_post_promotion_evidence_required_for_demotion": True,
        "repeat_demotion_of_same_promotion_prevented": True,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "VERSION",
    "demote_once_with_fresh_evidence",
    "evaluate_fresh_tournament",
    "generate_stable_challengers",
    "install_v52_learning_governance_hardening",
    "promote_with_fresh_epoch",
    "record_exact_challenger_outcome",
    "status",
]
