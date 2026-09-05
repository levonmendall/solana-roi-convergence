from __future__ import annotations

import asyncio
import json
import math
import statistics
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from . import robinhood_entity_quota_architecture as quota
from . import robinhood_entity_universe as universe
from . import robinhood_pumpfun_wallet_selection as selection
from . import robinhood_strategy_alignment_repair as alignment
from .robinhood_chain_core import ROBINHOOD_CHAIN_ID, _clean_address
from .wallet_intelligence import WalletPromotionPolicy


INTELLIGENCE_VERSION = "robinhood-pumpfun-wallet-intelligence-v1"
POLICY = WalletPromotionPolicy()
MAX_OBSERVATION_LAG_SECONDS = selection.PUMPFUN_POLICY.max_observation_lag_seconds
MAX_CHASE_FRACTION = selection.PUMPFUN_POLICY.max_chase_fraction
MIN_RISK_COVERAGE_RATE = selection.PUMPFUN_POLICY.min_risk_coverage_rate
MIN_FORWARD_CLOSED_EPISODES = POLICY.min_forward_episodes
MIN_SUPERIORITY_RATIO = POLICY.min_superiority_ratio
MAX_DRAWDOWN_DISADVANTAGE = POLICY.max_drawdown_disadvantage
MANIPULATION_TERMS = (
    "bundled_launch",
    "sniper_heavy",
    "common_funded_early_wallet_cluster",
    "scout_deployer_connection",
    "creator",
    "insider",
)

_ORIGINAL_BUILD: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_RANKINGS: Callable[..., list[dict[str, Any]]] | None = None
_ORIGINAL_DEMOTE: Callable[..., int] | None = None
_ORIGINAL_POLL: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_INSTALLED = False


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _ensure_schema(self: Any) -> None:
    if bool(getattr(self, "_roi_robinhood_wallet_intelligence_schema_ready", False)):
        return
    # The v1 selection schema is the prospective source ledger. Keep intelligence
    # additive so the existing audit trail remains intact.
    selection._ensure_schema(self)
    quota._ensure_schema(self)
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "CREATE TABLE IF NOT EXISTS robinhood_wallet_intelligence_forward ("
            "swap_id INTEGER PRIMARY KEY, actor TEXT NOT NULL, entity_id TEXT, token TEXT NOT NULL, market TEXT NOT NULL, "
            "side TEXT NOT NULL, token_amount_raw TEXT NOT NULL, wallet_quote_wei TEXT NOT NULL, fee_or_tax_wei TEXT, "
            "wallet_price_eth REAL, copyable_price_eth REAL, copyable_quote_wei REAL, chase_fraction REAL, "
            "observation_lag_ms REAL, copyable INTEGER NOT NULL, risk_complete INTEGER NOT NULL, "
            "risk_severity REAL, manipulation_flag INTEGER NOT NULL, side_wallet_flag INTEGER NOT NULL, "
            "observed_at TEXT NOT NULL, evaluated_at TEXT NOT NULL)"
        )
        self.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_robinhood_wallet_intelligence_actor "
            "ON robinhood_wallet_intelligence_forward(actor,swap_id)"
        )
        self.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_robinhood_wallet_intelligence_entity "
            "ON robinhood_wallet_intelligence_forward(entity_id,swap_id)"
        )
    setattr(self, "_roi_robinhood_wallet_intelligence_schema_ready", True)


def _entity_id_now(self: Any, actor: str) -> str | None:
    if not actor or not quota._ensure_schema(self):
        return None
    with self.store._lock:
        row = self.store.db.execute(
            "SELECT funding_anchor FROM robinhood_entity_proofs "
            "WHERE chain_id=? AND actor=? AND resolver_version=? ORDER BY resolved_at DESC LIMIT 1",
            (ROBINHOOD_CHAIN_ID, actor, quota.PROOF_VERSION),
        ).fetchone()
    if row is None:
        return None
    value = _clean_address(str(row["funding_anchor"]))
    return value or None


def _entity_id_as_of(self: Any, actor: str, observed_at: str) -> str | None:
    if not actor or not quota._ensure_schema(self):
        return None
    with self.store._lock:
        row = self.store.db.execute(
            "SELECT funding_anchor FROM robinhood_entity_proofs "
            "WHERE chain_id=? AND actor=? AND resolver_version=? AND resolved_at<=? "
            "ORDER BY resolved_at DESC LIMIT 1",
            (ROBINHOOD_CHAIN_ID, actor, quota.PROOF_VERSION, observed_at),
        ).fetchone()
    if row is None:
        return None
    value = _clean_address(str(row["funding_anchor"]))
    return value or None


def _shared_entity_as_of(self: Any, entity_id: str | None, observed_at: str) -> bool:
    if not entity_id:
        return True
    with self.store._lock:
        row = self.store.db.execute(
            "SELECT COUNT(DISTINCT actor) AS n FROM robinhood_entity_proofs "
            "WHERE chain_id=? AND funding_anchor=? AND resolver_version=? AND resolved_at<=?",
            (ROBINHOOD_CHAIN_ID, entity_id, quota.PROOF_VERSION, observed_at),
        ).fetchone()
    return int(row["n"] if row is not None else 0) > 1


def _risk_context_as_of(self: Any, actor: str, token: str, observed_at: str) -> tuple[bool, float | None, bool]:
    try:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT c.risk_severity,c.risk_json FROM robinhood_paper_trials t "
                "JOIN robinhood_v5_trial_context c ON c.trial_id=t.id "
                "WHERE t.trigger_actor=? AND t.token=? AND t.opened_at<=? "
                "ORDER BY t.id DESC LIMIT 1",
                (actor, token, observed_at),
            ).fetchone()
    except Exception:
        row = None
    if row is None:
        return False, None, True
    severity = _safe_float(row["risk_severity"])
    text = str(row["risk_json"] or "").lower()
    manipulation = bool((severity is not None and severity >= 0.70) or any(term in text for term in MANIPULATION_TERMS))
    return True, severity, manipulation


def _copyable_market_mark(self: Any, source: dict[str, Any]) -> tuple[float | None, float | None, float | None]:
    wallet_price = _safe_float(source.get("price_eth"))
    if wallet_price is None or wallet_price <= 0.0:
        return None, None, None
    with self.store._lock:
        row = self.store.db.execute(
            "SELECT price_eth,observed_at FROM robinhood_swaps WHERE market=? AND id>? "
            "AND price_eth IS NOT NULL ORDER BY id LIMIT 1",
            (str(source["market"]), int(source["id"])),
        ).fetchone()
    if row is None:
        return None, None, None
    copy_price = _safe_float(row["price_eth"])
    if copy_price is None or copy_price <= 0.0:
        return None, None, None
    try:
        source_at = datetime.fromisoformat(str(source["observed_at"]))
        copy_at = datetime.fromisoformat(str(row["observed_at"]))
        lag_ms = max(0.0, (copy_at - source_at).total_seconds() * 1000.0)
    except ValueError:
        return None, None, None
    side = str(source.get("side") or "").lower()
    chase = max(0.0, copy_price / wallet_price - 1.0) if side == "buy" else max(0.0, 1.0 - copy_price / wallet_price)
    return copy_price, chase, lag_ms


def _enrich_forward_observations(self: Any, limit: int = 250) -> int:
    _ensure_schema(self)
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT f.swap_id,s.id,s.actor,s.token,s.market,s.side,s.quote_amount_wei,s.token_amount_raw,"
            "s.price_eth,s.fee_or_tax_wei,s.observed_at FROM robinhood_wallet_selection_forward f "
            "JOIN robinhood_swaps s ON s.id=f.swap_id "
            "LEFT JOIN robinhood_wallet_intelligence_forward i ON i.swap_id=f.swap_id "
            "WHERE i.swap_id IS NULL ORDER BY f.swap_id LIMIT ?",
            (max(1, int(limit)),),
        ).fetchall()
    inserted = 0
    for raw in rows:
        source = dict(raw)
        actor = _clean_address(str(source.get("actor") or ""))
        if not actor:
            continue
        copy_price, chase, lag_ms = _copyable_market_mark(self, source)
        copyable = bool(
            copy_price is not None
            and chase is not None
            and lag_ms is not None
            and chase <= MAX_CHASE_FRACTION
            and lag_ms <= MAX_OBSERVATION_LAG_SECONDS * 1000.0
        )
        wallet_price = _safe_float(source.get("price_eth"))
        try:
            wallet_quote = float(int(str(source.get("quote_amount_wei") or "0")))
        except (TypeError, ValueError):
            wallet_quote = 0.0
        copy_quote = None
        if copy_price is not None and wallet_price is not None and wallet_price > 0.0 and wallet_quote > 0.0:
            copy_quote = wallet_quote * (copy_price / wallet_price)
        observed_at = str(source.get("observed_at") or _utcnow())
        entity_id = _entity_id_as_of(self, actor, observed_at)
        risk_complete, risk_severity, manipulation = _risk_context_as_of(
            self, actor, str(source.get("token") or ""), observed_at
        )
        side_wallet = _shared_entity_as_of(self, entity_id, observed_at)
        with self.store._lock, self.store.db:
            cursor = self.store.db.execute(
                "INSERT OR IGNORE INTO robinhood_wallet_intelligence_forward("
                "swap_id,actor,entity_id,token,market,side,token_amount_raw,wallet_quote_wei,fee_or_tax_wei,"
                "wallet_price_eth,copyable_price_eth,copyable_quote_wei,chase_fraction,observation_lag_ms,copyable,"
                "risk_complete,risk_severity,manipulation_flag,side_wallet_flag,observed_at,evaluated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    int(source["swap_id"]), actor, entity_id, str(source["token"]), str(source["market"]),
                    str(source["side"]), str(source["token_amount_raw"]), str(source["quote_amount_wei"]),
                    str(source.get("fee_or_tax_wei") or "0"), wallet_price, copy_price, copy_quote, chase, lag_ms,
                    1 if copyable else 0, 1 if risk_complete else 0, risk_severity,
                    1 if manipulation else 0, 1 if side_wallet else 0, observed_at, _utcnow(),
                ),
            )
        inserted += int(cursor.rowcount == 1)
    return inserted


def _realized_copyable_metrics(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    positions: dict[str, tuple[int, float]] = {}
    episode_returns: list[float] = []
    realized_cost = 0.0
    realized_pnl = 0.0
    gross_profit = 0.0
    gross_loss = 0.0
    tokens: set[str] = set()
    for row in rows:
        if not bool(row.get("copyable")):
            continue
        token = str(row.get("token") or "")
        side = str(row.get("side") or "").lower()
        if not token or side not in {"buy", "sell"}:
            continue
        try:
            units = int(str(row.get("token_amount_raw") or "0"))
            quote = float(row.get("copyable_quote_wei") or 0.0)
            fee = float(int(str(row.get("fee_or_tax_wei") or "0")))
        except (TypeError, ValueError):
            continue
        if units <= 0 or quote <= 0.0:
            continue
        tokens.add(token)
        held_units, held_cost = positions.get(token, (0, 0.0))
        if side == "buy":
            positions[token] = (held_units + units, held_cost + quote + max(0.0, fee))
            continue
        if held_units <= 0 or held_cost <= 0.0:
            continue
        closed_units = min(units, held_units)
        cost = held_cost * (closed_units / held_units)
        proceeds = max(0.0, quote * (closed_units / units) - max(0.0, fee))
        if cost <= 0.0:
            continue
        pnl = proceeds - cost
        value = pnl / cost
        episode_returns.append(value)
        realized_cost += cost
        realized_pnl += pnl
        if pnl >= 0.0:
            gross_profit += pnl
        else:
            gross_loss += -pnl
        remaining_units = held_units - closed_units
        remaining_cost = max(0.0, held_cost - cost)
        if remaining_units <= 0:
            positions.pop(token, None)
        else:
            positions[token] = (remaining_units, remaining_cost)
    roc = realized_pnl / realized_cost if realized_cost > 0.0 else 0.0
    if episode_returns:
        logs = [math.log(max(1e-9, 1.0 + value)) for value in episode_returns]
        geometric = math.exp(statistics.mean(logs)) - 1.0
        hit_rate = sum(value > 0.0 for value in episode_returns) / len(episode_returns)
    else:
        geometric = 0.0
        hit_rate = 0.0
    if gross_loss > 0.0:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > 0.0:
        profit_factor = 999.0
    else:
        profit_factor = 0.0
    equity = 1.0
    peak = 1.0
    drawdown = 0.0
    for value in episode_returns:
        equity *= max(1e-9, 1.0 + value)
        peak = max(peak, equity)
        drawdown = max(drawdown, (peak - equity) / peak if peak > 0.0 else 0.0)
    return {
        "closed_episodes": len(episode_returns),
        "distinct_tokens": len(tokens),
        "copyable_return_on_capital": roc,
        "geometric_growth": geometric,
        "profit_factor": profit_factor,
        "hit_rate": hit_rate,
        "max_drawdown": min(1.0, max(0.0, drawdown)),
    }


def _risk_adjusted_score(profile: dict[str, Any]) -> float:
    if int(profile.get("closed_episodes") or 0) <= 0:
        return float("-inf")
    return (
        float(profile.get("copyable_return_on_capital") or 0.0)
        * max(float(profile.get("profit_factor") or 0.0), 0.0)
        * max(float(profile.get("copyability_rate") or 0.0), 0.0)
        * max(0.0, 1.0 - float(profile.get("manipulation_risk") or 0.0))
        * max(0.0, 1.0 - float(profile.get("side_wallet_risk") or 0.0))
        / (1.0 + max(float(profile.get("max_drawdown") or 0.0), 0.0))
    )


def _evidence_blockers(profile: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    if not profile.get("entity_id"):
        blockers.append("economic_entity_unresolved")
    if int(profile.get("closed_episodes") or 0) < POLICY.min_forward_episodes:
        blockers.append("insufficient_forward_episodes")
    if float(profile.get("copyable_return_on_capital") or 0.0) <= POLICY.min_copyable_return_on_capital:
        blockers.append("copyable_return_not_positive")
    if float(profile.get("geometric_growth") or 0.0) <= POLICY.min_geometric_growth:
        blockers.append("geometric_growth_not_positive")
    if float(profile.get("profit_factor") or 0.0) <= POLICY.min_profit_factor:
        blockers.append("profit_factor_not_above_one")
    if float(profile.get("copyability_rate") or 0.0) < POLICY.min_copyability_rate:
        blockers.append("copyability_rate_below_minimum")
    if float(profile.get("manipulation_risk") or 0.0) > POLICY.max_manipulation_risk:
        blockers.append("manipulation_risk_too_high")
    if float(profile.get("side_wallet_risk") or 0.0) > POLICY.max_side_wallet_risk:
        blockers.append("side_wallet_risk_too_high")
    if float(profile.get("max_drawdown") or 0.0) > POLICY.max_drawdown:
        blockers.append("drawdown_too_high")
    return blockers


def _candidate_profile(self: Any, actor: str) -> dict[str, Any]:
    _ensure_schema(self)
    with self.store._lock:
        rows = [
            dict(row)
            for row in self.store.db.execute(
                "SELECT * FROM robinhood_wallet_intelligence_forward WHERE actor=? ORDER BY swap_id",
                (actor,),
            ).fetchall()
        ]
    metrics = _realized_copyable_metrics(rows)
    total = len(rows)
    copyable_count = sum(1 for row in rows if bool(row.get("copyable")))
    buys = [row for row in rows if str(row.get("side") or "").lower() == "buy"]
    complete_buys = [row for row in buys if bool(row.get("risk_complete"))]
    risk_coverage = len(complete_buys) / len(buys) if buys else 0.0
    if risk_coverage < MIN_RISK_COVERAGE_RATE:
        manipulation_risk = 1.0
        side_wallet_risk = 1.0
    else:
        manipulation_risk = (
            sum(1 for row in complete_buys if bool(row.get("manipulation_flag"))) / len(complete_buys)
            if complete_buys else 1.0
        )
        side_wallet_risk = (
            sum(1 for row in complete_buys if bool(row.get("side_wallet_flag"))) / len(complete_buys)
            if complete_buys else 1.0
        )
    lags = [float(row["observation_lag_ms"]) for row in rows if _safe_float(row.get("observation_lag_ms")) is not None]
    signals = sorted(
        {
            f"{row['token']}|{row['market']}"
            for row in rows
            if str(row.get("side") or "").lower() == "buy" and bool(row.get("copyable"))
        }
    )
    profile = {
        "actor": actor,
        "entity_id": _entity_id_now(self, actor),
        **metrics,
        "forward_observations": total,
        "copyable_observations": copyable_count,
        "copyability_rate": copyable_count / total if total else 0.0,
        "risk_coverage_rate": risk_coverage,
        "manipulation_risk": manipulation_risk,
        "side_wallet_risk": side_wallet_risk,
        "median_entry_lag_ms": statistics.median(lags) if lags else None,
        "copyable_signal_keys": signals,
    }
    blockers = _evidence_blockers(profile)
    profile["risk_adjusted_copyable_score"] = _risk_adjusted_score(profile)
    profile["promotion_evidence_eligible"] = not blockers
    profile["promotion_blockers"] = blockers
    return profile


def _overlap(left: Iterable[str], right: Iterable[str]) -> float:
    a, b = set(left), set(right)
    if not a or not b:
        return 0.0
    union = a | b
    return len(a & b) / len(union) if union else 0.0


def _research_rankings_with_intelligence(self: Any) -> list[dict[str, Any]]:
    original = list(_ORIGINAL_RANKINGS(self)) if _ORIGINAL_RANKINGS is not None else []
    try:
        with self.store._lock:
            prior = {
                str(row["entity"])
                for row in self.store.db.execute("SELECT entity FROM robinhood_entity_universe").fetchall()
            }
    except Exception:
        prior = set()
    rows: list[dict[str, Any]] = []
    for raw in original:
        row = dict(raw)
        actor = _clean_address(str(row.get("entity") or row.get("actor") or ""))
        if not actor:
            rows.append(row)
            continue
        profile = _candidate_profile(self, actor)
        resolved = str(profile.get("entity_id") or "")
        row.update(
            {
                "actor": actor,
                "resolved_entity_id": resolved or None,
                "economic_entity_resolved": bool(resolved),
                "forward_closed_episodes": int(profile["closed_episodes"]),
                "copyable_return_on_capital_pct": float(profile["copyable_return_on_capital"]) * 100.0,
                "forward_geometric_growth_pct": float(profile["geometric_growth"]) * 100.0,
                "forward_profit_factor": float(profile["profit_factor"]),
                "forward_hit_rate_pct": float(profile["hit_rate"]) * 100.0,
                "forward_max_drawdown_pct": float(profile["max_drawdown"]) * 100.0,
                "copyability_rate_pct": float(profile["copyability_rate"]) * 100.0,
                "risk_coverage_rate_pct": float(profile["risk_coverage_rate"]) * 100.0,
                "manipulation_risk_pct": float(profile["manipulation_risk"]) * 100.0,
                "side_wallet_risk_pct": float(profile["side_wallet_risk"]) * 100.0,
                "median_entry_lag_ms": profile["median_entry_lag_ms"],
                "risk_adjusted_copyable_score": profile["risk_adjusted_copyable_score"],
                "promotion_evidence_eligible": bool(profile["promotion_evidence_eligible"]),
                "promotion_blockers": list(profile["promotion_blockers"]),
                "copyable_signal_keys": list(profile["copyable_signal_keys"]),
                "previously_selected": actor in prior or (resolved and resolved in prior),
                # An unresolved raw address remains observable in the research ledger,
                # but it cannot consume one of the scarce global entity slots.
                "priority_research_challenger": bool(row.get("priority_research_challenger")) and bool(resolved),
                "historical_or_mark_evidence_has_paper_promotion_authority": False,
                "provider_requests_added": 0,
            }
        )
        rows.append(row)
    rows.sort(
        key=lambda row: (
            1 if row.get("promotion_evidence_eligible") else 0,
            float(row.get("risk_adjusted_copyable_score") or -999.0),
            1 if row.get("priority_research_challenger") else 0,
            float(row.get("historical_return_on_capital_pct") or -999.0),
        ),
        reverse=True,
    )
    for index, row in enumerate(rows, start=1):
        row["research_rank"] = index
    return rows[: alignment.RESEARCH_TOP_N]


def _greedy_diverse(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    remaining = list(rows)
    selected: list[dict[str, Any]] = []
    while remaining and len(selected) < max(0, int(limit)):
        def value(row: dict[str, Any]) -> float:
            base = _safe_float(row.get("risk_adjusted_copyable_score"))
            if base is None or not math.isfinite(base):
                base = max(-10.0, min(10.0, float(row.get("historical_return_on_capital_pct") or 0.0) / 100.0))
            redundancy = max(
                (_overlap(row.get("copyable_signal_keys") or [], other.get("copyable_signal_keys") or []) for other in selected),
                default=0.0,
            )
            return base - 2.0 * redundancy
        best = max(remaining, key=value)
        selected.append(best)
        remaining.remove(best)
    return selected


def build_intelligence_entity_universe(
    evidence_rows: Iterable[dict[str, Any]],
    research_rows: Iterable[dict[str, Any]] = (),
    *,
    capacity: int = universe.TRACKING_CAPACITY_LIMIT,
) -> dict[str, Any]:
    if _ORIGINAL_BUILD is None:
        raise RuntimeError("Robinhood Pump.fun intelligence parity is not installed")
    evidence = [dict(row) for row in evidence_rows]
    research = [dict(row) for row in research_rows]
    base = dict(_ORIGINAL_BUILD(evidence, research, capacity=capacity))
    capacity = max(1, int(capacity))

    # Collapse raw actors to one representative per resolved economic entity before
    # consuming capacity. Unknown identities remain in research status only.
    by_entity: dict[str, dict[str, Any]] = {}
    for row in research:
        entity_id = str(row.get("resolved_entity_id") or "")
        if not entity_id or not bool(row.get("priority_research_challenger")):
            continue
        current = by_entity.get(entity_id)
        if current is None:
            by_entity[entity_id] = row
            continue
        old_score = _safe_float(current.get("risk_adjusted_copyable_score")) or float("-inf")
        new_score = _safe_float(row.get("risk_adjusted_copyable_score")) or float("-inf")
        if (bool(row.get("promotion_evidence_eligible")), new_score) > (
            bool(current.get("promotion_evidence_eligible")), old_score
        ):
            by_entity[entity_id] = row

    candidates = list(by_entity.values())
    qualified = [row for row in candidates if bool(row.get("promotion_evidence_eligible"))]
    prospective = [
        row for row in candidates
        if not bool(row.get("promotion_evidence_eligible"))
        and int(row.get("forward_closed_episodes") or 0) < MIN_FORWARD_CLOSED_EPISODES
        and (bool(row.get("seed_label")) or bool(row.get("historical_screen_passed")))
    ]

    challenger_target = min(len(prospective), min(capacity, max(universe.MIN_CHALLENGER_SLOTS, len(prospective))))
    incumbent_slots = max(0, capacity - challenger_target)
    prior = [row for row in qualified if bool(row.get("previously_selected"))]
    prior = _greedy_diverse(prior, incumbent_slots)
    chosen_incumbents = list(prior)
    new_qualified = [row for row in qualified if row not in prior]
    if len(chosen_incumbents) < incumbent_slots:
        chosen_incumbents.extend(
            _greedy_diverse(new_qualified, incumbent_slots - len(chosen_incumbents))
        )
    else:
        # Once qualified capacity is full, a challenger replaces the weakest proven
        # teacher only when it clears Pump.fun's superiority ratio and drawdown rule.
        for challenger in _greedy_diverse(new_qualified, len(new_qualified)):
            if not chosen_incumbents:
                break
            weakest = min(
                chosen_incumbents,
                key=lambda row: float(row.get("risk_adjusted_copyable_score") or float("-inf")),
            )
            weak_score = float(weakest.get("risk_adjusted_copyable_score") or 0.0)
            cand_score = float(challenger.get("risk_adjusted_copyable_score") or float("-inf"))
            weak_dd = float(weakest.get("forward_max_drawdown_pct") or 0.0) / 100.0
            cand_dd = float(challenger.get("forward_max_drawdown_pct") or 0.0) / 100.0
            ratio_ok = cand_score > 0.0 if weak_score <= 0.0 else cand_score / weak_score >= MIN_SUPERIORITY_RATIO
            drawdown_ok = cand_dd <= weak_dd + MAX_DRAWDOWN_DISADVANTAGE
            if ratio_ok and drawdown_ok:
                chosen_incumbents.remove(weakest)
                chosen_incumbents.append(challenger)

    remaining = capacity - len(chosen_incumbents)
    chosen_research = _greedy_diverse(prospective, remaining)
    chosen = chosen_incumbents + chosen_research

    selected_actors = [str(row.get("actor") or row.get("entity") or "") for row in chosen]
    selected_entities = [str(row.get("resolved_entity_id") or "") for row in chosen]
    current_roles: list[dict[str, Any]] = []
    blockers: list[dict[str, Any]] = []
    for rank, row in enumerate(chosen, start=1):
        actor = str(row.get("actor") or row.get("entity") or "")
        entity_id = str(row.get("resolved_entity_id") or "")
        eligible = bool(row.get("promotion_evidence_eligible"))
        current_roles.append(
            {
                "rank": rank,
                "entity": actor,
                "economic_entity_id": entity_id,
                "representative_actor": actor,
                "selection_state": "qualified_copyable_teacher" if eligible else "prospective_research_tracking",
                "forward_closed_episodes": int(row.get("forward_closed_episodes") or 0),
                "copyable_return_on_capital_pct": row.get("copyable_return_on_capital_pct"),
                "copyability_rate_pct": row.get("copyability_rate_pct"),
                "risk_adjusted_copyable_score": row.get("risk_adjusted_copyable_score"),
                "promotion_evidence_eligible": eligible,
                "tracking_selection_independently_authorizes_entry": False,
            }
        )
        if not eligible:
            blockers.append({"entity": actor, "economic_entity_id": entity_id, "blockers": list(row.get("promotion_blockers") or [])})

    base.update(
        {
            "universe_version": "robinhood-entity-universe-v3-pumpfun-intelligence-parity",
            "selection_strategy": INTELLIGENCE_VERSION,
            "tracking_capacity_limit": capacity,
            "tracking_capacity_is_ceiling_not_target": True,
            "capacity_fill_required": False,
            "empty_slots_allowed": True,
            "quality_over_full_roster": True,
            "high_priority_entities": selected_actors,
            "selected_economic_entities": selected_entities,
            "high_priority_entity_count": len(selected_actors),
            "unfilled_capacity": max(0, capacity - len(selected_actors)),
            "current_role_for_high_priority_entity": current_roles,
            "candidate_promotion_blockers": blockers,
            "economic_entity_deduplication_before_slot": True,
            "unresolved_raw_actor_can_consume_slot": False,
            "signal_redundancy_penalty_enabled": True,
            "qualified_teacher_count": len(chosen_incumbents),
            "prospective_research_count": len(chosen_research),
            "pumpfun_forward_intelligence_parity": {
                "min_forward_closed_episodes": MIN_FORWARD_CLOSED_EPISODES,
                "min_copyable_return_on_capital": POLICY.min_copyable_return_on_capital,
                "min_geometric_growth": POLICY.min_geometric_growth,
                "min_profit_factor": POLICY.min_profit_factor,
                "min_copyability_rate": POLICY.min_copyability_rate,
                "max_manipulation_risk": POLICY.max_manipulation_risk,
                "max_side_wallet_risk": POLICY.max_side_wallet_risk,
                "max_drawdown": POLICY.max_drawdown,
                "min_superiority_ratio": MIN_SUPERIORITY_RATIO,
                "max_drawdown_disadvantage": MAX_DRAWDOWN_DISADVANTAGE,
                "max_chase_fraction": MAX_CHASE_FRACTION,
                "max_observation_lag_seconds": MAX_OBSERVATION_LAG_SECONDS,
                "min_risk_coverage_rate": MIN_RISK_COVERAGE_RATE,
                "copyability_transport": "canonical_next_market_observation_proxy_research_only",
                "history_or_research_mark_has_paper_authority": False,
            },
            "new_wallet_specific_provider_polling_added": False,
            "provider_requests_added": 0,
            "paper_only": True,
            "live_money_authority": False,
        }
    )
    return base


def _demote_with_copyable_intelligence(self: Any) -> int:
    _ensure_schema(self)
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT actor FROM robinhood_wallet_selection_candidates WHERE state IN ('seed_tracking','tracking')"
        ).fetchall()
    demoted = 0
    for row in rows:
        actor = str(row["actor"])
        profile = _candidate_profile(self, actor)
        if int(profile.get("closed_episodes") or 0) < MIN_FORWARD_CLOSED_EPISODES:
            continue
        blockers = list(profile.get("promotion_blockers") or [])
        if not blockers:
            continue
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE robinhood_wallet_selection_candidates SET state='forward_rejected',last_error=? WHERE actor=?",
                ("Pump.fun-equivalent copyable forward evidence failed: " + ",".join(blockers), actor),
            )
        demoted += 1
    return demoted


async def _poll_once_with_intelligence(self: Any) -> None:
    if _ORIGINAL_POLL is None:
        raise RuntimeError("Robinhood wallet intelligence parity is not installed")
    await _ORIGINAL_POLL(self)
    try:
        enriched = _enrich_forward_observations(self)
        # The selection wrapper calls the demotion function dynamically; the installer
        # replaces it with _demote_with_copyable_intelligence, so this second call is
        # only needed to evaluate rows enriched during this exact post-poll pass.
        demoted = _demote_with_copyable_intelligence(self)
        setattr(
            self,
            "_roi_robinhood_wallet_intelligence_last_cycle",
            {"forward_observations_enriched": enriched, "candidates_demoted": demoted, "provider_requests_added": 0},
        )
        setattr(self, "_roi_robinhood_wallet_intelligence_last_error", None)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        setattr(
            self,
            "_roi_robinhood_wallet_intelligence_last_error",
            f"{type(exc).__name__}: copyable wallet intelligence unavailable",
        )


def _intelligence_status(self: Any) -> dict[str, Any]:
    _ensure_schema(self)
    with self.store._lock:
        total = int(self.store.db.execute("SELECT COUNT(*) FROM robinhood_wallet_intelligence_forward").fetchone()[0])
        copyable = int(self.store.db.execute("SELECT COUNT(*) FROM robinhood_wallet_intelligence_forward WHERE copyable=1").fetchone()[0])
        resolved = int(self.store.db.execute("SELECT COUNT(*) FROM robinhood_wallet_intelligence_forward WHERE entity_id IS NOT NULL").fetchone()[0])
        candidates = [
            str(row["actor"])
            for row in self.store.db.execute(
                "SELECT actor FROM robinhood_wallet_selection_candidates WHERE state IN ('seed_tracking','tracking','forward_rejected') "
                "ORDER BY seed_priority IS NULL,seed_priority,actor LIMIT 30"
            ).fetchall()
        ]
    sample = [_candidate_profile(self, actor) for actor in candidates]
    return {
        "intelligence_version": INTELLIGENCE_VERSION,
        "selection_model": "pumpfun_copyable_forward_wallet_intelligence",
        "forward_observations": total,
        "copyable_forward_observations": copyable,
        "copyable_forward_fraction": copyable / total if total else 0.0,
        "entity_resolved_forward_observations": resolved,
        "candidate_sample": sample,
        "promotion_policy": {
            "min_forward_closed_episodes": MIN_FORWARD_CLOSED_EPISODES,
            "min_copyable_return_on_capital": POLICY.min_copyable_return_on_capital,
            "min_geometric_growth": POLICY.min_geometric_growth,
            "min_profit_factor": POLICY.min_profit_factor,
            "min_copyability_rate": POLICY.min_copyability_rate,
            "max_manipulation_risk": POLICY.max_manipulation_risk,
            "max_side_wallet_risk": POLICY.max_side_wallet_risk,
            "max_drawdown": POLICY.max_drawdown,
            "min_superiority_ratio": MIN_SUPERIORITY_RATIO,
            "max_drawdown_disadvantage": MAX_DRAWDOWN_DISADVANTAGE,
        },
        "economic_entity_deduplication_before_slot": True,
        "signal_redundancy_penalty_enabled": True,
        "copyability_transport": "canonical_next_market_observation_proxy_research_only",
        "wallet_specific_provider_requests_added": 0,
        "historical_or_research_evidence_has_paper_promotion_authority": False,
        "paper_only": True,
        "live_money_authority": False,
    }


def _status_with_intelligence(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("Robinhood wallet intelligence status wrapper is not installed")
    payload = dict(_ORIGINAL_STATUS(self))
    try:
        payload["pumpfun_wallet_intelligence_parity"] = _intelligence_status(self)
    except Exception as exc:
        payload["pumpfun_wallet_intelligence_parity"] = {
            "intelligence_version": INTELLIGENCE_VERSION,
            "failed_closed": True,
            "error": f"{type(exc).__name__}: wallet intelligence status unavailable",
            "wallet_specific_provider_requests_added": 0,
            "paper_only": True,
            "live_money_authority": False,
        }
    return payload


def install_robinhood_pumpfun_wallet_intelligence(plane_cls: type[Any]) -> None:
    global _ORIGINAL_BUILD, _ORIGINAL_RANKINGS, _ORIGINAL_DEMOTE, _ORIGINAL_POLL, _ORIGINAL_STATUS, _INSTALLED
    if _INSTALLED:
        return
    _ORIGINAL_BUILD = universe.build_entity_universe
    universe.build_entity_universe = build_intelligence_entity_universe

    _ORIGINAL_RANKINGS = alignment._research_rankings
    alignment._research_rankings = _research_rankings_with_intelligence

    _ORIGINAL_DEMOTE = selection._demote_mature_negative_candidates
    selection._demote_mature_negative_candidates = _demote_with_copyable_intelligence

    _ORIGINAL_POLL = plane_cls._poll_once
    setattr(_poll_once_with_intelligence, "_roi_robinhood_pumpfun_wallet_intelligence", True)
    plane_cls._poll_once = _poll_once_with_intelligence  # type: ignore[method-assign]

    _ORIGINAL_STATUS = plane_cls.status
    setattr(_status_with_intelligence, "_roi_robinhood_pumpfun_wallet_intelligence", True)
    plane_cls.status = _status_with_intelligence  # type: ignore[method-assign]

    setattr(plane_cls, "_roi_robinhood_pumpfun_wallet_intelligence_installed", True)
    setattr(plane_cls, "_roi_robinhood_pumpfun_wallet_intelligence_version", INTELLIGENCE_VERSION)
    _INSTALLED = True


__all__ = [
    "INTELLIGENCE_VERSION",
    "POLICY",
    "MIN_FORWARD_CLOSED_EPISODES",
    "MIN_SUPERIORITY_RATIO",
    "MAX_DRAWDOWN_DISADVANTAGE",
    "MAX_OBSERVATION_LAG_SECONDS",
    "MAX_CHASE_FRACTION",
    "MIN_RISK_COVERAGE_RATE",
    "_realized_copyable_metrics",
    "_risk_adjusted_score",
    "_evidence_blockers",
    "_overlap",
    "build_intelligence_entity_universe",
    "install_robinhood_pumpfun_wallet_intelligence",
]
