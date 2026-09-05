from __future__ import annotations

import asyncio
import hashlib
import math
import statistics
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable

from . import robinhood_entity_universe as universe
from . import robinhood_strategy_alignment_repair as alignment
from .robinhood_chain_core import KNOWN_NON_ACTORS, _clean_address
from .wallet_discovery import WalletDiscoveryPolicy


SELECTION_VERSION = "robinhood-pumpfun-wallet-selection-v1"
PUMPFUN_POLICY = WalletDiscoveryPolicy()
TRACKING_CAPACITY_LIMIT = universe.TRACKING_CAPACITY_LIMIT
MIN_CHALLENGER_SLOTS = universe.MIN_CHALLENGER_SLOTS
MIN_MATURE_FORWARD_SAMPLES = universe.MIN_MATURE_FORWARD_SAMPLES
BROAD_SAMPLE_MODULUS = PUMPFUN_POLICY.broad_sample_modulus
BROAD_SCAN_LIMIT = PUMPFUN_POLICY.broad_scan_limit
HISTORICAL_MAX_SWAPS = PUMPFUN_POLICY.historical_max_signatures
HISTORICAL_MIN_CLOSED_EPISODES = PUMPFUN_POLICY.historical_min_closed_episodes
HISTORICAL_MIN_DISTINCT_TOKENS = PUMPFUN_POLICY.historical_min_distinct_tokens
HISTORICAL_MIN_RETURN_ON_CAPITAL = PUMPFUN_POLICY.historical_min_return_on_capital
HISTORICAL_MIN_PROFIT_FACTOR = PUMPFUN_POLICY.historical_min_profit_factor
RESCREEN_HOURS = PUMPFUN_POLICY.rescreen_hours
FORWARD_MARK_SECONDS = 120

# These are research hypotheses discovered from public Robinhood Chain wallet
# analysis on 2026-09-05. They mirror Pump.fun's named seed entities: enrollment
# starts a fresh prospective clock, but the seed label is never promotion authority
# and mature negative forward evidence removes the wallet from the high-priority set.
CURATED_RESEARCH_SEEDS: tuple[dict[str, Any], ...] = (
    {
        "address": "0xf29f0a86420399f662577b68c48137d510084d96",
        "label": "public_high_roi_candidate_f29f",
        "research_priority": 1,
    },
    {
        "address": "0x5638484ba2d2f1d1d35020572b0aa439a9869192",
        "label": "public_high_roi_candidate_5638",
        "research_priority": 2,
    },
    {
        "address": "0xeee29d1a6fa5873065ad8789c6e15231b48318a0",
        "label": "public_high_roi_candidate_eee2",
        "research_priority": 3,
    },
    {
        "address": "0xdd35a714941a6777a835d21dc1b37fd474b59f4a",
        "label": "public_high_roi_candidate_dd35",
        "research_priority": 4,
    },
    {
        "address": "0xfb0d8b94027c5109ae89c5f08b025cc598cf6f49",
        "label": "public_high_roi_candidate_fb0d",
        "research_priority": 5,
    },
    {
        "address": "0x9963597a9246b39b13330992f571f8378c18c262",
        "label": "public_high_roi_candidate_9963",
        "research_priority": 6,
    },
)
SEED_BY_ADDRESS = {str(row["address"]): dict(row) for row in CURATED_RESEARCH_SEEDS}

_ORIGINAL_BUILD: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_RANKINGS: Callable[..., list[dict[str, Any]]] | None = None
_ORIGINAL_POLL: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_INSTALLED = False


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _sample(value: str, modulus: int = BROAD_SAMPLE_MODULUS) -> bool:
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % max(1, int(modulus)) == 0


def _ensure_schema(self: Any) -> None:
    if bool(getattr(self, "_roi_pumpfun_wallet_selection_schema_ready", False)):
        return
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "CREATE TABLE IF NOT EXISTS robinhood_wallet_selection_state ("
            "id INTEGER PRIMARY KEY CHECK(id=1), last_swap_id INTEGER NOT NULL DEFAULT 0, "
            "last_cycle_at TEXT, last_discovery_at TEXT, last_screen_at TEXT, last_forward_at TEXT, last_error TEXT)"
        )
        self.store.db.execute(
            "INSERT OR IGNORE INTO robinhood_wallet_selection_state(id,last_swap_id) VALUES (1,0)"
        )
        self.store.db.execute(
            "CREATE TABLE IF NOT EXISTS robinhood_wallet_selection_candidates ("
            "actor TEXT PRIMARY KEY, first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL, "
            "state TEXT NOT NULL, seed_label TEXT, seed_priority INTEGER, broad_sample_count INTEGER NOT NULL DEFAULT 0, "
            "distinct_token_count INTEGER NOT NULL DEFAULT 0, historical_closed_episodes INTEGER NOT NULL DEFAULT 0, "
            "historical_return_on_capital REAL NOT NULL DEFAULT 0, historical_profit_factor REAL NOT NULL DEFAULT 0, "
            "historical_hit_rate REAL NOT NULL DEFAULT 0, historical_max_drawdown REAL NOT NULL DEFAULT 0, "
            "forward_started_swap_id INTEGER, forward_started_at TEXT, next_screen_at TEXT, last_error TEXT)"
        )
        self.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_robinhood_wallet_selection_candidate_state "
            "ON robinhood_wallet_selection_candidates(state,next_screen_at)"
        )
        self.store.db.execute(
            "CREATE TABLE IF NOT EXISTS robinhood_wallet_selection_broad_samples ("
            "swap_id INTEGER PRIMARY KEY, actor TEXT NOT NULL, token TEXT NOT NULL, observed_at TEXT NOT NULL)"
        )
        self.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_robinhood_wallet_selection_broad_actor "
            "ON robinhood_wallet_selection_broad_samples(actor,swap_id)"
        )
        self.store.db.execute(
            "CREATE TABLE IF NOT EXISTS robinhood_wallet_selection_forward ("
            "swap_id INTEGER PRIMARY KEY, actor TEXT NOT NULL, token TEXT NOT NULL, market TEXT NOT NULL, side TEXT NOT NULL, "
            "price_eth REAL, observed_at TEXT NOT NULL, mark_price_eth REAL, mark_return REAL, marked_at TEXT)"
        )
        self.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_robinhood_wallet_selection_forward_actor "
            "ON robinhood_wallet_selection_forward(actor,swap_id)"
        )
        max_row = self.store.db.execute("SELECT COALESCE(MAX(id),0) FROM robinhood_swaps").fetchone()
        high_water = int(max_row[0] if max_row is not None else 0)
        now = _utcnow().isoformat()
        for seed in CURATED_RESEARCH_SEEDS:
            actor = _clean_address(str(seed["address"]))
            if not actor:
                continue
            self.store.db.execute(
                "INSERT OR IGNORE INTO robinhood_wallet_selection_candidates("
                "actor,first_seen_at,last_seen_at,state,seed_label,seed_priority,forward_started_swap_id,forward_started_at) "
                "VALUES (?,?,?,'seed_tracking',?,?,?,?)",
                (actor, now, now, str(seed["label"]), int(seed["research_priority"]), high_water, now),
            )
    setattr(self, "_roi_pumpfun_wallet_selection_schema_ready", True)


def _discover_from_ingested_swaps(self: Any) -> int:
    _ensure_schema(self)
    with self.store._lock, self.store.db:
        state = self.store.db.execute(
            "SELECT last_swap_id FROM robinhood_wallet_selection_state WHERE id=1"
        ).fetchone()
        cursor = int(state[0] if state is not None else 0)
        rows = self.store.db.execute(
            "SELECT id,tx_hash,log_index,actor,token,observed_at FROM robinhood_swaps "
            "WHERE id>? ORDER BY id LIMIT ?",
            (cursor, max(1, int(BROAD_SCAN_LIMIT))),
        ).fetchall()
        if not rows:
            return 0
        inserted = 0
        newest = max(int(row["id"]) for row in rows)
        for row in rows:
            actor = _clean_address(str(row["actor"] or ""))
            if not actor or actor in KNOWN_NON_ACTORS:
                continue
            sample_key = f"{row['tx_hash']}:{row['log_index']}"
            if not _sample(sample_key):
                continue
            cursor2 = self.store.db.execute(
                "INSERT OR IGNORE INTO robinhood_wallet_selection_broad_samples(swap_id,actor,token,observed_at) "
                "VALUES (?,?,?,?)",
                (int(row["id"]), actor, str(row["token"]), str(row["observed_at"])),
            )
            if cursor2.rowcount != 1:
                continue
            counts = self.store.db.execute(
                "SELECT COUNT(*) AS n,COUNT(DISTINCT token) AS tokens FROM robinhood_wallet_selection_broad_samples WHERE actor=?",
                (actor,),
            ).fetchone()
            existing = self.store.db.execute(
                "SELECT state FROM robinhood_wallet_selection_candidates WHERE actor=?",
                (actor,),
            ).fetchone()
            if existing is None:
                self.store.db.execute(
                    "INSERT INTO robinhood_wallet_selection_candidates("
                    "actor,first_seen_at,last_seen_at,state,broad_sample_count,distinct_token_count,next_screen_at) "
                    "VALUES (?,?,?,'discovered',?,?,?)",
                    (
                        actor,
                        str(row["observed_at"]),
                        str(row["observed_at"]),
                        int(counts["n"]),
                        int(counts["tokens"]),
                        str(row["observed_at"]),
                    ),
                )
            else:
                self.store.db.execute(
                    "UPDATE robinhood_wallet_selection_candidates SET last_seen_at=?,broad_sample_count=?,distinct_token_count=? "
                    "WHERE actor=?",
                    (str(row["observed_at"]), int(counts["n"]), int(counts["tokens"]), actor),
                )
            inserted += 1
        self.store.db.execute(
            "UPDATE robinhood_wallet_selection_state SET last_swap_id=?,last_discovery_at=? WHERE id=1",
            (newest, _utcnow().isoformat()),
        )
    return inserted


def _realized_metrics(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    positions: dict[str, tuple[int, float]] = {}
    episode_returns: list[float] = []
    realized_cost = 0.0
    realized_pnl = 0.0
    gross_profit = 0.0
    gross_loss = 0.0
    tokens: set[str] = set()
    for row in rows:
        token = str(row.get("token") or "")
        side = str(row.get("side") or "").lower()
        if not token or side not in {"buy", "sell"}:
            continue
        try:
            units = int(str(row.get("token_amount_raw") or "0"))
            quote = int(str(row.get("quote_amount_wei") or "0"))
        except (TypeError, ValueError):
            continue
        if units <= 0 or quote <= 0:
            continue
        tokens.add(token)
        held_units, held_cost = positions.get(token, (0, 0.0))
        if side == "buy":
            positions[token] = (held_units + units, held_cost + float(quote))
            continue
        if held_units <= 0 or held_cost <= 0.0:
            continue
        closed_units = min(units, held_units)
        closed_fraction_of_position = closed_units / held_units
        closed_fraction_of_sale = closed_units / units
        closed_cost = held_cost * closed_fraction_of_position
        proceeds = float(quote) * closed_fraction_of_sale
        if closed_cost <= 0.0:
            continue
        pnl = proceeds - closed_cost
        episode_returns.append(pnl / closed_cost)
        realized_cost += closed_cost
        realized_pnl += pnl
        if pnl >= 0.0:
            gross_profit += pnl
        else:
            gross_loss += -pnl
        remaining_units = held_units - closed_units
        remaining_cost = max(0.0, held_cost - closed_cost)
        if remaining_units <= 0:
            positions.pop(token, None)
        else:
            positions[token] = (remaining_units, remaining_cost)
    if realized_cost > 0.0:
        roc = realized_pnl / realized_cost
    else:
        roc = 0.0
    if gross_loss > 0.0:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > 0.0:
        profit_factor = 999.0
    else:
        profit_factor = 0.0
    hit_rate = sum(value > 0.0 for value in episode_returns) / len(episode_returns) if episode_returns else 0.0
    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    for value in episode_returns:
        equity *= max(1e-9, 1.0 + value)
        peak = max(peak, equity)
        if peak > 0.0:
            max_drawdown = max(max_drawdown, (peak - equity) / peak)
    return {
        "closed_episodes": len(episode_returns),
        "distinct_tokens": len(tokens),
        "return_on_capital": roc,
        "profit_factor": profit_factor,
        "hit_rate": hit_rate,
        "max_drawdown": min(1.0, max(0.0, max_drawdown)),
    }


def _screen_one_candidate(self: Any) -> bool:
    _ensure_schema(self)
    now = _utcnow()
    with self.store._lock:
        row = self.store.db.execute(
            "SELECT actor FROM robinhood_wallet_selection_candidates "
            "WHERE state IN ('discovered','screen_rejected') AND (next_screen_at IS NULL OR next_screen_at<=?) "
            "ORDER BY broad_sample_count DESC,distinct_token_count DESC,last_seen_at DESC LIMIT 1",
            (now.isoformat(),),
        ).fetchone()
    if row is None:
        return False
    actor = str(row["actor"])
    try:
        with self.store._lock:
            raw_rows = self.store.db.execute(
                "SELECT token,side,quote_amount_wei,token_amount_raw FROM ("
                "SELECT id,token,side,quote_amount_wei,token_amount_raw FROM robinhood_swaps "
                "WHERE actor=? ORDER BY id DESC LIMIT ?) ORDER BY id",
                (actor, max(1, int(HISTORICAL_MAX_SWAPS))),
            ).fetchall()
            max_row = self.store.db.execute("SELECT COALESCE(MAX(id),0) FROM robinhood_swaps").fetchone()
        metrics = _realized_metrics(dict(item) for item in raw_rows)
        passed = bool(
            int(metrics["closed_episodes"]) >= HISTORICAL_MIN_CLOSED_EPISODES
            and int(metrics["distinct_tokens"]) >= HISTORICAL_MIN_DISTINCT_TOKENS
            and float(metrics["return_on_capital"]) > HISTORICAL_MIN_RETURN_ON_CAPITAL
            and float(metrics["profit_factor"]) > HISTORICAL_MIN_PROFIT_FACTOR
        )
        state = "tracking" if passed else "screen_rejected"
        next_screen = None if passed else now + timedelta(hours=RESCREEN_HOURS)
        high_water = int(max_row[0] if max_row is not None else 0)
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE robinhood_wallet_selection_candidates SET state=?,historical_closed_episodes=?,"
                "historical_return_on_capital=?,historical_profit_factor=?,historical_hit_rate=?,historical_max_drawdown=?,"
                "forward_started_swap_id=?,forward_started_at=?,next_screen_at=?,last_error=NULL WHERE actor=?",
                (
                    state,
                    int(metrics["closed_episodes"]),
                    float(metrics["return_on_capital"]),
                    float(metrics["profit_factor"]),
                    float(metrics["hit_rate"]),
                    float(metrics["max_drawdown"]),
                    high_water if passed else None,
                    now.isoformat() if passed else None,
                    next_screen.isoformat() if next_screen else None,
                    actor,
                ),
            )
            self.store.db.execute(
                "UPDATE robinhood_wallet_selection_state SET last_screen_at=? WHERE id=1",
                (now.isoformat(),),
            )
        return passed
    except Exception as exc:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE robinhood_wallet_selection_candidates SET next_screen_at=?,last_error=? WHERE actor=?",
                ((now + timedelta(hours=1)).isoformat(), f"{type(exc).__name__}: historical screen failed", actor),
            )
        return False


def _record_forward_observations(self: Any) -> int:
    _ensure_schema(self)
    with self.store._lock, self.store.db:
        candidates = self.store.db.execute(
            "SELECT actor,forward_started_swap_id FROM robinhood_wallet_selection_candidates "
            "WHERE state IN ('seed_tracking','tracking') AND forward_started_swap_id IS NOT NULL"
        ).fetchall()
        inserted = 0
        for candidate in candidates:
            actor = str(candidate["actor"])
            start = int(candidate["forward_started_swap_id"] or 0)
            rows = self.store.db.execute(
                "SELECT id,actor,token,market,side,price_eth,observed_at FROM robinhood_swaps "
                "WHERE actor=? AND id>? ORDER BY id LIMIT 100",
                (actor, start),
            ).fetchall()
            for row in rows:
                cursor = self.store.db.execute(
                    "INSERT OR IGNORE INTO robinhood_wallet_selection_forward("
                    "swap_id,actor,token,market,side,price_eth,observed_at) VALUES (?,?,?,?,?,?,?)",
                    (
                        int(row["id"]),
                        actor,
                        str(row["token"]),
                        str(row["market"]),
                        str(row["side"]),
                        float(row["price_eth"]) if row["price_eth"] is not None else None,
                        str(row["observed_at"]),
                    ),
                )
                inserted += int(cursor.rowcount == 1)
        self.store.db.execute(
            "UPDATE robinhood_wallet_selection_state SET last_forward_at=? WHERE id=1",
            (_utcnow().isoformat(),),
        )
    return inserted


def _mark_forward_observations(self: Any) -> int:
    cutoff = (_utcnow() - timedelta(seconds=FORWARD_MARK_SECONDS)).isoformat()
    with self.store._lock, self.store.db:
        rows = self.store.db.execute(
            "SELECT swap_id,market,price_eth,observed_at FROM robinhood_wallet_selection_forward "
            "WHERE side='buy' AND marked_at IS NULL AND price_eth IS NOT NULL AND observed_at<=? "
            "ORDER BY swap_id LIMIT 200",
            (cutoff,),
        ).fetchall()
        marked = 0
        for row in rows:
            entry = float(row["price_eth"] or 0.0)
            if entry <= 0.0:
                continue
            try:
                target = (datetime.fromisoformat(str(row["observed_at"])) + timedelta(seconds=FORWARD_MARK_SECONDS)).isoformat()
            except ValueError:
                continue
            mark = self.store.db.execute(
                "SELECT price_eth,observed_at FROM robinhood_swaps WHERE market=? AND id>? "
                "AND price_eth IS NOT NULL AND observed_at>=? ORDER BY id LIMIT 1",
                (str(row["market"]), int(row["swap_id"]), target),
            ).fetchone()
            if mark is None:
                continue
            price = float(mark["price_eth"] or 0.0)
            if price <= 0.0:
                continue
            self.store.db.execute(
                "UPDATE robinhood_wallet_selection_forward SET mark_price_eth=?,mark_return=?,marked_at=? WHERE swap_id=?",
                (price, price / entry - 1.0, str(mark["observed_at"]), int(row["swap_id"])),
            )
            marked += 1
    return marked


def _candidate_forward_profile(self: Any, actor: str) -> dict[str, Any]:
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT token,mark_return FROM robinhood_wallet_selection_forward "
            "WHERE actor=? AND side='buy' AND mark_return IS NOT NULL ORDER BY swap_id",
            (actor,),
        ).fetchall()
    values = [float(row["mark_return"]) for row in rows]
    distinct_tokens = len({str(row["token"]) for row in rows})
    if not values:
        return {
            "sample_count": 0,
            "distinct_tokens": 0,
            "median_return": None,
            "trimmed_mean": None,
            "positive_rate": None,
            "score": None,
        }
    median_return = statistics.median(values)
    trimmed = statistics.mean(sorted(values)[:-1]) if len(values) > 1 else values[0]
    positive_rate = sum(value > 0.0 for value in values) / len(values)
    logs = [math.log(max(1e-9, 1.0 + value)) for value in values]
    confidence = min(1.0, math.sqrt(len(values) / 30.0))
    return {
        "sample_count": len(values),
        "distinct_tokens": distinct_tokens,
        "median_return": median_return,
        "trimmed_mean": trimmed,
        "positive_rate": positive_rate,
        "score": statistics.mean(logs) * confidence,
    }


def _demote_mature_negative_candidates(self: Any) -> int:
    _ensure_schema(self)
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT actor,state FROM robinhood_wallet_selection_candidates WHERE state IN ('seed_tracking','tracking')"
        ).fetchall()
    demoted = 0
    for row in rows:
        actor = str(row["actor"])
        profile = _candidate_forward_profile(self, actor)
        score = _safe_float(profile.get("score"))
        if int(profile["sample_count"]) < MIN_MATURE_FORWARD_SAMPLES or score is None or score > 0.0:
            continue
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE robinhood_wallet_selection_candidates SET state='forward_rejected',last_error=? WHERE actor=?",
                ("mature forward geometric value nonpositive; quality slot released", actor),
            )
        demoted += 1
    return demoted


def _candidate_research_rows(self: Any) -> list[dict[str, Any]]:
    _ensure_schema(self)
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT * FROM robinhood_wallet_selection_candidates WHERE state IN ('seed_tracking','tracking')"
        ).fetchall()
    result: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        actor = str(row["actor"])
        forward = _candidate_forward_profile(self, actor)
        trimmed = _safe_float(forward.get("trimmed_mean"))
        median = _safe_float(forward.get("median_return"))
        priority = bool(row["state"] in {"seed_tracking", "tracking"})
        result.append(
            {
                "entity": actor,
                "actor": actor,
                "identity_state": "raw_actor_research_until_decision_time_entity_resolution",
                "candidate_source": "curated_public_roi_seed" if row.get("seed_label") else "pumpfun_equivalent_local_historical_screen",
                "seed_label": row.get("seed_label"),
                "seed_priority": row.get("seed_priority"),
                "historical_screen_passed": row["state"] == "tracking",
                "historical_closed_episodes": int(row.get("historical_closed_episodes") or 0),
                "historical_return_on_capital_pct": float(row.get("historical_return_on_capital") or 0.0) * 100.0,
                "historical_profit_factor": float(row.get("historical_profit_factor") or 0.0),
                "historical_hit_rate_pct": float(row.get("historical_hit_rate") or 0.0) * 100.0,
                "historical_max_drawdown_pct": float(row.get("historical_max_drawdown") or 0.0) * 100.0,
                "distinct_tokens": max(int(row.get("distinct_token_count") or 0), int(forward.get("distinct_tokens") or 0)),
                "marked_buy_observations": int(forward.get("sample_count") or 0),
                "median_120s_followthrough_pct": median * 100.0 if median is not None else None,
                "trimmed_mean_120s_followthrough_ex_best_1_pct": trimmed * 100.0 if trimmed is not None else None,
                "positive_120s_followthrough_rate_pct": (
                    float(forward["positive_rate"]) * 100.0 if forward.get("positive_rate") is not None else None
                ),
                "forward_geometric_score": forward.get("score"),
                "priority_research_challenger": priority,
                "research_only": True,
                "historical_or_mark_evidence_has_paper_promotion_authority": False,
                "ranking_can_bypass_exact_executable_quote": False,
                "ranking_can_bypass_forward_paper_maturity": False,
                "provider_requests_added": 0,
            }
        )
    result.sort(
        key=lambda item: (
            1 if item.get("forward_geometric_score") is not None and float(item["forward_geometric_score"]) > 0 else 0,
            float(item.get("forward_geometric_score") or -999.0),
            1 if item.get("historical_screen_passed") else 0,
            float(item.get("historical_return_on_capital_pct") or 0.0),
            -int(item.get("seed_priority") or 999999),
        ),
        reverse=True,
    )
    return result


def _research_rankings_with_quality_candidates(self: Any) -> list[dict[str, Any]]:
    original = list(_ORIGINAL_RANKINGS(self)) if _ORIGINAL_RANKINGS is not None else []
    local = _candidate_research_rows(self)
    merged: dict[str, dict[str, Any]] = {}
    for row in original + local:
        entity = str(row.get("entity") or "")
        if not entity:
            continue
        current = merged.get(entity)
        if current is None:
            merged[entity] = dict(row)
            continue
        current_priority = bool(current.get("priority_research_challenger"))
        new_priority = bool(row.get("priority_research_challenger"))
        if new_priority and not current_priority:
            merged[entity] = dict(row)
            continue
        new_score = _safe_float(row.get("forward_geometric_score"))
        old_score = _safe_float(current.get("forward_geometric_score"))
        if new_score is not None and (old_score is None or new_score > old_score):
            merged[entity] = dict(row)
    rows = list(merged.values())
    rows.sort(
        key=lambda row: (
            1 if row.get("priority_research_challenger") else 0,
            1 if _safe_float(row.get("forward_geometric_score")) is not None and float(row["forward_geometric_score"]) > 0 else 0,
            _safe_float(row.get("forward_geometric_score")) or -999.0,
            _safe_float(row.get("trimmed_mean_120s_followthrough_ex_best_1_pct")) or -999.0,
            _safe_float(row.get("historical_return_on_capital_pct")) or -999.0,
            int(row.get("marked_buy_observations") or 0),
            int(row.get("distinct_tokens") or 0),
        ),
        reverse=True,
    )
    for index, row in enumerate(rows, start=1):
        row["research_rank"] = index
    return rows[: alignment.RESEARCH_TOP_N]


def build_quality_entity_universe(
    evidence_rows: Iterable[dict[str, Any]],
    research_rows: Iterable[dict[str, Any]] = (),
    *,
    capacity: int = TRACKING_CAPACITY_LIMIT,
) -> dict[str, Any]:
    if _ORIGINAL_BUILD is None:
        raise RuntimeError("Pump.fun-equivalent Robinhood selector is not installed")
    evidence = [dict(row) for row in evidence_rows]
    research = [dict(row) for row in research_rows]
    base = dict(_ORIGINAL_BUILD(evidence, research, capacity=capacity))
    scores = universe._role_scores(evidence)
    capacity = max(1, int(capacity))

    positive_forward: list[str] = []
    for entity in scores:
        mature, score, _samples = universe._forward_priority(entity, scores)
        if mature and math.isfinite(score) and score > 0.0:
            positive_forward.append(entity)
    positive_forward.sort(key=lambda entity: universe._forward_priority(entity, scores), reverse=True)

    research_by_entity: dict[str, dict[str, Any]] = {}
    for row in research:
        entity = str(row.get("entity") or "")
        if not entity or not bool(row.get("priority_research_challenger")):
            continue
        mature, score, samples = universe._forward_priority(entity, scores)
        if mature and math.isfinite(score) and score <= 0.0:
            continue
        item = dict(row)
        item["_forward_mature"] = mature
        item["_forward_score"] = score
        item["_forward_samples"] = samples
        research_by_entity[entity] = item

    challengers = list(research_by_entity.values())
    challengers.sort(
        key=lambda row: (
            1 if row.get("candidate_source") == "pumpfun_equivalent_local_historical_screen" else 0,
            1 if row.get("historical_screen_passed") else 0,
            _safe_float(row.get("historical_return_on_capital_pct")) or -999.0,
            _safe_float(row.get("trimmed_mean_120s_followthrough_ex_best_1_pct")) or -999.0,
            -int(row.get("seed_priority") or 999999),
        ),
        reverse=True,
    )

    selected: list[str] = []
    for row in challengers[: min(MIN_CHALLENGER_SLOTS, capacity)]:
        entity = str(row["entity"])
        if entity not in selected:
            selected.append(entity)

    remaining_entities: list[str] = []
    for entity in positive_forward:
        if entity not in selected:
            remaining_entities.append(entity)
    for row in challengers:
        entity = str(row["entity"])
        if entity not in selected and entity not in remaining_entities:
            remaining_entities.append(entity)

    def remaining_key(entity: str) -> tuple[float, ...]:
        mature, score, samples = universe._forward_priority(entity, scores)
        research_row = research_by_entity.get(entity, {})
        return (
            float(mature),
            score if math.isfinite(score) else -999.0,
            float(samples),
            1.0 if research_row.get("historical_screen_passed") else 0.0,
            _safe_float(research_row.get("historical_return_on_capital_pct")) or -999.0,
            -float(int(research_row.get("seed_priority") or 999999)),
        )

    remaining_entities.sort(key=remaining_key, reverse=True)
    for entity in remaining_entities:
        if len(selected) >= capacity:
            break
        selected.append(entity)

    base_roles = {
        str(row.get("entity") or ""): dict(row)
        for row in list(base.get("current_role_for_high_priority_entity") or [])
    }
    current_roles: list[dict[str, Any]] = []
    blockers: list[dict[str, Any]] = []
    for rank, entity in enumerate(selected, start=1):
        row = dict(base_roles.get(entity, {}))
        research_row = research_by_entity.get(entity, {})
        mature, score, samples = universe._forward_priority(entity, scores)
        row.update(
            {
                "rank": rank,
                "entity": entity,
                "forward_sample_count": samples,
                "selection_state": (
                    "mature_positive_forward_incumbent"
                    if mature and math.isfinite(score) and score > 0.0
                    else "prospective_research_tracking"
                ),
                "candidate_source": research_row.get("candidate_source"),
                "seed_label": research_row.get("seed_label"),
                "historical_screen_passed": bool(research_row.get("historical_screen_passed")),
                "tracking_selection_independently_authorizes_entry": False,
            }
        )
        current_roles.append(row)
        reasons: list[str] = []
        if not mature:
            reasons.append("prospective_forward_evidence_still_maturing")
        if not (mature and math.isfinite(score) and score > 0.0):
            reasons.append("tracking_only_no_paper_promotion_authority")
        if reasons:
            blockers.append({"entity": entity, "blockers": reasons})

    selected_seed_rows = [
        {
            "name": str(research_by_entity[entity].get("seed_label") or "public_roi_research_seed"),
            "address": entity,
            "initial_roles": [],
        }
        for entity in selected
        if entity in research_by_entity and research_by_entity[entity].get("seed_label")
    ]
    selected_challengers = [
        entity
        for entity in selected
        if entity in research_by_entity and not research_by_entity[entity].get("seed_label")
    ]

    base.update(
        {
            "universe_version": "robinhood-entity-universe-v2-pumpfun-quality-selection",
            "selection_strategy": SELECTION_VERSION,
            "tracking_capacity_limit": capacity,
            "tracking_capacity_is_ceiling_not_target": True,
            "capacity_fill_required": False,
            "empty_slots_allowed": True,
            "quality_over_full_roster": True,
            "minimum_wallet_count_required_for_operation": 0,
            "high_priority_entities": selected,
            "high_priority_entity_count": len(selected),
            "unfilled_capacity": max(0, capacity - len(selected)),
            "active_seed_entities": selected_seed_rows,
            "discovered_challengers": selected_challengers,
            "current_role_for_high_priority_entity": current_roles,
            "candidate_promotion_blockers": blockers,
            "pumpfun_selection_parity": {
                "broad_sample_modulus": BROAD_SAMPLE_MODULUS,
                "broad_scan_limit": BROAD_SCAN_LIMIT,
                "historical_max_swaps": HISTORICAL_MAX_SWAPS,
                "historical_min_closed_episodes": HISTORICAL_MIN_CLOSED_EPISODES,
                "historical_min_distinct_tokens": HISTORICAL_MIN_DISTINCT_TOKENS,
                "historical_min_return_on_capital": HISTORICAL_MIN_RETURN_ON_CAPITAL,
                "historical_min_profit_factor": HISTORICAL_MIN_PROFIT_FACTOR,
                "historical_success_only_grants_research_bandwidth": True,
                "forward_epoch_begins_after_historical_screen": True,
                "mature_negative_forward_candidate_releases_slot": True,
                "challenger_floor_when_quality_candidates_exist": MIN_CHALLENGER_SLOTS,
                "challengers_can_replace_incumbents": True,
            },
            "named_seed_is_permanent_whitelist": False,
            "historical_or_mark_evidence_has_paper_promotion_authority": False,
            "new_wallet_specific_provider_polling_added": False,
            "provider_requests_added": 0,
        }
    )
    return base


def _selection_status(self: Any) -> dict[str, Any]:
    _ensure_schema(self)
    with self.store._lock:
        counts = self.store.db.execute(
            "SELECT state,COUNT(*) AS n FROM robinhood_wallet_selection_candidates GROUP BY state ORDER BY state"
        ).fetchall()
        state = self.store.db.execute("SELECT * FROM robinhood_wallet_selection_state WHERE id=1").fetchone()
        candidates = self.store.db.execute(
            "SELECT actor,state,seed_label,seed_priority,broad_sample_count,distinct_token_count,historical_closed_episodes,"
            "historical_return_on_capital,historical_profit_factor,forward_started_at,last_error "
            "FROM robinhood_wallet_selection_candidates WHERE state IN ('seed_tracking','tracking','forward_rejected') "
            "ORDER BY CASE state WHEN 'tracking' THEN 0 WHEN 'seed_tracking' THEN 1 ELSE 2 END,"
            "historical_return_on_capital DESC,seed_priority ASC LIMIT 30"
        ).fetchall()
    rows: list[dict[str, Any]] = []
    for raw in candidates:
        row = dict(raw)
        profile = _candidate_forward_profile(self, str(row["actor"]))
        rows.append(
            {
                **row,
                "historical_return_on_capital_pct": float(row["historical_return_on_capital"] or 0.0) * 100.0,
                "forward_sample_count": int(profile["sample_count"]),
                "forward_geometric_score": profile["score"],
            }
        )
    return {
        "selection_version": SELECTION_VERSION,
        "selection_model": "pumpfun_broad_discovery_then_historical_quality_screen_then_fresh_forward_tracking",
        "capacity_limit": TRACKING_CAPACITY_LIMIT,
        "capacity_is_ceiling_not_target": True,
        "empty_slots_allowed": True,
        "quality_over_slot_fill": True,
        "candidate_state_counts": {str(row["state"]): int(row["n"]) for row in counts},
        "curated_public_roi_seed_count": len(CURATED_RESEARCH_SEEDS),
        "curated_public_roi_seeds": [dict(row) for row in CURATED_RESEARCH_SEEDS],
        "candidate_sample": rows,
        "last_cycle": dict(state) if state is not None else None,
        "pumpfun_historical_gate": {
            "min_closed_episodes": HISTORICAL_MIN_CLOSED_EPISODES,
            "min_distinct_tokens": HISTORICAL_MIN_DISTINCT_TOKENS,
            "min_return_on_capital_pct": HISTORICAL_MIN_RETURN_ON_CAPITAL * 100.0,
            "min_profit_factor": HISTORICAL_MIN_PROFIT_FACTOR,
            "history_has_promotion_authority": False,
        },
        "wallet_specific_provider_requests_added": 0,
        "paper_only": True,
        "live_money_authority": False,
    }


async def _poll_once_with_pumpfun_selection(self: Any) -> None:
    if _ORIGINAL_POLL is None:
        raise RuntimeError("Pump.fun-equivalent Robinhood selection wrapper is not installed")
    await _ORIGINAL_POLL(self)
    try:
        discovered = _discover_from_ingested_swaps(self)
        screened = _screen_one_candidate(self)
        forward = _record_forward_observations(self)
        marked = _mark_forward_observations(self)
        demoted = _demote_mature_negative_candidates(self)
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE robinhood_wallet_selection_state SET last_cycle_at=?,last_error=NULL WHERE id=1",
                (_utcnow().isoformat(),),
            )
        setattr(
            self,
            "_roi_pumpfun_wallet_selection_last_cycle",
            {
                "broad_samples_added": discovered,
                "historical_candidate_passed": bool(screened),
                "forward_observations_added": forward,
                "forward_marks_added": marked,
                "mature_negative_candidates_demoted": demoted,
                "provider_requests_added": 0,
            },
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        setattr(self, "_roi_pumpfun_wallet_selection_last_error", f"{type(exc).__name__}: selection cycle unavailable")
        try:
            with self.store._lock, self.store.db:
                self.store.db.execute(
                    "UPDATE robinhood_wallet_selection_state SET last_cycle_at=?,last_error=? WHERE id=1",
                    (_utcnow().isoformat(), f"{type(exc).__name__}: selection cycle unavailable"),
                )
        except Exception:
            pass


def _status_with_pumpfun_selection(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("Pump.fun-equivalent Robinhood selection status wrapper is not installed")
    payload = dict(_ORIGINAL_STATUS(self))
    try:
        payload["pumpfun_wallet_selection"] = _selection_status(self)
    except Exception as exc:
        payload["pumpfun_wallet_selection"] = {
            "selection_version": SELECTION_VERSION,
            "failed_closed": True,
            "error": f"{type(exc).__name__}: selection status unavailable",
            "capacity_is_ceiling_not_target": True,
            "empty_slots_allowed": True,
            "provider_requests_added": 0,
            "paper_only": True,
            "live_money_authority": False,
        }
    return payload


def install_robinhood_pumpfun_wallet_selection(plane_cls: type[Any]) -> None:
    global _ORIGINAL_BUILD, _ORIGINAL_RANKINGS, _ORIGINAL_POLL, _ORIGINAL_STATUS, _INSTALLED
    if _INSTALLED:
        return

    _ORIGINAL_BUILD = universe.build_entity_universe
    universe.build_entity_universe = build_quality_entity_universe

    _ORIGINAL_RANKINGS = alignment._research_rankings
    alignment._research_rankings = _research_rankings_with_quality_candidates

    _ORIGINAL_POLL = plane_cls._poll_once
    setattr(_poll_once_with_pumpfun_selection, "_roi_robinhood_pumpfun_wallet_selection", True)
    plane_cls._poll_once = _poll_once_with_pumpfun_selection  # type: ignore[method-assign]

    _ORIGINAL_STATUS = plane_cls.status
    setattr(_status_with_pumpfun_selection, "_roi_robinhood_pumpfun_wallet_selection", True)
    plane_cls.status = _status_with_pumpfun_selection  # type: ignore[method-assign]

    setattr(plane_cls, "_roi_robinhood_pumpfun_wallet_selection_installed", True)
    setattr(plane_cls, "_roi_robinhood_pumpfun_wallet_selection_version", SELECTION_VERSION)
    _INSTALLED = True


__all__ = [
    "SELECTION_VERSION",
    "CURATED_RESEARCH_SEEDS",
    "BROAD_SAMPLE_MODULUS",
    "BROAD_SCAN_LIMIT",
    "HISTORICAL_MAX_SWAPS",
    "HISTORICAL_MIN_CLOSED_EPISODES",
    "HISTORICAL_MIN_DISTINCT_TOKENS",
    "HISTORICAL_MIN_RETURN_ON_CAPITAL",
    "HISTORICAL_MIN_PROFIT_FACTOR",
    "build_quality_entity_universe",
    "install_robinhood_pumpfun_wallet_selection",
]
