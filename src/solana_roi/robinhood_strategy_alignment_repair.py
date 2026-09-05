from __future__ import annotations

import hashlib
import json
import statistics
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from . import regime_roi_wallet_authority as authority
from . import risk_conditioned_alpha_v5 as risk_v5
from .robinhood_chain_core import (
    LIVE_LAG_BLOCKS,
    ROBINHOOD_CHAIN_ID,
    V2Curve,
    V3Pool,
    WETH,
    _clean_address,
)
from .robinhood_chain_profit_maximizer import (
    ROBINHOOD_V5_MAX_POSITION,
    ROBINHOOD_V5_MIN_SAMPLES,
    ROBINHOOD_V5_POSITION_GRID,
    ROBINHOOD_V5_VERSION,
)
from .robinhood_entity_quota_architecture import PROOF_VERSION


REPAIR_VERSION = "robinhood-strategy-alignment-v1"
RESEARCH_HORIZON_SECONDS = 120
RESEARCH_MIN_MARKED_BUYS = 5
RESEARCH_MIN_DISTINCT_TOKENS = 3
RESEARCH_TOP_N = 50

_ORIGINAL_RESTORE: Callable[..., Any] | None = None
_ORIGINAL_SETTLE_OPEN: Callable[..., Any] | None = None
_ORIGINAL_FLOW: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_discovery_schema(self: Any) -> None:
    if bool(getattr(self, "_roi_robinhood_alignment_schema_ready", False)):
        return
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "CREATE TABLE IF NOT EXISTS robinhood_entity_discovery_state ("
            "strategy_version TEXT PRIMARY KEY, forward_start_swap_id INTEGER NOT NULL, "
            "initialized_at TEXT NOT NULL, last_scan_at TEXT, last_mark_at TEXT)"
        )
        self.store.db.execute(
            "CREATE TABLE IF NOT EXISTS robinhood_entity_discovery_observations ("
            "swap_id INTEGER PRIMARY KEY, strategy_version TEXT NOT NULL, release_commit TEXT NOT NULL, "
            "entity TEXT NOT NULL, actor TEXT NOT NULL, venue TEXT NOT NULL, lifecycle TEXT NOT NULL, "
            "token TEXT NOT NULL, market TEXT NOT NULL, side TEXT NOT NULL, quote_amount_wei TEXT NOT NULL, "
            "price_eth REAL, block_number INTEGER NOT NULL, observed_at TEXT NOT NULL, "
            "mark_price_eth REAL, mark_return REAL, marked_at TEXT, "
            "research_only INTEGER NOT NULL, paper_promotion_authority INTEGER NOT NULL)"
        )
        self.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_robinhood_entity_discovery_entity "
            "ON robinhood_entity_discovery_observations(strategy_version,entity,venue,lifecycle,swap_id)"
        )
        self.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_robinhood_entity_discovery_unmarked "
            "ON robinhood_entity_discovery_observations(strategy_version,marked_at,side,observed_at)"
        )
        state = self.store.db.execute(
            "SELECT 1 FROM robinhood_entity_discovery_state WHERE strategy_version=?",
            (ROBINHOOD_V5_VERSION,),
        ).fetchone()
        if state is None:
            row = self.store.db.execute("SELECT COALESCE(MAX(id),0) FROM robinhood_swaps").fetchone()
            high_water = int(row[0] if row is not None else 0)
            self.store.db.execute(
                "INSERT INTO robinhood_entity_discovery_state("
                "strategy_version,forward_start_swap_id,initialized_at) VALUES (?,?,?)",
                (ROBINHOOD_V5_VERSION, high_water, _utcnow()),
            )
    setattr(self, "_roi_robinhood_alignment_schema_ready", True)


def _restore_durable_markets(self: Any) -> None:
    if _ORIGINAL_RESTORE is None:
        raise RuntimeError("Robinhood durable restore wrapper is not installed")
    _ORIGINAL_RESTORE(self)
    restored_v3 = 0
    restored_v2 = 0
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT * FROM robinhood_launches ORDER BY id DESC LIMIT 4096"
        ).fetchall()
    for raw in rows:
        row = dict(raw)
        pool = _clean_address(row.get("pool"))
        curve = _clean_address(row.get("curve"))
        token = _clean_address(row.get("token"))
        venue = str(row.get("venue") or "")
        protocol = str(row.get("protocol") or "")
        if pool and token and venue.startswith(("PONS_V1", "UNISWAP_V3")) and pool not in self.v3_pools:
            token0, token1 = sorted([WETH, token], key=lambda item: int(item, 16))
            self.v3_pools[pool] = V3Pool(
                token=token,
                pool=pool,
                token0=token0,
                token1=token1,
                fee=int(row.get("fee") or 10_000),
                token_decimals=18,
                venue=venue or "UNISWAP_V3_DIRECT",
                lifecycle=str(row.get("lifecycle") or "new_weth_pool"),
                deployer=_clean_address(row.get("deployer")),
                launch_block=int(row.get("launch_block") or 0),
                restrictions_end_block=int(row.get("restrictions_end_block") or 0),
            )
            restored_v3 += 1
        if curve and token and protocol == "pons_v2" and curve not in self.v2_curves:
            self.v2_curves[curve] = V2Curve(
                token=token,
                curve=curve,
                deployer=_clean_address(row.get("deployer")),
                pair_token=_clean_address(row.get("pair_token")),
                launch_config_id=0,
                graduation_threshold=int(row.get("graduation_threshold") or 0),
                launch_block=int(row.get("launch_block") or 0),
            )
            restored_v2 += 1
    self._trim_tracking()
    setattr(
        self,
        "_roi_durable_market_restore",
        {
            "v3_added_from_prior_releases": restored_v3,
            "v2_added_from_prior_releases": restored_v2,
            "restore_scope": "latest_durable_chain_metadata_across_releases",
        },
    )
    _ensure_discovery_schema(self)


def _paper_nav_usd_durable(self: Any) -> float:
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT paper_nav_multiplier FROM robinhood_paper_outcomes "
            "WHERE paper_only=1 ORDER BY id"
        ).fetchall()
    multiplier = 1.0
    for row in rows:
        multiplier *= max(0.0, float(row["paper_nav_multiplier"]))
    return self.starting_nav_usd * multiplier


def _open_exposure_durable(self: Any) -> float:
    with self.store._lock:
        row = self.store.db.execute(
            "SELECT COALESCE(SUM(t.position_fraction),0) AS total "
            "FROM robinhood_paper_trials t LEFT JOIN robinhood_paper_outcomes o ON o.trial_id=t.id "
            "WHERE t.paper_only=1 AND o.id IS NULL"
        ).fetchone()
    return min(1.0, max(0.0, float(row["total"] or 0.0))) if row is not None else 0.0


def _token_open_durable(self: Any, token: str) -> bool:
    with self.store._lock:
        row = self.store.db.execute(
            "SELECT 1 FROM robinhood_paper_trials t LEFT JOIN robinhood_paper_outcomes o ON o.trial_id=t.id "
            "WHERE t.paper_only=1 AND t.token=? AND o.id IS NULL LIMIT 1",
            (token,),
        ).fetchone()
    return row is not None


def _context_returns_durable(self: Any, entity: str, venue: str, lifecycle: str) -> list[float]:
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT o.net_return FROM robinhood_paper_outcomes o JOIN robinhood_paper_trials t ON t.id=o.trial_id "
            "WHERE t.paper_only=1 AND o.paper_only=1 AND o.trigger_entity=? AND o.venue=? AND o.lifecycle=? ORDER BY o.id",
            (entity, venue, lifecycle),
        ).fetchall()
    return [float(row["net_return"]) for row in rows]


async def _settle_open_positions_durable(self: Any) -> None:
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT t.* FROM robinhood_paper_trials t LEFT JOIN robinhood_paper_outcomes o ON o.trial_id=t.id "
            "WHERE t.paper_only=1 AND o.id IS NULL ORDER BY t.id"
        ).fetchall()
    for raw in rows:
        try:
            await self._settle_one(dict(raw))
        except Exception:
            continue


def _v5_context_returns_durable(
    self: Any,
    *,
    entity: str,
    role: str,
    lane: str,
    venue: str,
    lifecycle: str,
    regime: str,
    risk_signature: str,
    flow_state: str,
) -> tuple[list[float], str]:
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
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT o.net_return FROM robinhood_paper_outcomes o "
            "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
            "WHERE c.strategy_version=? AND c.context_key=? AND c.paper_only=1 AND o.paper_only=1 ORDER BY o.id",
            (ROBINHOOD_V5_VERSION, key),
        ).fetchall()
        if len(rows) >= 20:
            return [float(row["net_return"]) for row in rows], "exact_context_cross_release"
        rows = self.store.db.execute(
            "SELECT o.net_return FROM robinhood_paper_outcomes o "
            "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
            "JOIN robinhood_paper_trials t ON t.id=o.trial_id "
            "WHERE c.strategy_version=? AND c.lane=? AND t.venue=? AND t.lifecycle=? AND c.regime=? "
            "AND c.paper_only=1 AND o.paper_only=1 ORDER BY o.id",
            (ROBINHOOD_V5_VERSION, lane, venue, lifecycle, regime),
        ).fetchall()
        if len(rows) >= ROBINHOOD_V5_MIN_SAMPLES:
            return [float(row["net_return"]) for row in rows], "lane_venue_lifecycle_regime_cross_release"
        rows = self.store.db.execute(
            "SELECT o.net_return FROM robinhood_paper_outcomes o "
            "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
            "JOIN robinhood_paper_trials t ON t.id=o.trial_id "
            "WHERE c.strategy_version=? AND c.lane=? AND t.venue=? AND t.lifecycle=? "
            "AND c.paper_only=1 AND o.paper_only=1 ORDER BY o.id",
            (ROBINHOOD_V5_VERSION, lane, venue, lifecycle),
        ).fetchall()
    return [float(row["net_return"]) for row in rows], "lane_venue_lifecycle_cross_release" if rows else "none"


def _profitability_segments_durable(self: Any) -> list[dict[str, Any]]:
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT venue,lifecycle,net_return FROM robinhood_paper_outcomes WHERE paper_only=1 ORDER BY id"
        ).fetchall()
    grouped: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["venue"]), str(row["lifecycle"]))].append(float(row["net_return"]))
    result: list[dict[str, Any]] = []
    for (venue, lifecycle), values in sorted(grouped.items()):
        trimmed = statistics.mean(sorted(values)[:-1]) if len(values) > 1 else values[0]
        result.append(
            {
                "venue": venue,
                "lifecycle": lifecycle,
                "sample_count": len(values),
                "mean_roi_pct": statistics.mean(values) * 100.0,
                "median_roi_pct": statistics.median(values) * 100.0,
                "trimmed_mean_roi_ex_best_1_pct": trimmed * 100.0,
                "positive_rate_pct": sum(value > 0 for value in values) / len(values) * 100.0,
                "evidence_scope": "all_releases_continuous_paper_sleeve",
            }
        )
    return result


def _wallet_contexts_durable(self: Any) -> list[dict[str, Any]]:
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT trigger_entity,venue,lifecycle,net_return FROM robinhood_paper_outcomes "
            "WHERE paper_only=1 ORDER BY id"
        ).fetchall()
    grouped: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["trigger_entity"]), str(row["venue"]), str(row["lifecycle"]))].append(float(row["net_return"]))
    contexts: list[dict[str, Any]] = []
    from .robinhood_chain_core import classify_context_returns

    for (entity, venue, lifecycle), values in grouped.items():
        contexts.append(
            {
                "wallet_or_effective_entity": entity,
                "venue": venue,
                "lifecycle": lifecycle,
                "role": "momentum_alpha",
                **classify_context_returns(values),
                "cross_chain_success_transfer_allowed": False,
                "cross_venue_success_transfer_allowed": False,
                "evidence_scope": "all_releases_continuous_paper_sleeve",
            }
        )
    contexts.sort(
        key=lambda row: (
            1 if row["state"] == "promoted_paper_context" else 0,
            row["trimmed_mean_roi_ex_best_1_pct"]
            if row["trimmed_mean_roi_ex_best_1_pct"] is not None
            else float("-inf"),
            row["sample_count"],
        ),
        reverse=True,
    )
    return contexts[:50]


def _robinhood_rows_durable(plane: Any) -> list[dict[str, Any]]:
    try:
        with plane.store._lock:
            rows = plane.store.db.execute(
                "SELECT t.trigger_entity,c.lane,t.venue,t.lifecycle,c.regime,c.trigger_role,c.risk_signature,c.flow_state,o.net_return "
                "FROM robinhood_paper_outcomes o JOIN robinhood_paper_trials t ON t.id=o.trial_id "
                "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
                "WHERE c.strategy_version=? AND c.paper_only=1 AND o.paper_only=1 ORDER BY o.id",
                (ROBINHOOD_V5_VERSION,),
            ).fetchall()
    except Exception:
        return []
    grouped: dict[tuple[str, str, str, str, str, str, str, str], list[float]] = defaultdict(list)
    for row in rows:
        key = (
            str(row["trigger_entity"] or ""),
            str(row["lane"] or ""),
            str(row["venue"] or ""),
            str(row["lifecycle"] or ""),
            str(row["regime"] or "unknown"),
            str(row["trigger_role"] or "unknown"),
            str(row["risk_signature"] or "clean"),
            str(row["flow_state"] or "unknown"),
        )
        value = authority._safe_float(row["net_return"])
        if key[0] and value is not None:
            grouped[key].append(value)
    result: list[dict[str, Any]] = []
    for key, values in grouped.items():
        profile = risk_v5.robust_return_profile(
            values,
            grid=ROBINHOOD_V5_POSITION_GRID,
            max_fraction=ROBINHOOD_V5_MAX_POSITION,
            min_samples=ROBINHOOD_V5_MIN_SAMPLES,
        )
        result.append(
            {
                "wallet": key[0],
                "strategy_family": key[1],
                "venue": key[2],
                "lifecycle_stage": key[3],
                "regime": key[4],
                "role": key[5],
                "risk_signature": key[6],
                "risk_class": "clean" if key[6] == "clean" else "hazard",
                "flow_state": key[7],
                "source_kind": "robinhood_chain_v5_forward_cross_release",
                "sample_count": profile.sample_count,
                "mature_forward_context": profile.sample_count >= ROBINHOOD_V5_MIN_SAMPLES,
                "specialist_positive": profile.state == "promoted_positive_log_growth",
                "best_expected_log_growth": profile.best_expected_log_growth,
                "trimmed_mean_residual_roi_ex_best_1": profile.trimmed_mean_ex_best,
                "mean_return": profile.mean_return,
            }
        )
    return result


def _record_resolved_entity_observations(self: Any) -> int:
    _ensure_discovery_schema(self)
    latest = int(getattr(self, "_latest_block", 0) or 0)
    if latest <= 0:
        return 0
    min_block = max(0, latest - int(LIVE_LAG_BLOCKS))
    with self.store._lock, self.store.db:
        state = self.store.db.execute(
            "SELECT forward_start_swap_id FROM robinhood_entity_discovery_state WHERE strategy_version=?",
            (ROBINHOOD_V5_VERSION,),
        ).fetchone()
        forward_start = int(state[0] if state is not None else 0)
        rows = self.store.db.execute(
            "SELECT s.id,s.release_commit,s.venue,s.lifecycle,s.token,s.market,s.actor,s.side,s.quote_amount_wei,"
            "s.price_eth,s.block_number,s.observed_at,p.funding_anchor "
            "FROM robinhood_swaps s JOIN robinhood_entity_proofs p "
            "ON p.chain_id=? AND p.actor=s.actor AND p.resolver_version=? "
            "LEFT JOIN robinhood_entity_discovery_observations d ON d.swap_id=s.id "
            "WHERE s.id>? AND s.block_number>=? AND d.swap_id IS NULL "
            "ORDER BY s.id LIMIT 512",
            (ROBINHOOD_CHAIN_ID, PROOF_VERSION, forward_start, min_block),
        ).fetchall()
        inserted = 0
        for row in rows:
            entity = _clean_address(str(row["funding_anchor"] or ""))
            actor = _clean_address(str(row["actor"] or ""))
            if not entity or not actor:
                continue
            cursor = self.store.db.execute(
                "INSERT OR IGNORE INTO robinhood_entity_discovery_observations("
                "swap_id,strategy_version,release_commit,entity,actor,venue,lifecycle,token,market,side,quote_amount_wei,"
                "price_eth,block_number,observed_at,research_only,paper_promotion_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    int(row["id"]),
                    ROBINHOOD_V5_VERSION,
                    str(row["release_commit"]),
                    entity,
                    actor,
                    str(row["venue"]),
                    str(row["lifecycle"]),
                    str(row["token"]),
                    str(row["market"]),
                    str(row["side"]),
                    str(row["quote_amount_wei"]),
                    float(row["price_eth"]) if row["price_eth"] is not None else None,
                    int(row["block_number"]),
                    str(row["observed_at"]),
                ),
            )
            inserted += int(cursor.rowcount == 1)
        self.store.db.execute(
            "UPDATE robinhood_entity_discovery_state SET last_scan_at=? WHERE strategy_version=?",
            (_utcnow(), ROBINHOOD_V5_VERSION),
        )
    return inserted


def _mark_discovery_observations(self: Any) -> int:
    _ensure_discovery_schema(self)
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=RESEARCH_HORIZON_SECONDS)).isoformat()
    with self.store._lock, self.store.db:
        rows = self.store.db.execute(
            "SELECT swap_id,market,price_eth,observed_at FROM robinhood_entity_discovery_observations "
            "WHERE strategy_version=? AND side='buy' AND marked_at IS NULL AND price_eth IS NOT NULL "
            "AND observed_at<=? ORDER BY swap_id LIMIT 200",
            (ROBINHOOD_V5_VERSION, cutoff),
        ).fetchall()
        marked = 0
        for row in rows:
            entry = float(row["price_eth"] or 0.0)
            if entry <= 0.0:
                continue
            try:
                target = (datetime.fromisoformat(str(row["observed_at"])) + timedelta(seconds=RESEARCH_HORIZON_SECONDS)).isoformat()
            except ValueError:
                continue
            mark = self.store.db.execute(
                "SELECT price_eth,observed_at FROM robinhood_swaps "
                "WHERE market=? AND id>? AND price_eth IS NOT NULL AND observed_at>=? "
                "ORDER BY id LIMIT 1",
                (str(row["market"]), int(row["swap_id"]), target),
            ).fetchone()
            if mark is None:
                continue
            mark_price = float(mark["price_eth"] or 0.0)
            if mark_price <= 0.0:
                continue
            self.store.db.execute(
                "UPDATE robinhood_entity_discovery_observations SET mark_price_eth=?,mark_return=?,marked_at=? "
                "WHERE swap_id=?",
                (mark_price, mark_price / entry - 1.0, str(mark["observed_at"]), int(row["swap_id"])),
            )
            marked += 1
        self.store.db.execute(
            "UPDATE robinhood_entity_discovery_state SET last_mark_at=? WHERE strategy_version=?",
            (_utcnow(), ROBINHOOD_V5_VERSION),
        )
    return marked


def _research_rankings(self: Any) -> list[dict[str, Any]]:
    _ensure_discovery_schema(self)
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT entity,actor,venue,lifecycle,token,side,mark_return,observed_at "
            "FROM robinhood_entity_discovery_observations WHERE strategy_version=? ORDER BY swap_id",
            (ROBINHOOD_V5_VERSION,),
        ).fetchall()
    grouped: dict[str, dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        entity = str(row["entity"])
        item = grouped.setdefault(
            entity,
            {
                "actors": set(),
                "tokens": set(),
                "venues": set(),
                "lifecycles": set(),
                "buys": 0,
                "sells": 0,
                "returns": [],
                "last_seen_at": None,
            },
        )
        item["actors"].add(str(row["actor"]))
        item["tokens"].add(str(row["token"]))
        item["venues"].add(str(row["venue"]))
        item["lifecycles"].add(str(row["lifecycle"]))
        if str(row["side"]) == "buy":
            item["buys"] += 1
        elif str(row["side"]) == "sell":
            item["sells"] += 1
        if row["mark_return"] is not None:
            item["returns"].append(float(row["mark_return"]))
        item["last_seen_at"] = str(row["observed_at"])

    rankings: list[dict[str, Any]] = []
    for entity, item in grouped.items():
        values = list(item["returns"])
        median_return = statistics.median(values) if values else None
        mean_return = statistics.mean(values) if values else None
        trimmed = statistics.mean(sorted(values)[:-1]) if len(values) > 1 else (values[0] if values else None)
        positive_rate = sum(value > 0 for value in values) / len(values) if values else None
        priority = bool(
            len(values) >= RESEARCH_MIN_MARKED_BUYS
            and len(item["tokens"]) >= RESEARCH_MIN_DISTINCT_TOKENS
            and median_return is not None
            and trimmed is not None
            and median_return > 0.0
            and trimmed > 0.0
        )
        rankings.append(
            {
                "entity": entity,
                "actor_count": len(item["actors"]),
                "distinct_tokens": len(item["tokens"]),
                "venues": sorted(item["venues"]),
                "lifecycles": sorted(item["lifecycles"]),
                "buy_observations": int(item["buys"]),
                "sell_observations": int(item["sells"]),
                "marked_buy_observations": len(values),
                "mean_120s_followthrough_pct": mean_return * 100.0 if mean_return is not None else None,
                "median_120s_followthrough_pct": median_return * 100.0 if median_return is not None else None,
                "trimmed_mean_120s_followthrough_ex_best_1_pct": trimmed * 100.0 if trimmed is not None else None,
                "positive_120s_followthrough_rate_pct": positive_rate * 100.0 if positive_rate is not None else None,
                "priority_research_challenger": priority,
                "last_seen_at": item["last_seen_at"],
                "research_only": True,
                "historical_or_mark_evidence_has_paper_promotion_authority": False,
                "ranking_can_bypass_exact_executable_quote": False,
                "ranking_can_bypass_forward_paper_maturity": False,
            }
        )
    rankings.sort(
        key=lambda row: (
            1 if row["priority_research_challenger"] else 0,
            row["trimmed_mean_120s_followthrough_ex_best_1_pct"]
            if row["trimmed_mean_120s_followthrough_ex_best_1_pct"] is not None
            else float("-inf"),
            row["median_120s_followthrough_pct"]
            if row["median_120s_followthrough_pct"] is not None
            else float("-inf"),
            row["marked_buy_observations"],
            row["distinct_tokens"],
        ),
        reverse=True,
    )
    for index, row in enumerate(rankings, start=1):
        row["research_rank"] = index
    return rankings[:RESEARCH_TOP_N]


async def _flow_with_discovery(self: Any, swaps: Any, *, deployer: str = "") -> dict[str, Any]:
    if _ORIGINAL_FLOW is None:
        raise RuntimeError("Robinhood entity discovery flow wrapper is not installed")
    metrics = dict(await _ORIGINAL_FLOW(self, swaps, deployer=deployer))
    try:
        added = _record_resolved_entity_observations(self)
        marked = _mark_discovery_observations(self)
        rankings = _research_rankings(self)
        trigger_entity = str(metrics.get("trigger_entity") or "")
        trigger = next((row for row in rankings if row["entity"] == trigger_entity), None)
        metrics["entity_discovery"] = {
            "resolved_forward_observations_added": added,
            "marks_added": marked,
            "trigger_entity_research_rank": trigger.get("research_rank") if trigger else None,
            "trigger_entity_priority_research_challenger": bool(trigger and trigger.get("priority_research_challenger")),
            "research_only": True,
            "paper_promotion_authority": False,
            "provider_requests_added": 0,
        }
    except Exception as exc:
        metrics["entity_discovery"] = {
            "failed_closed": True,
            "error": f"{type(exc).__name__}: entity discovery accounting unavailable",
            "research_only": True,
            "paper_promotion_authority": False,
            "provider_requests_added": 0,
        }
    return metrics


def _status_with_alignment(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("Robinhood alignment status wrapper is not installed")
    payload = dict(_ORIGINAL_STATUS(self))
    _ensure_discovery_schema(self)
    try:
        with self.store._lock:
            durable_outcomes = int(
                self.store.db.execute(
                    "SELECT COUNT(*) FROM robinhood_paper_outcomes WHERE paper_only=1"
                ).fetchone()[0]
            )
            open_trials = int(
                self.store.db.execute(
                    "SELECT COUNT(*) FROM robinhood_paper_trials t LEFT JOIN robinhood_paper_outcomes o ON o.trial_id=t.id "
                    "WHERE t.paper_only=1 AND o.id IS NULL"
                ).fetchone()[0]
            )
            strategy_contexts = int(
                self.store.db.execute(
                    "SELECT COUNT(*) FROM robinhood_v5_trial_context WHERE strategy_version=? AND paper_only=1",
                    (ROBINHOOD_V5_VERSION,),
                ).fetchone()[0]
            )
            discovery_count = int(
                self.store.db.execute(
                    "SELECT COUNT(*) FROM robinhood_entity_discovery_observations WHERE strategy_version=?",
                    (ROBINHOOD_V5_VERSION,),
                ).fetchone()[0]
            )
            marked_count = int(
                self.store.db.execute(
                    "SELECT COUNT(*) FROM robinhood_entity_discovery_observations "
                    "WHERE strategy_version=? AND mark_return IS NOT NULL",
                    (ROBINHOOD_V5_VERSION,),
                ).fetchone()[0]
            )
            state = self.store.db.execute(
                "SELECT forward_start_swap_id,initialized_at,last_scan_at,last_mark_at "
                "FROM robinhood_entity_discovery_state WHERE strategy_version=?",
                (ROBINHOOD_V5_VERSION,),
            ).fetchone()
        rankings = _research_rankings(self)
        payload["durable_strategy_memory"] = {
            "repair_version": REPAIR_VERSION,
            "paper_nav_scope": "continuous_sleeve_across_releases",
            "open_position_scope": "all_unsettled_paper_trials_across_releases",
            "context_learning_scope": "compatible_strategy_version_across_releases",
            "launch_restore_scope": "durable_chain_metadata_across_releases",
            "paper_outcomes_all_releases": durable_outcomes,
            "open_paper_trials_all_releases": open_trials,
            "compatible_v5_context_rows_all_releases": strategy_contexts,
            "release_sha_is_evidence_lineage_not_learning_boundary": True,
            "cross_strategy_version_success_transfer_allowed": False,
            "cross_chain_success_transfer_allowed": False,
            "paper_only": True,
            "live_money_authority": False,
        }
        payload["entity_discovery"] = {
            "enabled": True,
            "strategy_version": ROBINHOOD_V5_VERSION,
            "source": "already_ingested_robinhood_swaps_joined_to_durable_entity_proofs",
            "provider_requests_added": 0,
            "forward_only_start_swap_id": int(state["forward_start_swap_id"]) if state is not None else None,
            "initialized_at": str(state["initialized_at"]) if state is not None else None,
            "last_scan_at": str(state["last_scan_at"] or "") or None if state is not None else None,
            "last_mark_at": str(state["last_mark_at"] or "") or None if state is not None else None,
            "resolved_forward_observations": discovery_count,
            "marked_buy_observations": marked_count,
            "research_horizon_seconds": RESEARCH_HORIZON_SECONDS,
            "priority_min_marked_buys": RESEARCH_MIN_MARKED_BUYS,
            "priority_min_distinct_tokens": RESEARCH_MIN_DISTINCT_TOKENS,
            "rankings": rankings,
            "research_only": True,
            "historical_or_mark_evidence_has_paper_promotion_authority": False,
            "paper_authority_still_requires_forward_settled_paper_outcomes": True,
            "exact_executable_quotes_still_required": True,
            "unresolved_raw_addresses_count_as_independent": False,
            "paper_only": True,
            "live_money_authority": False,
        }
        payload["durable_market_restore"] = getattr(self, "_roi_durable_market_restore", {})
    except Exception as exc:
        payload["durable_strategy_memory"] = {
            "repair_version": REPAIR_VERSION,
            "failed_closed": True,
            "error": f"{type(exc).__name__}: durable Robinhood strategy memory status unavailable",
            "paper_only": True,
            "live_money_authority": False,
        }
    return payload


def install_robinhood_strategy_alignment_repair(plane_cls: type[Any]) -> None:
    """Make Robinhood strategy memory durable and evaluate resolved entities continuously.

    Release SHAs remain lineage/audit identifiers, not learning or portfolio-reset
    boundaries. The new discovery lane reuses already-ingested swaps and durable
    Blockscout proofs only; it performs zero additional provider requests and has no
    paper-promotion authority. Exact executable quotes and settled forward paper
    outcomes remain the only path to active paper promotion.
    """

    global _ORIGINAL_RESTORE, _ORIGINAL_SETTLE_OPEN, _ORIGINAL_FLOW, _ORIGINAL_STATUS
    if bool(getattr(plane_cls, "_roi_robinhood_strategy_alignment_installed", False)):
        return

    _ORIGINAL_RESTORE = plane_cls._restore
    plane_cls._restore = _restore_durable_markets  # type: ignore[method-assign]

    plane_cls._paper_nav_usd = _paper_nav_usd_durable  # type: ignore[method-assign]
    plane_cls._open_exposure = _open_exposure_durable  # type: ignore[method-assign]
    plane_cls._token_open = _token_open_durable  # type: ignore[method-assign]
    plane_cls._context_returns = _context_returns_durable  # type: ignore[method-assign]
    plane_cls._v5_context_returns = _v5_context_returns_durable  # type: ignore[method-assign]
    plane_cls._profitability_segments = _profitability_segments_durable  # type: ignore[method-assign]
    plane_cls._wallet_contexts = _wallet_contexts_durable  # type: ignore[method-assign]

    _ORIGINAL_SETTLE_OPEN = plane_cls._settle_open_positions
    plane_cls._settle_open_positions = _settle_open_positions_durable  # type: ignore[method-assign]

    _ORIGINAL_FLOW = plane_cls._v5_flow_metrics
    plane_cls._v5_flow_metrics = _flow_with_discovery  # type: ignore[method-assign]

    authority._robinhood_rows = _robinhood_rows_durable

    _ORIGINAL_STATUS = plane_cls.status
    plane_cls.status = _status_with_alignment  # type: ignore[method-assign]

    setattr(plane_cls, "_roi_robinhood_strategy_alignment_installed", True)
    setattr(plane_cls, "_roi_robinhood_strategy_alignment_version", REPAIR_VERSION)


__all__ = [
    "REPAIR_VERSION",
    "RESEARCH_HORIZON_SECONDS",
    "RESEARCH_MIN_MARKED_BUYS",
    "RESEARCH_MIN_DISTINCT_TOKENS",
    "install_robinhood_strategy_alignment_repair",
]
