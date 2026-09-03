from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Iterable


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class WalletPerformanceSnapshot:
    """Point-in-time, forward-copyable wallet evidence.

    Metrics must use only information observable by the strategy at or before
    ``observed_at``. Raw wallet profit that cannot be reproduced after detection,
    chase, fees, liquidity and latency is intentionally not a promotion metric.
    """

    wallet: str
    entity_id: str
    observed_at: datetime
    closed_episodes: int
    copyable_return_on_capital: float
    geometric_growth: float
    profit_factor: float
    hit_rate: float
    max_drawdown: float
    copyability_rate: float
    manipulation_risk: float
    side_wallet_risk: float
    median_entry_lag_ms: float
    source: str = "continuous-wallet-intelligence"

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["observed_at"] = self.observed_at.isoformat()
        return payload


@dataclass(frozen=True, slots=True)
class WalletPromotionPolicy:
    """Fail-closed evidence required before a wallet may join a future cohort."""

    min_forward_episodes: int = 30
    min_copyable_return_on_capital: float = 0.0
    min_geometric_growth: float = 0.0
    min_profit_factor: float = 1.0
    min_copyability_rate: float = 0.80
    max_manipulation_risk: float = 0.10
    max_side_wallet_risk: float = 0.10
    max_drawdown: float = 0.60
    min_superiority_ratio: float = 1.15
    max_drawdown_disadvantage: float = 0.05


@dataclass(frozen=True, slots=True)
class WalletPromotionDecision:
    candidate_wallet: str
    incumbent_wallet: str | None
    eligible: bool
    blockers: tuple[str, ...]
    candidate_score: float
    incumbent_score: float | None
    superiority_ratio: float | None


class ContinuousWalletIntelligence:
    """Research-only wallet ranking and governed next-cohort promotion engine.

    This component never mutates the currently frozen/armed forward cohort.
    A superior wallet can only be staged for a *new* immutable cohort/version.
    That preserves prospective validity while allowing the strategy to adapt.
    """

    def __init__(self, store: Any, policy: WalletPromotionPolicy | None = None):
        self.store = store
        self.policy = policy or WalletPromotionPolicy()
        with store._lock, store.db:
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS wallet_intelligence_snapshots ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, wallet TEXT NOT NULL, entity_id TEXT NOT NULL, "
                "observed_at TEXT NOT NULL, closed_episodes INTEGER NOT NULL, "
                "copyable_return_on_capital REAL NOT NULL, geometric_growth REAL NOT NULL, "
                "profit_factor REAL NOT NULL, hit_rate REAL NOT NULL, max_drawdown REAL NOT NULL, "
                "copyability_rate REAL NOT NULL, manipulation_risk REAL NOT NULL, "
                "side_wallet_risk REAL NOT NULL, median_entry_lag_ms REAL NOT NULL, "
                "source TEXT NOT NULL, UNIQUE(wallet, observed_at, source))"
            )
            store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_wallet_intelligence_latest "
                "ON wallet_intelligence_snapshots(wallet, observed_at DESC)"
            )
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS adaptive_wallet_cohorts ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, strategy_version TEXT NOT NULL UNIQUE, "
                "created_at TEXT NOT NULL, parent_version TEXT NOT NULL, cohort_json TEXT NOT NULL, "
                "rationale_json TEXT NOT NULL, cohort_sha256 TEXT NOT NULL UNIQUE, "
                "status TEXT NOT NULL CHECK(status IN ('proposed','approved','retired')))"
            )

    @staticmethod
    def risk_adjusted_copyable_score(snapshot: WalletPerformanceSnapshot) -> float:
        if snapshot.closed_episodes <= 0:
            return float("-inf")
        return (
            snapshot.copyable_return_on_capital
            * max(snapshot.profit_factor, 0.0)
            * max(snapshot.copyability_rate, 0.0)
            * max(0.0, 1.0 - snapshot.manipulation_risk)
            * max(0.0, 1.0 - snapshot.side_wallet_risk)
            / (1.0 + max(snapshot.max_drawdown, 0.0))
        )

    def record_snapshot(self, snapshot: WalletPerformanceSnapshot) -> bool:
        if not snapshot.wallet or not snapshot.entity_id:
            raise ValueError("wallet and entity_id are required")
        if snapshot.closed_episodes < 0:
            raise ValueError("closed_episodes cannot be negative")
        bounded = (
            snapshot.hit_rate,
            snapshot.max_drawdown,
            snapshot.copyability_rate,
            snapshot.manipulation_risk,
            snapshot.side_wallet_risk,
        )
        if any(value < 0.0 or value > 1.0 for value in bounded):
            raise ValueError("rate and risk fields must be in [0,1]")
        with self.store._lock, self.store.db:
            cursor = self.store.db.execute(
                "INSERT OR IGNORE INTO wallet_intelligence_snapshots("
                "wallet, entity_id, observed_at, closed_episodes, copyable_return_on_capital, "
                "geometric_growth, profit_factor, hit_rate, max_drawdown, copyability_rate, "
                "manipulation_risk, side_wallet_risk, median_entry_lag_ms, source) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    snapshot.wallet,
                    snapshot.entity_id,
                    snapshot.observed_at.isoformat(),
                    int(snapshot.closed_episodes),
                    float(snapshot.copyable_return_on_capital),
                    float(snapshot.geometric_growth),
                    float(snapshot.profit_factor),
                    float(snapshot.hit_rate),
                    float(snapshot.max_drawdown),
                    float(snapshot.copyability_rate),
                    float(snapshot.manipulation_risk),
                    float(snapshot.side_wallet_risk),
                    float(snapshot.median_entry_lag_ms),
                    snapshot.source,
                ),
            )
        if cursor.rowcount == 1:
            self.store.append("wallet_intelligence_snapshot", snapshot.observed_at.isoformat(), snapshot.to_payload())
            return True
        return False

    def latest_snapshot(self, wallet: str) -> WalletPerformanceSnapshot | None:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT wallet, entity_id, observed_at, closed_episodes, copyable_return_on_capital, "
                "geometric_growth, profit_factor, hit_rate, max_drawdown, copyability_rate, "
                "manipulation_risk, side_wallet_risk, median_entry_lag_ms, source "
                "FROM wallet_intelligence_snapshots WHERE wallet=? "
                "ORDER BY observed_at DESC, id DESC LIMIT 1",
                (wallet,),
            ).fetchone()
        if row is None:
            return None
        item = dict(row)
        item["observed_at"] = datetime.fromisoformat(str(item["observed_at"]))
        return WalletPerformanceSnapshot(**item)

    def latest_snapshots(self) -> list[WalletPerformanceSnapshot]:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT s.wallet, s.entity_id, s.observed_at, s.closed_episodes, "
                "s.copyable_return_on_capital, s.geometric_growth, s.profit_factor, s.hit_rate, "
                "s.max_drawdown, s.copyability_rate, s.manipulation_risk, s.side_wallet_risk, "
                "s.median_entry_lag_ms, s.source FROM wallet_intelligence_snapshots s "
                "JOIN (SELECT wallet, MAX(observed_at) AS observed_at FROM wallet_intelligence_snapshots "
                "GROUP BY wallet) latest ON latest.wallet=s.wallet AND latest.observed_at=s.observed_at"
            ).fetchall()
        snapshots: list[WalletPerformanceSnapshot] = []
        for row in rows:
            item = dict(row)
            item["observed_at"] = datetime.fromisoformat(str(item["observed_at"]))
            snapshots.append(WalletPerformanceSnapshot(**item))
        return snapshots

    def _evidence_blockers(self, snapshot: WalletPerformanceSnapshot) -> list[str]:
        policy = self.policy
        blockers: list[str] = []
        if snapshot.closed_episodes < policy.min_forward_episodes:
            blockers.append("insufficient_forward_episodes")
        if snapshot.copyable_return_on_capital <= policy.min_copyable_return_on_capital:
            blockers.append("copyable_return_not_positive")
        if snapshot.geometric_growth <= policy.min_geometric_growth:
            blockers.append("geometric_growth_not_positive")
        if snapshot.profit_factor <= policy.min_profit_factor:
            blockers.append("profit_factor_not_above_one")
        if snapshot.copyability_rate < policy.min_copyability_rate:
            blockers.append("copyability_rate_below_minimum")
        if snapshot.manipulation_risk > policy.max_manipulation_risk:
            blockers.append("manipulation_risk_too_high")
        if snapshot.side_wallet_risk > policy.max_side_wallet_risk:
            blockers.append("side_wallet_risk_too_high")
        if snapshot.max_drawdown > policy.max_drawdown:
            blockers.append("drawdown_too_high")
        return blockers

    def compare(self, candidate: WalletPerformanceSnapshot, incumbent: WalletPerformanceSnapshot) -> WalletPromotionDecision:
        blockers = self._evidence_blockers(candidate)
        candidate_score = self.risk_adjusted_copyable_score(candidate)
        incumbent_score = self.risk_adjusted_copyable_score(incumbent)
        if candidate.entity_id == incumbent.entity_id:
            blockers.append("same_economic_entity_as_incumbent")
        if candidate.profit_factor < incumbent.profit_factor:
            blockers.append("profit_factor_below_incumbent")
        if candidate.max_drawdown > incumbent.max_drawdown + self.policy.max_drawdown_disadvantage:
            blockers.append("drawdown_materially_worse_than_incumbent")
        if incumbent_score > 0.0:
            ratio = candidate_score / incumbent_score
            if ratio < self.policy.min_superiority_ratio:
                blockers.append("risk_adjusted_superiority_not_proven")
        else:
            ratio = None
            if candidate_score <= 0.0:
                blockers.append("positive_superiority_not_proven")
        return WalletPromotionDecision(
            candidate_wallet=candidate.wallet,
            incumbent_wallet=incumbent.wallet,
            eligible=not blockers,
            blockers=tuple(blockers),
            candidate_score=candidate_score,
            incumbent_score=incumbent_score,
            superiority_ratio=ratio,
        )

    def rankings(self, *, exclude_wallets: Iterable[str] = ()) -> list[dict[str, Any]]:
        excluded = set(exclude_wallets)
        rows: list[dict[str, Any]] = []
        for snapshot in self.latest_snapshots():
            if snapshot.wallet in excluded:
                continue
            blockers = self._evidence_blockers(snapshot)
            rows.append(
                {
                    **snapshot.to_payload(),
                    "risk_adjusted_copyable_score": self.risk_adjusted_copyable_score(snapshot),
                    "promotion_evidence_eligible": not blockers,
                    "blockers": blockers,
                }
            )
        rows.sort(key=lambda row: float(row["risk_adjusted_copyable_score"]), reverse=True)
        return rows

    def current_incumbents(self) -> list[str]:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT wallet FROM wallet_profiles WHERE historically_eligible=1 AND tier IN ('S','A') "
                "ORDER BY tier, wallet"
            ).fetchall()
        return [str(row["wallet"]) for row in rows]

    def propose_next_cohort(self, *, parent_version: str, strategy_version: str) -> dict[str, Any]:
        """Stage at most one superior replacement without touching the active cohort."""
        incumbents = self.current_incumbents()
        incumbent_snapshots = {wallet: self.latest_snapshot(wallet) for wallet in incumbents}
        missing = [wallet for wallet, snapshot in incumbent_snapshots.items() if snapshot is None]
        if not incumbents:
            return {"proposed": False, "blockers": ["no_incumbent_cohort"]}
        if missing:
            return {"proposed": False, "blockers": ["incumbent_forward_evidence_incomplete"], "missing_incumbents": missing}

        incumbent_rows = [snapshot for snapshot in incumbent_snapshots.values() if snapshot is not None]
        weakest = min(incumbent_rows, key=self.risk_adjusted_copyable_score)
        candidates = [snapshot for snapshot in self.latest_snapshots() if snapshot.wallet not in set(incumbents)]
        decisions = [self.compare(candidate, weakest) for candidate in candidates]
        eligible = [decision for decision in decisions if decision.eligible]
        if not eligible:
            return {
                "proposed": False,
                "blockers": ["no_superior_candidate_proven"],
                "weakest_incumbent": weakest.wallet,
                "candidate_decisions": [asdict(decision) for decision in decisions],
            }

        winner = max(eligible, key=lambda decision: decision.candidate_score)
        winner_snapshot = self.latest_snapshot(winner.candidate_wallet)
        assert winner_snapshot is not None
        cohort = [wallet for wallet in incumbents if wallet != weakest.wallet] + [winner.candidate_wallet]
        rationale = {
            "promotion": winner.candidate_wallet,
            "demotion": weakest.wallet,
            "decision": asdict(winner),
            "candidate_snapshot": winner_snapshot.to_payload(),
            "incumbent_snapshot": weakest.to_payload(),
            "policy": asdict(self.policy),
            "active_cohort_mutated": False,
            "activation_boundary": "future immutable strategy cohort only",
        }
        raw = json.dumps(cohort, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(raw.encode()).hexdigest()
        created_at = utcnow().isoformat()
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT OR IGNORE INTO adaptive_wallet_cohorts("
                "strategy_version, created_at, parent_version, cohort_json, rationale_json, cohort_sha256, status) "
                "VALUES (?, ?, ?, ?, ?, ?, 'proposed')",
                (
                    strategy_version,
                    created_at,
                    parent_version,
                    json.dumps(cohort, separators=(",", ":")),
                    json.dumps(rationale, sort_keys=True, separators=(",", ":"), default=str),
                    digest,
                ),
            )
        self.store.append(
            "adaptive_wallet_cohort_proposed",
            created_at,
            {
                "strategy_version": strategy_version,
                "parent_version": parent_version,
                "cohort": cohort,
                "cohort_sha256": digest,
                "promotion": winner.candidate_wallet,
                "demotion": weakest.wallet,
                "active_cohort_mutated": False,
            },
        )
        return {
            "proposed": True,
            "strategy_version": strategy_version,
            "parent_version": parent_version,
            "cohort": cohort,
            "cohort_sha256": digest,
            "promotion": winner.candidate_wallet,
            "demotion": weakest.wallet,
            "decision": asdict(winner),
            "active_cohort_mutated": False,
        }

    def status(self) -> dict[str, Any]:
        incumbents = self.current_incumbents()
        rankings = self.rankings(exclude_wallets=incumbents)
        with self.store._lock:
            proposed = self.store.db.execute(
                "SELECT strategy_version, created_at, parent_version, cohort_json, rationale_json, "
                "cohort_sha256, status FROM adaptive_wallet_cohorts ORDER BY id DESC LIMIT 1"
            ).fetchone()
        latest_proposal = None
        if proposed is not None:
            latest_proposal = dict(proposed)
            latest_proposal["cohort"] = json.loads(str(latest_proposal.pop("cohort_json")))
            latest_proposal["rationale"] = json.loads(str(latest_proposal.pop("rationale_json")))
        return {
            "research_lane": True,
            "paper_only": True,
            "active_forward_cohort_immutable": True,
            "incumbent_wallets": incumbents,
            "observed_wallets": len(self.latest_snapshots()),
            "eligible_challengers": sum(1 for row in rankings if row["promotion_evidence_eligible"]),
            "top_challengers": rankings[:10],
            "latest_proposed_cohort": latest_proposal,
            "promotion_policy": asdict(self.policy),
        }
