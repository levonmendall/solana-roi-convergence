from __future__ import annotations

import contextvars
import json
import math
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from statistics import mean
from typing import Any, Callable, Iterable

from .profit_first_entity_final import FINAL_STRATEGY_VERSION
from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from .wallet_discovery import ContinuousWalletDiscovery


PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
ACTIVE_STRATEGY_MUTATION_ALLOWED = False
HISTORICAL_PROMOTION_AUTHORITY = False
SIGNAL_DECAY_HORIZONS_SECONDS = (1, 2, 5, 10, 20, 30, 60)
MIN_MATURE_FORWARD_SAMPLES = 5
MIN_CHALLENGER_SLOTS = 4

_ORIGINAL_TRACKED_WALLETS: Callable[..., Any] | None = None
_ORIGINAL_RUN_ONCE: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., Any] | None = None
_ORIGINAL_FINAL_CONFIRMATION_CONTEXT: Callable[..., Any] | None = None
_ORIGINAL_FINAL_MANIFEST: Callable[..., Any] | None = None
_ORIGINAL_FINAL_CREATOR_FLOW_STATE: Callable[..., Any] | None = None
_ORIGINAL_FINAL_SELL: Callable[..., Any] | None = None
_ORIGINAL_FINAL_SELLER_ENTITY: Callable[..., Any] | None = None
_UNIVERSE_ATTR = "_roi_v4_wallet_entity_universe"
_CURRENT_EXIT_TOKEN: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "roi_v4_current_exit_token", default=None
)


class WalletRole(str, Enum):
    SCOUT_ALPHA = "scout_alpha"
    CREATOR_ALPHA = "creator_alpha"
    MOMENTUM_ALPHA = "momentum_alpha"
    CONFIRMATION_ALPHA = "confirmation_alpha"
    EXIT_ALPHA = "exit_alpha"
    DISTRIBUTION_WARNING = "distribution_warning_value"
    COPYABLE_ROC = "copyable_return_on_capital"
    SIGNAL_DECAY = "signal_decay"


@dataclass(frozen=True, slots=True)
class SeedEntity:
    name: str
    address: str
    initial_roles: tuple[WalletRole, ...]
    treatment: str


SEED_ENTITIES: tuple[SeedEntity, ...] = (
    SeedEntity(
        "Jijo",
        "4BdKaxN8G6ka4GYtQQWk4G4dZRUTX2vQH9GcXdBREFUk",
        (WalletRole.SCOUT_ALPHA, WalletRole.MOMENTUM_ALPHA, WalletRole.CONFIRMATION_ALPHA, WalletRole.EXIT_ALPHA),
        "high_priority_token_specific_independence",
    ),
    SeedEntity(
        "trunoest",
        "ardinRsN1mNYVeoJWTBsWeYeXvuR9UUDGMsCDKpb6AT",
        (WalletRole.SCOUT_ALPHA, WalletRole.MOMENTUM_ALPHA, WalletRole.CONFIRMATION_ALPHA, WalletRole.EXIT_ALPHA),
        "high_priority_forward_challenger_no_automatic_authority",
    ),
    SeedEntity(
        "decu",
        "4vw54BmAogeRV3vPKWyFet5yf8DTLcREzdSzx4rw9Ud9",
        (WalletRole.CREATOR_ALPHA, WalletRole.MOMENTUM_ALPHA, WalletRole.DISTRIBUTION_WARNING, WalletRole.SCOUT_ALPHA),
        "creator_entity_alpha_not_blanket_veto",
    ),
    SeedEntity(
        "Wugi",
        "862TYSvRYoiHAK3F3WwTRYAfuGiQaGdxedN9AGvRGWo2",
        (WalletRole.CREATOR_ALPHA, WalletRole.MOMENTUM_ALPHA, WalletRole.DISTRIBUTION_WARNING),
        "creator_entity_intelligence_not_assumed_independent",
    ),
    SeedEntity(
        "The Doc",
        "DYAn4XpAkN5mhiXkRB7dGq4Jadnx6XYgu8L5b3WGhbrt",
        (WalletRole.CREATOR_ALPHA, WalletRole.MOMENTUM_ALPHA, WalletRole.EXIT_ALPHA, WalletRole.SCOUT_ALPHA),
        "creator_rapid_rotation_exit_intelligence_token_specific_independence",
    ),
    SeedEntity(
        "Theo",
        "Bi4rd5FH5bYEN8scZ7wevxNZyNmKHdaBcvewdPFxYdLt",
        (WalletRole.MOMENTUM_ALPHA, WalletRole.SIGNAL_DECAY, WalletRole.CONFIRMATION_ALPHA, WalletRole.EXIT_ALPHA),
        "high_frequency_copyability_and_signal_decay_research",
    ),
    SeedEntity(
        "Schoen",
        "5hAgYC8TJCcEZV7LTXAzkTrm7YL29YXyQQJPCNrG84zM",
        (WalletRole.CREATOR_ALPHA, WalletRole.MOMENTUM_ALPHA, WalletRole.EXIT_ALPHA),
        "creator_entity_momentum_research_not_default_independent_confirmation",
    ),
    SeedEntity(
        "Cented",
        "CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o",
        (WalletRole.DISTRIBUTION_WARNING, WalletRole.CREATOR_ALPHA),
        "entity_distribution_market_structure_research_no_headline_pnl_authority",
    ),
)
SEED_BY_ADDRESS = {seed.address: seed for seed in SEED_ENTITIES}


@dataclass(frozen=True, slots=True)
class RoleScore:
    role: WalletRole
    sample_count: int
    mean_residual_return: float | None
    geometric_value: float | None
    positive_rate: float | None
    confidence: float
    score: float | None


def score_role(role: WalletRole, residual_returns: Iterable[float]) -> RoleScore:
    values = [float(value) for value in residual_returns if math.isfinite(float(value))]
    if not values:
        return RoleScore(role, 0, None, None, None, 0.0, None)
    log_terms = [math.log(max(1e-9, 1.0 + value)) for value in values]
    geometric = math.exp(mean(log_terms)) - 1.0
    confidence = min(1.0, math.sqrt(len(values) / 30.0))
    return RoleScore(
        role=role,
        sample_count=len(values),
        mean_residual_return=mean(values),
        geometric_value=geometric,
        positive_rate=sum(value > 0.0 for value in values) / len(values),
        confidence=confidence,
        score=mean(log_terms) * confidence,
    )


def residual_return(*, system_entry_price: float, exit_price: float) -> float:
    """Return available to this system; source-wallet pre-observation gains are excluded."""
    if system_entry_price <= 0.0 or exit_price <= 0.0:
        raise ValueError("prices must be positive")
    return exit_price / system_entry_price - 1.0


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _release_commit() -> str:
    for key in ("RENDER_GIT_COMMIT", "SOLANA_ROI_RELEASE_COMMIT", "GITHUB_SHA"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return "unbound-local-release"


def _ensure_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v4_token_entity_links ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, token_mint TEXT NOT NULL, wallet_a TEXT NOT NULL, wallet_b TEXT NOT NULL, "
            "relationship TEXT NOT NULL, confidence REAL NOT NULL, observed_at TEXT NOT NULL, received_at TEXT NOT NULL, "
            "source TEXT NOT NULL, UNIQUE(token_mint,wallet_a,wallet_b,relationship,observed_at,source))"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v4_token_entity_links_lookup ON "
            "v4_token_entity_links(token_mint,received_at,wallet_a,wallet_b)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v4_entity_signal_context ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_id TEXT NOT NULL, token_mint TEXT NOT NULL, trigger_wallet TEXT NOT NULL, "
            "received_at TEXT NOT NULL, trigger_entity TEXT NOT NULL, creator_wallet TEXT, creator_entity TEXT, "
            "confirmation_wallets_json TEXT NOT NULL, independent_wallets_json TEXT NOT NULL, independent_entities_json TEXT NOT NULL, "
            "relationship_count INTEGER NOT NULL, created_at TEXT NOT NULL, "
            "UNIQUE(epoch_id,token_mint,trigger_wallet,received_at))"
        )


def record_token_entity_link(
    store: Any,
    *,
    token_mint: str,
    wallet_a: str,
    wallet_b: str,
    relationship: str,
    confidence: float,
    observed_at: datetime,
    received_at: datetime,
    source: str,
) -> bool:
    """Persist a prospective token-scoped relationship without changing the legacy global graph."""
    if not token_mint or not wallet_a or not wallet_b or wallet_a == wallet_b:
        return False
    if not 0.0 <= confidence <= 1.0:
        raise ValueError("token entity-link confidence must be between 0 and 1")
    _ensure_schema(store)
    left, right = sorted((wallet_a, wallet_b))
    with store._lock, store.db:
        cursor = store.db.execute(
            "INSERT OR IGNORE INTO v4_token_entity_links("
            "token_mint,wallet_a,wallet_b,relationship,confidence,observed_at,received_at,source) VALUES (?,?,?,?,?,?,?,?)",
            (
                token_mint,
                left,
                right,
                relationship,
                float(confidence),
                observed_at.isoformat(),
                received_at.isoformat(),
                source,
            ),
        )
    return cursor.rowcount == 1


class TokenScopedEntityResolver:
    """Resolve identity for one token using only evidence available at the decision time."""

    def __init__(self, discovery: ContinuousWalletDiscovery):
        self.discovery = discovery
        self.store = discovery.store
        _ensure_schema(self.store)

    def _stable_anchor(self, wallet: str) -> str | None:
        try:
            profile = self.discovery.entity_resolver.registry.get(wallet)
            value = getattr(profile, "entity_id", None) if profile is not None else None
            return str(value) if value else None
        except Exception:
            return None

    def _stored_links(self, token_mint: str, *, as_of: datetime) -> list[tuple[str, str]]:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT wallet_a,wallet_b FROM v4_token_entity_links "
                "WHERE token_mint=? AND confidence>=? AND observed_at<=? AND received_at<=? ORDER BY id",
                (
                    token_mint,
                    float(getattr(self.discovery.entity_resolver, "min_confidence", 0.95)),
                    as_of.isoformat(),
                    as_of.isoformat(),
                ),
            ).fetchall()
        return [(str(row["wallet_a"]), str(row["wallet_b"])) for row in rows]

    def _active_wallets(self, token_mint: str, *, as_of: datetime) -> set[str]:
        try:
            with self.store._lock:
                rows = self.store.db.execute(
                    "SELECT DISTINCT wallet FROM wallet_discovery_forward_observations "
                    "WHERE token_mint=? AND received_at<=? ORDER BY wallet LIMIT 500",
                    (token_mint, as_of.isoformat()),
                ).fetchall()
            return {str(row["wallet"]) for row in rows}
        except Exception:
            return set()

    def _contextualize_existing_links(
        self,
        token_mint: str,
        addresses: Iterable[str],
        *,
        as_of: datetime,
        creator_wallet: str | None = None,
    ) -> None:
        """Project only contemporaneously relevant legacy links into this token's graph.

        The legacy entity plane is intentionally left unchanged. A legacy relationship
        becomes usable by final-v4 independence logic only when both endpoints are
        participating in this token by the decision time, or when one endpoint is this
        token's known creator/deployer. The projection itself is append-only and
        token-scoped, so evidence learned later cannot leak backward.
        """
        requested = {str(value) for value in addresses if str(value)}
        active = self._active_wallets(token_mint, as_of=as_of)
        anchors = set(requested)
        if creator_wallet:
            anchors.add(creator_wallet)
        min_confidence = float(getattr(self.discovery.entity_resolver, "min_confidence", 0.95))
        for wallet in sorted(anchors):
            try:
                rows = self.store.entity_neighbors(
                    wallet,
                    as_of_received_at=as_of.isoformat(),
                    min_confidence=min_confidence,
                )
            except Exception:
                continue
            for row in rows:
                left = str(row["wallet_a"])
                right = str(row["wallet_b"])
                other = right if left == wallet else left
                relevant = (
                    (wallet in active and other in active)
                    or (creator_wallet is not None and (wallet == creator_wallet or other == creator_wallet) and other in (active | requested))
                )
                if not relevant:
                    continue
                try:
                    observed_at = datetime.fromisoformat(str(row["observed_at"]))
                    received_at = datetime.fromisoformat(str(row["received_at"]))
                    relationship = str(row["relationship"])
                    confidence = float(row["confidence"])
                    source = str(row["source"])
                except Exception:
                    continue
                record_token_entity_link(
                    self.store,
                    token_mint=token_mint,
                    wallet_a=left,
                    wallet_b=right,
                    relationship=f"contextual:{relationship}",
                    confidence=confidence,
                    observed_at=observed_at,
                    received_at=received_at,
                    source=f"v4-token-context:{source}",
                )

    def _links(
        self,
        token_mint: str,
        *,
        as_of: datetime,
        addresses: Iterable[str] = (),
        creator_wallet: str | None = None,
    ) -> list[tuple[str, str]]:
        self._contextualize_existing_links(
            token_mint,
            addresses,
            as_of=as_of,
            creator_wallet=creator_wallet,
        )
        return self._stored_links(token_mint, as_of=as_of)

    def components(
        self,
        token_mint: str,
        addresses: Iterable[str],
        *,
        as_of: datetime,
        creator_wallet: str | None = None,
    ) -> tuple[dict[str, str], int]:
        members = {str(address) for address in addresses if str(address)}
        links = self._links(
            token_mint,
            as_of=as_of,
            addresses=members,
            creator_wallet=creator_wallet,
        )
        for left, right in links:
            if left in members or right in members:
                members.add(left)
                members.add(right)
        parent = {member: member for member in members}

        def find(item: str) -> str:
            parent.setdefault(item, item)
            while parent[item] != item:
                parent[item] = parent[parent[item]]
                item = parent[item]
            return item

        def union(left: str, right: str) -> None:
            a, b = find(left), find(right)
            if a != b:
                parent[max(a, b)] = min(a, b)

        by_anchor: dict[str, str] = {}
        for member in sorted(members):
            anchor = self._stable_anchor(member)
            if anchor:
                previous = by_anchor.get(anchor)
                if previous:
                    union(previous, member)
                else:
                    by_anchor[anchor] = member
        used_links = 0
        for left, right in links:
            if left in parent and right in parent:
                union(left, right)
                used_links += 1

        groups: dict[str, list[str]] = {}
        for member in sorted(parent):
            groups.setdefault(find(member), []).append(member)
        result: dict[str, str] = {}
        for group_members in groups.values():
            anchors = sorted({anchor for member in group_members if (anchor := self._stable_anchor(member))})
            entity_id = anchors[0] if anchors else "token-graph:" + token_mint + ":" + sorted(group_members)[0]
            for member in group_members:
                result[member] = entity_id
        return result, used_links

    def resolve_context(
        self,
        token_mint: str,
        trigger_wallet: str,
        creator_wallet: str | None,
        confirmation_wallets: Iterable[str],
        *,
        as_of: datetime,
    ) -> tuple[str, str | None, tuple[str, ...], tuple[str, ...], int]:
        confirmations = tuple(dict.fromkeys(str(value) for value in confirmation_wallets if str(value)))
        addresses = (trigger_wallet, creator_wallet or "", *confirmations)
        entities, relation_count = self.components(
            token_mint,
            addresses,
            as_of=as_of,
            creator_wallet=creator_wallet,
        )
        trigger_entity = entities.get(trigger_wallet, f"token-graph:{token_mint}:{trigger_wallet}")
        creator_entity = entities.get(creator_wallet) if creator_wallet else None
        excluded = {trigger_entity}
        if creator_entity:
            excluded.add(creator_entity)
        independent_wallets: list[str] = []
        independent_entities: list[str] = []
        seen_entities: set[str] = set()
        for wallet in confirmations:
            entity = entities.get(wallet, f"token-graph:{token_mint}:{wallet}")
            if entity in excluded or entity in seen_entities:
                continue
            seen_entities.add(entity)
            independent_wallets.append(wallet)
            independent_entities.append(entity)
        return (
            trigger_entity,
            creator_entity,
            tuple(independent_wallets),
            tuple(independent_entities),
            relation_count,
        )

    def component(self, token_mint: str, wallet: str, *, as_of: datetime) -> set[str]:
        links = self._links(
            token_mint,
            as_of=as_of,
            addresses=(wallet,),
            creator_wallet=wallet,
        )
        members = {wallet}
        changed = True
        while changed:
            changed = False
            for left, right in links:
                if left in members and right not in members:
                    members.add(right)
                    changed = True
                elif right in members and left not in members:
                    members.add(left)
                    changed = True
        return members


class WalletEntityUniverseV4:
    """Bounded observation-universe governance with no current-cohort mutation authority."""

    def __init__(self, discovery: ContinuousWalletDiscovery):
        self.discovery = discovery
        self.store = discovery.store
        self.token_entities = TokenScopedEntityResolver(discovery)
        _ensure_schema(self.store)
        self.ensure_seed_candidates()

    def ensure_seed_candidates(self) -> None:
        now = self.discovery.now_fn().isoformat()
        with self.store._lock, self.store.db:
            for seed in SEED_ENTITIES:
                self.store.db.execute(
                    "INSERT OR IGNORE INTO wallet_discovery_candidates("
                    "wallet,first_seen_at,last_seen_at,broad_sample_count,distinct_token_count,state,forward_started_at,last_polled_at) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (seed.address, now, now, 0, 0, "tracking", now, now),
                )
                self.store.db.execute(
                    "UPDATE wallet_discovery_candidates SET state='tracking', "
                    "last_signature=CASE WHEN forward_started_at IS NULL THEN NULL ELSE last_signature END, "
                    "forward_started_at=COALESCE(forward_started_at,?), next_screen_at=NULL "
                    "WHERE wallet=? AND state NOT IN ('incumbent_tracking','tracking')",
                    (now, seed.address),
                )

    def _epoch_id(self) -> str | None:
        try:
            with self.store._lock:
                row = self.store.db.execute(
                    "SELECT epoch_id FROM profit_first_final_epochs WHERE strategy_version=? AND release_commit=? "
                    "ORDER BY started_at DESC LIMIT 1",
                    (FINAL_STRATEGY_VERSION, _release_commit()),
                ).fetchone()
                if row is None:
                    row = self.store.db.execute(
                        "SELECT epoch_id FROM profit_first_final_epochs WHERE strategy_version=? ORDER BY started_at DESC LIMIT 1",
                        (FINAL_STRATEGY_VERSION,),
                    ).fetchone()
            return str(row["epoch_id"]) if row is not None else None
        except Exception:
            return None

    def _trigger_outcomes(self, epoch_id: str) -> list[dict[str, Any]]:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT source_signature,trigger_wallet,token_mint,lane,context_json,net_return,signal_to_entry_seconds,"
                "exit_signature,exit_reason FROM profit_first_final_outcomes "
                "WHERE epoch_id=? AND evidence_phase='forward' ORDER BY id",
                (epoch_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def _signal_contexts(self, epoch_id: str) -> list[dict[str, Any]]:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT * FROM v4_entity_signal_context WHERE epoch_id=? ORDER BY id",
                (epoch_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def _exit_sellers(self, epoch_id: str) -> dict[str, tuple[str, dict[str, Any], float]]:
        result: dict[str, tuple[str, dict[str, Any], float]] = {}
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT s.source_signature,s.seller_wallet,s.features_json,s.signal_json "
                "FROM profit_first_final_exit_signals s WHERE s.epoch_id=?",
                (epoch_id,),
            ).fetchall()
        for row in rows:
            try:
                signal = json.loads(str(row["signal_json"]))
                features = json.loads(str(row["features_json"]))
                result[str(row["source_signature"])] = (
                    str(row["seller_wallet"]),
                    features,
                    float(signal.get("urgency_score") or 0.0),
                )
            except Exception:
                continue
        return result

    def role_returns(self) -> dict[str, dict[WalletRole, list[float]]]:
        epoch_id = self._epoch_id()
        result: dict[str, dict[WalletRole, list[float]]] = {}
        if not epoch_id:
            return result

        def add(wallet: str, role: WalletRole, value: float) -> None:
            result.setdefault(wallet, {}).setdefault(role, []).append(float(value))

        outcomes = self._trigger_outcomes(epoch_id)
        seen_copyable: set[tuple[str, str]] = set()
        seen_decay: set[tuple[str, str]] = set()
        for row in outcomes:
            wallet = str(row["trigger_wallet"])
            signature = str(row["source_signature"])
            lane = str(row["lane"])
            value = float(row["net_return"])
            observation_key = (wallet, signature)
            if observation_key not in seen_copyable:
                add(wallet, WalletRole.COPYABLE_ROC, value)
                seen_copyable.add(observation_key)
            if lane == "clean_scout_alpha":
                add(wallet, WalletRole.SCOUT_ALPHA, value)
            if lane == "creator_insider_continuation":
                add(wallet, WalletRole.CREATOR_ALPHA, value)
            if lane in {"elite_wallet_continuation", "entity_flow_momentum"}:
                add(wallet, WalletRole.MOMENTUM_ALPHA, value)
            lag = float(row.get("signal_to_entry_seconds") or 0.0)
            if lag >= 5.0 and observation_key not in seen_decay:
                add(wallet, WalletRole.SIGNAL_DECAY, value)
                seen_decay.add(observation_key)

        contexts = self._signal_contexts(epoch_id)
        with self.store._lock:
            trial_rows = self.store.db.execute(
                "SELECT source_signature,token_mint,trigger_wallet,received_at FROM profit_first_final_trials "
                "WHERE epoch_id=? AND lane='entity_flow_momentum'",
                (epoch_id,),
            ).fetchall()
            outcome_rows = self.store.db.execute(
                "SELECT source_signature,net_return FROM profit_first_final_outcomes "
                "WHERE epoch_id=? AND lane='entity_flow_momentum' AND evidence_phase='forward'",
                (epoch_id,),
            ).fetchall()
        outcome_by_sig = {str(row["source_signature"]): float(row["net_return"]) for row in outcome_rows}
        context_index = {
            (str(row["token_mint"]), str(row["trigger_wallet"]), str(row["received_at"])): row
            for row in contexts
        }
        for trial in trial_rows:
            value = outcome_by_sig.get(str(trial["source_signature"]))
            if value is None:
                continue
            context = context_index.get(
                (str(trial["token_mint"]), str(trial["trigger_wallet"]), str(trial["received_at"]))
            )
            if context is None:
                continue
            try:
                wallets = json.loads(str(context["independent_wallets_json"]))
            except Exception:
                wallets = []
            for wallet in wallets:
                add(str(wallet), WalletRole.CONFIRMATION_ALPHA, value)

        sellers = self._exit_sellers(epoch_id)
        seen_exit: set[tuple[str, str]] = set()
        for row in outcomes:
            seller = sellers.get(str(row["exit_signature"]))
            if seller is None:
                continue
            seller_wallet, features, urgency = seller
            exit_key = (seller_wallet, str(row["source_signature"]))
            if exit_key in seen_exit:
                continue
            seen_exit.add(exit_key)
            value = float(row["net_return"])
            add(seller_wallet, WalletRole.EXIT_ALPHA, value)
            if features.get("creator_distribution") or features.get("linked_entity_distribution"):
                add(seller_wallet, WalletRole.DISTRIBUTION_WARNING, -value * min(1.0, urgency / 3.0))
        return result

    def role_scores(self) -> dict[str, dict[WalletRole, RoleScore]]:
        return {
            wallet: {role: score_role(role, values) for role, values in roles.items()}
            for wallet, roles in self.role_returns().items()
        }

    def _forward_priority(self, wallet: str, scores: dict[str, dict[WalletRole, RoleScore]]) -> tuple[int, float, int]:
        role_scores = scores.get(wallet, {})
        available = [item for item in role_scores.values() if item.score is not None]
        samples = max((item.sample_count for item in available), default=0)
        best = max((float(item.score) for item in available if item.score is not None), default=float("-inf"))
        mature = 1 if samples >= MIN_MATURE_FORWARD_SAMPLES else 0
        return mature, best, samples

    def _wallet_signal_sets(self) -> dict[str, set[str]]:
        epoch_id = self._epoch_id()
        if not epoch_id:
            return {}
        sets: dict[str, set[str]] = {}
        for row in self._trigger_outcomes(epoch_id):
            wallet = str(row["trigger_wallet"])
            token = str(row.get("token_mint") or "")
            signature = str(row.get("source_signature") or "")
            if token and signature:
                sets.setdefault(wallet, set()).add(f"{token}|{signature}")
        return sets

    @staticmethod
    def _overlap(left: set[str], right: set[str]) -> float:
        if not left or not right:
            return 0.0
        union = left | right
        return len(left & right) / len(union) if union else 0.0

    def signal_redundancy(self, wallets: Iterable[str]) -> list[dict[str, Any]]:
        sets = self._wallet_signal_sets()
        ordered = list(dict.fromkeys(str(wallet) for wallet in wallets))
        rows: list[dict[str, Any]] = []
        for index, left in enumerate(ordered):
            for right in ordered[index + 1:]:
                overlap = self._overlap(sets.get(left, set()), sets.get(right, set()))
                if overlap <= 0.0:
                    continue
                rows.append({"wallet_a": left, "wallet_b": right, "forward_signal_overlap": overlap})
        rows.sort(key=lambda row: float(row["forward_signal_overlap"]), reverse=True)
        return rows[:50]

    def regime_wallet_value(self) -> dict[str, list[dict[str, Any]]]:
        epoch_id = self._epoch_id()
        if not epoch_id:
            return {}
        grouped: dict[str, dict[str, list[float]]] = {}
        seen: set[tuple[str, str]] = set()
        for row in self._trigger_outcomes(epoch_id):
            wallet = str(row["trigger_wallet"])
            signature = str(row["source_signature"])
            key = (wallet, signature)
            if key in seen:
                continue
            seen.add(key)
            try:
                context = json.loads(str(row.get("context_json") or "{}"))
                regime = str(context.get("regime") or "unknown")
            except Exception:
                regime = "unknown"
            grouped.setdefault(regime, {}).setdefault(wallet, []).append(float(row["net_return"]))
        result: dict[str, list[dict[str, Any]]] = {}
        for regime, wallets in grouped.items():
            scored = []
            for wallet, values in wallets.items():
                score = score_role(WalletRole.COPYABLE_ROC, values)
                if score.score is None:
                    continue
                scored.append({"wallet": wallet, **asdict(score)})
            scored.sort(key=lambda row: (float(row["score"]), int(row["sample_count"])), reverse=True)
            result[regime] = scored[:5]
        return result

    def select_tracked_wallets(self) -> list[str]:
        self.ensure_seed_candidates()
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT wallet,state,historical_return_on_capital,historical_profit_factor,broad_sample_count "
                "FROM wallet_discovery_candidates WHERE state IN ('incumbent_tracking','tracking')"
            ).fetchall()
        incumbents = [str(row["wallet"]) for row in rows if str(row["state"]) == "incumbent_tracking"]
        candidates = [row for row in rows if str(row["state"]) == "tracking"]
        max_slots = max(1, int(self.discovery.policy.max_tracked_challengers))
        scores = self.role_scores()

        def sort_key(row: Any) -> tuple[float, ...]:
            wallet = str(row["wallet"])
            mature, forward_score, samples = self._forward_priority(wallet, scores)
            seed = SEED_BY_ADDRESS.get(wallet)
            bootstrap = 1.0 if seed is not None and samples < MIN_MATURE_FORWARD_SAMPLES else 0.0
            return (
                float(mature),
                forward_score if math.isfinite(forward_score) else -999.0,
                float(samples),
                bootstrap,
                float(row["historical_return_on_capital"] or 0.0),
                float(row["historical_profit_factor"] or 0.0),
                float(row["broad_sample_count"] or 0),
            )

        challengers = sorted(
            [row for row in candidates if str(row["wallet"]) not in SEED_BY_ADDRESS],
            key=sort_key,
            reverse=True,
        )
        selected: list[str] = []
        challenger_slots = min(len(challengers), min(MIN_CHALLENGER_SLOTS, max_slots))
        for row in challengers[:challenger_slots]:
            selected.append(str(row["wallet"]))

        signal_sets = self._wallet_signal_sets()

        def numeric_priority(row: Any, already_selected: list[str]) -> float:
            wallet = str(row["wallet"])
            mature, forward_score, samples = self._forward_priority(wallet, scores)
            seed = SEED_BY_ADDRESS.get(wallet)
            bootstrap = 1.0 if seed is not None and samples < MIN_MATURE_FORWARD_SAMPLES else 0.0
            value = (
                mature * 1000.0
                + (forward_score if math.isfinite(forward_score) else -9.99) * 100.0
                + min(samples, 100) * 0.10
                + bootstrap
                + max(-10.0, min(10.0, float(row["historical_return_on_capital"] or 0.0))) * 0.01
                + math.log1p(max(0.0, float(row["historical_profit_factor"] or 0.0))) * 0.001
            )
            redundancy = max(
                (
                    self._overlap(signal_sets.get(wallet, set()), signal_sets.get(other, set()))
                    for other in already_selected
                ),
                default=0.0,
            )
            return value - 2.0 * redundancy

        remaining_rows = [row for row in candidates if str(row["wallet"]) not in set(selected)]
        while len(selected) < max_slots and remaining_rows:
            best = max(remaining_rows, key=lambda row: numeric_priority(row, selected))
            selected.append(str(best["wallet"]))
            remaining_rows.remove(best)
        return list(dict.fromkeys((*incumbents, *selected)))

    def _leader_rows(self, role: WalletRole, limit: int = 5) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for wallet, scores in self.role_scores().items():
            score = scores.get(role)
            if score is None or score.score is None:
                continue
            seed = SEED_BY_ADDRESS.get(wallet)
            rows.append({"wallet": wallet, "seed_name": seed.name if seed else None, **asdict(score)})
        rows.sort(key=lambda row: (float(row["score"]), int(row["sample_count"])), reverse=True)
        return rows[:limit]

    def _current_entity(self, wallet: str, at: datetime) -> str:
        try:
            profile = self.discovery.entity_resolver.registry.get(wallet)
            fallback = getattr(profile, "entity_id", None) if profile is not None else None
            return str(
                self.discovery.entity_resolver.entity_id_for(
                    wallet,
                    fallback_entity_id=fallback,
                    as_of=at,
                )
            )
        except Exception:
            return f"address:{wallet}"

    def status(self) -> dict[str, Any]:
        self.ensure_seed_candidates()
        at = self.discovery.now_fn()
        selected = self.select_tracked_wallets()
        scores = self.role_scores()
        with self.store._lock:
            candidate_rows = self.store.db.execute(
                "SELECT wallet,state,broad_sample_count,historical_closed_episodes,historical_return_on_capital,"
                "historical_profit_factor,forward_started_at FROM wallet_discovery_candidates"
            ).fetchall()
            relationship_count = int(
                self.store.db.execute(
                    "SELECT COUNT(*) FROM v4_token_entity_links WHERE observed_at<=? AND received_at<=?",
                    (at.isoformat(), at.isoformat()),
                ).fetchone()[0]
            )
            relationship_rows = self.store.db.execute(
                "SELECT token_mint,wallet_a,wallet_b,relationship,confidence,observed_at,received_at "
                "FROM v4_token_entity_links WHERE observed_at<=? AND received_at<=? ORDER BY id DESC LIMIT 50",
                (at.isoformat(), at.isoformat()),
            ).fetchall()

        all_wallets = {str(row["wallet"]) for row in candidate_rows}
        observed_wallets = {
            str(row["wallet"])
            for row in candidate_rows
            if int(row["broad_sample_count"] or 0) > 0
        }
        try:
            with self.store._lock:
                observed_wallets.update(
                    str(row["wallet"])
                    for row in self.store.db.execute(
                        "SELECT DISTINCT wallet FROM wallet_discovery_forward_observations"
                    ).fetchall()
                )
        except Exception:
            pass
        entity_ids = {self._current_entity(wallet, at) for wallet in all_wallets}
        selected_entities = {wallet: self._current_entity(wallet, at) for wallet in selected}

        current_roles: list[dict[str, Any]] = []
        forward_counts: dict[str, int] = {}
        blockers: list[dict[str, Any]] = []
        for wallet in selected:
            wallet_scores = scores.get(wallet, {})
            ranked = sorted(
                (item for item in wallet_scores.values() if item.score is not None),
                key=lambda item: (float(item.score), item.sample_count),
                reverse=True,
            )
            seed = SEED_BY_ADDRESS.get(wallet)
            best = ranked[0] if ranked else None
            total_samples = max((item.sample_count for item in wallet_scores.values()), default=0)
            forward_counts[wallet] = total_samples
            current_roles.append(
                {
                    "wallet": wallet,
                    "entity_id": selected_entities[wallet],
                    "seed_name": seed.name if seed else None,
                    "current_role": best.role.value if best is not None else None,
                    "initial_role_hypotheses": [item.value for item in seed.initial_roles] if seed else [],
                    "role_is_forward_evidence_backed": best is not None,
                }
            )
            wallet_blockers: list[str] = []
            if total_samples < MIN_MATURE_FORWARD_SAMPLES:
                wallet_blockers.append("insufficient_forward_role_samples")
            if best is None or best.score is None or best.score <= 0.0:
                wallet_blockers.append("no_positive_forward_geometric_value")
            if wallet_blockers:
                blockers.append({"wallet": wallet, "blockers": wallet_blockers})

        seed_addresses = set(SEED_BY_ADDRESS)
        discovered = [wallet for wallet in selected if wallet not in seed_addresses]
        active_seeds = [
            {
                "name": SEED_BY_ADDRESS[wallet].name,
                "address": wallet,
                "initial_roles": [role.value for role in SEED_BY_ADDRESS[wallet].initial_roles],
            }
            for wallet in selected
            if wallet in seed_addresses
        ]
        historical_rows = [
            {
                "wallet": str(row["wallet"]),
                "closed_episodes": int(row["historical_closed_episodes"] or 0),
                "return_on_capital": float(row["historical_return_on_capital"] or 0.0),
                "profit_factor": float(row["historical_profit_factor"] or 0.0),
            }
            for row in candidate_rows
            if int(row["historical_closed_episodes"] or 0) > 0
        ]

        return {
            "strategy_version": FINAL_STRATEGY_VERSION,
            "architecture": "large_observation_universe_to_token_specific_economic_entities_to_role_alpha_to_dynamic_high_priority_set",
            "total_observed_addresses": len(observed_wallets),
            "known_candidate_addresses": len(all_wallets),
            "resolved_economic_entities": len(entity_ids),
            "high_priority_entities": sorted(set(selected_entities.values())),
            "high_priority_entity_count": len(set(selected_entities.values())),
            "tracking_capacity_limit": int(self.discovery.policy.max_tracked_challengers),
            "active_seed_entities": active_seeds,
            "discovered_challengers": discovered,
            "entity_relationships": [dict(row) for row in relationship_rows],
            "point_in_time_relationship_count": relationship_count,
            "scout_alpha_leaders": self._leader_rows(WalletRole.SCOUT_ALPHA),
            "creator_alpha_leaders": self._leader_rows(WalletRole.CREATOR_ALPHA),
            "momentum_alpha_leaders": self._leader_rows(WalletRole.MOMENTUM_ALPHA),
            "confirmation_alpha_leaders": self._leader_rows(WalletRole.CONFIRMATION_ALPHA),
            "exit_alpha_leaders": self._leader_rows(WalletRole.EXIT_ALPHA),
            "distribution_warning_leaders": self._leader_rows(WalletRole.DISTRIBUTION_WARNING),
            "copyable_roc_leaders": self._leader_rows(WalletRole.COPYABLE_ROC),
            "signal_decay_leaders": self._leader_rows(WalletRole.SIGNAL_DECAY),
            "regime_wallet_value": self.regime_wallet_value(),
            "signal_redundancy": self.signal_redundancy(selected),
            "current_role_for_high_priority_entity": current_roles,
            "entity_independence_state": "token_specific_point_in_time_no_permanent_wallet_label",
            "forward_sample_counts": forward_counts,
            "candidate_promotion_blockers": blockers,
            "historical_evidence": {
                "wallets_with_historical_screen_evidence": len(historical_rows),
                "sample": historical_rows[:10],
                "promotion_authority": False,
            },
            "prospective_evidence": {
                "release_commit": _release_commit(),
                "role_scored_wallets": len(scores),
                "promotion_authority_requires_current_final_forward_evidence": True,
            },
            "named_seed_is_permanent_whitelist": False,
            "challengers_can_replace_seeds_for_future_influence": True,
            "creator_association_automatic_veto": False,
            "duplicate_confirmation_collapsed_by_token_entity": True,
            "pre_observation_gain_has_role_score_authority": False,
            "active_strategy_mutation_allowed": False,
            "historical_promotion_authority": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
            "research_yields_to_existing_tracking_capacity": True,
        }


def _universe(discovery: ContinuousWalletDiscovery) -> WalletEntityUniverseV4:
    current = getattr(discovery, _UNIVERSE_ATTR, None)
    if isinstance(current, WalletEntityUniverseV4):
        return current
    current = WalletEntityUniverseV4(discovery)
    setattr(discovery, _UNIVERSE_ATTR, current)
    return current


def _record_signal_context(
    adapter: FinalProfitFirstResearchAdapter,
    *,
    token_mint: str,
    trigger_wallet: str,
    creator_wallet: str | None,
    received_at: datetime,
    confirmations: tuple[str, ...],
    independent_wallets: tuple[str, ...],
    independent_entities: tuple[str, ...],
    trigger_entity: str,
    creator_entity: str | None,
    relationship_count: int,
) -> None:
    _ensure_schema(adapter.store)
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "INSERT OR IGNORE INTO v4_entity_signal_context("
            "epoch_id,token_mint,trigger_wallet,received_at,trigger_entity,creator_wallet,creator_entity,"
            "confirmation_wallets_json,independent_wallets_json,independent_entities_json,relationship_count,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                adapter.epoch_id,
                token_mint,
                trigger_wallet,
                received_at.isoformat(),
                trigger_entity,
                creator_wallet,
                creator_entity,
                json.dumps(confirmations, separators=(",", ":")),
                json.dumps(independent_wallets, separators=(",", ":")),
                json.dumps(independent_entities, separators=(",", ":")),
                relationship_count,
                _utcnow().isoformat(),
            ),
        )


def _manifest_with_v4_wallet_universe(self: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    if _ORIGINAL_FINAL_MANIFEST is None:
        raise RuntimeError("v4 wallet/entity manifest wrapper not installed")
    payload = _ORIGINAL_FINAL_MANIFEST(self)
    payload.update(
        {
            "wallet_entity_universe": "broad_continuous_discovery_dynamic_high_priority_economic_entities",
            "wallet_entity_seed_addresses": [seed.address for seed in SEED_ENTITIES],
            "wallet_entity_seed_role_hypotheses_only": True,
            "entity_resolution": "token_specific_append_only_point_in_time_plus_explicit_identity_anchors",
            "entity_independence_is_token_specific": True,
            "related_addresses_count_as_one_confirmation": True,
            "dynamic_wallet_influence_from_final_forward_evidence": True,
            "active_strategy_mutation_allowed": False,
            "historical_evidence_promotion_authority": False,
        }
    )
    return payload


def _confirmation_context_token_specific(
    self: FinalProfitFirstResearchAdapter,
    token_mint: str,
    wallet: str,
    creator: str | None,
    at: datetime,
) -> tuple[str, str | None, int]:
    confirmations = tuple(self.execution._confirmations(token_mint, at, wallet))
    resolver = _universe(self.discovery).token_entities
    trigger_entity, creator_entity, independent_wallets, independent_entities, link_count = resolver.resolve_context(
        token_mint,
        wallet,
        creator,
        confirmations,
        as_of=at,
    )
    _record_signal_context(
        self,
        token_mint=token_mint,
        trigger_wallet=wallet,
        creator_wallet=creator,
        received_at=at,
        confirmations=confirmations,
        independent_wallets=independent_wallets,
        independent_entities=independent_entities,
        trigger_entity=trigger_entity,
        creator_entity=creator_entity,
        relationship_count=link_count,
    )
    return trigger_entity, creator_entity, len(independent_entities)


def _creator_flow_state_token_specific(
    self: FinalProfitFirstResearchAdapter,
    token_mint: str,
    creator: str | None,
    at: datetime,
) -> str:
    if not creator:
        return "neutral"
    members = sorted(_universe(self.discovery).token_entities.component(token_mint, creator, as_of=at))
    if not members:
        members = [creator]
    placeholders = ",".join("?" for _ in members)
    from datetime import timedelta
    start = (at - timedelta(minutes=10)).isoformat()
    sql = (
        "SELECT side,SUM(token_amount) amount FROM wallet_discovery_forward_observations "
        f"WHERE token_mint=? AND wallet IN ({placeholders}) AND received_at>=? AND received_at<=? GROUP BY side"
    )
    params: tuple[Any, ...] = (token_mint, *members, start, at.isoformat())
    with self.store._lock:
        rows = self.store.db.execute(sql, params).fetchall()
    flow = {str(row["side"]): float(row["amount"] or 0.0) for row in rows}
    buys, sells = flow.get("buy", 0.0), flow.get("sell", 0.0)
    if buys > sells * 1.10 and buys > 0:
        return "accumulating"
    if sells > buys * 1.10 and sells > 0:
        return "distributing"
    return "neutral"


async def _sell_with_token_context(self: FinalProfitFirstResearchAdapter, row: dict[str, Any]) -> None:
    if _ORIGINAL_FINAL_SELL is None:
        raise RuntimeError("v4 token-specific sell context not installed")
    token = _CURRENT_EXIT_TOKEN.set(str(row.get("token_mint") or ""))
    try:
        await _ORIGINAL_FINAL_SELL(self, row)
    finally:
        _CURRENT_EXIT_TOKEN.reset(token)


def _seller_entity_token_specific(
    self: FinalProfitFirstResearchAdapter,
    seller: str,
    creator_wallet: str | None,
    at: datetime,
) -> tuple[str, str | None]:
    token_mint = _CURRENT_EXIT_TOKEN.get()
    if not token_mint:
        if _ORIGINAL_FINAL_SELLER_ENTITY is None:
            return f"entity:{seller}", f"entity:{creator_wallet}" if creator_wallet else None
        return _ORIGINAL_FINAL_SELLER_ENTITY(self, seller, creator_wallet, at)
    resolver = _universe(self.discovery).token_entities
    entities, _ = resolver.components(
        token_mint,
        (seller, creator_wallet or ""),
        as_of=at,
        creator_wallet=creator_wallet,
    )
    return (
        entities.get(seller, f"token-graph:{token_mint}:{seller}"),
        entities.get(creator_wallet) if creator_wallet else None,
    )


async def _run_once_with_v4_universe(self: ContinuousWalletDiscovery) -> dict[str, Any]:
    if _ORIGINAL_RUN_ONCE is None:
        raise RuntimeError("v4 wallet/entity universe not installed")
    _universe(self).ensure_seed_candidates()
    return await _ORIGINAL_RUN_ONCE(self)


def _tracked_wallets_with_v4_universe(self: ContinuousWalletDiscovery) -> list[str]:
    return _universe(self).select_tracked_wallets()


def _status_with_v4_universe(self: ContinuousWalletDiscovery) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("v4 wallet/entity universe not installed")
    payload = _ORIGINAL_STATUS(self)
    try:
        payload["wallet_entity_intelligence_v4"] = _universe(self).status()
    except Exception as exc:
        payload["wallet_entity_intelligence_v4"] = {
            "strategy_version": FINAL_STRATEGY_VERSION,
            "failed_closed": True,
            "error": f"{type(exc).__name__}: wallet/entity intelligence status unavailable",
            "active_strategy_mutation_allowed": False,
            "historical_promotion_authority": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    return payload


def install_v4_wallet_entity_universe() -> None:
    global _ORIGINAL_TRACKED_WALLETS
    global _ORIGINAL_RUN_ONCE
    global _ORIGINAL_STATUS
    global _ORIGINAL_FINAL_CONFIRMATION_CONTEXT
    global _ORIGINAL_FINAL_MANIFEST
    global _ORIGINAL_FINAL_CREATOR_FLOW_STATE
    global _ORIGINAL_FINAL_SELL
    global _ORIGINAL_FINAL_SELLER_ENTITY

    if getattr(ContinuousWalletDiscovery._tracked_wallets, "_roi_v4_wallet_entity_universe", False):
        return

    _ORIGINAL_TRACKED_WALLETS = ContinuousWalletDiscovery._tracked_wallets
    _ORIGINAL_RUN_ONCE = ContinuousWalletDiscovery.run_once
    _ORIGINAL_STATUS = ContinuousWalletDiscovery.status
    _ORIGINAL_FINAL_CONFIRMATION_CONTEXT = FinalProfitFirstResearchAdapter._confirmation_context
    _ORIGINAL_FINAL_MANIFEST = FinalProfitFirstResearchAdapter._manifest
    _ORIGINAL_FINAL_CREATOR_FLOW_STATE = FinalProfitFirstResearchAdapter._creator_flow_state
    _ORIGINAL_FINAL_SELL = FinalProfitFirstResearchAdapter._sell
    _ORIGINAL_FINAL_SELLER_ENTITY = FinalProfitFirstResearchAdapter._seller_entity

    ContinuousWalletDiscovery._tracked_wallets = _tracked_wallets_with_v4_universe  # type: ignore[method-assign]
    ContinuousWalletDiscovery.run_once = _run_once_with_v4_universe  # type: ignore[method-assign]
    ContinuousWalletDiscovery.status = _status_with_v4_universe  # type: ignore[method-assign]

    FinalProfitFirstResearchAdapter._manifest = _manifest_with_v4_wallet_universe  # type: ignore[method-assign]
    FinalProfitFirstResearchAdapter._confirmation_context = _confirmation_context_token_specific  # type: ignore[method-assign]
    FinalProfitFirstResearchAdapter._creator_flow_state = _creator_flow_state_token_specific  # type: ignore[method-assign]
    FinalProfitFirstResearchAdapter._sell = _sell_with_token_context  # type: ignore[method-assign]
    FinalProfitFirstResearchAdapter._seller_entity = _seller_entity_token_specific  # type: ignore[method-assign]

    setattr(ContinuousWalletDiscovery._tracked_wallets, "_roi_v4_wallet_entity_universe", True)
    setattr(ContinuousWalletDiscovery.status, "_roi_v4_wallet_entity_universe", True)


__all__ = [
    "ACTIVE_STRATEGY_MUTATION_ALLOWED",
    "HISTORICAL_PROMOTION_AUTHORITY",
    "LIVE_MONEY_AUTHORITY",
    "PAPER_ONLY",
    "SEED_ENTITIES",
    "SEED_BY_ADDRESS",
    "SIGNING_AVAILABLE",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "TokenScopedEntityResolver",
    "WalletEntityUniverseV4",
    "WalletRole",
    "install_v4_wallet_entity_universe",
    "record_token_entity_link",
    "residual_return",
    "score_role",
]
