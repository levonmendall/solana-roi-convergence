from __future__ import annotations

import asyncio
import hashlib
import math
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from .config import BASELINE
from .direct_transaction import normalize_standard_transaction
from .ingestion import NormalizedSwap
from .observation import DexScreenerSolMarkProvider
from .source_coverage import FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE
from .wallet_intelligence import ContinuousWalletIntelligence, WalletPerformanceSnapshot


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


PROGRAM_SOURCES = frozenset(source for source, _ids in FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE)
MANIPULATION_BLOCKERS = frozenset(
    {
        "bundled_launch",
        "sniper_heavy",
        "common_funded_early_wallet_cluster",
        "scout_deployer_connection",
    }
)
SIDE_WALLET_BLOCKERS = frozenset(
    {
        "common_funded_early_wallet_cluster",
        "scout_deployer_connection",
    }
)


@dataclass(frozen=True, slots=True)
class WalletDiscoveryPolicy:
    """Bounded research policy for discovering and prospectively tracking wallets.

    These thresholds only decide which wallets deserve research bandwidth. They do
    not change ROI Convergence v3.1 entry, sizing, exit, certification, or paper
    execution authority.
    """

    broad_sample_modulus: int = 20
    broad_scan_limit: int = 600
    historical_max_signatures: int = 120
    historical_rpc_concurrency: int = 6
    historical_min_closed_episodes: int = 5
    historical_min_distinct_tokens: int = 5
    historical_min_return_on_capital: float = 0.05
    historical_min_profit_factor: float = 1.05
    max_tracked_challengers: int = 12
    forward_poll_limit: int = 100
    forward_max_pages: int = 3
    forward_rpc_concurrency: int = 6
    poll_interval_seconds: float = 10.0
    rescreen_hours: float = 6.0
    max_observation_lag_seconds: float = 20.0
    min_risk_coverage_rate: float = 0.80
    max_chase_fraction: float = BASELINE.max_chase_fraction


@dataclass(frozen=True, slots=True)
class RealizedMetrics:
    closed_episodes: int
    distinct_tokens: int
    return_on_capital: float
    geometric_growth: float
    profit_factor: float
    hit_rate: float
    max_drawdown: float


def _realized_metrics(rows: Iterable[dict[str, Any]], *, price_key: str) -> RealizedMetrics:
    positions: dict[str, tuple[float, float]] = {}
    episode_returns: list[float] = []
    realized_cost = 0.0
    realized_pnl = 0.0
    gross_profit = 0.0
    gross_loss = 0.0
    tokens: set[str] = set()

    ordered = sorted(
        rows,
        key=lambda row: (
            str(row.get("observed_at") or ""),
            str(row.get("received_at") or ""),
            str(row.get("signature") or ""),
        ),
    )
    for row in ordered:
        if not bool(row.get("include", True)):
            continue
        mint = str(row.get("token_mint") or "")
        side = str(row.get("side") or "").lower()
        try:
            units = float(row.get("token_amount") or 0.0)
            price = float(row.get(price_key) or 0.0)
        except (TypeError, ValueError):
            continue
        if not mint or units <= 0.0 or price <= 0.0 or side not in {"buy", "sell"}:
            continue
        tokens.add(mint)
        held_units, held_cost = positions.get(mint, (0.0, 0.0))
        if side == "buy":
            positions[mint] = (held_units + units, held_cost + units * price)
            continue
        if held_units <= 0.0 or held_cost <= 0.0:
            continue
        closed_units = min(units, held_units)
        closed_cost = held_cost * (closed_units / held_units)
        proceeds = closed_units * price
        pnl = proceeds - closed_cost
        if closed_cost <= 0.0:
            continue
        episode_return = pnl / closed_cost
        episode_returns.append(episode_return)
        realized_cost += closed_cost
        realized_pnl += pnl
        if pnl >= 0.0:
            gross_profit += pnl
        else:
            gross_loss += -pnl
        remaining_units = held_units - closed_units
        remaining_cost = max(0.0, held_cost - closed_cost)
        if remaining_units <= 1e-18:
            positions.pop(mint, None)
        else:
            positions[mint] = (remaining_units, remaining_cost)

    return_on_capital = realized_pnl / realized_cost if realized_cost > 0.0 else 0.0
    if episode_returns:
        log_terms = [math.log(max(1e-9, 1.0 + value)) for value in episode_returns]
        geometric_growth = math.exp(sum(log_terms) / len(log_terms)) - 1.0
        hit_rate = sum(1 for value in episode_returns if value > 0.0) / len(episode_returns)
    else:
        geometric_growth = 0.0
        hit_rate = 0.0
    if gross_loss > 0.0:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > 0.0:
        profit_factor = 999.0
    else:
        profit_factor = 0.0

    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    for value in episode_returns:
        equity *= max(1e-9, 1.0 + value)
        peak = max(peak, equity)
        if peak > 0.0:
            max_drawdown = max(max_drawdown, (peak - equity) / peak)

    return RealizedMetrics(
        closed_episodes=len(episode_returns),
        distinct_tokens=len(tokens),
        return_on_capital=return_on_capital,
        geometric_growth=geometric_growth,
        profit_factor=profit_factor,
        hit_rate=hit_rate,
        max_drawdown=min(1.0, max(0.0, max_drawdown)),
    )


class ContinuousWalletDiscovery:
    """Discover, screen, and prospectively score wallets without strategy authority.

    Discovery begins from a deterministic sample of the direct Solana plane's raw
    full-program receipt journal. Historical wallet RPC reads are used only to
    decide whether a newly observed wallet deserves scarce prospective tracking
    bandwidth. Promotion evidence begins strictly after ``forward_started_at``.
    """

    def __init__(
        self,
        *,
        store: Any,
        rpc: Any,
        entity_resolver: Any,
        risk: Any,
        risk_collectors: Any,
        intelligence: ContinuousWalletIntelligence | None = None,
        mark_provider: Any | None = None,
        policy: WalletDiscoveryPolicy | None = None,
        enabled: bool = True,
        now_fn: Any = utcnow,
    ):
        self.store = store
        self.rpc = rpc
        self.entity_resolver = entity_resolver
        self.risk = risk
        self.risk_collectors = risk_collectors
        self.intelligence = intelligence or ContinuousWalletIntelligence(store)
        self.mark_provider = mark_provider or DexScreenerSolMarkProvider()
        self.policy = policy or WalletDiscoveryPolicy()
        self.enabled = bool(enabled)
        self.now_fn = now_fn
        self._historical_gate = asyncio.Semaphore(max(1, self.policy.historical_rpc_concurrency))
        self._forward_gate = asyncio.Semaphore(max(1, self.policy.forward_rpc_concurrency))
        with store._lock, store.db:
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS wallet_discovery_candidates ("
                "wallet TEXT PRIMARY KEY, first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL, "
                "broad_sample_count INTEGER NOT NULL DEFAULT 0, distinct_token_count INTEGER NOT NULL DEFAULT 0, "
                "state TEXT NOT NULL, historical_closed_episodes INTEGER NOT NULL DEFAULT 0, "
                "historical_return_on_capital REAL NOT NULL DEFAULT 0, historical_profit_factor REAL NOT NULL DEFAULT 0, "
                "historical_hit_rate REAL NOT NULL DEFAULT 0, historical_max_drawdown REAL NOT NULL DEFAULT 0, "
                "forward_started_at TEXT, last_signature TEXT, last_polled_at TEXT, next_screen_at TEXT, "
                "forward_epoch_resets INTEGER NOT NULL DEFAULT 0, last_error TEXT)"
            )
            store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_wallet_discovery_state ON wallet_discovery_candidates(state, next_screen_at)"
            )
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS wallet_discovery_broad_samples ("
                "signature TEXT PRIMARY KEY, wallet TEXT NOT NULL, token_mint TEXT NOT NULL, side TEXT NOT NULL, "
                "observed_at TEXT NOT NULL, received_at TEXT NOT NULL, source TEXT NOT NULL)"
            )
            store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_wallet_discovery_broad_wallet ON wallet_discovery_broad_samples(wallet, received_at)"
            )
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS wallet_discovery_forward_observations ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, signature TEXT NOT NULL UNIQUE, wallet TEXT NOT NULL, "
                "token_mint TEXT NOT NULL, side TEXT NOT NULL, token_amount REAL NOT NULL, "
                "observed_at TEXT NOT NULL, received_at TEXT NOT NULL, wallet_price_sol REAL NOT NULL, "
                "copyable_price_sol REAL, chase_fraction REAL, copyable INTEGER NOT NULL, "
                "observation_lag_ms REAL NOT NULL, risk_complete INTEGER NOT NULL, "
                "manipulation_flag INTEGER NOT NULL, side_wallet_flag INTEGER NOT NULL, source TEXT NOT NULL)"
            )
            store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_wallet_discovery_forward_wallet ON wallet_discovery_forward_observations(wallet, received_at, id)"
            )
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS wallet_discovery_state ("
                "id INTEGER PRIMARY KEY CHECK(id=1), last_raw_receipt_id INTEGER NOT NULL DEFAULT 0, "
                "last_cycle_at TEXT, last_broad_scan_at TEXT, last_error TEXT)"
            )
            store.db.execute("INSERT OR IGNORE INTO wallet_discovery_state(id, last_raw_receipt_id) VALUES (1, 0)")

    @staticmethod
    def _sample(signature: str, modulus: int) -> bool:
        digest = hashlib.sha256(signature.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") % max(1, int(modulus)) == 0

    def _incumbent_wallets(self) -> list[str]:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT wallet FROM wallet_profiles WHERE historically_eligible=1 AND tier IN ('S','A') ORDER BY wallet"
            ).fetchall()
        return [str(row["wallet"]) for row in rows]

    async def _head_signature(self, wallet: str) -> str | None:
        rows, _provider, _latency = await self.rpc.get_signatures_for_address(wallet, limit=1, hedge=False)
        if not rows:
            return None
        signature = str(rows[0].get("signature") or "")
        return signature or None

    async def ensure_incumbents(self) -> None:
        now = self.now_fn()
        for wallet in self._incumbent_wallets():
            with self.store._lock:
                existing = self.store.db.execute(
                    "SELECT wallet, last_signature FROM wallet_discovery_candidates WHERE wallet=?", (wallet,)
                ).fetchone()
            if existing is not None:
                continue
            anchor: str | None = None
            error: str | None = None
            try:
                anchor = await self._head_signature(wallet)
            except Exception as exc:
                error = f"{type(exc).__name__}: incumbent anchor acquisition failed"
            with self.store._lock, self.store.db:
                self.store.db.execute(
                    "INSERT OR IGNORE INTO wallet_discovery_candidates("
                    "wallet, first_seen_at, last_seen_at, state, forward_started_at, last_signature, last_polled_at, last_error) "
                    "VALUES (?, ?, ?, 'incumbent_tracking', ?, ?, ?, ?)",
                    (wallet, now.isoformat(), now.isoformat(), now.isoformat(), anchor, now.isoformat(), error),
                )
            self.store.append(
                "wallet_discovery_incumbent_enrolled",
                now.isoformat(),
                {"wallet": wallet, "forward_started_at": now.isoformat(), "anchor_signature": anchor, "error": error},
            )

    def _raw_receipt_batch(self) -> tuple[int, list[dict[str, Any]]]:
        with self.store._lock:
            state = self.store.db.execute(
                "SELECT last_raw_receipt_id FROM wallet_discovery_state WHERE id=1"
            ).fetchone()
            cursor = int(state["last_raw_receipt_id"]) if state is not None else 0
            rows = self.store.db.execute(
                "SELECT id, signature, source_key, slot, received_at FROM direct_solana_recent_receipts "
                "WHERE id>? ORDER BY id LIMIT ?",
                (cursor, max(1, int(self.policy.broad_scan_limit))),
            ).fetchall()
        return cursor, [dict(row) for row in rows]

    async def _get_transaction(self, signature: str, *, historical: bool) -> Any:
        gate = self._historical_gate if historical else self._forward_gate
        async with gate:
            result, _provider, _latency = await self.rpc.get_transaction(signature, hedge=False)
            return result

    async def discover_from_raw_receipts(self) -> int:
        _cursor, rows = self._raw_receipt_batch()
        if not rows:
            return 0
        discovered = 0
        newest_id = max(int(row["id"]) for row in rows)
        for row in rows:
            signature = str(row.get("signature") or "")
            source = str(row.get("source_key") or "").upper()
            if not signature or source not in PROGRAM_SOURCES or not self._sample(signature, self.policy.broad_sample_modulus):
                continue
            received_at = datetime.fromisoformat(str(row["received_at"]))
            try:
                result = await self._get_transaction(signature, historical=True)
                swap = normalize_standard_transaction(
                    result,
                    signature=signature,
                    trigger_received_at=received_at,
                    source_hint=source,
                )
            except Exception:
                swap = None
            if swap is None:
                continue
            inserted = self._record_broad_sample(swap)
            if inserted:
                discovered += 1
        now = self.now_fn()
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE wallet_discovery_state SET last_raw_receipt_id=?, last_broad_scan_at=?, last_error=NULL WHERE id=1",
                (newest_id, now.isoformat()),
            )
        return discovered

    def _record_broad_sample(self, swap: NormalizedSwap) -> bool:
        with self.store._lock, self.store.db:
            cursor = self.store.db.execute(
                "INSERT OR IGNORE INTO wallet_discovery_broad_samples("
                "signature, wallet, token_mint, side, observed_at, received_at, source) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    swap.signature,
                    swap.wallet,
                    swap.token_mint,
                    swap.side,
                    swap.observed_at.isoformat(),
                    swap.received_at.isoformat(),
                    swap.source,
                ),
            )
            if cursor.rowcount != 1:
                return False
            counts = self.store.db.execute(
                "SELECT COUNT(*) AS n, COUNT(DISTINCT token_mint) AS tokens FROM wallet_discovery_broad_samples WHERE wallet=?",
                (swap.wallet,),
            ).fetchone()
            incumbent = self.store.db.execute(
                "SELECT 1 FROM wallet_profiles WHERE wallet=? AND historically_eligible=1 AND tier IN ('S','A') LIMIT 1",
                (swap.wallet,),
            ).fetchone()
            if incumbent is None:
                self.store.db.execute(
                    "INSERT INTO wallet_discovery_candidates("
                    "wallet, first_seen_at, last_seen_at, broad_sample_count, distinct_token_count, state, next_screen_at) "
                    "VALUES (?, ?, ?, ?, ?, 'discovered', ?) "
                    "ON CONFLICT(wallet) DO UPDATE SET last_seen_at=excluded.last_seen_at, "
                    "broad_sample_count=excluded.broad_sample_count, distinct_token_count=excluded.distinct_token_count, "
                    "next_screen_at=CASE WHEN wallet_discovery_candidates.state='screen_rejected' "
                    "THEN MIN(COALESCE(wallet_discovery_candidates.next_screen_at, excluded.next_screen_at), excluded.next_screen_at) "
                    "ELSE wallet_discovery_candidates.next_screen_at END",
                    (
                        swap.wallet,
                        swap.received_at.isoformat(),
                        swap.received_at.isoformat(),
                        int(counts["n"]),
                        int(counts["tokens"]),
                        swap.received_at.isoformat(),
                    ),
                )
        self.store.append(
            "wallet_discovery_broad_sample",
            swap.received_at.isoformat(),
            {"wallet": swap.wallet, "token_mint": swap.token_mint, "side": swap.side, "signature": swap.signature},
        )
        return True

    async def _historical_swaps(self, wallet: str) -> tuple[list[NormalizedSwap], str | None]:
        signatures: list[dict[str, Any]] = []
        before: str | None = None
        remaining = max(1, int(self.policy.historical_max_signatures))
        while remaining > 0:
            limit = min(100, remaining)
            rows, _provider, _latency = await self.rpc.get_signatures_for_address(
                wallet, before=before, limit=limit, hedge=False
            )
            if not rows:
                break
            signatures.extend(row for row in rows if row.get("err") is None)
            remaining -= len(rows)
            before = str(rows[-1].get("signature") or "") or None
            if len(rows) < limit or before is None:
                break
        if not signatures:
            return [], None
        newest = str(signatures[0].get("signature") or "") or None

        async def hydrate(row: dict[str, Any]) -> NormalizedSwap | None:
            signature = str(row.get("signature") or "")
            if not signature:
                return None
            try:
                result = await self._get_transaction(signature, historical=True)
            except Exception:
                return None
            return normalize_standard_transaction(
                result,
                signature=signature,
                trigger_received_at=self.now_fn(),
                source_hint=None,
            )

        swaps = await asyncio.gather(*(hydrate(row) for row in signatures))
        return [swap for swap in swaps if swap is not None and swap.wallet == wallet], newest

    async def screen_one_candidate(self) -> bool:
        now = self.now_fn()
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT wallet FROM wallet_discovery_candidates "
                "WHERE state IN ('discovered','screen_rejected') AND (next_screen_at IS NULL OR next_screen_at<=?) "
                "ORDER BY broad_sample_count DESC, distinct_token_count DESC, last_seen_at DESC LIMIT 1",
                (now.isoformat(),),
            ).fetchone()
        if row is None:
            return False
        wallet = str(row["wallet"])
        try:
            swaps, newest = await self._historical_swaps(wallet)
            metric_rows = [
                {
                    "signature": swap.signature,
                    "token_mint": swap.token_mint,
                    "side": swap.side,
                    "token_amount": swap.token_amount,
                    "reference_price_sol": swap.reference_price_sol,
                    "observed_at": swap.observed_at.isoformat(),
                    "received_at": swap.received_at.isoformat(),
                }
                for swap in swaps
            ]
            metrics = _realized_metrics(metric_rows, price_key="reference_price_sol")
            passed = bool(
                metrics.closed_episodes >= self.policy.historical_min_closed_episodes
                and metrics.distinct_tokens >= self.policy.historical_min_distinct_tokens
                and metrics.return_on_capital > self.policy.historical_min_return_on_capital
                and metrics.profit_factor > self.policy.historical_min_profit_factor
            )
            state = "tracking" if passed else "screen_rejected"
            next_screen = None if passed else now + timedelta(hours=self.policy.rescreen_hours)
            with self.store._lock, self.store.db:
                self.store.db.execute(
                    "UPDATE wallet_discovery_candidates SET state=?, historical_closed_episodes=?, "
                    "historical_return_on_capital=?, historical_profit_factor=?, historical_hit_rate=?, "
                    "historical_max_drawdown=?, forward_started_at=?, last_signature=?, last_polled_at=?, "
                    "next_screen_at=?, last_error=NULL WHERE wallet=?",
                    (
                        state,
                        metrics.closed_episodes,
                        metrics.return_on_capital,
                        metrics.profit_factor,
                        metrics.hit_rate,
                        metrics.max_drawdown,
                        now.isoformat() if passed else None,
                        newest if passed else None,
                        now.isoformat(),
                        next_screen.isoformat() if next_screen else None,
                        wallet,
                    ),
                )
            self.store.append(
                "wallet_discovery_historical_screen",
                now.isoformat(),
                {
                    "wallet": wallet,
                    "passed": passed,
                    "metrics": asdict(metrics),
                    "forward_started_at": now.isoformat() if passed else None,
                    "historical_evidence_has_promotion_authority": False,
                },
            )
            return passed
        except Exception as exc:
            with self.store._lock, self.store.db:
                self.store.db.execute(
                    "UPDATE wallet_discovery_candidates SET next_screen_at=?, last_error=? WHERE wallet=?",
                    ((now + timedelta(hours=1)).isoformat(), f"{type(exc).__name__}: historical screen failed", wallet),
                )
            return False

    def _tracked_wallets(self) -> list[str]:
        incumbents = set(self._incumbent_wallets())
        with self.store._lock:
            challenger_rows = self.store.db.execute(
                "SELECT wallet FROM wallet_discovery_candidates WHERE state='tracking' "
                "ORDER BY historical_return_on_capital DESC, historical_profit_factor DESC LIMIT ?",
                (max(0, int(self.policy.max_tracked_challengers)),),
            ).fetchall()
            incumbent_rows = self.store.db.execute(
                "SELECT wallet FROM wallet_discovery_candidates WHERE state='incumbent_tracking' ORDER BY wallet"
            ).fetchall()
        ordered = [str(row["wallet"]) for row in incumbent_rows]
        ordered.extend(str(row["wallet"]) for row in challenger_rows if str(row["wallet"]) not in incumbents)
        return list(dict.fromkeys(ordered))

    def _candidate_state(self, wallet: str) -> dict[str, Any] | None:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT * FROM wallet_discovery_candidates WHERE wallet=?", (wallet,)
            ).fetchone()
        return dict(row) if row is not None else None

    async def _forward_signature_rows(self, wallet: str, anchor: str | None, started_at: datetime) -> tuple[list[dict[str, Any]], bool]:
        collected: list[dict[str, Any]] = []
        before: str | None = None
        anchor_found = anchor is None
        for _ in range(max(1, int(self.policy.forward_max_pages))):
            rows, _provider, _latency = await self.rpc.get_signatures_for_address(
                wallet,
                before=before,
                limit=max(1, min(1000, int(self.policy.forward_poll_limit))),
                hedge=False,
            )
            if not rows:
                break
            stop = False
            for row in rows:
                signature = str(row.get("signature") or "")
                if anchor and signature == anchor:
                    anchor_found = True
                    stop = True
                    break
                try:
                    block_time = int(row.get("blockTime") or 0)
                except (TypeError, ValueError):
                    block_time = 0
                if block_time and datetime.fromtimestamp(block_time, tz=timezone.utc) < started_at:
                    anchor_found = True
                    stop = True
                    break
                if row.get("err") is None and signature:
                    collected.append(row)
            if stop or len(rows) < max(1, min(1000, int(self.policy.forward_poll_limit))):
                break
            before = str(rows[-1].get("signature") or "") or None
            if before is None:
                break
        return collected, anchor_found

    async def _reset_forward_epoch(self, wallet: str, newest_signature: str | None, *, reason: str) -> None:
        now = self.now_fn()
        with self.store._lock, self.store.db:
            self.store.db.execute("DELETE FROM wallet_discovery_forward_observations WHERE wallet=?", (wallet,))
            self.store.db.execute(
                "UPDATE wallet_discovery_candidates SET forward_started_at=?, last_signature=?, last_polled_at=?, "
                "forward_epoch_resets=forward_epoch_resets+1, last_error=? WHERE wallet=?",
                (now.isoformat(), newest_signature, now.isoformat(), reason, wallet),
            )
        self.store.append(
            "wallet_discovery_forward_epoch_reset",
            now.isoformat(),
            {"wallet": wallet, "reason": reason, "new_anchor_signature": newest_signature},
        )

    async def _risk_flags(self, swap: NormalizedSwap) -> tuple[bool, bool, bool]:
        if swap.side != "buy":
            return True, False, False
        now = swap.received_at
        try:
            if self.risk_collectors is not None:
                await self.risk_collectors.refresh(swap.token_mint, now, current_swap=swap)
            entity_id = self.entity_resolver.entity_id_for(
                swap.wallet, fallback_entity_id=None, as_of=now
            )
            snapshot = await self.risk.snapshot(
                swap.token_mint,
                now,
                scout_wallet=swap.wallet,
                scout_entity_id=entity_id,
            )
            component = self.entity_resolver.component(swap.wallet, as_of=now)
        except Exception:
            return False, True, True
        if snapshot is None:
            return False, True, len(component) > 1
        blockers = set(snapshot.blockers)
        manipulation = bool(blockers & MANIPULATION_BLOCKERS)
        side_wallet = bool(blockers & SIDE_WALLET_BLOCKERS) or len(component) > 1
        return True, manipulation, side_wallet

    async def _record_forward_swap(self, swap: NormalizedSwap) -> bool:
        received_at = swap.received_at
        mark: dict[str, Any] | None = None
        try:
            mark = await self.mark_provider.mark(swap.token_mint)
        except Exception:
            mark = None
        copyable_price: float | None = None
        chase_fraction: float | None = None
        if isinstance(mark, dict):
            try:
                copyable_price = float(mark.get("price_sol") or 0.0)
            except (TypeError, ValueError):
                copyable_price = None
            if copyable_price is not None and copyable_price > 0.0:
                self.store.record_price_mark(
                    token_mint=swap.token_mint,
                    observed_at=mark.get("observed_at", received_at).isoformat(),
                    received_at=mark.get("received_at", received_at).isoformat(),
                    price_sol=copyable_price,
                    source="wallet-discovery:" + str(mark.get("source") or "current-mark"),
                    source_ref=str(mark.get("source_ref") or "") or None,
                )
                if swap.side == "buy":
                    chase_fraction = max(0.0, copyable_price / swap.reference_price_sol - 1.0)
                else:
                    chase_fraction = max(0.0, 1.0 - copyable_price / swap.reference_price_sol)
        lag_ms = swap.ingestion_latency_ms
        copyable = bool(
            copyable_price is not None
            and copyable_price > 0.0
            and chase_fraction is not None
            and chase_fraction <= self.policy.max_chase_fraction
            and lag_ms <= self.policy.max_observation_lag_seconds * 1000.0
        )
        risk_complete, manipulation_flag, side_wallet_flag = await self._risk_flags(swap)
        with self.store._lock, self.store.db:
            cursor = self.store.db.execute(
                "INSERT OR IGNORE INTO wallet_discovery_forward_observations("
                "signature, wallet, token_mint, side, token_amount, observed_at, received_at, wallet_price_sol, "
                "copyable_price_sol, chase_fraction, copyable, observation_lag_ms, risk_complete, manipulation_flag, "
                "side_wallet_flag, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    swap.signature,
                    swap.wallet,
                    swap.token_mint,
                    swap.side,
                    float(swap.token_amount),
                    swap.observed_at.isoformat(),
                    received_at.isoformat(),
                    float(swap.reference_price_sol),
                    copyable_price,
                    chase_fraction,
                    1 if copyable else 0,
                    float(lag_ms),
                    1 if risk_complete else 0,
                    1 if manipulation_flag else 0,
                    1 if side_wallet_flag else 0,
                    swap.source,
                ),
            )
        if cursor.rowcount == 1:
            self.store.append(
                "wallet_discovery_forward_observation",
                received_at.isoformat(),
                {
                    "wallet": swap.wallet,
                    "token_mint": swap.token_mint,
                    "side": swap.side,
                    "signature": swap.signature,
                    "copyable": copyable,
                    "chase_fraction": chase_fraction,
                    "observation_lag_ms": lag_ms,
                    "risk_complete": risk_complete,
                    "manipulation_flag": manipulation_flag,
                    "side_wallet_flag": side_wallet_flag,
                },
            )
            return True
        return False

    async def poll_wallet(self, wallet: str) -> int:
        state = self._candidate_state(wallet)
        if state is None or not state.get("forward_started_at"):
            return 0
        started_at = datetime.fromisoformat(str(state["forward_started_at"]))
        anchor = str(state.get("last_signature") or "") or None
        try:
            rows, anchor_found = await self._forward_signature_rows(wallet, anchor, started_at)
        except Exception as exc:
            now = self.now_fn()
            with self.store._lock, self.store.db:
                self.store.db.execute(
                    "UPDATE wallet_discovery_candidates SET last_polled_at=?, last_error=? WHERE wallet=?",
                    (now.isoformat(), f"{type(exc).__name__}: forward signature poll failed", wallet),
                )
            return 0
        newest_signature = str(rows[0].get("signature") or "") if rows else anchor
        if anchor is not None and not anchor_found:
            await self._reset_forward_epoch(
                wallet,
                newest_signature,
                reason="forward signature anchor fell outside bounded pagination; evidence epoch reset fail-closed",
            )
            return 0
        inserted = 0
        for row in reversed(rows):
            signature = str(row.get("signature") or "")
            if not signature:
                continue
            received_at = self.now_fn()
            try:
                result = await self._get_transaction(signature, historical=False)
                swap = normalize_standard_transaction(
                    result,
                    signature=signature,
                    trigger_received_at=received_at,
                    source_hint=None,
                )
            except Exception:
                swap = None
            if swap is None or swap.wallet != wallet:
                continue
            swap = NormalizedSwap(
                signature=swap.signature,
                slot=swap.slot,
                observed_at=swap.observed_at,
                received_at=swap.received_at,
                wallet=swap.wallet,
                token_mint=swap.token_mint,
                side=swap.side,
                token_amount=swap.token_amount,
                native_amount_sol=swap.native_amount_sol,
                reference_price_sol=swap.reference_price_sol,
                source="wallet-discovery-forward:" + swap.source,
            )
            if await self._record_forward_swap(swap):
                inserted += 1
        now = self.now_fn()
        if rows:
            newest_signature = str(rows[0].get("signature") or "") or anchor
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE wallet_discovery_candidates SET last_signature=?, last_polled_at=?, last_error=NULL WHERE wallet=?",
                (newest_signature, now.isoformat(), wallet),
            )
        if inserted:
            self.refresh_wallet_snapshot(wallet)
        return inserted

    def refresh_wallet_snapshot(self, wallet: str) -> WalletPerformanceSnapshot | None:
        state = self._candidate_state(wallet)
        if state is None or not state.get("forward_started_at"):
            return None
        with self.store._lock:
            rows = [
                dict(row)
                for row in self.store.db.execute(
                    "SELECT signature, wallet, token_mint, side, token_amount, observed_at, received_at, "
                    "wallet_price_sol, copyable_price_sol, chase_fraction, copyable, observation_lag_ms, "
                    "risk_complete, manipulation_flag, side_wallet_flag, source "
                    "FROM wallet_discovery_forward_observations WHERE wallet=? ORDER BY observed_at, received_at, id",
                    (wallet,),
                ).fetchall()
            ]
        if not rows:
            return None
        replay_rows = [
            {**row, "include": bool(row["copyable"])}
            for row in rows
        ]
        metrics = _realized_metrics(replay_rows, price_key="copyable_price_sol")
        copyability_rate = sum(1 for row in rows if bool(row["copyable"])) / len(rows)
        buys = [row for row in rows if str(row["side"]).lower() == "buy"]
        complete_buys = [row for row in buys if bool(row["risk_complete"])]
        risk_coverage = len(complete_buys) / len(buys) if buys else 0.0
        if risk_coverage < self.policy.min_risk_coverage_rate:
            manipulation_risk = 1.0
            side_wallet_risk = 1.0
        else:
            manipulation_risk = (
                sum(1 for row in complete_buys if bool(row["manipulation_flag"])) / len(complete_buys)
                if complete_buys else 1.0
            )
            side_wallet_risk = (
                sum(1 for row in complete_buys if bool(row["side_wallet_flag"])) / len(complete_buys)
                if complete_buys else 1.0
            )
        now = self.now_fn()
        try:
            component = self.entity_resolver.component(wallet, as_of=now)
            entity_id = self.entity_resolver.entity_id_for(wallet, fallback_entity_id=None, as_of=now)
        except Exception:
            component = {wallet}
            entity_id = "graph:" + wallet
        if len(component) > 1:
            side_wallet_risk = 1.0
        buy_lags = [float(row["observation_lag_ms"]) for row in buys]
        median_lag = statistics.median(buy_lags) if buy_lags else 0.0
        snapshot = WalletPerformanceSnapshot(
            wallet=wallet,
            entity_id=entity_id,
            observed_at=now,
            closed_episodes=metrics.closed_episodes,
            copyable_return_on_capital=metrics.return_on_capital,
            geometric_growth=metrics.geometric_growth,
            profit_factor=metrics.profit_factor,
            hit_rate=metrics.hit_rate,
            max_drawdown=metrics.max_drawdown,
            copyability_rate=copyability_rate,
            manipulation_risk=min(1.0, max(0.0, manipulation_risk)),
            side_wallet_risk=min(1.0, max(0.0, side_wallet_risk)),
            median_entry_lag_ms=median_lag,
            source="continuous-wallet-discovery-forward-v1",
        )
        self.intelligence.record_snapshot(snapshot)
        return snapshot

    def _proposal_exists(self) -> bool:
        status = self.intelligence.status()
        proposal = status.get("latest_proposed_cohort")
        return bool(isinstance(proposal, dict) and proposal.get("status") == "proposed")

    def maybe_propose_adaptive_cohort(self) -> dict[str, Any] | None:
        if self._proposal_exists():
            return None
        now = self.now_fn()
        version = f"{BASELINE.version}-adaptive-{now.strftime('%Y%m%dT%H%M%SZ')}"
        return self.intelligence.propose_next_cohort(
            parent_version=BASELINE.version,
            strategy_version=version,
        )

    async def run_once(self) -> dict[str, Any]:
        if not self.enabled:
            return self.status()
        await self.ensure_incumbents()
        broad = await self.discover_from_raw_receipts()
        await self.screen_one_candidate()
        tracked = self._tracked_wallets()
        forward = 0
        if tracked:
            results = await asyncio.gather(*(self.poll_wallet(wallet) for wallet in tracked))
            forward = sum(int(value) for value in results)
        proposal = self.maybe_propose_adaptive_cohort()
        now = self.now_fn()
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE wallet_discovery_state SET last_cycle_at=?, last_error=NULL WHERE id=1",
                (now.isoformat(),),
            )
        payload = self.status()
        payload["cycle"] = {
            "broad_samples_added": broad,
            "tracked_wallets_polled": len(tracked),
            "forward_observations_added": forward,
            "adaptive_proposal_evaluated": proposal is not None,
            "adaptive_proposal": proposal,
        }
        return payload

    async def run(self, stop: asyncio.Event) -> None:
        if not self.enabled:
            await stop.wait()
            return
        while not stop.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                now = self.now_fn()
                with self.store._lock, self.store.db:
                    self.store.db.execute(
                        "UPDATE wallet_discovery_state SET last_cycle_at=?, last_error=? WHERE id=1",
                        (now.isoformat(), f"{type(exc).__name__}: wallet discovery cycle failed"),
                    )
            try:
                await asyncio.wait_for(stop.wait(), timeout=max(1.0, float(self.policy.poll_interval_seconds)))
            except asyncio.TimeoutError:
                continue

    def status(self) -> dict[str, Any]:
        with self.store._lock:
            state_rows = self.store.db.execute(
                "SELECT state, COUNT(*) AS n FROM wallet_discovery_candidates GROUP BY state"
            ).fetchall()
            broad_count = int(self.store.db.execute("SELECT COUNT(*) FROM wallet_discovery_broad_samples").fetchone()[0])
            forward_count = int(self.store.db.execute("SELECT COUNT(*) FROM wallet_discovery_forward_observations").fetchone()[0])
            copyable_count = int(
                self.store.db.execute(
                    "SELECT COUNT(*) FROM wallet_discovery_forward_observations WHERE copyable=1"
                ).fetchone()[0]
            )
            control = self.store.db.execute(
                "SELECT last_raw_receipt_id, last_cycle_at, last_broad_scan_at, last_error FROM wallet_discovery_state WHERE id=1"
            ).fetchone()
            leaders = [
                dict(row)
                for row in self.store.db.execute(
                    "SELECT wallet, state, broad_sample_count, distinct_token_count, historical_closed_episodes, "
                    "historical_return_on_capital, historical_profit_factor, forward_started_at, last_polled_at, "
                    "forward_epoch_resets, last_error FROM wallet_discovery_candidates "
                    "ORDER BY CASE state WHEN 'tracking' THEN 0 WHEN 'incumbent_tracking' THEN 1 ELSE 2 END, "
                    "historical_return_on_capital DESC LIMIT 20"
                ).fetchall()
            ]
        states = {str(row["state"]): int(row["n"]) for row in state_rows}
        return {
            "enabled": self.enabled,
            "paper_only": True,
            "live_money_authority": False,
            "signing_or_submission_available": False,
            "research_lane": True,
            "broad_program_receipt_sampling": True,
            "broad_sample_modulus": self.policy.broad_sample_modulus,
            "approximate_broad_sample_fraction": 1.0 / max(1, self.policy.broad_sample_modulus),
            "ecosystem_wide_exhaustive": False,
            "historical_screen_has_promotion_authority": False,
            "promotion_evidence_boundary": "forward_started_at only",
            "active_strategy_mutation_allowed": False,
            "future_cohort_proposal_enabled": True,
            "candidate_states": states,
            "broad_samples": broad_count,
            "forward_observations": forward_count,
            "copyable_forward_observations": copyable_count,
            "copyable_forward_fraction": copyable_count / forward_count if forward_count else 0.0,
            "tracked_wallet_limit": self.policy.max_tracked_challengers,
            "max_chase_fraction": self.policy.max_chase_fraction,
            "max_observation_lag_seconds": self.policy.max_observation_lag_seconds,
            "last_raw_receipt_id": int(control["last_raw_receipt_id"]) if control is not None else 0,
            "last_cycle_at": str(control["last_cycle_at"] or "") or None if control is not None else None,
            "last_broad_scan_at": str(control["last_broad_scan_at"] or "") or None if control is not None else None,
            "last_error": str(control["last_error"] or "") or None if control is not None else None,
            "tracked_wallets": leaders,
            "wallet_intelligence": self.intelligence.status(),
        }
