from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from .strategy_v52_authority import target_sizing_policy

REFINEMENT_VERSION = "v52-wallet-alpha-refinement-v1"


@dataclass(frozen=True, slots=True)
class WalletMarginalAlphaObservation:
    wallet: str
    context_key: str
    candidate_id: str
    observed_at: datetime
    wallet_policy_return: float
    matched_control_return: float
    executable_mfe: float
    executable_mae: float
    copyable: bool = True

    @property
    def marginal_alpha(self) -> float:
        return self.wallet_policy_return - self.matched_control_return

    @property
    def capture_ratio(self) -> float | None:
        if self.executable_mfe <= 0.0:
            return None
        return self.wallet_policy_return / self.executable_mfe


@dataclass(frozen=True, slots=True)
class ContextualWalletScore:
    wallet: str
    context_key: str
    paired_forward_episodes: int
    effective_episode_weight: float
    decayed_marginal_alpha: float
    decayed_capture_ratio: float | None
    decayed_executable_mae: float
    copyability_rate: float
    eligible_for_strategy_influence: bool
    blockers: tuple[str, ...]


class WalletAlphaRefinementLedger:
    """Forward-only paired wallet alpha measurement with contextual time decay.

    The ledger compares the wallet-informed policy with a matched control on the
    same candidate stream. Scoring begins at the first realistic executable
    timestamp supplied by the caller. It may influence future governed strategy
    research only after the authoritative minimum-forward-sample gate; it never
    grants paper entry, signing, transaction submission, or live-money authority.
    """

    def __init__(self, store: Any, *, half_life_hours: float = 72.0) -> None:
        if half_life_hours <= 0:
            raise ValueError("half_life_hours must be positive")
        self.store = store
        self.half_life_hours = float(half_life_hours)
        with store._lock, store.db:
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_wallet_marginal_alpha ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, wallet TEXT NOT NULL, context_key TEXT NOT NULL, "
                "candidate_id TEXT NOT NULL, observed_at TEXT NOT NULL, wallet_policy_return REAL NOT NULL, "
                "matched_control_return REAL NOT NULL, marginal_alpha REAL NOT NULL, executable_mfe REAL NOT NULL, "
                "executable_mae REAL NOT NULL, capture_ratio REAL, copyable INTEGER NOT NULL, "
                "UNIQUE(wallet, context_key, candidate_id))"
            )
            store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_v52_wallet_alpha_context "
                "ON v52_wallet_marginal_alpha(wallet, context_key, observed_at)"
            )
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_wallet_missed_opportunities ("
                "candidate_id TEXT PRIMARY KEY, context_key TEXT NOT NULL, first_executable_at TEXT NOT NULL, "
                "reason TEXT NOT NULL, executable_mfe REAL NOT NULL, executable_mae REAL NOT NULL, "
                "recorded_at TEXT NOT NULL)"
            )

    def record_paired(self, observation: WalletMarginalAlphaObservation) -> bool:
        values = (
            observation.wallet_policy_return,
            observation.matched_control_return,
            observation.executable_mfe,
            observation.executable_mae,
        )
        if not observation.wallet or not observation.context_key or not observation.candidate_id:
            raise ValueError("wallet, context_key, and candidate_id are required")
        if any(not math.isfinite(float(value)) for value in values):
            raise ValueError("paired alpha values must be finite")
        if observation.wallet_policy_return <= -1.0 or observation.matched_control_return <= -1.0:
            raise ValueError("returns must be greater than -1")
        if observation.executable_mfe < 0.0 or observation.executable_mae < 0.0:
            raise ValueError("executable MFE/MAE cannot be negative")
        capture = observation.capture_ratio
        with self.store._lock, self.store.db:
            cursor = self.store.db.execute(
                "INSERT OR IGNORE INTO v52_wallet_marginal_alpha("
                "wallet, context_key, candidate_id, observed_at, wallet_policy_return, matched_control_return, "
                "marginal_alpha, executable_mfe, executable_mae, capture_ratio, copyable) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    observation.wallet,
                    observation.context_key,
                    observation.candidate_id,
                    observation.observed_at.isoformat(),
                    float(observation.wallet_policy_return),
                    float(observation.matched_control_return),
                    float(observation.marginal_alpha),
                    float(observation.executable_mfe),
                    float(observation.executable_mae),
                    float(capture) if capture is not None else None,
                    1 if observation.copyable else 0,
                ),
            )
        if cursor.rowcount == 1:
            payload = asdict(observation)
            payload["observed_at"] = observation.observed_at.isoformat()
            payload["marginal_alpha"] = observation.marginal_alpha
            payload["capture_ratio"] = capture
            payload["paired_same_candidate_stream"] = True
            payload["paper_only"] = True
            self.store.append("v52_wallet_marginal_alpha", observation.observed_at.isoformat(), payload)
            return True
        return False

    def record_missed_opportunity(
        self,
        *,
        candidate_id: str,
        context_key: str,
        first_executable_at: datetime,
        reason: str,
        executable_mfe: float,
        executable_mae: float,
        recorded_at: datetime | None = None,
    ) -> bool:
        if not candidate_id or not context_key or not reason:
            raise ValueError("candidate_id, context_key, and reason are required")
        if executable_mfe < 0.0 or executable_mae < 0.0:
            raise ValueError("executable MFE/MAE cannot be negative")
        now = recorded_at or datetime.now(timezone.utc)
        with self.store._lock, self.store.db:
            cursor = self.store.db.execute(
                "INSERT OR IGNORE INTO v52_wallet_missed_opportunities("
                "candidate_id, context_key, first_executable_at, reason, executable_mfe, executable_mae, recorded_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    candidate_id,
                    context_key,
                    first_executable_at.isoformat(),
                    reason,
                    float(executable_mfe),
                    float(executable_mae),
                    now.isoformat(),
                ),
            )
        if cursor.rowcount == 1:
            self.store.append(
                "v52_wallet_missed_opportunity",
                now.isoformat(),
                {
                    "candidate_id": candidate_id,
                    "context_key": context_key,
                    "first_executable_at": first_executable_at.isoformat(),
                    "reason": reason,
                    "executable_mfe": executable_mfe,
                    "executable_mae": executable_mae,
                    "paper_only": True,
                },
            )
            return True
        return False

    def score(self, wallet: str, context_key: str, *, as_of: datetime | None = None) -> ContextualWalletScore:
        now = as_of or datetime.now(timezone.utc)
        with self.store._lock:
            rows = [
                dict(row)
                for row in self.store.db.execute(
                    "SELECT observed_at, marginal_alpha, executable_mae, capture_ratio, copyable "
                    "FROM v52_wallet_marginal_alpha WHERE wallet=? AND context_key=? AND observed_at<=? "
                    "ORDER BY observed_at, id",
                    (wallet, context_key, now.isoformat()),
                ).fetchall()
            ]
        weights: list[float] = []
        for row in rows:
            observed = datetime.fromisoformat(str(row["observed_at"]))
            age_hours = max(0.0, (now - observed).total_seconds() / 3600.0)
            weights.append(2.0 ** (-age_hours / self.half_life_hours))
        total_weight = sum(weights)

        def weighted(key: str) -> float:
            if total_weight <= 0.0:
                return 0.0
            return sum(weight * float(row[key]) for weight, row in zip(weights, rows)) / total_weight

        marginal = weighted("marginal_alpha")
        mae = weighted("executable_mae")
        capture_rows = [
            (weight, float(row["capture_ratio"]))
            for weight, row in zip(weights, rows)
            if row["capture_ratio"] is not None
        ]
        capture_weight = sum(weight for weight, _value in capture_rows)
        capture = (
            sum(weight * value for weight, value in capture_rows) / capture_weight
            if capture_weight > 0.0
            else None
        )
        copyability = (
            sum(weight for weight, row in zip(weights, rows) if bool(row["copyable"])) / total_weight
            if total_weight > 0.0
            else 0.0
        )
        minimum = int(target_sizing_policy()["minimum_forward_samples"])
        blockers: list[str] = []
        if len(rows) < minimum:
            blockers.append("insufficient_forward_episodes")
        if marginal <= 0.0:
            blockers.append("paired_marginal_alpha_not_positive")
        if copyability < 0.80:
            blockers.append("copyability_rate_below_minimum")
        return ContextualWalletScore(
            wallet=wallet,
            context_key=context_key,
            paired_forward_episodes=len(rows),
            effective_episode_weight=total_weight,
            decayed_marginal_alpha=marginal,
            decayed_capture_ratio=capture,
            decayed_executable_mae=mae,
            copyability_rate=copyability,
            eligible_for_strategy_influence=not blockers,
            blockers=tuple(blockers),
        )

    def status(self) -> dict[str, Any]:
        with self.store._lock:
            paired = int(self.store.db.execute("SELECT COUNT(*) FROM v52_wallet_marginal_alpha").fetchone()[0])
            missed = int(self.store.db.execute("SELECT COUNT(*) FROM v52_wallet_missed_opportunities").fetchone()[0])
        return {
            "version": REFINEMENT_VERSION,
            "paired_forward_observations": paired,
            "missed_opportunity_rows": missed,
            "contextual_scoring": True,
            "time_decay_half_life_hours": self.half_life_hours,
            "paired_same_candidate_stream_required": True,
            "minimum_forward_samples": int(target_sizing_policy()["minimum_forward_samples"]),
            "executable_mfe_mae_from_first_realistic_executable_timestamp": True,
            "capture_ratio_definition": "realized_executable_net_return / executable_mfe",
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }


__all__ = [
    "ContextualWalletScore",
    "REFINEMENT_VERSION",
    "WalletAlphaRefinementLedger",
    "WalletMarginalAlphaObservation",
]
