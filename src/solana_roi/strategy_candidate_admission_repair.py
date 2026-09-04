from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any, Callable

from .profit_first_entity_final import MarketRegime
from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter


REPAIR_VERSION = "strategy-candidate-admission-v1"
ENTRY_WINDOW_SECONDS = 20.0
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_ORIGINAL_BUY: Callable[..., Any] | None = None
_ORIGINAL_MARKET_REGIME: Callable[..., MarketRegime] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_INSTALLED = False


def _finite(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def strategy_evaluation_eligible(row: dict[str, Any]) -> bool:
    """Return whether a forward buy deserves the final v5/v5.1 evaluation.

    The legacy ``copyable`` bit is intentionally not consulted. It was created for
    the old <=15% mark-copyability policy, while the active v5/v5.1 strategy owns
    amount-specific Jupiter execution, the 15-40% challenger bands, the unchanged
    20-second entry ceiling and mechanical hard stops. Keeping the old bit as a
    pre-v5 veto made those newer strategy surfaces unreachable.
    """
    if str(row.get("side") or "").lower() != "buy":
        return False
    if not str(row.get("signature") or ""):
        return False
    if not str(row.get("token_mint") or ""):
        return False
    if not str(row.get("wallet") or ""):
        return False
    wallet_price = _finite(row.get("wallet_price_sol"))
    if wallet_price is None or wallet_price <= 0.0:
        return False
    lag_ms = _finite(row.get("observation_lag_ms"))
    if lag_ms is None or lag_ms < 0.0 or lag_ms > ENTRY_WINDOW_SECONDS * 1000.0:
        return False
    return True


def _trial_exists(adapter: Any, signature: str) -> bool:
    try:
        with adapter.store._lock:
            row = adapter.store.db.execute(
                "SELECT 1 FROM risk_conditioned_alpha_v5_trials "
                "WHERE release_commit=? AND source_signature=? LIMIT 1",
                (adapter.release_commit, signature),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _selected_actionable(adapter: Any, signature: str) -> bool:
    try:
        with adapter.store._lock:
            row = adapter.store.db.execute(
                "SELECT 1 FROM risk_conditioned_alpha_v5_trials "
                "WHERE release_commit=? AND source_signature=? AND selected=1 "
                "AND decision LIKE 'paper_enter%' LIMIT 1",
                (adapter.release_commit, signature),
            ).fetchone()
        return row is not None
    except Exception:
        return False


async def _buy_with_modern_admission(self: Any, row: dict[str, Any]) -> None:
    if _ORIGINAL_BUY is None:
        raise RuntimeError("strategy candidate admission repair is not installed")

    if bool(row.get("copyable")):
        await _ORIGINAL_BUY(self, row)
        return

    if not strategy_evaluation_eligible(row):
        setattr(
            self,
            "_roi_strategy_admission_legacy_rejections",
            int(getattr(self, "_roi_strategy_admission_legacy_rejections", 0) or 0) + 1,
        )
        await _ORIGINAL_BUY(self, row)
        return

    admitted = dict(row)
    admitted["copyable"] = 1
    setattr(
        self,
        "_roi_strategy_admission_bypasses",
        int(getattr(self, "_roi_strategy_admission_bypasses", 0) or 0) + 1,
    )
    await _ORIGINAL_BUY(self, admitted)

    signature = str(row.get("signature") or "")
    if _trial_exists(self, signature):
        setattr(
            self,
            "_roi_strategy_admission_evaluated",
            int(getattr(self, "_roi_strategy_admission_evaluated", 0) or 0) + 1,
        )
    if _selected_actionable(self, signature):
        setattr(
            self,
            "_roi_strategy_admission_actionable",
            int(getattr(self, "_roi_strategy_admission_actionable", 0) or 0) + 1,
        )


setattr(_buy_with_modern_admission, "_roi_strategy_candidate_admission", True)


def _market_regime_with_modern_admission(self: Any, at: datetime) -> MarketRegime:
    if _ORIGINAL_MARKET_REGIME is None:
        raise RuntimeError("strategy candidate admission regime repair is not installed")
    regime = _ORIGINAL_MARKET_REGIME(self, at)
    if regime is not MarketRegime.NEUTRAL:
        return regime

    start = (at - timedelta(minutes=5)).isoformat()
    try:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT SUM(CASE WHEN side='buy' THEN 1 ELSE 0 END) buys, "
                "SUM(CASE WHEN side='sell' THEN 1 ELSE 0 END) sells, "
                "SUM(CASE WHEN side='buy' AND observation_lag_ms<=? "
                "AND wallet_price_sol>0 THEN 1 ELSE 0 END) timely_buys "
                "FROM wallet_discovery_forward_observations "
                "WHERE received_at>=? AND received_at<=?",
                (ENTRY_WINDOW_SECONDS * 1000.0, start, at.isoformat()),
            ).fetchone()
        buys = int(row["buys"] or 0) if row else 0
        sells = int(row["sells"] or 0) if row else 0
        timely_buys = int(row["timely_buys"] or 0) if row else 0
    except Exception:
        return regime

    if buys > sells and timely_buys >= 2:
        setattr(
            self,
            "_roi_strategy_admission_high_speculation_repairs",
            int(getattr(self, "_roi_strategy_admission_high_speculation_repairs", 0) or 0) + 1,
        )
        return MarketRegime.HIGH_SPECULATION
    return regime


setattr(_market_regime_with_modern_admission, "_roi_strategy_candidate_admission", True)


def _status_with_modern_admission(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("strategy candidate admission status is not installed")
    payload = _ORIGINAL_STATUS(self)
    payload["strategy_candidate_admission_repair"] = {
        "repair_version": REPAIR_VERSION,
        "legacy_copyable_is_pre_v5_entry_veto": False,
        "durable_legacy_copyability_evidence_mutated": False,
        "v5_amount_specific_execution_remains_authoritative": True,
        "strategy_entry_window_seconds": ENTRY_WINDOW_SECONDS,
        "v5_chase_above_40pct_remains_nonactionable": True,
        "mechanical_hard_stops_unchanged": True,
        "risk_thresholds_changed": False,
        "regime_numeric_thresholds_changed": False,
        "legacy_copyable_bypasses_session": int(getattr(self, "_roi_strategy_admission_bypasses", 0) or 0),
        "bypassed_candidates_evaluated_session": int(getattr(self, "_roi_strategy_admission_evaluated", 0) or 0),
        "bypassed_candidates_actionable_session": int(getattr(self, "_roi_strategy_admission_actionable", 0) or 0),
        "legacy_rejections_session": int(getattr(self, "_roi_strategy_admission_legacy_rejections", 0) or 0),
        "high_speculation_regime_repairs_session": int(getattr(self, "_roi_strategy_admission_high_speculation_repairs", 0) or 0),
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    return payload


setattr(_status_with_modern_admission, "_roi_strategy_candidate_admission", True)


def install_strategy_candidate_admission_repair() -> None:
    """Install after v5.1/regime authority and before the E2E regime probe."""
    global _INSTALLED, _ORIGINAL_BUY, _ORIGINAL_MARKET_REGIME, _ORIGINAL_STATUS
    if _INSTALLED:
        return

    current_buy = FinalProfitFirstResearchAdapter._buy
    if not bool(getattr(current_buy, "_roi_risk_conditioned_v51", False)):
        raise RuntimeError("strategy candidate admission repair requires final v5.1 buy composition")
    _ORIGINAL_BUY = current_buy
    FinalProfitFirstResearchAdapter._buy = _buy_with_modern_admission  # type: ignore[method-assign]

    current_regime = FinalProfitFirstResearchAdapter._market_regime
    _ORIGINAL_MARKET_REGIME = current_regime
    FinalProfitFirstResearchAdapter._market_regime = _market_regime_with_modern_admission  # type: ignore[method-assign]

    current_status = FinalProfitFirstResearchAdapter.status
    _ORIGINAL_STATUS = current_status
    FinalProfitFirstResearchAdapter.status = _status_with_modern_admission  # type: ignore[method-assign]
    _INSTALLED = True

    # The current strategy is continuation-first rather than a sniper. Compose the
    # final paper-only recalibration here so it wraps the already-final v5.1/wallet
    # authority path and is in place before the unified regime probe is installed.
    from .continuation_market_recalibration import install_continuation_market_recalibration

    install_continuation_market_recalibration()

    # Restore legacy helper/band/version contracts around the new final authority.
    # This keeps historical evidence labels and direct helper tests stable while the
    # outer runtime buy/FOMO/Robinhood policies remain continuation-first.
    from .continuation_market_recalibration_finalize import (
        install_continuation_market_recalibration_finalize,
    )

    install_continuation_market_recalibration_finalize()


__all__ = [
    "ENTRY_WINDOW_SECONDS",
    "REPAIR_VERSION",
    "strategy_evaluation_eligible",
    "install_strategy_candidate_admission_repair",
]
