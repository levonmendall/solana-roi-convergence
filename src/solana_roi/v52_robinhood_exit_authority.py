from __future__ import annotations

from statistics import median
from typing import Any, Callable

from . import robinhood_chain_profit_maximizer as robinhood_strategy
from .robinhood_chain_profit_maximizer import RobinhoodProfitMaximizerMixin
from .strategy_v52_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH, STRATEGY_VERSION


EXIT_AUTHORITY_VERSION = "v52-robinhood-forward-exit-authority-2-release-epoch"
_INSTALLED = False
_ORIGINAL_EXIT_POLICY: Callable[..., Any] | None = None


def _bootstrap() -> dict[str, Any]:
    return {
        "source": "v52_frozen_bootstrap_exit",
        "stop": robinhood_strategy.STOP_LOSS_FRACTION,
        "harvest": robinhood_strategy.HARVEST_FRACTION,
        "max_hold": robinhood_strategy.MAX_HOLD_SECONDS,
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "v51_evidence_used": False,
    }


def _v52_learned_exit_policy(self: Any, trial: dict[str, Any]) -> dict[str, Any]:
    """Learn exits only from outcomes settled in a registered v5.2 release epoch.

    Robinhood's durable trial/context strategy labels remain the v5.1-compatible
    storage schema. They are not authority evidence. The release/authority epoch is
    the sole filter deciding which forward outcomes can train this v5.2 exit owner.
    """
    trial_id = int(trial["id"])
    try:
        with self.store._lock:
            context = self.store.db.execute(
                "SELECT lane FROM robinhood_v5_trial_context WHERE trial_id=? LIMIT 1",
                (trial_id,),
            ).fetchone()
        if context is None:
            return _bootstrap()
        lane = str(context["lane"])
        with self.store._lock:
            closed = self.store.db.execute(
                "SELECT o.trial_id FROM robinhood_paper_outcomes o "
                "JOIN robinhood_paper_trials t ON t.id=o.trial_id "
                "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
                "JOIN v52_economic_freeze_releases e ON e.release_commit=o.release_commit "
                "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND e.strategy_version=? "
                "AND c.lane=? AND t.venue=? AND t.lifecycle=? ORDER BY o.id",
                (
                    ECONOMIC_FREEZE_EPOCH,
                    AUTHORITY_ID,
                    STRATEGY_VERSION,
                    lane,
                    str(trial["venue"]),
                    str(trial["lifecycle"]),
                ),
            ).fetchall()
    except Exception:
        return _bootstrap()

    ids = [int(row["trial_id"]) for row in closed]
    if len(ids) < robinhood_strategy.ROBINHOOD_V5_MIN_SAMPLES:
        return _bootstrap()
    placeholders = ",".join("?" for _ in ids)
    try:
        with self.store._lock:
            marks = self.store.db.execute(
                f"SELECT trial_id,elapsed_seconds,net_return FROM robinhood_v5_marks "
                f"WHERE trial_id IN ({placeholders}) ORDER BY id",
                tuple(ids),
            ).fetchall()
    except Exception:
        return _bootstrap()

    grouped: dict[int, list[tuple[float, float]]] = {}
    for row in marks:
        grouped.setdefault(int(row["trial_id"]), []).append(
            (float(row["elapsed_seconds"]), float(row["net_return"]))
        )
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
    if len(mfes) < robinhood_strategy.ROBINHOOD_V5_MIN_SAMPLES:
        return _bootstrap()

    median_mfe = median(mfes)
    median_mae = median(maes)
    harvest = (
        min(0.75, max(0.15, median_mfe * 0.70))
        if median_mfe > 0.0
        else robinhood_strategy.HARVEST_FRACTION
    )
    stop = (
        min(-0.08, max(-0.30, median_mae * 1.20))
        if median_mae < 0.0
        else robinhood_strategy.STOP_LOSS_FRACTION
    )
    max_hold = min(
        float(robinhood_strategy.MAX_HOLD_SECONDS),
        max(120.0, median(time_to_mfe) * 1.50),
    )
    return {
        "source": "v52_forward_epoch_mfe_mae",
        "stop": stop,
        "harvest": harvest,
        "max_hold": max_hold,
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "v51_evidence_used": False,
    }


def install_v52_robinhood_exit_authority() -> None:
    global _INSTALLED, _ORIGINAL_EXIT_POLICY
    if _INSTALLED:
        return
    _ORIGINAL_EXIT_POLICY = RobinhoodProfitMaximizerMixin._v5_learned_exit_policy
    RobinhoodProfitMaximizerMixin._v5_learned_exit_policy = _v52_learned_exit_policy  # type: ignore[method-assign]
    setattr(RobinhoodProfitMaximizerMixin._v5_learned_exit_policy, "_roi_v52_exit_authority", True)
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": EXIT_AUTHORITY_VERSION,
        "installed": _INSTALLED,
        "final_exit_policy_owner": "v52",
        "authority_filter": "v52_release_epoch",
        "v51_evidence_used": False,
        "fresh_v52_forward_learning_only": True,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "EXIT_AUTHORITY_VERSION",
    "install_v52_robinhood_exit_authority",
    "status",
]
