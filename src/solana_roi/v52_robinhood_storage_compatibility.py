from __future__ import annotations

from typing import Any

from . import robinhood_chain_profit_maximizer as robinhood_strategy
from .risk_conditioned_alpha_v51 import ROBINHOOD_V51_VERSION
from .risk_conditioned_alpha_v5 import robust_return_profile
from .robinhood_chain_profit_maximizer import RobinhoodProfitMaximizerMixin
from .strategy_v52_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH, STRATEGY_VERSION, target_sizing_policy
from .v52_authoritative_strategy import _ensure_v52_epoch, _table_exists


COMPATIBILITY_VERSION = "v52-robinhood-storage-compatibility-1"
_INSTALLED = False


def _v52_epoch_profile(self: Any, **context: Any) -> dict[str, Any]:
    """Read v5.2 Robinhood learning by authority epoch, not legacy table label.

    Robinhood's durable trial/context schemas predate v5.2 and their strategy_version
    values are part of the compatibility contract used by the v5.1 read-only control
    and cross-release research surfaces. v5.2 economic ownership is therefore proven
    by v52_economic_freeze_releases rather than by rewriting that storage label.
    """
    _ensure_v52_epoch(self)
    entity = str(context.get("entity") or "")
    role = str(context.get("role") or "unknown")
    lane = str(context.get("lane") or "unknown")
    venue = str(context.get("venue") or "UNKNOWN")
    lifecycle = str(context.get("lifecycle") or "unknown")
    regime = str(context.get("regime") or "unknown")
    risk_signature = str(context.get("risk_signature") or "clean")
    flow_state = str(context.get("flow_state") or "neutral")
    values: list[float] = []
    if _table_exists(self.store, "robinhood_paper_outcomes") and _table_exists(
        self.store, "robinhood_v5_trial_context"
    ):
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT o.net_return FROM robinhood_paper_outcomes o "
                "JOIN robinhood_paper_trials t ON t.id=o.trial_id "
                "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
                "JOIN v52_economic_freeze_releases e ON e.release_commit=o.release_commit "
                "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND e.strategy_version=? "
                "AND t.trigger_entity=? AND c.trigger_role=? AND c.lane=? AND t.venue=? AND t.lifecycle=? "
                "AND c.regime=? AND c.risk_signature=? AND c.flow_state=? ORDER BY o.id",
                (
                    ECONOMIC_FREEZE_EPOCH,
                    AUTHORITY_ID,
                    STRATEGY_VERSION,
                    entity,
                    role,
                    lane,
                    venue,
                    lifecycle,
                    regime,
                    risk_signature,
                    flow_state,
                ),
            ).fetchall()
        values = [float(row["net_return"]) for row in rows]

    profile = robust_return_profile(
        values,
        grid=robinhood_strategy.ROBINHOOD_V5_POSITION_GRID,
        max_fraction=float(target_sizing_policy()["robinhood_max_target_fraction"]),
        min_samples=int(target_sizing_policy()["minimum_forward_samples"]),
    )
    if profile.state == "promoted_positive_log_growth":
        state = "promoted_positive_log_growth"
    elif profile.state == "demoted_nonpositive_log_growth":
        state = "demoted_nonpositive_log_growth"
    else:
        state = "bootstrap_forward_evidence"
    return {
        "sample_count": profile.sample_count,
        "state": state,
        "best_fraction": profile.best_fraction,
        "best_expected_log_growth": profile.best_expected_log_growth,
        "mean_return": profile.mean_return,
        "median_return": profile.median_return,
        "hit_rate": profile.hit_rate,
        "trimmed_mean_ex_best": profile.trimmed_mean_ex_best,
        "expected_shortfall_20": profile.expected_shortfall_20,
        "winner_concentration": profile.winner_concentration,
        "max_drawdown": profile.max_drawdown_at_best_fraction,
        "evidence_source": "v52_authoritative_release_epoch_only",
        "storage_strategy_version": ROBINHOOD_V51_VERSION,
        "authority_strategy_version": STRATEGY_VERSION,
        "v51_promotion_evidence_used": False,
        "hit_rate_is_promotion_veto": False,
    }


def install_v52_robinhood_storage_compatibility() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    # Restore the durable compatibility label changed transiently by the cutover
    # installer. No decision authority follows from this module-level constant.
    robinhood_strategy.ROBINHOOD_V5_VERSION = ROBINHOOD_V51_VERSION
    RobinhoodProfitMaximizerMixin._v5_profile = _v52_epoch_profile  # type: ignore[method-assign]
    setattr(RobinhoodProfitMaximizerMixin._v5_profile, "_roi_v52_forward_profile", True)
    setattr(RobinhoodProfitMaximizerMixin._v5_profile, "_roi_v52_storage_compatibility", True)
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": COMPATIBILITY_VERSION,
        "installed": _INSTALLED,
        "durable_storage_strategy_version": robinhood_strategy.ROBINHOOD_V5_VERSION,
        "authority_strategy_version": STRATEGY_VERSION,
        "v52_authority_from_release_epoch": True,
        "v51_storage_label_grants_v52_promotion": False,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = ["COMPATIBILITY_VERSION", "install_v52_robinhood_storage_compatibility", "status"]
