from __future__ import annotations

from typing import Any

from .robinhood_chain_core import *
from .robinhood_chain_state import RobinhoodStateMixin
from .robinhood_chain_identity import RobinhoodIdentityMixin
from .robinhood_chain_metrics import RobinhoodMetricsMixin
from .robinhood_chain_ingest import RobinhoodIngestMixin
from .robinhood_chain_decision import RobinhoodDecisionMixin
from .robinhood_chain_settlement import RobinhoodSettlementMixin
from .robinhood_chain_runtime import RobinhoodRuntimeMixin
from .robinhood_chain_profit_maximizer import RobinhoodProfitMaximizerMixin
from .strategy_specialist_wallet_allocator_repair import (
    install_strategy_specialist_wallet_allocator_repair,
)

# robinhood_runtime_install composes PR120 v5 and then installs the Solana specialist
# wallet allocator before importing this module. Apply the narrow fairness repair at
# that boundary so each strategy family gets a specialist before a second regime of
# the same strategy can consume scarce capacity. This changes no Robinhood entity
# authority, strategy threshold, mechanical hard stop, signing, or live-money scope.
install_strategy_specialist_wallet_allocator_repair()


class RobinhoodChainPaperPlane(
    RobinhoodProfitMaximizerMixin,
    RobinhoodStateMixin,
    RobinhoodIdentityMixin,
    RobinhoodMetricsMixin,
    RobinhoodIngestMixin,
    RobinhoodDecisionMixin,
    RobinhoodSettlementMixin,
    RobinhoodRuntimeMixin,
):
    """Active Robinhood Chain paper plane with risk-conditioned v5 policy authority."""

    async def _settle_one(self, trial: dict[str, Any]) -> None:
        """Use learned v5 exits only for v5 trials; preserve legacy audit semantics."""
        try:
            with self.store._lock:
                context = self.store.db.execute(
                    "SELECT 1 FROM robinhood_v5_trial_context WHERE trial_id=? LIMIT 1",
                    (int(trial["id"]),),
                ).fetchone()
        except Exception:
            context = None
        if context is None:
            await RobinhoodSettlementMixin._settle_one(self, trial)
            return
        await RobinhoodProfitMaximizerMixin._settle_one(self, trial)


__all__ = [
    "RobinhoodChainPaperPlane",
    "ROBINHOOD_CHAIN_ID",
    "ROBINHOOD_CHAIN_PAPER_VERSION",
    "PAPER_TRADING_AUTHORITY",
    "PAPER_ONLY",
    "LIVE_MONEY_AUTHORITY",
    "SIGNING_AVAILABLE",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "classify_context_returns",
]
