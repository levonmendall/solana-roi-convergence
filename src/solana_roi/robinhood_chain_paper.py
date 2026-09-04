from __future__ import annotations

from .robinhood_chain_core import *
from .robinhood_chain_state import RobinhoodStateMixin
from .robinhood_chain_identity import RobinhoodIdentityMixin
from .robinhood_chain_metrics import RobinhoodMetricsMixin
from .robinhood_chain_ingest import RobinhoodIngestMixin
from .robinhood_chain_decision import RobinhoodDecisionMixin
from .robinhood_chain_settlement import RobinhoodSettlementMixin
from .robinhood_chain_runtime import RobinhoodRuntimeMixin
from .robinhood_chain_profit_maximizer import RobinhoodProfitMaximizerMixin


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
