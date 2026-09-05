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

# Production status proved the persistent Robinhood cursor could remain behind the
# live chain when the public rate-limited RPC was scanned in 200-block batches with
# a mandatory five-second sleep after every successful historical batch. Increase
# only non-live acquisition capacity, parallelize independent market-log reads with
# a small bound, and expose catch-up rate/lag telemetry. The existing <=2-block
# paper-decision gate remains authoritative, so historical catch-up cannot trade.
from .robinhood_catchup_capacity_repair import install_robinhood_catchup_capacity_repair

install_robinhood_catchup_capacity_repair()

# Historical catch-up remains bounded to the same exact block work, but its dense
# Python work is CPU-governed so it cannot consume the full single-CPU service budget.
from .robinhood_event_loop_fairness_repair import (
    install_robinhood_event_loop_fairness_repair,
)

install_robinhood_event_loop_fairness_repair()

# PR #132/#133 removed Robinhood event-loop, SQLite and CPU-budget explanations but
# production still reproduced the five-second Render health timeout. Install a
# read-only Render-only watchdog that samples the Python main thread and emits all
# thread stacks whenever Uvicorn fails to return to its normal selector wait for 2.5s.
# It acquires no canonical lock, makes no RPC calls, and has no strategy authority.
from .render_main_thread_stall_diagnostic import install_render_main_thread_stall_diagnostic

install_render_main_thread_stall_diagnostic()

# PR #134-#136 then proved the health failures are a class of problem, not one single
# call site: after each synchronous SQLite hot path was removed, other canonical
# Solana/FOMO tasks (wallet risk queue, live-poll receipt journaling, forward-evidence
# claims, TLS certificate setup) could still occupy the same Uvicorn event loop past
# Render's health deadline. Move the existing canonical worker graph intact onto one
# dedicated OS thread/private asyncio loop. Robinhood remains separately isolated.
# This changes scheduling topology only; market scope, continuity, strategy and paper
# authority are unchanged.
from .canonical_worker_isolation_repair import install_canonical_worker_isolation

install_canonical_worker_isolation()


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


# Production imports this module from robinhood_runtime_install only after base v5,
# the governed FOMO maturity override, specialist wallet allocation and the unified
# regime paper probe are composed. Install v5.1 at this final policy boundary so the
# newer production repairs remain authoritative while entity-exact promotion and
# amount-specific sizing become the active paper policy. Direct/research imports
# retain their historical module semantics and cannot wrap a pre-v5 adapter.
from . import risk_conditioned_alpha_v5 as _risk_v5

if bool(getattr(_risk_v5, "_INSTALLED", False)):
    from .risk_conditioned_alpha_v51 import install_risk_conditioned_alpha_v51

    install_risk_conditioned_alpha_v51()

# Exact-release Robinhood telemetry proved raw ingestion can be healthy while every
# paper context is suppressed by repeated Blockscout identity failures. Install the
# repair only after the final v5.1 policy composition so it wraps the actual active
# methods: entity lookups are deduplicated and negatively cached, unresolved raw
# addresses never count as independent evidence, only decision-critical trigger and
# deployer identities fail closed, and aggregate rejection-funnel telemetry becomes
# visible. The documented Robinhood Stock Token registry also gets bounded retry
# backoff without ever allowing direct-v3 entry while the registry is unavailable.
from .robinhood_entity_resolution_repair import (
    install_robinhood_entity_resolution_repair,
)

install_robinhood_entity_resolution_repair(RobinhoodChainPaperPlane)

# Blockscout retired unauthenticated per-instance API traffic on 2026-07-01. Replace
# only the identity provider adapter with the universal chain-scoped Pro API. The
# resolver now asks for inbound transactions oldest-first, so one provider request
# yields the earliest positive native funder instead of approximating from up to
# three newest-first pages. A missing key fails closed and never makes raw addresses
# independent evidence.
from .robinhood_blockscout_pro_repair import install_robinhood_blockscout_pro_repair

install_robinhood_blockscout_pro_repair(RobinhoodChainPaperPlane)

# PR146 installs the continuation-first Robinhood state machine before this concrete
# production class is created. The entity repair above must remain a substrate repair,
# not replace that newer strategy authority. Rebind PR146's captured base flow to the
# repaired entity resolver, then restore its bootstrap/extended-continuation wrapper
# as the final method. This changes no continuation threshold, position limit, hard
# exit, signing, submission, or live-money boundary.
from .robinhood_continuation_entity_composition_repair import (
    install_robinhood_continuation_entity_composition_repair,
)

install_robinhood_continuation_entity_composition_repair(RobinhoodChainPaperPlane)

# Treat Blockscout as a persistent cache-miss-only identity oracle. Successful
# chain->actor funding proofs are stored in the dedicated Robinhood SQLite database
# across releases, trigger/deployer identities keep protected decision-critical
# credits, and non-trigger buyers are enriched progressively without ever allowing
# unresolved raw addresses to count as independent evidence. This changes provider
# work only: token/venue universe, continuation thresholds, position limits and all
# paper-only/no-signing/no-submission boundaries remain unchanged.
from .robinhood_entity_quota_architecture import (
    install_robinhood_entity_quota_architecture,
)

install_robinhood_entity_quota_architecture(RobinhoodChainPaperPlane)


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
