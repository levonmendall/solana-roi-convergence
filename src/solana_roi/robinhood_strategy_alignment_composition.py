from __future__ import annotations

import asyncio
from typing import Any, Callable

from . import continuation_market_recalibration as continuation
from . import robinhood_entity_quota_architecture as quota
from . import robinhood_strategy_alignment_repair as alignment
from .post161_candidate_attribution_repair import install_post161_candidate_attribution_repair
from .robinhood_rpc_rate_limit_repair import install_robinhood_rpc_rate_limit_repair


COMPOSITION_VERSION = "robinhood-strategy-alignment-composition-v5-pumpfun-shadow-boundary"
_ORIGINAL_POLL: Callable[..., Any] | None = None


async def _poll_once_with_research_discovery(self: Any) -> None:
    """Observe discovery after the canonical Robinhood poll without touching policy authority."""
    if _ORIGINAL_POLL is None:
        raise RuntimeError("Robinhood strategy alignment poll wrapper is not installed")
    await _ORIGINAL_POLL(self)
    try:
        # Create/read the existing durable proof tables locally. This performs no
        # Blockscout request; provider use remains exclusively in the decision-time
        # entity resolver and under its protected credit budget.
        if not quota._ensure_schema(self):
            raise RuntimeError("Robinhood entity proof schema unavailable")
        added = alignment._record_resolved_entity_observations(self)
        marked = alignment._mark_discovery_observations(self)
        setattr(
            self,
            "_roi_entity_discovery_last_cycle",
            {
                "resolved_forward_observations_added": int(added),
                "marks_added": int(marked),
                "provider_requests_added": 0,
            },
        )
        setattr(self, "_roi_entity_discovery_last_error", None)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        setattr(
            self,
            "_roi_entity_discovery_last_error",
            f"{type(exc).__name__}: post-poll entity discovery accounting unavailable",
        )


def install_robinhood_strategy_alignment_composition(plane_cls: type[Any]) -> None:
    """Keep PR146 continuation flow authoritative and attach research below it."""
    global _ORIGINAL_POLL

    # The post-161 Solana repair extends only transaction-fact decoding and bounded
    # diagnostics at the already fail-closed venue graph. It has no entry authority
    # and is installed before runtime workers start so fresh scout transactions use it.
    install_post161_candidate_attribution_repair()

    # Robinhood's public RPC can return HTTP 429 during exact catch-up. Coordinate
    # Retry-After/cooldown on the same read without skipping block ranges or changing
    # catch-up capacity, strategy thresholds, or the historical-entry prohibition.
    install_robinhood_rpc_rate_limit_repair(plane_cls)

    if bool(getattr(plane_cls, "_roi_robinhood_strategy_alignment_composition_installed", False)):
        return

    # The alignment module originally attached discovery directly to the flow method.
    # Restore the protected PR146 continuation function exactly; existing regressions
    # intentionally assert this object identity because later wrappers must not become
    # an alternate strategy authority.
    plane_cls._v5_flow_metrics = continuation._rh_flow_without_sniper_cap

    _ORIGINAL_POLL = plane_cls._poll_once
    plane_cls._poll_once = _poll_once_with_research_discovery  # type: ignore[method-assign]

    # Mirror Pump.fun at the discovery layer: broad sample -> historical ROI/quality
    # gate -> fresh prospective tracking. Twelve is a ceiling, not a quota, and weak
    # wallets are never retained merely to make the universe look full.
    from .robinhood_pumpfun_wallet_selection import (
        install_robinhood_pumpfun_wallet_selection,
    )

    install_robinhood_pumpfun_wallet_selection(plane_cls)

    # Creator/insider participation is context, not a blanket manipulation veto.
    # Install the point-in-time local risk policy before composing intelligence so
    # the forward teacher gate uses true manipulation blockers and a token-level
    # persisted-risk fallback without any new provider request.
    from .robinhood_wallet_intelligence_policy import (
        install_robinhood_wallet_intelligence_policy,
    )

    install_robinhood_wallet_intelligence_policy()

    # Apply Pump.fun's stricter forward teacher qualification only to the production
    # universe path. The generic role-scoring and discovery helpers stay pure so
    # diagnostic/research consumers retain their original semantics.
    from .robinhood_pumpfun_wallet_intelligence_integration import (
        install_robinhood_pumpfun_wallet_intelligence_integration,
    )

    install_robinhood_pumpfun_wallet_intelligence_integration(plane_cls)

    # Keep one dynamic global entity universe. Roles describe what a wallet/entity is
    # good at; lanes and regimes remain execution context and never create watchlists.
    from .robinhood_entity_universe import install_robinhood_entity_universe

    install_robinhood_entity_universe(plane_cls)

    # Install the trading boundary LAST so it wraps the final global-universe chooser.
    # Robinhood therefore follows the same promotion sequence as Pump.fun:
    # wallet signal -> opportunity classification -> executable/copyable test ->
    # zero-allocation contextual shadow evidence -> positive geometric edge -> sizing
    # -> paper entry. Bootstrap paper allocation is prohibited.
    from .robinhood_pumpfun_shadow_boundary import (
        install_robinhood_pumpfun_shadow_boundary,
    )

    install_robinhood_pumpfun_shadow_boundary(plane_cls)

    # Preserve the existing regression-visible fact that the global entity universe
    # remains in the final poll/status composition even though the shadow boundary is
    # now the outermost wrapper.
    setattr(plane_cls._poll_once, "_roi_robinhood_entity_universe", True)
    setattr(plane_cls.status, "_roi_robinhood_entity_universe", True)
    setattr(plane_cls._v5_choose_lane_fraction, "_roi_robinhood_pumpfun_shadow_boundary", True)

    setattr(plane_cls, "_roi_robinhood_strategy_alignment_composition_installed", True)
    setattr(plane_cls, "_roi_robinhood_strategy_alignment_composition_version", COMPOSITION_VERSION)


__all__ = [
    "COMPOSITION_VERSION",
    "install_robinhood_strategy_alignment_composition",
]
