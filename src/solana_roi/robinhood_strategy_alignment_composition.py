from __future__ import annotations

import asyncio
from typing import Any, Callable

from . import continuation_market_recalibration as continuation
from . import robinhood_entity_quota_architecture as quota
from . import robinhood_strategy_alignment_repair as alignment


COMPOSITION_VERSION = "robinhood-strategy-alignment-composition-v1"
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
    if bool(getattr(plane_cls, "_roi_robinhood_strategy_alignment_composition_installed", False)):
        return

    # The alignment module originally attached discovery directly to the flow method.
    # Restore the protected PR146 continuation function exactly; existing regressions
    # intentionally assert this object identity because later wrappers must not become
    # an alternate strategy authority.
    plane_cls._v5_flow_metrics = continuation._rh_flow_without_sniper_cap

    _ORIGINAL_POLL = plane_cls._poll_once
    plane_cls._poll_once = _poll_once_with_research_discovery  # type: ignore[method-assign]

    setattr(plane_cls, "_roi_robinhood_strategy_alignment_composition_installed", True)
    setattr(plane_cls, "_roi_robinhood_strategy_alignment_composition_version", COMPOSITION_VERSION)


__all__ = [
    "COMPOSITION_VERSION",
    "install_robinhood_strategy_alignment_composition",
]
