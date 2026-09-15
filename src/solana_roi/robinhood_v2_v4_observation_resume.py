from __future__ import annotations

from typing import Any

from . import robinhood_v2_v4_observation as observation


RESUME_VERSION = "robinhood-v2-v4-observation-resume-v1"
_ORIGINAL_OBSERVE_RANGE = observation._observe_range


async def _resume_from_observer_cursor(
    self: Any,
    *,
    from_block: int,
    to_block: int,
) -> None:
    """Resume short observer gaps from observer state, not the newer canonical range."""
    observation._ensure_state(self)
    cursor = getattr(self, "_roi_v2v4_observation_cursor", None)
    effective_from = int(cursor) + 1 if cursor is not None else int(from_block)
    await _ORIGINAL_OBSERVE_RANGE(
        self,
        from_block=effective_from,
        to_block=int(to_block),
    )


setattr(_resume_from_observer_cursor, "_roi_v2_v4_observation_resume", True)


def install_robinhood_v2_v4_observation_resume() -> None:
    current = observation._observe_range
    if bool(getattr(current, "_roi_v2_v4_observation_resume", False)):
        return
    observation._observe_range = _resume_from_observer_cursor  # type: ignore[assignment]


__all__ = [
    "RESUME_VERSION",
    "install_robinhood_v2_v4_observation_resume",
]
