from __future__ import annotations

import os
from functools import wraps
from typing import Any, Callable

from . import robinhood_live_frontier_verification_repair as frontier
from . import robinhood_v2_v4_observation as observation


PAUSE_GUARD_VERSION = "robinhood-v2-v4-pause-guard-v3-enforced-fetch-seam"
_ENABLE_ENV = "ROBINHOOD_V2_V4_OBSERVATION_ENABLED"
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
SCHEDULE_GUARD_ATTR = "_roi_v2_v4_explicit_opt_in_schedule"
_FETCH_FACTORY_GUARD_ATTR = "_roi_v2_v4_pause_guard_factory"
_FETCH_GUARD_ATTR = "_roi_v2_v4_pause_guard_fetch"

_INSTALLED = False
_ORIGINAL_ENABLED: Callable[[], bool] = observation._enabled
_ORIGINAL_FETCH_FACTORY: Callable[..., Any] = observation._fetch_with_observation


def _explicit_enable_only() -> bool:
    """Return true only when production explicitly opts V2/V4 back in."""
    raw = os.getenv(_ENABLE_ENV)
    if raw is None:
        return False
    return raw.strip().lower() in _TRUE_VALUES


def _is_schedule_guarded(target: Any) -> bool:
    """Identify production-composed objects whose scheduler must fail closed."""
    return bool(
        getattr(target, SCHEDULE_GUARD_ATTR, False)
        or getattr(type(target), SCHEDULE_GUARD_ATTR, False)
    )


def schedule_enabled(target: Any) -> bool:
    """Resolve scheduling authority without mutating the observer primitive.

    Production-composed Robinhood planes require an explicit true deployment value.
    Uncomposed helpers/tests retain the observation module's historical predicate so
    the primitive remains independently callable and deterministic.
    """
    if _is_schedule_guarded(target):
        return _explicit_enable_only()
    return bool(observation._enabled())


def _guarded_fetch_factory(original: Callable[..., Any]) -> Callable[..., Any]:
    """Build the observation wrapper with an enforced production scheduling gate.

    When a production-composed plane is paused, call the canonical fetch directly and
    never enter the V2/V4 observer. When explicitly enabled, preserve the observation
    wrapper unchanged. Direct/uncomposed helpers keep their historical predicate.
    """
    observed = _ORIGINAL_FETCH_FACTORY(original)

    @wraps(observed)
    async def wrapped(self: Any, *, from_block: int, to_block: int):
        if _is_schedule_guarded(self) and not _explicit_enable_only():
            return await original(self, from_block=from_block, to_block=to_block)
        return await observed(self, from_block=from_block, to_block=to_block)

    setattr(wrapped, "_roi_v2_v4_observation_fetch", True)
    setattr(wrapped, _FETCH_GUARD_ATTR, True)
    return wrapped


setattr(_guarded_fetch_factory, _FETCH_FACTORY_GUARD_ATTR, True)


def _guard_existing_observation_fetch() -> None:
    """Guard an already-composed observation wrapper on repeated installation.

    Normal production composition installs this pause guard before observation. This
    fallback makes re-installation deterministic without weakening fail-closed safety.
    """
    current = frontier._fetch_market_logs
    if not bool(getattr(current, "_roi_v2_v4_observation_fetch", False)):
        return
    if bool(getattr(current, _FETCH_GUARD_ATTR, False)):
        return
    original = observation._ORIGINAL_FETCH
    if original is None:
        return

    @wraps(current)
    async def guarded(self: Any, *, from_block: int, to_block: int):
        if _is_schedule_guarded(self) and not _explicit_enable_only():
            return await original(self, from_block=from_block, to_block=to_block)
        return await current(self, from_block=from_block, to_block=to_block)

    setattr(guarded, "_roi_v2_v4_observation_fetch", True)
    setattr(guarded, _FETCH_GUARD_ATTR, True)
    frontier._fetch_market_logs = guarded  # type: ignore[assignment]


def install_robinhood_v2_v4_pause_guard(plane_cls: type[Any] | None = None) -> None:
    """Install the fail-closed marker and enforce it at the fetch scheduling seam."""
    global _INSTALLED
    if plane_cls is not None:
        setattr(plane_cls, SCHEDULE_GUARD_ATTR, True)

    current_factory = observation._fetch_with_observation
    if not bool(getattr(current_factory, _FETCH_FACTORY_GUARD_ATTR, False)):
        observation._fetch_with_observation = _guarded_fetch_factory  # type: ignore[assignment]

    _guard_existing_observation_fetch()
    _INSTALLED = True


def status() -> dict[str, Any]:
    raw = os.getenv(_ENABLE_ENV)
    return {
        "version": PAUSE_GUARD_VERSION,
        "installed": _INSTALLED,
        "environment_present": raw is not None,
        "explicitly_enabled": _explicit_enable_only(),
        "default_when_missing": False,
        "reactivation_requires_explicit_true": True,
        "schedule_boundary_only": True,
        "schedule_boundary_enforced_at_fetch": bool(
            getattr(observation._fetch_with_observation, _FETCH_FACTORY_GUARD_ATTR, False)
        ),
        "paused_path_calls_canonical_fetch_only": True,
        "observer_predicate_globally_mutated": False,
        "strategy_thresholds_changed": False,
        "market_scope_changed_when_enabled": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "PAUSE_GUARD_VERSION",
    "SCHEDULE_GUARD_ATTR",
    "install_robinhood_v2_v4_pause_guard",
    "schedule_enabled",
    "status",
]
