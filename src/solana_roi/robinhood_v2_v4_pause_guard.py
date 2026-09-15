from __future__ import annotations

import os
from typing import Any, Callable

from . import robinhood_v2_v4_observation as observation


PAUSE_GUARD_VERSION = "robinhood-v2-v4-pause-guard-v2-schedule-boundary"
_ENABLE_ENV = "ROBINHOOD_V2_V4_OBSERVATION_ENABLED"
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
SCHEDULE_GUARD_ATTR = "_roi_v2_v4_explicit_opt_in_schedule"

_INSTALLED = False
_ORIGINAL_ENABLED: Callable[[], bool] = observation._enabled


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


def install_robinhood_v2_v4_pause_guard(plane_cls: type[Any] | None = None) -> None:
    """Install a fail-closed marker at the production scheduling seam.

    Do not replace ``observation._enabled`` globally. Global replacement leaked the
    production pause into direct observer primitives and unrelated deterministic
    tests, while the required safety property is narrower: production scheduling must
    not invoke V2/V4 unless explicitly enabled.
    """
    global _INSTALLED
    if plane_cls is not None:
        setattr(plane_cls, SCHEDULE_GUARD_ATTR, True)
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
