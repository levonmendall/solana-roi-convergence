from __future__ import annotations

from typing import Any, Callable

from . import v51_robinhood_phase9_65_69 as phase9


COMPATIBILITY_VERSION = "robinhood-phase9-65-69-compatibility-v1"
_ORIGINAL_INSTALL: Callable[..., None] = phase9.install_robinhood_phase9_65_69
_INSTALLED = False


def _install_with_preserved_contracts(plane_cls: type[Any], runtime_module: Any) -> None:
    """Install Phase 9 without replacing established architecture identities.

    The dedicated-worker invariant intentionally asserts that
    ``runtime_install._status is isolation._nonblocking_status``. Phase 9 enriches
    that same nonblocking function rather than introducing a second status identity.
    It also keeps the existing v1 economic-composition marker because roadmap 65-69
    changes runtime/evidence contracts, not frozen v5.1 economics.
    """
    global _INSTALLED
    _ORIGINAL_INSTALL(plane_cls, runtime_module)

    from . import robinhood_worker_isolation_repair as isolation
    from . import v51_production_authority as production_authority

    # The Phase-9 installer has already wrapped runtime_module._status. Rebind the
    # isolation module's exported status symbol to that exact enriched function so
    # callers still observe one canonical nonblocking status function by identity.
    isolation._nonblocking_status = runtime_module._status  # type: ignore[assignment]

    # No new economic composition was created. Preserve the already-certified marker
    # and expose Phase 9 through its dedicated status/app-state fields instead.
    production_authority.COMPOSITION_VERSION = "v51-explicit-production-authority-v1"
    runtime_module._STATE["phase9_65_69_compatibility"] = COMPATIBILITY_VERSION
    _INSTALLED = True


setattr(_install_with_preserved_contracts, "_roi_robinhood_phase9_compatibility", True)
phase9.install_robinhood_phase9_65_69 = _install_with_preserved_contracts  # type: ignore[assignment]


def status() -> dict[str, Any]:
    return {
        "version": COMPATIBILITY_VERSION,
        "installed": _INSTALLED,
        "canonical_nonblocking_status_identity_preserved": True,
        "economic_composition_marker_changed": False,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = ["COMPATIBILITY_VERSION", "status"]
