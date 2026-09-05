from __future__ import annotations

from typing import Any, Callable

from .v51_consolidated_strategy import install_v51_consolidated_strategy
from .v51_robinhood_consolidation import install_v51_robinhood_consolidation
from .v51_strategy_api import install_v51_strategy_api

_INSTALLED = False


def install_v51_final_production_hook() -> None:
    """Wrap the existing production Robinhood installer at its final import boundary.

    `production.py` intentionally remains the canonical Render entrypoint because
    its constant-time liveness and post104 continuity invariants are separately
    certified. `robinhood_runtime_install` is imported only after the Solana app is
    constructed and all prior economic/transport wrappers are installed, so wrapping
    its final installer gives v5.1 one last economic-authority boundary without
    replacing the ASGI entrypoint or startup architecture.
    """
    global _INSTALLED
    if _INSTALLED:
        return
    from . import robinhood_runtime_install as module

    original: Callable[[Any, Callable[[], Any]], None] = module.install_robinhood_chain_paper_runtime
    if bool(getattr(original, "_roi_v51_final_authority", False)):
        _INSTALLED = True
        return

    def install(app: Any, runtime_provider: Callable[[], Any]) -> None:
        original(app, runtime_provider)
        install_v51_consolidated_strategy()
        install_v51_robinhood_consolidation()
        install_v51_strategy_api(app, runtime_provider)
        app.state.roi_v51_final_economic_authority = True

    setattr(install, "_roi_v51_final_authority", True)
    module.install_robinhood_chain_paper_runtime = install
    _INSTALLED = True


__all__ = ["install_v51_final_production_hook"]
