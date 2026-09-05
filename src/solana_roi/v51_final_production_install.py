from __future__ import annotations

from typing import Any, Callable

from .v51_consolidated_strategy import install_v51_consolidated_strategy
from .v51_robinhood_consolidation import install_v51_robinhood_consolidation
from .v51_robinhood_proof import cached_robinhood_proof
from .v51_strategy_api import install_v51_strategy_api

_INSTALLED = False


def _install_isolated_robinhood_proof_cache(module: Any) -> None:
    """Publish proof from Robinhood's private store through its status cache.

    Robinhood intentionally owns a separate SQLite database and OS thread. The
    Uvicorn/canonical path must not reopen that database merely to build a combined
    dashboard. The isolation worker already calls ``_ORIGINAL_STATUS`` inside its
    private thread before deep-copying the result into a nonblocking status cache;
    enrich that exact publication point with a 30-second proof snapshot.
    """
    from . import robinhood_worker_isolation_repair as isolation

    current = isolation._ORIGINAL_STATUS
    if current is None or bool(getattr(current, "_roi_v51_isolated_proof", False)):
        return

    def status_with_v51_proof() -> dict[str, Any]:
        payload = dict(current())
        plane = getattr(module, "_PLANE", None)
        if plane is None:
            payload["v51_proof"] = {
                "available": False,
                "reason": "isolated_robinhood_plane_not_ready",
                "paper_only": True,
                "live_money_authority": False,
            }
            return payload
        try:
            proof = cached_robinhood_proof(plane.store)
            proof["available"] = True
            payload["v51_proof"] = proof
        except Exception as exc:
            payload["v51_proof"] = {
                "available": False,
                "reason": f"{type(exc).__name__}: isolated Robinhood proof unavailable",
                "paper_only": True,
                "live_money_authority": False,
            }
        return payload

    setattr(status_with_v51_proof, "_roi_v51_isolated_proof", True)
    isolation._ORIGINAL_STATUS = status_with_v51_proof


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

    # This function is called from the end of robinhood_worker_isolation_repair,
    # after that repair has captured the original deep status provider. Enrich the
    # private-thread publication point before any production worker can start.
    _install_isolated_robinhood_proof_cache(module)

    def install(app: Any, runtime_provider: Callable[[], Any]) -> None:
        original(app, runtime_provider)
        install_v51_consolidated_strategy()
        install_v51_robinhood_consolidation()
        # module._status is the worker-isolation repair's nonblocking cache reader.
        install_v51_strategy_api(app, runtime_provider, robinhood_status_provider=module._status)
        app.state.roi_v51_final_economic_authority = True

    setattr(install, "_roi_v51_final_authority", True)
    module.install_robinhood_chain_paper_runtime = install
    _INSTALLED = True


__all__ = ["install_v51_final_production_hook"]
