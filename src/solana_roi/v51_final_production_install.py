from __future__ import annotations

from typing import Any, Callable

from .v51_consolidated_strategy import install_v51_consolidated_strategy
from .v51_robinhood_consolidation import install_v51_robinhood_consolidation
from .v51_strategy_api import install_v51_strategy_api

_INSTALLED = False


def _install_isolated_robinhood_proof_cache(module: Any) -> None:
    """Publish v5.1 proof from Robinhood's private worker/store.

    The worker-isolation repair calls ``_ORIGINAL_STATUS`` from the dedicated
    Robinhood thread and then deep-copies the result into a nonblocking cache.
    Enriching that producer is therefore the safe place to read Robinhood SQLite:
    Uvicorn and the canonical Solana store only consume the copied proof object.
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
            from .v51_robinhood_consolidation import refresh_robinhood_candidate_learning
            from .v51_robinhood_proof import cached_robinhood_proof

            # Register this private store's exact release into the frozen epoch.
            # The installer is already globally active; supplying a store here only
            # establishes evidence lineage for this isolated SQLite database.
            install_v51_consolidated_strategy(
                store=plane.store,
                release_commit=getattr(plane, "release_commit", None),
            )
            refresh_robinhood_candidate_learning(plane.store)
            proof = cached_robinhood_proof(plane.store)
            proof["available"] = True
            payload["v51_proof"] = proof
        except Exception as exc:
            payload["v51_proof"] = {
                "available": False,
                "reason": "isolated_robinhood_proof_failed_closed",
                "error_type": type(exc).__name__,
                "paper_only": True,
                "live_money_authority": False,
            }
        return payload

    setattr(status_with_v51_proof, "_roi_v51_isolated_proof", True)
    isolation._ORIGINAL_STATUS = status_with_v51_proof


def install_v51_final_production_hook() -> None:
    """Wrap the existing production Robinhood installer at its final boundary.

    ``solana_roi.production:app`` intentionally remains the certified Render
    entrypoint. ``robinhood_runtime_install`` is imported only after all prior
    Solana/FOMO/Robinhood compatibility composition exists, so its final installer
    is the last safe place to make v5.1 the economic authority without replacing
    constant-time liveness or startup architecture.
    """
    global _INSTALLED
    if _INSTALLED:
        return
    from . import robinhood_runtime_install as module

    original: Callable[[Any, Callable[[], Any]], None] = module.install_robinhood_chain_paper_runtime
    if bool(getattr(original, "_roi_v51_final_authority", False)):
        _INSTALLED = True
        return

    # Called from the end of robinhood_worker_isolation_repair, after that repair
    # captured the worker-thread status producer but before any lifespan worker can
    # start. The proof is therefore produced in the isolated thread and consumed
    # from module._status's existing nonblocking cache reader.
    _install_isolated_robinhood_proof_cache(module)

    def install(app: Any, runtime_provider: Callable[[], Any]) -> None:
        original(app, runtime_provider)
        install_v51_consolidated_strategy()
        install_v51_robinhood_consolidation()
        install_v51_strategy_api(
            app,
            runtime_provider,
            robinhood_status_provider=module._status,
        )
        app.state.roi_v51_final_economic_authority = True

    setattr(install, "_roi_v51_final_authority", True)
    module.install_robinhood_chain_paper_runtime = install
    _INSTALLED = True


__all__ = ["install_v51_final_production_hook"]
