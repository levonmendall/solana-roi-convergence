from __future__ import annotations

from .v51_production_authority import install_isolated_robinhood_proof_cache

_INSTALLED = False


def install_v51_final_production_hook() -> None:
    """Legacy import-compatibility shim for the isolated Robinhood proof cache.

    PR #179 originally made this function monkeypatch the Robinhood production
    installer so v5.1 economics depended on import order. Production authority is
    now installed explicitly by ``solana_roi.production`` through
    ``install_v51_production_authority``. This compatibility hook may restore the
    nonblocking proof publisher after an isolation-module reload, but it cannot
    install, replace, or alter strategy economics.
    """
    global _INSTALLED
    from . import robinhood_runtime_install as module

    install_isolated_robinhood_proof_cache(module)
    _INSTALLED = True


__all__ = ["install_v51_final_production_hook"]
