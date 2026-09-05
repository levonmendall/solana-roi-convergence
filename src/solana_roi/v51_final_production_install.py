from __future__ import annotations

from .v51_production_authority import install_isolated_robinhood_proof_cache

_INSTALLED = False


def install_v51_final_production_hook() -> None:
    """Compatibility-only proof/readiness preparation for Robinhood production.

    PR #179 originally made this hook monkeypatch the Robinhood production installer,
    which meant v5.1 economics depended on import order. Final economic authority is
    now installed explicitly by ``solana_roi.production`` through
    ``install_v51_production_authority``.

    This hook is allowed to restore the isolated-worker proof publisher and install
    the PR #182 zero-allocation/public-status proof-readiness repair before the
    Robinhood transport installer captures its status provider. It cannot select,
    size, promote, kill, settle, allocate, or otherwise change strategy economics.
    """
    global _INSTALLED
    from . import robinhood_runtime_install as module
    from . import robinhood_worker_isolation_repair as isolation
    from .final_production_proof_readiness_repair import (
        install_final_production_proof_readiness_repair,
    )

    # Private-store proof is produced inside the isolated Robinhood worker and copied
    # into the nonblocking status cache. This remains observability-only.
    install_isolated_robinhood_proof_cache(module)

    # PR #182 proof/readiness must exist before install_robinhood_chain_paper_runtime
    # captures its app-facing status provider. The repair is zero-allocation and has
    # no candidate/paper/live authority.
    install_final_production_proof_readiness_repair()

    # Preserve the dedicated-worker identity invariant expected by the isolation
    # layer while making the captured public status provider the sanitized wrapper.
    try:
        module._status.__dict__.update(getattr(isolation._nonblocking_status, "__dict__", {}))
    except Exception:
        pass
    isolation._nonblocking_status = module._status

    _INSTALLED = True


__all__ = ["install_v51_final_production_hook"]
