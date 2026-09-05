from __future__ import annotations

from .v51_production_authority import install_isolated_robinhood_proof_cache

_INSTALLED = False


def install_v51_final_production_hook() -> None:
    """Compatibility-only proof/readiness preparation for Robinhood production.

    PR #179 originally made this hook monkeypatch the Robinhood production installer,
    which meant v5.1 economics depended on import order. Final economic authority is
    now installed explicitly by ``solana_roi.production`` through
    ``install_v51_production_authority``.

    This hook may restore the isolated-worker proof publisher and the PR #182
    zero-allocation/public-status proof-readiness surface before Robinhood transport
    captures its status provider. It cannot select, size, promote, kill, settle,
    allocate, or otherwise change strategy economics.
    """
    global _INSTALLED
    from . import final_production_proof_readiness_repair as readiness
    from . import robinhood_runtime_install as module
    from . import robinhood_worker_isolation_repair as isolation

    # Private-store proof is produced inside the isolated Robinhood worker and copied
    # into the nonblocking status cache. This remains observability-only.
    install_isolated_robinhood_proof_cache(module)

    # Capture the current raw nonblocking cache function before readiness sanitation.
    # On an isolation-module reload this is a fresh function even when the readiness
    # module itself is already installed, so explicitly refreshing the predecessor
    # avoids a stale wrapper chain.
    cache_status = isolation._nonblocking_status
    already_sanitized = bool(
        getattr(cache_status, "_roi_final_production_proof_readiness", False)
    )

    readiness.install_final_production_proof_readiness_repair()

    if not already_sanitized:
        readiness._ORIGINAL_ROBINHOOD_STATUS = cache_status
        try:
            readiness._public_robinhood_status.__dict__.update(
                getattr(cache_status, "__dict__", {})
            )
        except Exception:
            pass
        setattr(
            readiness._public_robinhood_status,
            "_roi_final_production_proof_readiness",
            True,
        )
        module._status = readiness._public_robinhood_status
        isolation._nonblocking_status = readiness._public_robinhood_status

    _INSTALLED = True


__all__ = ["install_v51_final_production_hook"]
