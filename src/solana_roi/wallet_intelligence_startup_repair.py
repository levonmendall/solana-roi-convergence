from __future__ import annotations

from dataclasses import asdict
from typing import Any

from .wallet_intelligence import (
    ContinuousWalletIntelligence as _OriginalWalletIntelligence,
    WalletPromotionPolicy,
)


_ORIGINAL_INTELLIGENCE = _OriginalWalletIntelligence


class StartupIsolatedWalletIntelligence:
    """Defer research-only wallet-intelligence schema work until background use.

    PR #59 moved ``ContinuousWalletIntelligence`` from an on-demand API helper
    into ``build_runtime()``, making its SQLite schema bootstrap part of Uvicorn
    startup. This proxy restores the old availability boundary: construction and
    status are side-effect free, while the real implementation is created only
    when the background wallet-discovery lane first needs an intelligence method.

    The component remains research-only and cannot authorize paper or live money.
    """

    def __init__(self, store: Any, policy: WalletPromotionPolicy | None = None):
        self.store = store
        self.policy = policy or WalletPromotionPolicy()
        self._inner: Any | None = None
        self._startup_state = "deferred"
        self._startup_attempts = 0
        self._startup_error_type: str | None = None
        self._startup_error_message: str | None = None

    def _record_failure(self, exc: BaseException) -> None:
        self._inner = None
        self._startup_state = "bootstrap_failed"
        self._startup_error_type = type(exc).__name__
        self._startup_error_message = str(exc)[:300] or type(exc).__name__

    def _ensure_inner(self) -> Any:
        if self._inner is not None:
            return self._inner
        self._startup_attempts += 1
        try:
            inner = _ORIGINAL_INTELLIGENCE(self.store, self.policy)
        except Exception as exc:
            self._record_failure(exc)
            raise
        self._inner = inner
        self._startup_state = "ready"
        self._startup_error_type = None
        self._startup_error_message = None
        return inner

    def status(self) -> dict[str, Any]:
        if self._inner is not None:
            try:
                payload = self._inner.status()
            except Exception as exc:
                self._record_failure(exc)
            else:
                if isinstance(payload, dict):
                    result = dict(payload)
                    result.update(
                        {
                            "startup_isolation_enabled": True,
                            "startup_state": self._startup_state,
                            "startup_attempts": self._startup_attempts,
                            "startup_error_type": None,
                            "startup_error_message": None,
                        }
                    )
                    return result

        # Deliberately avoid SQLite access here. Render health/readiness must be
        # able to report a degraded research lane even when its schema cannot be
        # opened yet. The active strategy never depends on these values.
        return {
            "research_lane": True,
            "paper_only": True,
            "live_money_authority": False,
            "signing_or_submission_available": False,
            "active_forward_cohort_immutable": True,
            "incumbent_wallets": [],
            "observed_wallets": 0,
            "eligible_challengers": 0,
            "top_challengers": [],
            "latest_proposed_cohort": None,
            "promotion_policy": asdict(self.policy),
            "startup_isolation_enabled": True,
            "startup_state": self._startup_state,
            "startup_attempts": self._startup_attempts,
            "startup_error_type": self._startup_error_type,
            "startup_error_message": self._startup_error_message,
            "active_strategy_mutation_allowed": False,
        }

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self._ensure_inner(), name)


def install_wallet_intelligence_startup_isolation() -> None:
    """Replace only the constructor captured by runtime.build_runtime."""

    from . import runtime as runtime_module

    current = runtime_module.ContinuousWalletIntelligence
    if current is StartupIsolatedWalletIntelligence:
        return
    runtime_module.ContinuousWalletIntelligence = StartupIsolatedWalletIntelligence  # type: ignore[assignment]


__all__ = [
    "StartupIsolatedWalletIntelligence",
    "install_wallet_intelligence_startup_isolation",
]
