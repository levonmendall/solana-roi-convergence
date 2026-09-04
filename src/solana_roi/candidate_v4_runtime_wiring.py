from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .candidate_risk_quote_v4_handoff import attach_candidate_v4_wallet_discovery
from .direct_solana import DirectSolanaIngestionPlane
from .wallet_discovery import ContinuousWalletDiscovery


_ORIGINAL_DIRECT_INIT: Callable[..., Any] | None = None
_ORIGINAL_DISCOVERY_INIT: Callable[..., Any] | None = None
_DIRECT_BY_STORE: dict[int, DirectSolanaIngestionPlane] = {}
_DISCOVERY_BY_STORE: dict[int, ContinuousWalletDiscovery] = {}


def _wire(store: Any) -> None:
    key = id(store)
    direct = _DIRECT_BY_STORE.get(key)
    discovery = _DISCOVERY_BY_STORE.get(key)
    if direct is not None and discovery is not None:
        attach_candidate_v4_wallet_discovery(direct, discovery)


def _direct_init_with_v4_wiring(self: DirectSolanaIngestionPlane, *args: Any, **kwargs: Any) -> None:
    if _ORIGINAL_DIRECT_INIT is None:
        raise RuntimeError("candidate V4 runtime wiring is not installed")
    _ORIGINAL_DIRECT_INIT(self, *args, **kwargs)
    _DIRECT_BY_STORE[id(self.store)] = self
    _wire(self.store)


setattr(_direct_init_with_v4_wiring, "_roi_candidate_v4_runtime_wiring", True)


def _discovery_init_with_v4_wiring(self: ContinuousWalletDiscovery, *args: Any, **kwargs: Any) -> None:
    if _ORIGINAL_DISCOVERY_INIT is None:
        raise RuntimeError("candidate V4 runtime wiring is not installed")
    _ORIGINAL_DISCOVERY_INIT(self, *args, **kwargs)
    _DISCOVERY_BY_STORE[id(self.store)] = self
    _wire(self.store)


setattr(_discovery_init_with_v4_wiring, "_roi_candidate_v4_runtime_wiring", True)


def install_candidate_v4_runtime_wiring() -> None:
    global _ORIGINAL_DIRECT_INIT, _ORIGINAL_DISCOVERY_INIT

    current_direct = DirectSolanaIngestionPlane.__init__
    if not bool(getattr(current_direct, "_roi_candidate_v4_runtime_wiring", False)):
        _ORIGINAL_DIRECT_INIT = current_direct
        try:
            _direct_init_with_v4_wiring.__dict__.update(getattr(current_direct, "__dict__", {}))
        except Exception:
            pass
        setattr(_direct_init_with_v4_wiring, "_roi_candidate_v4_runtime_wiring", True)
        DirectSolanaIngestionPlane.__init__ = _direct_init_with_v4_wiring  # type: ignore[method-assign]

    current_discovery = ContinuousWalletDiscovery.__init__
    if not bool(getattr(current_discovery, "_roi_candidate_v4_runtime_wiring", False)):
        _ORIGINAL_DISCOVERY_INIT = current_discovery
        try:
            _discovery_init_with_v4_wiring.__dict__.update(getattr(current_discovery, "__dict__", {}))
        except Exception:
            pass
        setattr(_discovery_init_with_v4_wiring, "_roi_candidate_v4_runtime_wiring", True)
        ContinuousWalletDiscovery.__init__ = _discovery_init_with_v4_wiring  # type: ignore[method-assign]


__all__ = ["install_candidate_v4_runtime_wiring"]
