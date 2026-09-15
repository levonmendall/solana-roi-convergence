from __future__ import annotations

"""Bind the Robinhood shared-capital bridge to the canonical runtime store.

The v5.2 production composition already resolves one ``IngestionRuntime`` and that
runtime owns the canonical ``ObservationEventStore`` used by the Solana/FOMO
paper lifecycle.  Robinhood's market store remains isolated, but buying-power
reservations must use this runtime store even before the lifecycle worker has
published its transient active-adapter handle.
"""

from types import SimpleNamespace
from typing import Any

from . import v52_shared_paper_capital_bridge as bridge


BINDING_VERSION = "v52-shared-paper-capital-runtime-binding-1"
_BOUND_RUNTIME: Any | None = None
_BASE_CANONICAL_ADAPTER: Any | None = None
_INSTALLED = False


def _canonical_adapter_with_runtime(owner: Any | None = None) -> Any | None:
    if _BASE_CANONICAL_ADAPTER is not None:
        adapter = _BASE_CANONICAL_ADAPTER(owner)
        if adapter is not None and getattr(adapter, "store", None) is not None:
            return adapter
    runtime = _BOUND_RUNTIME
    store = getattr(runtime, "store", None) if runtime is not None else None
    if store is None:
        return None
    release = str(getattr(owner, "release_commit", "") or "") if owner is not None else ""
    # Reservation rows are release-scoped by the caller.  The adapter's release
    # label is compatibility metadata only; the canonical store is the authority.
    return SimpleNamespace(store=store, release_commit=release)


def bind_v52_shared_paper_capital_runtime(runtime: Any) -> None:
    global _BOUND_RUNTIME, _BASE_CANONICAL_ADAPTER, _INSTALLED
    if runtime is None or getattr(runtime, "store", None) is None:
        raise RuntimeError("v52_shared_paper_capital_runtime_store_unavailable")
    _BOUND_RUNTIME = runtime
    if not _INSTALLED:
        _BASE_CANONICAL_ADAPTER = bridge._canonical_adapter
        setattr(_canonical_adapter_with_runtime, "__wrapped__", _BASE_CANONICAL_ADAPTER)
        setattr(_canonical_adapter_with_runtime, "_roi_v52_canonical_runtime_store", True)
        bridge._canonical_adapter = _canonical_adapter_with_runtime
        _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": BINDING_VERSION,
        "installed": _INSTALLED,
        "canonical_runtime_bound": _BOUND_RUNTIME is not None,
        "canonical_store_available": bool(
            _BOUND_RUNTIME is not None and getattr(_BOUND_RUNTIME, "store", None) is not None
        ),
        "robinhood_market_store_remains_isolated": True,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = ["BINDING_VERSION", "bind_v52_shared_paper_capital_runtime", "status"]
