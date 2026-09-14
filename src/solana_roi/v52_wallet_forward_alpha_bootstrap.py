from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from . import v52_wallet_forward_alpha_runtime as runtime_mod
from .v52_wallet_forward_alpha_strict_validation import install_strict_wallet_forward_alpha_validation

BOOTSTRAP_VERSION = "v52-wallet-forward-alpha-bootstrap-v2-strict-validation"
_INSTALLED = False
_BASE_RECORD: Any = None
_BASE_RUN: Any = None


def _ensure_runtime(tracker: Any) -> runtime_mod.WalletForwardAlphaRuntime:
    current = runtime_mod._RUNTIME
    if current is not None and current.store is tracker.store:
        owner = getattr(current, "runtime", None)
        if owner is not None and getattr(owner, "wallet_discovery", None) is None:
            try:
                owner.wallet_discovery = tracker.discovery
            except Exception:
                pass
        return current
    owner = SimpleNamespace(store=tracker.store, wallet_discovery=tracker.discovery)
    current = runtime_mod.WalletForwardAlphaRuntime(tracker.store, owner)
    runtime_mod._RUNTIME = current
    return current


async def _record(self: Any, swap: Any) -> bool:
    if _BASE_RECORD is None:
        raise RuntimeError("Wallet Forward Alpha bootstrap record predecessor unavailable")
    inserted = await _BASE_RECORD(self, swap)
    if inserted:
        current = _ensure_runtime(self)
        try:
            current.capture_initial_observation(self, swap)
        except Exception as exc:
            current.last_error = f"capture_initial:{type(exc).__name__}:{exc}"
    return inserted


async def _run(self: Any, stop: Any) -> None:
    if _BASE_RUN is None:
        raise RuntimeError("Wallet Forward Alpha bootstrap run predecessor unavailable")
    import asyncio

    current = _ensure_runtime(self)
    task = asyncio.create_task(current.run(self, stop), name="v52-wallet-forward-alpha-runtime")
    try:
        await _BASE_RUN(self, stop)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


setattr(_record, "_roi_v52_wallet_forward_alpha_runtime", True)
setattr(_run, "_roi_v52_wallet_forward_alpha_runtime", True)


def install_v52_wallet_forward_alpha_bootstrap() -> None:
    global _INSTALLED, _BASE_RECORD, _BASE_RUN
    if _INSTALLED:
        return
    from .wallet_realtime_tracking_repair import RealtimeWalletTracker

    install_strict_wallet_forward_alpha_validation()
    record = RealtimeWalletTracker._record_quick_forward_swap
    run = RealtimeWalletTracker.run
    if not bool(getattr(record, "_roi_v52_wallet_forward_alpha_runtime", False)):
        _BASE_RECORD = record
        try:
            _record.__dict__.update(getattr(record, "__dict__", {}))
        except Exception:
            pass
        setattr(_record, "_roi_v52_wallet_forward_alpha_runtime", True)
        RealtimeWalletTracker._record_quick_forward_swap = _record  # type: ignore[method-assign]
    if not bool(getattr(run, "_roi_v52_wallet_forward_alpha_runtime", False)):
        _BASE_RUN = run
        try:
            _run.__dict__.update(getattr(run, "__dict__", {}))
        except Exception:
            pass
        setattr(_run, "_roi_v52_wallet_forward_alpha_runtime", True)
        RealtimeWalletTracker.run = _run  # type: ignore[method-assign]
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "installed": _INSTALLED,
        "version": BOOTSTRAP_VERSION,
        "single_realtime_tracker_worker_tree": True,
        "automatic_point_in_time_capture": True,
        "strict_incremental_acceptance_gate": True,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = ["BOOTSTRAP_VERSION", "install_v52_wallet_forward_alpha_bootstrap", "status"]
