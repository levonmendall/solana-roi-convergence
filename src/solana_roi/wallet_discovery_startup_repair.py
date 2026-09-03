from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from .wallet_discovery import ContinuousWalletDiscovery as _OriginalWalletDiscovery


RETRY_SECONDS = 30.0
_ORIGINAL_DISCOVERY = _OriginalWalletDiscovery


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class StartupIsolatedWalletDiscovery:
    """Keep the research-only wallet lane from becoming a web-service prerequisite.

    Wallet discovery has no paper-trade or live-money authority. Its schema/RPC
    bootstrap is therefore deferred until after FastAPI has started. A bootstrap
    failure is exposed in status and retried, but it can never terminate the core
    ingestion/certification service.
    """

    def __init__(self, **kwargs: Any):
        self._kwargs = dict(kwargs)
        self._inner: Any | None = None
        self._startup_state = "deferred"
        self._startup_attempts = 0
        self._startup_error_type: str | None = None
        self._startup_error_message: str | None = None
        self._enabled_requested = bool(kwargs.get("enabled", True))

    def _record_failure(self, exc: BaseException, *, state: str) -> None:
        self._startup_state = state
        self._startup_error_type = type(exc).__name__
        self._startup_error_message = str(exc)[:300] or type(exc).__name__
        store = self._kwargs.get("store")
        append = getattr(store, "append", None)
        if callable(append):
            try:
                append(
                    "wallet_discovery_startup_error",
                    _utcnow_iso(),
                    {
                        "error_type": self._startup_error_type,
                        "state": state,
                        "research_lane": True,
                        "paper_only": True,
                        "live_money_authority": False,
                    },
                )
            except Exception:
                pass

    async def _attempt_bootstrap(self) -> bool:
        self._startup_attempts += 1
        try:
            inner = _ORIGINAL_DISCOVERY(**self._kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._inner = None
            self._record_failure(exc, state="bootstrap_failed")
            return False
        self._inner = inner
        self._startup_state = "ready"
        self._startup_error_type = None
        self._startup_error_message = None
        return True

    async def run(self, stop: asyncio.Event) -> None:
        if not self._enabled_requested:
            self._startup_state = "disabled"
            await stop.wait()
            return

        while not stop.is_set():
            if self._inner is None:
                ready = await self._attempt_bootstrap()
                if not ready:
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=RETRY_SECONDS)
                    except asyncio.TimeoutError:
                        continue
                    return
            try:
                await self._inner.run(stop)
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._inner = None
                self._record_failure(exc, state="runtime_failed")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=RETRY_SECONDS)
                except asyncio.TimeoutError:
                    continue
                return

    def _intelligence_status(self) -> dict[str, Any]:
        intelligence = self._kwargs.get("intelligence")
        status = getattr(intelligence, "status", None)
        if not callable(status):
            return {}
        try:
            value = status()
            return value if isinstance(value, dict) else {}
        except Exception:
            return {}

    def status(self) -> dict[str, Any]:
        if self._inner is not None:
            try:
                payload = self._inner.status()
            except Exception as exc:
                self._inner = None
                self._record_failure(exc, state="status_failed")
            else:
                if isinstance(payload, dict):
                    payload = dict(payload)
                    payload.update(
                        {
                            "startup_isolation_enabled": True,
                            "startup_state": self._startup_state,
                            "startup_attempts": self._startup_attempts,
                            "startup_error_type": None,
                            "startup_error_message": None,
                        }
                    )
                    return payload

        return {
            "enabled": self._enabled_requested,
            "operational": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_or_submission_available": False,
            "research_lane": True,
            "broad_program_receipt_sampling": False,
            "ecosystem_wide_exhaustive": False,
            "historical_screen_has_promotion_authority": False,
            "promotion_evidence_boundary": "forward_started_at only",
            "active_strategy_mutation_allowed": False,
            "future_cohort_proposal_enabled": False,
            "candidate_states": {},
            "broad_samples": 0,
            "forward_observations": 0,
            "copyable_forward_observations": 0,
            "copyable_forward_fraction": 0.0,
            "tracked_wallets": [],
            "startup_isolation_enabled": True,
            "startup_state": self._startup_state,
            "startup_attempts": self._startup_attempts,
            "startup_error_type": self._startup_error_type,
            "startup_error_message": self._startup_error_message,
            "wallet_intelligence": self._intelligence_status(),
        }

    def __getattr__(self, name: str) -> Any:
        inner = object.__getattribute__(self, "_inner")
        if inner is None:
            raise AttributeError(name)
        return getattr(inner, name)


def install_wallet_discovery_startup_isolation() -> None:
    # Import here so this repair can be installed by the production composition
    # immediately before api.py captures runtime.build_runtime.
    from . import runtime as runtime_module

    current = runtime_module.ContinuousWalletDiscovery
    if current is StartupIsolatedWalletDiscovery:
        return
    runtime_module.ContinuousWalletDiscovery = StartupIsolatedWalletDiscovery  # type: ignore[assignment]


__all__ = [
    "RETRY_SECONDS",
    "StartupIsolatedWalletDiscovery",
    "install_wallet_discovery_startup_isolation",
]
