from __future__ import annotations

import asyncio
import os
import threading
from functools import wraps
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse

from . import robinhood_alchemy_budget_guard as alchemy_guard
from . import robinhood_chain_runtime as runtime
from . import robinhood_provider_budget_transport as budget
from . import robinhood_provider_failover as failover


THROUGHPUT_REPAIR_VERSION = "robinhood-provider-pool-throughput-v3-budgeted-private-pool"
DEFAULT_PROVIDER_POOL_LIVE_MARKET_CAP = 16
MAX_PROVIDER_POOL_LIVE_MARKET_CAP = 64
DEFAULT_PRIVATE_RESEARCH_POLL_SECONDS = 1.0
DEFAULT_PUBLIC_RESEARCH_POLL_SECONDS = 5.0
_INSTALLED = False
_ORIGINAL_AUGMENT_STATUS_WRAPPER: Callable[..., Any] | None = None
_ORIGINAL_MODULE_STATUS: Callable[[], dict[str, Any]] | None = None
_TELEMETRY_LOCK = threading.Lock()
_RESEARCH_STATS: dict[str, Any] = {
    "passes": 0,
    "failures": 0,
    "provider_switches": 0,
    "last_provider_kind": None,
    "last_generation": None,
    "last_error_type": None,
}


def _normalized(value: str) -> str:
    return str(value or "").strip().rstrip("/").lower()


def _float_env(name: str, default: float, minimum: float) -> float:
    raw = os.getenv(name)
    try:
        value = float(raw) if raw is not None else float(default)
    except (TypeError, ValueError):
        value = float(default)
    return max(minimum, value)


def _configured_pool_cap() -> int:
    raw = os.getenv("ROBINHOOD_PROVIDER_POOL_LIVE_MARKET_CAP")
    if raw is None:
        raw = os.getenv("ROBINHOOD_ALCHEMY_LIVE_MARKET_CAP", str(DEFAULT_PROVIDER_POOL_LIVE_MARKET_CAP))
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = DEFAULT_PROVIDER_POOL_LIVE_MARKET_CAP
    return max(1, min(MAX_PROVIDER_POOL_LIVE_MARKET_CAP, value))


def _provider_kind(provider: Any | None) -> str:
    if provider is None:
        return "none"
    try:
        host = (urlparse(str(provider.http)).hostname or "").lower()
    except Exception:
        return "private_rpc"
    if "chainstack" in host:
        return "chainstack"
    if "drpc" in host:
        return "drpc"
    if "alchemy" in host:
        return "alchemy"
    return "private_rpc"


def _active_private_provider() -> Any | None:
    try:
        active = failover.active_provider()
    except Exception:
        return None
    if active is None:
        return None
    candidate = _normalized(getattr(active, "http", ""))
    if not candidate or candidate == _normalized(runtime.ROBINHOOD_PUBLIC_RPC):
        return None
    return active


def _active_non_alchemy_private_provider() -> Any | None:
    """Compatibility helper retained for diagnostics; capacity no longer depends on it."""
    active = _active_private_provider()
    if active is None or _provider_kind(active) == "alchemy":
        return None
    return active


def _effective_live_market_cap() -> int:
    """Keep the configured market universe while any budgeted private provider is healthy."""
    configured = _configured_pool_cap()
    if _active_private_provider() is not None:
        return configured
    return min(configured, 16)


def _research_target() -> tuple[str, str, float, int | None]:
    """Use whichever private provider failover selected; capacity layer governs consumption."""
    active = _active_private_provider()
    if active is not None:
        poll = _float_env(
            "ROBINHOOD_PROVIDER_POOL_RESEARCH_POLL_SECONDS",
            DEFAULT_PRIVATE_RESEARCH_POLL_SECONDS,
            0.25,
        )
        try:
            generation = int(failover.generation())
        except Exception:
            generation = None
        return str(active.http), _provider_kind(active), poll, generation

    poll = _float_env(
        "ROBINHOOD_PUBLIC_RESEARCH_POLL_SECONDS",
        DEFAULT_PUBLIC_RESEARCH_POLL_SECONDS,
        1.0,
    )
    return runtime.ROBINHOOD_PUBLIC_RPC, "public_rpc", poll, None


def _record_research_provider(provider_kind: str, generation: int | None, poll_seconds: float) -> None:
    with _TELEMETRY_LOCK:
        changed = (
            _RESEARCH_STATS.get("last_provider_kind") != provider_kind
            or _RESEARCH_STATS.get("last_generation") != generation
        )
        if changed:
            _RESEARCH_STATS["provider_switches"] = int(_RESEARCH_STATS.get("provider_switches", 0) or 0) + 1
        _RESEARCH_STATS["last_provider_kind"] = provider_kind
        _RESEARCH_STATS["last_generation"] = generation
    if changed:
        print(
            "ROBINHOOD_RESEARCH_PROVIDER_ACTIVE "
            f"provider_kind={provider_kind} generation={generation if generation is not None else 'none'} "
            f"poll_seconds={poll_seconds:.3f} live_market_cap={_effective_live_market_cap()}",
            flush=True,
        )


def _record_research_success(provider_kind: str, generation: int | None) -> None:
    with _TELEMETRY_LOCK:
        passes = int(_RESEARCH_STATS.get("passes", 0) or 0) + 1
        _RESEARCH_STATS["passes"] = passes
        _RESEARCH_STATS["last_error_type"] = None
    if passes == 1 or passes % 60 == 0:
        print(
            "ROBINHOOD_RESEARCH_PROVIDER_TRAFFIC "
            f"provider_kind={provider_kind} generation={generation if generation is not None else 'none'} "
            f"successful_passes={passes} live_market_cap={_effective_live_market_cap()}",
            flush=True,
        )


def _record_research_failure(provider_kind: str, generation: int | None, exc_name: str) -> None:
    with _TELEMETRY_LOCK:
        failures = int(_RESEARCH_STATS.get("failures", 0) or 0) + 1
        _RESEARCH_STATS["failures"] = failures
        _RESEARCH_STATS["last_error_type"] = exc_name
    if failures == 1 or failures % 10 == 0:
        print(
            "ROBINHOOD_RESEARCH_PROVIDER_FAILED "
            f"provider_kind={provider_kind} generation={generation if generation is not None else 'none'} "
            f"error_type={exc_name} failures={failures}",
            flush=True,
        )


async def _provider_pool_research_async(self: Any, stop: Any) -> None:
    rpc: runtime.RobinhoodRpc | None = None
    rpc_url = ""
    try:
        while not stop.is_set():
            target_url, provider_kind, poll_seconds, generation = _research_target()
            if rpc is None or _normalized(rpc_url) != _normalized(target_url):
                if rpc is not None:
                    await rpc.close()
                rpc = runtime.RobinhoodRpc(rpc_url=target_url, timeout_seconds=3.0)
                rpc_url = target_url
                _record_research_provider(provider_kind, generation, poll_seconds)

            budget._update_research_state(
                self,
                research_provider_kind=provider_kind,
                research_provider_private=provider_kind not in {"public_rpc", "none"},
                research_provider_generation=generation,
                research_poll_seconds=poll_seconds,
                provider_pool_live_market_cap=_effective_live_market_cap(),
                provider_pool_capacity_source="budgeted_active_private_provider" if provider_kind not in {"public_rpc", "none"} else "public_research_fallback",
            )
            try:
                await budget._research_pass(self, rpc)
                _record_research_success(provider_kind, generation)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                state = budget._research_state(self)
                budget._update_research_state(
                    self,
                    ready=False,
                    last_error_type=type(exc).__name__,
                    rpc_failures=int(state.get("rpc_failures", 0) or 0) + 1,
                )
                _record_research_failure(provider_kind, generation, type(exc).__name__)

            if not stop.is_set():
                await asyncio.sleep(poll_seconds)
    finally:
        if rpc is not None:
            await rpc.close()


def _research_stats() -> dict[str, Any]:
    with _TELEMETRY_LOCK:
        return dict(_RESEARCH_STATS)


def _throughput_augment_status_wrapper(original_factory: Callable[..., Any]) -> Callable[..., Any]:
    assert _ORIGINAL_AUGMENT_STATUS_WRAPPER is not None
    base_factory = _ORIGINAL_AUGMENT_STATUS_WRAPPER(original_factory)

    def factory(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
        wrapped = base_factory(original)

        @wraps(wrapped)
        def throughput_status(self: Any) -> dict[str, Any]:
            payload = wrapped(self)
            authority = payload.setdefault("production_transport_authority", {})
            research = budget._research_state(self)
            stats = _research_stats()
            authority.update(
                {
                    "throughput_repair_version": THROUGHPUT_REPAIR_VERSION,
                    "provider_pool_live_market_cap": _effective_live_market_cap(),
                    "configured_provider_pool_live_market_cap": _configured_pool_cap(),
                    "provider_pool_cap_is_canonical": True,
                    "alchemy_named_cap_can_restrict_budgeted_private_pool": False,
                    "research_screening_provider_kind": research.get("research_provider_kind"),
                    "research_screening_private_provider": bool(research.get("research_provider_private", False)),
                    "research_screening_provider_generation": research.get("research_provider_generation"),
                    "research_screening_poll_seconds": research.get("research_poll_seconds"),
                    "research_provider_successful_passes": int(stats.get("passes", 0) or 0),
                    "research_provider_failures": int(stats.get("failures", 0) or 0),
                    "research_provider_last_error_type": stats.get("last_error_type"),
                    "broad_research_uses_active_private_provider": True,
                    "alchemy_active_uses_budgeted_private_research": True,
                    "alchemy_active_uses_public_research_fallback": False,
                    "public_research_fallback_only_without_private_provider": True,
                    "research_transport_authority": "promotion_only_no_paper_entry",
                    "paper_entry_still_requires_subsequent_private_live_event": True,
                }
            )
            return payload

        setattr(throughput_status, "_roi_robinhood_provider_pool_throughput_status", True)
        return throughput_status

    return factory


def _throughput_module_status() -> dict[str, Any]:
    assert _ORIGINAL_MODULE_STATUS is not None
    result = dict(_ORIGINAL_MODULE_STATUS())
    _target_url, provider_kind, poll_seconds, generation = _research_target()
    stats = _research_stats()
    result.update(
        {
            "throughput_repair_version": THROUGHPUT_REPAIR_VERSION,
            "provider_pool_live_market_cap": _effective_live_market_cap(),
            "configured_provider_pool_live_market_cap": _configured_pool_cap(),
            "provider_pool_cap_is_canonical": True,
            "alchemy_named_cap_can_restrict_budgeted_private_pool": False,
            "research_screening_provider_kind": provider_kind,
            "research_screening_private_provider": provider_kind not in {"public_rpc", "none"},
            "research_screening_provider_generation": generation,
            "research_screening_poll_seconds": poll_seconds,
            "research_provider_successful_passes": int(stats.get("passes", 0) or 0),
            "research_provider_failures": int(stats.get("failures", 0) or 0),
            "research_provider_last_error_type": stats.get("last_error_type"),
            "broad_research_uses_active_private_provider": True,
            "alchemy_active_uses_budgeted_private_research": True,
            "alchemy_active_uses_public_research_fallback": False,
            "public_research_fallback_only_without_private_provider": True,
            "research_transport_authority": "promotion_only_no_paper_entry",
            "paper_entry_still_requires_subsequent_private_live_event": True,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    )
    return result


def install_robinhood_provider_pool_throughput_repair() -> None:
    global _INSTALLED, _ORIGINAL_AUGMENT_STATUS_WRAPPER, _ORIGINAL_MODULE_STATUS
    if _INSTALLED:
        return
    if bool(getattr(budget, "_INSTALLED", False)):
        raise RuntimeError("provider-pool throughput repair must install before provider-budget transport")

    _ORIGINAL_AUGMENT_STATUS_WRAPPER = budget._augment_status_wrapper
    _ORIGINAL_MODULE_STATUS = budget.status

    budget.BUDGET_VERSION = "robinhood-production-ws-transport-v5-budgeted-private-pool"
    budget.SUBSCRIPTION_MODE = "factory_discovery_plus_budgeted_private_pool_research_promoted_live_shortlist"
    budget._live_market_cap = _effective_live_market_cap
    budget._research_async = _provider_pool_research_async
    budget._augment_status_wrapper = _throughput_augment_status_wrapper
    budget.status = _throughput_module_status
    alchemy_guard._provider_pool_live_market_cap = _effective_live_market_cap
    _INSTALLED = True


def status() -> dict[str, Any]:
    stats = _research_stats()
    active = _active_private_provider()
    return {
        "version": THROUGHPUT_REPAIR_VERSION,
        "installed": _INSTALLED,
        "configured_provider_pool_live_market_cap": _configured_pool_cap(),
        "effective_live_market_cap": _effective_live_market_cap(),
        "active_private_provider": _provider_kind(active),
        "active_non_alchemy_private_provider": _provider_kind(_active_non_alchemy_private_provider()),
        "private_research_poll_seconds": _float_env(
            "ROBINHOOD_PROVIDER_POOL_RESEARCH_POLL_SECONDS",
            DEFAULT_PRIVATE_RESEARCH_POLL_SECONDS,
            0.25,
        ),
        "public_research_poll_seconds": _float_env(
            "ROBINHOOD_PUBLIC_RESEARCH_POLL_SECONDS",
            DEFAULT_PUBLIC_RESEARCH_POLL_SECONDS,
            1.0,
        ),
        "alchemy_active_uses_budgeted_private_research": True,
        "alchemy_active_uses_public_research_fallback": False,
        "research_provider_successful_passes": int(stats.get("passes", 0) or 0),
        "research_provider_failures": int(stats.get("failures", 0) or 0),
        "research_provider_last_error_type": stats.get("last_error_type"),
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "THROUGHPUT_REPAIR_VERSION",
    "install_robinhood_provider_pool_throughput_repair",
    "status",
]
