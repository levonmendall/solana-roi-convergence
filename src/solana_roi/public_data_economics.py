from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Callable, Mapping
from typing import Any

from . import direct_solana as direct_solana_module
from . import solana_rpc as solana_rpc_module
from .direct_solana import DirectSolanaIngestionPlane, DirectSolanaJournal
from .solana_rpc import RpcEndpoint


METERED_ALCHEMY_OPT_IN_ENV = "SOLANA_ROI_ENABLE_METERED_ALCHEMY"
COVERAGE_BOOTSTRAP_MIN_INTERVAL_SECONDS = 2.0
COVERAGE_BOOTSTRAP_MAX_OUTSTANDING_PER_SOURCE = 2
LEGACY_SAMPLE_RETIREMENT_REASON = "superseded by selective public-data hydration"


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _is_alchemy_endpoint(endpoint: RpcEndpoint) -> bool:
    return bool(
        endpoint.name == "alchemy"
        or ".alchemy.com" in endpoint.http_url.split("/", 3)[2]
        or ".alchemy.com" in endpoint.ws_url.split("/", 3)[2]
    )


def _values_from_call(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Mapping[str, str]:
    if args and isinstance(args[0], Mapping):
        return args[0]
    candidate = kwargs.get("env")
    if isinstance(candidate, Mapping):
        return candidate
    return os.environ


def _public_first_endpoint_factory(
    original: Callable[..., tuple[RpcEndpoint, ...]],
) -> Callable[..., tuple[RpcEndpoint, ...]]:
    """Keep metered Alchemy completely idle unless the operator opts in.

    The API key may remain configured so a future emergency opt-in does not require
    secret rotation. Merely having the key present no longer opens WebSockets or
    sends HTTP hydration/poll requests to Alchemy.
    """

    def endpoints(*args: Any, **kwargs: Any) -> tuple[RpcEndpoint, ...]:
        configured = tuple(original(*args, **kwargs))
        values = _values_from_call(args, kwargs)
        if _truthy(values.get(METERED_ALCHEMY_OPT_IN_ENV)):
            return configured
        return tuple(endpoint for endpoint in configured if not _is_alchemy_endpoint(endpoint))

    try:
        endpoints.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(endpoints, "_roi_public_data_default", True)
    return endpoints


def _source_needs_bootstrap(self: Any, source: str) -> bool:
    status_fn = getattr(self, "coverage_status_fn", None)
    if status_fn is None:
        return True
    try:
        status = status_fn()
    except Exception:
        return True
    if not isinstance(status, dict):
        return True
    counts = status.get("program_source_counts")
    requirements = status.get("requirements")
    if not isinstance(counts, dict) or not isinstance(requirements, dict):
        return True
    try:
        required = max(1, int(requirements.get("min_normalized_swaps_per_source") or 10))
        observed = int(counts.get(source, 0) or 0)
    except (TypeError, ValueError):
        return True
    return observed < required


def _bootstrap_capacity_available(self: Any, source: str) -> bool:
    with self.store._lock:
        row = self.store.db.execute(
            "SELECT COUNT(*) AS n FROM direct_solana_hydration_queue "
            "WHERE source_hint=? AND reason='deterministic_market_sample' "
            "AND status IN ('pending','processing')",
            (source,),
        ).fetchone()
    outstanding = int(row["n"] or 0) if row is not None else 0
    if outstanding >= COVERAGE_BOOTSTRAP_MAX_OUTSTANDING_PER_SOURCE:
        return False

    now = time.monotonic()
    last_by_source = getattr(self, "_roi_coverage_bootstrap_last_enqueue", None)
    if not isinstance(last_by_source, dict):
        last_by_source = {}
        setattr(self, "_roi_coverage_bootstrap_last_enqueue", last_by_source)
    previous = float(last_by_source.get(source, 0.0) or 0.0)
    if previous and now - previous < COVERAGE_BOOTSTRAP_MIN_INTERVAL_SECONDS:
        return False
    last_by_source[source] = now
    return True


async def _selective_notification_handler(
    self: Any,
    provider: str,
    subscription_targets: dict[int, Any],
    message: dict[str, Any],
) -> None:
    """Persist every raw receipt but hydrate only decision-relevant evidence.

    Scout activity and precise launch-like program events remain fully hydrated.
    Ordinary program traffic is hydrated only as a tiny bounded bootstrap while a
    source is below the unchanged empirical normalized-swap minimum. Once that
    source minimum is met, the full raw stream continues to be journaled but no
    random transaction hydration is created for it.
    """

    try:
        if message.get("method") != "logsNotification":
            return
        params = message.get("params")
        if not isinstance(params, dict):
            return
        try:
            subscription = int(params["subscription"])
            result = params["result"]
            slot = int(result["context"]["slot"])
            value = result["value"]
            signature = str(value["signature"])
        except (KeyError, TypeError, ValueError):
            return
        if not isinstance(value, dict):
            return
        target = subscription_targets.get(subscription)
        if target is None or not signature:
            return

        received_at = direct_solana_module.utcnow()
        self.journal.touch_provider(provider, received_at)
        launch_like = self._launch_like(value.get("logs") if isinstance(value, dict) else [])
        source_key = target.source_hint or f"SCOUT:{target.address}"
        inserted = self.journal.record_receipt(
            signature=signature,
            source_key=source_key,
            slot=slot,
            received_at=received_at,
            launch_like=launch_like,
        )
        if not inserted or value.get("err") is not None:
            return

        if target.kind == "scout":
            self.journal.enqueue(
                signature=signature,
                slot=slot,
                trigger_received_at=received_at,
                source_hint=None,
                priority=0,
                reason="frozen_scout_processed_trigger",
            )
            return

        source = str(target.source_hint)
        if launch_like:
            self.journal.enqueue(
                signature=signature,
                slot=slot,
                trigger_received_at=received_at,
                source_hint=source,
                priority=10,
                reason="prospective_launch",
            )
            return

        if _source_needs_bootstrap(self, source) and _bootstrap_capacity_available(self, source):
            # Reuse the existing lightweight hydration reason so runtime_guards
            # persists the normalized transaction without invoking deep risk work.
            self.journal.enqueue(
                signature=signature,
                slot=slot,
                trigger_received_at=received_at,
                source_hint=source,
                priority=20,
                reason="deterministic_market_sample",
            )
    finally:
        # Preserve the cooperative event-loop handoff expected by the bounded
        # WebSocket notification infrastructure.
        await asyncio.sleep(0)


setattr(_selective_notification_handler, "_roi_public_data_selective_hydration", True)
setattr(_selective_notification_handler, "_roi_cooperative_yield", True)


def _journal_init_with_legacy_sample_retirement(
    original: Callable[..., None],
) -> Callable[..., None]:
    def init(self: Any, *args: Any, **kwargs: Any) -> None:
        original(self, *args, **kwargs)
        now = direct_solana_module.utcnow().isoformat()
        with self.store._lock, self.store.db:
            cur = self.store.db.execute(
                "UPDATE direct_solana_hydration_queue SET status='failed', last_error=?, updated_at=? "
                "WHERE status IN ('pending','processing') AND reason='deterministic_market_sample'",
                (LEGACY_SAMPLE_RETIREMENT_REASON, now),
            )
        self._roi_retired_legacy_market_samples = int(cur.rowcount or 0)

    try:
        init.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(init, "_roi_public_data_selective_hydration", True)
    return init


def _status_with_public_data_economics(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        endpoints = tuple(getattr(self, "endpoints", ()) or ())
        metered_enabled = any(_is_alchemy_endpoint(endpoint) for endpoint in endpoints)
        retired = int(getattr(getattr(self, "journal", None), "_roi_retired_legacy_market_samples", 0) or 0)

        payload["public_data_economics"] = {
            "default_data_plane": "public-standard-solana",
            "metered_alchemy_default_enabled": False,
            "metered_alchemy_enabled": metered_enabled,
            "metered_alchemy_opt_in_env": METERED_ALCHEMY_OPT_IN_ENV,
            "alchemy_api_key_can_remain_configured_without_use": True,
            "commercial_provider_required_for_continuity": False,
            "full_scope_raw_receipts_preserved": True,
            "launch_events_fully_hydrated": True,
            "scout_events_fully_hydrated": True,
            "broad_random_market_hydration_enabled": False,
            "coverage_bootstrap_interval_seconds": COVERAGE_BOOTSTRAP_MIN_INTERVAL_SECONDS,
            "coverage_bootstrap_max_outstanding_per_source": COVERAGE_BOOTSTRAP_MAX_OUTSTANDING_PER_SOURCE,
            "legacy_random_samples_retired_at_startup": retired,
            "strategy_scope_reduced": False,
        }
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "metered_alchemy_required": False,
                    "metered_alchemy_default_enabled": False,
                    "metered_alchemy_explicit_opt_in_only": True,
                    "continuity_transport_model": "two-public-websocket-providers-plus-continuous-bounded-live-poll",
                    "full_target_count_unchanged": len(self.watch_targets),
                }
            )
        throughput = payload.setdefault("throughput_policy", {})
        if isinstance(throughput, dict):
            throughput.update(
                {
                    "raw_full_scope_observation_preserved": True,
                    "ordinary_program_hydration": "under-sampled-source-bootstrap-only",
                    "coverage_bootstrap_max_outstanding_per_source": COVERAGE_BOOTSTRAP_MAX_OUTSTANDING_PER_SOURCE,
                    "coverage_bootstrap_interval_seconds": COVERAGE_BOOTSTRAP_MIN_INTERVAL_SECONDS,
                    "random_market_sampling_after_source_minimum": False,
                    "launches_and_scouts_preserve_deep_analysis": True,
                    "strategy_scope_reduced": False,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_public_data_selective_hydration", True)
    return status


def _public_data_preflight(
    original: Callable[..., dict[str, Any]],
) -> Callable[..., dict[str, Any]]:
    def preflight(*args: Any, **kwargs: Any) -> dict[str, Any]:
        payload = original(*args, **kwargs)
        checks = payload.get("checks")
        endpoints = payload.get("rpc_endpoints")
        endpoint_count = len(endpoints) if isinstance(endpoints, list) else 0
        if isinstance(checks, list):
            for row in checks:
                if not isinstance(row, dict) or row.get("name") != "independent_standard_rpc_quorum":
                    continue
                row["ok"] = endpoint_count >= 2
                row["detail"] = (
                    "at least two distinct public standard Solana HTTP/WebSocket providers are required; "
                    "prospective recovery is additionally enforced by the continuously baselined bounded live-poll lane"
                )
            payload["ready_for_live_shadow_collection"] = all(
                bool(row.get("ok")) for row in checks if isinstance(row, dict)
            )
        payload["metered_provider_required"] = False
        payload["metered_alchemy_default_enabled"] = False
        payload["continuity_transport_model"] = (
            "two-public-websocket-providers-plus-continuous-bounded-live-poll"
        )
        return payload

    try:
        preflight.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(preflight, "_roi_public_data_default", True)
    return preflight


def install_public_data_economics() -> None:
    current_factory = solana_rpc_module.rpc_endpoints_from_env
    if not bool(getattr(current_factory, "_roi_public_data_default", False)):
        solana_rpc_module.rpc_endpoints_from_env = _public_first_endpoint_factory(current_factory)  # type: ignore[assignment]
    direct_solana_module.rpc_endpoints_from_env = solana_rpc_module.rpc_endpoints_from_env

    current_journal_init = DirectSolanaJournal.__init__
    if not bool(getattr(current_journal_init, "_roi_public_data_selective_hydration", False)):
        DirectSolanaJournal.__init__ = _journal_init_with_legacy_sample_retirement(current_journal_init)  # type: ignore[method-assign]

    current_handler = DirectSolanaIngestionPlane._handle_notification
    if not bool(getattr(current_handler, "_roi_public_data_selective_hydration", False)):
        try:
            _selective_notification_handler.__dict__.update(getattr(current_handler, "__dict__", {}))
        except Exception:
            pass
        DirectSolanaIngestionPlane._handle_notification = _selective_notification_handler  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_public_data_selective_hydration", False)):
        DirectSolanaIngestionPlane.status = _status_with_public_data_economics(current_status)  # type: ignore[method-assign]

    # Import after the endpoint factory is repaired so direct_deployment captures
    # the public-first factory even when it has not previously been imported.
    from . import direct_deployment as direct_deployment_module

    direct_deployment_module.rpc_endpoints_from_env = solana_rpc_module.rpc_endpoints_from_env
    current_preflight = direct_deployment_module.deployment_preflight
    if not bool(getattr(current_preflight, "_roi_public_data_default", False)):
        direct_deployment_module.deployment_preflight = _public_data_preflight(current_preflight)  # type: ignore[assignment]


__all__ = [
    "COVERAGE_BOOTSTRAP_MAX_OUTSTANDING_PER_SOURCE",
    "COVERAGE_BOOTSTRAP_MIN_INTERVAL_SECONDS",
    "METERED_ALCHEMY_OPT_IN_ENV",
    "install_public_data_economics",
]
