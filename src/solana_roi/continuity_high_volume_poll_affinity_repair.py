from __future__ import annotations

from collections.abc import Callable
from typing import Any

from . import continuity_storage_capacity_repair as storage
from . import production_capacity_repair as capacity
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget
from .solana_rpc import RpcEndpoint


HIGH_VOLUME_ROUTINE_SOURCES = frozenset({"PUMP_FUN", "PUMP_AMM"})
_ORIGINAL_ASSIGNED_ENDPOINT: Callable[[Any, WatchTarget], RpcEndpoint] | None = None
_ORIGINAL_STATUS: Callable[[Any], dict[str, Any]] | None = None


def _is_high_volume_target(target: WatchTarget) -> bool:
    return bool(
        str(getattr(target, "kind", "") or "") == "program"
        and str(getattr(target, "source_hint", "") or "").upper() in HIGH_VOLUME_ROUTINE_SOURCES
    )


def _non_official_routine_endpoints(self: Any) -> tuple[RpcEndpoint, ...]:
    return tuple(
        endpoint
        for endpoint in storage._routine_endpoints(self)
        if not capacity._is_official_public(endpoint)
    )


def _assigned_endpoint_with_high_volume_affinity(self: Any, target: WatchTarget) -> RpcEndpoint:
    """Keep burst-heavy Pump standby cursors on a non-official routine primary.

    The official Solana public HTTP endpoint remains configured and available to the
    existing urgent recovery/fallback paths. It is simply not selected as the
    routine primary for PUMP_FUN/PUMP_AMM when at least one non-official public
    endpoint is already configured. Lower-volume targets preserve the established
    deterministic shard exactly.
    """

    original = _ORIGINAL_ASSIGNED_ENDPOINT
    if original is None:
        original = storage._assigned_endpoint

    if _is_high_volume_target(target):
        preferred = _non_official_routine_endpoints(self)
        if preferred:
            index = storage._target_index(self, target) % len(preferred)
            return preferred[index]
    return original(self, target)


setattr(_assigned_endpoint_with_high_volume_affinity, "_roi_high_volume_poll_affinity", True)


def _status_with_high_volume_affinity(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("high-volume routine poll affinity repair is not installed")
    payload = _ORIGINAL_STATUS(self)
    endpoints = storage._routine_endpoints(self)
    non_official = _non_official_routine_endpoints(self)
    targets = storage._watch_targets(self)
    high_volume_assignments: dict[str, str] = {}
    high_volume_official_primaries: list[str] = []

    # Some intrinsic/status regressions intentionally construct a partial plane
    # without configured endpoints. Telemetry must remain observational and may not
    # make such a status call fail merely because assignment is not meaningful yet.
    if endpoints:
        for target in targets:
            if not _is_high_volume_target(target):
                continue
            endpoint = storage._assigned_endpoint(self, target)
            key = storage._target_key(target)
            high_volume_assignments[key] = endpoint.name
            if capacity._is_official_public(endpoint):
                high_volume_official_primaries.append(key)

    poll = payload.get("live_poll_redundancy")
    if isinstance(poll, dict):
        sharding = poll.get("routine_provider_sharding")
        if isinstance(sharding, dict):
            sharding.update(
                {
                    "assignment_policy": "high-volume-pump-nonofficial-affinity;others-preserve-index-shard",
                    "high_volume_sources": sorted(HIGH_VOLUME_ROUTINE_SOURCES),
                    "high_volume_target_assignments": high_volume_assignments,
                    "non_official_routine_provider_count": len(non_official),
                    "official_public_routine_primary_for_high_volume": bool(high_volume_official_primaries),
                    "high_volume_official_primary_targets": high_volume_official_primaries,
                    "official_public_urgent_recovery_fallback_unchanged": True,
                    "routine_poll_interval_unchanged": True,
                    "recovery_bound_unchanged": True,
                }
            )

    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "routine_high_volume_pump_prefers_nonofficial_public_primary": bool(non_official),
                "routine_low_volume_sharding_preserved": True,
                "official_public_urgent_recovery_fallback_unchanged": True,
                "continuity_lease_unchanged": True,
                "recovery_bound_unchanged": True,
                "provider_scope_unchanged": True,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    return payload


setattr(_status_with_high_volume_affinity, "_roi_high_volume_poll_affinity", True)


def install_continuity_high_volume_poll_affinity_repair() -> None:
    global _ORIGINAL_ASSIGNED_ENDPOINT, _ORIGINAL_STATUS

    current_assignment = storage._assigned_endpoint
    if not bool(getattr(current_assignment, "_roi_high_volume_poll_affinity", False)):
        _ORIGINAL_ASSIGNED_ENDPOINT = current_assignment
        storage._assigned_endpoint = _assigned_endpoint_with_high_volume_affinity  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_high_volume_poll_affinity", False)):
        _ORIGINAL_STATUS = current_status
        try:
            _status_with_high_volume_affinity.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_status_with_high_volume_affinity, "_roi_high_volume_poll_affinity", True)
        DirectSolanaIngestionPlane.status = _status_with_high_volume_affinity  # type: ignore[method-assign]


__all__ = [
    "HIGH_VOLUME_ROUTINE_SOURCES",
    "_assigned_endpoint_with_high_volume_affinity",
    "_is_high_volume_target",
    "_non_official_routine_endpoints",
    "install_continuity_high_volume_poll_affinity_repair",
]
