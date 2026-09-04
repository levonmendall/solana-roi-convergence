from __future__ import annotations

from collections.abc import Callable
from typing import Any

from . import continuity_high_volume_poll_affinity_repair as affinity
from . import continuity_standby_rpc_priority_repair as standby_priority
from . import continuity_target_frontier_repair as frontier
from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from . import rpc_workload_governor as governor
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


_ORIGINAL_SLOT_FETCH: Callable[..., Any] | None = None
_ORIGINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None


def _checkpoint_counts(self: Any) -> dict[str, int]:
    value = getattr(self, "_roi_high_volume_standby_checkpoint_counts", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_high_volume_standby_checkpoint_counts", value)
    return value


def _last_checkpoints(self: Any) -> dict[str, dict[str, Any]]:
    value = getattr(self, "_roi_high_volume_standby_last_checkpoints", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_high_volume_standby_last_checkpoints", value)
    return value


def _latest_observed_target_slot(self: Any, target: WatchTarget) -> int:
    rows = list(frontier._target_history(self, target))
    return max((int(row.get("slot") or 0) for row in rows), default=0)


async def _checkpointed_slot_fetch_delta(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    """Keep high-volume standby cursors current from confirmed live receipts.

    This helper sits *outside* the canonical poll-watermark module. Existing poll,
    pagination, exception-rearm, recoverability-lease, generation-floor and hedged
    real-gap helpers therefore retain their exact identities and monkeypatch
    contracts. Only the leased worker's outer standby view changes while a real
    high-volume target WebSocket remains continuously authoritative.
    """

    if _ORIGINAL_SLOT_FETCH is None:
        raise RuntimeError("high-volume standby checkpoint architecture is not installed")

    if not affinity._is_high_volume_target(target) or not live_poll._ws_target_covered(self, target):
        return await _ORIGINAL_SLOT_FETCH(self, target, cursor_slot)

    latest_observed_slot = _latest_observed_target_slot(self, target)
    if latest_observed_slot <= int(cursor_slot) + 1:
        return [], True, None, None

    generation = int(lease._current_ws_generation(self, target))
    try:
        with governor.rpc_workload(standby_priority.WORKLOAD_STANDBY):
            effective_cursor, anchor = await frontier._confirmed_target_frontier_cursor(
                self,
                target,
                int(cursor_slot),
                generation,
            )
    except Exception:
        return await _ORIGINAL_SLOT_FETCH(self, target, cursor_slot)

    if (
        not live_poll._ws_target_covered(self, target)
        or int(lease._current_ws_generation(self, target)) != generation
        or not isinstance(anchor, dict)
        or str(anchor.get("source") or "") != "confirmed-target-websocket-frontier"
        or int(effective_cursor) <= int(cursor_slot)
    ):
        return await _ORIGINAL_SLOT_FETCH(self, target, cursor_slot)

    key = live_poll._poll_target_key(target)
    counts = _checkpoint_counts(self)
    counts[key] = int(counts.get(key, 0) or 0) + 1
    _last_checkpoints(self)[key] = {
        "target": key,
        "source": str(target.source_hint or target.kind),
        "generation": generation,
        "prior_cursor_slot": int(cursor_slot),
        "checkpoint_cursor_slot": int(effective_cursor),
        "confirmed_frontier_slot": int(anchor.get("confirmed_frontier_slot") or 0),
        "confirmation_provider": anchor.get("confirmation_provider"),
        "confirmation_latency_ms": anchor.get("confirmation_latency_ms"),
        "same_slot_replay_required": True,
    }

    # The leased worker derives its cursor from returned row slots. This marker has
    # no signature, so _record_poll_rows() can never persist it as a raw receipt,
    # hydration item, candidate, strategy signal or paper observation.
    marker = {
        "signature": "",
        "slot": int(effective_cursor),
        "err": None,
        "_roi_standby_checkpoint": True,
    }
    return (
        [marker],
        True,
        str(anchor.get("confirmation_provider") or "") or None,
        float(anchor["confirmation_latency_ms"])
        if anchor.get("confirmation_latency_ms") is not None
        else None,
    )


setattr(_checkpointed_slot_fetch_delta, "_roi_high_volume_standby_checkpoint", True)


class _HighVolumeCheckpointProxy:
    """Outer leased-watermark view that preserves every canonical inner contract."""

    def __init__(self, delegate: Any):
        # Deliberately do not use the name `_base`: existing continuity contracts
        # expose lease.watermark._base as the canonical poll_watermark module.
        self._checkpoint_delegate = delegate

    def __getattr__(self, name: str) -> Any:
        return getattr(self._checkpoint_delegate, name)

    async def _slot_fetch_delta(
        self,
        plane: Any,
        target: WatchTarget,
        cursor_slot: int,
    ) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
        return await _checkpointed_slot_fetch_delta(plane, target, cursor_slot)


def _status_with_checkpoint_architecture(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _ORIGINAL_DIRECT_STATUS is None:
        raise RuntimeError("high-volume standby checkpoint architecture is not installed")
    payload = _ORIGINAL_DIRECT_STATUS(self)
    counts = _checkpoint_counts(self)

    poll = payload.get("live_poll_redundancy")
    if isinstance(poll, dict):
        targets = poll.get("targets")
        if isinstance(targets, dict):
            for key, count in counts.items():
                row = targets.get(key)
                if not isinstance(row, dict):
                    continue
                suppressed = int(row.get("suppressed_while_websocket_covered_total") or 0)
                row["suppressed_while_websocket_covered_total"] = max(0, suppressed - int(count))
                row["confirmed_ws_standby_checkpoints_total"] = int(count)

    payload["high_volume_standby_checkpoint_architecture"] = {
        "installed": True,
        "sources": sorted(affinity.HIGH_VOLUME_ROUTINE_SOURCES),
        "mode": "outer-lease-proxy-confirmed-real-websocket-frontier-to-standby-watermark",
        "checkpoint_count": sum(int(value) for value in counts.values()),
        "checkpoint_counts_by_target": dict(sorted(counts.items())),
        "last_checkpoints": dict(sorted(_last_checkpoints(self).items())),
        "canonical_poll_module_function_identity_preserved": True,
        "canonical_recoverability_proxy_preserved": True,
        "real_gap_hedged_recovery_path_unchanged": True,
        "healthy_websocket_avoids_replaying_suppressed_high_volume_history": True,
        "checkpoint_requires_confirmed_target_receipt": True,
        "checkpoint_requires_continuous_real_websocket_generation": True,
        "same_slot_replay_preserved": True,
        "checkpoint_marker_has_receipt_authority": False,
        "checkpoint_marker_has_candidate_authority": False,
        "checkpoint_marker_has_strategy_authority": False,
        "fallback_to_existing_bounded_delta_on_uncertainty": True,
        "poll_interval_seconds_unchanged": live_poll.POLL_INTERVAL_SECONDS,
        "recoverability_lease_seconds_unchanged": lease.POLL_RECOVERABILITY_LEASE_SECONDS,
        "hard_page_limit_unchanged": live_poll.POLL_CURSOR_MAX_PAGES,
        "hard_page_size_unchanged": live_poll.POLL_LIMIT,
        "full_raw_market_scope_preserved": True,
        "paper_only": True,
        "signing_or_submission_available": False,
    }
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "high_volume_standby_uses_confirmed_ws_frontier_checkpoint": True,
                "high_volume_healthy_ws_history_replay_removed": True,
                "high_volume_checkpoint_same_slot_replay_preserved": True,
                "high_volume_checkpoint_never_persists_synthetic_receipt": True,
                "canonical_poll_helper_identity_preserved": True,
                "urgent_real_gap_recovery_behavior_unchanged": True,
                "continuity_lease_unchanged": True,
                "recovery_bound_unchanged": True,
                "provider_scope_unchanged": True,
                "paper_only_authority_unchanged": True,
            }
        )
    return payload


setattr(_status_with_checkpoint_architecture, "_roi_high_volume_standby_checkpoint", True)


def install_high_volume_standby_checkpoint_architecture() -> None:
    """Install healthy high-volume checkpointing outside canonical poll helpers."""

    global _ORIGINAL_SLOT_FETCH, _ORIGINAL_DIRECT_STATUS

    current_watermark = getattr(lease, "watermark", None)
    if not isinstance(current_watermark, _HighVolumeCheckpointProxy):
        if current_watermark is None or not callable(getattr(current_watermark, "_slot_fetch_delta", None)):
            raise RuntimeError("canonical leased watermark is unavailable")
        _ORIGINAL_SLOT_FETCH = current_watermark._slot_fetch_delta
        lease.watermark = _HighVolumeCheckpointProxy(current_watermark)  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_high_volume_standby_checkpoint", False)):
        _ORIGINAL_DIRECT_STATUS = current_status
        try:
            _status_with_checkpoint_architecture.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_status_with_checkpoint_architecture, "_roi_high_volume_standby_checkpoint", True)
        DirectSolanaIngestionPlane.status = _status_with_checkpoint_architecture  # type: ignore[method-assign]


__all__ = [
    "_HighVolumeCheckpointProxy",
    "_checkpointed_slot_fetch_delta",
    "_latest_observed_target_slot",
    "install_high_volume_standby_checkpoint_architecture",
]
