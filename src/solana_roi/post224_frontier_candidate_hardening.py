from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime
from typing import Any

from . import direct_solana as direct_module
from . import launch_coverage_bridge as launch_bridge
from . import launch_reference_timing_repair as reference
from . import launch_ws_frontier_timing_repair as ws_timing
from . import live_poll_redundancy as live_poll
from . import public_ws_shard_transport_repair as ws_shards
from . import rpc_workload_governor as governor
from . import scout_candidate_continuity_repair as scout
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget

REPAIR_VERSION = "post224-frontier-candidate-hardening-v1"
PROGRAM_TARGETS_PER_SOCKET = 2
REFERENCE_SAMPLE_INTERVAL_SECONDS = 0.5

_ORIGINAL_WS_CAPTURE: Callable[..., bool] | None = None
_ORIGINAL_WS_HYDRATE: Callable[..., Any] | None = None
_ORIGINAL_WS_LAG: Callable[..., tuple[float | None, str]] | None = None
_ORIGINAL_TARGET_SHARDS: Callable[..., Any] | None = None
_ORIGINAL_NORMALIZE: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[[Any], dict[str, Any]] | None = None
_INSTALLED = False


def _inc(obj: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_post224_frontier_{name}"
    setattr(obj, attr, int(getattr(obj, attr, 0) or 0) + int(amount))


async def _confirmed_reference_sampler(self: Any, stop: asyncio.Event) -> None:
    """Maintain one pre-existing confirmed head without consuming candidate capacity.

    v7 disabled the older sampler to reduce RPC pressure. Exact-release telemetry now
    proves the websocket receipt itself can arrive tens of seconds behind a fresh
    head. Restore one bounded sampler on the already-isolated live-poll RPC plane.
    It is standby/background work, never candidate/critical work, and never runs an
    RPC synchronously from the launch receipt handler.
    """

    while not stop.is_set():
        try:
            with governor.rpc_workload(governor.WORKLOAD_STANDBY):
                await reference._sample_reference_once(self)
        except asyncio.CancelledError:
            raise
        except Exception:
            _inc(self, "reference_sampler_errors")
        try:
            await asyncio.wait_for(stop.wait(), timeout=REFERENCE_SAMPLE_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            continue


def _capture_hybrid_frontier(
    self: Any,
    signature: str,
    launch_slot: int,
    receipt_monotonic: float,
) -> bool:
    if _ORIGINAL_WS_CAPTURE is None:
        raise RuntimeError("post224 hybrid frontier is not installed")
    ws_ok = bool(_ORIGINAL_WS_CAPTURE(self, signature, launch_slot, receipt_monotonic))
    ref_ok = False
    try:
        ref_ok = bool(reference._capture_preexisting_reference(self, signature, launch_slot))
    except Exception:
        _inc(self, "reference_capture_errors")
    if ws_ok:
        _inc(self, "ws_reference_captures")
    if ref_ok:
        _inc(self, "confirmed_reference_captures")
    return ws_ok or ref_ok


async def _hydrate_with_hybrid_reference(
    self: Any,
    *,
    mint: str,
    source: str,
    launch_signature: str,
    created_at: datetime,
) -> tuple[int, bool, int]:
    if _ORIGINAL_WS_HYDRATE is None:
        raise RuntimeError("post224 hybrid hydration is not installed")
    result = await _ORIGINAL_WS_HYDRATE(
        self,
        mint=mint,
        source=source,
        launch_signature=launch_signature,
        created_at=created_at,
    )

    # The selected reference head was frozen before the launch receipt. Resolve its
    # block time only during context hydration, never in the websocket receipt path.
    row = reference._reference_row(self.store, launch_signature)
    if isinstance(row, dict):
        try:
            launch_slot = int(row.get("launch_slot") or 0)
            head_slot = int(row.get("reference_head_slot") or 0)
        except (TypeError, ValueError):
            launch_slot = 0
            head_slot = 0
        if head_slot > launch_slot > 0 and row.get("reference_head_block_time") is None:
            try:
                with governor.rpc_workload(governor.WORKLOAD_STANDBY):
                    value, _provider, _latency = await live_poll._poll_rpc(self).call_with_meta(
                        "getBlockTime", [head_slot], hedge=True
                    )
                if value is None:
                    raise RuntimeError("confirmed reference blockTime unavailable")
                reference._set_reference_block_time(
                    self.store, launch_signature, block_time=float(value)
                )
                _inc(self, "reference_block_times_complete")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                reference._set_reference_block_time(
                    self.store,
                    launch_signature,
                    block_time=None,
                    error_type=type(exc).__name__,
                )
                _inc(self, "reference_block_times_failed")
        elif head_slot > 0 and row.get("reference_head_block_time") is None:
            reference._set_reference_block_time(
                self.store, launch_signature, block_time=float(created_at.timestamp())
            )
    return result


def _hybrid_frontier_lag_seconds(
    store: Any,
    *,
    signature: str,
    created_at: datetime,
    max_age_seconds: float,
) -> tuple[float | None, str]:
    if _ORIGINAL_WS_LAG is None:
        raise RuntimeError("post224 hybrid timing proof is not installed")
    ws_lag, ws_proof = _ORIGINAL_WS_LAG(
        store,
        signature=signature,
        created_at=created_at,
        max_age_seconds=max_age_seconds,
    )
    ref_lag, ref_proof = reference._reference_lag_seconds(
        store, signature=signature, created_at=created_at
    )
    proofs = [
        (float(lag), proof)
        for lag, proof in ((ws_lag, ws_proof), (ref_lag, ref_proof))
        if lag is not None
    ]
    if not proofs:
        return None, f"{ws_proof}|{ref_proof}"
    # Both are conservative, independently pre-existing proofs. The tighter valid
    # upper bound remains a conservative upper bound; no threshold is relaxed.
    lag, proof = min(proofs, key=lambda item: item[0])
    return lag, f"hybrid:{proof}"


def _kind(target: WatchTarget) -> str:
    return str(getattr(target, "kind", "") or "")


def _isolated_target_shards(
    targets: tuple[WatchTarget, ...],
    provider: str,
    targets_per_socket: int | None = None,
) -> tuple[tuple[WatchTarget, ...], ...]:
    """Keep scouts isolated and cap high-volume program fan-in per physical socket."""

    scouts = tuple(target for target in targets if _kind(target) == "scout")
    programs = tuple(target for target in targets if _kind(target) != "scout")
    rotated_programs = ws_shards._rotated_targets(programs, provider)
    shards: list[tuple[WatchTarget, ...]] = []
    if scouts:
        # Three frozen scouts share one low-volume socket; program firehose traffic
        # cannot starve their keepalive or notification reader.
        shards.append(scouts)
    for index in range(0, len(rotated_programs), PROGRAM_TARGETS_PER_SOCKET):
        shards.append(tuple(rotated_programs[index : index + PROGRAM_TARGETS_PER_SOCKET]))
    return tuple(shards)


def _tracked_accounts(result: Any, configured: tuple[str, ...]) -> list[str]:
    allowed = {str(value) for value in configured if str(value)}
    return list(
        dict.fromkeys(
            pubkey
            for pubkey, _signer, _index in scout._account_entries(result)
            if pubkey in allowed
        )
    )


def _normalize_with_independent_multi_scout_resolution(
    result: Any,
    *,
    signature: str,
    trigger_received_at: datetime,
    source_hint: str | None = None,
) -> Any:
    if _ORIGINAL_NORMALIZE is None:
        raise RuntimeError("post224 multi-scout normalizer is not installed")
    plane = scout._SCOUT_HYDRATION_PLANE.get()
    if plane is None or source_hint is not None:
        return _ORIGINAL_NORMALIZE(
            result,
            signature=signature,
            trigger_received_at=trigger_received_at,
            source_hint=source_hint,
        )

    accounts = _tracked_accounts(result, tuple(getattr(plane, "scout_wallets", ()) or ()))
    if len(accounts) <= 1:
        return _ORIGINAL_NORMALIZE(
            result,
            signature=signature,
            trigger_received_at=trigger_received_at,
            source_hint=source_hint,
        )

    _inc(plane, "multi_scout_transactions")
    valid = []
    failures: dict[str, str] = {}
    for wallet in accounts:
        swap, error = scout._normalize_tracked_wallet(
            result,
            signature=signature,
            trigger_received_at=trigger_received_at,
            wallet=wallet,
            source_hint=None,
        )
        if swap is not None:
            valid.append(swap)
        else:
            failures[wallet] = str(error or "unresolved")

    if len(valid) == 1:
        # Multiple tracked accounts were merely present, but exactly one has proven
        # economic movement. Recover that candidate without inventing confirmation.
        scout._inc(plane, "normalization_attempts")
        scout._inc(plane, "normalization_complete")
        _inc(plane, "multi_scout_single_economic_actor_resolved")
        return valid[0]

    scout._inc(plane, "normalization_attempts")
    scout._inc(plane, "normalization_failed")
    reason = (
        "multiple_tracked_scouts_economically_active"
        if len(valid) > 1
        else "multiple_tracked_scout_accounts_no_unique_economic_actor"
    )
    scout._normalization_failures(plane)[reason] += 1
    _inc(plane, reason)
    setattr(
        plane,
        "_roi_post224_frontier_last_multi_scout_failure",
        {"signature": signature, "valid_actor_count": len(valid), "failure_count": len(failures)},
    )
    return None


def _status_with_post224(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("post224 frontier hardening status is not installed")
    payload = _ORIGINAL_STATUS(self)
    bridge = payload.get("launch_coverage_bridge")
    if isinstance(bridge, dict):
        bridge.update(
            {
                "near_creation_reference_transport": "hybrid-preexisting-websocket-plus-continuous-confirmed-head",
                "near_creation_legacy_reference_rpc_sampler_enabled": True,
                "reference_sample_interval_seconds": REFERENCE_SAMPLE_INTERVAL_SECONDS,
                "reference_sampler_workload": "standby",
                "reference_sampler_uses_isolated_live_poll_rpc": True,
                "near_creation_launch_path_rpc_reads": False,
                "near_creation_threshold_unchanged": True,
            }
        )
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "launch_near_creation_continuous_getslot_sampler": True,
                "launch_near_creation_hybrid_preexisting_proof": True,
                "strategy_critical_scout_shards_isolated": True,
                "high_volume_program_targets_per_socket": PROGRAM_TARGETS_PER_SOCKET,
                "provider_scope_unchanged": True,
                "certification_thresholds_unchanged": True,
                "paper_only_authority_unchanged": True,
            }
        )
    payload["post224_frontier_candidate_hardening"] = {
        "installed": True,
        "repair_version": REPAIR_VERSION,
        "reference_sampler_interval_seconds": REFERENCE_SAMPLE_INTERVAL_SECONDS,
        "confirmed_reference_captures": int(getattr(self, "_roi_post224_frontier_confirmed_reference_captures", 0) or 0),
        "ws_reference_captures": int(getattr(self, "_roi_post224_frontier_ws_reference_captures", 0) or 0),
        "reference_block_times_complete": int(getattr(self, "_roi_post224_frontier_reference_block_times_complete", 0) or 0),
        "reference_block_times_failed": int(getattr(self, "_roi_post224_frontier_reference_block_times_failed", 0) or 0),
        "multi_scout_transactions": int(getattr(self, "_roi_post224_frontier_multi_scout_transactions", 0) or 0),
        "multi_scout_single_economic_actor_resolved": int(getattr(self, "_roi_post224_frontier_multi_scout_single_economic_actor_resolved", 0) or 0),
        "multiple_tracked_scouts_economically_active": int(getattr(self, "_roi_post224_frontier_multiple_tracked_scouts_economically_active", 0) or 0),
        "program_targets_per_socket": PROGRAM_TARGETS_PER_SOCKET,
        "scout_socket_isolated_from_program_firehose": True,
        "exact_durable_boundary_generation_checks_unchanged": True,
        "candidate_processing_target_seconds_unchanged": 5.0,
        "candidate_entry_window_seconds_unchanged": 20.0,
        "launch_near_creation_threshold_seconds_unchanged": 3.0,
        "provider_count_unchanged": True,
        "full_market_scope_reduced": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    return payload


def install_post224_frontier_candidate_hardening() -> None:
    global _INSTALLED, _ORIGINAL_WS_CAPTURE, _ORIGINAL_WS_HYDRATE, _ORIGINAL_WS_LAG
    global _ORIGINAL_TARGET_SHARDS, _ORIGINAL_NORMALIZE, _ORIGINAL_STATUS
    if _INSTALLED:
        return

    # Restore the pre-existing confirmed-head sampler after v7 intentionally parked
    # it. The existing DirectSolanaIngestionPlane.run wrapper resolves this global
    # dynamically, so no second runtime task topology is introduced.
    reference._reference_sampler = _confirmed_reference_sampler  # type: ignore[assignment]

    _ORIGINAL_WS_CAPTURE = ws_timing._capture_preexisting_frontier
    ws_timing._capture_preexisting_frontier = _capture_hybrid_frontier  # type: ignore[assignment]

    _ORIGINAL_WS_HYDRATE = launch_bridge._hydrate_mint_launch_context
    launch_bridge._hydrate_mint_launch_context = _hydrate_with_hybrid_reference  # type: ignore[assignment]

    _ORIGINAL_WS_LAG = ws_timing._ws_frontier_lag_seconds
    ws_timing._ws_frontier_lag_seconds = _hybrid_frontier_lag_seconds  # type: ignore[assignment]

    _ORIGINAL_TARGET_SHARDS = ws_shards._target_shards
    ws_shards._target_shards = _isolated_target_shards  # type: ignore[assignment]

    _ORIGINAL_NORMALIZE = direct_module.normalize_standard_transaction
    direct_module.normalize_standard_transaction = _normalize_with_independent_multi_scout_resolution  # type: ignore[assignment]

    _ORIGINAL_STATUS = DirectSolanaIngestionPlane.status
    try:
        _status_with_post224.__dict__.update(getattr(_ORIGINAL_STATUS, "__dict__", {}))
    except Exception:
        pass
    setattr(_status_with_post224, "_roi_post224_frontier_candidate_hardening", True)
    DirectSolanaIngestionPlane.status = _status_with_post224  # type: ignore[method-assign]
    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "install_post224_frontier_candidate_hardening",
]
