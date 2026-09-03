from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime
from typing import Any

from . import launch_coverage_bridge as bridge
from . import launch_reference_timing_repair as reference
from . import live_poll_redundancy as live_poll
from .coverage_completeness_repair import _launch_contexts
from .direct_solana import DirectSolanaIngestionPlane


_PREVIOUS_TIMING_LAG = reference._timing_lag_with_v4_compatibility


async def _hydrate_mint_launch_context_with_signed_reference(
    self: Any,
    *,
    mint: str,
    source: str,
    launch_signature: str,
    created_at: datetime,
) -> tuple[int, bool, int]:
    """Resolve the immutable pre-receipt reference head's actual blockTime.

    The reference slot was selected before the launch receipt, so resolving that
    already-fixed slot later cannot move the timing reference. Fetching its real
    blockTime for *all* relative slot orderings lets the proof account for chain
    progress between the reference head and the launch instead of conservatively
    charging the whole interval as observer lateness.
    """

    persisted, complete, candidate_count = await reference._PRE_CHAIN_HYDRATE_MINT_LAUNCH_CONTEXT(
        self,
        mint=mint,
        source=source,
        launch_signature=launch_signature,
        created_at=created_at,
    )

    row = reference._reference_row(self.store, launch_signature)
    if isinstance(row, dict):
        try:
            head_slot = int(row.get("reference_head_slot") or 0)
        except (TypeError, ValueError):
            head_slot = 0
        if head_slot > 0:
            try:
                value, _provider, _latency = await live_poll._poll_rpc(self).call_with_meta(
                    "getBlockTime",
                    [head_slot],
                    hedge=True,
                )
                if value is None:
                    raise RuntimeError("preexisting reference head blockTime unavailable")
                reference._set_reference_block_time(
                    self.store,
                    launch_signature,
                    block_time=float(value),
                )
                reference._increment(self, "reference_block_times_complete")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                reference._set_reference_block_time(
                    self.store,
                    launch_signature,
                    block_time=None,
                    error_type=type(exc).__name__,
                )
                reference._increment(self, "reference_block_times_failed")

    raw = bridge._raw_collectors(self)
    launch = getattr(raw, "launch", None)
    if launch is not None:
        context = _launch_contexts(launch).get(mint)
        if isinstance(context, dict):
            context["launch_signature"] = launch_signature
            context["launch_slot"] = int(
                (reference._reference_row(self.store, launch_signature) or {}).get("launch_slot") or 0
            )
    return persisted, complete, candidate_count


def _reference_lag_seconds_signed(
    store: Any,
    *,
    signature: str,
    created_at: datetime,
) -> tuple[float | None, str]:
    row = reference._reference_row(store, signature)
    if not isinstance(row, dict):
        return None, "missing_preexisting_chain_reference"
    if str(row.get("status") or "") != "complete":
        return None, "incomplete_preexisting_chain_reference"
    try:
        launch_slot = int(row.get("launch_slot") or 0)
        head_slot = int(row.get("reference_head_slot") or 0)
        rpc_seconds = max(0.0, float(row.get("reference_rpc_latency_ms") or 0.0) / 1000.0)
        age_seconds = max(0.0, float(row.get("reference_age_ms") or 0.0) / 1000.0)
        head_block_time = float(row.get("reference_head_block_time"))
    except (TypeError, ValueError):
        return None, "invalid_preexisting_chain_reference"
    if launch_slot <= 0 or head_slot <= 0:
        return None, "invalid_preexisting_chain_reference"

    launch_block_time = created_at.timestamp()
    if head_slot > launch_slot and head_block_time < launch_block_time:
        return None, "non_monotonic_reference_block_time"
    if head_slot < launch_slot and head_block_time > launch_block_time:
        return None, "non_monotonic_reference_block_time"

    # Let m0..m1 be the measured reference RPC request interval and mr the launch
    # receipt monotonic time. The returned head existed at some point inside m0..m1.
    # Therefore a conservative lateness upper bound is:
    #   (mr - m1) + (m1 - m0) + (head_chain_time - launch_chain_time)
    # = reference_age + RPC_RTT + signed_chain_delta.
    # When the preexisting head is earlier than the launch, the immutable chain
    # progress is subtracted. This tightens the proof without subtracting transport
    # uncertainty or changing the fixed three-second threshold.
    signed_chain_delta = head_block_time - launch_block_time
    upper_bound = max(0.0, age_seconds + rpc_seconds + signed_chain_delta)
    return upper_bound, "preexisting-confirmed-head-signed-chain-upper-bound"


def _timing_lag_with_signed_reference(
    store: Any,
    *,
    signature: str,
    created_at: datetime,
) -> tuple[float | None, str, str]:
    """Use v6 only for completed production reference rows.

    Keep the published v5 helper contract intact for direct/offline callers and
    regressions. Production v6 hydration resolves the fixed reference head's actual
    blockTime and marks that row complete before the launch collector evaluates it.
    """

    row = reference._reference_row(store, signature)
    if isinstance(row, dict) and str(row.get("status") or "") == "complete":
        lag, proof = _reference_lag_seconds_signed(
            store,
            signature=signature,
            created_at=created_at,
        )
        return (
            lag,
            proof,
            "program-wide-swaps+preexisting-confirmed-chain-reference-signed-delta:launch-window-v6",
        )
    return _PREVIOUS_TIMING_LAG(
        store,
        signature=signature,
        created_at=created_at,
    )


setattr(_timing_lag_with_signed_reference, "_roi_signed_chain_delta", True)


def _status_with_signed_reference(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        launch_bridge = payload.get("launch_coverage_bridge")
        if isinstance(launch_bridge, dict):
            launch_bridge.update(
                {
                    "coverage_semantics": "preexisting-confirmed-head-signed-chain-upper-bound+immutable-window-acquisition-v6",
                    "near_creation_timing_model": "preexisting-confirmed-head-signed-chain-upper-bound-at-first-live-receipt",
                    "near_creation_reference_block_time_required": True,
                    "near_creation_reference_chain_delta_signed": True,
                    "near_creation_reference_chain_progress_subtracted": True,
                    "near_creation_reference_uncertainty_includes_rpc_rtt": True,
                    "near_creation_reference_uncertainty_includes_sample_age": True,
                    "near_creation_threshold_unchanged": True,
                    "near_creation_missing_timing_proof_fails_closed": True,
                    "candidate_activation_from_launch_bridge": False,
                    "latency_samples_from_launch_bridge": False,
                    "quote_samples_from_launch_bridge": False,
                    "paper_authority": False,
                }
            )
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "launch_near_creation_signed_chain_delta_bound": True,
                    "launch_near_creation_reference_block_time_required": True,
                    "launch_near_creation_threshold_unchanged": True,
                    "launch_near_creation_missing_proof_fails_closed": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_launch_reference_signed_delta_repair", True)
    return status


def install_launch_reference_signed_delta_repair() -> None:
    bridge._hydrate_mint_launch_context = _hydrate_mint_launch_context_with_signed_reference  # type: ignore[assignment]

    current_timing = reference._timing_lag_with_v4_compatibility
    if not bool(getattr(current_timing, "_roi_signed_chain_delta", False)):
        reference._timing_lag_with_v4_compatibility = _timing_lag_with_signed_reference  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_launch_reference_signed_delta_repair", False)):
        DirectSolanaIngestionPlane.status = _status_with_signed_reference(current_status)  # type: ignore[method-assign]


__all__ = [
    "install_launch_reference_signed_delta_repair",
    "_hydrate_mint_launch_context_with_signed_reference",
    "_reference_lag_seconds_signed",
    "_timing_lag_with_signed_reference",
]
