from __future__ import annotations

import math
from collections.abc import Callable
from datetime import datetime
from typing import Any

from . import launch_reference_timing_repair as reference
from . import launch_ws_frontier_timing_repair as ws_timing
from . import post224_frontier_candidate_hardening as hardening
from . import public_ws_shard_transport_repair as ws_shards
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget

REPAIR_VERSION = "post226-compatibility-finalize-v1"
PROGRAM_SHARD_COUNT = 2
MAX_PHYSICAL_SHARDS_WITH_SCOUTS = 3

_ORIGINAL_STATUS: Callable[[Any], dict[str, Any]] | None = None
_INSTALLED = False


def _compat_hybrid_frontier_lag_seconds(
    store: Any,
    *,
    signature: str,
    created_at: datetime,
    max_age_seconds: float,
) -> tuple[float | None, str]:
    """Preserve v7 proof vocabulary while allowing the new reference to improve it."""
    original_ws = hardening._ORIGINAL_WS_LAG
    if original_ws is None:
        raise RuntimeError("post224 websocket timing delegate missing")
    ws_lag, ws_proof = original_ws(
        store,
        signature=signature,
        created_at=created_at,
        max_age_seconds=max_age_seconds,
    )

    # No post224 reference row means the legacy websocket helper contract is exact.
    # This preserves all established proof strings and fail-closed semantics.
    if reference._reference_row(store, signature) is None:
        return ws_lag, ws_proof

    ref_lag, ref_proof = reference._reference_lag_seconds(
        store, signature=signature, created_at=created_at
    )
    if ws_lag is not None and (ref_lag is None or float(ws_lag) <= float(ref_lag)):
        return float(ws_lag), ws_proof
    if ref_lag is not None:
        return float(ref_lag), f"hybrid-confirmed-reference:{ref_proof}"
    return None, f"{ws_proof}|{ref_proof}"


def _kind(target: WatchTarget) -> str:
    return str(getattr(target, "kind", "") or "")


def _three_socket_target_shards(
    targets: tuple[WatchTarget, ...],
    provider: str,
    targets_per_socket: int | None = None,
) -> tuple[tuple[WatchTarget, ...], ...]:
    """Use one scout-only socket plus at most two balanced program sockets."""
    scouts = tuple(target for target in targets if _kind(target) == "scout")
    programs = tuple(target for target in targets if _kind(target) != "scout")
    if not scouts:
        original = hardening._ORIGINAL_TARGET_SHARDS
        if original is None:
            raise RuntimeError("post224 shard delegate missing")
        return original(targets, provider, targets_per_socket)

    rotated_programs = ws_shards._rotated_targets(programs, provider)
    shards: list[tuple[WatchTarget, ...]] = [scouts]
    if rotated_programs:
        count = min(PROGRAM_SHARD_COUNT, len(rotated_programs))
        size = max(1, math.ceil(len(rotated_programs) / count))
        for index in range(0, len(rotated_programs), size):
            shards.append(tuple(rotated_programs[index : index + size]))
    return tuple(shards)


def _status_with_compatibility(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("post226 compatibility status not installed")
    payload = _ORIGINAL_STATUS(self)
    targets = tuple(getattr(self, "watch_targets", ()) or ())
    program_count = sum(1 for target in targets if _kind(target) != "scout")
    balanced_program_size = math.ceil(program_count / PROGRAM_SHARD_COUNT) if program_count else 0

    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "strategy_critical_scout_shards_isolated": True,
                "public_program_shard_count": PROGRAM_SHARD_COUNT,
                "public_physical_shards_with_scouts_max": MAX_PHYSICAL_SHARDS_WITH_SCOUTS,
                "high_volume_program_targets_per_socket": balanced_program_size,
                "legacy_websocket_proof_vocabulary_preserved": True,
                "certification_thresholds_unchanged": True,
                "paper_only_authority_unchanged": True,
            }
        )

    hardening_status = payload.get("post224_frontier_candidate_hardening")
    if isinstance(hardening_status, dict):
        hardening_status.update(
            {
                "compatibility_finalize_version": REPAIR_VERSION,
                "program_shard_count": PROGRAM_SHARD_COUNT,
                "program_targets_per_socket": balanced_program_size,
                "max_physical_shards_with_scouts": MAX_PHYSICAL_SHARDS_WITH_SCOUTS,
                "legacy_websocket_proof_vocabulary_preserved": True,
            }
        )

    payload["post226_compatibility_finalize"] = {
        "installed": True,
        "repair_version": REPAIR_VERSION,
        "scout_socket_count": 1 if any(_kind(target) == "scout" for target in targets) else 0,
        "program_shard_count": PROGRAM_SHARD_COUNT,
        "balanced_program_targets_per_socket": balanced_program_size,
        "max_physical_shards_with_scouts": MAX_PHYSICAL_SHARDS_WITH_SCOUTS,
        "legacy_websocket_proof_vocabulary_preserved": True,
        "hybrid_confirmed_reference_additive_only": True,
        "candidate_entry_window_seconds_unchanged": 20.0,
        "candidate_processing_target_seconds_unchanged": 5.0,
        "launch_near_creation_threshold_seconds_unchanged": 3.0,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    return payload


def install_post226_compatibility_finalize() -> None:
    global _ORIGINAL_STATUS, _INSTALLED
    if _INSTALLED:
        return

    # The final public helpers retain the pre-PR226 observable contracts while the
    # post224 repair remains underneath and supplies the new reference/attribution.
    ws_timing._ws_frontier_lag_seconds = _compat_hybrid_frontier_lag_seconds  # type: ignore[assignment]
    ws_shards._target_shards = _three_socket_target_shards  # type: ignore[assignment]

    _ORIGINAL_STATUS = DirectSolanaIngestionPlane.status
    try:
        _status_with_compatibility.__dict__.update(getattr(_ORIGINAL_STATUS, "__dict__", {}))
    except Exception:
        pass
    setattr(_status_with_compatibility, "_roi_post226_compatibility_finalize", True)
    DirectSolanaIngestionPlane.status = _status_with_compatibility  # type: ignore[method-assign]
    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "install_post226_compatibility_finalize",
]
