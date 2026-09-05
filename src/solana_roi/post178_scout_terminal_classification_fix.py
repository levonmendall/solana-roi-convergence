from __future__ import annotations

from typing import Any, Callable

from . import release_bound_scout_classification_repair as release_bound
from . import scout_candidate_continuity_repair as scout


REPAIR_VERSION = "post178-scout-terminal-classification-v1"
_ORIGINAL_TRACKED_NORMALIZER: Callable[..., Any] | None = None


def _tracked_normalizer_with_terminal_noncopyable(
    result: Any,
    *,
    signature: str,
    trigger_received_at: Any,
    wallet: str,
    source_hint: str | None = None,
) -> tuple[Any, str | None]:
    if _ORIGINAL_TRACKED_NORMALIZER is None:
        raise RuntimeError("post-178 tracked scout terminal classification is not installed")
    swap, error = _ORIGINAL_TRACKED_NORMALIZER(
        result,
        signature=signature,
        trigger_received_at=trigger_received_at,
        wallet=wallet,
        source_hint=source_hint,
    )
    if swap is not None or str(error or "") != "economic_movement_price_unresolved":
        return swap, error

    plane = scout._SCOUT_HYDRATION_PLANE.get()
    if plane is None:
        return swap, error
    if release_bound._terminal_classification(plane.store, signature) is not None:
        return swap, error

    release_bound._record_terminal_non_candidate(
        plane.store,
        signature=signature,
        trigger_received_at=trigger_received_at,
        reason="economic_movement_price_unresolved_noncopyable",
    )
    setattr(
        plane,
        "_roi_post178_economic_movement_noncopyable_classifications",
        int(getattr(plane, "_roi_post178_economic_movement_noncopyable_classifications", 0) or 0) + 1,
    )
    return swap, error


setattr(_tracked_normalizer_with_terminal_noncopyable, "_roi_post178_terminal_noncopyable_runtime", True)


def install_post178_scout_terminal_classification_fix() -> None:
    global _ORIGINAL_TRACKED_NORMALIZER
    current = scout._normalize_tracked_wallet
    if bool(getattr(current, "_roi_post178_terminal_noncopyable_runtime", False)):
        return
    _ORIGINAL_TRACKED_NORMALIZER = current
    try:
        _tracked_normalizer_with_terminal_noncopyable.__dict__.update(getattr(current, "__dict__", {}))
    except Exception:
        pass
    setattr(_tracked_normalizer_with_terminal_noncopyable, "_roi_post178_terminal_noncopyable_runtime", True)
    scout._normalize_tracked_wallet = _tracked_normalizer_with_terminal_noncopyable  # type: ignore[assignment]


__all__ = [
    "REPAIR_VERSION",
    "_tracked_normalizer_with_terminal_noncopyable",
    "install_post178_scout_terminal_classification_fix",
]
