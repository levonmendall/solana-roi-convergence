from __future__ import annotations

import asyncio
import time
from typing import Any, Callable

from . import candidate_hydration_work_conserving_repair as hydration
from . import forward_evidence_runtime_repair as forward
from . import runtime_guards as guards
from . import target_stream_fanout as fanout
from .direct_solana import DirectSolanaIngestionPlane


_ORIGINAL_FORWARD_STATUS: Callable[..., dict[str, Any]] | None = None


async def _compat_candidate_worker(
    self: Any,
    stop: asyncio.Event,
    *,
    fast_only: bool,
) -> None:
    """Preserve the canonical claim seam for lightweight/offline test planes.

    Real production journals expose ``store`` and use the new bounded-admission
    scheduler. Older unit/offline callers deliberately supply an opaque journal and
    monkeypatch ``runtime_guards._claim_priority``; keep that contract intact so the
    architecture repair does not silently replace a long-standing composition seam.
    """

    journal = getattr(self, "journal", None)
    if getattr(journal, "store", None) is not None:
        await forward._bounded_candidate_worker(self, stop, fast_only=fast_only)
        return

    next_cleanup = 0.0
    while not stop.is_set():
        if not fast_only and time.monotonic() >= next_cleanup:
            guards._expire_stale_background(self)
            next_cleanup = time.monotonic() + 5.0

        if fast_only:
            row = guards._claim_priority(self.journal, fast_only=True)
            lane = "candidate_reserved" if row is not None else "none"
        elif hydration._background_worker_can_flex(self):
            row = guards._claim_priority(self.journal, fast_only=True)
            if row is not None:
                lane = "candidate_flex"
            else:
                row = guards._claim_priority(self.journal, fast_only=False)
                lane = "background" if row is not None else "none"
        else:
            row = guards._claim_priority(self.journal, fast_only=False)
            lane = "background" if row is not None else "none"

        if row is None:
            await asyncio.sleep(0.01 if fast_only else 0.025)
            continue
        if lane == "candidate_reserved":
            hydration._increment(self, "reserved_candidate_claims")
        elif lane == "candidate_flex":
            hydration._increment(self, "flex_candidate_claims")
        elif lane == "background":
            hydration._increment(self, "background_claims")
        await self._hydrate_one(row)


try:
    _compat_candidate_worker.__dict__.update(
        getattr(forward._bounded_candidate_worker, "__dict__", {})
    )
except Exception:
    pass
setattr(_compat_candidate_worker, "_roi_candidate_work_conserving", True)
setattr(_compat_candidate_worker, "_roi_forward_evidence_bounded_admission", True)
setattr(_compat_candidate_worker, "_roi_forward_evidence_compatibility", True)


def _status_without_runtime_service(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    """Keep intrinsic/minimal DirectSolana status construction valid.

    Production planes always have ``service``. Several repository contracts create
    a minimal plane solely to inspect intrinsic transport/memory telemetry. The new
    forward-evidence overlay must therefore treat the collector plane as optional,
    rather than making unrelated status surfaces depend on runtime construction.
    """

    if _ORIGINAL_FORWARD_STATUS is None:
        raise RuntimeError("forward evidence compatibility is not installed")
    if hasattr(self, "service"):
        return _ORIGINAL_FORWARD_STATUS(self)

    base = forward._ORIGINAL_DIRECT_STATUS
    if base is None:
        raise RuntimeError("forward evidence base status unavailable")
    payload = base(self)
    payload["forward_evidence_runtime"] = {
        "installed": True,
        "candidate_max_inflight": forward.CANDIDATE_MAX_INFLIGHT,
        "candidate_active": int(getattr(self, "_roi_forward_evidence_active_candidates", 0) or 0),
        "collector_plane_available": False,
        "point_in_time_prewarm_only": True,
        "evidence_backdating_allowed": False,
        "candidate_latency_threshold_unchanged": True,
        "candidate_entry_window_unchanged": True,
        "coverage_thresholds_unchanged": True,
        "paper_only_authority_unchanged": True,
        "signing_or_submission_available": False,
    }
    return payload


setattr(_status_without_runtime_service, "_roi_forward_evidence_status", True)
setattr(_status_without_runtime_service, "_roi_forward_evidence_compatibility", True)


def install_forward_evidence_compatibility() -> None:
    global _ORIGINAL_FORWARD_STATUS

    # Candidate hydration module is part of the repository's public composition
    # surface, so keep all three imported references aligned.
    hydration._work_conserving_reserved_worker = _compat_candidate_worker  # type: ignore[assignment]
    guards._reserved_worker = _compat_candidate_worker  # type: ignore[assignment]
    fanout._reserved_worker = _compat_candidate_worker  # type: ignore[assignment]

    current = DirectSolanaIngestionPlane.status
    if not bool(getattr(current, "_roi_forward_evidence_compatibility", False)):
        _ORIGINAL_FORWARD_STATUS = current
        try:
            _status_without_runtime_service.__dict__.update(getattr(current, "__dict__", {}))
        except Exception:
            pass
        setattr(_status_without_runtime_service, "_roi_forward_evidence_status", True)
        setattr(_status_without_runtime_service, "_roi_forward_evidence_compatibility", True)
        DirectSolanaIngestionPlane.status = _status_without_runtime_service  # type: ignore[method-assign]


__all__ = ["install_forward_evidence_compatibility"]
