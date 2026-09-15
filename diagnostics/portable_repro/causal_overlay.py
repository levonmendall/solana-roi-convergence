from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

NORMAL_MODE = "normal"
HYDRATION_BOOTSTRAP_PAUSED_MODE = "hydration-bootstrap-paused"
ALLOWED_MODES = {NORMAL_MODE, HYDRATION_BOOTSTRAP_PAUSED_MODE}


def apply_hydration_causal_overlay(module: Any, *, mode: str) -> dict[str, Any]:
    """Apply a harness-only causal control without changing canonical production source.

    In paused mode only the incremental hydration-history bootstrap scan is suppressed.
    The status path remains fail-closed because bootstrap completion is never fabricated:
    `_recent_metrics()` receives `(False, 0, cursor)` and therefore publishes no exact
    hydration sample until the real bootstrap is reintroduced.
    """
    if mode not in ALLOWED_MODES:
        raise RuntimeError(f"unsupported PORTABLE_REPRO_CAUSAL_MODE: {mode}")

    evidence: dict[str, Any] = {
        "mode": mode,
        "overlay_applied": False,
        "canonical_source_mutated": False,
        "source_history_mutated": False,
        "paper_only": True,
        "live_money_authority": False,
        "bootstrap_completion_fabricated": False,
    }
    if mode == NORMAL_MODE:
        return evidence

    original = module._advance_bootstrap

    def _paused_advance_bootstrap(self: Any) -> tuple[bool, int, int]:
        module._ensure_state(self)
        state = module._meta(self)
        # Deliberately do not advance the cursor and do not mark completion.  This is
        # an A/B diagnostic control, not a production repair.
        return False, 0, int(state.get("bootstrap_rowid", 0) or 0)

    setattr(_paused_advance_bootstrap, "_portable_repro_original", original)
    module._advance_bootstrap = _paused_advance_bootstrap
    evidence["overlay_applied"] = True
    evidence["suppressed_producer"] = "direct-solana-hydration-status:bootstrap"
    evidence["fail_closed"] = True
    return evidence


def write_causal_evidence(evidence: dict[str, Any]) -> None:
    out = Path(os.environ.get("PORTABLE_REPRO_OUTPUT", "/evidence"))
    out.mkdir(parents=True, exist_ok=True)
    target = out / "causal-mode.json"
    target.write_text(json.dumps(evidence, sort_keys=True) + "\n", encoding="utf-8")


__all__ = [
    "ALLOWED_MODES",
    "HYDRATION_BOOTSTRAP_PAUSED_MODE",
    "NORMAL_MODE",
    "apply_hydration_causal_overlay",
    "write_causal_evidence",
]
