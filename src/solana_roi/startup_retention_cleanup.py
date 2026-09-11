from __future__ import annotations

"""One-shot production startup maintenance for already-proven stale artifacts."""

import json
import logging
from typing import Any

from .safe_retention_cleanup import install_safe_retention_cleanup as _run_cleanup


_LOG = logging.getLogger("solana_roi.safe_retention")


def run_safe_retention_cleanup(app: Any, ingestion_runtime: Any) -> dict[str, Any]:
    """Run guarded artifact cleanup and emit bounded production evidence."""

    state = _run_cleanup(app, ingestion_runtime)
    cleanup = dict(state.get("startup_stale_export_cleanup") or {})
    evidence = {
        "version": state.get("version"),
        "scope": list(state.get("scope") or ()),
        "examined": int(cleanup.get("examined") or 0),
        "removed": int(cleanup.get("removed") or 0),
        "skipped": int(cleanup.get("skipped") or 0),
        "scan_truncated": bool(cleanup.get("scan_truncated", False)),
        "outcomes": dict(cleanup.get("outcomes") or {}),
        "error": cleanup.get("error"),
        "paper_only": bool(state.get("paper_only", True)),
        "live_money_authority": bool(state.get("live_money_authority", False)),
        "signing_available": bool(state.get("signing_available", False)),
        "transaction_submission_available": bool(
            state.get("transaction_submission_available", False)
        ),
    }
    _LOG.info(
        "ROI_SAFE_RETENTION_CLEANUP %s",
        json.dumps(evidence, sort_keys=True, separators=(",", ":")),
    )
    return state
