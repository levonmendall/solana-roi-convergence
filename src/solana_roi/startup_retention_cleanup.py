from __future__ import annotations

"""One-shot production startup maintenance for already-proven stale artifacts."""

from typing import Any

from .safe_retention_cleanup import install_safe_retention_cleanup as _run_cleanup


def run_safe_retention_cleanup(app: Any, ingestion_runtime: Any) -> dict[str, Any]:
    """Run the guarded artifact cleanup without adding a production installer chain."""

    return _run_cleanup(app, ingestion_runtime)
