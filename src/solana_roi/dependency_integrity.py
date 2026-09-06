from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


_REPO_ROOT = Path(__file__).resolve().parents[2]
_LOCK_PATH = _REPO_ROOT / "requirements.lock"
_COMPATIBILITY_PATH = _REPO_ROOT / "dependency_compatibility.json"


def _sha256(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def _compatibility_manifest() -> dict[str, Any]:
    try:
        value = json.loads(_COMPATIBILITY_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(value) if isinstance(value, dict) else {}


def dependency_integrity_status() -> dict[str, Any]:
    lock_sha256 = _sha256(_LOCK_PATH)
    manifest = _compatibility_manifest()
    reviewed_sha256 = str(manifest.get("requirements_lock_sha256") or "").strip() or None
    lock_present = lock_sha256 is not None
    compatibility_manifest_present = bool(manifest)
    compatibility_review_matches_lock = bool(
        lock_sha256 and reviewed_sha256 and lock_sha256 == reviewed_sha256
    )
    return {
        "lock_file": "requirements.lock",
        "requirements_lock_present": lock_present,
        "requirements_lock_sha256": lock_sha256,
        "compatibility_manifest": "dependency_compatibility.json",
        "compatibility_manifest_present": compatibility_manifest_present,
        "reviewed_requirements_lock_sha256": reviewed_sha256,
        "compatibility_review_matches_lock": compatibility_review_matches_lock,
        "review_epoch": manifest.get("review_epoch"),
        "architecture_release": manifest.get("architecture_release"),
        "execution_compatibility_review": manifest.get("execution_compatibility_review"),
        "measurement_compatibility_review": manifest.get("measurement_compatibility_review"),
        "strategy_economic_authority_changed": bool(
            manifest.get("strategy_economic_authority_changed", False)
        ),
        "deterministic_production_install": True,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = ["dependency_integrity_status"]
